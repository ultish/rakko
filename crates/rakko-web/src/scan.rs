//! Server-side scan sessions.
//!
//! A scan walks a whole topic newest-first applying a filter and keeps a list of the
//! matches' summaries (not their payloads — those are fetched on demand). The scan is
//! owned by the server session, not by any HTTP request: a runner task advances it in
//! windows until enough matches exist for what the client has asked to see, and requests
//! just wait (briefly) on that list. Closing the tab or dropping a request never
//! corrupts or restarts a scan.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rakko_core::error::AppError;
use rakko_core::kafka::scan::{ScanProgress, ScanRange, TopicScan};
use rakko_core::message_filter::MessageFilter;
use serde::Serialize;

use crate::dto::{summarize, MessageSummary};
use crate::runtime::{schema_ids, ProfileRuntime};

/// Sorting needs every match in memory; past this many the scan stops with an error
/// asking for a narrower filter or range.
const MAX_SORT_MATCHES: usize = 200_000;

#[derive(Debug, Clone, Copy, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SortKey {
    Time,
    Offset,
    Partition,
    Key,
    Value,
}

/// A non-default ordering. The scan normally streams newest-first; any other order has to
/// see every match before the first row can be shown.
#[derive(Debug, Clone, Copy, serde::Deserialize)]
pub struct Sort {
    pub key: SortKey,
    #[serde(default)]
    pub desc: bool,
}

fn sort_matches(matches: &mut [MessageSummary], sort: Sort) {
    use std::cmp::Ordering;
    matches.sort_by(|a, b| {
        let primary = match sort.key {
            SortKey::Time => a.timestamp_ms.cmp(&b.timestamp_ms),
            SortKey::Offset => a.offset.cmp(&b.offset),
            SortKey::Partition => a.partition.cmp(&b.partition),
            SortKey::Key => a.key.preview.cmp(&b.key.preview),
            SortKey::Value => a.value.preview.cmp(&b.value.preview),
        };
        let primary = if sort.desc { primary.reverse() } else { primary };
        let tie = a.partition.cmp(&b.partition).then(a.offset.cmp(&b.offset));
        match primary {
            Ordering::Equal => tie,
            other => other,
        }
    });
}

/// How a scan reads the topic, beyond filter and range.
#[derive(Debug, Clone, Copy)]
pub struct ScanOptions {
    /// Offsets fetched per window.
    pub window: usize,
    /// Latest record per key only (see `ScanSession::latest_per_key`).
    pub table: bool,
    pub sort: Option<Sort>,
}

/// A session may have this many live scans; the oldest is dropped past it.
pub const MAX_SCANS_PER_SESSION: usize = 4;
pub const MAX_SCANS_TOTAL: usize = 64;
/// Table rows carry more of the value so the UI can split it into columns.
const TABLE_PREVIEW_CHARS: usize = 6_000;
const IDLE_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Default)]
struct ScanState {
    matches: Vec<MessageSummary>,
    progress: Option<ScanProgress>,
    done: bool,
    error: Option<String>,
    /// Messages the filter could not be evaluated on because their Avro schema was
    /// unavailable. They are *not* matches — surfaced so the UI never hides that.
    skipped_undecodable: u64,
    /// Oldest-first merge: matches read but not yet safe to release, and per partition the
    /// largest sort key read so far (over *every* message, not just matches).
    pending: Vec<MessageSummary>,
    max_seen: HashMap<i32, i64>,
}

pub struct ScanSession {
    pub id: String,
    pub owner: String,
    pub topic: String,
    profile: Arc<ProfileRuntime>,
    filter: Arc<MessageFilter>,
    range: ScanRange,
    /// Table mode: only the newest record of each key counts (a tombstone removes the key),
    /// i.e. the topic read as a KTable. Applied before the filter, so a filter can't resurrect
    /// an older version of a key whose newest version it rejects.
    sort: Option<Sort>,
    /// Set for time/offset ascending: the scan walks oldest-first and merges partitions by this key.
    ascending: Option<SortKey>,
    latest_per_key: Option<Arc<Mutex<HashSet<Vec<u8>>>>>,
    preview_chars: usize,
    scan: Mutex<Option<TopicScan>>,
    state: Mutex<ScanState>,
    /// How many matches the client has asked to see so far (`from + limit`).
    want: AtomicUsize,
    running: AtomicBool,
    cancelled: AtomicBool,
    last_used: Mutex<Instant>,
}

#[derive(Debug, Serialize)]
pub struct MatchesPage {
    pub matches: Vec<MessageSummary>,
    /// Matches found so far (may exceed what this page returned).
    pub found: usize,
    pub scanned_offsets: u64,
    pub total_offsets: u64,
    pub done: bool,
    pub error: Option<String>,
    pub skipped_undecodable: u64,
}

/// Applies `filter` to `message`, fetching any Avro schemas the filter needs first.
/// Returns `(matched, undecodable)`; `undecodable` means a schema wasn't available, so a
/// `false` match is "unknown", not "no" — callers surface that instead of hiding it.
/// Blocking: call from a `spawn_blocking` thread.
pub fn evaluate(
    profile: &ProfileRuntime,
    handle: &tokio::runtime::Handle,
    filter: &MessageFilter,
    message: &rakko_core::raw_message::RawMessage,
) -> (bool, bool) {
    let mut undecodable = false;
    if !filter.is_empty() {
        for id in schema_ids(message) {
            if !profile.ensure_schema_blocking(handle, id) {
                undecodable = true;
            }
        }
    }
    (profile.with_registry(|reg| filter.matches(message, reg)), undecodable)
}

/// Summary of a message whose preview may want its schema (fetched here if needed).
pub fn summarize_loaded(
    profile: &ProfileRuntime,
    handle: &tokio::runtime::Handle,
    message: &rakko_core::raw_message::RawMessage,
    preview_chars: usize,
) -> MessageSummary {
    for id in schema_ids(message) {
        profile.ensure_schema_blocking(handle, id);
    }
    profile.with_registry(|reg| summarize(message, reg, preview_chars))
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl ScanSession {
    /// Opens the scan (blocking Kafka metadata calls run off the async threads).
    pub async fn open(
        owner: String,
        profile: Arc<ProfileRuntime>,
        topic: String,
        filter: MessageFilter,
        range: ScanRange,
        options: ScanOptions,
    ) -> Result<Arc<Self>, AppError> {
        let ScanOptions { window, table, sort } = options;
        let (p, t, r) = (profile.profile.clone(), topic.clone(), range.clone());
        let ascending = match sort {
            Some(Sort { key: key @ (SortKey::Time | SortKey::Offset), desc: false }) if !table => Some(key),
            _ => None,
        };
        let scan = tokio::task::spawn_blocking(move || {
            TopicScan::open_range(&p, &t, window, &r).map(|s| if ascending.is_some() { s.ascending() } else { s })
        })
            .await
            .map_err(|e| AppError::Other(format!("scan open panicked: {e}")))??;
        let progress = scan.progress();
        let state = ScanState { progress: Some(progress), done: progress.done(), ..Default::default() };
        Ok(Arc::new(Self {
            id: uuid::Uuid::new_v4().to_string(),
            owner,
            topic,
            profile,
            filter: Arc::new(filter),
            range,
            sort,
            ascending,
            latest_per_key: table.then(Default::default),
            preview_chars: if table { TABLE_PREVIEW_CHARS } else { crate::dto::ROW_PREVIEW_CHARS },
            scan: Mutex::new(Some(scan)),
            state: Mutex::new(state),
            want: AtomicUsize::new(0),
            running: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            last_used: Mutex::new(Instant::now()),
        }))
    }

    pub fn total_offsets(&self) -> u64 {
        lock(&self.state).progress.map_or(0, |p| p.total_offsets)
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    fn idle_for(&self) -> Duration {
        lock(&self.last_used).elapsed()
    }

    /// A sorted scan that can't stream has to read everything before showing anything.
    fn must_finish_first(&self) -> bool {
        self.sort.is_some() && self.ascending.is_none()
    }

    fn needs_more(&self) -> bool {
        if self.cancelled.load(Ordering::SeqCst) {
            return false;
        }
        let state = lock(&self.state);
        // A sorted scan has to finish before anything can be shown.
        !state.done && state.error.is_none() && (self.must_finish_first() || state.matches.len() < self.want.load(Ordering::SeqCst))
    }

    /// Starts the runner if none is active and there is something to do.
    fn ensure_running(self: &Arc<Self>) {
        if !self.needs_more() || self.running.swap(true, Ordering::SeqCst) {
            return;
        }
        let me = self.clone();
        tokio::spawn(async move {
            me.run().await;
            me.running.store(false, Ordering::SeqCst);
            // A request may have raised `want` after the loop's last check.
            me.ensure_running();
        });
    }

    async fn run(self: &Arc<Self>) {
        while self.needs_more() {
            let Some(mut scan) = lock(&self.scan).take() else { return };
            let (profile, filter, range) = (self.profile.clone(), self.filter.clone(), self.range.clone());
            let (latest, chars, ascending) = (self.latest_per_key.clone(), self.preview_chars, self.ascending);
            let handle = tokio::runtime::Handle::current();
            let result = tokio::task::spawn_blocking(move || {
                let mut found: Vec<MessageSummary> = Vec::new();
                let mut skipped = 0u64;
                let mut window: Vec<rakko_core::raw_message::RawMessage> = Vec::new();
                let mut seen: HashMap<i32, i64> = HashMap::new();
                let outcome = scan.scan_next(&mut |message| {
                    if let Some(key) = ascending {
                        let k = match key {
                            SortKey::Offset => message.offset,
                            _ => message.timestamp_millis.unwrap_or(-1),
                        };
                        let slot = seen.entry(message.partition).or_insert(i64::MIN);
                        *slot = (*slot).max(k);
                    }
                    // Timestamps aren't monotonic, so the window is only approximate at the edges.
                    if range.contains_timestamp(message.timestamp_millis) {
                        window.push(message);
                    }
                });
                if latest.is_some() {
                    // Newest first per partition, so the first record seen for a key is its current state.
                    window.sort_by(|a, b| a.partition.cmp(&b.partition).then(b.offset.cmp(&a.offset)));
                }
                for message in window {
                    if let (Some(seen), Some(key)) = (&latest, message.key.as_ref()) {
                        let first = lock(seen).insert(key.clone());
                        if !first || message.value.is_none() {
                            continue;
                        }
                    }
                    let (matched, undecodable) = evaluate(&profile, &handle, &filter, &message);
                    if undecodable && !matched {
                        skipped += 1;
                    }
                    if matched {
                        found.push(summarize_loaded(&profile, &handle, &message, chars));
                    }
                }
                let unfinished: Vec<i32> = scan.partitions().into_iter().filter(|p| !scan.partition_done(*p)).collect();
                (scan, found, skipped, outcome, seen, unfinished)
            })
            .await;

            match result {
                Ok((scan, mut found, skipped, outcome, seen, unfinished)) => {
                    // Windows are fetched per partition, so order within one by time.
                    found.sort_by(|a, b| {
                        b.timestamp_ms.cmp(&a.timestamp_ms).then(b.offset.cmp(&a.offset)).then(a.partition.cmp(&b.partition))
                    });
                    let progress = scan.progress();
                    *lock(&self.scan) = Some(scan);
                    let mut state = lock(&self.state);
                    match self.ascending {
                        None => state.matches.extend(found),
                        Some(key) => {
                            // Oldest-first across partitions: a match is only final once every partition
                            // still being read has gone past it, otherwise a slower partition could
                            // still produce something older.
                            for (partition, k) in seen {
                                let slot = state.max_seen.entry(partition).or_insert(i64::MIN);
                                *slot = (*slot).max(k);
                            }
                            state.pending.extend(found);
                            let order = |m: &MessageSummary| match key {
                                SortKey::Offset => m.offset,
                                _ => m.timestamp_ms.unwrap_or(-1),
                            };
                            let mut pending = std::mem::take(&mut state.pending);
                            pending.sort_by(|a, b| order(a).cmp(&order(b)).then(a.partition.cmp(&b.partition)).then(a.offset.cmp(&b.offset)));
                            let threshold = unfinished.iter().map(|p| state.max_seen.get(p).copied().unwrap_or(i64::MIN)).min();
                            let ready = threshold.map_or(pending.len(), |t| pending.partition_point(|m| order(m) <= t));
                            let held = pending.split_off(ready);
                            state.matches.extend(pending);
                            state.pending = held;
                        }
                    }
                    state.skipped_undecodable += skipped;
                    state.progress = Some(progress);
                    state.done = progress.done();
                    if let Err(err) = outcome {
                        state.error = Some(err.to_string());
                    }
                    if let (Some(sort), true) = (self.sort, self.must_finish_first()) {
                        if state.matches.len() > MAX_SORT_MATCHES {
                            state.error = Some(format!(
                                "more than {MAX_SORT_MATCHES} matches — too many to sort; narrow the filter or range, or use the default order"
                            ));
                        } else if state.done && self.ascending.is_none() {
                            sort_matches(&mut state.matches, sort);
                        }
                    }
                }
                Err(join_err) => {
                    lock(&self.state).error = Some(format!("scan task panicked: {join_err}"));
                    return;
                }
            }
        }
    }

    fn snapshot(&self, from: usize, limit: usize) -> MatchesPage {
        let state = lock(&self.state);
        // Rows of a sorted scan are only meaningful once the scan is complete.
        let hidden = self.must_finish_first() && !state.done;
        let end = if hidden { 0 } else { (from + limit).min(state.matches.len()) };
        let progress = state.progress.unwrap_or(ScanProgress { total_offsets: 0, scanned_offsets: 0 });
        MatchesPage {
            matches: state.matches.get(from..end).map(<[_]>::to_vec).unwrap_or_default(),
            found: if hidden { 0 } else { state.matches.len() },
            scanned_offsets: progress.scanned_offsets,
            total_offsets: progress.total_offsets,
            done: state.done,
            error: state.error.clone(),
            skipped_undecodable: state.skipped_undecodable,
        }
    }

    /// Returns matches `from..from+limit`, advancing the scan if needed and waiting up to
    /// `wait` for them. Returns early with whatever exists once the scan is done or failed.
    pub async fn matches(self: &Arc<Self>, from: usize, limit: usize, wait: Duration) -> MatchesPage {
        *lock(&self.last_used) = Instant::now();
        self.want.fetch_max(from + limit, Ordering::SeqCst);
        self.ensure_running();
        let deadline = Instant::now() + wait;
        loop {
            let page = self.snapshot(from, limit);
            let satisfied = page.found >= from + limit || page.done || page.error.is_some();
            if satisfied || Instant::now() >= deadline || self.cancelled.load(Ordering::SeqCst) {
                return page;
            }
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }
}

#[derive(Default)]
pub struct ScanStore {
    scans: Mutex<HashMap<String, Arc<ScanSession>>>,
}

impl ScanStore {
    pub fn insert(&self, scan: Arc<ScanSession>) -> Result<(), &'static str> {
        let mut scans = lock(&self.scans);
        if scans.len() >= MAX_SCANS_TOTAL {
            return Err("too many active scans on this server; try again shortly");
        }
        let owned: Vec<(String, Instant)> = scans
            .values()
            .filter(|s| s.owner == scan.owner)
            .map(|s| (s.id.clone(), *lock(&s.last_used)))
            .collect();
        if owned.len() >= MAX_SCANS_PER_SESSION {
            if let Some((oldest, _)) = owned.into_iter().min_by_key(|(_, used)| *used) {
                if let Some(old) = scans.remove(&oldest) {
                    old.cancel();
                }
            }
        }
        scans.insert(scan.id.clone(), scan);
        Ok(())
    }

    /// The scan, only if `owner` is the session that created it.
    pub fn get(&self, id: &str, owner: &str) -> Option<Arc<ScanSession>> {
        lock(&self.scans).get(id).filter(|s| s.owner == owner).cloned()
    }

    pub fn remove(&self, id: &str, owner: &str) -> bool {
        let mut scans = lock(&self.scans);
        if scans.get(id).is_some_and(|s| s.owner == owner) {
            if let Some(s) = scans.remove(id) {
                s.cancel();
            }
            return true;
        }
        false
    }

    /// Drops scans nobody has touched for a while.
    pub fn sweep(&self) -> usize {
        let mut scans = lock(&self.scans);
        let stale: Vec<String> = scans.iter().filter(|(_, s)| s.idle_for() > IDLE_TTL).map(|(id, _)| id.clone()).collect();
        for id in &stale {
            if let Some(s) = scans.remove(id) {
                s.cancel();
            }
        }
        stale.len()
    }

    pub fn len(&self) -> usize {
        lock(&self.scans).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
