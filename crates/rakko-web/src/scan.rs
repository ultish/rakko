//! Server-side scan sessions.
//!
//! A scan walks a whole topic newest-first applying a filter and keeps a list of the
//! matches' summaries (not their payloads — those are fetched on demand). The scan is
//! owned by the server session, not by any HTTP request: a runner task advances it in
//! windows until enough matches exist for what the client has asked to see, and requests
//! just wait (briefly) on that list. Closing the tab or dropping a request never
//! corrupts or restarts a scan.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rakko_core::error::AppError;
use rakko_core::kafka::scan::{ScanProgress, TopicScan};
use rakko_core::message_filter::MessageFilter;
use serde::Serialize;

use crate::dto::{summarize, MessageSummary};
use crate::runtime::{schema_ids, ProfileRuntime};

/// A session may have this many live scans; the oldest is dropped past it.
pub const MAX_SCANS_PER_SESSION: usize = 4;
pub const MAX_SCANS_TOTAL: usize = 64;
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
}

pub struct ScanSession {
    pub id: String,
    pub owner: String,
    pub topic: String,
    profile: Arc<ProfileRuntime>,
    filter: Arc<MessageFilter>,
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
) -> MessageSummary {
    for id in schema_ids(message) {
        profile.ensure_schema_blocking(handle, id);
    }
    profile.with_registry(|reg| summarize(message, reg))
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
        window: usize,
    ) -> Result<Arc<Self>, AppError> {
        let (p, t) = (profile.profile.clone(), topic.clone());
        let scan = tokio::task::spawn_blocking(move || TopicScan::open(&p, &t, window))
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

    fn needs_more(&self) -> bool {
        if self.cancelled.load(Ordering::SeqCst) {
            return false;
        }
        let state = lock(&self.state);
        !state.done && state.error.is_none() && state.matches.len() < self.want.load(Ordering::SeqCst)
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
            let (profile, filter) = (self.profile.clone(), self.filter.clone());
            let handle = tokio::runtime::Handle::current();
            let result = tokio::task::spawn_blocking(move || {
                let mut found: Vec<MessageSummary> = Vec::new();
                let mut skipped = 0u64;
                let outcome = scan.scan_next(&mut |message| {
                    let (matched, undecodable) = evaluate(&profile, &handle, &filter, &message);
                    if undecodable && !matched {
                        skipped += 1;
                    }
                    if matched {
                        found.push(summarize_loaded(&profile, &handle, &message));
                    }
                });
                (scan, found, skipped, outcome)
            })
            .await;

            match result {
                Ok((scan, mut found, skipped, outcome)) => {
                    // Windows are fetched per partition, so order within one by time.
                    found.sort_by(|a, b| {
                        b.timestamp_ms.cmp(&a.timestamp_ms).then(b.offset.cmp(&a.offset)).then(a.partition.cmp(&b.partition))
                    });
                    let progress = scan.progress();
                    *lock(&self.scan) = Some(scan);
                    let mut state = lock(&self.state);
                    state.matches.extend(found);
                    state.skipped_undecodable += skipped;
                    state.progress = Some(progress);
                    state.done = progress.done();
                    if let Err(err) = outcome {
                        state.error = Some(err.to_string());
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
        let end = (from + limit).min(state.matches.len());
        let progress = state.progress.unwrap_or(ScanProgress { total_offsets: 0, scanned_offsets: 0 });
        MatchesPage {
            matches: state.matches.get(from..end).map(<[_]>::to_vec).unwrap_or_default(),
            found: state.matches.len(),
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
