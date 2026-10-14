//! Full-topic scan: walks every partition newest-first in bounded offset windows and
//! hands each message to a visitor, so a filter can look at the whole topic without the
//! caller ever holding it in memory.
//!
//! * **Resumable.** The cursor is one number per partition (the end of the next window),
//!   so a session scans a little, stops, and later continues where it left off.
//! * **Bounded.** Each call to [`TopicScan::scan_next`] reads at most one window per
//!   partition. Retention is `visit`'s business: nothing is buffered here.
//! * **Exact.** A window ends on a definite signal — a message at or past the window end,
//!   or a partition-EOF event — never on a "nothing arrived for a while" guess, so a
//!   compacted gap or a slow broker cannot silently skip data. A scan that makes no
//!   progress for [`STALL_TIMEOUT`] fails instead.
//! * **A snapshot.** Windows are fixed against the watermarks seen at `open`; messages
//!   produced afterwards are not part of the scan.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::types::RDKafkaErrorCode;

use crate::config::Profile;
use crate::error::{AppError, AppResult};
use crate::kafka::client_config::consumer_client_config;
use crate::kafka::consumer::{partition_watermarks_with, to_raw_message, PartitionWatermarks};
use crate::raw_message::RawMessage;

const SCAN_GROUP_ID: &str = "rakko-scan";
const POLL_TIMEOUT: Duration = Duration::from_millis(500);
/// A window with no message and no EOF for this long is treated as a failure.
const STALL_TIMEOUT: Duration = Duration::from_secs(30);

/// How far a scan has got, in offsets (not messages — compaction and transaction markers
/// leave gaps, so offsets are the only honest unit for "how much is left").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanProgress {
    pub total_offsets: u64,
    pub scanned_offsets: u64,
}

impl ScanProgress {
    pub fn done(&self) -> bool {
        self.scanned_offsets >= self.total_offsets
    }
}

/// Restricts a scan to part of a topic. Empty = the whole topic.
///
/// * Time bounds (`from_ms` inclusive, `to_ms` exclusive, epoch millis) are resolved to
///   offsets per partition with the broker's timestamp index, so a narrow window is
///   found without reading what lies outside it. Message timestamps are not guaranteed
///   monotonic, so callers wanting exact edges should also check each message
///   ([`ScanRange::contains_timestamp`]).
/// * `partition` limits the scan to one partition; `from_offset`/`to_offset` (inclusive
///   start, exclusive end) only make sense together with it, since offsets are per
///   partition.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ScanRange {
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
    pub partition: Option<i32>,
    pub from_offset: Option<i64>,
    pub to_offset: Option<i64>,
}

impl ScanRange {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    pub fn validate(&self) -> AppResult<()> {
        if (self.from_offset.is_some() || self.to_offset.is_some()) && self.partition.is_none() {
            return Err(AppError::Other("an offset range needs a partition (offsets are per partition)".into()));
        }
        if let (Some(a), Some(b)) = (self.from_ms, self.to_ms) {
            if a >= b {
                return Err(AppError::Other("the time range must end after it starts".into()));
            }
        }
        if let (Some(a), Some(b)) = (self.from_offset, self.to_offset) {
            if a >= b {
                return Err(AppError::Other("the offset range must end after it starts".into()));
            }
        }
        if self.from_offset.is_some_and(|o| o < 0) || self.to_offset.is_some_and(|o| o < 0) {
            return Err(AppError::Other("offsets must be >= 0".into()));
        }
        Ok(())
    }

    /// Whether a message with this timestamp is inside the time bounds. A message with no
    /// timestamp can't be placed, so it is kept.
    pub fn contains_timestamp(&self, timestamp_ms: Option<i64>) -> bool {
        let Some(ts) = timestamp_ms else { return true };
        self.from_ms.is_none_or(|from| ts >= from) && self.to_ms.is_none_or(|to| ts < to)
    }

    /// The offset window each partition is scanned over. `time_to_offset(partition, ms)`
    /// is the first offset whose timestamp is >= `ms`, or `None` when there is none (every
    /// message is older). Pure so the arithmetic is testable without a broker.
    pub fn narrow(
        &self,
        marks: &[PartitionWatermarks],
        mut time_to_offset: impl FnMut(i32, i64) -> Option<i64>,
    ) -> Vec<PartitionWatermarks> {
        marks
            .iter()
            .map(|m| {
                let empty = PartitionWatermarks { partition: m.partition, low: m.high, high: m.high };
                if self.partition.is_some_and(|p| p != m.partition) {
                    return empty;
                }
                let (mut low, mut high) = (m.low, m.high);
                if let Some(from) = self.from_ms {
                    low = low.max(time_to_offset(m.partition, from).unwrap_or(m.high));
                }
                if let Some(to) = self.to_ms {
                    high = high.min(time_to_offset(m.partition, to).unwrap_or(m.high));
                }
                if let Some(o) = self.from_offset {
                    low = low.max(o);
                }
                if let Some(o) = self.to_offset {
                    high = high.min(o);
                }
                if low >= high {
                    return empty;
                }
                PartitionWatermarks { partition: m.partition, low, high }
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
struct Cursor {
    partition: i32,
    low: i64,
    high: i64,
    /// End (exclusive) of the next window; walks down to `low`.
    next_end: i64,
}

pub struct TopicScan {
    consumer: BaseConsumer,
    topic: String,
    window: i64,
    cursors: Vec<Cursor>,
}

impl TopicScan {
    /// Connects and snapshots the topic's watermarks. Blocking — call from
    /// `spawn_blocking`. `window` is offsets per partition per [`scan_next`](Self::scan_next).
    pub fn open(profile: &Profile, topic: &str, window: usize) -> AppResult<Self> {
        let mut config = consumer_client_config(profile, SCAN_GROUP_ID);
        config.set("enable.partition.eof", "true");
        // A retention sweep mid-scan must be an error, not a silent jump to the log end.
        config.set("auto.offset.reset", "error");
        let consumer: BaseConsumer = config.create()?;
        let watermarks = partition_watermarks_with(&consumer, topic)?;
        Ok(Self::from_watermarks(consumer, topic, window, &watermarks))
    }

    /// [`open`](Self::open) restricted to `range`. Blocking.
    pub fn open_range(profile: &Profile, topic: &str, window: usize, range: &ScanRange) -> AppResult<Self> {
        range.validate()?;
        let mut config = consumer_client_config(profile, SCAN_GROUP_ID);
        config.set("enable.partition.eof", "true");
        config.set("auto.offset.reset", "error");
        let consumer: BaseConsumer = config.create()?;
        let marks = partition_watermarks_with(&consumer, topic)?;
        if let Some(p) = range.partition {
            if !marks.iter().any(|m| m.partition == p) {
                return Err(AppError::Other(format!("topic {topic} has no partition {p}")));
            }
        }
        let mut lookup_error = None;
        let narrowed = range.narrow(&marks, |partition, ms| {
            let mut tpl = TopicPartitionList::with_capacity(1);
            if tpl.add_partition_offset(topic, partition, Offset::Offset(ms)).is_err() {
                return None;
            }
            match consumer.offsets_for_times(tpl, Duration::from_secs(10)) {
                Ok(resolved) => resolved.elements().into_iter().find_map(|e| match e.offset() {
                    Offset::Offset(n) => Some(n),
                    _ => None,
                }),
                Err(err) => {
                    lookup_error.get_or_insert(err);
                    None
                }
            }
        });
        if let Some(err) = lookup_error {
            return Err(err.into());
        }
        Ok(Self::from_watermarks(consumer, topic, window, &narrowed))
    }

    fn from_watermarks(consumer: BaseConsumer, topic: &str, window: usize, marks: &[PartitionWatermarks]) -> Self {
        Self {
            consumer,
            topic: topic.to_string(),
            window: window.max(1) as i64,
            cursors: marks
                .iter()
                .map(|m| Cursor { partition: m.partition, low: m.low, high: m.high, next_end: m.high })
                .collect(),
        }
    }

    pub fn progress(&self) -> ScanProgress {
        progress_of(&self.cursors)
    }

    pub fn done(&self) -> bool {
        self.progress().done()
    }

    /// Reads the next window of every partition that has data left, calling `visit` for
    /// each message (partition order interleaved as the broker delivers it). Blocking.
    pub fn scan_next(&mut self, visit: &mut dyn FnMut(RawMessage)) -> AppResult<()> {
        let windows = plan_windows(&self.cursors, self.window);
        if windows.is_empty() {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::with_capacity(windows.len());
        for w in &windows {
            tpl.add_partition_offset(&self.topic, w.partition, Offset::Offset(w.start))
                .map_err(|err| AppError::Other(format!("failed to build partition assignment: {err}")))?;
        }
        self.consumer.assign(&tpl)?;

        let mut remaining: HashMap<i32, i64> = windows.iter().map(|w| (w.partition, w.end)).collect();
        let mut last_progress = Instant::now();
        while !remaining.is_empty() {
            match self.consumer.poll(POLL_TIMEOUT) {
                Some(Ok(msg)) => {
                    last_progress = Instant::now();
                    let Some(&end) = remaining.get(&msg.partition()) else { continue };
                    use rdkafka::Message;
                    if msg.offset() >= end {
                        remaining.remove(&msg.partition());
                        continue;
                    }
                    let (partition, offset) = (msg.partition(), msg.offset());
                    visit(to_raw_message(&msg));
                    if offset + 1 >= end {
                        remaining.remove(&partition);
                    }
                }
                Some(Err(KafkaError::PartitionEOF(partition))) => {
                    last_progress = Instant::now();
                    remaining.remove(&partition);
                }
                // With `auto.offset.reset=error`, an offset that retention has since removed
                // surfaces as AutoOffsetReset (OffsetOutOfRange on some broker paths).
                Some(Err(KafkaError::MessageConsumption(
                    RDKafkaErrorCode::OffsetOutOfRange | RDKafkaErrorCode::AutoOffsetReset,
                ))) => {
                    return Err(AppError::Other(format!(
                        "scan of {} lost data to retention while running; start a new scan",
                        self.topic
                    )));
                }
                Some(Err(err)) => {
                    tracing::warn!("transient error scanning {}: {err}", self.topic);
                }
                None => {
                    if last_progress.elapsed() > STALL_TIMEOUT {
                        return Err(AppError::Other(format!(
                            "scan of {} stalled with {} partition window(s) unread",
                            self.topic,
                            remaining.len()
                        )));
                    }
                }
            }
        }

        for w in &windows {
            if let Some(c) = self.cursors.iter_mut().find(|c| c.partition == w.partition) {
                c.next_end = w.start;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Window {
    partition: i32,
    /// Inclusive.
    start: i64,
    /// Exclusive.
    end: i64,
}

fn plan_windows(cursors: &[Cursor], window: i64) -> Vec<Window> {
    cursors
        .iter()
        .filter(|c| c.next_end > c.low)
        .map(|c| Window {
            partition: c.partition,
            start: (c.next_end - window).max(c.low),
            end: c.next_end,
        })
        .collect()
}

fn progress_of(cursors: &[Cursor]) -> ScanProgress {
    let mut total = 0u64;
    let mut scanned = 0u64;
    for c in cursors {
        total += (c.high - c.low).max(0) as u64;
        scanned += (c.high - c.next_end.max(c.low)).max(0) as u64;
    }
    ScanProgress { total_offsets: total, scanned_offsets: scanned }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor(partition: i32, low: i64, high: i64) -> Cursor {
        Cursor { partition, low, high, next_end: high }
    }

    fn marks() -> Vec<PartitionWatermarks> {
        vec![
            PartitionWatermarks { partition: 0, low: 100, high: 1000 },
            PartitionWatermarks { partition: 1, low: 0, high: 50 },
        ]
    }

    /// Timestamps are the offset times ten, so `ms / 10` is the offset at `ms`.
    fn by_ten(_partition: i32, ms: i64) -> Option<i64> {
        Some(ms / 10)
    }

    #[test]
    fn empty_range_is_the_whole_topic() {
        assert_eq!(ScanRange::default().narrow(&marks(), by_ten), marks());
    }

    #[test]
    fn time_bounds_become_offset_bounds_clamped_to_what_exists() {
        let range = ScanRange { from_ms: Some(2_000), to_ms: Some(5_000), ..Default::default() };
        let narrowed = range.narrow(&marks(), by_ten);
        assert_eq!((narrowed[0].low, narrowed[0].high), (200, 500));
        // Partition 1 only holds 0..50, all older than the window: nothing to scan.
        assert_eq!(narrowed[1].low, narrowed[1].high);
    }

    #[test]
    fn a_start_past_every_message_scans_nothing() {
        let range = ScanRange { from_ms: Some(9_999_999), ..Default::default() };
        let narrowed = range.narrow(&marks(), |_, _| None);
        assert!(narrowed.iter().all(|m| m.low >= m.high));
    }

    #[test]
    fn an_open_ended_range_runs_to_the_end() {
        let range = ScanRange { from_ms: Some(9_000), ..Default::default() };
        let narrowed = range.narrow(&marks(), by_ten);
        assert_eq!((narrowed[0].low, narrowed[0].high), (900, 1000));
    }

    #[test]
    fn partition_and_offset_range_pick_one_partition_window() {
        let range = ScanRange { partition: Some(0), from_offset: Some(150), to_offset: Some(300), ..Default::default() };
        let narrowed = range.narrow(&marks(), by_ten);
        assert_eq!((narrowed[0].low, narrowed[0].high), (150, 300));
        assert_eq!(narrowed[1].low, narrowed[1].high, "other partitions are skipped");
    }

    #[test]
    fn range_validation() {
        let bad = |r: ScanRange| r.validate().is_err();
        assert!(bad(ScanRange { from_offset: Some(1), ..Default::default() }), "offsets need a partition");
        assert!(bad(ScanRange { from_ms: Some(5), to_ms: Some(5), ..Default::default() }));
        assert!(bad(ScanRange { partition: Some(0), from_offset: Some(9), to_offset: Some(3), ..Default::default() }));
        assert!(bad(ScanRange { partition: Some(0), from_offset: Some(-1), ..Default::default() }));
        assert!(ScanRange { partition: Some(0), from_offset: Some(1), ..Default::default() }.validate().is_ok());
    }

    #[test]
    fn timestamp_edges_are_exact_and_untimed_messages_are_kept() {
        let range = ScanRange { from_ms: Some(10), to_ms: Some(20), ..Default::default() };
        assert!(range.contains_timestamp(Some(10)));
        assert!(!range.contains_timestamp(Some(20)), "end is exclusive");
        assert!(!range.contains_timestamp(Some(9)));
        assert!(range.contains_timestamp(None));
    }

    #[test]
    fn windows_walk_each_partition_down_from_its_high_watermark() {
        let cursors = [cursor(0, 0, 250), cursor(1, 100, 130)];
        assert_eq!(
            plan_windows(&cursors, 100),
            vec![
                Window { partition: 0, start: 150, end: 250 },
                Window { partition: 1, start: 100, end: 130 },
            ]
        );
    }

    #[test]
    fn finished_and_empty_partitions_get_no_window() {
        let mut done = cursor(0, 10, 50);
        done.next_end = 10;
        assert!(plan_windows(&[done, cursor(1, 7, 7)], 100).is_empty());
    }

    #[test]
    fn the_last_window_is_clamped_to_the_low_watermark() {
        let mut c = cursor(0, 20, 250);
        c.next_end = 60;
        assert_eq!(plan_windows(&[c], 100), vec![Window { partition: 0, start: 20, end: 60 }]);
    }

    #[test]
    fn progress_counts_offsets_scanned_over_offsets_retained() {
        let mut a = cursor(0, 0, 100);
        a.next_end = 40; // 60 scanned of 100
        let b = cursor(1, 50, 80); // 0 of 30
        let p = progress_of(&[a, b]);
        assert_eq!(p, ScanProgress { total_offsets: 130, scanned_offsets: 60 });
        assert!(!p.done());
    }

    #[test]
    fn an_empty_topic_is_immediately_done() {
        assert!(progress_of(&[cursor(0, 5, 5), cursor(1, 0, 0)]).done());
        assert!(progress_of(&[]).done());
    }
}

/// Docker-compose-gated: `docker compose up -d` then `cargo test --workspace -- --ignored`.
#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::kafka::integration_support::{local_profile, unique_name};
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use std::collections::HashSet;

    async fn create_topic(profile: &Profile, name: &str, partitions: i32) {
        let admin: AdminClient<DefaultClientContext> =
            crate::kafka::client_config::base_client_config(profile).create().expect("admin client");
        let results = admin
            .create_topics(
                &[NewTopic::new(name, partitions, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .expect("create_topics request");
        results[0].as_ref().expect("topic created");
    }

    /// Produces `count` messages through one producer (the public `produce` builds a new
    /// producer per call, which makes a few hundred messages take minutes).
    async fn produce_many(profile: &Profile, topic: &str, count: usize, keyed: bool) {
        use rdkafka::producer::{FutureProducer, FutureRecord};
        let producer: FutureProducer =
            crate::kafka::client_config::producer_client_config(profile).create().expect("producer");
        let mut sends = Vec::new();
        for i in 0..count {
            let key = format!("k{i}");
            let value = format!("v{i}");
            let producer = producer.clone();
            let topic = topic.to_string();
            sends.push(tokio::spawn(async move {
                let mut record = FutureRecord::to(&topic).payload(&value);
                if keyed {
                    record = record.key(&key);
                }
                producer.send(record, Duration::from_secs(30)).await.map(|_| ()).map_err(|(e, _)| e)
            }));
        }
        for send in sends {
            send.await.unwrap().expect("produce");
        }
    }

    #[tokio::test]
    #[ignore = "requires `docker compose up -d` (localhost:19092)"]
    async fn scan_visits_every_message_exactly_once_across_partitions() {
        let profile = local_profile();
        let topic = unique_name("scan");
        create_topic(&profile, &topic, 3).await;
        produce_many(&profile, &topic, 500, true).await;

        let (profile2, topic2) = (profile.clone(), topic.clone());
        let (seen, windows, progress) = tokio::task::spawn_blocking(move || {
            let mut scan = TopicScan::open(&profile2, &topic2, 40).expect("open scan");
            let mut seen: Vec<(i32, i64)> = Vec::new();
            let mut windows = 0;
            while !scan.done() {
                scan.scan_next(&mut |m| seen.push((m.partition, m.offset))).expect("scan_next");
                windows += 1;
                assert!(windows < 100, "scan must terminate");
            }
            (seen, windows, scan.progress())
        })
        .await
        .unwrap();

        let unique: HashSet<_> = seen.iter().copied().collect();
        assert_eq!(seen.len(), 500, "every message visited");
        assert_eq!(unique.len(), 500, "no message visited twice");
        assert!(windows > 3, "small windows should take several passes, took {windows}");
        assert_eq!(progress.scanned_offsets, progress.total_offsets);
        assert_eq!(progress.total_offsets, 500);
    }

    #[tokio::test]
    #[ignore = "requires `docker compose up -d` (localhost:19092)"]
    async fn scan_walks_newest_first_within_a_partition() {
        let profile = local_profile();
        let topic = unique_name("scan-order");
        create_topic(&profile, &topic, 1).await;
        produce_many(&profile, &topic, 120, false).await;
        let (p, t) = (profile.clone(), topic.clone());
        let first_window = tokio::task::spawn_blocking(move || {
            let mut scan = TopicScan::open(&p, &t, 50).unwrap();
            let mut offsets = Vec::new();
            scan.scan_next(&mut |m| offsets.push(m.offset)).unwrap();
            offsets
        })
        .await
        .unwrap();
        assert_eq!(first_window.first(), Some(&70), "first window is the newest 50: offsets 70..120");
        assert_eq!(first_window.last(), Some(&119));
        assert_eq!(first_window.len(), 50);
    }
}
