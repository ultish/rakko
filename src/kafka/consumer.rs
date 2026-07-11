use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use crate::config::Profile;
use crate::error::{AppError, AppResult};
use crate::events::{AppEvent, SeekPageMeta, SeekPageRequest};
use crate::kafka::client_config::consumer_client_config;
use crate::raw_message::RawMessage;

/// Throwaway `group.id` for ad-hoc browsing consumers (tail/seek never join a real
/// consumer group or commit offsets).
const BROWSE_GROUP_ID: &str = "rakko-browse";
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const WATERMARK_TIMEOUT: Duration = Duration::from_secs(10);
/// Short so the tail loop wakes up regularly to check for a stop signal even when no
/// messages are arriving.
const TAIL_POLL_TIMEOUT: Duration = Duration::from_millis(200);
const SEEK_POLL_TIMEOUT: Duration = Duration::from_millis(300);
/// Safety net for `load_seek_page`: stop after this many consecutive empty/errored polls
/// even if the watermark-derived stopping point hasn't been reached, so a watermark/poll
/// race or a slow broker can't hang the one-shot load.
const SEEK_STALL_LIMIT: u32 = 5;

/// Converts a borrowed Kafka message into the owned, byte-preserving `RawMessage` type
/// used everywhere outside of the consumer poll loop.
fn to_raw_message(msg: &rdkafka::message::BorrowedMessage<'_>) -> RawMessage {
    let headers = msg
        .headers()
        .map(|headers| {
            headers
                .iter()
                .map(|header| (header.key.to_string(), header.value.map(|v| v.to_vec()).unwrap_or_default()))
                .collect()
        })
        .unwrap_or_default();

    RawMessage {
        topic: msg.topic().to_string(),
        partition: msg.partition(),
        offset: msg.offset(),
        timestamp_millis: msg.timestamp().to_millis(),
        key: msg.key().map(|b| b.to_vec()),
        value: msg.payload().map(|b| b.to_vec()),
        headers,
    }
}

/// Runs the continuous tail-mode poll loop until `stop_rx` flips to `true`, sending one
/// `AppEvent::MessageArrived` per message received. Cooperatively cancellable: the actual
/// blocking work happens in `spawn_blocking`, and the loop checks `stop_rx` after every
/// bounded `poll()` call rather than relying on the caller aborting the task (which would
/// not stop an in-flight blocking closure - see PLAN.md).
pub async fn run_tail(
    profile: Profile,
    topic: String,
    tx: tokio::sync::mpsc::UnboundedSender<AppEvent>,
    stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    let result =
        tokio::task::spawn_blocking(move || run_tail_blocking(&profile, &topic, &tx, stop_rx)).await;
    if let Err(err) = result {
        tracing::warn!("tail task panicked: {err}");
    }
}

fn run_tail_blocking(
    profile: &Profile,
    topic: &str,
    tx: &tokio::sync::mpsc::UnboundedSender<AppEvent>,
    stop_rx: tokio::sync::watch::Receiver<bool>,
) {
    let consumer: BaseConsumer = match consumer_client_config(profile, BROWSE_GROUP_ID).create() {
        Ok(consumer) => consumer,
        Err(err) => {
            let _ = tx.send(AppEvent::BrowseFailed(format!("failed to create consumer: {err}")));
            return;
        }
    };

    let metadata = match consumer.fetch_metadata(Some(topic), METADATA_TIMEOUT) {
        Ok(metadata) => metadata,
        Err(err) => {
            let _ = tx.send(AppEvent::BrowseFailed(format!("failed to fetch metadata for {topic}: {err}")));
            return;
        }
    };

    let Some(topic_metadata) = metadata.topics().iter().find(|t| t.name() == topic) else {
        let _ = tx.send(AppEvent::BrowseFailed(format!("topic {topic} not found")));
        return;
    };

    let mut tpl = TopicPartitionList::with_capacity(topic_metadata.partitions().len());
    for partition in topic_metadata.partitions() {
        if let Err(err) = tpl.add_partition_offset(topic, partition.id(), Offset::End) {
            let _ = tx.send(AppEvent::BrowseFailed(format!("failed to build partition assignment: {err}")));
            return;
        }
    }

    if let Err(err) = consumer.assign(&tpl) {
        let _ = tx.send(AppEvent::BrowseFailed(format!("failed to assign partitions: {err}")));
        return;
    }

    loop {
        match consumer.poll(TAIL_POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                let partition = msg.partition();
                let message = to_raw_message(&msg);
                let event = AppEvent::MessageArrived {
                    topic: topic.to_string(),
                    partition,
                    message,
                };
                if tx.send(event).is_err() {
                    // Receiver dropped (app shutting down) - normal shutdown race, not an error.
                    break;
                }
            }
            Some(Err(err)) => {
                tracing::warn!("transient error polling {topic} in tail mode: {err}");
            }
            None => {}
        }

        if *stop_rx.borrow() {
            break;
        }
    }
}

/// Loads exactly one bounded page of messages for seek mode, sending
/// `AppEvent::SeekPageLoaded` on success or `AppEvent::BrowseFailed` on failure. One-shot:
/// no cancellation needed since the work is bounded and expected to finish quickly.
pub async fn load_seek_page(
    profile: Profile,
    topic: String,
    partition: i32,
    request: SeekPageRequest,
    tx: tokio::sync::mpsc::UnboundedSender<AppEvent>,
) {
    let topic_for_result = topic.clone();
    let result = tokio::task::spawn_blocking(move || load_seek_page_blocking(&profile, &topic, partition, &request))
        .await;

    let event = match result {
        Ok(Ok((messages, meta))) => AppEvent::SeekPageLoaded {
            topic: topic_for_result,
            messages,
            meta,
        },
        Ok(Err(err)) => AppEvent::BrowseFailed(err.to_string()),
        Err(err) => AppEvent::BrowseFailed(format!("seek page load task panicked: {err}")),
    };

    let _ = tx.send(event);
}

/// Pure description of how a seek page should be loaded, derived from the low/high
/// watermarks and the requested page. Split out from the polling code so the offset-range
/// math (the easy-to-get-subtly-wrong part) can be unit tested without a live broker.
#[derive(Debug, Clone, PartialEq)]
struct SeekPlan {
    /// Offset to assign the consumer to, and the page's anchor offset (`page_start_offset`
    /// in the resulting `SeekPageMeta`).
    start_offset: i64,
    /// Max number of messages to collect for this page.
    limit: usize,
    /// Offset at which the page is known to be complete once the next expected offset
    /// reaches it (`Latest`/`Forward`). `None` for `Backward`, which stops purely on
    /// collected count.
    stop_at_offset: Option<i64>,
    at_beginning: bool,
    /// `at_end` when it's knowable purely from the request/watermarks (`Latest` is always
    /// true by construction, `Backward` is always false by construction). `None` means the
    /// caller must compute it after polling, from the actual number of messages collected
    /// (`Forward`'s non-empty case).
    fixed_at_end: Option<bool>,
}

// Note: `SeekPageMeta` (defined in `events.rs`) doesn't derive `PartialEq`, so this enum
// can't either; tests match on the variant and compare fields individually instead.
#[derive(Debug, Clone)]
enum SeekOutcome {
    /// The page can be returned immediately, with no messages and no consumer interaction.
    Immediate(SeekPageMeta),
    Poll(SeekPlan),
}

fn resolve_seek_plan(partition: i32, low: i64, high: i64, request: &SeekPageRequest) -> SeekOutcome {
    match *request {
        SeekPageRequest::Latest { page_size } => {
            let start_offset = std::cmp::max(low, high - page_size as i64);
            SeekOutcome::Poll(SeekPlan {
                start_offset,
                limit: page_size,
                stop_at_offset: Some(high),
                at_beginning: start_offset <= low,
                fixed_at_end: Some(true),
            })
        }
        SeekPageRequest::Forward { from_offset, page_size } => {
            if from_offset >= high {
                return SeekOutcome::Immediate(SeekPageMeta {
                    partition,
                    page_start_offset: from_offset,
                    at_beginning: from_offset <= low,
                    at_end: true,
                    low_watermark: low,
                    high_watermark: high,
                });
            }
            SeekOutcome::Poll(SeekPlan {
                start_offset: from_offset,
                limit: page_size,
                stop_at_offset: Some(high),
                at_beginning: from_offset <= low,
                fixed_at_end: None,
            })
        }
        SeekPageRequest::Backward { before_offset, page_size } => {
            let start_offset = std::cmp::max(low, before_offset - page_size as i64);
            if start_offset >= before_offset {
                return SeekOutcome::Immediate(SeekPageMeta {
                    partition,
                    page_start_offset: start_offset,
                    at_beginning: true,
                    at_end: false,
                    low_watermark: low,
                    high_watermark: high,
                });
            }
            let limit = (before_offset - start_offset) as usize;
            SeekOutcome::Poll(SeekPlan {
                start_offset,
                limit,
                stop_at_offset: None,
                at_beginning: start_offset <= low,
                fixed_at_end: Some(false),
            })
        }
    }
}

fn load_seek_page_blocking(
    profile: &Profile,
    topic: &str,
    partition: i32,
    request: &SeekPageRequest,
) -> AppResult<(Vec<RawMessage>, SeekPageMeta)> {
    let consumer: BaseConsumer = consumer_client_config(profile, BROWSE_GROUP_ID).create()?;

    let (low, high) = consumer.fetch_watermarks(topic, partition, WATERMARK_TIMEOUT)?;

    let plan = match resolve_seek_plan(partition, low, high, request) {
        SeekOutcome::Immediate(meta) => return Ok((Vec::new(), meta)),
        SeekOutcome::Poll(plan) => plan,
    };

    let mut tpl = TopicPartitionList::with_capacity(1);
    tpl.add_partition_offset(topic, partition, Offset::Offset(plan.start_offset))
        .map_err(|err| AppError::Other(format!("failed to build partition assignment: {err}")))?;
    consumer.assign(&tpl)?;

    let mut messages = Vec::new();
    let mut stall_count = 0u32;

    while messages.len() < plan.limit {
        match consumer.poll(SEEK_POLL_TIMEOUT) {
            Some(Ok(msg)) => {
                let next_expected_offset = msg.offset() + 1;
                messages.push(to_raw_message(&msg));
                stall_count = 0;

                if let Some(stop_at) = plan.stop_at_offset {
                    if next_expected_offset >= stop_at {
                        break;
                    }
                }
            }
            Some(Err(err)) => {
                tracing::warn!("transient error polling {topic}/{partition} in seek mode: {err}");
                stall_count += 1;
            }
            None => {
                stall_count += 1;
            }
        }

        if stall_count >= SEEK_STALL_LIMIT {
            break;
        }
    }

    let at_end = plan
        .fixed_at_end
        .unwrap_or_else(|| plan.start_offset + messages.len() as i64 >= high);

    let meta = SeekPageMeta {
        partition,
        page_start_offset: plan.start_offset,
        at_beginning: plan.at_beginning,
        at_end,
        low_watermark: low,
        high_watermark: high,
    };

    Ok((messages, meta))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn latest(page_size: usize) -> SeekPageRequest {
        SeekPageRequest::Latest { page_size }
    }

    fn forward(from_offset: i64, page_size: usize) -> SeekPageRequest {
        SeekPageRequest::Forward { from_offset, page_size }
    }

    fn backward(before_offset: i64, page_size: usize) -> SeekPageRequest {
        SeekPageRequest::Backward { before_offset, page_size }
    }

    /// `SeekPageMeta` doesn't derive `PartialEq` (it's a shared type owned by
    /// `events.rs`), so assert on an `Immediate` outcome by matching the variant and
    /// comparing fields individually.
    fn assert_immediate(outcome: SeekOutcome, page_start_offset: i64, at_beginning: bool, at_end: bool) {
        match outcome {
            SeekOutcome::Immediate(meta) => {
                assert_eq!(meta.page_start_offset, page_start_offset, "page_start_offset");
                assert_eq!(meta.at_beginning, at_beginning, "at_beginning");
                assert_eq!(meta.at_end, at_end, "at_end");
            }
            SeekOutcome::Poll(plan) => panic!("expected Immediate outcome, got Poll({plan:?})"),
        }
    }

    fn assert_poll(outcome: SeekOutcome, expected: SeekPlan) {
        match outcome {
            SeekOutcome::Poll(plan) => assert_eq!(plan, expected),
            SeekOutcome::Immediate(meta) => panic!("expected Poll outcome, got Immediate({meta:?})"),
        }
    }

    #[test]
    fn latest_page_smaller_than_available_range() {
        // low=0, high=100, page_size=10 -> last 10 messages, not at the beginning.
        let outcome = resolve_seek_plan(0, 0, 100, &latest(10));
        assert_poll(
            outcome,
            SeekPlan {
                start_offset: 90,
                limit: 10,
                stop_at_offset: Some(100),
                at_beginning: false,
                fixed_at_end: Some(true),
            },
        );
    }

    #[test]
    fn latest_page_size_larger_than_available_range() {
        // low=90, high=100, page_size=1000 -> clamped to low, at_beginning true.
        let outcome = resolve_seek_plan(0, 90, 100, &latest(1000));
        assert_poll(
            outcome,
            SeekPlan {
                start_offset: 90,
                limit: 1000,
                stop_at_offset: Some(100),
                at_beginning: true,
                fixed_at_end: Some(true),
            },
        );
    }

    #[test]
    fn latest_on_empty_topic() {
        // low == high: start_offset == high == low, still a Poll plan (stall detector
        // handles the empty-poll case at runtime), at_beginning true.
        let outcome = resolve_seek_plan(0, 42, 42, &latest(20));
        assert_poll(
            outcome,
            SeekPlan {
                start_offset: 42,
                limit: 20,
                stop_at_offset: Some(42),
                at_beginning: true,
                fixed_at_end: Some(true),
            },
        );
    }

    #[test]
    fn forward_from_offset_at_or_past_high_is_immediate() {
        let outcome = resolve_seek_plan(0, 0, 100, &forward(100, 10));
        assert_immediate(outcome, 100, false, true);

        // Past-high case too.
        let outcome = resolve_seek_plan(0, 0, 100, &forward(150, 10));
        assert_immediate(outcome, 150, false, true);
    }

    #[test]
    fn forward_from_offset_before_high_polls() {
        let outcome = resolve_seek_plan(0, 0, 100, &forward(50, 10));
        assert_poll(
            outcome,
            SeekPlan {
                start_offset: 50,
                limit: 10,
                stop_at_offset: Some(100),
                at_beginning: false,
                fixed_at_end: None,
            },
        );
    }

    #[test]
    fn forward_from_offset_at_low_is_at_beginning() {
        let outcome = resolve_seek_plan(0, 10, 100, &forward(10, 10));
        assert_poll(
            outcome,
            SeekPlan {
                start_offset: 10,
                limit: 10,
                stop_at_offset: Some(100),
                at_beginning: true,
                fixed_at_end: None,
            },
        );
    }

    #[test]
    fn backward_before_offset_within_range_polls() {
        // low=0, high=100, before_offset=50, page_size=10 -> start_offset=40.
        let outcome = resolve_seek_plan(0, 0, 100, &backward(50, 10));
        assert_poll(
            outcome,
            SeekPlan {
                start_offset: 40,
                limit: 10,
                stop_at_offset: None,
                at_beginning: false,
                fixed_at_end: Some(false),
            },
        );
    }

    #[test]
    fn backward_page_size_larger_than_available_range_clamps_to_low() {
        // low=20, high=100, before_offset=30, page_size=1000 -> start_offset clamped to
        // 20, and the collection limit is the actual gap (10), not the requested
        // page_size (1000).
        let outcome = resolve_seek_plan(0, 20, 100, &backward(30, 1000));
        assert_poll(
            outcome,
            SeekPlan {
                start_offset: 20,
                limit: 10,
                stop_at_offset: None,
                at_beginning: true,
                fixed_at_end: Some(false),
            },
        );
    }

    #[test]
    fn backward_already_at_beginning_is_immediate() {
        // start_offset (clamped to low=20) >= before_offset=20 -> nothing to page backward.
        let outcome = resolve_seek_plan(0, 20, 100, &backward(20, 10));
        assert_immediate(outcome, 20, true, false);
    }

    #[test]
    fn backward_before_offset_below_low_is_immediate() {
        // before_offset itself is below low (shouldn't normally happen, but must not
        // panic or underflow): start_offset clamps to low which is >= before_offset.
        let outcome = resolve_seek_plan(0, 50, 100, &backward(10, 10));
        assert_immediate(outcome, 50, true, false);
    }
}
