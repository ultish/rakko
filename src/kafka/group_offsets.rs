//! Consumer-group listing, lag computation, and offset reset.
//!
//! rdkafka's `AdminClient` does not expose group offset APIs, so this module works
//! around that with a throwaway `BaseConsumer`:
//! - list groups/members: `fetch_group_list`
//! - lag: `committed_offsets` for the real `group.id` vs. high watermarks
//! - reset: `assign` target offsets then `commit(Sync)` with auto-commit off
//!
//! Offset reset is only reliable when the group has no active members — callers must
//! surface the active-member warning from `GroupDetail.has_active_members` before
//! confirming.

use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use crate::config::Profile;
use crate::error::{AppError, AppResult};
use crate::kafka::client_config::consumer_client_config;

const TIMEOUT: Duration = Duration::from_secs(10);
/// Throwaway group id used only for cluster-wide metadata / group listing.
const ADMIN_GROUP_ID: &str = "rakko-admin";

#[derive(Debug, Clone)]
pub struct GroupSummary {
    pub name: String,
    pub state: String,
    pub member_count: usize,
    pub protocol: String,
    pub protocol_type: String,
}

#[derive(Debug, Clone)]
pub struct GroupMember {
    pub id: String,
    pub client_id: String,
    pub client_host: String,
}

#[derive(Debug, Clone)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: Option<i64>,
    pub high_watermark: i64,
    pub low_watermark: i64,
    pub lag: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct GroupDetail {
    pub name: String,
    pub state: String,
    pub members: Vec<GroupMember>,
    pub lags: Vec<PartitionLag>,
}

impl GroupDetail {
    pub fn has_active_members(&self) -> bool {
        !self.members.is_empty()
    }

    pub fn total_lag(&self) -> i64 {
        self.lags.iter().filter_map(|lag| lag.lag).sum()
    }
}

/// Where to reset consumer-group offsets. Absolute offsets are "next message to
/// consume" (Kafka commit semantics); earliest/latest resolve via watermarks;
/// timestamp resolves via `offsets_for_times`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetResetTarget {
    Earliest,
    Latest,
    Absolute(i64),
    /// Epoch milliseconds.
    Timestamp(i64),
}

/// Pure lag math — unit-tested without a broker.
///
/// Returns `None` when there is no committed offset (the group has never consumed
/// this partition), so UI can show "—" rather than inventing lag.
pub fn compute_lag(committed: Option<i64>, high_watermark: i64) -> Option<i64> {
    committed.map(|offset| (high_watermark - offset).max(0))
}

/// Pure resolution of a reset target against watermarks / a timestamp lookup result.
/// Used by the reset path and unit-tested without a broker.
pub fn resolve_reset_offset(
    target: &OffsetResetTarget,
    low: i64,
    high: i64,
    timestamp_offset: Option<i64>,
) -> AppResult<i64> {
    match target {
        OffsetResetTarget::Earliest => Ok(low),
        OffsetResetTarget::Latest => Ok(high),
        OffsetResetTarget::Absolute(offset) => {
            if *offset < 0 {
                return Err(AppError::Other(format!(
                    "absolute offset must be >= 0, got {offset}"
                )));
            }
            // Clamp into the currently available range so a commit outside
            // [low, high] doesn't leave the group permanently stuck.
            Ok((*offset).clamp(low, high))
        }
        OffsetResetTarget::Timestamp(_) => {
            let offset = timestamp_offset.ok_or_else(|| {
                AppError::Other("no offset found for the given timestamp".into())
            })?;
            if offset < 0 {
                // librdkafka returns -1 when the timestamp is past the end of the log.
                Ok(high)
            } else {
                Ok(offset.clamp(low, high))
            }
        }
    }
}

/// Lists all consumer groups known to the cluster (name, state, member count).
pub async fn list_groups(profile: &Profile) -> AppResult<Vec<GroupSummary>> {
    let profile = profile.clone();
    tokio::task::spawn_blocking(move || list_groups_blocking(&profile))
        .await
        .map_err(|err| AppError::Other(format!("list groups task panicked: {err}")))?
}

fn list_groups_blocking(profile: &Profile) -> AppResult<Vec<GroupSummary>> {
    let consumer: BaseConsumer = consumer_client_config(profile, ADMIN_GROUP_ID).create()?;
    let group_list = consumer.fetch_group_list(None, TIMEOUT)?;

    let mut groups: Vec<GroupSummary> = group_list
        .groups()
        .iter()
        .map(|group| GroupSummary {
            name: group.name().to_string(),
            state: group.state().to_string(),
            member_count: group.members().len(),
            protocol: group.protocol().to_string(),
            protocol_type: group.protocol_type().to_string(),
        })
        .collect();

    groups.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(groups)
}

/// Loads members + per-partition lag for a single consumer group.
pub async fn describe_group(profile: &Profile, group_id: &str) -> AppResult<GroupDetail> {
    let profile = profile.clone();
    let group_id = group_id.to_string();
    tokio::task::spawn_blocking(move || describe_group_blocking(&profile, &group_id))
        .await
        .map_err(|err| AppError::Other(format!("describe group task panicked: {err}")))?
}

fn describe_group_blocking(profile: &Profile, group_id: &str) -> AppResult<GroupDetail> {
    let listing_consumer: BaseConsumer =
        consumer_client_config(profile, ADMIN_GROUP_ID).create()?;
    let group_list = listing_consumer.fetch_group_list(Some(group_id), TIMEOUT)?;

    let group_info = group_list
        .groups()
        .iter()
        .find(|g| g.name() == group_id)
        .ok_or_else(|| AppError::Other(format!("consumer group '{group_id}' not found")))?;

    let members: Vec<GroupMember> = group_info
        .members()
        .iter()
        .map(|member| GroupMember {
            id: member.id().to_string(),
            client_id: member.client_id().to_string(),
            client_host: member.client_host().to_string(),
        })
        .collect();
    let state = group_info.state().to_string();

    // Lag consumer uses the *real* group id so committed_offsets reads that group's
    // stored offsets. enable.auto.commit is false and we never poll, so this is
    // read-only and safe while the real group is active.
    let lag_consumer: BaseConsumer = consumer_client_config(profile, group_id).create()?;
    let metadata = lag_consumer.fetch_metadata(None, TIMEOUT)?;

    let mut tpl = TopicPartitionList::new();
    for topic in metadata.topics() {
        let name = topic.name();
        if name.starts_with("__") {
            continue;
        }
        for partition in topic.partitions() {
            tpl.add_partition(name, partition.id());
        }
    }

    let committed = if tpl.count() == 0 {
        TopicPartitionList::new()
    } else {
        lag_consumer.committed_offsets(tpl, TIMEOUT)?
    };

    let mut lags = Vec::new();
    for elem in committed.elements() {
        let topic = elem.topic().to_string();
        let partition = elem.partition();
        let committed_offset = match elem.offset() {
            Offset::Offset(n) if n >= 0 => Some(n),
            _ => None,
        };

        let (low, high) = match lag_consumer.fetch_watermarks(&topic, partition, TIMEOUT) {
            Ok(pair) => pair,
            Err(err) => {
                tracing::warn!(
                    "failed to fetch watermarks for {topic}/{partition}: {err}; skipping"
                );
                continue;
            }
        };

        lags.push(PartitionLag {
            topic,
            partition,
            committed_offset,
            high_watermark: high,
            low_watermark: low,
            lag: compute_lag(committed_offset, high),
        });
    }

    // Only surface partitions the group has actually committed to (or has lag on).
    // Empty groups with no history show an empty lag table rather than every topic.
    lags.retain(|lag| lag.committed_offset.is_some());
    lags.sort_by(|a, b| {
        a.topic
            .cmp(&b.topic)
            .then(a.partition.cmp(&b.partition))
    });

    Ok(GroupDetail {
        name: group_id.to_string(),
        state,
        members,
        lags,
    })
}

/// Resets offsets for every partition currently present in the group's lag table
/// (i.e. partitions with a prior committed offset). Partitions without history are
/// left alone — creating offsets for arbitrary topics is out of scope for v1.
pub async fn reset_group_offsets(
    profile: &Profile,
    group_id: &str,
    target: OffsetResetTarget,
    partitions: &[(String, i32)],
) -> AppResult<()> {
    let profile = profile.clone();
    let group_id = group_id.to_string();
    let partitions = partitions.to_vec();
    tokio::task::spawn_blocking(move || {
        reset_group_offsets_blocking(&profile, &group_id, &target, &partitions)
    })
    .await
    .map_err(|err| AppError::Other(format!("reset offsets task panicked: {err}")))?
}

fn reset_group_offsets_blocking(
    profile: &Profile,
    group_id: &str,
    target: &OffsetResetTarget,
    partitions: &[(String, i32)],
) -> AppResult<()> {
    if partitions.is_empty() {
        return Err(AppError::Other(
            "no partitions with committed offsets to reset".into(),
        ));
    }

    // Re-check membership immediately before the destructive commit so a group that
    // became active between dialog open and confirm still surfaces a hard error.
    let listing: BaseConsumer = consumer_client_config(profile, ADMIN_GROUP_ID).create()?;
    let group_list = listing.fetch_group_list(Some(group_id), TIMEOUT)?;
    if let Some(info) = group_list.groups().iter().find(|g| g.name() == group_id) {
        if !info.members().is_empty() {
            return Err(AppError::Other(format!(
                "group '{group_id}' has {} active member(s); stop consumers before resetting offsets",
                info.members().len()
            )));
        }
    }

    let consumer: BaseConsumer = consumer_client_config(profile, group_id).create()?;

    // For timestamp targets, resolve all partitions in one offsets_for_times call.
    let timestamp_map: std::collections::HashMap<(String, i32), i64> =
        if let OffsetResetTarget::Timestamp(ts) = target {
            let mut tpl = TopicPartitionList::with_capacity(partitions.len());
            for (topic, partition) in partitions {
                tpl.add_partition_offset(topic, *partition, Offset::Offset(*ts))?;
            }
            let resolved = consumer.offsets_for_times(tpl, TIMEOUT)?;
            resolved
                .elements()
                .into_iter()
                .filter_map(|elem| match elem.offset() {
                    Offset::Offset(n) => Some(((elem.topic().to_string(), elem.partition()), n)),
                    _ => None,
                })
                .collect()
        } else {
            std::collections::HashMap::new()
        };

    let mut commit_tpl = TopicPartitionList::with_capacity(partitions.len());
    for (topic, partition) in partitions {
        let (low, high) = consumer.fetch_watermarks(topic, *partition, TIMEOUT)?;
        let ts_offset = timestamp_map.get(&(topic.clone(), *partition)).copied();
        let offset = resolve_reset_offset(target, low, high, ts_offset)?;
        commit_tpl.add_partition_offset(topic, *partition, Offset::Offset(offset))?;
    }

    // assign + commit is the documented workaround for AdminClient's missing group
    // offset APIs. The consumer never joins the group as a member (no subscribe).
    consumer.assign(&commit_tpl)?;
    consumer.commit(&commit_tpl, CommitMode::Sync)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_is_high_minus_committed_floored_at_zero() {
        assert_eq!(compute_lag(Some(10), 15), Some(5));
        assert_eq!(compute_lag(Some(15), 15), Some(0));
        assert_eq!(compute_lag(Some(20), 15), Some(0));
        assert_eq!(compute_lag(None, 15), None);
    }

    #[test]
    fn resolve_earliest_and_latest() {
        assert_eq!(
            resolve_reset_offset(&OffsetResetTarget::Earliest, 5, 100, None).unwrap(),
            5
        );
        assert_eq!(
            resolve_reset_offset(&OffsetResetTarget::Latest, 5, 100, None).unwrap(),
            100
        );
    }

    #[test]
    fn resolve_absolute_clamps_into_range() {
        assert_eq!(
            resolve_reset_offset(&OffsetResetTarget::Absolute(50), 5, 100, None).unwrap(),
            50
        );
        assert_eq!(
            resolve_reset_offset(&OffsetResetTarget::Absolute(0), 5, 100, None).unwrap(),
            5
        );
        assert_eq!(
            resolve_reset_offset(&OffsetResetTarget::Absolute(200), 5, 100, None).unwrap(),
            100
        );
    }

    #[test]
    fn resolve_absolute_rejects_negative() {
        assert!(resolve_reset_offset(&OffsetResetTarget::Absolute(-1), 0, 10, None).is_err());
    }

    #[test]
    fn resolve_timestamp_uses_lookup_and_clamps() {
        assert_eq!(
            resolve_reset_offset(&OffsetResetTarget::Timestamp(1_700_000_000_000), 0, 100, Some(42))
                .unwrap(),
            42
        );
        // past end of log
        assert_eq!(
            resolve_reset_offset(&OffsetResetTarget::Timestamp(1), 0, 100, Some(-1)).unwrap(),
            100
        );
        assert!(resolve_reset_offset(&OffsetResetTarget::Timestamp(1), 0, 100, None).is_err());
    }

    #[test]
    fn group_detail_active_members_and_total_lag() {
        let detail = GroupDetail {
            name: "g".into(),
            state: "Stable".into(),
            members: vec![GroupMember {
                id: "m1".into(),
                client_id: "c".into(),
                client_host: "h".into(),
            }],
            lags: vec![
                PartitionLag {
                    topic: "t".into(),
                    partition: 0,
                    committed_offset: Some(10),
                    high_watermark: 15,
                    low_watermark: 0,
                    lag: Some(5),
                },
                PartitionLag {
                    topic: "t".into(),
                    partition: 1,
                    committed_offset: None,
                    high_watermark: 3,
                    low_watermark: 0,
                    lag: None,
                },
            ],
        };
        assert!(detail.has_active_members());
        assert_eq!(detail.total_lag(), 5);
    }
}
