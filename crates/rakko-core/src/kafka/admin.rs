use std::collections::HashMap;
use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};

use crate::config::Profile;
use crate::error::{AppError, AppResult};
use crate::kafka::client_config::consumer_client_config;

/// Throwaway `group.id` for admin-only consumers (metadata/watermark lookups never
/// commit offsets or join a real consumer group).
const ADMIN_GROUP_ID: &str = "rakko-admin";
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const WATERMARK_TIMEOUT: Duration = Duration::from_secs(10);
const DESCRIBE_CONFIGS_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct TopicSummary {
    pub name: String,
    pub partition_count: usize,
    pub replication_factor: i32,
    pub compression_type: String,
    pub total_message_count: i64,
}

#[derive(Debug, Clone)]
pub struct BrokerSummary {
    pub id: i32,
    pub host: String,
    pub port: i32,
    /// Partitions this broker leads. Computed from the same `fetch_metadata()` call
    /// used to list brokers (no extra round trip) — shows load distribution.
    pub leader_partitions: usize,
    /// Partitions this broker holds a replica for (including the led ones).
    pub replica_partitions: usize,
}

/// Cluster-wide partition health, computed from the same `fetch_metadata()` call used
/// to list brokers (no extra round trip).
#[derive(Debug, Clone, Copy, Default)]
pub struct ClusterHealth {
    pub under_replicated: usize,
    pub offline: usize,
}

/// A single non-default broker config entry (`describe_configs`, filtered to values
/// that differ from Kafka's hardcoded default — mirrors `kafka-configs.sh --describe`
/// without `--all`, since the full default set is ~200 entries of noise).
#[derive(Debug, Clone)]
pub struct BrokerConfigEntry {
    pub name: String,
    pub value: String,
}

/// Kafka/librdkafka convention: internal topics (e.g. `__consumer_offsets`,
/// `__transaction_state`) are prefixed with `__` and shouldn't show up in a
/// user-facing topic list.
fn is_internal_topic(name: &str) -> bool {
    name.starts_with("__")
}

/// Lists all non-internal topics on the cluster with basic per-topic stats.
///
/// rdkafka's metadata/watermark/describe-configs calls are blocking (or, for
/// describe_configs, driven by a dedicated background polling thread rather than the
/// tokio reactor - see `AdminClient`'s internals), so the whole lookup runs on a
/// blocking-pool thread via `spawn_blocking` rather than inline on the render loop.
pub async fn list_topics(profile: &Profile) -> AppResult<Vec<TopicSummary>> {
    let profile = profile.clone();
    tokio::task::spawn_blocking(move || list_topics_blocking(&profile))
        .await
        .map_err(|err| AppError::Other(format!("topic listing task panicked: {err}")))?
}

fn list_topics_blocking(profile: &Profile) -> AppResult<Vec<TopicSummary>> {
    let consumer: BaseConsumer = consumer_client_config(profile, ADMIN_GROUP_ID).create()?;
    let admin_client: AdminClient<DefaultClientContext> =
        consumer_client_config(profile, ADMIN_GROUP_ID).create()?;

    let metadata = consumer.fetch_metadata(None, METADATA_TIMEOUT)?;

    let mut summaries = Vec::new();
    for topic in metadata.topics() {
        let name = topic.name();
        if is_internal_topic(name) {
            continue;
        }

        let partition_count = topic.partitions().len();
        // Replication factor of partition 0, assumed uniform across all partitions of
        // the topic - a reasonable v1 simplification (real clusters can briefly skew
        // per-partition during a reassignment, but that's not surfaced here).
        let replication_factor = topic
            .partitions()
            .first()
            .map(|p| p.replicas().len() as i32)
            .unwrap_or(0);

        let mut total_message_count: i64 = 0;
        for partition in topic.partitions() {
            if let Ok((low, high)) =
                consumer.fetch_watermarks(name, partition.id(), WATERMARK_TIMEOUT)
            {
                total_message_count += high - low;
            }
        }

        summaries.push(TopicSummary {
            name: name.to_string(),
            partition_count,
            replication_factor,
            compression_type: fetch_compression_type(&admin_client, name),
            total_message_count,
        });
    }

    Ok(summaries)
}

/// Best-effort `compression.type` lookup: this is a nice-to-have stat, so any failure
/// (timeout, missing config entry, etc) falls back to `"unknown"` rather than failing
/// the whole topic listing.
fn fetch_compression_type(admin_client: &AdminClient<DefaultClientContext>, topic: &str) -> String {
    let specifier = ResourceSpecifier::Topic(topic);
    let opts = AdminOptions::new().request_timeout(Some(DESCRIBE_CONFIGS_TIMEOUT));

    // `describe_configs` returns a future driven by AdminClient's own background
    // polling thread (not the tokio reactor), so it's safe to block on it here from
    // this synchronous, spawn_blocking-run function.
    let result = futures::executor::block_on(admin_client.describe_configs([&specifier], &opts));

    result
        .ok()
        .into_iter()
        .flatten()
        .next()
        .and_then(|resource| resource.ok())
        .and_then(|resource| resource.get("compression.type")?.value.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Lists all brokers plus cluster-wide health, from a single `fetch_metadata()` call.
///
/// Under-replicated = a partition whose ISR set is smaller than its replica set;
/// offline = a partition with no leader (`leader() == -1`) or a metadata error.
/// No `describe_cluster`/KIP-700 binding exists in rdkafka 0.39's Rust API, so there is
/// no safe-Rust way to identify the controller broker — not attempted here.
pub async fn list_brokers(profile: &Profile) -> AppResult<(Vec<BrokerSummary>, ClusterHealth)> {
    let profile = profile.clone();
    tokio::task::spawn_blocking(move || list_brokers_blocking(&profile))
        .await
        .map_err(|err| AppError::Other(format!("broker listing task panicked: {err}")))?
}

fn list_brokers_blocking(profile: &Profile) -> AppResult<(Vec<BrokerSummary>, ClusterHealth)> {
    let consumer: BaseConsumer = consumer_client_config(profile, ADMIN_GROUP_ID).create()?;
    let metadata = consumer.fetch_metadata(None, METADATA_TIMEOUT)?;

    let mut leader_counts: HashMap<i32, usize> = HashMap::new();
    let mut replica_counts: HashMap<i32, usize> = HashMap::new();
    let mut health = ClusterHealth::default();
    for topic in metadata.topics() {
        for partition in topic.partitions() {
            *leader_counts.entry(partition.leader()).or_insert(0) += 1;
            for replica in partition.replicas() {
                *replica_counts.entry(*replica).or_insert(0) += 1;
            }
            if partition.isr().len() < partition.replicas().len() {
                health.under_replicated += 1;
            }
            if partition.leader() == -1 || partition.error().is_some() {
                health.offline += 1;
            }
        }
    }

    let brokers = metadata
        .brokers()
        .iter()
        .map(|b| BrokerSummary {
            id: b.id(),
            host: b.host().to_string(),
            port: b.port(),
            leader_partitions: leader_counts.get(&b.id()).copied().unwrap_or(0),
            replica_partitions: replica_counts.get(&b.id()).copied().unwrap_or(0),
        })
        .collect();

    Ok((brokers, health))
}

/// Fetches a single broker's non-default config values via `describe_configs`.
/// Sensitive entries (`is_sensitive`) are redacted rather than sent to the UI.
pub async fn fetch_broker_configs(profile: &Profile, broker_id: i32) -> AppResult<Vec<BrokerConfigEntry>> {
    let profile = profile.clone();
    tokio::task::spawn_blocking(move || fetch_broker_configs_blocking(&profile, broker_id))
        .await
        .map_err(|err| AppError::Other(format!("broker config fetch task panicked: {err}")))?
}

fn fetch_broker_configs_blocking(profile: &Profile, broker_id: i32) -> AppResult<Vec<BrokerConfigEntry>> {
    let admin_client: AdminClient<DefaultClientContext> =
        consumer_client_config(profile, ADMIN_GROUP_ID).create()?;
    let specifier = ResourceSpecifier::Broker(broker_id);
    let opts = AdminOptions::new().request_timeout(Some(DESCRIBE_CONFIGS_TIMEOUT));
    let results = futures::executor::block_on(admin_client.describe_configs([&specifier], &opts))?;

    let resource = results
        .into_iter()
        .next()
        .ok_or_else(|| AppError::Other(format!("no config resource returned for broker {broker_id}")))?
        .map_err(|err| {
            AppError::Other(format!("describe_configs failed for broker {broker_id}: {err}"))
        })?;

    let mut entries: Vec<BrokerConfigEntry> = resource
        .entries
        .into_iter()
        .filter(|e| !e.is_default)
        .map(|e| BrokerConfigEntry {
            name: e.name,
            value: if e.is_sensitive {
                "<sensitive>".to_string()
            } else {
                e.value.unwrap_or_default()
            },
        })
        .collect();
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}

/// Reads the broker's `message.max.bytes` via `describe_configs` on the first
/// broker from metadata. Used to auto-fill profile `message_max_bytes` when unset.
///
/// Returns `Ok(None)` when the config is missing or unparseable (caller keeps the
/// client default). Errors are connection/admin failures.
pub async fn fetch_broker_message_max_bytes(profile: &Profile) -> AppResult<Option<u32>> {
    let profile = profile.clone();
    tokio::task::spawn_blocking(move || fetch_broker_message_max_bytes_blocking(&profile))
        .await
        .map_err(|err| AppError::Other(format!("message.max.bytes detect task panicked: {err}")))?
}

fn fetch_broker_message_max_bytes_blocking(profile: &Profile) -> AppResult<Option<u32>> {
    let consumer: BaseConsumer = consumer_client_config(profile, ADMIN_GROUP_ID).create()?;
    let admin_client: AdminClient<DefaultClientContext> =
        consumer_client_config(profile, ADMIN_GROUP_ID).create()?;

    let metadata = consumer.fetch_metadata(None, METADATA_TIMEOUT)?;
    let broker_id = metadata
        .brokers()
        .first()
        .map(|b| b.id())
        .ok_or_else(|| AppError::Other("cluster metadata lists no brokers".into()))?;

    let specifier = ResourceSpecifier::Broker(broker_id);
    let opts = AdminOptions::new().request_timeout(Some(DESCRIBE_CONFIGS_TIMEOUT));
    let result = futures::executor::block_on(admin_client.describe_configs([&specifier], &opts));

    let value = result
        .ok()
        .into_iter()
        .flatten()
        .next()
        .and_then(|resource| resource.ok())
        .and_then(|resource| resource.get("message.max.bytes")?.value.clone());

    Ok(value.as_deref().and_then(parse_message_max_bytes))
}

/// Parse a broker config string for `message.max.bytes` into a positive `u32`.
fn parse_message_max_bytes(raw: &str) -> Option<u32> {
    let n: u64 = raw.trim().parse().ok()?;
    if n == 0 || n > u64::from(u32::MAX) {
        return None;
    }
    Some(n as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_dunder_prefixed_internal_topics() {
        assert!(is_internal_topic("__consumer_offsets"));
        assert!(is_internal_topic("__transaction_state"));
        assert!(!is_internal_topic("orders"));
        assert!(!is_internal_topic("_single_underscore_not_internal"));
    }

    #[test]
    fn parse_message_max_bytes_accepts_positive_u32() {
        assert_eq!(parse_message_max_bytes("1048576"), Some(1_048_576));
        assert_eq!(parse_message_max_bytes("20971520"), Some(20_971_520));
        assert_eq!(parse_message_max_bytes("  1000  "), Some(1000));
        assert_eq!(parse_message_max_bytes("0"), None);
        assert_eq!(parse_message_max_bytes("-1"), None);
        assert_eq!(parse_message_max_bytes("not-a-number"), None);
    }
}

/// Docker-compose-gated: `docker compose up -d` then `cargo test -- --ignored`.
/// See `kafka::integration_support` for the shared setup rationale.
#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::kafka::integration_support::{local_profile, unique_name};
    use crate::kafka::producer;

    #[tokio::test]
    #[ignore = "requires `docker compose up -d` (localhost:19092)"]
    async fn list_topics_finds_a_freshly_produced_topic() {
        let profile = local_profile();
        let topic = unique_name("listing");

        producer::produce(&profile, &topic, None, Some(b"v".to_vec()), Vec::new())
            .await
            .expect("produce should succeed against the local broker");

        let topics = list_topics(&profile).await.expect("list_topics");
        let found = topics
            .iter()
            .find(|t| t.name == topic)
            .unwrap_or_else(|| panic!("topic {topic} not found in {topics:?}"));

        assert_eq!(found.partition_count, 1);
        assert_eq!(found.total_message_count, 1);
    }

    #[tokio::test]
    #[ignore = "requires `docker compose up -d` (localhost:19092)"]
    async fn fetch_broker_message_max_bytes_reflects_the_compose_override() {
        let profile = local_profile();

        // docker-compose.yml sets KAFKA_MESSAGE_MAX_BYTES=20971520 for rakko
        // producer/replay testing (see the file's comment).
        let detected = fetch_broker_message_max_bytes(&profile)
            .await
            .expect("fetch_broker_message_max_bytes");
        assert_eq!(detected, Some(20_971_520));
    }

    #[tokio::test]
    #[ignore = "requires `docker compose up -d` (localhost:19092)"]
    async fn list_brokers_finds_the_single_node_cluster_and_a_led_partition() {
        let profile = local_profile();
        let topic = unique_name("brokerlist");
        producer::produce(&profile, &topic, None, Some(b"v".to_vec()), Vec::new())
            .await
            .expect("produce should succeed against the local broker");

        let (brokers, health) = list_brokers(&profile).await.expect("list_brokers");
        assert_eq!(brokers.len(), 1, "docker-compose stack is single-node KRaft");
        let broker = &brokers[0];
        // The freshly produced topic's single partition is led (and replicated) by
        // the only broker in this single-node stack.
        assert!(broker.leader_partitions >= 1);
        assert!(broker.replica_partitions >= broker.leader_partitions);
        // Single-node, replication factor 1: nothing under-replicated or offline.
        assert_eq!(health.under_replicated, 0);
        assert_eq!(health.offline, 0);
    }

    #[tokio::test]
    #[ignore = "requires `docker compose up -d` (localhost:19092)"]
    async fn fetch_broker_configs_includes_the_compose_message_max_bytes_override() {
        let profile = local_profile();
        let (brokers, _) = list_brokers(&profile).await.expect("list_brokers");
        let broker_id = brokers.first().expect("at least one broker").id;

        let entries = fetch_broker_configs(&profile, broker_id)
            .await
            .expect("fetch_broker_configs");
        let entry = entries
            .iter()
            .find(|e| e.name == "message.max.bytes")
            .unwrap_or_else(|| panic!("message.max.bytes not in non-default entries: {entries:?}"));
        assert_eq!(entry.value, "20971520");
        // Non-default filter must actually be filtering — the full default set is
        // ~200 entries, so a real broker returning far fewer confirms the filter ran.
        assert!(entries.len() < 100);
    }
}
