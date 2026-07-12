pub mod admin;
pub mod client_config;
pub mod consumer;
pub mod group_offsets;
pub mod producer;
pub mod schema_registry;

/// Docker-compose-gated integration test helpers — see `integration_support`'s doc
/// comment. Compiled only under `cargo test`, unused otherwise.
#[cfg(test)]
pub(crate) mod integration_support;

/// Thin per-profile facade over the free functions in this module's submodules, so
/// callers hold one handle per connected cluster instead of threading a `Profile`
/// through every call site.
pub struct KafkaClient {
    profile: crate::config::Profile,
}

impl KafkaClient {
    pub fn new(profile: crate::config::Profile) -> Self {
        KafkaClient { profile }
    }

    pub async fn list_topics(&self) -> crate::error::AppResult<Vec<admin::TopicSummary>> {
        admin::list_topics(&self.profile).await
    }

    pub async fn list_groups(&self) -> crate::error::AppResult<Vec<group_offsets::GroupSummary>> {
        group_offsets::list_groups(&self.profile).await
    }

    pub async fn list_brokers(
        &self,
    ) -> crate::error::AppResult<(Vec<admin::BrokerSummary>, admin::ClusterHealth)> {
        admin::list_brokers(&self.profile).await
    }

    pub async fn describe_group(
        &self,
        group_id: &str,
    ) -> crate::error::AppResult<group_offsets::GroupDetail> {
        group_offsets::describe_group(&self.profile, group_id).await
    }

    pub async fn reset_group_offsets(
        &self,
        group_id: &str,
        target: group_offsets::OffsetResetTarget,
        partitions: &[(String, i32)],
    ) -> crate::error::AppResult<()> {
        group_offsets::reset_group_offsets(&self.profile, group_id, target, partitions).await
    }

    pub async fn produce(
        &self,
        topic: &str,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<(String, Vec<u8>)>,
    ) -> crate::error::AppResult<()> {
        producer::produce(&self.profile, topic, key, value, headers).await
    }
}
