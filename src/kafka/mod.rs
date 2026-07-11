pub mod admin;
pub mod client_config;
pub mod consumer;

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
}
