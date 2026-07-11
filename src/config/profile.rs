use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::auth::AuthMode;

/// A named connection to a single Kafka cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Profile {
    pub name: String,
    /// e.g. "localhost:9092,localhost:9093"
    pub bootstrap_servers: String,
    #[serde(default)]
    pub tls_enabled: bool,
    #[serde(default)]
    pub auth: AuthMode,
    /// Schema Registry base URL, if this cluster has one (e.g. "http://localhost:8081").
    #[serde(default)]
    pub schema_registry_url: Option<String>,
    /// Consumer-side `message.max.bytes` override, for brokers configured with
    /// `max.message.bytes` above Kafka's 1MB default.
    #[serde(default)]
    pub message_max_bytes: Option<u32>,
    /// Extra raw librdkafka producer config properties (e.g. "compression.type" = "zstd"),
    /// applied on top of the mapped profile settings without hardcoding every possible
    /// property into this struct.
    #[serde(default)]
    pub extra_producer_config: HashMap<String, String>,
}

impl Profile {
    pub fn security_protocol(&self) -> &'static str {
        match (&self.auth, self.tls_enabled) {
            (AuthMode::None, false) => "PLAINTEXT",
            (AuthMode::None, true) => "SSL",
            (AuthMode::Mtls { .. }, _) => "SSL",
        }
    }
}
