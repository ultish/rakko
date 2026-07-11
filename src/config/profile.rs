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
    /// Client `message.max.bytes` (produce + consume). When `None`, the first
    /// successful topic load auto-detects the broker's `message.max.bytes` and
    /// persists it here. Set explicitly to pin a value and skip auto-detect.
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
            (AuthMode::Tls { .. }, _) => "SSL",
            (AuthMode::Mtls { .. }, _) => "SSL",
        }
    }
}
