use serde::{Deserialize, Serialize};

/// Authentication mode for a cluster profile.
///
/// Tagged so a future `Sasl` variant is additive to both the enum and the
/// `Profile` -> `rdkafka::ClientConfig` mapping, without touching existing
/// TOML files or this struct's shape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum AuthMode {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "mtls")]
    Mtls {
        cert_path: String,
        key_path: String,
        ca_path: String,
    },
    // Future: Sasl { mechanism: SaslMechanism, username: String, password: String },
}

impl Default for AuthMode {
    fn default() -> Self {
        AuthMode::None
    }
}
