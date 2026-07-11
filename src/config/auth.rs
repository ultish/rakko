use serde::{Deserialize, Serialize};

/// Authentication mode for a cluster profile.
///
/// Tagged so a future `Sasl` variant is additive to both the enum and the
/// `Profile` -> `rdkafka::ClientConfig` mapping, without touching existing
/// TOML files or this struct's shape.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum AuthMode {
    /// No client auth. TLS (if `Profile.tls_enabled`) verifies against the system's
    /// default trust store — fine for a broker cert signed by a public CA, but there's
    /// no way to point at a private CA here. Use `Tls` for that.
    #[serde(rename = "none")]
    #[default]
    None,
    /// Server-side TLS verified against a custom CA, no client certificate presented.
    /// The common case for an internal Kafka cluster whose broker cert is signed by a
    /// private CA but that doesn't require mutual TLS.
    #[serde(rename = "tls")]
    Tls { ca_path: String },
    /// Mutual TLS: client cert + key, verified against `ca_path`.
    #[serde(rename = "mtls")]
    Mtls {
        cert_path: String,
        key_path: String,
        ca_path: String,
    },
    // Future: Sasl { mechanism: SaslMechanism, username: String, password: String },
}
