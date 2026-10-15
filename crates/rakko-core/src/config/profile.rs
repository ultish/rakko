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
    /// TLS settings for an `https://` registry. Anything left unset falls back to the
    /// cluster's own TLS material (`auth`'s CA / client cert), so the common case — one
    /// private CA for Kafka and the registry — needs no extra config.
    #[serde(default)]
    pub schema_registry_tls: RegistryTls,
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

/// Optional TLS overrides for the Schema Registry connection (PEM files).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RegistryTls {
    /// CA bundle that signed the registry's certificate.
    #[serde(default)]
    pub ca_path: Option<String>,
    /// Client certificate (chain) and key, if the registry requires mutual TLS.
    #[serde(default)]
    pub client_cert_path: Option<String>,
    #[serde(default)]
    pub client_key_path: Option<String>,
}

impl Profile {
    /// `(ca, client cert, client key)` paths to use for the registry connection.
    pub fn registry_tls_paths(&self) -> (Option<String>, Option<(String, String)>) {
        let (auth_ca, auth_pair) = match &self.auth {
            AuthMode::Tls { ca_path } => (Some(ca_path.clone()), None),
            AuthMode::Mtls { cert_path, key_path, ca_path } => {
                (Some(ca_path.clone()), Some((cert_path.clone(), key_path.clone())))
            }
            AuthMode::None => (None, None),
        };
        let own = &self.schema_registry_tls;
        let pair = match (&own.client_cert_path, &own.client_key_path) {
            (Some(c), Some(k)) => Some((c.clone(), k.clone())),
            _ => auth_pair,
        };
        (own.ca_path.clone().or(auth_ca), pair)
    }

    pub fn security_protocol(&self) -> &'static str {
        match (&self.auth, self.tls_enabled) {
            (AuthMode::None, false) => "PLAINTEXT",
            (AuthMode::None, true) => "SSL",
            (AuthMode::Tls { .. }, _) => "SSL",
            (AuthMode::Mtls { .. }, _) => "SSL",
        }
    }
}
