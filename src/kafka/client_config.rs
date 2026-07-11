use rdkafka::ClientConfig;

use crate::config::{AuthMode, Profile};

/// Maps a `Profile` onto an `rdkafka::ClientConfig`.
///
/// | Profile fields | librdkafka `security.protocol` | extra properties |
/// |---|---|---|
/// | `tls_enabled = false`, `auth = None` | `PLAINTEXT` | - |
/// | `tls_enabled = true`, `auth = None` | `SSL` | - (verifies against the system default trust store) |
/// | `auth = Tls { ca }` | `SSL` | `ssl.ca.location` (no client cert - server TLS against a private CA) |
/// | `auth = Mtls { cert, key, ca }` | `SSL` | `ssl.ca.location`, `ssl.certificate.location`, `ssl.key.location` |
pub fn base_client_config(profile: &Profile) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &profile.bootstrap_servers);
    config.set("security.protocol", profile.security_protocol());

    match &profile.auth {
        AuthMode::None => {}
        AuthMode::Tls { ca_path } => {
            config.set("ssl.ca.location", ca_path);
        }
        AuthMode::Mtls { cert_path, key_path, ca_path } => {
            config.set("ssl.certificate.location", cert_path);
            config.set("ssl.key.location", key_path);
            config.set("ssl.ca.location", ca_path);
        }
    }

    if let Some(max_bytes) = profile.message_max_bytes {
        config.set("message.max.bytes", max_bytes.to_string());
    }

    config
}

/// Consumer-side client config: base config plus a `group.id` when one is needed
/// (ad-hoc browsing consumers use a throwaway group id; group-offset inspection uses
/// the real group id being inspected).
pub fn consumer_client_config(profile: &Profile, group_id: &str) -> ClientConfig {
    let mut config = base_client_config(profile);
    config.set("group.id", group_id);
    config.set("enable.auto.commit", "false");
    config
}

/// Producer-side client config: base config plus any per-profile extra producer
/// properties (e.g. `compression.type`), applied last so they can override defaults.
pub fn producer_client_config(profile: &Profile) -> ClientConfig {
    let mut config = base_client_config(profile);
    for (key, value) in &profile.extra_producer_config {
        config.set(key, value);
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuthMode;
    use std::collections::HashMap;

    fn profile_with(tls_enabled: bool, auth: AuthMode) -> Profile {
        Profile {
            name: "test".into(),
            bootstrap_servers: "localhost:9092".into(),
            tls_enabled,
            auth,
            schema_registry_url: None,
            message_max_bytes: None,
            extra_producer_config: HashMap::new(),
        }
    }

    fn get(config: &ClientConfig, key: &str) -> Option<String> {
        config.config_map().get(key).map(|value| value.to_string())
    }

    #[test]
    fn plaintext_when_no_tls_and_no_auth() {
        let profile = profile_with(false, AuthMode::None);
        let config = base_client_config(&profile);
        assert_eq!(get(&config, "security.protocol").as_deref(), Some("PLAINTEXT"));
        assert_eq!(get(&config, "bootstrap.servers").as_deref(), Some("localhost:9092"));
    }

    #[test]
    fn ssl_when_tls_enabled_no_auth() {
        let profile = profile_with(true, AuthMode::None);
        let config = base_client_config(&profile);
        assert_eq!(get(&config, "security.protocol").as_deref(), Some("SSL"));
        assert!(get(&config, "ssl.certificate.location").is_none());
    }

    #[test]
    fn ssl_with_custom_ca_and_no_client_cert_for_tls_only() {
        let profile = profile_with(true, AuthMode::Tls { ca_path: "/certs/private-ca.pem".into() });
        let config = base_client_config(&profile);
        assert_eq!(get(&config, "security.protocol").as_deref(), Some("SSL"));
        assert_eq!(get(&config, "ssl.ca.location").as_deref(), Some("/certs/private-ca.pem"));
        assert!(get(&config, "ssl.certificate.location").is_none());
        assert!(get(&config, "ssl.key.location").is_none());
    }

    #[test]
    fn tls_forces_ssl_even_if_tls_enabled_flag_is_false() {
        let profile = profile_with(false, AuthMode::Tls { ca_path: "/certs/private-ca.pem".into() });
        let config = base_client_config(&profile);
        assert_eq!(get(&config, "security.protocol").as_deref(), Some("SSL"));
    }

    #[test]
    fn ssl_with_client_certs_for_mtls() {
        let profile = profile_with(
            true,
            AuthMode::Mtls {
                cert_path: "/certs/client.pem".into(),
                key_path: "/certs/client.key".into(),
                ca_path: "/certs/ca.pem".into(),
            },
        );
        let config = base_client_config(&profile);
        assert_eq!(get(&config, "security.protocol").as_deref(), Some("SSL"));
        assert_eq!(get(&config, "ssl.certificate.location").as_deref(), Some("/certs/client.pem"));
        assert_eq!(get(&config, "ssl.key.location").as_deref(), Some("/certs/client.key"));
        assert_eq!(get(&config, "ssl.ca.location").as_deref(), Some("/certs/ca.pem"));
    }

    #[test]
    fn mtls_forces_ssl_even_if_tls_enabled_flag_is_false() {
        let profile = profile_with(
            false,
            AuthMode::Mtls {
                cert_path: "/certs/client.pem".into(),
                key_path: "/certs/client.key".into(),
                ca_path: "/certs/ca.pem".into(),
            },
        );
        let config = base_client_config(&profile);
        assert_eq!(get(&config, "security.protocol").as_deref(), Some("SSL"));
    }

    #[test]
    fn message_max_bytes_is_applied_when_set() {
        let mut profile = profile_with(false, AuthMode::None);
        profile.message_max_bytes = Some(20_000_000);
        let config = base_client_config(&profile);
        assert_eq!(get(&config, "message.max.bytes").as_deref(), Some("20000000"));
    }

    #[test]
    fn consumer_config_sets_group_id_and_disables_auto_commit() {
        let profile = profile_with(false, AuthMode::None);
        let config = consumer_client_config(&profile, "my-group");
        assert_eq!(get(&config, "group.id").as_deref(), Some("my-group"));
        assert_eq!(get(&config, "enable.auto.commit").as_deref(), Some("false"));
    }

    #[test]
    fn producer_config_applies_extra_properties_last() {
        let mut profile = profile_with(false, AuthMode::None);
        profile
            .extra_producer_config
            .insert("compression.type".to_string(), "zstd".to_string());
        let config = producer_client_config(&profile);
        assert_eq!(get(&config, "compression.type").as_deref(), Some("zstd"));
    }
}
