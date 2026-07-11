pub mod auth;
pub mod profile;

use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub use auth::AuthMode;
pub use profile::Profile;

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Config {
    #[serde(default)]
    pub profiles: Vec<Profile>,
}

impl Config {
    pub fn find_profile(&self, name: &str) -> Option<&Profile> {
        self.profiles.iter().find(|p| p.name == name)
    }
}

/// `~/.config/kaf-tui/`, constructed manually on both macOS and Linux rather than via a
/// crate's "native" platform config dir (which returns `~/Library/Application Support`
/// on macOS as of recent `dirs`/`directories` releases).
pub fn config_dir() -> AppResult<PathBuf> {
    let home = dirs::home_dir()
        .ok_or_else(|| AppError::Config("could not determine home directory".into()))?;
    Ok(home.join(".config").join("kaf-tui"))
}

pub fn config_file_path() -> AppResult<PathBuf> {
    Ok(config_dir()?.join("config.toml"))
}

pub fn load(path: &PathBuf) -> AppResult<Config> {
    if !path.exists() {
        return Ok(Config::default());
    }
    let contents = fs::read_to_string(path)?;
    let config: Config = toml::from_str(&contents)?;
    Ok(config)
}

pub fn save(path: &PathBuf, config: &Config) -> AppResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let contents = toml::to_string_pretty(config)?;
    fs::write(path, contents)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn sample_config() -> Config {
        Config {
            profiles: vec![
                Profile {
                    name: "local".into(),
                    bootstrap_servers: "localhost:9092".into(),
                    tls_enabled: false,
                    auth: AuthMode::None,
                    schema_registry_url: None,
                    message_max_bytes: None,
                    extra_producer_config: HashMap::new(),
                },
                Profile {
                    name: "prod".into(),
                    bootstrap_servers: "kafka.example.com:9093".into(),
                    tls_enabled: true,
                    auth: AuthMode::Mtls {
                        cert_path: "/certs/client.pem".into(),
                        key_path: "/certs/client.key".into(),
                        ca_path: "/certs/ca.pem".into(),
                    },
                    schema_registry_url: Some("https://schema.example.com".into()),
                    message_max_bytes: Some(20_000_000),
                    extra_producer_config: HashMap::from([(
                        "compression.type".to_string(),
                        "zstd".to_string(),
                    )]),
                },
            ],
        }
    }

    #[test]
    fn round_trips_through_toml() {
        let config = sample_config();
        let serialized = toml::to_string_pretty(&config).expect("serialize");
        let deserialized: Config = toml::from_str(&serialized).expect("deserialize");
        assert_eq!(config, deserialized);
    }

    #[test]
    fn tagged_auth_mode_uses_type_field() {
        let config = sample_config();
        let serialized = toml::to_string_pretty(&config).expect("serialize");
        assert!(serialized.contains("type = \"mtls\""));
        assert!(serialized.contains("type = \"none\""));
    }

    #[test]
    fn load_missing_file_returns_default() {
        let path = std::env::temp_dir().join("kaf-tui-test-nonexistent-config.toml");
        let _ = fs::remove_file(&path);
        let config = load(&path).expect("load should not fail on missing file");
        assert_eq!(config, Config::default());
    }

    #[test]
    fn save_then_load_round_trips() {
        let path = std::env::temp_dir().join(format!(
            "kaf-tui-test-config-{}.toml",
            std::process::id()
        ));
        let config = sample_config();
        save(&path, &config).expect("save");
        let loaded = load(&path).expect("load");
        assert_eq!(config, loaded);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn config_dir_uses_dot_config_on_this_platform() {
        let dir = config_dir().expect("home dir resolvable in test env");
        assert!(dir.ends_with(".config/kaf-tui"));
    }
}
