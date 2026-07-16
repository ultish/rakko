pub mod auth;
pub mod profile;

use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

pub use auth::AuthMode;
pub use profile::Profile;

use crate::error::{AppError, AppResult};
use crate::ui::theme::ThemeName;

/// Top banner animation mode — stored under `[ui].banner_mode` and cycled with `A`.
///
/// Diagnostic modes share the same paint samples: **`Ms`** shows wall time per
/// draw; **`Fps`** shows the inverse capacity (`1000 / ms`) — useful when you
/// want the familiar high number rather than milliseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BannerMode {
    #[default]
    Wave,
    /// Paint cost in milliseconds per `terminal.draw`.
    Ms,
    /// Instantaneous paint capacity (`1000 / ms`), not screen refresh rate.
    Fps,
    Off,
}

impl BannerMode {
    pub fn next(self) -> Self {
        match self {
            BannerMode::Wave => BannerMode::Ms,
            BannerMode::Ms => BannerMode::Fps,
            BannerMode::Fps => BannerMode::Off,
            BannerMode::Off => BannerMode::Wave,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            BannerMode::Wave => "wave",
            BannerMode::Ms => "ms",
            BannerMode::Fps => "fps",
            BannerMode::Off => "off",
        }
    }
}

/// Appearance / TUI preferences under `[ui]` — separate from cluster profiles.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct UiConfig {
    /// Color theme (`dark` | `light`). Default `dark`.
    #[serde(default)]
    pub theme: ThemeName,
    /// Top banner animation (`wave` | `ms` | `fps` | `off`). Default `wave`.
    #[serde(default)]
    pub banner_mode: BannerMode,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Config {
    #[serde(default)]
    pub profiles: Vec<Profile>,
    /// How many messages a seek-mode page request loads at a time (n/p paging,
    /// the initial page on switching into seek mode, and mode-switch-to-latest).
    /// Not per-profile — this is a browsing preference, unrelated to any one
    /// cluster's connection details. `None` (the field is absent from the TOML)
    /// falls back to `crate::app::DEFAULT_SEEK_PAGE_SIZE`.
    #[serde(default)]
    pub seek_page_size: Option<usize>,
    /// TUI appearance (`theme`, `banner_mode`). Absent section → defaults.
    #[serde(default)]
    pub ui: UiConfig,
}

impl Config {
    pub fn find_profile(&self, name: &str) -> Option<&Profile> {
        self.profiles.iter().find(|p| p.name == name)
    }
}

/// `~/.config/rakko/`, constructed manually on both macOS and Linux rather than via a
/// crate's "native" platform config dir (which returns `~/Library/Application Support`
/// on macOS as of recent `dirs`/`directories` releases).
pub fn config_dir() -> AppResult<PathBuf> {
    let home = dirs::home_dir()
        .ok_or_else(|| AppError::Config("could not determine home directory".into()))?;
    Ok(home.join(".config").join("rakko"))
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
            seek_page_size: Some(25),
            ui: UiConfig {
                theme: ThemeName::Light,
                banner_mode: BannerMode::Fps,
            },
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
                Profile {
                    name: "staging".into(),
                    bootstrap_servers: "kafka-staging.example.com:9093".into(),
                    tls_enabled: true,
                    auth: AuthMode::Tls {
                        ca_path: "/certs/private-ca.pem".into(),
                    },
                    schema_registry_url: None,
                    message_max_bytes: None,
                    extra_producer_config: HashMap::new(),
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
        assert!(serialized.contains("type = \"tls\""));
        assert!(serialized.contains("type = \"none\""));
    }

    #[test]
    fn seek_page_size_is_none_when_absent_from_toml() {
        let config: Config = toml::from_str(
            r#"
            [[profiles]]
            name = "local"
            bootstrap_servers = "localhost:9092"
            tls_enabled = false

            [profiles.auth]
            type = "none"
            "#,
        )
        .expect("deserialize");
        assert_eq!(config.seek_page_size, None);
        assert_eq!(config.ui, UiConfig::default());
    }

    #[test]
    fn ui_section_round_trips() {
        let config: Config = toml::from_str(
            r#"
            [ui]
            theme = "light"
            banner_mode = "off"

            [[profiles]]
            name = "local"
            bootstrap_servers = "localhost:9092"
            tls_enabled = false

            [profiles.auth]
            type = "none"
            "#,
        )
        .expect("deserialize");
        assert_eq!(config.ui.theme, ThemeName::Light);
        assert_eq!(config.ui.banner_mode, BannerMode::Off);
        let serialized = toml::to_string_pretty(&config).expect("serialize");
        assert!(serialized.contains("theme = \"light\""));
        assert!(serialized.contains("banner_mode = \"off\""));
    }

    #[test]
    fn load_missing_file_returns_default() {
        let path = std::env::temp_dir().join("rakko-test-nonexistent-config.toml");
        let _ = fs::remove_file(&path);
        let config = load(&path).expect("load should not fail on missing file");
        assert_eq!(config, Config::default());
    }

    #[test]
    fn save_then_load_round_trips() {
        let path = std::env::temp_dir().join(format!(
            "rakko-test-config-{}.toml",
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
        assert!(dir.ends_with(".config/rakko"));
    }
}
