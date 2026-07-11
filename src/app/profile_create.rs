//! First-run / "n" create-profile form, and the profile-picker's edit-in-place flow.

use super::App;
use crate::config::{self, AuthMode, Profile};
use crate::events::Command;

/// Field focus for the first-run / "n" create-profile form. `CaPath`/`CertPath`/`KeyPath`
/// only appear in the focus cycle when `auth_choice` needs them — see
/// [`ProfileCreateState::active_fields`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileCreateFocus {
    Name,
    Bootstrap,
    Auth,
    CaPath,
    CertPath,
    KeyPath,
    SchemaRegistry,
}

/// The four `(AuthMode, tls_enabled)` combinations reachable from the wizard, cycled
/// with Space/t while the `Auth` field is focused. Distinct from `AuthMode` itself
/// because `AuthMode::None` covers two different UI states (plaintext vs. TLS against
/// the system trust store).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProfileCreateAuthChoice {
    #[default]
    Plaintext,
    /// TLS on, no client cert, verified against the system's default trust store.
    TlsSystemTrust,
    /// TLS on, verified against a private CA (`ca_path`), no client cert.
    TlsCustomCa,
    /// Mutual TLS: client cert + key, verified against `ca_path`.
    Mtls,
}

impl ProfileCreateAuthChoice {
    pub fn next(self) -> Self {
        match self {
            Self::Plaintext => Self::TlsSystemTrust,
            Self::TlsSystemTrust => Self::TlsCustomCa,
            Self::TlsCustomCa => Self::Mtls,
            Self::Mtls => Self::Plaintext,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Plaintext => "plaintext",
            Self::TlsSystemTrust => "tls (system trust store)",
            Self::TlsCustomCa => "tls (private CA)",
            Self::Mtls => "mtls (client cert)",
        }
    }

    fn needs_ca(self) -> bool {
        matches!(self, Self::TlsCustomCa | Self::Mtls)
    }

    fn needs_client_cert(self) -> bool {
        matches!(self, Self::Mtls)
    }
}

/// In-TUI wizard to create or edit a profile and save it to config.toml.
/// `message_max_bytes` / extra producer props stay TOML-only for advanced fields —
/// edit mode **preserves** those when saving form fields.
#[derive(Debug, Clone)]
pub struct ProfileCreateState {
    pub name: String,
    pub bootstrap_servers: String,
    pub auth_choice: ProfileCreateAuthChoice,
    pub ca_path: String,
    pub cert_path: String,
    pub key_path: String,
    pub schema_registry_url: String,
    pub focus: ProfileCreateFocus,
    /// Cursor as a **char** index within the focused text field (0..=len).
    pub cursor: usize,
    pub error: Option<String>,
    /// `None` = create (append). `Some(i)` = replace `config.profiles[i]`.
    pub edit_index: Option<usize>,
}

impl ProfileCreateState {
    pub fn new() -> Self {
        let name = "local".to_string();
        let cursor = name.chars().count();
        Self {
            name,
            bootstrap_servers: "localhost:9092".into(),
            auth_choice: ProfileCreateAuthChoice::default(),
            ca_path: String::new(),
            cert_path: String::new(),
            key_path: String::new(),
            schema_registry_url: String::new(),
            focus: ProfileCreateFocus::Name,
            cursor,
            error: None,
            edit_index: None,
        }
    }

    /// Prefill the form from an existing profile for in-place edit, including its
    /// auth mode and cert/key/CA paths (previously only reachable by hand-editing
    /// the TOML).
    pub fn from_profile(profile: &Profile, index: usize) -> Self {
        let name = profile.name.clone();
        let cursor = name.chars().count();
        let (auth_choice, ca_path, cert_path, key_path) = match &profile.auth {
            AuthMode::None if profile.tls_enabled => {
                (ProfileCreateAuthChoice::TlsSystemTrust, String::new(), String::new(), String::new())
            }
            AuthMode::None => {
                (ProfileCreateAuthChoice::Plaintext, String::new(), String::new(), String::new())
            }
            AuthMode::Tls { ca_path } => {
                (ProfileCreateAuthChoice::TlsCustomCa, ca_path.clone(), String::new(), String::new())
            }
            AuthMode::Mtls { cert_path, key_path, ca_path } => {
                (ProfileCreateAuthChoice::Mtls, ca_path.clone(), cert_path.clone(), key_path.clone())
            }
        };
        Self {
            name,
            bootstrap_servers: profile.bootstrap_servers.clone(),
            auth_choice,
            ca_path,
            cert_path,
            key_path,
            schema_registry_url: profile
                .schema_registry_url
                .clone()
                .unwrap_or_default(),
            focus: ProfileCreateFocus::Name,
            cursor,
            error: None,
            edit_index: Some(index),
        }
    }

    pub fn is_edit(&self) -> bool {
        self.edit_index.is_some()
    }

    /// Fields in cycle order, filtered to those `auth_choice` actually needs — mirrors
    /// `ProducerState::focus_cycle`'s mode-dependent field set.
    fn active_fields(&self) -> Vec<ProfileCreateFocus> {
        let mut fields = vec![
            ProfileCreateFocus::Name,
            ProfileCreateFocus::Bootstrap,
            ProfileCreateFocus::Auth,
        ];
        if self.auth_choice.needs_ca() {
            fields.push(ProfileCreateFocus::CaPath);
        }
        if self.auth_choice.needs_client_cert() {
            fields.push(ProfileCreateFocus::CertPath);
            fields.push(ProfileCreateFocus::KeyPath);
        }
        fields.push(ProfileCreateFocus::SchemaRegistry);
        fields
    }

    fn active_text(&self) -> &str {
        match self.focus {
            ProfileCreateFocus::Name => &self.name,
            ProfileCreateFocus::Bootstrap => &self.bootstrap_servers,
            ProfileCreateFocus::CaPath => &self.ca_path,
            ProfileCreateFocus::CertPath => &self.cert_path,
            ProfileCreateFocus::KeyPath => &self.key_path,
            ProfileCreateFocus::SchemaRegistry => &self.schema_registry_url,
            ProfileCreateFocus::Auth => "",
        }
    }

    fn active_text_mut(&mut self) -> Option<&mut String> {
        match self.focus {
            ProfileCreateFocus::Name => Some(&mut self.name),
            ProfileCreateFocus::Bootstrap => Some(&mut self.bootstrap_servers),
            ProfileCreateFocus::CaPath => Some(&mut self.ca_path),
            ProfileCreateFocus::CertPath => Some(&mut self.cert_path),
            ProfileCreateFocus::KeyPath => Some(&mut self.key_path),
            ProfileCreateFocus::SchemaRegistry => Some(&mut self.schema_registry_url),
            ProfileCreateFocus::Auth => None,
        }
    }

    fn snap_cursor_to_end(&mut self) {
        self.cursor = self.active_text().chars().count();
    }

    pub fn focus_next(&mut self) {
        let fields = self.active_fields();
        let current = fields.iter().position(|f| *f == self.focus).unwrap_or(0);
        self.focus = fields[(current + 1) % fields.len()];
        self.snap_cursor_to_end();
    }

    pub fn focus_prev(&mut self) {
        let fields = self.active_fields();
        let current = fields.iter().position(|f| *f == self.focus).unwrap_or(0);
        self.focus = fields[(current + fields.len() - 1) % fields.len()];
        self.snap_cursor_to_end();
    }

    /// Cycles `auth_choice` (Space/t while the `Auth` field is focused). `Auth` is
    /// unconditionally in `active_fields()`, so focus never goes stale here — unlike
    /// `CaPath`/`CertPath`/`KeyPath`, which can drop out of the cycle if a later choice
    /// no longer needs them; `submit`'s validation (not focus) is what guards those.
    pub fn cycle_auth(&mut self) {
        if self.focus == ProfileCreateFocus::Auth {
            self.auth_choice = self.auth_choice.next();
        }
    }

    pub fn insert_char(&mut self, c: char) {
        let mut cursor = self.cursor;
        {
            let Some(text) = self.active_text_mut() else {
                return;
            };
            crate::text_field::insert_char(text, &mut cursor, c);
        }
        self.cursor = cursor;
    }

    pub fn backspace(&mut self) {
        let mut cursor = self.cursor;
        {
            let Some(text) = self.active_text_mut() else {
                return;
            };
            crate::text_field::backspace(text, &mut cursor);
        }
        self.cursor = cursor;
    }

    pub fn delete_forward(&mut self) {
        let mut cursor = self.cursor;
        {
            let Some(text) = self.active_text_mut() else {
                return;
            };
            crate::text_field::delete_forward(text, &mut cursor);
        }
        self.cursor = cursor;
    }

    pub fn cursor_left(&mut self) {
        crate::text_field::cursor_left(&mut self.cursor);
    }

    pub fn cursor_right(&mut self) {
        let len = self.active_text().chars().count();
        if self.cursor < len {
            self.cursor += 1;
        }
    }

    pub fn cursor_home(&mut self) {
        crate::text_field::cursor_home(&mut self.cursor);
    }

    pub fn cursor_end(&mut self) {
        self.snap_cursor_to_end();
    }

    /// Display string with a block cursor inserted at `cursor` for the focused field.
    pub fn display_with_cursor(&self, field: ProfileCreateFocus) -> String {
        let text = match field {
            ProfileCreateFocus::Name => self.name.as_str(),
            ProfileCreateFocus::Bootstrap => self.bootstrap_servers.as_str(),
            ProfileCreateFocus::CaPath => self.ca_path.as_str(),
            ProfileCreateFocus::CertPath => self.cert_path.as_str(),
            ProfileCreateFocus::KeyPath => self.key_path.as_str(),
            ProfileCreateFocus::SchemaRegistry => self.schema_registry_url.as_str(),
            ProfileCreateFocus::Auth => {
                return format!("{}  (Space/t to cycle)", self.auth_choice.label());
            }
        };
        if field != self.focus {
            return text.to_string();
        }
        crate::text_field::display_with_cursor(text, self.cursor)
    }

    /// Builds a `Profile` after validation.
    pub fn to_profile(&self) -> Result<Profile, String> {
        let name = self.name.trim();
        if name.is_empty() {
            return Err("profile name is required".into());
        }
        let bootstrap = self.bootstrap_servers.trim();
        if bootstrap.is_empty() {
            return Err("bootstrap servers are required".into());
        }
        let schema_registry_url = {
            let s = self.schema_registry_url.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        };
        let (auth, tls_enabled) = match self.auth_choice {
            ProfileCreateAuthChoice::Plaintext => (AuthMode::None, false),
            ProfileCreateAuthChoice::TlsSystemTrust => (AuthMode::None, true),
            ProfileCreateAuthChoice::TlsCustomCa => {
                let ca_path = self.ca_path.trim();
                if ca_path.is_empty() {
                    return Err("CA path is required for TLS (private CA)".into());
                }
                (AuthMode::Tls { ca_path: ca_path.to_string() }, true)
            }
            ProfileCreateAuthChoice::Mtls => {
                let ca_path = self.ca_path.trim();
                let cert_path = self.cert_path.trim();
                let key_path = self.key_path.trim();
                if ca_path.is_empty() || cert_path.is_empty() || key_path.is_empty() {
                    return Err("cert, key, and CA paths are all required for mTLS".into());
                }
                (
                    AuthMode::Mtls {
                        cert_path: cert_path.to_string(),
                        key_path: key_path.to_string(),
                        ca_path: ca_path.to_string(),
                    },
                    true,
                )
            }
        };
        Ok(Profile {
            name: name.to_string(),
            bootstrap_servers: bootstrap.to_string(),
            tls_enabled,
            auth,
            schema_registry_url,
            message_max_bytes: None,
            extra_producer_config: std::collections::HashMap::new(),
        })
    }

    /// Apply form fields (including auth) onto `base`, keeping `message_max_bytes` /
    /// extra producer config — the only fields the wizard still doesn't edit.
    pub fn apply_to_profile(&self, base: &Profile) -> Result<Profile, String> {
        let mut profile = self.to_profile()?;
        profile.message_max_bytes = base.message_max_bytes;
        profile.extra_producer_config = base.extra_producer_config.clone();
        Ok(profile)
    }
}

impl Default for ProfileCreateState {
    fn default() -> Self {
        Self::new()
    }
}

impl App {
    pub(super) fn submit_profile_create(&mut self) -> Vec<Command> {
        let Some(state) = self.profile_create.as_ref() else {
            return vec![];
        };
        let edit_index = state.edit_index;

        let profile = if let Some(idx) = edit_index {
            let Some(existing) = self.config.profiles.get(idx) else {
                if let Some(s) = self.profile_create.as_mut() {
                    s.error = Some("profile no longer exists".into());
                }
                return vec![];
            };
            match state.apply_to_profile(existing) {
                Ok(p) => p,
                Err(err) => {
                    if let Some(s) = self.profile_create.as_mut() {
                        s.error = Some(err);
                    }
                    return vec![];
                }
            }
        } else {
            match state.to_profile() {
                Ok(p) => p,
                Err(err) => {
                    if let Some(s) = self.profile_create.as_mut() {
                        s.error = Some(err);
                    }
                    return vec![];
                }
            }
        };

        // Name must be unique among other profiles (ok to keep same name when editing).
        let name_taken = self.config.profiles.iter().enumerate().any(|(i, p)| {
            Some(i) != edit_index && p.name == profile.name
        });
        if name_taken {
            if let Some(s) = self.profile_create.as_mut() {
                s.error = Some(format!("profile '{}' already exists", profile.name));
            }
            return vec![];
        }

        let backup = self.config.profiles.clone();
        if let Some(idx) = edit_index {
            self.config.profiles[idx] = profile.clone();
            self.selected_profile_index = idx;
        } else {
            self.config.profiles.push(profile.clone());
            self.selected_profile_index = self.config.profiles.len() - 1;
        }

        if let Err(err) = config::save(&self.config_path, &self.config) {
            self.config.profiles = backup;
            if let Some(s) = self.profile_create.as_mut() {
                s.error = Some(format!("failed to save config: {err}"));
            }
            return vec![];
        }

        // If we edited the profile currently in use, refresh the in-memory copy so
        // later produce/browse commands pick up bootstrap/TLS/SR changes.
        if let Some(idx) = edit_index {
            if let Some(old) = backup.get(idx) {
                if self
                    .active_profile
                    .as_ref()
                    .is_some_and(|a| a.name == old.name)
                {
                    self.attach_profile(profile.clone());
                }
            }
        }

        self.profile_create = None;
        let verb = if edit_index.is_some() {
            "updated"
        } else {
            "saved"
        };
        self.status_message = Some(format!(
            "{verb} profile '{}' → {}",
            profile.name,
            self.config_path.display()
        ));
        vec![]
    }
}
