//! First-run / "n" create-profile form, and the profile-picker's edit-in-place flow.

use super::App;
use crate::config::{self, AuthMode, Profile};
use crate::events::Command;

/// Field focus for the first-run / "n" create-profile form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileCreateFocus {
    Name,
    Bootstrap,
    Tls,
    SchemaRegistry,
}

/// In-TUI wizard to create or edit a profile and save it to config.toml.
/// mTLS cert paths / `message_max_bytes` / extra producer props stay TOML-only for
/// advanced fields — edit mode **preserves** those when saving form fields.
#[derive(Debug, Clone)]
pub struct ProfileCreateState {
    pub name: String,
    pub bootstrap_servers: String,
    pub tls_enabled: bool,
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
            tls_enabled: false,
            schema_registry_url: String::new(),
            focus: ProfileCreateFocus::Name,
            cursor,
            error: None,
            edit_index: None,
        }
    }

    /// Prefill the form from an existing profile for in-place edit.
    pub fn from_profile(profile: &Profile, index: usize) -> Self {
        let name = profile.name.clone();
        let cursor = name.chars().count();
        Self {
            name,
            bootstrap_servers: profile.bootstrap_servers.clone(),
            tls_enabled: profile.tls_enabled,
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

    fn active_text(&self) -> &str {
        match self.focus {
            ProfileCreateFocus::Name => &self.name,
            ProfileCreateFocus::Bootstrap => &self.bootstrap_servers,
            ProfileCreateFocus::SchemaRegistry => &self.schema_registry_url,
            ProfileCreateFocus::Tls => "",
        }
    }

    fn active_text_mut(&mut self) -> Option<&mut String> {
        match self.focus {
            ProfileCreateFocus::Name => Some(&mut self.name),
            ProfileCreateFocus::Bootstrap => Some(&mut self.bootstrap_servers),
            ProfileCreateFocus::SchemaRegistry => Some(&mut self.schema_registry_url),
            ProfileCreateFocus::Tls => None,
        }
    }

    fn snap_cursor_to_end(&mut self) {
        self.cursor = self.active_text().chars().count();
    }

    pub fn focus_next(&mut self) {
        self.focus = match self.focus {
            ProfileCreateFocus::Name => ProfileCreateFocus::Bootstrap,
            ProfileCreateFocus::Bootstrap => ProfileCreateFocus::Tls,
            ProfileCreateFocus::Tls => ProfileCreateFocus::SchemaRegistry,
            ProfileCreateFocus::SchemaRegistry => ProfileCreateFocus::Name,
        };
        self.snap_cursor_to_end();
    }

    pub fn focus_prev(&mut self) {
        self.focus = match self.focus {
            ProfileCreateFocus::Name => ProfileCreateFocus::SchemaRegistry,
            ProfileCreateFocus::Bootstrap => ProfileCreateFocus::Name,
            ProfileCreateFocus::Tls => ProfileCreateFocus::Bootstrap,
            ProfileCreateFocus::SchemaRegistry => ProfileCreateFocus::Tls,
        };
        self.snap_cursor_to_end();
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
            ProfileCreateFocus::SchemaRegistry => self.schema_registry_url.as_str(),
            ProfileCreateFocus::Tls => {
                return if self.tls_enabled {
                    "yes  (Space/t to toggle)".into()
                } else {
                    "no   (Space/t to toggle)".into()
                };
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
        Ok(Profile {
            name: name.to_string(),
            bootstrap_servers: bootstrap.to_string(),
            tls_enabled: self.tls_enabled,
            auth: AuthMode::None,
            schema_registry_url,
            message_max_bytes: None,
            extra_producer_config: std::collections::HashMap::new(),
        })
    }

    /// Apply form fields onto `base`, keeping auth / message_max_bytes / extra producer
    /// config that the wizard does not edit.
    pub fn apply_to_profile(&self, base: &Profile) -> Result<Profile, String> {
        let mut profile = self.to_profile()?;
        profile.auth = base.auth.clone();
        profile.message_max_bytes = base.message_max_bytes;
        profile.extra_producer_config = base.extra_producer_config.clone();
        // If auth already requires TLS (tls/mtls), keep tls_enabled true even if the
        // form toggle was flipped off — avoids saving a contradictory profile.
        if !matches!(profile.auth, AuthMode::None) {
            profile.tls_enabled = true;
        }
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
