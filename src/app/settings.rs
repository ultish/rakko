//! In-TUI settings screen — `seek_page_size` and `[ui]` (`theme`, `banner_mode`), the
//! only editable `Config` fields (profiles are managed on the profile picker instead).

use super::App;
use crate::config::{self, BannerMode};
use crate::events::{Action, Command};
use crate::text_field;

/// One editable settings row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingId {
    SeekPageSize,
    UiTheme,
    UiBannerMode,
}

impl SettingId {
    pub const ALL: &'static [SettingId] =
        &[SettingId::SeekPageSize, SettingId::UiTheme, SettingId::UiBannerMode];

    pub fn key(self) -> &'static str {
        match self {
            SettingId::SeekPageSize => "seek_page_size",
            SettingId::UiTheme => "ui.theme",
            SettingId::UiBannerMode => "ui.banner_mode",
        }
    }

    pub fn kind(self) -> SettingKind {
        match self {
            SettingId::SeekPageSize => SettingKind::Int,
            SettingId::UiTheme | SettingId::UiBannerMode => SettingKind::Cycle,
        }
    }

    pub fn help(self) -> &'static str {
        match self {
            SettingId::SeekPageSize => {
                "Messages loaded per seek-mode page (n/p paging, initial page on open)"
            }
            SettingId::UiTheme => "Color theme (also T key)",
            SettingId::UiBannerMode => "Top banner mode (also A key)",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingKind {
    Int,
    Cycle,
}

/// Text overlay while editing `seek_page_size`.
#[derive(Debug, Clone)]
pub struct SettingsEditorState {
    pub id: SettingId,
    pub text: String,
    pub cursor: usize,
    pub error: Option<String>,
}

impl SettingsEditorState {
    pub fn new(id: SettingId, initial: String) -> Self {
        let cursor = initial.chars().count();
        Self { id, text: initial, cursor, error: None }
    }

    pub fn insert_char(&mut self, c: char) {
        text_field::insert_char(&mut self.text, &mut self.cursor, c);
        self.error = None;
    }
    pub fn insert_str(&mut self, s: &str) {
        text_field::insert_str(&mut self.text, &mut self.cursor, s);
        self.error = None;
    }
    pub fn backspace(&mut self) {
        text_field::backspace(&mut self.text, &mut self.cursor);
        self.error = None;
    }
    pub fn delete_forward(&mut self) {
        text_field::delete_forward(&mut self.text, &mut self.cursor);
        self.error = None;
    }
    pub fn cursor_left(&mut self) {
        text_field::cursor_left(&mut self.cursor);
    }
    pub fn cursor_right(&mut self) {
        text_field::cursor_right(&self.text, &mut self.cursor);
    }
    pub fn cursor_home(&mut self) {
        text_field::cursor_home(&mut self.cursor);
    }
    pub fn cursor_end(&mut self) {
        text_field::cursor_end(&self.text, &mut self.cursor);
    }
    pub fn display_with_cursor(&self) -> String {
        text_field::display_with_cursor(&self.text, self.cursor)
    }
}

impl App {
    pub fn setting_display_value(&self, id: SettingId) -> String {
        match id {
            SettingId::SeekPageSize => self.seek_page_size().to_string(),
            SettingId::UiTheme => self.theme.name.label().to_string(),
            SettingId::UiBannerMode => match self.banner_mode {
                BannerMode::Wave => "wave".into(),
                BannerMode::Ms => "ms".into(),
                BannerMode::Fps => "fps".into(),
                BannerMode::Off => "off".into(),
            },
        }
    }

    /// Focused setting: cycles theme/banner mode in place (reusing the same
    /// `CycleTheme`/`CycleBannerMode` actions bound to `T`/`A`), or opens the text
    /// editor for `seek_page_size`.
    pub(super) fn activate_setting(&mut self) -> Vec<Command> {
        let Some(&id) = SettingId::ALL.get(self.selected_setting) else {
            return vec![];
        };
        match id {
            SettingId::SeekPageSize => {
                self.settings_editor =
                    Some(SettingsEditorState::new(id, self.setting_display_value(id)));
                vec![]
            }
            SettingId::UiTheme => self.update(Action::CycleTheme),
            SettingId::UiBannerMode => self.update(Action::CycleBannerMode),
        }
    }

    pub(super) fn nudge_setting_int(&mut self, delta: i32) -> Vec<Command> {
        let Some(&id) = SettingId::ALL.get(self.selected_setting) else {
            return vec![];
        };
        if id.kind() != SettingKind::Int {
            return vec![];
        }
        let v = (self.seek_page_size() as i32 + delta * 10).clamp(1, 10_000) as usize;
        self.config.seek_page_size = Some(v);
        self.persist_settings(&format!("{} = {v}", id.key()))
    }

    pub(super) fn cancel_settings_editor(&mut self) {
        self.settings_editor = None;
    }

    pub(super) fn save_settings_editor(&mut self) -> Vec<Command> {
        let Some(ed) = self.settings_editor.as_ref() else {
            return vec![];
        };
        let raw = ed.text.trim();
        let v: usize = match raw.parse() {
            Ok(v) if v > 0 => v,
            _ => {
                if let Some(ed) = self.settings_editor.as_mut() {
                    ed.error = Some("seek_page_size must be a number ≥ 1".to_string());
                }
                return vec![];
            }
        };
        self.config.seek_page_size = Some(v);
        self.settings_editor = None;
        self.persist_settings(&format!("seek_page_size = {v}"))
    }

    fn persist_settings(&mut self, status: &str) -> Vec<Command> {
        if let Err(err) = config::save(&self.config_path, &self.config) {
            self.set_ephemeral_status(format!("failed to save config: {err}"));
            return vec![];
        }
        self.set_ephemeral_status(status.to_string());
        vec![]
    }
}
