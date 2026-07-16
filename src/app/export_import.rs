//! Export/import screen: JSONL export of selected/visible messages, and JSONL import
//! onto a (possibly different) target topic.

use super::{App, Screen};
use crate::events::Command;
use crate::raw_message::RawMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportImportMode {
    Export,
    Import,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportImportFocus {
    Path,
    TargetTopic,
}

pub struct ExportImportState {
    pub mode: ExportImportMode,
    pub path_input: String,
    pub target_topic: String,
    pub focus: ExportImportFocus,
    /// Char index into the focused text field (0..=len).
    pub cursor: usize,
    /// Snapshot of messages to export (Export mode only).
    pub messages: Vec<RawMessage>,
}

impl ExportImportState {
    fn active_text(&self) -> &str {
        match self.focus {
            ExportImportFocus::Path => self.path_input.as_str(),
            ExportImportFocus::TargetTopic => self.target_topic.as_str(),
        }
    }

    fn active_text_mut(&mut self) -> &mut String {
        match self.focus {
            ExportImportFocus::Path => &mut self.path_input,
            ExportImportFocus::TargetTopic => &mut self.target_topic,
        }
    }

    fn snap_cursor_to_end(&mut self) {
        self.cursor = self.active_text().chars().count();
    }

    pub fn set_focus(&mut self, focus: ExportImportFocus) {
        self.focus = focus;
        self.snap_cursor_to_end();
    }

    pub fn insert_char(&mut self, c: char) {
        let mut cursor = self.cursor;
        {
            let text = self.active_text_mut();
            crate::text_field::insert_char(text, &mut cursor, c);
        }
        self.cursor = cursor;
    }

    pub fn insert_str(&mut self, s: &str) {
        let mut cursor = self.cursor;
        {
            let text = self.active_text_mut();
            crate::text_field::insert_str(text, &mut cursor, s);
        }
        self.cursor = cursor;
    }

    pub fn backspace(&mut self) {
        let mut cursor = self.cursor;
        {
            let text = self.active_text_mut();
            crate::text_field::backspace(text, &mut cursor);
        }
        self.cursor = cursor;
    }

    pub fn delete_forward(&mut self) {
        let mut cursor = self.cursor;
        {
            let text = self.active_text_mut();
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

    /// Focused field with a block cursor; other fields plain.
    pub fn display_with_cursor(&self, field: ExportImportFocus) -> String {
        let text = match field {
            ExportImportFocus::Path => self.path_input.as_str(),
            ExportImportFocus::TargetTopic => self.target_topic.as_str(),
        };
        if field != self.focus {
            return text.to_string();
        }
        crate::text_field::display_with_cursor(text, self.cursor)
    }
}

/// Which messages to snapshot when opening the export screen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExportScope {
    /// Highlighted list row, or the message open in the inspector.
    Selected,
    /// All rows currently visible under the active filter.
    AllVisible,
}

impl App {
    pub(super) fn open_export(&mut self, scope: ExportScope) -> Vec<Command> {
        if self.screen != Screen::TopicDetail {
            return vec![];
        }
        let Some(detail) = self.topic_detail.as_ref() else {
            return vec![];
        };
        if detail.replay_phase.is_some() {
            return vec![];
        }

        let messages: Vec<RawMessage> = match scope {
            ExportScope::Selected => {
                // Prefer the open inspector snapshot so export matches what you're viewing.
                if let Some(view) = &detail.message_view {
                    vec![view.message.clone()]
                } else {
                    detail
                        .visible_messages_with_registry(self.schema_registry.as_ref())
                        .get(detail.selected_index)
                        .map(|m| vec![(*m).clone()])
                        .unwrap_or_default()
                }
            }
            ExportScope::AllVisible => detail
                .visible_messages_with_registry(self.schema_registry.as_ref())
                .into_iter()
                .cloned()
                .collect(),
        };

        if messages.is_empty() {
            self.status_message = Some(match scope {
                ExportScope::Selected => "no message selected to export".into(),
                ExportScope::AllVisible => "no messages to export".into(),
            });
            return vec![];
        }

        let topic = detail.topic.clone();
        let path_input = if messages.len() == 1 {
            let m = &messages[0];
            format!("{topic}-p{}-o{}.jsonl", m.partition, m.offset)
        } else {
            format!("{topic}.jsonl")
        };

        if let Some(detail) = self.topic_detail.as_mut() {
            detail.message_view = None;
        }
        let cursor = path_input.chars().count();
        self.export_import = Some(ExportImportState {
            mode: ExportImportMode::Export,
            path_input,
            target_topic: topic,
            focus: ExportImportFocus::Path,
            cursor,
            messages,
        });
        self.screen = Screen::ExportImport;
        self.status_message = None;
        vec![Command::StopTail]
    }

    pub(super) fn open_import(&mut self) -> Vec<Command> {
        if self.screen != Screen::TopicDetail {
            return vec![];
        }
        let Some(detail) = self.topic_detail.as_mut() else {
            return vec![];
        };
        if detail.replay_phase.is_some() {
            return vec![];
        }
        detail.message_view = None;
        let topic = detail.topic.clone();
        self.export_import = Some(ExportImportState {
            mode: ExportImportMode::Import,
            path_input: String::new(),
            target_topic: topic,
            focus: ExportImportFocus::Path,
            cursor: 0,
            messages: Vec::new(),
        });
        self.screen = Screen::ExportImport;
        self.status_message = None;
        vec![Command::StopTail]
    }

    pub(super) fn export_import_submit(&mut self) -> Vec<Command> {
        let Some(state) = self.export_import.as_ref() else {
            return vec![];
        };
        let raw_path = state.path_input.trim().to_string();
        if raw_path.is_empty() {
            self.status_message = Some("enter a file path".into());
            return vec![];
        }
        // Expand `~/…` so shell-style paths work (File::create does not expand ~).
        let path = match crate::export::expand_user_path(&raw_path) {
            Ok(p) => p.to_string_lossy().into_owned(),
            Err(err) => {
                self.status_message = Some(format!("bad path: {err}"));
                return vec![];
            }
        };
        match state.mode {
            ExportImportMode::Export => {
                let messages = state.messages.clone();
                self.status_message = Some(format!(
                    "exporting {} message(s) to {path}...",
                    messages.len()
                ));
                vec![Command::ExportMessages { path, messages }]
            }
            ExportImportMode::Import => {
                let Some(profile) = self.active_profile.clone() else {
                    return vec![];
                };
                let target_topic = state.target_topic.trim().to_string();
                if target_topic.is_empty() {
                    self.status_message = Some("enter a target topic".into());
                    return vec![];
                }
                self.status_message = Some(format!("importing from {path} into {target_topic}..."));
                vec![Command::ImportMessages {
                    profile,
                    path,
                    target_topic,
                }]
            }
        }
    }
}
