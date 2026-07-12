//! Producer screen: 3 input modes (inline / file-path / `$EDITOR`) and their fields.

use super::{App, Screen};
use crate::events::Command;

/// How the producer collects the message value body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerInputMode {
    Inline,
    FilePath,
    ExternalEditor,
}

impl ProducerInputMode {
    pub fn next(self) -> Self {
        match self {
            Self::Inline => Self::FilePath,
            Self::FilePath => Self::ExternalEditor,
            Self::ExternalEditor => Self::Inline,
        }
    }
}

/// Which producer field currently receives typed input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerFocus {
    Key,
    Value,
    FilePath,
}

pub struct ProducerState {
    pub topic: String,
    pub key_input: String,
    pub value_input: String,
    pub mode: ProducerInputMode,
    pub focus: ProducerFocus,
    pub file_path_input: String,
    /// Char index into the focused text field.
    pub cursor: usize,
}

impl ProducerState {
    pub fn new(topic: String) -> Self {
        Self {
            topic,
            key_input: String::new(),
            value_input: String::new(),
            mode: ProducerInputMode::Inline,
            focus: ProducerFocus::Key,
            file_path_input: String::new(),
            cursor: 0,
        }
    }

    /// Focus targets valid for the current input mode.
    fn focus_cycle(&self) -> &'static [ProducerFocus] {
        match self.mode {
            ProducerInputMode::Inline => &[ProducerFocus::Key, ProducerFocus::Value],
            ProducerInputMode::FilePath => &[ProducerFocus::Key, ProducerFocus::FilePath],
            // External editor only types into the key; value comes from $EDITOR.
            ProducerInputMode::ExternalEditor => &[ProducerFocus::Key],
        }
    }

    fn active_text(&self) -> &str {
        match self.focus {
            ProducerFocus::Key => self.key_input.as_str(),
            ProducerFocus::Value => self.value_input.as_str(),
            ProducerFocus::FilePath => self.file_path_input.as_str(),
        }
    }

    fn active_text_mut(&mut self) -> &mut String {
        match self.focus {
            ProducerFocus::Key => &mut self.key_input,
            ProducerFocus::Value => &mut self.value_input,
            ProducerFocus::FilePath => &mut self.file_path_input,
        }
    }

    fn snap_cursor_to_end(&mut self) {
        self.cursor = self.active_text().chars().count();
    }

    pub fn focus_next(&mut self) {
        let cycle = self.focus_cycle();
        let current = cycle.iter().position(|f| *f == self.focus).unwrap_or(0);
        self.focus = cycle[(current + 1) % cycle.len()];
        self.snap_cursor_to_end();
    }

    /// Mouse click on a field's box: focuses it directly. No-op if `field` isn't
    /// valid for the current mode (e.g. clicking a stale region right after a mode
    /// switch reflowed the layout).
    pub fn set_focus(&mut self, field: ProducerFocus) {
        if !self.focus_cycle().contains(&field) {
            return;
        }
        self.focus = field;
        self.snap_cursor_to_end();
    }

    /// After a mode change, snap focus onto a field that mode actually uses.
    pub fn normalize_focus(&mut self) {
        let cycle = self.focus_cycle();
        if !cycle.contains(&self.focus) {
            self.focus = cycle[0];
        }
        let mut cursor = self.cursor;
        crate::text_field::clamp_cursor(self.active_text(), &mut cursor);
        self.cursor = cursor;
    }

    pub fn insert_char(&mut self, c: char) {
        let mut cursor = self.cursor;
        {
            let text = self.active_text_mut();
            crate::text_field::insert_char(text, &mut cursor, c);
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

    pub fn display_field(&self, field: ProducerFocus) -> String {
        let text = match field {
            ProducerFocus::Key => self.key_input.as_str(),
            ProducerFocus::Value => self.value_input.as_str(),
            ProducerFocus::FilePath => self.file_path_input.as_str(),
        };
        if field == self.focus {
            crate::text_field::display_with_cursor(text, self.cursor)
        } else {
            text.to_string()
        }
    }
}

impl App {
    pub(super) fn open_producer(&mut self) -> Vec<Command> {
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
        self.producer = Some(ProducerState::new(topic));
        self.screen = Screen::Producer;
        self.status_message = None;
        // Pause tail while producing so background MessageArrived events don't fight the UI.
        vec![Command::StopTail]
    }

    pub(super) fn producer_insert_char(&mut self, c: char) {
        let Some(state) = self.producer.as_mut() else {
            return;
        };
        if state.focus == ProducerFocus::Value && state.mode != ProducerInputMode::Inline {
            return;
        }
        state.insert_char(c);
    }

    pub(super) fn producer_backspace(&mut self) {
        let Some(state) = self.producer.as_mut() else {
            return;
        };
        if state.focus == ProducerFocus::Value && state.mode != ProducerInputMode::Inline {
            return;
        }
        state.backspace();
    }

    pub(super) fn producer_submit(&mut self) -> Vec<Command> {
        if self.screen != Screen::Producer {
            return vec![];
        }
        let Some(profile) = self.active_profile.clone() else {
            return vec![];
        };
        let Some(state) = self.producer.as_ref() else {
            return vec![];
        };
        let topic = state.topic.clone();
        let key = if state.key_input.is_empty() {
            None
        } else {
            Some(state.key_input.as_bytes().to_vec())
        };
        // Empty body becomes a null payload (Kafka null), not an empty byte array — matches
        // the common "produce with just a key" case.
        let value = if state.value_input.is_empty() {
            None
        } else {
            Some(state.value_input.as_bytes().to_vec())
        };
        self.status_message = Some(format!("producing to {topic}..."));
        vec![Command::ProduceMessage {
            profile,
            topic,
            key,
            value,
            headers: vec![],
        }]
    }

    pub(super) fn producer_load_file(&mut self) -> Vec<Command> {
        if self.screen != Screen::Producer {
            return vec![];
        }
        let Some(state) = self.producer.as_ref() else {
            return vec![];
        };
        if state.mode != ProducerInputMode::FilePath {
            return vec![];
        }
        let path = state.file_path_input.trim().to_string();
        if path.is_empty() {
            self.status_message = Some("enter a file path".into());
            return vec![];
        }
        self.status_message = Some(format!("loading {path}..."));
        vec![Command::LoadFileIntoProducer { path }]
    }

    pub(super) fn producer_open_external_editor(&mut self) -> Vec<Command> {
        if self.screen != Screen::Producer {
            return vec![];
        }
        let Some(state) = self.producer.as_ref() else {
            return vec![];
        };
        if state.mode != ProducerInputMode::ExternalEditor {
            return vec![];
        }
        let initial = state.value_input.clone();
        self.status_message = Some("opening external editor...".into());
        vec![Command::RunExternalEditor { initial }]
    }
}
