use std::collections::HashSet;

use std::path::PathBuf;

use crate::config::{self, AuthMode, Config, Profile};
use crate::events::{Action, AppEvent, Command, SeekPageRequest};
use crate::kafka::admin::TopicSummary;
use crate::kafka::group_offsets::{
    GroupMember, GroupSummary, OffsetResetTarget, PartitionLag,
};
use crate::kafka::schema_registry::SchemaRegistry;
use crate::raw_message::RawMessage;
use crate::ring_buffer::RingBuffer;
use crate::serde_detect::{detect_format, DetectedFormat};

const TAIL_BUFFER_CAPACITY: usize = 500;
const SEEK_PAGE_SIZE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    ProfilePicker,
    TopicList,
    TopicDetail,
    GroupList,
    GroupDetail,
    Producer,
    ExportImport,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetInputKind {
    AbsoluteOffset,
    TimestampMillis,
}

/// Multi-step offset-reset wizard on the group-detail screen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetResetPhase {
    ChooseMode,
    Input {
        target_kind: ResetInputKind,
        input: String,
        cursor: usize,
    },
    Confirm {
        target: OffsetResetTarget,
    },
}

pub struct GroupDetailState {
    pub name: String,
    pub state: String,
    pub members: Vec<GroupMember>,
    pub lags: Vec<PartitionLag>,
    pub selected_index: usize,
    pub has_active_members: bool,
    pub total_lag: i64,
    pub reset_phase: Option<OffsetResetPhase>,
}

/// Tail and seek are mutually exclusive browsing modes over the same topic: switching one
/// way drops the other's state entirely (tail's ring buffer, seek's current page) rather
/// than trying to reconcile "always at the end" semantics with "pinned at an exact offset"
/// semantics on shared state. See PLAN.md's "Ring buffer + pagination coexistence".
pub enum BrowseMode {
    Tail(RingBuffer<RawMessage>),
    Seek(SeekState),
}

pub struct SeekState {
    pub partition: i32,
    pub messages: Vec<RawMessage>,
    /// Offset of the first message in `messages` (or the last-requested start offset if
    /// the page came back empty) — the anchor for computing the next Forward/Backward
    /// page request.
    pub page_start_offset: i64,
    pub at_beginning: bool,
    pub at_end: bool,
    /// Earliest available offset on the partition when this page was loaded.
    pub low_watermark: i64,
    /// Next offset to be written on the partition when this page was loaded.
    pub high_watermark: i64,
}

/// Display order for the message list (both tail buffer and seek page).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MessageSort {
    /// Highest offset first — default so live/tail browsing shows the newest on top.
    #[default]
    NewestFirst,
    /// Natural Kafka offset order (ascending).
    OldestFirst,
}

impl MessageSort {
    pub fn toggle(self) -> Self {
        match self {
            MessageSort::NewestFirst => MessageSort::OldestFirst,
            MessageSort::OldestFirst => MessageSort::NewestFirst,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            MessageSort::NewestFirst => "newest↑",
            MessageSort::OldestFirst => "oldest↑",
        }
    }
}

/// Single-message replay wizard: confirm raw same-topic resend, or open producer to edit.
#[derive(Debug, Clone)]
pub enum ReplayPhase {
    /// Confirm same-topic raw replay (byte-identical; no decode).
    Confirm {
        message: RawMessage,
    },
}

/// Full-message inspector opened with Enter on the message list.
/// Snapshots the record so a live tail refresh doesn't swap content under the cursor.
#[derive(Debug, Clone)]
pub struct MessageViewState {
    pub message: RawMessage,
    /// Vertical line offset into the rendered body.
    pub scroll: usize,
}

pub struct TopicDetailState {
    pub topic: String,
    pub partition_count: usize,
    pub mode: BrowseMode,
    pub selected_index: usize,
    pub filter_input: String,
    /// Cursor into `filter_input` while `filter_active`.
    pub filter_cursor: usize,
    pub filter_active: bool,
    pub applied_filter: Option<String>,
    pub replay_phase: Option<ReplayPhase>,
    pub message_view: Option<MessageViewState>,
    /// Newest-first by default so the latest messages appear at the top of the list.
    pub sort: MessageSort,
}

impl TopicDetailState {
    /// Messages currently on screen (no registry-aware decoding). Used by unit tests.
    #[cfg(test)]
    pub fn visible_messages(&self) -> Vec<&RawMessage> {
        self.visible_messages_with_registry(None)
    }

    /// Visible messages for the list / export / replay selection, decoding Avro via the
    /// schema-registry cache when filtering so field-level search works after schemas load.
    pub fn visible_messages_with_registry(
        &self,
        registry: Option<&SchemaRegistry>,
    ) -> Vec<&RawMessage> {
        let all: Box<dyn Iterator<Item = &RawMessage> + '_> = match &self.mode {
            BrowseMode::Tail(buffer) => Box::new(buffer.iter()),
            BrowseMode::Seek(state) => Box::new(state.messages.iter()),
        };
        let mut msgs: Vec<&RawMessage> = match &self.applied_filter {
            None => all.collect(),
            Some(filter) => {
                let needle = filter.to_lowercase();
                all.filter(|message| message_matches_filter(message, &needle, registry))
                    .collect()
            }
        };
        // Storage order is oldest→newest (ring buffer / seek poll). Reverse for newest↑.
        if self.sort == MessageSort::NewestFirst {
            msgs.reverse();
        }
        msgs
    }
}

/// Case-insensitive substring match against key, raw value, and the serde_detect
/// decoded view (JSON/Avro text/hex). Never mutates the raw message.
fn message_matches_filter(
    message: &RawMessage,
    needle_lowercase: &str,
    registry: Option<&SchemaRegistry>,
) -> bool {
    // Key: raw UTF-8 view and decoded view (Avro/JSON/text/hex) — formats can differ from value.
    if let Some(key) = message.key.as_deref() {
        if String::from_utf8_lossy(key)
            .to_lowercase()
            .contains(needle_lowercase)
        {
            return true;
        }
        if crate::serde_detect::decode_value(key, registry)
            .as_str()
            .to_lowercase()
            .contains(needle_lowercase)
        {
            return true;
        }
    }
    let Some(value) = message.value.as_deref() else {
        return false;
    };
    if String::from_utf8_lossy(value)
        .to_lowercase()
        .contains(needle_lowercase)
    {
        return true;
    }
    // Decoded value view (JSON / Avro-as-JSON when schema is cached / text / hex).
    crate::serde_detect::decode_value(value, registry)
        .as_str()
        .to_lowercase()
        .contains(needle_lowercase)
}

enum PageDirection {
    Forward,
    Backward,
}

/// Which messages to snapshot when opening the export screen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExportScope {
    /// Highlighted list row, or the message open in the inspector.
    Selected,
    /// All rows currently visible under the active filter.
    AllVisible,
}

pub struct App {
    pub screen: Screen,
    pub config: Config,
    /// Selection cursor for the profile picker.
    pub selected_profile_index: usize,
    pub topics: Vec<TopicSummary>,
    /// Selection cursor for the topic list.
    pub topic_list_selected_index: usize,
    pub topic_detail: Option<TopicDetailState>,
    pub groups: Vec<GroupSummary>,
    pub group_list_selected_index: usize,
    pub group_detail: Option<GroupDetailState>,
    pub producer: Option<ProducerState>,
    pub export_import: Option<ExportImportState>,
    /// Schema Registry client for the active profile (if `schema_registry_url` is set).
    /// Cache is filled by background `FetchSchema` commands as Avro messages are seen.
    pub schema_registry: Option<SchemaRegistry>,
    /// Schema ids with an in-flight HTTP fetch (dedupe concurrent MessageArrived).
    schema_fetch_inflight: HashSet<u32>,
    /// Schema ids that failed to fetch; not retried until the profile reconnects.
    schema_fetch_failed: HashSet<u32>,
    /// Path to `config.toml` (used when creating/saving profiles from the TUI).
    pub config_path: PathBuf,
    /// First-run / "n" create-profile form (overlays the profile picker).
    pub profile_create: Option<ProfileCreateState>,
    /// Transient status text: connect errors, load errors, "loading..." etc.
    pub status_message: Option<String>,
    pub should_quit: bool,
    /// When true, a centered "quit?" dialog is open (`q`); y/Enter exits, n/Esc cancels.
    pub quit_confirm: bool,
    pub active_profile: Option<Profile>,
    /// Braille stream banner animation frame index.
    pub banner_frame: usize,
    /// When false, stream glyph is static (`A` toggles).
    pub banner_animation: bool,
    /// Full-screen detailed otter splash — shown once at startup until dismissed.
    pub show_splash: bool,
}

impl App {
    pub fn new(config: Config, config_path: PathBuf) -> Self {
        let empty = config.profiles.is_empty();
        Self {
            screen: Screen::ProfilePicker,
            config,
            selected_profile_index: 0,
            topics: Vec::new(),
            topic_list_selected_index: 0,
            topic_detail: None,
            groups: Vec::new(),
            group_list_selected_index: 0,
            group_detail: None,
            producer: None,
            export_import: None,
            schema_registry: None,
            schema_fetch_inflight: HashSet::new(),
            schema_fetch_failed: HashSet::new(),
            config_path,
            // Auto-open create form when there are no profiles yet (after splash).
            profile_create: if empty {
                Some(ProfileCreateState::new())
            } else {
                None
            },
            status_message: None,
            should_quit: false,
            quit_confirm: false,
            active_profile: None,
            banner_frame: 0,
            banner_animation: true,
            show_splash: true,
        }
    }

    /// Wires up `--profile <name>`: if the name matches a configured profile, starts
    /// directly on `TopicList` with it active. Construction stays synchronous (no I/O
    /// here) — the caller in main.rs is responsible for spawning the actual topic-load
    /// task when it sees `screen == TopicList` right after this returns.
    pub fn new_with_profile(
        config: Config,
        config_path: PathBuf,
        profile_name: Option<&str>,
    ) -> Self {
        let mut app = Self::new(config, config_path);
        if let Some(name) = profile_name {
            if let Some(profile) = app.config.find_profile(name).cloned() {
                app.attach_profile(profile);
                app.screen = Screen::TopicList;
                app.profile_create = None;
            }
        }
        app
    }

    /// Activates a profile and (re)builds the Schema Registry client from its URL.
    fn attach_profile(&mut self, profile: Profile) {
        self.schema_fetch_inflight.clear();
        self.schema_fetch_failed.clear();
        self.status_message = None;
        self.schema_registry = match profile.schema_registry_url.as_deref() {
            Some(url) => match SchemaRegistry::new(url) {
                Ok(sr) => {
                    tracing::info!("schema registry attached: {url}");
                    Some(sr)
                }
                Err(err) => {
                    tracing::warn!("schema registry init failed for {url}: {err}");
                    self.status_message =
                        Some(format!("schema registry unavailable: {err}"));
                    None
                }
            },
            None => None,
        };
        self.active_profile = Some(profile);
    }

    /// Persist broker-detected `message.max.bytes` onto a profile that still has
    /// `message_max_bytes = None`. Returns a short status string for the UI.
    ///
    /// Profiles that already set a limit are left alone (explicit config wins).
    fn apply_auto_message_max_bytes(
        &mut self,
        profile_name: &str,
        bytes: u32,
    ) -> Option<String> {
        let profile = self
            .config
            .profiles
            .iter_mut()
            .find(|p| p.name == profile_name)?;
        if profile.message_max_bytes.is_some() {
            return None;
        }
        profile.message_max_bytes = Some(bytes);
        if let Err(err) = config::save(&self.config_path, &self.config) {
            // Roll back so we never claim a save that failed.
            if let Some(p) = self
                .config
                .profiles
                .iter_mut()
                .find(|p| p.name == profile_name)
            {
                p.message_max_bytes = None;
            }
            return Some(format!(
                "detected message.max.bytes={bytes} but failed to save config: {err}"
            ));
        }
        if let Some(active) = self.active_profile.as_mut() {
            if active.name == profile_name {
                active.message_max_bytes = Some(bytes);
            }
        }
        Some(format!(
            "detected message.max.bytes={bytes} from broker; saved to profile '{profile_name}'"
        ))
    }

    /// If key/value bytes look like Confluent Avro and the schema is not cached, queue a fetch.
    fn queue_schema_fetch_for_bytes(&mut self, bytes: Option<&[u8]>) -> Option<Command> {
        let bytes = bytes?;
        let DetectedFormat::Avro { schema_id } = detect_format(bytes) else {
            return None;
        };
        self.queue_schema_fetch(schema_id)
    }

    fn queue_schema_fetch(&mut self, schema_id: u32) -> Option<Command> {
        self.schema_registry.as_ref()?;
        if self
            .schema_registry
            .as_ref()
            .is_some_and(|sr| sr.cached_schema(schema_id).is_some())
        {
            return None;
        }
        if self.schema_fetch_inflight.contains(&schema_id)
            || self.schema_fetch_failed.contains(&schema_id)
        {
            return None;
        }
        let registry_url = self
            .active_profile
            .as_ref()?
            .schema_registry_url
            .clone()?;
        self.schema_fetch_inflight.insert(schema_id);
        Some(Command::FetchSchema {
            registry_url,
            schema_id,
        })
    }

    /// Elm-style reducer. Stays synchronous and non-blocking; any background I/O the
    /// action implies is communicated back via the returned `Command`s rather than
    /// performed here.
    pub fn update(&mut self, action: Action) -> Vec<Command> {
        match action {
            Action::Quit => {
                // `q` always goes through the confirm dialog (even over other overlays).
                self.quit_confirm = true;
                vec![]
            }
            Action::ForceQuit => {
                self.should_quit = true;
                vec![]
            }
            Action::ConfirmQuit => {
                self.quit_confirm = false;
                self.should_quit = true;
                vec![]
            }
            Action::CancelQuit => {
                self.quit_confirm = false;
                vec![]
            }
            Action::MoveSelectionUp => {
                self.move_selection(-1);
                vec![]
            }
            Action::MoveSelectionDown => {
                self.move_selection(1);
                vec![]
            }
            Action::Confirm => self.confirm(),
            Action::Back => self.back(),
            Action::ToggleBrowseMode => {
                if self.topic_detail.as_ref().is_some_and(|d| d.message_view.is_some()) {
                    return vec![];
                }
                self.toggle_browse_mode()
            }
            Action::ToggleMessageSort => {
                self.toggle_message_sort();
                vec![]
            }
            Action::PageForward => {
                if self.scroll_message_view(10) {
                    return vec![];
                }
                self.request_seek_page(PageDirection::Forward)
            }
            Action::PageBackward => {
                if self.scroll_message_view(-10) {
                    return vec![];
                }
                self.request_seek_page(PageDirection::Backward)
            }
            Action::StartFilterInput => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    if detail.message_view.is_some() {
                        return vec![];
                    }
                    detail.filter_input = detail.applied_filter.clone().unwrap_or_default();
                    detail.filter_cursor = detail.filter_input.chars().count();
                    detail.filter_active = true;
                }
                vec![]
            }
            Action::FilterChar(c) => {
                self.text_insert(c);
                vec![]
            }
            Action::FilterBackspace => {
                self.text_backspace();
                vec![]
            }
            Action::FilterDelete => {
                self.text_delete();
                vec![]
            }
            Action::FilterCursorLeft => {
                self.text_cursor_left();
                vec![]
            }
            Action::FilterCursorRight => {
                self.text_cursor_right();
                vec![]
            }
            Action::FilterCursorHome => {
                self.text_cursor_home();
                vec![]
            }
            Action::FilterCursorEnd => {
                self.text_cursor_end();
                vec![]
            }
            Action::ApplyFilter => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    if detail.filter_active {
                        detail.applied_filter = if detail.filter_input.is_empty() {
                            None
                        } else {
                            Some(detail.filter_input.clone())
                        };
                        detail.filter_active = false;
                        detail.selected_index = 0;
                    }
                }
                vec![]
            }
            Action::CancelFilterInput => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    detail.filter_active = false;
                    detail.filter_input.clear();
                }
                vec![]
            }
            Action::ClearFilter => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    detail.applied_filter = None;
                    detail.filter_input.clear();
                    detail.selected_index = 0;
                }
                vec![]
            }
            Action::OpenGroups => self.open_groups(),
            Action::StartOffsetReset => self.start_offset_reset(),
            Action::OffsetResetChooseEarliest => {
                self.choose_offset_reset_target(OffsetResetTarget::Earliest)
            }
            Action::OffsetResetChooseLatest => {
                self.choose_offset_reset_target(OffsetResetTarget::Latest)
            }
            Action::OffsetResetChooseAbsolute => self.begin_offset_reset_input(ResetInputKind::AbsoluteOffset),
            Action::OffsetResetChooseTimestamp => {
                self.begin_offset_reset_input(ResetInputKind::TimestampMillis)
            }
            Action::ConfirmOffsetReset => self.confirm_offset_reset(),
            Action::CancelOffsetReset => {
                if let Some(detail) = self.group_detail.as_mut() {
                    detail.reset_phase = None;
                }
                vec![]
            }
            Action::OpenProducer => self.open_producer(),
            Action::ProducerToggleMode => {
                if let Some(state) = self.producer.as_mut() {
                    state.mode = state.mode.next();
                    state.normalize_focus();
                }
                vec![]
            }
            Action::ProducerFocusNext => {
                if let Some(state) = self.producer.as_mut() {
                    state.focus_next();
                }
                vec![]
            }
            Action::ProducerChar(c) => {
                self.producer_insert_char(c);
                vec![]
            }
            Action::ProducerBackspace => {
                self.producer_backspace();
                vec![]
            }
            Action::ProducerNewline => {
                if let Some(state) = self.producer.as_mut() {
                    // Multi-line key/value editing (inline); key also accepts newlines in
                    // file-path / external-editor modes when the key field is focused.
                    let allow = matches!(
                        (state.mode, state.focus),
                        (ProducerInputMode::Inline, ProducerFocus::Key | ProducerFocus::Value)
                            | (_, ProducerFocus::Key)
                    );
                    if allow {
                        state.insert_char('\n');
                    }
                }
                vec![]
            }
            Action::ProducerDelete => {
                if let Some(state) = self.producer.as_mut() {
                    if state.focus == ProducerFocus::Value
                        && state.mode != ProducerInputMode::Inline
                    {
                        return vec![];
                    }
                    state.delete_forward();
                }
                vec![]
            }
            Action::ProducerCursorLeft => {
                if let Some(state) = self.producer.as_mut() {
                    state.cursor_left();
                }
                vec![]
            }
            Action::ProducerCursorRight => {
                if let Some(state) = self.producer.as_mut() {
                    state.cursor_right();
                }
                vec![]
            }
            Action::ProducerCursorHome => {
                if let Some(state) = self.producer.as_mut() {
                    state.cursor_home();
                }
                vec![]
            }
            Action::ProducerCursorEnd => {
                if let Some(state) = self.producer.as_mut() {
                    state.cursor_end();
                }
                vec![]
            }
            Action::ProducerSubmit => self.producer_submit(),
            Action::ProducerLoadFile => self.producer_load_file(),
            Action::ProducerOpenExternalEditor => self.producer_open_external_editor(),
            Action::RequestReplay => self.request_replay(),
            Action::ConfirmReplay => self.confirm_replay(),
            Action::ReplayEdit => self.open_replay_edit(),
            Action::CancelReplay => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    detail.replay_phase = None;
                }
                vec![]
            }
            Action::OpenExport => self.open_export(ExportScope::Selected),
            Action::OpenExportAll => self.open_export(ExportScope::AllVisible),
            Action::OpenImport => self.open_import(),
            Action::ExportImportChar(c) => {
                if let Some(state) = self.export_import.as_mut() {
                    state.insert_char(c);
                }
                vec![]
            }
            Action::ExportImportBackspace => {
                if let Some(state) = self.export_import.as_mut() {
                    state.backspace();
                }
                vec![]
            }
            Action::ExportImportDelete => {
                if let Some(state) = self.export_import.as_mut() {
                    state.delete_forward();
                }
                vec![]
            }
            Action::ExportImportCursorLeft => {
                if let Some(state) = self.export_import.as_mut() {
                    state.cursor_left();
                }
                vec![]
            }
            Action::ExportImportCursorRight => {
                if let Some(state) = self.export_import.as_mut() {
                    state.cursor_right();
                }
                vec![]
            }
            Action::ExportImportCursorHome => {
                if let Some(state) = self.export_import.as_mut() {
                    state.cursor_home();
                }
                vec![]
            }
            Action::ExportImportCursorEnd => {
                if let Some(state) = self.export_import.as_mut() {
                    state.cursor_end();
                }
                vec![]
            }
            Action::ExportImportSubmit => self.export_import_submit(),
            Action::ExportImportFocusNext => {
                if let Some(state) = self.export_import.as_mut() {
                    if state.mode == ExportImportMode::Import {
                        let next = match state.focus {
                            ExportImportFocus::Path => ExportImportFocus::TargetTopic,
                            ExportImportFocus::TargetTopic => ExportImportFocus::Path,
                        };
                        state.set_focus(next);
                    }
                }
                vec![]
            }
            Action::Refresh => self.refresh(true),
            Action::AutoRefreshGroupDetail => self.refresh_group_detail_if_idle(false),
            Action::StartCreateProfile => {
                if self.screen == Screen::ProfilePicker {
                    self.profile_create = Some(ProfileCreateState::new());
                }
                vec![]
            }
            Action::StartEditProfile => {
                if self.screen == Screen::ProfilePicker && self.profile_create.is_none() {
                    if let Some(profile) = self.config.profiles.get(self.selected_profile_index) {
                        self.profile_create = Some(ProfileCreateState::from_profile(
                            profile,
                            self.selected_profile_index,
                        ));
                    } else {
                        self.status_message = Some("no profile selected to edit".into());
                    }
                }
                vec![]
            }
            Action::ProfileCreateChar(c) => {
                if let Some(state) = self.profile_create.as_mut() {
                    state.error = None;
                    state.insert_char(c);
                }
                vec![]
            }
            Action::ProfileCreateBackspace => {
                if let Some(state) = self.profile_create.as_mut() {
                    state.error = None;
                    state.backspace();
                }
                vec![]
            }
            Action::ProfileCreateDelete => {
                if let Some(state) = self.profile_create.as_mut() {
                    state.error = None;
                    state.delete_forward();
                }
                vec![]
            }
            Action::ProfileCreateCursorLeft => {
                if let Some(state) = self.profile_create.as_mut() {
                    state.cursor_left();
                }
                vec![]
            }
            Action::ProfileCreateCursorRight => {
                if let Some(state) = self.profile_create.as_mut() {
                    state.cursor_right();
                }
                vec![]
            }
            Action::ProfileCreateCursorHome => {
                if let Some(state) = self.profile_create.as_mut() {
                    state.cursor_home();
                }
                vec![]
            }
            Action::ProfileCreateCursorEnd => {
                if let Some(state) = self.profile_create.as_mut() {
                    state.cursor_end();
                }
                vec![]
            }
            Action::ProfileCreateFocusNext => {
                if let Some(state) = self.profile_create.as_mut() {
                    state.focus_next();
                }
                vec![]
            }
            Action::ProfileCreateFocusPrev => {
                if let Some(state) = self.profile_create.as_mut() {
                    state.focus_prev();
                }
                vec![]
            }
            Action::ProfileCreateToggleTls => {
                if let Some(state) = self.profile_create.as_mut() {
                    if state.focus == ProfileCreateFocus::Tls {
                        state.tls_enabled = !state.tls_enabled;
                    }
                }
                vec![]
            }
            Action::ProfileCreateSubmit => self.submit_profile_create(),
            Action::ProfileCreateCancel => {
                // If there are still no profiles, cancel quits rather than leaving an
                // empty unusable picker (user can re-run to create again).
                if self.config.profiles.is_empty() {
                    self.should_quit = true;
                }
                self.profile_create = None;
                vec![]
            }
            Action::BannerTick => {
                if self.banner_animation {
                    self.banner_frame = self.banner_frame.wrapping_add(1);
                }
                vec![]
            }
            Action::ToggleBannerAnimation => {
                self.banner_animation = !self.banner_animation;
                if !self.banner_animation {
                    self.banner_frame = 0;
                }
                self.status_message = Some(if self.banner_animation {
                    "banner animation on".into()
                } else {
                    "banner animation off".into()
                });
                vec![]
            }
            Action::DismissSplash => {
                self.show_splash = false;
                vec![]
            }
        }
    }

    fn submit_profile_create(&mut self) -> Vec<Command> {
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

    fn move_selection(&mut self, delta: i64) {
        // Freeze selection while wizards own the keyboard.
        if self.profile_create.is_some() {
            return;
        }
        if self
            .group_detail
            .as_ref()
            .is_some_and(|d| d.reset_phase.is_some())
        {
            return;
        }
        if self
            .topic_detail
            .as_ref()
            .is_some_and(|d| d.replay_phase.is_some())
        {
            return;
        }
        // Message inspector owns j/k for scrolling while open.
        if self.scroll_message_view(delta) {
            return;
        }
        match self.screen {
            Screen::ProfilePicker => {
                Self::clamp_index(
                    &mut self.selected_profile_index,
                    self.config.profiles.len(),
                    delta,
                );
            }
            Screen::TopicList => {
                Self::clamp_index(&mut self.topic_list_selected_index, self.topics.len(), delta);
            }
            Screen::TopicDetail => {
                let len = self
                    .topic_detail
                    .as_ref()
                    .map(|detail| {
                        detail
                            .visible_messages_with_registry(self.schema_registry.as_ref())
                            .len()
                    })
                    .unwrap_or(0);
                if let Some(detail) = &mut self.topic_detail {
                    Self::clamp_index(&mut detail.selected_index, len, delta);
                }
            }
            Screen::GroupList => {
                Self::clamp_index(&mut self.group_list_selected_index, self.groups.len(), delta);
            }
            Screen::GroupDetail => {
                if let Some(detail) = &mut self.group_detail {
                    Self::clamp_index(&mut detail.selected_index, detail.lags.len(), delta);
                }
            }
            Screen::Producer | Screen::ExportImport => {
                // Text-entry screens; j/k are characters there.
            }
        }
    }

    /// Bounds-checked cursor move: clamps to `[0, len-1]`, never wraps, never panics on
    /// an empty list (clamps at 0).
    fn clamp_index(index: &mut usize, len: usize, delta: i64) {
        if len == 0 {
            *index = 0;
            return;
        }
        let max = (len - 1) as i64;
        let moved = (*index as i64 + delta).clamp(0, max);
        *index = moved as usize;
    }

    fn confirm(&mut self) -> Vec<Command> {
        // Create-profile form: Enter saves.
        if self.profile_create.is_some() {
            return self.submit_profile_create();
        }
        // Replay wizard: Enter confirms raw same-topic resend.
        if let Some(detail) = self.topic_detail.as_ref() {
            if matches!(detail.replay_phase, Some(ReplayPhase::Confirm { .. })) {
                return self.confirm_replay();
            }
        }

        // Offset-reset wizard intercepts Enter at Confirm or Input phases.
        if let Some(detail) = self.group_detail.as_mut() {
            if let Some(phase) = detail.reset_phase.clone() {
                return match phase {
                    OffsetResetPhase::Input {
                        target_kind,
                        input,
                        ..
                    } => {
                        match parse_reset_input(target_kind, &input) {
                            Ok(target) => {
                                detail.reset_phase = Some(OffsetResetPhase::Confirm { target });
                                vec![]
                            }
                            Err(message) => {
                                self.status_message = Some(message);
                                vec![]
                            }
                        }
                    }
                    OffsetResetPhase::Confirm { .. } => self.confirm_offset_reset(),
                    OffsetResetPhase::ChooseMode => vec![],
                };
            }
        }

        match self.screen {
            Screen::ProfilePicker => {
                let Some(profile) = self.config.profiles.get(self.selected_profile_index).cloned()
                else {
                    // Nothing to select on an empty profile list: safe no-op.
                    return vec![];
                };
                self.attach_profile(profile.clone());
                self.screen = Screen::TopicList;
                self.topics.clear();
                self.topic_list_selected_index = 0;
                // attach_profile may already set a schema-registry error status.
                if self.status_message.is_none() {
                    self.status_message = Some("loading topics...".to_string());
                }
                vec![Command::LoadTopics(profile)]
            }
            Screen::TopicList => {
                let Some(topic) = self.topics.get(self.topic_list_selected_index).cloned() else {
                    return vec![];
                };
                let Some(profile) = self.active_profile.clone() else {
                    return vec![];
                };
                self.topic_detail = Some(TopicDetailState {
                    topic: topic.name.clone(),
                    partition_count: topic.partition_count,
                    mode: BrowseMode::Tail(RingBuffer::new(TAIL_BUFFER_CAPACITY)),
                    selected_index: 0,
                    filter_input: String::new(),
                    filter_cursor: 0,
                    filter_active: false,
                    applied_filter: None,
                    replay_phase: None,
                    message_view: None,
                    sort: MessageSort::default(),
                });
                self.screen = Screen::TopicDetail;
                vec![Command::StartTail { profile, topic: topic.name }]
            }
            Screen::TopicDetail => self.open_or_close_message_view(),
            Screen::GroupList => {
                let Some(group) = self.groups.get(self.group_list_selected_index).cloned() else {
                    return vec![];
                };
                let Some(profile) = self.active_profile.clone() else {
                    return vec![];
                };
                self.group_detail = None;
                self.screen = Screen::GroupDetail;
                self.status_message = Some(format!("loading group {}...", group.name));
                vec![Command::LoadGroupDetail {
                    profile,
                    group: group.name,
                }]
            }
            Screen::GroupDetail => vec![],
            Screen::Producer => vec![],
            Screen::ExportImport => self.export_import_submit(),
        }
    }

    fn back(&mut self) -> Vec<Command> {
        // Create-profile overlay only lives on the profile picker.
        if self.profile_create.is_some() && self.screen == Screen::ProfilePicker {
            return self.update(Action::ProfileCreateCancel);
        }
        // Cancel replay wizard before leaving topic detail.
        if let Some(detail) = self.topic_detail.as_mut() {
            if detail.replay_phase.is_some() {
                detail.replay_phase = None;
                return vec![];
            }
        }
        // Cancel an open offset-reset wizard before leaving the screen.
        if let Some(detail) = self.group_detail.as_mut() {
            if detail.reset_phase.is_some() {
                detail.reset_phase = None;
                return vec![];
            }
        }

        match self.screen {
            Screen::ProfilePicker => vec![],
            Screen::TopicList => {
                self.screen = Screen::ProfilePicker;
                self.status_message = None;
                vec![]
            }
            Screen::TopicDetail => {
                // Esc closes the message inspector first; a second Esc leaves the topic.
                if let Some(detail) = self.topic_detail.as_mut() {
                    if detail.message_view.is_some() {
                        detail.message_view = None;
                        return vec![];
                    }
                }
                self.screen = Screen::TopicList;
                self.topic_detail = None;
                // Always safe to emit: main.rs's tail-task abort is a no-op if seek mode
                // (no continuous task) was active when Back was pressed.
                vec![Command::StopTail]
            }
            Screen::GroupList => {
                self.screen = Screen::TopicList;
                self.status_message = None;
                vec![]
            }
            Screen::GroupDetail => {
                self.screen = Screen::GroupList;
                self.group_detail = None;
                self.status_message = None;
                vec![]
            }
            Screen::Producer => {
                self.screen = Screen::TopicDetail;
                self.producer = None;
                self.status_message = None;
                // Resume tail if topic detail is still in Tail mode (we stopped it on open).
                if let (Some(profile), Some(detail)) =
                    (self.active_profile.clone(), self.topic_detail.as_ref())
                {
                    if matches!(detail.mode, BrowseMode::Tail(_)) {
                        return vec![Command::StartTail {
                            profile,
                            topic: detail.topic.clone(),
                        }];
                    }
                }
                vec![]
            }
            Screen::ExportImport => {
                self.screen = Screen::TopicDetail;
                self.export_import = None;
                self.status_message = None;
                if let (Some(profile), Some(detail)) =
                    (self.active_profile.clone(), self.topic_detail.as_ref())
                {
                    if matches!(detail.mode, BrowseMode::Tail(_)) {
                        return vec![Command::StartTail {
                            profile,
                            topic: detail.topic.clone(),
                        }];
                    }
                }
                vec![]
            }
        }
    }

    fn open_export(&mut self, scope: ExportScope) -> Vec<Command> {
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

    fn open_import(&mut self) -> Vec<Command> {
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

    fn export_import_submit(&mut self) -> Vec<Command> {
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

    fn open_producer(&mut self) -> Vec<Command> {
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

    fn request_replay(&mut self) -> Vec<Command> {
        if self.screen != Screen::TopicDetail {
            return vec![];
        }
        let Some(detail) = self.topic_detail.as_ref() else {
            return vec![];
        };
        if detail.replay_phase.is_some() || detail.filter_active {
            return vec![];
        }
        // Prefer the open inspector snapshot so `y` replays what you're viewing.
        let message = if let Some(view) = &detail.message_view {
            Some(view.message.clone())
        } else {
            detail
                .visible_messages_with_registry(self.schema_registry.as_ref())
                .get(detail.selected_index)
                .map(|m| (*m).clone())
        };
        let Some(message) = message else {
            self.status_message = Some("no message selected to replay".into());
            return vec![];
        };
        // Clone the raw message — never decode, never mutate the buffered original.
        if let Some(detail) = self.topic_detail.as_mut() {
            detail.message_view = None;
            detail.replay_phase = Some(ReplayPhase::Confirm { message });
        }
        vec![]
    }

    /// Enter toggles the full-message inspector for the highlighted row.
    fn open_or_close_message_view(&mut self) -> Vec<Command> {
        if self.screen != Screen::TopicDetail {
            return vec![];
        }
        let Some(detail) = self.topic_detail.as_mut() else {
            return vec![];
        };
        if detail.filter_active || detail.replay_phase.is_some() {
            return vec![];
        }
        if detail.message_view.is_some() {
            detail.message_view = None;
            return vec![];
        }
        let selected = detail.selected_index;
        let message = detail
            .visible_messages_with_registry(self.schema_registry.as_ref())
            .get(selected)
            .map(|m| (*m).clone());
        let Some(message) = message else {
            self.status_message = Some("no message selected".into());
            return vec![];
        };
        detail.message_view = Some(MessageViewState {
            message,
            scroll: 0,
        });
        vec![]
    }

    /// Scroll the open message inspector. Returns true if the action was consumed
    /// (so list navigation / page seek should not also run).
    fn scroll_message_view(&mut self, delta: i64) -> bool {
        let Some(detail) = self.topic_detail.as_mut() else {
            return false;
        };
        let Some(view) = detail.message_view.as_mut() else {
            return false;
        };
        if delta < 0 {
            view.scroll = view.scroll.saturating_sub((-delta) as usize);
        } else {
            view.scroll = view.scroll.saturating_add(delta as usize);
        }
        true
    }

    /// Flip newest↑ / oldest↑ and keep the highlight on the same message.
    fn toggle_message_sort(&mut self) {
        if self.screen != Screen::TopicDetail {
            return;
        }
        let Some(detail) = self.topic_detail.as_mut() else {
            return;
        };
        if detail.filter_active || detail.replay_phase.is_some() || detail.message_view.is_some() {
            return;
        }
        let len = detail
            .visible_messages_with_registry(self.schema_registry.as_ref())
            .len();
        let old_index = detail.selected_index;
        detail.sort = detail.sort.toggle();
        if len > 0 {
            // Reverse is an involution on indices: same message lands at len-1-i.
            detail.selected_index = len - 1 - old_index.min(len - 1);
        }
        self.status_message = Some(format!("sort: {}", detail.sort.label()));
    }

    /// Open the producer prefilled with decoded key/value for editing, then produce
    /// as UTF-8 text (does not re-encode Confluent Avro wire format).
    fn open_replay_edit(&mut self) -> Vec<Command> {
        if self.screen != Screen::TopicDetail {
            return vec![];
        }
        let Some(detail) = self.topic_detail.as_mut() else {
            return vec![];
        };
        let Some(ReplayPhase::Confirm { message }) = detail.replay_phase.take() else {
            return vec![];
        };
        let topic = detail.topic.clone();
        let registry = self.schema_registry.as_ref();
        let key_text = editable_bytes_text(message.key.as_deref(), registry);
        let value_text = editable_bytes_text(message.value.as_deref(), registry);
        let mut producer = ProducerState::new(topic);
        producer.key_input = key_text;
        producer.value_input = value_text;
        producer.focus = ProducerFocus::Value;
        producer.cursor = producer.value_input.chars().count();
        self.producer = Some(producer);
        self.screen = Screen::Producer;
        let header_note = if message.headers.is_empty() {
            String::new()
        } else {
            format!(
                " ({} header(s) not carried into edit — raw replay keeps them)",
                message.headers.len()
            )
        };
        self.status_message = Some(format!(
            "edit mode: decoded text in producer{header_note}; F2/C-p to send"
        ));
        vec![Command::StopTail]
    }

    /// Replays the selected message's raw bytes onto the **same topic** (byte-identical).
    fn confirm_replay(&mut self) -> Vec<Command> {
        let Some(profile) = self.active_profile.clone() else {
            return vec![];
        };
        let Some(detail) = self.topic_detail.as_mut() else {
            return vec![];
        };
        let Some(ReplayPhase::Confirm { message }) = detail.replay_phase.take() else {
            return vec![];
        };
        let topic = detail.topic.clone();
        self.status_message = Some(format!(
            "replaying message partition={} offset={}...",
            message.partition, message.offset
        ));
        vec![Command::ProduceMessage {
            profile,
            topic,
            key: message.key,
            value: message.value,
            headers: message.headers,
        }]
    }

    /// Active text entry surface for shared cursor editing (filter / offset-reset).
    fn text_insert(&mut self, c: char) {
        if let Some(detail) = self.group_detail.as_mut() {
            if let Some(OffsetResetPhase::Input { input, cursor, .. }) = detail.reset_phase.as_mut()
            {
                if c.is_ascii_digit() || c == '-' {
                    crate::text_field::insert_char(input, cursor, c);
                }
                return;
            }
        }
        if let Some(detail) = self.topic_detail.as_mut() {
            if detail.filter_active {
                crate::text_field::insert_char(
                    &mut detail.filter_input,
                    &mut detail.filter_cursor,
                    c,
                );
            }
        }
    }

    fn text_backspace(&mut self) {
        if let Some(detail) = self.group_detail.as_mut() {
            if let Some(OffsetResetPhase::Input { input, cursor, .. }) = detail.reset_phase.as_mut()
            {
                crate::text_field::backspace(input, cursor);
                return;
            }
        }
        if let Some(detail) = self.topic_detail.as_mut() {
            if detail.filter_active {
                crate::text_field::backspace(&mut detail.filter_input, &mut detail.filter_cursor);
            }
        }
    }

    fn text_delete(&mut self) {
        if let Some(detail) = self.group_detail.as_mut() {
            if let Some(OffsetResetPhase::Input { input, cursor, .. }) = detail.reset_phase.as_mut()
            {
                crate::text_field::delete_forward(input, cursor);
                return;
            }
        }
        if let Some(detail) = self.topic_detail.as_mut() {
            if detail.filter_active {
                crate::text_field::delete_forward(
                    &mut detail.filter_input,
                    &mut detail.filter_cursor,
                );
            }
        }
    }

    fn text_cursor_left(&mut self) {
        if let Some(detail) = self.group_detail.as_mut() {
            if let Some(OffsetResetPhase::Input { cursor, .. }) = detail.reset_phase.as_mut() {
                crate::text_field::cursor_left(cursor);
                return;
            }
        }
        if let Some(detail) = self.topic_detail.as_mut() {
            if detail.filter_active {
                crate::text_field::cursor_left(&mut detail.filter_cursor);
            }
        }
    }

    fn text_cursor_right(&mut self) {
        if let Some(detail) = self.group_detail.as_mut() {
            if let Some(OffsetResetPhase::Input { input, cursor, .. }) = detail.reset_phase.as_mut()
            {
                crate::text_field::cursor_right(input, cursor);
                return;
            }
        }
        if let Some(detail) = self.topic_detail.as_mut() {
            if detail.filter_active {
                crate::text_field::cursor_right(&detail.filter_input, &mut detail.filter_cursor);
            }
        }
    }

    fn text_cursor_home(&mut self) {
        if let Some(detail) = self.group_detail.as_mut() {
            if let Some(OffsetResetPhase::Input { cursor, .. }) = detail.reset_phase.as_mut() {
                crate::text_field::cursor_home(cursor);
                return;
            }
        }
        if let Some(detail) = self.topic_detail.as_mut() {
            if detail.filter_active {
                crate::text_field::cursor_home(&mut detail.filter_cursor);
            }
        }
    }

    fn text_cursor_end(&mut self) {
        if let Some(detail) = self.group_detail.as_mut() {
            if let Some(OffsetResetPhase::Input { input, cursor, .. }) = detail.reset_phase.as_mut()
            {
                crate::text_field::cursor_end(input, cursor);
                return;
            }
        }
        if let Some(detail) = self.topic_detail.as_mut() {
            if detail.filter_active {
                crate::text_field::cursor_end(&detail.filter_input, &mut detail.filter_cursor);
            }
        }
    }

    fn producer_insert_char(&mut self, c: char) {
        let Some(state) = self.producer.as_mut() else {
            return;
        };
        if state.focus == ProducerFocus::Value && state.mode != ProducerInputMode::Inline {
            return;
        }
        state.insert_char(c);
    }

    fn producer_backspace(&mut self) {
        let Some(state) = self.producer.as_mut() else {
            return;
        };
        if state.focus == ProducerFocus::Value && state.mode != ProducerInputMode::Inline {
            return;
        }
        state.backspace();
    }

    fn producer_submit(&mut self) -> Vec<Command> {
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

    fn producer_load_file(&mut self) -> Vec<Command> {
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

    fn producer_open_external_editor(&mut self) -> Vec<Command> {
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

    fn open_groups(&mut self) -> Vec<Command> {
        if self.screen != Screen::TopicList {
            return vec![];
        }
        let Some(profile) = self.active_profile.clone() else {
            return vec![];
        };
        self.screen = Screen::GroupList;
        self.groups.clear();
        self.group_list_selected_index = 0;
        self.group_detail = None;
        self.status_message = Some("loading consumer groups...".to_string());
        vec![Command::LoadGroups(profile)]
    }

    /// Manual (`R`) or contextual refresh for the active screen.
    /// `announce` controls whether a brief "refreshing..." status is shown.
    fn refresh(&mut self, announce: bool) -> Vec<Command> {
        match self.screen {
            Screen::TopicList => {
                let Some(profile) = self.active_profile.clone() else {
                    return vec![];
                };
                if announce {
                    self.status_message = Some("refreshing topics...".into());
                }
                vec![Command::LoadTopics(profile)]
            }
            Screen::GroupList => {
                let Some(profile) = self.active_profile.clone() else {
                    return vec![];
                };
                if announce {
                    self.status_message = Some("refreshing groups...".into());
                }
                vec![Command::LoadGroups(profile)]
            }
            Screen::GroupDetail => self.refresh_group_detail_if_idle(announce),
            Screen::TopicDetail => self.refresh_topic_detail(announce),
            _ => vec![],
        }
    }

    /// Reloads the current seek page in place. Tail mode is already live — no I/O.
    fn refresh_topic_detail(&mut self, announce: bool) -> Vec<Command> {
        if self.screen != Screen::TopicDetail {
            return vec![];
        }
        let Some(profile) = self.active_profile.clone() else {
            return vec![];
        };
        let Some(detail) = self.topic_detail.as_ref() else {
            return vec![];
        };
        // Don't clobber an in-progress filter, replay wizard, or open inspector.
        if detail.filter_active || detail.replay_phase.is_some() || detail.message_view.is_some() {
            return vec![];
        }
        match &detail.mode {
            BrowseMode::Seek(state) => {
                if announce {
                    self.status_message = Some("refreshing page...".into());
                }
                vec![Command::LoadSeekPage {
                    profile,
                    topic: detail.topic.clone(),
                    partition: state.partition,
                    request: SeekPageRequest::Forward {
                        from_offset: state.page_start_offset,
                        page_size: SEEK_PAGE_SIZE,
                    },
                }]
            }
            BrowseMode::Tail(_) => {
                if announce {
                    self.status_message =
                        Some("tail is live — switch to page mode (Tab/s) to refresh a page".into());
                }
                vec![]
            }
        }
    }

    /// Reloads group lag/members if we're on group detail and not mid offset-reset wizard.
    fn refresh_group_detail_if_idle(&mut self, announce: bool) -> Vec<Command> {
        if self.screen != Screen::GroupDetail {
            return vec![];
        }
        if self
            .group_detail
            .as_ref()
            .is_some_and(|d| d.reset_phase.is_some())
        {
            return vec![];
        }
        let Some(profile) = self.active_profile.clone() else {
            return vec![];
        };
        let Some(group) = self.group_detail.as_ref().map(|d| d.name.clone()) else {
            return vec![];
        };
        if announce {
            self.status_message = Some(format!("refreshing group {group}..."));
        }
        vec![Command::LoadGroupDetail { profile, group }]
    }

    fn start_offset_reset(&mut self) -> Vec<Command> {
        if self.screen != Screen::GroupDetail {
            return vec![];
        }
        let Some(detail) = self.group_detail.as_mut() else {
            return vec![];
        };
        if detail.lags.is_empty() {
            self.status_message = Some("no committed offsets to reset".into());
            return vec![];
        }
        detail.reset_phase = Some(OffsetResetPhase::ChooseMode);
        vec![]
    }

    fn choose_offset_reset_target(&mut self, target: OffsetResetTarget) -> Vec<Command> {
        let Some(detail) = self.group_detail.as_mut() else {
            return vec![];
        };
        if !matches!(detail.reset_phase, Some(OffsetResetPhase::ChooseMode)) {
            return vec![];
        }
        detail.reset_phase = Some(OffsetResetPhase::Confirm { target });
        vec![]
    }

    fn begin_offset_reset_input(&mut self, kind: ResetInputKind) -> Vec<Command> {
        let Some(detail) = self.group_detail.as_mut() else {
            return vec![];
        };
        if !matches!(detail.reset_phase, Some(OffsetResetPhase::ChooseMode)) {
            return vec![];
        }
        detail.reset_phase = Some(OffsetResetPhase::Input {
            target_kind: kind,
            input: String::new(),
            cursor: 0,
        });
        vec![]
    }

    fn confirm_offset_reset(&mut self) -> Vec<Command> {
        let Some(profile) = self.active_profile.clone() else {
            return vec![];
        };
        let Some(detail) = self.group_detail.as_ref() else {
            return vec![];
        };
        let Some(OffsetResetPhase::Confirm { target }) = detail.reset_phase.clone() else {
            return vec![];
        };
        let partitions: Vec<(String, i32)> = detail
            .lags
            .iter()
            .map(|lag| (lag.topic.clone(), lag.partition))
            .collect();
        let group = detail.name.clone();
        // Clear wizard; status reflects in-flight work.
        if let Some(detail) = self.group_detail.as_mut() {
            detail.reset_phase = None;
        }
        self.status_message = Some(format!("resetting offsets for {group}..."));
        vec![Command::ResetGroupOffsets {
            profile,
            group,
            target,
            partitions,
        }]
    }

    fn toggle_browse_mode(&mut self) -> Vec<Command> {
        let Some(profile) = self.active_profile.clone() else {
            return vec![];
        };
        let Some(detail) = self.topic_detail.as_mut() else {
            return vec![];
        };
        match &detail.mode {
            BrowseMode::Tail(_) => {
                let partition = 0;
                detail.mode = BrowseMode::Seek(SeekState {
                    partition,
                    messages: Vec::new(),
                    page_start_offset: 0,
                    at_beginning: false,
                    at_end: false,
                    low_watermark: 0,
                    high_watermark: 0,
                });
                detail.selected_index = 0;
                vec![
                    Command::StopTail,
                    Command::LoadSeekPage {
                        profile,
                        topic: detail.topic.clone(),
                        partition,
                        request: SeekPageRequest::Latest { page_size: SEEK_PAGE_SIZE },
                    },
                ]
            }
            BrowseMode::Seek(_) => {
                detail.mode = BrowseMode::Tail(RingBuffer::new(TAIL_BUFFER_CAPACITY));
                detail.selected_index = 0;
                vec![Command::StartTail { profile, topic: detail.topic.clone() }]
            }
        }
    }

    fn request_seek_page(&mut self, direction: PageDirection) -> Vec<Command> {
        let Some(profile) = self.active_profile.clone() else {
            return vec![];
        };
        let Some(detail) = self.topic_detail.as_ref() else {
            return vec![];
        };
        let BrowseMode::Seek(state) = &detail.mode else {
            return vec![];
        };
        match direction {
            PageDirection::Forward => {
                if state.at_end {
                    return vec![];
                }
                let from_offset = state.page_start_offset + state.messages.len() as i64;
                vec![Command::LoadSeekPage {
                    profile,
                    topic: detail.topic.clone(),
                    partition: state.partition,
                    request: SeekPageRequest::Forward { from_offset, page_size: SEEK_PAGE_SIZE },
                }]
            }
            PageDirection::Backward => {
                if state.at_beginning {
                    return vec![];
                }
                vec![Command::LoadSeekPage {
                    profile,
                    topic: detail.topic.clone(),
                    partition: state.partition,
                    request: SeekPageRequest::Backward {
                        before_offset: state.page_start_offset,
                        page_size: SEEK_PAGE_SIZE,
                    },
                }]
            }
        }
    }

    /// Applies a background event. May return `FetchSchema` commands when Avro messages
    /// are seen and the schema is not yet cached.
    pub fn apply_event(&mut self, event: AppEvent) -> Vec<Command> {
        match event {
            AppEvent::TopicsLoaded {
                topics,
                auto_message_max_bytes,
            } => {
                self.topics = topics;
                // Preserve selection across refresh; clamp if the list shrank.
                if self.topics.is_empty() {
                    self.topic_list_selected_index = 0;
                } else {
                    self.topic_list_selected_index = self
                        .topic_list_selected_index
                        .min(self.topics.len() - 1);
                }

                let mut detect_status = None;
                if let Some((profile_name, bytes)) = auto_message_max_bytes {
                    detect_status = self.apply_auto_message_max_bytes(&profile_name, bytes);
                }

                // Don't clobber a schema-registry init warning.
                if self
                    .status_message
                    .as_deref()
                    .is_some_and(|s| s.contains("schema registry"))
                {
                    // keep warning
                } else if let Some(msg) = detect_status {
                    self.status_message = Some(msg);
                } else {
                    self.status_message = None;
                }
                vec![]
            }
            AppEvent::TopicsLoadFailed(message) => {
                self.status_message = Some(format!("failed to load topics: {message}"));
                vec![]
            }
            AppEvent::MessageArrived {
                topic,
                partition,
                message,
            } => {
                let mut commands = Vec::new();
                if let Some(cmd) = self.queue_schema_fetch_for_bytes(message.key.as_deref()) {
                    commands.push(cmd);
                }
                if let Some(cmd) = self.queue_schema_fetch_for_bytes(message.value.as_deref()) {
                    commands.push(cmd);
                }
                if let Some(detail) = self.topic_detail.as_mut() {
                    if detail.topic == topic {
                        if let BrowseMode::Tail(buffer) = &mut detail.mode {
                            // Prefer the event tag if it ever diverges from the payload.
                            let mut message = message;
                            message.partition = partition;
                            buffer.push(message);
                        }
                        // else: stale arrival from a just-aborted tail task racing the
                        // switch to seek mode - silently dropped.
                    }
                }
                commands
            }
            AppEvent::SeekPageLoaded { topic, messages, meta } => {
                let mut commands = Vec::new();
                for message in &messages {
                    if let Some(cmd) = self.queue_schema_fetch_for_bytes(message.key.as_deref()) {
                        commands.push(cmd);
                    }
                    if let Some(cmd) = self.queue_schema_fetch_for_bytes(message.value.as_deref()) {
                        commands.push(cmd);
                    }
                }
                if let Some(detail) = self.topic_detail.as_mut() {
                    if detail.topic == topic {
                        if let BrowseMode::Seek(state) = &mut detail.mode {
                            if state.partition == meta.partition {
                                state.messages = messages;
                                state.page_start_offset = meta.page_start_offset;
                                state.at_beginning = meta.at_beginning;
                                state.at_end = meta.at_end;
                                state.low_watermark = meta.low_watermark;
                                state.high_watermark = meta.high_watermark;
                                detail.selected_index = 0;
                                // Clear stale "refreshing page..." once data lands.
                                if self
                                    .status_message
                                    .as_deref()
                                    .is_some_and(|s| s.starts_with("refreshing"))
                                {
                                    self.status_message = None;
                                }
                            }
                        }
                    }
                }
                commands
            }
            AppEvent::BrowseFailed(message) => {
                self.status_message = Some(format!("browse error: {message}"));
                vec![]
            }
            AppEvent::GroupsLoaded(groups) => {
                self.groups = groups;
                if self.groups.is_empty() {
                    self.group_list_selected_index = 0;
                } else {
                    self.group_list_selected_index = self
                        .group_list_selected_index
                        .min(self.groups.len() - 1);
                }
                self.status_message = None;
                vec![]
            }
            AppEvent::GroupsLoadFailed(message) => {
                self.status_message = Some(format!("failed to load groups: {message}"));
                vec![]
            }
            AppEvent::GroupDetailLoaded(detail) => {
                if self.screen == Screen::GroupDetail {
                    let has_active_members = detail.has_active_members();
                    let total_lag = detail.total_lag();
                    // Keep cursor + any in-progress offset-reset wizard across soft refresh.
                    let (prev_selected, prev_reset) = self
                        .group_detail
                        .as_ref()
                        .map(|d| (d.selected_index, d.reset_phase.clone()))
                        .unwrap_or((0, None));
                    let lag_len = detail.lags.len();
                    let selected_index = if lag_len == 0 {
                        0
                    } else {
                        prev_selected.min(lag_len - 1)
                    };
                    self.group_detail = Some(GroupDetailState {
                        name: detail.name,
                        state: detail.state,
                        has_active_members,
                        total_lag,
                        members: detail.members,
                        lags: detail.lags,
                        selected_index,
                        reset_phase: prev_reset,
                    });
                    if !self
                        .status_message
                        .as_deref()
                        .is_some_and(|s| s.contains("schema"))
                    {
                        self.status_message = None;
                    }
                }
                vec![]
            }
            AppEvent::GroupDetailLoadFailed(message) => {
                self.status_message = Some(format!("failed to load group: {message}"));
                vec![]
            }
            AppEvent::OffsetResetSucceeded { group } => {
                self.status_message = Some(format!("offsets reset for {group}"));
                vec![]
            }
            AppEvent::OffsetResetFailed(message) => {
                self.status_message = Some(format!("offset reset failed: {message}"));
                vec![]
            }
            AppEvent::ProduceSucceeded => {
                self.status_message = Some("message produced".into());
                vec![]
            }
            AppEvent::ProduceFailed(message) => {
                self.status_message = Some(format!("produce failed: {message}"));
                vec![]
            }
            AppEvent::FileLoaded { content } => {
                if let Some(state) = self.producer.as_mut() {
                    state.value_input = content;
                    self.status_message = Some("file loaded into value".into());
                }
                vec![]
            }
            AppEvent::FileLoadFailed(message) => {
                self.status_message = Some(format!("file load failed: {message}"));
                vec![]
            }
            AppEvent::ExternalEditorDone { content } => {
                if let Some(state) = self.producer.as_mut() {
                    state.value_input = content;
                    self.status_message = Some("editor closed — value updated".into());
                }
                vec![]
            }
            AppEvent::ExternalEditorFailed(message) => {
                self.status_message = Some(format!("external editor failed: {message}"));
                vec![]
            }
            AppEvent::ExportSucceeded { path, count } => {
                self.status_message = Some(format!("exported {count} message(s) to {path}"));
                vec![]
            }
            AppEvent::ExportFailed(message) => {
                self.status_message = Some(format!("export failed: {message}"));
                vec![]
            }
            AppEvent::ImportSucceeded { count, topic } => {
                self.status_message = Some(format!("imported {count} message(s) into {topic}"));
                vec![]
            }
            AppEvent::ImportFailed(message) => {
                self.status_message = Some(format!("import failed: {message}"));
                vec![]
            }
            AppEvent::SchemaLoaded { schema_id, schema } => {
                self.schema_fetch_inflight.remove(&schema_id);
                if let Some(sr) = self.schema_registry.as_mut() {
                    sr.insert_schema(schema_id, schema);
                    tracing::info!("schema id {schema_id} cached ({} total)", sr.cache_len());
                }
                // Quiet status once decode works; clear fetch-related noise.
                if self
                    .status_message
                    .as_deref()
                    .is_some_and(|s| s.contains("schema"))
                {
                    self.status_message = None;
                }
                vec![]
            }
            AppEvent::SchemaLoadFailed { schema_id, message } => {
                self.schema_fetch_inflight.remove(&schema_id);
                self.schema_fetch_failed.insert(schema_id);
                self.status_message =
                    Some(format!("schema id {schema_id} fetch failed: {message}"));
                tracing::warn!("schema id {schema_id} fetch failed: {message}");
                vec![]
            }
        }
    }

    /// After a successful offset reset, re-fetch group detail so the lag table updates.
    pub fn reload_group_detail_command(&self) -> Option<Command> {
        let profile = self.active_profile.clone()?;
        let group = self.group_detail.as_ref()?.name.clone();
        Some(Command::LoadGroupDetail { profile, group })
    }
}

fn parse_reset_input(kind: ResetInputKind, input: &str) -> Result<OffsetResetTarget, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("enter a numeric value".into());
    }
    let value: i64 = trimmed
        .parse()
        .map_err(|_| format!("invalid number: {trimmed}"))?;
    match kind {
        ResetInputKind::AbsoluteOffset => {
            if value < 0 {
                return Err("offset must be >= 0".into());
            }
            Ok(OffsetResetTarget::Absolute(value))
        }
        ResetInputKind::TimestampMillis => Ok(OffsetResetTarget::Timestamp(value)),
    }
}

/// Text for the producer when editing a replayed message: prefer decoded view
/// (JSON / Avro-as-JSON / text); null/empty becomes an empty field.
fn editable_bytes_text(
    bytes: Option<&[u8]>,
    registry: Option<&crate::kafka::schema_registry::SchemaRegistry>,
) -> String {
    let Some(bytes) = bytes else {
        return String::new();
    };
    if bytes.is_empty() {
        return String::new();
    }
    let decoded = crate::serde_detect::decode_value(bytes, registry);
    let text = decoded.as_str();
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
        if let Ok(pretty) = serde_json::to_string_pretty(&value) {
            return pretty;
        }
    }
    text.to_string()
}

#[cfg(test)]
mod tests {

    fn test_config_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "rakko-test-config-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    use super::*;
    use crate::config::AuthMode;
    use std::collections::HashMap;

    fn profile(name: &str) -> Profile {
        Profile {
            name: name.into(),
            bootstrap_servers: "localhost:9092".into(),
            tls_enabled: false,
            auth: AuthMode::None,
            schema_registry_url: None,
            message_max_bytes: None,
            extra_producer_config: HashMap::new(),
        }
    }

    fn topic(name: &str) -> TopicSummary {
        TopicSummary {
            name: name.into(),
            partition_count: 1,
            replication_factor: 1,
            compression_type: "none".into(),
            total_message_count: 0,
        }
    }

    #[test]
    fn quit_opens_confirm_dialog_without_exiting() {
        let mut app = App::new(Config::default(), test_config_path());
        assert!(!app.should_quit);
        assert!(!app.quit_confirm);
        app.update(Action::Quit);
        assert!(app.quit_confirm);
        assert!(!app.should_quit);
    }

    #[test]
    fn confirm_quit_exits() {
        let mut app = App::new(Config::default(), test_config_path());
        app.update(Action::Quit);
        app.update(Action::ConfirmQuit);
        assert!(app.should_quit);
        assert!(!app.quit_confirm);
    }

    #[test]
    fn cancel_quit_dismisses_dialog() {
        let mut app = App::new(Config::default(), test_config_path());
        app.update(Action::Quit);
        app.update(Action::CancelQuit);
        assert!(!app.should_quit);
        assert!(!app.quit_confirm);
    }

    #[test]
    fn force_quit_exits_immediately() {
        let mut app = App::new(Config::default(), test_config_path());
        app.update(Action::ForceQuit);
        assert!(app.should_quit);
        assert!(!app.quit_confirm);
    }

    #[test]
    fn selection_clamps_at_zero_on_empty_profile_list() {
        let mut app = App::new(Config::default(), test_config_path());
        app.update(Action::MoveSelectionDown);
        assert_eq!(app.selected_profile_index, 0);
        app.update(Action::MoveSelectionUp);
        assert_eq!(app.selected_profile_index, 0);
    }

    #[test]
    fn selection_clamps_at_bounds_on_populated_profile_list() {
        let config = Config {
            profiles: vec![profile("a"), profile("b")],
        };
        let mut app = App::new(config, test_config_path());

        // Clamp at 0 going up from the start.
        app.update(Action::MoveSelectionUp);
        assert_eq!(app.selected_profile_index, 0);

        app.update(Action::MoveSelectionDown);
        assert_eq!(app.selected_profile_index, 1);

        // Clamp at len-1 going past the end.
        app.update(Action::MoveSelectionDown);
        assert_eq!(app.selected_profile_index, 1);
    }

    #[test]
    fn topic_list_selection_clamps_independently_of_profile_picker() {
        let mut app = App::new(Config::default(), test_config_path());
        app.profile_create = None; // empty-config wizard would freeze navigation
        app.screen = Screen::TopicList;
        app.topics = vec![topic("t1"), topic("t2"), topic("t3")];
        app.update(Action::MoveSelectionDown);
        app.update(Action::MoveSelectionDown);
        app.update(Action::MoveSelectionDown);
        assert_eq!(app.topic_list_selected_index, 2);
    }

    #[test]
    fn confirm_on_empty_profile_list_is_a_safe_no_op() {
        let mut app = App::new(Config::default(), test_config_path());
        // Empty config auto-opens the create wizard; cancel it to exercise the bare picker.
        app.profile_create = None;
        let commands = app.update(Action::Confirm);
        assert!(commands.is_empty());
        assert_eq!(app.screen, Screen::ProfilePicker);
        assert!(app.active_profile.is_none());
    }

    #[test]
    fn empty_config_auto_opens_create_profile_wizard() {
        let app = App::new(Config::default(), test_config_path());
        assert!(app.profile_create.is_some());
        assert_eq!(app.screen, Screen::ProfilePicker);
    }

    #[test]
    fn submit_profile_create_saves_and_closes_wizard() {
        let path = test_config_path();
        let mut app = App::new(Config::default(), path.clone());
        assert!(app.profile_create.is_some());
        // Defaults: name=local, bootstrap=localhost:9092
        app.update(Action::ProfileCreateSubmit);
        assert!(app.profile_create.is_none());
        assert_eq!(app.config.profiles.len(), 1);
        assert_eq!(app.config.profiles[0].name, "local");
        assert!(path.exists());
        let loaded = config::load(&path).unwrap();
        assert_eq!(loaded.profiles.len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn auto_message_max_bytes_saved_when_profile_unset() {
        let path = test_config_path();
        let mut app = App::new(
            Config {
                profiles: vec![profile("local")],
            },
            path.clone(),
        );
        app.attach_profile(profile("local"));
        assert!(app.config.profiles[0].message_max_bytes.is_none());

        app.apply_event(AppEvent::TopicsLoaded {
            topics: vec![topic("t1")],
            auto_message_max_bytes: Some(("local".into(), 20_971_520)),
        });

        assert_eq!(app.config.profiles[0].message_max_bytes, Some(20_971_520));
        assert_eq!(
            app.active_profile.as_ref().and_then(|p| p.message_max_bytes),
            Some(20_971_520)
        );
        assert!(app
            .status_message
            .as_ref()
            .is_some_and(|s| s.contains("detected message.max.bytes=20971520")));
        let loaded = config::load(&path).unwrap();
        assert_eq!(loaded.profiles[0].message_max_bytes, Some(20_971_520));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn auto_message_max_bytes_does_not_override_explicit() {
        let path = test_config_path();
        let mut explicit = profile("local");
        explicit.message_max_bytes = Some(1_000_000);
        let mut app = App::new(
            Config {
                profiles: vec![explicit.clone()],
            },
            path.clone(),
        );
        app.attach_profile(explicit);

        // Even if a buggy task sent a detect result, apply must refuse overwrite
        // when the profile already has a value (apply checks is_some).
        // First clear the "detect" path by calling apply with a name that has a value.
        let status = app.apply_auto_message_max_bytes("local", 20_971_520);
        assert!(status.is_none());
        assert_eq!(app.config.profiles[0].message_max_bytes, Some(1_000_000));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn start_edit_profile_prefills_form() {
        let mut app = App::new(
            Config {
                profiles: vec![Profile {
                    name: "prod".into(),
                    bootstrap_servers: "kafka:9093".into(),
                    tls_enabled: true,
                    auth: AuthMode::None,
                    schema_registry_url: Some("http://sr:8081".into()),
                    message_max_bytes: Some(2_000_000),
                    extra_producer_config: HashMap::from([(
                        "compression.type".into(),
                        "zstd".into(),
                    )]),
                }],
            },
            test_config_path(),
        );
        app.selected_profile_index = 0;
        app.update(Action::StartEditProfile);
        let state = app.profile_create.as_ref().expect("edit form open");
        assert!(state.is_edit());
        assert_eq!(state.edit_index, Some(0));
        assert_eq!(state.name, "prod");
        assert_eq!(state.bootstrap_servers, "kafka:9093");
        assert!(state.tls_enabled);
        assert_eq!(state.schema_registry_url, "http://sr:8081");
    }

    #[test]
    fn submit_profile_edit_preserves_advanced_fields() {
        let path = test_config_path();
        let mut app = App::new(
            Config {
                profiles: vec![Profile {
                    name: "local".into(),
                    bootstrap_servers: "localhost:9092".into(),
                    tls_enabled: false,
                    auth: AuthMode::Tls {
                        ca_path: "/certs/ca.pem".into(),
                    },
                    schema_registry_url: None,
                    message_max_bytes: Some(20_971_520),
                    extra_producer_config: HashMap::from([(
                        "compression.type".into(),
                        "zstd".into(),
                    )]),
                }],
            },
            path.clone(),
        );
        app.selected_profile_index = 0;
        app.update(Action::StartEditProfile);
        {
            let state = app.profile_create.as_mut().unwrap();
            state.bootstrap_servers = "192.168.1.10:9093".into();
            state.tls_enabled = true;
            state.schema_registry_url = "http://localhost:8081".into();
        }
        app.update(Action::ProfileCreateSubmit);
        assert!(app.profile_create.is_none());
        assert_eq!(app.config.profiles.len(), 1);
        let p = &app.config.profiles[0];
        assert_eq!(p.name, "local");
        assert_eq!(p.bootstrap_servers, "192.168.1.10:9093");
        assert!(p.tls_enabled);
        assert_eq!(
            p.schema_registry_url.as_deref(),
            Some("http://localhost:8081")
        );
        assert_eq!(p.message_max_bytes, Some(20_971_520));
        assert_eq!(
            p.extra_producer_config.get("compression.type").map(String::as_str),
            Some("zstd")
        );
        assert!(matches!(
            &p.auth,
            AuthMode::Tls { ca_path } if ca_path == "/certs/ca.pem"
        ));
        let loaded = config::load(&path).unwrap();
        assert_eq!(loaded.profiles[0].bootstrap_servers, "192.168.1.10:9093");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn edit_rename_conflict_rejected() {
        let mut app = App::new(
            Config {
                profiles: vec![profile("a"), profile("b")],
            },
            test_config_path(),
        );
        app.selected_profile_index = 0;
        app.update(Action::StartEditProfile);
        app.profile_create.as_mut().unwrap().name = "b".into();
        app.update(Action::ProfileCreateSubmit);
        assert!(app.profile_create.is_some());
        assert!(app
            .profile_create
            .as_ref()
            .unwrap()
            .error
            .as_ref()
            .is_some_and(|e| e.contains("already exists")));
        assert_eq!(app.config.profiles[0].name, "a");
    }

    #[test]
    fn cancel_create_with_no_profiles_quits() {
        let mut app = App::new(Config::default(), test_config_path());
        app.update(Action::ProfileCreateCancel);
        assert!(app.should_quit);
        assert!(app.profile_create.is_none());
    }

    #[test]
    fn cancel_create_with_existing_profiles_returns_to_picker() {
        let mut app = App::new(
            Config {
                profiles: vec![profile("a")],
            },
            test_config_path(),
        );
        app.profile_create = Some(ProfileCreateState::new());
        app.update(Action::ProfileCreateCancel);
        assert!(!app.should_quit);
        assert!(app.profile_create.is_none());
        assert_eq!(app.config.profiles.len(), 1);
    }

    #[test]
    fn profile_create_cursor_left_right_insert_and_backspace() {
        let mut state = ProfileCreateState::new();
        // name starts as "local", cursor at end
        assert_eq!(state.name, "local");
        assert_eq!(state.cursor, 5);
        state.cursor_left();
        state.cursor_left();
        assert_eq!(state.cursor, 3);
        state.insert_char('X');
        assert_eq!(state.name, "locXal");
        assert_eq!(state.cursor, 4);
        state.backspace();
        assert_eq!(state.name, "local");
        assert_eq!(state.cursor, 3);
        state.cursor_home();
        assert_eq!(state.cursor, 0);
        state.delete_forward();
        assert_eq!(state.name, "ocal");
        state.cursor_end();
        assert_eq!(state.cursor, 4);
    }

    #[test]
    fn toggle_banner_animation() {
        let mut app = App::new(Config::default(), test_config_path());
        assert!(app.banner_animation);
        app.update(Action::ToggleBannerAnimation);
        assert!(!app.banner_animation);
        app.update(Action::BannerTick);
        assert_eq!(app.banner_frame, 0); // frozen
        app.update(Action::ToggleBannerAnimation);
        assert!(app.banner_animation);
        app.update(Action::BannerTick);
        assert_eq!(app.banner_frame, 1);
    }

    #[test]
    fn dismiss_splash() {
        let mut app = App::new(Config::default(), test_config_path());
        assert!(app.show_splash);
        app.update(Action::DismissSplash);
        assert!(!app.show_splash);
    }

    #[test]
    fn confirm_on_profile_picker_transitions_to_topic_list_and_returns_load_command() {
        let config = Config {
            profiles: vec![profile("a")],
        };
        let mut app = App::new(config, test_config_path());
        let commands = app.update(Action::Confirm);
        assert_eq!(app.screen, Screen::TopicList);
        assert_eq!(app.active_profile.as_ref().map(|p| p.name.as_str()), Some("a"));
        match commands.as_slice() {
            [Command::LoadTopics(profile)] => assert_eq!(profile.name, "a"),
            other => panic!("expected exactly one LoadTopics command, got {other:?}"),
        }
    }

    #[test]
    fn back_on_topic_list_returns_to_profile_picker() {
        let mut app = App::new(Config::default(), test_config_path());
        app.profile_create = None;
        app.screen = Screen::TopicList;
        app.update(Action::Back);
        assert_eq!(app.screen, Screen::ProfilePicker);
    }

    #[test]
    fn apply_event_topics_loaded_populates_topics_and_clears_status() {
        let mut app = App::new(Config::default(), test_config_path());
        app.status_message = Some("loading...".into());
        app.apply_event(AppEvent::TopicsLoaded {
            topics: vec![topic("t1")],
            auto_message_max_bytes: None,
        });
        assert_eq!(app.topics.len(), 1);
        assert!(app.status_message.is_none());
    }

    #[test]
    fn apply_event_load_failed_sets_status_without_panicking() {
        let mut app = App::new(Config::default(), test_config_path());
        app.apply_event(AppEvent::TopicsLoadFailed("boom".into()));
        assert!(app.status_message.is_some());
    }

    fn message(key: &str, value: &str) -> RawMessage {
        RawMessage {
            topic: "t1".into(),
            partition: 0,
            offset: 0,
            timestamp_millis: None,
            key: Some(key.as_bytes().to_vec()),
            value: Some(value.as_bytes().to_vec()),
            headers: vec![],
        }
    }

    fn app_in_topic_detail(topic_name: &str, partition_count: usize) -> App {
        let mut app = App::new(Config {
            profiles: vec![profile("a")],
        }, test_config_path());
        app.active_profile = Some(profile("a"));
        app.screen = Screen::TopicDetail;
        app.topic_detail = Some(TopicDetailState {
            topic: topic_name.to_string(),
            partition_count,
            mode: BrowseMode::Tail(RingBuffer::new(TAIL_BUFFER_CAPACITY)),
            selected_index: 0,
            filter_input: String::new(),
            filter_cursor: 0,
            filter_active: false,
            applied_filter: None,
            replay_phase: None,
            message_view: None,
            sort: MessageSort::default(),
        });
        app
    }

    fn seek_state(
        messages: Vec<RawMessage>,
        page_start_offset: i64,
        at_beginning: bool,
        at_end: bool,
    ) -> SeekState {
        SeekState {
            partition: 0,
            messages,
            page_start_offset,
            at_beginning,
            at_end,
            low_watermark: 0,
            high_watermark: 1000,
        }
    }

    #[test]
    fn confirm_on_topic_list_enters_topic_detail_in_tail_mode_and_starts_tail() {
        let config = Config {
            profiles: vec![profile("a")],
        };
        let mut app = App::new(config, test_config_path());
        app.active_profile = Some(profile("a"));
        app.screen = Screen::TopicList;
        app.topics = vec![topic("orders")];

        let commands = app.update(Action::Confirm);

        assert_eq!(app.screen, Screen::TopicDetail);
        let detail = app.topic_detail.as_ref().expect("topic_detail set");
        assert_eq!(detail.topic, "orders");
        assert!(matches!(detail.mode, BrowseMode::Tail(_)));
        match commands.as_slice() {
            [Command::StartTail { topic, .. }] => assert_eq!(topic, "orders"),
            other => panic!("expected exactly one StartTail command, got {other:?}"),
        }
    }

    #[test]
    fn message_arrived_pushes_into_tail_ring_buffer() {
        let mut app = app_in_topic_detail("orders", 1);
        app.apply_event(AppEvent::MessageArrived {
            topic: "orders".into(),
            partition: 0,
            message: message("k1", "v1"),
        });
        let detail = app.topic_detail.as_ref().unwrap();
        assert_eq!(detail.visible_messages().len(), 1);
    }

    #[test]
    fn message_arrived_for_a_different_topic_is_ignored() {
        let mut app = app_in_topic_detail("orders", 1);
        app.apply_event(AppEvent::MessageArrived {
            topic: "other-topic".into(),
            partition: 0,
            message: message("k1", "v1"),
        });
        let detail = app.topic_detail.as_ref().unwrap();
        assert_eq!(detail.visible_messages().len(), 0);
    }

    #[test]
    fn message_arrived_while_in_seek_mode_is_dropped_not_applied() {
        let mut app = app_in_topic_detail("orders", 1);
        // Switch to seek mode without going through toggle_browse_mode (no active
        // profile plumbing needed for this test - just testing apply_event's guard).
        if let Some(detail) = app.topic_detail.as_mut() {
            detail.mode = BrowseMode::Seek(SeekState {
                partition: 0,
                messages: vec![],
                page_start_offset: 0,
                at_beginning: false,
                at_end: false,
                low_watermark: 0,
                high_watermark: 1000,
            });
        }
        app.apply_event(AppEvent::MessageArrived {
            topic: "orders".into(),
            partition: 0,
            message: message("k1", "v1"),
        });
        let detail = app.topic_detail.as_ref().unwrap();
        assert_eq!(detail.visible_messages().len(), 0);
    }

    #[test]
    fn toggle_browse_mode_switches_tail_to_seek_and_requests_latest_page() {
        let mut app = app_in_topic_detail("orders", 3);
        let commands = app.update(Action::ToggleBrowseMode);
        let detail = app.topic_detail.as_ref().unwrap();
        assert!(matches!(detail.mode, BrowseMode::Seek(_)));
        assert!(matches!(commands.as_slice(), [Command::StopTail, Command::LoadSeekPage { .. }]));
    }

    #[test]
    fn toggle_browse_mode_switches_seek_back_to_tail_and_starts_tail() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            detail.mode = BrowseMode::Seek(SeekState {
                partition: 0,
                messages: vec![message("k", "v")],
                page_start_offset: 5,
                at_beginning: false,
                at_end: true,
                low_watermark: 0,
                high_watermark: 1000,
            });
        }
        let commands = app.update(Action::ToggleBrowseMode);
        let detail = app.topic_detail.as_ref().unwrap();
        assert!(matches!(detail.mode, BrowseMode::Tail(_)));
        assert!(matches!(commands.as_slice(), [Command::StartTail { .. }]));
    }

    #[test]
    fn page_forward_at_end_is_a_no_op() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            detail.mode = BrowseMode::Seek(SeekState {
                partition: 0,
                messages: vec![message("k", "v")],
                page_start_offset: 0,
                at_beginning: true,
                at_end: true,
                low_watermark: 0,
                high_watermark: 1000,
            });
        }
        let commands = app.update(Action::PageForward);
        assert!(commands.is_empty());
    }

    #[test]
    fn page_forward_requests_next_page_from_current_page_end() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            detail.mode = BrowseMode::Seek(SeekState {
                partition: 0,
                messages: vec![message("k1", "v1"), message("k2", "v2")],
                page_start_offset: 10,
                at_beginning: false,
                at_end: false,
                low_watermark: 0,
                high_watermark: 1000,
            });
        }
        let commands = app.update(Action::PageForward);
        match commands.as_slice() {
            [Command::LoadSeekPage {
                request: SeekPageRequest::Forward { from_offset, .. },
                ..
            }] => assert_eq!(*from_offset, 12),
            other => panic!("expected one Forward LoadSeekPage command, got {other:?}"),
        }
    }

    #[test]
    fn page_backward_at_beginning_is_a_no_op() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            detail.mode = BrowseMode::Seek(SeekState {
                partition: 0,
                messages: vec![message("k", "v")],
                page_start_offset: 0,
                at_beginning: true,
                at_end: false,
                low_watermark: 0,
                high_watermark: 1000,
            });
        }
        let commands = app.update(Action::PageBackward);
        assert!(commands.is_empty());
    }

    #[test]
    fn filter_input_lifecycle_apply() {
        let mut app = app_in_topic_detail("orders", 1);
        app.update(Action::StartFilterInput);
        app.update(Action::FilterChar('a'));
        app.update(Action::FilterChar('b'));
        app.update(Action::FilterBackspace);
        app.update(Action::FilterChar('c'));
        let detail = app.topic_detail.as_ref().unwrap();
        assert_eq!(detail.filter_input, "ac");
        assert!(detail.filter_active);

        app.update(Action::ApplyFilter);
        let detail = app.topic_detail.as_ref().unwrap();
        assert_eq!(detail.applied_filter.as_deref(), Some("ac"));
        assert!(!detail.filter_active);
    }

    #[test]
    fn filter_input_cancel_discards_typed_text_without_touching_applied_filter() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            detail.applied_filter = Some("existing".to_string());
        }
        app.update(Action::StartFilterInput);
        app.update(Action::FilterChar('x'));
        app.update(Action::CancelFilterInput);
        let detail = app.topic_detail.as_ref().unwrap();
        assert_eq!(detail.applied_filter.as_deref(), Some("existing"));
        assert!(!detail.filter_active);
        assert!(detail.filter_input.is_empty());
    }

    #[test]
    fn clear_filter_removes_applied_filter() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            detail.applied_filter = Some("existing".to_string());
        }
        app.update(Action::ClearFilter);
        let detail = app.topic_detail.as_ref().unwrap();
        assert!(detail.applied_filter.is_none());
    }

    #[test]
    fn visible_messages_filters_by_key_or_value_case_insensitively() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                buffer.push(message("order-1", "{\"status\":\"SHIPPED\"}"));
                buffer.push(message("order-2", "{\"status\":\"pending\"}"));
            }
            detail.applied_filter = Some("shipped".to_string());
        }
        let detail = app.topic_detail.as_ref().unwrap();
        let visible = detail.visible_messages();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].key.as_deref(), Some("order-1".as_bytes()));
    }

    #[test]
    fn topic_detail_selection_clamps_against_filtered_not_raw_count() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                buffer.push(message("a", "apple"));
                buffer.push(message("b", "banana"));
                buffer.push(message("c", "apricot"));
            }
            detail.applied_filter = Some("ap".to_string());
        }
        // Only "apple" and "apricot" contain "ap" - 2 of the 3 raw messages are visible.
        let visible_len = app.topic_detail.as_ref().unwrap().visible_messages().len();
        assert_eq!(visible_len, 2);
        app.update(Action::MoveSelectionDown);
        app.update(Action::MoveSelectionDown);
        app.update(Action::MoveSelectionDown);
        app.update(Action::MoveSelectionDown);
        let detail = app.topic_detail.as_ref().unwrap();
        assert_eq!(detail.selected_index, visible_len - 1);
    }

    #[test]
    fn back_on_topic_detail_returns_to_topic_list_and_stops_tail() {
        let mut app = app_in_topic_detail("orders", 1);
        app.screen = Screen::TopicDetail;
        let commands = app.update(Action::Back);
        assert_eq!(app.screen, Screen::TopicList);
        assert!(app.topic_detail.is_none());
        assert!(matches!(commands.as_slice(), [Command::StopTail]));
    }

    fn app_on_topic_list() -> App {
        let mut app = App::new(Config {
            profiles: vec![profile("a")],
        }, test_config_path());
        app.active_profile = Some(profile("a"));
        app.screen = Screen::TopicList;
        app.topics = vec![topic("orders")];
        app
    }

    fn sample_group_detail() -> GroupDetailState {
        GroupDetailState {
            name: "my-group".into(),
            state: "Empty".into(),
            members: vec![],
            lags: vec![PartitionLag {
                topic: "orders".into(),
                partition: 0,
                committed_offset: Some(10),
                high_watermark: 20,
                low_watermark: 0,
                lag: Some(10),
            }],
            selected_index: 0,
            has_active_members: false,
            total_lag: 10,
            reset_phase: None,
        }
    }

    #[test]
    fn open_groups_from_topic_list_loads_groups() {
        let mut app = app_on_topic_list();
        let commands = app.update(Action::OpenGroups);
        assert_eq!(app.screen, Screen::GroupList);
        assert!(matches!(commands.as_slice(), [Command::LoadGroups(_)]));
    }

    #[test]
    fn open_groups_from_other_screens_is_noop() {
        let mut app = app_in_topic_detail("orders", 1);
        let commands = app.update(Action::OpenGroups);
        assert!(commands.is_empty());
        assert_eq!(app.screen, Screen::TopicDetail);
    }

    #[test]
    fn confirm_on_group_list_loads_group_detail() {
        let mut app = app_on_topic_list();
        app.screen = Screen::GroupList;
        app.groups = vec![GroupSummary {
            name: "g1".into(),
            state: "Empty".into(),
            member_count: 0,
            protocol: String::new(),
            protocol_type: "consumer".into(),
        }];
        let commands = app.update(Action::Confirm);
        assert_eq!(app.screen, Screen::GroupDetail);
        match commands.as_slice() {
            [Command::LoadGroupDetail { group, .. }] => assert_eq!(group, "g1"),
            other => panic!("expected LoadGroupDetail, got {other:?}"),
        }
    }

    #[test]
    fn offset_reset_wizard_earliest_to_confirm_to_command() {
        let mut app = app_on_topic_list();
        app.screen = Screen::GroupDetail;
        app.group_detail = Some(sample_group_detail());

        app.update(Action::StartOffsetReset);
        assert!(matches!(
            app.group_detail.as_ref().unwrap().reset_phase,
            Some(OffsetResetPhase::ChooseMode)
        ));

        app.update(Action::OffsetResetChooseEarliest);
        assert!(matches!(
            app.group_detail.as_ref().unwrap().reset_phase,
            Some(OffsetResetPhase::Confirm {
                target: OffsetResetTarget::Earliest
            })
        ));

        let commands = app.update(Action::ConfirmOffsetReset);
        match commands.as_slice() {
            [Command::ResetGroupOffsets {
                group,
                target: OffsetResetTarget::Earliest,
                partitions,
                ..
            }] => {
                assert_eq!(group, "my-group");
                assert_eq!(partitions, &vec![("orders".into(), 0)]);
            }
            other => panic!("expected ResetGroupOffsets, got {other:?}"),
        }
        assert!(app.group_detail.as_ref().unwrap().reset_phase.is_none());
    }

    #[test]
    fn offset_reset_absolute_input_path() {
        let mut app = app_on_topic_list();
        app.screen = Screen::GroupDetail;
        app.group_detail = Some(sample_group_detail());

        app.update(Action::StartOffsetReset);
        app.update(Action::OffsetResetChooseAbsolute);
        app.update(Action::FilterChar('4'));
        app.update(Action::FilterChar('2'));
        app.update(Action::Confirm);
        assert!(matches!(
            app.group_detail.as_ref().unwrap().reset_phase,
            Some(OffsetResetPhase::Confirm {
                target: OffsetResetTarget::Absolute(42)
            })
        ));
    }

    #[test]
    fn cancel_offset_reset_clears_wizard() {
        let mut app = app_on_topic_list();
        app.screen = Screen::GroupDetail;
        app.group_detail = Some(sample_group_detail());
        app.update(Action::StartOffsetReset);
        app.update(Action::CancelOffsetReset);
        assert!(app.group_detail.as_ref().unwrap().reset_phase.is_none());
    }

    #[test]
    fn back_cancels_wizard_before_leaving_screen() {
        let mut app = app_on_topic_list();
        app.screen = Screen::GroupDetail;
        app.group_detail = Some(sample_group_detail());
        app.update(Action::StartOffsetReset);
        app.update(Action::Back);
        assert_eq!(app.screen, Screen::GroupDetail);
        assert!(app.group_detail.as_ref().unwrap().reset_phase.is_none());
        app.update(Action::Back);
        assert_eq!(app.screen, Screen::GroupList);
    }

    #[test]
    fn parse_reset_input_validates() {
        assert_eq!(
            parse_reset_input(ResetInputKind::AbsoluteOffset, "10").unwrap(),
            OffsetResetTarget::Absolute(10)
        );
        assert!(parse_reset_input(ResetInputKind::AbsoluteOffset, "-1").is_err());
        assert!(parse_reset_input(ResetInputKind::AbsoluteOffset, "").is_err());
        assert_eq!(
            parse_reset_input(ResetInputKind::TimestampMillis, "1700000000000").unwrap(),
            OffsetResetTarget::Timestamp(1700000000000)
        );
    }

    #[test]
    fn replay_selected_message_produces_raw_bytes_same_topic() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                let mut msg = message("k1", "v1");
                msg.partition = 2;
                msg.offset = 99;
                msg.headers = vec![("orig".into(), b"h".to_vec())];
                buffer.push(msg);
            }
        }
        app.update(Action::RequestReplay);
        assert!(matches!(
            app.topic_detail.as_ref().unwrap().replay_phase,
            Some(ReplayPhase::Confirm { .. })
        ));
        let commands = app.update(Action::ConfirmReplay);
        match commands.as_slice() {
            [Command::ProduceMessage {
                topic,
                key,
                value,
                headers,
                ..
            }] => {
                assert_eq!(topic, "orders");
                assert_eq!(key.as_deref(), Some(b"k1".as_slice()));
                assert_eq!(value.as_deref(), Some(b"v1".as_slice()));
                assert_eq!(headers, &vec![("orig".into(), b"h".to_vec())]);
            }
            other => panic!("expected ProduceMessage, got {other:?}"),
        }
        assert!(app.topic_detail.as_ref().unwrap().replay_phase.is_none());
    }

    #[test]
    fn replay_edit_opens_producer_with_decoded_fields() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                let mut msg = message("k1", r#"{"a":1}"#);
                msg.headers = vec![("x".into(), b"1".to_vec())];
                buffer.push(msg);
            }
        }
        app.update(Action::RequestReplay);
        let commands = app.update(Action::ReplayEdit);
        assert_eq!(app.screen, Screen::Producer);
        assert!(app.topic_detail.as_ref().unwrap().replay_phase.is_none());
        let state = app.producer.as_ref().unwrap();
        assert_eq!(state.topic, "orders");
        assert_eq!(state.key_input, "k1");
        assert!(state.value_input.contains("\"a\""));
        assert_eq!(state.focus, ProducerFocus::Value);
        assert!(matches!(commands.as_slice(), [Command::StopTail]));
        assert!(app
            .status_message
            .as_ref()
            .is_some_and(|s| s.contains("edit mode") && s.contains("header")));
    }

    #[test]
    fn cancel_replay_clears_wizard() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                buffer.push(message("k", "v"));
            }
        }
        app.update(Action::RequestReplay);
        app.update(Action::CancelReplay);
        assert!(app.topic_detail.as_ref().unwrap().replay_phase.is_none());
    }

    fn app_on_producer(topic: &str) -> App {
        let mut app = app_in_topic_detail(topic, 1);
        let commands = app.update(Action::OpenProducer);
        assert!(matches!(commands.as_slice(), [Command::StopTail]));
        assert_eq!(app.screen, Screen::Producer);
        app
    }

    #[test]
    fn open_producer_from_topic_detail_stops_tail() {
        let mut app = app_in_topic_detail("orders", 1);
        let commands = app.update(Action::OpenProducer);
        assert_eq!(app.screen, Screen::Producer);
        assert_eq!(app.producer.as_ref().unwrap().topic, "orders");
        assert!(matches!(commands.as_slice(), [Command::StopTail]));
    }

    #[test]
    fn open_producer_from_other_screens_is_noop() {
        let mut app = app_on_topic_list();
        let commands = app.update(Action::OpenProducer);
        assert!(commands.is_empty());
        assert_eq!(app.screen, Screen::TopicList);
        assert!(app.producer.is_none());
    }

    #[test]
    fn producer_toggle_mode_cycles_and_normalizes_focus() {
        let mut app = app_on_producer("orders");
        {
            let state = app.producer.as_mut().unwrap();
            state.focus = ProducerFocus::Value;
        }
        app.update(Action::ProducerToggleMode);
        let state = app.producer.as_ref().unwrap();
        assert_eq!(state.mode, ProducerInputMode::FilePath);
        // Value focus is invalid in FilePath mode — snaps to Key.
        assert_eq!(state.focus, ProducerFocus::Key);

        app.update(Action::ProducerToggleMode);
        assert_eq!(
            app.producer.as_ref().unwrap().mode,
            ProducerInputMode::ExternalEditor
        );
        app.update(Action::ProducerToggleMode);
        assert_eq!(app.producer.as_ref().unwrap().mode, ProducerInputMode::Inline);
    }

    #[test]
    fn producer_focus_next_cycles_key_and_value_in_inline_mode() {
        let mut app = app_on_producer("orders");
        assert_eq!(app.producer.as_ref().unwrap().focus, ProducerFocus::Key);
        app.update(Action::ProducerFocusNext);
        assert_eq!(app.producer.as_ref().unwrap().focus, ProducerFocus::Value);
        app.update(Action::ProducerFocusNext);
        assert_eq!(app.producer.as_ref().unwrap().focus, ProducerFocus::Key);
    }

    #[test]
    fn producer_char_backspace_newline_edit_fields() {
        let mut app = app_on_producer("orders");
        app.update(Action::ProducerChar('a'));
        app.update(Action::ProducerChar('b'));
        app.update(Action::ProducerBackspace);
        assert_eq!(app.producer.as_ref().unwrap().key_input, "a");

        // Key accepts newlines (multi-line key pane).
        app.update(Action::ProducerNewline);
        app.update(Action::ProducerChar('c'));
        assert_eq!(app.producer.as_ref().unwrap().key_input, "a\nc");

        app.update(Action::ProducerFocusNext);
        app.update(Action::ProducerChar('x'));
        app.update(Action::ProducerNewline);
        app.update(Action::ProducerChar('y'));
        assert_eq!(app.producer.as_ref().unwrap().value_input, "x\ny");
    }

    #[test]
    fn producer_submit_returns_produce_command_with_bytes() {
        let mut app = app_on_producer("orders");
        app.update(Action::ProducerChar('k'));
        app.update(Action::ProducerFocusNext);
        app.update(Action::ProducerChar('v'));
        let commands = app.update(Action::ProducerSubmit);
        match commands.as_slice() {
            [Command::ProduceMessage {
                topic,
                key,
                value,
                headers,
                ..
            }] => {
                assert_eq!(topic, "orders");
                assert_eq!(key.as_deref(), Some(b"k".as_slice()));
                assert_eq!(value.as_deref(), Some(b"v".as_slice()));
                assert!(headers.is_empty());
            }
            other => panic!("expected ProduceMessage, got {other:?}"),
        }
    }

    #[test]
    fn producer_submit_empty_key_and_value_are_null() {
        let mut app = app_on_producer("orders");
        let commands = app.update(Action::ProducerSubmit);
        match commands.as_slice() {
            [Command::ProduceMessage { key, value, .. }] => {
                assert!(key.is_none());
                assert!(value.is_none());
            }
            other => panic!("expected ProduceMessage, got {other:?}"),
        }
    }

    #[test]
    fn producer_load_file_emits_command_in_file_path_mode() {
        let mut app = app_on_producer("orders");
        app.update(Action::ProducerToggleMode); // FilePath
        app.update(Action::ProducerFocusNext); // FilePath field
        for c in "/tmp/msg.json".chars() {
            app.update(Action::ProducerChar(c));
        }
        let commands = app.update(Action::ProducerLoadFile);
        match commands.as_slice() {
            [Command::LoadFileIntoProducer { path }] => assert_eq!(path, "/tmp/msg.json"),
            other => panic!("expected LoadFileIntoProducer, got {other:?}"),
        }
    }

    #[test]
    fn producer_load_file_noop_outside_file_path_mode() {
        let mut app = app_on_producer("orders");
        let commands = app.update(Action::ProducerLoadFile);
        assert!(commands.is_empty());
    }

    #[test]
    fn producer_open_external_editor_emits_command() {
        let mut app = app_on_producer("orders");
        app.update(Action::ProducerToggleMode); // FilePath
        app.update(Action::ProducerToggleMode); // ExternalEditor
        app.update(Action::ProducerFocusNext); // still Key
        // Seed a value then open editor with that initial content.
        if let Some(state) = app.producer.as_mut() {
            state.value_input = "seed".into();
        }
        let commands = app.update(Action::ProducerOpenExternalEditor);
        match commands.as_slice() {
            [Command::RunExternalEditor { initial }] => assert_eq!(initial, "seed"),
            other => panic!("expected RunExternalEditor, got {other:?}"),
        }
    }

    #[test]
    fn back_from_producer_returns_to_topic_detail_and_restarts_tail() {
        let mut app = app_on_producer("orders");
        let commands = app.update(Action::Back);
        assert_eq!(app.screen, Screen::TopicDetail);
        assert!(app.producer.is_none());
        assert!(app.topic_detail.is_some());
        match commands.as_slice() {
            [Command::StartTail { topic, .. }] => assert_eq!(topic, "orders"),
            other => panic!("expected StartTail, got {other:?}"),
        }
    }

    #[test]
    fn back_from_producer_in_seek_mode_does_not_start_tail() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            detail.mode = BrowseMode::Seek(SeekState {
                partition: 0,
                messages: vec![],
                page_start_offset: 0,
                at_beginning: true,
                at_end: true,
                low_watermark: 0,
                high_watermark: 1000,
            });
        }
        app.update(Action::OpenProducer);
        let commands = app.update(Action::Back);
        assert_eq!(app.screen, Screen::TopicDetail);
        assert!(commands.is_empty());
    }

    #[test]
    fn apply_event_file_loaded_updates_value() {
        let mut app = app_on_producer("orders");
        app.apply_event(AppEvent::FileLoaded {
            content: "from-file".into(),
        });
        assert_eq!(app.producer.as_ref().unwrap().value_input, "from-file");
        assert!(app.status_message.is_some());
    }

    #[test]
    fn apply_event_external_editor_done_updates_value() {
        let mut app = app_on_producer("orders");
        app.apply_event(AppEvent::ExternalEditorDone {
            content: "from-editor".into(),
        });
        assert_eq!(app.producer.as_ref().unwrap().value_input, "from-editor");
    }

    #[test]
    fn apply_event_produce_succeeded_sets_status() {
        let mut app = app_on_producer("orders");
        app.apply_event(AppEvent::ProduceSucceeded);
        assert_eq!(app.status_message.as_deref(), Some("message produced"));
    }

    #[test]
    fn open_export_exports_selected_message_only() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                let mut m0 = message("k0", "v0");
                m0.offset = 10;
                let mut m1 = message("k1", "v1");
                m1.offset = 11;
                buffer.push(m0);
                buffer.push(m1);
            }
            // Newest-first: index 0 is offset 11.
            detail.selected_index = 0;
        }
        let commands = app.update(Action::OpenExport);
        assert_eq!(app.screen, Screen::ExportImport);
        let state = app.export_import.as_ref().unwrap();
        assert_eq!(state.mode, ExportImportMode::Export);
        assert_eq!(state.messages.len(), 1);
        assert_eq!(state.messages[0].offset, 11);
        assert_eq!(state.path_input, "orders-p0-o11.jsonl");
        assert!(matches!(commands.as_slice(), [Command::StopTail]));
    }

    #[test]
    fn open_export_all_exports_visible_messages() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                buffer.push(message("k0", "v0"));
                buffer.push(message("k1", "v1"));
            }
        }
        let commands = app.update(Action::OpenExportAll);
        assert_eq!(app.screen, Screen::ExportImport);
        let state = app.export_import.as_ref().unwrap();
        assert_eq!(state.messages.len(), 2);
        assert_eq!(state.path_input, "orders.jsonl");
        assert!(matches!(commands.as_slice(), [Command::StopTail]));
    }

    #[test]
    fn open_export_from_inspector_uses_viewed_message() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                let mut m0 = message("k0", "v0");
                m0.offset = 5;
                let mut m1 = message("k1", "v1");
                m1.offset = 6;
                buffer.push(m0);
                buffer.push(m1);
            }
            detail.selected_index = 0; // newest = offset 6
        }
        app.update(Action::Confirm); // open inspector on selected
        // Move list selection away — export should still use the inspector snapshot.
        app.topic_detail.as_mut().unwrap().selected_index = 1;
        app.update(Action::OpenExport);
        let state = app.export_import.as_ref().unwrap();
        assert_eq!(state.messages.len(), 1);
        assert_eq!(state.messages[0].offset, 6);
    }

    #[test]
    fn export_submit_emits_export_command() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                buffer.push(message("k", "v"));
            }
        }
        app.update(Action::OpenExport);
        let commands = app.update(Action::ExportImportSubmit);
        match commands.as_slice() {
            [Command::ExportMessages { path, messages }] => {
                assert_eq!(path, "orders-p0-o0.jsonl");
                assert_eq!(messages.len(), 1);
            }
            other => panic!("expected ExportMessages, got {other:?}"),
        }
    }

    #[test]
    fn export_path_cursor_left_right_insert_and_backspace() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                buffer.push(message("k", "v"));
            }
        }
        app.update(Action::OpenExport);
        {
            let state = app.export_import.as_mut().unwrap();
            state.path_input = "ab.jsonl".into();
            state.cursor = 2; // after "ab"
        }
        app.update(Action::ExportImportCursorLeft);
        app.update(Action::ExportImportChar('X'));
        assert_eq!(app.export_import.as_ref().unwrap().path_input, "aXb.jsonl");
        app.update(Action::ExportImportCursorRight);
        app.update(Action::ExportImportBackspace);
        assert_eq!(app.export_import.as_ref().unwrap().path_input, "aX.jsonl");
        app.update(Action::ExportImportCursorHome);
        assert_eq!(app.export_import.as_ref().unwrap().cursor, 0);
        app.update(Action::ExportImportCursorEnd);
        assert_eq!(
            app.export_import.as_ref().unwrap().cursor,
            "aX.jsonl".chars().count()
        );
    }

    #[test]
    fn open_import_and_submit() {
        let mut app = app_in_topic_detail("orders", 1);
        app.update(Action::OpenImport);
        assert_eq!(app.screen, Screen::ExportImport);
        {
            let state = app.export_import.as_mut().unwrap();
            state.path_input = "/tmp/in.jsonl".into();
            state.target_topic = "other".into();
        }
        let commands = app.update(Action::ExportImportSubmit);
        match commands.as_slice() {
            [Command::ImportMessages {
                path,
                target_topic,
                ..
            }] => {
                assert_eq!(path, "/tmp/in.jsonl");
                assert_eq!(target_topic, "other");
            }
            other => panic!("expected ImportMessages, got {other:?}"),
        }
    }

    fn confluent_avro_bytes(schema_id: u32, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0x00];
        bytes.extend_from_slice(&schema_id.to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn avro_message_queues_schema_fetch_when_registry_configured() {
        let mut app = app_in_topic_detail("orders", 1);
        let mut profile = profile("a");
        profile.schema_registry_url = Some("http://localhost:8081".into());
        app.attach_profile(profile);
        assert!(app.schema_registry.is_some());

        let mut msg = message("k", "v");
        msg.value = Some(confluent_avro_bytes(42, b"not-real-avro-payload"));

        let commands = app.apply_event(AppEvent::MessageArrived {
            topic: "orders".into(),
            partition: 0,
            message: msg,
        });
        match commands.as_slice() {
            [Command::FetchSchema {
                registry_url,
                schema_id,
            }] => {
                assert_eq!(registry_url, "http://localhost:8081");
                assert_eq!(*schema_id, 42);
            }
            other => panic!("expected FetchSchema, got {other:?}"),
        }
        // Second arrival of same id must not re-fetch while inflight.
        let mut msg2 = message("k", "v");
        msg2.value = Some(confluent_avro_bytes(42, b"x"));
        let again = app.apply_event(AppEvent::MessageArrived {
            topic: "orders".into(),
            partition: 0,
            message: msg2,
        });
        assert!(again.is_empty());
    }

    #[test]
    fn avro_message_without_registry_does_not_fetch() {
        let mut app = app_in_topic_detail("orders", 1);
        assert!(app.schema_registry.is_none());
        let mut msg = message("k", "v");
        msg.value = Some(confluent_avro_bytes(7, b"x"));
        let commands = app.apply_event(AppEvent::MessageArrived {
            topic: "orders".into(),
            partition: 0,
            message: msg,
        });
        assert!(commands.is_empty());
    }

    #[test]
    fn schema_loaded_inserts_into_cache() {
        let mut app = app_in_topic_detail("orders", 1);
        let mut profile = profile("a");
        profile.schema_registry_url = Some("http://localhost:8081".into());
        app.attach_profile(profile);
        app.schema_fetch_inflight.insert(9);

        let schema = apache_avro::Schema::parse_str(
            r#"{"type":"record","name":"T","fields":[{"name":"n","type":"string"}]}"#,
        )
        .unwrap();
        app.apply_event(AppEvent::SchemaLoaded {
            schema_id: 9,
            schema,
        });
        assert!(!app.schema_fetch_inflight.contains(&9));
        assert!(app
            .schema_registry
            .as_ref()
            .unwrap()
            .cached_schema(9)
            .is_some());
    }

    #[test]
    fn refresh_on_topic_list_reloads_topics() {
        let mut app = app_on_topic_list();
        app.topic_list_selected_index = 0;
        let commands = app.update(Action::Refresh);
        assert!(matches!(commands.as_slice(), [Command::LoadTopics(_)]));
    }

    #[test]
    fn default_sort_is_newest_first() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                let mut m0 = message("old", "v0");
                m0.offset = 10;
                let mut m1 = message("new", "v1");
                m1.offset = 11;
                buffer.push(m0);
                buffer.push(m1);
            }
        }
        let visible = app
            .topic_detail
            .as_ref()
            .unwrap()
            .visible_messages();
        assert_eq!(visible[0].offset, 11);
        assert_eq!(visible[1].offset, 10);
        assert_eq!(
            String::from_utf8_lossy(visible[0].key.as_deref().unwrap()),
            "new"
        );
    }

    #[test]
    fn toggle_sort_preserves_selected_message() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                let mut m0 = message("a", "1");
                m0.offset = 1;
                let mut m1 = message("b", "2");
                m1.offset = 2;
                let mut m2 = message("c", "3");
                m2.offset = 3;
                buffer.push(m0);
                buffer.push(m1);
                buffer.push(m2);
            }
            // Newest-first list: offsets 3,2,1 — select middle (offset 2).
            detail.selected_index = 1;
        }
        app.update(Action::ToggleMessageSort);
        let detail = app.topic_detail.as_ref().unwrap();
        assert_eq!(detail.sort, MessageSort::OldestFirst);
        // Oldest-first: 1,2,3 — same message (offset 2) is still selected.
        assert_eq!(detail.selected_index, 1);
        let visible = detail.visible_messages();
        assert_eq!(visible[detail.selected_index].offset, 2);
        assert_eq!(app.status_message.as_deref(), Some("sort: oldest↑"));
    }

    #[test]
    fn seek_page_loaded_stores_watermarks() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            detail.mode = BrowseMode::Seek(seek_state(vec![], 0, false, false));
        }
        let mut m = message("k", "v");
        m.offset = 50;
        app.apply_event(AppEvent::SeekPageLoaded {
            topic: "orders".into(),
            messages: vec![m],
            meta: crate::events::SeekPageMeta {
                partition: 0,
                page_start_offset: 50,
                at_beginning: false,
                at_end: true,
                low_watermark: 0,
                high_watermark: 51,
            },
        });
        let BrowseMode::Seek(state) = &app.topic_detail.as_ref().unwrap().mode else {
            panic!("expected seek mode");
        };
        assert_eq!(state.low_watermark, 0);
        assert_eq!(state.high_watermark, 51);
        assert_eq!(state.page_start_offset, 50);
        assert_eq!(state.messages.len(), 1);
    }

    #[test]
    fn enter_opens_message_inspector_for_selected_row() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                buffer.push(message("k1", r#"{"a":1}"#));
                buffer.push(message("k2", r#"{"b":2}"#));
            }
            // Newest-first: index 0 is k2 (last pushed), index 1 is k1.
            detail.selected_index = 0;
        }
        let commands = app.update(Action::Confirm);
        assert!(commands.is_empty());
        let view = app
            .topic_detail
            .as_ref()
            .and_then(|d| d.message_view.as_ref())
            .expect("message_view should open");
        assert_eq!(
            String::from_utf8_lossy(view.message.key.as_deref().unwrap()),
            "k2"
        );
        assert_eq!(view.scroll, 0);
    }

    #[test]
    fn enter_again_and_esc_close_message_inspector() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                buffer.push(message("k", "v"));
            }
        }
        app.update(Action::Confirm);
        assert!(app.topic_detail.as_ref().unwrap().message_view.is_some());

        app.update(Action::Confirm);
        assert!(app.topic_detail.as_ref().unwrap().message_view.is_none());

        app.update(Action::Confirm);
        assert!(app.topic_detail.as_ref().unwrap().message_view.is_some());
        app.update(Action::Back);
        assert!(app.topic_detail.as_ref().unwrap().message_view.is_none());
        // Still on topic detail — Esc only closed the inspector.
        assert_eq!(app.screen, Screen::TopicDetail);
    }

    #[test]
    fn j_k_scroll_message_inspector_instead_of_list() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            if let BrowseMode::Tail(buffer) = &mut detail.mode {
                buffer.push(message("k1", "v1"));
                buffer.push(message("k2", "v2"));
            }
            detail.selected_index = 0;
        }
        app.update(Action::Confirm);
        app.update(Action::MoveSelectionDown);
        app.update(Action::MoveSelectionDown);
        let detail = app.topic_detail.as_ref().unwrap();
        assert_eq!(detail.selected_index, 0, "list selection stays put");
        assert_eq!(detail.message_view.as_ref().unwrap().scroll, 2);
        app.update(Action::MoveSelectionUp);
        assert_eq!(
            app.topic_detail
                .as_ref()
                .unwrap()
                .message_view
                .as_ref()
                .unwrap()
                .scroll,
            1
        );
    }

    #[test]
    fn enter_with_no_messages_sets_status() {
        let mut app = app_in_topic_detail("orders", 1);
        app.update(Action::Confirm);
        assert!(app.topic_detail.as_ref().unwrap().message_view.is_none());
        assert_eq!(app.status_message.as_deref(), Some("no message selected"));
    }

    #[test]
    fn refresh_on_seek_reloads_current_page() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            detail.mode = BrowseMode::Seek(SeekState {
                partition: 0,
                messages: vec![message("k1", "v1"), message("k2", "v2")],
                page_start_offset: 10,
                at_beginning: false,
                at_end: false,
                low_watermark: 0,
                high_watermark: 1000,
            });
        }
        let commands = app.update(Action::Refresh);
        match commands.as_slice() {
            [Command::LoadSeekPage {
                topic,
                partition,
                request: SeekPageRequest::Forward { from_offset, page_size },
                ..
            }] => {
                assert_eq!(topic, "orders");
                assert_eq!(*partition, 0);
                assert_eq!(*from_offset, 10);
                assert_eq!(*page_size, SEEK_PAGE_SIZE);
            }
            other => panic!("expected LoadSeekPage Forward from page_start, got {other:?}"),
        }
        assert_eq!(app.status_message.as_deref(), Some("refreshing page..."));
    }

    #[test]
    fn refresh_on_tail_is_a_no_op_with_hint() {
        let mut app = app_in_topic_detail("orders", 1);
        assert!(matches!(
            app.topic_detail.as_ref().unwrap().mode,
            BrowseMode::Tail(_)
        ));
        let commands = app.update(Action::Refresh);
        assert!(commands.is_empty());
        assert!(app
            .status_message
            .as_ref()
            .is_some_and(|m| m.contains("tail is live")));
    }

    #[test]
    fn refresh_on_seek_skips_while_filter_active() {
        let mut app = app_in_topic_detail("orders", 1);
        if let Some(detail) = app.topic_detail.as_mut() {
            detail.mode = BrowseMode::Seek(SeekState {
                partition: 0,
                messages: vec![message("k", "v")],
                page_start_offset: 0,
                at_beginning: true,
                at_end: false,
                low_watermark: 0,
                high_watermark: 1000,
            });
            detail.filter_active = true;
        }
        let commands = app.update(Action::Refresh);
        assert!(commands.is_empty());
    }

    #[test]
    fn refresh_on_group_list_reloads_groups() {
        let mut app = app_on_topic_list();
        app.screen = Screen::GroupList;
        let commands = app.update(Action::Refresh);
        assert!(matches!(commands.as_slice(), [Command::LoadGroups(_)]));
    }

    #[test]
    fn refresh_on_group_detail_reloads_group() {
        let mut app = app_on_topic_list();
        app.screen = Screen::GroupDetail;
        app.group_detail = Some(sample_group_detail());
        let commands = app.update(Action::Refresh);
        match commands.as_slice() {
            [Command::LoadGroupDetail { group, .. }] => assert_eq!(group, "my-group"),
            other => panic!("expected LoadGroupDetail, got {other:?}"),
        }
    }

    #[test]
    fn auto_refresh_skips_during_offset_reset_wizard() {
        let mut app = app_on_topic_list();
        app.screen = Screen::GroupDetail;
        app.group_detail = Some(sample_group_detail());
        app.group_detail.as_mut().unwrap().reset_phase = Some(OffsetResetPhase::ChooseMode);
        let commands = app.update(Action::AutoRefreshGroupDetail);
        assert!(commands.is_empty());
    }

    #[test]
    fn auto_refresh_on_idle_group_detail() {
        let mut app = app_on_topic_list();
        app.screen = Screen::GroupDetail;
        app.group_detail = Some(sample_group_detail());
        let commands = app.update(Action::AutoRefreshGroupDetail);
        assert!(matches!(
            commands.as_slice(),
            [Command::LoadGroupDetail { .. }]
        ));
    }

    #[test]
    fn topics_loaded_preserves_selection() {
        let mut app = app_on_topic_list();
        app.topics = vec![topic("a"), topic("b"), topic("c")];
        app.topic_list_selected_index = 2;
        app.apply_event(AppEvent::TopicsLoaded {
            topics: vec![topic("a"), topic("b")],
            auto_message_max_bytes: None,
        });
        assert_eq!(app.topic_list_selected_index, 1); // clamped
    }

    #[test]
    fn group_detail_loaded_preserves_selection_and_reset_wizard() {
        let mut app = app_on_topic_list();
        app.screen = Screen::GroupDetail;
        let mut detail = sample_group_detail();
        detail.selected_index = 0;
        detail.reset_phase = Some(OffsetResetPhase::ChooseMode);
        app.group_detail = Some(detail);

        let loaded = crate::kafka::group_offsets::GroupDetail {
            name: "my-group".into(),
            state: "Empty".into(),
            members: vec![],
            lags: sample_group_detail().lags,
        };
        app.apply_event(AppEvent::GroupDetailLoaded(loaded));
        let g = app.group_detail.as_ref().unwrap();
        assert!(matches!(g.reset_phase, Some(OffsetResetPhase::ChooseMode)));
        assert_eq!(g.selected_index, 0);
    }
}
