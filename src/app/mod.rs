//! Elm-style `App` struct + `Screen` enum + `Action`/`AppEvent` reducer.
//!
//! Per-screen state and its handler methods live in submodules (mirroring
//! `ui/screens/`); this file keeps the cross-cutting dispatchers (`update`,
//! `confirm`, `back`, `apply_event`) that route into them, plus construction and
//! the handful of genuinely shared helpers (schema-fetch dedup, the filter /
//! offset-reset-input text editing shared between two screens).

mod broker_detail;
mod export_import;
mod group_detail;
mod producer;
mod profile_create;
mod topic_detail;

#[cfg(test)]
mod tests;

pub use broker_detail::BrokerDetailState;
pub use export_import::{ExportImportFocus, ExportImportMode, ExportImportState};
pub use group_detail::{GroupDetailState, OffsetResetPhase, ResetInputKind};
pub use producer::{ProducerFocus, ProducerInputMode, ProducerState};
pub use profile_create::{ProfileCreateAuthChoice, ProfileCreateFocus, ProfileCreateState};
pub use topic_detail::{
    BrowseMode, InspectorFocus, MessageSort, MessageViewState, ReplayPhase, TopicDetailState,
};
pub(crate) use topic_detail::capped_body;

use export_import::ExportScope;
use group_detail::parse_reset_input;
use topic_detail::PageDirection;

use std::cell::{Cell, RefCell};
use std::collections::HashSet;

use std::path::PathBuf;

use crate::config::{self, Config, Profile};
use crate::events::{Action, AppEvent, Command};
use crate::kafka::admin::{BrokerSummary, ClusterHealth, TopicSummary};
use crate::kafka::group_offsets::{GroupSummary, OffsetResetTarget};
use crate::kafka::schema_registry::SchemaRegistry;
use crate::ring_buffer::RingBuffer;
use crate::serde_detect::{detect_format, DetectedFormat};

const TAIL_BUFFER_CAPACITY: usize = 500;
/// Fallback for `Config::seek_page_size` when not set in `config.toml`.
pub const DEFAULT_SEEK_PAGE_SIZE: usize = 50;
/// How many recent per-frame FPS samples the banner's FPS graph/readout keeps —
/// also the graph's effective time window at typical interaction rates.
const FPS_SAMPLE_CAPACITY: usize = 30;

/// Cycles the top banner's animated content (`A` key): the decorative wave, a
/// live FPS graph + numeric readout (see `App::push_fps_sample`), or off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BannerMode {
    Wave,
    Fps,
    Off,
}

impl BannerMode {
    fn next(self) -> Self {
        match self {
            BannerMode::Wave => BannerMode::Fps,
            BannerMode::Fps => BannerMode::Off,
            BannerMode::Off => BannerMode::Wave,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            BannerMode::Wave => "wave",
            BannerMode::Fps => "fps",
            BannerMode::Off => "off",
        }
    }
}

/// A clickable region of the last-rendered frame, in terminal cell coordinates.
/// `ui::draw` clears these at the start of every frame; screens re-register them
/// while rendering (kept geometry-only here — plain `u16`s, not `ratatui::Rect` —
/// so `App` stays ratatui-agnostic; the `ui` layer does the `Rect` conversion).
pub struct ClickRegion {
    pub x: u16,
    pub y: u16,
    pub width: u16,
    pub height: u16,
    pub action: Action,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    ProfilePicker,
    TopicList,
    TopicDetail,
    GroupList,
    GroupDetail,
    BrokerList,
    BrokerDetail,
    Producer,
    ExportImport,
}

pub struct App {
    pub screen: Screen,
    pub config: Config,
    /// Selection cursor for the profile picker.
    pub selected_profile_index: usize,
    pub topics: Vec<TopicSummary>,
    /// Selection cursor for the topic list. Indexes into `visible_topics()`, not
    /// `topics` directly, so it stays valid while a filter is applied.
    pub topic_list_selected_index: usize,
    pub topic_list_filter_input: String,
    /// Cursor into `topic_list_filter_input` while `topic_list_filter_active`.
    pub topic_list_filter_cursor: usize,
    pub topic_list_filter_active: bool,
    pub topic_list_applied_filter: Option<String>,
    pub topic_detail: Option<TopicDetailState>,
    pub groups: Vec<GroupSummary>,
    /// Selection cursor for the group list. Indexes into `visible_groups()`, not
    /// `groups` directly, so it stays valid while a filter is applied.
    pub group_list_selected_index: usize,
    pub group_list_filter_input: String,
    /// Cursor into `group_list_filter_input` while `group_list_filter_active`.
    pub group_list_filter_cursor: usize,
    pub group_list_filter_active: bool,
    pub group_list_applied_filter: Option<String>,
    pub group_detail: Option<GroupDetailState>,
    pub brokers: Vec<BrokerSummary>,
    pub broker_list_selected_index: usize,
    pub cluster_health: ClusterHealth,
    pub broker_detail: Option<BrokerDetailState>,
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
    /// Index into `config.profiles` pending deletion (`z` on the profile picker) —
    /// a centered confirm dialog is open; y/Enter deletes, n/Esc cancels.
    pub profile_delete_confirm: Option<usize>,
    /// Transient status text: connect errors, load errors, "loading..." etc.
    pub status_message: Option<String>,
    pub should_quit: bool,
    /// When true, a centered "quit?" dialog is open (`q`); y/Enter exits, n/Esc cancels.
    pub quit_confirm: bool,
    pub active_profile: Option<Profile>,
    /// Braille stream banner animation frame index.
    pub banner_frame: usize,
    /// Cycles wave → fps → off (`A`). `Off` freezes `banner_frame` and stops the
    /// periodic tick redraw.
    pub banner_mode: BannerMode,
    /// Recent per-frame FPS samples (oldest first), for the banner's FPS graph +
    /// numeric readout. Raw `Instant`s are deliberately kept out of `App` — see
    /// `last_row_click` in main.rs — so `main.rs` computes each sample from real
    /// timestamps and hands in only the derived `f64` via `push_fps_sample`.
    pub fps_samples: RingBuffer<f64>,
    /// Full-screen detailed otter splash — shown once at startup until dismissed.
    pub show_splash: bool,
    /// Clickable regions registered by the most recent `ui::draw` call. Interior
    /// mutability so screen `render(frame, app, area)` functions (which only get
    /// `&App`) can register as they go, without becoming `&mut App`.
    click_regions: RefCell<Vec<ClickRegion>>,
    /// Last-known mouse cell position (crossterm reports `Moved` events whenever
    /// mouse capture is on, even with no button held — see `EnableMouseCapture` in
    /// main.rs). `None` until the first mouse event arrives. Used for hover
    /// highlighting only; unrelated to `click_regions`, which is action targets.
    mouse_pos: Cell<Option<(u16, u16)>>,
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
            topic_list_filter_input: String::new(),
            topic_list_filter_cursor: 0,
            topic_list_filter_active: false,
            topic_list_applied_filter: None,
            topic_detail: None,
            groups: Vec::new(),
            group_list_selected_index: 0,
            group_list_filter_input: String::new(),
            group_list_filter_cursor: 0,
            group_list_filter_active: false,
            group_list_applied_filter: None,
            group_detail: None,
            brokers: Vec::new(),
            broker_list_selected_index: 0,
            cluster_health: ClusterHealth::default(),
            broker_detail: None,
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
            profile_delete_confirm: None,
            status_message: None,
            should_quit: false,
            quit_confirm: false,
            active_profile: None,
            banner_frame: 0,
            banner_mode: BannerMode::Wave,
            fps_samples: RingBuffer::new(FPS_SAMPLE_CAPACITY),
            show_splash: true,
            click_regions: RefCell::new(Vec::new()),
            mouse_pos: Cell::new(None),
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

    /// Seek-mode page size: `Config::seek_page_size` if set, else `DEFAULT_SEEK_PAGE_SIZE`.
    fn seek_page_size(&self) -> usize {
        self.config.seek_page_size.unwrap_or(DEFAULT_SEEK_PAGE_SIZE)
    }

    /// Topics for the list / selection, filtered by name when a filter is applied.
    /// `topic_list_selected_index` indexes into this, not `topics` directly.
    pub fn visible_topics(&self) -> Vec<&TopicSummary> {
        match &self.topic_list_applied_filter {
            None => self.topics.iter().collect(),
            Some(filter) => {
                let needle = filter.to_lowercase();
                self.topics
                    .iter()
                    .filter(|t| t.name.to_lowercase().contains(&needle))
                    .collect()
            }
        }
    }

    /// Groups for the list / selection, filtered by name when a filter is applied.
    /// `group_list_selected_index` indexes into this, not `groups` directly.
    pub fn visible_groups(&self) -> Vec<&GroupSummary> {
        match &self.group_list_applied_filter {
            None => self.groups.iter().collect(),
            Some(filter) => {
                let needle = filter.to_lowercase();
                self.groups
                    .iter()
                    .filter(|g| g.name.to_lowercase().contains(&needle))
                    .collect()
            }
        }
    }

    /// Discards last frame's clickable regions; called once at the top of `ui::draw`
    /// before screens re-register theirs.
    pub fn clear_click_regions(&self) {
        self.click_regions.borrow_mut().clear();
    }

    /// Records one render's instantaneous FPS (`1 / render_duration_secs`, timed
    /// around the `terminal.draw` call itself — not the gap between draws, which
    /// at idle just measures the banner tick's 200ms cadence rather than actual
    /// render performance) for the banner's FPS graph/readout. `main.rs` computes
    /// this from real `Instant`s (which don't cross into `App`) and calls this
    /// once per actual `terminal.draw`, regardless of `banner_mode` — so flipping
    /// to FPS mode always shows real recent history, not just samples collected
    /// since you switched to it.
    pub fn push_fps_sample(&mut self, fps: f64) {
        self.fps_samples.push(fps);
    }

    /// Registers a clickable region for the frame currently being drawn.
    pub fn register_click(&self, x: u16, y: u16, width: u16, height: u16, action: Action) {
        self.click_regions
            .borrow_mut()
            .push(ClickRegion { x, y, width, height, action });
    }

    /// The action for the topmost region containing `(x, y)`, if any — later
    /// registrations (e.g. a dialog rendered over the screen behind it) win.
    pub fn action_at(&self, x: u16, y: u16) -> Option<Action> {
        self.click_regions
            .borrow()
            .iter()
            .rev()
            .find(|r| x >= r.x && x < r.x + r.width && y >= r.y && y < r.y + r.height)
            .map(|r| r.action.clone())
    }

    /// Records the mouse's current cell position — called on every mouse event,
    /// independent of `click_regions`/actions, purely so rendering can decide what
    /// to draw as hovered.
    pub fn set_mouse_pos(&self, x: u16, y: u16) {
        self.mouse_pos.set(Some((x, y)));
    }

    /// Whether the last-known mouse position falls inside the given rect.
    pub fn is_hovered(&self, x: u16, y: u16, width: u16, height: u16) -> bool {
        self.mouse_pos
            .get()
            .is_some_and(|(mx, my)| mx >= x && mx < x + width && my >= y && my < y + height)
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
            Action::SelectRow(index) => {
                self.set_selection(index);
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
            Action::ToggleInspectorFocus => {
                self.toggle_inspector_focus();
                vec![]
            }
            Action::SetInspectorFocus(focus) => {
                self.set_inspector_focus(focus);
                vec![]
            }
            Action::ShrinkInspectorPanel => {
                self.resize_inspector_panel(false);
                vec![]
            }
            Action::GrowInspectorPanel => {
                self.resize_inspector_panel(true);
                vec![]
            }
            Action::PageForward => {
                if self.scroll_message_view(10) {
                    return vec![];
                }
                if self.scroll_producer_preview(10) {
                    return vec![];
                }
                self.request_seek_page(PageDirection::Forward)
            }
            Action::PageBackward => {
                if self.scroll_message_view(-10) {
                    return vec![];
                }
                if self.scroll_producer_preview(-10) {
                    return vec![];
                }
                self.request_seek_page(PageDirection::Backward)
            }
            Action::StartFilterInput => {
                match self.screen {
                    Screen::TopicDetail => {
                        if let Some(detail) = self.topic_detail.as_mut() {
                            if detail.message_view.is_some() {
                                return vec![];
                            }
                            detail.filter_input = detail.applied_filter.clone().unwrap_or_default();
                            detail.filter_cursor = detail.filter_input.chars().count();
                            detail.filter_active = true;
                        }
                    }
                    Screen::TopicList => {
                        self.topic_list_filter_input =
                            self.topic_list_applied_filter.clone().unwrap_or_default();
                        self.topic_list_filter_cursor = self.topic_list_filter_input.chars().count();
                        self.topic_list_filter_active = true;
                    }
                    Screen::GroupList => {
                        self.group_list_filter_input =
                            self.group_list_applied_filter.clone().unwrap_or_default();
                        self.group_list_filter_cursor = self.group_list_filter_input.chars().count();
                        self.group_list_filter_active = true;
                    }
                    _ => {}
                }
                vec![]
            }
            Action::StartQueryFilterInput => {
                if self.screen == Screen::TopicDetail {
                    if let Some(detail) = self.topic_detail.as_mut() {
                        if detail.message_view.is_none() {
                            detail.query_filter_input = detail
                                .applied_query_filter
                                .as_ref()
                                .map(|q| q.raw.clone())
                                .unwrap_or_default();
                            detail.query_filter_cursor = detail.query_filter_input.chars().count();
                            detail.query_filter_active = true;
                            detail.query_filter_help_visible = false;
                            detail.query_filter_completion = None;
                        }
                    }
                }
                vec![]
            }
            Action::ToggleQueryFilterHelp => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    if detail.query_filter_active {
                        detail.query_filter_help_visible = !detail.query_filter_help_visible;
                    }
                }
                vec![]
            }
            Action::QueryFilterAutocomplete => {
                self.query_filter_autocomplete();
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
                        detail.visible_revision = detail.visible_revision.wrapping_add(1);
                    }
                }
                if self.topic_list_filter_active {
                    self.topic_list_applied_filter = if self.topic_list_filter_input.is_empty() {
                        None
                    } else {
                        Some(self.topic_list_filter_input.clone())
                    };
                    self.topic_list_filter_active = false;
                    self.topic_list_selected_index = 0;
                }
                if self.group_list_filter_active {
                    self.group_list_applied_filter = if self.group_list_filter_input.is_empty() {
                        None
                    } else {
                        Some(self.group_list_filter_input.clone())
                    };
                    self.group_list_filter_active = false;
                    self.group_list_selected_index = 0;
                }
                vec![]
            }
            Action::ApplyQueryFilter => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    if detail.query_filter_active {
                        if detail.query_filter_input.trim().is_empty() {
                            detail.applied_query_filter = None;
                            detail.query_filter_active = false;
                            detail.selected_index = 0;
                            detail.visible_revision = detail.visible_revision.wrapping_add(1);
                        } else {
                            match crate::query_filter::parse(&detail.query_filter_input) {
                                Ok(query) => {
                                    detail.applied_query_filter = Some(query);
                                    detail.query_filter_active = false;
                                    detail.selected_index = 0;
                                    detail.visible_revision = detail.visible_revision.wrapping_add(1);
                                }
                                Err(err) => {
                                    // Stay open so the user can fix it, same pattern as
                                    // the offset-reset wizard's Input-phase parse errors.
                                    self.status_message = Some(format!("query filter error: {err}"));
                                }
                            }
                        }
                    }
                }
                vec![]
            }
            Action::CancelFilterInput => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    detail.filter_active = false;
                    detail.filter_input.clear();
                    detail.query_filter_active = false;
                    detail.query_filter_input.clear();
                }
                self.topic_list_filter_active = false;
                self.topic_list_filter_input.clear();
                self.group_list_filter_active = false;
                self.group_list_filter_input.clear();
                vec![]
            }
            Action::ClearFilter => {
                self.topic_list_applied_filter = None;
                self.topic_list_filter_input.clear();
                self.topic_list_selected_index = 0;
                self.group_list_applied_filter = None;
                self.group_list_filter_input.clear();
                self.group_list_selected_index = 0;
                if let Some(detail) = self.topic_detail.as_mut() {
                    detail.applied_filter = None;
                    detail.filter_input.clear();
                    detail.applied_query_filter = None;
                    detail.query_filter_input.clear();
                    detail.selected_index = 0;
                    detail.visible_revision = detail.visible_revision.wrapping_add(1);
                }
                vec![]
            }
            Action::SwitchToTopics => self.switch_top_level(Screen::TopicList),
            Action::SwitchToGroups => self.switch_top_level(Screen::GroupList),
            Action::SwitchToBrokers => self.switch_top_level(Screen::BrokerList),
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
            Action::ProducerFocusField(field) => {
                if let Some(state) = self.producer.as_mut() {
                    state.set_focus(field);
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
            Action::ProducerCursorUp => {
                if let Some(state) = self.producer.as_mut() {
                    state.cursor_up();
                }
                vec![]
            }
            Action::ProducerCursorDown => {
                if let Some(state) = self.producer.as_mut() {
                    state.cursor_down();
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
            Action::ExportImportFocusField(field) => {
                if let Some(state) = self.export_import.as_mut() {
                    state.set_focus(field);
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
            Action::StartDeleteProfile => {
                if self.screen == Screen::ProfilePicker && self.profile_create.is_none() {
                    if self.selected_profile_index < self.config.profiles.len() {
                        self.profile_delete_confirm = Some(self.selected_profile_index);
                    } else {
                        self.status_message = Some("no profile selected to delete".into());
                    }
                }
                vec![]
            }
            Action::ConfirmDeleteProfile => self.confirm_delete_profile(),
            Action::CancelDeleteProfile => {
                self.profile_delete_confirm = None;
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
            Action::ProfileCreateCycleAuth => {
                if let Some(state) = self.profile_create.as_mut() {
                    state.cycle_auth();
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
                if self.banner_mode != BannerMode::Off {
                    self.banner_frame = self.banner_frame.wrapping_add(1);
                }
                vec![]
            }
            Action::CycleBannerMode => {
                self.banner_mode = self.banner_mode.next();
                if self.banner_mode == BannerMode::Off {
                    self.banner_frame = 0;
                }
                self.status_message = Some(format!("banner: {}", self.banner_mode.label()));
                vec![]
            }
            Action::DismissSplash => {
                self.show_splash = false;
                vec![]
            }
        }
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
        // Mouse wheel over the producer's read-only value preview scrolls it.
        if self.scroll_producer_preview(delta) {
            return;
        }
        self.with_current_selection(|index, len| Self::clamp_index(index, len, delta));
    }

    /// Mouse click on a list row: jumps the current screen's selection straight to
    /// `index` (clamped), rather than nudging it by a delta.
    fn set_selection(&mut self, index: usize) {
        // Same freeze conditions as `move_selection` — a rendered row behind an
        // overlay shouldn't be clickable through it.
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
            .is_some_and(|d| d.replay_phase.is_some() || d.message_view.is_some())
        {
            return;
        }
        self.with_current_selection(|current, len| {
            *current = if len == 0 { 0 } else { index.min(len - 1) };
        });
    }

    /// Runs `f` against the current screen's selection index and its list length.
    /// Shared by `move_selection` (delta) and `set_selection` (absolute, from a
    /// mouse click) so the per-screen dispatch only lives in one place.
    fn with_current_selection(&mut self, f: impl FnOnce(&mut usize, usize)) {
        match self.screen {
            Screen::ProfilePicker => {
                f(&mut self.selected_profile_index, self.config.profiles.len());
            }
            Screen::TopicList => {
                let len = self.visible_topics().len();
                f(&mut self.topic_list_selected_index, len);
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
                    f(&mut detail.selected_index, len);
                }
            }
            Screen::GroupList => {
                let len = self.visible_groups().len();
                f(&mut self.group_list_selected_index, len);
            }
            Screen::GroupDetail => {
                if let Some(detail) = &mut self.group_detail {
                    let len = detail.lags.len();
                    f(&mut detail.selected_index, len);
                }
            }
            Screen::BrokerList => {
                let len = self.brokers.len();
                f(&mut self.broker_list_selected_index, len);
            }
            Screen::BrokerDetail => {
                if let Some(detail) = &mut self.broker_detail {
                    let len = detail.entries.len();
                    f(&mut detail.selected_index, len);
                }
            }
            Screen::Producer | Screen::ExportImport => {
                // Text-entry screens; no row selection there.
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
                let Some(topic) = self
                    .visible_topics()
                    .get(self.topic_list_selected_index)
                    .map(|t| (*t).clone())
                else {
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
                    query_filter_input: String::new(),
                    query_filter_cursor: 0,
                    query_filter_active: false,
                    query_filter_help_visible: false,
                    query_filter_completion: None,
                    applied_query_filter: None,
                    replay_phase: None,
                    message_view: None,
                    inspector_top_split: 50,
                    inspector_bottom_split: 40,
                    sort: MessageSort::default(),
                    visible_revision: 0,
                    visible_cache: RefCell::new(None),
                });
                self.screen = Screen::TopicDetail;
                vec![Command::StartTail { profile, topic: topic.name }]
            }
            Screen::TopicDetail => self.open_or_close_message_view(),
            Screen::GroupList => {
                let Some(group) = self
                    .visible_groups()
                    .get(self.group_list_selected_index)
                    .map(|g| (*g).clone())
                else {
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
            Screen::BrokerList => self.open_broker_detail(),
            Screen::BrokerDetail => vec![],
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
            Screen::BrokerList => {
                self.screen = Screen::TopicList;
                self.status_message = None;
                vec![]
            }
            Screen::BrokerDetail => {
                self.screen = Screen::BrokerList;
                self.broker_detail = None;
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
                return;
            }
            if detail.query_filter_active {
                detail.query_filter_completion = None;
                crate::text_field::insert_char(
                    &mut detail.query_filter_input,
                    &mut detail.query_filter_cursor,
                    c,
                );
                return;
            }
        }
        if self.topic_list_filter_active {
            crate::text_field::insert_char(
                &mut self.topic_list_filter_input,
                &mut self.topic_list_filter_cursor,
                c,
            );
            return;
        }
        if self.group_list_filter_active {
            crate::text_field::insert_char(
                &mut self.group_list_filter_input,
                &mut self.group_list_filter_cursor,
                c,
            );
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
                return;
            }
            if detail.query_filter_active {
                detail.query_filter_completion = None;
                crate::text_field::backspace(
                    &mut detail.query_filter_input,
                    &mut detail.query_filter_cursor,
                );
                return;
            }
        }
        if self.topic_list_filter_active {
            crate::text_field::backspace(
                &mut self.topic_list_filter_input,
                &mut self.topic_list_filter_cursor,
            );
            return;
        }
        if self.group_list_filter_active {
            crate::text_field::backspace(
                &mut self.group_list_filter_input,
                &mut self.group_list_filter_cursor,
            );
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
                return;
            }
            if detail.query_filter_active {
                detail.query_filter_completion = None;
                crate::text_field::delete_forward(
                    &mut detail.query_filter_input,
                    &mut detail.query_filter_cursor,
                );
                return;
            }
        }
        if self.topic_list_filter_active {
            crate::text_field::delete_forward(
                &mut self.topic_list_filter_input,
                &mut self.topic_list_filter_cursor,
            );
            return;
        }
        if self.group_list_filter_active {
            crate::text_field::delete_forward(
                &mut self.group_list_filter_input,
                &mut self.group_list_filter_cursor,
            );
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
                return;
            }
            if detail.query_filter_active {
                detail.query_filter_completion = None;
                crate::text_field::cursor_left(&mut detail.query_filter_cursor);
                return;
            }
        }
        if self.topic_list_filter_active {
            crate::text_field::cursor_left(&mut self.topic_list_filter_cursor);
            return;
        }
        if self.group_list_filter_active {
            crate::text_field::cursor_left(&mut self.group_list_filter_cursor);
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
                return;
            }
            if detail.query_filter_active {
                detail.query_filter_completion = None;
                crate::text_field::cursor_right(
                    &detail.query_filter_input,
                    &mut detail.query_filter_cursor,
                );
                return;
            }
        }
        if self.topic_list_filter_active {
            crate::text_field::cursor_right(
                &self.topic_list_filter_input,
                &mut self.topic_list_filter_cursor,
            );
            return;
        }
        if self.group_list_filter_active {
            crate::text_field::cursor_right(
                &self.group_list_filter_input,
                &mut self.group_list_filter_cursor,
            );
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
                return;
            }
            if detail.query_filter_active {
                detail.query_filter_completion = None;
                crate::text_field::cursor_home(&mut detail.query_filter_cursor);
                return;
            }
        }
        if self.topic_list_filter_active {
            crate::text_field::cursor_home(&mut self.topic_list_filter_cursor);
            return;
        }
        if self.group_list_filter_active {
            crate::text_field::cursor_home(&mut self.group_list_filter_cursor);
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
                return;
            }
            if detail.query_filter_active {
                detail.query_filter_completion = None;
                crate::text_field::cursor_end(
                    &detail.query_filter_input,
                    &mut detail.query_filter_cursor,
                );
                return;
            }
        }
        if self.topic_list_filter_active {
            crate::text_field::cursor_end(
                &self.topic_list_filter_input,
                &mut self.topic_list_filter_cursor,
            );
            return;
        }
        if self.group_list_filter_active {
            crate::text_field::cursor_end(
                &self.group_list_filter_input,
                &mut self.group_list_filter_cursor,
            );
        }
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
            Screen::BrokerList => {
                let Some(profile) = self.active_profile.clone() else {
                    return vec![];
                };
                if announce {
                    self.status_message = Some("refreshing brokers...".into());
                }
                vec![Command::LoadBrokers(profile)]
            }
            Screen::BrokerDetail => self.refresh_broker_detail(announce),
            _ => vec![],
        }
    }

    /// Digit-key top-level switch (M10): jumps directly between Topics/Groups/Brokers
    /// from any list-level screen, always re-issuing the load command (matches this
    /// app's "eventual consistency, no manual cache invalidation" style elsewhere).
    fn switch_top_level(&mut self, target: Screen) -> Vec<Command> {
        if self.screen == target {
            return vec![];
        }

        let mut commands = Vec::new();
        // Leaving TopicDetail while tailing needs the same StopTail as `back()` — don't
        // leave an orphaned tail task running.
        if self.screen == Screen::TopicDetail {
            if let Some(detail) = self.topic_detail.as_ref() {
                if matches!(detail.mode, BrowseMode::Tail(_)) {
                    commands.push(Command::StopTail);
                }
            }
            self.topic_detail = None;
        }
        if self.screen == Screen::GroupDetail {
            self.group_detail = None;
        }
        if self.screen == Screen::BrokerDetail {
            self.broker_detail = None;
        }

        self.screen = target;
        self.status_message = None;

        let Some(profile) = self.active_profile.clone() else {
            return commands;
        };
        match target {
            Screen::TopicList => {
                self.status_message = Some("loading topics...".into());
                commands.push(Command::LoadTopics(profile));
            }
            Screen::GroupList => {
                self.groups.clear();
                self.group_list_selected_index = 0;
                self.status_message = Some("loading consumer groups...".into());
                commands.push(Command::LoadGroups(profile));
            }
            Screen::BrokerList => {
                self.brokers.clear();
                self.broker_list_selected_index = 0;
                self.status_message = Some("loading brokers...".into());
                commands.push(Command::LoadBrokers(profile));
            }
            _ => {}
        }
        commands
    }

    /// Applies a background event. May return `FetchSchema` commands when Avro messages
    /// are seen and the schema is not yet cached.
    pub fn apply_event(&mut self, event: AppEvent) -> Vec<Command> {
        match event {
            AppEvent::TopicsLoaded {
                mut topics,
                auto_message_max_bytes,
            } => {
                topics.sort_by(|a, b| a.name.cmp(&b.name));
                self.topics = topics;
                // Preserve selection (and any applied filter) across refresh; clamp
                // against the filtered count if the visible list shrank.
                let visible_len = self.visible_topics().len();
                if visible_len == 0 {
                    self.topic_list_selected_index = 0;
                } else {
                    self.topic_list_selected_index =
                        self.topic_list_selected_index.min(visible_len - 1);
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
                            detail.visible_revision = detail.visible_revision.wrapping_add(1);
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
                                detail.visible_revision = detail.visible_revision.wrapping_add(1);
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
                // Preserve selection (and any applied filter) across refresh; clamp
                // against the filtered count if the visible list shrank.
                let visible_len = self.visible_groups().len();
                if visible_len == 0 {
                    self.group_list_selected_index = 0;
                } else {
                    self.group_list_selected_index =
                        self.group_list_selected_index.min(visible_len - 1);
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
                    // Lag history only carries over if it's the same group — switching groups
                    // starts a fresh trend rather than splicing in an unrelated group's history.
                    let (prev_selected, prev_reset, mut lag_history) = self
                        .group_detail
                        .as_ref()
                        .filter(|d| d.name == detail.name)
                        .map(|d| (d.selected_index, d.reset_phase.clone(), d.lag_history.clone()))
                        .unwrap_or((0, None, std::collections::VecDeque::new()));
                    if lag_history.len() >= group_detail::LAG_HISTORY_CAPACITY {
                        lag_history.pop_front();
                    }
                    lag_history.push_back(total_lag);
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
                        lag_history,
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
            AppEvent::BrokersLoaded { brokers, health } => {
                self.brokers = brokers;
                self.cluster_health = health;
                if self.brokers.is_empty() {
                    self.broker_list_selected_index = 0;
                } else {
                    self.broker_list_selected_index =
                        self.broker_list_selected_index.min(self.brokers.len() - 1);
                }
                self.status_message = None;
                vec![]
            }
            AppEvent::BrokersLoadFailed(message) => {
                self.status_message = Some(format!("failed to load brokers: {message}"));
                vec![]
            }
            AppEvent::BrokerConfigLoaded { broker_id, entries } => {
                if let Some(detail) = self.broker_detail.as_mut() {
                    if detail.broker_id == broker_id {
                        let entries_len = entries.len();
                        detail.entries = entries;
                        detail.selected_index = if entries_len == 0 {
                            0
                        } else {
                            detail.selected_index.min(entries_len - 1)
                        };
                        self.status_message = None;
                    }
                    // else: stale reply for a broker the user has since navigated away
                    // from — dropped, same pattern as MessageArrived's topic-tag check.
                }
                vec![]
            }
            AppEvent::BrokerConfigLoadFailed(message) => {
                self.status_message = Some(format!("failed to load broker config: {message}"));
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
                    state.value_preview_scroll = 0;
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
                    state.value_preview_scroll = 0;
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
