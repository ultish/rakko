use crate::config::{Config, Profile};
use crate::events::{Action, AppEvent, Command, SeekPageRequest};
use crate::kafka::admin::TopicSummary;
use crate::raw_message::RawMessage;
use crate::ring_buffer::RingBuffer;

const TAIL_BUFFER_CAPACITY: usize = 500;
const SEEK_PAGE_SIZE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    ProfilePicker,
    TopicList,
    TopicDetail,
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
}

pub struct TopicDetailState {
    pub topic: String,
    pub partition_count: usize,
    pub mode: BrowseMode,
    pub selected_index: usize,
    pub filter_input: String,
    pub filter_active: bool,
    pub applied_filter: Option<String>,
}

impl TopicDetailState {
    /// Messages currently on screen: the active mode's full set, minus anything the
    /// applied filter excludes. A pure function over in-memory state — no I/O, cheap to
    /// unit test without a broker.
    pub fn visible_messages(&self) -> Vec<&RawMessage> {
        let all: Box<dyn Iterator<Item = &RawMessage> + '_> = match &self.mode {
            BrowseMode::Tail(buffer) => Box::new(buffer.iter()),
            BrowseMode::Seek(state) => Box::new(state.messages.iter()),
        };
        match &self.applied_filter {
            None => all.collect(),
            Some(filter) => {
                let needle = filter.to_lowercase();
                all.filter(|message| message_matches_filter(message, &needle)).collect()
            }
        }
    }
}

/// Case-insensitive substring match against the key and value, decoded as UTF-8 lossily.
/// Deliberately simple for M2 — structured JSON/Avro field-level filtering lands in M6
/// alongside `serde_detect`, per PLAN.md.
fn message_matches_filter(message: &RawMessage, needle_lowercase: &str) -> bool {
    let key_matches = message
        .key
        .as_deref()
        .map(|bytes| String::from_utf8_lossy(bytes).to_lowercase().contains(needle_lowercase))
        .unwrap_or(false);
    let value_matches = message
        .value
        .as_deref()
        .map(|bytes| String::from_utf8_lossy(bytes).to_lowercase().contains(needle_lowercase))
        .unwrap_or(false);
    key_matches || value_matches
}

enum PageDirection {
    Forward,
    Backward,
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
    /// Transient status text: connect errors, load errors, "loading..." etc.
    pub status_message: Option<String>,
    pub should_quit: bool,
    pub active_profile: Option<Profile>,
}

impl App {
    pub fn new(config: Config) -> Self {
        Self {
            screen: Screen::ProfilePicker,
            config,
            selected_profile_index: 0,
            topics: Vec::new(),
            topic_list_selected_index: 0,
            topic_detail: None,
            status_message: None,
            should_quit: false,
            active_profile: None,
        }
    }

    /// Wires up `--profile <name>`: if the name matches a configured profile, starts
    /// directly on `TopicList` with it active. Construction stays synchronous (no I/O
    /// here) — the caller in main.rs is responsible for spawning the actual topic-load
    /// task when it sees `screen == TopicList` right after this returns.
    pub fn new_with_profile(config: Config, profile_name: Option<&str>) -> Self {
        let mut app = Self::new(config);
        if let Some(name) = profile_name {
            if let Some(profile) = app.config.find_profile(name).cloned() {
                app.active_profile = Some(profile);
                app.screen = Screen::TopicList;
            }
        }
        app
    }

    /// Elm-style reducer. Stays synchronous and non-blocking; any background I/O the
    /// action implies is communicated back via the returned `Command`s rather than
    /// performed here.
    pub fn update(&mut self, action: Action) -> Vec<Command> {
        match action {
            Action::Quit => {
                self.should_quit = true;
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
            Action::ToggleBrowseMode => self.toggle_browse_mode(),
            Action::PageForward => self.request_seek_page(PageDirection::Forward),
            Action::PageBackward => self.request_seek_page(PageDirection::Backward),
            Action::StartFilterInput => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    detail.filter_input = detail.applied_filter.clone().unwrap_or_default();
                    detail.filter_active = true;
                }
                vec![]
            }
            Action::FilterChar(c) => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    if detail.filter_active {
                        detail.filter_input.push(c);
                    }
                }
                vec![]
            }
            Action::FilterBackspace => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    if detail.filter_active {
                        detail.filter_input.pop();
                    }
                }
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
        }
    }

    fn move_selection(&mut self, delta: i64) {
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
                if let Some(detail) = &mut self.topic_detail {
                    let len = detail.visible_messages().len();
                    Self::clamp_index(&mut detail.selected_index, len, delta);
                }
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
        match self.screen {
            Screen::ProfilePicker => {
                let Some(profile) = self.config.profiles.get(self.selected_profile_index).cloned()
                else {
                    // Nothing to select on an empty profile list: safe no-op.
                    return vec![];
                };
                self.active_profile = Some(profile.clone());
                self.screen = Screen::TopicList;
                self.topics.clear();
                self.topic_list_selected_index = 0;
                self.status_message = Some("loading topics...".to_string());
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
                    filter_active: false,
                    applied_filter: None,
                });
                self.screen = Screen::TopicDetail;
                vec![Command::StartTail { profile, topic: topic.name }]
            }
            Screen::TopicDetail => vec![],
        }
    }

    fn back(&mut self) -> Vec<Command> {
        match self.screen {
            Screen::ProfilePicker => vec![],
            Screen::TopicList => {
                self.screen = Screen::ProfilePicker;
                self.status_message = None;
                vec![]
            }
            Screen::TopicDetail => {
                self.screen = Screen::TopicList;
                self.topic_detail = None;
                // Always safe to emit: main.rs's tail-task abort is a no-op if seek mode
                // (no continuous task) was active when Back was pressed.
                vec![Command::StopTail]
            }
        }
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

    pub fn apply_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::TopicsLoaded(topics) => {
                self.topics = topics;
                self.topic_list_selected_index = 0;
                self.status_message = None;
            }
            AppEvent::TopicsLoadFailed(message) => {
                self.status_message = Some(format!("failed to load topics: {message}"));
            }
            AppEvent::ConnectFailed(message) => {
                self.status_message = Some(format!("connect failed: {message}"));
            }
            AppEvent::MessageArrived { topic, partition: _, message } => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    if detail.topic == topic {
                        if let BrowseMode::Tail(buffer) = &mut detail.mode {
                            buffer.push(message);
                        }
                        // else: stale arrival from a just-aborted tail task racing the
                        // switch to seek mode - silently dropped, per PLAN.md's staleness
                        // handling for BrowseMode transitions.
                    }
                }
            }
            AppEvent::SeekPageLoaded { topic, messages, meta } => {
                if let Some(detail) = self.topic_detail.as_mut() {
                    if detail.topic == topic {
                        if let BrowseMode::Seek(state) = &mut detail.mode {
                            if state.partition == meta.partition {
                                state.messages = messages;
                                state.page_start_offset = meta.page_start_offset;
                                state.at_beginning = meta.at_beginning;
                                state.at_end = meta.at_end;
                                detail.selected_index = 0;
                            }
                        }
                    }
                }
            }
            AppEvent::BrowseFailed(message) => {
                self.status_message = Some(format!("browse error: {message}"));
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
    fn quit_sets_should_quit_flag() {
        let mut app = App::new(Config::default());
        assert!(!app.should_quit);
        app.update(Action::Quit);
        assert!(app.should_quit);
    }

    #[test]
    fn selection_clamps_at_zero_on_empty_profile_list() {
        let mut app = App::new(Config::default());
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
        let mut app = App::new(config);

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
        let mut app = App::new(Config::default());
        app.screen = Screen::TopicList;
        app.topics = vec![topic("t1"), topic("t2"), topic("t3")];
        app.update(Action::MoveSelectionDown);
        app.update(Action::MoveSelectionDown);
        app.update(Action::MoveSelectionDown);
        assert_eq!(app.topic_list_selected_index, 2);
    }

    #[test]
    fn confirm_on_empty_profile_list_is_a_safe_no_op() {
        let mut app = App::new(Config::default());
        let commands = app.update(Action::Confirm);
        assert!(commands.is_empty());
        assert_eq!(app.screen, Screen::ProfilePicker);
        assert!(app.active_profile.is_none());
    }

    #[test]
    fn confirm_on_profile_picker_transitions_to_topic_list_and_returns_load_command() {
        let config = Config {
            profiles: vec![profile("a")],
        };
        let mut app = App::new(config);
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
        let mut app = App::new(Config::default());
        app.screen = Screen::TopicList;
        app.update(Action::Back);
        assert_eq!(app.screen, Screen::ProfilePicker);
    }

    #[test]
    fn apply_event_topics_loaded_populates_topics_and_clears_status() {
        let mut app = App::new(Config::default());
        app.status_message = Some("loading...".into());
        app.apply_event(AppEvent::TopicsLoaded(vec![topic("t1")]));
        assert_eq!(app.topics.len(), 1);
        assert!(app.status_message.is_none());
    }

    #[test]
    fn apply_event_load_failed_sets_status_without_panicking() {
        let mut app = App::new(Config::default());
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
        });
        app.active_profile = Some(profile("a"));
        app.screen = Screen::TopicDetail;
        app.topic_detail = Some(TopicDetailState {
            topic: topic_name.to_string(),
            partition_count,
            mode: BrowseMode::Tail(RingBuffer::new(TAIL_BUFFER_CAPACITY)),
            selected_index: 0,
            filter_input: String::new(),
            filter_active: false,
            applied_filter: None,
        });
        app
    }

    #[test]
    fn confirm_on_topic_list_enters_topic_detail_in_tail_mode_and_starts_tail() {
        let config = Config {
            profiles: vec![profile("a")],
        };
        let mut app = App::new(config);
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
}
