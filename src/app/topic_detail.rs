//! Message browser: tail/seek browsing modes, filter, sort, the message inspector, and
//! the single-message replay wizard.

use super::producer::{ProducerFocus, ProducerState};
use super::{App, Screen, SEEK_PAGE_SIZE, TAIL_BUFFER_CAPACITY};
use crate::events::{Command, SeekPageRequest};
use crate::kafka::schema_registry::SchemaRegistry;
use crate::raw_message::RawMessage;
use crate::ring_buffer::RingBuffer;

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

pub(super) enum PageDirection {
    Forward,
    Backward,
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

impl App {
    pub(super) fn request_replay(&mut self) -> Vec<Command> {
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
    pub(super) fn open_or_close_message_view(&mut self) -> Vec<Command> {
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
    pub(super) fn scroll_message_view(&mut self, delta: i64) -> bool {
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
    pub(super) fn toggle_message_sort(&mut self) {
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
    pub(super) fn open_replay_edit(&mut self) -> Vec<Command> {
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
    pub(super) fn confirm_replay(&mut self) -> Vec<Command> {
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

    /// Reloads the current seek page in place. Tail mode is already live — no I/O.
    pub(super) fn refresh_topic_detail(&mut self, announce: bool) -> Vec<Command> {
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

    pub(super) fn toggle_browse_mode(&mut self) -> Vec<Command> {
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

    pub(super) fn request_seek_page(&mut self, direction: PageDirection) -> Vec<Command> {
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
}
