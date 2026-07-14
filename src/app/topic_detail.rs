//! Message browser: tail/seek browsing modes, filter, sort, the message inspector, and
//! the single-message replay wizard.

use std::cell::RefCell;
use std::collections::BTreeSet;

use serde_json::Value;

use super::producer::{ProducerFocus, ProducerState};
use super::{App, Screen, TAIL_BUFFER_CAPACITY};
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

/// Which panel of the message inspector j/k/PgUp/PgDn scroll. `Attrs` (topic/
/// partition/offset/timestamp/formats) isn't here — it's fixed, deterministic
/// content that never needs scrolling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InspectorFocus {
    Key,
    Headers,
    Value,
}

impl InspectorFocus {
    /// **Tab** cycles Key → Headers → Value → Key.
    pub fn next(self) -> Self {
        match self {
            InspectorFocus::Key => InspectorFocus::Headers,
            InspectorFocus::Headers => InspectorFocus::Value,
            InspectorFocus::Value => InspectorFocus::Key,
        }
    }
}

/// Full-message inspector opened with Enter on the message list.
/// Snapshots the record so a live tail refresh doesn't swap content under the cursor.
#[derive(Debug, Clone)]
pub struct MessageViewState {
    pub message: RawMessage,
    /// Which panel **Tab** / a click currently directs j/k/PgUp/PgDn to.
    pub focus: InspectorFocus,
    /// Vertical line offset into the rendered key panel.
    pub key_scroll: usize,
    /// Vertical line offset into the rendered headers panel.
    pub headers_scroll: usize,
    /// Vertical line offset into the rendered value panel.
    pub value_scroll: usize,
    /// Memoized decoded+pretty-printed key/value bodies — see `decoded_bodies`.
    decoded_cache: RefCell<Option<DecodedBodyCache>>,
}

/// Cap full-body display so a multi-MB payload can't freeze the terminal redraw.
const INSPECTOR_MAX_BODY_CHARS: usize = 200_000;

#[derive(Debug, Clone)]
struct DecodedBodyCache {
    schema_cache_len: usize,
    key_body: String,
    value_body: String,
}

impl MessageViewState {
    pub fn new(message: RawMessage) -> Self {
        Self {
            message,
            focus: InspectorFocus::Key,
            key_scroll: 0,
            headers_scroll: 0,
            value_scroll: 0,
            decoded_cache: RefCell::new(None),
        }
    }

    /// Decoded+pretty-printed (but not yet width-wrapped) key/value bodies, capped at
    /// `INSPECTOR_MAX_BODY_CHARS`. Decoding a large Avro/JSON message (schema lookup,
    /// deserialize, `serde_json::to_string_pretty`) is expensive, and this screen
    /// redraws on every banner tick (~200ms) even at idle — recomputing it on every
    /// render was the same "decode-on-every-frame" cost 0.9.1/0.9.2 fixed for the
    /// list-row preview, just not applied to the single-message inspector. Cached
    /// bodies are invalidated when the schema-registry cache grows (a schema this
    /// message was waiting on may have just loaded, changing the decoded output),
    /// not on every call.
    pub fn decoded_bodies(&self, registry: Option<&SchemaRegistry>) -> (String, String) {
        let schema_cache_len = registry.map_or(0, SchemaRegistry::cache_len);
        let stale = self
            .decoded_cache
            .borrow()
            .as_ref()
            .is_none_or(|c| c.schema_cache_len != schema_cache_len);
        if stale {
            let key_body = capped_body(bytes_to_display_text(self.message.key.as_deref(), registry));
            let value_body = capped_body(bytes_to_display_text(self.message.value.as_deref(), registry));
            *self.decoded_cache.borrow_mut() =
                Some(DecodedBodyCache { schema_cache_len, key_body, value_body });
        }
        let cache = self.decoded_cache.borrow();
        let cache = cache.as_ref().expect("just populated above");
        (cache.key_body.clone(), cache.value_body.clone())
    }
}

pub(crate) fn capped_body(body: String) -> String {
    if body.chars().count() > INSPECTOR_MAX_BODY_CHARS {
        let truncated: String = body.chars().take(INSPECTOR_MAX_BODY_CHARS).collect();
        format!("{truncated}\n\n… (truncated for display)")
    } else {
        body
    }
}

/// Full decoded body for the inspector: pretty-print JSON when possible.
fn bytes_to_display_text(bytes: Option<&[u8]>, registry: Option<&SchemaRegistry>) -> String {
    let Some(bytes) = bytes else {
        return "<null>".into();
    };
    if bytes.is_empty() {
        return "<empty>".into();
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

/// Tab-completion state for the query-filter dialog: a cycle through candidate root
/// words or path segments (e.g. `events`/`house`/`tags` after typing `value.`),
/// sourced from whatever's actually present on the currently-visible page. Persists
/// across repeated Tab presses so each one advances to the next candidate; any other
/// edit to the query text invalidates it (see the `text_*` helpers in `app/mod.rs`).
pub struct QueryFilterCompletion {
    /// Input text before the token being completed.
    pub prefix: String,
    /// Sorted, deduped candidate values for the token (root words or path segments).
    pub candidates: Vec<String>,
    pub index: usize,
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
    /// Advanced structured filter (`key.a.b = "x" AND value.c != 5`) — separate from
    /// the plain substring filter above; when both are applied they AND-combine.
    pub query_filter_input: String,
    /// Cursor into `query_filter_input` while `query_filter_active`.
    pub query_filter_cursor: usize,
    pub query_filter_active: bool,
    /// Toggled by Ctrl-h while the query-filter dialog is open — shows syntax/examples.
    pub query_filter_help_visible: bool,
    /// Set by `Action::QueryFilterAutocomplete` (Tab) when there's more than one
    /// candidate at the cursor; `None` otherwise (including "no dialog open").
    pub query_filter_completion: Option<QueryFilterCompletion>,
    pub applied_query_filter: Option<crate::query_filter::QueryFilter>,
    pub replay_phase: Option<ReplayPhase>,
    pub message_view: Option<MessageViewState>,
    /// Percent width the **Attrs** panel takes in the inspector's top row (Headers
    /// gets the rest). Adjustable with ←/→ while Headers is focused; persists across
    /// different messages opened while browsing this topic (unlike per-message
    /// `MessageViewState` fields, which reset every time the inspector reopens).
    pub inspector_top_split: u16,
    /// Percent width the **Key** panel takes in the inspector's bottom row (Value
    /// gets the rest). Adjustable with ←/→ while Key or Value is focused.
    pub inspector_bottom_split: u16,
    /// Newest-first by default so the latest messages appear at the top of the list.
    pub sort: MessageSort,
    /// Bumped at every site that can change what `visible_messages_with_registry`
    /// returns (new tail message, seek page reload, filter/query-filter apply or
    /// clear, mode switch) — invalidates `visible_cache` below. Sort is deliberately
    /// excluded: the cache stores filter-matched indices in storage order and the
    /// newest-first reversal is applied on every call regardless of cache freshness,
    /// so toggling sort doesn't need to bust it.
    pub visible_revision: u64,
    /// Memoized filter result — see `visible_messages_with_registry`.
    pub(super) visible_cache: RefCell<Option<VisibleCache>>,
}

/// Cached output of the filter pass in `visible_messages_with_registry`: which
/// positions (into the mode's current message list, in storage/oldest-first order)
/// passed the filter. Indices rather than `&RawMessage`s, since the latter would
/// self-borrow from the state that owns this cache.
pub(super) struct VisibleCache {
    revision: u64,
    schema_cache_len: usize,
    indices: Vec<usize>,
}

impl TopicDetailState {
    /// Messages currently on screen (no registry-aware decoding). Used by unit tests.
    #[cfg(test)]
    pub fn visible_messages(&self) -> Vec<&RawMessage> {
        self.visible_messages_with_registry(None)
    }

    /// Visible messages for the list / export / replay selection, decoding Avro via the
    /// schema-registry cache when filtering so field-level search works after schemas load.
    /// The substring filter and the advanced query filter are independent — when both are
    /// applied, a message must satisfy both.
    ///
    /// Filtering can fully decode every message (Avro/JSON) to test the predicate —
    /// recomputing on every render (the banner tick redraws every ~200ms even at
    /// idle) would repeat that decode continuously while a filter is applied on a
    /// full tail buffer. The filtered-index result is cached, keyed by
    /// `visible_revision` (bumped wherever content/filters actually change) and the
    /// schema-registry cache size (grows monotonically as schemas load, which can
    /// change decode results without any of those actions firing).
    pub fn visible_messages_with_registry(
        &self,
        registry: Option<&SchemaRegistry>,
    ) -> Vec<&RawMessage> {
        let all: Vec<&RawMessage> = match &self.mode {
            BrowseMode::Tail(buffer) => buffer.iter().collect(),
            BrowseMode::Seek(state) => state.messages.iter().collect(),
        };

        let schema_cache_len = registry.map_or(0, SchemaRegistry::cache_len);
        let stale = self.visible_cache.borrow().as_ref().is_none_or(|c| {
            c.revision != self.visible_revision || c.schema_cache_len != schema_cache_len
        });
        if stale {
            let needle = self.applied_filter.as_ref().map(|f| f.to_lowercase());
            let indices: Vec<usize> = all
                .iter()
                .enumerate()
                .filter(|(_, message)| {
                    needle
                        .as_deref()
                        .is_none_or(|n| message_matches_filter(message, n, registry))
                        && self
                            .applied_query_filter
                            .as_ref()
                            .is_none_or(|q| query_filter_matches(message, q, registry))
                })
                .map(|(i, _)| i)
                .collect();
            *self.visible_cache.borrow_mut() =
                Some(VisibleCache { revision: self.visible_revision, schema_cache_len, indices });
        }

        let mut msgs: Vec<&RawMessage> = self
            .visible_cache
            .borrow()
            .as_ref()
            .expect("just populated above")
            .indices
            .iter()
            .map(|&i| all[i])
            .collect();
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

/// Structured-query match: decodes key/value to `serde_json::Value` (same Avro-cache
/// boundary as the substring filter) and evaluates the parsed query against them.
fn query_filter_matches(
    message: &RawMessage,
    query: &crate::query_filter::QueryFilter,
    registry: Option<&SchemaRegistry>,
) -> bool {
    let key = message
        .key
        .as_deref()
        .and_then(|k| crate::serde_detect::decode_json_value(k, registry));
    let value = message
        .value
        .as_deref()
        .and_then(|v| crate::serde_detect::decode_json_value(v, registry));
    query.matches(key.as_ref(), value.as_ref())
}

/// Child field names reachable from `value` at `path`, transparently fanning out over
/// arrays exactly like `query_filter::path_matches` does when *evaluating* a path —
/// so what's offered here always matches what would actually work if typed. `path`
/// empty at an object yields that object's own keys (the completion candidates);
/// arrays never consume a path segment; leaves have no children.
fn candidate_children(value: &Value, path: &[String]) -> BTreeSet<String> {
    match value {
        Value::Array(items) => {
            let mut out = BTreeSet::new();
            for item in items {
                out.extend(candidate_children(item, path));
            }
            out
        }
        Value::Object(map) => match path.split_first() {
            Some((head, rest)) => map
                .get(head)
                .map(|next| candidate_children(next, rest))
                .unwrap_or_default(),
            None => map.keys().cloned().collect(),
        },
        _leaf => BTreeSet::new(),
    }
}

/// What a trailing path-like token (see `trailing_path_token`) is asking to complete.
enum CompletionTarget {
    /// Still typing the root word itself (`val` → `key`/`value`).
    Root,
    /// Completing one path segment under `root`, `path_so_far` deep, with `partial`
    /// typed so far for the segment being completed (may be empty, e.g. right after
    /// a trailing `.`).
    Path {
        root: &'static str,
        path_so_far: Vec<String>,
        partial: String,
    },
}

/// The path-like token at the very end of `input` (letters/digits/underscore/dot) —
/// e.g. `"value.hou"` out of `"key.x = 1 AND value.hou"`. Completion only ever looks
/// at the end of the input (see `Action::QueryFilterAutocomplete`'s cursor-at-end
/// check), so this doesn't need to know about the cursor at all.
fn trailing_path_token(input: &str) -> &str {
    let is_path_char = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '.';
    match input.char_indices().rev().find(|(_, c)| !is_path_char(*c)) {
        Some((i, c)) => &input[i + c.len_utf8()..],
        None => input,
    }
}

/// Splits a trailing token into what it's completing. `None` means "not a key/value
/// path at all" (an empty token, or a root word that isn't a prefix of `key`/`value`)
/// — Tab is a no-op in that case.
fn classify_token(token: &str) -> Option<CompletionTarget> {
    if token.is_empty() {
        return None;
    }
    match token.split_once('.') {
        None => Some(CompletionTarget::Root),
        Some((root, rest)) => {
            let root = match root {
                "key" => "key",
                "value" => "value",
                _ => return None,
            };
            let mut segments: Vec<String> = rest.split('.').map(str::to_string).collect();
            let partial = segments.pop().unwrap_or_default();
            Some(CompletionTarget::Path { root, path_so_far: segments, partial })
        }
    }
}

impl App {
    /// Tab in the query-filter dialog. Continues an in-flight cycle (see
    /// `QueryFilterCompletion`) if the input still matches what was last inserted, or
    /// starts a fresh one from the trailing path token. A single unambiguous candidate
    /// is applied immediately with no cycle armed; zero candidates is a no-op.
    pub(super) fn query_filter_autocomplete(&mut self) {
        let registry = self.schema_registry.as_ref();
        let Some(detail) = self.topic_detail.as_mut() else { return };
        if !detail.query_filter_active {
            return;
        }
        // Only complete while typing at the end — mid-string completion would need to
        // reason about the token around the cursor, not just the tail of the input.
        if detail.query_filter_cursor != detail.query_filter_input.chars().count() {
            return;
        }

        if let Some(completion) = &detail.query_filter_completion {
            let expected = format!("{}{}", completion.prefix, completion.candidates[completion.index]);
            if detail.query_filter_input == expected {
                let next = (completion.index + 1) % completion.candidates.len();
                detail.query_filter_input =
                    format!("{}{}", completion.prefix, completion.candidates[next]);
                detail.query_filter_cursor = detail.query_filter_input.chars().count();
                detail.query_filter_completion.as_mut().unwrap().index = next;
                return;
            }
        }

        let token = trailing_path_token(&detail.query_filter_input);
        let Some(target) = classify_token(token) else {
            detail.query_filter_completion = None;
            return;
        };
        // Text before the whole trailing token — NOT necessarily what gets kept: for a
        // path target, only the last (partial) segment is replaced, so "value.house."
        // stays and only "o" in "value.house.o" is swapped out.
        let token_start = detail.query_filter_input.len() - token.len();
        let outer_prefix = &detail.query_filter_input[..token_start];

        let (prefix, candidates): (String, Vec<String>) = match &target {
            CompletionTarget::Root => {
                let candidates = ["key", "value"]
                    .into_iter()
                    .filter(|r| r.starts_with(token))
                    .map(str::to_string)
                    .collect();
                (outer_prefix.to_string(), candidates)
            }
            CompletionTarget::Path { root, path_so_far, partial } => {
                let mut set = BTreeSet::new();
                for message in detail.visible_messages_with_registry(registry) {
                    let bytes = if *root == "key" { message.key.as_deref() } else { message.value.as_deref() };
                    let Some(decoded) =
                        bytes.and_then(|b| crate::serde_detect::decode_json_value(b, registry))
                    else {
                        continue;
                    };
                    set.extend(candidate_children(&decoded, path_so_far));
                }
                let candidates =
                    set.into_iter().filter(|c| c.starts_with(partial.as_str())).collect();
                let mut prefix = format!("{outer_prefix}{root}.");
                for segment in path_so_far {
                    prefix.push_str(segment);
                    prefix.push('.');
                }
                (prefix, candidates)
            }
        };

        match candidates.len() {
            0 => detail.query_filter_completion = None,
            1 => {
                detail.query_filter_input = format!("{prefix}{}", candidates[0]);
                detail.query_filter_cursor = detail.query_filter_input.chars().count();
                detail.query_filter_completion = None;
            }
            _ => {
                detail.query_filter_input = format!("{prefix}{}", candidates[0]);
                detail.query_filter_cursor = detail.query_filter_input.chars().count();
                detail.query_filter_completion = Some(QueryFilterCompletion { prefix, candidates, index: 0 });
            }
        }
    }
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
        detail.message_view = Some(MessageViewState::new(message));
        vec![]
    }

    /// Scroll the open message inspector's focused panel. Returns true if the action
    /// was consumed (so list navigation / page seek should not also run).
    pub(super) fn scroll_message_view(&mut self, delta: i64) -> bool {
        let Some(detail) = self.topic_detail.as_mut() else {
            return false;
        };
        let Some(view) = detail.message_view.as_mut() else {
            return false;
        };
        let scroll = match view.focus {
            InspectorFocus::Key => &mut view.key_scroll,
            InspectorFocus::Headers => &mut view.headers_scroll,
            InspectorFocus::Value => &mut view.value_scroll,
        };
        if delta < 0 {
            *scroll = scroll.saturating_sub((-delta) as usize);
        } else {
            *scroll = scroll.saturating_add(delta as usize);
        }
        true
    }

    /// **Tab** while the inspector is open: cycles which panel j/k/PgUp/PgDn scroll.
    pub(super) fn toggle_inspector_focus(&mut self) {
        if let Some(view) = self.topic_detail.as_mut().and_then(|d| d.message_view.as_mut()) {
            view.focus = view.focus.next();
        }
    }

    /// A click on a panel: focuses it directly (as opposed to Tab's toggle).
    pub(super) fn set_inspector_focus(&mut self, focus: InspectorFocus) {
        if let Some(view) = self.topic_detail.as_mut().and_then(|d| d.message_view.as_mut()) {
            view.focus = focus;
        }
    }

    /// **←/→** while the inspector is open: grows (`grow: true`) or shrinks the
    /// focused panel's share of its row, at its row-mate's expense. Attrs↔Headers is
    /// the top row; Key↔Value is the bottom row — which one moves depends on which
    /// panel is focused (Attrs itself is never focused, so Headers always means the
    /// top row here).
    pub(super) fn resize_inspector_panel(&mut self, grow: bool) {
        const STEP: i16 = 5;
        const MIN: i16 = 10;
        const MAX: i16 = 90;
        let Some(detail) = self.topic_detail.as_mut() else { return };
        let Some(focus) = detail.message_view.as_ref().map(|v| v.focus) else { return };
        // sign: +1 if growing this panel means growing the row's *left* share
        // (the field we store), -1 if it means shrinking it.
        let (split, sign): (&mut u16, i16) = match focus {
            InspectorFocus::Headers => (&mut detail.inspector_top_split, -1),
            InspectorFocus::Key => (&mut detail.inspector_bottom_split, 1),
            InspectorFocus::Value => (&mut detail.inspector_bottom_split, -1),
        };
        let delta = if grow { STEP } else { -STEP } * sign;
        *split = (*split as i16 + delta).clamp(MIN, MAX) as u16;
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
                        page_size: self.seek_page_size(),
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
        let page_size = self.seek_page_size();
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
                detail.visible_revision = detail.visible_revision.wrapping_add(1);
                vec![
                    Command::StopTail,
                    Command::LoadSeekPage {
                        profile,
                        topic: detail.topic.clone(),
                        partition,
                        request: SeekPageRequest::Latest { page_size },
                    },
                ]
            }
            BrowseMode::Seek(_) => {
                detail.mode = BrowseMode::Tail(RingBuffer::new(TAIL_BUFFER_CAPACITY));
                detail.selected_index = 0;
                detail.visible_revision = detail.visible_revision.wrapping_add(1);
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
                    request: SeekPageRequest::Forward { from_offset, page_size: self.seek_page_size() },
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
                        page_size: self.seek_page_size(),
                    },
                }]
            }
        }
    }
}
