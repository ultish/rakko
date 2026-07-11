use crate::kafka::admin::TopicSummary;
use crate::raw_message::RawMessage;

/// Which end of a topic/partition a seek page request is anchored to. `Forward`/`Backward`
/// page relative to the current page's edge offset; `Latest` is used the first time a
/// partition enters seek mode.
#[derive(Debug, Clone)]
pub enum SeekPageRequest {
    Latest { page_size: usize },
    Forward { from_offset: i64, page_size: usize },
    Backward { before_offset: i64, page_size: usize },
}

/// Metadata describing where a loaded seek page sits relative to the partition's
/// watermarks, so the UI can grey out "page further" keys at either edge.
#[derive(Debug, Clone)]
pub struct SeekPageMeta {
    pub partition: i32,
    /// Offset of the first message in the page (or the requested start if the page is
    /// empty), used as the anchor for the next Forward/Backward request.
    pub page_start_offset: i64,
    pub at_beginning: bool,
    pub at_end: bool,
}

/// Events reported from background tasks (Kafka I/O, HTTP calls) back to the render loop.
/// Never constructed on the render loop thread itself.
#[derive(Debug)]
pub enum AppEvent {
    TopicsLoaded(Vec<TopicSummary>),
    TopicsLoadFailed(String),
    ConnectFailed(String),
    /// One message arrived on the continuous tail-mode poll. Tagged with topic/partition
    /// so a stale event from a just-torn-down tail task (a race between abort() and the
    /// task's last in-flight send) can be recognized and dropped by the reducer instead of
    /// corrupting a newly-entered screen.
    MessageArrived {
        topic: String,
        partition: i32,
        message: RawMessage,
    },
    /// A bounded page of messages loaded for seek mode. Tagged with topic for the same
    /// stale-event-rejection reason as `MessageArrived`.
    SeekPageLoaded {
        topic: String,
        messages: Vec<RawMessage>,
        meta: SeekPageMeta,
    },
    BrowseFailed(String),
}

/// User- or timer-driven state transitions, dispatched by the render loop into
/// `App::update`.
#[derive(Debug, Clone)]
pub enum Action {
    Quit,
    MoveSelectionUp,
    MoveSelectionDown,
    Confirm,
    Back,
    /// Flips between Tail and Seek browsing on the topic-detail screen; a no-op on any
    /// other screen.
    ToggleBrowseMode,
    /// Seek-mode only: request the next/previous page. No-op in Tail mode or at an edge
    /// (`at_beginning`/`at_end` already true for the requested direction).
    PageForward,
    PageBackward,
    /// Enters filter-text-input mode (topic-detail screen only).
    StartFilterInput,
    FilterChar(char),
    FilterBackspace,
    /// Applies the currently-typed filter text and exits input mode.
    ApplyFilter,
    /// Discards the currently-typed (not-yet-applied) filter text and exits input mode,
    /// leaving any previously-applied filter untouched.
    CancelFilterInput,
    /// Clears an already-applied filter (topic-detail screen only).
    ClearFilter,
}

/// Side effects the reducer wants performed outside of itself. `App::update` stays
/// synchronous and returns a `Command` for the caller (main.rs's event loop) to act on,
/// per PLAN.md's "background I/O never called inline on the render loop" rule.
#[derive(Debug, Clone)]
pub enum Command {
    LoadTopics(crate::config::Profile),
    /// Spawn (or replace) the continuous tail-mode poll task, spanning all of the topic's
    /// partitions (the task itself discovers partition ids via metadata on startup). The
    /// caller is responsible for aborting any previously-running tail task first — tail
    /// and seek are mutually exclusive per PLAN.md's `BrowseMode` design, and there is
    /// never more than one live tail task at a time.
    StartTail {
        profile: crate::config::Profile,
        topic: String,
    },
    /// Abort the currently-running tail task, if any (switching to seek mode, leaving the
    /// topic-detail screen, or switching topics).
    StopTail,
    /// One-shot: load a single seek page. Multiple in-flight requests are fine (superseded
    /// results are just ignored via the topic-tag staleness check on `SeekPageLoaded`).
    LoadSeekPage {
        profile: crate::config::Profile,
        topic: String,
        partition: i32,
        request: SeekPageRequest,
    },
}
