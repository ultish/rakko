use apache_avro::Schema;

use crate::kafka::admin::TopicSummary;
use crate::kafka::group_offsets::{GroupDetail, GroupSummary, OffsetResetTarget};
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
    /// Partition low watermark (earliest available offset) at load time.
    pub low_watermark: i64,
    /// Partition high watermark (next offset to be written) at load time.
    pub high_watermark: i64,
}

/// Events reported from background tasks (Kafka I/O, HTTP calls) back to the render loop.
/// Never constructed on the render loop thread itself.
#[derive(Debug)]
pub enum AppEvent {
    /// Topic list finished loading. Optional auto-detect of broker
    /// `message.max.bytes` for a profile that had no `message_max_bytes` set.
    TopicsLoaded {
        topics: Vec<TopicSummary>,
        /// `(profile_name, bytes)` when the broker limit was discovered and should
        /// be persisted onto that profile (only when the profile had none).
        auto_message_max_bytes: Option<(String, u32)>,
    },
    TopicsLoadFailed(String),
    /// One message arrived on the continuous tail-mode poll. Tagged with topic/partition
    /// so a stale event from a just-torn-down tail task (a race between abort() and the
    /// task's last in-flight send) can be recognized and dropped by the reducer instead of
    /// corrupting a newly-entered screen.
    MessageArrived {
        topic: String,
        /// Partition the message was consumed from (also on `message.partition`).
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
    GroupsLoaded(Vec<GroupSummary>),
    GroupsLoadFailed(String),
    GroupDetailLoaded(GroupDetail),
    GroupDetailLoadFailed(String),
    OffsetResetSucceeded { group: String },
    OffsetResetFailed(String),
    ProduceSucceeded,
    ProduceFailed(String),
    FileLoaded { content: String },
    FileLoadFailed(String),
    ExternalEditorDone { content: String },
    ExternalEditorFailed(String),
    ExportSucceeded { path: String, count: usize },
    ExportFailed(String),
    ImportSucceeded { count: usize, topic: String },
    ImportFailed(String),
    /// Schema Registry returned a schema for Confluent wire-format Avro decode.
    SchemaLoaded {
        schema_id: u32,
        schema: Schema,
    },
    /// Schema fetch failed; app should stop retrying this id until reconnect.
    SchemaLoadFailed {
        schema_id: u32,
        message: String,
    },
}

/// User- or timer-driven state transitions, dispatched by the render loop into
/// `App::update`.
#[derive(Debug, Clone)]
pub enum Action {
    /// Request quit: opens a confirmation dialog (`q`). Confirm with y/Enter.
    Quit,
    /// Leave immediately without a dialog (Ctrl-c).
    ForceQuit,
    /// User confirmed the quit dialog.
    ConfirmQuit,
    /// User dismissed the quit dialog.
    CancelQuit,
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
    FilterDelete,
    FilterCursorLeft,
    FilterCursorRight,
    FilterCursorHome,
    FilterCursorEnd,
    /// Applies the currently-typed filter text and exits input mode.
    ApplyFilter,
    /// Discards the currently-typed (not-yet-applied) filter text and exits input mode,
    /// leaving any previously-applied filter untouched.
    CancelFilterInput,
    /// Clears an already-applied filter (topic-detail screen only).
    ClearFilter,
    /// Open the consumer-group list (from topic list).
    OpenGroups,
    /// Begin the offset-reset wizard on the group-detail screen.
    StartOffsetReset,
    /// Choose earliest/latest/absolute/timestamp in the offset-reset wizard.
    OffsetResetChooseEarliest,
    OffsetResetChooseLatest,
    OffsetResetChooseAbsolute,
    OffsetResetChooseTimestamp,
    /// Confirm the pending destructive offset reset.
    ConfirmOffsetReset,
    /// Cancel the offset-reset wizard without committing.
    CancelOffsetReset,
    /// Open the producer screen for the current topic (from topic detail).
    OpenProducer,
    /// Cycle Inline → FilePath → ExternalEditor.
    ProducerToggleMode,
    /// Cycle focus among fields valid for the current input mode.
    ProducerFocusNext,
    ProducerChar(char),
    ProducerBackspace,
    ProducerDelete,
    ProducerCursorLeft,
    ProducerCursorRight,
    ProducerCursorHome,
    ProducerCursorEnd,
    /// Insert a newline into the multi-line value field (inline mode).
    ProducerNewline,
    /// Submit the current key/value to Kafka.
    ProducerSubmit,
    /// Load a file path into the value body (file-path mode).
    ProducerLoadFile,
    /// Shell out to `$EDITOR` for the value body (external-editor mode).
    ProducerOpenExternalEditor,
    /// Start single-message replay confirm for the selected browse message.
    RequestReplay,
    /// Confirm replay (raw bytes, same topic, no decode).
    ConfirmReplay,
    /// From the replay confirm dialog: open the optional add-header form.
    ReplayWithHeaderAppend,
    /// Tab between header key / value fields on the replay header form.
    ReplayHeaderFocusNext,
    /// Esc from the header form: return to the replay confirm dialog.
    ReplayHeaderBack,
    /// Cancel the replay wizard entirely.
    CancelReplay,
    /// Open export for the highlighted message (or the one open in the inspector).
    OpenExport,
    /// Open export for all currently visible (filtered) messages on the list.
    OpenExportAll,
    /// Open import screen (target topic defaults to current topic).
    OpenImport,
    ExportImportChar(char),
    ExportImportBackspace,
    ExportImportDelete,
    ExportImportCursorLeft,
    ExportImportCursorRight,
    ExportImportCursorHome,
    ExportImportCursorEnd,
    /// Submit export/import using the path (and target topic for import).
    ExportImportSubmit,
    /// Toggle focus between path and target-topic fields on the import screen.
    ExportImportFocusNext,
    /// Manual refresh of the current list/detail screen (topics, groups, or group lag).
    Refresh,
    /// Toggle message list order between newest-first and oldest-first (topic detail).
    ToggleMessageSort,
    /// Periodic tick: soft-refresh consumer-group lag while on group detail.
    AutoRefreshGroupDetail,
    /// Open the create-profile form (profile picker; auto-opened when config is empty).
    StartCreateProfile,
    /// Open the profile form prefilled for the selected picker row (edit in place).
    StartEditProfile,
    ProfileCreateChar(char),
    ProfileCreateBackspace,
    /// Forward-delete character under the cursor.
    ProfileCreateDelete,
    ProfileCreateCursorLeft,
    ProfileCreateCursorRight,
    ProfileCreateCursorHome,
    ProfileCreateCursorEnd,
    ProfileCreateFocusNext,
    ProfileCreateFocusPrev,
    ProfileCreateToggleTls,
    ProfileCreateSubmit,
    ProfileCreateCancel,
    /// Advance braille banner animation frame (timer-driven).
    BannerTick,
    /// Toggle banner animation on/off (`A` key).
    ToggleBannerAnimation,
    /// Dismiss the startup splash (detailed otter).
    DismissSplash,
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
    LoadGroups(crate::config::Profile),
    LoadGroupDetail {
        profile: crate::config::Profile,
        group: String,
    },
    ResetGroupOffsets {
        profile: crate::config::Profile,
        group: String,
        target: OffsetResetTarget,
        partitions: Vec<(String, i32)>,
    },
    ProduceMessage {
        profile: crate::config::Profile,
        topic: String,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<(String, Vec<u8>)>,
    },
    LoadFileIntoProducer {
        path: String,
    },
    /// Leave the alternate screen, disable raw mode, run `$EDITOR`, then restore the TUI.
    /// Handled synchronously in main (not via tokio::spawn) because the editor needs the
    /// real terminal.
    RunExternalEditor {
        initial: String,
    },
    /// Write the given messages to `path` as JSONL (base64 raw bytes).
    ExportMessages {
        path: String,
        messages: Vec<RawMessage>,
    },
    /// Stream-import a JSONL file onto `target_topic` using raw bytes (topic override).
    ImportMessages {
        profile: crate::config::Profile,
        path: String,
        target_topic: String,
    },
    /// Fetch Avro schema by id from the Confluent Schema Registry REST API.
    FetchSchema {
        registry_url: String,
        schema_id: u32,
    },
}
