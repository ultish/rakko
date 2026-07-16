mod app;
mod cli;
mod clipboard;
mod config;
mod error;
mod events;
mod export;
mod external_editor;
mod kafka;
mod query_filter;
mod raw_message;
mod ring_buffer;
mod serde_detect;
mod text_field;
mod ui;

use std::io::Stdout;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::Parser;
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use futures::StreamExt;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use tokio::sync::{mpsc, watch};

use app::{App, BannerMode, OffsetResetPhase, ProducerFocus, ProducerInputMode, ReplayPhase, Screen};
use cli::Cli;
use error::AppResult;
use events::{Action, AppEvent, Command};

type Term = Terminal<CrosstermBackend<Stdout>>;

/// Leaves the alternate screen and disables raw mode. Shared between the normal exit
/// path and the panic hook so a mid-render panic never leaves the user's terminal
/// broken — best-effort (errors are swallowed since we may already be unwinding).
fn restore_terminal() {
    let _ = disable_raw_mode();
    let _ = execute!(std::io::stdout(), DisableMouseCapture, LeaveAlternateScreen);
}

fn init_terminal() -> AppResult<Term> {
    enable_raw_mode()?;
    execute!(std::io::stdout(), EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(std::io::stdout());
    Ok(Terminal::new(backend)?)
}

/// Logs go to a file under the config dir only — never stdout/stderr, which would
/// corrupt the alternate screen.
fn init_tracing(log_dir: &Path) -> AppResult<tracing_appender::non_blocking::WorkerGuard> {
    std::fs::create_dir_all(log_dir)?;
    let file_appender = tracing_appender::rolling::never(log_dir, "rakko.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    Ok(guard)
}

/// Producer screen hijacks the keyboard for text entry — meta actions use F-keys / Ctrl
/// combos so ordinary characters (including `j`/`k`/`m`) type into the focused field.
/// Ctrl+V or Cmd+V (Super+V) — Grok-style host clipboard paste.
fn is_paste_key(key: &KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('v') | KeyCode::Char('V'))
        && key
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::SUPER)
        && !key.modifiers.contains(KeyModifiers::ALT)
}

fn producer_key_to_action(key: KeyEvent, app: &App) -> Option<Action> {
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::ForceQuit)
        }
        _ if is_paste_key(&key) => Some(Action::PasteClipboard),
        // `q` is a normal char in the producer (types into fields); use Ctrl-c to force-quit.
        // From any other screen `q` still opens the quit dialog via key_to_action.
        KeyCode::Esc => Some(Action::Back),
        KeyCode::Tab => Some(Action::ProducerFocusNext),
        KeyCode::F(2) => Some(Action::ProducerSubmit),
        KeyCode::F(3) => Some(Action::ProducerToggleMode),
        KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::ProducerSubmit)
        }
        KeyCode::Char('m') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::ProducerToggleMode)
        }
        KeyCode::Enter => {
            let state = app.producer.as_ref()?;
            match state.mode {
                ProducerInputMode::Inline => {
                    // Key and value are multi-line; Tab switches focus.
                    Some(Action::ProducerNewline)
                }
                ProducerInputMode::FilePath => {
                    if state.focus == ProducerFocus::FilePath {
                        Some(Action::ProducerLoadFile)
                    } else if state.focus == ProducerFocus::Key {
                        Some(Action::ProducerNewline)
                    } else {
                        Some(Action::ProducerFocusNext)
                    }
                }
                ProducerInputMode::ExternalEditor => {
                    if state.focus == ProducerFocus::Key {
                        Some(Action::ProducerNewline)
                    } else {
                        Some(Action::ProducerOpenExternalEditor)
                    }
                }
            }
        }
        KeyCode::Backspace => Some(Action::ProducerBackspace),
        KeyCode::Delete => Some(Action::ProducerDelete),
        KeyCode::Left => Some(Action::ProducerCursorLeft),
        KeyCode::Right => Some(Action::ProducerCursorRight),
        // Up/Down move the cursor within the focused multi-line field (autoscrolling
        // it into view); PageUp/PageDown scroll the read-only value preview shown in
        // file-path / external-editor mode (a no-op in inline mode, where there's no
        // separate preview to scroll — see `App::scroll_producer_preview`).
        KeyCode::Up => Some(Action::ProducerCursorUp),
        KeyCode::Down => Some(Action::ProducerCursorDown),
        KeyCode::PageUp => Some(Action::PageBackward),
        KeyCode::PageDown => Some(Action::PageForward),
        KeyCode::Home => Some(Action::ProducerCursorHome),
        KeyCode::End => Some(Action::ProducerCursorEnd),
        KeyCode::Char(c) if !key.modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) => {
            Some(Action::ProducerChar(c))
        }
        _ => None,
    }
}

/// Translates a raw key press into an `Action`. Needs `app` (rather than just the key)
/// because several screens hijack the keyboard (filter input, offset-reset wizard).
fn key_to_action(key: KeyEvent, app: &App) -> Option<Action> {
    if key.kind != KeyEventKind::Press {
        return None;
    }

    // Quit confirmation owns the keyboard while open (above splash and other hijacks).
    if app.quit_confirm {
        return match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::ForceQuit)
            }
            KeyCode::Char('y') | KeyCode::Enter => Some(Action::ConfirmQuit),
            KeyCode::Char('n') | KeyCode::Esc => Some(Action::CancelQuit),
            _ => None,
        };
    }

    // Help overlay owns the keyboard (close with ? / Esc; quit still works).
    if app.help_visible {
        return match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::ForceQuit)
            }
            KeyCode::Char('q') => Some(Action::Quit),
            KeyCode::Char('?') | KeyCode::Esc => Some(Action::ToggleHelp),
            _ => None,
        };
    }

    // Startup splash: any key dismisses (so the detailed otter doesn't trap the user).
    if app.show_splash {
        return match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::ForceQuit)
            }
            KeyCode::Char('q') => Some(Action::Quit),
            _ => Some(Action::DismissSplash),
        };
    }

    if app.screen == Screen::Producer {
        return producer_key_to_action(key, app);
    }

    // Create-profile wizard owns the keyboard while open (profile picker overlay).
    if app.profile_create.is_some() {
        return match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::ForceQuit)
            }
            _ if is_paste_key(&key) => Some(Action::PasteClipboard),
            // `q` types into text fields; Esc still cancels (and quits if no profiles).
            KeyCode::Esc => Some(Action::ProfileCreateCancel),
            KeyCode::Enter => Some(Action::ProfileCreateSubmit),
            KeyCode::Tab if key.modifiers.contains(KeyModifiers::SHIFT) => {
                Some(Action::ProfileCreateFocusPrev)
            }
            KeyCode::BackTab => Some(Action::ProfileCreateFocusPrev),
            KeyCode::Tab => Some(Action::ProfileCreateFocusNext),
            KeyCode::Char('t') | KeyCode::Char(' ')
                if app
                    .profile_create
                    .as_ref()
                    .is_some_and(|s| s.focus == app::ProfileCreateFocus::Auth) =>
            {
                Some(Action::ProfileCreateCycleAuth)
            }
            KeyCode::Backspace => Some(Action::ProfileCreateBackspace),
            KeyCode::Delete => Some(Action::ProfileCreateDelete),
            KeyCode::Left => Some(Action::ProfileCreateCursorLeft),
            KeyCode::Right => Some(Action::ProfileCreateCursorRight),
            KeyCode::Home => Some(Action::ProfileCreateCursorHome),
            KeyCode::End => Some(Action::ProfileCreateCursorEnd),
            KeyCode::Char(c)
                if !key.modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                Some(Action::ProfileCreateChar(c))
            }
            _ => None,
        };
    }

    // Delete-profile confirm dialog owns the keyboard while open (profile picker
    // overlay, mutually exclusive with profile_create's own hijack above).
    if app.profile_delete_confirm.is_some() {
        return match key.code {
            KeyCode::Char('q') => Some(Action::Quit),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::ForceQuit)
            }
            KeyCode::Char('y') | KeyCode::Enter => Some(Action::ConfirmDeleteProfile),
            KeyCode::Char('n') | KeyCode::Esc => Some(Action::CancelDeleteProfile),
            _ => None,
        };
    }

    if app.screen == Screen::ExportImport {
        return match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::ForceQuit)
            }
            _ if is_paste_key(&key) => Some(Action::PasteClipboard),
            KeyCode::Char('q') => Some(Action::Quit),
            KeyCode::Esc => Some(Action::Back),
            KeyCode::Enter => Some(Action::ExportImportSubmit),
            KeyCode::Tab => Some(Action::ExportImportFocusNext),
            KeyCode::Backspace => Some(Action::ExportImportBackspace),
            KeyCode::Delete => Some(Action::ExportImportDelete),
            KeyCode::Left => Some(Action::ExportImportCursorLeft),
            KeyCode::Right => Some(Action::ExportImportCursorRight),
            KeyCode::Home => Some(Action::ExportImportCursorHome),
            KeyCode::End => Some(Action::ExportImportCursorEnd),
            KeyCode::Char(c)
                if !key.modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                Some(Action::ExportImportChar(c))
            }
            _ => None,
        };
    }

    // Offset-reset wizard takes over the keyboard while open.
    if let Some(detail) = app.group_detail.as_ref() {
        if let Some(phase) = &detail.reset_phase {
            return match phase {
                OffsetResetPhase::ChooseMode => match key.code {
                    KeyCode::Char('q') => Some(Action::Quit),
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        Some(Action::ForceQuit)
                    }
                    KeyCode::Char('e') => Some(Action::OffsetResetChooseEarliest),
                    KeyCode::Char('l') => Some(Action::OffsetResetChooseLatest),
                    KeyCode::Char('o') => Some(Action::OffsetResetChooseAbsolute),
                    KeyCode::Char('t') => Some(Action::OffsetResetChooseTimestamp),
                    KeyCode::Char('n') | KeyCode::Esc => Some(Action::CancelOffsetReset),
                    _ => None,
                },
                OffsetResetPhase::Input { .. } => match key.code {
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        Some(Action::ForceQuit)
                    }
                    _ if is_paste_key(&key) => Some(Action::PasteClipboard),
                    KeyCode::Char(c) => Some(Action::FilterChar(c)),
                    KeyCode::Backspace => Some(Action::FilterBackspace),
                    KeyCode::Delete => Some(Action::FilterDelete),
                    KeyCode::Left => Some(Action::FilterCursorLeft),
                    KeyCode::Right => Some(Action::FilterCursorRight),
                    KeyCode::Home => Some(Action::FilterCursorHome),
                    KeyCode::End => Some(Action::FilterCursorEnd),
                    KeyCode::Enter => Some(Action::Confirm),
                    KeyCode::Esc => Some(Action::CancelOffsetReset),
                    _ => None,
                },
                OffsetResetPhase::Confirm { .. } => match key.code {
                    KeyCode::Char('q') => Some(Action::Quit),
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        Some(Action::ForceQuit)
                    }
                    KeyCode::Char('y') | KeyCode::Enter => Some(Action::ConfirmOffsetReset),
                    KeyCode::Char('n') | KeyCode::Esc => Some(Action::CancelOffsetReset),
                    _ => None,
                },
            };
        }
    }

    // Single-message replay wizard (topic detail).
    if let Some(detail) = app.topic_detail.as_ref() {
        if let Some(phase) = &detail.replay_phase {
            return match phase {
                ReplayPhase::Confirm { .. } => match key.code {
                    KeyCode::Char('q') => Some(Action::Quit),
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        Some(Action::ForceQuit)
                    }
                    KeyCode::Char('y') | KeyCode::Enter => Some(Action::ConfirmReplay),
                    KeyCode::Char('e') => Some(Action::ReplayEdit),
                    KeyCode::Char('n') | KeyCode::Esc => Some(Action::CancelReplay),
                    _ => None,
                },
            };
        }
    }

    // Advanced query-filter wizard (topic detail only) — checked before the plain
    // substring filter below since both hijack the keyboard the same way but need
    // different Enter handling (parse-and-apply vs trivial store).
    let query_filter_active = app.topic_detail.as_ref().is_some_and(|detail| detail.query_filter_active);
    if query_filter_active {
        return match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::ForceQuit)
            }
            _ if is_paste_key(&key) => Some(Action::PasteClipboard),
            // Ctrl-h (not Ctrl-c) toggles help — plain 'h' has to stay typeable, since
            // query text routinely contains it (field names, string literals like
            // "shipped").
            KeyCode::Char('h') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::ToggleQueryFilterHelp)
            }
            KeyCode::Tab => Some(Action::QueryFilterAutocomplete),
            KeyCode::Char(c) => Some(Action::FilterChar(c)),
            KeyCode::Backspace => Some(Action::FilterBackspace),
            KeyCode::Delete => Some(Action::FilterDelete),
            KeyCode::Left => Some(Action::FilterCursorLeft),
            KeyCode::Right => Some(Action::FilterCursorRight),
            KeyCode::Home => Some(Action::FilterCursorHome),
            KeyCode::End => Some(Action::FilterCursorEnd),
            KeyCode::Enter => Some(Action::ApplyQueryFilter),
            KeyCode::Esc => Some(Action::CancelFilterInput),
            _ => None,
        };
    }

    let filter_active = app.topic_detail.as_ref().is_some_and(|detail| detail.filter_active)
        || app.topic_list_filter_active
        || app.group_list_filter_active;
    if filter_active {
        return match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(Action::ForceQuit)
            }
            _ if is_paste_key(&key) => Some(Action::PasteClipboard),
            // `q` in filter input types 'q'; use Ctrl-c to force-quit.
            KeyCode::Char(c) => Some(Action::FilterChar(c)),
            KeyCode::Backspace => Some(Action::FilterBackspace),
            KeyCode::Delete => Some(Action::FilterDelete),
            KeyCode::Left => Some(Action::FilterCursorLeft),
            KeyCode::Right => Some(Action::FilterCursorRight),
            KeyCode::Home => Some(Action::FilterCursorHome),
            KeyCode::End => Some(Action::FilterCursorEnd),
            KeyCode::Enter => Some(Action::ApplyFilter),
            KeyCode::Esc => Some(Action::CancelFilterInput),
            _ => None,
        };
    }

    let filter_applied = app.topic_detail.as_ref().is_some_and(|detail| detail.applied_filter.is_some())
        || app.topic_detail.as_ref().is_some_and(|detail| detail.applied_query_filter.is_some())
        || app.topic_list_applied_filter.is_some()
        || app.group_list_applied_filter.is_some();

    let inspector_open = app.topic_detail.as_ref().is_some_and(|d| d.message_view.is_some());

    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Action::ForceQuit)
        }
        KeyCode::Char('q') => Some(Action::Quit),
        KeyCode::Up | KeyCode::Char('k') => Some(Action::MoveSelectionUp),
        KeyCode::Down | KeyCode::Char('j') => Some(Action::MoveSelectionDown),
        KeyCode::Enter => Some(Action::Confirm),
        KeyCode::Esc => Some(Action::Back),
        KeyCode::Tab if inspector_open => Some(Action::ToggleInspectorFocus),
        // ←/→ resize the inspector's focused panel — free while it's open since
        // Left/Right are otherwise only bound inside mutually-exclusive text-entry
        // states (producer, profile create, export/import, filter inputs).
        KeyCode::Left if inspector_open => Some(Action::ShrinkInspectorPanel),
        KeyCode::Right if inspector_open => Some(Action::GrowInspectorPanel),
        KeyCode::Tab | KeyCode::Char('s') => Some(Action::ToggleBrowseMode),
        // `n` is seek page-forward on topic detail only — profile picker uses `n` for new profile.
        KeyCode::PageDown => Some(Action::PageForward),
        KeyCode::Char('n') if app.screen == Screen::TopicDetail => Some(Action::PageForward),
        KeyCode::Char('n') if app.screen == Screen::ProfilePicker => {
            Some(Action::StartCreateProfile)
        }
        KeyCode::Char('e') if app.screen == Screen::ProfilePicker => {
            Some(Action::StartEditProfile)
        }
        // `z` deliberately mirrors group detail's offset-reset binding: a non-mnemonic
        // key for a destructive action, reducing accidental presses. Followed by a
        // confirm dialog either way.
        KeyCode::Char('z') if app.screen == Screen::ProfilePicker => {
            Some(Action::StartDeleteProfile)
        }
        KeyCode::PageUp | KeyCode::Char('p') => Some(Action::PageBackward),
        KeyCode::Char('/') => Some(Action::StartFilterInput),
        // Global help overlay. Query filter used to live on `?` (pairing with `/`);
        // it moved to `Q` so `?` can mean help app-wide like other TUIs.
        KeyCode::Char('?') => Some(Action::ToggleHelp),
        // Structured query filter (JSON/Avro field paths) — topic detail only.
        KeyCode::Char('Q') if app.screen == Screen::TopicDetail => {
            Some(Action::StartQueryFilterInput)
        }
        // Clipboard yank on the message browser (list selection or open inspector).
        KeyCode::Char('V') if app.screen == Screen::TopicDetail => Some(Action::CopyMessageValue),
        KeyCode::Char('K') if app.screen == Screen::TopicDetail => Some(Action::CopyMessageKey),
        KeyCode::Char('Y') if app.screen == Screen::TopicDetail => Some(Action::CopyMessageOffset),
        KeyCode::Char('c') if filter_applied => Some(Action::ClearFilter),
        // Group detail: `z` starts offset-reset (deliberately not a common/mnemonic key —
        // reduces accidental presses of a destructive action; `x` is reserved app-wide
        // for export); `r` refreshes lag (same as other screens).
        KeyCode::Char('z') if app.screen == Screen::GroupDetail => Some(Action::StartOffsetReset),
        KeyCode::Char('r')
            if matches!(
                app.screen,
                Screen::TopicList
                    | Screen::GroupList
                    | Screen::GroupDetail
                    | Screen::TopicDetail
                    | Screen::BrokerList
                    | Screen::BrokerDetail
            ) =>
        {
            Some(Action::Refresh)
        }
        // M10: digit keys jump directly between list-level screens — the sole way to
        // move between Topics/Groups/Brokers (no per-screen 'g'/'b' shortcuts, so the
        // switcher bar's meaning is consistent no matter which list screen you're on).
        // Reached only after the filter-input / replay-wizard / offset-reset-wizard /
        // profile-create / export-import early-return guards above, so a stray digit
        // while one of those is capturing keystrokes never fires this.
        // Producer/ExportImport/ProfileCreate are deliberately excluded so a digit
        // doesn't blow away an in-progress draft.
        KeyCode::Char('1' | '2' | '3')
            if matches!(
                app.screen,
                Screen::TopicList
                    | Screen::TopicDetail
                    | Screen::GroupList
                    | Screen::GroupDetail
                    | Screen::BrokerList
                    | Screen::BrokerDetail
            ) =>
        {
            match key.code {
                KeyCode::Char('1') => Some(Action::SwitchToTopics),
                KeyCode::Char('2') => Some(Action::SwitchToGroups),
                KeyCode::Char('3') => Some(Action::SwitchToBrokers),
                _ => unreachable!(),
            }
        }
        KeyCode::Char('w') if app.screen == Screen::TopicDetail => Some(Action::OpenProducer),
        KeyCode::Char('y') if app.screen == Screen::TopicDetail => Some(Action::RequestReplay),
        // `x`/`X` = export selected/all — `e` is reserved app-wide for "edit" (profile
        // picker's edit-profile, replay's edit-in-producer), so export doesn't use it.
        KeyCode::Char('x') if app.screen == Screen::TopicDetail => Some(Action::OpenExport),
        KeyCode::Char('X') if app.screen == Screen::TopicDetail => Some(Action::OpenExportAll),
        KeyCode::Char('i') if app.screen == Screen::TopicDetail => Some(Action::OpenImport),
        // `o` = order: toggle newest↑ / oldest↑ (offset-reset wizard hijacks keys when open).
        KeyCode::Char('o') if app.screen == Screen::TopicDetail => Some(Action::ToggleMessageSort),
        // Banner / theme cycles — capitals avoid clashing with typed text (producer /
        // filter already capture keys before this match). Both persist to `[ui]`.
        KeyCode::Char('A') => Some(Action::CycleBannerMode),
        KeyCode::Char('T') => Some(Action::CycleTheme),
        _ => None,
    }
}

/// Scroll wheel nudges whatever list/pane the currently-active `move_selection`
/// dispatch targets (message inspector included — see `App::move_selection`'s own
/// guard for that). A left click looks up whatever `Action` the last-drawn frame
/// registered at that cell (list row, producer/export field) — a miss (empty
/// space, a non-interactive click) is silently ignored, not an error.
fn mouse_to_action(mouse: MouseEvent, app: &App) -> Option<Action> {
    match mouse.kind {
        MouseEventKind::ScrollUp => Some(Action::MoveSelectionUp),
        MouseEventKind::ScrollDown => Some(Action::MoveSelectionDown),
        MouseEventKind::Down(MouseButton::Left) => app.action_at(mouse.column, mouse.row),
        _ => None,
    }
}

/// How close together two clicks on the same row need to be to count as a
/// double-click — long enough for a deliberate double-click, short enough that two
/// unrelated single clicks on the same row don't get merged.
const DOUBLE_CLICK_WINDOW: Duration = Duration::from_millis(400);

/// Double-click only applies to row selection (open-on-double-click, like a file
/// manager) — deliberately NOT to `ProducerFocusField`/`ExportImportFocusField`
/// clicks, since `Confirm` submits on the export/import screen (`ExportImportSubmit`)
/// and a double-click to reposition a cursor in that field must never trigger it.
fn check_double_click(action: &Action, last: &mut Option<(Instant, Action)>) -> bool {
    if !matches!(action, Action::SelectRow(_)) {
        *last = None;
        return false;
    }
    let is_double = last
        .as_ref()
        .is_some_and(|(t, prev)| prev == action && t.elapsed() < DOUBLE_CLICK_WINDOW);
    *last = if is_double { None } else { Some((Instant::now(), action.clone())) };
    is_double
}

fn spawn_topic_load(profile: config::Profile, tx: mpsc::UnboundedSender<AppEvent>) {
    tokio::spawn(async move {
        // If the profile omits message_max_bytes, ask the broker once and pass the
        // value back so the app can persist it. Profiles that already set a limit
        // are left untouched.
        let auto_message_max_bytes = if profile.message_max_bytes.is_none() {
            match kafka::admin::fetch_broker_message_max_bytes(&profile).await {
                Ok(Some(bytes)) => {
                    tracing::info!(
                        profile = %profile.name,
                        bytes,
                        "detected broker message.max.bytes"
                    );
                    Some((profile.name.clone(), bytes))
                }
                Ok(None) => {
                    tracing::debug!(
                        profile = %profile.name,
                        "broker message.max.bytes missing or unparseable; leaving profile unset"
                    );
                    None
                }
                Err(err) => {
                    tracing::warn!(
                        profile = %profile.name,
                        error = %err,
                        "failed to detect broker message.max.bytes; leaving profile unset"
                    );
                    None
                }
            }
        } else {
            None
        };

        let client = kafka::KafkaClient::new(profile);
        let event = match client.list_topics().await {
            Ok(topics) => AppEvent::TopicsLoaded {
                topics,
                auto_message_max_bytes,
            },
            Err(err) => AppEvent::TopicsLoadFailed(err.to_string()),
        };
        let _ = tx.send(event);
    });
}

fn spawn_group_load(profile: config::Profile, tx: mpsc::UnboundedSender<AppEvent>) {
    tokio::spawn(async move {
        let client = kafka::KafkaClient::new(profile);
        let event = match client.list_groups().await {
            Ok(groups) => AppEvent::GroupsLoaded(groups),
            Err(err) => AppEvent::GroupsLoadFailed(err.to_string()),
        };
        let _ = tx.send(event);
    });
}

fn spawn_broker_load(profile: config::Profile, tx: mpsc::UnboundedSender<AppEvent>) {
    tokio::spawn(async move {
        let client = kafka::KafkaClient::new(profile);
        let event = match client.list_brokers().await {
            Ok((brokers, health)) => AppEvent::BrokersLoaded { brokers, health },
            Err(err) => AppEvent::BrokersLoadFailed(err.to_string()),
        };
        let _ = tx.send(event);
    });
}

fn spawn_broker_config_load(profile: config::Profile, broker_id: i32, tx: mpsc::UnboundedSender<AppEvent>) {
    tokio::spawn(async move {
        let client = kafka::KafkaClient::new(profile);
        let event = match client.fetch_broker_configs(broker_id).await {
            Ok(entries) => AppEvent::BrokerConfigLoaded { broker_id, entries },
            Err(err) => AppEvent::BrokerConfigLoadFailed(err.to_string()),
        };
        let _ = tx.send(event);
    });
}

fn spawn_group_detail_load(profile: config::Profile, group: String, tx: mpsc::UnboundedSender<AppEvent>) {
    tokio::spawn(async move {
        let client = kafka::KafkaClient::new(profile);
        let event = match client.describe_group(&group).await {
            Ok(detail) => AppEvent::GroupDetailLoaded(detail),
            Err(err) => AppEvent::GroupDetailLoadFailed(err.to_string()),
        };
        let _ = tx.send(event);
    });
}

fn spawn_offset_reset(
    profile: config::Profile,
    group: String,
    target: kafka::group_offsets::OffsetResetTarget,
    partitions: Vec<(String, i32)>,
    tx: mpsc::UnboundedSender<AppEvent>,
) {
    tokio::spawn(async move {
        let client = kafka::KafkaClient::new(profile);
        let event = match client.reset_group_offsets(&group, target, &partitions).await {
            Ok(()) => AppEvent::OffsetResetSucceeded { group },
            Err(err) => AppEvent::OffsetResetFailed(err.to_string()),
        };
        let _ = tx.send(event);
    });
}

fn spawn_produce(
    profile: config::Profile,
    topic: String,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<(String, Vec<u8>)>,
    tx: mpsc::UnboundedSender<AppEvent>,
) {
    tokio::spawn(async move {
        let client = kafka::KafkaClient::new(profile);
        let event = match client.produce(&topic, key, value, headers).await {
            Ok(()) => AppEvent::ProduceSucceeded,
            Err(err) => AppEvent::ProduceFailed(err.to_string()),
        };
        let _ = tx.send(event);
    });
}

fn spawn_load_file(path: String, tx: mpsc::UnboundedSender<AppEvent>) {
    tokio::spawn(async move {
        let result = tokio::task::spawn_blocking(move || std::fs::read_to_string(path)).await;
        let event = match result {
            Ok(Ok(content)) => AppEvent::FileLoaded { content },
            Ok(Err(err)) => AppEvent::FileLoadFailed(err.to_string()),
            Err(err) => AppEvent::FileLoadFailed(format!("task panicked: {err}")),
        };
        let _ = tx.send(event);
    });
}

/// Sends the stop signal to the currently-tracked tail task, if any, and clears the
/// slot. Idempotent: a no-op when no tail task is running (e.g. seek mode is active).
/// `run_tail` doesn't stop on `JoinHandle::abort()` (see PLAN.md's `spawn_blocking`
/// polling-loop note) so this cooperative signal is the only reliable way to stop it.
fn stop_tail(tail_stop: &mut Option<watch::Sender<bool>>) {
    if let Some(sender) = tail_stop.take() {
        let _ = sender.send(true);
    }
}

/// Executes a single `Command` returned from `App::update`, spawning whatever
/// background task it implies. `tail_stop` is the event loop's local handle on the
/// currently-running tail task's stop signal (there is never more than one at a time).
///
/// `RunExternalEditor` is **not** handled here — it needs the live terminal and is
/// processed synchronously in the event loop (leave alt screen → editor → re-enter).
/// Runs `action` through the reducer and dispatches every resulting `Command` —
/// shared by keyboard and mouse input, which both feed `Action`s into the same
/// pipeline from here on.
fn dispatch_action(
    action: Action,
    terminal: &mut Term,
    app: &mut App,
    tx: &mpsc::UnboundedSender<AppEvent>,
    tail_stop: &mut Option<watch::Sender<bool>>,
) -> AppResult<()> {
    for command in app.update(action) {
        if let Command::RunExternalEditor { initial } = command {
            run_external_editor(terminal, app, initial)?;
        } else {
            handle_command(command, tx, tail_stop);
        }
    }
    Ok(())
}

fn handle_command(
    command: Command,
    tx: &mpsc::UnboundedSender<AppEvent>,
    tail_stop: &mut Option<watch::Sender<bool>>,
) {
    match command {
        Command::LoadTopics(profile) => spawn_topic_load(profile, tx.clone()),
        Command::StartTail { profile, topic } => {
            // Only one tail task may run at a time; starting a new one (or leaving the
            // screen without an explicit StopTail first) always replaces the old one.
            stop_tail(tail_stop);
            let (stop_tx, stop_rx) = watch::channel(false);
            *tail_stop = Some(stop_tx);
            tokio::spawn(kafka::consumer::run_tail(profile, topic, tx.clone(), stop_rx));
        }
        Command::StopTail => stop_tail(tail_stop),
        Command::LoadSeekPage {
            profile,
            topic,
            partition,
            request,
        } => {
            // One-shot; no cancellation tracking needed. Superseded results are ignored
            // by the reducer's topic-tag staleness check on `SeekPageLoaded`.
            tokio::spawn(kafka::consumer::load_seek_page(
                profile,
                topic,
                partition,
                request,
                tx.clone(),
            ));
        }
        Command::LoadGroups(profile) => spawn_group_load(profile, tx.clone()),
        Command::LoadGroupDetail { profile, group } => {
            spawn_group_detail_load(profile, group, tx.clone())
        }
        Command::LoadBrokers(profile) => spawn_broker_load(profile, tx.clone()),
        Command::LoadBrokerConfig { profile, broker_id } => {
            spawn_broker_config_load(profile, broker_id, tx.clone())
        }
        Command::ResetGroupOffsets {
            profile,
            group,
            target,
            partitions,
        } => spawn_offset_reset(profile, group, target, partitions, tx.clone()),
        Command::ProduceMessage {
            profile,
            topic,
            key,
            value,
            headers,
        } => spawn_produce(profile, topic, key, value, headers, tx.clone()),
        Command::LoadFileIntoProducer { path } => spawn_load_file(path, tx.clone()),
        Command::RunExternalEditor { .. } => {
            // Handled synchronously in the event loop — see `run_external_editor`.
            tracing::warn!("RunExternalEditor reached handle_command; should be handled in run_loop");
        }
        Command::ExportMessages { path, messages } => {
            let tx = tx.clone();
            tokio::spawn(async move {
                let count = messages.len();
                let event = match export::write_jsonl_messages(&path, &messages) {
                    Ok(()) => AppEvent::ExportSucceeded { path, count },
                    Err(err) => AppEvent::ExportFailed(err.to_string()),
                };
                let _ = tx.send(event);
            });
        }
        Command::ImportMessages {
            profile,
            path,
            target_topic,
        } => {
            let tx = tx.clone();
            tokio::spawn(async move {
                let event = match import_jsonl_to_topic(profile, &path, &target_topic).await {
                    Ok(count) => AppEvent::ImportSucceeded {
                        count,
                        topic: target_topic,
                    },
                    Err(err) => AppEvent::ImportFailed(err.to_string()),
                };
                let _ = tx.send(event);
            });
        }
        Command::FetchSchema {
            registry_url,
            schema_id,
        } => {
            let tx = tx.clone();
            tokio::spawn(async move {
                let client = match reqwest::Client::builder().build() {
                    Ok(c) => c,
                    Err(err) => {
                        let _ = tx.send(AppEvent::SchemaLoadFailed {
                            schema_id,
                            message: format!("http client: {err}"),
                        });
                        return;
                    }
                };
                let event =
                    match kafka::schema_registry::fetch_schema_by_id(&client, &registry_url, schema_id)
                        .await
                    {
                        Ok(schema) => AppEvent::SchemaLoaded { schema_id, schema },
                        Err(err) => AppEvent::SchemaLoadFailed {
                            schema_id,
                            message: err.to_string(),
                        },
                    };
                let _ = tx.send(event);
            });
        }
    }
}

/// Stream a JSONL file and produce each message's raw bytes onto `target_topic`.
async fn import_jsonl_to_topic(
    profile: config::Profile,
    path: &str,
    target_topic: &str,
) -> error::AppResult<usize> {
    let mut reader = export::JsonlReader::open(path)?;
    let mut count = 0usize;
    while let Some(message) = reader.next_message()? {
        kafka::producer::produce(
            &profile,
            target_topic,
            message.key,
            message.value,
            message.headers,
        )
        .await?;
        count += 1;
    }
    Ok(count)
}

/// Leave the TUI, run `$EDITOR` on a tempfile seeded with `initial`, restore the TUI,
/// and apply the resulting `AppEvent` immediately.
fn run_external_editor(terminal: &mut Term, app: &mut App, initial: String) -> AppResult<()> {
    // Release the terminal so the editor can use the real tty. Do not clear the main
    // screen buffer — only leave the alternate screen / raw mode.
    disable_raw_mode()?;
    execute!(std::io::stdout(), DisableMouseCapture, LeaveAlternateScreen)?;

    let event = match external_editor::edit_in_external_editor(&initial) {
        Ok(content) => AppEvent::ExternalEditorDone { content },
        Err(err) => AppEvent::ExternalEditorFailed(err.to_string()),
    };

    enable_raw_mode()?;
    execute!(std::io::stdout(), EnterAlternateScreen, EnableMouseCapture)?;
    // Force a full redraw after re-entering the alternate screen.
    terminal.clear()?;
    let _ = app.apply_event(event);
    Ok(())
}

#[tokio::main]
async fn main() -> AppResult<()> {
    let cli = Cli::parse();

    let config_dir: PathBuf = match &cli.config_dir {
        Some(dir) => dir.clone(),
        None => config::config_dir()?,
    };
    let config_path = config_dir.join("config.toml");

    // Held for the process lifetime: dropping it stops the non-blocking writer thread.
    let _tracing_guard = init_tracing(&config_dir)?;
    tracing::info!("rakko starting up, config path: {}", config_path.display());

    let cfg = config::load(&config_path)?;
    let mut app = App::new_with_profile(cfg, config_path.clone(), cli.profile.as_deref());

    let panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore_terminal();
        panic_hook(info);
    }));

    let mut terminal = init_terminal()?;
    let (tx, mut rx) = mpsc::unbounded_channel::<AppEvent>();

    // `--profile` may have already put us on TopicList synchronously; if so kick off
    // the load here since App construction never performs I/O itself.
    if app.screen == Screen::TopicList {
        if let Some(profile) = app.active_profile.clone() {
            app.status_message = Some("loading topics...".to_string());
            spawn_topic_load(profile, tx.clone());
        }
    }

    let mut events = EventStream::new();
    let result = run_loop(&mut terminal, &mut app, &mut events, &mut rx, &tx).await;

    restore_terminal();
    result
}

async fn run_loop(
    terminal: &mut Term,
    app: &mut App,
    events: &mut EventStream,
    rx: &mut mpsc::UnboundedReceiver<AppEvent>,
    tx: &mpsc::UnboundedSender<AppEvent>,
) -> AppResult<()> {
    // Tracks the currently-running tail task's stop signal, if any. Lives here (not on
    // `App`) since it's a background-task handle, not UI state.
    let mut tail_stop: Option<watch::Sender<bool>> = None;

    // Tracks the most recent `SelectRow` click for double-click detection (see
    // `check_double_click`). Not on `App`: it's input-timing bookkeeping, not model
    // state, and `Instant` can't cross the reducer boundary cleanly.
    let mut last_row_click: Option<(Instant, Action)> = None;

    // Soft-refresh consumer-group lag while the group-detail screen is open.
    let mut lag_refresh = tokio::time::interval(std::time::Duration::from_secs(3));
    lag_refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick so we don't double-fetch right after opening a group.
    lag_refresh.tick().await;

    // Braille banner animation (~5 fps when enabled).
    let mut banner_tick = tokio::time::interval(std::time::Duration::from_millis(200));
    banner_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    banner_tick.tick().await;

    loop {
        // Timed around the draw call itself, not the gap between draws: rakko only
        // redraws on an event (no fixed render clock), so "draws per second" is
        // mostly a measure of how often *something happened* — at idle the only
        // thing driving a redraw is the 200ms banner tick, so that reading pins at
        // ~5 regardless of actual performance, which reads as "broken" when it's
        // exactly the intended idle behavior. Render *duration* doesn't have that
        // problem: idle draws are fast (sub-ms → a very high implied fps, correctly
        // signaling "no bottleneck"), and a stalled render (the bug this exists to
        // catch) shows up as a low number regardless of how rarely it's polled.
        let t0 = Instant::now();
        terminal.draw(|f| ui::draw(f, app))?;
        let draw_secs = t0.elapsed().as_secs_f64();
        if draw_secs > 0.0 {
            app.push_fps_sample(1.0 / draw_secs);
        }

        if app.should_quit {
            return Ok(());
        }

        tokio::select! {
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) => {
                        if let Some(action) = key_to_action(key, app) {
                            dispatch_action(action, terminal, app, tx, &mut tail_stop)?;
                        }
                    }
                    Some(Ok(Event::Mouse(mouse))) => {
                        app.set_mouse_pos(mouse.column, mouse.row);
                        if let Some(action) = mouse_to_action(mouse, app) {
                            let double_click = check_double_click(&action, &mut last_row_click);
                            dispatch_action(action, terminal, app, tx, &mut tail_stop)?;
                            if double_click {
                                dispatch_action(Action::Confirm, terminal, app, tx, &mut tail_stop)?;
                            }
                        }
                    }
                    Some(Err(err)) => {
                        tracing::warn!("terminal event stream error: {err}");
                    }
                    _ => {}
                }
            }
            Some(event) = rx.recv() => {
                let reload_group = matches!(&event, AppEvent::OffsetResetSucceeded { .. });
                for command in app.apply_event(event) {
                    handle_command(command, tx, &mut tail_stop);
                }
                if reload_group {
                    if let Some(command) = app.reload_group_detail_command() {
                        handle_command(command, tx, &mut tail_stop);
                    }
                }
            }
            _ = lag_refresh.tick() => {
                for command in app.update(Action::AutoRefreshGroupDetail) {
                    handle_command(command, tx, &mut tail_stop);
                }
            }
            _ = banner_tick.tick(), if app.banner_mode != BannerMode::Off => {
                let _ = app.update(Action::BannerTick);
            }
        }
    }
}
