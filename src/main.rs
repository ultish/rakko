mod app;
mod cli;
mod config;
mod error;
mod events;
mod kafka;
mod raw_message;
mod ring_buffer;
mod ui;

use std::io::Stdout;
use std::path::{Path, PathBuf};

use clap::Parser;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use futures::StreamExt;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use tokio::sync::{mpsc, watch};

use app::{App, Screen};
use cli::Cli;
use error::AppResult;
use events::{Action, AppEvent, Command};

type Term = Terminal<CrosstermBackend<Stdout>>;

/// Leaves the alternate screen and disables raw mode. Shared between the normal exit
/// path and the panic hook so a mid-render panic never leaves the user's terminal
/// broken — best-effort (errors are swallowed since we may already be unwinding).
fn restore_terminal() {
    let _ = disable_raw_mode();
    let _ = execute!(std::io::stdout(), LeaveAlternateScreen);
}

fn init_terminal() -> AppResult<Term> {
    enable_raw_mode()?;
    execute!(std::io::stdout(), EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(std::io::stdout());
    Ok(Terminal::new(backend)?)
}

/// Logs go to a file under the config dir only — never stdout/stderr, which would
/// corrupt the alternate screen.
fn init_tracing(log_dir: &Path) -> AppResult<tracing_appender::non_blocking::WorkerGuard> {
    std::fs::create_dir_all(log_dir)?;
    let file_appender = tracing_appender::rolling::never(log_dir, "kaf-tui.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    Ok(guard)
}

/// Translates a raw key press into an `Action`. Needs `app` (rather than just the key)
/// because the topic-detail screen's filter-input mode hijacks nearly the whole keyboard
/// (typed text, not navigation) — the mapping is context-dependent, not a fixed table.
fn key_to_action(key: KeyEvent, app: &App) -> Option<Action> {
    if key.kind != KeyEventKind::Press {
        return None;
    }

    let filter_active = app.topic_detail.as_ref().is_some_and(|detail| detail.filter_active);
    if filter_active {
        return match key.code {
            KeyCode::Char(c) => Some(Action::FilterChar(c)),
            KeyCode::Backspace => Some(Action::FilterBackspace),
            KeyCode::Enter => Some(Action::ApplyFilter),
            KeyCode::Esc => Some(Action::CancelFilterInput),
            _ => None,
        };
    }

    let filter_applied = app.topic_detail.as_ref().is_some_and(|detail| detail.applied_filter.is_some());

    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => Some(Action::Quit),
        KeyCode::Char('q') => Some(Action::Quit),
        KeyCode::Up | KeyCode::Char('k') => Some(Action::MoveSelectionUp),
        KeyCode::Down | KeyCode::Char('j') => Some(Action::MoveSelectionDown),
        KeyCode::Enter => Some(Action::Confirm),
        KeyCode::Esc => Some(Action::Back),
        KeyCode::Tab | KeyCode::Char('s') => Some(Action::ToggleBrowseMode),
        KeyCode::PageDown | KeyCode::Char('n') => Some(Action::PageForward),
        KeyCode::PageUp | KeyCode::Char('p') => Some(Action::PageBackward),
        KeyCode::Char('/') => Some(Action::StartFilterInput),
        KeyCode::Char('c') if filter_applied => Some(Action::ClearFilter),
        _ => None,
    }
}

fn spawn_topic_load(profile: config::Profile, tx: mpsc::UnboundedSender<AppEvent>) {
    tokio::spawn(async move {
        let client = kafka::KafkaClient::new(profile);
        let event = match client.list_topics().await {
            Ok(topics) => AppEvent::TopicsLoaded(topics),
            Err(err) => AppEvent::TopicsLoadFailed(err.to_string()),
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
        Command::LoadSeekPage { profile, topic, partition, request } => {
            // One-shot; no cancellation tracking needed. Superseded results are ignored
            // by the reducer's topic-tag staleness check on `SeekPageLoaded`.
            tokio::spawn(kafka::consumer::load_seek_page(profile, topic, partition, request, tx.clone()));
        }
    }
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
    tracing::info!("kaf-tui starting up, config path: {}", config_path.display());

    let cfg = config::load(&config_path)?;
    let mut app = App::new_with_profile(cfg, cli.profile.as_deref());

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

    loop {
        terminal.draw(|f| ui::draw(f, app))?;

        if app.should_quit {
            return Ok(());
        }

        tokio::select! {
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) => {
                        if let Some(action) = key_to_action(key, app) {
                            for command in app.update(action) {
                                handle_command(command, tx, &mut tail_stop);
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
                app.apply_event(event);
            }
        }
    }
}
