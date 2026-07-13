use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::app::{App, ProducerFocus, ProducerInputMode, ProducerState};
use crate::events::Action;
use crate::text_field::wrap_lines_for_width;
use crate::ui::theme::{STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::editor_pane::render_editor_pane;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let Some(state) = app.producer.as_ref() else {
        let placeholder = Paragraph::new("No producer session.")
            .style(STATUS_STYLE)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Producer")
                    .title_style(TITLE_STYLE),
            );
        frame.render_widget(placeholder, area);
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // header
            Constraint::Min(6),    // key | value columns
            Constraint::Length(1), // status
            Constraint::Length(1), // footer
        ])
        .split(area);

    render_header(frame, chunks[0], state);
    render_fields(frame, app, chunks[1], state);
    render_status(frame, chunks[2], app);
    render_footer(frame, chunks[3], state);
}

/// Key and value as side-by-side vertical slices — same shape as the message
/// inspector's Key/Value row (`topic_detail::render_message_inspector`). Key gets
/// the narrower share since it's typically short; value/body gets the rest.
fn render_fields(frame: &mut Frame, app: &App, area: Rect, state: &ProducerState) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(area);
    render_key(frame, app, cols[0], state);
    render_body(frame, app, cols[1], state);
}

fn render_header(frame: &mut Frame, area: Rect, state: &ProducerState) {
    let mode = mode_label(state.mode);
    let text = format!("Produce → {}  [mode: {mode}]", state.topic);
    frame.render_widget(Paragraph::new(text).style(TITLE_STYLE), area);
}

fn mode_label(mode: ProducerInputMode) -> &'static str {
    match mode {
        ProducerInputMode::Inline => "Inline",
        ProducerInputMode::FilePath => "File path",
        ProducerInputMode::ExternalEditor => "External editor",
    }
}

fn render_key(frame: &mut Frame, app: &App, area: Rect, state: &ProducerState) {
    let focused = state.focus == ProducerFocus::Key;
    let title = if focused { "Key (focused)" } else { "Key" };
    let cursor = if focused {
        Some(state.cursor)
    } else {
        None
    };
    render_editor_pane(frame, area, &state.key_input, cursor, title);
    app.register_click(area.x, area.y, area.width, area.height, Action::ProducerFocusField(ProducerFocus::Key));
}

fn render_body(frame: &mut Frame, app: &App, area: Rect, state: &ProducerState) {
    match state.mode {
        ProducerInputMode::Inline => {
            let focused = state.focus == ProducerFocus::Value;
            let cursor = if focused {
                Some(state.cursor)
            } else {
                None
            };
            render_editor_pane(
                frame,
                area,
                &state.value_input,
                cursor,
                if focused {
                    "Value (focused)"
                } else {
                    "Value"
                },
            );
            app.register_click(area.x, area.y, area.width, area.height, Action::ProducerFocusField(ProducerFocus::Value));
        }
        ProducerInputMode::FilePath => {
            let focused = state.focus == ProducerFocus::FilePath;
            let title = if focused {
                "File path (Enter to load)"
            } else {
                "File path"
            };
            let display = state.display_field(ProducerFocus::FilePath);
            // Show loaded value preview below the path when present.
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(2)])
                .split(area);
            let border_style = if focused {
                Style::default().add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            let block = Block::default()
                .borders(Borders::ALL)
                .title(title)
                .title_style(if focused {
                    TITLE_STYLE.add_modifier(Modifier::REVERSED)
                } else {
                    TITLE_STYLE
                })
                .border_style(border_style);
            frame.render_widget(
                Paragraph::new(display).style(STATUS_STYLE).block(block),
                chunks[0],
            );
            app.register_click(
                chunks[0].x,
                chunks[0].y,
                chunks[0].width,
                chunks[0].height,
                Action::ProducerFocusField(ProducerFocus::FilePath),
            );
            render_scrollable_preview(frame, chunks[1], &state.value_input, state.value_preview_scroll, "Loaded value");
        }
        ProducerInputMode::ExternalEditor => {
            let hint = "Press Enter to open $EDITOR for the value body.";
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(2), Constraint::Min(2)])
                .split(area);
            frame.render_widget(Paragraph::new(hint).style(STATUS_STYLE), chunks[0]);
            render_scrollable_preview(
                frame,
                chunks[1],
                &state.value_input,
                state.value_preview_scroll,
                "Value (from editor)",
            );
        }
    }
}

/// Read-only, scrollable text panel for the file-path / external-editor value
/// preview: pre-wrap then window at `scroll`, same technique as the message
/// inspector's key/value panels (`topic_detail::render_inspector_panel`) — there's
/// no cursor here to autoscroll toward, so scrolling is driven by
/// `App::scroll_producer_preview` (PageUp/PageDown, mouse wheel) instead.
fn render_scrollable_preview(frame: &mut Frame, area: Rect, content: &str, scroll: usize, title: &str) {
    let block = Block::default().borders(Borders::ALL).title(title).title_style(TITLE_STYLE);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let width = inner.width.max(1) as usize;
    let height = inner.height.max(1) as usize;
    let wrapped = wrap_lines_for_width(content, width);
    let scroll = scroll.min(wrapped.len().saturating_sub(height));
    let window = if scroll >= wrapped.len() {
        String::new()
    } else {
        wrapped[scroll..].join("\n")
    };
    frame.render_widget(Paragraph::new(window).style(STATUS_STYLE), inner);
}

fn render_status(frame: &mut Frame, area: Rect, app: &App) {
    let text = app.status_message.clone().unwrap_or_default();
    frame.render_widget(Paragraph::new(text).style(STATUS_STYLE), area);
}

fn render_footer(frame: &mut Frame, area: Rect, state: &ProducerState) {
    let text = match state.mode {
        ProducerInputMode::Inline => {
            "Tab: focus  ←/→/↑/↓/Home/End: cursor  F3/C-m: mode  Enter: newline  F2/C-p: produce  Esc: back"
        }
        ProducerInputMode::FilePath => {
            "Tab: focus  F3/C-m: mode  Enter: load file  PgUp/PgDn/wheel: scroll preview  F2/C-p: produce  Esc: back"
        }
        ProducerInputMode::ExternalEditor => {
            "F3/C-m: mode  Enter: open $EDITOR  PgUp/PgDn/wheel: scroll preview  F2/C-p: produce  Esc: back"
        }
    };
    frame.render_widget(Paragraph::new(text).style(STATUS_STYLE), area);
}
