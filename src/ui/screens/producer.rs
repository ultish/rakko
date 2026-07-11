use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::app::{App, ProducerFocus, ProducerInputMode, ProducerState};
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

    // Key and value both get multi-line panes; remaining height is shared.
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // header
            Constraint::Min(6),    // key (multi-line)
            Constraint::Min(6),    // value / file path body
            Constraint::Length(1), // status
            Constraint::Length(1), // footer
        ])
        .split(area);

    render_header(frame, chunks[0], state);
    render_key(frame, chunks[1], state);
    render_body(frame, chunks[2], state);
    render_status(frame, chunks[3], app);
    render_footer(frame, chunks[4], state);
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

fn render_key(frame: &mut Frame, area: Rect, state: &ProducerState) {
    let focused = state.focus == ProducerFocus::Key;
    let title = if focused { "Key (focused)" } else { "Key" };
    let cursor = if focused {
        Some(state.cursor)
    } else {
        None
    };
    render_editor_pane(frame, area, &state.key_input, cursor, title);
}

fn render_body(frame: &mut Frame, area: Rect, state: &ProducerState) {
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
            render_editor_pane(frame, chunks[1], &state.value_input, None, "Loaded value");
        }
        ProducerInputMode::ExternalEditor => {
            let hint = "Press Enter to open $EDITOR for the value body.";
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(2), Constraint::Min(2)])
                .split(area);
            frame.render_widget(Paragraph::new(hint).style(STATUS_STYLE), chunks[0]);
            render_editor_pane(
                frame,
                chunks[1],
                &state.value_input,
                None,
                "Value (from editor)",
            );
        }
    }
}

fn render_status(frame: &mut Frame, area: Rect, app: &App) {
    let text = app.status_message.clone().unwrap_or_default();
    frame.render_widget(Paragraph::new(text).style(STATUS_STYLE), area);
}

fn render_footer(frame: &mut Frame, area: Rect, state: &ProducerState) {
    let text = match state.mode {
        ProducerInputMode::Inline => {
            "Tab: focus  ←/→/Home/End: cursor  F3/C-m: mode  Enter: newline  F2/C-p: produce  Esc: back"
        }
        ProducerInputMode::FilePath => {
            "Tab: focus  F3/C-m: mode  Enter: load file  F2/C-p: produce  Esc: back"
        }
        ProducerInputMode::ExternalEditor => {
            "F3/C-m: mode  Enter: open $EDITOR  F2/C-p: produce  Esc: back"
        }
    };
    frame.render_widget(Paragraph::new(text).style(STATUS_STYLE), area);
}
