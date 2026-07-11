use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, ExportImportFocus, ExportImportMode};
use crate::ui::theme::{STATUS_STYLE, TITLE_STYLE};

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let Some(state) = app.export_import.as_ref() else {
        let placeholder = Paragraph::new("No export/import in progress.")
            .style(STATUS_STYLE)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Export/Import")
                    .title_style(TITLE_STYLE),
            );
        frame.render_widget(placeholder, area);
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(if state.mode == ExportImportMode::Import {
                3
            } else {
                0
            }),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(area);

    let title = match state.mode {
        ExportImportMode::Export => format!("Export {} message(s) as JSONL", state.messages.len()),
        ExportImportMode::Import => "Import JSONL (raw bytes → target topic)".to_string(),
    };
    frame.render_widget(
        Paragraph::new(title)
            .style(TITLE_STYLE)
            .block(Block::default().borders(Borders::ALL).title("Export / Import")),
        chunks[0],
    );

    render_field(
        frame,
        chunks[1],
        "Path",
        &state.display_with_cursor(ExportImportFocus::Path),
        state.focus == ExportImportFocus::Path,
    );

    if state.mode == ExportImportMode::Import {
        render_field(
            frame,
            chunks[2],
            "Target topic",
            &state.display_with_cursor(ExportImportFocus::TargetTopic),
            state.focus == ExportImportFocus::TargetTopic,
        );
    }

    let help = match state.mode {
        ExportImportMode::Export => {
            let scope = if state.messages.len() == 1 {
                "single message"
            } else {
                "all visible messages"
            };
            format!(
                "Scope: {scope}. ←/→/Home/End: cursor   Enter: export   Esc: back"
            )
        }
        ExportImportMode::Import => {
            "←/→/Home/End: cursor   Tab: field   Enter: import   Esc: back".to_string()
        }
    };
    frame.render_widget(
        Paragraph::new(help)
            .style(STATUS_STYLE)
            .wrap(Wrap { trim: true })
            .block(Block::default().borders(Borders::ALL).title("Notes")),
        chunks[3],
    );

    let status = app
        .status_message
        .clone()
        .unwrap_or_else(|| "type a path, then Enter".to_string());
    frame.render_widget(Paragraph::new(status).style(STATUS_STYLE), chunks[4]);
}

fn render_field(frame: &mut Frame, area: Rect, label: &str, value: &str, focused: bool) {
    let style = if focused {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
    };
    // `value` already includes the ▌ cursor when focused (see ExportImportState::display_with_cursor).
    frame.render_widget(
        Paragraph::new(value)
            .style(style)
            .block(Block::default().borders(Borders::ALL).title(label).title_style(TITLE_STYLE)),
        area,
    );
}
