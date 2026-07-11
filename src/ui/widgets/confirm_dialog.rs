use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::ui::theme::{ERROR_STYLE, STATUS_STYLE, TITLE_STYLE};

/// Centered yes/no modal. Used for destructive actions (offset reset, bulk import).
pub fn render_confirm_dialog(frame: &mut Frame, area: Rect, title: &str, body: &str, warning: Option<&str>) {
    let dialog = centered_rect(60, 40, area);
    frame.render_widget(Clear, dialog);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(3),
            Constraint::Length(if warning.is_some() { 3 } else { 0 }),
            Constraint::Length(1),
        ])
        .margin(1)
        .split(dialog);

    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .title_style(TITLE_STYLE);
    frame.render_widget(block, dialog);

    frame.render_widget(
        Paragraph::new(body).style(STATUS_STYLE).wrap(Wrap { trim: true }),
        chunks[0],
    );

    if let Some(warning) = warning {
        frame.render_widget(
            Paragraph::new(warning)
                .style(ERROR_STYLE)
                .wrap(Wrap { trim: true }),
            chunks[1],
        );
    }

    let footer = Paragraph::new("y: confirm   n/Esc: cancel")
        .style(Style::default().add_modifier(Modifier::BOLD));
    frame.render_widget(footer, chunks[2]);
}

/// Center a dialog of the given percentage size within `area`.
pub fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}
