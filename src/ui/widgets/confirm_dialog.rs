use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::ui::theme::Theme;

/// Centered yes/no modal. Used for destructive actions (offset reset, bulk import).
pub fn render_confirm_dialog(
    frame: &mut Frame,
    area: Rect,
    theme: &Theme,
    title: &str,
    body: &str,
    warning: Option<&str>,
) {
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

    // Active title (purple), base grey border, base body text, secondary footer keys.
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .title_style(theme.title)
        .border_style(theme.border)
        .style(theme.panel_style());
    frame.render_widget(block, dialog);

    frame.render_widget(
        Paragraph::new(body).style(theme.text).wrap(Wrap { trim: true }),
        chunks[0],
    );

    if let Some(warning) = warning {
        frame.render_widget(
            Paragraph::new(warning)
                .style(theme.error)
                .wrap(Wrap { trim: true }),
            chunks[1],
        );
    }

    let footer = Paragraph::new("y: confirm   n/Esc: cancel")
        .style(theme.secondary.add_modifier(Modifier::BOLD));
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

/// Center a dialog of exact row `height` (clamped to `area`) and percentage width —
/// for content-sized dialogs (a handful of fields + a footer) that would otherwise
/// float in a sea of blank space under `centered_rect`'s screen-percentage height.
pub fn centered_rect_fixed_height(percent_x: u16, height: u16, area: Rect) -> Rect {
    let height = height.min(area.height);
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(0), Constraint::Length(height), Constraint::Min(0)])
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
