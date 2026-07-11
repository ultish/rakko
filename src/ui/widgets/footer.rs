use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::ui::theme::STATUS_STYLE;

/// Split `area` into (main content, bottom keybind footer).
pub fn split_with_footer(area: Rect) -> (Rect, Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(area);
    (chunks[0], chunks[1])
}

/// One-line keybind / help strip — same style as the topic-detail footer.
pub fn render_keybind_footer(frame: &mut Frame, area: Rect, text: &str) {
    frame.render_widget(Paragraph::new(text).style(STATUS_STYLE), area);
}
