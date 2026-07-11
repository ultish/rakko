use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::ui::theme::{STATUS_STYLE, TITLE_STYLE};

/// Multi-line inline editor display. When focused, `content` should already include
/// the cursor glyph (see `text_field::display_with_cursor`).
pub fn render_editor_pane(
    frame: &mut Frame,
    area: Rect,
    content: &str,
    focused: bool,
    title: &str,
) {
    let border_style = if focused {
        Style::default().add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };

    let display = content.to_string();

    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .title_style(if focused {
            TITLE_STYLE.add_modifier(Modifier::REVERSED)
        } else {
            TITLE_STYLE
        })
        .border_style(border_style);

    let style = if focused { STATUS_STYLE } else { Style::default() };

    frame.render_widget(
        Paragraph::new(display)
            .style(style)
            .wrap(Wrap { trim: false })
            .block(block),
        area,
    );
}
