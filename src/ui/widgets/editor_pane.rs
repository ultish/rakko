use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::text_field::text_with_cursor;
use crate::ui::theme::Theme;

/// Multi-line inline editor. When `cursor` is `Some`, draws a high-contrast block
/// caret and scrolls so the caret line stays in view (accounting for soft wrap).
///
/// Focus chrome: **purple** title + border; unfocused: **grey** title + border;
/// body: **base** text.
pub fn render_editor_pane(
    frame: &mut Frame,
    area: Rect,
    theme: &Theme,
    content: &str,
    cursor: Option<usize>,
    title: &str,
) {
    let focused = cursor.is_some();
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .title_style(theme.focus_title(focused))
        .border_style(theme.focus_border(focused))
        .style(theme.root_style());

    // Body is always base text; focus is shown by the purple frame, not gold fill.
    let base = theme.text;
    let text = if let Some(c) = cursor {
        text_with_cursor(content, c, base)
    } else {
        ratatui::text::Text::from(content.to_string()).style(base)
    };

    let inner_h = area.height.saturating_sub(2);
    let inner_w = area.width.saturating_sub(2);
    let scroll_y = if let Some(c) = cursor {
        scroll_to_keep_cursor_visible(content, c, inner_w, inner_h)
    } else {
        0
    };

    frame.render_widget(
        Paragraph::new(text)
            .wrap(Wrap { trim: false })
            .scroll((scroll_y, 0))
            .block(block),
        area,
    );
}

fn scroll_to_keep_cursor_visible(text: &str, cursor: usize, width: u16, height: u16) -> u16 {
    if height == 0 || width == 0 {
        return 0;
    }
    let line = visual_cursor_line(text, cursor, width);
    (line + 1).saturating_sub(height)
}

fn visual_cursor_line(text: &str, cursor: usize, width: u16) -> u16 {
    let width = width.max(1) as usize;
    let cursor = cursor.min(text.chars().count());
    let mut line = 0u16;
    let mut col = 0usize;
    for (i, ch) in text.chars().enumerate() {
        if i >= cursor {
            break;
        }
        if ch == '\n' {
            line = line.saturating_add(1);
            col = 0;
        } else {
            col += 1;
            if col >= width {
                line = line.saturating_add(1);
                col = 0;
            }
        }
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn visual_line_wraps_long_line() {
        assert_eq!(visual_cursor_line("abcdefghij", 9, 5), 1);
        assert_eq!(visual_cursor_line("abcdefghij", 5, 5), 1);
        assert_eq!(visual_cursor_line("abcdefghij", 4, 5), 0);
    }

    #[test]
    fn visual_line_hard_newlines() {
        assert_eq!(visual_cursor_line("ab\ncd", 3, 80), 1);
    }

    #[test]
    fn scroll_keeps_last_line_in_view() {
        let text = "a\nb\nc\nd\ne\nf\ng\nh\ni\nj";
        assert_eq!(scroll_to_keep_cursor_visible(text, text.chars().count(), 80, 3), 7);
    }
}
