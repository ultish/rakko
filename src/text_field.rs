//! Cursor-aware text editing shared by forms (profile, export, producer, filter, …).
//!
//! Cursor positions are **char** indices (Unicode scalars), not bytes.

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};

/// Byte offset of the n-th Unicode scalar in `s` (or `s.len()` if past the end).
pub fn char_byte_index(s: &str, char_idx: usize) -> usize {
    s.char_indices()
        .nth(char_idx)
        .map(|(i, _)| i)
        .unwrap_or(s.len())
}

pub fn insert_char(text: &mut String, cursor: &mut usize, c: char) {
    let byte = char_byte_index(text, *cursor);
    text.insert(byte, c);
    *cursor += 1;
}

pub fn backspace(text: &mut String, cursor: &mut usize) {
    if *cursor == 0 {
        return;
    }
    let start = char_byte_index(text, *cursor - 1);
    let end = char_byte_index(text, *cursor);
    text.replace_range(start..end, "");
    *cursor -= 1;
}

pub fn delete_forward(text: &mut String, cursor: &mut usize) {
    let len = text.chars().count();
    if *cursor >= len {
        return;
    }
    let start = char_byte_index(text, *cursor);
    let end = char_byte_index(text, *cursor + 1);
    text.replace_range(start..end, "");
}

pub fn cursor_left(cursor: &mut usize) {
    if *cursor > 0 {
        *cursor -= 1;
    }
}

pub fn cursor_right(text: &str, cursor: &mut usize) {
    let len = text.chars().count();
    if *cursor < len {
        *cursor += 1;
    }
}

pub fn cursor_home(cursor: &mut usize) {
    *cursor = 0;
}

pub fn cursor_end(text: &str, cursor: &mut usize) {
    *cursor = text.chars().count();
}

pub fn clamp_cursor(text: &str, cursor: &mut usize) {
    let len = text.chars().count();
    if *cursor > len {
        *cursor = len;
    }
}

/// High-contrast block cursor style (black on white) — stays visible on yellow/status text.
pub fn cursor_style() -> Style {
    Style::default()
        .fg(Color::Black)
        .bg(Color::White)
        .add_modifier(Modifier::BOLD)
}

/// Insert a solid block cursor glyph at `cursor` for focused single-line fields.
pub fn display_with_cursor(text: &str, cursor: usize) -> String {
    let cursor = cursor.min(text.chars().count());
    let mut out = String::with_capacity(text.len() + 3);
    for (i, ch) in text.chars().enumerate() {
        if i == cursor {
            out.push('█');
        }
        out.push(ch);
    }
    if cursor >= text.chars().count() {
        out.push('█');
    }
    out
}

/// Multi-line styled text with a reverse-video block on the character under the cursor
/// (or a block at end-of-text). Suitable for `Paragraph`.
pub fn text_with_cursor(text: &str, cursor: usize, base: Style) -> Text<'static> {
    let cursor = cursor.min(text.chars().count());
    let caret = cursor_style();
    let mut lines: Vec<Line<'static>> = Vec::new();
    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut plain = String::new();

    let flush_plain = |plain: &mut String, spans: &mut Vec<Span<'static>>, style: Style| {
        if !plain.is_empty() {
            spans.push(Span::styled(std::mem::take(plain), style));
        }
    };

    for (i, ch) in text.chars().enumerate() {
        if i == cursor {
            flush_plain(&mut plain, &mut spans, base);
            if ch == '\n' {
                // Cursor sits on the newline: show a block, then break the visual line.
                spans.push(Span::styled("█".to_string(), caret));
                lines.push(Line::from(std::mem::take(&mut spans)));
            } else {
                spans.push(Span::styled(ch.to_string(), caret));
            }
        } else if ch == '\n' {
            flush_plain(&mut plain, &mut spans, base);
            lines.push(Line::from(std::mem::take(&mut spans)));
        } else {
            plain.push(ch);
        }
    }

    flush_plain(&mut plain, &mut spans, base);
    if cursor >= text.chars().count() {
        spans.push(Span::styled("█".to_string(), caret));
    }
    lines.push(Line::from(spans));
    Text::from(lines)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_middle_and_backspace() {
        let mut s = String::from("ab");
        let mut c = 1;
        insert_char(&mut s, &mut c, 'X');
        assert_eq!(s, "aXb");
        assert_eq!(c, 2);
        backspace(&mut s, &mut c);
        assert_eq!(s, "ab");
        assert_eq!(c, 1);
    }

    #[test]
    fn display_cursor_at_ends() {
        assert_eq!(display_with_cursor("hi", 0), "█hi");
        assert_eq!(display_with_cursor("hi", 2), "hi█");
        assert_eq!(display_with_cursor("hi", 1), "h█i");
    }

    #[test]
    fn text_with_cursor_preserves_lines() {
        let t = text_with_cursor("a\nb", 2, Style::default());
        assert_eq!(t.lines.len(), 2);
    }
}
