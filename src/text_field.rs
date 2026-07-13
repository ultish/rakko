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

/// (start, end) char-offsets of each hard-newline-delimited line in `chars`, `end`
/// excluding the newline itself.
fn line_bounds(chars: &[char]) -> Vec<(usize, usize)> {
    let mut bounds = Vec::new();
    let mut start = 0;
    for (i, ch) in chars.iter().enumerate() {
        if *ch == '\n' {
            bounds.push((start, i));
            start = i + 1;
        }
    }
    bounds.push((start, chars.len()));
    bounds
}

/// Index into `bounds` of the line containing char offset `cursor`.
fn line_index_at(bounds: &[(usize, usize)], cursor: usize) -> usize {
    bounds
        .iter()
        .position(|(start, end)| cursor >= *start && cursor <= *end)
        .unwrap_or_else(|| bounds.len().saturating_sub(1))
}

/// Move the cursor up one hard-newline-delimited line, preserving column where
/// possible (clamped to the shorter line's length). No-op on the first line.
pub fn cursor_up(text: &str, cursor: &mut usize) {
    let chars: Vec<char> = text.chars().collect();
    let bounds = line_bounds(&chars);
    let idx = line_index_at(&bounds, *cursor);
    if idx == 0 {
        return;
    }
    let (cur_start, _) = bounds[idx];
    let col = *cursor - cur_start;
    let (prev_start, prev_end) = bounds[idx - 1];
    *cursor = prev_start + col.min(prev_end - prev_start);
}

/// Move the cursor down one hard-newline-delimited line, preserving column where
/// possible (clamped to the shorter line's length). No-op on the last line.
pub fn cursor_down(text: &str, cursor: &mut usize) {
    let chars: Vec<char> = text.chars().collect();
    let bounds = line_bounds(&chars);
    let idx = line_index_at(&bounds, *cursor);
    if idx + 1 >= bounds.len() {
        return;
    }
    let (cur_start, _) = bounds[idx];
    let col = *cursor - cur_start;
    let (next_start, next_end) = bounds[idx + 1];
    *cursor = next_start + col.min(next_end - next_start);
}

/// Break `text` into display lines of at most `width` characters (Unicode scalars).
/// Hard newlines are preserved as row boundaries. Shared by any panel that needs to
/// pre-wrap text so a scroll offset can window into it (see
/// `ui::screens::topic_detail::render_message_inspector`).
pub fn wrap_lines_for_width(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut out = Vec::new();
    for raw in text.split('\n') {
        if raw.is_empty() {
            out.push(String::new());
            continue;
        }
        let mut col = 0usize;
        let mut line = String::new();
        for ch in raw.chars() {
            if col >= width {
                out.push(std::mem::take(&mut line));
                col = 0;
            }
            line.push(ch);
            col += 1;
        }
        out.push(line);
    }
    if out.is_empty() {
        out.push(String::new());
    }
    out
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

    #[test]
    fn cursor_vertical_movement_preserves_column() {
        let text = "abc\nde\nfghij";
        let mut c = 1; // 'b' on line 0
        cursor_up(text, &mut c);
        assert_eq!(c, 1, "no-op on first line");
        cursor_down(text, &mut c);
        assert_eq!(c, 5, "line 1 col 1 -> 'e'");
        cursor_down(text, &mut c);
        assert_eq!(c, 8, "line 2 col 1 -> 'g'");
        cursor_down(text, &mut c);
        assert_eq!(c, 8, "no-op on last line");
    }

    #[test]
    fn cursor_vertical_movement_clamps_to_shorter_line() {
        let text = "abcdef\nxy";
        let mut c = 6; // end of line 0
        cursor_down(text, &mut c);
        assert_eq!(c, 9, "clamped to end of shorter line 1");
    }

    #[test]
    fn wrap_lines_for_width_splits_long_lines_and_keeps_newlines() {
        let wrapped = wrap_lines_for_width("abcdef\nxy", 3);
        assert_eq!(wrapped, vec!["abc", "def", "xy"]);
    }
}
