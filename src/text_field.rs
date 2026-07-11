//! Cursor-aware text editing shared by forms (profile, export, producer, filter, …).
//!
//! Cursor positions are **char** indices (Unicode scalars), not bytes.

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

/// Insert a block cursor glyph at `cursor` for focused single-line fields.
pub fn display_with_cursor(text: &str, cursor: usize) -> String {
    let cursor = cursor.min(text.chars().count());
    let mut out = String::with_capacity(text.len() + 3);
    for (i, ch) in text.chars().enumerate() {
        if i == cursor {
            out.push('▌');
        }
        out.push(ch);
    }
    if cursor >= text.chars().count() {
        out.push('▌');
    }
    out
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
        assert_eq!(display_with_cursor("hi", 0), "▌hi");
        assert_eq!(display_with_cursor("hi", 2), "hi▌");
        assert_eq!(display_with_cursor("hi", 1), "h▌i");
    }
}
