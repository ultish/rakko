//! Slim top banner: app name + braille stream animation.
//! Toggle with `A`. Detailed otter is splash-only.
//!
//! The stream is a short strip of block braille whose **height (density) is
//! random** per cell, scrolling continuously so new random heights enter from
//! the right. Heights are lightly smoothed so it still reads as a wave, not
//! pure static.

use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::app::App;
use crate::ui::theme::TITLE_STYLE;

/// One content line + bottom border.
pub const BANNER_HEIGHT: u16 = 3;

/// Short strip — same scale as the original animation.
const WAVE_WIDTH: usize = 9;

/// Density ladder (height): empty-ish → full.
const LEVELS: &[char] = &['⡀', '⣀', '⣄', '⣤', '⣦', '⣶', '⣷', '⣿'];

/// Deterministic “random” height at absolute stream position `x` (0..7).
fn raw_height(x: usize) -> usize {
    // cheap mix — stable for a given x so scrolling looks continuous
    let mut h = x.wrapping_mul(0x9E37_79B9);
    h ^= h >> 16;
    h = h.wrapping_mul(0x85EB_CA6B);
    h ^= h >> 13;
    h % LEVELS.len()
}

/// Light smoothing (2:1 with next cell) so it still feels wavy, but peaks vary.
fn height_at(x: usize) -> usize {
    let a = raw_height(x);
    let b = raw_height(x.wrapping_add(1));
    (a * 2 + b) / 3
}

/// Visible strip at scroll phase `phase` (cells `phase .. phase+WAVE_WIDTH`).
pub fn stream_wave(phase: usize) -> String {
    let mut s = String::with_capacity(WAVE_WIDTH);
    for i in 0..WAVE_WIDTH {
        s.push(LEVELS[height_at(phase.wrapping_add(i))]);
    }
    s
}

/// Renders the top banner into `area` (should be `BANNER_HEIGHT` tall).
pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let _ = area;
    let stream = if app.banner_animation {
        stream_wave(app.banner_frame)
    } else {
        stream_wave(0)
    };

    let anim_hint = if app.banner_animation {
        "A: pause"
    } else {
        "A: animate"
    };
    let profile = app
        .active_profile
        .as_ref()
        .map(|p| format!(" · {}", p.name))
        .unwrap_or_default();

    let cyan = Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD);
    let dim = Style::default().fg(Color::DarkGray);
    let stream_style = Style::default().fg(Color::Yellow);
    let katakana = Style::default().fg(Color::Cyan);

    let line = Line::from(vec![
        Span::styled(" rakko ", cyan),
        Span::styled(stream, stream_style),
        Span::styled(" ラッコ", katakana),
        Span::styled(format!("  stream{profile}"), dim),
        Span::styled(format!("  [{anim_hint}]"), dim),
    ]);

    frame.render_widget(
        Paragraph::new(line).block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(Color::DarkGray))
                .title("rakko")
                .title_style(TITLE_STYLE),
        ),
        area,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn width_stays_small() {
        assert_eq!(stream_wave(0).chars().count(), WAVE_WIDTH);
        assert_eq!(stream_wave(100).chars().count(), WAVE_WIDTH);
    }

    #[test]
    fn scroll_is_continuous() {
        // Shifting phase by 1 slides the same random field left by one.
        let a: Vec<char> = stream_wave(0).chars().collect();
        let b: Vec<char> = stream_wave(1).chars().collect();
        assert_eq!(b[..WAVE_WIDTH - 1], a[1..]);
    }

    #[test]
    fn heights_use_level_glyphs() {
        let s = stream_wave(42);
        assert!(s.chars().all(|c| LEVELS.contains(&c)));
    }
}
