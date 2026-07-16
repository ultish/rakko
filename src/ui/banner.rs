//! Slim top banner: app name + a braille strip that's either a decorative wave, a
//! live frame-cost graph (`ms/frame`), or off (`A` cycles wave → fps → off).
//! Detailed otter is splash-only.
//!
//! The wave is a short strip of block braille whose height (density) at each
//! column is interpolated between sparse random "keyframe" heights a few columns
//! apart, easing in/out with a smoothstep curve — real peaks and troughs that
//! flow continuously as the strip scrolls, not independent random noise per
//! column (which is what an earlier version of this did: light 2:1 smoothing
//! with only the *next* cell doesn't stop adjacent columns from jumping the full
//! glyph range).
//!
//! The frame-cost mode reuses the same strip/glyph style, but each cell is a
//! recent real per-paint sample of wall milliseconds for `terminal.draw` (see
//! `App::push_frame_ms_sample`) — deliberately timed around the render call
//! itself, not the gap between renders. rakko only redraws on an event (no fixed
//! render clock), so at idle the gap between draws just measures the banner
//! tick's 200ms cadence, not paint cost. Reporting **ms/frame** matches what we
//! measure and stays honest whether samples arrive at ~5 Hz (idle) or much
//! faster (mouse/key). Taller glyphs = slower paints.

use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::Frame;

use crate::app::{App, BannerMode};
use crate::ring_buffer::RingBuffer;

/// One content line + bottom border.
pub const BANNER_HEIGHT: u16 = 3;

/// Short strip — same scale as the original animation.
const WAVE_WIDTH: usize = 9;

/// Density ladder (height): empty-ish → full.
const LEVELS: &[char] = &['⡀', '⣀', '⣄', '⣤', '⣦', '⣶', '⣷', '⣿'];

/// Columns between wave peak/trough keyframes — between them, height is
/// interpolated rather than independently random, so motion reads as one
/// continuous flowing wave with real peaks and troughs.
const WAVE_PERIOD: usize = 6;

/// Deterministic pseudo-random keyframe height (as a float in `[0, LEVELS.len() -
/// 1]`) at keyframe index `k` — the "random" choice now happens once per
/// keyframe, not once per column; columns between keyframes interpolate.
fn keyframe_height(k: usize) -> f64 {
    let mut h = k.wrapping_mul(0x9E37_79B9);
    h ^= h >> 16;
    h = h.wrapping_mul(0x85EB_CA6B);
    h ^= h >> 13;
    (h % 1000) as f64 / 999.0 * (LEVELS.len() - 1) as f64
}

/// Smoothstep ease (3t² − 2t³): an S-curve between two keyframes, so the wave
/// eases in/out of each peak/trough instead of moving at a constant linear rate
/// (which would put a visible kink exactly at each keyframe).
fn smoothstep(t: f64) -> f64 {
    t * t * (3.0 - 2.0 * t)
}

/// Interpolated height (glyph index) at absolute stream position `x`, between the
/// two keyframes surrounding it. Continuous at keyframe boundaries by
/// construction: column `x` at `t≈1` in one interval and column `x+1` at `t=0` in
/// the next both resolve to the same `keyframe_height`.
fn height_at(x: usize) -> usize {
    let k0 = x / WAVE_PERIOD;
    let k1 = k0 + 1;
    let t = (x % WAVE_PERIOD) as f64 / WAVE_PERIOD as f64;
    let h0 = keyframe_height(k0);
    let h1 = keyframe_height(k1);
    let h = h0 + (h1 - h0) * smoothstep(t);
    (h.round() as usize).min(LEVELS.len() - 1)
}

/// Visible strip at scroll phase `phase` (cells `phase .. phase+WAVE_WIDTH`).
pub fn stream_wave(phase: usize) -> String {
    let mut s = String::with_capacity(WAVE_WIDTH);
    for i in 0..WAVE_WIDTH {
        s.push(LEVELS[height_at(phase.wrapping_add(i))]);
    }
    s
}

/// Braille-density strip driven by recent per-paint durations (milliseconds).
/// Newest sample is the rightmost cell; height is normalized against the current
/// window's own max so the shape stays legible for both sub-ms paints and
/// multi-tens-of-ms stalls. Taller = slower.
fn frame_ms_graph(samples: &RingBuffer<f64>) -> String {
    density_graph(samples.iter().copied())
}

/// Same samples as [`frame_ms_graph`], plotted as paint capacity (`1000 / ms`).
/// Taller = faster paints (the familiar high number from the old FPS readout).
fn frame_fps_graph(samples: &RingBuffer<f64>) -> String {
    density_graph(samples.iter().copied().map(ms_to_fps))
}

fn ms_to_fps(ms: f64) -> f64 {
    if ms > 0.0 {
        1000.0 / ms
    } else {
        0.0
    }
}

/// Shared braille strip: newest value rightmost, height normalized to the
/// window max. `values` is already in display units (ms or fps).
fn density_graph(values: impl IntoIterator<Item = f64>) -> String {
    let values: Vec<f64> = values.into_iter().collect();
    if values.is_empty() {
        return " ".repeat(WAVE_WIDTH);
    }
    let max = values.iter().cloned().fold(0.0_f64, f64::max).max(1.0);
    let recent = &values[values.len().saturating_sub(WAVE_WIDTH)..];
    let pad = WAVE_WIDTH.saturating_sub(recent.len());

    let mut s = String::with_capacity(WAVE_WIDTH);
    s.extend(std::iter::repeat_n(' ', pad));
    for &v in recent {
        let level = ((v / max) * (LEVELS.len() - 1) as f64).round() as usize;
        s.push(LEVELS[level.min(LEVELS.len() - 1)]);
    }
    s
}

/// Short rolling average (last few samples) for the numeric `ms/frame` readout.
fn recent_frame_ms(samples: &RingBuffer<f64>) -> f64 {
    let values: Vec<f64> = samples.iter().copied().collect();
    let recent = &values[values.len().saturating_sub(5)..];
    if recent.is_empty() {
        return 0.0;
    }
    recent.iter().sum::<f64>() / recent.len() as f64
}

/// Rolling average of `1000 / ms` over the last few paint samples.
fn recent_frame_fps(samples: &RingBuffer<f64>) -> f64 {
    let ms = recent_frame_ms(samples);
    ms_to_fps(ms)
}

/// Renders the top banner into `area` (should be `BANNER_HEIGHT` tall).
pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let _ = area;

    let (glyphs, glyph_style, mode_label, next_hint) = match app.banner_mode {
        BannerMode::Wave => (
            stream_wave(app.banner_frame),
            // Wave is secondary cyan — purple reserved for brand + selection.
            app.theme.secondary,
            "stream".to_string(),
            "A: ms",
        ),
        BannerMode::Ms => (
            frame_ms_graph(&app.frame_ms_samples),
            app.theme.success,
            format!("{:.0} ms/frame", recent_frame_ms(&app.frame_ms_samples)),
            "A: fps",
        ),
        BannerMode::Fps => (
            frame_fps_graph(&app.frame_ms_samples),
            app.theme.success,
            // Capacity from paint duration — not how often the screen actually
            // redraws (idle is still ~5 Hz from the banner tick).
            format!("{:.0} fps", recent_frame_fps(&app.frame_ms_samples)),
            "A: off",
        ),
        BannerMode::Off => (
            stream_wave(0),
            app.theme.dim,
            "paused".to_string(),
            "A: wave",
        ),
    };

    let profile = app
        .active_profile
        .as_ref()
        .map(|p| format!(" · {}", p.name))
        .unwrap_or_default();

    // One purple touch: the word “rakko”. Everything else cyan/grey.
    let line = Line::from(vec![
        Span::styled(" rakko ", app.theme.accent),
        Span::styled(glyphs, glyph_style),
        Span::styled(" ラッコ", app.theme.secondary),
        Span::styled(format!("  {mode_label}{profile}"), app.theme.dim),
        Span::styled(format!("  [{next_hint}]"), app.theme.secondary),
    ]);

    frame.render_widget(
        Paragraph::new(line).block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(app.theme.border)
                .style(app.theme.root_style())
                .title("rakko")
                .title_style(app.theme.accent),
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

    #[test]
    fn adjacent_columns_never_jump_more_than_a_couple_levels() {
        // The whole point of keyframe interpolation: no more full-range jumps
        // between neighboring columns (the old per-column-random-plus-light-
        // smoothing approach could jump from level 0 to level 7 between adjacent
        // cells — that reads as noise, not a wave).
        for x in 0..200 {
            let a = height_at(x) as i64;
            let b = height_at(x + 1) as i64;
            assert!(
                (a - b).abs() <= 2,
                "adjacent columns at x={x} jumped from level {a} to {b}"
            );
        }
    }

    #[test]
    fn interpolation_is_continuous_at_keyframe_boundaries() {
        // No seam exactly at a keyframe: the column just before a keyframe and
        // the keyframe column itself should be at most one level apart, same as
        // any other adjacent pair (covered above, but explicit here since this is
        // the exact spot a boundary bug would show up).
        for k in 1..20 {
            let x = k * WAVE_PERIOD;
            let before = height_at(x - 1) as i64;
            let at = height_at(x) as i64;
            assert!((before - at).abs() <= 2, "seam at keyframe boundary x={x}");
        }
    }

    #[test]
    fn frame_ms_graph_is_blank_when_no_samples_yet() {
        let samples = RingBuffer::new(30);
        let s = frame_ms_graph(&samples);
        assert_eq!(s.chars().count(), WAVE_WIDTH);
        assert!(s.chars().all(|c| c == ' '));
    }

    #[test]
    fn frame_ms_graph_uses_level_glyphs_once_samples_exist() {
        let mut samples = RingBuffer::new(30);
        // ms/frame samples — tallest glyph tracks the slowest paint in-window.
        for v in [10.0, 20.0, 5.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0] {
            samples.push(v);
        }
        let s = frame_ms_graph(&samples);
        assert_eq!(s.chars().count(), WAVE_WIDTH);
        assert!(s.chars().all(|c| LEVELS.contains(&c)));
        assert!(s.ends_with('⣿'));
    }

    #[test]
    fn recent_frame_ms_averages_the_last_few_samples() {
        let mut samples = RingBuffer::new(30);
        for v in [1.0, 2.0, 3.0, 10.0, 20.0, 30.0] {
            samples.push(v);
        }
        // Last 5: 2,3,10,20,30 → avg 13.
        assert_eq!(recent_frame_ms(&samples), 13.0);
    }

    #[test]
    fn recent_frame_ms_is_zero_with_no_samples() {
        let samples: RingBuffer<f64> = RingBuffer::new(30);
        assert_eq!(recent_frame_ms(&samples), 0.0);
    }

    #[test]
    fn recent_frame_fps_is_inverse_of_ms() {
        let mut samples = RingBuffer::new(30);
        // Steady 10 ms paints → 100 fps capacity.
        for _ in 0..5 {
            samples.push(10.0);
        }
        assert!((recent_frame_fps(&samples) - 100.0).abs() < 1e-9);
    }

    #[test]
    fn frame_fps_graph_tallest_when_paint_is_fastest() {
        let mut samples = RingBuffer::new(30);
        // 50ms, 25ms, then many 5ms (200 fps) — rightmost cells are the max.
        for v in [50.0, 25.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0] {
            samples.push(v);
        }
        let s = frame_fps_graph(&samples);
        assert!(s.ends_with('⣿'), "fast paints should be tall in fps mode: {s}");
    }
}
