//! Semantic color theme for the TUI.
//!
//! Screens and widgets take styles from a single `Theme` (on `App`) rather than
//! hardcoding colors independently. Named themes live under `[ui].theme` in
//! `config.toml` and can be cycled at runtime with `T`.
//!
//! # Color roles (dark & light)
//!
//! | Role | Slot(s) | Use for |
//! |------|---------|---------|
//! | **Active (purple)** | `selected_row`, `cursor`, `focus_*`, brand `accent` | Only selection, focus, and the word “rakko” |
//! | **Secondary (cyan)** | `title`, `secondary`, `status` | Panel titles, column headers, footers, hints, tabs |
//! | **Base (grey / fg)** | `border`, `dim`, `text`, `bg` | Borders, muted chrome, body content, surfaces |
//! | **Semantic** | `error`, `success`, `warning` | Failures / OK / warnings only — not decoration |
//!
//! Purple is scarce on purpose: static chrome is cyan/grey so selection and
//! focus still read as “active.”

use ratatui::style::{Color, Modifier, Style};
use serde::{Deserialize, Serialize};

/// User-facing theme name — stored in `[ui].theme` and used to rebuild `Theme`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThemeName {
    #[default]
    Dark,
    Light,
}

impl ThemeName {
    pub fn next(self) -> Self {
        match self {
            ThemeName::Dark => ThemeName::Light,
            ThemeName::Light => ThemeName::Dark,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            ThemeName::Dark => "dark",
            ThemeName::Light => "light",
        }
    }
}

const fn rgb(r: u8, g: u8, b: u8) -> Color {
    Color::Rgb(r, g, b)
}

/// Semantic style slots shared by every screen/widget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Theme {
    pub name: ThemeName,
    /// Full-frame fill (near-black on dark).
    pub bg: Color,
    /// Elevated surface (dialogs, panels).
    pub bg_panel: Color,
    /// **Secondary** — panel / block titles (cyan, not purple).
    pub title: Style,
    /// **Active** — selected list row (purple band).
    pub selected_row: Style,
    /// **Base** — mouse hover on non-selected rows (grey lift).
    pub hover_row: Style,
    /// **Secondary** — footers, status line, loading placeholders (cyan).
    pub status: Style,
    pub error: Style,
    /// **Base** — muted chrome (mode labels, inactive hints).
    pub dim: Style,
    /// **Active** — brand (“rakko”) and focused-field emphasis only (purple).
    pub accent: Style,
    /// **Secondary** — inactive switcher tabs, help keys/descriptions (cyan).
    pub secondary: Style,
    pub success: Style,
    pub warning: Style,
    /// **Base** — borders and field frames (grey).
    pub border: Style,
    /// **Base** — body content (message values, dialog prose, previews).
    pub text: Style,
    /// **Active** — block cursor (purple).
    pub cursor: Style,
}

impl Theme {
    pub fn from_name(name: ThemeName) -> Self {
        match name {
            ThemeName::Dark => Self::dark(),
            ThemeName::Light => Self::light(),
        }
    }

    /// Base style for filling the terminal (bg + default text).
    pub fn root_style(&self) -> Style {
        Style::new().bg(self.bg).fg(self.text_fg())
    }

    /// Panel / dialog surface style.
    pub fn panel_style(&self) -> Style {
        Style::new().bg(self.bg_panel).fg(self.text_fg())
    }

    fn text_fg(&self) -> Color {
        match self.text.fg {
            Some(c) => c,
            None => Color::White,
        }
    }

    /// Field title: purple reversed when focused, quiet grey when not.
    pub fn focus_title(&self, focused: bool) -> Style {
        if focused {
            self.accent.add_modifier(Modifier::REVERSED)
        } else {
            self.dim
        }
    }

    /// Field border: purple only when focused, grey otherwise.
    pub fn focus_border(&self, focused: bool) -> Style {
        if focused {
            self.accent.add_modifier(Modifier::BOLD)
        } else {
            self.border
        }
    }

    /// Dark theme: scarce purple (selection / focus / brand), cyan for static chrome.
    pub fn dark() -> Self {
        let bg = rgb(20, 20, 20); // #141414
        let bg_panel = rgb(28, 28, 28);
        let bg_hover = rgb(44, 44, 44);
        let purple = rgb(187, 154, 247); // #bb9af7 — active only
        let purple_sel = rgb(72, 56, 110);
        let cyan = rgb(125, 207, 255); // #7dcfff — secondary chrome
        let cyan_soft = rgb(110, 175, 210);
        let fg = rgb(225, 225, 225);
        let fg_dim = rgb(108, 108, 108);
        let border = rgb(72, 72, 78);
        let green = rgb(158, 206, 106);
        let red = rgb(247, 118, 142);

        let secondary = Style::new().fg(cyan);
        let status = Style::new().fg(cyan_soft);
        // Static panel titles = cyan, not purple.
        let title = Style::new().fg(cyan).add_modifier(Modifier::BOLD);

        Self {
            name: ThemeName::Dark,
            bg,
            bg_panel,
            title,
            selected_row: Style::new()
                .fg(fg)
                .bg(purple_sel)
                .add_modifier(Modifier::BOLD),
            hover_row: Style::new().bg(bg_hover),
            status,
            error: Style::new().fg(red).add_modifier(Modifier::BOLD),
            dim: Style::new().fg(fg_dim),
            accent: Style::new().fg(purple).add_modifier(Modifier::BOLD),
            secondary,
            success: Style::new().fg(green),
            warning: Style::new().fg(red),
            border: Style::new().fg(border),
            text: Style::new().fg(fg),
            cursor: Style::new()
                .fg(bg)
                .bg(purple)
                .add_modifier(Modifier::BOLD),
        }
    }

    /// Light theme: same scarce-purple / cyan-chrome roles on a softer (not
    /// near-white) surface — easier on the eyes in a bright room or OLED.
    pub fn light() -> Self {
        // Warm-ish paper grey rather than pure white (#fafafc was too bright).
        let bg = rgb(232, 232, 236); // #e8e8ec
        let bg_panel = rgb(222, 222, 228); // slightly elevated, still soft
        let bg_hover = rgb(210, 210, 218);
        let purple = rgb(120, 90, 200);
        let cyan = rgb(30, 120, 160);
        let secondary = Style::new().fg(cyan);
        let title = Style::new().fg(cyan).add_modifier(Modifier::BOLD);
        let border = rgb(160, 160, 170);
        let fg = rgb(28, 28, 32);
        let fg_dim = rgb(110, 110, 120);
        Self {
            name: ThemeName::Light,
            bg,
            bg_panel,
            title,
            selected_row: Style::new()
                .fg(Color::White)
                .bg(purple)
                .add_modifier(Modifier::BOLD),
            hover_row: Style::new().bg(bg_hover),
            status: secondary,
            error: Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
            dim: Style::new().fg(fg_dim),
            accent: Style::new().fg(purple).add_modifier(Modifier::BOLD),
            secondary,
            success: Style::new().fg(Color::Green),
            warning: Style::new().fg(Color::Red),
            border: Style::new().fg(border),
            text: Style::new().fg(fg),
            cursor: Style::new()
                .fg(Color::White)
                .bg(purple)
                .add_modifier(Modifier::BOLD),
        }
    }
}

impl Default for Theme {
    fn default() -> Self {
        Self::dark()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn theme_name_cycles() {
        assert_eq!(ThemeName::Dark.next(), ThemeName::Light);
        assert_eq!(ThemeName::Light.next(), ThemeName::Dark);
    }

    #[test]
    fn from_name_matches_constructors() {
        assert_eq!(Theme::from_name(ThemeName::Dark).name, ThemeName::Dark);
        assert_eq!(Theme::from_name(ThemeName::Light).name, ThemeName::Light);
    }

    #[test]
    fn dark_bg_is_near_black() {
        assert_eq!(Theme::dark().bg, Color::Rgb(20, 20, 20));
    }

    #[test]
    fn dark_roles_use_purple_cyan_grey() {
        let t = Theme::dark();
        // Active purple (scarce)
        assert_eq!(t.accent.fg, Some(Color::Rgb(187, 154, 247)));
        // Static titles are cyan, not purple
        assert_eq!(t.title.fg, Some(Color::Rgb(125, 207, 255)));
        assert_eq!(t.secondary.fg, Some(Color::Rgb(125, 207, 255)));
        // Base grey border
        assert_eq!(t.border.fg, Some(Color::Rgb(72, 72, 78)));
    }

    #[test]
    fn theme_name_serde_lowercase() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Wrap {
            theme: ThemeName,
        }
        let s = toml::to_string(&Wrap {
            theme: ThemeName::Dark,
        })
        .unwrap();
        assert!(s.contains("theme = \"dark\""), "{s}");
        let w: Wrap = toml::from_str("theme = \"light\"").unwrap();
        assert_eq!(w.theme, ThemeName::Light);
    }
}
