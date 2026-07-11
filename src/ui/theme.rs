use ratatui::style::{Color, Modifier, Style};

/// Shared styling constants so colors aren't hardcoded independently in every screen.
/// Intentionally minimal — this isn't a design system.
pub const TITLE_STYLE: Style = Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD);
pub const SELECTED_ROW_STYLE: Style = Style::new()
    .fg(Color::Black)
    .bg(Color::Cyan)
    .add_modifier(Modifier::BOLD);
pub const STATUS_STYLE: Style = Style::new().fg(Color::Yellow);
pub const ERROR_STYLE: Style = Style::new().fg(Color::Red).add_modifier(Modifier::BOLD);
