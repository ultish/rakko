use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::App;
use crate::ui::theme::{STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::table_nav::render_selectable_list;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    if app.config.profiles.is_empty() {
        let message = Paragraph::new(
            "No profiles configured — edit ~/.config/kaf-tui/config.toml and restart.",
        )
        .style(STATUS_STYLE)
        .wrap(Wrap { trim: true })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Profiles")
                .title_style(TITLE_STYLE),
        );
        frame.render_widget(message, area);
        return;
    }

    let items: Vec<Vec<String>> = app
        .config
        .profiles
        .iter()
        .map(|profile| vec![profile.name.clone(), profile.bootstrap_servers.clone()])
        .collect();

    render_selectable_list(
        frame,
        area,
        "Select a profile (Enter to connect, q to quit)",
        &items,
        Some(&["Name", "Bootstrap servers"]),
        app.selected_profile_index,
    );
}
