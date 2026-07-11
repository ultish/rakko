use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::App;
use crate::ui::theme::{STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::footer::{render_keybind_footer, split_with_footer};
use crate::ui::widgets::table_nav::render_selectable_list;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let (main, footer) = split_with_footer(area);

    if app.groups.is_empty() {
        let text = app
            .status_message
            .clone()
            .unwrap_or_else(|| "No consumer groups found.".to_string());
        let message = Paragraph::new(text)
            .style(STATUS_STYLE)
            .wrap(Wrap { trim: true })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Consumer groups")
                    .title_style(TITLE_STYLE),
            );
        frame.render_widget(message, main);
        render_keybind_footer(frame, footer, "r: refresh   Esc: back   q: quit");
        return;
    }

    let items: Vec<Vec<String>> = app
        .groups
        .iter()
        .map(|group| {
            let protocol = if group.protocol.is_empty() {
                group.protocol_type.clone()
            } else if group.protocol_type.is_empty() {
                group.protocol.clone()
            } else {
                format!("{} ({})", group.protocol, group.protocol_type)
            };
            vec![
                group.name.clone(),
                group.state.clone(),
                group.member_count.to_string(),
                protocol,
            ]
        })
        .collect();

    let title = match &app.status_message {
        Some(status) => format!("Consumer groups — {status}"),
        None => "Consumer groups".to_string(),
    };

    render_selectable_list(
        frame,
        main,
        &title,
        &items,
        Some(&["Name", "State", "Members", "Protocol"]),
        app.group_list_selected_index,
    );
    render_keybind_footer(
        frame,
        footer,
        "Enter: open   r: refresh   Esc: back   q: quit",
    );
}
