use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::App;
use crate::ui::theme::{STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::footer::{render_keybind_footer, split_with_footer};
use crate::ui::widgets::table_nav::render_selectable_list;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let (main, footer) = split_with_footer(area);

    let Some(detail) = app.broker_detail.as_ref() else {
        return;
    };

    let title_base = format!("Broker {} ({}:{})", detail.broker_id, detail.host, detail.port);

    if detail.entries.is_empty() {
        let text = app.status_message.clone().unwrap_or_else(|| {
            "No non-default config values for this broker.".to_string()
        });
        let message = Paragraph::new(text)
            .style(STATUS_STYLE)
            .wrap(Wrap { trim: true })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title_base)
                    .title_style(TITLE_STYLE),
            );
        frame.render_widget(message, main);
        render_keybind_footer(frame, footer, "r: refresh   Esc: back   q: quit");
        return;
    }

    let items: Vec<Vec<String>> = detail
        .entries
        .iter()
        .map(|entry| vec![entry.name.clone(), entry.value.clone()])
        .collect();

    let title = match &app.status_message {
        Some(status) => format!("{title_base} — {status}"),
        None => title_base,
    };

    render_selectable_list(frame, main, &title, &items, Some(&["Name", "Value"]), detail.selected_index);
    render_keybind_footer(frame, footer, "r: refresh   Esc: back   q: quit");
}
