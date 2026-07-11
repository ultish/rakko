use ratatui::layout::Rect;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::App;
use crate::ui::theme::{STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::table_nav::render_selectable_list;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    if app.topics.is_empty() {
        let text = app
            .status_message
            .clone()
            .unwrap_or_else(|| "No topics loaded.".to_string());
        let message = Paragraph::new(text)
            .style(STATUS_STYLE)
            .wrap(Wrap { trim: true })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Topics")
                    .title_style(TITLE_STYLE),
            );
        frame.render_widget(message, area);
        return;
    }

    let items: Vec<Vec<String>> = app
        .topics
        .iter()
        .map(|topic| {
            vec![
                topic.name.clone(),
                topic.partition_count.to_string(),
                topic.replication_factor.to_string(),
                topic.compression_type.clone(),
                topic.total_message_count.to_string(),
            ]
        })
        .collect();

    let title = match &app.status_message {
        Some(status) => format!("Topics — {status} (Esc to go back)"),
        None => "Topics (Esc to go back)".to_string(),
    };

    render_selectable_list(
        frame,
        area,
        &title,
        &items,
        Some(&["Name", "Partitions", "Replication", "Compression", "Messages"]),
        app.topic_list_selected_index,
    );
}
