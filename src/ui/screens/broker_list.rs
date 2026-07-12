use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::App;
use crate::ui::theme::{ERROR_STYLE, STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::footer::render_keybind_footer;
use crate::ui::widgets::table_nav::render_selectable_list;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1), Constraint::Length(1)])
        .split(area);
    let health_area = chunks[0];
    let main = chunks[1];
    let footer = chunks[2];

    render_health_line(frame, health_area, app);

    if app.brokers.is_empty() {
        let text = app
            .status_message
            .clone()
            .unwrap_or_else(|| "No brokers found.".to_string());
        let message = Paragraph::new(text)
            .style(STATUS_STYLE)
            .wrap(Wrap { trim: true })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Brokers")
                    .title_style(TITLE_STYLE),
            );
        frame.render_widget(message, main);
        render_keybind_footer(frame, footer, "r: refresh   Esc: back   q: quit");
        return;
    }

    let items: Vec<Vec<String>> = app
        .brokers
        .iter()
        .map(|broker| {
            vec![
                broker.id.to_string(),
                broker.host.clone(),
                broker.port.to_string(),
                broker.leader_partitions.to_string(),
                broker.replica_partitions.to_string(),
            ]
        })
        .collect();

    let title = match &app.status_message {
        Some(status) => format!("Brokers — {status}"),
        None => "Brokers".to_string(),
    };

    render_selectable_list(
        frame,
        main,
        &title,
        &items,
        Some(&["ID", "Host", "Port", "Leader", "Replicas"]),
        app.broker_list_selected_index,
    );
    render_keybind_footer(
        frame,
        footer,
        "Enter: config   r: refresh   Esc: back   q: quit",
    );
}

fn render_health_line(frame: &mut Frame, area: Rect, app: &App) {
    let health = &app.cluster_health;
    let text = format!(
        "{} broker(s) · {} under-replicated · {} offline",
        app.brokers.len(),
        health.under_replicated,
        health.offline
    );
    let style = if health.under_replicated > 0 || health.offline > 0 {
        ERROR_STYLE
    } else {
        STATUS_STYLE
    };
    frame.render_widget(Paragraph::new(text).style(style), area);
}
