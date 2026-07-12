use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::App;
use crate::ui::theme::{STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::footer::render_keybind_footer;
use crate::ui::widgets::table_nav::render_selectable_list;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let mut constraints = vec![Constraint::Min(1)]; // main content
    if app.topic_list_filter_active {
        constraints.push(Constraint::Length(1)); // filter input line
    }
    constraints.push(Constraint::Length(1)); // footer

    let chunks = Layout::default().direction(Direction::Vertical).constraints(constraints).split(area);
    let mut next = chunks.iter();
    let main = *next.next().unwrap();
    let filter_area = app.topic_list_filter_active.then(|| *next.next().unwrap());
    let footer = *next.next().unwrap();

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
        frame.render_widget(message, main);
        render_keybind_footer(frame, footer, "r: refresh   g: groups   Esc: back   q: quit");
        return;
    }

    if let Some(filter_area) = filter_area {
        render_filter_input(frame, filter_area, app);
    }

    let visible = app.visible_topics();
    let filter_label = match &app.topic_list_applied_filter {
        Some(filter) => format!(" — filter: \"{filter}\""),
        None => String::new(),
    };
    let title = match &app.status_message {
        Some(status) => format!("Topics — {status}{filter_label}"),
        None => format!("Topics{filter_label}"),
    };

    if visible.is_empty() {
        let text = if app.topic_list_applied_filter.is_some() {
            "No topics match the current filter. Press 'c' to clear it.".to_string()
        } else {
            "No topics.".to_string()
        };
        let message = Paragraph::new(text)
            .style(STATUS_STYLE)
            .wrap(Wrap { trim: true })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
                    .title_style(TITLE_STYLE),
            );
        frame.render_widget(message, main);
    } else {
        let items: Vec<Vec<String>> = visible
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

        render_selectable_list(
            frame,
            main,
            &title,
            &items,
            Some(&["Name", "Partitions", "Replication", "Compression", "Messages"]),
            app.topic_list_selected_index,
        );
    }

    let filter_hint = if app.topic_list_applied_filter.is_some() {
        "   c: clear filter"
    } else {
        ""
    };
    render_keybind_footer(
        frame,
        footer,
        &format!("Enter: open   g: groups   r: refresh   /: filter{filter_hint}   Esc: back   q: quit"),
    );
}

/// Visually distinct from normal browsing (reversed video) so it's obvious keystrokes
/// are being typed into the filter, not used for navigation — same pattern as the
/// topic-detail message browser's filter bar.
fn render_filter_input(frame: &mut Frame, area: Rect, app: &App) {
    let field = crate::text_field::display_with_cursor(
        &app.topic_list_filter_input,
        app.topic_list_filter_cursor,
    );
    let text = format!("filter> {field}");
    let style = Style::default().add_modifier(Modifier::REVERSED);
    frame.render_widget(Paragraph::new(text).style(style), area);
}
