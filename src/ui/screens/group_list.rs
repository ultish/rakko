use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::App;
use crate::ui::theme::{STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::footer::render_keybind_footer;
use crate::ui::widgets::table_nav::render_selectable_list;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    // Filter bar sits above the list (same position as the message browser's), not
    // below it — keeps the filter's on-screen placement consistent across screens.
    let mut constraints = Vec::new();
    if app.group_list_filter_active {
        constraints.push(Constraint::Length(1)); // filter input line
    }
    constraints.push(Constraint::Min(1)); // main content
    constraints.push(Constraint::Length(1)); // footer

    let chunks = Layout::default().direction(Direction::Vertical).constraints(constraints).split(area);
    let mut next = chunks.iter();
    let filter_area = app.group_list_filter_active.then(|| *next.next().unwrap());
    let main = *next.next().unwrap();
    let footer = *next.next().unwrap();

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

    if let Some(filter_area) = filter_area {
        render_filter_input(frame, filter_area, app);
    }

    let visible = app.visible_groups();
    let filter_label = match &app.group_list_applied_filter {
        Some(filter) => format!(" — filter: \"{filter}\""),
        None => String::new(),
    };
    let title = match &app.status_message {
        Some(status) => format!("Consumer groups — {status}{filter_label}"),
        None => format!("Consumer groups{filter_label}"),
    };

    if visible.is_empty() {
        let text = if app.group_list_applied_filter.is_some() {
            "No groups match the current filter. Press 'c' to clear it.".to_string()
        } else {
            "No consumer groups.".to_string()
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

        render_selectable_list(
            frame,
            app,
            main,
            &title,
            &items,
            Some(&["Name", "State", "Members", "Protocol"]),
            app.group_list_selected_index,
            true,
        );
    }

    let filter_hint = if app.group_list_applied_filter.is_some() {
        "   c: clear filter"
    } else {
        ""
    };
    render_keybind_footer(
        frame,
        footer,
        &format!("Enter: open   r: refresh   /: filter{filter_hint}   Esc: back   q: quit"),
    );
}

/// Visually distinct from normal browsing (reversed video) so it's obvious keystrokes
/// are being typed into the filter, not used for navigation — same pattern as the
/// topic list's filter bar.
fn render_filter_input(frame: &mut Frame, area: Rect, app: &App) {
    let field = crate::text_field::display_with_cursor(
        &app.group_list_filter_input,
        app.group_list_filter_cursor,
    );
    let text = format!("filter> {field}");
    let style = Style::default().add_modifier(Modifier::REVERSED);
    frame.render_widget(Paragraph::new(text).style(style), area);
}
