use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, BrowseMode, TopicDetailState};
use crate::ui::theme::{STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::table_nav::render_selectable_list;

/// Long raw key/value bytes get truncated to this many characters for the list view —
/// full message inspection is a later milestone (this is a terminal, not a message
/// viewer).
const PREVIEW_MAX_LEN: usize = 60;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let Some(detail) = app.topic_detail.as_ref() else {
        // Not normally reachable (Screen::TopicDetail always implies topic_detail is
        // Some), but render harmlessly rather than panicking if it ever happens.
        let placeholder = Paragraph::new("No topic selected.").style(STATUS_STYLE).block(
            Block::default().borders(Borders::ALL).title("Topic").title_style(TITLE_STYLE),
        );
        frame.render_widget(placeholder, area);
        return;
    };

    let mut constraints = vec![Constraint::Length(1)]; // header
    if detail.filter_active {
        constraints.push(Constraint::Length(1)); // filter input line
    }
    constraints.push(Constraint::Min(3)); // message list
    constraints.push(Constraint::Length(1)); // footer help

    let chunks = Layout::default().direction(Direction::Vertical).constraints(constraints).split(area);

    let mut next = chunks.iter();
    render_header(frame, *next.next().unwrap(), detail);
    if detail.filter_active {
        render_filter_input(frame, *next.next().unwrap(), detail);
    }
    render_message_list(frame, *next.next().unwrap(), app, detail);
    render_footer(frame, *next.next().unwrap());
}

fn render_header(frame: &mut Frame, area: Rect, detail: &TopicDetailState) {
    let mode_label = match &detail.mode {
        BrowseMode::Tail(_) => "Tail".to_string(),
        BrowseMode::Seek(state) => format!("Seek (partition {})", state.partition),
    };
    let filter_label = match &detail.applied_filter {
        Some(filter) => format!("  filter: \"{filter}\""),
        None => String::new(),
    };
    let text =
        format!("Topic: {}  [{} partitions]  mode: {}{}", detail.topic, detail.partition_count, mode_label, filter_label);
    frame.render_widget(Paragraph::new(text).style(TITLE_STYLE), area);
}

/// Visually distinct from normal browsing (reversed video) so it's obvious keystrokes
/// are being typed into the filter, not used for navigation.
fn render_filter_input(frame: &mut Frame, area: Rect, detail: &TopicDetailState) {
    let text = format!("filter> {}_", detail.filter_input);
    let style = Style::default().add_modifier(Modifier::REVERSED);
    frame.render_widget(Paragraph::new(text).style(style), area);
}

fn render_message_list(frame: &mut Frame, area: Rect, app: &App, detail: &TopicDetailState) {
    let visible = detail.visible_messages();

    if visible.is_empty() {
        let text = empty_state_message(app, detail);
        let message = Paragraph::new(text).style(STATUS_STYLE).wrap(Wrap { trim: true }).block(
            Block::default().borders(Borders::ALL).title(list_title(detail)).title_style(TITLE_STYLE),
        );
        frame.render_widget(message, area);
        return;
    }

    let items: Vec<Vec<String>> = visible
        .iter()
        .map(|message| {
            vec![
                message.partition.to_string(),
                message.offset.to_string(),
                preview(message.key.as_deref()),
                preview(message.value.as_deref()),
            ]
        })
        .collect();

    render_selectable_list(
        frame,
        area,
        &list_title(detail),
        &items,
        Some(&["Partition", "Offset", "Key", "Value"]),
        detail.selected_index,
    );
}

/// Distinguishes "nothing has arrived yet" (tail, no filter) from "the filter excludes
/// everything" from "this seek page is genuinely empty" — otherwise all three look like
/// an identical blank screen and read as broken.
fn empty_state_message(app: &App, detail: &TopicDetailState) -> String {
    if let Some(status) = &app.status_message {
        return status.clone();
    }
    if detail.applied_filter.is_some() {
        return "No messages match the current filter. Press 'c' to clear it.".to_string();
    }
    match &detail.mode {
        BrowseMode::Tail(_) => "No messages yet — waiting for new messages to arrive.".to_string(),
        BrowseMode::Seek(_) => "Empty page — no messages at this offset range.".to_string(),
    }
}

fn list_title(detail: &TopicDetailState) -> String {
    match &detail.mode {
        BrowseMode::Tail(buffer) => format!("Messages ({} buffered)", buffer.len()),
        BrowseMode::Seek(state) => {
            let mut edges = Vec::new();
            if state.at_beginning {
                edges.push("at beginning");
            }
            if state.at_end {
                edges.push("at end");
            }
            let edge_label = if edges.is_empty() { String::new() } else { format!(" [{}]", edges.join(", ")) };
            format!(
                "Messages — partition {} from offset {} ({} loaded){}",
                state.partition,
                state.page_start_offset,
                state.messages.len(),
                edge_label
            )
        }
    }
}

fn render_footer(frame: &mut Frame, area: Rect) {
    let text = "Tab/s: toggle tail/seek  n/PgDn: page forward  p/PgUp: page back  /: filter  c: clear filter  Esc: back";
    frame.render_widget(Paragraph::new(text).style(STATUS_STYLE), area);
}

/// Lossy UTF-8 decode, truncated to `PREVIEW_MAX_LEN` characters — good enough for a
/// list preview; raw bytes can be arbitrarily large and this isn't a byte viewer.
fn preview(bytes: Option<&[u8]>) -> String {
    match bytes {
        None => "<null>".to_string(),
        Some(bytes) => {
            let text = String::from_utf8_lossy(bytes);
            if text.chars().count() > PREVIEW_MAX_LEN {
                let truncated: String = text.chars().take(PREVIEW_MAX_LEN).collect();
                format!("{truncated}…")
            } else {
                text.into_owned()
            }
        }
    }
}
