use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, BrowseMode, MessageViewState, ReplayPhase, TopicDetailState};
use crate::kafka::schema_registry::SchemaRegistry;
use crate::raw_message::RawMessage;
use crate::ui::theme::{STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::confirm_dialog::{centered_rect, render_confirm_dialog};
use crate::ui::widgets::footer::render_keybind_footer;
use crate::ui::widgets::table_nav::render_selectable_list;

/// Safety cap when building list-row text (before the table truncates to the
/// **terminal-allocated** column width). Prevents multi‑MB payloads from being
/// held in every visible row; the table still clips to remaining width.
const LIST_PREVIEW_SAFETY_CAP: usize = 2_048;

/// Cap full-body display so a multi‑MB payload can't freeze the terminal redraw.
const INSPECTOR_MAX_BODY_CHARS: usize = 200_000;

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
    render_header(frame, app, *next.next().unwrap(), detail);
    if detail.filter_active {
        render_filter_input(frame, *next.next().unwrap(), detail);
    }
    render_message_list(frame, *next.next().unwrap(), app, detail);
    render_footer(frame, *next.next().unwrap());

    if let Some(view) = &detail.message_view {
        render_message_inspector(frame, area, view, app.schema_registry.as_ref());
    }

    if let Some(phase) = &detail.replay_phase {
        render_replay_overlay(frame, area, phase);
    }
}

fn render_header(frame: &mut Frame, app: &App, area: Rect, detail: &TopicDetailState) {
    let mode_label = match &detail.mode {
        BrowseMode::Tail(_) => "tail",
        BrowseMode::Seek(_) => "seek",
    };
    let filter_label = match &detail.applied_filter {
        Some(filter) => format!("  filter: \"{filter}\""),
        None => String::new(),
    };
    let sort_label = format!("  sort {}", detail.sort.label());
    let sr_label = match &app.schema_registry {
        Some(sr) => format!("  SR:{} ({} schemas)", sr.base_url(), sr.cache_len()),
        None => {
            if app
                .active_profile
                .as_ref()
                .and_then(|p| p.schema_registry_url.as_ref())
                .is_some()
            {
                "  SR:error".to_string()
            } else {
                String::new()
            }
        }
    };
    let text = format!(
        "Topic: {}  ·  {} partitions  ·  mode {}{}{}{}",
        detail.topic, detail.partition_count, mode_label, sort_label, filter_label, sr_label
    );
    frame.render_widget(Paragraph::new(text).style(TITLE_STYLE), area);
}

/// Visually distinct from normal browsing (reversed video) so it's obvious keystrokes
/// are being typed into the filter, not used for navigation.
fn render_filter_input(frame: &mut Frame, area: Rect, detail: &TopicDetailState) {
    let field = crate::text_field::display_with_cursor(&detail.filter_input, detail.filter_cursor);
    let text = format!("filter> {field}");
    let style = Style::default().add_modifier(Modifier::REVERSED);
    frame.render_widget(Paragraph::new(text).style(style), area);
}

fn render_message_list(frame: &mut Frame, area: Rect, app: &App, detail: &TopicDetailState) {
    let registry = app.schema_registry.as_ref();
    let visible = detail.visible_messages_with_registry(registry);

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
            let (key_fmt, key_preview) = bytes_display(message.key.as_deref(), registry);
            let (val_fmt, value_preview) = bytes_display(message.value.as_deref(), registry);
            vec![
                message.partition.to_string(),
                message.offset.to_string(),
                key_fmt,
                key_preview,
                val_fmt,
                value_preview,
            ]
        })
        .collect();

    render_selectable_list(
        frame,
        area,
        &list_title(detail),
        &items,
        Some(&["P", "Offset", "KFmt", "Key", "VFmt", "Value"]),
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
        BrowseMode::Tail(buffer) => {
            format!(
                "Messages · {} in buffer · {}",
                buffer.len(),
                detail.sort.label()
            )
        }
        BrowseMode::Seek(state) => {
            // Inclusive offsets on this screen (from actual messages when present).
            let (page_lo, page_hi) = match state.messages.as_slice() {
                [] => (state.page_start_offset, state.page_start_offset),
                msgs => {
                    let first = msgs
                        .first()
                        .map(|m| m.offset)
                        .unwrap_or(state.page_start_offset);
                    let last = msgs.last().map(|m| m.offset).unwrap_or(first);
                    (first.min(last), first.max(last))
                }
            };
            // Kafka high watermark is exclusive (next offset to write).
            let log_empty = state.high_watermark <= state.low_watermark;
            let log_hi_inclusive = state
                .high_watermark
                .saturating_sub(1)
                .max(state.low_watermark);

            // Where this page sits for n/p paging — short labels, not prose.
            let page_pos = match (state.at_beginning, state.at_end) {
                (true, true) => "only page",
                (true, false) => "first page",
                (false, true) => "last page",
                (false, false) => "mid log",
            };

            if log_empty {
                return format!(
                    "Seek · partition {} · empty (no offsets) · {} · {}",
                    state.partition,
                    detail.sort.label(),
                    page_pos,
                );
            }

            // "on screen offs … of full log …" — both inclusive ranges.
            let page_part = if state.messages.is_empty() {
                format!("showing none (anchor {})", state.page_start_offset)
            } else if page_lo == page_hi {
                format!("showing offset {page_lo}")
            } else {
                format!("showing offsets {page_lo}–{page_hi}")
            };

            let log_part = if state.low_watermark == log_hi_inclusive {
                format!("log has offset {}", state.low_watermark)
            } else {
                format!(
                    "log has offsets {}–{}",
                    state.low_watermark, log_hi_inclusive
                )
            };

            format!(
                "Seek · partition {} · {page_part} · {log_part} · {} msgs · {} · {}",
                state.partition,
                state.messages.len(),
                detail.sort.label(),
                page_pos,
            )
        }
    }
}

fn render_footer(frame: &mut Frame, area: Rect) {
    render_keybind_footer(
        frame,
        area,
        "Enter: view   Tab/s: mode   o: sort   n/p: page   r: refresh   /: filter   w: produce   y: replay   x: export one   X: export all   i: import   Esc: back",
    );
}

fn render_message_inspector(
    frame: &mut Frame,
    area: Rect,
    view: &MessageViewState,
    registry: Option<&SchemaRegistry>,
) {
    let dialog = centered_rect(86, 78, area);
    frame.render_widget(Clear, dialog);

    let title = format!(
        "Message · partition {} · offset {}",
        view.message.partition, view.message.offset
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .title_style(TITLE_STYLE)
        .border_style(Style::default().fg(ratatui::style::Color::Cyan));
    let inner = block.inner(dialog);
    frame.render_widget(block, dialog);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(1)])
        .split(inner);

    let body = format_message_body(&view.message, registry);
    // Soft-wrap to the pane width *before* scrolling. Counting only `\n` lines made
    // max_scroll=0 for long single-line JSON (looked unscrollable even with j/k
    // updating the offset), because Paragraph wrap was separate from our clamp.
    let width = chunks[0].width.max(1) as usize;
    let wrapped = wrap_lines_for_width(&body, width);
    let line_count = wrapped.len().max(1);
    let visible = chunks[0].height.max(1) as usize;
    let max_scroll = line_count.saturating_sub(visible);
    let scroll = view.scroll.min(max_scroll);

    let window = if scroll >= wrapped.len() {
        String::new()
    } else {
        wrapped[scroll..].join("\n")
    };

    frame.render_widget(
        // No Wrap here — lines are already width-bounded; Paragraph wrap would
        // desync scroll offsets again.
        Paragraph::new(window).style(STATUS_STYLE),
        chunks[0],
    );

    let hint = Line::from(vec![
        Span::styled(
            "j/k/PgUp/PgDn: scroll",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("   "),
        Span::styled(
            "Enter/Esc: close",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("   "),
        Span::styled("y: replay", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw("   "),
        Span::styled("x: export", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw(format!(
            "   line {}/{} ",
            scroll + 1,
            line_count
        )),
    ]);
    frame.render_widget(Paragraph::new(hint), chunks[1]);
}

/// Break `text` into display lines of at most `width` characters (Unicode scalars).
/// Hard newlines are preserved as row boundaries.
fn wrap_lines_for_width(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut out = Vec::new();
    for raw in text.split('\n') {
        if raw.is_empty() {
            out.push(String::new());
            continue;
        }
        let mut col = 0usize;
        let mut line = String::new();
        for ch in raw.chars() {
            if col >= width {
                out.push(std::mem::take(&mut line));
                col = 0;
            }
            line.push(ch);
            col += 1;
        }
        out.push(line);
    }
    if out.is_empty() {
        out.push(String::new());
    }
    out
}

fn format_message_body(message: &RawMessage, registry: Option<&SchemaRegistry>) -> String {
    let mut out = String::new();
    out.push_str(&format!("topic:      {}\n", message.topic));
    out.push_str(&format!("partition:  {}\n", message.partition));
    out.push_str(&format!("offset:     {}\n", message.offset));
    out.push_str(&format!(
        "timestamp:  {}\n",
        format_timestamp(message.timestamp_millis)
    ));

    let (key_fmt, _) = bytes_display(message.key.as_deref(), registry);
    let (val_fmt, _) = bytes_display(message.value.as_deref(), registry);
    out.push_str(&format!("key format:   {key_fmt}\n"));
    out.push_str(&format!("value format: {val_fmt}\n"));

    out.push_str("\n── headers ──\n");
    if message.headers.is_empty() {
        out.push_str("(none)\n");
    } else {
        for (key, value) in &message.headers {
            out.push_str(&format!(
                "{key}: {}\n",
                bytes_to_display_text(Some(value.as_slice()), None)
            ));
        }
    }

    out.push_str(&format!("\n── key ({key_fmt}) ──\n"));
    out.push_str(&bytes_to_display_text(message.key.as_deref(), registry));
    out.push('\n');

    out.push_str(&format!("\n── value ({val_fmt}) ──\n"));
    out.push_str(&bytes_to_display_text(message.value.as_deref(), registry));
    out.push('\n');

    // Soft cap for pathological payloads.
    if out.chars().count() > INSPECTOR_MAX_BODY_CHARS {
        let truncated: String = out.chars().take(INSPECTOR_MAX_BODY_CHARS).collect();
        format!("{truncated}\n\n… (truncated for display)")
    } else {
        out
    }
}

fn format_timestamp(millis: Option<i64>) -> String {
    match millis {
        Some(ms) => format!("{ms} (epoch ms)"),
        None => "—".into(),
    }
}

/// Full decoded body for the inspector: pretty-print JSON when possible.
fn bytes_to_display_text(
    bytes: Option<&[u8]>,
    registry: Option<&SchemaRegistry>,
) -> String {
    let Some(bytes) = bytes else {
        return "<null>".into();
    };
    if bytes.is_empty() {
        return "<empty>".into();
    }
    let decoded = crate::serde_detect::decode_value(bytes, registry);
    let text = decoded.as_str();
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
        if let Ok(pretty) = serde_json::to_string_pretty(&value) {
            return pretty;
        }
    }
    text.to_string()
}

fn render_replay_overlay(frame: &mut Frame, area: Rect, phase: &ReplayPhase) {
    match phase {
        ReplayPhase::Confirm { message } => {
            let existing = if message.headers.is_empty() {
                "(none)".to_string()
            } else {
                message
                    .headers
                    .iter()
                    .map(|(k, v)| format!("{k}={}", String::from_utf8_lossy(v)))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let (_, key_preview) = bytes_display(message.key.as_deref(), None);
            let body = format!(
                "Replay onto the same topic.\n\n\
                 partition {} · offset {}\n\
                 key: {key_preview}\n\
                 headers: {existing}\n\n\
                 y/Enter: replay raw (byte-identical, keeps headers)\n\
                 e: edit in producer (decoded text; headers not carried)\n\
                 n/Esc: cancel",
                message.partition, message.offset
            );
            render_confirm_dialog(frame, area, "Replay message", &body, None);
        }
    }
}

/// Format label + decoded preview for key or value bytes via `serde_detect`
/// (never mutates raw bytes). Key and value are detected independently — they
/// often differ (e.g. raw string key + Avro value).
///
/// Preview text is only soft-capped for memory; the message table truncates to
/// the live column width so Value grows with the terminal.
fn bytes_display(
    bytes: Option<&[u8]>,
    registry: Option<&crate::kafka::schema_registry::SchemaRegistry>,
) -> (String, String) {
    let Some(bytes) = bytes else {
        return ("—".into(), "<null>".into());
    };
    if bytes.is_empty() {
        return ("raw".into(), "<empty>".into());
    }
    let format = crate::serde_detect::detect_format(bytes);
    let label = match format {
        crate::serde_detect::DetectedFormat::Avro { schema_id } => {
            let cached = registry.is_some_and(|sr| sr.cached_schema(schema_id).is_some());
            if cached {
                format!("avro:{schema_id}")
            } else {
                format!("avro:{schema_id}?")
            }
        }
        crate::serde_detect::DetectedFormat::Json => "json".into(),
        crate::serde_detect::DetectedFormat::Raw => "raw".into(),
    };
    let decoded = crate::serde_detect::decode_value(bytes, registry);
    (label, soft_cap_preview(decoded.as_str(), LIST_PREVIEW_SAFETY_CAP))
}

fn soft_cap_preview(text: &str, max: usize) -> String {
    if text.chars().count() > max {
        let truncated: String = text.chars().take(max).collect();
        format!("{truncated}…")
    } else {
        text.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::wrap_lines_for_width;

    #[test]
    fn wrap_splits_long_line_so_scroll_has_room() {
        let long = "x".repeat(100);
        let lines = wrap_lines_for_width(&long, 40);
        assert_eq!(lines.len(), 3); // 40 + 40 + 20
        assert_eq!(lines[0].chars().count(), 40);
        assert_eq!(lines[2].chars().count(), 20);
    }

    #[test]
    fn wrap_keeps_hard_newlines() {
        let lines = wrap_lines_for_width("a\n\nbcdef", 3);
        assert_eq!(lines, vec!["a", "", "bcd", "ef"]);
    }
}
