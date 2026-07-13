use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, BrowseMode, InspectorFocus, MessageViewState, ReplayPhase, TopicDetailState};
use crate::events::Action;
use crate::kafka::schema_registry::SchemaRegistry;
use crate::raw_message::RawMessage;
use crate::text_field::wrap_lines_for_width;
use crate::ui::theme::{STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::confirm_dialog::{centered_rect, centered_rect_fixed_height};
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

    // Query filter gets its own dialog (below) rather than a row here — a query can
    // get long enough (multiple AND-chained conditions) that a single terminal-width
    // line isn't enough room to see what you're typing.
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
        render_message_inspector(
            frame,
            app,
            area,
            view,
            app.schema_registry.as_ref(),
            detail.inspector_top_split,
            detail.inspector_bottom_split,
        );
    }

    if let Some(phase) = &detail.replay_phase {
        render_replay_overlay(frame, area, phase);
    }

    if detail.query_filter_active {
        render_query_filter_dialog(frame, area, detail);
    }
}

fn render_header(frame: &mut Frame, app: &App, area: Rect, detail: &TopicDetailState) {
    let mode_label = match &detail.mode {
        BrowseMode::Tail(_) => "tail",
        BrowseMode::Seek(_) => "seek",
    };

    // Every segment joins with the same wide separator — previously only the first
    // three did; sort/filter/SR were tacked on with a bare double-space, which read
    // as cramped even when the terminal had plenty of width to spare.
    let mut segments = vec![
        format!("Topic: {}", detail.topic),
        format!("{} partitions", detail.partition_count),
        format!("mode {mode_label}"),
        format!("sort {}", detail.sort.label()),
    ];
    if let Some(filter) = &detail.applied_filter {
        segments.push(format!("filter: \"{filter}\""));
    }
    if let Some(query) = &detail.applied_query_filter {
        segments.push(format!("query: {}", query.raw));
    }
    match &app.schema_registry {
        Some(sr) => segments.push(format!("SR:{} ({} schemas)", sr.base_url(), sr.cache_len())),
        None => {
            if app
                .active_profile
                .as_ref()
                .and_then(|p| p.schema_registry_url.as_ref())
                .is_some()
            {
                segments.push("SR:error".to_string());
            }
        }
    }

    let text = segments.join("  ·  ");
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

// Each operator is paired with a word label (equals/not equals/greater than/etc) so
// the line stays unambiguous even on terminal fonts that render `!=`/`>=`/`<=` as a
// single ligature glyph (e.g. Fira Code, Cascadia Code, JetBrains Mono) — rakko emits
// plain ASCII either way, this is purely a readability hedge against that rendering.
const QUERY_FILTER_HELP: &str = "\
Fields:   key.<path>   value.<path>   (dot-separated, any nesting depth)
Complete: Tab — completes key/value, then cycles field names found on the current
          page (value. shows every top-level field; keep tabbing to go deeper)
Ops:      =  equals        != not equals
          >  greater than  <  less than
          >= greater/equal <= less/equal      (>,<,>=,<= need a numeric value)
Combine:  AND  (only AND for now — no OR / parentheses)
Strings:  bare word (jxhui) or quoted for spaces (\"hello world\") — case-insensitive
Arrays:   matches if ANY element satisfies the rest of the path, at any depth
          e.g. value.items.sku = \"ABC123\" matches if any item has that sku

Examples:
  key.person.name = jxhui
  key.person.age = 20 AND value.house.owner = jxhui
  value.tags = \"urgent\"
  value.timestamp > 23434
  value.orders.items.sku != \"X\"";

/// Query-filter input as a centered dialog rather than a one-line bar — a chained
/// query (`a = 1 AND b = 2 AND ...`) needs more room than a terminal-width row gives,
/// and the dialog has space for the `Ctrl-h` help panel below the input.
fn render_query_filter_dialog(frame: &mut Frame, area: Rect, detail: &TopicDetailState) {
    let dialog = if detail.query_filter_help_visible {
        centered_rect(80, 70, area)
    } else {
        centered_rect(80, 25, area)
    };
    frame.render_widget(Clear, dialog);

    let mut constraints = vec![Constraint::Length(3), Constraint::Length(1), Constraint::Length(1)];
    if detail.query_filter_help_visible {
        constraints.push(Constraint::Min(1));
    }
    let inner = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .margin(1)
        .split(dialog);

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Advanced Query Filter")
        .title_style(TITLE_STYLE);
    frame.render_widget(block, dialog);

    let field = crate::text_field::display_with_cursor(
        &detail.query_filter_input,
        detail.query_filter_cursor,
    );
    let input = Paragraph::new(format!("query> {field}"))
        .style(Style::default().add_modifier(Modifier::REVERSED))
        .block(Block::default().borders(Borders::ALL));
    frame.render_widget(input, inner[0]);

    if let Some(completion) = &detail.query_filter_completion {
        let options: Vec<String> = completion
            .candidates
            .iter()
            .enumerate()
            .map(|(i, c)| if i == completion.index { format!("[{c}]") } else { c.clone() })
            .collect();
        frame.render_widget(
            Paragraph::new(format!("Tab to cycle: {}", options.join(" | ")))
                .style(STATUS_STYLE)
                .wrap(Wrap { trim: true }),
            inner[1],
        );
    }

    let help_hint = if detail.query_filter_help_visible {
        "Ctrl-h: hide help"
    } else {
        "Ctrl-h: show syntax & examples"
    };
    frame.render_widget(
        Paragraph::new(format!("Enter: apply   Esc: cancel   Tab: complete   {help_hint}"))
            .style(STATUS_STYLE),
        inner[2],
    );

    if detail.query_filter_help_visible {
        frame.render_widget(
            Paragraph::new(QUERY_FILTER_HELP).wrap(Wrap { trim: false }),
            inner[3],
        );
    }
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
        app,
        area,
        &list_title(detail),
        &items,
        Some(&["P", "Offset", "KFmt", "Key", "VFmt", "Value"]),
        detail.selected_index,
        true,
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
        "Enter: view   Tab/s: mode   o: sort   n/p: page   r: refresh   /: filter   ?: query filter   w: produce   y: replay   x: export one   X: export all   i: import   Esc: back",
    );
}

#[allow(clippy::too_many_arguments)]
fn render_message_inspector(
    frame: &mut Frame,
    app: &App,
    area: Rect,
    view: &MessageViewState,
    registry: Option<&SchemaRegistry>,
    top_split: u16,
    bottom_split: u16,
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

    let (key_fmt, _) = bytes_display(view.message.key.as_deref(), registry);
    let (val_fmt, _) = bytes_display(view.message.value.as_deref(), registry);
    let attrs = format_message_attrs(&view.message, &key_fmt, &val_fmt);
    let attrs_lines = attrs.lines().count() as u16;
    // Attrs is fixed/deterministic (always 6 lines) and has no scrollback, so it
    // dictates the top row's height — headers is a scrollable panel that shares
    // whatever room that leaves. Key/value (the actual payload — often deep, often
    // long) get the rest of the dialog, not squeezed alongside metadata. Only an
    // extreme case (a tiny terminal) caps the top row, leaving a guaranteed minimum
    // for the key/value row + footer.
    let top_height = attrs_lines.min(inner.height.saturating_sub(1 + 3)).max(1);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(top_height), Constraint::Min(3), Constraint::Length(1)])
        .split(inner);

    let top_panels = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(top_split), Constraint::Percentage(100 - top_split)])
        .split(rows[0]);
    // Key can be just as deeply nested as value, so it isn't starved — value gets
    // the larger share by default since it's typically the bigger payload, not an
    // exclusive one; ←/→ (while Key/Value is focused) adjusts `bottom_split`.
    let bottom_panels = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(bottom_split), Constraint::Percentage(100 - bottom_split)])
        .split(rows[1]);

    render_static_panel(frame, top_panels[0], "Attrs", &attrs);

    let key_body = capped_body(bytes_to_display_text(view.message.key.as_deref(), registry));
    let headers_body = capped_body(format_message_headers(&view.message));
    let value_body = capped_body(bytes_to_display_text(view.message.value.as_deref(), registry));

    // Soft-wrap to each pane's width *before* scrolling. Counting only `\n` lines
    // made max_scroll=0 for long single-line JSON (looked unscrollable even with
    // j/k updating the offset), because Paragraph wrap was separate from our clamp.
    let headers_wrapped =
        wrap_lines_for_width(&headers_body, top_panels[1].width.saturating_sub(2).max(1) as usize);
    let key_wrapped = wrap_lines_for_width(&key_body, bottom_panels[0].width.saturating_sub(2).max(1) as usize);
    let value_wrapped =
        wrap_lines_for_width(&value_body, bottom_panels[1].width.saturating_sub(2).max(1) as usize);

    let headers_scroll = view
        .headers_scroll
        .min(headers_wrapped.len().saturating_sub(top_panels[1].height.saturating_sub(2).max(1) as usize));
    let key_scroll = view
        .key_scroll
        .min(key_wrapped.len().saturating_sub(bottom_panels[0].height.saturating_sub(2).max(1) as usize));
    let value_scroll = view
        .value_scroll
        .min(value_wrapped.len().saturating_sub(bottom_panels[1].height.saturating_sub(2).max(1) as usize));

    render_inspector_panel(
        frame,
        app,
        top_panels[1],
        "Headers",
        &headers_wrapped,
        headers_scroll,
        view.focus == InspectorFocus::Headers,
        Action::SetInspectorFocus(InspectorFocus::Headers),
    );
    render_inspector_panel(
        frame,
        app,
        bottom_panels[0],
        &format!("Key ({key_fmt})"),
        &key_wrapped,
        key_scroll,
        view.focus == InspectorFocus::Key,
        Action::SetInspectorFocus(InspectorFocus::Key),
    );
    render_inspector_panel(
        frame,
        app,
        bottom_panels[1],
        &format!("Value ({val_fmt})"),
        &value_wrapped,
        value_scroll,
        view.focus == InspectorFocus::Value,
        Action::SetInspectorFocus(InspectorFocus::Value),
    );

    let (focused_scroll, focused_line_count) = match view.focus {
        InspectorFocus::Key => (key_scroll, key_wrapped.len().max(1)),
        InspectorFocus::Headers => (headers_scroll, headers_wrapped.len().max(1)),
        InspectorFocus::Value => (value_scroll, value_wrapped.len().max(1)),
    };
    let hint = Line::from(vec![
        Span::styled(
            "j/k/PgUp/PgDn: scroll",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("   "),
        Span::styled("Tab/click: switch panel", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw("   "),
        Span::styled("←/→: resize", Style::default().add_modifier(Modifier::BOLD)),
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
            focused_scroll + 1,
            focused_line_count
        )),
    ]);
    frame.render_widget(Paragraph::new(hint), rows[2]);
}

/// The attrs panel: plain metadata, no scrolling and no click-to-focus (there's
/// nothing to scroll to — see `format_message_attrs`).
fn render_static_panel(frame: &mut Frame, area: Rect, title: &str, body: &str) {
    let block = Block::default().borders(Borders::ALL).title(title).title_style(TITLE_STYLE);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    frame.render_widget(Paragraph::new(body.to_string()).style(STATUS_STYLE), inner);
}

/// One key/headers/value panel: a bordered, titled box showing `wrapped` starting at
/// `scroll`. `focused` gets the same bold-border/reversed-title treatment as a
/// focused producer field; clicking anywhere in the panel dispatches `focus_action`
/// (`Action::SetInspectorFocus`) so the mouse can pick a panel directly, same as Tab.
#[allow(clippy::too_many_arguments)]
fn render_inspector_panel(
    frame: &mut Frame,
    app: &App,
    area: Rect,
    title: &str,
    wrapped: &[String],
    scroll: usize,
    focused: bool,
    focus_action: Action,
) {
    let border_style = if focused {
        Style::default().add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    let title_style = if focused {
        TITLE_STYLE.add_modifier(Modifier::REVERSED)
    } else {
        TITLE_STYLE
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title.to_string())
        .title_style(title_style)
        .border_style(border_style);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    app.register_click(area.x, area.y, area.width, area.height, focus_action);

    let window = if scroll >= wrapped.len() {
        String::new()
    } else {
        wrapped[scroll..].join("\n")
    };
    frame.render_widget(
        // No Wrap here — lines are already width-bounded; Paragraph wrap would
        // desync scroll offsets again.
        Paragraph::new(window).style(STATUS_STYLE),
        inner,
    );
}

/// Metadata banner across the top of the inspector: topic/partition/offset/timestamp,
/// key/value formats, and headers — everything *except* the key/value bodies
/// themselves, which get their own side-by-side panels (see `render_message_inspector`).
/// Fixed, deterministic metadata — always exactly 6 lines, never needs scrolling
/// (unlike headers, which can be an arbitrarily long list — see `format_message_headers`).
fn format_message_attrs(message: &RawMessage, key_fmt: &str, val_fmt: &str) -> String {
    format!(
        "topic:      {}\npartition:  {}\noffset:     {}\ntimestamp:  {}\nkey format:   {key_fmt}\nvalue format: {val_fmt}",
        message.topic,
        message.partition,
        message.offset,
        format_timestamp(message.timestamp_millis),
    )
}

fn format_message_headers(message: &RawMessage) -> String {
    if message.headers.is_empty() {
        return "(none)".to_string();
    }
    message
        .headers
        .iter()
        .map(|(key, value)| format!("{key}: {}", bytes_to_display_text(Some(value.as_slice()), None)))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Soft cap for pathological payloads so a multi-MB key/value can't freeze the
/// terminal redraw.
fn capped_body(body: String) -> String {
    if body.chars().count() > INSPECTOR_MAX_BODY_CHARS {
        let truncated: String = body.chars().take(INSPECTOR_MAX_BODY_CHARS).collect();
        format!("{truncated}\n\n… (truncated for display)")
    } else {
        body
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

/// A dedicated (not `render_confirm_dialog`-based) layout: replay has three real
/// outcomes (raw / edit / cancel), not confirm_dialog's fixed yes/no footer, and the
/// message metadata reads better as aligned fields than one wrapped prose blob.
/// Dialog height tracks the field count instead of a fixed percentage, so there's no
/// leftover blank space below a handful of short lines.
fn render_replay_overlay(frame: &mut Frame, area: Rect, phase: &ReplayPhase) {
    match phase {
        ReplayPhase::Confirm { message } => {
            let headers = if message.headers.is_empty() {
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
            let fields = format!(
                "topic:     {}\npartition: {}\noffset:    {}\nkey:       {key_preview}\nheaders:   {headers}",
                message.topic, message.partition, message.offset,
            );
            let field_lines = fields.lines().count() as u16;
            // fields + spacer + footer, + margin(1) top/bottom, + the block's own border.
            let dialog_height = field_lines + 2 + 2 + 2;

            let dialog = centered_rect_fixed_height(64, dialog_height, area);
            frame.render_widget(Clear, dialog);
            let block = Block::default()
                .borders(Borders::ALL)
                .title("Replay onto the same topic")
                .title_style(TITLE_STYLE);
            let inner = block.inner(dialog);
            frame.render_widget(block, dialog);

            let rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(field_lines),
                    Constraint::Length(1), // spacer
                    Constraint::Length(1), // footer
                ])
                .margin(1)
                .split(inner);

            frame.render_widget(Paragraph::new(fields).style(STATUS_STYLE), rows[0]);
            let footer = Line::from(vec![
                Span::styled("y/Enter", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(": replay raw (byte-identical)   "),
                Span::styled("e", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(": edit in producer   "),
                Span::styled("n/Esc", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(": cancel"),
            ]);
            frame.render_widget(Paragraph::new(footer), rows[2]);
        }
    }
}

/// Above this many bytes, a payload gets its format/preview computed cheaply
/// instead of a full decode — `bytes_display` runs for every visible row on every
/// render (the message list redraws on the ~200ms banner-tick animation, not just
/// on data changes), so redoing a full decode of a multi-MB message here on every
/// frame makes browsing large-message topics grind to a halt (mouse/keyboard input
/// piles up behind each slow re-render), for work whose result is soft-capped to
/// `LIST_PREVIEW_SAFETY_CAP` characters anyway. Sized well above that cap for
/// UTF-8/JSON-syntax margin.
const LIST_PREVIEW_DECODE_BYTE_CAP: usize = 8 * 1024;

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
    if bytes.len() > LIST_PREVIEW_DECODE_BYTE_CAP {
        // Confluent Avro magic prefix (0x00 + 4-byte schema id) is a cheap 5-byte
        // check — `detect_format` returns as soon as it sees this, so it's cheap
        // regardless of payload size. The binary decode itself is also cheap
        // regardless of size; what isn't is converting an untruncated multi-MB
        // field to `serde_json::Value` and JSON-escape-serializing it back to a
        // string. `avro_value_preview` truncates fields before that conversion so
        // it stays cheap while still showing real (if partial) content.
        if let crate::serde_detect::DetectedFormat::Avro { schema_id } = crate::serde_detect::detect_format(bytes) {
            let cached = registry.is_some_and(|sr| sr.cached_schema(schema_id).is_some());
            let label = if cached {
                format!("avro:{schema_id}")
            } else {
                format!("avro:{schema_id}?")
            };
            // `max_chars` here is a total content budget shared across the whole
            // record (see `truncate_avro_value_bounded`), not a per-field cap — a
            // single huge field and a record with many small fields both stay
            // within it. Fields are visited (and the budget spent) in schema
            // declaration order, not serde_json's key-sorted output order, so a
            // schema with identifying fields (id, type, ...) declared before a
            // large payload field shows those first rather than having them
            // crowded out by whichever field name sorts alphabetically first.
            // Budget is a fraction of the final display cap, not the cap itself:
            // JSON syntax (quotes/colons/commas/key names) adds overhead on top of
            // the raw content the budget counts, so passing the full cap can push
            // the serialized size just over it — leaving the *outer* soft-cap
            // below to trim the tail, which (since output is key-sorted) can cut
            // off a small field like `id` after a big one, undoing the ordering
            // guarantee above. This margin keeps normal-sized records comfortably
            // under the outer cap so it stays a no-op safety net, not a second
            // truncation pass.
            let preview =
                crate::serde_detect::avro_value_preview(bytes, registry, LIST_PREVIEW_SAFETY_CAP * 3 / 4)
                    .unwrap_or_else(|| format!("<{} bytes — Enter to view>", bytes.len()));
            return (label, soft_cap_preview(&preview, LIST_PREVIEW_SAFETY_CAP));
        }
        let bounded = &bytes[..LIST_PREVIEW_DECODE_BYTE_CAP];
        let label = if crate::serde_detect::looks_json_shaped(bounded) {
            "json"
        } else {
            "raw"
        };
        let preview = String::from_utf8_lossy(bounded);
        return (label.into(), soft_cap_preview(&preview, LIST_PREVIEW_SAFETY_CAP));
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
    use super::bytes_display;

    #[test]
    fn small_json_is_labeled_and_previewed_exactly() {
        let (label, preview) = bytes_display(Some(br#"{"a":1}"#), None);
        assert_eq!(label, "json");
        assert_eq!(preview, r#"{"a":1}"#);
    }

    #[test]
    fn large_json_over_the_decode_cap_still_labels_as_json() {
        // Bigger than LIST_PREVIEW_DECODE_BYTE_CAP so it takes the bounded-prefix
        // path — the badge must come from the cheap shape probe, not a full parse
        // (a truncated document can never itself parse successfully).
        let body = "x".repeat(20_000);
        let big = format!(r#"{{"a":"{body}"}}"#);
        let (label, preview) = bytes_display(Some(big.as_bytes()), None);
        assert_eq!(label, "json");
        assert!(preview.starts_with("{\"a\":\"xxxx"));
        assert!(preview.len() < big.len(), "preview must not clone the whole payload");
    }

    #[test]
    fn large_non_json_over_the_decode_cap_labels_as_raw() {
        let big = "x".repeat(20_000);
        let (label, _) = bytes_display(Some(big.as_bytes()), None);
        assert_eq!(label, "raw");
    }

    #[test]
    fn large_avro_without_a_cached_schema_falls_back_to_a_placeholder() {
        // Confluent magic byte + schema id, then a payload big enough to trip the
        // bound. No registry is passed, so there's no cached schema to decode
        // against regardless of size — labeling still only reads the 5-byte magic
        // prefix, and the preview falls back to a placeholder rather than showing
        // raw undecoded bytes.
        let mut bytes = vec![0x00, 0, 0, 0, 7];
        bytes.extend(std::iter::repeat_n(b'x', 20_000));
        let (label, preview) = bytes_display(Some(&bytes), None);
        assert_eq!(label, "avro:7?");
        assert!(preview.contains("Enter to view"));
    }

    #[test]
    fn large_avro_with_a_cached_schema_shows_a_bounded_real_preview() {
        use apache_avro::types::Value as AvroValue;
        use apache_avro::Schema;
        use crate::kafka::schema_registry::SchemaRegistry;

        let schema = Schema::parse_str(
            r#"{"type":"record","name":"Big","fields":[
                {"name":"id","type":"int"},
                {"name":"blob","type":"string"}
            ]}"#,
        )
        .unwrap();
        let value = AvroValue::Record(vec![
            ("id".into(), AvroValue::Int(42)),
            ("blob".into(), AvroValue::String("y".repeat(2_000_000))),
        ]);
        let payload = apache_avro::to_avro_datum(&schema, value).unwrap();
        let mut bytes = vec![0x00, 0, 0, 0, 3];
        bytes.extend(payload);

        let mut registry = SchemaRegistry::new("http://localhost:8081").unwrap();
        registry.insert_schema(3, schema);

        let t0 = std::time::Instant::now();
        let (label, preview) = bytes_display(Some(&bytes), Some(&registry));
        // Regression guard for the actual bug: an untruncated decode+serialize of
        // this record took ~50ms in a debug build (~5s across a 100-row page,
        // redone on every ~200ms render) — generous ceiling so this doesn't flake
        // on a loaded CI box while still catching that regression.
        assert!(t0.elapsed().as_millis() < 50, "elapsed: {:?}", t0.elapsed());
        assert_eq!(label, "avro:3");
        assert!(preview.contains("\"id\":42"), "preview: {preview}");
        assert!(preview.len() < 4096, "preview should be bounded, got {} bytes", preview.len());
    }

    #[test]
    fn empty_and_null_bytes_are_unaffected_by_the_size_bound() {
        assert_eq!(bytes_display(None, None), ("—".into(), "<null>".into()));
        assert_eq!(bytes_display(Some(&[]), None), ("raw".into(), "<empty>".into()));
    }
}
