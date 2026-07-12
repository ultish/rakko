use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, GroupDetailState, OffsetResetPhase};
use crate::kafka::group_offsets::OffsetResetTarget;
use crate::ui::theme::{ERROR_STYLE, STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::confirm_dialog::render_confirm_dialog;
use crate::ui::widgets::footer::render_keybind_footer;
use crate::ui::widgets::table_nav::render_selectable_list;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let Some(detail) = app.group_detail.as_ref() else {
        let placeholder = Paragraph::new("No group selected.")
            .style(STATUS_STYLE)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Group")
                    .title_style(TITLE_STYLE),
            );
        frame.render_widget(placeholder, area);
        return;
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(if detail.members.is_empty() { 0 } else { 6 }),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(area);

    render_header(frame, chunks[0], detail);
    if !detail.members.is_empty() {
        render_members(frame, app, chunks[1], detail);
    }
    render_lag_table(frame, app, chunks[2], detail);
    render_footer(frame, chunks[3], detail);

    if let Some(phase) = &detail.reset_phase {
        render_reset_overlay(frame, area, detail, phase);
    }

    if let Some(status) = &app.status_message {
        // Surface load/reset status without stealing the main layout.
        let status_area = Rect {
            x: area.x,
            y: area.y + area.height.saturating_sub(2),
            width: area.width,
            height: 1,
        };
        frame.render_widget(Paragraph::new(status.as_str()).style(STATUS_STYLE), status_area);
    }
}

fn render_header(frame: &mut Frame, area: Rect, detail: &GroupDetailState) {
    let active = if detail.has_active_members {
        format!("  ⚠ {} active member(s)", detail.members.len())
    } else {
        "  (idle)".to_string()
    };
    let text = format!(
        "Group: {}  state: {}  total lag: {}{}",
        detail.name, detail.state, detail.total_lag, active
    );
    let style = if detail.has_active_members {
        ERROR_STYLE
    } else {
        TITLE_STYLE
    };
    frame.render_widget(
        Paragraph::new(text)
            .style(style)
            .block(Block::default().borders(Borders::ALL).title("Consumer group")),
        area,
    );
}

fn render_members(frame: &mut Frame, app: &App, area: Rect, detail: &GroupDetailState) {
    let items: Vec<Vec<String>> = detail
        .members
        .iter()
        .map(|m| vec![m.id.clone(), m.client_id.clone(), m.client_host.clone()])
        .collect();
    render_selectable_list(
        frame,
        app,
        area,
        "Members",
        &items,
        Some(&["Member id", "Client id", "Host"]),
        0,
        false,
    );
}

fn render_lag_table(frame: &mut Frame, app: &App, area: Rect, detail: &GroupDetailState) {
    if detail.lags.is_empty() {
        let message = Paragraph::new(
            "No committed offsets found for this group (it may never have consumed).",
        )
        .style(STATUS_STYLE)
        .wrap(Wrap { trim: true })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Partition lag")
                .title_style(TITLE_STYLE),
        );
        frame.render_widget(message, area);
        return;
    }

    let items: Vec<Vec<String>> = detail
        .lags
        .iter()
        .map(|lag| {
            vec![
                lag.topic.clone(),
                lag.partition.to_string(),
                lag.committed_offset
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "—".into()),
                lag.low_watermark.to_string(),
                lag.high_watermark.to_string(),
                lag.lag.map(|n| n.to_string()).unwrap_or_else(|| "—".into()),
            ]
        })
        .collect();

    render_selectable_list(
        frame,
        app,
        area,
        "Partition lag",
        &items,
        Some(&["Topic", "Partition", "Committed", "Low", "High", "Lag"]),
        detail.selected_index,
        true,
    );
}

fn render_footer(frame: &mut Frame, area: Rect, detail: &GroupDetailState) {
    let text = if detail.reset_phase.is_some() {
        "Offset reset in progress — follow the dialog"
    } else {
        "z: reset offsets   r: refresh lag (auto every 3s)   Esc: back"
    };
    render_keybind_footer(frame, area, text);
}

fn render_reset_overlay(frame: &mut Frame, area: Rect, detail: &GroupDetailState, phase: &OffsetResetPhase) {
    match phase {
        OffsetResetPhase::ChooseMode => {
            render_confirm_dialog(
                frame,
                area,
                "Reset offsets — choose target",
                "e: earliest   l: latest   o: absolute offset   t: timestamp (epoch ms)\n\nEsc/n: cancel",
                active_warning(detail),
            );
        }
        OffsetResetPhase::Input {
            target_kind,
            input,
            cursor,
        } => {
            let kind = match target_kind {
                crate::app::ResetInputKind::AbsoluteOffset => "absolute offset",
                crate::app::ResetInputKind::TimestampMillis => "timestamp (epoch ms)",
            };
            let field = crate::text_field::display_with_cursor(input, *cursor);
            render_confirm_dialog(
                frame,
                area,
                &format!("Reset offsets — enter {kind}"),
                &format!(
                    "value> {field}\n\n←/→/Home/End: cursor   Enter: continue   Esc: cancel"
                ),
                active_warning(detail),
            );
        }
        OffsetResetPhase::Confirm { target } => {
            let target_label = match target {
                OffsetResetTarget::Earliest => "earliest".to_string(),
                OffsetResetTarget::Latest => "latest".to_string(),
                OffsetResetTarget::Absolute(n) => format!("absolute offset {n}"),
                OffsetResetTarget::Timestamp(n) => format!("timestamp {n} ms"),
            };
            let body = format!(
                "Reset ALL {} partition offset(s) for group '{}' to {target_label}?\n\nThis is destructive.",
                detail.lags.len(),
                detail.name
            );
            render_confirm_dialog(
                frame,
                area,
                "Confirm offset reset",
                &body,
                active_warning(detail),
            );
        }
    }
}

fn active_warning(detail: &GroupDetailState) -> Option<&'static str> {
    if detail.has_active_members {
        Some(
            "WARNING: group has active members. Reset only works reliably when idle — \
             active consumers may re-commit and clobber this reset.",
        )
    } else {
        None
    }
}
