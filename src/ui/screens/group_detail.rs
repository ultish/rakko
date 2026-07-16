use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::symbols::Marker;
use ratatui::widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, GroupDetailState, OffsetResetPhase};
use crate::kafka::group_offsets::OffsetResetTarget;
use crate::ui::widgets::confirm_dialog::render_confirm_dialog;
use crate::ui::widgets::footer::render_keybind_footer;
use crate::ui::widgets::table_nav::render_selectable_list;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let Some(detail) = app.group_detail.as_ref() else {
        let placeholder = Paragraph::new("No group selected.")
            .style(app.theme.status)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Group")
                    .title_style(app.theme.title),
            );
        frame.render_widget(placeholder, area);
        return;
    };

    // Header grows to fit a 2-row trend sparkline underneath the summary text (1 text
    // row + 2 sparkline rows + 2 border rows = 5) once there are at least 2 lag
    // samples — a single sample has nothing to trend, so skip the extra rows until
    // there's something to show.
    let header_height = if detail.lag_history.len() >= 2 { 5 } else { 3 };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(header_height),
            Constraint::Length(if detail.members.is_empty() { 0 } else { 6 }),
            Constraint::Min(3),
            Constraint::Length(1),
        ])
        .split(area);

    render_header(frame, app, chunks[0], detail);
    if !detail.members.is_empty() {
        render_members(frame, app, chunks[1], detail);
    }
    render_lag_table(frame, app, chunks[2], detail);
    render_footer(frame, app, chunks[3], detail);

    if let Some(phase) = &detail.reset_phase {
        render_reset_overlay(frame, app, area, detail, phase);
    }

    if let Some(status) = &app.status_message {
        // Surface load/reset status without stealing the main layout.
        let status_area = Rect {
            x: area.x,
            y: area.y + area.height.saturating_sub(2),
            width: area.width,
            height: 1,
        };
        frame.render_widget(Paragraph::new(status.as_str()).style(app.theme.status), status_area);
    }
}

fn render_header(frame: &mut Frame, app: &App, area: Rect, detail: &GroupDetailState) {
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
        app.theme.error
    } else {
        // Summary line is secondary info chrome, not a panel title.
        app.theme.secondary
    };
    let block = Block::default().borders(Borders::ALL).title("Consumer group");
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if detail.lag_history.len() >= 2 {
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(2)])
            .split(inner);
        frame.render_widget(Paragraph::new(text).style(style), rows[0]);
        render_lag_sparkline(frame, rows[1], detail);
    } else {
        frame.render_widget(Paragraph::new(text).style(style), inner);
    }
}

/// Braille-marker line plot rather than `Sparkline`'s block glyphs — a braille cell packs a
/// 2x4 dot grid, giving noticeably finer height/width resolution than the 8-level blocks a
/// `Sparkline` is limited to at this widget's height.
fn render_lag_sparkline(frame: &mut Frame, area: Rect, detail: &GroupDetailState) {
    // total_lag shouldn't go negative in practice (it's a watermark difference), but the
    // type is i64 — clamp defensively rather than plot a bogus negative point.
    let points: Vec<(f64, f64)> = detail
        .lag_history
        .iter()
        .enumerate()
        .map(|(i, &lag)| (i as f64, lag.max(0) as f64))
        .collect();
    let max_x = (points.len() - 1) as f64;
    let max_y = points.iter().map(|(_, y)| *y).fold(0.0_f64, f64::max).max(1.0);

    let dataset = Dataset::default()
        .marker(Marker::Braille)
        .graph_type(GraphType::Line)
        .style(Style::new().cyan())
        .data(&points);

    let chart = Chart::new(vec![dataset])
        .x_axis(Axis::default().bounds([0.0, max_x]))
        .y_axis(Axis::default().bounds([0.0, max_y]));
    frame.render_widget(chart, area);
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
        .style(app.theme.status)
        .wrap(Wrap { trim: true })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Partition lag")
                .title_style(app.theme.title),
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

fn render_footer(frame: &mut Frame, app: &App, area: Rect, detail: &GroupDetailState) {
    let text = if detail.reset_phase.is_some() {
        "Offset reset in progress — follow the dialog"
    } else {
        "z: reset offsets   r: refresh lag (auto every 3s)   Esc: back"
    };
    render_keybind_footer(frame, area, &app.theme, text);
}

fn render_reset_overlay(frame: &mut Frame, app: &App, area: Rect, detail: &GroupDetailState, phase: &OffsetResetPhase) {
    match phase {
        OffsetResetPhase::ChooseMode => {
            render_confirm_dialog(frame, area, &app.theme, "Reset offsets — choose target",
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
                &app.theme,
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
            render_confirm_dialog(frame, area, &app.theme, "Confirm offset reset",
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
