use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::widgets::{Block, Borders, Cell, Row, Table, TableState};
use ratatui::Frame;

use crate::app::App;
use crate::events::Action;

/// Matches `Table` default spacing between columns.
const COLUMN_SPACING: u16 = 1;
/// Matches our highlight symbol (`"> "`).
const HIGHLIGHT_SYMBOL: &str = "> ";

/// How many data rows fit in `area` for a table built by [`render_selectable_list`]
/// (borders + optional header). Used by the message browser to fill list-row
/// previews only for the viewport (+ overscan), not the entire buffer.
pub fn selectable_list_viewport_rows(area: Rect, has_header: bool) -> usize {
    let inner = Block::default().borders(Borders::ALL).inner(area);
    let header_h = u16::from(has_header);
    inner.height.saturating_sub(header_h) as usize
}

/// Scroll offset ratatui's `Table` derives when `TableState` starts at offset 0
/// and then keeps `selected` in view — matching [`render_selectable_list`], which
/// constructs a fresh `TableState` every frame.
pub fn selectable_list_offset(selected: usize, item_count: usize, viewport_rows: usize) -> usize {
    if viewport_rows == 0 || item_count == 0 {
        return 0;
    }
    let max_offset = item_count.saturating_sub(viewport_rows);
    if selected < viewport_rows {
        0
    } else {
        (selected + 1).saturating_sub(viewport_rows).min(max_offset)
    }
}

/// Renders `items` as a selectable table with the row at `selected` highlighted.
/// Shared by the profile picker, topic list, groups, and message browser.
///
/// Column widths are content-aware for metadata columns; one primary column
/// (`Value` / `Name` / …) uses `Fill` so it expands with the **terminal/area
/// width**. Cell text is truncated to the resolved column width so long values
/// use the remaining space instead of a fixed character cap.
///
/// `selectable` registers a `SelectRow(index)` click region per visible row when
/// true — set false for read-only informational tables (e.g. group-detail's
/// member list) that don't back a real selection cursor.
#[allow(clippy::too_many_arguments)]
pub fn render_selectable_list(
    frame: &mut Frame,
    app: &App,
    area: Rect,
    title: &str,
    items: &[Vec<String>],
    header: Option<&[&str]>,
    selected: usize,
    selectable: bool,
) {
    let column_count = header
        .map(<[&str]>::len)
        .or_else(|| items.first().map(Vec::len))
        .unwrap_or(1)
        .max(1);

    let constraints = auto_column_widths(column_count, header, items);
    // Space left after borders + selection gutter — this is what the table columns share.
    let highlight_w = HIGHLIGHT_SYMBOL.chars().count() as u16;
    let available = area
        .width
        .saturating_sub(2) // block borders
        .saturating_sub(highlight_w);
    let col_widths = resolve_column_widths(&constraints, available, COLUMN_SPACING);

    let rows = items.iter().map(|row| {
        let cells: Vec<Cell> = (0..column_count)
            .map(|i| {
                let raw = row.get(i).map(String::as_str).unwrap_or("");
                let max = col_widths.get(i).copied().unwrap_or(0) as usize;
                Cell::new(truncate_to_width(raw, max))
            })
            .collect();
        Row::new(cells)
    });

    // Header cells truncated the same way so titles don't force layout overflow.
    let header_row = header.map(|header_cells| {
        let cells: Vec<Cell> = header_cells
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let max = col_widths.get(i).copied().unwrap_or(0) as usize;
                Cell::new(truncate_to_width(name, max))
            })
            .collect();
        // Column headers: secondary cyan (static chrome, not purple).
        Row::new(cells).style(app.theme.title)
    });

    // Table chrome: cyan title, grey border, base body, purple selection only.
    let mut table = Table::new(rows, constraints)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .title_style(app.theme.title)
                .border_style(app.theme.border)
                .style(app.theme.root_style()),
        )
        .column_spacing(COLUMN_SPACING)
        .style(app.theme.text)
        .row_highlight_style(app.theme.selected_row)
        .highlight_symbol(HIGHLIGHT_SYMBOL);

    if let Some(header_row) = header_row {
        table = table.header(header_row);
    }

    let mut state = TableState::default();
    let clamped_selected = (!items.is_empty()).then(|| selected.min(items.len() - 1));
    state.select(clamped_selected);

    frame.render_stateful_widget(table, area, &mut state);

    if selectable && !items.is_empty() {
        register_row_interactions(
            frame,
            app,
            area,
            header.is_some(),
            items.len(),
            state.offset(),
            clamped_selected.unwrap_or(usize::MAX),
        );
    }
}

/// `state.offset()` (captured just before the render call above, which is where
/// `Table` computes it — auto-scrolling to keep the selection in view) plus the row
/// height (always 1; `render_selectable_list` never sets a custom `Row::height`) is
/// enough to derive each visible row's rect without duplicating `Table`'s internal
/// layout. Registers a `SelectRow` click per visible row and, separately, paints a
/// hover tint (skipping `selected`, which already has its own highlight style).
fn register_row_interactions(
    frame: &mut Frame,
    app: &App,
    area: Rect,
    has_header: bool,
    item_count: usize,
    offset: usize,
    selected: usize,
) {
    let inner = Block::default().borders(Borders::ALL).inner(area);
    let header_h = u16::from(has_header);
    let rows_y = inner.y + header_h;
    let rows_h = inner.height.saturating_sub(header_h);
    let visible = (rows_h as usize).min(item_count.saturating_sub(offset));
    for i in 0..visible {
        let row_index = offset + i;
        let y = rows_y + i as u16;
        app.register_click(inner.x, y, inner.width, 1, Action::SelectRow(row_index));
        if row_index != selected && app.is_hovered(inner.x, y, inner.width, 1) {
            frame
                .buffer_mut()
                .set_style(Rect { x: inner.x, y, width: inner.width, height: 1 }, app.theme.hover_row);
        }
    }
}

/// Resolve `Constraint`s into concrete column widths for `available` columns space.
fn resolve_column_widths(
    constraints: &[Constraint],
    available: u16,
    spacing: u16,
) -> Vec<u16> {
    let n = constraints.len();
    if n == 0 || available == 0 {
        return vec![0; n];
    }
    let spacing_total = spacing.saturating_mul((n as u16).saturating_sub(1));
    let inner = available.saturating_sub(spacing_total).max(1);
    Layout::horizontal(constraints.to_vec())
        .split(Rect {
            x: 0,
            y: 0,
            width: inner,
            height: 1,
        })
        .iter()
        .map(|r| r.width)
        .collect()
}

/// Truncate to at most `max` display characters, appending `…` when clipped.
fn truncate_to_width(text: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    let count = text.chars().count();
    if count <= max {
        return text.to_string();
    }
    if max == 1 {
        return "…".to_string();
    }
    let truncated: String = text.chars().take(max - 1).collect();
    format!("{truncated}…")
}

/// Size metadata columns to content (clamped); one primary column fills the rest.
///
/// The fill column is chosen by header name (Value / Name / Topic / …) so message
/// lists give space to Value and topic lists give space to Name — not equal splits.
fn auto_column_widths(
    column_count: usize,
    header: Option<&[&str]>,
    items: &[Vec<String>],
) -> Vec<Constraint> {
    if column_count == 1 {
        return vec![Constraint::Fill(1)];
    }

    let mut max_w = vec![0usize; column_count];
    if let Some(headers) = header {
        for (i, name) in headers.iter().enumerate().take(column_count) {
            max_w[i] = max_w[i].max(name.chars().count());
        }
    }
    for row in items {
        for (i, cell) in row.iter().enumerate().take(column_count) {
            // Cap contribution so a multi-KB value doesn't inflate metadata columns;
            // the fill column still expands via Constraint::Fill, not content length.
            let sample = cell.chars().count().min(48);
            max_w[i] = max_w[i].max(sample);
        }
    }

    let fill_idx = fill_column_index(column_count, header);

    let mut constraints = Vec::with_capacity(column_count);
    for (i, &content_w) in max_w.iter().enumerate() {
        if i == fill_idx {
            constraints.push(Constraint::Fill(1));
            continue;
        }
        // +1 gutter; keep metadata columns compact.
        let cap = column_cap(i, header);
        let width = (content_w + 1).clamp(3, cap) as u16;
        constraints.push(Constraint::Length(width));
    }
    constraints
}

/// Which column should expand. Prefer content-heavy fields by header name.
fn fill_column_index(column_count: usize, header: Option<&[&str]>) -> usize {
    const PREFERENCE: &[&str] = &[
        "value",
        "bootstrap servers",
        "name",
        "topic",
        "member id",
        "key",
        "host",
    ];
    if let Some(headers) = header {
        for pref in PREFERENCE {
            if let Some(i) = headers
                .iter()
                .position(|h| h.eq_ignore_ascii_case(pref))
            {
                if i < column_count {
                    return i;
                }
            }
        }
    }
    column_count - 1
}

/// Max width for non-fill columns, keyed by header name when available.
fn column_cap(index: usize, header: Option<&[&str]>) -> usize {
    if let Some(headers) = header {
        if let Some(name) = headers.get(index) {
            let lower = name.to_ascii_lowercase();
            return match lower.as_str() {
                "p" => 4,
                "partition" => 12,
                "offset" | "committed" | "low" | "high" | "lag" => 14,
                "fmt" | "format" | "kfmt" | "vfmt" => 12,
                "key" => 28,
                "tls" => 6,
                "state" => 14,
                "members" | "member" => 10,
                "partitions" | "replication" | "messages" => 12,
                "compression" | "protocol" => 14,
                "client id" => 20,
                _ => 24,
            };
        }
    }
    24
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selectable_list_offset_stays_zero_while_selection_fits() {
        assert_eq!(selectable_list_offset(0, 100, 20), 0);
        assert_eq!(selectable_list_offset(19, 100, 20), 0);
    }

    #[test]
    fn selectable_list_offset_scrolls_to_keep_selection_visible() {
        // Fresh TableState offset=0 → selected past the bottom → offset grows.
        assert_eq!(selectable_list_offset(20, 100, 20), 1);
        assert_eq!(selectable_list_offset(50, 100, 20), 31);
        assert_eq!(selectable_list_offset(99, 100, 20), 80);
    }

    #[test]
    fn selectable_list_offset_handles_empty_and_short_lists() {
        assert_eq!(selectable_list_offset(0, 0, 20), 0);
        assert_eq!(selectable_list_offset(3, 5, 20), 0);
        assert_eq!(selectable_list_offset(0, 10, 0), 0);
    }

    #[test]
    fn message_list_widths_keep_value_as_fill() {
        let header = ["P", "Offset", "KFmt", "Key", "VFmt", "Value"];
        let items = vec![
            vec![
                "0".into(),
                "123456".into(),
                "raw".into(),
                "user-id-abc".into(),
                "json".into(),
                r#"{"hello":"world"}"#.into(),
            ],
            vec![
                "12".into(),
                "99".into(),
                "json".into(),
                r#"{"id":1}"#.into(),
                "avro:3".into(),
                "payload".into(),
            ],
        ];
        let widths = auto_column_widths(6, Some(&header), &items);
        assert_eq!(widths.len(), 6);
        assert!(matches!(widths[5], Constraint::Fill(1)));
        assert!(matches!(widths[0], Constraint::Length(w) if w <= 5));
        assert!(matches!(widths[1], Constraint::Length(w) if (3..=15).contains(&w)));
        assert!(matches!(widths[3], Constraint::Length(_)));
        assert!(matches!(widths[2], Constraint::Length(w) if w <= 12));
        assert!(matches!(widths[4], Constraint::Length(w) if w <= 12));
    }

    #[test]
    fn topic_list_name_column_fills() {
        let header = ["Name", "Partitions", "Replication", "Compression", "Messages"];
        let items = vec![vec![
            "orders.events".into(),
            "12".into(),
            "3".into(),
            "zstd".into(),
            "1000".into(),
        ]];
        let widths = auto_column_widths(5, Some(&header), &items);
        assert!(matches!(widths[0], Constraint::Fill(1)));
        assert!(matches!(widths[1], Constraint::Length(_)));
    }

    #[test]
    fn single_column_is_fill() {
        let widths = auto_column_widths(1, Some(&["Name"]), &[vec!["a".into()]]);
        assert!(matches!(widths.as_slice(), [Constraint::Fill(1)]));
    }

    #[test]
    fn resolve_gives_remaining_space_to_fill_column() {
        let header = ["P", "Offset", "KFmt", "Key", "VFmt", "Value"];
        let items = [vec![
            "0".into(),
            "1".into(),
            "raw".into(),
            "k".into(),
            "json".into(),
            "short".into(),
        ]];
        let constraints = auto_column_widths(6, Some(&header), &items);
        let resolved = resolve_column_widths(&constraints, 100, COLUMN_SPACING);
        assert_eq!(resolved.len(), 6);
        // Value (last / Fill) should be the widest by a clear margin on a 100-col budget.
        let value_w = resolved[5];
        assert!(value_w >= 40, "value width {value_w} should use remaining terminal space");
        let fixed_sum: u16 = resolved[..5].iter().sum();
        let spacing = COLUMN_SPACING * 5;
        assert_eq!(fixed_sum + spacing + value_w, 100);
    }

    #[test]
    fn group_detail_partition_header_is_not_truncated_when_room_is_available() {
        // Distinct from the message list's single-letter "P" column (capped at 4) —
        // the group-detail lag table spells out "Partition" and needs room for it.
        let header = ["Topic", "Partition", "Committed", "Low", "High", "Lag"];
        let items = vec![vec![
            "orders.events".into(),
            "3".into(),
            "1000".into(),
            "0".into(),
            "1050".into(),
            "50".into(),
        ]];
        let constraints = auto_column_widths(6, Some(&header), &items);
        let resolved = resolve_column_widths(&constraints, 120, COLUMN_SPACING);
        // "Partition" is 9 chars; the resolved width must fit it without truncation.
        assert!(
            resolved[1] as usize > "Partition".chars().count(),
            "partition column width {} too narrow for header text",
            resolved[1]
        );
    }

    #[test]
    fn truncate_uses_full_width_and_ellipsis() {
        assert_eq!(truncate_to_width("hello", 10), "hello");
        assert_eq!(truncate_to_width("hello world", 8), "hello w…");
        assert_eq!(truncate_to_width("ab", 1), "…");
    }
}
