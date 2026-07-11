use ratatui::layout::{Constraint, Rect};
use ratatui::widgets::{Block, Borders, Cell, Row, Table, TableState};
use ratatui::Frame;

use crate::ui::theme::{SELECTED_ROW_STYLE, TITLE_STYLE};

/// Renders `items` as a selectable table with the row at `selected` highlighted.
/// Shared by the profile picker and topic list so neither screen hand-rolls its own
/// table styling.
pub fn render_selectable_list(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    items: &[Vec<String>],
    header: Option<&[&str]>,
    selected: usize,
) {
    let column_count = header
        .map(<[&str]>::len)
        .or_else(|| items.first().map(Vec::len))
        .unwrap_or(1);
    let widths = vec![Constraint::Fill(1); column_count.max(1)];

    let rows = items
        .iter()
        .map(|row| Row::new(row.iter().map(|cell| Cell::new(cell.as_str()))));

    let mut table = Table::new(rows, widths)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .title_style(TITLE_STYLE),
        )
        .row_highlight_style(SELECTED_ROW_STYLE)
        .highlight_symbol("> ");

    if let Some(header_cells) = header {
        table = table.header(Row::new(header_cells.iter().copied()).style(TITLE_STYLE));
    }

    let mut state = TableState::default();
    if !items.is_empty() {
        state.select(Some(selected.min(items.len() - 1)));
    }

    frame.render_stateful_widget(table, area, &mut state);
}
