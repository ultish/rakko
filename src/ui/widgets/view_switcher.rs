//! Persistent top-level view switcher bar (M10's `1`/`2`/`3` bindings), rendered in the
//! same place on every list-level screen so the switch mechanism is visually consistent
//! rather than restated differently in each screen's footer.

use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::Frame;

use crate::app::{App, Screen};
use crate::events::Action;
use crate::ui::theme::{SELECTED_ROW_STYLE, STATUS_STYLE};

const ENTRIES: [(&str, &str, Action); 3] = [
    ("1", "Topics", Action::SwitchToTopics),
    ("2", "Groups", Action::SwitchToGroups),
    ("3", "Brokers", Action::SwitchToBrokers),
];
/// Column gap between switcher entries — matches the `"   "` spacer pushed between
/// spans in `render` below.
const GAP: u16 = 3;

/// Which of the three switcher entries (if any) `screen` belongs to.
fn active_index(screen: Screen) -> Option<usize> {
    match screen {
        Screen::TopicList | Screen::TopicDetail => Some(0),
        Screen::GroupList | Screen::GroupDetail => Some(1),
        Screen::BrokerList | Screen::BrokerDetail => Some(2),
        _ => None,
    }
}

/// Whether `screen` shows the switcher bar at all — the same set of screens where the
/// `1`/`2`/`3` keys in `main.rs`'s `key_to_action` are actually wired up.
pub fn is_visible(screen: Screen) -> bool {
    active_index(screen).is_some()
}

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let Some(active) = active_index(app.screen) else {
        return;
    };

    let mut spans = Vec::new();
    let mut x = area.x;
    for (i, (key, label, action)) in ENTRIES.iter().enumerate() {
        if i > 0 {
            spans.push(Span::raw(" ".repeat(GAP as usize)));
            x += GAP;
        }
        let style = if i == active { SELECTED_ROW_STYLE } else { STATUS_STYLE };
        let text = format!("{key} {label}");
        let width = text.chars().count() as u16;
        spans.push(Span::styled(text, style));
        app.register_click(x, area.y, width, 1, action.clone());
        x += width;
    }
    frame.render_widget(Paragraph::new(Line::from(spans)).style(Style::default()), area);
}
