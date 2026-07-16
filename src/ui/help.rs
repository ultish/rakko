//! Global help overlay (`?`) built from a shared per-screen keybind table.
//!
//! Footers stay short; this is the full reference. Keep entries here in sync
//! with `main.rs` key routing — when you add a keybind, add a row.

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, Screen};
use crate::ui::widgets::confirm_dialog::centered_rect;

pub struct KeybindEntry {
    pub keys: &'static str,
    pub description: &'static str,
}

pub struct KeybindSection {
    pub title: &'static str,
    pub entries: &'static [KeybindEntry],
}

const GLOBAL: KeybindSection = KeybindSection {
    title: "Global",
    entries: &[
        KeybindEntry {
            keys: "?",
            description: "Toggle this help",
        },
        KeybindEntry {
            keys: "q",
            description: "Quit (confirm)",
        },
        KeybindEntry {
            keys: "Ctrl-c",
            description: "Force quit",
        },
        KeybindEntry {
            keys: "A",
            description: "Cycle banner (wave → ms/frame → fps → off); saved to config",
        },
        KeybindEntry {
            keys: "T",
            description: "Cycle theme (dark ↔ light); saved to config",
        },
        KeybindEntry {
            keys: "Ctrl/Cmd+V",
            description: "Paste clipboard into focused text field",
        },
        KeybindEntry {
            keys: "j/k · ↑/↓",
            description: "Move selection",
        },
        KeybindEntry {
            keys: "Enter",
            description: "Confirm / open",
        },
        KeybindEntry {
            keys: "Esc",
            description: "Back / cancel",
        },
        KeybindEntry {
            keys: "1/2/3",
            description: "Jump Topics / Groups / Brokers",
        },
    ],
};

const PROFILE_PICKER: KeybindSection = KeybindSection {
    title: "Profile picker",
    entries: &[
        KeybindEntry {
            keys: "n",
            description: "New profile",
        },
        KeybindEntry {
            keys: "e",
            description: "Edit selected profile",
        },
        KeybindEntry {
            keys: "z",
            description: "Delete selected profile (confirm)",
        },
        KeybindEntry {
            keys: "Enter",
            description: "Connect with selected profile",
        },
    ],
};

const TOPIC_LIST: KeybindSection = KeybindSection {
    title: "Topic list",
    entries: &[
        KeybindEntry {
            keys: "/",
            description: "Filter topics by name",
        },
        KeybindEntry {
            keys: "c",
            description: "Clear applied filter",
        },
        KeybindEntry {
            keys: "r",
            description: "Refresh topic list",
        },
        KeybindEntry {
            keys: "Enter",
            description: "Open topic (message browser)",
        },
    ],
};

const TOPIC_DETAIL: KeybindSection = KeybindSection {
    title: "Message browser",
    entries: &[
        KeybindEntry {
            keys: "Tab · s",
            description: "Toggle page (seek) ↔ live tail (opens in page mode)",
        },
        KeybindEntry {
            keys: "n/p · PgDn/PgUp",
            description: "Page mode: next / previous page",
        },
        KeybindEntry {
            keys: "/",
            description: "Substring filter",
        },
        KeybindEntry {
            keys: "Q",
            description: "Structured query filter (JSON/Avro fields)",
        },
        KeybindEntry {
            keys: "c",
            description: "Clear applied filter(s)",
        },
        KeybindEntry {
            keys: "o",
            description: "Toggle sort (newest↑ / oldest↑)",
        },
        KeybindEntry {
            keys: "r",
            description: "Refresh seek page / restart tail",
        },
        KeybindEntry {
            keys: "Enter",
            description: "Open message inspector",
        },
        KeybindEntry {
            keys: "V",
            description: "Copy value to clipboard",
        },
        KeybindEntry {
            keys: "K",
            description: "Copy key to clipboard",
        },
        KeybindEntry {
            keys: "Y",
            description: "Copy topic:partition@offset",
        },
        KeybindEntry {
            keys: "y",
            description: "Replay message (same topic, raw bytes)",
        },
        KeybindEntry {
            keys: "x / X",
            description: "Export selected / all visible (JSONL)",
        },
        KeybindEntry {
            keys: "i",
            description: "Import JSONL into this topic",
        },
        KeybindEntry {
            keys: "w",
            description: "Open producer for this topic",
        },
    ],
};

const INSPECTOR: KeybindSection = KeybindSection {
    title: "Message inspector",
    entries: &[
        KeybindEntry {
            keys: "Tab · click",
            description: "Focus key / headers / value panel",
        },
        KeybindEntry {
            keys: "j/k · PgUp/PgDn",
            description: "Scroll focused panel",
        },
        KeybindEntry {
            keys: "←/→",
            description: "Resize focused panel pair",
        },
        KeybindEntry {
            keys: "V / K / Y",
            description: "Copy value / key / offset (same as list)",
        },
        KeybindEntry {
            keys: "Enter · Esc",
            description: "Close inspector",
        },
    ],
};

const GROUP_LIST: KeybindSection = KeybindSection {
    title: "Consumer groups",
    entries: &[
        KeybindEntry {
            keys: "/",
            description: "Filter groups by name",
        },
        KeybindEntry {
            keys: "c",
            description: "Clear applied filter",
        },
        KeybindEntry {
            keys: "r",
            description: "Refresh group list",
        },
        KeybindEntry {
            keys: "Enter",
            description: "Open group detail (lag)",
        },
    ],
};

const GROUP_DETAIL: KeybindSection = KeybindSection {
    title: "Group detail",
    entries: &[
        KeybindEntry {
            keys: "r",
            description: "Refresh lag",
        },
        KeybindEntry {
            keys: "z",
            description: "Reset offsets (confirm; destructive)",
        },
    ],
};

const BROKER_LIST: KeybindSection = KeybindSection {
    title: "Brokers",
    entries: &[
        KeybindEntry {
            keys: "r",
            description: "Refresh broker list / health",
        },
        KeybindEntry {
            keys: "Enter",
            description: "Open broker config (non-default)",
        },
    ],
};

const BROKER_DETAIL: KeybindSection = KeybindSection {
    title: "Broker detail",
    entries: &[
        KeybindEntry {
            keys: "r",
            description: "Refresh broker config",
        },
    ],
};

const PRODUCER: KeybindSection = KeybindSection {
    title: "Producer",
    entries: &[
        KeybindEntry {
            keys: "Tab",
            description: "Next field",
        },
        KeybindEntry {
            keys: "F2 · Ctrl-p",
            description: "Submit (produce)",
        },
        KeybindEntry {
            keys: "F3 · Ctrl-m",
            description: "Cycle input mode (inline / file / $EDITOR)",
        },
        KeybindEntry {
            keys: "Ctrl/Cmd+V",
            description: "Paste into focused key / value / path field",
        },
        KeybindEntry {
            keys: "Esc",
            description: "Back to topic",
        },
    ],
};

const EXPORT_IMPORT: KeybindSection = KeybindSection {
    title: "Export / import",
    entries: &[
        KeybindEntry {
            keys: "Tab",
            description: "Next field (import: path ↔ topic)",
        },
        KeybindEntry {
            keys: "Enter",
            description: "Run export or import",
        },
        KeybindEntry {
            keys: "Esc",
            description: "Cancel",
        },
    ],
};

/// Sections shown for the current screen (global always first).
pub fn sections_for(app: &App) -> Vec<&'static KeybindSection> {
    let mut sections = vec![&GLOBAL];
    match app.screen {
        Screen::ProfilePicker => sections.push(&PROFILE_PICKER),
        Screen::TopicList => sections.push(&TOPIC_LIST),
        Screen::TopicDetail => {
            sections.push(&TOPIC_DETAIL);
            if app
                .topic_detail
                .as_ref()
                .is_some_and(|d| d.message_view.is_some())
            {
                sections.push(&INSPECTOR);
            }
        }
        Screen::GroupList => sections.push(&GROUP_LIST),
        Screen::GroupDetail => sections.push(&GROUP_DETAIL),
        Screen::BrokerList => sections.push(&BROKER_LIST),
        Screen::BrokerDetail => sections.push(&BROKER_DETAIL),
        Screen::Producer => sections.push(&PRODUCER),
        Screen::ExportImport => sections.push(&EXPORT_IMPORT),
    }
    sections
}

/// Full-screen dimmed overlay listing keybinds for the active screen.
pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let dialog = centered_rect(78, 80, area);
    frame.render_widget(Clear, dialog);

    let theme = &app.theme;
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(
            " Help — {}  (theme: {}) ",
            screen_label(app.screen),
            theme.name.label()
        ))
        .title_style(theme.title)
        .border_style(theme.border)
        .style(theme.root_style().bg(theme.bg_panel));
    let inner = block.inner(dialog);
    frame.render_widget(block, dialog);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1)])
        .split(inner);

    let mut lines: Vec<Line> = Vec::new();
    for section in sections_for(app) {
        lines.push(Line::from(Span::styled(
            section.title,
            theme.title.add_modifier(Modifier::UNDERLINED),
        )));
        for entry in section.entries {
            lines.push(Line::from(vec![
                // Keys + descriptions both secondary cyan (purple only on selection/focus).
                Span::styled(format!("  {:<18}", entry.keys), theme.secondary.add_modifier(Modifier::BOLD)),
                Span::styled(entry.description, theme.dim),
            ]));
        }
        lines.push(Line::from(""));
    }

    frame.render_widget(
        Paragraph::new(lines)
            .style(theme.text)
            .wrap(Wrap { trim: false }),
        chunks[0],
    );
    // Footer chrome: secondary cyan (same role as keybind footers).
    frame.render_widget(
        Paragraph::new("?: close   Esc: close").style(theme.secondary),
        chunks[1],
    );
}

fn screen_label(screen: Screen) -> &'static str {
    match screen {
        Screen::ProfilePicker => "Profile picker",
        Screen::TopicList => "Topics",
        Screen::TopicDetail => "Messages",
        Screen::GroupList => "Groups",
        Screen::GroupDetail => "Group detail",
        Screen::BrokerList => "Brokers",
        Screen::BrokerDetail => "Broker detail",
        Screen::Producer => "Producer",
        Screen::ExportImport => "Export / import",
    }
}
