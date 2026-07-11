use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, ProfileCreateFocus, ProfileCreateState};
use crate::ui::theme::{ERROR_STYLE, STATUS_STYLE, TITLE_STYLE};
use crate::ui::widgets::confirm_dialog::centered_rect;
use crate::ui::widgets::footer::{render_keybind_footer, split_with_footer};
use crate::ui::widgets::table_nav::render_selectable_list;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let (main, footer) = split_with_footer(area);

    if app.config.profiles.is_empty() && app.profile_create.is_none() {
        let message = Paragraph::new(
            "No profiles configured.\n\nPress n to create one, or q to quit.",
        )
        .style(STATUS_STYLE)
        .wrap(Wrap { trim: true })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Profiles")
                .title_style(TITLE_STYLE),
        );
        frame.render_widget(message, main);
        render_keybind_footer(frame, footer, "n: new profile   q: quit");
    } else if !app.config.profiles.is_empty() {
        let items: Vec<Vec<String>> = app
            .config
            .profiles
            .iter()
            .map(|profile| {
                let tls = if profile.tls_enabled { "tls" } else { "plain" };
                vec![
                    profile.name.clone(),
                    profile.bootstrap_servers.clone(),
                    tls.to_string(),
                ]
            })
            .collect();

        let title = match &app.status_message {
            Some(status) => format!("Profiles — {status}"),
            None => "Profiles".to_string(),
        };

        render_selectable_list(
            frame,
            main,
            &title,
            &items,
            Some(&["Name", "Bootstrap servers", "TLS"]),
            app.selected_profile_index,
        );
        render_keybind_footer(
            frame,
            footer,
            "Enter: connect   n: new   e: edit   q: quit",
        );
    } else {
        // Empty + wizard open: dim background under the dialog.
        let message = Paragraph::new("Create your first connection profile.")
            .style(STATUS_STYLE)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Profiles")
                    .title_style(TITLE_STYLE),
            );
        frame.render_widget(message, main);
        render_keybind_footer(
            frame,
            footer,
            "Tab: fields   ←/→: cursor   Enter: save   Esc: quit",
        );
    }

    if let Some(state) = app.profile_create.as_ref() {
        // Dialog draws over main+footer; its own footer is inside the modal.
        render_create_dialog(frame, area, state, app.config.profiles.is_empty());
    }
}

fn render_create_dialog(frame: &mut Frame, area: Rect, state: &ProfileCreateState, first_run: bool) {
    let dialog = centered_rect(70, 70, area);
    frame.render_widget(Clear, dialog);

    let editing = state.is_edit();
    let title = if first_run {
        "Create a profile (required)"
    } else if editing {
        "Edit profile"
    } else {
        "Create a profile"
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(2),
            Constraint::Length(2),
        ])
        .margin(1)
        .split(dialog);

    frame.render_widget(
        Block::default()
            .borders(Borders::ALL)
            .title(title)
            .title_style(TITLE_STYLE),
        dialog,
    );

    let intro = if first_run {
        "No config yet — fill in the form and press Enter to save."
    } else {
        "Tab / Shift-Tab: fields   ←/→/Home/End: edit   Enter: save   Esc: cancel"
    };
    frame.render_widget(Paragraph::new(intro).style(STATUS_STYLE), chunks[0]);

    render_field(
        frame,
        chunks[1],
        "Name",
        &state.display_with_cursor(ProfileCreateFocus::Name),
        state.focus == ProfileCreateFocus::Name,
    );
    render_field(
        frame,
        chunks[2],
        "Bootstrap servers",
        &state.display_with_cursor(ProfileCreateFocus::Bootstrap),
        state.focus == ProfileCreateFocus::Bootstrap,
    );
    render_field(
        frame,
        chunks[3],
        "TLS enabled",
        &state.display_with_cursor(ProfileCreateFocus::Tls),
        state.focus == ProfileCreateFocus::Tls,
    );

    let sr_display = if state.schema_registry_url.is_empty()
        && state.focus != ProfileCreateFocus::SchemaRegistry
    {
        "(optional — leave empty if none)".to_string()
    } else {
        state.display_with_cursor(ProfileCreateFocus::SchemaRegistry)
    };
    render_field(
        frame,
        chunks[4],
        "Schema Registry URL",
        &sr_display,
        state.focus == ProfileCreateFocus::SchemaRegistry,
    );

    if let Some(err) = &state.error {
        frame.render_widget(
            Paragraph::new(err.as_str())
                .style(ERROR_STYLE)
                .wrap(Wrap { trim: true }),
            chunks[5],
        );
    } else {
        let hint = if editing {
            "Auth, message_max_bytes, and extra producer config are preserved. \
             Edit those in ~/.config/rakko/config.toml if needed."
        } else {
            "Auth defaults to none. For mTLS / message_max_bytes, edit \
             ~/.config/rakko/config.toml after save."
        };
        frame.render_widget(
            Paragraph::new(hint)
                .style(STATUS_STYLE)
                .wrap(Wrap { trim: true }),
            chunks[5],
        );
    }

    let footer = if first_run {
        "Enter: save & continue   Esc/q: quit"
    } else if editing {
        "Enter: save changes   Esc: cancel"
    } else {
        "Enter: save   Esc: cancel"
    };
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().add_modifier(Modifier::BOLD)),
        chunks[6],
    );
}

fn render_field(frame: &mut Frame, area: Rect, label: &str, value: &str, focused: bool) {
    let style = if focused {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
    };
    frame.render_widget(
        Paragraph::new(value).style(style).block(
            Block::default()
                .borders(Borders::ALL)
                .title(label)
                .title_style(TITLE_STYLE),
        ),
        area,
    );
}


