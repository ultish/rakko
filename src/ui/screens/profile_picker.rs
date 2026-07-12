use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, ProfileCreateAuthChoice, ProfileCreateFocus, ProfileCreateState};
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
            app,
            main,
            &title,
            &items,
            Some(&["Name", "Bootstrap servers", "TLS"]),
            app.selected_profile_index,
            true,
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
    // mTLS adds 3 extra rows (CA/cert/key) over the base 4 fields — grow the dialog to
    // fit rather than clipping.
    let dialog = centered_rect(75, 85, area);
    frame.render_widget(Clear, dialog);

    let editing = state.is_edit();
    let title = if first_run {
        "Create a profile (required)"
    } else if editing {
        "Edit profile"
    } else {
        "Create a profile"
    };

    let show_ca = matches!(
        state.auth_choice,
        ProfileCreateAuthChoice::TlsCustomCa | ProfileCreateAuthChoice::Mtls
    );
    let show_client_cert = matches!(state.auth_choice, ProfileCreateAuthChoice::Mtls);

    let mut constraints = vec![
        Constraint::Length(2), // intro
        Constraint::Length(3), // name
        Constraint::Length(3), // bootstrap
        Constraint::Length(3), // auth
    ];
    if show_ca {
        constraints.push(Constraint::Length(3)); // CA path
    }
    if show_client_cert {
        constraints.push(Constraint::Length(3)); // cert path
        constraints.push(Constraint::Length(3)); // key path
    }
    constraints.push(Constraint::Length(3)); // schema registry
    constraints.push(Constraint::Min(2)); // hint/error
    constraints.push(Constraint::Length(2)); // footer

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
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
        "Auth",
        &state.display_with_cursor(ProfileCreateFocus::Auth),
        state.focus == ProfileCreateFocus::Auth,
    );

    let mut next_row = 4;
    if show_ca {
        render_field(
            frame,
            chunks[next_row],
            "CA path",
            &state.display_with_cursor(ProfileCreateFocus::CaPath),
            state.focus == ProfileCreateFocus::CaPath,
        );
        next_row += 1;
    }
    if show_client_cert {
        render_field(
            frame,
            chunks[next_row],
            "Client cert path",
            &state.display_with_cursor(ProfileCreateFocus::CertPath),
            state.focus == ProfileCreateFocus::CertPath,
        );
        next_row += 1;
        render_field(
            frame,
            chunks[next_row],
            "Client key path",
            &state.display_with_cursor(ProfileCreateFocus::KeyPath),
            state.focus == ProfileCreateFocus::KeyPath,
        );
        next_row += 1;
    }

    let sr_display = if state.schema_registry_url.is_empty()
        && state.focus != ProfileCreateFocus::SchemaRegistry
    {
        "(optional — leave empty if none)".to_string()
    } else {
        state.display_with_cursor(ProfileCreateFocus::SchemaRegistry)
    };
    render_field(
        frame,
        chunks[next_row],
        "Schema Registry URL",
        &sr_display,
        state.focus == ProfileCreateFocus::SchemaRegistry,
    );
    next_row += 1;

    if let Some(err) = &state.error {
        frame.render_widget(
            Paragraph::new(err.as_str())
                .style(ERROR_STYLE)
                .wrap(Wrap { trim: true }),
            chunks[next_row],
        );
    } else {
        let hint = if editing {
            "message_max_bytes and extra producer config are preserved. \
             Edit those in ~/.config/rakko/config.toml if needed."
        } else {
            "message_max_bytes / extra producer config: edit \
             ~/.config/rakko/config.toml after save."
        };
        frame.render_widget(
            Paragraph::new(hint)
                .style(STATUS_STYLE)
                .wrap(Wrap { trim: true }),
            chunks[next_row],
        );
    }
    next_row += 1;

    let footer = if first_run {
        "Enter: save & continue   Esc/q: quit"
    } else if editing {
        "Enter: save changes   Esc: cancel"
    } else {
        "Enter: save   Esc: cancel"
    };
    frame.render_widget(
        Paragraph::new(footer).style(Style::default().add_modifier(Modifier::BOLD)),
        chunks[next_row],
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


