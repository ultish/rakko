//! Settings screen — edit `seek_page_size` and `[ui]` (`theme`, `banner_mode`), the
//! only editable `Config` fields. Profiles are managed on the profile picker instead.

use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use crate::app::{App, SettingId, SettingKind, SettingsEditorState};
use crate::ui::widgets::confirm_dialog::centered_rect_fixed_height;
use crate::ui::widgets::footer::{render_keybind_footer, split_with_footer};
use crate::ui::widgets::table_nav::render_selectable_list;

pub fn render(frame: &mut Frame, app: &App, area: Rect) {
    let (main, footer) = split_with_footer(area);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(main);

    render_settings_list(frame, app, chunks[0]);
    render_settings_detail(frame, app, chunks[1]);

    let footer_text = if app.settings_editor.is_some() {
        "type value   Enter: save   Esc: cancel"
    } else {
        "j/k: move   Enter/Space: edit/cycle   ←/→: nudge   1/2/3: other tabs   ?: help   Esc: back"
    };
    render_keybind_footer(frame, footer, &app.theme, footer_text);

    if let Some(ed) = app.settings_editor.as_ref() {
        render_settings_editor(frame, app, area, ed);
    }
}

fn render_settings_list(frame: &mut Frame, app: &App, area: Rect) {
    let items: Vec<Vec<String>> = SettingId::ALL
        .iter()
        .map(|id| vec![id.key().to_string(), app.setting_display_value(*id)])
        .collect();

    let title = match &app.status_message {
        Some(status) => format!("Settings — {status}"),
        None => "Settings — config.toml".to_string(),
    };

    render_selectable_list(
        frame,
        app,
        area,
        &title,
        &items,
        Some(&["Key", "Value"]),
        app.selected_setting,
        true,
    );
}

fn render_settings_detail(frame: &mut Frame, app: &App, area: Rect) {
    let Some(&id) = SettingId::ALL.get(app.selected_setting) else {
        return;
    };
    let value = app.setting_display_value(id);
    let action = match id.kind() {
        SettingKind::Cycle => "Enter/Space: cycle",
        SettingKind::Int => "Enter: edit · ←/→: nudge",
    };

    let lines = vec![
        Line::from(Span::styled(id.key(), app.theme.accent.add_modifier(Modifier::BOLD))),
        Line::from(""),
        Line::from(vec![
            Span::styled("Value  ", app.theme.dim),
            Span::styled(value, app.theme.text.add_modifier(Modifier::BOLD)),
        ]),
        Line::from(""),
        Line::from(Span::styled(id.help(), app.theme.status)),
        Line::from(""),
        Line::from(Span::styled(action, app.theme.secondary)),
        Line::from(""),
        Line::from(Span::styled("Profiles live on the profile picker.", app.theme.dim)),
        Line::from(Span::styled("Saves immediately to config.toml.", app.theme.dim)),
        Line::from(""),
        Line::from(Span::styled(
            format!("File: {}", app.config_path.display()),
            app.theme.dim,
        )),
    ];

    frame.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: false }).block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Detail ")
                .title_style(app.theme.title)
                .border_style(app.theme.border)
                .style(app.theme.root_style()),
        ),
        area,
    );
}

fn render_settings_editor(frame: &mut Frame, app: &App, area: Rect, ed: &SettingsEditorState) {
    let dialog = centered_rect_fixed_height(72, 11, area);
    frame.render_widget(Clear, dialog);

    let title = format!(" Edit {} ", ed.id.key());
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .title_style(app.theme.title)
        .border_style(app.theme.secondary.add_modifier(Modifier::BOLD))
        .style(app.theme.panel_style());
    let inner = block.inner(dialog);
    frame.render_widget(block, dialog);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Length(3), Constraint::Min(2), Constraint::Length(1)])
        .split(inner);

    frame.render_widget(
        Paragraph::new(ed.id.help()).style(app.theme.status).wrap(Wrap { trim: true }),
        chunks[0],
    );

    frame.render_widget(
        Paragraph::new(ed.display_with_cursor()).style(app.theme.text).block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Value ")
                .title_style(app.theme.focus_title(true))
                .border_style(app.theme.focus_border(true)),
        ),
        chunks[1],
    );

    if let Some(err) = &ed.error {
        frame.render_widget(
            Paragraph::new(err.as_str()).style(app.theme.error.add_modifier(Modifier::BOLD)),
            chunks[2],
        );
    }

    frame.render_widget(
        Paragraph::new("Enter: save     Esc: cancel")
            .style(app.theme.secondary.add_modifier(Modifier::BOLD))
            .alignment(Alignment::Center),
        chunks[3],
    );
}
