pub mod banner;
pub mod screens;
pub mod splash;
pub mod theme;
pub mod widgets;

use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::Frame;

use crate::app::{App, Screen};
use crate::ui::widgets::confirm_dialog::render_confirm_dialog;
use crate::ui::widgets::view_switcher;

/// Draws splash (first paint) or banner + active screen.
pub fn draw(frame: &mut Frame, app: &App) {
    app.clear_click_regions();
    let area = frame.area();

    if app.show_splash {
        splash::render(frame, area);
        // Quit confirm can still appear if the user hit `q` on the splash.
        if app.quit_confirm {
            render_quit_confirm(frame, area);
        }
        return;
    }

    let show_switcher = view_switcher::is_visible(app.screen);
    let mut constraints = vec![Constraint::Length(banner::BANNER_HEIGHT)];
    if show_switcher {
        constraints.push(Constraint::Length(1));
    }
    constraints.push(Constraint::Min(1));
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    banner::render(frame, app, chunks[0]);
    let content = if show_switcher {
        view_switcher::render(frame, app, chunks[1]);
        chunks[2]
    } else {
        chunks[1]
    };

    match app.screen {
        Screen::ProfilePicker => screens::profile_picker::render(frame, app, content),
        Screen::TopicList => screens::topic_list::render(frame, app, content),
        Screen::TopicDetail => screens::topic_detail::render(frame, app, content),
        Screen::GroupList => screens::group_list::render(frame, app, content),
        Screen::GroupDetail => screens::group_detail::render(frame, app, content),
        Screen::BrokerList => screens::broker_list::render(frame, app, content),
        Screen::BrokerDetail => screens::broker_detail::render(frame, app, content),
        Screen::Producer => screens::producer::render(frame, app, content),
        Screen::ExportImport => screens::export_import::render(frame, app, content),
    }

    if app.quit_confirm {
        render_quit_confirm(frame, area);
    }
}

fn render_quit_confirm(frame: &mut Frame, area: ratatui::layout::Rect) {
    render_confirm_dialog(
        frame,
        area,
        "Quit rakko?",
        "Exit the TUI?\n\ny/Enter: quit   n/Esc: cancel",
        None,
    );
}
