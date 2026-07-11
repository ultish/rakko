pub mod screens;
pub mod theme;
pub mod widgets;

use ratatui::Frame;

use crate::app::{App, Screen};

/// Dispatches to the active screen's render function based on `app.screen`.
pub fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();
    match app.screen {
        Screen::ProfilePicker => screens::profile_picker::render(frame, app, area),
        Screen::TopicList => screens::topic_list::render(frame, app, area),
        Screen::TopicDetail => screens::topic_detail::render(frame, app, area),
    }
}
