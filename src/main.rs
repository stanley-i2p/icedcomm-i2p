mod app;
mod backup;
mod constants;
mod deaddrop;
mod e2e;
mod protocol;
mod sam;
mod storage;
mod vault;

mod app_home;

use app::TermchatApp;
use iced::{Font, Theme, application, window};

fn app_title(_: &TermchatApp) -> String {
    String::from("IcedComm-I2P")
}

fn app_theme(_: &TermchatApp) -> Theme {
    Theme::Dark
}

fn main() -> iced::Result {
    application(TermchatApp::boot, TermchatApp::update, TermchatApp::view)
        .title(app_title)
        .theme(app_theme)
        .subscription(TermchatApp::subscription)
        .exit_on_close_request(false)
        .window(window::Settings {
            min_size: Some(iced::Size::new(1280.0, 700.0)),
            ..Default::default()
        })
        .default_font(Font::MONOSPACE)
        .run()
}
