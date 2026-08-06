mod app;
mod backup;
mod constants;
mod deaddrop;
mod e2e;
mod protocol;
mod sam;
mod sam_runtime;
mod storage;
mod vault;

mod app_home;

use app::IcedCommApp;
use constants::APP_FONT_FAMILY;
use iced::{Font, Theme, application, window};

const INTER_FONT_BYTES: &[u8] = include_bytes!("../fonts/Inter-VariableFont_opsz,wght.ttf");
const MATERIAL_SYMBOLS_ROUNDED_BYTES: &[u8] =
    include_bytes!("../fonts/MaterialSymbolsRounded[FILL,GRAD,opsz,wght].ttf");

fn app_title(_: &IcedCommApp) -> String {
    String::from("IcedComm-I2P")
}

fn app_theme(_: &IcedCommApp) -> Theme {
    Theme::Dark
}

fn main() -> iced::Result {
    application(IcedCommApp::boot, IcedCommApp::update, IcedCommApp::view)
        .title(app_title)
        .theme(app_theme)
        .subscription(IcedCommApp::subscription)
        .exit_on_close_request(false)
        .window(window::Settings {
            min_size: Some(iced::Size::new(1280.0, 700.0)),
            ..Default::default()
        })
        .font(INTER_FONT_BYTES)
        .font(MATERIAL_SYMBOLS_ROUNDED_BYTES)
        .default_font(Font::with_name(APP_FONT_FAMILY))
        .run()
}
