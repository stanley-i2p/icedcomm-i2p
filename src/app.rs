use crate::constants::{
    APP_FONT_FAMILY, APP_NAME, APP_VERSION, DEFAULT_SAM_HOST, DEFAULT_SAM_PORT,
    MAX_ACTIVE_DEADDROP_REPLICAS, MAX_FILE_SIZE,
};
use crate::deaddrop::{DeadDropClient, DeaddropOpStat};
use crate::e2e::E2E;
use crate::protocol::{Frame, MsgType};
use crate::sam::{AcceptedIncoming, LiveConnection, SamClient, SamInitResult};
use iced::border;
use iced::widget::Id as ScrollableId;
use iced::widget::operation;
use iced::widget::{
    Space, button, column, container, image, progress_bar, row, scrollable, stack, text, text_input,
};
use iced::{
    Alignment, Background, Color, ContentFit, Element, Font, Length, Subscription, Task, exit,
    font, futures::SinkExt, stream, time, window,
};
use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};

use std::time::Duration;
use tokio::time::sleep;

use crate::storage::{self, AppLock, ContactMeta, OfflineState};

use base64::{Engine as _, engine::general_purpose};
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

use arboard::Clipboard;

use rand::random;

use sha2::Digest;
use tokio::sync::Mutex as TokioMutex;
const DEADDROP_POLL_INTERVAL_MS: u64 = 5_000;

const PY_GREEN: Color = Color::from_rgb8(105, 200, 0); // Vibrant Lime
const PY_GREEN50: Color = Color::from_rgb8(52, 100, 0);
const PY_CYAN: Color = Color::from_rgb8(0, 200, 200); // Electric Cyan
const PY_CYAN30: Color = Color::from_rgb8(0, 140, 140);
const PY_YELLOW: Color = Color::from_rgb8(220, 120, 0); // Pure Yellow
const PY_MAGENTA: Color = Color::from_rgb8(200, 0, 200); // Bright Fuchsia
const PY_RED: Color = Color::from_rgb8(200, 0, 0); // Pure Red
const PY_GREY62: Color = Color::from_rgb8(150, 150, 150); // Lighter Grey
const PY_GREY_SYS: Color = Color::from_rgb8(100, 100, 110);

const APP_BUTTON_BG: Color = Color::from_rgb8(0x58, 0x65, 0xF2);
const APP_BUTTON_HOVER_BG: Color = Color {
    r: 0.421005309,
    g: 0.460214496,
    b: 0.999999940,
    a: 1.0,
};
const APP_BUTTON_PRESSED_BG: Color = APP_BUTTON_BG;
const APP_BUTTON_DISABLED_BG: Color = Color {
    r: 0.345098048,
    g: 0.396078438,
    b: 0.949019611,
    a: 0.5,
};
const APP_BUTTON_TEXT: Color = Color {
    r: 0.979898274,
    g: 0.980632722,
    b: 0.997532547,
    a: 1.0,
};
const APP_BUTTON_DISABLED_TEXT: Color = Color {
    r: 0.979898274,
    g: 0.980632722,
    b: 0.997532547,
    a: 0.5,
};

// const APP_TAB_SELECTED_BORDER: Color = Color::from_rgb8(50, 90, 140);
// const APP_TAB_UNSELECTED_BORDER: Color = Color::from_rgb8(35, 35, 40);
// const APP_TAB_HOVER_BORDER: Color = Color::from_rgb8(80, 120, 220);
// const APP_TAB_PRESSED_BORDER: Color = APP_TAB_SELECTED_BORDER;
// const APP_TAB_TEXT: Color = Color::WHITE;
// const APP_TAB_DISABLED_TEXT: Color = APP_BUTTON_DISABLED_TEXT;
// const APP_TAB_BORDER_WIDTH: f32 = 2.0;
// const APP_TAB_BORDER_RADIUS: f32 = 6.0;

const APP_TAB_SELECTED_BORDER: Color = PY_GREEN;
const APP_TAB_UNSELECTED_BORDER: Color = APP_BUTTON_BG;
const APP_TAB_HOVER_BORDER: Color = PY_GREEN50;
const APP_TAB_PRESSED_BORDER: Color = APP_TAB_SELECTED_BORDER;
const APP_TAB_TEXT: Color = Color::WHITE;
const APP_TAB_DISABLED_TEXT: Color = APP_BUTTON_DISABLED_TEXT;
const APP_TAB_BORDER_WIDTH: f32 = 2.0;
const APP_TAB_BORDER_RADIUS: f32 = 6.0;

const APP_TAB_SPINNER_FRAMES: [&str; 12] = [
    "⠉⠉", "⠈⠙", "⠀⠹", "⠀⢸", "⠀⣰", "⢀⣠", "⣀⣀", "⣄⡀", "⣆⠀", "⡇⠀", "⠏⠀", "⠋⠁",
];

const APP_PROFILE_SELECTED_BG: Option<Color> = Some(Color::from_rgb8(55, 55, 60));
const APP_PROFILE_UNSELECTED_BG: Option<Color> = Some(Color::from_rgb8(35, 35, 40));
const APP_PROFILE_HOVER_BG: Option<Color> = None;
const APP_PROFILE_PRESSED_BG: Option<Color> = Some(Color::from_rgb8(26, 32, 40));
//const APP_PROFILE_HOVER_BG: Option<Color> = Some(Color::from_rgb8(32, 38, 48));
//const APP_PROFILE_PRESSED_BG: Option<Color> = Some(Color::from_rgb8(0, 51, 102));

const APP_PROFILE_SELECTED_BORDER: Color = PY_CYAN;
const APP_PROFILE_UNSELECTED_BORDER: Color = PY_GREY_SYS;
const APP_PROFILE_HOVER_BORDER: Color = PY_CYAN30;
const APP_PROFILE_PRESSED_BORDER: Color = PY_CYAN;
const APP_PROFILE_TEXT: Color = Color::WHITE;
const APP_PROFILE_DISABLED_TEXT: Color = APP_BUTTON_DISABLED_TEXT;
const APP_PROFILE_BORDER_WIDTH: f32 = 2.0;
const APP_PROFILE_BORDER_RADIUS: f32 = 4.0;

const DD_STATS_EMA_ALPHA: f64 = 0.30;
const DD_FAILURE_PENALTY: f64 = 2500.0;
const DD_UNKNOWN_SERVER_SCORE: f64 = -1e18;
const DD_STATS_SAVE_INTERVAL_MS: u64 = 15_000;
const DEADDROP_PANEL_HEIGHT_PORTION: u16 = 2;
const TEXT_BUBBLE_MAX_WIDTH: f32 = 460.0;
const TEXT_BUBBLE_MIN_BODY_WIDTH: f32 = 92.0;
const FILE_BUBBLE_WIDTH: f32 = 340.0;
const IMAGE_BUBBLE_MAX_WIDTH: f32 = 420.0;
const IMAGE_BUBBLE_MAX_HEIGHT: f32 = 360.0;
const SYSTEM_BUBBLE_MAX_WIDTH: f32 = 700.0;
const IMAGE_TRANSFER_MAX_DIMENSION: u32 = 1280;
const IMAGE_TRANSFER_JPEG_QUALITY: u8 = 82;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupGate {
    Locked,
    Unlocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TabKind {
    AppHome,
    Chat,
}

#[derive(Debug, Clone)]
pub struct ChatTab {
    pub kind: TabKind,
    pub title: String,
    pub profile_name: String,
    pub has_unread: bool,
    pub has_incoming: bool,
    pub connected: bool,
    pub initializing: bool,
    pub initialized: bool,
}

pub struct OpenedTab {
    pub id: u64,
    pub meta: ChatTab,
    pub session: SessionState,
    pub sam: SamClient,
    pub e2e: E2E,
    pub deaddrop: std::sync::Arc<TokioMutex<DeadDropClient>>,
    pub deaddrop_started: bool,
    pub deaddrop_poller_started: bool,
    pub deaddrop_poll_in_flight: bool,
    pub deaddrop_poll_queue: Vec<(u64, String)>,
    pub deaddrop_put_in_flight: bool,
    pub deaddrop_last_poll_ms: u64,
    pub live_conn: Option<LiveConnection>,
    pub pending_conn: Option<LiveConnection>,

    pub incoming_file: Option<StdFile>,
    pub incoming_filename: Option<String>,
    pub incoming_expected: u64,
    pub incoming_received: u64,
    pub incoming_save_path: Option<PathBuf>,
    pub incoming_bubble_index: Option<usize>,
    pub incoming_image_name: Option<String>,
    pub incoming_image_mime: Option<String>,
    pub incoming_image_expected: u64,
    pub incoming_image_received: u64,
    pub incoming_image_msg_id: u64,
    pub incoming_image_bytes: Vec<u8>,

    pub outgoing_bubble_index: Option<usize>,
    pub outgoing_file: Option<StdFile>,
    pub outgoing_filename: Option<String>,
    pub outgoing_total: u64,
    pub outgoing_sent: u64,
    pub outgoing_phase: OutgoingFilePhase,
    pub outgoing_send_in_flight: bool,
    pub outgoing_image_name: Option<String>,
    pub outgoing_image_mime: Option<String>,
    pub outgoing_image_bytes: Vec<u8>,
    pub outgoing_image_total: u64,
    pub outgoing_image_sent: u64,
    pub outgoing_image_msg_id: u64,
    pub outgoing_image_phase: OutgoingImagePhase,
    pub outgoing_image_send_in_flight: bool,
}

#[derive(Debug, Clone)]
pub struct ProfileEntry {
    pub name: String,
    pub persistent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SidebarConfirm {
    DeleteProfile(String),
    ResetProfile(String),
}

impl ProfileEntry {
    fn transient() -> Self {
        Self {
            name: "default".into(),
            persistent: false,
        }
    }

    fn persistent(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            persistent: true,
        }
    }
}

#[derive(Debug, Clone)]
pub enum BubbleContent {
    Text(String),
    Image(ImageBubbleData),
    File(FileBubbleData),
    System(String),
}

#[derive(Debug, Clone)]
pub struct ImageBubbleData {
    pub bytes: Vec<u8>,
    pub handle: iced::widget::image::Handle,
    pub width: u32,
    pub height: u32,
}

#[derive(Debug, Clone)]
pub struct FileBubbleData {
    pub filename: String,
    pub saved_path: Option<String>,
    pub total_bytes: u64,
    pub done_bytes: u64,
    pub outgoing: bool,
    pub complete: bool,
    pub failed: bool,
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutgoingFilePhase {
    Idle,
    Header,
    Chunks,
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutgoingImagePhase {
    Idle,
    Header,
    Chunks,
    End,
}

#[derive(Debug, Clone)]
pub struct Bubble {
    pub author: String,
    pub content: BubbleContent,
    pub mine: bool,
    pub offline: bool,
    pub timestamp_utc: String,
    pub msg_id: Option<u64>,
    pub delivered: bool,
}

impl Bubble {
    fn me(text: impl Into<String>) -> Self {
        Self {
            author: "Me".into(),
            content: BubbleContent::Text(text.into()),
            mine: true,
            offline: false,
            timestamp_utc: TermchatApp::now_utc_hms(),
            msg_id: None,
            delivered: false,
        }
    }

    fn me_with_id(text: impl Into<String>, msg_id: u64) -> Self {
        Self {
            author: "Me".into(),
            content: BubbleContent::Text(text.into()),
            mine: true,
            offline: false,
            timestamp_utc: TermchatApp::now_utc_hms(),
            msg_id: Some(msg_id),
            delivered: false,
        }
    }

    fn peer(text: impl Into<String>) -> Self {
        Self {
            author: "Peer".into(),
            content: BubbleContent::Text(text.into()),
            mine: false,
            offline: false,
            timestamp_utc: TermchatApp::now_utc_hms(),
            msg_id: None,
            delivered: false,
        }
    }

    fn me_offline(text: impl Into<String>) -> Self {
        Self::me_offline_with_id(text, 0)
    }

    fn me_offline_with_id(text: impl Into<String>, msg_id: u64) -> Self {
        Self {
            author: "Me-Offline".into(),
            content: BubbleContent::Text(text.into()),
            mine: true,
            offline: true,
            timestamp_utc: TermchatApp::now_utc_hms(),
            msg_id: if msg_id == 0 { None } else { Some(msg_id) },
            delivered: false,
        }
    }

    fn peer_offline(text: impl Into<String>) -> Self {
        Self {
            author: "Peer-Offline".into(),
            content: BubbleContent::Text(text.into()),
            mine: false,
            offline: true,
            timestamp_utc: TermchatApp::now_utc_hms(),
            msg_id: None,
            delivered: false,
        }
    }

    fn system(text: impl Into<String>) -> Self {
        Self {
            author: "System".into(),
            content: BubbleContent::System(text.into()),
            mine: false,
            offline: false,
            timestamp_utc: TermchatApp::now_utc_hms(),
            msg_id: None,
            delivered: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkStatus {
    Initializing,
    LocalOk,
    Visible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuiAction {
    Connect,
    Disconnect,
    SendFile,
    SendImage,
    CopyMyB32,
    Accept,
    Decline,
    Lock,
    Unlock,
    Offline,
    Online,
    Pq,
    Help,
    DdList,
}

#[derive(Debug, Clone)]
pub struct SessionState {
    pub profile: String,
    pub profiles: Vec<ProfileEntry>,
    pub selected_profile_idx: usize,
    pub profile_name_input: String,
    pub sidebar_confirm: Option<SidebarConfirm>,
    pub tabs: Vec<ChatTab>,
    pub active_tab_idx: Option<usize>,
    pub my_b32: Option<String>,
    pub my_dest_b64: Option<String>,
    pub my_pub_dest_b64: Option<String>,
    pub sam_session_id: Option<String>,
    pub peer_b32: Option<String>,

    pub network_status: NetworkStatus,
    pub live_ready: bool,
    pub offline_mode: bool,
    pub tofu_verified: bool,
    pub tofu_mismatch: bool,
    pub pq_enabled: bool,
    pub pq_active: bool,

    pub stored_peer: Option<String>,
    pub stored_peer_dest_b64: Option<String>,
    pub current_peer_addr: Option<String>,
    pub current_peer_dest_b64: Option<String>,
    pub pending_peer_addr: Option<String>,
    pub pending_peer_dest_b64: Option<String>,
    pub dd_status: String,
    pub dd_status_at_ms: u64,
    pub accept_armed: bool,

    pub call_blink_on: bool,
    pub call_blink_ticks: u8,

    pub pending_action: Option<GuiAction>,
    pub action_param: String,

    pub input: String,
    pub bubbles: Vec<Bubble>,
    pub status_lines: Vec<String>,
    pub show_logs: bool,
    pub show_deaddrop_panel: bool,
    pub deaddrop_server_input: String,
    pub log_lines: Vec<String>,
    pub messages_scroll_id: ScrollableId,
    pub logs_scroll_id: ScrollableId,

    pub deaddrop_servers: Vec<String>,
    pub deaddrop_stats: HashMap<String, storage::DeaddropServerStat>,
    pub deaddrop_stats_dirty: bool,
    pub deaddrop_stats_last_save_ms: u64,
    pub offline_shared_secret: Option<[u8; 32]>,
    pub drop_send_index: u64,
    pub drop_recv_base: u64,
    pub drop_window: u32,
    pub consumed_drop_recv: Vec<u64>,
    pub seen_drop_msgs: Vec<String>,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            profile: "default".into(),
            profiles: vec![ProfileEntry::transient()],
            selected_profile_idx: 0,
            profile_name_input: String::new(),
            sidebar_confirm: None,
            tabs: vec![],
            active_tab_idx: None,
            my_b32: None,
            my_dest_b64: None,
            my_pub_dest_b64: None,
            sam_session_id: None,
            peer_b32: None,
            network_status: NetworkStatus::Initializing,
            live_ready: false,
            offline_mode: false,
            tofu_verified: false,
            tofu_mismatch: false,
            pq_enabled: false,
            pq_active: false,
            stored_peer: None,
            stored_peer_dest_b64: None,
            current_peer_addr: None,
            current_peer_dest_b64: None,
            pending_peer_addr: None,
            pending_peer_dest_b64: None,
            dd_status: "idle".into(),
            dd_status_at_ms: 0,
            accept_armed: false,
            pending_action: None,
            call_blink_on: true,
            call_blink_ticks: 0,
            action_param: String::new(),
            input: String::new(),
            bubbles: vec![],
            status_lines: vec![
                format!("{APP_NAME} {APP_VERSION}"),
                "Application ready.".into(),
                "Open a profile to start a chat tab.".into(),
            ],
            show_logs: false,
            show_deaddrop_panel: false,
            deaddrop_server_input: String::new(),
            log_lines: vec![
                format!("{APP_NAME} {APP_VERSION}"),
                "Application ready.".into(),
                "Open a profile to start a chat tab.".into(),
            ],
            messages_scroll_id: ScrollableId::unique(),
            logs_scroll_id: ScrollableId::unique(),
            deaddrop_servers: vec![],
            deaddrop_stats: HashMap::new(),
            deaddrop_stats_dirty: false,
            deaddrop_stats_last_save_ms: 0,
            offline_shared_secret: None,
            drop_send_index: 0,
            drop_recv_base: 0,
            drop_window: 8,
            consumed_drop_recv: vec![],
            seen_drop_msgs: vec![],
        }
    }
}

pub struct TermchatApp {
    pub session: SessionState,
    pub opened_tabs: Vec<OpenedTab>,
    pub app_lock: Option<AppLock>,
    pub clipboard: Option<Clipboard>,

    pub startup_gate: StartupGate,
    pub unlock_input: String,
    pub unlock_confirm_input: String,
    pub unlock_status: String,
    pub sam_host_input: String,
    pub sam_port_input: String,
    pub sam_status: String,
    pub sam_test_in_flight: bool,
    pub backup_export_passphrase: String,
    pub backup_export_status: String,
    pub backup_export_include_files: bool,
    pub backup_import_passphrase: String,
    pub backup_import_status: String,
    pub backup_import_restore_files: bool,
    pub pending_backup_import_path: Option<PathBuf>,
    pub pending_backup_import_passphrase: String,
    pub wipe_all_passphrase: String,
    pub wipe_all_status: String,
    pub profile_export_passphrase: String,
    pub profile_export_status: String,
    pub profile_import_passphrase: String,
    pub profile_import_status: String,
    pub pending_profile_import_path: Option<PathBuf>,
    pub pending_profile_import_passphrase: String,
    pub pending_profile_import_name: Option<String>,
    pub backup_operation: BackupOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupOperation {
    Idle,
    Exporting,
    Importing,
    AwaitingReplaceConfirm,
    ProfileExporting,
    ProfileImporting,
    AwaitingProfileReplaceConfirm,
    Wiping,
    AwaitingWipeConfirm,
}

#[derive(Debug, Clone)]
pub enum Message {
    InputChanged(String),
    SendPressed,

    UnlockInputChanged(String),
    UnlockConfirmInputChanged(String),
    UnlockPressed,
    BackupExportPassphraseChanged(String),
    BackupImportPassphraseChanged(String),
    BackupExportIncludeFilesChanged(bool),
    BackupImportRestoreFilesChanged(bool),
    SamHostInputChanged(String),
    SamPortInputChanged(String),
    SaveSamSettingsPressed,
    TestSamPressed,
    SamTestFinished(Result<String, String>),
    WipeAllPassphraseChanged(String),
    ProfileExportPassphraseChanged(String),
    ProfileImportPassphraseChanged(String),
    ExportBackupPressed,
    ImportBackupPressed,
    WipeAllPressed,
    ExportProfileBackupPressed,
    ImportProfileBackupPressed,
    BackupExportPathChosen(Option<PathBuf>),
    BackupImportPathChosen(Option<PathBuf>),
    ProfileBackupExportPathChosen(Option<PathBuf>),
    ProfileBackupImportPathChosen(Option<PathBuf>),
    BackupExportFinished(Result<PathBuf, String>),
    BackupImportFinished(Result<PathBuf, String>),
    ProfileBackupImportScanned(Result<(PathBuf, String, bool), String>),
    ProfileBackupExportFinished(Result<(PathBuf, String), String>),
    ProfileBackupImportFinished(Result<String, String>),
    BackupImportReplaceConfirmed,
    BackupImportReplaceCancelled,
    WipeAllConfirmed,
    WipeAllCancelled,
    ProfileBackupImportReplaceConfirmed,
    ProfileBackupImportReplaceCancelled,

    ProfileSelected(usize),
    ProfileNameInputChanged(String),
    CreateProfilePressed,
    DeleteProfilePressed,
    ResetProfilePressed,
    SidebarConfirmYes,
    SidebarConfirmNo,
    WipeAllFinished(Result<(), String>),

    OpenSelectedProfilePressed,

    TabSelected(usize),
    TabClosed(usize),

    ActionPressed(GuiAction),
    ActionParamChanged(String),
    ActionConfirm,
    ActionCancel,
    CopyStatusMyB32Pressed,
    CopyStatusPeerB32Pressed,
    ToggleLogsPressed,
    DdServerInputChanged(String),
    DdServerAddPressed,
    DdServerDeletePressed(usize),
    DdServerSharePressed,

    SamInitialized(u64, Result<(SamClient, SamInitResult), String>),
    ConnectFinished(u64, Result<(String, LiveConnection), String>),
    IncomingAccepted(u64, Result<AcceptedIncoming, String>),
    SendFinished(u64, Result<(), String>),
    CloseFinished(u64, Result<(), String>),
    SamCloseFinished(u64, Result<(), String>),
    QuitSignalSent(u64, Result<(), String>),

    DeaddropStarted(u64, Result<(), String>),
    DeaddropClosed(u64),
    OfflinePutFinished(
        u64,
        Result<(String, Vec<String>, u64, u64, Vec<DeaddropOpStat>), String>,
    ),
    OfflinePollKeyFinished(
        u64,
        u64,
        String,
        Vec<(String, Vec<u8>)>,
        Vec<DeaddropOpStat>,
    ),

    FileChosen(Option<PathBuf>),
    ImageChosen(Option<PathBuf>),
    OutgoingFileHeaderSent(u64, Result<(), String>),
    OutgoingFileChunkSent(u64, Result<usize, String>),
    OutgoingFileEndSent(u64, Result<(), String>),
    OutgoingImageHeaderSent(u64, Result<(), String>),
    OutgoingImageChunkSent(u64, Result<usize, String>),
    OutgoingImageEndSent(u64, Result<(), String>),

    WindowCloseRequested(window::Id),
    ProcessShutdownRequested,
    ExitAfterNotify(ShutdownTarget),
    Tick,
}

#[derive(Debug, Clone, Copy)]
pub enum ShutdownTarget {
    Window(window::Id),
    Runtime,
}

impl Default for TermchatApp {
    fn default() -> Self {
        Self {
            session: SessionState::default(),
            opened_tabs: vec![],
            app_lock: None,
            clipboard: Clipboard::new().ok(),

            startup_gate: StartupGate::Locked,
            unlock_input: String::new(),
            unlock_confirm_input: String::new(),
            unlock_status: "Vault is locked.".into(),
            sam_host_input: DEFAULT_SAM_HOST.to_string(),
            sam_port_input: DEFAULT_SAM_PORT.to_string(),
            sam_status: "SAM settings apply to newly opened chat tabs.".into(),
            sam_test_in_flight: false,
            backup_export_passphrase: String::new(),
            backup_export_status: "Export creates a v2 encrypted backup file.".into(),
            backup_export_include_files: true,
            backup_import_passphrase: String::new(),
            backup_import_status: "Import restores a v2 encrypted backup file.".into(),
            backup_import_restore_files: true,
            pending_backup_import_path: None,
            pending_backup_import_passphrase: String::new(),
            wipe_all_passphrase: String::new(),
            wipe_all_status: "Wipe deletes all profiles and stored files.".into(),
            profile_export_passphrase: String::new(),
            profile_export_status: "Export selected profile without files.".into(),
            profile_import_passphrase: String::new(),
            profile_import_status: "Import one persistent profile without files.".into(),
            pending_profile_import_path: None,
            pending_profile_import_passphrase: String::new(),
            pending_profile_import_name: None,
            backup_operation: BackupOperation::Idle,
        }
    }
}

impl Drop for TermchatApp {
    fn drop(&mut self) {
        if let Err(err) = self.encrypt_for_shutdown() {
            eprintln!("Vault encryption failed during app drop: {err}");
        }
    }
}

impl TermchatApp {
    pub fn boot() -> (Self, Task<Message>) {
        let mut app = Self::default();

        let lock = match storage::acquire_app_lock() {
            Ok(lock) => lock,
            Err(err) => {
                eprintln!("Cannot start GUI app: {err}");
                std::process::exit(1);
            }
        };

        app.app_lock = Some(lock);

        (app, Task::none())
    }

    fn load_unlocked_storage(&mut self) -> Result<(), String> {
        match storage::load_app_config() {
            Ok(config) => {
                self.sam_host_input = config.sam_host;
                self.sam_port_input = config.sam_port.to_string();
            }
            Err(err) => {
                self.sam_status = format!("Failed to load SAM settings: {err}");
            }
        }

        let contacts = storage::load_contacts().map_err(|e| e.to_string())?;
        self.session.profiles = vec![ProfileEntry::transient()];
        for contact in contacts {
            self.session
                .profiles
                .push(ProfileEntry::persistent(contact.name));
        }
        self.session.selected_profile_idx = 0;
        self.session.tabs = vec![Self::new_app_home_tab()];
        self.session.active_tab_idx = Some(0);
        self.session.profile = "__app__".into();
        Ok(())
    }

    fn generate_tab_id() -> u64 {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let random_bits: u64 = rand::rng().random::<u32>() as u64;
        millis ^ random_bits
    }

    fn new_opened_tab(&self, profile_name: &str) -> OpenedTab {
        let mut sam_host = self.sam_host_input.trim().to_string();
        if sam_host.is_empty() {
            sam_host = DEFAULT_SAM_HOST.to_string();
        }
        let sam_port = self
            .sam_port_input
            .trim()
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .unwrap_or(DEFAULT_SAM_PORT);
        let mut session = SessionState::default();
        session.profile = profile_name.to_string();
        session.profiles = vec![];
        session.selected_profile_idx = 0;
        session.profile_name_input.clear();

        let mut tab = OpenedTab {
            id: Self::generate_tab_id(),
            meta: ChatTab {
                kind: TabKind::Chat,
                title: profile_name.to_string(),
                profile_name: profile_name.to_string(),
                has_unread: false,
                has_incoming: false,
                connected: false,
                initializing: false,
                initialized: false,
            },

            deaddrop: std::sync::Arc::new(TokioMutex::new(DeadDropClient::new_with_sam(
                format!(
                    "dd_{}_{}",
                    profile_name,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                ),
                Self::active_deaddrop_replicas(&session.deaddrop_servers),
                sam_host.clone(),
                sam_port,
            ))),

            deaddrop_started: false,
            deaddrop_poller_started: false,
            deaddrop_poll_in_flight: false,
            deaddrop_poll_queue: vec![],
            deaddrop_put_in_flight: false,
            deaddrop_last_poll_ms: 0,
            sam: SamClient::new(sam_host.clone(), sam_port),
            e2e: E2E::new(false),
            live_conn: None,
            pending_conn: None,

            incoming_file: None,
            incoming_filename: None,
            incoming_expected: 0,
            incoming_received: 0,
            incoming_save_path: None,
            incoming_bubble_index: None,
            incoming_image_name: None,
            incoming_image_mime: None,
            incoming_image_expected: 0,
            incoming_image_received: 0,
            incoming_image_msg_id: 0,
            incoming_image_bytes: Vec::new(),

            outgoing_bubble_index: None,
            outgoing_file: None,
            outgoing_filename: None,
            outgoing_total: 0,
            outgoing_sent: 0,
            outgoing_phase: OutgoingFilePhase::Idle,
            outgoing_send_in_flight: false,
            outgoing_image_name: None,
            outgoing_image_mime: None,
            outgoing_image_bytes: Vec::new(),
            outgoing_image_total: 0,
            outgoing_image_sent: 0,
            outgoing_image_msg_id: 0,
            outgoing_image_phase: OutgoingImagePhase::Idle,
            outgoing_image_send_in_flight: false,

            session,
        };

        if profile_name != "default" {
            if let Ok(meta) = storage::load_contact_meta(profile_name) {
                Self::apply_contact_meta_to_opened_tab(&mut tab, &meta);
                tab.deaddrop = std::sync::Arc::new(TokioMutex::new(DeadDropClient::new_with_sam(
                    format!(
                        "dd_{}_{}",
                        profile_name,
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0)
                    ),
                    Self::active_deaddrop_replicas(&tab.session.deaddrop_servers),
                    sam_host.clone(),
                    sam_port,
                )));
                tab.e2e = E2E::new(tab.session.pq_enabled);
            }
        }

        tab
    }

    pub fn subscription(_state: &Self) -> Subscription<Message> {
        Subscription::batch(vec![
            time::every(Duration::from_millis(150)).map(|_| Message::Tick),
            window::close_requests().map(Message::WindowCloseRequested),
            Subscription::run(Self::process_signal_stream),
        ])
    }

    fn process_signal_stream() -> impl iced::futures::Stream<Item = Message> {
        stream::channel(1, async |mut output| {
            #[cfg(unix)]
            {
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(mut sigterm) => {
                        let ctrl_c = Box::pin(tokio::signal::ctrl_c());
                        let sigterm = Box::pin(sigterm.recv());
                        let _ = iced::futures::future::select(ctrl_c, sigterm).await;
                    }
                    Err(_) => {
                        let _ = tokio::signal::ctrl_c().await;
                    }
                }
            }

            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }

            let _ = output.send(Message::ProcessShutdownRequested).await;
        })
    }

    fn encrypt_for_shutdown(&mut self) -> Result<(), String> {
        if self.startup_gate != StartupGate::Unlocked || self.unlock_input.is_empty() {
            return Ok(());
        }

        let base = storage::base_dir();
        if !base.exists() {
            self.startup_gate = StartupGate::Locked;
            self.unlock_input.clear();
            self.unlock_confirm_input.clear();
            return Ok(());
        }

        crate::vault::fs_encrypt(&base.to_string_lossy(), &self.unlock_input)
            .map_err(|err| err.to_string())?;
        self.startup_gate = StartupGate::Locked;
        self.unlock_input.clear();
        self.unlock_confirm_input.clear();
        Ok(())
    }

    fn begin_shutdown(&mut self, target: ShutdownTarget) -> Task<Message> {
        let quit_frame = self.make_signal_frame("QUIT");
        let mut tasks: Vec<Task<Message>> = Vec::new();

        for tab in &mut self.opened_tabs {
            let tab_id = tab.id;

            Self::flush_deaddrop_stats_for_tab(tab, true);

            if let Some(conn) = tab.live_conn.clone() {
                let quit_frame_clone = quit_frame.clone();
                tasks.push(Task::perform(
                    async move {
                        let _ = conn.send_frame(&quit_frame_clone).await;
                        conn.close().await.map_err(|e| e.to_string())
                    },
                    move |result| Message::CloseFinished(tab_id, result),
                ));
            }

            if let Some(conn) = tab.pending_conn.clone() {
                let quit_frame_clone = quit_frame.clone();
                tasks.push(Task::perform(
                    async move {
                        let _ = conn.send_frame(&quit_frame_clone).await;
                        conn.close().await.map_err(|e| e.to_string())
                    },
                    move |result| Message::CloseFinished(tab_id, result),
                ));
            }

            let mut sam = tab.sam.clone();
            tasks.push(Task::perform(
                async move { sam.close().await.map_err(|e| e.to_string()) },
                move |result| Message::SamCloseFinished(tab_id, result),
            ));

            if tab.deaddrop_started {
                let dd = std::sync::Arc::clone(&tab.deaddrop);

                tasks.push(Task::perform(
                    async move {
                        let mut dd = dd.lock().await;
                        dd.close().await;
                        tab_id
                    },
                    Message::DeaddropClosed,
                ));

                tab.deaddrop_started = false;
                tab.deaddrop_poller_started = false;
                tab.deaddrop_poll_in_flight = false;
                tab.deaddrop_poll_queue.clear();
                tab.deaddrop_put_in_flight = false;
            }

            tab.live_conn = None;
            tab.pending_conn = None;
            tab.session.sam_session_id = None;
            tab.session.live_ready = false;
            tab.session.pending_peer_addr = None;
            tab.session.pending_peer_dest_b64 = None;
            tab.session.current_peer_addr = None;
            tab.session.peer_b32 = None;
            tab.session.accept_armed = false;
        }

        self.reset_connection_state();
        self.store_active_runtime();

        tasks.push(Task::perform(
            async move { target },
            Message::ExitAfterNotify,
        ));
        Task::batch(tasks)
    }

    fn accept_task(&self, tab_id: u64) -> Task<Message> {
        let sam = self
            .opened_tabs
            .iter()
            .find(|t| t.id == tab_id)
            .map(|t| t.sam.clone());

        if let Some(sam) = sam {
            Task::perform(
                async move { sam.stream_accept().await.map_err(|e| e.to_string()) },
                move |result| Message::IncomingAccepted(tab_id, result),
            )
        } else {
            Task::none()
        }
    }

    fn active_tab(&self) -> Option<&OpenedTab> {
        self.session
            .active_tab_idx
            .and_then(Self::visible_to_real_tab_index)
            .and_then(|idx| self.opened_tabs.get(idx))
    }

    fn find_tab_index_by_id(&self, tab_id: u64) -> Option<usize> {
        self.opened_tabs.iter().position(|t| t.id == tab_id)
    }

    fn visible_to_real_tab_index(visible_idx: usize) -> Option<usize> {
        if visible_idx == 0 {
            None
        } else {
            Some(visible_idx - 1)
        }
    }

    fn real_to_visible_tab_index(real_idx: usize) -> usize {
        real_idx + 1
    }

    fn tab_by_id_mut(&mut self, tab_id: u64) -> Option<&mut OpenedTab> {
        let idx = self.find_tab_index_by_id(tab_id)?;
        self.opened_tabs.get_mut(idx)
    }

    fn active_tab_mut(&mut self) -> Option<&mut OpenedTab> {
        self.session
            .active_tab_idx
            .and_then(Self::visible_to_real_tab_index)
            .and_then(|idx| self.opened_tabs.get_mut(idx))
    }

    fn store_active_runtime(&mut self) {
        let sidebar_profiles = self.session.profiles.clone();
        let sidebar_selected = self.session.selected_profile_idx;
        let sidebar_input = self.session.profile_name_input.clone();
        let sidebar_confirm = self.session.sidebar_confirm.clone();
        let tabs = self.session.tabs.clone();
        let active_idx = self.session.active_tab_idx;

        let mut snapshot = self.session.clone();
        snapshot.profiles = vec![];
        snapshot.selected_profile_idx = 0;
        snapshot.profile_name_input.clear();
        snapshot.sidebar_confirm = None;

        if let Some(tab) = self.active_tab_mut() {
            tab.session = snapshot;
            tab.meta.connected = tab.session.live_ready;
            tab.meta.has_incoming = tab.session.pending_peer_addr.is_some();
        }

        self.session.profiles = sidebar_profiles;
        self.session.selected_profile_idx = sidebar_selected;
        self.session.profile_name_input = sidebar_input;
        self.session.sidebar_confirm = sidebar_confirm;
        self.session.tabs = tabs;
        self.session.active_tab_idx = active_idx;
    }

    fn load_active_runtime(&mut self) {
        let sidebar_profiles = self.session.profiles.clone();
        let sidebar_selected = self.session.selected_profile_idx;
        let sidebar_input = self.session.profile_name_input.clone();
        let sidebar_confirm = self.session.sidebar_confirm.clone();
        let tabs = self.session.tabs.clone();
        let active_idx = self.session.active_tab_idx;

        if let Some(snapshot) = self.active_tab().map(|t| t.session.clone()) {
            self.session = snapshot;
            self.session.profiles = sidebar_profiles;
            self.session.selected_profile_idx = sidebar_selected;
            self.session.profile_name_input = sidebar_input;
            self.session.sidebar_confirm = sidebar_confirm;
            self.session.tabs = tabs;
            self.session.active_tab_idx = active_idx;
        }
    }

    fn refresh_visible_from_active_tab(&mut self) {
        let sidebar_profiles = self.session.profiles.clone();
        let sidebar_selected = self.session.selected_profile_idx;
        let sidebar_input = self.session.profile_name_input.clone();
        let sidebar_confirm = self.session.sidebar_confirm.clone();

        self.session.tabs = std::iter::once(Self::new_app_home_tab())
            .chain(self.opened_tabs.iter().map(|t| t.meta.clone()))
            .collect();

        match self.session.active_tab_idx {
            Some(0) | None => {
                self.session.profiles = sidebar_profiles;
                self.session.selected_profile_idx = sidebar_selected;
                self.session.profile_name_input = sidebar_input;
                self.session.sidebar_confirm = sidebar_confirm;
                self.session.active_tab_idx = Some(0);
                self.session.profile = "__app__".into();
            }
            Some(visible_idx) => {
                if let Some(real_idx) = Self::visible_to_real_tab_index(visible_idx) {
                    if let Some(snapshot) =
                        self.opened_tabs.get(real_idx).map(|t| t.session.clone())
                    {
                        let tabs = self.session.tabs.clone();

                        self.session = snapshot;
                        self.session.profiles = sidebar_profiles;
                        self.session.selected_profile_idx = sidebar_selected;
                        self.session.profile_name_input = sidebar_input;
                        self.session.sidebar_confirm = sidebar_confirm;
                        self.session.tabs = tabs;
                        self.session.active_tab_idx = Some(visible_idx);

                        if let Some(tab) = self.opened_tabs.get_mut(real_idx) {
                            tab.meta.has_unread = false;
                        }
                    } else {
                        self.session.profiles = sidebar_profiles;
                        self.session.selected_profile_idx = sidebar_selected;
                        self.session.profile_name_input = sidebar_input;
                        self.session.sidebar_confirm = sidebar_confirm;
                        self.session.active_tab_idx = Some(0);
                        self.session.profile = "__app__".into();
                    }
                }
            }
        }
    }

    fn start_tab_runtime_task(&self, tab_idx: usize) -> Task<Message> {
        let (tab_id, profile_name, sam_host, sam_port, saved_dest_b64) =
            if let Some(tab) = self.opened_tabs.get(tab_idx) {
                (
                    tab.id,
                    tab.session.profile.clone(),
                    tab.sam.sam_host.clone(),
                    tab.sam.sam_port,
                    tab.session.my_dest_b64.clone(),
                )
            } else {
                return Task::none();
            };

        Task::perform(
            async move {
                let session_id = format!(
                    "chat_{}_{}",
                    profile_name,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                );

                let mut sam = SamClient::new(sam_host, sam_port);

                let init = if profile_name == "default" {
                    sam.initialize_transient(session_id)
                        .await
                        .map_err(|e| e.to_string())?
                } else if let Some(my_dest_b64) = saved_dest_b64 {
                    sam.initialize_persistent(session_id, my_dest_b64)
                        .await
                        .map_err(|e| e.to_string())?
                } else {
                    sam.initialize_transient(session_id)
                        .await
                        .map_err(|e| e.to_string())?
                };

                Ok((sam, init))
            },
            move |result| Message::SamInitialized(tab_id, result),
        )
    }

    fn ensure_tab_runtime_started(&mut self, tab_idx: usize) -> Task<Message> {
        if let Some(tab) = self.opened_tabs.get_mut(tab_idx) {
            if tab.meta.initialized || tab.meta.initializing {
                return Task::none();
            }

            tab.meta.initializing = true;
            self.session.tabs = std::iter::once(Self::new_app_home_tab())
                .chain(self.opened_tabs.iter().map(|t| t.meta.clone()))
                .collect();

            return Task::batch(vec![
                self.logs_snap_task(),
                self.start_tab_runtime_task(tab_idx),
            ]);
        }

        Task::none()
    }

    fn logs_snap_task(&self) -> Task<Message> {
        operation::snap_to_end(self.session.logs_scroll_id.clone())
    }

    pub fn update(state: &mut Self, message: Message) -> Task<Message> {
        match message {
            Message::InputChanged(value) => {
                let should_snap_messages = state.session.input.is_empty() && !value.is_empty();
                state.session.input = value;
                state.store_active_runtime();
                if should_snap_messages {
                    return operation::snap_to_end(state.session.messages_scroll_id.clone());
                }

                return Task::none();
            }

            Message::ProfileSelected(idx) => {
                if idx < state.session.profiles.len() {
                    state.session.selected_profile_idx = idx;
                    state.session.sidebar_confirm = None;
                    return Task::none();
                }
            }

            Message::OpenSelectedProfilePressed => {
                let idx = state.session.selected_profile_idx;

                if idx < state.session.profiles.len() {
                    let profile_name = state.session.profiles[idx].name.clone();
                    state.open_or_focus_tab_for_profile(&profile_name);
                    state.post_system(format!("Opened tab for: {}", profile_name));

                    if let Some(visible_idx) = state.session.active_tab_idx {
                        if let Some(real_idx) = Self::visible_to_real_tab_index(visible_idx) {
                            let start_runtime = state.ensure_tab_runtime_started(real_idx);

                            let dd_tab_id = state.opened_tabs.get(real_idx).map(|tab| tab.id);

                            let dd_task = if let Some(tab_id) = dd_tab_id {
                                state.ensure_deaddrop_runtime_started(tab_id)
                            } else {
                                Task::none()
                            };

                            return Task::batch(vec![
                                operation::snap_to_end(state.session.logs_scroll_id.clone()),
                                start_runtime,
                                dd_task,
                            ]);
                        }
                    }

                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }
            }

            Message::ProfileNameInputChanged(value) => {
                state.session.profile_name_input = value;
                return Task::none();
            }

            Message::TabSelected(idx) => {
                state.store_active_runtime();

                if idx == 0 {
                    state.session.active_tab_idx = Some(0);
                    state.session.profile = "__app__".into();
                    state.refresh_visible_from_active_tab();
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                let real_idx = idx - 1;

                if real_idx < state.opened_tabs.len() {
                    state.session.active_tab_idx = Some(idx);

                    if let Some(tab) = state.opened_tabs.get_mut(real_idx) {
                        tab.meta.has_unread = false;
                    }

                    state.session.profile = state.opened_tabs[real_idx].meta.profile_name.clone();

                    if let Some(profile_idx) = state
                        .session
                        .profiles
                        .iter()
                        .position(|p| p.name == state.opened_tabs[real_idx].meta.profile_name)
                    {
                        state.session.selected_profile_idx = profile_idx;
                    }

                    state.refresh_visible_from_active_tab();

                    return Task::batch(vec![
                        operation::snap_to_end(state.session.logs_scroll_id.clone()),
                        state.ensure_tab_runtime_started(real_idx),
                    ]);
                }
            }

            Message::TabClosed(idx) => {
                if idx == 0 {
                    return Task::none();
                }

                let idx = idx - 1;

                if idx < state.opened_tabs.len() {
                    state.store_active_runtime();

                    let mut tasks = state.close_tab_runtime_tasks(idx);

                    if let Some(tab) = state.opened_tabs.get_mut(idx) {
                        tab.live_conn = None;
                        tab.pending_conn = None;
                        tab.session.live_ready = false;
                        tab.session.pending_peer_addr = None;
                        tab.session.pending_peer_dest_b64 = None;
                        tab.session.current_peer_addr = None;
                        tab.session.current_peer_dest_b64 = None;
                        tab.session.peer_b32 = None;
                        tab.session.network_status = NetworkStatus::LocalOk;
                        tab.session.accept_armed = false;
                        tab.session.sam_session_id = None;
                        tab.session.tofu_verified = false;
                        tab.session.tofu_mismatch = false;
                        tab.meta.connected = false;
                        tab.meta.has_incoming = false;
                        tab.meta.initialized = false;
                        tab.meta.initializing = false;

                        tab.e2e = E2E::new(tab.session.pq_enabled);

                        if tab.deaddrop_started {
                            let dd = std::sync::Arc::clone(&tab.deaddrop);
                            let tab_id = tab.id;

                            tasks.push(Task::perform(
                                async move {
                                    let mut dd = dd.lock().await;
                                    dd.close().await;
                                    tab_id
                                },
                                Message::DeaddropClosed,
                            ));

                            tab.deaddrop_started = false;
                            tab.deaddrop_poller_started = false;
                            tab.deaddrop_poll_in_flight = false;
                            tab.deaddrop_poll_queue.clear();
                            tab.deaddrop_put_in_flight = false;
                        }
                    }

                    let _removed = state.opened_tabs.remove(idx);

                    match state.session.active_tab_idx {
                        Some(active_visible)
                            if active_visible == Self::real_to_visible_tab_index(idx) =>
                        {
                            if state.opened_tabs.is_empty() {
                                state.session.active_tab_idx = Some(0);
                                state.session.profile = "__app__".into();
                                state.session = SessionState {
                                    profiles: state.session.profiles.clone(),
                                    selected_profile_idx: state.session.selected_profile_idx,
                                    profile_name_input: state.session.profile_name_input.clone(),
                                    sidebar_confirm: state.session.sidebar_confirm.clone(),
                                    tabs: vec![Self::new_app_home_tab()],
                                    active_tab_idx: Some(0),
                                    bubbles: vec![],
                                    input: String::new(),
                                    status_lines: vec![
                                        format!("{APP_NAME} {APP_VERSION}"),
                                        "Application ready.".into(),
                                        "Open a profile to start a chat tab.".into(),
                                    ],
                                    log_lines: state.session.log_lines.clone(),
                                    ..SessionState::default()
                                };
                            } else {
                                let new_real_idx =
                                    idx.saturating_sub(1).min(state.opened_tabs.len() - 1);
                                state.session.active_tab_idx =
                                    Some(Self::real_to_visible_tab_index(new_real_idx));
                                state.session.profile =
                                    state.opened_tabs[new_real_idx].meta.profile_name.clone();

                                if let Some(profile_idx) =
                                    state.session.profiles.iter().position(|p| {
                                        p.name == state.opened_tabs[new_real_idx].meta.profile_name
                                    })
                                {
                                    state.session.selected_profile_idx = profile_idx;
                                }
                            }
                        }
                        Some(active_visible)
                            if active_visible > Self::real_to_visible_tab_index(idx) =>
                        {
                            state.session.active_tab_idx = Some(active_visible - 1);
                        }
                        _ => {}
                    }

                    state.refresh_visible_from_active_tab();
                    return Task::batch(tasks);
                }
            }

            Message::CreateProfilePressed => {
                let name = state.session.profile_name_input.trim().to_string();

                if name.is_empty() {
                    state.post_system("Profile name cannot be empty.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                if name.eq_ignore_ascii_case("default")
                    || name.eq_ignore_ascii_case("__app__")
                    || name.eq_ignore_ascii_case("global")
                {
                    state.post_system("That profile name is reserved.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                if state
                    .session
                    .profiles
                    .iter()
                    .any(|p| p.name.eq_ignore_ascii_case(&name))
                {
                    state.post_system("Profile already exists.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                match storage::create_contact(&name) {
                    Ok(_meta) => {
                        state
                            .session
                            .profiles
                            .push(ProfileEntry::persistent(name.clone()));
                        state.session.selected_profile_idx = state.session.profiles.len() - 1;
                        state.session.profile_name_input.clear();
                        state.session.sidebar_confirm = None;
                        state.post_system(format!("Created profile: {name}"));
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }
                    Err(err) => {
                        state.post_system(format!("Create profile failed: {err}"));
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }
                }
            }

            Message::DeleteProfilePressed => {
                let idx = state.session.selected_profile_idx;

                if idx >= state.session.profiles.len() {
                    return Task::none();
                }

                let selected = state.session.profiles[idx].clone();

                if !selected.persistent {
                    state.post_system("Transient profile cannot be deleted.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                if state.is_profile_open_in_any_tab(&selected.name) {
                    state.post_system("Close that profile tab before deleting it.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                state.session.sidebar_confirm =
                    Some(SidebarConfirm::DeleteProfile(selected.name.clone()));
                return Task::none();
            }

            Message::ResetProfilePressed => {
                let idx = state.session.selected_profile_idx;

                if idx >= state.session.profiles.len() {
                    return Task::none();
                }

                let selected = state.session.profiles[idx].clone();

                if !selected.persistent {
                    state.post_system("Transient profile cannot be reset.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                if state.is_profile_open_in_any_tab(&selected.name) {
                    state.post_system("Close that profile tab before resetting it.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                state.session.sidebar_confirm =
                    Some(SidebarConfirm::ResetProfile(selected.name.clone()));
                return Task::none();
            }

            Message::SidebarConfirmYes => {
                let Some(confirm) = state.session.sidebar_confirm.clone() else {
                    return Task::none();
                };
                state.session.sidebar_confirm = None;

                match confirm {
                    SidebarConfirm::DeleteProfile(name) => {
                        if state.is_profile_open_in_any_tab(&name) {
                            state.post_system("Close that profile tab before deleting it.");
                            return operation::snap_to_end(state.session.logs_scroll_id.clone());
                        }

                        match storage::delete_contact(&name) {
                            Ok(()) => {
                                if let Some(idx) =
                                    state.session.profiles.iter().position(|p| p.name == name)
                                {
                                    state.session.profiles.remove(idx);
                                }
                                state.session.selected_profile_idx = 0;
                                state.session.profile = state.session.profiles[0].name.clone();
                                state.post_system(format!("Deleted profile: {name}"));
                                return operation::snap_to_end(
                                    state.session.logs_scroll_id.clone(),
                                );
                            }
                            Err(err) => {
                                state.post_system(format!("Delete profile failed: {err}"));
                                return operation::snap_to_end(
                                    state.session.logs_scroll_id.clone(),
                                );
                            }
                        }
                    }
                    SidebarConfirm::ResetProfile(name) => {
                        if state.is_profile_open_in_any_tab(&name) {
                            state.post_system("Close that profile tab before resetting it.");
                            return operation::snap_to_end(state.session.logs_scroll_id.clone());
                        }

                        match storage::reset_contact(&name) {
                            Ok(_) => {
                                state.post_system(format!("Reset profile: {name}"));
                                return operation::snap_to_end(
                                    state.session.logs_scroll_id.clone(),
                                );
                            }
                            Err(err) => {
                                state.post_system(format!("Reset profile failed: {err}"));
                                return operation::snap_to_end(
                                    state.session.logs_scroll_id.clone(),
                                );
                            }
                        }
                    }
                }
            }

            Message::SidebarConfirmNo => {
                state.session.sidebar_confirm = None;
                return Task::none();
            }

            Message::SendPressed => {
                let trimmed = state.session.input.trim().to_string();

                if !trimmed.is_empty() {
                    if state.active_live_conn().is_some() && !state.session.live_ready {
                        state.post_system("Live connection is not ready yet. Wait for secure session to be established.");
                        state.store_active_runtime();
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }

                    if let Some(conn) = state.active_live_conn() {
                        let tab_id = match state.active_tab() {
                            Some(tab) => tab.id,
                            None => return Task::none(),
                        };

                        let enc_payload = state
                            .active_tab()
                            .map(|t| t.e2e.encrypt(trimmed.as_bytes()))
                            .unwrap_or_else(|| trimmed.as_bytes().to_vec());

                        let msg_id = state.generate_msg_id();

                        let frame = Frame {
                            msg_type: MsgType::U,
                            msg_id,
                            payload: enc_payload,
                        };

                        state
                            .session
                            .bubbles
                            .push(Bubble::me_with_id(trimmed, msg_id));

                        state.session.input.clear();
                        state.store_active_runtime();

                        return Task::batch(vec![
                            operation::snap_to_end(state.session.messages_scroll_id.clone()),
                            Task::perform(
                                async move { conn.send_frame(&frame).await.map_err(|e| e.to_string()) },
                                move |result| Message::SendFinished(tab_id, result),
                            ),
                        ]);
                    } else if state.can_send_offline_now() {
                        let tab_id = match state.active_tab() {
                            Some(tab) => tab.id,
                            None => return Task::none(),
                        };

                        let real_idx = match state.find_tab_index_by_id(tab_id) {
                            Some(idx) => idx,
                            None => return Task::none(),
                        };

                        let msg_id = state.generate_msg_id();

                        let (dd, key, blob, send_index, offline_msg_id) = {
                            let tab = match state.opened_tabs.get_mut(real_idx) {
                                Some(tab) => tab,
                                None => return Task::none(),
                            };

                            if tab.deaddrop_put_in_flight {
                                state.post_system("Offline PUT already in progress for this tab.");
                                return operation::snap_to_end(
                                    state.session.logs_scroll_id.clone(),
                                );
                            }

                            tab.deaddrop_put_in_flight = true;
                            Self::set_dd_status(&mut tab.session, "put");

                            let shared_secret = match tab.session.offline_shared_secret {
                                Some(s) => s,
                                None => {
                                    state.post_system("Offline shared secret is missing.");
                                    return operation::snap_to_end(
                                        state.session.logs_scroll_id.clone(),
                                    );
                                }
                            };

                            let my_b32 = match tab.session.my_b32.clone() {
                                Some(v) => v,
                                None => {
                                    state.post_system("My b32 address is missing.");
                                    return operation::snap_to_end(
                                        state.session.logs_scroll_id.clone(),
                                    );
                                }
                            };

                            let peer_b32 = match tab.session.stored_peer.clone() {
                                Some(v) => v,
                                None => {
                                    state.post_system("Locked peer is missing.");
                                    return operation::snap_to_end(
                                        state.session.logs_scroll_id.clone(),
                                    );
                                }
                            };

                            let send_index = tab.session.drop_send_index;

                            let frame = Frame {
                                msg_type: MsgType::U,
                                msg_id,
                                payload: tab.e2e.encrypt(trimmed.as_bytes()),
                            };

                            let key = Self::offline_directional_key(
                                &shared_secret,
                                &my_b32,
                                &peer_b32,
                                "send",
                                send_index,
                            );

                            let blob = match Self::build_offline_blob_for_frame(
                                &tab.e2e,
                                &frame,
                                &shared_secret,
                                &my_b32,
                                &peer_b32,
                            ) {
                                Ok(b) => b,
                                Err(err) => {
                                    state.post_system(format!("Offline blob build failed: {err}"));
                                    return operation::snap_to_end(
                                        state.session.logs_scroll_id.clone(),
                                    );
                                }
                            };

                            (
                                std::sync::Arc::clone(&tab.deaddrop),
                                key,
                                blob,
                                send_index,
                                msg_id,
                            )
                        };

                        state
                            .session
                            .bubbles
                            .push(Bubble::me_offline_with_id(trimmed.clone(), offline_msg_id));

                        state.session.input.clear();
                        state.store_active_runtime();

                        return Task::batch(vec![
                            operation::snap_to_end(state.session.messages_scroll_id.clone()),
                            Task::perform(
                                async move {
                                    let pow_key = key.clone();
                                    let pow_blob = blob.clone();

                                    let pow_counter = tokio::task::spawn_blocking(move || {
                                        DeadDropClient::find_pow_counter_for(
                                            20, &pow_key, &pow_blob,
                                        )
                                    })
                                    .await
                                    .map_err(|e| e.to_string())?;

                                    let mut dd = dd.lock().await;
                                    let (status, drops, stats) = dd
                                        .put_with_pow_counter_and_stats(&key, &blob, pow_counter)
                                        .await;

                                    Ok::<(String, Vec<String>, u64, u64, Vec<DeaddropOpStat>), String>(
                                        (status, drops, send_index, offline_msg_id, stats),
                                    )
                                },
                                move |result| Message::OfflinePutFinished(tab_id, result),
                            ),
                        ]);
                    } else {
                        state.post_system("No live connection.");
                        state.store_active_runtime();
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }
                }
            }

            Message::ActionPressed(action) => match action {
                GuiAction::Connect => {
                    if state.session.profile != "default" {
                        if let Some(peer) = state.session.stored_peer.clone() {
                            let sam = state
                                .active_tab()
                                .map(|t| t.sam.clone())
                                .unwrap_or_default();
                            let tab_id = match state.active_tab() {
                                Some(tab) => tab.id,
                                None => return Task::none(),
                            };

                            state.session.pending_action = None;
                            state.session.action_param.clear();
                            state.post_system(format!("Connecting to locked peer {peer}..."));
                            state.store_active_runtime();

                            return Task::perform(
                                async move {
                                    sam.stream_connect(&peer)
                                        .await
                                        .map(|conn| (peer, conn))
                                        .map_err(|e| e.to_string())
                                },
                                move |result| Message::ConnectFinished(tab_id, result),
                            );
                        }
                    }

                    state.session.pending_action = Some(action);
                    state.session.action_param.clear();
                    state.store_active_runtime();
                    return Task::none();
                }

                GuiAction::Disconnect => {
                    let tab_id = match state.active_tab() {
                        Some(tab) => tab.id,
                        None => return Task::none(),
                    };

                    let live = state.active_live_conn();
                    let pending = state.active_pending_conn();
                    state.set_active_live_conn(None);
                    state.set_active_pending_conn(None);

                    state.session.pending_peer_addr = None;
                    state.session.pending_peer_dest_b64 = None;
                    state.session.call_blink_on = true;
                    state.session.call_blink_ticks = 0;
                    state.mock_disconnect();

                    if let Some(tab) = state.active_tab_mut() {
                        tab.e2e = E2E::new(tab.session.pq_enabled);
                    }

                    state.session.accept_armed = true;
                    state.post_system("Incoming accept loop re-armed.");

                    let mut tasks = vec![
                        operation::snap_to_end(state.session.logs_scroll_id.clone()),
                        state.accept_task(tab_id),
                    ];

                    if let Some(conn) = live {
                        let quit_frame = state.make_signal_frame("QUIT");
                        let conn_for_send = conn.clone();

                        tasks.push(Task::perform(
                            async move {
                                conn_for_send
                                    .send_frame(&quit_frame)
                                    .await
                                    .map_err(|e| e.to_string())
                            },
                            move |result| Message::QuitSignalSent(tab_id, result),
                        ));

                        tasks.push(Task::perform(
                            async move { conn.close().await.map_err(|e| e.to_string()) },
                            move |result| Message::CloseFinished(tab_id, result),
                        ));
                    }

                    if let Some(conn) = pending {
                        tasks.push(Task::perform(
                            async move { conn.close().await.map_err(|e| e.to_string()) },
                            move |result| Message::CloseFinished(tab_id, result),
                        ));
                    }

                    state.store_active_runtime();
                    return Task::batch(tasks);
                }

                GuiAction::Accept => {
                    let tab_id = match state.active_tab() {
                        Some(tab) => tab.id,
                        None => return Task::none(),
                    };

                    state.accept_pending();
                    state.store_active_runtime();

                    if let (Some(conn), Some(my_dest_b64)) = (
                        state.active_live_conn(),
                        state.session.my_pub_dest_b64.clone(),
                    ) {
                        let frame_s = Frame {
                            msg_type: MsgType::S,
                            msg_id: state.generate_msg_id(),
                            payload: my_dest_b64.into_bytes(),
                        };

                        let frame_k = Frame {
                            msg_type: MsgType::K,
                            msg_id: state.generate_msg_id(),
                            payload: state
                                .active_tab()
                                .map(|t| t.e2e.public_bytes())
                                .unwrap_or_default(),
                        };

                        return Task::batch(vec![
                            operation::snap_to_end(state.session.logs_scroll_id.clone()),
                            Task::perform(
                                async move {
                                    conn.send_frame(&frame_s).await.map_err(|e| e.to_string())?;
                                    conn.send_frame(&frame_k).await.map_err(|e| e.to_string())
                                },
                                move |result| Message::SendFinished(tab_id, result),
                            ),
                        ]);
                    }

                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                GuiAction::Decline => {
                    let tab_id = match state.active_tab() {
                        Some(tab) => tab.id,
                        None => return Task::none(),
                    };

                    let pending = state.active_pending_conn();
                    state.set_active_pending_conn(None);
                    state.decline_pending();
                    state.post_system("Incoming accept loop re-armed.");

                    let mut tasks = vec![
                        operation::snap_to_end(state.session.logs_scroll_id.clone()),
                        state.accept_task(tab_id),
                    ];

                    if let Some(conn) = pending {
                        tasks.push(Task::perform(
                            async move { conn.close().await.map_err(|e| e.to_string()) },
                            move |result| Message::CloseFinished(tab_id, result),
                        ));
                    }

                    state.store_active_runtime();
                    return Task::batch(tasks);
                }

                GuiAction::Lock => {
                    state.session.pending_action = Some(action);
                    state.session.action_param.clear();
                    state.store_active_runtime();
                    return Task::none();
                }

                GuiAction::Unlock => {
                    state.session.pending_action = Some(action);
                    state.session.action_param.clear();
                    state.store_active_runtime();
                    return Task::none();
                }

                GuiAction::Offline => {
                    let tab_id = match state.active_tab() {
                        Some(tab) => tab.id,
                        None => return Task::none(),
                    };

                    state.mock_offline();
                    state.store_active_runtime();

                    let dd_task = state.ensure_deaddrop_runtime_started(tab_id);

                    return Task::batch(vec![
                        operation::snap_to_end(state.session.logs_scroll_id.clone()),
                        dd_task,
                    ]);
                }

                GuiAction::Online => {
                    state.mock_online();
                    state.store_active_runtime();
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                GuiAction::Pq => {
                    state.toggle_pq();
                    state.store_active_runtime();
                    state.save_active_contact_meta();
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                GuiAction::SendFile => {
                    if !state.session.live_ready {
                        state.post_system("File send requires an active live connection.");
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }

                    return Task::perform(
                        async move {
                            rfd::AsyncFileDialog::new()
                                .pick_file()
                                .await
                                .map(|f| f.path().to_path_buf())
                        },
                        Message::FileChosen,
                    );
                }

                GuiAction::SendImage => {
                    if !state.session.live_ready {
                        state.post_system("Image send requires an active live connection.");
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }

                    return Task::perform(
                        async move {
                            rfd::AsyncFileDialog::new()
                                .add_filter("Images", &["png", "jpg", "jpeg", "gif", "bmp", "webp"])
                                .pick_file()
                                .await
                                .map(|f| f.path().to_path_buf())
                        },
                        Message::ImageChosen,
                    );
                }

                GuiAction::CopyMyB32 => {
                    state.copy_my_b32_to_clipboard();
                    state.store_active_runtime();
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                GuiAction::Help => {
                    state.post_system("Help window not implemented yet.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                GuiAction::DdList => {
                    if !Self::deaddrop_panel_allowed(&state.session) {
                        state.post_system(
                            "Deaddrop servers are available only for persistent locked profiles.",
                        );
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }

                    state.session.show_deaddrop_panel = !state.session.show_deaddrop_panel;
                    state.store_active_runtime();
                    return operation::snap_to_end(state.session.messages_scroll_id.clone());
                }
            },

            Message::CopyStatusMyB32Pressed => {
                state.copy_my_b32_to_clipboard();
                state.store_active_runtime();
                return operation::snap_to_end(state.session.logs_scroll_id.clone());
            }

            Message::CopyStatusPeerB32Pressed => {
                state.copy_peer_b32_to_clipboard();
                state.store_active_runtime();
                return operation::snap_to_end(state.session.logs_scroll_id.clone());
            }

            Message::ActionParamChanged(value) => {
                state.session.action_param = value;
                state.store_active_runtime();
                return Task::none();
            }

            Message::ActionConfirm => {
                let value = state.session.action_param.trim().to_string();

                if let Some(action) = state.session.pending_action {
                    match action {
                        GuiAction::Connect => {
                            if !value.is_empty() {
                                let sam = state
                                    .active_tab()
                                    .map(|t| t.sam.clone())
                                    .unwrap_or_default();
                                let peer = value.clone();
                                let tab_id = match state.active_tab() {
                                    Some(tab) => tab.id,
                                    None => return Task::none(),
                                };

                                state.session.pending_action = None;
                                state.session.action_param.clear();
                                state.post_system(format!("Connecting to {peer}..."));
                                state.store_active_runtime();

                                return Task::perform(
                                    async move {
                                        sam.stream_connect(&peer)
                                            .await
                                            .map(|conn| (peer, conn))
                                            .map_err(|e| e.to_string())
                                    },
                                    move |result| Message::ConnectFinished(tab_id, result),
                                );
                            }
                        }

                        GuiAction::Lock => {
                            state.session.pending_action = None;
                            state.session.action_param.clear();

                            if !state.session.live_ready {
                                state.post_system("Lock requires an active connected peer.");
                                state.store_active_runtime();
                                return operation::snap_to_end(
                                    state.session.logs_scroll_id.clone(),
                                );
                            }

                            let Some(peer_b32) = state.session.current_peer_addr.clone() else {
                                state.post_system("Cannot lock: current peer address is unknown.");
                                state.store_active_runtime();
                                return operation::snap_to_end(
                                    state.session.logs_scroll_id.clone(),
                                );
                            };

                            let Some(peer_dest_b64) = state.session.current_peer_dest_b64.clone()
                            else {
                                state.post_system(
                                    "Cannot lock: current peer destination is unknown.",
                                );
                                state.store_active_runtime();
                                return operation::snap_to_end(
                                    state.session.logs_scroll_id.clone(),
                                );
                            };

                            state.session.stored_peer = Some(peer_b32.clone());
                            state.session.stored_peer_dest_b64 = Some(peer_dest_b64);
                            state.session.offline_shared_secret = None;
                            state.session.drop_send_index = 0;
                            state.session.drop_recv_base = 0;
                            state.session.drop_window = 8;
                            state.session.consumed_drop_recv.clear();
                            state.session.tofu_verified = true;
                            state.session.tofu_mismatch = false;

                            state.post_system(format!("Locked peer: {peer_b32}"));
                            state.store_active_runtime();
                            state.save_active_contact_meta();

                            let send_secret_task = if state
                                .active_tab()
                                .map(|tab| tab.e2e.ready() && tab.session.live_ready)
                                .unwrap_or(false)
                            {
                                if let Some(tab_id) = state.active_tab().map(|t| t.id) {
                                    state.send_offline_secret_if_needed_task(tab_id)
                                } else {
                                    Task::none()
                                }
                            } else {
                                Task::none()
                            };

                            return Task::batch(vec![
                                operation::snap_to_end(state.session.logs_scroll_id.clone()),
                                send_secret_task,
                            ]);
                        }

                        GuiAction::Unlock => {
                            state.session.pending_action = None;
                            state.session.action_param.clear();
                            state.mock_unlock();
                            state.store_active_runtime();
                            state.save_active_contact_meta();
                            return operation::snap_to_end(state.session.logs_scroll_id.clone());
                        }

                        _ => {}
                    }
                }

                state.session.pending_action = None;
                state.session.action_param.clear();
                state.store_active_runtime();
                return operation::snap_to_end(state.session.logs_scroll_id.clone());
            }

            Message::ActionCancel => {
                state.session.pending_action = None;
                state.session.action_param.clear();
                state.store_active_runtime();
                return Task::none();
            }

            Message::ToggleLogsPressed => {
                state.session.show_logs = !state.session.show_logs;
                state.store_active_runtime();
                if state.session.show_logs {
                    return Task::batch(vec![
                        operation::snap_to_end(state.session.messages_scroll_id.clone()),
                        operation::snap_to_end(state.session.logs_scroll_id.clone()),
                    ]);
                }

                return operation::snap_to_end(state.session.messages_scroll_id.clone());
            }

            Message::DdServerInputChanged(value) => {
                state.session.deaddrop_server_input = value;
                state.store_active_runtime();
                return Task::none();
            }

            Message::DdServerAddPressed => {
                if !Self::deaddrop_panel_allowed(&state.session) {
                    state.post_system(
                        "Deaddrop servers are available only for persistent locked profiles.",
                    );
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                let server = state.session.deaddrop_server_input.trim().to_lowercase();

                if !Self::is_valid_deaddrop_server(&server) {
                    state.post_system("Invalid deaddrop server address.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                if state
                    .session
                    .deaddrop_servers
                    .iter()
                    .any(|existing| existing == &server)
                {
                    state.post_system("Deaddrop server already exists.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                state.session.deaddrop_servers.push(server.clone());
                state
                    .session
                    .deaddrop_stats
                    .entry(server.clone())
                    .or_insert_with(storage::DeaddropServerStat::default);
                Self::rank_deaddrop_servers(&mut state.session);
                state.session.deaddrop_server_input.clear();
                state.post_system(format!("Added deaddrop server: {server}"));
                state.store_active_runtime();
                state.sync_active_deaddrop_servers();
                state.save_active_contact_meta();
                return operation::snap_to_end(state.session.logs_scroll_id.clone());
            }

            Message::DdServerDeletePressed(index) => {
                if !Self::deaddrop_panel_allowed(&state.session) {
                    state.post_system(
                        "Deaddrop servers are available only for persistent locked profiles.",
                    );
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                if index >= state.session.deaddrop_servers.len() {
                    state.post_system("Invalid deaddrop server number.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                let removed = state.session.deaddrop_servers.remove(index);
                state.session.deaddrop_stats.remove(&removed);
                state.post_system(format!("Removed deaddrop server: {removed}"));
                state.store_active_runtime();
                state.sync_active_deaddrop_servers();
                state.save_active_contact_meta();
                return operation::snap_to_end(state.session.logs_scroll_id.clone());
            }

            Message::DdServerSharePressed => {
                if !Self::deaddrop_panel_allowed(&state.session) {
                    state.post_system(
                        "Deaddrop servers are available only for persistent locked profiles.",
                    );
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                let Some(tab_id) = state.active_tab().map(|tab| tab.id) else {
                    return Task::none();
                };

                if state.active_live_conn().is_none() {
                    state.post_system("No active connection to share deaddrop servers.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                state.post_system(format!(
                    "Sharing {} deaddrop servers with peer.",
                    state.session.deaddrop_servers.len()
                ));
                state.store_active_runtime();
                return Task::batch(vec![
                    operation::snap_to_end(state.session.logs_scroll_id.clone()),
                    state.send_deaddrop_server_list_task(tab_id),
                ]);
            }

            Message::WindowCloseRequested(window_id) => {
                return state.begin_shutdown(ShutdownTarget::Window(window_id));
            }

            Message::ProcessShutdownRequested => {
                return state.begin_shutdown(ShutdownTarget::Runtime);
            }

            Message::ExitAfterNotify(target) => {
                if let Err(err) = state.encrypt_for_shutdown() {
                    state.post_system(format!(
                        "Vault encryption failed; app was not closed: {err}"
                    ));
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }
                return match target {
                    ShutdownTarget::Window(window_id) => window::close(window_id),
                    ShutdownTarget::Runtime => exit(),
                };
            }

            Message::SamInitialized(tab_id, result) => match result {
                Ok((sam, init)) => {
                    let mut profile_name: Option<String> = None;

                    if let Some(tab) = state.tab_by_id_mut(tab_id) {
                        profile_name = Some(tab.session.profile.clone());

                        tab.sam = sam;
                        tab.session.my_b32 = Some(init.my_b32.clone());
                        tab.session.my_dest_b64 = Some(init.my_dest_b64.clone());
                        tab.session.my_pub_dest_b64 = Some(init.my_pub_dest_b64.clone());
                        tab.session.sam_session_id = Some(init.session_id.clone());
                        tab.session.network_status = NetworkStatus::LocalOk;
                        tab.session.accept_armed = true;
                        tab.session
                            .log_lines
                            .push("SAM session initialized.".into());
                        tab.session
                            .log_lines
                            .push(format!("My address: {}", init.my_b32));
                        tab.session
                            .log_lines
                            .push("Incoming accept loop started.".into());

                        tab.meta.initializing = false;
                        tab.meta.initialized = true;
                    } else {
                        return Task::none();
                    }

                    if let Some(name) = profile_name {
                        if name != "default" {
                            state.save_active_contact_meta_for_name(&name);
                        }
                    }

                    if let Some(idx) = state.find_tab_index_by_id(tab_id) {
                        state.sync_tab_meta(idx);
                    }

                    if state.active_tab().map(|t| t.id) == Some(tab_id) {
                        state.load_active_runtime();
                    }

                    return state.accept_task(tab_id);
                }
                Err(err) => {
                    if let Some(tab) = state.tab_by_id_mut(tab_id) {
                        tab.meta.initializing = false;
                        tab.meta.initialized = false;
                        tab.session
                            .log_lines
                            .push(format!("SAM init failed: {err}"));
                    }

                    if state.active_tab().map(|t| t.id) == Some(tab_id) {
                        state.load_active_runtime();
                    }

                    return Task::none();
                }
            },

            Message::ConnectFinished(tab_id, result) => {
                match result {
                    Ok((peer, conn)) => {
                        let msg_id_s = state.generate_msg_id();
                        let msg_id_k = state.generate_msg_id();

                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            tab.pending_conn = None;
                            tab.session.pending_peer_addr = None;
                            tab.session.pending_peer_dest_b64 = None;
                            tab.session.accept_armed = false;
                            tab.session.call_blink_on = true;
                            tab.session.call_blink_ticks = 0;
                            tab.e2e = E2E::new(tab.session.pq_enabled);
                            tab.live_conn = Some(conn.clone());

                            tab.session.current_peer_addr = Some(peer.clone());
                            tab.session.current_peer_dest_b64 = None;
                            tab.session.peer_b32 = Some(peer.clone());
                            tab.session.network_status = NetworkStatus::Visible;
                            tab.session.live_ready = false;
                            tab.session.offline_mode = false;
                            tab.session.log_lines.push(format!(
                                "Handshake sent to {peer}. Establishing secure session..."
                            ));

                            if let Some(my_dest_b64) = tab.session.my_pub_dest_b64.clone() {
                                let line = format!("{my_dest_b64}\n");
                                let frame_s = Frame {
                                    msg_type: MsgType::S,
                                    msg_id: msg_id_s,
                                    payload: my_dest_b64.clone().into_bytes(),
                                };

                                let frame_k = Frame {
                                    msg_type: MsgType::K,
                                    msg_id: msg_id_k,
                                    payload: tab.e2e.public_bytes(),
                                };

                                if let Some(idx) = state.find_tab_index_by_id(tab_id) {
                                    state.sync_tab_meta(idx);
                                }

                                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                                    state.load_active_runtime();
                                }

                                return Task::batch(vec![Task::perform(
                                    async move {
                                        conn.send_raw_line(&line)
                                            .await
                                            .map_err(|e| e.to_string())?;
                                        conn.send_frame(&frame_s)
                                            .await
                                            .map_err(|e| e.to_string())?;
                                        conn.send_frame(&frame_k).await.map_err(|e| e.to_string())
                                    },
                                    move |result| Message::SendFinished(tab_id, result),
                                )]);
                            }
                        } else {
                            return Task::none();
                        }
                    }
                    Err(err) => {
                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            tab.session.log_lines.push(format!("Connect failed: {err}"));
                        }
                    }
                }

                if let Some(idx) = state.find_tab_index_by_id(tab_id) {
                    state.sync_tab_meta(idx);
                }

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                }

                return Task::none();
            }

            Message::IncomingAccepted(tab_id, result) => {
                match result {
                    Ok(incoming) => {
                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            tab.e2e = E2E::new(tab.session.pq_enabled);
                            tab.pending_conn = Some(incoming.conn);
                            tab.session.pending_peer_addr = Some(incoming.peer_b32.clone());
                            tab.session.pending_peer_dest_b64 =
                                Some(incoming.peer_dest_b64.clone());

                            let tofu_ok = match &tab.session.stored_peer_dest_b64 {
                                Some(stored) => stored == &incoming.peer_dest_b64,
                                None => true,
                            };

                            if tofu_ok {
                                if tab.session.stored_peer_dest_b64.is_some() {
                                    tab.session.tofu_verified = true;
                                    tab.session.tofu_mismatch = false;
                                } else {
                                    tab.session.tofu_verified = false;
                                    tab.session.tofu_mismatch = false;
                                }
                            } else {
                                tab.session.tofu_verified = false;
                                tab.session.tofu_mismatch = true;
                                tab.session.log_lines.push(format!(
                                    "TOFU mismatch for incoming peer: {}",
                                    incoming.peer_b32
                                ));
                            }

                            tab.session.accept_armed = false;
                            tab.session.call_blink_on = true;
                            tab.session.call_blink_ticks = 0;
                            tab.session.log_lines.push(format!(
                                "Incoming call from {}. Awaiting Accept / Decline.",
                                incoming.peer_b32
                            ));
                            tab.meta.has_incoming = true;
                        } else {
                            return Task::none();
                        }
                    }
                    Err(err) => {
                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            tab.session.log_lines.push(format!("Accept failed: {err}"));
                        }
                    }
                }

                if let Some(idx) = state.find_tab_index_by_id(tab_id) {
                    state.sync_tab_meta(idx);
                }

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                }

                return Task::none();
            }

            Message::SendFinished(tab_id, result) => {
                if let Err(err) = result {
                    if let Some(tab) = state.tab_by_id_mut(tab_id) {
                        tab.session.log_lines.push(format!("Send failed: {err}"));
                    }

                    if state.active_tab().map(|t| t.id) == Some(tab_id) {
                        state.load_active_runtime();
                    }
                }
            }

            Message::CloseFinished(tab_id, result) => {
                if let Err(err) = result {
                    if let Some(tab) = state.tab_by_id_mut(tab_id) {
                        tab.session.log_lines.push(format!("Close failed: {err}"));
                    }

                    if state.active_tab().map(|t| t.id) == Some(tab_id) {
                        state.load_active_runtime();
                    }
                }
            }

            Message::SamCloseFinished(tab_id, result) => {
                if let Err(err) = result {
                    if let Some(tab) = state.tab_by_id_mut(tab_id) {
                        tab.session
                            .log_lines
                            .push(format!("SAM close failed: {err}"));
                    }

                    if state.active_tab().map(|t| t.id) == Some(tab_id) {
                        state.load_active_runtime();
                    }
                }
            }

            Message::QuitSignalSent(tab_id, result) => {
                if let Err(err) = result {
                    if let Some(tab) = state.tab_by_id_mut(tab_id) {
                        tab.session
                            .log_lines
                            .push(format!("QUIT signal send failed: {err}"));
                    }

                    if state.active_tab().map(|t| t.id) == Some(tab_id) {
                        state.load_active_runtime();
                    }
                }
            }

            Message::Tick => {
                let mut tasks: Vec<Task<Message>> = Vec::new();

                for idx in 0..state.opened_tabs.len() {
                    let mut tab_tasks = state.tick_one_tab(idx);
                    tasks.append(&mut tab_tasks);
                }

                let now_ms = Self::now_epoch_millis();
                let mut poll_tab_ids: Vec<u64> = Vec::new();

                for tab in &mut state.opened_tabs {
                    if !tab.deaddrop_started || !tab.session.offline_mode {
                        continue;
                    }

                    if tab.deaddrop_poll_in_flight || tab.deaddrop_put_in_flight {
                        continue;
                    }

                    if tab.session.my_b32.is_none()
                        || tab.session.stored_peer.is_none()
                        || tab.session.offline_shared_secret.is_none()
                    {
                        continue;
                    }

                    let my_b32 = tab.session.my_b32.clone().unwrap();
                    let peer_b32 = tab.session.stored_peer.clone().unwrap();

                    let recv_window =
                        Self::get_deaddrop_recv_window(&tab.session, &my_b32, &peer_b32);
                    if recv_window.is_empty() {
                        continue;
                    }

                    if tab.deaddrop_last_poll_ms != 0
                        && now_ms.saturating_sub(tab.deaddrop_last_poll_ms)
                            < DEADDROP_POLL_INTERVAL_MS
                    {
                        continue;
                    }

                    tab.deaddrop_poll_in_flight = true;
                    tab.deaddrop_poll_queue = recv_window;
                    Self::set_dd_status(&mut tab.session, "poll");
                    poll_tab_ids.push(tab.id);
                }

                for tab_id in poll_tab_ids {
                    tasks.push(state.start_next_deaddrop_poll_key_task(tab_id));
                }

                state.refresh_visible_from_active_tab();

                if tasks.is_empty() {
                    return Task::none();
                }

                return Task::batch(tasks);
            }

            Message::FileChosen(path_opt) => {
                let Some(path) = path_opt else {
                    return Task::none();
                };

                let filename = match path.file_name().and_then(|s| s.to_str()) {
                    Some(v) => v.to_string(),
                    None => {
                        state.post_system("Invalid file name.");
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }
                };

                let total_bytes = match std::fs::metadata(&path) {
                    Ok(m) => m.len(),
                    Err(e) => {
                        state.post_system(format!("File metadata failed: {e}"));
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }
                };

                if total_bytes == 0 {
                    state.post_system("File is empty.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                if total_bytes > MAX_FILE_SIZE as u64 {
                    state.post_system(format!(
                        "File is too large ({} bytes). Maximum is {} bytes.",
                        total_bytes, MAX_FILE_SIZE
                    ));
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                let file = match StdFile::open(&path) {
                    Ok(f) => f,
                    Err(e) => {
                        state.post_system(format!("File open failed: {e}"));
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }
                };

                state.push_outgoing_file_bubble(filename.clone(), total_bytes);

                if let Some(tab) = state.active_tab_mut() {
                    tab.outgoing_file = Some(file);
                    tab.outgoing_filename = Some(filename);
                    tab.outgoing_total = total_bytes;
                    tab.outgoing_sent = 0;
                    tab.outgoing_phase = OutgoingFilePhase::Header;
                    tab.outgoing_send_in_flight = false;
                }

                state.store_active_runtime();

                return operation::snap_to_end(state.session.messages_scroll_id.clone());
            }

            Message::ImageChosen(path_opt) => {
                let Some(path) = path_opt else {
                    return Task::none();
                };

                if let Some(tab) = state.active_tab() {
                    if tab.live_conn.is_none() || !tab.session.live_ready {
                        state.post_system("Image send requires a live secure chat.");
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }

                    if tab.outgoing_phase != OutgoingFilePhase::Idle
                        || tab.outgoing_image_phase != OutgoingImagePhase::Idle
                    {
                        state.post_system("Another transfer is already in progress.");
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }
                } else {
                    state.post_system("Open a chat tab before sending an image.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                let filename = match path.file_name().and_then(|s| s.to_str()) {
                    Some(v) => v.replace('|', "_"),
                    None => {
                        state.post_system("Invalid image file name.");
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }
                };

                if Self::image_mime_for_path(&path).is_none() {
                    state.post_system("Unsupported image type.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                let (bytes, mime) = match Self::prepare_image_preview_bytes(&path) {
                    Ok(v) => v,
                    Err(err) => {
                        state.post_system(err);
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }
                };

                if bytes.is_empty() {
                    state.post_system("Image preview is empty.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                if bytes.len() > MAX_FILE_SIZE {
                    state.post_system(format!("Image preview too large ({} bytes).", bytes.len()));
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                let msg_id = state.generate_msg_id();

                state.session.bubbles.push(Bubble {
                    author: "Me".into(),
                    content: BubbleContent::Image(Self::image_bubble_data(bytes.clone())),
                    mine: true,
                    offline: false,
                    timestamp_utc: Self::now_utc_hms(),
                    msg_id: Some(msg_id),
                    delivered: false,
                });

                if let Some(tab) = state.active_tab_mut() {
                    tab.outgoing_image_name = Some(filename);
                    tab.outgoing_image_mime = Some(mime);
                    tab.outgoing_image_total = bytes.len() as u64;
                    tab.outgoing_image_sent = 0;
                    tab.outgoing_image_msg_id = msg_id;
                    tab.outgoing_image_bytes = bytes;
                    tab.outgoing_image_phase = OutgoingImagePhase::Header;
                    tab.outgoing_image_send_in_flight = false;
                }

                state.store_active_runtime();
                return operation::snap_to_end(state.session.messages_scroll_id.clone());
            }

            Message::OutgoingFileHeaderSent(tab_id, result) => {
                if let Some(tab) = state.tab_by_id_mut(tab_id) {
                    tab.outgoing_send_in_flight = false;

                    match result {
                        Ok(()) => {
                            tab.outgoing_phase = OutgoingFilePhase::Chunks;
                            Self::update_outgoing_file_bubble(
                                tab,
                                tab.outgoing_sent,
                                "Sending...".into(),
                                false,
                                false,
                            );
                        }
                        Err(err) => {
                            Self::update_outgoing_file_bubble(
                                tab,
                                tab.outgoing_sent,
                                format!("Send failed: {err}"),
                                false,
                                true,
                            );
                            Self::clear_outgoing_file_state(tab);
                        }
                    }
                }

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                    return operation::snap_to_end(state.session.messages_scroll_id.clone());
                }

                return Task::none();
            }

            Message::OutgoingFileChunkSent(tab_id, result) => {
                if let Some(tab) = state.tab_by_id_mut(tab_id) {
                    tab.outgoing_send_in_flight = false;

                    match result {
                        Ok(sent_now) => {
                            tab.outgoing_sent += sent_now as u64;

                            if tab.outgoing_sent >= tab.outgoing_total {
                                tab.outgoing_phase = OutgoingFilePhase::End;
                            } else {
                                tab.outgoing_phase = OutgoingFilePhase::Chunks;
                            }

                            Self::update_outgoing_file_bubble(
                                tab,
                                tab.outgoing_sent,
                                format!("Sending... {}/{}", tab.outgoing_sent, tab.outgoing_total),
                                false,
                                false,
                            );
                        }
                        Err(err) => {
                            Self::update_outgoing_file_bubble(
                                tab,
                                tab.outgoing_sent,
                                format!("Send failed: {err}"),
                                false,
                                true,
                            );
                            Self::clear_outgoing_file_state(tab);
                        }
                    }
                }

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                    return operation::snap_to_end(state.session.messages_scroll_id.clone());
                }

                return Task::none();
            }

            Message::OutgoingFileEndSent(tab_id, result) => {
                if let Some(tab) = state.tab_by_id_mut(tab_id) {
                    tab.outgoing_send_in_flight = false;

                    match result {
                        Ok(()) => {
                            Self::update_outgoing_file_bubble(
                                tab,
                                tab.outgoing_total,
                                "Sent".into(),
                                true,
                                false,
                            );
                            tab.outgoing_file = None;
                            tab.outgoing_filename = None;
                            tab.outgoing_total = 0;
                            tab.outgoing_sent = 0;
                            tab.outgoing_phase = OutgoingFilePhase::Idle;
                            tab.outgoing_send_in_flight = false;
                        }
                        Err(err) => {
                            Self::update_outgoing_file_bubble(
                                tab,
                                tab.outgoing_sent,
                                format!("Send failed: {err}"),
                                false,
                                true,
                            );
                            Self::clear_outgoing_file_state(tab);
                        }
                    }
                }

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                    return operation::snap_to_end(state.session.messages_scroll_id.clone());
                }

                return Task::none();
            }

            Message::OutgoingImageHeaderSent(tab_id, result) => {
                if let Some(tab) = state.tab_by_id_mut(tab_id) {
                    tab.outgoing_image_send_in_flight = false;

                    match result {
                        Ok(()) => {
                            tab.outgoing_image_phase = OutgoingImagePhase::Chunks;
                        }
                        Err(err) => {
                            tab.session
                                .log_lines
                                .push(format!("Image send failed: {err}"));
                            Self::clear_outgoing_image_state(tab);
                        }
                    }
                }

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                }

                return Task::none();
            }

            Message::OutgoingImageChunkSent(tab_id, result) => {
                if let Some(tab) = state.tab_by_id_mut(tab_id) {
                    tab.outgoing_image_send_in_flight = false;

                    match result {
                        Ok(sent_now) => {
                            tab.outgoing_image_sent += sent_now as u64;

                            if tab.outgoing_image_sent >= tab.outgoing_image_total {
                                tab.outgoing_image_phase = OutgoingImagePhase::End;
                            } else {
                                tab.outgoing_image_phase = OutgoingImagePhase::Chunks;
                            }
                        }
                        Err(err) => {
                            tab.session
                                .log_lines
                                .push(format!("Image send failed: {err}"));
                            Self::clear_outgoing_image_state(tab);
                        }
                    }
                }

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                }

                return Task::none();
            }

            Message::OutgoingImageEndSent(tab_id, result) => {
                if let Some(tab) = state.tab_by_id_mut(tab_id) {
                    tab.outgoing_image_send_in_flight = false;

                    match result {
                        Ok(()) => {
                            tab.session.log_lines.push(format!(
                                "Image sent: {} ({} bytes)",
                                tab.outgoing_image_name
                                    .clone()
                                    .unwrap_or_else(|| "image".into()),
                                tab.outgoing_image_total
                            ));
                            Self::clear_outgoing_image_state(tab);
                        }
                        Err(err) => {
                            tab.session
                                .log_lines
                                .push(format!("Image send failed: {err}"));
                            Self::clear_outgoing_image_state(tab);
                        }
                    }
                }

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                }

                return Task::none();
            }

            Message::UnlockInputChanged(value) => {
                state.unlock_input = value;
                return Task::none();
            }

            Message::UnlockConfirmInputChanged(value) => {
                state.unlock_confirm_input = value;
                return Task::none();
            }

            Message::UnlockPressed => {
                if state.unlock_input.trim().is_empty() {
                    state.unlock_status = "Enter passphrase.".into();
                    return Task::none();
                }

                let base_dir = storage::base_dir();
                let vault_exists = crate::vault::vault_path(&base_dir).exists();
                let plaintext_exists = base_dir.exists();
                let base = base_dir.to_string_lossy().to_string();

                let open_result = if vault_exists {
                    crate::vault::fs_decrypt(&base, &state.unlock_input)
                } else if plaintext_exists {
                    Err(crate::vault::VaultError::Format(
                        "plaintext storage exists without encrypted vault; recovery is required"
                            .into(),
                    ))
                } else {
                    if state.unlock_confirm_input.trim().is_empty() {
                        state.unlock_status = "Confirm passphrase.".into();
                        return Task::none();
                    }
                    if state.unlock_input != state.unlock_confirm_input {
                        state.unlock_status = "Passphrases do not match.".into();
                        return Task::none();
                    }

                    storage::ensure_base_layout().map_err(|e| {
                        crate::vault::VaultError::Format(format!(
                            "failed to initialize storage: {e}"
                        ))
                    })
                };

                match open_result {
                    Ok(()) => match state.load_unlocked_storage() {
                        Ok(()) => {
                            state.startup_gate = StartupGate::Unlocked;
                            state.unlock_confirm_input.clear();
                            state.unlock_status = if vault_exists {
                                "Unlocked.".into()
                            } else {
                                "New encrypted storage initialized. It will be encrypted on exit."
                                    .into()
                            };
                        }
                        Err(err) => {
                            state.unlock_status = format!("Storage load failed: {err}");
                        }
                    },
                    Err(err) => {
                        state.unlock_status = format!("Unlock failed: {err}");
                    }
                }
                return Task::none();
            }

            Message::BackupExportPassphraseChanged(value) => {
                if state.backup_operation == BackupOperation::Idle {
                    state.backup_export_passphrase = value;
                }
                return Task::none();
            }

            Message::BackupImportPassphraseChanged(value) => {
                if state.backup_operation == BackupOperation::Idle {
                    state.backup_import_passphrase = value;
                }
                return Task::none();
            }

            Message::BackupExportIncludeFilesChanged(value) => {
                if state.backup_operation == BackupOperation::Idle {
                    state.backup_export_include_files = value;
                }
                return Task::none();
            }

            Message::BackupImportRestoreFilesChanged(value) => {
                if state.backup_operation == BackupOperation::Idle {
                    state.backup_import_restore_files = value;
                }
                return Task::none();
            }

            Message::SamHostInputChanged(value) => {
                if !state.sam_test_in_flight {
                    state.sam_host_input = value;
                }
                return Task::none();
            }

            Message::SamPortInputChanged(value) => {
                if !state.sam_test_in_flight {
                    state.sam_port_input = value;
                }
                return Task::none();
            }

            Message::SaveSamSettingsPressed => {
                let host = state.sam_host_input.trim().to_string();
                if host.is_empty() {
                    state.sam_status = "SAM host cannot be empty.".into();
                    return Task::none();
                }

                let Ok(port) = state.sam_port_input.trim().parse::<u16>() else {
                    state.sam_status = "SAM port must be 1-65535.".into();
                    return Task::none();
                };
                if port == 0 {
                    state.sam_status = "SAM port must be 1-65535.".into();
                    return Task::none();
                }

                let config = storage::AppConfig {
                    sam_host: host.clone(),
                    sam_port: port,
                };

                match storage::save_app_config(&config) {
                    Ok(()) => {
                        state.sam_host_input = host;
                        state.sam_port_input = port.to_string();
                        state.sam_status =
                            "Saved SAM settings. New chat tabs will use this endpoint.".into();
                    }
                    Err(err) => {
                        state.sam_status = format!("Saving SAM settings failed: {err}");
                    }
                }
                return Task::none();
            }

            Message::TestSamPressed => {
                if state.sam_test_in_flight {
                    return Task::none();
                }

                let host = state.sam_host_input.trim().to_string();
                if host.is_empty() {
                    state.sam_status = "SAM host cannot be empty.".into();
                    return Task::none();
                }

                let Ok(port) = state.sam_port_input.trim().parse::<u16>() else {
                    state.sam_status = "SAM port must be 1-65535.".into();
                    return Task::none();
                };
                if port == 0 {
                    state.sam_status = "SAM port must be 1-65535.".into();
                    return Task::none();
                }

                state.sam_test_in_flight = true;
                state.sam_status = format!("Testing SAM at {host}:{port}...");
                return Task::perform(
                    async move {
                        SamClient::test_endpoint(host.clone(), port)
                            .await
                            .map(|hello| format!("SAM OK at {host}:{port}: {}", hello.trim()))
                            .map_err(|e| format!("SAM test failed at {host}:{port}: {e}"))
                    },
                    Message::SamTestFinished,
                );
            }

            Message::SamTestFinished(result) => {
                state.sam_test_in_flight = false;
                state.sam_status = match result {
                    Ok(msg) => msg,
                    Err(err) => err,
                };
                return Task::none();
            }

            Message::WipeAllPassphraseChanged(value) => {
                if state.backup_operation == BackupOperation::Idle {
                    state.wipe_all_passphrase = value;
                }
                return Task::none();
            }

            Message::ProfileExportPassphraseChanged(value) => {
                if state.backup_operation == BackupOperation::Idle {
                    state.profile_export_passphrase = value;
                }
                return Task::none();
            }

            Message::ProfileImportPassphraseChanged(value) => {
                if state.backup_operation == BackupOperation::Idle {
                    state.profile_import_passphrase = value;
                }
                return Task::none();
            }

            Message::ExportBackupPressed => {
                if state.backup_operation != BackupOperation::Idle {
                    state.backup_export_status =
                        "Another backup operation is already active.".into();
                    return Task::none();
                }

                if state.backup_export_passphrase.trim().is_empty() {
                    state.backup_export_status = "Enter export passphrase before export.".into();
                    return Task::none();
                }

                if !state.opened_tabs.is_empty() {
                    state.backup_export_status =
                        "Close all chat tabs before exporting a full backup.".into();
                    return Task::none();
                }

                return Task::perform(
                    async move {
                        rfd::AsyncFileDialog::new()
                            .add_filter("IcedComm-I2P backup", &["tcbak"])
                            .set_file_name("icedcomm-i2p-backup-v2.tcbak")
                            .save_file()
                            .await
                            .map(|f| f.path().to_path_buf())
                    },
                    Message::BackupExportPathChosen,
                );
            }

            Message::ImportBackupPressed => {
                if state.backup_operation != BackupOperation::Idle {
                    state.backup_import_status =
                        "Another backup operation is already active.".into();
                    return Task::none();
                }

                if state.backup_import_passphrase.trim().is_empty() {
                    state.backup_import_status = "Enter import passphrase before import.".into();
                    return Task::none();
                }

                return Task::perform(
                    async move {
                        rfd::AsyncFileDialog::new()
                            .add_filter("IcedComm-I2P backup", &["tcbak"])
                            .pick_file()
                            .await
                            .map(|f| f.path().to_path_buf())
                    },
                    Message::BackupImportPathChosen,
                );
            }

            Message::WipeAllPressed => {
                if state.backup_operation != BackupOperation::Idle {
                    state.wipe_all_status = "Another backup operation is already active.".into();
                    return Task::none();
                }

                if !state.opened_tabs.is_empty() {
                    state.wipe_all_status = "Close all chat tabs before wiping storage.".into();
                    return Task::none();
                }

                if state.wipe_all_passphrase.trim().is_empty() {
                    state.wipe_all_status = "Enter unlock passphrase before wipe.".into();
                    return Task::none();
                }

                if !state.unlock_input.is_empty() && state.wipe_all_passphrase != state.unlock_input
                {
                    state.wipe_all_status =
                        "Wipe passphrase does not match unlock passphrase.".into();
                    return Task::none();
                }

                state.backup_operation = BackupOperation::AwaitingWipeConfirm;
                state.wipe_all_status = "Confirm wipe: this deletes all profiles and files.".into();
                return Task::none();
            }

            Message::ExportProfileBackupPressed => {
                if state.backup_operation != BackupOperation::Idle {
                    state.profile_export_status =
                        "Another backup operation is already active.".into();
                    return Task::none();
                }

                let Some(profile) = state
                    .session
                    .profiles
                    .get(state.session.selected_profile_idx)
                    .filter(|profile| profile.persistent)
                else {
                    state.profile_export_status =
                        "Select a persistent profile before profile export.".into();
                    return Task::none();
                };

                if state
                    .opened_tabs
                    .iter()
                    .any(|tab| tab.session.profile == profile.name)
                {
                    state.profile_export_status =
                        format!("Close the {} chat tab before profile export.", profile.name);
                    return Task::none();
                }

                if state.profile_export_passphrase.trim().is_empty() {
                    state.profile_export_status =
                        "Enter profile export passphrase before export.".into();
                    return Task::none();
                }

                let profile_name = profile.name.clone();
                return Task::perform(
                    async move {
                        rfd::AsyncFileDialog::new()
                            .add_filter("IcedComm-I2P backup", &["tcbak"])
                            .set_file_name(format!("{profile_name}-profile-v2.tcbak"))
                            .save_file()
                            .await
                            .map(|f| f.path().to_path_buf())
                    },
                    Message::ProfileBackupExportPathChosen,
                );
            }

            Message::ImportProfileBackupPressed => {
                if state.backup_operation != BackupOperation::Idle {
                    state.profile_import_status =
                        "Another backup operation is already active.".into();
                    return Task::none();
                }

                if state.profile_import_passphrase.trim().is_empty() {
                    state.profile_import_status =
                        "Enter profile import passphrase before import.".into();
                    return Task::none();
                }

                return Task::perform(
                    async move {
                        rfd::AsyncFileDialog::new()
                            .add_filter("IcedComm-I2P backup", &["tcbak"])
                            .pick_file()
                            .await
                            .map(|f| f.path().to_path_buf())
                    },
                    Message::ProfileBackupImportPathChosen,
                );
            }

            Message::BackupExportPathChosen(path_opt) => {
                let Some(path) = path_opt else {
                    return Task::none();
                };

                if !state.opened_tabs.is_empty() {
                    state.backup_export_status =
                        "Close all chat tabs before exporting a full backup.".into();
                    return Task::none();
                }

                let passphrase = state.backup_export_passphrase.clone();
                let include_files = state.backup_export_include_files;
                state.backup_export_status = "Exporting encrypted backup...".into();
                state.backup_operation = BackupOperation::Exporting;

                return Task::perform(
                    async move {
                        crate::backup::export_backup(&path, &passphrase, include_files)
                            .map(|()| path.clone())
                            .map_err(|e| e.to_string())
                    },
                    Message::BackupExportFinished,
                );
            }

            Message::ProfileBackupExportPathChosen(path_opt) => {
                let Some(path) = path_opt else {
                    return Task::none();
                };

                let Some(profile) = state
                    .session
                    .profiles
                    .get(state.session.selected_profile_idx)
                    .filter(|profile| profile.persistent)
                else {
                    state.profile_export_status =
                        "Select a persistent profile before profile export.".into();
                    return Task::none();
                };

                if state
                    .opened_tabs
                    .iter()
                    .any(|tab| tab.session.profile == profile.name)
                {
                    state.profile_export_status =
                        format!("Close the {} chat tab before profile export.", profile.name);
                    return Task::none();
                }

                let profile_name = profile.name.clone();
                let passphrase = state.profile_export_passphrase.clone();
                state.profile_export_status =
                    format!("Exporting encrypted profile backup for {profile_name}...");
                state.backup_operation = BackupOperation::ProfileExporting;

                return Task::perform(
                    async move {
                        crate::backup::export_profile_backup(&path, &passphrase, &profile_name)
                            .map(|()| (path.clone(), profile_name))
                            .map_err(|e| e.to_string())
                    },
                    Message::ProfileBackupExportFinished,
                );
            }

            Message::BackupImportPathChosen(path_opt) => {
                let Some(path) = path_opt else {
                    return Task::none();
                };

                let passphrase = if state.startup_gate == StartupGate::Locked {
                    state.unlock_input.clone()
                } else {
                    state.backup_import_passphrase.clone()
                };
                let restore_files = state.backup_import_restore_files;

                if !state.opened_tabs.is_empty() {
                    let msg = "Close all chat tabs before importing a full backup.".to_string();
                    if state.startup_gate == StartupGate::Locked {
                        state.unlock_status = msg;
                    } else {
                        state.backup_import_status = msg;
                    }
                    return Task::none();
                }

                if state.startup_gate == StartupGate::Locked {
                    state.unlock_status = "Checking local storage before import...".into();
                } else {
                    state.backup_import_status = "Checking local storage before import...".into();
                }
                state.backup_operation = BackupOperation::Importing;

                match crate::backup::has_import_conflicts() {
                    Ok(true) => {
                        state.pending_backup_import_path = Some(path.clone());
                        state.pending_backup_import_passphrase = passphrase;
                        state.backup_operation = BackupOperation::AwaitingReplaceConfirm;
                        let msg = format!(
                            "Local profiles or files already exist. Confirm replacement before importing {}.",
                            path.display()
                        );
                        if state.startup_gate == StartupGate::Locked {
                            state.unlock_status = msg;
                        } else {
                            state.backup_import_status = msg;
                        }
                        return Task::none();
                    }
                    Ok(false) => {}
                    Err(err) => {
                        state.backup_operation = BackupOperation::Idle;
                        let msg = format!("Backup import precheck failed: {err}");
                        if state.startup_gate == StartupGate::Locked {
                            state.unlock_status = msg;
                        } else {
                            state.backup_import_status = msg;
                        }
                        return Task::none();
                    }
                }

                return Task::perform(
                    async move {
                        crate::backup::import_backup(&path, &passphrase, restore_files)
                            .map(|()| path.clone())
                            .map_err(|e| e.to_string())
                    },
                    Message::BackupImportFinished,
                );
            }

            Message::ProfileBackupImportPathChosen(path_opt) => {
                let Some(path) = path_opt else {
                    return Task::none();
                };

                let passphrase = state.profile_import_passphrase.clone();
                state.profile_import_status = "Inspecting encrypted profile backup...".into();
                state.backup_operation = BackupOperation::ProfileImporting;

                return Task::perform(
                    async move {
                        let profile_name =
                            crate::backup::inspect_profile_backup(&path, &passphrase)
                                .map_err(|e| e.to_string())?;
                        let exists = crate::storage::contact_dir(&profile_name).exists();
                        Ok((path, profile_name, exists))
                    },
                    Message::ProfileBackupImportScanned,
                );
            }

            Message::ProfileBackupImportScanned(result) => {
                return match result {
                    Ok((path, profile_name, true)) => {
                        if state
                            .opened_tabs
                            .iter()
                            .any(|tab| tab.session.profile == profile_name)
                        {
                            state.backup_operation = BackupOperation::Idle;
                            state.profile_import_status = format!(
                                "Close the {profile_name} chat tab before replacing that profile."
                            );
                            Task::none()
                        } else {
                            state.pending_profile_import_path = Some(path.clone());
                            state.pending_profile_import_passphrase =
                                state.profile_import_passphrase.clone();
                            state.pending_profile_import_name = Some(profile_name.clone());
                            state.backup_operation = BackupOperation::AwaitingProfileReplaceConfirm;
                            state.profile_import_status = format!(
                                "Profile {profile_name} already exists. Confirm replacement before importing {}.",
                                path.display()
                            );
                            Task::none()
                        }
                    }
                    Ok((path, profile_name, false)) => {
                        let passphrase = state.profile_import_passphrase.clone();
                        state.profile_import_status =
                            format!("Importing encrypted profile backup for {profile_name}...");

                        Task::perform(
                            async move {
                                crate::backup::import_profile_backup(&path, &passphrase, false)
                                    .map(|name| {
                                        format!("Imported profile {name} from {}", path.display())
                                    })
                                    .map_err(|e| e.to_string())
                            },
                            Message::ProfileBackupImportFinished,
                        )
                    }
                    Err(err) => {
                        state.backup_operation = BackupOperation::Idle;
                        state.profile_import_status =
                            format!("Profile backup import failed: {err}");
                        Task::none()
                    }
                };
            }

            Message::BackupImportReplaceConfirmed => {
                if state.backup_operation != BackupOperation::AwaitingReplaceConfirm {
                    return Task::none();
                }

                let Some(path) = state.pending_backup_import_path.clone() else {
                    return Task::none();
                };
                let passphrase = state.pending_backup_import_passphrase.clone();
                let restore_files = state.backup_import_restore_files;

                state.pending_backup_import_path = None;
                state.pending_backup_import_passphrase.clear();
                state.backup_operation = BackupOperation::Importing;

                if state.startup_gate == StartupGate::Locked {
                    state.unlock_status =
                        "Replacing local data and importing encrypted backup...".into();
                } else {
                    state.backup_import_status =
                        "Replacing local data and importing encrypted backup...".into();
                }

                return Task::perform(
                    async move {
                        crate::backup::import_backup_replace(&path, &passphrase, restore_files)
                            .map(|()| path.clone())
                            .map_err(|e| e.to_string())
                    },
                    Message::BackupImportFinished,
                );
            }

            Message::BackupImportReplaceCancelled => {
                state.pending_backup_import_path = None;
                state.pending_backup_import_passphrase.clear();
                state.backup_operation = BackupOperation::Idle;
                if state.startup_gate == StartupGate::Locked {
                    state.unlock_status = "Backup import cancelled.".into();
                } else {
                    state.backup_import_status = "Backup import cancelled.".into();
                }
                return Task::none();
            }

            Message::WipeAllConfirmed => {
                if state.backup_operation != BackupOperation::AwaitingWipeConfirm {
                    return Task::none();
                }

                if !state.opened_tabs.is_empty() {
                    state.backup_operation = BackupOperation::Idle;
                    state.wipe_all_status = "Close all chat tabs before wiping storage.".into();
                    return Task::none();
                }

                state.backup_operation = BackupOperation::Wiping;
                state.wipe_all_status = "Wiping all profiles and stored files...".into();

                return Task::perform(
                    async move {
                        crate::storage::wipe_all_profiles_and_files().map_err(|e| e.to_string())
                    },
                    Message::WipeAllFinished,
                );
            }

            Message::WipeAllCancelled => {
                state.backup_operation = BackupOperation::Idle;
                state.wipe_all_status = "Wipe cancelled.".into();
                return Task::none();
            }

            Message::ProfileBackupImportReplaceConfirmed => {
                if state.backup_operation != BackupOperation::AwaitingProfileReplaceConfirm {
                    return Task::none();
                }

                let Some(path) = state.pending_profile_import_path.clone() else {
                    return Task::none();
                };
                let passphrase = state.pending_profile_import_passphrase.clone();
                let profile_name = state
                    .pending_profile_import_name
                    .clone()
                    .unwrap_or_else(|| "profile".into());

                if state
                    .opened_tabs
                    .iter()
                    .any(|tab| tab.session.profile == profile_name)
                {
                    state.backup_operation = BackupOperation::Idle;
                    state.pending_profile_import_path = None;
                    state.pending_profile_import_passphrase.clear();
                    state.pending_profile_import_name = None;
                    state.profile_import_status =
                        format!("Close the {profile_name} chat tab before replacing that profile.");
                    return Task::none();
                }

                state.pending_profile_import_path = None;
                state.pending_profile_import_passphrase.clear();
                state.pending_profile_import_name = None;
                state.backup_operation = BackupOperation::ProfileImporting;
                state.profile_import_status = format!("Replacing local profile {profile_name}...");

                return Task::perform(
                    async move {
                        crate::backup::import_profile_backup(&path, &passphrase, true)
                            .map(|name| format!("Imported profile {name} from {}", path.display()))
                            .map_err(|e| e.to_string())
                    },
                    Message::ProfileBackupImportFinished,
                );
            }

            Message::ProfileBackupImportReplaceCancelled => {
                state.pending_profile_import_path = None;
                state.pending_profile_import_passphrase.clear();
                state.pending_profile_import_name = None;
                state.backup_operation = BackupOperation::Idle;
                state.profile_import_status = "Profile backup import cancelled.".into();
                return Task::none();
            }

            Message::BackupExportFinished(result) => {
                state.backup_operation = BackupOperation::Idle;
                match result {
                    Ok(path) => {
                        state.backup_export_status =
                            format!("Exported encrypted backup to {}", path.display());
                    }
                    Err(err) => {
                        state.backup_export_status = format!("Backup export failed: {err}");
                    }
                }
                return Task::none();
            }

            Message::WipeAllFinished(result) => {
                state.backup_operation = BackupOperation::Idle;
                match result {
                    Ok(()) => {
                        state.opened_tabs.clear();
                        state.session = SessionState::default();
                        state.wipe_all_passphrase.clear();
                        state.unlock_input.clear();
                        state.unlock_confirm_input.clear();
                        state.unlock_status =
                            "Storage wiped. Enter a passphrase to initialize new storage.".into();
                        state.startup_gate = StartupGate::Locked;
                        state.sam_host_input = DEFAULT_SAM_HOST.to_string();
                        state.sam_port_input = DEFAULT_SAM_PORT.to_string();
                        state.sam_status = "SAM settings apply to newly opened chat tabs.".into();
                        state.backup_export_passphrase.clear();
                        state.backup_import_passphrase.clear();
                        state.profile_export_passphrase.clear();
                        state.profile_import_passphrase.clear();
                        state.wipe_all_status =
                            "Wipe deletes all profiles and stored files.".into();
                    }
                    Err(err) => {
                        state.wipe_all_status = format!("Wipe failed: {err}");
                    }
                }
                return Task::none();
            }

            Message::ProfileBackupExportFinished(result) => {
                state.backup_operation = BackupOperation::Idle;
                match result {
                    Ok((path, profile_name)) => {
                        state.profile_export_status = format!(
                            "Exported profile {profile_name} backup to {}",
                            path.display()
                        );
                    }
                    Err(err) => {
                        state.profile_export_status =
                            format!("Profile backup export failed: {err}");
                    }
                }
                return Task::none();
            }

            Message::ProfileBackupImportFinished(result) => {
                state.pending_profile_import_path = None;
                state.pending_profile_import_passphrase.clear();
                state.pending_profile_import_name = None;
                state.backup_operation = BackupOperation::Idle;
                match result {
                    Ok(msg) => {
                        state.profile_import_status = msg;
                        match storage::load_contacts() {
                            Ok(contacts) => {
                                state.session.profiles = vec![ProfileEntry::transient()];
                                for contact in contacts {
                                    state
                                        .session
                                        .profiles
                                        .push(ProfileEntry::persistent(contact.name));
                                }
                                if state.session.selected_profile_idx
                                    >= state.session.profiles.len()
                                {
                                    state.session.selected_profile_idx = 0;
                                }
                            }
                            Err(err) => {
                                state.profile_import_status = format!(
                                    "Imported profile, but failed to refresh profiles: {err}"
                                );
                            }
                        }
                    }
                    Err(err) => {
                        state.profile_import_status =
                            format!("Profile backup import failed: {err}");
                    }
                }
                return Task::none();
            }

            Message::BackupImportFinished(result) => {
                state.pending_backup_import_path = None;
                state.pending_backup_import_passphrase.clear();
                state.backup_operation = BackupOperation::Idle;
                match result {
                    Ok(path) => {
                        let msg = format!("Imported encrypted backup from {}", path.display());
                        state.opened_tabs.clear();
                        state.session.active_tab_idx = Some(0);
                        if state.startup_gate == StartupGate::Locked {
                            match state.load_unlocked_storage() {
                                Ok(()) => {
                                    state.startup_gate = StartupGate::Unlocked;
                                    state.unlock_status = msg;
                                }
                                Err(err) => {
                                    state.unlock_status = format!(
                                        "Imported backup, but failed to load storage: {err}"
                                    );
                                }
                            }
                        } else {
                            state.backup_import_status = msg;
                            match storage::load_contacts() {
                                Ok(contacts) => {
                                    state.session.profiles = vec![ProfileEntry::transient()];
                                    for contact in contacts {
                                        state
                                            .session
                                            .profiles
                                            .push(ProfileEntry::persistent(contact.name));
                                    }
                                    state.session.tabs = vec![Self::new_app_home_tab()];
                                }
                                Err(err) => {
                                    state.backup_import_status = format!(
                                        "Imported backup, but failed to refresh profiles: {err}"
                                    );
                                }
                            }
                        }
                    }
                    Err(err) => {
                        let msg = format!("Backup import failed: {err}");
                        if state.startup_gate == StartupGate::Locked {
                            state.unlock_status = msg;
                        } else {
                            state.backup_import_status = msg;
                        }
                    }
                }
                return Task::none();
            }

            Message::OfflinePutFinished(tab_id, result) => {
                if let Some(tab) = state.tab_by_id_mut(tab_id) {
                    tab.deaddrop_put_in_flight = false;
                }

                match result {
                    Ok((status, drops, used_index, offline_msg_id, stats)) => {
                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            Self::record_deaddrop_stats_for_tab(tab, &stats);
                            Self::flush_deaddrop_stats_for_tab(tab, false);

                            match status.as_str() {
                                "OK" | "EXISTS" => {
                                    Self::mark_delivered(tab, offline_msg_id);
                                    if tab.session.drop_send_index == used_index {
                                        tab.session.drop_send_index = used_index + 1;
                                    }

                                    if let Some(peer_b32) = tab.session.stored_peer.clone() {
                                        let offline = OfflineState {
                                            offline_shared_secret: tab
                                                .session
                                                .offline_shared_secret
                                                .unwrap_or([0u8; 32]),
                                            drop_send_index: tab.session.drop_send_index,
                                            drop_recv_base: tab.session.drop_recv_base,
                                            drop_window: tab.session.drop_window,
                                            consumed_drop_recv: tab
                                                .session
                                                .consumed_drop_recv
                                                .clone(),
                                        };

                                        match storage::save_offline_state(
                                            &tab.session.profile,
                                            &peer_b32,
                                            &offline,
                                        ) {
                                            Ok(()) => {
                                                Self::set_dd_status(&mut tab.session, "put_ok");
                                                tab.session.log_lines.push(format!(
                                                    "Offline PUT {} at index {} on {} drop(s).",
                                                    status,
                                                    used_index,
                                                    drops.len()
                                                ));
                                            }
                                            Err(err) => {
                                                Self::set_dd_status(&mut tab.session, "put_fail");
                                                tab.session.log_lines.push(format!(
                                                    "Offline PUT saved, but state persist failed: {err}"
                                                ));
                                            }
                                        }
                                    } else {
                                        Self::set_dd_status(&mut tab.session, "put_fail");
                                        tab.session.log_lines.push(
                                            "Offline PUT succeeded, but locked peer is missing."
                                                .into(),
                                        );
                                    }
                                }
                                _ => {
                                    Self::set_dd_status(&mut tab.session, "put_fail");
                                    tab.deaddrop_started = false;
                                    tab.session.log_lines.push(format!(
                                        "Offline PUT failed at index {}. Deaddrop runtime will need restart.",
                                        used_index
                                    ));
                                }
                            }
                        }
                    }
                    Err(err) => {
                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            Self::set_dd_status(&mut tab.session, "put_fail");
                            tab.deaddrop_started = false;
                            tab.session.log_lines.push(format!(
                                "Offline PUT error: {err}. Deaddrop runtime will need restart."
                            ));
                        }
                    }
                }

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                return Task::none();
            }

            Message::DeaddropStarted(tab_id, result) => {
                match result {
                    Ok(()) => {
                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            tab.deaddrop_started = true;
                            tab.deaddrop_poller_started = true;
                            tab.deaddrop_poll_in_flight = false;
                            tab.deaddrop_poll_queue.clear();
                            tab.deaddrop_last_poll_ms = 0;
                            tab.session
                                .log_lines
                                .push("Deaddrop runtime started.".into());
                        }
                    }
                    Err(err) => {
                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            tab.session
                                .log_lines
                                .push(format!("Deaddrop runtime start failed: {err}"));
                        }
                    }
                }

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                return Task::none();
            }

            Message::DeaddropClosed(_tab_id) => {
                return Task::none();
            }

            Message::OfflinePollKeyFinished(tab_id, recv_index, _dd_key, blobs, stats) => {
                if let Some(tab) = state.tab_by_id_mut(tab_id) {
                    Self::record_deaddrop_stats_for_tab(tab, &stats);
                    Self::flush_deaddrop_stats_for_tab(tab, false);
                    Self::handle_offline_poll_key_result(tab, recv_index, blobs);
                }

                let next_poll_task = state.start_next_deaddrop_poll_key_task(tab_id);

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                    return Task::batch(vec![
                        next_poll_task,
                        operation::snap_to_end(state.session.logs_scroll_id.clone()),
                        operation::snap_to_end(state.session.messages_scroll_id.clone()),
                    ]);
                }

                return next_poll_task;
            }
        }

        Task::none()
    }

    //uuu

    pub fn view(state: &Self) -> Element<'_, Message> {
        if state.startup_gate == StartupGate::Locked {
            let base_dir = storage::base_dir();
            let vault_exists = crate::vault::vault_path(&base_dir).exists();
            let plaintext_exists = base_dir.exists();
            let gate_title = if vault_exists {
                "Unlock encrypted storage"
            } else if plaintext_exists {
                "Storage recovery required"
            } else {
                "Set storage passphrase"
            };
            let gate_button = if vault_exists {
                "Unlock"
            } else {
                "Set Passphrase"
            };
            let gate_placeholder = if vault_exists {
                "Enter passphrase..."
            } else {
                "Set passphrase..."
            };
            let show_confirm = !vault_exists && !plaintext_exists;

            let unlock_card = container(
                column![
                    text(APP_NAME).size(28),
                    text(format!("Version {APP_VERSION}"))
                        .size(13)
                        .color(Color::from_rgb8(180, 180, 180)),
                    text(gate_title).size(16),
                    Space::new().height(10),
                    text_input(gate_placeholder, &state.unlock_input)
                        .on_input(Message::UnlockInputChanged)
                        .on_submit(Message::UnlockPressed)
                        .secure(true)
                        .padding(12)
                        .size(16)
                        .width(320),
                    if show_confirm {
                        text_input("Confirm passphrase...", &state.unlock_confirm_input)
                            .on_input(Message::UnlockConfirmInputChanged)
                            .on_submit(Message::UnlockPressed)
                            .secure(true)
                            .padding(12)
                            .size(16)
                            .width(320)
                    } else {
                        text_input("", "").padding(0).size(1).width(0)
                    },
                    row![
                        button(text(gate_button).size(14))
                            .padding([8, 16])
                            .style(app_button_style)
                            .on_press(Message::UnlockPressed),
                    ]
                    .spacing(10)
                    .align_y(Alignment::Center),
                    Space::new().height(6),
                    text(&state.unlock_status)
                        .size(13)
                        .color(Color::from_rgb8(180, 180, 180)),
                    if let Some(path) = &state.pending_backup_import_path {
                        row![
                            text(format!("Replace local data with {}?", path.display()))
                                .size(12)
                                .width(Length::Fill),
                            button(text("OK").size(12))
                                .padding([6, 10])
                                .style(app_button_style)
                                .on_press(Message::BackupImportReplaceConfirmed),
                            button(text("Cancel").size(12))
                                .padding([6, 10])
                                .style(app_button_style)
                                .on_press(Message::BackupImportReplaceCancelled),
                        ]
                        .spacing(8)
                        .align_y(Alignment::Center)
                        .width(Length::Fill)
                    } else {
                        row![]
                    },
                ]
                .spacing(12)
                .align_x(Alignment::Center),
            )
            .padding(24)
            .style(|_| sidebar_panel_style());

            return container(
                container(unlock_card)
                    .center_x(Length::Fill)
                    .center_y(Length::Fill)
                    .width(Length::Fill)
                    .height(Length::Fill),
            )
            .width(Length::Fill)
            .height(Length::Fill)
            .into();
        }

        let is_app_home = state.active_tab_is_app_home();

        let selected_profile = state
            .session
            .profiles
            .get(state.session.selected_profile_idx);

        let delete_allowed = match selected_profile {
            Some(profile) if profile.persistent => !state.is_profile_open_in_any_tab(&profile.name),
            _ => false,
        };
        let sidebar_idle = state.session.sidebar_confirm.is_none();
        let sidebar_ops_allowed = delete_allowed && sidebar_idle;

        let sidebar_confirm = match &state.session.sidebar_confirm {
            Some(SidebarConfirm::DeleteProfile(name)) => column![
                text(format!("Delete profile {name}?")).size(12),
                row![
                    button(text("Yes").size(12))
                        .padding([4, 8])
                        .style(app_button_style)
                        .on_press(Message::SidebarConfirmYes),
                    button(text("No").size(12))
                        .padding([4, 8])
                        .style(app_button_style)
                        .on_press(Message::SidebarConfirmNo),
                ]
                .spacing(6)
            ]
            .spacing(6),
            Some(SidebarConfirm::ResetProfile(name)) => column![
                text(format!("Reset profile {name}?")).size(12),
                row![
                    button(text("Yes").size(12))
                        .padding([4, 8])
                        .style(app_button_style)
                        .on_press(Message::SidebarConfirmYes),
                    button(text("No").size(12))
                        .padding([4, 8])
                        .style(app_button_style)
                        .on_press(Message::SidebarConfirmNo),
                ]
                .spacing(6)
            ]
            .spacing(6),
            None => column![],
        };

        let profile_list = state.session.profiles.iter().enumerate().fold(
            column!().spacing(6).width(Length::Fill),
            |col, (idx, profile)| {
                let label = if profile.persistent {
                    row![
                        text("P").size(13).color(PY_GREEN),
                        text(&profile.name).size(13).color(Color::WHITE),
                    ]
                } else {
                    row![
                        text("T").size(13).color(Color::WHITE),
                        text(&profile.name).size(13).color(Color::WHITE),
                    ]
                };

                let selected = idx == state.session.selected_profile_idx;

                col.push(
                    button(
                        container(label.spacing(8).align_y(Alignment::Center))
                            .width(Length::Fill)
                            .padding([6, 8])
                            .style(|_| profile_row_content_style(APP_PROFILE_TEXT)),
                    )
                    .width(Length::Fill)
                    .style(move |theme, status| profile_button_style(theme, status, selected))
                    .on_press(Message::ProfileSelected(idx)),
                )
            },
        );

        let profile_sidebar = container(
            column![
                container(
                    scrollable(profile_list)
                        .height(Length::Fill)
                        .width(Length::Fill)
                )
                .height(Length::Fill)
                .width(Length::Fill)
                .padding(4)
                .style(|_| sidebar_panel_style()),
                Space::new().height(8),
                text_input("New profile name...", &state.session.profile_name_input)
                    .on_input(Message::ProfileNameInputChanged)
                    .padding(8)
                    .size(13)
                    .width(Length::Fill),
                column![
                    row![
                        {
                            let btn = button(text("Open").size(12))
                                .padding([4, 8])
                                .width(Length::Fill)
                                .style(app_button_style);

                            if sidebar_idle {
                                btn.on_press(Message::OpenSelectedProfilePressed)
                            } else {
                                btn
                            }
                        },
                        {
                            let btn = button(text("New").size(12))
                                .padding([4, 8])
                                .width(Length::Fill)
                                .style(app_button_style);

                            if sidebar_idle {
                                btn.on_press(Message::CreateProfilePressed)
                            } else {
                                btn
                            }
                        },
                    ]
                    .spacing(6)
                    .width(Length::Fill),
                    row![
                        {
                            let btn = button(text("Delete").size(12))
                                .padding([4, 8])
                                .width(Length::Fill)
                                .style(app_button_style);

                            if sidebar_ops_allowed {
                                btn.on_press(Message::DeleteProfilePressed)
                            } else {
                                btn
                            }
                        },
                        {
                            let btn = button(text("Reset").size(12))
                                .padding([4, 8])
                                .width(Length::Fill)
                                .style(app_button_style);

                            if sidebar_ops_allowed {
                                btn.on_press(Message::ResetProfilePressed)
                            } else {
                                btn
                            }
                        },
                    ]
                    .spacing(6)
                    .width(Length::Fill),
                ]
                .spacing(6)
                .width(Length::Fill),
                sidebar_confirm,
            ]
            .spacing(8)
            .width(Length::Fill)
            .height(Length::Fill),
        )
        .width(220)
        .height(Length::Fill)
        .padding(8)
        .style(|_| sidebar_panel_style());

        let tab_bar = state.session.tabs.iter().enumerate().fold(
            row!().spacing(6).align_y(Alignment::Center),
            |row_acc, (idx, tab)| {
                let selected = Some(idx) == state.session.active_tab_idx;

                let blink_on = if tab.kind == TabKind::AppHome {
                    true
                } else if idx > 0 {
                    state
                        .opened_tabs
                        .get(idx - 1)
                        .map(|t| t.session.call_blink_on)
                        .unwrap_or(true)
                } else {
                    true
                };

                row_acc.push(if tab.kind == TabKind::AppHome {
                    row![
                        button(
                            container(
                                row![text(&tab.title).size(13), tab_status_marker(tab, blink_on),]
                                    .spacing(6)
                                    .align_y(Alignment::Center)
                            )
                            .padding([4, 10])
                            .style(|_| tab_indicator_style(APP_TAB_TEXT))
                        )
                        .style(move |theme, status| tab_button_style(theme, status, selected))
                        .on_press(Message::TabSelected(idx)),
                    ]
                    .spacing(4)
                    .align_y(Alignment::Center)
                } else {
                    row![
                        button(
                            container(
                                row![text(&tab.title).size(13), tab_status_marker(tab, blink_on),]
                                    .spacing(6)
                                    .align_y(Alignment::Center)
                            )
                            .padding([4, 10])
                            .style(|_| tab_indicator_style(APP_TAB_TEXT))
                        )
                        .style(move |theme, status| tab_button_style(theme, status, selected))
                        .on_press(Message::TabSelected(idx)),
                        button(text("x").size(11))
                            .padding([2, 6])
                            .style(tab_close_button_style)
                            .on_press(Message::TabClosed(idx)),
                    ]
                    .spacing(4)
                    .align_y(Alignment::Center)
                })
            },
        );

        let tab_panel = container(scrollable(row![tab_bar].width(Length::Shrink)).direction(
            scrollable::Direction::Horizontal(scrollable::Scrollbar::default()),
        ))
        .width(Length::Fill)
        .padding([4, 6])
        .style(|_| tab_panel_style());

        let left_status = Self::left_status_indicators(&state.session);

        let center_status = indicator(
            connection_status_text(&state.session),
            connection_status_color(&state.session),
        );

        let my_b32_available = state.session.my_b32.is_some();
        let peer_b32_available =
            state.session.live_ready && state.session.current_peer_addr.is_some();
        let my_b32_button = button(
            text(short_b32(state.session.my_b32.as_deref()))
                .size(13)
                .color(PY_GREEN),
        )
        .padding([4, 10])
        .style(my_status_address_button_style);
        let peer_b32_button = button(
            text(short_peer_b32(
                state.session.current_peer_addr.as_deref(),
                state.session.live_ready,
            ))
            .size(13)
            .color(PY_CYAN),
        )
        .padding([4, 10])
        .style(peer_status_address_button_style);

        let right_status = container(
            row![
                if my_b32_available {
                    my_b32_button.on_press(Message::CopyStatusMyB32Pressed)
                } else {
                    my_b32_button
                },
                text(" : ").size(13),
                if peer_b32_available {
                    peer_b32_button.on_press(Message::CopyStatusPeerB32Pressed)
                } else {
                    peer_b32_button
                },
            ]
            .align_y(Alignment::Center),
        );

        let status_inner = container(
            stack![
                row![left_status, Space::new().width(Length::Fill), right_status,]
                    .align_y(Alignment::Center)
                    .width(Length::Fill),
                container(center_status)
                    .width(Length::Fill)
                    .center_x(Length::Fill),
            ]
            .height(Length::Shrink)
            .width(Length::Fill),
        )
        .width(Length::Fill)
        .padding(9)
        .style(|_| status_bar_style());

        let status_bar = status_inner;

        let messages = state.session.bubbles.iter().fold(
            column!().spacing(12).padding([8, 4]).width(Length::Fill),
            |col, bubble| col.push(message_row(bubble)),
        );

        let chat_panel = container(
            scrollable(messages)
                .id(state.session.messages_scroll_id.clone())
                .height(Length::Fill)
                .width(Length::Fill),
        )
        .width(Length::Fill)
        .height(Length::FillPortion(if state.session.show_logs {
            3
        } else {
            1
        }))
        .padding(6)
        .style(|_| message_panel_style());

        let log_lines = state.session.log_lines.iter().fold(
            column!().spacing(4).padding([6, 4]).width(Length::Fill),
            |col, line| col.push(text(line).size(12).width(Length::Fill)),
        );

        let log_inner = container(
            scrollable(log_lines)
                .id(state.session.logs_scroll_id.clone())
                .height(Length::Fill)
                .width(Length::Fill),
        )
        .width(Length::Fill)
        .height(Length::Fill)
        .padding(6)
        .style(|_| log_panel_style());

        let log_panel = log_inner.height(Length::FillPortion(1));

        let show_deaddrop_panel =
            state.session.show_deaddrop_panel && Self::deaddrop_panel_allowed(&state.session);

        let deaddrop_rows = if state.session.deaddrop_servers.is_empty() {
            column![
                text("No deaddrop servers configured.")
                    .size(12)
                    .color(PY_GREY62)
            ]
            .spacing(6)
            .padding([6, 4])
            .width(Length::Fill)
        } else {
            state.session.deaddrop_servers.iter().enumerate().fold(
                column!().spacing(6).padding([6, 4]).width(Length::Fill),
                |col, (idx, server)| {
                    let active = idx < MAX_ACTIVE_DEADDROP_REPLICAS;
                    let stats = state.session.deaddrop_stats.get(server);
                    let put_ok = stats.map(|s| s.put_ok).unwrap_or(0);
                    let put_fail = stats.map(|s| s.put_fail).unwrap_or(0);
                    let get_ok = stats.map(|s| s.get_ok).unwrap_or(0);
                    let get_fail = stats.map(|s| s.get_fail).unwrap_or(0);
                    let latency = stats.map(|s| s.latency_ema_ms).unwrap_or(0.0);
                    let server_record = row![
                        column![
                            text(format!(
                                "{}.{} {}",
                                idx + 1,
                                if active { "*" } else { " " },
                                server
                            ))
                            .size(12)
                            .width(Length::Fill),
                            text(format!(
                                "put ok/fail={}/{}  get ok/fail={}/{}  lat={:.1}ms",
                                put_ok, put_fail, get_ok, get_fail, latency
                            ))
                            .size(11)
                            .color(Color::from_rgb8(155, 155, 160))
                            .width(Length::Fill),
                        ]
                        .spacing(2)
                        .width(Length::Fill),
                        button(text("Delete").size(12))
                            .padding([4, 8])
                            .style(app_button_style)
                            .on_press(Message::DdServerDeletePressed(idx)),
                    ]
                    .spacing(8)
                    .align_y(Alignment::Center)
                    .width(Length::Fill);

                    col.push(
                        container(server_record)
                            .padding(12)
                            .width(Length::Fill)
                            .style(|_| operation_panel_style()),
                    )
                },
            )
        };

        let share_button = if state.session.live_ready {
            button(text("Share").size(12))
                .padding([6, 10])
                .style(app_button_style)
                .on_press(Message::DdServerSharePressed)
        } else {
            button(text("Share").size(12))
                .padding([6, 10])
                .style(app_button_style)
        };

        let deaddrop_panel = container(
            column![
                row![
                    text(format!(
                        "Deaddrop servers: {} (active replicas: {})",
                        state.session.deaddrop_servers.len(),
                        state
                            .session
                            .deaddrop_servers
                            .len()
                            .min(MAX_ACTIVE_DEADDROP_REPLICAS)
                    ))
                    .size(13)
                    .width(Length::Fill),
                    share_button,
                ]
                .spacing(8)
                .align_y(Alignment::Center),
                row![
                    text_input(
                        "Add deaddrop server b32.i2p...",
                        &state.session.deaddrop_server_input
                    )
                    .on_input(Message::DdServerInputChanged)
                    .on_submit(Message::DdServerAddPressed)
                    .padding(8)
                    .size(13)
                    .width(Length::Fill),
                    button(text("Add").size(12))
                        .padding([6, 10])
                        .style(app_button_style)
                        .on_press(Message::DdServerAddPressed),
                ]
                .spacing(8)
                .align_y(Alignment::Center),
                scrollable(deaddrop_rows)
                    .height(Length::Fill)
                    .width(Length::Fill),
            ]
            .spacing(8),
        )
        .width(Length::Fill)
        .height(Length::FillPortion(DEADDROP_PANEL_HEIGHT_PORTION))
        .padding(6)
        .style(|_| log_panel_style());

        let selected_persistent_profile = state
            .session
            .profiles
            .get(state.session.selected_profile_idx)
            .filter(|profile| profile.persistent)
            .map(|profile| profile.name.as_str());

        let center_panel: Element<'_, Message> = if is_app_home {
            crate::app_home::app_home_view(
                state.session.show_logs,
                &state.sam_host_input,
                &state.sam_port_input,
                &state.sam_status,
                state.sam_test_in_flight,
                &state.backup_export_passphrase,
                &state.backup_export_status,
                state.backup_export_include_files,
                &state.backup_import_passphrase,
                &state.backup_import_status,
                state.backup_import_restore_files,
                state.pending_backup_import_path.as_deref(),
                &state.wipe_all_passphrase,
                &state.wipe_all_status,
                &state.profile_export_passphrase,
                &state.profile_export_status,
                selected_persistent_profile,
                &state.profile_import_passphrase,
                &state.profile_import_status,
                state.pending_profile_import_path.as_deref(),
                state.pending_profile_import_name.as_deref(),
                state.backup_operation,
            )
        } else if show_deaddrop_panel && state.session.show_logs {
            column![chat_panel, deaddrop_panel, log_panel]
                .spacing(8)
                .width(Length::Fill)
                .height(Length::Fill)
                .into()
        } else if show_deaddrop_panel {
            column![chat_panel, deaddrop_panel]
                .spacing(8)
                .width(Length::Fill)
                .height(Length::Fill)
                .into()
        } else if state.session.show_logs {
            column![chat_panel, log_panel]
                .spacing(8)
                .width(Length::Fill)
                .height(Length::Fill)
                .into()
        } else {
            chat_panel.into()
        };

        let message_input_enabled = state.message_input_enabled();
        let message_input = text_input(
            if message_input_enabled {
                "Type message..."
            } else {
                "Chat is not connected or offline-ready."
            },
            &state.session.input,
        )
        .padding(12)
        .size(16)
        .width(Length::Fill);

        let message_input = if message_input_enabled {
            message_input
                .on_input(Message::InputChanged)
                .on_submit(Message::SendPressed)
        } else {
            message_input
        };

        let message_input_panel = container(message_input)
            .width(Length::Fill)
            .padding(8)
            .style(container::rounded_box);

        let actions_row = state
            .available_actions()
            .into_iter()
            .fold(
                row!().spacing(8).align_y(Alignment::Center),
                |row_acc, action| {
                    let btn = button(
                        text(TermchatApp::action_label_for_session(
                            &state.session,
                            action,
                        ))
                        .size(13),
                    )
                    .padding([6, 10])
                    .style(app_button_style);

                    if TermchatApp::action_enabled(action) {
                        row_acc.push(btn.on_press(Message::ActionPressed(action)))
                    } else {
                        row_acc.push(btn)
                    }
                },
            )
            .push(
                button(
                    text(if state.session.show_logs {
                        "Hide Logs"
                    } else {
                        "Show Logs"
                    })
                    .size(13),
                )
                .padding([6, 10])
                .style(app_button_style)
                .on_press(Message::ToggleLogsPressed),
            )
            .push(
                button(text(TermchatApp::action_label(GuiAction::Help)).size(13))
                    .padding([6, 10])
                    .style(app_button_style),
            );

        let command_panel_content = if let Some(action) = state.session.pending_action {
            if TermchatApp::action_needs_param(action) {
                column![
                    actions_row,
                    row![
                        text_input(
                            TermchatApp::action_placeholder(action),
                            &state.session.action_param
                        )
                        .on_input(Message::ActionParamChanged)
                        .on_submit(Message::ActionConfirm)
                        .padding(10)
                        .size(15)
                        .width(Length::Fill),
                        button(text("OK").size(12))
                            .padding([6, 10])
                            .style(app_button_style)
                            .on_press(Message::ActionConfirm),
                        button(text("Cancel").size(13))
                            .padding([6, 10])
                            .style(app_button_style)
                            .on_press(Message::ActionCancel),
                    ]
                    .spacing(8)
                    .align_y(Alignment::Center)
                ]
                .spacing(8)
            } else if TermchatApp::action_needs_confirm(action) {
                column![
                    actions_row,
                    row![
                        text(TermchatApp::action_confirm_prompt(action))
                            .size(13)
                            .width(Length::Fill),
                        button(text("OK").size(12))
                            .padding([6, 10])
                            .style(app_button_style)
                            .on_press(Message::ActionConfirm),
                        button(text("Cancel").size(13))
                            .padding([6, 10])
                            .style(app_button_style)
                            .on_press(Message::ActionCancel),
                    ]
                    .spacing(8)
                    .align_y(Alignment::Center)
                ]
                .spacing(8)
            } else {
                column![actions_row].spacing(8)
            }
        } else {
            column![actions_row].spacing(8)
        };

        let command_panel = container(command_panel_content)
            .width(Length::Fill)
            .padding(8)
            .style(container::rounded_box);

        let main_column = if is_app_home {
            column![tab_panel, center_panel]
                .spacing(10)
                .width(Length::Fill)
                .height(Length::Fill)
        } else {
            column![
                tab_panel,
                status_bar,
                center_panel,
                message_input_panel,
                command_panel
            ]
            .spacing(10)
            .width(Length::Fill)
            .height(Length::Fill)
        };

        container(
            row![
                profile_sidebar,
                container(main_column)
                    .width(Length::Fill)
                    .height(Length::Fill)
            ]
            .spacing(10)
            .width(Length::Fill)
            .height(Length::Fill),
        )
        .padding(10)
        .width(Length::Fill)
        .height(Length::Fill)
        .into()
    }

    fn now_epoch_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn now_utc_hms() -> String {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let day = secs % 86_400;
        let h = day / 3_600;
        let m = (day % 3_600) / 60;
        let s = day % 60;

        format!("{:02}:{:02}:{:02} UTC", h, m, s)
    }

    fn generate_msg_id(&self) -> u64 {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let random_bits: u64 = rand::rng().random::<u32>() as u64;
        (millis ^ random_bits) & 0xFFFF_FFFF_FFFF_FFFF
    }

    fn make_signal_frame(&self, signal: &str) -> Frame {
        Frame {
            msg_type: MsgType::S,
            msg_id: self.generate_msg_id(),
            payload: format!("__SIGNAL__:{signal}").into_bytes(),
        }
    }

    fn reset_connection_state(&mut self) {
        self.set_active_live_conn(None);
        self.set_active_pending_conn(None);
        self.session.current_peer_addr = None;
        self.session.current_peer_dest_b64 = None;
        self.session.peer_b32 = None;
        self.session.pending_peer_addr = None;
        self.session.pending_peer_dest_b64 = None;
        self.session.live_ready = false;
        self.session.offline_mode = false;
        self.session.network_status = NetworkStatus::LocalOk;
        self.session.call_blink_on = true;
        self.session.call_blink_ticks = 0;
        self.clear_tofu_runtime_status();
    }

    fn open_or_focus_tab_for_profile(&mut self, profile_name: &str) {
        self.store_active_runtime();

        if let Some(real_idx) = self
            .opened_tabs
            .iter()
            .position(|t| t.meta.profile_name == profile_name)
        {
            self.session.active_tab_idx = Some(Self::real_to_visible_tab_index(real_idx));
            self.session.profile = self.opened_tabs[real_idx].meta.profile_name.clone();
            self.refresh_visible_from_active_tab();
            return;
        }

        self.opened_tabs.push(self.new_opened_tab(profile_name));
        let real_idx = self.opened_tabs.len() - 1;
        self.session.active_tab_idx = Some(Self::real_to_visible_tab_index(real_idx));
        self.session.profile = profile_name.to_string();
        self.refresh_visible_from_active_tab();
    }

    fn sync_active_tab_flags(&mut self) {
        if let Some(visible_idx) = self.session.active_tab_idx {
            if let Some(real_idx) = Self::visible_to_real_tab_index(visible_idx) {
                self.sync_tab_meta(real_idx);
            }
        }

        self.session.tabs = std::iter::once(Self::new_app_home_tab())
            .chain(self.opened_tabs.iter().map(|t| t.meta.clone()))
            .collect();
    }

    fn sync_tab_meta(&mut self, idx: usize) {
        if let Some(tab) = self.opened_tabs.get_mut(idx) {
            tab.meta.connected = tab.session.live_ready;
            tab.meta.has_incoming = tab.session.pending_peer_addr.is_some();
        }
    }

    fn active_live_conn(&self) -> Option<LiveConnection> {
        self.active_tab().and_then(|t| t.live_conn.clone())
    }

    fn active_pending_conn(&self) -> Option<LiveConnection> {
        self.active_tab().and_then(|t| t.pending_conn.clone())
    }

    fn set_active_live_conn(&mut self, conn: Option<LiveConnection>) {
        if let Some(tab) = self.active_tab_mut() {
            tab.live_conn = conn;
        }
    }

    fn set_active_pending_conn(&mut self, conn: Option<LiveConnection>) {
        if let Some(tab) = self.active_tab_mut() {
            tab.pending_conn = conn;
        }
    }

    fn close_tab_runtime_tasks(&self, idx: usize) -> Vec<Task<Message>> {
        let mut tasks = Vec::new();

        if let Some(tab) = self.opened_tabs.get(idx) {
            let tab_id = tab.id;
            let live = tab.live_conn.clone();
            let pending = tab.pending_conn.clone();
            let mut sam = tab.sam.clone();

            let quit_live = Frame {
                msg_type: MsgType::S,
                msg_id: self.generate_msg_id(),
                payload: b"__SIGNAL__:QUIT".to_vec(),
            };

            let quit_pending = Frame {
                msg_type: MsgType::S,
                msg_id: self.generate_msg_id(),
                payload: b"__SIGNAL__:QUIT".to_vec(),
            };

            tasks.push(Task::perform(
                async move {
                    if let Some(conn) = live {
                        let _ = conn.send_frame(&quit_live).await;
                        sleep(Duration::from_millis(120)).await;
                        let _ = conn.close().await;
                    }

                    if let Some(conn) = pending {
                        let _ = conn.send_frame(&quit_pending).await;
                        sleep(Duration::from_millis(120)).await;
                        let _ = conn.close().await;
                    }

                    sam.close().await.map_err(|e| e.to_string())
                },
                move |result| Message::SamCloseFinished(tab_id, result),
            ));
        }

        tasks
    }

    fn is_transient_profile_name(name: &str) -> bool {
        name == "default"
    }

    fn active_contact_meta(&self) -> Option<ContactMeta> {
        let profile_name = self.active_tab()?.session.profile.clone();

        if Self::is_transient_profile_name(&profile_name) {
            return None;
        }

        Some(ContactMeta {
            name: profile_name,
            my_dest_b64: self.active_tab()?.session.my_dest_b64.clone(),
            locked_peer: self.active_tab()?.session.stored_peer.clone(),
            locked_peer_dest_b64: self.active_tab()?.session.stored_peer_dest_b64.clone(),
            pq_enabled: self.active_tab()?.session.pq_enabled,
            deaddrop_servers: self.active_tab()?.session.deaddrop_servers.clone(),
        })
    }

    fn save_active_contact_meta(&mut self) {
        let Some(profile_name) = self.active_tab().map(|t| t.session.profile.clone()) else {
            return;
        };

        self.save_active_contact_meta_for_name(&profile_name);
    }

    fn save_active_contact_meta_for_name(&mut self, profile_name: &str) {
        if profile_name == "default" {
            return;
        }

        let maybe_meta = self
            .opened_tabs
            .iter()
            .find(|t| t.session.profile == profile_name)
            .map(|tab| ContactMeta {
                name: profile_name.to_string(),
                my_dest_b64: tab.session.my_dest_b64.clone(),
                locked_peer: tab.session.stored_peer.clone(),
                locked_peer_dest_b64: tab.session.stored_peer_dest_b64.clone(),
                pq_enabled: tab.session.pq_enabled,
                deaddrop_servers: tab.session.deaddrop_servers.clone(),
            });

        if let Some(meta) = maybe_meta {
            if let Err(err) = storage::save_contact_meta(&meta) {
                self.session
                    .log_lines
                    .push(format!("Save contact metadata failed: {err}"));
            }

            if let Some(tab) = self
                .opened_tabs
                .iter()
                .find(|t| t.session.profile == profile_name)
            {
                if let Err(err) =
                    storage::save_deaddrop_stats(profile_name, &tab.session.deaddrop_stats)
                {
                    self.session
                        .log_lines
                        .push(format!("Save deaddrop stats failed: {err}"));
                }

                if let Some(peer_b32) = Self::offline_state_peer_b32_for_session(&tab.session) {
                    let offline = Self::offline_state_from_session(&tab.session);

                    if let Err(err) = storage::save_offline_state(profile_name, peer_b32, &offline)
                    {
                        self.session
                            .log_lines
                            .push(format!("Save offline state failed: {err}"));
                    }
                }
            }
        }
    }

    fn apply_contact_meta_to_opened_tab(tab: &mut OpenedTab, meta: &ContactMeta) {
        tab.session.my_dest_b64 = meta.my_dest_b64.clone();
        tab.session.stored_peer = meta.locked_peer.clone();
        tab.session.stored_peer_dest_b64 = meta.locked_peer_dest_b64.clone();
        tab.session.pq_enabled = meta.pq_enabled;
        tab.session.pq_active = meta.pq_enabled;
        tab.session.tofu_verified = false;
        tab.session.tofu_mismatch = false;
        tab.session.deaddrop_servers = meta.deaddrop_servers.clone();
        tab.session.deaddrop_stats = storage::load_deaddrop_stats(&meta.name)
            .unwrap_or_default()
            .into_iter()
            .filter(|(server, _)| Self::is_valid_deaddrop_server(server))
            .collect();
        Self::ensure_deaddrop_stat_entries(&mut tab.session);
        Self::rank_deaddrop_servers(&mut tab.session);
        tab.session.deaddrop_stats_dirty = false;
        tab.session.deaddrop_stats_last_save_ms = Self::now_epoch_millis();

        tab.session.offline_shared_secret = None;
        tab.session.drop_send_index = 0;
        tab.session.drop_recv_base = 0;
        tab.session.drop_window = 8;
        tab.session.consumed_drop_recv.clear();

        if let Some(peer_b32) = meta.locked_peer.as_deref() {
            if let Ok(offline) = storage::load_offline_state(&meta.name, peer_b32) {
                Self::apply_offline_state_to_session(&mut tab.session, &offline);
            }
        }
    }

    fn clear_tofu_runtime_status(&mut self) {
        self.session.tofu_verified = false;
        self.session.tofu_mismatch = false;
    }

    fn set_tofu_verified(&mut self) {
        self.session.tofu_verified = true;
        self.session.tofu_mismatch = false;
    }

    fn set_tofu_mismatch(&mut self) {
        self.session.tofu_verified = false;
        self.session.tofu_mismatch = true;
    }

    fn peer_dest_matches_tofu(&self, dest_b64: &str) -> bool {
        match &self.session.stored_peer_dest_b64 {
            Some(stored) => stored == dest_b64,
            None => true,
        }
    }

    fn is_persistent_profile(&self) -> bool {
        self.session.profile != "default"
    }

    fn post_system(&mut self, text: impl Into<String>) {
        self.session.log_lines.push(text.into());
    }

    fn copy_text_to_clipboard(&mut self, value: String, label: &str) {
        match self.clipboard.as_mut() {
            Some(clipboard) => {
                if let Err(err) = clipboard.set_text(value.clone()) {
                    self.post_system(format!("Clipboard copy failed: {err}"));
                } else {
                    self.post_system(format!("Copied {label}: {value}"));
                }
            }
            None => {
                self.post_system("Clipboard is not available.");
            }
        }
    }

    fn copy_my_b32_to_clipboard(&mut self) {
        let Some(my_b32) = self.session.my_b32.clone() else {
            self.post_system("My b32 address is not available yet.");
            return;
        };

        self.copy_text_to_clipboard(my_b32, "my b32 address");
    }

    fn copy_peer_b32_to_clipboard(&mut self) {
        if !self.session.live_ready {
            self.post_system("Peer b32 address is not available.");
            return;
        }

        let Some(peer_b32) = self.session.current_peer_addr.clone() else {
            self.post_system("Peer b32 address is not available.");
            return;
        };

        self.copy_text_to_clipboard(peer_b32, "peer b32 address");
    }

    fn mock_connect(&mut self, peer: String) {
        self.session.current_peer_addr = Some(peer.clone());
        self.session.peer_b32 = Some(peer.clone());
        self.session.network_status = NetworkStatus::Visible;
        self.session.live_ready = true;
        self.session.offline_mode = false;
        self.post_system(format!("Connected to {peer}"));
        self.sync_active_tab_flags();
    }

    fn mock_disconnect(&mut self) {
        self.session.current_peer_addr = None;
        self.session.current_peer_dest_b64 = None;
        self.session.peer_b32 = None;
        self.session.live_ready = false;
        self.session.offline_mode = false;
        self.session.network_status = NetworkStatus::LocalOk;
        self.clear_tofu_runtime_status();
        self.post_system("Disconnected");
        self.sync_active_tab_flags();
    }

    fn accept_pending(&mut self) {
        if let Some(conn) = self.active_pending_conn() {
            self.set_active_pending_conn(None);
            let accepted_from = self
                .session
                .pending_peer_addr
                .take()
                .unwrap_or_else(|| "Unknown".into());

            let accepted_dest_b64 = self.session.pending_peer_dest_b64.take();

            self.set_active_live_conn(Some(conn));
            self.session.live_ready = true;
            self.session.offline_mode = false;
            self.session.network_status = NetworkStatus::Visible;
            self.session.current_peer_addr = Some(accepted_from.clone());
            self.session.current_peer_dest_b64 = accepted_dest_b64.clone();

            if let Some(dest_b64) = &self.session.current_peer_dest_b64 {
                if self.peer_dest_matches_tofu(dest_b64) {
                    if self.session.stored_peer_dest_b64.is_some() {
                        self.set_tofu_verified();
                    }
                } else {
                    self.set_tofu_mismatch();
                    self.post_system("TOFU mismatch on accepted incoming connection.");
                    self.reset_connection_state();
                    self.store_active_runtime();
                    return;
                }
            }

            self.session.peer_b32 = Some(accepted_from.clone());
            self.session.call_blink_on = true;
            self.session.call_blink_ticks = 0;
            self.post_system(format!("Accepted incoming call from {accepted_from}"));
            self.sync_active_tab_flags();

            self.store_active_runtime();
            let _ = accepted_dest_b64;
        }
    }

    fn decline_pending(&mut self) {
        self.set_active_pending_conn(None);
        self.session.pending_peer_addr = None;
        self.session.pending_peer_dest_b64 = None;

        self.session.current_peer_addr = None;
        self.session.current_peer_dest_b64 = None;
        self.session.peer_b32 = None;

        self.session.call_blink_on = true;
        self.session.call_blink_ticks = 0;
        self.session.network_status = NetworkStatus::LocalOk;
        self.clear_tofu_runtime_status();

        self.post_system("Declined incoming call.");
        self.sync_active_tab_flags();

        self.store_active_runtime();
    }

    fn clear_pending_dead_call(tab: &mut OpenedTab, message: &str) {
        tab.pending_conn = None;
        tab.session.pending_peer_addr = None;
        tab.session.pending_peer_dest_b64 = None;

        tab.session.current_peer_addr = None;
        tab.session.current_peer_dest_b64 = None;
        tab.session.peer_b32 = None;

        tab.session.live_ready = false;
        tab.session.offline_mode = false;
        tab.session.network_status = NetworkStatus::LocalOk;
        tab.session.pq_active = false;
        tab.session.tofu_verified = false;
        tab.session.tofu_mismatch = false;

        tab.session.call_blink_on = true;
        tab.session.call_blink_ticks = 0;
        tab.session.accept_armed = true;

        tab.session.log_lines.push(message.to_string());
        tab.session
            .log_lines
            .push("Incoming accept loop re-armed.".to_string());

        tab.meta.has_incoming = false;
    }

    fn mock_lock(&mut self) {
        let Some(peer) = self.session.current_peer_addr.clone() else {
            self.post_system("Cannot lock: current peer address is unknown.");
            return;
        };

        let Some(current_dest) = self.session.current_peer_dest_b64.clone() else {
            self.post_system("Cannot lock: current peer destination is unknown.");
            return;
        };

        self.session.stored_peer = Some(peer.clone());
        self.session.stored_peer_dest_b64 = Some(current_dest);
        self.set_tofu_verified();
        self.post_system(format!("Locked peer: {peer}"));
    }

    fn mock_unlock(&mut self) {
        let profile_name = self.session.profile.clone();
        let old_peer = self.session.stored_peer.clone();

        self.session.stored_peer = None;
        self.session.stored_peer_dest_b64 = None;
        self.session.show_deaddrop_panel = false;
        self.session.deaddrop_server_input.clear();
        self.session.offline_shared_secret = None;
        self.session.drop_send_index = 0;
        self.session.drop_recv_base = 0;
        self.session.drop_window = 8;
        self.session.consumed_drop_recv.clear();

        if profile_name != "default" {
            if let Some(peer_b32) = old_peer.as_deref() {
                let _ = storage::delete_offline_state(&profile_name, peer_b32);
            }
        }

        self.clear_tofu_runtime_status();
        self.post_system("Unlocked peer");
    }

    fn mock_offline(&mut self) {
        if !self.offline_ready() {
            self.post_system("Offline mode requires persistent locked-peer mode with deaddrop servers configured.");
            return;
        }

        if !self.has_real_offline_secret() {
            self.post_system("Offline mode requires an offline shared secret.");
            return;
        }

        if let Some(tab) = self.active_tab_mut() {
            tab.live_conn = None;
            tab.pending_conn = None;
            tab.session.live_ready = false;
            tab.session.pending_peer_addr = None;
            tab.session.pending_peer_dest_b64 = None;
            tab.session.current_peer_addr = None;
            tab.session.current_peer_dest_b64 = None;
            tab.session.peer_b32 = None;
            tab.session.network_status = NetworkStatus::LocalOk;
            tab.session.call_blink_on = true;
            tab.session.call_blink_ticks = 0;
            tab.session.offline_mode = true;
            tab.meta.connected = false;
            tab.meta.has_incoming = false;
        }

        self.session.live_ready = false;
        self.session.pending_peer_addr = None;
        self.session.pending_peer_dest_b64 = None;
        self.session.current_peer_addr = None;
        self.session.current_peer_dest_b64 = None;
        self.session.peer_b32 = None;
        self.session.network_status = NetworkStatus::LocalOk;
        self.session.call_blink_on = true;
        self.session.call_blink_ticks = 0;
        self.session.offline_mode = true;

        self.post_system("Entered OFFLINE mode");
        self.sync_active_tab_flags();
    }

    fn mock_online(&mut self) {
        self.session.offline_mode = false;
        self.session.live_ready = false;
        self.session.network_status = NetworkStatus::LocalOk;
        self.session.pending_peer_addr = None;
        self.session.pending_peer_dest_b64 = None;
        self.session.current_peer_addr = None;
        self.session.current_peer_dest_b64 = None;
        self.session.peer_b32 = None;
        self.session.call_blink_on = true;
        self.session.call_blink_ticks = 0;

        if let Some(tab) = self.active_tab_mut() {
            tab.live_conn = None;
            tab.pending_conn = None;
            tab.session.offline_mode = false;
            tab.session.live_ready = false;
            tab.session.network_status = NetworkStatus::LocalOk;
            tab.session.pending_peer_addr = None;
            tab.session.pending_peer_dest_b64 = None;
            tab.session.current_peer_addr = None;
            tab.session.current_peer_dest_b64 = None;
            tab.session.peer_b32 = None;
            tab.session.call_blink_on = true;
            tab.session.call_blink_ticks = 0;
            tab.meta.connected = false;
            tab.meta.has_incoming = false;
        }

        self.post_system("Returned to ONLINE standby");
        self.sync_active_tab_flags();
    }

    fn toggle_pq(&mut self) {
        self.session.pq_enabled = !self.session.pq_enabled;
        self.session.pq_active = self.session.pq_enabled;
        self.post_system(format!(
            "Post-quantum mode: {}",
            if self.session.pq_active { "ON" } else { "OFF" }
        ));
    }

    fn set_dd_status(session: &mut SessionState, status: &str) {
        session.dd_status = status.into();
        session.dd_status_at_ms = Self::now_epoch_millis();
    }

    fn deaddrop_panel_allowed(session: &SessionState) -> bool {
        session.profile != "default" && session.stored_peer.is_some()
    }

    fn available_actions(&self) -> Vec<GuiAction> {
        let mut out = Vec::new();

        if self.session.pending_peer_addr.is_some() {
            out.push(GuiAction::Accept);
            out.push(GuiAction::Decline);
            return out;
        }

        if self.has_active_connection_attempt() {
            out.push(GuiAction::Disconnect);

            if self.session.live_ready {
                out.push(GuiAction::SendImage);
                out.push(GuiAction::SendFile);
            }

            out.push(GuiAction::Pq);

            if self.session.profile != "default" {
                if self.session.stored_peer.is_some() {
                    out.push(GuiAction::Unlock);
                    out.push(GuiAction::DdList);
                } else if self.session.live_ready {
                    out.push(GuiAction::Lock);
                }
            }

            return out;
        }

        if self.session.offline_mode {
            out.push(GuiAction::Online);

            if Self::deaddrop_panel_allowed(&self.session) {
                out.push(GuiAction::DdList);
            }

            return out;
        }

        out.push(GuiAction::Connect);
        out.push(GuiAction::Pq);

        if self.session.profile != "default" {
            if self.session.stored_peer.is_some() {
                out.push(GuiAction::Unlock);
                if self.offline_ready() {
                    out.push(GuiAction::Offline);
                }
                out.push(GuiAction::DdList);
            } else {
                out.push(GuiAction::Lock);
            }
        }

        out
    }

    fn action_label(action: GuiAction) -> &'static str {
        match action {
            GuiAction::Connect => "Connect",
            GuiAction::Disconnect => "Disconnect",
            GuiAction::SendFile => "Send File",
            GuiAction::SendImage => "Send Image",
            GuiAction::CopyMyB32 => "Copy My B32",
            GuiAction::Accept => "Accept",
            GuiAction::Decline => "Decline",
            GuiAction::Lock => "Lock",
            GuiAction::Unlock => "Unlock",
            GuiAction::Offline => "Offline",
            GuiAction::Online => "Online",
            GuiAction::Pq => "PQ",
            GuiAction::Help => "Help",
            GuiAction::DdList => "Show DD List",
        }
    }

    fn action_label_for_session(session: &SessionState, action: GuiAction) -> &'static str {
        if action == GuiAction::DdList && session.show_deaddrop_panel {
            "Hide DD List"
        } else {
            Self::action_label(action)
        }
    }

    fn action_enabled(action: GuiAction) -> bool {
        !matches!(action, GuiAction::Help | GuiAction::Pq)
    }

    fn action_needs_param(action: GuiAction) -> bool {
        matches!(action, GuiAction::Connect)
    }

    fn action_needs_confirm(action: GuiAction) -> bool {
        matches!(action, GuiAction::Lock | GuiAction::Unlock)
    }

    fn action_confirm_prompt(action: GuiAction) -> &'static str {
        match action {
            GuiAction::Lock => "Lock the current peer to this profile?",
            GuiAction::Unlock => "Unlock this profile from its stored peer?",
            _ => "",
        }
    }

    fn message_input_enabled(&self) -> bool {
        (self.session.live_ready && self.active_live_conn().is_some())
            || self.can_send_offline_now()
    }

    fn tick_one_tab(&mut self, idx: usize) -> Vec<Task<Message>> {
        let mut tasks = Vec::new();
        //let is_active = self.session.active_tab_idx == Some(idx);
        let is_active = self.session.active_tab_idx == Some(Self::real_to_visible_tab_index(idx));

        let mut secure_session_just_established = false;
        let mut secure_session_tab_id: Option<u64> = None;

        let push_log = |tab: &mut OpenedTab, line: String| {
            tab.session.log_lines.push(line);
        };

        if let Some(tab) = self.opened_tabs.get_mut(idx) {
            let tab_id = tab.id;
            if tab.session.pending_peer_addr.is_some() {
                tab.session.call_blink_ticks = tab.session.call_blink_ticks.wrapping_add(1);
                if tab.session.call_blink_ticks >= 4 {
                    tab.session.call_blink_ticks = 0;
                    tab.session.call_blink_on = !tab.session.call_blink_on;
                }
            } else {
                tab.session.call_blink_on = true;
                tab.session.call_blink_ticks = 0;
            }

            if let Some(conn) = tab.live_conn.clone() {
                while let Some(frame) = conn.try_recv_frame() {
                    match frame.msg_type {
                        MsgType::F => {
                            let plain = tab.e2e.decrypt(&frame.payload);

                            match String::from_utf8(plain) {
                                Ok(body) => {
                                    let mut parts = body.split('|');
                                    let Some(filename_raw) = parts.next() else {
                                        push_log(tab, "Invalid file header.".to_string());
                                        continue;
                                    };
                                    let Some(size_raw) = parts.next() else {
                                        push_log(tab, "Invalid file header.".to_string());
                                        continue;
                                    };

                                    let filename = PathBuf::from(filename_raw)
                                        .file_name()
                                        .and_then(|s| s.to_str())
                                        .unwrap_or("file.bin")
                                        .to_string();

                                    let total_bytes: u64 = match size_raw.parse() {
                                        Ok(v) => v,
                                        Err(_) => {
                                            push_log(tab, "Invalid file size.".to_string());
                                            continue;
                                        }
                                    };

                                    if total_bytes == 0 || total_bytes > MAX_FILE_SIZE as u64 {
                                        push_log(
                                            tab,
                                            format!("Rejected file size: {total_bytes} bytes."),
                                        );
                                        continue;
                                    }

                                    let dir = match Self::ensure_files_dir() {
                                        Ok(v) => v,
                                        Err(err) => {
                                            push_log(
                                                tab,
                                                format!("Failed to create file dir: {err}"),
                                            );
                                            continue;
                                        }
                                    };

                                    let save_path =
                                        dir.join(format!("recv_{}_{}", frame.msg_id, filename));

                                    match storage::create_file_secure(&save_path) {
                                        Ok(file) => {
                                            tab.incoming_file = Some(file);
                                            tab.incoming_filename = Some(filename.clone());
                                            tab.incoming_expected = total_bytes;
                                            tab.incoming_received = 0;
                                            tab.incoming_save_path = Some(save_path.clone());

                                            Self::push_incoming_file_bubble(
                                                tab,
                                                filename,
                                                save_path.display().to_string(),
                                                total_bytes,
                                            );
                                        }
                                        Err(err) => {
                                            push_log(
                                                tab,
                                                format!("Failed to open incoming file: {err}"),
                                            );
                                        }
                                    }
                                }
                                Err(_) => {
                                    push_log(tab, "Invalid UTF-8 file header.".to_string());
                                }
                            }
                        }

                        MsgType::C => {
                            let plain = tab.e2e.decrypt(&frame.payload);

                            if let Some(file) = tab.incoming_file.as_mut() {
                                match general_purpose::STANDARD.decode(&plain) {
                                    Ok(chunk) => {
                                        tab.incoming_received += chunk.len() as u64;

                                        if tab.incoming_received > tab.incoming_expected {
                                            push_log(
                                                tab,
                                                "File transfer overflow detected.".to_string(),
                                            );
                                            tab.incoming_file = None;
                                            tab.incoming_filename = None;
                                            tab.incoming_expected = 0;
                                            tab.incoming_received = 0;
                                            tab.incoming_save_path = None;
                                            tab.incoming_bubble_index = None;
                                            continue;
                                        }

                                        if let Err(err) = file.write_all(&chunk) {
                                            push_log(
                                                tab,
                                                format!("File chunk write failed: {err}"),
                                            );
                                            tab.incoming_file = None;
                                            continue;
                                        }

                                        if let Some(idx) = tab.incoming_bubble_index {
                                            if let Some(bubble) = tab.session.bubbles.get_mut(idx) {
                                                if let BubbleContent::File(file_bubble) =
                                                    &mut bubble.content
                                                {
                                                    file_bubble.done_bytes = tab.incoming_received;
                                                    file_bubble.status = "Receiving...".into();
                                                }
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        push_log(tab, format!("File chunk decode failed: {err}"));
                                    }
                                }
                            }
                        }

                        MsgType::E => {
                            if let Some(mut file) = tab.incoming_file.take() {
                                let _ = file.flush();

                                if let Some(idx) = tab.incoming_bubble_index.take() {
                                    if let Some(bubble) = tab.session.bubbles.get_mut(idx) {
                                        if let BubbleContent::File(file_bubble) =
                                            &mut bubble.content
                                        {
                                            file_bubble.done_bytes = tab.incoming_received;
                                            file_bubble.complete = true;
                                            file_bubble.failed = false;
                                            file_bubble.status = "Received".into();
                                        }
                                    }
                                }

                                push_log(
                                    tab,
                                    format!(
                                        "File received: {} ({} bytes)",
                                        tab.incoming_filename
                                            .clone()
                                            .unwrap_or_else(|| "unknown".into()),
                                        tab.incoming_received
                                    ),
                                );

                                tab.incoming_filename = None;
                                tab.incoming_expected = 0;
                                tab.incoming_received = 0;
                                tab.incoming_save_path = None;
                            }
                        }

                        MsgType::J => {
                            let plain = tab.e2e.decrypt(&frame.payload);

                            match String::from_utf8(plain) {
                                Ok(body) => {
                                    let mut parts = body.split('|');
                                    let Some(filename_raw) = parts.next() else {
                                        push_log(tab, "Invalid image header.".to_string());
                                        continue;
                                    };
                                    let Some(mime_raw) = parts.next() else {
                                        push_log(tab, "Invalid image header.".to_string());
                                        continue;
                                    };
                                    let Some(size_raw) = parts.next() else {
                                        push_log(tab, "Invalid image header.".to_string());
                                        continue;
                                    };

                                    let filename = PathBuf::from(filename_raw)
                                        .file_name()
                                        .and_then(|s| s.to_str())
                                        .unwrap_or("image")
                                        .to_string();

                                    let total_bytes: u64 = match size_raw.parse() {
                                        Ok(v) => v,
                                        Err(_) => {
                                            push_log(tab, "Invalid image size.".to_string());
                                            continue;
                                        }
                                    };

                                    if total_bytes == 0 || total_bytes > MAX_FILE_SIZE as u64 {
                                        push_log(
                                            tab,
                                            format!("Rejected image size: {total_bytes} bytes."),
                                        );
                                        continue;
                                    }

                                    if !Self::is_supported_image_mime(mime_raw) {
                                        push_log(
                                            tab,
                                            format!("Unsupported incoming image type: {mime_raw}"),
                                        );
                                        continue;
                                    }

                                    Self::clear_incoming_image_state(tab);
                                    tab.incoming_image_name = Some(filename);
                                    tab.incoming_image_mime = Some(mime_raw.to_string());
                                    tab.incoming_image_expected = total_bytes;
                                    tab.incoming_image_received = 0;
                                    tab.incoming_image_msg_id = frame.msg_id;
                                    tab.incoming_image_bytes = Vec::with_capacity(
                                        total_bytes.min(MAX_FILE_SIZE as u64) as usize,
                                    );
                                }
                                Err(_) => {
                                    push_log(tab, "Invalid UTF-8 image header.".to_string());
                                }
                            }
                        }

                        MsgType::G => {
                            if tab.incoming_image_name.is_none() {
                                push_log(
                                    tab,
                                    "Image chunk received without image header.".to_string(),
                                );
                                continue;
                            }

                            if tab.incoming_image_msg_id != frame.msg_id {
                                push_log(tab, "Image chunk transfer id mismatch.".to_string());
                                continue;
                            }

                            let plain = tab.e2e.decrypt(&frame.payload);

                            match general_purpose::STANDARD.decode(&plain) {
                                Ok(chunk) => {
                                    let next_total =
                                        tab.incoming_image_received + chunk.len() as u64;

                                    if next_total > tab.incoming_image_expected {
                                        push_log(
                                            tab,
                                            "Image transfer overflow detected.".to_string(),
                                        );
                                        Self::clear_incoming_image_state(tab);
                                        continue;
                                    }

                                    tab.incoming_image_bytes.extend_from_slice(&chunk);
                                    tab.incoming_image_received = next_total;
                                }
                                Err(err) => {
                                    push_log(tab, format!("Image chunk decode failed: {err}"));
                                    Self::clear_incoming_image_state(tab);
                                }
                            }
                        }

                        MsgType::Z => {
                            if tab.incoming_image_name.is_none() {
                                push_log(
                                    tab,
                                    "Image end received without image header.".to_string(),
                                );
                                continue;
                            }

                            if tab.incoming_image_msg_id != frame.msg_id {
                                push_log(tab, "Image end transfer id mismatch.".to_string());
                                continue;
                            }

                            if tab.incoming_image_received != tab.incoming_image_expected {
                                push_log(
                                    tab,
                                    format!(
                                        "Incomplete image transfer: {}/{} bytes.",
                                        tab.incoming_image_received, tab.incoming_image_expected
                                    ),
                                );
                                Self::clear_incoming_image_state(tab);
                                continue;
                            }

                            let image_name = tab
                                .incoming_image_name
                                .clone()
                                .unwrap_or_else(|| "image".into());
                            let image_bytes = std::mem::take(&mut tab.incoming_image_bytes);

                            tab.session.bubbles.push(Bubble {
                                author: "Peer".into(),
                                content: BubbleContent::Image(Self::image_bubble_data(image_bytes)),
                                mine: false,
                                offline: false,
                                timestamp_utc: Self::now_utc_hms(),
                                msg_id: None,
                                delivered: false,
                            });

                            push_log(
                                tab,
                                format!(
                                    "Image received: {image_name} ({} bytes)",
                                    tab.incoming_image_received
                                ),
                            );

                            Self::clear_incoming_image_state(tab);

                            if !is_active {
                                tab.meta.has_unread = true;
                            }

                            let ack_msg_id = {
                                let millis = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .map(|d| d.as_millis() as u64)
                                    .unwrap_or(0);

                                let random_bits: u64 = rand::rng().random::<u32>() as u64;
                                (millis ^ random_bits) & 0xFFFF_FFFF_FFFF_FFFF
                            };

                            let ack = Frame {
                                msg_type: MsgType::D,
                                msg_id: ack_msg_id,
                                payload: frame.msg_id.to_be_bytes().to_vec(),
                            };

                            let conn_for_ack = conn.clone();

                            tasks.push(Task::perform(
                                async move {
                                    conn_for_ack
                                        .send_frame(&ack)
                                        .await
                                        .map_err(|e| e.to_string())
                                },
                                move |result| Message::SendFinished(tab_id, result),
                            ));
                        }

                        MsgType::U => {
                            let delivered_original_msg_id = frame.msg_id;
                            let plain = tab.e2e.decrypt(&frame.payload);

                            match String::from_utf8(plain) {
                                Ok(text) => {
                                    tab.session.bubbles.push(Bubble::peer(text));

                                    let ack_msg_id = {
                                        let millis = SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .map(|d| d.as_millis() as u64)
                                            .unwrap_or(0);

                                        let random_bits: u64 = rand::rng().random::<u32>() as u64;
                                        (millis ^ random_bits) & 0xFFFF_FFFF_FFFF_FFFF
                                    };

                                    let ack = Frame {
                                        msg_type: MsgType::D,
                                        msg_id: ack_msg_id,
                                        payload: delivered_original_msg_id.to_be_bytes().to_vec(),
                                    };

                                    let conn_for_ack = conn.clone();

                                    tasks.push(Task::perform(
                                        async move {
                                            conn_for_ack
                                                .send_frame(&ack)
                                                .await
                                                .map_err(|e| e.to_string())
                                        },
                                        move |result| Message::SendFinished(tab_id, result),
                                    ));

                                    if !is_active {
                                        tab.meta.has_unread = true;
                                    }
                                }
                                Err(_) => {
                                    push_log(
                                        tab,
                                        "Received invalid UTF-8 chat payload.".to_string(),
                                    );
                                }
                            }
                        }

                        MsgType::D => {
                            if frame.payload.len() == 8 {
                                let mut bytes = [0u8; 8];
                                bytes.copy_from_slice(&frame.payload);
                                let delivered_id = u64::from_be_bytes(bytes);

                                Self::mark_delivered(tab, delivered_id);
                            } else {
                                push_log(tab, "Invalid delivery ACK payload.".to_string());
                            }
                        }

                        MsgType::K => {
                            tab.e2e.receive_peer_key(&frame.payload);

                            if tab.e2e.ready() {
                                tab.session.live_ready = true;
                                push_log(tab, "Secure session established.".to_string());

                                secure_session_just_established = true;
                                secure_session_tab_id = Some(tab_id);
                            } else {
                                push_log(tab, "Received peer E2E key.".to_string());
                            }
                        }

                        MsgType::S => match String::from_utf8(frame.payload) {
                            Ok(body) => {
                                if body == "__SIGNAL__:QUIT" {
                                    tab.live_conn = None;
                                    tab.session.current_peer_addr = None;
                                    tab.session.current_peer_dest_b64 = None;
                                    tab.session.peer_b32 = None;
                                    tab.session.live_ready = false;
                                    tab.session.offline_mode = false;
                                    tab.session.network_status = NetworkStatus::LocalOk;
                                    tab.session.tofu_verified = false;
                                    tab.session.tofu_mismatch = false;
                                    tab.e2e = E2E::new(tab.session.pq_enabled);
                                    Self::clear_outgoing_image_state(tab);
                                    Self::clear_incoming_image_state(tab);
                                    push_log(tab, "Peer disconnected.".to_string());
                                    tab.session.accept_armed = true;
                                    push_log(tab, "Incoming accept loop re-armed.".to_string());

                                    let sam = tab.sam.clone();
                                    tasks.push(Task::perform(
                                        async move {
                                            sam.stream_accept().await.map_err(|e| e.to_string())
                                        },
                                        move |result| Message::IncomingAccepted(tab_id, result),
                                    ));
                                    break;
                                }

                                match SamClient::destination_to_b32(&body) {
                                    Ok(peer_b32) => {
                                        tab.session.current_peer_addr = Some(peer_b32.clone());
                                        tab.session.current_peer_dest_b64 = Some(body.clone());
                                        tab.session.peer_b32 = Some(peer_b32.clone());

                                        let tofu_ok = match &tab.session.stored_peer_dest_b64 {
                                            Some(stored) => stored == &body,
                                            None => true,
                                        };

                                        if tofu_ok {
                                            if tab.session.stored_peer_dest_b64.is_some() {
                                                tab.session.tofu_verified = true;
                                                tab.session.tofu_mismatch = false;
                                                push_log(
                                                    tab,
                                                    format!("Peer identity verified: {peer_b32}"),
                                                );
                                            } else {
                                                push_log(tab, format!("Peer identity: {peer_b32}"));
                                            }
                                        } else {
                                            tab.session.tofu_verified = false;
                                            tab.session.tofu_mismatch = true;
                                            push_log(
                                                tab,
                                                format!("TOFU mismatch for peer: {peer_b32}"),
                                            );

                                            tab.live_conn = None;
                                            tab.session.current_peer_addr = None;
                                            tab.session.current_peer_dest_b64 = None;
                                            tab.session.peer_b32 = None;
                                            tab.session.live_ready = false;
                                            tab.session.offline_mode = false;
                                            tab.session.network_status = NetworkStatus::LocalOk;
                                            Self::clear_outgoing_image_state(tab);
                                            Self::clear_incoming_image_state(tab);
                                        }
                                    }
                                    Err(err) => {
                                        push_log(
                                            tab,
                                            format!("Failed to parse peer identity: {err}"),
                                        );
                                    }
                                }
                            }
                            Err(_) => {
                                push_log(
                                    tab,
                                    "Received invalid UTF-8 identity payload.".to_string(),
                                );
                            }
                        },

                        MsgType::X => {
                            if !(tab.session.profile != "default"
                                && tab.session.stored_peer.is_some()
                                && tab.session.stored_peer_dest_b64.is_some()
                                && !tab.session.deaddrop_servers.is_empty())
                            {
                                push_log(
                                    tab,
                                    "Received offline secret outside persistent locked-peer mode."
                                        .to_string(),
                                );
                                continue;
                            }

                            if frame.payload.len() != 32 {
                                push_log(tab, "Invalid offline secret length.".to_string());
                                continue;
                            }

                            let already_has_real = tab
                                .session
                                .offline_shared_secret
                                .map(|s| s.iter().any(|b| *b != 0))
                                .unwrap_or(false);

                            if already_has_real {
                                push_log(
                                    tab,
                                    "Offline secret already exists. Ignoring replacement."
                                        .to_string(),
                                );
                                continue;
                            }

                            let mut secret = [0u8; 32];
                            secret.copy_from_slice(&frame.payload);
                            tab.session.offline_shared_secret = Some(secret);

                            if let Some(peer_b32) = tab.session.stored_peer.clone() {
                                let offline = OfflineState {
                                    offline_shared_secret: secret,
                                    drop_send_index: tab.session.drop_send_index,
                                    drop_recv_base: tab.session.drop_recv_base,
                                    drop_window: tab.session.drop_window,
                                    consumed_drop_recv: tab.session.consumed_drop_recv.clone(),
                                };

                                match storage::save_offline_state(
                                    &tab.session.profile,
                                    &peer_b32,
                                    &offline,
                                ) {
                                    Ok(()) => {
                                        push_log(
                                            tab,
                                            "Offline secret received and saved.".to_string(),
                                        );
                                    }
                                    Err(err) => {
                                        push_log(tab, format!("Offline secret save failed: {err}"));
                                    }
                                }
                            } else {
                                push_log(
                                    tab,
                                    "Offline secret received but locked peer is missing."
                                        .to_string(),
                                );
                            }
                        }

                        MsgType::L => match String::from_utf8(frame.payload) {
                            Ok(body) => {
                                let servers: Vec<String> = body
                                    .lines()
                                    .map(|line| line.trim().to_string())
                                    .filter(|line| !line.is_empty())
                                    .collect();

                                if servers.is_empty() {
                                    continue;
                                }

                                let changed = Self::merge_deaddrop_servers_into_session(
                                    &mut tab.session,
                                    &servers,
                                );
                                Self::sync_tab_deaddrop_servers(tab);

                                if changed {
                                    match storage::load_contact_meta(&tab.session.profile) {
                                        Ok(mut meta) => {
                                            meta.deaddrop_servers =
                                                tab.session.deaddrop_servers.clone();

                                            match storage::save_contact_meta(&meta) {
                                                Ok(()) => {
                                                    push_log(
                                                        tab,
                                                        format!(
                                                            "Merged deaddrop server list from peer. Total: {}",
                                                            tab.session.deaddrop_servers.len()
                                                        ),
                                                    );
                                                }
                                                Err(err) => {
                                                    push_log(
                                                        tab,
                                                        format!(
                                                            "Failed to save merged deaddrop servers: {err}"
                                                        ),
                                                    );
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            push_log(
                                                tab,
                                                format!(
                                                    "Failed to load contact metadata for deaddrop merge: {err}"
                                                ),
                                            );
                                        }
                                    }
                                } else {
                                    push_log(
                                        tab,
                                        "Received deaddrop server list from peer (no new entries)."
                                            .to_string(),
                                    );
                                }
                            }
                            Err(_) => {
                                push_log(
                                    tab,
                                    "Received invalid UTF-8 deaddrop server list.".to_string(),
                                );
                            }
                        },

                        other => {
                            push_log(tab, format!("Received unsupported frame type: {:?}", other));
                        }
                    }
                }

                if conn.is_closed() && !conn.has_pending_frames() {
                    tab.live_conn = None;
                    tab.session.current_peer_addr = None;
                    tab.session.current_peer_dest_b64 = None;
                    tab.session.peer_b32 = None;
                    tab.session.live_ready = false;
                    tab.session.offline_mode = false;
                    tab.session.network_status = NetworkStatus::LocalOk;
                    tab.session.tofu_verified = false;
                    tab.session.tofu_mismatch = false;
                    tab.e2e = E2E::new(tab.session.pq_enabled);
                    Self::clear_outgoing_image_state(tab);
                    Self::clear_incoming_image_state(tab);
                    push_log(tab, "Live connection closed.".to_string());
                    tab.session.accept_armed = true;
                    push_log(tab, "Incoming accept loop re-armed.".to_string());

                    let sam = tab.sam.clone();
                    tasks.push(Task::perform(
                        async move { sam.stream_accept().await.map_err(|e| e.to_string()) },
                        move |result| Message::IncomingAccepted(tab_id, result),
                    ));

                    tasks.push(Task::perform(
                        async move { conn.close().await.map_err(|e| e.to_string()) },
                        move |result| Message::CloseFinished(tab_id, result),
                    ));
                }
            } else if let Some(conn) = tab.pending_conn.clone() {
                if conn.is_dead() && !conn.has_pending_frames() {
                    let caller = tab
                        .session
                        .pending_peer_addr
                        .clone()
                        .unwrap_or_else(|| "Unknown".to_string());

                    Self::clear_pending_dead_call(
                        tab,
                        &format!("Incoming caller disconnected: {}", caller),
                    );

                    let sam = tab.sam.clone();
                    tasks.push(Task::perform(
                        async move { sam.stream_accept().await.map_err(|e| e.to_string()) },
                        move |result| Message::IncomingAccepted(tab_id, result),
                    ));

                    tab.meta.connected = tab.session.live_ready;
                    tab.meta.has_incoming = tab.session.pending_peer_addr.is_some();
                    return tasks;
                }

                while let Some(frame) = conn.try_recv_frame() {
                    match frame.msg_type {
                        MsgType::S => match String::from_utf8(frame.payload) {
                            Ok(body) => {
                                if body == "__SIGNAL__:QUIT" {
                                    tab.pending_conn = None;
                                    tab.session.pending_peer_addr = None;
                                    tab.session.pending_peer_dest_b64 = None;

                                    tab.session.current_peer_addr = None;
                                    tab.session.current_peer_dest_b64 = None;
                                    tab.session.peer_b32 = None;

                                    tab.session.live_ready = false;
                                    tab.session.offline_mode = false;
                                    tab.session.network_status = NetworkStatus::LocalOk;
                                    tab.session.pq_active = false;
                                    tab.session.tofu_verified = false;
                                    tab.session.tofu_mismatch = false;

                                    tab.session.call_blink_on = true;
                                    tab.session.call_blink_ticks = 0;

                                    push_log(tab, "Incoming caller disconnected.".to_string());

                                    tab.session.accept_armed = true;
                                    push_log(tab, "Incoming accept loop re-armed.".to_string());

                                    let sam = tab.sam.clone();
                                    tasks.push(Task::perform(
                                        async move {
                                            sam.stream_accept().await.map_err(|e| e.to_string())
                                        },
                                        move |result| Message::IncomingAccepted(tab_id, result),
                                    ));
                                    break;
                                }
                            }
                            Err(_) => {
                                push_log(
                                    tab,
                                    "Received invalid UTF-8 pending signal payload.".to_string(),
                                );
                            }
                        },
                        MsgType::K => {
                            tab.e2e.receive_peer_key(&frame.payload);

                            if tab.e2e.ready() {
                                push_log(tab, "Pending secure session key received.".to_string());
                            }
                        }
                        MsgType::Q | MsgType::Y | MsgType::P | MsgType::O | MsgType::D => {}
                        MsgType::U => {}
                        other => {
                            push_log(tab, format!("Ignoring pre-accept frame type: {:?}", other));
                        }
                    }
                }

                if conn.is_closed() && !conn.has_pending_frames() {
                    let had_visible_call = tab.session.pending_peer_addr.is_some();

                    tab.pending_conn = None;
                    tab.session.pending_peer_addr = None;
                    tab.session.pending_peer_dest_b64 = None;

                    tab.session.current_peer_addr = None;
                    tab.session.current_peer_dest_b64 = None;
                    tab.session.peer_b32 = None;

                    tab.session.live_ready = false;
                    tab.session.offline_mode = false;
                    tab.session.network_status = NetworkStatus::LocalOk;
                    tab.session.pq_active = false;
                    tab.session.tofu_verified = false;
                    tab.session.tofu_mismatch = false;

                    tab.session.call_blink_on = true;
                    tab.session.call_blink_ticks = 0;

                    if had_visible_call {
                        push_log(tab, "Incoming caller disconnected.".to_string());
                    }

                    tab.session.accept_armed = true;
                    push_log(tab, "Incoming accept loop re-armed.".to_string());

                    let conn_for_close = conn.clone();
                    tasks.push(Task::perform(
                        async move { conn_for_close.close().await.map_err(|e| e.to_string()) },
                        move |result| Message::CloseFinished(tab_id, result),
                    ));

                    let sam = tab.sam.clone();
                    tasks.push(Task::perform(
                        async move { sam.stream_accept().await.map_err(|e| e.to_string()) },
                        move |result| Message::IncomingAccepted(tab_id, result),
                    ));
                }
            } else if tab.session.network_status != NetworkStatus::Initializing
                && !tab.session.accept_armed
            {
                tab.session.accept_armed = true;
                push_log(tab, "Incoming accept loop re-armed.".to_string());

                let sam = tab.sam.clone();
                tasks.push(Task::perform(
                    async move { sam.stream_accept().await.map_err(|e| e.to_string()) },
                    move |result| Message::IncomingAccepted(tab_id, result),
                ));
            }

            if tab.live_conn.is_some()
                && tab.outgoing_phase != OutgoingFilePhase::Idle
                && !tab.outgoing_send_in_flight
            {
                let Some(conn) = tab.live_conn.clone() else {
                    Self::update_outgoing_file_bubble(
                        tab,
                        tab.outgoing_sent,
                        "Send failed: no live connection".into(),
                        false,
                        true,
                    );
                    Self::clear_outgoing_file_state(tab);
                    tab.meta.connected = tab.session.live_ready;
                    tab.meta.has_incoming = tab.session.pending_peer_addr.is_some();
                    return tasks;
                };

                match tab.outgoing_phase {
                    OutgoingFilePhase::Idle => {}

                    OutgoingFilePhase::Header => {
                        let filename = tab
                            .outgoing_filename
                            .clone()
                            .unwrap_or_else(|| "file.bin".into());
                        let total = tab.outgoing_total;
                        let e2e = tab.e2e.clone();

                        let frame_f = Frame {
                            msg_type: MsgType::F,
                            msg_id: 0,
                            payload: e2e.encrypt(format!("{filename}|{total}").as_bytes()),
                        };

                        tab.outgoing_send_in_flight = true;

                        tasks.push(Task::perform(
                            async move { conn.send_frame(&frame_f).await.map_err(|e| e.to_string()) },
                            move |result| Message::OutgoingFileHeaderSent(tab_id, result),
                        ));
                    }

                    OutgoingFilePhase::Chunks => {
                        let mut buf = [0u8; 4096];

                        let read_n = match tab.outgoing_file.as_mut() {
                            Some(file) => match file.read(&mut buf) {
                                Ok(n) => n,
                                Err(err) => {
                                    Self::update_outgoing_file_bubble(
                                        tab,
                                        tab.outgoing_sent,
                                        format!("Send failed: {err}"),
                                        false,
                                        true,
                                    );
                                    Self::clear_outgoing_file_state(tab);
                                    0
                                }
                            },
                            None => 0,
                        };

                        if read_n > 0 {
                            let encoded = general_purpose::STANDARD.encode(&buf[..read_n]);
                            let e2e = tab.e2e.clone();

                            let frame_c = Frame {
                                msg_type: MsgType::C,
                                msg_id: 0,
                                payload: e2e.encrypt(encoded.as_bytes()),
                            };

                            tab.outgoing_send_in_flight = true;

                            tasks.push(Task::perform(
                                async move {
                                    conn.send_frame(&frame_c).await.map_err(|e| e.to_string())?;
                                    Ok(read_n)
                                },
                                move |result| Message::OutgoingFileChunkSent(tab_id, result),
                            ));
                        } else if tab.outgoing_file.is_some() {
                            tab.outgoing_phase = OutgoingFilePhase::End;
                        }
                    }

                    OutgoingFilePhase::End => {
                        let frame_e = Frame {
                            msg_type: MsgType::E,
                            msg_id: 0,
                            payload: Vec::new(),
                        };

                        tab.outgoing_send_in_flight = true;

                        tasks.push(Task::perform(
                            async move { conn.send_frame(&frame_e).await.map_err(|e| e.to_string()) },
                            move |result| Message::OutgoingFileEndSent(tab_id, result),
                        ));
                    }
                }
            }

            if tab.live_conn.is_some()
                && tab.outgoing_image_phase != OutgoingImagePhase::Idle
                && !tab.outgoing_image_send_in_flight
            {
                let Some(conn) = tab.live_conn.clone() else {
                    tab.session
                        .log_lines
                        .push("Image send failed: no live connection".into());
                    Self::clear_outgoing_image_state(tab);
                    tab.meta.connected = tab.session.live_ready;
                    tab.meta.has_incoming = tab.session.pending_peer_addr.is_some();
                    return tasks;
                };

                match tab.outgoing_image_phase {
                    OutgoingImagePhase::Idle => {}

                    OutgoingImagePhase::Header => {
                        let filename = tab
                            .outgoing_image_name
                            .clone()
                            .unwrap_or_else(|| "image".into());
                        let mime = tab
                            .outgoing_image_mime
                            .clone()
                            .unwrap_or_else(|| "application/octet-stream".into());
                        let total = tab.outgoing_image_total;
                        let msg_id = tab.outgoing_image_msg_id;
                        let e2e = tab.e2e.clone();

                        let frame_j = Frame {
                            msg_type: MsgType::J,
                            msg_id,
                            payload: e2e.encrypt(format!("{filename}|{mime}|{total}").as_bytes()),
                        };

                        tab.outgoing_image_send_in_flight = true;

                        tasks.push(Task::perform(
                            async move { conn.send_frame(&frame_j).await.map_err(|e| e.to_string()) },
                            move |result| Message::OutgoingImageHeaderSent(tab_id, result),
                        ));
                    }

                    OutgoingImagePhase::Chunks => {
                        let start = tab.outgoing_image_sent as usize;
                        let total = tab.outgoing_image_bytes.len();

                        if start < total {
                            let end = (start + 4096).min(total);
                            let chunk = tab.outgoing_image_bytes[start..end].to_vec();
                            let sent_now = chunk.len();
                            let encoded = general_purpose::STANDARD.encode(&chunk);
                            let msg_id = tab.outgoing_image_msg_id;
                            let e2e = tab.e2e.clone();

                            let frame_g = Frame {
                                msg_type: MsgType::G,
                                msg_id,
                                payload: e2e.encrypt(encoded.as_bytes()),
                            };

                            tab.outgoing_image_send_in_flight = true;

                            tasks.push(Task::perform(
                                async move {
                                    conn.send_frame(&frame_g).await.map_err(|e| e.to_string())?;
                                    Ok(sent_now)
                                },
                                move |result| Message::OutgoingImageChunkSent(tab_id, result),
                            ));
                        } else {
                            tab.outgoing_image_phase = OutgoingImagePhase::End;
                        }
                    }

                    OutgoingImagePhase::End => {
                        let frame_z = Frame {
                            msg_type: MsgType::Z,
                            msg_id: tab.outgoing_image_msg_id,
                            payload: Vec::new(),
                        };

                        tab.outgoing_image_send_in_flight = true;

                        tasks.push(Task::perform(
                            async move { conn.send_frame(&frame_z).await.map_err(|e| e.to_string()) },
                            move |result| Message::OutgoingImageEndSent(tab_id, result),
                        ));
                    }
                }
            }

            tab.meta.connected = tab.session.live_ready;
            tab.meta.has_incoming = tab.session.pending_peer_addr.is_some();
        }

        if secure_session_just_established {
            if let Some(tab_id) = secure_session_tab_id {
                let is_persistent_tab = self
                    .opened_tabs
                    .iter()
                    .find(|t| t.id == tab_id)
                    .map(|t| t.session.profile != "default")
                    .unwrap_or(false);

                tasks.push(self.send_offline_secret_if_needed_task(tab_id));

                if is_persistent_tab {
                    tasks.push(self.send_deaddrop_server_list_task(tab_id));
                }

                if self.active_tab().map(|t| t.id) == Some(tab_id) {
                    self.load_active_runtime();
                }
            }
        }

        tasks
    }

    fn action_placeholder(action: GuiAction) -> &'static str {
        match action {
            GuiAction::Connect => "Enter peer b32.i2p address...",
            GuiAction::Lock => "Enter peer b32.i2p address to lock...",
            _ => "",
        }
    }

    fn push_outgoing_file_bubble(&mut self, filename: String, total_bytes: u64) {
        let bubble = Bubble {
            author: "Me".into(),
            content: BubbleContent::File(FileBubbleData {
                filename,
                saved_path: None,
                total_bytes,
                done_bytes: 0,
                outgoing: true,
                complete: false,
                failed: false,
                status: "Sending...".into(),
            }),
            mine: true,
            offline: false,
            timestamp_utc: Self::now_utc_hms(),
            msg_id: None,
            delivered: false,
        };

        let idx = self.session.bubbles.len();
        self.session.bubbles.push(bubble);

        if let Some(tab) = self.active_tab_mut() {
            tab.outgoing_bubble_index = Some(idx);
        }
    }

    fn update_outgoing_file_bubble(
        tab: &mut OpenedTab,
        done_bytes: u64,
        status: String,
        complete: bool,
        failed: bool,
    ) {
        if let Some(idx) = tab.outgoing_bubble_index {
            if let Some(bubble) = tab.session.bubbles.get_mut(idx) {
                if let BubbleContent::File(file) = &mut bubble.content {
                    file.done_bytes = done_bytes;
                    file.status = status;
                    file.complete = complete;
                    file.failed = failed;
                }
            }
        }
    }

    fn clear_outgoing_file_state(tab: &mut OpenedTab) {
        tab.outgoing_file = None;
        tab.outgoing_filename = None;
        tab.outgoing_total = 0;
        tab.outgoing_sent = 0;
        tab.outgoing_phase = OutgoingFilePhase::Idle;
        tab.outgoing_send_in_flight = false;
        tab.outgoing_bubble_index = None;
    }

    fn clear_outgoing_image_state(tab: &mut OpenedTab) {
        tab.outgoing_image_name = None;
        tab.outgoing_image_mime = None;
        tab.outgoing_image_bytes.clear();
        tab.outgoing_image_total = 0;
        tab.outgoing_image_sent = 0;
        tab.outgoing_image_msg_id = 0;
        tab.outgoing_image_phase = OutgoingImagePhase::Idle;
        tab.outgoing_image_send_in_flight = false;
    }

    fn clear_incoming_image_state(tab: &mut OpenedTab) {
        tab.incoming_image_name = None;
        tab.incoming_image_mime = None;
        tab.incoming_image_expected = 0;
        tab.incoming_image_received = 0;
        tab.incoming_image_msg_id = 0;
        tab.incoming_image_bytes.clear();
    }

    fn image_mime_for_path(path: &Path) -> Option<&'static str> {
        let ext = path
            .extension()
            .and_then(|s| s.to_str())?
            .to_ascii_lowercase();

        match ext.as_str() {
            "png" => Some("image/png"),
            "jpg" | "jpeg" => Some("image/jpeg"),
            "gif" => Some("image/gif"),
            "bmp" => Some("image/bmp"),
            "webp" => Some("image/webp"),
            _ => None,
        }
    }

    fn is_supported_image_mime(mime: &str) -> bool {
        matches!(
            mime,
            "image/png" | "image/jpeg" | "image/gif" | "image/bmp" | "image/webp"
        )
    }

    fn prepare_image_preview_bytes(path: &Path) -> Result<(Vec<u8>, String), String> {
        let source = std::fs::read(path).map_err(|e| format!("Image read failed: {e}"))?;

        if source.is_empty() {
            return Err("Image is empty.".into());
        }

        if source.len() > MAX_FILE_SIZE {
            return Err(format!(
                "Image source is too large for inline preview ({} bytes). Use Send File for the original.",
                source.len()
            ));
        }

        let decoded =
            ::image::load_from_memory(&source).map_err(|e| format!("Image decode failed: {e}"))?;
        let keep_alpha = decoded.has_alpha();
        let preview = if decoded.width() > IMAGE_TRANSFER_MAX_DIMENSION
            || decoded.height() > IMAGE_TRANSFER_MAX_DIMENSION
        {
            decoded.resize(
                IMAGE_TRANSFER_MAX_DIMENSION,
                IMAGE_TRANSFER_MAX_DIMENSION,
                ::image::imageops::FilterType::Lanczos3,
            )
        } else {
            decoded
        };

        if keep_alpha {
            let mut cursor = Cursor::new(Vec::new());
            preview
                .write_to(&mut cursor, ::image::ImageFormat::Png)
                .map_err(|e| format!("Image preview encode failed: {e}"))?;
            Ok((cursor.into_inner(), "image/png".into()))
        } else {
            let rgb = preview.to_rgb8();
            let mut out = Vec::new();
            let mut encoder = ::image::codecs::jpeg::JpegEncoder::new_with_quality(
                &mut out,
                IMAGE_TRANSFER_JPEG_QUALITY,
            );
            encoder
                .encode(
                    &rgb,
                    rgb.width(),
                    rgb.height(),
                    ::image::ExtendedColorType::Rgb8,
                )
                .map_err(|e| format!("Image preview encode failed: {e}"))?;
            Ok((out, "image/jpeg".into()))
        }
    }

    fn image_bubble_data(bytes: Vec<u8>) -> ImageBubbleData {
        let (width, height) = ::image::load_from_memory(&bytes)
            .map(|img| (img.width(), img.height()))
            .unwrap_or((300, 220));
        let handle = iced::widget::image::Handle::from_bytes(bytes.clone());
        ImageBubbleData {
            bytes,
            handle,
            width,
            height,
        }
    }

    fn push_incoming_file_bubble(
        tab: &mut OpenedTab,
        filename: String,
        saved_path: String,
        total_bytes: u64,
    ) {
        let bubble = Bubble {
            author: "Peer".into(),
            content: BubbleContent::File(FileBubbleData {
                filename,
                saved_path: Some(saved_path),
                total_bytes,
                done_bytes: 0,
                outgoing: false,
                complete: false,
                failed: false,
                status: "Receiving...".into(),
            }),
            mine: false,
            offline: false,
            timestamp_utc: Self::now_utc_hms(),
            msg_id: None,
            delivered: false,
        };

        let idx = tab.session.bubbles.len();
        tab.session.bubbles.push(bubble);
        tab.incoming_bubble_index = Some(idx);
    }

    fn files_dir() -> PathBuf {
        storage::base_dir().join("files")
    }

    fn ensure_files_dir() -> Result<PathBuf, String> {
        let dir = storage::files_dir();
        storage::create_dir_secure_all(&dir).map_err(|e| e.to_string())?;
        Ok(dir)
    }

    fn mark_delivered(tab: &mut OpenedTab, delivered_id: u64) {
        for bubble in tab.session.bubbles.iter_mut().rev() {
            if bubble.mine && bubble.msg_id == Some(delivered_id) {
                bubble.delivered = true;
                break;
            }
        }
    }

    fn left_status_indicators<'a>(session: &'a SessionState) -> iced::widget::Row<'a, Message> {
        let mut row_acc = row![
            mode_indicator(&session.profile),
            profile_indicator(&session.profile),
            indicator(
                if session.stored_peer.is_some() {
                    "LOCK"
                } else {
                    "UNLOCK"
                },
                if session.stored_peer.is_some() {
                    PY_GREEN
                } else {
                    PY_RED
                }
            ),
        ]
        .spacing(6);

        if !session.offline_mode && (session.live_ready || session.pending_peer_addr.is_some()) {
            if session.tofu_mismatch {
                row_acc = row_acc.push(indicator("TOFU", PY_RED));
            } else if session.tofu_verified {
                row_acc = row_acc.push(indicator("TOFU", PY_GREEN));
            }
        }

        if session.pq_active {
            row_acc = row_acc.push(indicator("PQ", PY_MAGENTA));
        }

        if session.offline_mode {
            row_acc = row_acc.push(indicator("OFF", PY_YELLOW));

            let dd_status = visible_dd_status(session);
            let dd_label = dd_status_text(dd_status);
            if !dd_label.is_empty() && dd_label != "DD" {
                row_acc = row_acc.push(indicator(dd_label, dd_status_color(dd_status)));
            }
        }

        if session.pending_peer_addr.is_some() {
            row_acc = row_acc.push(if session.call_blink_on {
                wide_indicator("INCOMING", PY_CYAN)
            } else {
                wide_indicator("CALL", PY_GREY62)
            });
        }

        row_acc
    }

    fn has_active_connection_attempt(&self) -> bool {
        self.active_live_conn().is_some() || self.active_pending_conn().is_some()
    }

    fn new_app_home_tab() -> ChatTab {
        ChatTab {
            kind: TabKind::AppHome,
            title: "GLOBAL".into(),
            profile_name: "__app__".into(),
            has_unread: false,
            has_incoming: false,
            connected: false,
            initializing: false,
            initialized: true,
        }
    }

    fn active_tab_is_app_home(&self) -> bool {
        match self.session.active_tab_idx {
            Some(idx) => self
                .session
                .tabs
                .get(idx)
                .map(|t| t.kind == TabKind::AppHome)
                .unwrap_or(false),
            None => true,
        }
    }

    fn is_profile_open_in_any_tab(&self, profile_name: &str) -> bool {
        self.opened_tabs
            .iter()
            .any(|tab| tab.meta.profile_name == profile_name)
    }

    fn offline_state_peer_b32_for_session(session: &SessionState) -> Option<&str> {
        session
            .stored_peer
            .as_deref()
            .or(session.current_peer_addr.as_deref())
    }

    fn apply_offline_state_to_session(session: &mut SessionState, offline: &OfflineState) {
        let has_real_secret = offline.offline_shared_secret.iter().any(|b| *b != 0);

        session.offline_shared_secret = if has_real_secret {
            Some(offline.offline_shared_secret)
        } else {
            None
        };

        session.drop_send_index = offline.drop_send_index;
        session.drop_recv_base = offline.drop_recv_base;
        session.drop_window = offline.drop_window;
        session.consumed_drop_recv = offline.consumed_drop_recv.clone();
    }

    fn offline_state_from_session(session: &SessionState) -> OfflineState {
        OfflineState {
            offline_shared_secret: session.offline_shared_secret.unwrap_or([0u8; 32]),
            drop_send_index: session.drop_send_index,
            drop_recv_base: session.drop_recv_base,
            drop_window: session.drop_window,
            consumed_drop_recv: session.consumed_drop_recv.clone(),
        }
    }

    fn has_real_offline_secret(&self) -> bool {
        self.session
            .offline_shared_secret
            .map(|s| s.iter().any(|b| *b != 0))
            .unwrap_or(false)
    }

    fn should_initiate_offline_secret_for_session(session: &SessionState) -> bool {
        let Some(my_b32) = session.my_b32.as_ref() else {
            return false;
        };

        let Some(peer_b32) = Self::offline_state_peer_b32_for_session(session) else {
            return false;
        };

        let my_id = my_b32.replace(".b32.i2p", "").trim().to_lowercase();

        let peer_id = peer_b32.replace(".b32.i2p", "").trim().to_lowercase();

        my_id < peer_id
    }

    fn is_persistent_mode(&self) -> bool {
        self.session.profile != "default"
    }

    fn has_locked_peer(&self) -> bool {
        self.session.stored_peer.is_some() && self.session.stored_peer_dest_b64.is_some()
    }

    fn offline_ready(&self) -> bool {
        self.is_persistent_mode()
            && self.has_locked_peer()
            && !self.session.deaddrop_servers.is_empty()
    }

    fn send_offline_secret_if_needed_task(&mut self, tab_id: u64) -> Task<Message> {
        let Some(idx) = self.find_tab_index_by_id(tab_id) else {
            return Task::none();
        };

        let Some(conn) = self.opened_tabs[idx].live_conn.clone() else {
            return Task::none();
        };

        if !self.opened_tabs[idx].session.live_ready {
            return Task::none();
        }

        if !(self.opened_tabs[idx].session.profile != "default"
            && self.opened_tabs[idx].session.stored_peer.is_some()
            && self.opened_tabs[idx].session.stored_peer_dest_b64.is_some()
            && !self.opened_tabs[idx].session.deaddrop_servers.is_empty())
        {
            return Task::none();
        }

        let already_has_real = self.opened_tabs[idx]
            .session
            .offline_shared_secret
            .map(|s| s.iter().any(|b| *b != 0))
            .unwrap_or(false);

        if already_has_real {
            return Task::none();
        }

        if !Self::should_initiate_offline_secret_for_session(&self.opened_tabs[idx].session) {
            return Task::none();
        }

        let secret: [u8; 32] = random();

        self.opened_tabs[idx].session.offline_shared_secret = Some(secret);

        if self.active_tab().map(|t| t.id) == Some(tab_id) {
            self.session.offline_shared_secret = Some(secret);
        }

        if let Some(peer_b32) = self.opened_tabs[idx].session.stored_peer.clone() {
            let offline = Self::offline_state_from_session(&self.opened_tabs[idx].session);

            match storage::save_offline_state(
                &self.opened_tabs[idx].session.profile,
                &peer_b32,
                &offline,
            ) {
                Ok(()) => {
                    self.opened_tabs[idx]
                        .session
                        .log_lines
                        .push("Offline secret generated and saved.".into());
                }
                Err(err) => {
                    self.opened_tabs[idx]
                        .session
                        .log_lines
                        .push(format!("Offline secret save failed: {err}"));
                    return Task::none();
                }
            }
        } else {
            return Task::none();
        }

        let frame = Frame {
            msg_type: MsgType::X,
            msg_id: self.generate_msg_id(),
            payload: secret.to_vec(),
        };

        Task::perform(
            async move { conn.send_frame(&frame).await.map_err(|e| e.to_string()) },
            move |result| Message::SendFinished(tab_id, result),
        )
    }

    fn is_valid_deaddrop_server(server: &str) -> bool {
        let server = server.trim().to_lowercase();

        if server.is_empty() {
            return false;
        }

        if !server.ends_with(".b32.i2p") {
            return false;
        }

        let host = &server[..server.len() - 8];

        if !matches!(host.len(), 52 | 56) {
            return false;
        }

        host.chars().all(|ch| matches!(ch, 'a'..='z' | '2'..='7'))
    }

    fn active_deaddrop_replicas(servers: &[String]) -> Vec<String> {
        servers
            .iter()
            .take(MAX_ACTIVE_DEADDROP_REPLICAS)
            .cloned()
            .collect()
    }

    fn ensure_deaddrop_stat_entries(session: &mut SessionState) {
        let servers = session.deaddrop_servers.clone();

        for server in servers {
            if Self::is_valid_deaddrop_server(&server) {
                session
                    .deaddrop_stats
                    .entry(server)
                    .or_insert_with(storage::DeaddropServerStat::default);
            }
        }

        session
            .deaddrop_stats
            .retain(|server, _| Self::is_valid_deaddrop_server(server));
    }

    fn deaddrop_server_score(session: &SessionState, server: &str) -> f64 {
        Self::deaddrop_server_score_from_stats(&session.deaddrop_stats, server)
    }

    fn deaddrop_server_score_from_stats(
        stats: &HashMap<String, storage::DeaddropServerStat>,
        server: &str,
    ) -> f64 {
        let Some(stat) = stats.get(server) else {
            return DD_UNKNOWN_SERVER_SCORE;
        };

        let put_ok = stat.put_ok as f64;
        let put_fail = stat.put_fail as f64;
        let get_ok = stat.get_ok as f64;
        let get_fail = stat.get_fail as f64;
        let total_ok = put_ok + get_ok;
        let total_fail = put_fail + get_fail;
        let total = total_ok + total_fail;

        if total <= 0.0 {
            return DD_UNKNOWN_SERVER_SCORE;
        }

        let success_ratio = total_ok / total;
        let latency_component = if stat.latency_ema_ms > 0.0 {
            -stat.latency_ema_ms
        } else {
            0.0
        };
        let failure_penalty = total_fail * DD_FAILURE_PENALTY;
        let recency_bonus = if stat.last_success_ts > 0.0 {
            stat.last_success_ts / 1_000_000.0
        } else {
            0.0
        };

        (success_ratio * 100_000.0) + latency_component + recency_bonus - failure_penalty
    }

    fn rank_deaddrop_servers(session: &mut SessionState) {
        if session.deaddrop_servers.is_empty() {
            return;
        }

        Self::ensure_deaddrop_stat_entries(session);

        let original_order: HashMap<String, usize> = session
            .deaddrop_servers
            .iter()
            .enumerate()
            .map(|(idx, server)| (server.clone(), idx))
            .collect();
        let stats_snapshot = session.deaddrop_stats.clone();

        session.deaddrop_servers.sort_by(|a, b| {
            let score_a = Self::deaddrop_server_score_from_stats(&stats_snapshot, a);
            let score_b = Self::deaddrop_server_score_from_stats(&stats_snapshot, b);

            score_b
                .partial_cmp(&score_a)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    original_order
                        .get(a)
                        .copied()
                        .unwrap_or(usize::MAX)
                        .cmp(&original_order.get(b).copied().unwrap_or(usize::MAX))
                })
        });
    }

    fn record_deaddrop_stats_for_tab(tab: &mut OpenedTab, op_stats: &[DeaddropOpStat]) {
        if tab.session.profile == "default" {
            return;
        }

        let mut changed = false;

        for op_stat in op_stats {
            let server = op_stat.drop.trim().to_lowercase();
            if !Self::is_valid_deaddrop_server(&server) {
                continue;
            }

            let stat = tab
                .session
                .deaddrop_stats
                .entry(server)
                .or_insert_with(storage::DeaddropServerStat::default);

            match op_stat.op {
                "put" if op_stat.ok => stat.put_ok += 1,
                "put" => stat.put_fail += 1,
                "get" if op_stat.ok => stat.get_ok += 1,
                "get" => stat.get_fail += 1,
                _ => continue,
            }

            if op_stat.ok {
                stat.last_success_ts = Self::now_epoch_millis() as f64 / 1000.0;
            }

            if op_stat.latency_ms > 0.0 {
                if stat.latency_samples == 0 || stat.latency_ema_ms <= 0.0 {
                    stat.latency_ema_ms = op_stat.latency_ms;
                } else {
                    stat.latency_ema_ms = (DD_STATS_EMA_ALPHA * op_stat.latency_ms)
                        + ((1.0 - DD_STATS_EMA_ALPHA) * stat.latency_ema_ms);
                }
                stat.latency_samples += 1;
            }

            changed = true;
        }

        if changed {
            Self::rank_deaddrop_servers(&mut tab.session);
            Self::sync_tab_deaddrop_servers(tab);
            let meta = ContactMeta {
                name: tab.session.profile.clone(),
                my_dest_b64: tab.session.my_dest_b64.clone(),
                locked_peer: tab.session.stored_peer.clone(),
                locked_peer_dest_b64: tab.session.stored_peer_dest_b64.clone(),
                pq_enabled: tab.session.pq_enabled,
                deaddrop_servers: tab.session.deaddrop_servers.clone(),
            };

            if let Err(err) = storage::save_contact_meta(&meta) {
                tab.session
                    .log_lines
                    .push(format!("Save ranked deaddrop server order failed: {err}"));
            }
            tab.session.deaddrop_stats_dirty = true;
        }
    }

    fn flush_deaddrop_stats_for_tab(tab: &mut OpenedTab, force: bool) {
        if tab.session.profile == "default" {
            return;
        }

        if !tab.session.deaddrop_stats_dirty && !force {
            return;
        }

        let now = Self::now_epoch_millis();
        if !force
            && tab.session.deaddrop_stats_last_save_ms != 0
            && now.saturating_sub(tab.session.deaddrop_stats_last_save_ms)
                < DD_STATS_SAVE_INTERVAL_MS
        {
            return;
        }

        match storage::save_deaddrop_stats(&tab.session.profile, &tab.session.deaddrop_stats) {
            Ok(()) => {
                tab.session.deaddrop_stats_dirty = false;
                tab.session.deaddrop_stats_last_save_ms = now;
            }
            Err(err) => {
                tab.session
                    .log_lines
                    .push(format!("Save deaddrop stats failed: {err}"));
            }
        }
    }

    fn sync_active_deaddrop_servers(&mut self) {
        let drops = Self::active_deaddrop_replicas(&self.session.deaddrop_servers);

        if let Some(tab) = self.active_tab_mut() {
            if let Ok(mut deaddrop) = tab.deaddrop.try_lock() {
                deaddrop.drops = drops;
            }
        }
    }

    fn merge_deaddrop_servers_into_session(
        session: &mut SessionState,
        new_servers: &[String],
    ) -> bool {
        let mut changed = false;

        for s in new_servers {
            let s = s.trim().to_lowercase();

            if !Self::is_valid_deaddrop_server(&s) {
                continue;
            }

            if !session
                .deaddrop_servers
                .iter()
                .any(|existing| existing == &s)
            {
                session.deaddrop_servers.push(s.clone());
                session
                    .deaddrop_stats
                    .entry(s)
                    .or_insert_with(storage::DeaddropServerStat::default);
                changed = true;
            }
        }

        if changed {
            Self::rank_deaddrop_servers(session);
        }

        changed
    }

    fn sync_tab_deaddrop_servers(tab: &mut OpenedTab) {
        let drops = Self::active_deaddrop_replicas(&tab.session.deaddrop_servers);

        if let Ok(mut deaddrop) = tab.deaddrop.try_lock() {
            deaddrop.drops = drops;
        }
    }

    fn send_deaddrop_server_list_task(&self, tab_id: u64) -> Task<Message> {
        let Some(tab) = self.opened_tabs.iter().find(|t| t.id == tab_id) else {
            return Task::none();
        };

        let Some(conn) = tab.live_conn.clone() else {
            return Task::none();
        };

        let payload = tab.session.deaddrop_servers.join("\n").into_bytes();

        let frame = Frame {
            msg_type: MsgType::L,
            msg_id: self.generate_msg_id(),
            payload,
        };

        Task::perform(
            async move { conn.send_frame(&frame).await.map_err(|e| e.to_string()) },
            move |result| Message::SendFinished(tab_id, result),
        )
    }

    fn offline_directional_key(
        shared_secret: &[u8; 32],
        my_b32: &str,
        peer_b32: &str,
        direction: &str,
        index: u64,
    ) -> String {
        let my_id = my_b32
            .trim()
            .to_lowercase()
            .trim_end_matches(".b32.i2p")
            .to_string();

        let peer_id = peer_b32
            .trim()
            .to_lowercase()
            .trim_end_matches(".b32.i2p")
            .to_string();

        let (low_id, high_id) = if my_id <= peer_id {
            (my_id.as_str(), peer_id.as_str())
        } else {
            (peer_id.as_str(), my_id.as_str())
        };

        let (send_label, recv_label) = if my_id == low_id {
            ("LOW_TO_HIGH", "HIGH_TO_LOW")
        } else {
            ("HIGH_TO_LOW", "LOW_TO_HIGH")
        };

        let dir_label = match direction {
            "send" => send_label,
            "recv" => recv_label,
            _ => return String::new(),
        };

        let mut material = Vec::new();
        material.extend_from_slice(shared_secret);
        material.push(b'|');
        material.extend_from_slice(low_id.as_bytes());
        material.push(b'|');
        material.extend_from_slice(high_id.as_bytes());
        material.push(b'|');
        material.extend_from_slice(dir_label.as_bytes());
        material.push(b'|');
        material.extend_from_slice(index.to_string().as_bytes());

        hex::encode(sha2::Sha256::digest(&material))
    }

    fn build_offline_blob_for_frame(
        e2e: &E2E,
        frame: &Frame,
        shared_secret: &[u8; 32],
        my_b32: &str,
        peer_b32: &str,
    ) -> Result<Vec<u8>, String> {
        let encoded = frame.encode().map_err(|e| e.to_string())?;
        let blob_key = e2e.derive_offline_blob_key(shared_secret, my_b32, peer_b32);
        Ok(e2e.encrypt_offline_blob(&encoded, &blob_key))
    }

    fn can_send_offline_now(&self) -> bool {
        self.session.offline_mode
            && self.offline_ready()
            && self.has_real_offline_secret()
            && self.session.my_b32.is_some()
            && self.session.stored_peer.is_some()
    }

    fn offline_runtime_ready_for_tab(tab: &OpenedTab) -> bool {
        tab.session.profile != "default"
            && tab.session.stored_peer.is_some()
            && tab.session.stored_peer_dest_b64.is_some()
            && !tab.session.deaddrop_servers.is_empty()
            && tab
                .session
                .offline_shared_secret
                .map(|s| s.iter().any(|b| *b != 0))
                .unwrap_or(false)
    }

    fn ensure_deaddrop_runtime_started(&mut self, tab_id: u64) -> Task<Message> {
        let Some(tab) = self.opened_tabs.iter_mut().find(|t| t.id == tab_id) else {
            return Task::none();
        };

        if tab.deaddrop_started {
            return Task::none();
        }

        if !Self::offline_runtime_ready_for_tab(tab) {
            return Task::none();
        }

        let dd = std::sync::Arc::clone(&tab.deaddrop);
        let drops = tab.session.deaddrop_servers.clone();

        Task::perform(
            async move {
                let mut dd = dd.lock().await;
                dd.drops = drops;
                dd.start().await
            },
            move |result| Message::DeaddropStarted(tab_id, result),
        )
    }

    fn get_deaddrop_recv_window(
        session: &SessionState,
        my_b32: &str,
        peer_b32: &str,
    ) -> Vec<(u64, String)> {
        let mut out = Vec::new();

        let window = session.drop_window as u64;
        for recv_index in session.drop_recv_base..(session.drop_recv_base + window) {
            if session.consumed_drop_recv.iter().any(|n| *n == recv_index) {
                continue;
            }

            let dd_key = Self::offline_directional_key(
                &session.offline_shared_secret.unwrap_or([0u8; 32]),
                my_b32,
                peer_b32,
                "recv",
                recv_index,
            );

            out.push((recv_index, dd_key));
        }

        out
    }

    fn advance_drop_recv_base(session: &mut SessionState) {
        loop {
            if session
                .consumed_drop_recv
                .iter()
                .any(|n| *n == session.drop_recv_base)
            {
                session.drop_recv_base += 1;
            } else {
                break;
            }
        }

        session.consumed_drop_recv.sort_unstable();
        session.consumed_drop_recv.dedup();

        session
            .consumed_drop_recv
            .retain(|n| *n >= session.drop_recv_base);
    }

    fn start_next_deaddrop_poll_key_task(&mut self, tab_id: u64) -> Task<Message> {
        let Some(idx) = self.find_tab_index_by_id(tab_id) else {
            return Task::none();
        };

        if !self.opened_tabs[idx].deaddrop_started
            || !self.opened_tabs[idx].session.offline_mode
            || self.opened_tabs[idx].deaddrop_put_in_flight
        {
            self.opened_tabs[idx].deaddrop_poll_in_flight = false;
            self.opened_tabs[idx].deaddrop_poll_queue.clear();
            self.opened_tabs[idx].deaddrop_last_poll_ms = Self::now_epoch_millis();
            return Task::none();
        }

        let Some((recv_index, dd_key)) = self.opened_tabs[idx].deaddrop_poll_queue.first().cloned()
        else {
            self.opened_tabs[idx].deaddrop_poll_in_flight = false;
            self.opened_tabs[idx].deaddrop_last_poll_ms = Self::now_epoch_millis();
            return Task::none();
        };

        self.opened_tabs[idx].deaddrop_poll_queue.remove(0);

        let dd = std::sync::Arc::clone(&self.opened_tabs[idx].deaddrop);
        let dd_key_for_task = dd_key.clone();

        Task::perform(
            async move {
                let mut dd = dd.lock().await;
                dd.get_with_stats(&dd_key_for_task).await
            },
            move |(blobs, stats)| {
                Message::OfflinePollKeyFinished(tab_id, recv_index, dd_key, blobs, stats)
            },
        )
    }

    fn handle_offline_poll_key_result(
        tab: &mut OpenedTab,
        recv_index: u64,
        blobs: Vec<(String, Vec<u8>)>,
    ) {
        if blobs.is_empty() {
            Self::set_dd_status(&mut tab.session, "get_miss");
            return;
        }

        let Some(shared_secret) = tab.session.offline_shared_secret else {
            Self::set_dd_status(&mut tab.session, "get_miss");
            return;
        };

        let Some(my_b32) = tab.session.my_b32.clone() else {
            Self::set_dd_status(&mut tab.session, "get_miss");
            return;
        };

        let Some(peer_b32) = tab.session.stored_peer.clone() else {
            Self::set_dd_status(&mut tab.session, "get_miss");
            return;
        };

        let blob_key = tab
            .e2e
            .derive_offline_blob_key(&shared_secret, &my_b32, &peer_b32);

        let mut got_valid_blob = false;

        for (drop, blob) in blobs {
            let blob_hash = hex::encode(sha2::Sha256::digest(&blob));

            if tab.session.seen_drop_msgs.iter().any(|h| h == &blob_hash) {
                continue;
            }

            let frame_bytes = tab.e2e.decrypt_offline_blob(&blob, &blob_key);

            match Frame::decode(&frame_bytes) {
                Ok(frame) => {
                    tab.session.seen_drop_msgs.push(blob_hash);
                    got_valid_blob = true;

                    match frame.msg_type {
                        MsgType::U => {
                            let plain = tab.e2e.decrypt(&frame.payload);
                            match String::from_utf8(plain) {
                                Ok(text) => {
                                    tab.session.bubbles.push(Bubble::peer_offline(text));
                                    tab.meta.has_unread = false;
                                    Self::set_dd_status(&mut tab.session, "get_hit");
                                    tab.session.log_lines.push(format!(
                                        "Offline message received from deaddrop {} at recv index {}.",
                                        drop, recv_index
                                    ));
                                }
                                Err(_) => {
                                    tab.session
                                        .log_lines
                                        .push("Offline message payload is invalid UTF-8.".into());
                                }
                            }
                        }
                        other => {
                            tab.session.log_lines.push(format!(
                                "Ignoring offline frame type {:?} at recv index {}.",
                                other, recv_index
                            ));
                        }
                    }
                }
                Err(err) => {
                    tab.session.log_lines.push(format!(
                        "Offline frame decode failed at recv index {}: {}",
                        recv_index, err
                    ));
                }
            }
        }

        if got_valid_blob {
            if !tab
                .session
                .consumed_drop_recv
                .iter()
                .any(|n| *n == recv_index)
            {
                tab.session.consumed_drop_recv.push(recv_index);
            }

            Self::advance_drop_recv_base(&mut tab.session);

            if let Some(peer_b32) = tab.session.stored_peer.clone() {
                let offline = OfflineState {
                    offline_shared_secret: tab.session.offline_shared_secret.unwrap_or([0u8; 32]),
                    drop_send_index: tab.session.drop_send_index,
                    drop_recv_base: tab.session.drop_recv_base,
                    drop_window: tab.session.drop_window,
                    consumed_drop_recv: tab.session.consumed_drop_recv.clone(),
                };

                if let Err(err) =
                    storage::save_offline_state(&tab.session.profile, &peer_b32, &offline)
                {
                    tab.session
                        .log_lines
                        .push(format!("Failed to save offline receive state: {err}"));
                }
            }
        } else {
            Self::set_dd_status(&mut tab.session, "get_miss");
        }
    }
}

//fffff

fn bubble_timestamp_row<'a>(bubble: &'a Bubble) -> Element<'a, Message> {
    row![
        Space::new().width(Length::Fill),
        text(&bubble.timestamp_utc)
            .size(10)
            .color(Color::from_rgb8(140, 140, 140)),
        if bubble.mine && bubble.delivered {
            let mark = if bubble.offline { " ✓" } else { " ✓✓" };
            container(text(mark).size(11).color(Color::from_rgb8(0, 200, 0))).padding(
                iced::Padding {
                    top: 0.0,
                    right: 0.0,
                    bottom: 1.0,
                    left: 0.0,
                },
            )
        } else {
            container(text(""))
        },
    ]
    .spacing(2)
    .align_y(Alignment::Center)
    .width(Length::Fill)
    .into()
}

fn message_row<'a>(bubble: &'a Bubble) -> Element<'a, Message> {
    let (body, max_width): (Element<'a, Message>, f32) = match &bubble.content {
        BubbleContent::Text(value) => {
            let body_width = text_bubble_body_width(value, bubble.mine && bubble.delivered);

            (
                column![
                    text(value)
                        .size(12)
                        .width(Length::Fill)
                        .wrapping(iced::widget::text::Wrapping::WordOrGlyph),
                    bubble_timestamp_row(bubble),
                ]
                .spacing(6)
                .width(body_width)
                .into(),
                body_width + 24.0,
            )
        }

        BubbleContent::Image(data) => {
            let (display_width, display_height) = image_display_size(data.width, data.height);

            (
                column![
                    image(data.handle.clone())
                        .width(display_width)
                        .height(display_height)
                        .content_fit(ContentFit::Contain),
                    bubble_timestamp_row(bubble),
                ]
                .spacing(6)
                .width(display_width)
                .into(),
                display_width + 24.0,
            )
        }

        BubbleContent::File(file) => {
            let total = if file.total_bytes == 0 {
                1.0
            } else {
                file.total_bytes as f32
            };
            let done = file.done_bytes.min(file.total_bytes) as f32;

            (
                column![
                    text(&file.filename)
                        .size(13)
                        .width(Length::Fill)
                        .wrapping(iced::widget::text::Wrapping::WordOrGlyph),
                    text(format!("{} / {} bytes", file.done_bytes, file.total_bytes))
                        .size(11)
                        .color(Color::from_rgb8(160, 160, 160)),
                    container(progress_bar(0.0..=total, done)).width(Length::Fill),
                    text(&file.status).size(11).color(if file.failed {
                        Color::from_rgb8(190, 80, 80)
                    } else if file.complete {
                        Color::from_rgb8(80, 170, 80)
                    } else {
                        Color::from_rgb8(160, 160, 160)
                    }),
                    if let Some(path) = &file.saved_path {
                        text(path).size(10).color(Color::from_rgb8(130, 130, 130))
                    } else {
                        text("").size(10)
                    },
                    bubble_timestamp_row(bubble),
                ]
                .spacing(6)
                .width(FILE_BUBBLE_WIDTH)
                .into(),
                FILE_BUBBLE_WIDTH + 24.0,
            )
        }

        BubbleContent::System(value) => (
            container(text(value).size(15).width(Length::Fill))
                .width(Length::Fill)
                .into(),
            SYSTEM_BUBBLE_MAX_WIDTH,
        ),
    };

    let mine = bubble.mine;
    let offline = bubble.offline;
    let is_system = matches!(bubble.content, BubbleContent::System(_));

    let bubble_widget = container(body)
        .padding(12)
        .max_width(max_width)
        .style(move |_| {
            if is_system {
                system_bubble_style()
            } else {
                bubble_style(mine, offline)
            }
        });

    if is_system {
        row![
            Space::new().width(Length::Fill),
            bubble_widget,
            Space::new().width(Length::Fill)
        ]
        .width(Length::Fill)
        .into()
    } else if bubble.mine {
        row![
            Space::new().width(12),
            bubble_widget,
            Space::new().width(Length::Fill)
        ]
        .width(Length::Fill)
        .into()
    } else {
        row![
            Space::new().width(Length::Fill),
            bubble_widget,
            Space::new().width(12)
        ]
        .width(Length::Fill)
        .into()
    }
}

fn image_display_size(width: u32, height: u32) -> (f32, f32) {
    let source_width = width.max(1) as f32;
    let source_height = height.max(1) as f32;
    let scale = (IMAGE_BUBBLE_MAX_WIDTH / source_width)
        .min(IMAGE_BUBBLE_MAX_HEIGHT / source_height)
        .min(1.0);

    (source_width * scale, source_height * scale)
}

fn text_bubble_body_width(value: &str, delivered: bool) -> f32 {
    let longest_line_chars = value
        .lines()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0) as f32;

    let estimated_text_width = longest_line_chars * 8.0 + 4.0;
    let footer_width = if delivered {
        TEXT_BUBBLE_MIN_BODY_WIDTH
    } else {
        64.0
    };
    let max_body_width = TEXT_BUBBLE_MAX_WIDTH - 24.0;

    estimated_text_width
        .max(footer_width)
        .clamp(TEXT_BUBBLE_MIN_BODY_WIDTH, max_body_width)
}

fn short_b32(addr: Option<&str>) -> String {
    if let Some(addr) = addr {
        let clean = addr.replace(".b32.i2p", "");
        if clean.len() > 12 {
            format!("{}...{}", &clean[..6], &clean[clean.len() - 6..])
        } else {
            clean
        }
    } else {
        "----".into()
    }
}

fn short_peer_b32(addr: Option<&str>, is_active: bool) -> String {
    if is_active {
        if let Some(addr) = addr {
            let clean = addr.replace(".b32.i2p", "");
            if clean.len() > 12 {
                format!("{}..{}", &clean[..6], &clean[clean.len() - 6..])
            } else {
                clean
            }
        } else {
            "??????".into()
        }
    } else {
        "----".into()
    }
}

fn mode_indicator<'a>(profile: &'a str) -> iced::widget::Container<'a, Message> {
    if profile == "default" {
        bold_indicator("T", PY_GREY62)
    } else {
        bold_indicator("P", PY_GREEN)
    }
}

fn profile_indicator<'a>(profile: &'a str) -> iced::widget::Container<'a, Message> {
    let name = profile.to_uppercase();
    container(text(name).size(13).font(indicator_font()))
        .padding([4, 10])
        .style(|_| indicator_style(Color::from_rgb8(35, 35, 40), Color::WHITE))
}

fn connection_status_text(session: &SessionState) -> &'static str {
    if session.live_ready {
        "CONNECTED"
    } else {
        match session.network_status {
            NetworkStatus::Initializing => "INITIALIZING",
            NetworkStatus::LocalOk => "STANDBY",
            NetworkStatus::Visible => "CONNECTING",
        }
    }
}

fn connection_status_color(session: &SessionState) -> Color {
    if session.live_ready {
        PY_GREEN
    } else {
        match session.network_status {
            NetworkStatus::Initializing => PY_GREY62,
            NetworkStatus::LocalOk => PY_GREY62,
            NetworkStatus::Visible => PY_CYAN,
        }
    }
}

fn dd_status_text(status: &str) -> &'static str {
    match status {
        "idle" => "DD IDLE",
        "poll" => "DD POLL",
        "put_ok" => "DD PUT",
        "put_fail" => "DD FAIL",
        "get_hit" => "DD HIT",
        "get_miss" => "DD MISS",
        "get_fail" => "DD FAIL",
        _ => "DD IDLE",
    }
}

fn dd_status_color(status: &str) -> Color {
    match status {
        "idle" => PY_GREY62,
        "poll" => PY_YELLOW,
        "put_ok" => PY_GREEN,
        "put_fail" => PY_RED,
        "get_hit" => PY_MAGENTA,
        "get_miss" => PY_GREY62,
        "get_fail" => PY_RED,
        _ => PY_GREY62,
    }
}

fn visible_dd_status(session: &SessionState) -> &str {
    if session.dd_status_at_ms == 0 {
        return "idle";
    }

    let age_ms = TermchatApp::now_epoch_millis().saturating_sub(session.dd_status_at_ms);

    if age_ms > 8_000 {
        "idle"
    } else {
        session.dd_status.as_str()
    }
}

fn indicator<'a>(label: &'a str, bg: Color) -> iced::widget::Container<'a, Message> {
    container(text(label).size(13))
        .padding([4, 10])
        .style(move |_| indicator_style(bg, Color::BLACK))
}

fn wide_indicator<'a>(label: &'a str, bg: Color) -> iced::widget::Container<'a, Message> {
    container(
        text(label)
            .size(13)
            .width(Length::Fixed(104.0))
            .align_x(Alignment::Center),
    )
    .padding([4, 10])
    .style(move |_| indicator_style(bg, Color::BLACK))
}

fn bold_indicator<'a>(label: &'a str, bg: Color) -> iced::widget::Container<'a, Message> {
    container(text(label).size(13).font(indicator_font()))
        .padding([4, 10])
        .style(move |_| indicator_style(bg, Color::BLACK))
}

fn indicator_font() -> Font {
    Font {
        family: font::Family::Name(APP_FONT_FAMILY),
        weight: font::Weight::Semibold,
        ..Font::DEFAULT
    }
}

// fn bubble_style(mine: bool) -> container::Style {
//     let border_color = if mine {
//         Color::from_rgb8(0, 204, 0)
//     } else {
//         Color::from_rgb8(0, 204, 204)
//     };
//
//     container::Style {
//         background: Some(Background::Color(Color::from_rgb8(35, 35, 40))),
//         //text_color: Some(Color::WHITE),
//         text_color: Some(Color::from_rgb8(200,200,200)),
//         border: iced::Border {
//             radius: 9.0.into(),
//             width: 1.5,
//             color: border_color,
//         },
//         ..Default::default()
//     }
// }

fn bubble_style(mine: bool, offline: bool) -> container::Style {
    let border_color = match (mine, offline) {
        (true, false) => PY_GREEN,
        (false, false) => PY_CYAN,
        (true, true) => PY_YELLOW,
        (false, true) => PY_MAGENTA,
    };

    container::Style {
        background: Some(Background::Color(Color::from_rgb8(35, 35, 40))),
        text_color: Some(Color::from_rgb8(200, 200, 200)),
        border: iced::Border {
            radius: 9.0.into(),
            width: 1.5,
            color: border_color,
        },
        ..Default::default()
    }
}

fn system_bubble_style() -> container::Style {
    container::Style {
        background: Some(Background::Color(Color::from_rgb8(35, 35, 40))),
        text_color: Some(Color::from_rgb8(210, 210, 210)),
        border: border::Border {
            color: Color::from_rgb8(90, 90, 100),
            width: 1.0,
            radius: border::Radius::from(12.0),
        },
        ..Default::default()
    }
}

fn indicator_style(bg: Color, fg: Color) -> container::Style {
    container::Style {
        background: Some(Background::Color(bg)),
        text_color: Some(fg),
        border: border::rounded(3),
        ..Default::default()
    }
}

fn tab_indicator_style(fg: Color) -> container::Style {
    container::Style {
        background: None,
        text_color: Some(fg),
        ..Default::default()
    }
}

fn tab_button_style(
    _theme: &iced::Theme,
    status: iced::widget::button::Status,
    selected: bool,
) -> iced::widget::button::Style {
    let (border_color, text_color) = match status {
        iced::widget::button::Status::Hovered => (APP_TAB_HOVER_BORDER, APP_TAB_TEXT),
        iced::widget::button::Status::Pressed => (APP_TAB_PRESSED_BORDER, APP_TAB_TEXT),
        iced::widget::button::Status::Disabled => {
            (APP_TAB_UNSELECTED_BORDER, APP_TAB_DISABLED_TEXT)
        }
        iced::widget::button::Status::Active => {
            let border_color = if selected {
                APP_TAB_SELECTED_BORDER
            } else {
                APP_TAB_UNSELECTED_BORDER
            };
            (border_color, APP_TAB_TEXT)
        }
    };

    iced::widget::button::Style {
        background: None,
        text_color,
        border: border::Border {
            color: border_color,
            width: APP_TAB_BORDER_WIDTH,
            radius: border::Radius::from(APP_TAB_BORDER_RADIUS),
        },
        ..Default::default()
    }
}

fn tab_close_button_style(
    _theme: &iced::Theme,
    status: iced::widget::button::Status,
) -> iced::widget::button::Style {
    let (border_color, text_color) = match status {
        iced::widget::button::Status::Hovered => (PY_RED, APP_BUTTON_TEXT),
        iced::widget::button::Status::Pressed => (PY_RED, APP_BUTTON_TEXT),
        iced::widget::button::Status::Disabled => (PY_GREY62, APP_BUTTON_DISABLED_TEXT),
        iced::widget::button::Status::Active => (Color::from_rgb8(80, 80, 90), APP_BUTTON_TEXT),
    };

    iced::widget::button::Style {
        background: None,
        text_color,
        border: border::Border {
            color: border_color,
            width: 2.0,
            radius: border::Radius::from(4.0),
        },
        ..Default::default()
    }
}

fn profile_row_content_style(fg: Color) -> container::Style {
    container::Style {
        background: None,
        text_color: Some(fg),
        ..Default::default()
    }
}

fn profile_button_style(
    _theme: &iced::Theme,
    status: iced::widget::button::Status,
    selected: bool,
) -> iced::widget::button::Style {
    let (background, border_color, text_color) = match status {
        iced::widget::button::Status::Hovered => (
            APP_PROFILE_HOVER_BG,
            APP_PROFILE_HOVER_BORDER,
            APP_PROFILE_TEXT,
        ),
        iced::widget::button::Status::Pressed => (
            APP_PROFILE_PRESSED_BG,
            APP_PROFILE_PRESSED_BORDER,
            APP_PROFILE_TEXT,
        ),
        iced::widget::button::Status::Disabled => (
            APP_PROFILE_UNSELECTED_BG,
            APP_PROFILE_UNSELECTED_BORDER,
            APP_PROFILE_DISABLED_TEXT,
        ),
        iced::widget::button::Status::Active => {
            if selected {
                (
                    APP_PROFILE_SELECTED_BG,
                    APP_PROFILE_SELECTED_BORDER,
                    APP_PROFILE_TEXT,
                )
            } else {
                (
                    APP_PROFILE_UNSELECTED_BG,
                    APP_PROFILE_UNSELECTED_BORDER,
                    APP_PROFILE_TEXT,
                )
            }
        }
    };

    iced::widget::button::Style {
        background: background.map(Background::Color),
        text_color,
        border: border::Border {
            color: border_color,
            width: APP_PROFILE_BORDER_WIDTH,
            radius: border::Radius::from(APP_PROFILE_BORDER_RADIUS),
        },
        ..Default::default()
    }
}

fn my_status_address_button_style(
    _theme: &iced::Theme,
    status: iced::widget::button::Status,
) -> iced::widget::button::Style {
    status_address_button_style(status, PY_GREEN)
}

fn peer_status_address_button_style(
    _theme: &iced::Theme,
    status: iced::widget::button::Status,
) -> iced::widget::button::Style {
    status_address_button_style(status, PY_CYAN)
}

fn status_address_button_style(
    status: iced::widget::button::Status,
    active_color: Color,
) -> iced::widget::button::Style {
    let border_color = match status {
        iced::widget::button::Status::Hovered => active_color,
        iced::widget::button::Status::Pressed => active_color,
        iced::widget::button::Status::Disabled => PY_GREY62,
        iced::widget::button::Status::Active => Color::from_rgb8(70, 70, 80),
    };

    iced::widget::button::Style {
        background: None,
        text_color: APP_BUTTON_TEXT,
        border: border::Border {
            color: border_color,
            width: 1.5,
            radius: border::Radius::from(3.0),
        },
        ..Default::default()
    }
}

fn tab_status_marker<'a>(tab: &'a ChatTab, blink_on: bool) -> Element<'a, Message> {
    if tab.initializing {
        let frame = APP_TAB_SPINNER_FRAMES
            [((TermchatApp::now_epoch_millis() / 120) as usize) % APP_TAB_SPINNER_FRAMES.len()];

        text(frame)
            .size(13)
            .color(Color::from_rgb8(180, 180, 180))
            .into()
    } else if tab.has_incoming {
        if blink_on {
            text("●")
                .size(13)
                .color(Color::from_rgb8(0, 200, 200))
                .into()
        } else {
            text("●")
                .size(13)
                .color(Color::from_rgb8(90, 90, 90))
                .into()
        }
    } else if tab.connected {
        text("●").size(13).color(Color::from_rgb8(0, 200, 0)).into()
    } else if tab.has_unread {
        text("+")
            .size(13)
            .color(Color::from_rgb8(220, 220, 220))
            .into()
    } else {
        Space::new().width(Length::Shrink).into()
    }
}

fn status_bar_style() -> container::Style {
    container::Style {
        background: Some(Background::Color(Color::from_rgb8(25, 25, 30))),
        border: border::Border {
            color: PY_GREEN,
            width: 1.2,
            radius: border::Radius::from(6.0),
        },
        ..Default::default()
    }
}

fn tab_panel_style() -> container::Style {
    container::Style {
        background: Some(Background::Color(Color::from_rgb8(20, 20, 26))),
        border: border::Border {
            color: Color::from_rgb8(100, 100, 110),
            width: 1.0,
            radius: border::Radius::from(6.0),
        },
        ..Default::default()
    }
}

fn sidebar_panel_style() -> container::Style {
    container::Style {
        background: Some(Background::Color(Color::from_rgb8(18, 18, 24))),
        border: border::Border {
            color: Color::from_rgb8(120, 120, 120),
            width: 1.0,
            radius: border::Radius::from(6.0),
        },
        ..Default::default()
    }
}

fn message_panel_style() -> container::Style {
    container::Style {
        background: Some(Background::Color(Color::from_rgb8(15, 15, 20))),
        border: border::Border {
            color: Color::WHITE,
            width: 1.2,
            radius: border::Radius::from(6.0),
        },
        ..Default::default()
    }
}

fn log_panel_style() -> container::Style {
    container::Style {
        background: Some(Background::Color(Color::from_rgb8(12, 12, 16))),
        border: border::Border {
            color: Color::from_rgb8(180, 180, 180),
            width: 1.0,
            radius: border::Radius::from(6.0),
        },
        ..Default::default()
    }
}

fn operation_panel_style() -> container::Style {
    container::Style {
        background: Some(Background::Color(Color::from_rgb8(24, 24, 30))),
        border: border::Border {
            color: Color::from_rgb8(64, 64, 72),
            width: 1.0,
            radius: border::Radius::from(6.0),
        },
        ..Default::default()
    }
}

pub fn app_button_style(
    _theme: &iced::Theme,
    status: iced::widget::button::Status,
) -> iced::widget::button::Style {
    let (bg, text_color) = match status {
        iced::widget::button::Status::Active => (APP_BUTTON_BG, APP_BUTTON_TEXT),
        iced::widget::button::Status::Hovered => (APP_BUTTON_HOVER_BG, APP_BUTTON_TEXT),
        iced::widget::button::Status::Pressed => (APP_BUTTON_PRESSED_BG, APP_BUTTON_TEXT),
        iced::widget::button::Status::Disabled => {
            (APP_BUTTON_DISABLED_BG, APP_BUTTON_DISABLED_TEXT)
        }
    };

    iced::widget::button::Style {
        background: Some(Background::Color(bg)),
        text_color,
        border: border::rounded(3),
        ..Default::default()
    }
}
