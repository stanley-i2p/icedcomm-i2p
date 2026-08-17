use crate::constants::{
    APP_FONT_FAMILY, APP_ICON_FONT_FAMILY, APP_NAME, APP_VERSION, DEFAULT_SAM_HOST,
    DEFAULT_SAM_PORT, MAX_ACTIVE_DEADDROP_REPLICAS, MAX_FILE_SIZE,
};
use crate::deaddrop::{DeadDropClient, DeaddropOpStat};
use crate::e2e::E2E;
use crate::group_invite::{self, PrivateJoinCredential, PrivateJoinProof};
use crate::protocol::{Frame, MsgType};
use crate::rendezvous::{
    self, IssuedAccess as RendezvousIssuedAccess, IssuedState as RendezvousIssuedState,
    OutgoingAccess as RendezvousOutgoingAccess, PendingRequest as RendezvousPendingRequest,
};
use crate::sam::{AcceptedIncoming, LiveConnection, SamClient, SamInitResult};
use crate::sam_runtime::SamRuntime;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use iced::border;
use iced::widget::Id as ScrollableId;
use iced::widget::operation;
use iced::widget::{
    Space, button, column, container, image, opaque, progress_bar, row, scrollable, stack, text,
    text_editor, text_input, tooltip,
};
use iced::{
    Alignment, Background, Color, ContentFit, Element, Font, Length, Subscription, Task, exit,
    font, futures::SinkExt, stream, time, window,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use std::time::Duration;
use tokio::time::{sleep, timeout};

use crate::storage::{
    self, AppLock, ContactMeta, GroupInvite, GroupIssuedInvite, GroupMember, GroupMeta,
    OfflineMissingIndexState, OfflineSkippedIndexState, OfflineState,
};

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
const OFFLINE_INDEX_SYNC_VERSION: u8 = 1;
const OFFLINE_INDEX_SYNC_PAYLOAD_LEN: usize = 17;
const OFFLINE_GAP_MISS_ROUNDS: u32 = 3;
const OFFLINE_FORWARD_PROBE_STALL_ROUNDS: u32 = 3;
const OFFLINE_RECOVERY_STATE_LIMIT: usize = 512;
const OFFLINE_SKIPPED_RETENTION_MS: u64 = 14 * 24 * 60 * 60 * 1_000;
const OFFLINE_RECOVERY_PROBE_INTERVAL_MS: u64 = 60_000;

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
const DEADDROP_PANEL_HEIGHT_PORTION: u16 = 3;
const GROUP_PANEL_HEIGHT_PORTION: u16 = 3;
const OFFLINE_SECRET_REQUEST_SIGNAL: &str = "__SIGNAL__:OFFLINE_SECRET_REQUEST";
const TEXT_BUBBLE_MAX_WIDTH: f32 = 460.0;
const TEXT_BUBBLE_MIN_BODY_WIDTH: f32 = 92.0;
const FILE_BUBBLE_WIDTH: f32 = 340.0;
const IMAGE_BUBBLE_MAX_WIDTH: f32 = 420.0;
const IMAGE_BUBBLE_MAX_HEIGHT: f32 = 360.0;
const SYSTEM_BUBBLE_MAX_WIDTH: f32 = 700.0;
const REPLY_BEGIN_MARKER: &str = "[COMMTOOLS-I2P-REPLY-v1]";
const REPLY_QUOTE_MARKER: &str = "[COMMTOOLS-I2P-QUOTE]";
const REPLY_END_MARKER: &str = "[/COMMTOOLS-I2P-REPLY]";
const IMAGE_TRANSFER_MAX_DIMENSION: u32 = 1280;
const IMAGE_TRANSFER_JPEG_QUALITY: u8 = 82;
const GROUP_IMAGE_TRANSFER_MAX_BYTES: usize = 2 * 1024 * 1024;
const GROUP_INVITE_STRING_PREFIX: &str = "COMMTOOLS-I2P-GROUP-INVITE-v1:";
const GROUP_CONTROL_JOIN_PROOF: &str = "join_proof";
const GROUP_CONTROL_RENAME_REQUEST: &str = "rename_request";
const SHUTDOWN_NOTIFY_GRACE_MS: u64 = 1_200;
const GROUP_STREAM_CLOSE_TIMEOUT_MS: u64 = 120;
const SAM_CONNECT_CANCEL_GRACE_MS: u64 = 4_500;
const GROUP_PUBLISH_LOOKUP_TIMEOUT_MS: u64 = 5_000;
const GROUP_PUBLISH_LOOKUP_RETRY_MS: u64 = 2_000;
const GROUP_HANDSHAKE_TIMEOUT_MS: u64 = 45_000;
const SAM_LIFECYCLE_DEBUG: bool = false;
const HEARTBEAT_PING_INTERVAL_MS: u64 = 10_000;
const HEARTBEAT_TIMEOUT_MS: u64 = 35_000;
const HEARTBEAT_PING_PREFIX: &str = "__SIGNAL__:PING:";
const HEARTBEAT_PONG_PREFIX: &str = "__SIGNAL__:PONG:";
const SAM_MONITOR_INTERVAL_MS: u64 = 5_000;
const SAM_MONITOR_PROBE_TIMEOUT_MS: u64 = 4_000;
const SAM_MONITOR_FAILURE_LIMIT: u8 = 3;
const SAM_SHUTDOWN_COUNTDOWN_MS: u64 = 10_000;
const MAX_LOG_LINES: usize = 500;
const LOG_TRIM_BATCH: usize = 50;

fn current_utc_hms() -> String {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupGate {
    Locked,
    Unlocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TabKind {
    AppHome,
    Chat,
    Group,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone)]
pub struct ChatTab {
    pub kind: TabKind,
    pub title: String,
    pub profile_name: String,
    pub has_unread: bool,
    pub has_incoming: bool,
    pub connected: bool,
    pub closing: bool,
    pub initializing: bool,
    pub initialized: bool,
}

pub struct OpenedTab {
    pub id: u64,
    pub meta: ChatTab,
    pub session: SessionState,
    pub sam_runtime: SamRuntime,
    pub e2e: E2E,
    pub deaddrop: std::sync::Arc<TokioMutex<DeadDropClient>>,
    pub deaddrop_started: bool,
    pub deaddrop_poller_started: bool,
    pub deaddrop_poll_in_flight: bool,
    pub deaddrop_poll_queue: Vec<OfflinePollTarget>,
    pub deaddrop_poll_round_misses: Vec<u64>,
    pub deaddrop_poll_round_authenticated: Vec<u64>,
    pub deaddrop_stalled_sweeps: u32,
    pub deaddrop_last_recovery_probe_ms: u64,
    pub deaddrop_put_in_flight: bool,
    pub deaddrop_last_poll_ms: u64,
    pub live_conn: Option<LiveConnection>,
    pub pending_conn: Option<LiveConnection>,
    pub connect_in_flight: bool,
    pub connect_generation: u64,
    pub connect_peer: Option<String>,
    pub connection_direction: Option<ConnectionDirection>,
    pub offline_index_sync_sent: bool,

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
    pub group: Option<GroupRuntime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OfflinePollKind {
    Window,
    ForwardProbe,
    RecoveryProbe,
}

#[derive(Debug, Clone)]
pub struct OfflinePollTarget {
    pub index: u64,
    pub key: String,
    pub kind: OfflinePollKind,
}

pub struct GroupPeerRuntime {
    pub member: GroupMember,
    pub conn: Option<LiveConnection>,
    pub pending_conn: Option<LiveConnection>,
    pub e2e: E2E,
    pub ready: bool,
    pub authorized: bool,
    pub connecting: bool,
    pub last_connect_attempt_ms: u64,
    pub handshake_started_ms: u64,
    pub handshake_identity_received: bool,
    pub handshake_key_received: bool,
    pub heartbeat_last_rx_ms: u64,
    pub heartbeat_last_ping_ms: u64,
    pub incoming_image_name: Option<String>,
    pub incoming_image_mime: Option<String>,
    pub incoming_image_expected: u64,
    pub incoming_image_received: u64,
    pub incoming_image_msg_id: u64,
    pub incoming_image_bytes: Vec<u8>,
}

pub struct GroupRuntime {
    pub meta: GroupMeta,
    pub peers: Vec<GroupPeerRuntime>,
    pub accept_armed: bool,
    pub publish_ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupControlMessage {
    kind: String,
    token: String,
    b32: String,
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    private_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    private_proof_nonce: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    private_proof_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct GroupRosterSignaturePayload {
    format: String,
    version: u32,
    group_name: String,
    owner_b32: String,
    roster_version: u64,
    members: Vec<GroupMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupRosterSync {
    format: String,
    version: u32,
    group_name: String,
    owner_b32: String,
    roster_version: u64,
    members: Vec<GroupMember>,
    roster_signing_pubkey: String,
    roster_signature: String,
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
    DeleteGroup {
        key: String,
        name: String,
    },
    DeleteGroupMember {
        group_key: String,
        member_b32: String,
        member_name: String,
    },
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
pub struct PendingImageDraft {
    pub filename: String,
    pub mime: String,
    pub image: ImageBubbleData,
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
    pub group_expected_acks: Vec<String>,
    pub group_received_acks: Vec<String>,
}

impl Bubble {
    fn me(text: impl Into<String>) -> Self {
        Self {
            author: "Me".into(),
            content: BubbleContent::Text(text.into()),
            mine: true,
            offline: false,
            timestamp_utc: IcedCommApp::now_utc_hms(),
            msg_id: None,
            delivered: false,
            group_expected_acks: Vec::new(),
            group_received_acks: Vec::new(),
        }
    }

    fn me_with_id(text: impl Into<String>, msg_id: u64) -> Self {
        Self {
            author: "Me".into(),
            content: BubbleContent::Text(text.into()),
            mine: true,
            offline: false,
            timestamp_utc: IcedCommApp::now_utc_hms(),
            msg_id: Some(msg_id),
            delivered: false,
            group_expected_acks: Vec::new(),
            group_received_acks: Vec::new(),
        }
    }

    fn group_me_with_id(text: impl Into<String>, msg_id: u64, expected_acks: Vec<String>) -> Self {
        Self {
            author: "Me".into(),
            content: BubbleContent::Text(text.into()),
            mine: true,
            offline: false,
            timestamp_utc: IcedCommApp::now_utc_hms(),
            msg_id: Some(msg_id),
            delivered: expected_acks.is_empty(),
            group_expected_acks: expected_acks,
            group_received_acks: Vec::new(),
        }
    }

    fn peer(text: impl Into<String>) -> Self {
        Self {
            author: "Peer".into(),
            content: BubbleContent::Text(text.into()),
            mine: false,
            offline: false,
            timestamp_utc: IcedCommApp::now_utc_hms(),
            msg_id: None,
            delivered: false,
            group_expected_acks: Vec::new(),
            group_received_acks: Vec::new(),
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
            timestamp_utc: IcedCommApp::now_utc_hms(),
            msg_id: if msg_id == 0 { None } else { Some(msg_id) },
            delivered: false,
            group_expected_acks: Vec::new(),
            group_received_acks: Vec::new(),
        }
    }

    fn peer_offline(text: impl Into<String>) -> Self {
        Self {
            author: "Peer-Offline".into(),
            content: BubbleContent::Text(text.into()),
            mine: false,
            offline: true,
            timestamp_utc: IcedCommApp::now_utc_hms(),
            msg_id: None,
            delivered: false,
            group_expected_acks: Vec::new(),
            group_received_acks: Vec::new(),
        }
    }

    fn system(text: impl Into<String>) -> Self {
        Self {
            author: "System".into(),
            content: BubbleContent::System(text.into()),
            mine: false,
            offline: false,
            timestamp_utc: IcedCommApp::now_utc_hms(),
            msg_id: None,
            delivered: false,
            group_expected_acks: Vec::new(),
            group_received_acks: Vec::new(),
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
pub struct LogLines {
    lines: Vec<String>,
}

impl LogLines {
    fn from_messages(messages: Vec<String>) -> Self {
        let mut log_lines = Self { lines: Vec::new() };
        for message in messages {
            log_lines.push(message);
        }
        log_lines
    }

    fn push(&mut self, message: String) {
        if self.lines.len() >= MAX_LOG_LINES {
            let trim_count = LOG_TRIM_BATCH.min(self.lines.len());
            self.lines.drain(..trim_count);
        }

        self.lines
            .push(format!("[{}] {message}", current_utc_hms()));
    }

    fn iter(&self) -> impl Iterator<Item = &String> {
        self.lines.iter()
    }

    fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    fn len(&self) -> usize {
        self.lines.len()
    }

    fn joined(&self) -> String {
        self.lines.join("\n")
    }
}

#[derive(Debug, Clone)]
pub struct SessionState {
    pub profile: String,
    pub profiles: Vec<ProfileEntry>,
    pub selected_profile_idx: usize,
    pub profile_name_input: String,
    pub sidebar_confirm: Option<SidebarConfirm>,
    pub groups: Vec<GroupMeta>,
    pub selected_group_idx: Option<usize>,
    pub group_name_input: String,
    pub group_display_name_input: String,
    pub group_member_name_input: String,
    pub group_member_b32_input: String,
    pub group_invite_string_input: String,
    pub group_generated_invite_string: String,
    pub group_private_request_string: String,
    pub group_private_request_input: String,
    pub group_generated_private_invite_string: String,
    pub group_status: String,
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
    pub heartbeat_last_rx_ms: u64,
    pub heartbeat_last_ping_ms: u64,

    pub call_blink_on: bool,
    pub call_blink_ticks: u8,

    pub pending_action: Option<GuiAction>,
    pub action_param: String,

    pub show_rendezvous_panel: bool,
    pub rendezvous_input: String,
    pub rendezvous_output: String,
    pub rendezvous_status: String,
    pub rendezvous_request: Option<RendezvousPendingRequest>,
    pub rendezvous_issued: Option<RendezvousIssuedAccess>,
    pub rendezvous_outgoing: Option<RendezvousOutgoingAccess>,
    pub pending_rendezvous_request_id: Option<[u8; 16]>,

    pub input: String,
    pub input_editor: text_editor::Content,
    pub reply_to: Option<ReplyDraft>,
    pub pending_image: Option<PendingImageDraft>,
    pub bubbles: Vec<Bubble>,
    pub status_lines: Vec<String>,
    pub show_logs: bool,
    pub show_deaddrop_panel: bool,
    pub show_group_panel: bool,
    pub deaddrop_server_input: String,
    pub deaddrop_delete_confirm: Option<DdServerDeleteConfirm>,
    pub log_lines: LogLines,
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
    pub known_remote_next_send: u64,
    pub highest_authenticated_recv_index: Option<u64>,
    pub missing_drop_recv: Vec<OfflineMissingIndexState>,
    pub skipped_drop_recv: Vec<OfflineSkippedIndexState>,
    pub forward_probe_index: u64,
    pub seen_drop_msgs: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DdServerDeleteConfirm {
    pub index: usize,
    pub server: String,
}

#[derive(Debug, Clone)]
pub struct ReplyDraft {
    pub author: String,
    pub text: String,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            profile: "default".into(),
            profiles: vec![ProfileEntry::transient()],
            selected_profile_idx: 0,
            profile_name_input: String::new(),
            sidebar_confirm: None,
            groups: Vec::new(),
            selected_group_idx: None,
            group_name_input: String::new(),
            group_display_name_input: String::new(),
            group_member_name_input: String::new(),
            group_member_b32_input: String::new(),
            group_invite_string_input: String::new(),
            group_generated_invite_string: String::new(),
            group_private_request_string: String::new(),
            group_private_request_input: String::new(),
            group_generated_private_invite_string: String::new(),
            group_status: "Groups use separate I2P identities and live fan-out only.".into(),
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
            heartbeat_last_rx_ms: 0,
            heartbeat_last_ping_ms: 0,
            pending_action: None,
            call_blink_on: true,
            call_blink_ticks: 0,
            action_param: String::new(),
            show_rendezvous_panel: false,
            rendezvous_input: String::new(),
            rendezvous_output: String::new(),
            rendezvous_status: "Optional one-time bootstrap for a transient call.".into(),
            rendezvous_request: None,
            rendezvous_issued: None,
            rendezvous_outgoing: None,
            pending_rendezvous_request_id: None,
            input: String::new(),
            input_editor: text_editor::Content::new(),
            reply_to: None,
            pending_image: None,
            bubbles: vec![],
            status_lines: vec![
                format!("{APP_NAME} {APP_VERSION}"),
                "Application ready.".into(),
                "Open a profile to start a chat tab.".into(),
            ],
            show_logs: false,
            show_deaddrop_panel: false,
            show_group_panel: false,
            deaddrop_server_input: String::new(),
            deaddrop_delete_confirm: None,
            log_lines: LogLines::from_messages(vec![
                format!("{APP_NAME} {APP_VERSION}"),
                "Application ready.".into(),
                "Open a profile to start a chat tab.".into(),
            ]),
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
            known_remote_next_send: 0,
            highest_authenticated_recv_index: None,
            missing_drop_recv: vec![],
            skipped_drop_recv: vec![],
            forward_probe_index: 0,
            seen_drop_msgs: vec![],
        }
    }
}

pub struct IcedCommApp {
    pub session: SessionState,
    pub opened_tabs: Vec<OpenedTab>,
    pub app_lock: Option<AppLock>,
    pub clipboard: Option<Clipboard>,
    pub window_id: Option<window::Id>,
    pub window_focused: bool,
    pub unread_attention_active: bool,

    pub startup_gate: StartupGate,
    pub unlock_input: String,
    pub unlock_confirm_input: String,
    pub unlock_status: String,
    pub sam_host_input: String,
    pub sam_port_input: String,
    pub sam_status: String,
    pub sam_test_in_flight: bool,
    pub sam_monitor_host: Option<String>,
    pub sam_monitor_port: Option<u16>,
    pub sam_monitor_generation: u64,
    pub sam_monitor_last_probe_ms: u64,
    pub sam_monitor_probe_in_flight: bool,
    pub sam_monitor_failures: u8,
    pub sam_shutdown_deadline_ms: Option<u64>,
    pub sam_shutdown_started: bool,
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
    InputChanged(text_editor::Action),
    PasteFromClipboard,
    CancelPendingImagePressed,
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
    SamMonitorProbeFinished(u64, Result<(), String>),
    SamShutdownNowPressed,
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
    FinalizeTabClosed(u64),

    ActionPressed(GuiAction),
    ActionParamChanged(String),
    ActionConfirm,
    ActionCancel,
    CopyStatusMyB32Pressed,
    CopyStatusPeerB32Pressed,
    CopyBubbleTextPressed(usize),
    ReplyBubblePressed(usize),
    CancelReplyPressed,
    CopyLogsPressed,
    ToggleLogsPressed,
    ToggleGroupPanelPressed,
    ToggleRendezvousPanelPressed,
    RendezvousInputChanged(String),
    GenerateRendezvousRequestPressed,
    AnswerRendezvousRequestPressed,
    ConnectRendezvousResponsePressed,
    CopyRendezvousOutputPressed,
    ClearRendezvousPressed,
    RevokeRendezvousPressed,
    DdServerInputChanged(String),
    DdServerAddPressed,
    DdServerDeletePressed(usize),
    DdServerDeleteConfirmed,
    DdServerDeleteCancelled,
    DdServerSharePressed,
    GroupSelected(usize),
    GroupNameInputChanged(String),
    GroupDisplayNameInputChanged(String),
    SaveGroupDisplayNamePressed,
    GroupMemberNameInputChanged(String),
    GroupMemberB32InputChanged(String),
    CreateGroupPressed,
    OpenGroupPressed,
    AddGroupMemberPressed,
    DeleteGroupPressed,
    DeleteGroupMemberPressed(usize),
    ExportGroupInvitePressed,
    ImportGroupInvitePressed,
    GroupInviteStringInputChanged(String),
    GenerateGroupInvitePressed,
    CopyGeneratedGroupInvitePressed,
    GeneratePrivateGroupRequestPressed,
    CopyPrivateGroupRequestPressed,
    PrivateGroupRequestInputChanged(String),
    GeneratePrivateGroupInvitePressed,
    CopyGeneratedPrivateGroupInvitePressed,
    RevokePrivateGroupInvitePressed(String),
    CopyGroupInviteStringPressed,
    ImportGroupInviteStringPressed,
    GroupInviteExportPathChosen(Option<PathBuf>),
    GroupInviteImportPathChosen(Option<PathBuf>),
    GroupInviteExportFinished(Result<(PathBuf, GroupMeta), String>),
    GroupInviteImportFinished(Result<String, String>),

    SamInitialized(u64, Result<(SamClient, SamInitResult), String>),
    GroupPublishReady(u64, Result<(), String>),
    GroupConnectFinished(u64, String, Result<(String, LiveConnection), String>),
    GroupIncomingAccepted(u64, Result<AcceptedIncoming, String>),
    ConnectFinished(u64, u64, Result<(String, LiveConnection), String>),
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
        OfflinePollKind,
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
    WindowOpened(window::Id),
    WindowFocusChanged(window::Id, bool),
    ProcessShutdownRequested,
    ExitAfterNotify(ShutdownTarget),
    Tick,
}

#[derive(Debug, Clone, Copy)]
pub enum ShutdownTarget {
    Window(window::Id),
    Runtime,
}

impl Default for IcedCommApp {
    fn default() -> Self {
        Self {
            session: SessionState::default(),
            opened_tabs: vec![],
            app_lock: None,
            clipboard: Clipboard::new().ok(),
            window_id: None,
            window_focused: true,
            unread_attention_active: false,

            startup_gate: StartupGate::Locked,
            unlock_input: String::new(),
            unlock_confirm_input: String::new(),
            unlock_status: "Vault is locked.".into(),
            sam_host_input: DEFAULT_SAM_HOST.to_string(),
            sam_port_input: DEFAULT_SAM_PORT.to_string(),
            sam_status: "SAM settings apply to newly opened chat tabs.".into(),
            sam_test_in_flight: false,
            sam_monitor_host: None,
            sam_monitor_port: None,
            sam_monitor_generation: 0,
            sam_monitor_last_probe_ms: 0,
            sam_monitor_probe_in_flight: false,
            sam_monitor_failures: 0,
            sam_shutdown_deadline_ms: None,
            sam_shutdown_started: false,
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

impl Drop for IcedCommApp {
    fn drop(&mut self) {
        if let Err(err) = self.encrypt_for_shutdown() {
            eprintln!("Vault encryption failed during app drop: {err}");
        }
    }
}

impl IcedCommApp {
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
        let groups = storage::load_groups().map_err(|e| e.to_string())?;
        self.session.profiles = vec![ProfileEntry::transient()];
        for contact in contacts {
            self.session
                .profiles
                .push(ProfileEntry::persistent(contact.name));
        }
        self.session.groups = groups;
        self.session.selected_group_idx = if self.session.groups.is_empty() {
            None
        } else {
            Some(0)
        };
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
                closing: false,
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
            deaddrop_poll_round_misses: vec![],
            deaddrop_poll_round_authenticated: vec![],
            deaddrop_stalled_sweeps: 0,
            deaddrop_last_recovery_probe_ms: 0,
            deaddrop_put_in_flight: false,
            deaddrop_last_poll_ms: 0,
            sam_runtime: SamRuntime::new(sam_host.clone(), sam_port),
            e2e: E2E::new(false),
            live_conn: None,
            pending_conn: None,
            connect_in_flight: false,
            connect_generation: 0,
            connect_peer: None,
            connection_direction: None,
            offline_index_sync_sent: false,

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
            group: None,

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

    fn new_opened_group_tab(&self, group_meta: GroupMeta) -> OpenedTab {
        let profile_name = format!("group:{}", storage::group_storage_key(&group_meta));
        let mut tab = self.new_opened_tab(&profile_name);

        tab.meta.kind = TabKind::Group;
        tab.meta.title = format!("#{}", group_meta.name);
        tab.meta.profile_name = profile_name.clone();
        tab.session.profile = profile_name;
        tab.session.my_dest_b64 = group_meta.my_dest_b64.clone();
        tab.session.stored_peer = None;
        tab.session.stored_peer_dest_b64 = None;
        tab.session.current_peer_addr = None;
        tab.session.current_peer_dest_b64 = None;
        tab.session.peer_b32 = None;
        tab.session.pending_peer_addr = None;
        tab.session.pending_peer_dest_b64 = None;
        tab.session.offline_mode = false;
        tab.session.deaddrop_servers.clear();
        tab.session.show_deaddrop_panel = false;
        tab.session.log_lines = LogLines::from_messages(vec![
            format!("Group opened: {}", group_meta.name),
            "Group chat uses separate identity and live fan-out only.".into(),
        ]);
        tab.session.bubbles.clear();

        let peers = group_meta
            .members
            .iter()
            .cloned()
            .map(|member| Self::new_group_peer_runtime(member, true))
            .collect();

        tab.group = Some(GroupRuntime {
            meta: group_meta,
            peers,
            accept_armed: false,
            publish_ready: false,
        });

        tab
    }

    fn new_group_peer_runtime(member: GroupMember, authorized: bool) -> GroupPeerRuntime {
        GroupPeerRuntime {
            member,
            conn: None,
            pending_conn: None,
            e2e: E2E::new(false),
            ready: false,
            authorized,
            connecting: false,
            last_connect_attempt_ms: 0,
            handshake_started_ms: 0,
            handshake_identity_received: false,
            handshake_key_received: false,
            heartbeat_last_rx_ms: 0,
            heartbeat_last_ping_ms: 0,
            incoming_image_name: None,
            incoming_image_mime: None,
            incoming_image_expected: 0,
            incoming_image_received: 0,
            incoming_image_msg_id: 0,
            incoming_image_bytes: Vec::new(),
        }
    }

    fn local_prefers_outbound(my_b32: Option<&str>, peer_b32: &str) -> bool {
        let Some(my_b32) = my_b32 else {
            return false;
        };

        my_b32
            .to_ascii_lowercase()
            .cmp(&peer_b32.to_ascii_lowercase())
            .is_lt()
    }

    fn invalidate_one_to_one_connect(tab: &mut OpenedTab, cancel: bool) {
        if cancel && tab.connect_in_flight {
            tab.sam_runtime.cancel_connect();
        }
        tab.connect_in_flight = false;
        tab.connect_peer = None;
        tab.connect_generation = tab.connect_generation.wrapping_add(1);
    }

    fn start_one_to_one_connect(&mut self, peer: String) -> Option<Task<Message>> {
        let tab = self.active_tab_mut()?;
        if tab.meta.kind != TabKind::Chat
            || tab.sam_runtime.is_closing()
            || tab.session.offline_mode
            || tab.connect_in_flight
            || tab.live_conn.is_some()
            || tab.pending_conn.is_some()
        {
            return None;
        }

        let (sam, connect_cancelled) = tab.sam_runtime.connect_parts()?;
        tab.connect_generation = tab.connect_generation.wrapping_add(1);
        let generation = tab.connect_generation;
        let tab_id = tab.id;
        tab.connect_in_flight = true;
        tab.connect_peer = Some(peer.clone());
        tab.connection_direction = None;
        tab.session.network_status = NetworkStatus::Visible;

        let task = Task::perform(
            async move {
                sam.stream_connect_cancelled(&peer, connect_cancelled)
                    .await
                    .map(|conn| (peer, conn))
                    .map_err(|e| e.to_string())
            },
            move |result| Message::ConnectFinished(tab_id, generation, result),
        );

        Some(tab.sam_runtime.track_connect_task(task))
    }

    pub fn subscription(_state: &Self) -> Subscription<Message> {
        Subscription::batch(vec![
            time::every(Duration::from_millis(150)).map(|_| Message::Tick),
            window::close_requests().map(Message::WindowCloseRequested),
            iced::event::listen_with(|event, _status, window_id| match event {
                iced::Event::Window(window::Event::Opened { .. }) => {
                    Some(Message::WindowOpened(window_id))
                }
                iced::Event::Window(window::Event::Focused) => {
                    Some(Message::WindowFocusChanged(window_id, true))
                }
                iced::Event::Window(window::Event::Unfocused) => {
                    Some(Message::WindowFocusChanged(window_id, false))
                }
                _ => None,
            }),
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
        let mut tasks: Vec<Task<Message>> = Vec::new();

        for tab in &mut self.opened_tabs {
            let tab_id = tab.id;
            let live = tab.live_conn.clone();
            let pending = tab.pending_conn.clone();
            let group_conns: Vec<LiveConnection> = tab
                .group
                .as_ref()
                .map(|group| {
                    group
                        .peers
                        .iter()
                        .flat_map(|peer| [peer.conn.clone(), peer.pending_conn.clone()])
                        .flatten()
                        .collect()
                })
                .unwrap_or_default();
            let (registered_conns, mut sam) = tab.sam_runtime.shutdown_parts();
            let dd = if tab.deaddrop_started {
                Some(std::sync::Arc::clone(&tab.deaddrop))
            } else {
                None
            };

            Self::flush_deaddrop_stats_for_tab(tab, true);

            tasks.push(Task::perform(
                async move {
                    if let Some(conn) = live {
                        let quit_live = Frame {
                            msg_type: MsgType::S,
                            msg_id: 0,
                            payload: b"__SIGNAL__:QUIT".to_vec(),
                        };
                        let _ = conn.send_frame(&quit_live).await;
                        sleep(Duration::from_millis(120)).await;
                        let _ = conn.close().await;
                    }

                    if let Some(conn) = pending {
                        let quit_pending = Frame {
                            msg_type: MsgType::S,
                            msg_id: 0,
                            payload: b"__SIGNAL__:QUIT".to_vec(),
                        };
                        let _ = conn.send_frame(&quit_pending).await;
                        sleep(Duration::from_millis(120)).await;
                        let _ = conn.close().await;
                    }

                    for conn in group_conns {
                        let quit_group = Frame {
                            msg_type: MsgType::S,
                            msg_id: 0,
                            payload: b"__SIGNAL__:QUIT".to_vec(),
                        };
                        let _ = conn.send_frame(&quit_group).await;
                        sleep(Duration::from_millis(30)).await;
                        let _ = conn.close().await;
                    }

                    for conn in registered_conns {
                        let _ = conn.close().await;
                    }

                    if let Some(dd) = dd {
                        let mut dd = dd.lock().await;
                        dd.close().await;
                    }

                    sleep(Duration::from_millis(SAM_CONNECT_CANCEL_GRACE_MS)).await;
                    sam.close().await.map_err(|e| e.to_string())
                },
                move |result| Message::SamCloseFinished(tab_id, result),
            ));

            if tab.deaddrop_started {
                tab.deaddrop_started = false;
                tab.deaddrop_poller_started = false;
                tab.deaddrop_poll_in_flight = false;
                tab.deaddrop_poll_queue.clear();
                tab.deaddrop_put_in_flight = false;
            }

            tab.live_conn = None;
            tab.pending_conn = None;
            tab.connect_in_flight = false;
            tab.connect_peer = None;
            tab.connection_direction = None;
            tab.session.sam_session_id = None;
            tab.session.live_ready = false;
            tab.session.pending_peer_addr = None;
            tab.session.pending_peer_dest_b64 = None;
            tab.session.current_peer_addr = None;
            tab.session.peer_b32 = None;
            tab.session.accept_armed = false;
            if let Some(group) = tab.group.as_mut() {
                group.publish_ready = false;
                for peer in &mut group.peers {
                    Self::reset_group_peer_transport_state(peer);
                }
                group.accept_armed = false;
            }
            tab.sam_runtime.clear_registered_streams();
        }

        self.reset_connection_state();
        self.store_active_runtime();

        tasks.push(Task::perform(
            async move {
                sleep(Duration::from_millis(
                    SHUTDOWN_NOTIFY_GRACE_MS + SAM_CONNECT_CANCEL_GRACE_MS,
                ))
                .await;
                target
            },
            Message::ExitAfterNotify,
        ));
        Task::batch(tasks)
    }

    fn accept_task(&self, tab_id: u64) -> Task<Message> {
        let accept = self
            .opened_tabs
            .iter()
            .find(|t| t.id == tab_id)
            .and_then(|t| t.sam_runtime.accept_parts());

        if let Some((sam, cancelled)) = accept {
            Task::perform(
                async move {
                    sam.stream_accept_cancelled(cancelled)
                        .await
                        .map_err(|e| e.to_string())
                },
                move |result| Message::IncomingAccepted(tab_id, result),
            )
        } else {
            Task::none()
        }
    }

    fn group_accept_task(&self, tab_id: u64) -> Task<Message> {
        let Some(tab) = self
            .opened_tabs
            .iter()
            .find(|t| t.id == tab_id)
        else {
            return Task::none();
        };

        if let Some((sam, cancelled)) = tab.sam_runtime.accept_parts() {
            let task = Task::perform(
                async move {
                    sam.stream_accept_cancelled(cancelled)
                        .await
                        .map_err(|e| e.to_string())
                },
                move |result| Message::GroupIncomingAccepted(tab_id, result),
            );
            tab.sam_runtime.track_accept_task(task)
        } else {
            Task::none()
        }
    }

    fn incoming_accept_task_from_parts(
        tab_id: u64,
        sam: SamClient,
        cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Task<Message> {
        Task::perform(
            async move {
                sam.stream_accept_cancelled(cancelled)
                    .await
                    .map_err(|e| e.to_string())
            },
            move |result| Message::IncomingAccepted(tab_id, result),
        )
    }

    fn group_connect_tasks(&mut self, tab_id: u64) -> Vec<Task<Message>> {
        let Some(idx) = self.find_tab_index_by_id(tab_id) else {
            return Vec::new();
        };

        let sam_runtime = self.opened_tabs[idx].sam_runtime.clone();
        let Some(group) = self.opened_tabs[idx].group.as_mut() else {
            return Vec::new();
        };

        if !group.publish_ready {
            return Vec::new();
        }

        let mut tasks = Vec::new();
        let now_ms = Self::now_epoch_millis();

        for peer in &mut group.peers {
            if !peer.authorized {
                continue;
            }

            if peer.ready || peer.connecting || peer.conn.is_some() {
                continue;
            }

            if peer.last_connect_attempt_ms != 0
                && now_ms.saturating_sub(peer.last_connect_attempt_ms) < 5_000
            {
                continue;
            }

            let Some((sam, connect_cancelled)) = sam_runtime.connect_parts() else {
                break;
            };

            peer.connecting = true;
            peer.last_connect_attempt_ms = now_ms;
            let peer_b32 = peer.member.b32.clone();
            Self::sam_lifecycle_log(format!(
                "group connect task start tab={tab_id} peer={peer_b32}"
            ));
            let task = Task::perform(
                async move {
                    let conn = sam
                        .stream_connect_cancelled(&peer_b32, connect_cancelled)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok((peer_b32.clone(), conn))
                },
                {
                    let peer_b32 = peer.member.b32.clone();
                    move |result| Message::GroupConnectFinished(tab_id, peer_b32.clone(), result)
                },
            );
            tasks.push(sam_runtime.track_connect_task(task));
        }

        tasks
    }

    fn group_publish_ready_task(&self, tab_id: u64) -> Task<Message> {
        let Some(tab) = self.opened_tabs.iter().find(|tab| tab.id == tab_id) else {
            return Task::none();
        };
        let Some(group_b32) = tab
            .group
            .as_ref()
            .and_then(|group| group.meta.my_b32.clone())
        else {
            return Task::none();
        };
        let Some((sam, cancelled)) = tab.sam_runtime.lookup_parts() else {
            return Task::none();
        };

        let task = Task::perform(
            async move {
                loop {
                    if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                        return Err("group publication lookup cancelled".to_string());
                    }

                    if let Ok(Ok(_)) = timeout(
                        Duration::from_millis(GROUP_PUBLISH_LOOKUP_TIMEOUT_MS),
                        sam.naming_lookup_cancelled(&group_b32, cancelled.clone()),
                    )
                    .await
                    {
                        return Ok(());
                    }

                    let mut waited_ms = 0u64;
                    while waited_ms < GROUP_PUBLISH_LOOKUP_RETRY_MS {
                        if cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                            return Err("group publication lookup cancelled".to_string());
                        }
                        let step_ms = 50u64.min(GROUP_PUBLISH_LOOKUP_RETRY_MS - waited_ms);
                        sleep(Duration::from_millis(step_ms)).await;
                        waited_ms += step_ms;
                    }
                }
            },
            move |result| Message::GroupPublishReady(tab_id, result),
        );

        tab.sam_runtime.track_lookup_task(task)
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

    fn sync_unread_attention(&mut self) -> Option<Task<Message>> {
        let should_request =
            !self.window_focused && self.opened_tabs.iter().any(|tab| tab.meta.has_unread);

        if should_request == self.unread_attention_active {
            return None;
        }

        let Some(window_id) = self.window_id else {
            return None;
        };
        self.unread_attention_active = should_request;

        Some(window::request_user_attention(
            window_id,
            should_request.then_some(window::UserAttention::Informational),
        ))
    }

    fn refresh_sam_monitor_state(&mut self) {
        if self.sam_shutdown_deadline_ms.is_some() || self.sam_shutdown_started {
            return;
        }

        let endpoint = self
            .opened_tabs
            .iter()
            .find(|tab| {
                tab.meta.initialized
                    && tab.session.sam_session_id.is_some()
                    && !tab.sam_runtime.is_closing()
            })
            .map(|tab| {
                (
                    tab.sam_runtime.client.sam_host.clone(),
                    tab.sam_runtime.client.sam_port,
                )
            });

        let current = self
            .sam_monitor_host
            .as_ref()
            .zip(self.sam_monitor_port)
            .map(|(host, port)| (host.as_str(), port));
        let next = endpoint.as_ref().map(|(host, port)| (host.as_str(), *port));

        if current == next {
            return;
        }

        self.sam_monitor_generation = self.sam_monitor_generation.wrapping_add(1);
        self.sam_monitor_probe_in_flight = false;
        self.sam_monitor_failures = 0;
        self.sam_monitor_last_probe_ms = Self::now_epoch_millis();

        if let Some((host, port)) = endpoint {
            self.sam_monitor_host = Some(host);
            self.sam_monitor_port = Some(port);
        } else {
            self.sam_monitor_host = None;
            self.sam_monitor_port = None;
            self.sam_monitor_last_probe_ms = 0;
        }
    }

    fn start_sam_monitor_probe(&mut self) -> Option<Task<Message>> {
        if self.sam_monitor_probe_in_flight || self.sam_shutdown_deadline_ms.is_some() {
            return None;
        }

        let host = self.sam_monitor_host.clone()?;
        let port = self.sam_monitor_port?;
        let generation = self.sam_monitor_generation;
        self.sam_monitor_probe_in_flight = true;
        self.sam_monitor_last_probe_ms = Self::now_epoch_millis();

        Some(Task::perform(
            async move {
                match timeout(
                    Duration::from_millis(SAM_MONITOR_PROBE_TIMEOUT_MS),
                    SamClient::test_endpoint(host, port),
                )
                .await
                {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(err)) => Err(err.to_string()),
                    Err(_) => Err("SAM monitor probe timed out".to_string()),
                }
            },
            move |result| Message::SamMonitorProbeFinished(generation, result),
        ))
    }

    fn store_active_runtime(&mut self) {
        let sidebar_profiles = self.session.profiles.clone();
        let sidebar_selected = self.session.selected_profile_idx;
        let sidebar_input = self.session.profile_name_input.clone();
        let sidebar_confirm = self.session.sidebar_confirm.clone();
        let groups = self.session.groups.clone();
        let selected_group_idx = self.session.selected_group_idx;
        let group_name_input = self.session.group_name_input.clone();
        let group_display_name_input = self.session.group_display_name_input.clone();
        let group_member_name_input = self.session.group_member_name_input.clone();
        let group_member_b32_input = self.session.group_member_b32_input.clone();
        let group_invite_string_input = self.session.group_invite_string_input.clone();
        let group_generated_invite_string = self.session.group_generated_invite_string.clone();
        let group_private_request_string = self.session.group_private_request_string.clone();
        let group_private_request_input = self.session.group_private_request_input.clone();
        let group_generated_private_invite_string = self
            .session
            .group_generated_private_invite_string
            .clone();
        let group_status = self.session.group_status.clone();
        let tabs = self.session.tabs.clone();
        let active_idx = self.session.active_tab_idx;

        let mut snapshot = self.session.clone();
        snapshot.profiles = vec![];
        snapshot.selected_profile_idx = 0;
        snapshot.profile_name_input.clear();
        snapshot.sidebar_confirm = None;
        snapshot.groups = Vec::new();
        snapshot.selected_group_idx = None;
        snapshot.group_name_input.clear();
        snapshot.group_display_name_input.clear();
        snapshot.group_member_name_input.clear();
        snapshot.group_member_b32_input.clear();
        snapshot.group_invite_string_input.clear();
        snapshot.group_generated_invite_string.clear();
        snapshot.group_private_request_string.clear();
        snapshot.group_private_request_input.clear();
        snapshot.group_generated_private_invite_string.clear();
        snapshot.group_status.clear();

        if let Some(tab) = self.active_tab_mut() {
            tab.session = snapshot;
            tab.meta.connected = tab.session.live_ready;
            tab.meta.has_incoming = tab.session.pending_peer_addr.is_some();
            tab.meta.closing = tab.sam_runtime.is_closing();
        }

        self.session.profiles = sidebar_profiles;
        self.session.selected_profile_idx = sidebar_selected;
        self.session.profile_name_input = sidebar_input;
        self.session.sidebar_confirm = sidebar_confirm;
        self.session.groups = groups;
        self.session.selected_group_idx = selected_group_idx;
        self.session.group_name_input = group_name_input;
        self.session.group_display_name_input = group_display_name_input;
        self.session.group_member_name_input = group_member_name_input;
        self.session.group_member_b32_input = group_member_b32_input;
        self.session.group_invite_string_input = group_invite_string_input;
        self.session.group_generated_invite_string = group_generated_invite_string;
        self.session.group_private_request_string = group_private_request_string;
        self.session.group_private_request_input = group_private_request_input;
        self.session.group_generated_private_invite_string =
            group_generated_private_invite_string;
        self.session.group_status = group_status;
        self.session.tabs = tabs;
        self.session.active_tab_idx = active_idx;
    }

    fn load_active_runtime(&mut self) {
        let sidebar_profiles = self.session.profiles.clone();
        let sidebar_selected = self.session.selected_profile_idx;
        let sidebar_input = self.session.profile_name_input.clone();
        let sidebar_confirm = self.session.sidebar_confirm.clone();
        let groups = self.session.groups.clone();
        let selected_group_idx = self.session.selected_group_idx;
        let group_name_input = self.session.group_name_input.clone();
        let group_display_name_input = self.session.group_display_name_input.clone();
        let group_member_name_input = self.session.group_member_name_input.clone();
        let group_member_b32_input = self.session.group_member_b32_input.clone();
        let group_invite_string_input = self.session.group_invite_string_input.clone();
        let group_generated_invite_string = self.session.group_generated_invite_string.clone();
        let group_private_request_string = self.session.group_private_request_string.clone();
        let group_private_request_input = self.session.group_private_request_input.clone();
        let group_generated_private_invite_string = self
            .session
            .group_generated_private_invite_string
            .clone();
        let group_status = self.session.group_status.clone();
        let tabs = self.session.tabs.clone();
        let active_idx = self.session.active_tab_idx;

        if let Some(snapshot) = self.active_tab().map(|t| t.session.clone()) {
            let preserve_editor = self.session.active_tab_idx == active_idx
                && self.session.profile == snapshot.profile
                && self.session.input == snapshot.input;
            let preserved_editor = if preserve_editor {
                Some(std::mem::take(&mut self.session.input_editor))
            } else {
                None
            };

            self.session = snapshot;
            if let Some(input_editor) = preserved_editor {
                self.session.input_editor = input_editor;
            } else {
                self.session.input_editor = Self::message_editor_with_text(&self.session.input);
            }
            self.session.profiles = sidebar_profiles;
            self.session.selected_profile_idx = sidebar_selected;
            self.session.profile_name_input = sidebar_input;
            self.session.sidebar_confirm = sidebar_confirm;
            self.session.groups = groups;
            self.session.selected_group_idx = selected_group_idx;
            self.session.group_name_input = group_name_input;
            self.session.group_display_name_input = group_display_name_input;
            self.session.group_member_name_input = group_member_name_input;
            self.session.group_member_b32_input = group_member_b32_input;
            self.session.group_invite_string_input = group_invite_string_input;
            self.session.group_generated_invite_string = group_generated_invite_string;
            self.session.group_private_request_string = group_private_request_string;
            self.session.group_private_request_input = group_private_request_input;
            self.session.group_generated_private_invite_string =
                group_generated_private_invite_string;
            self.session.group_status = group_status;
            self.session.tabs = tabs;
            self.session.active_tab_idx = active_idx;
        }
    }

    fn refresh_visible_from_active_tab(&mut self) {
        self.refresh_visible_from_active_tab_with_editor(true);
    }

    fn refresh_visible_from_active_tab_reset_editor(&mut self) {
        self.refresh_visible_from_active_tab_with_editor(false);
    }

    fn refresh_visible_from_active_tab_with_editor(&mut self, preserve_editor: bool) {
        let sidebar_profiles = self.session.profiles.clone();
        let sidebar_selected = self.session.selected_profile_idx;
        let sidebar_input = self.session.profile_name_input.clone();
        let sidebar_confirm = self.session.sidebar_confirm.clone();
        let groups = self.session.groups.clone();
        let selected_group_idx = self.session.selected_group_idx;
        let group_name_input = self.session.group_name_input.clone();
        let group_display_name_input = self.session.group_display_name_input.clone();
        let group_member_name_input = self.session.group_member_name_input.clone();
        let group_member_b32_input = self.session.group_member_b32_input.clone();
        let group_invite_string_input = self.session.group_invite_string_input.clone();
        let group_generated_invite_string = self.session.group_generated_invite_string.clone();
        let group_private_request_string = self.session.group_private_request_string.clone();
        let group_private_request_input = self.session.group_private_request_input.clone();
        let group_generated_private_invite_string = self
            .session
            .group_generated_private_invite_string
            .clone();
        let group_status = self.session.group_status.clone();

        self.session.tabs = std::iter::once(Self::new_app_home_tab())
            .chain(self.opened_tabs.iter().map(|t| t.meta.clone()))
            .collect();

        match self.session.active_tab_idx {
            Some(0) | None => {
                self.session.profiles = sidebar_profiles.clone();
                self.session.selected_profile_idx = sidebar_selected;
                self.session.profile_name_input = sidebar_input.clone();
                self.session.sidebar_confirm = sidebar_confirm.clone();
                self.session.groups = groups.clone();
                self.session.selected_group_idx = selected_group_idx;
                self.session.group_name_input = group_name_input.clone();
                self.session.group_display_name_input = group_display_name_input.clone();
                self.session.group_member_name_input = group_member_name_input.clone();
                self.session.group_member_b32_input = group_member_b32_input.clone();
                self.session.group_invite_string_input = group_invite_string_input.clone();
                self.session.group_generated_invite_string = group_generated_invite_string.clone();
                self.session.group_private_request_string = group_private_request_string.clone();
                self.session.group_private_request_input = group_private_request_input.clone();
                self.session.group_generated_private_invite_string =
                    group_generated_private_invite_string.clone();
                self.session.group_status = group_status.clone();
                self.session.active_tab_idx = Some(0);
                self.session.profile = "__app__".into();
            }
            Some(visible_idx) => {
                if let Some(real_idx) = Self::visible_to_real_tab_index(visible_idx) {
                    if let Some(snapshot) =
                        self.opened_tabs.get(real_idx).map(|t| t.session.clone())
                    {
                        let tabs = self.session.tabs.clone();
                        let can_preserve_editor = preserve_editor
                            && self.session.profile == snapshot.profile
                            && self.session.input == snapshot.input;
                        let preserved_editor = if can_preserve_editor {
                            Some(std::mem::take(&mut self.session.input_editor))
                        } else {
                            None
                        };

                        self.session = snapshot;
                        if let Some(input_editor) = preserved_editor {
                            self.session.input_editor = input_editor;
                        } else {
                            self.session.input_editor =
                                Self::message_editor_with_text(&self.session.input);
                        }
                        self.session.profiles = sidebar_profiles.clone();
                        self.session.selected_profile_idx = sidebar_selected;
                        self.session.profile_name_input = sidebar_input.clone();
                        self.session.sidebar_confirm = sidebar_confirm.clone();
                        self.session.groups = groups.clone();
                        self.session.selected_group_idx = selected_group_idx;
                        self.session.group_name_input = group_name_input.clone();
                        self.session.group_display_name_input = group_display_name_input.clone();
                        self.session.group_member_name_input = group_member_name_input.clone();
                        self.session.group_member_b32_input = group_member_b32_input.clone();
                        self.session.group_invite_string_input = group_invite_string_input.clone();
                        self.session.group_generated_invite_string =
                            group_generated_invite_string.clone();
                        self.session.group_private_request_string =
                            group_private_request_string.clone();
                        self.session.group_private_request_input =
                            group_private_request_input.clone();
                        self.session.group_generated_private_invite_string =
                            group_generated_private_invite_string.clone();
                        self.session.group_status = group_status.clone();
                        self.session.tabs = tabs;
                        self.session.active_tab_idx = Some(visible_idx);

                        if self.window_focused {
                            if let Some(tab) = self.opened_tabs.get_mut(real_idx) {
                                tab.meta.has_unread = false;
                            }
                        }
                    } else {
                        self.session.profiles = sidebar_profiles.clone();
                        self.session.selected_profile_idx = sidebar_selected;
                        self.session.profile_name_input = sidebar_input.clone();
                        self.session.sidebar_confirm = sidebar_confirm.clone();
                        self.session.groups = groups.clone();
                        self.session.selected_group_idx = selected_group_idx;
                        self.session.group_name_input = group_name_input.clone();
                        self.session.group_display_name_input = group_display_name_input.clone();
                        self.session.group_member_name_input = group_member_name_input.clone();
                        self.session.group_member_b32_input = group_member_b32_input.clone();
                        self.session.group_invite_string_input = group_invite_string_input.clone();
                        self.session.group_generated_invite_string =
                            group_generated_invite_string.clone();
                        self.session.group_private_request_string =
                            group_private_request_string.clone();
                        self.session.group_private_request_input =
                            group_private_request_input.clone();
                        self.session.group_generated_private_invite_string =
                            group_generated_private_invite_string.clone();
                        self.session.group_status = group_status.clone();
                        self.session.active_tab_idx = Some(0);
                        self.session.profile = "__app__".into();
                    }
                }
            }
        }
    }

    fn start_tab_runtime_task(&self, tab_idx: usize) -> Task<Message> {
        let (tab_id, tab_kind, profile_name, sam_host, sam_port, saved_dest_b64) =
            if let Some(tab) = self.opened_tabs.get(tab_idx) {
                (
                    tab.id,
                    tab.meta.kind,
                    tab.session.profile.clone(),
                    tab.sam_runtime.client.sam_host.clone(),
                    tab.sam_runtime.client.sam_port,
                    tab.group
                        .as_ref()
                        .and_then(|group| group.meta.my_dest_b64.clone())
                        .or_else(|| tab.session.my_dest_b64.clone()),
                )
            } else {
                return Task::none();
            };

        Task::perform(
            async move {
                let session_label = if tab_kind == TabKind::Group {
                    group_sam_session_label(&profile_name)
                } else {
                    profile_name
                        .chars()
                        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                        .collect()
                };
                let session_id = format!(
                    "chat_{}_{}",
                    session_label,
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
                } else if tab_kind == TabKind::Group {
                    if let Some(my_dest_b64) = saved_dest_b64 {
                        sam.initialize_persistent(session_id, my_dest_b64)
                            .await
                            .map_err(|e| e.to_string())?
                    } else {
                        sam.initialize_transient(session_id)
                            .await
                            .map_err(|e| e.to_string())?
                    }
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

    fn clear_message_draft(session: &mut SessionState) {
        session.input.clear();
        session.input_editor = text_editor::Content::new();
    }

    fn message_editor_with_text(value: &str) -> text_editor::Content {
        let mut content = text_editor::Content::with_text(value);
        content.perform(text_editor::Action::Move(text_editor::Motion::DocumentEnd));
        content
    }

    fn message_editor_key_binding(
        key_press: text_editor::KeyPress,
    ) -> Option<text_editor::Binding<Message>> {
        let is_paste = matches!(key_press.status, text_editor::Status::Focused { .. })
            && key_press.modifiers.command()
            && !key_press.modifiers.alt()
            && key_press.key.to_latin(key_press.physical_key) == Some('v');

        if is_paste {
            Some(text_editor::Binding::Custom(Message::PasteFromClipboard))
        } else {
            text_editor::Binding::from_key_press(key_press)
        }
    }

    pub fn update(state: &mut Self, message: Message) -> Task<Message> {
        if state.sam_shutdown_deadline_ms.is_some()
            && !matches!(
                &message,
                Message::Tick
                    | Message::SamShutdownNowPressed
                    | Message::WindowCloseRequested(_)
                    | Message::WindowOpened(_)
                    | Message::WindowFocusChanged(_, _)
                    | Message::ProcessShutdownRequested
                    | Message::ExitAfterNotify(_)
                    | Message::CloseFinished(_, _)
                    | Message::SamCloseFinished(_, _)
                    | Message::QuitSignalSent(_, _)
                    | Message::DeaddropClosed(_)
            )
        {
            return Task::none();
        }

        match message {
            Message::InputChanged(action) => {
                let was_empty = state.session.input.is_empty();
                state.session.input_editor.perform(action);
                state.session.input = state.session.input_editor.text();
                let should_snap_messages = was_empty && !state.session.input.is_empty();
                state.store_active_runtime();
                if should_snap_messages {
                    return operation::snap_to_end(state.session.messages_scroll_id.clone());
                }

                return Task::none();
            }

            Message::PasteFromClipboard => {
                if state.clipboard.is_none() {
                    state.post_system("Clipboard is not available.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                let clipboard_image = state
                    .clipboard
                    .as_mut()
                    .and_then(|clipboard| clipboard.get_image().ok());
                if let Some(image) = clipboard_image {
                    if !state.can_send_live_image() {
                        state.post_system("Image paste requires a live secure chat.");
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }

                    let draft = match Self::prepare_clipboard_image_draft(image) {
                        Ok(draft) => draft,
                        Err(err) => {
                            state.post_system(err);
                            return operation::snap_to_end(state.session.logs_scroll_id.clone());
                        }
                    };

                    if state.active_tab_is_group()
                        && draft.image.bytes.len() > GROUP_IMAGE_TRANSFER_MAX_BYTES
                    {
                        state.post_system(format!(
                            "Group image preview too large ({} bytes). Maximum is {} bytes.",
                            draft.image.bytes.len(),
                            GROUP_IMAGE_TRANSFER_MAX_BYTES
                        ));
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }

                    state.session.pending_image = Some(draft);
                    state.store_active_runtime();
                    return Task::none();
                }

                let clipboard_text = state
                    .clipboard
                    .as_mut()
                    .and_then(|clipboard| clipboard.get_text().ok());
                if let Some(text) = clipboard_text {
                    let was_empty = state.session.input.is_empty();
                    state
                        .session
                        .input_editor
                        .perform(text_editor::Action::Edit(text_editor::Edit::Paste(
                            std::sync::Arc::new(text),
                        )));
                    state.session.input = state.session.input_editor.text();
                    let should_snap_messages = was_empty && !state.session.input.is_empty();
                    state.store_active_runtime();
                    if should_snap_messages {
                        return operation::snap_to_end(state.session.messages_scroll_id.clone());
                    }
                }

                return Task::none();
            }

            Message::CancelPendingImagePressed => {
                state.session.pending_image = None;
                state.store_active_runtime();
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

            Message::GroupSelected(idx) => {
                if idx < state.session.groups.len() {
                    state.session.selected_group_idx = Some(idx);
                    state.session.group_display_name_input =
                        state.session.groups[idx].my_name.clone();
                    state.session.group_generated_invite_string.clear();
                    state.session.group_private_request_input.clear();
                    state.session.group_generated_private_invite_string.clear();
                    state.session.group_status.clear();
                }
                return Task::none();
            }

            Message::GroupNameInputChanged(value) => {
                state.session.group_name_input = value;
                return Task::none();
            }

            Message::GroupDisplayNameInputChanged(value) => {
                state.session.group_display_name_input = value;
                return Task::none();
            }

            Message::SaveGroupDisplayNamePressed => {
                let Some(group_idx) = state.session.selected_group_idx else {
                    state.session.group_status = "Select a group first.".into();
                    return Task::none();
                };

                let Some(mut group) = state.session.groups.get(group_idx).cloned() else {
                    state.session.group_status = "Selected group is missing.".into();
                    return Task::none();
                };

                let display_name = state.session.group_display_name_input.trim().to_string();
                if let Err(err) = Self::validate_group_display_name(&display_name) {
                    state.session.group_status = err;
                    return Task::none();
                }

                group.my_name = display_name.clone();
                let is_admin = Self::group_is_admin(&group);
                if is_admin {
                    group.roster_version = group.roster_version.saturating_add(1);
                    if let Err(err) = Self::sign_group_roster_if_admin(&mut group) {
                        state.session.group_status = format!("Group roster signing failed: {err}");
                        return Task::none();
                    }
                }

                match storage::save_group_meta(&group) {
                    Ok(()) => {
                        state.session.groups[group_idx] = group.clone();
                        state.update_open_group_roster(&group);
                        state.session.group_display_name_input = display_name.clone();
                        state.session.group_status =
                            format!("Saved group display name: {display_name}");
                        if is_admin {
                            return state.send_group_roster_sync_for_group_task(
                                &storage::group_storage_key(&group),
                            );
                        }
                        return state.send_group_rename_request_task(
                            &storage::group_storage_key(&group),
                            display_name,
                        );
                    }
                    Err(err) => {
                        state.session.group_status =
                            format!("Save group display name failed: {err}");
                    }
                }

                return Task::none();
            }

            Message::GroupMemberNameInputChanged(value) => {
                state.session.group_member_name_input = value;
                return Task::none();
            }

            Message::GroupMemberB32InputChanged(value) => {
                state.session.group_member_b32_input = value;
                return Task::none();
            }

            Message::GroupInviteStringInputChanged(value) => {
                state.session.group_invite_string_input = value;
                return Task::none();
            }

            Message::GeneratePrivateGroupRequestPressed => {
                let now_ms = Self::now_epoch_millis();
                match group_invite::generate_request(now_ms) {
                    Ok((pending_request, request_string)) => {
                        let mut pending = match storage::load_pending_private_group_invites() {
                            Ok(pending) => pending,
                            Err(err) => {
                                state.session.group_status = format!(
                                    "Load pending private group requests failed: {err}"
                                );
                                return Task::none();
                            }
                        };
                        pending.retain(|request| request.expires_ms > now_ms);
                        pending.retain(|request| request.request_id != pending_request.request_id);
                        pending.push(pending_request);
                        match storage::save_pending_private_group_invites(&pending) {
                            Ok(()) => {
                                state.session.group_private_request_string = request_string;
                                state.session.group_status =
                                    "Generated a private group invite request.".into();
                            }
                            Err(err) => {
                                state.session.group_status = format!(
                                    "Save pending private group request failed: {err}"
                                );
                            }
                        }
                    }
                    Err(err) => {
                        state.session.group_status =
                            format!("Private group request generation failed: {err}");
                    }
                }
                return Task::none();
            }

            Message::CopyPrivateGroupRequestPressed => {
                if state.session.group_private_request_string.trim().is_empty() {
                    state.session.group_status =
                        "Generate a private group request first.".into();
                    return Task::none();
                }
                state.copy_text_to_clipboard(
                    state.session.group_private_request_string.clone(),
                    "private group invite request",
                );
                state.session.group_status = "Copied private group invite request.".into();
                return Task::none();
            }

            Message::PrivateGroupRequestInputChanged(value) => {
                state.session.group_private_request_input = value;
                return Task::none();
            }

            Message::CreateGroupPressed => {
                let name = state.session.group_name_input.trim().to_string();

                if name.is_empty() {
                    state.session.group_status = "Group name cannot be empty.".into();
                    return Task::none();
                }

                if state
                    .session
                    .profiles
                    .iter()
                    .any(|profile| profile.name.eq_ignore_ascii_case(&name))
                {
                    state.session.group_status = "That name already exists.".into();
                    return Task::none();
                }

                match storage::create_group(&name) {
                    Ok(group) => {
                        let group_key = storage::group_storage_key(&group);
                        state.session.groups.push(group);
                        state.session.groups.sort_by(|a, b| {
                            a.name
                                .to_lowercase()
                                .cmp(&b.name.to_lowercase())
                                .then_with(|| {
                                    storage::group_storage_key(a)
                                        .cmp(&storage::group_storage_key(b))
                                })
                        });
                        state.session.selected_group_idx = state
                            .session
                            .groups
                            .iter()
                            .position(|group| storage::group_storage_key(group) == group_key);
                        state.session.group_display_name_input = state
                            .session
                            .selected_group_idx
                            .and_then(|idx| state.session.groups.get(idx))
                            .map(|group| group.my_name.clone())
                            .unwrap_or_default();
                        state.session.group_name_input.clear();
                        state.session.group_generated_invite_string.clear();
                        state.session.group_private_request_input.clear();
                        state.session.group_generated_private_invite_string.clear();
                        state.session.group_status = format!("Created group: {name}");
                    }
                    Err(err) => {
                        state.session.group_status = format!("Create group failed: {err}");
                    }
                }

                return Task::none();
            }

            Message::AddGroupMemberPressed => {
                let Some(group_idx) = state.session.selected_group_idx else {
                    state.session.group_status = "Select a group first.".into();
                    return Task::none();
                };

                if group_idx >= state.session.groups.len() {
                    state.session.group_status = "Selected group is missing.".into();
                    return Task::none();
                }

                let member_name = state.session.group_member_name_input.trim().to_string();
                let member_b32 = state.session.group_member_b32_input.trim().to_lowercase();

                if member_name.is_empty() {
                    state.session.group_status = "Member name cannot be empty.".into();
                    return Task::none();
                }

                if !Self::is_valid_b32_address(&member_b32) {
                    state.session.group_status = "Member address must be a b32.i2p address.".into();
                    return Task::none();
                }

                let mut group = state.session.groups[group_idx].clone();
                if !Self::group_is_admin(&group) {
                    state.session.group_status = "Only the group admin can add members.".into();
                    return Task::none();
                }

                if group
                    .members
                    .iter()
                    .any(|member| member.b32.eq_ignore_ascii_case(&member_b32))
                {
                    state.session.group_status = "That member address already exists.".into();
                    return Task::none();
                }

                group.members.push(GroupMember {
                    name: member_name.clone(),
                    b32: member_b32.clone(),
                });
                group.roster_version = group.roster_version.saturating_add(1);
                if let Err(err) = Self::sign_group_roster_if_admin(&mut group) {
                    state.session.group_status = format!("Group roster signing failed: {err}");
                    return Task::none();
                }

                match storage::save_group_meta(&group) {
                    Ok(()) => {
                        state.session.groups[group_idx] = group;
                        let updated_group = state.session.groups[group_idx].clone();
                        state.update_open_group_roster(&updated_group);
                        state.session.group_member_name_input.clear();
                        state.session.group_member_b32_input.clear();
                        state.session.group_status =
                            format!("Added {member_name} to group roster.");
                        return state.send_group_roster_sync_for_group_task(
                            &storage::group_storage_key(&updated_group),
                        );
                    }
                    Err(err) => {
                        state.session.group_status = format!("Save group failed: {err}");
                    }
                }

                return Task::none();
            }

            Message::DeleteGroupMemberPressed(member_idx) => {
                let Some(group_idx) = state.session.selected_group_idx else {
                    state.session.group_status = "Select a group first.".into();
                    return Task::none();
                };

                let Some(group) = state.session.groups.get(group_idx).cloned() else {
                    state.session.group_status = "Selected group is missing.".into();
                    return Task::none();
                };

                if !Self::group_is_admin(&group) {
                    state.session.group_status = "Only the group admin can delete members.".into();
                    return Task::none();
                }

                if member_idx >= group.members.len() {
                    state.session.group_status = "Selected member is missing.".into();
                    return Task::none();
                }

                let member = group.members[member_idx].clone();
                state.session.sidebar_confirm = Some(SidebarConfirm::DeleteGroupMember {
                    group_key: storage::group_storage_key(&group),
                    member_b32: member.b32,
                    member_name: member.name,
                });

                return Task::none();
            }

            Message::DeleteGroupPressed => {
                let Some(group_idx) = state.session.selected_group_idx else {
                    state.session.group_status = "Select a group first.".into();
                    return Task::none();
                };

                let Some(group) = state.session.groups.get(group_idx).cloned() else {
                    state.session.group_status = "Selected group is missing.".into();
                    return Task::none();
                };

                let group_key = storage::group_storage_key(&group);
                if state.is_group_open_in_any_tab(&group_key) {
                    state.session.group_status =
                        format!("Close #{} before deleting the group.", group.name);
                    return Task::none();
                }

                state.session.sidebar_confirm = Some(SidebarConfirm::DeleteGroup {
                    key: group_key,
                    name: group.name,
                });

                return Task::none();
            }

            Message::ExportGroupInvitePressed => {
                let Some(group_idx) = state.session.selected_group_idx else {
                    state.session.group_status = "Select a group first.".into();
                    return Task::none();
                };

                let Some(group) = state.session.groups.get(group_idx) else {
                    state.session.group_status = "Selected group is missing.".into();
                    return Task::none();
                };

                if group.my_b32.is_none() {
                    state.session.group_status =
                        "Open this group once before exporting its invite.".into();
                    return Task::none();
                }

                let file_name = format!("{}-group-invite.tcginvite", group.name);
                return Task::perform(
                    async move {
                        rfd::AsyncFileDialog::new()
                            .add_filter("IcedComm-I2P group invite", &["tcginvite"])
                            .set_file_name(file_name)
                            .save_file()
                            .await
                            .map(|f| f.path().to_path_buf())
                    },
                    Message::GroupInviteExportPathChosen,
                );
            }

            Message::ImportGroupInvitePressed => {
                return Task::perform(
                    async move {
                        rfd::AsyncFileDialog::new()
                            .add_filter("IcedComm-I2P group invite", &["tcginvite", "json"])
                            .pick_file()
                            .await
                            .map(|f| f.path().to_path_buf())
                    },
                    Message::GroupInviteImportPathChosen,
                );
            }

            Message::GenerateGroupInvitePressed | Message::CopyGroupInviteStringPressed => {
                let Some(group_idx) = state.session.selected_group_idx else {
                    state.session.group_status = "Select a group first.".into();
                    return Task::none();
                };

                let Some(group) = state.session.groups.get(group_idx).cloned() else {
                    state.session.group_status = "Selected group is missing.".into();
                    return Task::none();
                };

                if !Self::group_is_admin(&group) {
                    state.session.group_status =
                        "Only the group admin can generate invites.".into();
                    return Task::none();
                }

                match Self::encode_group_invite_string(&group) {
                    Ok((updated_group, invite_string)) => {
                        if let Some(idx) = state.session.groups.iter().position(|existing| {
                            storage::group_storage_key(existing)
                                == storage::group_storage_key(&updated_group)
                        }) {
                            state.session.groups[idx] = updated_group.clone();
                        }
                        state.update_open_group_roster(&updated_group);
                        state.session.group_generated_invite_string = invite_string;
                        state.session.group_status =
                            "Generated new single-use group invite.".into();
                    }
                    Err(err) => {
                        state.session.group_status =
                            format!("Group invite string export failed: {err}");
                    }
                }

                return Task::none();
            }

            Message::CopyGeneratedGroupInvitePressed => {
                if state
                    .session
                    .group_generated_invite_string
                    .trim()
                    .is_empty()
                {
                    state.session.group_status = "Generate a group invite first.".into();
                    return Task::none();
                }

                state.copy_text_to_clipboard(
                    state.session.group_generated_invite_string.clone(),
                    "new group invite",
                );
                state.session.group_status = "Copied generated group invite.".into();
                return Task::none();
            }

            Message::GeneratePrivateGroupInvitePressed => {
                let Some(group_idx) = state.session.selected_group_idx else {
                    state.session.group_status = "Select a group first.".into();
                    return Task::none();
                };
                let Some(group) = state.session.groups.get(group_idx).cloned() else {
                    state.session.group_status = "Selected group is missing.".into();
                    return Task::none();
                };
                if !Self::group_is_admin(&group) {
                    state.session.group_status =
                        "Only the group admin can generate private invites.".into();
                    return Task::none();
                }

                let request = state.session.group_private_request_input.trim().to_string();
                if request.is_empty() {
                    state.session.group_status =
                        "Paste a private group request first.".into();
                    return Task::none();
                }

                match Self::encode_private_group_invite_string(&group, &request) {
                    Ok((updated_group, invite_string)) => {
                        if let Some(idx) = state.session.groups.iter().position(|existing| {
                            storage::group_storage_key(existing)
                                == storage::group_storage_key(&updated_group)
                        }) {
                            state.session.groups[idx] = updated_group.clone();
                        }
                        state.update_open_group_roster(&updated_group);
                        state.session.group_generated_private_invite_string = invite_string;
                        state.session.group_status =
                            "Generated a recipient-bound private group invite.".into();
                    }
                    Err(err) => {
                        state.session.group_status =
                            format!("Private group invite generation failed: {err}");
                    }
                }
                return Task::none();
            }

            Message::CopyGeneratedPrivateGroupInvitePressed => {
                if state
                    .session
                    .group_generated_private_invite_string
                    .trim()
                    .is_empty()
                {
                    state.session.group_status =
                        "Generate a private group invite first.".into();
                    return Task::none();
                }
                state.copy_text_to_clipboard(
                    state.session.group_generated_private_invite_string.clone(),
                    "private group invite",
                );
                state.session.group_status = "Copied private group invite.".into();
                return Task::none();
            }

            Message::RevokePrivateGroupInvitePressed(request_id) => {
                let Some(group_idx) = state.session.selected_group_idx else {
                    state.session.group_status = "Select a group first.".into();
                    return Task::none();
                };
                let Some(mut group) = state.session.groups.get(group_idx).cloned() else {
                    state.session.group_status = "Selected group is missing.".into();
                    return Task::none();
                };
                if !Self::group_is_admin(&group) {
                    state.session.group_status =
                        "Only the group admin can revoke private invites.".into();
                    return Task::none();
                }

                let before = group.issued_invites.len();
                group.issued_invites.retain(|issued| {
                    issued
                        .private_binding
                        .as_ref()
                        .map(|binding| binding.request_id != request_id)
                        .unwrap_or(true)
                });
                if group.issued_invites.len() == before {
                    state.session.group_status = "Private invite is no longer pending.".into();
                    return Task::none();
                }

                match storage::save_group_meta(&group) {
                    Ok(()) => {
                        state.session.groups[group_idx] = group.clone();
                        state.update_open_group_roster(&group);
                        if group_invite::response_request_id(
                            &state.session.group_generated_private_invite_string,
                        )
                        .map(|generated_id| generated_id == request_id)
                        .unwrap_or(false)
                        {
                            state.session.group_generated_private_invite_string.clear();
                        }
                        state.session.group_status = "Revoked private group invite.".into();
                    }
                    Err(err) => {
                        state.session.group_status =
                            format!("Private group invite revocation failed: {err}");
                    }
                }
                return Task::none();
            }

            Message::ImportGroupInviteStringPressed => {
                let invite_string = state.session.group_invite_string_input.trim().to_string();

                if invite_string.is_empty() {
                    state.session.group_status = "Paste a group invite string first.".into();
                    return Task::none();
                }

                let import_result = match group_invite::input_kind(
                    &invite_string,
                    GROUP_INVITE_STRING_PREFIX,
                ) {
                    group_invite::InputKind::Shareable => {
                        Self::import_group_invite_string(&invite_string)
                    }
                    group_invite::InputKind::Private => {
                        Self::import_private_group_invite_string(&invite_string)
                    }
                    group_invite::InputKind::Unknown => {
                        Err("group invite string has an unsupported prefix".into())
                    }
                };

                match import_result {
                    Ok(group_key) => match storage::load_groups() {
                        Ok(groups) => {
                            state.session.groups = groups;
                            state.session.selected_group_idx =
                                state.session.groups.iter().position(|group| {
                                    storage::group_storage_key(group) == group_key
                                });
                            state.session.group_display_name_input = state
                                .session
                                .selected_group_idx
                                .and_then(|idx| state.session.groups.get(idx))
                                .map(|group| group.my_name.clone())
                                .unwrap_or_default();
                            state.session.group_generated_invite_string.clear();
                            state.session.group_private_request_input.clear();
                            state.session.group_generated_private_invite_string.clear();
                            if let Some(group) = state
                                .session
                                .groups
                                .iter()
                                .find(|group| storage::group_storage_key(group) == group_key)
                                .cloned()
                            {
                                state.update_open_group_roster(&group);
                            }
                            let display_name = state
                                .session
                                .selected_group_idx
                                .and_then(|idx| state.session.groups.get(idx))
                                .map(|group| group.name.clone())
                                .unwrap_or_else(|| group_key.clone());
                            state.session.group_invite_string_input.clear();
                            state.session.group_status =
                                format!("Imported group invite string: {display_name}");
                            return state.send_group_roster_sync_for_group_task(&group_key);
                        }
                        Err(err) => {
                            state.session.group_status = format!("Reload groups failed: {err}");
                        }
                    },
                    Err(err) => {
                        state.session.group_status =
                            format!("Group invite string import failed: {err}");
                    }
                }

                return Task::none();
            }

            Message::OpenGroupPressed => {
                let Some(group_idx) = state.session.selected_group_idx else {
                    state.session.group_status = "Select a group first.".into();
                    return Task::none();
                };

                let Some(group) = state.session.groups.get(group_idx).cloned() else {
                    state.session.group_status = "Selected group is missing.".into();
                    return Task::none();
                };

                let group_key = storage::group_storage_key(&group);
                state.open_or_focus_tab_for_group(&group_key);

                if let Some(visible_idx) = state.session.active_tab_idx {
                    if let Some(real_idx) = Self::visible_to_real_tab_index(visible_idx) {
                        return Task::batch(vec![
                            operation::snap_to_end(state.session.logs_scroll_id.clone()),
                            state.ensure_tab_runtime_started(real_idx),
                        ]);
                    }
                }

                return Task::none();
            }

            Message::GroupInviteExportPathChosen(path_opt) => {
                let Some(path) = path_opt else {
                    state.session.group_status = "Group invite export cancelled.".into();
                    return Task::none();
                };

                let Some(group_idx) = state.session.selected_group_idx else {
                    state.session.group_status = "Select a group first.".into();
                    return Task::none();
                };

                let Some(group) = state.session.groups.get(group_idx).cloned() else {
                    state.session.group_status = "Selected group is missing.".into();
                    return Task::none();
                };

                return Task::perform(
                    async move {
                        Self::export_group_invite_file(&path, &group)
                            .map(|updated_group| (path, updated_group))
                    },
                    Message::GroupInviteExportFinished,
                );
            }

            Message::GroupInviteImportPathChosen(path_opt) => {
                let Some(path) = path_opt else {
                    state.session.group_status = "Group invite import cancelled.".into();
                    return Task::none();
                };

                return Task::perform(
                    async move { Self::import_group_invite_file(&path) },
                    Message::GroupInviteImportFinished,
                );
            }

            Message::GroupInviteExportFinished(result) => {
                state.session.group_status = match result {
                    Ok((path, updated_group)) => {
                        if let Some(idx) = state.session.groups.iter().position(|group| {
                            storage::group_storage_key(group)
                                == storage::group_storage_key(&updated_group)
                        }) {
                            state.session.groups[idx] = updated_group.clone();
                        }
                        state.update_open_group_roster(&updated_group);
                        format!("Exported group invite to {}", path.display())
                    }
                    Err(err) => format!("Group invite export failed: {err}"),
                };
                return Task::none();
            }

            Message::GroupInviteImportFinished(result) => {
                match result {
                    Ok(group_key) => match storage::load_groups() {
                        Ok(groups) => {
                            state.session.groups = groups;
                            state.session.selected_group_idx =
                                state.session.groups.iter().position(|group| {
                                    storage::group_storage_key(group) == group_key
                                });
                            state.session.group_display_name_input = state
                                .session
                                .selected_group_idx
                                .and_then(|idx| state.session.groups.get(idx))
                                .map(|group| group.my_name.clone())
                                .unwrap_or_default();
                            state.session.group_generated_invite_string.clear();
                            state.session.group_private_request_input.clear();
                            state.session.group_generated_private_invite_string.clear();
                            if let Some(group) = state
                                .session
                                .groups
                                .iter()
                                .find(|group| storage::group_storage_key(group) == group_key)
                                .cloned()
                            {
                                state.update_open_group_roster(&group);
                            }
                            let display_name = state
                                .session
                                .selected_group_idx
                                .and_then(|idx| state.session.groups.get(idx))
                                .map(|group| group.name.clone())
                                .unwrap_or_else(|| group_key.clone());
                            state.session.group_status =
                                format!("Imported group invite: {display_name}");
                            return state.send_group_roster_sync_for_group_task(&group_key);
                        }
                        Err(err) => {
                            state.session.group_status = format!("Reload groups failed: {err}");
                        }
                    },
                    Err(err) => {
                        state.session.group_status = format!("Group invite import failed: {err}");
                    }
                }
                return Task::none();
            }

            Message::TabSelected(idx) => {
                state.store_active_runtime();

                if idx == 0 {
                    state.session.active_tab_idx = Some(0);
                    state.session.profile = "__app__".into();
                    state.refresh_visible_from_active_tab_reset_editor();
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                let real_idx = idx - 1;

                if real_idx < state.opened_tabs.len() {
                    if state.opened_tabs[real_idx].sam_runtime.is_closing() {
                        return Task::none();
                    }

                    state.session.active_tab_idx = Some(idx);

                    if state.window_focused {
                        if let Some(tab) = state.opened_tabs.get_mut(real_idx) {
                            tab.meta.has_unread = false;
                        }
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

                    if state.opened_tabs[real_idx].meta.kind == TabKind::Group {
                        if let Some(group_key) = state.opened_tabs[real_idx]
                            .meta
                            .profile_name
                            .strip_prefix("group:")
                        {
                            state.session.selected_group_idx =
                                state.session.groups.iter().position(|group| {
                                    storage::group_storage_key(group) == group_key
                                });
                            state.session.group_display_name_input = state
                                .session
                                .selected_group_idx
                                .and_then(|idx| state.session.groups.get(idx))
                                .map(|group| group.my_name.clone())
                                .unwrap_or_default();
                        }
                    }

                    state.refresh_visible_from_active_tab_reset_editor();

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
                    if state.opened_tabs[idx].sam_runtime.is_closing() {
                        return Task::none();
                    }

                    state.store_active_runtime();

                    if let Some(tab) = state.opened_tabs.get_mut(idx) {
                        tab.sam_runtime.begin_closing();
                        tab.meta.closing = true;
                    }

                    let mut tasks = state.close_tab_runtime_tasks(idx);
                    let tab_id = state.opened_tabs[idx].id;

                    if let Some(tab) = state.opened_tabs.get_mut(idx) {
                        tab.live_conn = None;
                        tab.pending_conn = None;
                        tab.connect_in_flight = false;
                        tab.connect_peer = None;
                        tab.connection_direction = None;
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
                        tab.meta.closing = true;
                        tab.meta.initialized = false;
                        tab.meta.initializing = false;

                        tab.e2e = E2E::new(tab.session.pq_enabled);

                        if let Some(group) = tab.group.as_mut() {
                            group.publish_ready = false;
                            for peer in &mut group.peers {
                                Self::reset_group_peer_transport_state(peer);
                            }
                            group.accept_armed = false;
                        }

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

                    tasks.push(Task::perform(
                        async move {
                            sleep(Duration::from_millis(
                                SHUTDOWN_NOTIFY_GRACE_MS + SAM_CONNECT_CANCEL_GRACE_MS,
                            ))
                            .await;
                            tab_id
                        },
                        Message::FinalizeTabClosed,
                    ));

                    state.sync_active_tab_flags();
                    return Task::batch(tasks);
                }

                return Task::none();
            }

            Message::FinalizeTabClosed(tab_id) => {
                if let Some(idx) = state.find_tab_index_by_id(tab_id) {
                    if let Some(tab) = state.opened_tabs.get_mut(idx) {
                        tab.sam_runtime.clear_shutdown_state();
                        tab.meta.closing = false;
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
                                    groups: state.session.groups.clone(),
                                    selected_group_idx: state.session.selected_group_idx,
                                    group_name_input: state.session.group_name_input.clone(),
                                    group_display_name_input: state
                                        .session
                                        .group_display_name_input
                                        .clone(),
                                    group_member_name_input: state
                                        .session
                                        .group_member_name_input
                                        .clone(),
                                    group_member_b32_input: state
                                        .session
                                        .group_member_b32_input
                                        .clone(),
                                    group_invite_string_input: state
                                        .session
                                        .group_invite_string_input
                                        .clone(),
                                    group_generated_invite_string: state
                                        .session
                                        .group_generated_invite_string
                                        .clone(),
                                    group_private_request_string: state
                                        .session
                                        .group_private_request_string
                                        .clone(),
                                    group_private_request_input: state
                                        .session
                                        .group_private_request_input
                                        .clone(),
                                    group_generated_private_invite_string: state
                                        .session
                                        .group_generated_private_invite_string
                                        .clone(),
                                    group_status: state.session.group_status.clone(),
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

                    state.refresh_visible_from_active_tab_reset_editor();
                    return Task::none();
                }

                return Task::none();
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
                    || state
                        .session
                        .groups
                        .iter()
                        .any(|group| group.name.eq_ignore_ascii_case(&name))
                {
                    state.post_system("That name already exists.");
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
                    SidebarConfirm::DeleteGroup { key, name } => {
                        if state.is_group_open_in_any_tab(&key) {
                            state.session.group_status =
                                format!("Close #{name} before deleting the group.");
                            return Task::none();
                        }

                        match storage::delete_group(&key) {
                            Ok(()) => {
                                if let Some(group_idx) = state
                                    .session
                                    .groups
                                    .iter()
                                    .position(|group| storage::group_storage_key(group) == key)
                                {
                                    state.session.groups.remove(group_idx);
                                    state.session.selected_group_idx =
                                        if state.session.groups.is_empty() {
                                            None
                                        } else {
                                            Some(group_idx.min(state.session.groups.len() - 1))
                                        };
                                    state.session.group_display_name_input = state
                                        .session
                                        .selected_group_idx
                                        .and_then(|idx| state.session.groups.get(idx))
                                        .map(|group| group.my_name.clone())
                                        .unwrap_or_default();
                                }
                                state.session.group_generated_invite_string.clear();
                                state.session.group_private_request_input.clear();
                                state.session.group_generated_private_invite_string.clear();
                                state.session.group_status = format!("Deleted group: {name}");
                                return Task::none();
                            }
                            Err(err) => {
                                state.session.group_status = format!("Delete group failed: {err}");
                                return Task::none();
                            }
                        }
                    }
                    SidebarConfirm::DeleteGroupMember {
                        group_key,
                        member_b32,
                        member_name,
                    } => {
                        let Some(group_idx) = state
                            .session
                            .groups
                            .iter()
                            .position(|group| storage::group_storage_key(group) == group_key)
                        else {
                            state.session.group_status = "Selected group is missing.".into();
                            return Task::none();
                        };

                        let Some(mut group) = state.session.groups.get(group_idx).cloned() else {
                            state.session.group_status = "Selected group is missing.".into();
                            return Task::none();
                        };

                        if !Self::group_is_admin(&group) {
                            state.session.group_status =
                                "Only the group admin can delete members.".into();
                            return Task::none();
                        }

                        let Some(member_idx) = group
                            .members
                            .iter()
                            .position(|member| member.b32.eq_ignore_ascii_case(&member_b32))
                        else {
                            state.session.group_status = "Selected member is missing.".into();
                            return Task::none();
                        };

                        let removed = group.members.remove(member_idx);
                        group.issued_invites.retain(|invite| {
                            !invite
                                .redeemed_b32
                                .as_deref()
                                .map(|b32| b32.eq_ignore_ascii_case(&removed.b32))
                                .unwrap_or(false)
                        });
                        group.roster_version = group.roster_version.saturating_add(1);
                        if let Err(err) = Self::sign_group_roster_if_admin(&mut group) {
                            state.session.group_status =
                                format!("Group roster signing failed: {err}");
                            return Task::none();
                        }

                        match storage::save_group_meta(&group) {
                            Ok(()) => {
                                state.session.groups[group_idx] = group.clone();
                                state.update_open_group_roster(&group);
                                state.session.group_status =
                                    format!("Deleted member: {member_name}");
                                return state.send_group_roster_sync_for_group_task(
                                    &storage::group_storage_key(&group),
                                );
                            }
                            Err(err) => {
                                state.session.group_status = format!("Save group failed: {err}");
                                return Task::none();
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
                if state.can_send_live_image() {
                    if let Some(draft) = state.session.pending_image.clone() {
                        let send_task = state.send_prepared_image(
                            draft.filename,
                            draft.mime,
                            draft.image.bytes,
                        );
                        return match send_task {
                            Ok(task) => {
                                state.session.pending_image = None;
                                state.store_active_runtime();
                                task
                            }
                            Err(err) => {
                                state.post_system(err);
                                operation::snap_to_end(state.session.logs_scroll_id.clone())
                            }
                        };
                    }
                }

                let draft_text = state.session.input.clone();

                if !draft_text.trim().is_empty() {
                    let outgoing_text =
                        Self::compose_reply_text(state.session.reply_to.as_ref(), &draft_text);

                    if state.active_tab_is_group() {
                        let tab_id = match state.active_tab() {
                            Some(tab) => tab.id,
                            None => return Task::none(),
                        };

                        let msg_id = state.generate_msg_id();
                        let mut tasks = Vec::new();
                        let mut sent_count = 0usize;
                        let mut expected_acks = Vec::new();

                        if let Some(tab) = state.active_tab_mut() {
                            let sam_runtime = tab.sam_runtime.clone();
                            let Some(group) = tab.group.as_mut() else {
                                return Task::none();
                            };

                            for peer in &group.peers {
                                if !peer.ready || !peer.authorized {
                                    continue;
                                }

                                let Some(conn) = peer.conn.clone() else {
                                    continue;
                                };

                                let frame = Frame {
                                    msg_type: MsgType::U,
                                    msg_id,
                                    payload: peer.e2e.encrypt(outgoing_text.as_bytes()),
                                };
                                sent_count += 1;
                                expected_acks.push(peer.member.b32.to_ascii_lowercase());
                                let task = Task::perform(
                                    async move {
                                        conn.send_frame(&frame).await.map_err(|e| e.to_string())
                                    },
                                    move |result| Message::SendFinished(tab_id, result),
                                );
                                tasks.push(sam_runtime.track_send_task(task));
                            }
                        }

                        if sent_count == 0 {
                            state.post_system("No ready group members.");
                            state.store_active_runtime();
                            return operation::snap_to_end(state.session.logs_scroll_id.clone());
                        }

                        state.session.bubbles.push(Bubble::group_me_with_id(
                            outgoing_text.clone(),
                            msg_id,
                            expected_acks,
                        ));
                        Self::clear_message_draft(&mut state.session);
                        state.session.reply_to = None;
                        state.store_active_runtime();

                        tasks.push(operation::snap_to_end(
                            state.session.messages_scroll_id.clone(),
                        ));
                        return Task::batch(tasks);
                    }

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
                            .map(|t| t.e2e.encrypt(outgoing_text.as_bytes()))
                            .unwrap_or_else(|| outgoing_text.as_bytes().to_vec());

                        let msg_id = state.generate_msg_id();

                        let frame = Frame {
                            msg_type: MsgType::U,
                            msg_id,
                            payload: enc_payload,
                        };

                        state
                            .session
                            .bubbles
                            .push(Bubble::me_with_id(outgoing_text, msg_id));

                        Self::clear_message_draft(&mut state.session);
                        state.session.reply_to = None;
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
                                payload: tab.e2e.encrypt(outgoing_text.as_bytes()),
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

                        state.session.bubbles.push(Bubble::me_offline_with_id(
                            outgoing_text.clone(),
                            offline_msg_id,
                        ));

                        Self::clear_message_draft(&mut state.session);
                        state.session.reply_to = None;
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
                    if state.session.profile == "default"
                        && state.session.show_rendezvous_panel
                    {
                        return Task::none();
                    }

                    if state.session.profile != "default" {
                        if let Some(peer) = state.session.stored_peer.clone() {
                            let Some(connect_task) =
                                state.start_one_to_one_connect(peer.clone())
                            else {
                                return Task::none();
                            };

                            state.session.pending_action = None;
                            state.session.action_param.clear();
                            state.session.network_status = NetworkStatus::Visible;
                            state.post_system(format!("Connecting to locked peer {peer}..."));
                            state.store_active_runtime();

                            return connect_task;
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

                        let close_conn = conn.clone();
                        tasks.push(Task::perform(
                            async move { close_conn.close().await.map_err(|e| e.to_string()) },
                            move |result| Message::CloseFinished(tab_id, result),
                        ));
                    }

                    if let Some(conn) = pending {
                        let close_conn = conn.clone();
                        tasks.push(Task::perform(
                            async move { close_conn.close().await.map_err(|e| e.to_string()) },
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
                        let close_conn = conn.clone();
                        tasks.push(Task::perform(
                            async move { close_conn.close().await.map_err(|e| e.to_string()) },
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
                    if state.active_tab_is_group() {
                        if state.active_group_ready_count() == 0 {
                            state.post_system("Image send requires a ready group member.");
                            return operation::snap_to_end(state.session.logs_scroll_id.clone());
                        }
                    } else if !state.session.live_ready {
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

                    let show_deaddrop_panel = !state.session.show_deaddrop_panel;
                    state.session.show_deaddrop_panel = show_deaddrop_panel;
                    if show_deaddrop_panel {
                        state.session.show_logs = false;
                        state.session.show_group_panel = false;
                    }
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

            Message::CopyBubbleTextPressed(idx) => {
                let Some(text) = state.session.bubbles.get(idx).and_then(|bubble| {
                    if let BubbleContent::Text(value) = &bubble.content {
                        Some(display_reply_text(value))
                    } else {
                        None
                    }
                }) else {
                    state.post_system("Message text is not available.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                };

                state.copy_text_to_clipboard(text, "message text");
                state.store_active_runtime();
                return operation::snap_to_end(state.session.logs_scroll_id.clone());
            }

            Message::CopyLogsPressed => {
                let line_count = state.session.log_lines.len();
                if line_count == 0 {
                    return Task::none();
                }

                let contents = state.session.log_lines.joined();
                let copy_result = match state.clipboard.as_mut() {
                    Some(clipboard) => clipboard.set_text(contents).map_err(|err| err.to_string()),
                    None => Err("Clipboard is not available.".to_string()),
                };

                match copy_result {
                    Ok(()) => state.post_system(format!("Copied {line_count} log lines.")),
                    Err(err) => state.post_system(format!("Clipboard copy failed: {err}")),
                }
                state.store_active_runtime();
                return operation::snap_to_end(state.session.logs_scroll_id.clone());
            }

            Message::ReplyBubblePressed(idx) => {
                let Some((author, text)) = state.session.bubbles.get(idx).and_then(|bubble| {
                    if let BubbleContent::Text(value) = &bubble.content {
                        Some((bubble.author.clone(), reply_source_text(value)))
                    } else {
                        None
                    }
                }) else {
                    state.post_system("Message text is not available for reply.");
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                };

                state.session.reply_to = Some(ReplyDraft { author, text });
                state.store_active_runtime();
                return operation::snap_to_end(state.session.messages_scroll_id.clone());
            }

            Message::CancelReplyPressed => {
                state.session.reply_to = None;
                state.store_active_runtime();
                return Task::none();
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
                                let peer = value.clone();
                                state.session.rendezvous_outgoing = None;
                                let Some(connect_task) =
                                    state.start_one_to_one_connect(peer.clone())
                                else {
                                    return Task::none();
                                };

                                state.session.pending_action = None;
                                state.session.action_param.clear();
                                state.session.network_status = NetworkStatus::Visible;
                                state.post_system(format!("Connecting to {peer}..."));
                                state.store_active_runtime();

                                return connect_task;
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
                            state.session.known_remote_next_send = 0;
                            state.session.highest_authenticated_recv_index = None;
                            state.session.missing_drop_recv.clear();
                            state.session.skipped_drop_recv.clear();
                            state.session.forward_probe_index = 0;
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
                                    state.sync_offline_secret_if_needed_task(tab_id)
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
                let show_logs = !state.session.show_logs;
                state.session.show_logs = show_logs;
                if show_logs {
                    state.session.show_deaddrop_panel = false;
                    state.session.show_group_panel = false;
                }
                state.store_active_runtime();
                if state.session.show_logs {
                    return Task::batch(vec![
                        operation::snap_to_end(state.session.messages_scroll_id.clone()),
                        operation::snap_to_end(state.session.logs_scroll_id.clone()),
                    ]);
                }

                return operation::snap_to_end(state.session.messages_scroll_id.clone());
            }

            Message::ToggleGroupPanelPressed => {
                let show_group_panel = !state.session.show_group_panel;
                state.session.show_group_panel = show_group_panel;
                if show_group_panel {
                    state.session.show_logs = false;
                    state.session.show_deaddrop_panel = false;
                }
                state.store_active_runtime();
                return Task::none();
            }

            Message::ToggleRendezvousPanelPressed => {
                if state.session.profile == "default"
                    && !state.active_tab_is_group()
                    && !state.session.offline_mode
                    && !state.has_active_connection_attempt()
                    && state.session.pending_action != Some(GuiAction::Connect)
                {
                    state.session.show_rendezvous_panel =
                        !state.session.show_rendezvous_panel;
                    state.store_active_runtime();
                }
                return Task::none();
            }

            Message::RendezvousInputChanged(value) => {
                if value.len() <= 16 * 1_024 {
                    state.session.rendezvous_input = value;
                    state.store_active_runtime();
                }
                return Task::none();
            }

            Message::GenerateRendezvousRequestPressed => {
                if state.session.profile != "default" || state.active_tab_is_group() {
                    return Task::none();
                }

                match rendezvous::generate_request(Self::now_epoch_millis()) {
                    Ok((request, encoded)) => {
                        state.session.rendezvous_request = Some(request);
                        state.session.rendezvous_outgoing = None;
                        state.session.rendezvous_output = encoded;
                        state.session.rendezvous_status =
                            "One-time request generated. Send it through the separate channel."
                                .into();
                    }
                    Err(err) => {
                        state.session.rendezvous_status =
                            format!("Rendezvous request generation failed: {err}");
                    }
                }
                state.store_active_runtime();
                return Task::none();
            }

            Message::AnswerRendezvousRequestPressed => {
                if state.session.profile != "default" || state.active_tab_is_group() {
                    return Task::none();
                }
                let Some(my_b32) = state.session.my_b32.clone() else {
                    state.session.rendezvous_status =
                        "Transient address is not ready yet.".into();
                    state.store_active_runtime();
                    return Task::none();
                };
                let encoded = state.session.rendezvous_input.trim().to_string();
                match rendezvous::answer_request(&encoded, &my_b32, Self::now_epoch_millis()) {
                    Ok((issued, response)) => {
                        state.session.rendezvous_issued = Some(issued);
                        state.session.rendezvous_output = response;
                        state.session.rendezvous_status =
                            "Sealed one-time response generated. Return it to the requester."
                                .into();
                    }
                    Err(err) => {
                        state.session.rendezvous_status =
                            format!("Rendezvous request rejected: {err}");
                    }
                }
                state.store_active_runtime();
                return Task::none();
            }

            Message::ConnectRendezvousResponsePressed => {
                if state.session.profile != "default" || state.active_tab_is_group() {
                    return Task::none();
                }
                let Some(request) = state.session.rendezvous_request.as_ref() else {
                    state.session.rendezvous_status =
                        "Generate a request before importing its response.".into();
                    state.store_active_runtime();
                    return Task::none();
                };
                let encoded = state.session.rendezvous_input.trim().to_string();
                match rendezvous::open_response(&encoded, request, Self::now_epoch_millis()) {
                    Ok(access) if Self::is_valid_b32_address(&access.destination_b32) => {
                        let peer = access.destination_b32.clone();
                        state.session.rendezvous_outgoing = Some(access);
                        let Some(task) = state.start_one_to_one_connect(peer.clone()) else {
                            state.session.rendezvous_status =
                                "Close the current call before using a rendezvous response."
                                    .into();
                            state.store_active_runtime();
                            return Task::none();
                        };
                        state.session.rendezvous_request = None;
                        state.session.rendezvous_input.clear();
                        state.session.rendezvous_output.clear();
                        state.session.show_rendezvous_panel = false;
                        state.session.rendezvous_status =
                            "Connecting with one-time rendezvous authentication...".into();
                        state.session.network_status = NetworkStatus::Visible;
                        state.post_system(format!("Connecting to {peer}..."));
                        state.store_active_runtime();
                        return task;
                    }
                    Ok(_) => {
                        state.session.rendezvous_status =
                            "Rendezvous response contains an invalid destination.".into();
                    }
                    Err(err) => {
                        state.session.rendezvous_status =
                            format!("Rendezvous response rejected: {err}");
                    }
                }
                state.store_active_runtime();
                return Task::none();
            }

            Message::CopyRendezvousOutputPressed => {
                if !state.session.rendezvous_output.is_empty() {
                    state.copy_text_to_clipboard(
                        state.session.rendezvous_output.clone(),
                        "rendezvous value",
                    );
                    state.session.rendezvous_status = "Rendezvous value copied.".into();
                    state.store_active_runtime();
                }
                return Task::none();
            }

            Message::ClearRendezvousPressed => {
                state.session.rendezvous_input.clear();
                state.session.rendezvous_output.clear();
                state.session.rendezvous_status =
                    "Text fields cleared; active one-time state is unchanged.".into();
                state.store_active_runtime();
                return Task::none();
            }

            Message::RevokeRendezvousPressed => {
                if let Some(issued) = state.session.rendezvous_issued.as_mut() {
                    issued.state = RendezvousIssuedState::Revoked;
                }
                state.session.rendezvous_request = None;
                state.session.rendezvous_outgoing = None;
                state.session.pending_rendezvous_request_id = None;
                state.session.rendezvous_input.clear();
                state.session.rendezvous_output.clear();
                state.session.rendezvous_status = "One-time rendezvous state revoked.".into();
                state.store_active_runtime();
                return Task::none();
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

                state.session.deaddrop_delete_confirm = Some(DdServerDeleteConfirm {
                    index,
                    server: state.session.deaddrop_servers[index].clone(),
                });
                state.store_active_runtime();
                return Task::none();
            }

            Message::DdServerDeleteConfirmed => {
                let Some(confirm) = state.session.deaddrop_delete_confirm.clone() else {
                    return Task::none();
                };
                state.session.deaddrop_delete_confirm = None;

                if !Self::deaddrop_panel_allowed(&state.session) {
                    state.post_system(
                        "Deaddrop servers are available only for persistent locked profiles.",
                    );
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                }

                let remove_index = if state
                    .session
                    .deaddrop_servers
                    .get(confirm.index)
                    .map(|server| server == &confirm.server)
                    .unwrap_or(false)
                {
                    Some(confirm.index)
                } else {
                    state
                        .session
                        .deaddrop_servers
                        .iter()
                        .position(|server| server == &confirm.server)
                };

                let Some(remove_index) = remove_index else {
                    state.post_system("Deaddrop server is already removed.");
                    state.store_active_runtime();
                    return operation::snap_to_end(state.session.logs_scroll_id.clone());
                };

                let removed = state.session.deaddrop_servers.remove(remove_index);
                state.session.deaddrop_stats.remove(&removed);
                state.post_system(format!("Removed deaddrop server: {removed}"));
                state.store_active_runtime();
                state.sync_active_deaddrop_servers();
                state.save_active_contact_meta();
                return operation::snap_to_end(state.session.logs_scroll_id.clone());
            }

            Message::DdServerDeleteCancelled => {
                state.session.deaddrop_delete_confirm = None;
                state.store_active_runtime();
                return Task::none();
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
                state.sam_shutdown_started = true;
                return state.begin_shutdown(ShutdownTarget::Window(window_id));
            }

            Message::WindowOpened(window_id) => {
                state.window_id = Some(window_id);
                return state.sync_unread_attention().unwrap_or_else(Task::none);
            }

            Message::WindowFocusChanged(window_id, focused) => {
                state.window_id = Some(window_id);
                state.window_focused = focused;

                if focused {
                    if let Some(tab) = state.active_tab_mut() {
                        tab.meta.has_unread = false;
                    }
                    state.refresh_visible_from_active_tab();
                }

                return state.sync_unread_attention().unwrap_or_else(Task::none);
            }

            Message::ProcessShutdownRequested => {
                state.sam_shutdown_started = true;
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
                    if state
                        .opened_tabs
                        .iter()
                        .find(|tab| tab.id == tab_id)
                        .map(|tab| tab.sam_runtime.is_closing())
                        .unwrap_or(true)
                    {
                        let mut sam = sam;
                        return Task::perform(
                            async move { sam.close().await.map_err(|e| e.to_string()) },
                            move |result| Message::SamCloseFinished(tab_id, result),
                        );
                    }

                    let mut profile_name: Option<String> = None;
                    let mut group_name: Option<String> = None;
                    let mut group_old_key: Option<String> = None;

                    if let Some(tab) = state.tab_by_id_mut(tab_id) {
                        profile_name = Some(tab.session.profile.clone());

                        tab.sam_runtime.client = sam;
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

                        if let Some(group) = tab.group.as_mut() {
                            let old_key = storage::group_storage_key(&group.meta);
                            group.publish_ready = false;
                            group.meta.my_dest_b64 = Some(init.my_dest_b64.clone());
                            group.meta.my_b32 = Some(init.my_b32.clone());
                            if group.meta.owner_b32.is_none() && group.meta.join_token.is_none() {
                                group.meta.owner_b32 = Some(init.my_b32.clone());
                            }
                            if let Some(owner_b32) = group.meta.owner_b32.clone() {
                                group.meta.id = owner_b32;
                            }
                            if let Err(err) = Self::sign_group_roster_if_admin(&mut group.meta) {
                                tab.session
                                    .log_lines
                                    .push(format!("Group roster signing failed: {err}"));
                            }
                            let new_key = storage::group_storage_key(&group.meta);
                            let new_profile_name = format!("group:{new_key}");
                            tab.meta.profile_name = new_profile_name.clone();
                            tab.session.profile = new_profile_name;
                            group.accept_armed = true;
                            group_name = Some(group.meta.name.clone());
                            group_old_key = Some(old_key);
                            tab.session
                                .log_lines
                                .push(format!("Group address: {}", init.my_b32));
                            tab.session
                                .log_lines
                                .push("Waiting for group tunnels to be published.".into());
                        }
                    } else {
                        return Task::none();
                    }

                    if group_name.is_some() {
                        if let Some(group_meta) = state
                            .opened_tabs
                            .iter()
                            .find(|tab| tab.id == tab_id)
                            .and_then(|tab| tab.group.as_ref().map(|group| group.meta.clone()))
                        {
                            if let Err(err) = storage::save_group_meta(&group_meta) {
                                state.session.group_status =
                                    format!("Save group metadata failed: {err}");
                            }
                            let new_key = storage::group_storage_key(&group_meta);

                            if let Some(idx) = state.session.groups.iter().position(|group| {
                                storage::group_storage_key(group) == new_key
                                    || group_old_key
                                        .as_deref()
                                        .map(|old_key| storage::group_storage_key(group) == old_key)
                                        .unwrap_or(false)
                            }) {
                                state.session.groups[idx] = group_meta;
                            }
                            if let Some(old_key) = group_old_key {
                                if old_key != new_key {
                                    let _ = storage::delete_group(&old_key);
                                }
                            }
                        }
                    }

                    if let Some(name) = profile_name {
                        if name != "default" && !name.starts_with("group:") {
                            state.save_active_contact_meta_for_name(&name);
                        }
                    }

                    if let Some(idx) = state.find_tab_index_by_id(tab_id) {
                        state.sync_tab_meta(idx);
                    }

                    if state.active_tab().map(|t| t.id) == Some(tab_id) {
                        state.load_active_runtime();
                    }

                    let is_group_tab = state
                        .opened_tabs
                        .iter()
                        .find(|tab| tab.id == tab_id)
                        .map(|tab| tab.meta.kind == TabKind::Group)
                        .unwrap_or(false);

                    if is_group_tab {
                        return Task::batch(vec![
                            state.group_accept_task(tab_id),
                            state.group_publish_ready_task(tab_id),
                        ]);
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

            Message::GroupPublishReady(tab_id, result) => {
                match result {
                    Ok(()) => {
                        let can_start = if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            if tab.sam_runtime.is_closing() {
                                false
                            } else if let Some(group) = tab.group.as_mut() {
                                group.publish_ready = true;
                                tab.session.network_status = NetworkStatus::Visible;
                                tab.session.log_lines.push(
                                    "Group tunnels confirmed. Starting group member connections."
                                        .into(),
                                );
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        };

                        if state.active_tab().map(|tab| tab.id) == Some(tab_id) {
                            state.load_active_runtime();
                        }

                        if can_start {
                            return Task::batch(state.group_connect_tasks(tab_id));
                        }
                    }
                    Err(err) => {
                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            if !tab.sam_runtime.is_closing() {
                                tab.session
                                    .log_lines
                                    .push(format!("Group tunnel check stopped: {err}"));
                            }
                        }
                    }
                }

                return Task::none();
            }

            Message::GroupConnectFinished(tab_id, member_b32, result) => {
                let is_active = state.active_tab().map(|t| t.id) == Some(tab_id);

                match result {
                    Ok((_peer, conn)) => {
                        let closing = state
                            .opened_tabs
                            .iter()
                            .find(|tab| tab.id == tab_id)
                            .map(|tab| tab.sam_runtime.is_closing())
                            .unwrap_or(true);
                        Self::sam_lifecycle_log(format!(
                            "group connect result ok tab={tab_id} peer={member_b32} closing={closing}"
                        ));
                        let msg_id_s = state.generate_msg_id();
                        let msg_id_k = state.generate_msg_id();
                        let mut send_task: Option<Task<Message>> = None;
                        let mut close_task: Option<Task<Message>> = None;

                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            if tab.sam_runtime.is_closing() {
                                Self::sam_lifecycle_log(format!(
                                    "group connect late close tab={tab_id} peer={member_b32}"
                                ));
                                return Task::perform(
                                    async move { conn.close().await.map_err(|e| e.to_string()) },
                                    move |result| Message::CloseFinished(tab_id, result),
                                );
                            }

                            let Some(group) = tab.group.as_mut() else {
                                return Task::perform(
                                    async move { conn.close().await.map_err(|e| e.to_string()) },
                                    move |result| Message::CloseFinished(tab_id, result),
                                );
                            };

                            let my_b32 = group.meta.my_b32.as_deref();
                            let prefer_outbound =
                                Self::local_prefers_outbound(my_b32, &member_b32);

                            let Some(peer) = group
                                .peers
                                .iter_mut()
                                .find(|peer| peer.member.b32 == member_b32)
                            else {
                                return Task::perform(
                                    async move { conn.close().await.map_err(|e| e.to_string()) },
                                    move |result| Message::CloseFinished(tab_id, result),
                                );
                            };

                            peer.connecting = false;

                            if peer.ready && peer.conn.is_some() {
                                tab.session.log_lines.push(format!(
                                    "Group connection collision: kept ready session with {}.",
                                    peer.member.name
                                ));
                                close_task = Some(Task::perform(
                                    async move { conn.close().await.map_err(|e| e.to_string()) },
                                    move |result| Message::CloseFinished(tab_id, result),
                                ));
                            } else if peer.conn.is_some() && !prefer_outbound {
                                tab.session.log_lines.push(format!(
                                    "Group connection collision: kept inbound session with {}.",
                                    peer.member.name
                                ));
                                close_task = Some(Task::perform(
                                    async move { conn.close().await.map_err(|e| e.to_string()) },
                                    move |result| Message::CloseFinished(tab_id, result),
                                ));
                            } else {
                                if peer.conn.is_some() {
                                    tab.session.log_lines.push(format!(
                                        "Group connection collision: kept outbound session with {}.",
                                        peer.member.name
                                    ));
                                }

                                tab.sam_runtime.register_stream(&conn);
                                let old_conn = peer.conn.replace(conn.clone());
                                peer.pending_conn = None;
                                Self::start_group_peer_handshake(
                                    peer,
                                    Self::now_epoch_millis(),
                                );

                                if let Some(old_conn) = old_conn {
                                    close_task = Some(Task::perform(
                                        async move { old_conn.close().await.map_err(|e| e.to_string()) },
                                        move |result| Message::CloseFinished(tab_id, result),
                                    ));
                                }

                                tab.session
                                    .log_lines
                                    .push(format!("Group handshake sent to {}.", peer.member.name));

                                if let Some(my_dest_b64) = tab.session.my_pub_dest_b64.clone() {
                                    let line = format!("{my_dest_b64}\n");
                                    let frame_s = Frame {
                                        msg_type: MsgType::S,
                                        msg_id: msg_id_s,
                                        payload: my_dest_b64.into_bytes(),
                                    };
                                    let frame_k = Frame {
                                        msg_type: MsgType::K,
                                        msg_id: msg_id_k,
                                        payload: peer.e2e.public_bytes(),
                                    };

                                    let task = Task::perform(
                                        async move {
                                            conn.send_raw_line(&line)
                                                .await
                                                .map_err(|e| e.to_string())?;
                                            conn.send_frame(&frame_s)
                                                .await
                                                .map_err(|e| e.to_string())?;
                                            conn.send_frame(&frame_k)
                                                .await
                                                .map_err(|e| e.to_string())
                                        },
                                        move |result| Message::SendFinished(tab_id, result),
                                    );
                                    send_task = Some(tab.sam_runtime.track_send_task(task));
                                }
                            }
                        } else {
                            Self::sam_lifecycle_log(format!(
                                "group connect result without tab; closing conn tab={tab_id} peer={member_b32}"
                            ));
                            return Task::perform(
                                async move { conn.close().await.map_err(|e| e.to_string()) },
                                move |result| Message::CloseFinished(tab_id, result),
                            );
                        }

                        if is_active {
                            state.load_active_runtime();
                        }

                        let mut tasks = Vec::new();
                        if let Some(task) = close_task {
                            tasks.push(task);
                        }
                        if let Some(task) = send_task {
                            tasks.push(task);
                        }
                        if !tasks.is_empty() {
                            return Task::batch(tasks);
                        }
                    }
                    Err(err) => {
                        Self::sam_lifecycle_log(format!(
                            "group connect result err tab={tab_id} peer={member_b32} err={err}"
                        ));
                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            if let Some(group) = tab.group.as_mut() {
                                if let Some(peer) = group
                                    .peers
                                    .iter_mut()
                                    .find(|peer| peer.member.b32 == member_b32)
                                {
                                    peer.connecting = false;
                                }
                            }

                            tab.session
                                .log_lines
                                .push(format!("Group connect failed for {member_b32}: {err}"));
                        }
                    }
                }

                if is_active {
                    state.load_active_runtime();
                }

                return Task::none();
            }

            Message::ConnectFinished(tab_id, generation, result) => {
                let msg_id_auth = state.generate_msg_id();
                let msg_id_s = state.generate_msg_id();
                let msg_id_k = state.generate_msg_id();
                let mut tasks: Vec<Task<Message>> = Vec::new();
                let mut rearm_accept = false;

                match result {
                    Ok((peer, conn)) => {
                        let mut handshake: Option<(String, Option<Frame>, Frame, Frame)> = None;
                        let mut keep_outbound = false;

                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            let current_attempt = !tab.sam_runtime.is_closing()
                                && tab.connect_in_flight
                                && tab.connect_generation == generation
                                && tab.connect_peer.as_deref() == Some(peer.as_str());

                            if !current_attempt {
                                let conn_to_close = conn.clone();
                                tasks.push(Task::perform(
                                    async move {
                                        conn_to_close.close().await.map_err(|e| e.to_string())
                                    },
                                    move |result| Message::CloseFinished(tab_id, result),
                                ));
                            } else {
                                tab.connect_in_flight = false;
                                tab.connect_peer = None;

                                if tab.session.live_ready && tab.live_conn.is_some() {
                                    tab.session.log_lines.push(format!(
                                        "Connection collision: kept established session with {peer}."
                                    ));
                                    let conn_to_close = conn.clone();
                                    tasks.push(Task::perform(
                                        async move {
                                            conn_to_close.close().await.map_err(|e| e.to_string())
                                        },
                                        move |result| Message::CloseFinished(tab_id, result),
                                    ));
                                } else if let Some(pending) = tab.pending_conn.clone() {
                                    let same_peer = tab
                                        .session
                                        .pending_peer_addr
                                        .as_deref()
                                        .map(|pending_peer| pending_peer.eq_ignore_ascii_case(&peer))
                                        .unwrap_or(false);
                                    let prefer_outbound = same_peer
                                        && Self::local_prefers_outbound(
                                            tab.session.my_b32.as_deref(),
                                            &peer,
                                        );

                                    if prefer_outbound {
                                        tab.session.log_lines.push(format!(
                                            "Connection collision: kept outbound session with {peer}."
                                        ));
                                        Self::release_pending_rendezvous(tab);
                                        tab.pending_conn = None;
                                        tab.session.pending_peer_addr = None;
                                        tab.session.pending_peer_dest_b64 = None;
                                        tab.session.call_blink_on = true;
                                        tab.session.call_blink_ticks = 0;
                                        rearm_accept = true;
                                        tasks.push(Task::perform(
                                            async move {
                                                pending.close().await.map_err(|e| e.to_string())
                                            },
                                            move |result| Message::CloseFinished(tab_id, result),
                                        ));
                                        keep_outbound = true;
                                    } else {
                                        let collision_message = if same_peer {
                                            format!(
                                                "Connection collision: kept inbound session with {peer}."
                                            )
                                        } else {
                                            format!(
                                                "Connect result ignored while another incoming call is pending from {}.",
                                                tab.session
                                                    .pending_peer_addr
                                                    .as_deref()
                                                    .unwrap_or("unknown peer")
                                            )
                                        };
                                        tab.session.log_lines.push(collision_message);
                                        let conn_to_close = conn.clone();
                                        tasks.push(Task::perform(
                                            async move {
                                                conn_to_close.close().await.map_err(|e| e.to_string())
                                            },
                                            move |result| Message::CloseFinished(tab_id, result),
                                        ));
                                    }
                                } else if tab.live_conn.is_some() {
                                    tab.session.log_lines.push(format!(
                                        "Connection collision: kept existing session with {peer}."
                                    ));
                                    let conn_to_close = conn.clone();
                                    tasks.push(Task::perform(
                                        async move {
                                            conn_to_close.close().await.map_err(|e| e.to_string())
                                        },
                                        move |result| Message::CloseFinished(tab_id, result),
                                    ));
                                } else {
                                    keep_outbound = true;
                                }

                                if keep_outbound {
                                    tab.e2e = E2E::new(tab.session.pq_enabled);
                                    tab.sam_runtime.register_stream(&conn);
                                    tab.live_conn = Some(conn.clone());
                                    tab.connection_direction =
                                        Some(ConnectionDirection::Outbound);
                                    tab.session.current_peer_addr = Some(peer.clone());
                                    tab.session.current_peer_dest_b64 = None;
                                    tab.session.peer_b32 = Some(peer.clone());
                                    tab.session.network_status = NetworkStatus::Visible;
                                    tab.session.live_ready = false;
                                    tab.session.offline_mode = false;
                                    tab.session.log_lines.push(format!(
                                        "Handshake sent to {peer}. Establishing secure session..."
                                    ));

                                    if let Some(my_dest_b64) =
                                        tab.session.my_pub_dest_b64.clone()
                                    {
                                        let auth_frame_result = if let Some(access) =
                                            tab.session.rendezvous_outgoing.take()
                                        {
                                            let auth_result = tab
                                                .session
                                                .my_b32
                                                .as_deref()
                                                .ok_or_else(|| {
                                                    "local transient address is unavailable"
                                                        .to_string()
                                                })
                                                .and_then(|my_b32| {
                                                    if !access
                                                        .destination_b32
                                                        .eq_ignore_ascii_case(&peer)
                                                    {
                                                        return Err(
                                                            "rendezvous destination changed"
                                                                .to_string(),
                                                        );
                                                    }
                                                    rendezvous::make_auth_signal(
                                                        &access,
                                                        my_b32,
                                                        &peer,
                                                        Self::now_epoch_millis(),
                                                    )
                                                });

                                            auth_result.map(|signal| Some(Frame {
                                                    msg_type: MsgType::S,
                                                    msg_id: msg_id_auth,
                                                    payload: signal.into_bytes(),
                                                }))
                                        } else {
                                            Ok(None)
                                        };

                                        match auth_frame_result {
                                            Ok(auth_frame) => {
                                                let line = format!("{my_dest_b64}\n");
                                                let frame_s = Frame {
                                                    msg_type: MsgType::S,
                                                    msg_id: msg_id_s,
                                                    payload: my_dest_b64.into_bytes(),
                                                };
                                                let frame_k = Frame {
                                                    msg_type: MsgType::K,
                                                    msg_id: msg_id_k,
                                                    payload: tab.e2e.public_bytes(),
                                                };
                                                handshake =
                                                    Some((line, auth_frame, frame_s, frame_k));
                                            }
                                            Err(err) => {
                                                    tab.session.rendezvous_status = format!(
                                                        "Rendezvous authentication failed: {err}"
                                                    );
                                                    tab.session.log_lines.push(
                                                        "Rendezvous call authentication could not be prepared."
                                                            .into(),
                                                    );
                                                    tab.live_conn = None;
                                                    tab.connection_direction = None;
                                                    tab.session.current_peer_addr = None;
                                                    tab.session.peer_b32 = None;
                                                    tab.session.network_status =
                                                        NetworkStatus::LocalOk;
                                                    let conn_to_close = conn.clone();
                                                    tasks.push(Task::perform(
                                                        async move {
                                                            conn_to_close
                                                                .close()
                                                                .await
                                                                .map_err(|e| e.to_string())
                                                        },
                                                        move |result| {
                                                            Message::CloseFinished(tab_id, result)
                                                        },
                                                    ));
                                            }
                                            }
                                    }
                                } else if tab.pending_conn.is_none()
                                    && tab.live_conn.is_none()
                                {
                                    tab.connection_direction = None;
                                    tab.session.network_status = NetworkStatus::LocalOk;
                                }
                            }
                        } else {
                            let conn_to_close = conn.clone();
                            tasks.push(Task::perform(
                                async move {
                                    conn_to_close.close().await.map_err(|e| e.to_string())
                                },
                                move |result| Message::CloseFinished(tab_id, result),
                            ));
                        }

                        if let Some((line, auth_frame, frame_s, frame_k)) = handshake {
                            tasks.push(Task::perform(
                                async move {
                                    conn.send_raw_line(&line)
                                        .await
                                        .map_err(|e| e.to_string())?;
                                    if let Some(auth_frame) = auth_frame {
                                        conn.send_frame(&auth_frame)
                                            .await
                                            .map_err(|e| e.to_string())?;
                                    }
                                    conn.send_frame(&frame_s)
                                        .await
                                        .map_err(|e| e.to_string())?;
                                    conn.send_frame(&frame_k).await.map_err(|e| e.to_string())
                                },
                                move |result| Message::SendFinished(tab_id, result),
                            ));
                        }
                    }
                    Err(err) => {
                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            if tab.connect_in_flight && tab.connect_generation == generation {
                                tab.connect_in_flight = false;
                                tab.connect_peer = None;
                                if tab.live_conn.is_none() && tab.pending_conn.is_none() {
                                    tab.connection_direction = None;
                                    tab.session.network_status = NetworkStatus::LocalOk;
                                }
                                tab.session.log_lines.push(format!("Connect failed: {err}"));
                            }
                        }
                    }
                }

                if rearm_accept {
                    if let Some(tab) = state.tab_by_id_mut(tab_id) {
                        tab.session.accept_armed = true;
                        if let Some((sam, cancelled)) = tab.sam_runtime.accept_parts() {
                            tasks.push(Self::incoming_accept_task_from_parts(
                                tab_id, sam, cancelled,
                            ));
                        }
                    }
                }

                if let Some(idx) = state.find_tab_index_by_id(tab_id) {
                    state.sync_tab_meta(idx);
                }

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                }

                return Task::batch(tasks);
            }

            Message::GroupIncomingAccepted(tab_id, result) => match result {
                Ok(incoming) => {
                    Self::sam_lifecycle_log(format!(
                        "group accept result ok tab={tab_id} peer={}",
                        incoming.peer_b32
                    ));
                    if state
                        .tab_by_id_mut(tab_id)
                        .map(|tab| tab.sam_runtime.accept_cancelled())
                        .unwrap_or(true)
                    {
                        Self::sam_lifecycle_log(format!(
                            "group accept cancelled close tab={tab_id} peer={}",
                            incoming.peer_b32
                        ));
                        let conn_to_close = incoming.conn.clone();
                        return Task::perform(
                            async move { conn_to_close.close().await.map_err(|e| e.to_string()) },
                            move |result| Message::CloseFinished(tab_id, result),
                        );
                    }

                    let msg_id_s = state.generate_msg_id();
                    let msg_id_k = state.generate_msg_id();
                    let mut accepted_conn: Option<LiveConnection> = None;
                    let mut frames: Option<(String, Frame, Frame)> = None;
                    let mut close_tasks: Vec<Task<Message>> = Vec::new();
                    let is_active = state.active_tab().map(|t| t.id) == Some(tab_id);

                    if let Some(tab) = state.tab_by_id_mut(tab_id) {
                        let Some(group) = tab.group.as_mut() else {
                            Self::sam_lifecycle_log(format!(
                                "group accept missing group close tab={tab_id} peer={}",
                                incoming.peer_b32
                            ));
                            let conn_to_close = incoming.conn.clone();
                            return Task::perform(
                                async move { conn_to_close.close().await.map_err(|e| e.to_string()) },
                                move |result| Message::CloseFinished(tab_id, result),
                            );
                        };
                        let my_b32 = group.meta.my_b32.as_deref();
                        let prefer_outbound =
                            Self::local_prefers_outbound(my_b32, &incoming.peer_b32);

                        let peer_idx = if let Some(peer_idx) = group
                            .peers
                            .iter()
                            .position(|peer| peer.member.b32 == incoming.peer_b32)
                        {
                            tab.session.log_lines.push(format!(
                                "Accepted group connection from {}.",
                                group.peers[peer_idx].member.name
                            ));
                            peer_idx
                        } else {
                            let member_name =
                                format!("member-{}", short_b32(Some(&incoming.peer_b32)));
                            let member = GroupMember {
                                name: member_name.clone(),
                                b32: incoming.peer_b32.clone(),
                            };

                            tab.session.log_lines.push(format!(
                                "Provisional group caller {member_name}; awaiting invite proof."
                            ));

                            group
                                .peers
                                .push(Self::new_group_peer_runtime(member, false));

                            group.peers.len() - 1
                        };

                        let peer = &mut group.peers[peer_idx];
                        let has_existing_conn = peer.conn.is_some();
                        let has_collision = has_existing_conn || peer.connecting;

                        if peer.ready && has_existing_conn {
                            tab.session.log_lines.push(format!(
                                "Group connection collision: kept ready session with {}.",
                                peer.member.name
                            ));
                            let conn_to_close = incoming.conn.clone();
                            close_tasks.push(Task::perform(
                                async move { conn_to_close.close().await.map_err(|e| e.to_string()) },
                                move |result| Message::CloseFinished(tab_id, result),
                            ));
                        } else if has_collision && prefer_outbound {
                            tab.session.log_lines.push(format!(
                                "Group connection collision: kept outbound session with {}.",
                                peer.member.name
                            ));
                            let conn_to_close = incoming.conn.clone();
                            close_tasks.push(Task::perform(
                                async move { conn_to_close.close().await.map_err(|e| e.to_string()) },
                                move |result| Message::CloseFinished(tab_id, result),
                            ));
                        } else {
                            if has_existing_conn {
                                tab.session.log_lines.push(format!(
                                    "Group connection collision: kept inbound session with {}.",
                                    peer.member.name
                                ));
                            }

                            tab.sam_runtime.register_stream(&incoming.conn);
                            let old_conn = peer.conn.replace(incoming.conn.clone());
                            peer.pending_conn = None;
                            Self::start_group_peer_handshake(
                                peer,
                                Self::now_epoch_millis(),
                            );

                            if let Some(old_conn) = old_conn {
                                close_tasks.push(Task::perform(
                                    async move { old_conn.close().await.map_err(|e| e.to_string()) },
                                    move |result| Message::CloseFinished(tab_id, result),
                                ));
                            }

                            if let Some(my_dest_b64) = tab.session.my_pub_dest_b64.clone() {
                                let line = format!("{my_dest_b64}\n");
                                let frame_s = Frame {
                                    msg_type: MsgType::S,
                                    msg_id: msg_id_s,
                                    payload: my_dest_b64.into_bytes(),
                                };
                                let frame_k = Frame {
                                    msg_type: MsgType::K,
                                    msg_id: msg_id_k,
                                    payload: peer.e2e.public_bytes(),
                                };
                                accepted_conn = Some(incoming.conn.clone());
                                frames = Some((line, frame_s, frame_k));
                            }
                        }
                    } else {
                        Self::sam_lifecycle_log(format!(
                            "group accept result without tab; closing conn tab={tab_id} peer={}",
                            incoming.peer_b32
                        ));
                        let conn_to_close = incoming.conn.clone();
                        return Task::perform(
                            async move { conn_to_close.close().await.map_err(|e| e.to_string()) },
                            move |result| Message::CloseFinished(tab_id, result),
                        );
                    }

                    let mut tasks = vec![state.group_accept_task(tab_id)];
                    tasks.extend(close_tasks);
                    if let (Some(conn), Some((line, frame_s, frame_k))) = (accepted_conn, frames) {
                        let task = Task::perform(
                            async move {
                                conn.send_raw_line(&line).await.map_err(|e| e.to_string())?;
                                conn.send_frame(&frame_s).await.map_err(|e| e.to_string())?;
                                conn.send_frame(&frame_k).await.map_err(|e| e.to_string())
                            },
                            move |result| Message::SendFinished(tab_id, result),
                        );
                        tasks.push(state.track_group_send_task(tab_id, task));
                    }

                    if is_active {
                        state.load_active_runtime();
                    }

                    return Task::batch(tasks);
                }
                Err(err) => {
                    Self::sam_lifecycle_log(format!(
                        "group accept result err tab={tab_id} err={err}"
                    ));
                    if let Some(tab) = state.tab_by_id_mut(tab_id) {
                        let cancelled = tab.sam_runtime.accept_cancelled();
                        if !cancelled {
                            tab.session
                                .log_lines
                                .push(format!("Group accept failed: {err}"));
                        } else {
                            return Task::none();
                        }
                    } else {
                        return Task::none();
                    }

                    if state.active_tab().map(|t| t.id) == Some(tab_id) {
                        state.load_active_runtime();
                    }

                    return state.group_accept_task(tab_id);
                }
            },

            Message::IncomingAccepted(tab_id, result) => {
                let mut tasks: Vec<Task<Message>> = Vec::new();
                let mut rearm_accept = false;
                match result {
                    Ok(incoming) => {
                        if state
                            .tab_by_id_mut(tab_id)
                            .map(|tab| tab.sam_runtime.accept_cancelled())
                            .unwrap_or(true)
                        {
                            let conn_to_close = incoming.conn.clone();
                            return Task::perform(
                                async move {
                                    conn_to_close.close().await.map_err(|e| e.to_string())
                                },
                                move |result| Message::CloseFinished(tab_id, result),
                            );
                        }

                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            let outbound_peer = tab
                                .connect_peer
                                .as_deref()
                                .or(tab.session.current_peer_addr.as_deref());
                            let same_outbound_peer = outbound_peer
                                .map(|peer| peer.eq_ignore_ascii_case(&incoming.peer_b32))
                                .unwrap_or(false);
                            let has_outbound_candidate = tab.connect_in_flight
                                || (tab.live_conn.is_some()
                                    && tab.connection_direction
                                        == Some(ConnectionDirection::Outbound));
                            let keep_established = tab.session.live_ready
                                && tab.live_conn.is_some();
                            let keep_existing_pending = tab.pending_conn.is_some();
                            let prefer_outbound = same_outbound_peer
                                && Self::local_prefers_outbound(
                                    tab.session.my_b32.as_deref(),
                                    &incoming.peer_b32,
                                );

                            if keep_established
                                || keep_existing_pending
                                || (has_outbound_candidate
                                    && (!same_outbound_peer || prefer_outbound))
                            {
                                let reason = if keep_established {
                                    format!(
                                        "Connection collision: kept established session with {}.",
                                        incoming.peer_b32
                                    )
                                } else if keep_existing_pending {
                                    format!(
                                        "Connection collision: kept existing incoming call from {}.",
                                        tab.session
                                            .pending_peer_addr
                                            .as_deref()
                                            .unwrap_or("unknown peer")
                                    )
                                } else if same_outbound_peer {
                                    format!(
                                        "Connection collision: kept outbound session with {}.",
                                        incoming.peer_b32
                                    )
                                } else {
                                    format!(
                                        "Incoming call from {} rejected while connecting to another peer.",
                                        incoming.peer_b32
                                    )
                                };
                                tab.session.log_lines.push(reason);
                                let conn_to_close = incoming.conn.clone();
                                tasks.push(Task::perform(
                                    async move {
                                        conn_to_close.close().await.map_err(|e| e.to_string())
                                    },
                                    move |result| Message::CloseFinished(tab_id, result),
                                ));
                                rearm_accept = !keep_existing_pending;
                            } else {
                                if has_outbound_candidate {
                                    tab.session.log_lines.push(format!(
                                        "Connection collision: kept inbound session with {}.",
                                        incoming.peer_b32
                                    ));
                                    Self::invalidate_one_to_one_connect(tab, true);
                                }

                                if let Some(old_conn) = tab.live_conn.take() {
                                    tasks.push(Task::perform(
                                        async move {
                                            old_conn.close().await.map_err(|e| e.to_string())
                                        },
                                        move |result| Message::CloseFinished(tab_id, result),
                                    ));
                                }

                                tab.session.current_peer_addr = None;
                                tab.session.current_peer_dest_b64 = None;
                                tab.session.peer_b32 = None;
                                tab.session.live_ready = false;
                                tab.session.heartbeat_last_rx_ms = 0;
                                tab.session.heartbeat_last_ping_ms = 0;
                                tab.e2e = E2E::new(tab.session.pq_enabled);
                                tab.sam_runtime.register_stream(&incoming.conn);
                                tab.pending_conn = Some(incoming.conn);
                                tab.connection_direction = Some(ConnectionDirection::Inbound);
                                tab.session.pending_peer_addr = Some(incoming.peer_b32.clone());
                                tab.session.pending_peer_dest_b64 =
                                    Some(incoming.peer_dest_b64.clone());
                                tab.session.pending_rendezvous_request_id = None;

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
                            }
                        } else {
                            let conn_to_close = incoming.conn.clone();
                            tasks.push(Task::perform(
                                async move {
                                    conn_to_close.close().await.map_err(|e| e.to_string())
                                },
                                move |result| Message::CloseFinished(tab_id, result),
                            ));
                        }
                    }
                    Err(err) => {
                        if let Some(tab) = state.tab_by_id_mut(tab_id) {
                            let cancelled = tab.sam_runtime.accept_cancelled();
                            if !cancelled {
                                tab.session.log_lines.push(format!("Accept failed: {err}"));
                            }
                        }
                    }
                }

                if rearm_accept {
                    if let Some(tab) = state.tab_by_id_mut(tab_id) {
                        tab.session.accept_armed = true;
                        if let Some((sam, cancelled)) = tab.sam_runtime.accept_parts() {
                            tasks.push(Self::incoming_accept_task_from_parts(
                                tab_id, sam, cancelled,
                            ));
                        }
                    }
                }

                if let Some(idx) = state.find_tab_index_by_id(tab_id) {
                    state.sync_tab_meta(idx);
                }

                if state.active_tab().map(|t| t.id) == Some(tab_id) {
                    state.load_active_runtime();
                }

                return Task::batch(tasks);
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
                let now_ms = Self::now_epoch_millis();
                state.refresh_sam_monitor_state();

                if let Some(deadline) = state.sam_shutdown_deadline_ms {
                    if now_ms >= deadline && !state.sam_shutdown_started {
                        state.sam_shutdown_started = true;
                        return state.begin_shutdown(ShutdownTarget::Runtime);
                    }
                    return Task::none();
                }

                let mut tasks: Vec<Task<Message>> = Vec::new();

                if !state.sam_shutdown_started
                    && state.sam_shutdown_deadline_ms.is_none()
                    && !state.sam_monitor_probe_in_flight
                    && state.sam_monitor_host.is_some()
                    && now_ms.saturating_sub(state.sam_monitor_last_probe_ms)
                        >= SAM_MONITOR_INTERVAL_MS
                {
                    if let Some(task) = state.start_sam_monitor_probe() {
                        tasks.push(task);
                    }
                }

                for idx in 0..state.opened_tabs.len() {
                    let mut tab_tasks = state.tick_one_tab(idx);
                    tasks.append(&mut tab_tasks);
                }

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

                    let mut recv_window =
                        Self::get_deaddrop_recv_window(&tab.session, &my_b32, &peer_b32);

                    if tab.deaddrop_stalled_sweeps >= OFFLINE_FORWARD_PROBE_STALL_ROUNDS {
                        let window_end = tab
                            .session
                            .drop_recv_base
                            .saturating_add(tab.session.drop_window as u64);
                        let probe_index = tab.session.forward_probe_index.max(window_end);
                        let probe_key = Self::offline_directional_key(
                            &tab.session.offline_shared_secret.unwrap_or([0u8; 32]),
                            &my_b32,
                            &peer_b32,
                            "recv",
                            probe_index,
                        );
                        recv_window.push(OfflinePollTarget {
                            index: probe_index,
                            key: probe_key,
                            kind: OfflinePollKind::ForwardProbe,
                        });
                        tab.session.forward_probe_index = probe_index.saturating_add(1);
                    }

                    if now_ms.saturating_sub(tab.deaddrop_last_recovery_probe_ms)
                        >= OFFLINE_RECOVERY_PROBE_INTERVAL_MS
                    {
                        let recovery_index = tab
                            .session
                            .skipped_drop_recv
                            .iter_mut()
                            .find(|entry| {
                                now_ms.saturating_sub(entry.last_recovery_probe_ms)
                                    >= OFFLINE_RECOVERY_PROBE_INTERVAL_MS
                            })
                            .map(|skipped| {
                                skipped.last_recovery_probe_ms = now_ms;
                                skipped.index
                            });
                        if let Some(recovery_index) = recovery_index {
                            tab.deaddrop_last_recovery_probe_ms = now_ms;
                            recv_window.push(OfflinePollTarget {
                                index: recovery_index,
                                key: Self::offline_directional_key(
                                    &tab.session.offline_shared_secret.unwrap_or([0u8; 32]),
                                    &my_b32,
                                    &peer_b32,
                                    "recv",
                                    recovery_index,
                                ),
                                kind: OfflinePollKind::RecoveryProbe,
                            });
                        }
                    }

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
                    tab.deaddrop_poll_round_misses.clear();
                    tab.deaddrop_poll_round_authenticated.clear();
                    Self::set_dd_status(&mut tab.session, "poll");
                    poll_tab_ids.push(tab.id);
                }

                for tab_id in poll_tab_ids {
                    tasks.push(state.start_next_deaddrop_poll_key_task(tab_id));
                }

                state.refresh_visible_from_active_tab();
                if let Some(task) = state.sync_unread_attention() {
                    tasks.push(task);
                }

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
                    if tab.outgoing_phase != OutgoingFilePhase::Idle
                        || tab.outgoing_image_phase != OutgoingImagePhase::Idle
                    {
                        state.post_system("Another transfer is already in progress.");
                        return operation::snap_to_end(state.session.logs_scroll_id.clone());
                    }

                    if tab.meta.kind != TabKind::Group
                        && (tab.live_conn.is_none() || !tab.session.live_ready)
                    {
                        state.post_system("Image send requires a live secure chat.");
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

                return match state.send_prepared_image(filename, mime, bytes) {
                    Ok(task) => task,
                    Err(err) => {
                        state.post_system(err);
                        operation::snap_to_end(state.session.logs_scroll_id.clone())
                    }
                };
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
                if state.opened_tabs.is_empty() && !state.sam_test_in_flight {
                    state.sam_host_input = value;
                }
                return Task::none();
            }

            Message::SamPortInputChanged(value) => {
                if state.opened_tabs.is_empty() && !state.sam_test_in_flight {
                    state.sam_port_input = value;
                }
                return Task::none();
            }

            Message::SaveSamSettingsPressed => {
                if !state.opened_tabs.is_empty() {
                    state.sam_status =
                        "Close all chat and group tabs before changing SAM settings.".into();
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

            Message::SamMonitorProbeFinished(generation, result) => {
                state.refresh_sam_monitor_state();
                if state.sam_shutdown_started
                    || generation != state.sam_monitor_generation
                    || state.sam_monitor_host.is_none()
                    || state.sam_shutdown_deadline_ms.is_some()
                {
                    return Task::none();
                }

                state.sam_monitor_probe_in_flight = false;
                match result {
                    Ok(()) => {
                        state.sam_monitor_failures = 0;
                    }
                    Err(_) => {
                        state.sam_monitor_failures = state.sam_monitor_failures.saturating_add(1);

                        if state.sam_monitor_failures >= SAM_MONITOR_FAILURE_LIMIT {
                            state.sam_shutdown_deadline_ms = Some(
                                Self::now_epoch_millis().saturating_add(SAM_SHUTDOWN_COUNTDOWN_MS),
                            );

                            if let Some(window_id) = state.window_id {
                                return window::request_user_attention(
                                    window_id,
                                    Some(window::UserAttention::Informational),
                                );
                            }
                        }
                    }
                }
                return Task::none();
            }

            Message::SamShutdownNowPressed => {
                if state.sam_shutdown_started {
                    return Task::none();
                }

                state.sam_shutdown_started = true;
                return state.begin_shutdown(ShutdownTarget::Runtime);
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
                                        let offline =
                                            Self::offline_state_from_session(&tab.session);

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
                            tab.deaddrop_poll_round_misses.clear();
                            tab.deaddrop_poll_round_authenticated.clear();
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

            Message::OfflinePollKeyFinished(
                tab_id,
                recv_index,
                poll_kind,
                _dd_key,
                blobs,
                stats,
            ) => {
                let mark_unread = !state.window_focused
                    || state.active_tab().map(|tab| tab.id) != Some(tab_id);
                if let Some(tab) = state.tab_by_id_mut(tab_id) {
                    Self::record_deaddrop_stats_for_tab(tab, &stats);
                    Self::flush_deaddrop_stats_for_tab(tab, false);
                    Self::handle_offline_poll_key_result(
                        tab,
                        recv_index,
                        poll_kind,
                        blobs,
                        &stats,
                        mark_unread,
                    );
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
        let profile_confirm_active = matches!(
            state.session.sidebar_confirm,
            Some(SidebarConfirm::DeleteProfile(_) | SidebarConfirm::ResetProfile(_))
        );
        let group_delete_confirm_active = matches!(
            state.session.sidebar_confirm,
            Some(SidebarConfirm::DeleteGroup { .. })
        );
        let group_member_confirm_active = matches!(
            state.session.sidebar_confirm,
            Some(SidebarConfirm::DeleteGroupMember { .. })
        );
        let profile_buttons_allowed = !profile_confirm_active;
        let profile_ops_allowed = delete_allowed && profile_buttons_allowed;
        let group_buttons_allowed = !group_delete_confirm_active;
        let selected_group = state
            .session
            .selected_group_idx
            .and_then(|idx| state.session.groups.get(idx));
        let group_selected = selected_group.is_some();
        let selected_group_is_admin = selected_group.map(Self::group_is_admin).unwrap_or(false);
        let group_delete_allowed = selected_group
            .map(|group| !state.is_group_open_in_any_tab(&storage::group_storage_key(group)))
            .unwrap_or(false);
        let selected_group_role = if selected_group_is_admin {
            "Owner"
        } else {
            "Member"
        };
        let selected_group_total_known = selected_group
            .map(|group| group.members.len().saturating_add(1))
            .unwrap_or(0);
        let active_group_member_b32s: Vec<String> = state
            .active_tab()
            .and_then(|tab| tab.group.as_ref())
            .map(|runtime| {
                runtime
                    .peers
                    .iter()
                    .filter(|peer| peer.ready && peer.authorized)
                    .map(|peer| peer.member.b32.to_ascii_lowercase())
                    .collect()
            })
            .unwrap_or_default();
        let active_group_peer_count = active_group_member_b32s.len();

        let profile_sidebar_confirm = match &state.session.sidebar_confirm {
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
            _ => column![],
        };

        let group_delete_confirm = match &state.session.sidebar_confirm {
            Some(SidebarConfirm::DeleteGroup { name, .. }) => column![
                text(format!("Delete group #{name}?")).size(12),
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
            _ => column![],
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

        let group_list = if state.session.groups.is_empty() {
            column![
                text("No groups.")
                    .size(12)
                    .color(Color::from_rgb8(150, 150, 150))
            ]
            .spacing(6)
            .width(Length::Fill)
        } else {
            state.session.groups.iter().enumerate().fold(
                column!().spacing(6).width(Length::Fill),
                |col, (idx, group)| {
                    let selected = state.session.selected_group_idx == Some(idx);
                    let label = row![
                        text("#").size(13).color(PY_CYAN),
                        text(&group.name).size(13).color(Color::WHITE),
                        text(format!("({})", group.members.len()))
                            .size(12)
                            .color(Color::from_rgb8(160, 160, 168)),
                    ]
                    .spacing(6)
                    .align_y(Alignment::Center);

                    col.push(
                        button(
                            container(label)
                                .width(Length::Fill)
                                .padding([6, 8])
                                .style(|_| profile_row_content_style(APP_PROFILE_TEXT)),
                        )
                        .width(Length::Fill)
                        .style(move |theme, status| profile_button_style(theme, status, selected))
                        .on_press_maybe(
                            group_buttons_allowed.then_some(Message::GroupSelected(idx)),
                        ),
                    )
                },
            )
        };

        let profile_sidebar = container(
            column![
                container(
                    scrollable(profile_list)
                        .height(Length::Fill)
                        .width(Length::Fill)
                )
                .height(Length::FillPortion(2))
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

                            if profile_buttons_allowed {
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

                            if profile_buttons_allowed {
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

                            if profile_ops_allowed {
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

                            if profile_ops_allowed {
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
                profile_sidebar_confirm,
                Space::new().height(4),
                container(Space::new().height(3))
                    .width(Length::Fill)
                    .style(|_| sidebar_divider_style()),
                Space::new().height(4),
                container(
                    scrollable(group_list)
                        .height(Length::Fill)
                        .width(Length::Fill)
                )
                .height(Length::FillPortion(1))
                .width(Length::Fill)
                .padding(4)
                .style(|_| sidebar_panel_style()),
                {
                    let input = text_input("New group name...", &state.session.group_name_input)
                        .on_input(Message::GroupNameInputChanged)
                        .padding(8)
                        .size(13)
                        .width(Length::Fill);

                    if group_buttons_allowed {
                        input.on_submit(Message::CreateGroupPressed)
                    } else {
                        input
                    }
                },
                column![
                    row![
                        button(text("New").size(12))
                            .padding([4, 8])
                            .width(Length::Fill)
                            .style(app_button_style)
                            .on_press_maybe(
                                group_buttons_allowed.then_some(Message::CreateGroupPressed)
                            ),
                        button(text("Open").size(12))
                            .padding([4, 8])
                            .width(Length::Fill)
                            .style(app_button_style)
                            .on_press_maybe(
                                (group_selected && group_buttons_allowed)
                                    .then_some(Message::OpenGroupPressed)
                            ),
                    ]
                    .spacing(6)
                    .width(Length::Fill),
                    button(text("Delete").size(12))
                        .padding([4, 8])
                        .width(Length::Fill)
                        .style(app_button_style)
                        .on_press_maybe(
                            (group_delete_allowed && group_buttons_allowed)
                                .then_some(Message::DeleteGroupPressed)
                        ),
                ]
                .spacing(6)
                .width(Length::Fill),
                group_delete_confirm,
                row![
                    {
                        let input = text_input(
                            "Paste group invite...",
                            &state.session.group_invite_string_input,
                        )
                        .on_input(Message::GroupInviteStringInputChanged)
                        .padding(8)
                        .size(12)
                        .width(Length::Fill);

                        if group_buttons_allowed {
                            input.on_submit(Message::ImportGroupInviteStringPressed)
                        } else {
                            input
                        }
                    },
                    button(text("OK").size(12))
                        .padding([4, 8])
                        .style(app_button_style)
                        .on_press_maybe(
                            group_buttons_allowed
                                .then_some(Message::ImportGroupInviteStringPressed)
                        ),
                ]
                .spacing(6)
                .align_y(Alignment::Center)
                .width(Length::Fill),
                button(text("Generate Private Request").size(12))
                    .padding([4, 8])
                    .width(Length::Fill)
                    .style(app_button_style)
                    .on_press_maybe(
                        group_buttons_allowed
                            .then_some(Message::GeneratePrivateGroupRequestPressed)
                    ),
                row![
                    text_input(
                        "Private request appears here...",
                        &state.session.group_private_request_string,
                    )
                    .padding(8)
                    .size(12)
                    .width(Length::Fill),
                    button(text("Copy").size(12))
                        .padding([4, 8])
                        .style(app_button_style)
                        .on_press_maybe(
                            (group_buttons_allowed
                                && !state.session.group_private_request_string.trim().is_empty())
                            .then_some(Message::CopyPrivateGroupRequestPressed)
                        ),
                ]
                .spacing(6)
                .align_y(Alignment::Center)
                .width(Length::Fill),
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
                let tab_closing = if tab.kind == TabKind::AppHome || idx == 0 {
                    false
                } else {
                    state
                        .opened_tabs
                        .get(idx - 1)
                        .map(|opened| opened.sam_runtime.is_closing() || opened.meta.closing)
                        .unwrap_or(false)
                };
                let tab_text_color = if tab_closing {
                    APP_TAB_DISABLED_TEXT
                } else {
                    APP_TAB_TEXT
                };
                let tab_content = container(
                    row![
                        text(&tab.title).size(13),
                        tab_status_marker(tab, blink_on, tab_closing),
                    ]
                    .spacing(6)
                    .align_y(Alignment::Center),
                )
                .padding([4, 10])
                .style(move |_| tab_indicator_style(tab_text_color));

                row_acc.push(if tab.kind == TabKind::AppHome {
                    row![
                        button(tab_content)
                            .style(move |theme, status| tab_button_style(theme, status, selected))
                            .on_press(Message::TabSelected(idx)),
                    ]
                    .spacing(4)
                    .align_y(Alignment::Center)
                } else if tab_closing {
                    row![
                        button(tab_content)
                            .style(move |theme, status| tab_button_style(theme, status, selected)),
                        button(text("x").size(11))
                            .padding([2, 6])
                            .style(tab_close_button_style),
                    ]
                    .spacing(4)
                    .align_y(Alignment::Center)
                } else {
                    row![
                        button(tab_content)
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

        let active_group_counts = state.active_tab().and_then(|tab| {
            tab.group.as_ref().map(|group| {
                let active_peers = group
                    .peers
                    .iter()
                    .filter(|peer| peer.ready && peer.authorized)
                    .count();
                (
                    active_peers.saturating_add(1),
                    group.meta.members.len().saturating_add(1),
                )
            })
        });
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
        let group_active_label = active_group_counts
            .map(|(active, total)| format!("{active}/{total} active"))
            .unwrap_or_else(|| "0/0 active".into());
        let group_active_indicator = container(
            text(group_active_label)
                .size(13)
                .color(Color::from_rgb8(150, 150, 158)),
        )
        .padding([4, 10])
        .style(|_| status_address_container_style());

        let right_status = container(if active_group_counts.is_some() {
            row![
                if my_b32_available {
                    my_b32_button.on_press(Message::CopyStatusMyB32Pressed)
                } else {
                    my_b32_button
                },
                text(" : ").size(13),
                group_active_indicator,
            ]
            .align_y(Alignment::Center)
        } else {
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
            .align_y(Alignment::Center)
        });

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

        let show_deaddrop_panel =
            state.session.show_deaddrop_panel && Self::deaddrop_panel_allowed(&state.session);
        let show_group_panel = state.active_tab_is_group() && state.session.show_group_panel;
        let bottom_panel_open = state.session.show_logs || show_deaddrop_panel || show_group_panel;

        let messages = state.session.bubbles.iter().enumerate().fold(
            column!().spacing(12).padding([8, 4]).width(Length::Fill),
            |col, (idx, bubble)| col.push(message_row(idx, bubble)),
        );

        let chat_panel = container(
            scrollable(messages)
                .id(state.session.messages_scroll_id.clone())
                .height(Length::Fill)
                .width(Length::Fill),
        )
        .width(Length::Fill)
        .height(Length::FillPortion(if bottom_panel_open { 3 } else { 1 }))
        .padding(6)
        .style(|_| message_panel_style());

        let log_lines = state.session.log_lines.iter().fold(
            column!().spacing(4).padding([6, 4]).width(Length::Fill),
            |col, line| col.push(text(line).size(12).width(Length::Fill)),
        );

        let copy_logs_button = button(
            text("\u{e14d}")
                .font(Font {
                    family: font::Family::Name(APP_ICON_FONT_FAMILY),
                    ..Font::default()
                })
                .size(12),
        )
        .width(24)
        .height(20)
        .padding(iced::Padding {
            top: 1.0,
            right: 4.0,
            bottom: 3.0,
            left: 6.0,
        })
        .style(copy_bubble_button_style);
        let copy_logs_button = if state.session.log_lines.is_empty() {
            copy_logs_button
        } else {
            copy_logs_button.on_press(Message::CopyLogsPressed)
        };
        let log_toolbar = row![
            Space::new().width(Length::Fill),
            tooltip(
                copy_logs_button,
                container(text("Copy logs").size(11))
                    .padding([4, 6])
                    .style(|_| log_panel_style()),
                tooltip::Position::Top,
            ),
        ]
        .align_y(Alignment::Center)
        .width(Length::Fill);

        let log_inner = container(
            column![
                log_toolbar,
                scrollable(log_lines)
                    .id(state.session.logs_scroll_id.clone())
                    .height(Length::Fill)
                    .width(Length::Fill),
            ]
            .spacing(4)
            .height(Length::Fill),
        )
        .width(Length::Fill)
        .height(Length::Fill)
        .padding(6)
        .style(|_| log_panel_style());

        let log_panel = log_inner.height(Length::FillPortion(1));

        let dd_delete_confirm = state.session.deaddrop_delete_confirm.clone();
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
                            .on_press_maybe(
                                dd_delete_confirm
                                    .is_none()
                                    .then_some(Message::DdServerDeletePressed(idx))
                            ),
                    ]
                    .spacing(8)
                    .align_y(Alignment::Center)
                    .width(Length::Fill);

                    let server_confirm = match &dd_delete_confirm {
                        Some(confirm) if confirm.index == idx && confirm.server == *server => {
                            column![
                                text("Delete this deaddrop server?").size(12),
                                row![
                                    button(text("Yes").size(12))
                                        .padding([4, 8])
                                        .style(app_button_style)
                                        .on_press(Message::DdServerDeleteConfirmed),
                                    button(text("No").size(12))
                                        .padding([4, 8])
                                        .style(app_button_style)
                                        .on_press(Message::DdServerDeleteCancelled),
                                ]
                                .spacing(6)
                            ]
                            .spacing(6)
                        }
                        _ => column![],
                    };

                    col.push(
                        container(column![server_record, server_confirm].spacing(8))
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

        let group_roster_rows = if let Some(group) = selected_group {
            if group.members.is_empty() {
                column![
                    text("Roster is empty.")
                        .size(12)
                        .color(Color::from_rgb8(150, 150, 158))
                ]
                .spacing(6)
                .padding([6, 4])
                .width(Length::Fill)
            } else {
                group.members.iter().enumerate().fold(
                    column!().spacing(6).padding([6, 4]).width(Length::Fill),
                    |col, (member_idx, member)| {
                        let group_key = storage::group_storage_key(group);
                        let active = active_group_member_b32s
                            .iter()
                            .any(|b32| b32.eq_ignore_ascii_case(&member.b32));
                        let name_color = if active {
                            Color::WHITE
                        } else {
                            Color::from_rgb8(135, 135, 144)
                        };
                        let b32_color = if active {
                            Color::from_rgb8(155, 155, 162)
                        } else {
                            Color::from_rgb8(105, 105, 114)
                        };
                        let row_content = row![
                            text(if active { "●" } else { "○" })
                                .size(12)
                                .color(if active {
                                    PY_GREEN
                                } else {
                                    Color::from_rgb8(105, 105, 114)
                                }),
                            column![
                                text(&member.name)
                                    .size(12)
                                    .color(name_color)
                                    .width(Length::Fill),
                                text(&member.b32)
                                    .size(11)
                                    .color(b32_color)
                                    .width(Length::Fill),
                            ]
                            .spacing(2)
                            .width(Length::Fill),
                            button(text("Delete").size(12))
                                .padding([4, 8])
                                .style(app_button_style)
                                .on_press_maybe(
                                    (selected_group_is_admin && !group_member_confirm_active)
                                        .then_some(Message::DeleteGroupMemberPressed(member_idx))
                                ),
                        ]
                        .spacing(8)
                        .align_y(Alignment::Center)
                        .width(Length::Fill);

                        let member_confirm = match &state.session.sidebar_confirm {
                            Some(SidebarConfirm::DeleteGroupMember {
                                group_key: confirm_group_key,
                                member_b32,
                                member_name,
                            }) if confirm_group_key == &group_key
                                && member_b32.eq_ignore_ascii_case(&member.b32) =>
                            {
                                column![
                                    text(format!("Delete member {member_name}?")).size(12),
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
                                .spacing(6)
                            }
                            _ => column![],
                        };

                        col.push(
                            container(column![row_content, member_confirm].spacing(8))
                                .padding(10)
                                .width(Length::Fill)
                                .style(|_| operation_panel_style()),
                        )
                    },
                )
            }
        } else {
            column![
                text("Select or open a group.")
                    .size(12)
                    .color(Color::from_rgb8(150, 150, 158))
            ]
            .spacing(6)
            .padding([6, 4])
            .width(Length::Fill)
        };
        let now_ms = Self::now_epoch_millis();
        let pending_private_invites = selected_group
            .filter(|_| selected_group_is_admin)
            .map(|group| {
                group
                    .issued_invites
                    .iter()
                    .filter_map(|issued| {
                        issued
                            .private_binding
                            .as_ref()
                            .filter(|binding| binding.expires_ms > now_ms)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let pending_private_invite_rows = if pending_private_invites.is_empty() {
            column![]
        } else {
            pending_private_invites.into_iter().fold(
                column![text("Pending private invites").size(12)]
                    .spacing(6)
                    .width(Length::Fill),
                |rows, binding| {
                    let request_id = binding.request_id.clone();
                    let short_id = binding.request_id.chars().take(12).collect::<String>();
                    let remaining_minutes =
                        binding.expires_ms.saturating_sub(now_ms).div_ceil(60_000);
                    rows.push(
                        container(
                            row![
                                text(format!(
                                    "Request {short_id}  expires in {remaining_minutes} min"
                                ))
                                .size(11)
                                .width(Length::Fill),
                                button(text("Revoke").size(12))
                                    .padding([4, 8])
                                    .style(app_button_style)
                                    .on_press(Message::RevokePrivateGroupInvitePressed(request_id)),
                            ]
                            .spacing(8)
                            .align_y(Alignment::Center),
                        )
                        .padding(8)
                        .width(Length::Fill)
                        .style(|_| operation_panel_style()),
                    )
                },
            )
        };
        let group_status_line: Element<'_, Message> =
            if state.session.group_status.trim().is_empty() {
                Space::new().height(0).into()
            } else {
                text(&state.session.group_status).size(12).into()
            };

        let group_panel = container(
            column![
                column![
                    text(
                        selected_group
                            .map(|group| format!("#{}", group.name))
                            .unwrap_or_else(|| "Group".into())
                    )
                    .size(13)
                    .width(Length::Fill),
                    text(if selected_group.is_some() {
                        format!(
                            "Online peers: {}  Total known: {}",
                            active_group_peer_count, selected_group_total_known
                        )
                    } else {
                        "Online peers: 0  Total known: 0".into()
                    })
                    .size(11)
                    .color(Color::from_rgb8(155, 155, 164))
                    .width(Length::Fill),
                ]
                .spacing(2)
                .width(Length::Fill),
                row![
                    button(text("Save New Name").size(12))
                        .padding([6, 10])
                        .style(app_button_style)
                        .on_press_maybe(
                            group_selected.then_some(Message::SaveGroupDisplayNamePressed)
                        ),
                    text_input(
                        "My group display name...",
                        &state.session.group_display_name_input
                    )
                    .on_input(Message::GroupDisplayNameInputChanged)
                    .on_submit(Message::SaveGroupDisplayNamePressed)
                    .padding(8)
                    .size(13)
                    .width(Length::Fixed(220.0)),
                    container(text(format!("Role: {selected_group_role}")).size(12).color(
                        if selected_group_is_admin {
                            PY_GREEN
                        } else {
                            Color::from_rgb8(175, 175, 184)
                        }
                    ),)
                    .padding([4, 8])
                    .style(move |_| {
                        let role_color = if selected_group_is_admin {
                            PY_GREEN
                        } else {
                            Color::from_rgb8(175, 175, 184)
                        };
                        indicator_style(Color::from_rgb8(35, 35, 40), role_color)
                    }),
                ]
                .spacing(8)
                .align_y(Alignment::Center),
                row![
                    button(text("Generate Public Invite").size(12))
                        .padding([6, 10])
                        .style(app_button_style)
                        .on_press_maybe(
                            (group_selected && selected_group_is_admin)
                                .then_some(Message::GenerateGroupInvitePressed)
                        ),
                    text_input(
                        "Generated invite appears here...",
                        &state.session.group_generated_invite_string
                    )
                    .padding(8)
                    .size(12)
                    .width(Length::Fixed(360.0)),
                    button(text("Copy Public Invite").size(12))
                        .padding([6, 10])
                        .style(app_button_style)
                        .on_press_maybe(
                            (selected_group_is_admin
                                && !state
                                    .session
                                    .group_generated_invite_string
                                    .trim()
                                    .is_empty())
                            .then_some(Message::CopyGeneratedGroupInvitePressed),
                        ),
                ]
                .spacing(8)
                .align_y(Alignment::Center),
                row![
                    button(text("Generate Private Invite").size(12))
                        .padding([6, 10])
                        .style(app_button_style)
                        .on_press_maybe(
                            (group_selected && selected_group_is_admin)
                                .then_some(Message::GeneratePrivateGroupInvitePressed)
                        ),
                    text_input(
                        "Paste recipient's private request...",
                        &state.session.group_private_request_input,
                    )
                    .on_input_maybe(
                        selected_group_is_admin
                            .then_some(Message::PrivateGroupRequestInputChanged)
                    )
                    .on_submit_maybe(
                        selected_group_is_admin
                            .then_some(Message::GeneratePrivateGroupInvitePressed)
                    )
                    .padding(8)
                    .size(12)
                    .width(Length::Fixed(260.0)),
                    text_input(
                        "Generated private invite appears here...",
                        &state.session.group_generated_private_invite_string,
                    )
                    .padding(8)
                    .size(12)
                    .width(Length::Fixed(260.0)),
                    button(text("Copy Private Invite").size(12))
                        .padding([6, 10])
                        .style(app_button_style)
                        .on_press_maybe(
                            (selected_group_is_admin
                                && !state
                                    .session
                                    .group_generated_private_invite_string
                                    .trim()
                                    .is_empty())
                            .then_some(Message::CopyGeneratedPrivateGroupInvitePressed),
                        ),
                ]
                .spacing(8)
                .align_y(Alignment::Center),
                scrollable(
                    column![pending_private_invite_rows, group_roster_rows]
                        .spacing(8)
                        .width(Length::Fill)
                )
                    .height(Length::Fill)
                    .width(Length::Fill),
                group_status_line,
            ]
            .spacing(8),
        )
        .width(Length::Fill)
        .height(Length::FillPortion(GROUP_PANEL_HEIGHT_PORTION))
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
                !state.opened_tabs.is_empty(),
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
        } else if show_group_panel {
            column![chat_panel, group_panel]
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
        let message_input_placeholder = if message_input_enabled {
            "Type message..."
        } else {
            "Chat is not connected or offline-ready."
        };

        let message_input = text_editor(&state.session.input_editor)
            .placeholder(message_input_placeholder)
            .padding(12)
            .size(16)
            .height(Length::Fixed(82.0))
            .min_height(52.0)
            .max_height(140.0)
            .wrapping(iced::widget::text::Wrapping::WordOrGlyph)
            .key_binding(Self::message_editor_key_binding);

        let message_input = if message_input_enabled {
            message_input.on_action(Message::InputChanged)
        } else {
            message_input
        };

        let pending_image_sendable =
            state.session.pending_image.is_some() && state.can_send_live_image();
        let send_button_enabled = (message_input_enabled && !state.session.input.trim().is_empty())
            || pending_image_sendable;
        let send_button = button(text("Send").size(13))
            .padding([8, 14])
            .style(app_button_style);
        let send_button = if send_button_enabled {
            send_button.on_press(Message::SendPressed)
        } else {
            send_button
        };

        let reply_preview: Element<'_, Message> = if let Some(reply) = &state.session.reply_to {
            let preview = compact_reply_preview(&reply.text, 140);
            container(
                row![
                    column![
                        text(format!("Replying to {}", reply.author))
                            .size(11)
                            .color(Color::from_rgb8(170, 170, 178)),
                        text(preview)
                            .size(12)
                            .color(Color::from_rgb8(210, 210, 216))
                            .width(Length::Fill)
                            .wrapping(iced::widget::text::Wrapping::WordOrGlyph),
                    ]
                    .spacing(2)
                    .width(Length::Fill),
                    button(text("x").size(11))
                        .padding([2, 6])
                        .style(copy_bubble_button_style)
                        .on_press(Message::CancelReplyPressed),
                ]
                .spacing(8)
                .align_y(Alignment::Center),
            )
            .width(Length::Fill)
            .padding([6, 8])
            .style(|_| reply_preview_style())
            .into()
        } else {
            Space::new().height(0).into()
        };

        let pending_image_preview: Element<'_, Message> =
            if let Some(draft) = &state.session.pending_image {
                let source_width = draft.image.width.max(1) as f32;
                let source_height = draft.image.height.max(1) as f32;
                let scale = (96.0 / source_width).min(64.0 / source_height).min(1.0);
                let preview_width = (source_width * scale).max(1.0);
                let preview_height = (source_height * scale).max(1.0);

                container(
                    row![
                        image(draft.image.handle.clone())
                            .width(preview_width)
                            .height(preview_height)
                            .content_fit(ContentFit::Contain),
                        column![
                            text("Pasted image")
                                .size(12)
                                .color(Color::from_rgb8(210, 210, 216)),
                            text(format!("{} x {}", draft.image.width, draft.image.height))
                                .size(11)
                                .color(Color::from_rgb8(155, 155, 164)),
                        ]
                        .spacing(2)
                        .width(Length::Fill),
                        button(text("x").size(11))
                            .padding([2, 6])
                            .style(copy_bubble_button_style)
                            .on_press(Message::CancelPendingImagePressed),
                    ]
                    .spacing(8)
                    .align_y(Alignment::Center),
                )
                .width(Length::Fill)
                .padding([6, 8])
                .style(|_| reply_preview_style())
                .into()
            } else {
                Space::new().height(0).into()
            };

        let message_input_panel = container(
            column![
                pending_image_preview,
                reply_preview,
                row![container(message_input).width(Length::Fill), send_button,]
                    .spacing(8)
                    .align_y(Alignment::Center)
            ]
            .spacing(6),
        )
        .width(Length::Fill)
        .padding(8)
        .style(container::rounded_box);

        let rendezvous_ui_available = state.session.profile == "default"
            && !state.active_tab_is_group()
            && !state.session.offline_mode
            && !state.has_active_connection_attempt();

        let actions_row = state
            .available_actions()
            .into_iter()
            .fold(
                row!().spacing(8).align_y(Alignment::Center),
                |row_acc, action| {
                    let btn = button(
                        text(IcedCommApp::action_label_for_session(
                            &state.session,
                            action,
                        ))
                        .size(13),
                    )
                    .padding([6, 10])
                    .style(app_button_style);

                    if IcedCommApp::action_enabled_for_session(&state.session, action) {
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
            );

        let actions_row = if state.active_tab_is_group() {
            actions_row.push(
                button(
                    text(if state.session.show_group_panel {
                        "Hide Group"
                    } else {
                        "Show Group"
                    })
                    .size(13),
                )
                .padding([6, 10])
                .style(app_button_style)
                .on_press(Message::ToggleGroupPanelPressed),
            )
        } else {
            actions_row
        };

        let actions_row = actions_row.push(
            button(text(IcedCommApp::action_label(GuiAction::Help)).size(13))
                .padding([6, 10])
                .style(app_button_style),
        );

        let actions_row = if rendezvous_ui_available {
            let rendezvous_toggle_enabled =
                state.session.pending_action != Some(GuiAction::Connect);
            actions_row.push(
                button(
                    text(if state.session.show_rendezvous_panel {
                        "Hide Rendezvous"
                    } else {
                        "Rendezvous"
                    })
                    .size(13),
                )
                .padding([6, 10])
                .style(app_button_style)
                .on_press_maybe(
                    rendezvous_toggle_enabled.then_some(Message::ToggleRendezvousPanelPressed),
                ),
            )
        } else {
            actions_row
        };

        let command_panel_content = if let Some(action) = state.session.pending_action {
            if IcedCommApp::action_needs_param(action) {
                column![
                    actions_row,
                    row![
                        text_input(
                            IcedCommApp::action_placeholder(action),
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
            } else if IcedCommApp::action_needs_confirm(action) {
                column![
                    actions_row,
                    row![
                        text(IcedCommApp::action_confirm_prompt(action))
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

        let rendezvous_panel: Element<'_, Message> = if rendezvous_ui_available
            && state.session.show_rendezvous_panel
        {
            let has_input = !state.session.rendezvous_input.trim().is_empty();
            let has_output = !state.session.rendezvous_output.trim().is_empty();
            let rendezvous_input_kind =
                rendezvous::input_kind(&state.session.rendezvous_input);
            let can_answer_request = has_input
                && rendezvous_input_kind == rendezvous::InputKind::Request
                && state.session.my_b32.is_some();
            let can_connect_response = has_input
                && rendezvous_input_kind == rendezvous::InputKind::Response
                && state
                    .session
                    .rendezvous_request
                    .as_ref()
                    .map(|pending| {
                        rendezvous::response_matches_pending(
                            &state.session.rendezvous_input,
                            pending,
                        )
                    })
                    .unwrap_or(false);
            let has_state = state.session.rendezvous_request.is_some()
                || state.session.rendezvous_issued.is_some()
                || state.session.rendezvous_outgoing.is_some();

            container(
                column![
                    row![
                        button(text("Generate Request").size(12))
                            .padding([6, 10])
                            .style(app_button_style)
                            .on_press(Message::GenerateRendezvousRequestPressed),
                        button(text("Copy Output").size(12))
                            .padding([6, 10])
                            .style(app_button_style)
                            .on_press_maybe(
                                has_output.then_some(Message::CopyRendezvousOutputPressed)
                            ),
                        button(text("Revoke").size(12))
                            .padding([6, 10])
                            .style(app_button_style)
                            .on_press_maybe(has_state.then_some(Message::RevokeRendezvousPressed)),
                    ]
                    .spacing(8)
                    .align_y(Alignment::Center),
                    row![
                        text_input(
                            "Paste a rendezvous request or response...",
                            &state.session.rendezvous_input
                        )
                        .on_input(Message::RendezvousInputChanged)
                        .padding(8)
                        .size(12)
                        .width(Length::Fill),
                        button(text("Answer Request").size(12))
                            .padding([6, 10])
                            .style(app_button_style)
                            .on_press_maybe(
                                can_answer_request
                                    .then_some(Message::AnswerRendezvousRequestPressed)
                            ),
                        button(text("Connect Response").size(12))
                            .padding([6, 10])
                            .style(app_button_style)
                            .on_press_maybe(
                                can_connect_response
                                    .then_some(Message::ConnectRendezvousResponsePressed)
                            ),
                        button(text("Clear").size(12))
                            .padding([6, 10])
                            .style(app_button_style)
                            .on_press(Message::ClearRendezvousPressed),
                    ]
                    .spacing(8)
                    .align_y(Alignment::Center),
                    text_input(
                        "Generated request or sealed response appears here...",
                        &state.session.rendezvous_output
                    )
                    .padding(8)
                    .size(12)
                    .width(Length::Fill),
                    text(&state.session.rendezvous_status)
                        .size(11)
                        .color(Color::from_rgb8(155, 155, 164)),
                ]
                .spacing(8),
            )
            .width(Length::Fill)
            .padding(8)
            .style(|_| log_panel_style())
            .into()
        } else {
            Space::new().height(0).into()
        };

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
                rendezvous_panel,
                command_panel
            ]
            .spacing(10)
            .width(Length::Fill)
            .height(Length::Fill)
        };

        let app_content: Element<'_, Message> = container(
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
        .into();

        let Some(deadline_ms) = state.sam_shutdown_deadline_ms else {
            return app_content;
        };

        let remaining_ms = deadline_ms.saturating_sub(Self::now_epoch_millis());
        let remaining_seconds = remaining_ms.saturating_add(999) / 1_000;
        let shutdown_action: Element<'_, Message> = if state.sam_shutdown_started {
            button(text("Shutting down...").size(13))
                .padding([8, 14])
                .style(app_button_style)
                .into()
        } else {
            button(text("Shut Down Now").size(13))
                .padding([8, 14])
                .style(app_button_style)
                .on_press(Message::SamShutdownNowPressed)
                .into()
        };

        let shutdown_card = container(
            column![
                text("I2P Router Unavailable").size(20).color(PY_RED),
                text(
                    "The configured SAM endpoint is not responding. IcedComm will shut down securely."
                )
                .size(13),
                text(format!("Automatic shutdown in {remaining_seconds} seconds.")).size(14),
                shutdown_action,
            ]
            .spacing(14)
            .align_x(Alignment::Center),
        )
        .padding(24)
        .max_width(560)
        .style(|_| container::Style {
            background: Some(Background::Color(Color::from_rgb8(28, 28, 34))),
            border: border::Border {
                color: PY_RED,
                width: 1.5,
                radius: border::Radius::from(6.0),
            },
            ..Default::default()
        });

        let shutdown_overlay = opaque(
            container(shutdown_card)
                .width(Length::Fill)
                .height(Length::Fill)
                .center_x(Length::Fill)
                .center_y(Length::Fill)
                .style(|_| container::Style {
                    background: Some(Background::Color(Color::from_rgba8(0, 0, 0, 0.78))),
                    ..Default::default()
                }),
        );

        stack![app_content, shutdown_overlay]
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
        current_utc_hms()
    }

    fn sam_lifecycle_log(line: impl AsRef<str>) {
        if SAM_LIFECYCLE_DEBUG {
            eprintln!("[{}][SAM-LIFE] {}", Self::now_utc_hms(), line.as_ref());
        }
    }

    fn generate_msg_id(&self) -> u64 {
        Self::generate_msg_id_value()
    }

    fn generate_msg_id_value() -> u64 {
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

    fn heartbeat_nonce() -> String {
        format!("{:016x}", Self::generate_msg_id_value())
    }

    fn heartbeat_ping_frame() -> Frame {
        Frame {
            msg_type: MsgType::S,
            msg_id: Self::generate_msg_id_value(),
            payload: format!("{HEARTBEAT_PING_PREFIX}{}", Self::heartbeat_nonce()).into_bytes(),
        }
    }

    fn heartbeat_pong_frame(nonce: &str) -> Frame {
        Frame {
            msg_type: MsgType::S,
            msg_id: Self::generate_msg_id_value(),
            payload: format!("{HEARTBEAT_PONG_PREFIX}{nonce}").into_bytes(),
        }
    }

    fn heartbeat_ping_task(tab_id: u64, conn: LiveConnection) -> Task<Message> {
        let frame = Self::heartbeat_ping_frame();
        Task::perform(
            async move { conn.send_frame(&frame).await.map_err(|e| e.to_string()) },
            move |result| Message::SendFinished(tab_id, result),
        )
    }

    fn heartbeat_pong_task(tab_id: u64, conn: LiveConnection, nonce: String) -> Task<Message> {
        let frame = Self::heartbeat_pong_frame(&nonce);
        Task::perform(
            async move { conn.send_frame(&frame).await.map_err(|e| e.to_string()) },
            move |result| Message::SendFinished(tab_id, result),
        )
    }

    fn verify_pending_rendezvous_auth(
        tab: &mut OpenedTab,
        body: &str,
        now_ms: u64,
    ) -> Result<bool, String> {
        if !body.starts_with(rendezvous::AUTH_SIGNAL_PREFIX) {
            return Ok(false);
        }
        if tab.session.profile != "default" {
            return Err("rendezvous proof received outside transient mode".into());
        }

        let caller_b32 = tab
            .session
            .pending_peer_addr
            .clone()
            .ok_or_else(|| "pending caller address is unavailable".to_string())?;
        let receiver_b32 = tab
            .session
            .my_b32
            .clone()
            .ok_or_else(|| "local transient address is unavailable".to_string())?;
        let issued = tab
            .session
            .rendezvous_issued
            .as_ref()
            .ok_or_else(|| "no one-time rendezvous invitation is active".to_string())?;

        rendezvous::verify_auth_signal(body, issued, &caller_b32, &receiver_b32, now_ms)?;
        let request_id = issued.request_id;
        if let Some(issued) = tab.session.rendezvous_issued.as_mut() {
            issued.state = RendezvousIssuedState::Reserved;
        }
        tab.session.pending_rendezvous_request_id = Some(request_id);
        tab.session.rendezvous_status =
            "Authenticated rendezvous caller is awaiting Accept / Decline.".into();
        Ok(true)
    }

    fn verify_live_rendezvous_auth(
        tab: &mut OpenedTab,
        body: &str,
        now_ms: u64,
    ) -> Result<bool, String> {
        if !body.starts_with(rendezvous::AUTH_SIGNAL_PREFIX) {
            return Ok(false);
        }
        if tab.session.profile != "default"
            || tab.connection_direction != Some(ConnectionDirection::Inbound)
        {
            return Err("unexpected rendezvous proof".into());
        }

        let caller_b32 = tab
            .session
            .current_peer_addr
            .clone()
            .ok_or_else(|| "caller address is unavailable".to_string())?;
        let receiver_b32 = tab
            .session
            .my_b32
            .clone()
            .ok_or_else(|| "local transient address is unavailable".to_string())?;
        let issued = tab
            .session
            .rendezvous_issued
            .as_ref()
            .ok_or_else(|| "no one-time rendezvous invitation is active".to_string())?;

        rendezvous::verify_auth_signal(body, issued, &caller_b32, &receiver_b32, now_ms)?;
        if let Some(issued) = tab.session.rendezvous_issued.as_mut() {
            issued.state = RendezvousIssuedState::Consumed;
        }
        tab.session.rendezvous_status = "One-time rendezvous authentication accepted.".into();
        Ok(true)
    }

    fn release_pending_rendezvous(tab: &mut OpenedTab) {
        let Some(request_id) = tab.session.pending_rendezvous_request_id.take() else {
            return;
        };
        if let Some(issued) = tab.session.rendezvous_issued.as_mut() {
            if issued.request_id == request_id
                && issued.state == RendezvousIssuedState::Reserved
            {
                issued.state = if issued.expires_ms > Self::now_epoch_millis() {
                    RendezvousIssuedState::Available
                } else {
                    RendezvousIssuedState::Revoked
                };
            }
        }
    }

    fn track_group_send_task(&self, tab_id: u64, task: Task<Message>) -> Task<Message> {
        if let Some(tab) = self
            .opened_tabs
            .iter()
            .find(|tab| tab.id == tab_id && tab.meta.kind == TabKind::Group)
        {
            tab.sam_runtime.track_send_task(task)
        } else {
            task
        }
    }

    fn reset_connection_state(&mut self) {
        if let Some(tab) = self.active_tab_mut() {
            Self::release_pending_rendezvous(tab);
            Self::invalidate_one_to_one_connect(tab, true);
            tab.connection_direction = None;
        }
        self.set_active_live_conn(None);
        self.set_active_pending_conn(None);
        self.session.current_peer_addr = None;
        self.session.current_peer_dest_b64 = None;
        self.session.peer_b32 = None;
        self.session.pending_peer_addr = None;
        self.session.pending_peer_dest_b64 = None;
        self.session.pending_rendezvous_request_id = None;
        self.session.live_ready = false;
        self.session.offline_mode = false;
        self.session.network_status = NetworkStatus::LocalOk;
        self.session.heartbeat_last_rx_ms = 0;
        self.session.heartbeat_last_ping_ms = 0;
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
            if self.opened_tabs[real_idx].sam_runtime.is_closing()
                || self.opened_tabs[real_idx].meta.closing
            {
                self.post_system(format!(
                    "Tab for {profile_name} is still closing. Wait for cleanup to finish."
                ));
                return;
            }

            self.session.active_tab_idx = Some(Self::real_to_visible_tab_index(real_idx));
            self.session.profile = self.opened_tabs[real_idx].meta.profile_name.clone();
            self.refresh_visible_from_active_tab_reset_editor();
            return;
        }

        self.opened_tabs.push(self.new_opened_tab(profile_name));
        let real_idx = self.opened_tabs.len() - 1;
        self.session.active_tab_idx = Some(Self::real_to_visible_tab_index(real_idx));
        self.session.profile = profile_name.to_string();
        self.refresh_visible_from_active_tab_reset_editor();
    }

    fn open_or_focus_tab_for_group(&mut self, group_key: &str) {
        self.store_active_runtime();

        let profile_name = format!("group:{group_key}");
        if let Some(real_idx) = self
            .opened_tabs
            .iter()
            .position(|t| t.meta.kind == TabKind::Group && t.meta.profile_name == profile_name)
        {
            if self.opened_tabs[real_idx].sam_runtime.is_closing()
                || self.opened_tabs[real_idx].meta.closing
            {
                self.session.group_status =
                    "Group tab is still closing. Wait for cleanup to finish.".into();
                return;
            }

            self.session.active_tab_idx = Some(Self::real_to_visible_tab_index(real_idx));
            self.session.profile = self.opened_tabs[real_idx].meta.profile_name.clone();
            self.session.selected_group_idx = self
                .session
                .groups
                .iter()
                .position(|group| storage::group_storage_key(group) == group_key);
            self.session.group_display_name_input = self
                .session
                .selected_group_idx
                .and_then(|idx| self.session.groups.get(idx))
                .map(|group| group.my_name.clone())
                .unwrap_or_default();
            self.refresh_visible_from_active_tab_reset_editor();
            return;
        }

        let Some(group_meta) = self
            .session
            .groups
            .iter()
            .find(|group| storage::group_storage_key(group) == group_key)
            .cloned()
        else {
            self.session.group_status = format!("Group not found: {group_key}");
            return;
        };

        self.opened_tabs.push(self.new_opened_group_tab(group_meta));
        let real_idx = self.opened_tabs.len() - 1;
        self.session.active_tab_idx = Some(Self::real_to_visible_tab_index(real_idx));
        self.session.profile = profile_name;
        self.session.selected_group_idx = self
            .session
            .groups
            .iter()
            .position(|group| storage::group_storage_key(group) == group_key);
        self.session.group_display_name_input = self
            .session
            .selected_group_idx
            .and_then(|idx| self.session.groups.get(idx))
            .map(|group| group.my_name.clone())
            .unwrap_or_default();
        self.refresh_visible_from_active_tab_reset_editor();
    }

    fn export_group_invite_file(path: &Path, group: &GroupMeta) -> Result<GroupMeta, String> {
        let (updated_group, invite) = Self::issue_group_invite(group)?;
        let text = serde_json::to_string_pretty(&invite).map_err(|e| e.to_string())?;
        std::fs::write(path, text).map_err(|e| e.to_string())?;
        Ok(updated_group)
    }

    fn encode_group_invite_string(group: &GroupMeta) -> Result<(GroupMeta, String), String> {
        let (updated_group, invite) = Self::issue_group_invite(group)?;
        let json = serde_json::to_vec(&invite).map_err(|e| e.to_string())?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&json).map_err(|e| e.to_string())?;
        let compressed = encoder.finish().map_err(|e| e.to_string())?;
        let encoded = general_purpose::URL_SAFE_NO_PAD.encode(compressed);
        Ok((
            updated_group,
            format!("{GROUP_INVITE_STRING_PREFIX}{encoded}"),
        ))
    }

    fn encode_private_group_invite_string(
        group: &GroupMeta,
        request: &str,
    ) -> Result<(GroupMeta, String), String> {
        if group.my_b32.is_none() {
            return Err("open this group once before generating its invite".into());
        }
        if !Self::group_is_admin(group) {
            return Err("only the group admin can issue private invites".into());
        }

        let now_ms = Self::now_epoch_millis();
        let token = Self::generate_group_invite_token();
        let mut updated_group = group.clone();
        updated_group.issued_invites.retain(|issued| {
            issued
                .private_binding
                .as_ref()
                .map(|binding| binding.expires_ms > now_ms)
                .unwrap_or(true)
        });
        Self::sign_group_roster_if_admin(&mut updated_group)?;
        let invite =
            Self::group_invite_from_meta_with_token(&updated_group, Some(token.clone()))?;
        let invite_json = serde_json::to_vec(&invite).map_err(|err| err.to_string())?;
        let (binding, encoded) = group_invite::seal_invite(request, &invite_json, now_ms)?;

        if updated_group.issued_invites.iter().any(|issued| {
            issued
                .private_binding
                .as_ref()
                .map(|existing| existing.request_id == binding.request_id)
                .unwrap_or(false)
        }) {
            return Err("that private group request was already answered".into());
        }

        updated_group.issued_invites.push(GroupIssuedInvite {
            token,
            redeemed_b32: None,
            private_binding: Some(binding),
        });
        storage::save_group_meta(&updated_group).map_err(|err| err.to_string())?;
        Ok((updated_group, encoded))
    }

    fn import_group_invite_string(value: &str) -> Result<String, String> {
        let trimmed = value.trim();
        let encoded = trimmed
            .strip_prefix(GROUP_INVITE_STRING_PREFIX)
            .ok_or_else(|| "invite string has wrong prefix".to_string())?;

        let compressed = general_purpose::URL_SAFE_NO_PAD
            .decode(encoded.as_bytes())
            .map_err(|e| e.to_string())?;
        let mut decoder = GzDecoder::new(Cursor::new(compressed));
        let mut json = Vec::new();
        decoder.read_to_end(&mut json).map_err(|e| e.to_string())?;
        let invite: GroupInvite = serde_json::from_slice(&json).map_err(|e| e.to_string())?;
        Self::merge_group_invite(invite)
    }

    fn import_private_group_invite_string(value: &str) -> Result<String, String> {
        let request_id = group_invite::response_request_id(value)?;
        let mut pending = storage::load_pending_private_group_invites()
            .map_err(|err| err.to_string())?;
        let Some(request_idx) = pending
            .iter()
            .position(|request| request.request_id == request_id)
        else {
            return Err("this private invite belongs to a different request".into());
        };

        let request = pending[request_idx].clone();
        let (invite_json, credential) =
            group_invite::open_invite(value, &request, Self::now_epoch_millis())?;
        let invite: GroupInvite =
            serde_json::from_slice(&invite_json).map_err(|err| err.to_string())?;
        if invite.invite_token.is_none() {
            return Err("private group invite is missing its join token".into());
        }

        let group_key = Self::merge_group_invite_with_private_credential(
            invite,
            Some(credential),
        )?;
        pending.remove(request_idx);
        storage::save_pending_private_group_invites(&pending)
            .map_err(|err| format!("group imported but private request cleanup failed: {err}"))?;
        Ok(group_key)
    }

    fn group_invite_from_meta(group: &GroupMeta) -> Result<GroupInvite, String> {
        Self::group_invite_from_meta_with_token(group, None)
    }

    fn issue_group_invite(group: &GroupMeta) -> Result<(GroupMeta, GroupInvite), String> {
        if group.my_b32.is_none() {
            return Err("open this group once before exporting its invite".into());
        }
        if !Self::group_is_admin(group) {
            return Err("only the group admin can issue invites".into());
        }

        let token = Self::generate_group_invite_token();
        let mut updated_group = group.clone();
        Self::sign_group_roster_if_admin(&mut updated_group)?;
        updated_group.issued_invites.push(GroupIssuedInvite {
            token: token.clone(),
            redeemed_b32: None,
            private_binding: None,
        });
        storage::save_group_meta(&updated_group).map_err(|e| e.to_string())?;

        let invite = Self::group_invite_from_meta_with_token(&updated_group, Some(token))?;
        Ok((updated_group, invite))
    }

    fn group_invite_from_meta_with_token(
        group: &GroupMeta,
        invite_token: Option<String>,
    ) -> Result<GroupInvite, String> {
        let Some(inviter_b32) = group.my_b32.clone() else {
            return Err("open this group once before exporting its invite".into());
        };

        Ok(GroupInvite {
            format: "commtools-i2p-group-invite".into(),
            version: 1,
            group_name: group.name.clone(),
            inviter_name: Self::group_self_display_name(group),
            inviter_b32,
            owner_b32: group.owner_b32.clone().or_else(|| group.my_b32.clone()),
            invite_token,
            roster_version: group.roster_version,
            members: group.members.clone(),
            roster_signing_pubkey: group.roster_signing_pubkey.clone(),
            roster_signature: group.roster_signature.clone(),
        })
    }

    fn generate_group_invite_token() -> String {
        let token: [u8; 32] = random();
        general_purpose::URL_SAFE_NO_PAD.encode(token)
    }

    fn group_self_display_name(group: &GroupMeta) -> String {
        let trimmed = group.my_name.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }

        format!("member-{}", short_b32(group.my_b32.as_deref()))
    }

    fn group_is_admin(group: &GroupMeta) -> bool {
        match (group.my_b32.as_deref(), group.owner_b32.as_deref()) {
            (Some(my_b32), Some(owner_b32)) => my_b32.eq_ignore_ascii_case(owner_b32),
            _ => false,
        }
    }

    fn validate_group_display_name(name: &str) -> Result<(), String> {
        if name.trim().is_empty() {
            return Err("Group display name cannot be empty.".into());
        }
        if name.chars().count() > 32 {
            return Err("Group display name must be 32 characters or less.".into());
        }
        if name.chars().any(char::is_control) {
            return Err("Group display name cannot contain control characters.".into());
        }
        Ok(())
    }

    fn apply_group_member_rename(
        group: &mut GroupMeta,
        member_b32: &str,
        display_name: String,
    ) -> Result<bool, String> {
        Self::validate_group_display_name(&display_name)?;

        let Some(existing) = group
            .members
            .iter_mut()
            .find(|existing| existing.b32.eq_ignore_ascii_case(member_b32))
        else {
            return Err("group member is not in the roster".into());
        };

        if existing.name == display_name {
            return Ok(false);
        }

        existing.name = display_name;
        group.roster_version = group.roster_version.saturating_add(1);
        Self::sign_group_roster_if_admin(group)?;
        Ok(true)
    }

    fn canonical_group_members(group: &GroupMeta) -> Vec<GroupMember> {
        let mut members = group.members.clone();
        if let (Some(my_b32), Some(owner_b32)) =
            (group.my_b32.as_deref(), group.owner_b32.as_deref())
        {
            if my_b32.eq_ignore_ascii_case(owner_b32)
                && !members
                    .iter()
                    .any(|member| member.b32.eq_ignore_ascii_case(owner_b32))
            {
                members.push(GroupMember {
                    name: Self::group_self_display_name(group),
                    b32: owner_b32.to_string(),
                });
            }
        }
        members.sort_by(|a, b| {
            a.b32
                .to_lowercase()
                .cmp(&b.b32.to_lowercase())
                .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
        });
        members
    }

    fn group_roster_signature_payload(group: &GroupMeta) -> Result<Vec<u8>, String> {
        let Some(owner_b32) = group.owner_b32.clone() else {
            return Err("group owner address is not known".into());
        };

        let payload = GroupRosterSignaturePayload {
            format: "commtools-i2p-group-roster-signature".into(),
            version: 1,
            group_name: group.name.clone(),
            owner_b32,
            roster_version: group.roster_version,
            members: Self::canonical_group_members(group),
        };

        serde_json::to_vec(&payload).map_err(|e| e.to_string())
    }

    fn ensure_group_roster_signing_key(group: &mut GroupMeta) -> Result<(), String> {
        if group.roster_signing_secret.is_some() && group.roster_signing_pubkey.is_some() {
            return Ok(());
        }

        let secret: [u8; 32] = random();
        let signing_key = SigningKey::from_bytes(&secret);
        group.roster_signing_secret = Some(general_purpose::STANDARD.encode(secret));
        group.roster_signing_pubkey =
            Some(general_purpose::STANDARD.encode(signing_key.verifying_key().to_bytes()));
        Ok(())
    }

    fn sign_group_roster_if_admin(group: &mut GroupMeta) -> Result<(), String> {
        if !Self::group_is_admin(group) {
            return Ok(());
        }

        Self::ensure_group_roster_signing_key(group)?;

        let Some(secret_b64) = group.roster_signing_secret.as_deref() else {
            return Err("group roster signing secret is missing".into());
        };
        let secret = general_purpose::STANDARD
            .decode(secret_b64.as_bytes())
            .map_err(|e| e.to_string())?;
        let secret: [u8; 32] = secret
            .try_into()
            .map_err(|_| "group roster signing secret has invalid length".to_string())?;

        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();
        group.roster_signing_pubkey = Some(general_purpose::STANDARD.encode(pubkey));

        let payload = Self::group_roster_signature_payload(group)?;
        let signature = signing_key.sign(&payload);
        group.roster_signature = Some(general_purpose::STANDARD.encode(signature.to_bytes()));
        Ok(())
    }

    fn verify_group_roster_signature(
        group_name: &str,
        owner_b32: &str,
        roster_version: u64,
        members: &[GroupMember],
        pubkey_b64: &str,
        signature_b64: &str,
    ) -> Result<(), String> {
        let pubkey = general_purpose::STANDARD
            .decode(pubkey_b64.as_bytes())
            .map_err(|e| e.to_string())?;
        let pubkey: [u8; 32] = pubkey
            .try_into()
            .map_err(|_| "group roster signing public key has invalid length".to_string())?;
        let verifying_key = VerifyingKey::from_bytes(&pubkey).map_err(|e| e.to_string())?;

        let signature = general_purpose::STANDARD
            .decode(signature_b64.as_bytes())
            .map_err(|e| e.to_string())?;
        let signature: [u8; 64] = signature
            .try_into()
            .map_err(|_| "group roster signature has invalid length".to_string())?;
        let signature = Signature::from_bytes(&signature);

        let mut sorted_members = members.to_vec();
        sorted_members.sort_by(|a, b| {
            a.b32
                .to_lowercase()
                .cmp(&b.b32.to_lowercase())
                .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
        });
        let payload = GroupRosterSignaturePayload {
            format: "commtools-i2p-group-roster-signature".into(),
            version: 1,
            group_name: group_name.to_string(),
            owner_b32: owner_b32.to_string(),
            roster_version,
            members: sorted_members,
        };
        let payload = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;

        verifying_key
            .verify(&payload, &signature)
            .map_err(|e| e.to_string())
    }

    fn group_roster_sync_from_meta(group: &GroupMeta) -> Result<GroupRosterSync, String> {
        let Some(owner_b32) = group.owner_b32.clone() else {
            return Err("group owner address is not known".into());
        };
        let Some(pubkey) = group.roster_signing_pubkey.clone() else {
            return Err("group roster signing public key is missing".into());
        };
        let Some(signature) = group.roster_signature.clone() else {
            return Err("group roster signature is missing".into());
        };

        Ok(GroupRosterSync {
            format: "commtools-i2p-group-roster".into(),
            version: 1,
            group_name: group.name.clone(),
            owner_b32,
            roster_version: group.roster_version,
            members: Self::canonical_group_members(group),
            roster_signing_pubkey: pubkey,
            roster_signature: signature,
        })
    }

    fn import_group_invite_file(path: &Path) -> Result<String, String> {
        let text = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let invite: GroupInvite = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        Self::merge_group_invite(invite)
    }

    fn merge_group_invite(invite: GroupInvite) -> Result<String, String> {
        Self::merge_group_invite_with_private_credential(invite, None)
    }

    fn merge_group_invite_with_private_credential(
        invite: GroupInvite,
        private_credential: Option<PrivateJoinCredential>,
    ) -> Result<String, String> {
        if invite.format != "commtools-i2p-group-invite" || invite.version != 1 {
            return Err("unsupported group invite".into());
        }

        if invite.group_name.trim().is_empty() {
            return Err("invite group name is empty".into());
        }

        if !Self::is_valid_b32_address(&invite.inviter_b32) {
            return Err("invite contains invalid inviter address".into());
        }
        if let Some(owner_b32) = invite.owner_b32.as_deref() {
            if !Self::is_valid_b32_address(owner_b32) {
                return Err("invite contains invalid owner address".into());
            }
        }

        let incoming_owner = invite
            .owner_b32
            .clone()
            .unwrap_or_else(|| invite.inviter_b32.clone());

        let mut group = match storage::load_group_meta(&incoming_owner) {
            Ok(group) => group,
            Err(_) => {
                let mut group =
                    storage::create_group(&invite.group_name).map_err(|e| e.to_string())?;
                let old_key = storage::group_storage_key(&group);
                group.id = incoming_owner.clone();
                group.owner_b32 = Some(incoming_owner.clone());
                storage::save_group_meta(&group).map_err(|e| e.to_string())?;
                if old_key != incoming_owner {
                    let _ = storage::delete_group(&old_key);
                }
                group
            }
        };

        if group.owner_b32.is_none() {
            group.owner_b32 = Some(incoming_owner.clone());
        }
        group.id = incoming_owner.clone();

        let incoming_version = invite.roster_version;
        let invite_token = invite.invite_token.clone();
        let roster_signing_pubkey = invite.roster_signing_pubkey.clone();
        let roster_signature = invite.roster_signature.clone();
        if let Some(invite_owner) = invite.owner_b32.as_deref() {
            if !incoming_owner.eq_ignore_ascii_case(invite_owner) {
                return Err("invite owner does not match stored group owner".into());
            }
        }

        let mut incoming_members = Vec::new();
        incoming_members.push(GroupMember {
            name: invite.inviter_name,
            b32: invite.inviter_b32,
        });
        incoming_members.extend(
            invite
                .members
                .into_iter()
                .filter(|member| Self::is_valid_b32_address(&member.b32)),
        );

        if let (Some(pubkey), Some(signature)) = (
            roster_signing_pubkey.as_deref(),
            roster_signature.as_deref(),
        ) {
            if !incoming_members
                .iter()
                .any(|member| member.b32.eq_ignore_ascii_case(&incoming_owner))
            {
                return Err("signed invite roster does not contain the group owner".into());
            }
            if let Some(existing_pubkey) = group.roster_signing_pubkey.as_deref() {
                if existing_pubkey != pubkey {
                    return Err("invite roster signing key does not match stored group key".into());
                }
            }
            Self::verify_group_roster_signature(
                &invite.group_name,
                &incoming_owner,
                incoming_version,
                &incoming_members,
                pubkey,
                signature,
            )?;
            group.roster_signing_pubkey = Some(pubkey.to_string());
            group.roster_signature = Some(signature.to_string());
        } else if group.roster_signing_pubkey.is_some() {
            return Err("incoming invite roster is unsigned".into());
        }

        if incoming_version > group.roster_version {
            let my_b32 = group.my_b32.clone();
            group.members.clear();
            for member in incoming_members {
                if my_b32
                    .as_ref()
                    .map(|my_b32| my_b32.eq_ignore_ascii_case(&member.b32))
                    .unwrap_or(false)
                {
                    continue;
                }
                Self::merge_group_member(&mut group, member);
            }
            group.roster_version = incoming_version;
        } else {
            for member in incoming_members {
                Self::merge_group_member(&mut group, member);
            }
            group.roster_version = group.roster_version.max(incoming_version);
        }

        if invite_token.is_some() {
            group.join_token = invite_token;
            group.private_join_credential = private_credential;
        }

        storage::save_group_meta(&group).map_err(|e| e.to_string())?;
        Ok(storage::group_storage_key(&group))
    }

    fn merge_group_roster_sync(roster: GroupRosterSync) -> Result<String, String> {
        if roster.format != "commtools-i2p-group-roster" || roster.version != 1 {
            return Err("unsupported group roster sync".into());
        }

        if roster.group_name.trim().is_empty() {
            return Err("group roster name is empty".into());
        }
        if !Self::is_valid_b32_address(&roster.owner_b32) {
            return Err("group roster owner address is invalid".into());
        }

        let mut group = storage::load_group_meta(&roster.owner_b32).map_err(|e| e.to_string())?;
        if let Some(existing_owner) = group.owner_b32.as_deref() {
            if !existing_owner.eq_ignore_ascii_case(&roster.owner_b32) {
                return Err("group roster owner does not match stored group owner".into());
            }
        } else {
            group.owner_b32 = Some(roster.owner_b32.clone());
        }
        group.id = roster.owner_b32.clone();

        if let Some(existing_pubkey) = group.roster_signing_pubkey.as_deref() {
            if existing_pubkey != roster.roster_signing_pubkey.as_str() {
                return Err("group roster signing key does not match stored group key".into());
            }
        }

        let valid_members = roster
            .members
            .iter()
            .filter(|member| Self::is_valid_b32_address(&member.b32))
            .cloned()
            .collect::<Vec<_>>();

        Self::verify_group_roster_signature(
            &roster.group_name,
            &roster.owner_b32,
            roster.roster_version,
            &valid_members,
            &roster.roster_signing_pubkey,
            &roster.roster_signature,
        )?;

        if roster.roster_version > group.roster_version {
            let my_b32 = group.my_b32.clone();
            let self_removed = my_b32
                .as_ref()
                .map(|my_b32| {
                    !my_b32.eq_ignore_ascii_case(&roster.owner_b32)
                        && !valid_members
                            .iter()
                            .any(|member| member.b32.eq_ignore_ascii_case(my_b32))
                })
                .unwrap_or(false);

            if self_removed {
                group.members.clear();
                group.join_token = None;
                group.private_join_credential = None;
                group.roster_version = roster.roster_version;
                group.roster_signing_pubkey = Some(roster.roster_signing_pubkey);
                group.roster_signature = Some(roster.roster_signature);
                storage::save_group_meta(&group).map_err(|e| e.to_string())?;
                return Ok(storage::group_storage_key(&group));
            }

            group.members.clear();
            for member in valid_members {
                if my_b32
                    .as_ref()
                    .map(|my_b32| my_b32.eq_ignore_ascii_case(&member.b32))
                    .unwrap_or(false)
                {
                    continue;
                }
                Self::merge_group_member(&mut group, member);
            }
            group.roster_version = roster.roster_version;
            group.roster_signing_pubkey = Some(roster.roster_signing_pubkey);
            group.roster_signature = Some(roster.roster_signature);
            if group.private_join_credential.is_some()
                && my_b32
                    .as_ref()
                    .map(|my_b32| {
                        roster.owner_b32.eq_ignore_ascii_case(my_b32)
                            || roster
                                .members
                                .iter()
                                .any(|member| member.b32.eq_ignore_ascii_case(my_b32))
                    })
                    .unwrap_or(false)
            {
                group.private_join_credential = None;
                group.join_token = None;
            }
            storage::save_group_meta(&group).map_err(|e| e.to_string())?;
        }

        Ok(storage::group_storage_key(&group))
    }

    fn merge_group_member(group: &mut GroupMeta, member: GroupMember) {
        if group
            .my_b32
            .as_ref()
            .map(|my_b32| my_b32.eq_ignore_ascii_case(&member.b32))
            .unwrap_or(false)
        {
            return;
        }

        if let Some(existing) = group
            .members
            .iter_mut()
            .find(|existing| existing.b32.eq_ignore_ascii_case(&member.b32))
        {
            if !member.name.trim().is_empty() {
                existing.name = member.name;
            }
        } else {
            group.members.push(member);
        }
    }

    fn redeem_group_invite_token(
        group: &mut GroupMeta,
        token: &str,
        member: GroupMember,
    ) -> Result<(), String> {
        Self::validate_group_display_name(&member.name)?;

        let Some(invite) = group
            .issued_invites
            .iter_mut()
            .find(|invite| invite.token == token)
        else {
            return Err("invite token is unknown".into());
        };
        if invite.private_binding.is_some() {
            return Err("private invite requires proof of possession".into());
        }

        let mut changed = false;

        match &invite.redeemed_b32 {
            Some(redeemed_b32) if !redeemed_b32.eq_ignore_ascii_case(&member.b32) => {
                return Err("invite token was already redeemed".into());
            }
            Some(_) => {}
            None => {
                invite.redeemed_b32 = Some(member.b32.clone());
                changed = true;
            }
        }

        if let Some(existing) = group
            .members
            .iter_mut()
            .find(|existing| existing.b32.eq_ignore_ascii_case(&member.b32))
        {
            if existing.name != member.name {
                existing.name = member.name;
                changed = true;
            }
        } else {
            group.members.push(member);
            changed = true;
        }

        if changed {
            group.roster_version = group.roster_version.saturating_add(1);
            Self::sign_group_roster_if_admin(group)?;
        }

        Ok(())
    }

    fn redeem_private_group_invite_token(
        group: &mut GroupMeta,
        token: &str,
        member: GroupMember,
        proof: &PrivateJoinProof,
        now_ms: u64,
    ) -> Result<(), String> {
        Self::validate_group_display_name(&member.name)?;
        let Some(invite_idx) = group
            .issued_invites
            .iter()
            .position(|invite| invite.token == token)
        else {
            return Err("private invite token is unknown".into());
        };
        let Some(binding) = group.issued_invites[invite_idx].private_binding.clone() else {
            return Err("invite token is not a private invite".into());
        };
        let Some(owner_b32) = group.owner_b32.as_deref() else {
            return Err("group owner address is missing".into());
        };

        group_invite::verify_join_proof(
            &binding,
            owner_b32,
            token,
            &member.b32,
            proof,
            now_ms,
        )?;

        Self::merge_group_member(group, member);
        group.issued_invites.remove(invite_idx);
        group.roster_version = group.roster_version.saturating_add(1);
        Self::sign_group_roster_if_admin(group)?;
        Ok(())
    }

    fn send_active_group_roster_sync_task(&mut self) -> Task<Message> {
        let Some(tab_id) = self.active_tab().map(|tab| tab.id) else {
            return Task::none();
        };

        self.send_group_roster_sync_task(tab_id)
    }

    fn send_group_roster_sync_for_group_task(&mut self, group_key: &str) -> Task<Message> {
        let Some(tab_id) = self
            .opened_tabs
            .iter()
            .find(|tab| {
                tab.meta.kind == TabKind::Group
                    && tab
                        .group
                        .as_ref()
                        .map(|group| storage::group_storage_key(&group.meta) == group_key)
                        .unwrap_or(false)
            })
            .map(|tab| tab.id)
        else {
            return Task::none();
        };

        self.send_group_roster_sync_task(tab_id)
    }

    fn send_group_rename_request_task(
        &mut self,
        group_key: &str,
        display_name: String,
    ) -> Task<Message> {
        let Some(tab_id) = self
            .opened_tabs
            .iter()
            .find(|tab| {
                tab.meta.kind == TabKind::Group
                    && tab
                        .group
                        .as_ref()
                        .map(|group| storage::group_storage_key(&group.meta) == group_key)
                        .unwrap_or(false)
            })
            .map(|tab| tab.id)
        else {
            self.session.group_status =
                "Saved name locally. Open the group to send rename request.".into();
            return Task::none();
        };

        let Some(idx) = self.find_tab_index_by_id(tab_id) else {
            return Task::none();
        };

        let Some(group) = self.opened_tabs[idx].group.as_ref() else {
            return Task::none();
        };

        let (Some(my_b32), Some(owner_b32)) =
            (group.meta.my_b32.clone(), group.meta.owner_b32.clone())
        else {
            self.session.group_status =
                "Saved name locally. Group owner address is not known yet.".into();
            return Task::none();
        };

        if my_b32.eq_ignore_ascii_case(&owner_b32) {
            return Task::none();
        }

        let Some(owner_peer) = group.peers.iter().find(|peer| {
            peer.ready && peer.authorized && peer.member.b32.eq_ignore_ascii_case(&owner_b32)
        }) else {
            self.session.group_status =
                "Saved name locally. Rename request will send when owner is online.".into();
            return Task::none();
        };

        let Some(conn) = owner_peer.conn.clone() else {
            self.session.group_status =
                "Saved name locally. Rename request will send when owner is online.".into();
            return Task::none();
        };

        let control = GroupControlMessage {
            kind: GROUP_CONTROL_RENAME_REQUEST.into(),
            token: String::new(),
            b32: my_b32,
            name: display_name.clone(),
            private_request_id: None,
            private_proof_nonce: None,
            private_proof_signature: None,
        };

        let payload = match serde_json::to_vec(&control) {
            Ok(payload) => owner_peer.e2e.encrypt(&payload),
            Err(err) => {
                self.session.group_status = format!("Group rename request encode failed: {err}");
                return Task::none();
            }
        };

        let frame = Frame {
            msg_type: MsgType::L,
            msg_id: Self::generate_msg_id_value(),
            payload,
        };

        self.session.group_status =
            format!("Saved name locally. Sent rename request: {display_name}");
        let task = Task::perform(
            async move { conn.send_frame(&frame).await.map_err(|e| e.to_string()) },
            move |result| Message::SendFinished(tab_id, result),
        );
        self.track_group_send_task(tab_id, task)
    }

    fn send_group_roster_sync_task(&mut self, tab_id: u64) -> Task<Message> {
        let Some(idx) = self.find_tab_index_by_id(tab_id) else {
            return Task::none();
        };

        let Some(group) = self.opened_tabs[idx].group.as_ref() else {
            return Task::none();
        };

        if !Self::group_is_admin(&group.meta) {
            return Task::none();
        }

        let roster = match Self::group_roster_sync_from_meta(&group.meta) {
            Ok(roster) => roster,
            Err(_) => return Task::none(),
        };

        let payload = match serde_json::to_vec(&roster) {
            Ok(payload) => payload,
            Err(_) => return Task::none(),
        };

        let mut tasks = Vec::new();
        for peer in &group.peers {
            if !peer.ready || !peer.authorized {
                continue;
            }
            let Some(conn) = peer.conn.clone() else {
                continue;
            };

            let frame = Frame {
                msg_type: MsgType::L,
                msg_id: self.generate_msg_id(),
                payload: peer.e2e.encrypt(&payload),
            };
            let task = Task::perform(
                async move { conn.send_frame(&frame).await.map_err(|e| e.to_string()) },
                move |result| Message::SendFinished(tab_id, result),
            );
            tasks.push(self.track_group_send_task(tab_id, task));
        }

        Task::batch(tasks)
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
            tab.meta.closing = tab.sam_runtime.is_closing();
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

    fn close_tab_runtime_tasks(&mut self, idx: usize) -> Vec<Task<Message>> {
        let mut tasks = Vec::new();
        let quit_live_msg_id = self.generate_msg_id();
        let quit_pending_msg_id = self.generate_msg_id();

        if let Some(tab) = self.opened_tabs.get_mut(idx) {
            let tab_id = tab.id;
            let tab_kind = tab.meta.kind;
            let session_id = tab.session.sam_session_id.clone().unwrap_or_default();
            let live = tab.live_conn.clone();
            let pending = tab.pending_conn.clone();
            let group_conns: Vec<LiveConnection> = tab
                .group
                .as_ref()
                .map(|group| {
                    group
                        .peers
                        .iter()
                        .flat_map(|peer| [peer.conn.clone(), peer.pending_conn.clone()])
                        .flatten()
                        .collect()
                })
                .unwrap_or_default();
            let (registered_conns, mut sam) = tab.sam_runtime.shutdown_parts();
            let group_count = group_conns.len();
            let registered_count = registered_conns.len();

            let quit_live = Frame {
                msg_type: MsgType::S,
                msg_id: quit_live_msg_id,
                payload: b"__SIGNAL__:QUIT".to_vec(),
            };

            let quit_pending = Frame {
                msg_type: MsgType::S,
                msg_id: quit_pending_msg_id,
                payload: b"__SIGNAL__:QUIT".to_vec(),
            };

            tasks.push(Task::perform(
                async move {
                    Self::sam_lifecycle_log(format!(
                        "tab close begin tab={tab_id} kind={tab_kind:?} session={session_id} group_streams={group_count} registered_streams={registered_count}"
                    ));

                    if let Some(conn) = live {
                        Self::sam_lifecycle_log(format!("tab close live close start tab={tab_id}"));
                        let _ = conn.send_frame(&quit_live).await;
                        sleep(Duration::from_millis(120)).await;
                        let _ = conn.close().await;
                        Self::sam_lifecycle_log(format!("tab close live close done tab={tab_id}"));
                    }

                    if let Some(conn) = pending {
                        Self::sam_lifecycle_log(format!("tab close pending close start tab={tab_id}"));
                        let _ = conn.send_frame(&quit_pending).await;
                        sleep(Duration::from_millis(120)).await;
                        let _ = conn.close().await;
                        Self::sam_lifecycle_log(format!("tab close pending close done tab={tab_id}"));
                    }

                    for (stream_idx, conn) in group_conns.into_iter().enumerate() {
                        Self::sam_lifecycle_log(format!(
                            "tab close group stream close start tab={tab_id} stream={stream_idx}"
                        ));
                        let quit_group = Frame {
                            msg_type: MsgType::S,
                            msg_id: 0,
                            payload: b"__SIGNAL__:QUIT".to_vec(),
                        };
                        let _ = timeout(
                            Duration::from_millis(GROUP_STREAM_CLOSE_TIMEOUT_MS),
                            conn.send_frame(&quit_group),
                        )
                        .await;
                        let _ = conn.close().await;
                        Self::sam_lifecycle_log(format!(
                            "tab close group stream close done tab={tab_id} stream={stream_idx}"
                        ));
                    }

                    for (stream_idx, conn) in registered_conns.into_iter().enumerate() {
                        Self::sam_lifecycle_log(format!(
                            "tab close registered stream close start tab={tab_id} stream={stream_idx}"
                        ));
                        let _ = conn.close().await;
                        Self::sam_lifecycle_log(format!(
                            "tab close registered stream close done tab={tab_id} stream={stream_idx}"
                        ));
                    }

                    sleep(Duration::from_millis(SAM_CONNECT_CANCEL_GRACE_MS)).await;
                    Self::sam_lifecycle_log(format!("tab close SAM close start tab={tab_id}"));
                    let result = sam.close().await.map_err(|e| e.to_string());
                    Self::sam_lifecycle_log(format!(
                        "tab close SAM close done tab={tab_id} result={result:?}"
                    ));
                    result
                },
                move |result| Message::SamCloseFinished(tab_id, result),
            ));
        }

        tasks
    }

    fn is_transient_profile_name(name: &str) -> bool {
        name == "default"
    }

    fn is_persistent_contact_tab(tab: &OpenedTab) -> bool {
        tab.meta.kind == TabKind::Chat
            && !Self::is_transient_profile_name(&tab.session.profile)
    }

    fn active_contact_meta(&self) -> Option<ContactMeta> {
        let tab = self.active_tab()?;
        if !Self::is_persistent_contact_tab(tab) {
            return None;
        }

        Some(ContactMeta {
            name: tab.session.profile.clone(),
            my_dest_b64: tab.session.my_dest_b64.clone(),
            locked_peer: tab.session.stored_peer.clone(),
            locked_peer_dest_b64: tab.session.stored_peer_dest_b64.clone(),
            pq_enabled: tab.session.pq_enabled,
            deaddrop_servers: tab.session.deaddrop_servers.clone(),
        })
    }

    fn save_active_contact_meta(&mut self) {
        let Some(profile_name) = self.active_tab().map(|t| t.session.profile.clone()) else {
            return;
        };

        self.save_active_contact_meta_for_name(&profile_name);
    }

    fn save_active_contact_meta_for_name(&mut self, profile_name: &str) {
        let maybe_meta = self
            .opened_tabs
            .iter()
            .find(|tab| {
                tab.session.profile == profile_name && Self::is_persistent_contact_tab(tab)
            })
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
                .find(|tab| {
                    tab.session.profile == profile_name && Self::is_persistent_contact_tab(tab)
                })
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
        tab.session.known_remote_next_send = 0;
        tab.session.highest_authenticated_recv_index = None;
        tab.session.missing_drop_recv.clear();
        tab.session.skipped_drop_recv.clear();
        tab.session.forward_probe_index = 0;

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

    fn compose_reply_text(reply: Option<&ReplyDraft>, text: &str) -> String {
        let Some(reply) = reply else {
            return text.to_string();
        };

        format!(
            "{REPLY_BEGIN_MARKER}\n{}\n{REPLY_QUOTE_MARKER}\n{}\n{REPLY_END_MARKER}\n{}",
            reply.author, reply.text, text
        )
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
        if let Some(tab) = self.active_tab_mut() {
            Self::release_pending_rendezvous(tab);
            Self::invalidate_one_to_one_connect(tab, true);
            tab.connection_direction = None;
        }
        self.session.current_peer_addr = None;
        self.session.current_peer_dest_b64 = None;
        self.session.peer_b32 = None;
        self.session.pending_rendezvous_request_id = None;
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
            if let Some(request_id) = self.session.pending_rendezvous_request_id.take() {
                if let Some(issued) = self.session.rendezvous_issued.as_mut() {
                    if issued.request_id == request_id
                        && issued.state == RendezvousIssuedState::Reserved
                    {
                        issued.state = RendezvousIssuedState::Consumed;
                        self.session.rendezvous_status =
                            "One-time rendezvous invitation consumed.".into();
                    }
                }
            }
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
        if let Some(tab) = self.active_tab_mut() {
            tab.connection_direction = None;
        }
        self.session.pending_peer_addr = None;
        self.session.pending_peer_dest_b64 = None;
        if let Some(request_id) = self.session.pending_rendezvous_request_id.take() {
            if let Some(issued) = self.session.rendezvous_issued.as_mut() {
                if issued.request_id == request_id {
                    issued.state = RendezvousIssuedState::Revoked;
                    self.session.rendezvous_status =
                        "Declined caller; one-time rendezvous invitation revoked.".into();
                }
            }
        }

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
        Self::release_pending_rendezvous(tab);
        Self::invalidate_one_to_one_connect(tab, true);
        tab.connection_direction = None;
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
        tab.session.heartbeat_last_rx_ms = 0;
        tab.session.heartbeat_last_ping_ms = 0;

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
        self.session.known_remote_next_send = 0;
        self.session.highest_authenticated_recv_index = None;
        self.session.missing_drop_recv.clear();
        self.session.skipped_drop_recv.clear();
        self.session.forward_probe_index = 0;

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
            Self::invalidate_one_to_one_connect(tab, true);
            tab.connection_direction = None;
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
            Self::invalidate_one_to_one_connect(tab, true);
            tab.connection_direction = None;
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

        if self.active_tab_is_group() {
            if self.active_group_ready_count() > 0 {
                out.push(GuiAction::SendImage);
            }
            return out;
        }

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

    fn action_enabled_for_session(session: &SessionState, action: GuiAction) -> bool {
        Self::action_enabled(action)
            && !(action == GuiAction::Connect
                && session.profile == "default"
                && session.show_rendezvous_panel)
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
        if self.active_tab_is_group() {
            return self.active_group_ready_count() > 0;
        }

        (self.session.live_ready && self.active_live_conn().is_some())
            || self.can_send_offline_now()
    }

    fn can_send_live_image(&self) -> bool {
        if self.active_tab_is_group() {
            return self.active_group_ready_count() > 0;
        }

        self.session.live_ready && self.active_live_conn().is_some()
    }

    fn tick_one_tab(&mut self, idx: usize) -> Vec<Task<Message>> {
        if self
            .opened_tabs
            .get(idx)
            .map(|tab| tab.sam_runtime.is_closing())
            .unwrap_or(true)
        {
            return Vec::new();
        }

        if self
            .opened_tabs
            .get(idx)
            .map(|tab| tab.meta.kind == TabKind::Group)
            .unwrap_or(false)
        {
            return self.tick_group_tab(idx);
        }

        let mut tasks = Vec::new();
        //let is_active = self.session.active_tab_idx == Some(idx);
        let is_active = self.session.active_tab_idx == Some(Self::real_to_visible_tab_index(idx));
        let window_focused = self.window_focused;

        let mut secure_session_just_established = false;
        let mut secure_session_tab_id: Option<u64> = None;
        let mut offline_secret_request_tab_id: Option<u64> = None;

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
                let now_ms = Self::now_epoch_millis();
                while let Some(frame) = conn.try_recv_frame() {
                    tab.session.heartbeat_last_rx_ms = now_ms;
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
                                group_expected_acks: Vec::new(),
                                group_received_acks: Vec::new(),
                            });

                            push_log(
                                tab,
                                format!(
                                    "Image received: {image_name} ({} bytes)",
                                    tab.incoming_image_received
                                ),
                            );

                            Self::clear_incoming_image_state(tab);

                            if !is_active || !window_focused {
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

                                    if !is_active || !window_focused {
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
                            let was_live_ready = tab.session.live_ready;
                            tab.e2e.receive_peer_key(&frame.payload);

                            if tab.e2e.ready() {
                                tab.session.live_ready = true;
                                if !was_live_ready {
                                    tab.offline_index_sync_sent = false;
                                }
                                tab.session.heartbeat_last_rx_ms = now_ms;
                                tab.session.heartbeat_last_ping_ms = now_ms;
                                push_log(tab, "Secure session established.".to_string());

                                secure_session_just_established = true;
                                secure_session_tab_id = Some(tab_id);
                            } else {
                                push_log(tab, "Received peer E2E key.".to_string());
                            }
                        }

                        MsgType::S => match String::from_utf8(frame.payload) {
                            Ok(body) => {
                                match Self::verify_live_rendezvous_auth(tab, &body, now_ms) {
                                    Ok(true) => {
                                        push_log(
                                            tab,
                                            "One-time rendezvous proof verified.".to_string(),
                                        );
                                        continue;
                                    }
                                    Err(err) => {
                                        push_log(
                                            tab,
                                            format!("Rendezvous authentication rejected: {err}"),
                                        );
                                        tab.live_conn = None;
                                        tab.connection_direction = None;
                                        tab.session.current_peer_addr = None;
                                        tab.session.current_peer_dest_b64 = None;
                                        tab.session.peer_b32 = None;
                                        tab.session.pending_rendezvous_request_id = None;
                                        tab.session.live_ready = false;
                                        tab.session.network_status = NetworkStatus::LocalOk;
                                        tab.session.heartbeat_last_rx_ms = 0;
                                        tab.session.heartbeat_last_ping_ms = 0;
                                        tab.e2e = E2E::new(tab.session.pq_enabled);
                                        tab.session.accept_armed = true;
                                        if let Some((sam, cancelled)) =
                                            tab.sam_runtime.accept_parts()
                                        {
                                            tasks.push(Self::incoming_accept_task_from_parts(
                                                tab_id, sam, cancelled,
                                            ));
                                        }
                                        let close_conn = conn.clone();
                                        tasks.push(Task::perform(
                                            async move {
                                                close_conn
                                                    .close()
                                                    .await
                                                    .map_err(|e| e.to_string())
                                            },
                                            move |result| Message::CloseFinished(tab_id, result),
                                        ));
                                        break;
                                    }
                                    Ok(false) => {}
                                }

                                if body == "__SIGNAL__:QUIT" {
                                    tab.live_conn = None;
                                    tab.connection_direction = None;
                                    tab.session.current_peer_addr = None;
                                    tab.session.current_peer_dest_b64 = None;
                                    tab.session.peer_b32 = None;
                                    tab.session.live_ready = false;
                                    tab.session.offline_mode = false;
                                    tab.session.network_status = NetworkStatus::LocalOk;
                                    tab.session.tofu_verified = false;
                                    tab.session.tofu_mismatch = false;
                                    tab.session.heartbeat_last_rx_ms = 0;
                                    tab.session.heartbeat_last_ping_ms = 0;
                                    tab.e2e = E2E::new(tab.session.pq_enabled);
                                    Self::clear_outgoing_image_state(tab);
                                    Self::clear_incoming_image_state(tab);
                                    push_log(tab, "Peer disconnected.".to_string());
                                    tab.session.accept_armed = true;
                                    push_log(tab, "Incoming accept loop re-armed.".to_string());

                                    if let Some((sam, cancelled)) =
                                        tab.sam_runtime.accept_parts()
                                    {
                                        tasks.push(Self::incoming_accept_task_from_parts(
                                            tab_id, sam, cancelled,
                                        ));
                                    }
                                    break;
                                }

                                if let Some(nonce) = body.strip_prefix(HEARTBEAT_PING_PREFIX) {
                                    tasks.push(Self::heartbeat_pong_task(
                                        tab_id,
                                        conn.clone(),
                                        nonce.to_string(),
                                    ));
                                    continue;
                                }

                                if body.strip_prefix(HEARTBEAT_PONG_PREFIX).is_some() {
                                    continue;
                                }

                                if body == OFFLINE_SECRET_REQUEST_SIGNAL {
                                    push_log(
                                        tab,
                                        "Peer requested offline secret sync.".to_string(),
                                    );
                                    offline_secret_request_tab_id = Some(tab_id);
                                    continue;
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
                                            tab.connection_direction = None;
                                            tab.session.current_peer_addr = None;
                                            tab.session.current_peer_dest_b64 = None;
                                            tab.session.peer_b32 = None;
                                            tab.session.live_ready = false;
                                            tab.session.offline_mode = false;
                                            tab.session.network_status = NetworkStatus::LocalOk;
                                            tab.session.heartbeat_last_rx_ms = 0;
                                            tab.session.heartbeat_last_ping_ms = 0;
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

                            if !tab.session.live_ready || !tab.e2e.ready() {
                                push_log(
                                    tab,
                                    "Received offline secret before secure session was ready."
                                        .to_string(),
                                );
                                continue;
                            }

                            let payload = match tab.e2e.decrypt_strict(&frame.payload) {
                                Ok(payload) => payload,
                                Err(err) => {
                                    push_log(
                                        tab,
                                        format!("Offline secret authentication failed: {err}"),
                                    );
                                    continue;
                                }
                            };

                            if payload.len() != 32 {
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
                            secret.copy_from_slice(&payload);
                            tab.session.offline_shared_secret = Some(secret);

                            if let Some(peer_b32) = tab.session.stored_peer.clone() {
                                let offline = Self::offline_state_from_session(&tab.session);

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

                        MsgType::L => {
                            if !(tab.session.profile != "default"
                                && tab.session.stored_peer.is_some()
                                && tab.session.stored_peer_dest_b64.is_some())
                            {
                                push_log(
                                    tab,
                                    "Received deaddrop server list outside persistent locked-peer mode."
                                        .to_string(),
                                );
                                continue;
                            }

                            if !tab.session.live_ready || !tab.e2e.ready() {
                                push_log(
                                    tab,
                                    "Received deaddrop server list before secure session was ready."
                                        .to_string(),
                                );
                                continue;
                            }

                            let payload = match tab.e2e.decrypt_strict(&frame.payload) {
                                Ok(payload) => payload,
                                Err(err) => {
                                    push_log(
                                        tab,
                                        format!(
                                            "Deaddrop server list authentication failed: {err}"
                                        ),
                                    );
                                    continue;
                                }
                            };

                            match String::from_utf8(payload) {
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
                            }
                        }

                        MsgType::I => {
                            if !(tab.session.profile != "default"
                                && tab.session.stored_peer.is_some()
                                && tab.session.stored_peer_dest_b64.is_some()
                                && tab.session.tofu_verified
                                && tab.session.live_ready
                                && tab.e2e.ready())
                            {
                                push_log(
                                    tab,
                                    "Received offline index sync outside a verified persistent secure session."
                                        .to_string(),
                                );
                                continue;
                            }

                            let payload = match tab.e2e.decrypt_strict(&frame.payload) {
                                Ok(payload) => payload,
                                Err(err) => {
                                    push_log(
                                        tab,
                                        format!("Offline index sync authentication failed: {err}"),
                                    );
                                    continue;
                                }
                            };

                            let Some((remote_next_send, remote_receive_base)) =
                                Self::decode_offline_index_sync_payload(&payload)
                            else {
                                push_log(tab, "Invalid offline index sync payload.".to_string());
                                continue;
                            };

                            let old_send = tab.session.drop_send_index;
                            let old_known_remote = tab.session.known_remote_next_send;
                            tab.session.drop_send_index =
                                tab.session.drop_send_index.max(remote_receive_base);
                            tab.session.known_remote_next_send = tab
                                .session
                                .known_remote_next_send
                                .max(remote_next_send);
                            Self::save_offline_state_for_tab(
                                tab,
                                "Failed to save offline index synchronization",
                            );

                            if old_send != tab.session.drop_send_index
                                || old_known_remote != tab.session.known_remote_next_send
                            {
                                push_log(
                                    tab,
                                    format!(
                                        "Offline indexes synchronized: send={}, remote_next={}.",
                                        tab.session.drop_send_index,
                                        tab.session.known_remote_next_send
                                    ),
                                );
                            }
                        }

                        other => {
                            push_log(tab, format!("Received unsupported frame type: {:?}", other));
                        }
                    }
                }

                if tab.session.live_ready {
                    if tab.session.heartbeat_last_rx_ms == 0 {
                        tab.session.heartbeat_last_rx_ms = now_ms;
                    }

                    if now_ms.saturating_sub(tab.session.heartbeat_last_rx_ms)
                        >= HEARTBEAT_TIMEOUT_MS
                    {
                        tab.live_conn = None;
                        tab.connection_direction = None;
                        tab.session.current_peer_addr = None;
                        tab.session.current_peer_dest_b64 = None;
                        tab.session.peer_b32 = None;
                        tab.session.live_ready = false;
                        tab.session.offline_mode = false;
                        tab.session.network_status = NetworkStatus::LocalOk;
                        tab.session.tofu_verified = false;
                        tab.session.tofu_mismatch = false;
                        tab.session.heartbeat_last_rx_ms = 0;
                        tab.session.heartbeat_last_ping_ms = 0;
                        tab.e2e = E2E::new(tab.session.pq_enabled);
                        Self::clear_outgoing_image_state(tab);
                        Self::clear_incoming_image_state(tab);
                        push_log(tab, "Peer heartbeat timed out.".to_string());
                        tab.session.accept_armed = true;
                        push_log(tab, "Incoming accept loop re-armed.".to_string());

                        if let Some((sam, cancelled)) = tab.sam_runtime.accept_parts() {
                            tasks.push(Self::incoming_accept_task_from_parts(
                                tab_id, sam, cancelled,
                            ));
                        }

                        let close_conn = conn.clone();
                        tasks.push(Task::perform(
                            async move { close_conn.close().await.map_err(|e| e.to_string()) },
                            move |result| Message::CloseFinished(tab_id, result),
                        ));
                    } else if now_ms.saturating_sub(tab.session.heartbeat_last_ping_ms)
                        >= HEARTBEAT_PING_INTERVAL_MS
                        && now_ms.saturating_sub(tab.session.heartbeat_last_rx_ms)
                            >= HEARTBEAT_PING_INTERVAL_MS
                    {
                        tab.session.heartbeat_last_ping_ms = now_ms;
                        tasks.push(Self::heartbeat_ping_task(tab_id, conn.clone()));
                    }
                }

                if conn.is_closed() && !conn.has_pending_frames() {
                    tab.live_conn = None;
                    tab.connection_direction = None;
                    tab.session.current_peer_addr = None;
                    tab.session.current_peer_dest_b64 = None;
                    tab.session.peer_b32 = None;
                    tab.session.live_ready = false;
                    tab.session.offline_mode = false;
                    tab.session.network_status = NetworkStatus::LocalOk;
                    tab.session.tofu_verified = false;
                    tab.session.tofu_mismatch = false;
                    tab.session.heartbeat_last_rx_ms = 0;
                    tab.session.heartbeat_last_ping_ms = 0;
                    tab.e2e = E2E::new(tab.session.pq_enabled);
                    Self::clear_outgoing_image_state(tab);
                    Self::clear_incoming_image_state(tab);
                    push_log(tab, "Live connection closed.".to_string());
                    tab.session.accept_armed = true;
                    push_log(tab, "Incoming accept loop re-armed.".to_string());

                    if let Some((sam, cancelled)) = tab.sam_runtime.accept_parts() {
                        tasks.push(Self::incoming_accept_task_from_parts(
                            tab_id, sam, cancelled,
                        ));
                    }

                    let close_conn = conn.clone();
                    tasks.push(Task::perform(
                        async move { close_conn.close().await.map_err(|e| e.to_string()) },
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

                    if let Some((sam, cancelled)) = tab.sam_runtime.accept_parts() {
                        tasks.push(Self::incoming_accept_task_from_parts(
                            tab_id, sam, cancelled,
                        ));
                    }

                    tab.meta.connected = tab.session.live_ready;
                    tab.meta.has_incoming = tab.session.pending_peer_addr.is_some();
                    return tasks;
                }

                while let Some(frame) = conn.try_recv_frame() {
                    match frame.msg_type {
                        MsgType::S => match String::from_utf8(frame.payload) {
                            Ok(body) => {
                                match Self::verify_pending_rendezvous_auth(
                                    tab,
                                    &body,
                                    Self::now_epoch_millis(),
                                ) {
                                    Ok(true) => {
                                        push_log(
                                            tab,
                                            "One-time rendezvous proof verified for pending caller."
                                                .to_string(),
                                        );
                                        continue;
                                    }
                                    Err(err) => {
                                        push_log(
                                            tab,
                                            format!("Rendezvous authentication rejected: {err}"),
                                        );
                                        tab.pending_conn = None;
                                        tab.connection_direction = None;
                                        tab.session.pending_peer_addr = None;
                                        tab.session.pending_peer_dest_b64 = None;
                                        tab.session.pending_rendezvous_request_id = None;
                                        tab.session.call_blink_on = true;
                                        tab.session.call_blink_ticks = 0;
                                        tab.session.accept_armed = true;
                                        let close_conn = conn.clone();
                                        tasks.push(Task::perform(
                                            async move {
                                                close_conn
                                                    .close()
                                                    .await
                                                    .map_err(|e| e.to_string())
                                            },
                                            move |result| Message::CloseFinished(tab_id, result),
                                        ));
                                        if let Some((sam, cancelled)) =
                                            tab.sam_runtime.accept_parts()
                                        {
                                            tasks.push(Self::incoming_accept_task_from_parts(
                                                tab_id, sam, cancelled,
                                            ));
                                        }
                                        break;
                                    }
                                    Ok(false) => {}
                                }

                                if body == "__SIGNAL__:QUIT" {
                                    Self::release_pending_rendezvous(tab);
                                    tab.pending_conn = None;
                                    tab.connection_direction = None;
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

                                    if let Some((sam, cancelled)) =
                                        tab.sam_runtime.accept_parts()
                                    {
                                        tasks.push(Self::incoming_accept_task_from_parts(
                                            tab_id, sam, cancelled,
                                        ));
                                    }
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

                    Self::release_pending_rendezvous(tab);
                    tab.pending_conn = None;
                    tab.connection_direction = None;
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

                    if let Some((sam, cancelled)) = tab.sam_runtime.accept_parts() {
                        tasks.push(Self::incoming_accept_task_from_parts(
                            tab_id, sam, cancelled,
                        ));
                    }
                }
            } else if tab.session.network_status != NetworkStatus::Initializing
                && !tab.session.accept_armed
            {
                tab.session.accept_armed = true;
                push_log(tab, "Incoming accept loop re-armed.".to_string());

                if let Some((sam, cancelled)) = tab.sam_runtime.accept_parts() {
                    tasks.push(Self::incoming_accept_task_from_parts(tab_id, sam, cancelled));
                }
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

                tasks.push(self.sync_offline_secret_if_needed_task(tab_id));

                if is_persistent_tab {
                    tasks.push(self.send_deaddrop_server_list_task(tab_id));
                }

                if self.active_tab().map(|t| t.id) == Some(tab_id) {
                    self.load_active_runtime();
                }
            }
        }

        if let Some(tab_id) = offline_secret_request_tab_id {
            tasks.push(self.send_offline_secret_if_needed_task(tab_id));
        }

        let index_sync_tab_id = self.opened_tabs.get(idx).and_then(|tab| {
            if Self::can_send_offline_index_sync(tab) && !tab.offline_index_sync_sent {
                Some(tab.id)
            } else {
                None
            }
        });
        if let Some(tab_id) = index_sync_tab_id {
            tasks.push(self.send_offline_index_sync_task(tab_id));
        }

        tasks
    }

    fn tick_group_tab(&mut self, idx: usize) -> Vec<Task<Message>> {
        let mut tasks = Vec::new();
        let is_active = self.session.active_tab_idx == Some(Self::real_to_visible_tab_index(idx));
        let window_focused = self.window_focused;
        let tab_id;
        let mut roster_sync_needed = false;
        let mut received_roster_groups: Vec<String> = Vec::new();
        let now_ms = Self::now_epoch_millis();

        {
            let Some(tab) = self.opened_tabs.get_mut(idx) else {
                return tasks;
            };

            tab_id = tab.id;
            let sam_runtime = tab.sam_runtime.clone();
            let Some(group) = tab.group.as_mut() else {
                return tasks;
            };
            let is_group_admin = Self::group_is_admin(&group.meta);

            let mut any_ready = false;

            for peer in &mut group.peers {
                let Some(conn) = peer.conn.clone() else {
                    continue;
                };

                while let Some(frame) = conn.try_recv_frame() {
                    peer.heartbeat_last_rx_ms = now_ms;
                    match frame.msg_type {
                        MsgType::S => match String::from_utf8(frame.payload) {
                            Ok(body) => {
                                if body == "__SIGNAL__:QUIT" {
                                    Self::reset_group_peer_transport_state(peer);
                                    tab.session.log_lines.push(format!(
                                        "Group member disconnected: {}",
                                        peer.member.name
                                    ));
                                    let close_conn = conn.clone();
                                    tasks.push(Task::perform(
                                        async move {
                                            close_conn.close().await.map_err(|e| e.to_string())
                                        },
                                        move |result| Message::CloseFinished(tab_id, result),
                                    ));
                                    continue;
                                }

                                if let Some(nonce) = body.strip_prefix(HEARTBEAT_PING_PREFIX) {
                                    let task = Self::heartbeat_pong_task(
                                        tab_id,
                                        conn.clone(),
                                        nonce.to_string(),
                                    );
                                    tasks.push(sam_runtime.track_send_task(task));
                                    continue;
                                }

                                if body.strip_prefix(HEARTBEAT_PONG_PREFIX).is_some() {
                                    continue;
                                }

                                match SamClient::destination_to_b32(&body) {
                                    Ok(peer_b32)
                                        if peer_b32.eq_ignore_ascii_case(&peer.member.b32) =>
                                    {
                                        peer.handshake_identity_received = true;
                                        tab.session.log_lines.push(format!(
                                            "Group member identity verified: {}",
                                            peer.member.name
                                        ));
                                    }
                                    Ok(peer_b32) => {
                                        tab.session.log_lines.push(format!(
                                            "Group identity mismatch for {}: {}",
                                            peer.member.name, peer_b32
                                        ));
                                        Self::reset_group_peer_transport_state(peer);
                                        let close_conn = conn.clone();
                                        tasks.push(Task::perform(
                                            async move {
                                                close_conn.close().await.map_err(|e| e.to_string())
                                            },
                                            move |result| Message::CloseFinished(tab_id, result),
                                        ));
                                        continue;
                                    }
                                    Err(err) => {
                                        tab.session.log_lines.push(format!(
                                            "Invalid group identity from {}: {err}",
                                            peer.member.name
                                        ));
                                        Self::reset_group_peer_transport_state(peer);
                                        let close_conn = conn.clone();
                                        tasks.push(Task::perform(
                                            async move {
                                                close_conn.close().await.map_err(|e| e.to_string())
                                            },
                                            move |result| Message::CloseFinished(tab_id, result),
                                        ));
                                        continue;
                                    }
                                }
                            }
                            Err(_) => {
                                tab.session.log_lines.push(format!(
                                    "Invalid UTF-8 group identity payload from {}.",
                                    peer.member.name
                                ));
                                Self::reset_group_peer_transport_state(peer);
                                let close_conn = conn.clone();
                                tasks.push(Task::perform(
                                    async move {
                                        close_conn.close().await.map_err(|e| e.to_string())
                                    },
                                    move |result| Message::CloseFinished(tab_id, result),
                                ));
                                continue;
                            }
                        },
                        MsgType::K => {
                            peer.e2e.receive_peer_key(&frame.payload);
                            if peer.e2e.ready() {
                                peer.handshake_key_received = true;
                            } else {
                                tab.session.log_lines.push(format!(
                                    "Invalid group key from {}.",
                                    peer.member.name
                                ));
                            }
                        }
                        MsgType::L => {
                            if !peer.ready {
                                continue;
                            }

                            let plain = peer.e2e.decrypt(&frame.payload);
                            if let Ok(control) =
                                serde_json::from_slice::<GroupControlMessage>(&plain)
                            {
                                if control.kind == GROUP_CONTROL_JOIN_PROOF {
                                    if !control.b32.eq_ignore_ascii_case(&peer.member.b32) {
                                        tab.session.log_lines.push(format!(
                                            "Rejected group invite proof b32 mismatch from {}.",
                                            peer.member.name
                                        ));
                                        peer.authorized = false;
                                        Self::reset_group_peer_transport_state(peer);
                                        let close_conn = conn.clone();
                                        tasks.push(Task::perform(
                                            async move {
                                                close_conn.close().await.map_err(|e| e.to_string())
                                            },
                                            move |result| Message::CloseFinished(tab_id, result),
                                        ));
                                        continue;
                                    }

                                    let member = GroupMember {
                                        name: control.name,
                                        b32: control.b32,
                                    };

                                    let private_binding = group
                                        .meta
                                        .issued_invites
                                        .iter()
                                        .find(|invite| invite.token == control.token)
                                        .and_then(|invite| invite.private_binding.as_ref())
                                        .cloned();
                                    let private_fields_present =
                                        control.private_request_id.is_some()
                                            || control.private_proof_nonce.is_some()
                                            || control.private_proof_signature.is_some();

                                    if private_binding.is_none()
                                        && private_fields_present
                                        && peer.authorized
                                        && group.meta.members.iter().any(|existing| {
                                            existing.b32.eq_ignore_ascii_case(&member.b32)
                                        })
                                    {
                                        tab.session.log_lines.push(format!(
                                            "Ignored completed private invite proof from {}.",
                                            peer.member.name
                                        ));
                                        continue;
                                    }

                                    let redeem_result = if private_binding.is_some() {
                                        match (
                                            control.private_request_id,
                                            control.private_proof_nonce,
                                            control.private_proof_signature,
                                        ) {
                                            (Some(request_id), Some(nonce), Some(signature)) => {
                                                let proof = PrivateJoinProof {
                                                    request_id,
                                                    nonce,
                                                    signature,
                                                };
                                                Self::redeem_private_group_invite_token(
                                                    &mut group.meta,
                                                    &control.token,
                                                    member.clone(),
                                                    &proof,
                                                    now_ms,
                                                )
                                            }
                                            _ => Err(
                                                "private invite proof is incomplete".to_string()
                                            ),
                                        }
                                    } else {
                                        Self::redeem_group_invite_token(
                                            &mut group.meta,
                                            &control.token,
                                            member.clone(),
                                        )
                                    };

                                    match redeem_result {
                                        Ok(()) => {
                                            peer.member = member.clone();
                                            peer.authorized = true;
                                            if is_group_admin {
                                                roster_sync_needed = true;
                                            }
                                            match storage::save_group_meta(&group.meta) {
                                                Ok(()) => {
                                                    received_roster_groups.push(
                                                        storage::group_storage_key(&group.meta),
                                                    );
                                                    tab.session.log_lines.push(format!(
                                                        "Redeemed group invite for {}.",
                                                        peer.member.name
                                                    ));
                                                }
                                                Err(err) => {
                                                    tab.session.log_lines.push(format!(
                                                        "Group invite redeemed but save failed: {err}"
                                                    ));
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            tab.session.log_lines.push(format!(
                                                "Rejected group invite proof from {}: {err}",
                                                peer.member.name
                                            ));
                                            peer.authorized = false;
                                            Self::reset_group_peer_transport_state(peer);
                                            let close_conn = conn.clone();
                                            tasks.push(Task::perform(
                                                async move {
                                                    close_conn
                                                        .close()
                                                        .await
                                                        .map_err(|e| e.to_string())
                                                },
                                                move |result| Message::CloseFinished(tab_id, result),
                                            ));
                                        }
                                    }

                                    continue;
                                }

                                if control.kind == GROUP_CONTROL_RENAME_REQUEST {
                                    if !control.b32.eq_ignore_ascii_case(&peer.member.b32) {
                                        tab.session.log_lines.push(format!(
                                            "Rejected group rename request b32 mismatch from {}.",
                                            peer.member.name
                                        ));
                                        continue;
                                    }

                                    if !is_group_admin {
                                        tab.session.log_lines.push(format!(
                                            "Ignored group rename request from {}: not owner.",
                                            peer.member.name
                                        ));
                                        continue;
                                    }

                                    match Self::apply_group_member_rename(
                                        &mut group.meta,
                                        &control.b32,
                                        control.name.clone(),
                                    ) {
                                        Ok(changed) => {
                                            if changed {
                                                peer.member.name = control.name.clone();
                                                roster_sync_needed = true;
                                                match storage::save_group_meta(&group.meta) {
                                                    Ok(()) => {
                                                        received_roster_groups.push(
                                                            storage::group_storage_key(&group.meta),
                                                        );
                                                        tab.session.log_lines.push(format!(
                                                            "Accepted group rename request: {}.",
                                                            peer.member.name
                                                        ));
                                                    }
                                                    Err(err) => {
                                                        tab.session.log_lines.push(format!(
                                                            "Group rename accepted but save failed: {err}"
                                                        ));
                                                    }
                                                }
                                            } else {
                                                tab.session.log_lines.push(format!(
                                                    "Group rename request unchanged: {}.",
                                                    peer.member.name
                                                ));
                                            }
                                        }
                                        Err(err) => {
                                            tab.session.log_lines.push(format!(
                                                "Rejected group rename request from {}: {err}",
                                                peer.member.name
                                            ));
                                        }
                                    }

                                    continue;
                                }
                            }

                            if !peer.authorized {
                                tab.session.log_lines.push(format!(
                                    "Ignored group roster from unauthorised caller: {}",
                                    peer.member.name
                                ));
                                continue;
                            }

                            match serde_json::from_slice::<GroupRosterSync>(&plain) {
                                Ok(roster) => match Self::merge_group_roster_sync(roster) {
                                    Ok(group_name) => {
                                        received_roster_groups.push(group_name);
                                        tab.session.log_lines.push(format!(
                                            "Merged group roster from {}.",
                                            peer.member.name
                                        ));
                                    }
                                    Err(err) => {
                                        tab.session.log_lines.push(format!(
                                            "Group roster sync failed from {}: {err}",
                                            peer.member.name
                                        ));
                                    }
                                },
                                Err(err) => match serde_json::from_slice::<GroupInvite>(&plain) {
                                    Ok(invite) => match Self::merge_group_invite(invite) {
                                        Ok(group_name) => {
                                            received_roster_groups.push(group_name);
                                            tab.session.log_lines.push(format!(
                                                "Merged legacy group roster from {}.",
                                                peer.member.name
                                            ));
                                        }
                                        Err(legacy_err) => {
                                            tab.session.log_lines.push(format!(
                                                "Group roster sync failed from {}: {legacy_err}",
                                                peer.member.name
                                            ));
                                        }
                                    },
                                    Err(_) => {
                                        tab.session.log_lines.push(format!(
                                            "Invalid group roster sync from {}: {err}",
                                            peer.member.name
                                        ));
                                    }
                                },
                            }
                        }
                        MsgType::J => {
                            if !peer.ready || !peer.authorized {
                                tab.session.log_lines.push(format!(
                                    "Ignored group image header before authorised secure session: {}",
                                    peer.member.name
                                ));
                                continue;
                            }

                            let plain = peer.e2e.decrypt(&frame.payload);
                            match String::from_utf8(plain) {
                                Ok(body) => {
                                    let mut parts = body.split('|');
                                    let Some(filename_raw) = parts.next() else {
                                        tab.session.log_lines.push(format!(
                                            "Invalid group image header from {}.",
                                            peer.member.name
                                        ));
                                        continue;
                                    };
                                    let Some(mime_raw) = parts.next() else {
                                        tab.session.log_lines.push(format!(
                                            "Invalid group image header from {}.",
                                            peer.member.name
                                        ));
                                        continue;
                                    };
                                    let Some(size_raw) = parts.next() else {
                                        tab.session.log_lines.push(format!(
                                            "Invalid group image header from {}.",
                                            peer.member.name
                                        ));
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
                                            tab.session.log_lines.push(format!(
                                                "Invalid group image size from {}.",
                                                peer.member.name
                                            ));
                                            continue;
                                        }
                                    };

                                    if total_bytes == 0
                                        || total_bytes > GROUP_IMAGE_TRANSFER_MAX_BYTES as u64
                                    {
                                        tab.session.log_lines.push(format!(
                                            "Rejected group image size from {}: {} bytes.",
                                            peer.member.name, total_bytes
                                        ));
                                        continue;
                                    }

                                    if !Self::is_supported_image_mime(mime_raw) {
                                        tab.session.log_lines.push(format!(
                                            "Unsupported group image type from {}: {mime_raw}",
                                            peer.member.name
                                        ));
                                        continue;
                                    }

                                    Self::clear_group_peer_incoming_image_state(peer);
                                    peer.incoming_image_name = Some(filename);
                                    peer.incoming_image_mime = Some(mime_raw.to_string());
                                    peer.incoming_image_expected = total_bytes;
                                    peer.incoming_image_received = 0;
                                    peer.incoming_image_msg_id = frame.msg_id;
                                    peer.incoming_image_bytes =
                                        Vec::with_capacity(total_bytes as usize);
                                }
                                Err(_) => {
                                    tab.session.log_lines.push(format!(
                                        "Invalid UTF-8 group image header from {}.",
                                        peer.member.name
                                    ));
                                }
                            }
                        }
                        MsgType::G => {
                            if !peer.ready || !peer.authorized {
                                continue;
                            }

                            if peer.incoming_image_name.is_none() {
                                tab.session.log_lines.push(format!(
                                    "Group image chunk without header from {}.",
                                    peer.member.name
                                ));
                                continue;
                            }

                            if peer.incoming_image_msg_id != frame.msg_id {
                                tab.session.log_lines.push(format!(
                                    "Group image chunk transfer id mismatch from {}.",
                                    peer.member.name
                                ));
                                continue;
                            }

                            let plain = peer.e2e.decrypt(&frame.payload);
                            match general_purpose::STANDARD.decode(&plain) {
                                Ok(chunk) => {
                                    let next_total =
                                        peer.incoming_image_received + chunk.len() as u64;

                                    if next_total > peer.incoming_image_expected
                                        || next_total > GROUP_IMAGE_TRANSFER_MAX_BYTES as u64
                                    {
                                        tab.session.log_lines.push(format!(
                                            "Group image transfer overflow from {}.",
                                            peer.member.name
                                        ));
                                        Self::clear_group_peer_incoming_image_state(peer);
                                        continue;
                                    }

                                    peer.incoming_image_bytes.extend_from_slice(&chunk);
                                    peer.incoming_image_received = next_total;
                                }
                                Err(err) => {
                                    tab.session.log_lines.push(format!(
                                        "Group image chunk decode failed from {}: {err}",
                                        peer.member.name
                                    ));
                                    Self::clear_group_peer_incoming_image_state(peer);
                                }
                            }
                        }
                        MsgType::Z => {
                            if !peer.ready || !peer.authorized {
                                continue;
                            }

                            if peer.incoming_image_name.is_none() {
                                tab.session.log_lines.push(format!(
                                    "Group image end without header from {}.",
                                    peer.member.name
                                ));
                                continue;
                            }

                            if peer.incoming_image_msg_id != frame.msg_id {
                                tab.session.log_lines.push(format!(
                                    "Group image end transfer id mismatch from {}.",
                                    peer.member.name
                                ));
                                continue;
                            }

                            if peer.incoming_image_received != peer.incoming_image_expected {
                                tab.session.log_lines.push(format!(
                                    "Incomplete group image from {}: {}/{} bytes.",
                                    peer.member.name,
                                    peer.incoming_image_received,
                                    peer.incoming_image_expected
                                ));
                                Self::clear_group_peer_incoming_image_state(peer);
                                continue;
                            }

                            let image_name = peer
                                .incoming_image_name
                                .clone()
                                .unwrap_or_else(|| "image".into());
                            let image_bytes = std::mem::take(&mut peer.incoming_image_bytes);

                            tab.session.bubbles.push(Bubble {
                                author: peer.member.name.clone(),
                                content: BubbleContent::Image(Self::image_bubble_data(image_bytes)),
                                mine: false,
                                offline: false,
                                timestamp_utc: Self::now_utc_hms(),
                                msg_id: Some(frame.msg_id),
                                delivered: false,
                                group_expected_acks: Vec::new(),
                                group_received_acks: Vec::new(),
                            });

                            if !is_active || !window_focused {
                                tab.meta.has_unread = true;
                            }

                            tab.session.log_lines.push(format!(
                                "Group image received from {}: {image_name} ({} bytes)",
                                peer.member.name, peer.incoming_image_received
                            ));

                            Self::clear_group_peer_incoming_image_state(peer);

                            let ack = Frame {
                                msg_type: MsgType::D,
                                msg_id: Self::generate_msg_id_value(),
                                payload: frame.msg_id.to_be_bytes().to_vec(),
                            };
                            let conn_for_ack = conn.clone();
                            let task = Task::perform(
                                async move {
                                    conn_for_ack
                                        .send_frame(&ack)
                                        .await
                                        .map_err(|e| e.to_string())
                                },
                                move |result| Message::SendFinished(tab_id, result),
                            );
                            tasks.push(sam_runtime.track_send_task(task));
                        }
                        MsgType::U => {
                            if !peer.ready || !peer.authorized {
                                tab.session.log_lines.push(format!(
                                    "Ignored group message before authorised secure session: {}",
                                    peer.member.name
                                ));
                                continue;
                            }

                            let plain = peer.e2e.decrypt(&frame.payload);
                            match String::from_utf8(plain) {
                                Ok(body) => {
                                    tab.session.bubbles.push(Bubble {
                                        author: peer.member.name.clone(),
                                        content: BubbleContent::Text(body),
                                        mine: false,
                                        offline: false,
                                        timestamp_utc: Self::now_utc_hms(),
                                        msg_id: Some(frame.msg_id),
                                        delivered: false,
                                        group_expected_acks: Vec::new(),
                                        group_received_acks: Vec::new(),
                                    });
                                    if !is_active || !window_focused {
                                        tab.meta.has_unread = true;
                                    }
                                    let ack = Frame {
                                        msg_type: MsgType::D,
                                        msg_id: Self::generate_msg_id_value(),
                                        payload: frame.msg_id.to_be_bytes().to_vec(),
                                    };
                                    let conn_for_ack = conn.clone();
                                    let task = Task::perform(
                                        async move {
                                            conn_for_ack
                                                .send_frame(&ack)
                                                .await
                                                .map_err(|e| e.to_string())
                                        },
                                        move |result| Message::SendFinished(tab_id, result),
                                    );
                                    tasks.push(sam_runtime.track_send_task(task));
                                }
                                Err(_) => {
                                    tab.session.log_lines.push(format!(
                                        "Invalid UTF-8 group message from {}.",
                                        peer.member.name
                                    ));
                                }
                            }
                        }
                        MsgType::D => {
                            if !peer.ready || !peer.authorized {
                                continue;
                            }

                            if frame.payload.len() == 8 {
                                let mut bytes = [0u8; 8];
                                bytes.copy_from_slice(&frame.payload);
                                let delivered_id = u64::from_be_bytes(bytes);
                                Self::mark_group_delivered(
                                    &mut tab.session.bubbles,
                                    delivered_id,
                                    &peer.member.b32,
                                );
                            } else {
                                tab.session.log_lines.push(format!(
                                    "Invalid group delivery ACK from {}.",
                                    peer.member.name
                                ));
                            }
                        }
                        _ => {
                            tab.session.log_lines.push(format!(
                                "Ignored unsupported group frame from {}.",
                                peer.member.name
                            ));
                        }
                    }

                    if !peer.ready
                        && peer.handshake_identity_received
                        && peer.handshake_key_received
                        && peer.e2e.ready()
                    {
                        peer.ready = true;
                        peer.handshake_started_ms = 0;
                        peer.heartbeat_last_rx_ms = now_ms;
                        peer.heartbeat_last_ping_ms = now_ms;
                        if is_group_admin {
                            roster_sync_needed = true;
                        }
                        tab.session.log_lines.push(format!(
                            "Group secure session ready: {}",
                            peer.member.name
                        ));

                        if let (Some(my_b32), Some(owner_b32)) =
                            (group.meta.my_b32.clone(), group.meta.owner_b32.clone())
                        {
                            if peer.member.b32.eq_ignore_ascii_case(&owner_b32) {
                                let control = if let Some(token) = group.meta.join_token.clone() {
                                    if let Some(credential) =
                                        group.meta.private_join_credential.as_ref()
                                    {
                                        match group_invite::sign_join_proof(
                                            credential,
                                            &owner_b32,
                                            &token,
                                            &my_b32,
                                            now_ms,
                                        ) {
                                            Ok(proof) => Some(GroupControlMessage {
                                                kind: GROUP_CONTROL_JOIN_PROOF.into(),
                                                token,
                                                b32: my_b32,
                                                name: Self::group_self_display_name(&group.meta),
                                                private_request_id: Some(proof.request_id),
                                                private_proof_nonce: Some(proof.nonce),
                                                private_proof_signature: Some(proof.signature),
                                            }),
                                            Err(err) => {
                                                tab.session.log_lines.push(format!(
                                                    "Private group invite proof failed: {err}"
                                                ));
                                                None
                                            }
                                        }
                                    } else {
                                        Some(GroupControlMessage {
                                            kind: GROUP_CONTROL_JOIN_PROOF.into(),
                                            token,
                                            b32: my_b32,
                                            name: Self::group_self_display_name(&group.meta),
                                            private_request_id: None,
                                            private_proof_nonce: None,
                                            private_proof_signature: None,
                                        })
                                    }
                                } else if !is_group_admin
                                    && !group.meta.my_name.trim().is_empty()
                                {
                                    Some(GroupControlMessage {
                                        kind: GROUP_CONTROL_RENAME_REQUEST.into(),
                                        token: String::new(),
                                        b32: my_b32,
                                        name: Self::group_self_display_name(&group.meta),
                                        private_request_id: None,
                                        private_proof_nonce: None,
                                        private_proof_signature: None,
                                    })
                                } else {
                                    None
                                };

                                if let Some(control) = control {
                                    match serde_json::to_vec(&control) {
                                        Ok(payload) => {
                                            let frame = Frame {
                                                msg_type: MsgType::L,
                                                msg_id: Self::generate_msg_id_value(),
                                                payload: peer.e2e.encrypt(&payload),
                                            };
                                            let conn = conn.clone();
                                            let task = Task::perform(
                                                async move {
                                                    conn.send_frame(&frame)
                                                        .await
                                                        .map_err(|e| e.to_string())
                                                },
                                                move |result| Message::SendFinished(tab_id, result),
                                            );
                                            tasks.push(sam_runtime.track_send_task(task));
                                            if control.kind == GROUP_CONTROL_JOIN_PROOF {
                                                tab.session.log_lines.push(format!(
                                                    "Sent group invite proof to {}.",
                                                    peer.member.name
                                                ));
                                            } else {
                                                tab.session.log_lines.push(format!(
                                                    "Sent group rename request to {}.",
                                                    peer.member.name
                                                ));
                                            }
                                        }
                                        Err(err) => {
                                            tab.session.log_lines.push(format!(
                                                "Group invite proof encode failed: {err}"
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if conn.is_closed() && !conn.has_pending_frames() {
                    let was_ready = peer.ready;
                    Self::reset_group_peer_transport_state(peer);

                    if was_ready {
                        tab.session
                            .log_lines
                            .push(format!("Group member disconnected: {}", peer.member.name));
                    }

                    continue;
                }

                if !peer.ready
                    && peer.handshake_started_ms != 0
                    && now_ms.saturating_sub(peer.handshake_started_ms)
                        >= GROUP_HANDSHAKE_TIMEOUT_MS
                {
                    let identity_received = peer.handshake_identity_received;
                    let key_received = peer.handshake_key_received;
                    let stalled_conn = peer.conn.take();
                    Self::reset_group_peer_transport_state(peer);
                    tab.session.log_lines.push(format!(
                        "Group handshake timed out for {} (identity={}, key={}).",
                        peer.member.name, identity_received, key_received
                    ));

                    if let Some(stalled_conn) = stalled_conn {
                        tasks.push(Task::perform(
                            async move { stalled_conn.close().await.map_err(|e| e.to_string()) },
                            move |result| Message::CloseFinished(tab_id, result),
                        ));
                    }
                    continue;
                }

                if peer.ready && peer.authorized {
                    if peer.heartbeat_last_rx_ms == 0 {
                        peer.heartbeat_last_rx_ms = now_ms;
                    }

                    if now_ms.saturating_sub(peer.heartbeat_last_rx_ms) >= HEARTBEAT_TIMEOUT_MS {
                        Self::reset_group_peer_transport_state(peer);
                        tab.session.log_lines.push(format!(
                            "Group member heartbeat timed out: {}",
                            peer.member.name
                        ));
                        let close_conn = conn.clone();
                        tasks.push(Task::perform(
                            async move { close_conn.close().await.map_err(|e| e.to_string()) },
                            move |result| Message::CloseFinished(tab_id, result),
                        ));
                        continue;
                    } else if now_ms.saturating_sub(peer.heartbeat_last_ping_ms)
                        >= HEARTBEAT_PING_INTERVAL_MS
                        && now_ms.saturating_sub(peer.heartbeat_last_rx_ms)
                            >= HEARTBEAT_PING_INTERVAL_MS
                    {
                        peer.heartbeat_last_ping_ms = now_ms;
                        let task = Self::heartbeat_ping_task(tab_id, conn.clone());
                        tasks.push(sam_runtime.track_send_task(task));
                    }
                }

                if peer.ready && peer.authorized {
                    any_ready = true;
                }
            }

            tab.session.live_ready = any_ready;
            tab.session.network_status = if any_ready {
                NetworkStatus::Visible
            } else if group.publish_ready {
                NetworkStatus::Visible
            } else if tab.meta.initialized {
                NetworkStatus::LocalOk
            } else {
                NetworkStatus::Initializing
            };
            tab.meta.connected = any_ready;
            tab.meta.has_incoming = false;
        }

        for group_key in received_roster_groups {
            if let Ok(group) = storage::load_group_meta(&group_key) {
                if let Some(idx) = self.session.groups.iter().position(|existing| {
                    storage::group_storage_key(existing) == storage::group_storage_key(&group)
                }) {
                    self.session.groups[idx] = group.clone();
                } else {
                    self.session.groups.push(group.clone());
                    self.session
                        .groups
                        .sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
                }
                self.update_open_group_roster(&group);
            }
        }

        if is_active {
            self.load_active_runtime();
        }

        let should_connect = self
            .opened_tabs
            .get(idx)
            .and_then(|tab| tab.group.as_ref())
            .map(|group| {
                group.publish_ready
                    && group.peers.iter().any(|peer| {
                        peer.authorized && !peer.ready && !peer.connecting && peer.conn.is_none()
                    })
            })
            .unwrap_or(false);

        if should_connect {
            tasks.extend(self.group_connect_tasks(tab_id));
        }

        if roster_sync_needed {
            tasks.push(self.send_group_roster_sync_task(tab_id));
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
            group_expected_acks: Vec::new(),
            group_received_acks: Vec::new(),
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

    fn clear_group_peer_incoming_image_state(peer: &mut GroupPeerRuntime) {
        peer.incoming_image_name = None;
        peer.incoming_image_mime = None;
        peer.incoming_image_expected = 0;
        peer.incoming_image_received = 0;
        peer.incoming_image_msg_id = 0;
        peer.incoming_image_bytes.clear();
    }

    fn start_group_peer_handshake(peer: &mut GroupPeerRuntime, now_ms: u64) {
        peer.e2e = E2E::new(false);
        peer.ready = false;
        peer.connecting = false;
        peer.handshake_started_ms = now_ms;
        peer.handshake_identity_received = false;
        peer.handshake_key_received = false;
        peer.heartbeat_last_rx_ms = 0;
        peer.heartbeat_last_ping_ms = 0;
        Self::clear_group_peer_incoming_image_state(peer);
    }

    fn reset_group_peer_transport_state(peer: &mut GroupPeerRuntime) {
        peer.conn = None;
        peer.pending_conn = None;
        peer.e2e = E2E::new(false);
        peer.ready = false;
        peer.connecting = false;
        peer.handshake_started_ms = 0;
        peer.handshake_identity_received = false;
        peer.handshake_key_received = false;
        peer.heartbeat_last_rx_ms = 0;
        peer.heartbeat_last_ping_ms = 0;
        Self::clear_group_peer_incoming_image_state(peer);
    }

    fn send_prepared_image(
        &mut self,
        filename: String,
        mime: String,
        bytes: Vec<u8>,
    ) -> Result<Task<Message>, String> {
        let Some(tab) = self.active_tab() else {
            return Err("Open a chat tab before sending an image.".into());
        };
        if tab.outgoing_phase != OutgoingFilePhase::Idle
            || tab.outgoing_image_phase != OutgoingImagePhase::Idle
        {
            return Err("Another transfer is already in progress.".into());
        }
        if tab.meta.kind != TabKind::Group && (tab.live_conn.is_none() || !tab.session.live_ready) {
            return Err("Image send requires a live secure chat.".into());
        }
        if bytes.is_empty() {
            return Err("Image preview is empty.".into());
        }
        if bytes.len() > MAX_FILE_SIZE {
            return Err(format!("Image preview too large ({} bytes).", bytes.len()));
        }

        let msg_id = self.generate_msg_id();

        if self.active_tab_is_group() {
            if bytes.len() > GROUP_IMAGE_TRANSFER_MAX_BYTES {
                return Err(format!(
                    "Group image preview too large ({} bytes). Maximum is {} bytes.",
                    bytes.len(),
                    GROUP_IMAGE_TRANSFER_MAX_BYTES
                ));
            }

            let tab_id = self
                .active_tab()
                .map(|tab| tab.id)
                .ok_or_else(|| "Open a chat tab before sending an image.".to_string())?;
            let mut tasks = Vec::new();
            let mut expected_acks = Vec::new();

            if let Some(tab) = self.active_tab_mut() {
                let sam_runtime = tab.sam_runtime.clone();
                let Some(group) = tab.group.as_mut() else {
                    return Err("Group runtime is not available.".into());
                };

                for peer in &group.peers {
                    if !peer.ready || !peer.authorized {
                        continue;
                    }

                    let Some(conn) = peer.conn.clone() else {
                        continue;
                    };

                    expected_acks.push(peer.member.b32.to_ascii_lowercase());
                    let e2e = peer.e2e.clone();
                    let filename = filename.clone();
                    let mime = mime.clone();
                    let bytes = bytes.clone();
                    let task = Task::perform(
                        async move {
                            Self::send_group_image_sequence(
                                conn, e2e, filename, mime, bytes, msg_id,
                            )
                            .await
                        },
                        move |result| Message::SendFinished(tab_id, result),
                    );
                    tasks.push(sam_runtime.track_send_task(task));
                }
            }

            if expected_acks.is_empty() {
                return Err("No ready group members.".into());
            }

            self.session.bubbles.push(Bubble {
                author: "Me".into(),
                content: BubbleContent::Image(Self::image_bubble_data(bytes)),
                mine: true,
                offline: false,
                timestamp_utc: Self::now_utc_hms(),
                msg_id: Some(msg_id),
                delivered: false,
                group_expected_acks: expected_acks,
                group_received_acks: Vec::new(),
            });

            self.store_active_runtime();
            tasks.push(operation::snap_to_end(
                self.session.messages_scroll_id.clone(),
            ));
            return Ok(Task::batch(tasks));
        }

        self.session.bubbles.push(Bubble {
            author: "Me".into(),
            content: BubbleContent::Image(Self::image_bubble_data(bytes.clone())),
            mine: true,
            offline: false,
            timestamp_utc: Self::now_utc_hms(),
            msg_id: Some(msg_id),
            delivered: false,
            group_expected_acks: Vec::new(),
            group_received_acks: Vec::new(),
        });

        if let Some(tab) = self.active_tab_mut() {
            tab.outgoing_image_name = Some(filename);
            tab.outgoing_image_mime = Some(mime);
            tab.outgoing_image_total = bytes.len() as u64;
            tab.outgoing_image_sent = 0;
            tab.outgoing_image_msg_id = msg_id;
            tab.outgoing_image_bytes = bytes;
            tab.outgoing_image_phase = OutgoingImagePhase::Header;
            tab.outgoing_image_send_in_flight = false;
        }

        self.store_active_runtime();
        Ok(operation::snap_to_end(
            self.session.messages_scroll_id.clone(),
        ))
    }

    async fn send_group_image_sequence(
        conn: LiveConnection,
        e2e: E2E,
        filename: String,
        mime: String,
        bytes: Vec<u8>,
        msg_id: u64,
    ) -> Result<(), String> {
        let total = bytes.len() as u64;
        let frame_j = Frame {
            msg_type: MsgType::J,
            msg_id,
            payload: e2e.encrypt(format!("{filename}|{mime}|{total}").as_bytes()),
        };
        conn.send_frame(&frame_j).await.map_err(|e| e.to_string())?;

        for chunk in bytes.chunks(4096) {
            let encoded = general_purpose::STANDARD.encode(chunk);
            let frame_g = Frame {
                msg_type: MsgType::G,
                msg_id,
                payload: e2e.encrypt(encoded.as_bytes()),
            };
            conn.send_frame(&frame_g).await.map_err(|e| e.to_string())?;
        }

        let frame_z = Frame {
            msg_type: MsgType::Z,
            msg_id,
            payload: Vec::new(),
        };
        conn.send_frame(&frame_z).await.map_err(|e| e.to_string())?;
        Ok(())
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
        Self::encode_image_preview(decoded, keep_alpha)
    }

    fn prepare_clipboard_image_draft(
        image: arboard::ImageData<'static>,
    ) -> Result<PendingImageDraft, String> {
        if image.width == 0 || image.height == 0 {
            return Err("Clipboard image has invalid dimensions.".into());
        }

        let expected_bytes = image
            .width
            .checked_mul(image.height)
            .and_then(|pixels| pixels.checked_mul(4))
            .ok_or_else(|| "Clipboard image dimensions are too large.".to_string())?;
        if expected_bytes != image.bytes.len() {
            return Err("Clipboard image pixel data is invalid.".into());
        }
        if expected_bytes > MAX_FILE_SIZE {
            return Err(format!(
                "Clipboard image is too large ({expected_bytes} decoded bytes)."
            ));
        }

        let width = u32::try_from(image.width)
            .map_err(|_| "Clipboard image width is too large.".to_string())?;
        let height = u32::try_from(image.height)
            .map_err(|_| "Clipboard image height is too large.".to_string())?;
        let raw = image.bytes.into_owned();
        let keep_alpha = raw.chunks_exact(4).any(|pixel| pixel[3] != 255);
        let rgba = ::image::RgbaImage::from_raw(width, height, raw)
            .ok_or_else(|| "Clipboard image pixel data is invalid.".to_string())?;
        let decoded = ::image::DynamicImage::ImageRgba8(rgba);
        let (bytes, mime) = Self::encode_image_preview(decoded, keep_alpha)?;
        let extension = if mime == "image/png" { "png" } else { "jpg" };
        let filename = format!("pasted-image-{}.{}", Self::now_epoch_millis(), extension);

        Ok(PendingImageDraft {
            filename,
            mime,
            image: Self::image_bubble_data(bytes),
        })
    }

    fn encode_image_preview(
        decoded: ::image::DynamicImage,
        keep_alpha: bool,
    ) -> Result<(Vec<u8>, String), String> {
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
            group_expected_acks: Vec::new(),
            group_received_acks: Vec::new(),
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

    fn mark_group_delivered(bubbles: &mut [Bubble], delivered_id: u64, peer_b32: &str) {
        let peer_b32 = peer_b32.to_ascii_lowercase();

        for bubble in bubbles.iter_mut().rev() {
            if !bubble.mine
                || bubble.msg_id != Some(delivered_id)
                || bubble.group_expected_acks.is_empty()
            {
                continue;
            }

            if !bubble
                .group_expected_acks
                .iter()
                .any(|b32| b32.eq_ignore_ascii_case(&peer_b32))
            {
                break;
            }

            if !bubble
                .group_received_acks
                .iter()
                .any(|b32| b32.eq_ignore_ascii_case(&peer_b32))
            {
                bubble.group_received_acks.push(peer_b32);
            }

            bubble.delivered = bubble.group_received_acks.len() >= bubble.group_expected_acks.len();
            break;
        }
    }

    fn left_status_indicators<'a>(session: &'a SessionState) -> iced::widget::Row<'a, Message> {
        let mut row_acc = if session.profile.starts_with("group:") {
            row![
                mode_indicator(&session.profile),
                bold_indicator("G", PY_GREEN),
                owned_profile_indicator(Self::group_status_display_name(session)),
            ]
            .spacing(6)
        } else {
            row![
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
            .spacing(6)
        };

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

    fn group_status_display_name(session: &SessionState) -> String {
        let active_group_key = session.profile.strip_prefix("group:");

        let active_group = active_group_key
            .and_then(|key| {
                session
                    .groups
                    .iter()
                    .find(|group| storage::group_storage_key(group) == key)
            })
            .or_else(|| {
                session
                    .selected_group_idx
                    .and_then(|idx| session.groups.get(idx))
            });

        if let Some(group) = active_group {
            return Self::group_self_display_name(group);
        }

        let trimmed = session.group_display_name_input.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }

        if session.my_b32.is_some() {
            return format!("member-{}", short_b32(session.my_b32.as_deref()));
        }

        active_group_key.unwrap_or("group").to_string()
    }

    fn has_active_connection_attempt(&self) -> bool {
        self.active_tab()
            .map(|tab| {
                tab.connect_in_flight || tab.live_conn.is_some() || tab.pending_conn.is_some()
            })
            .unwrap_or(false)
    }

    fn new_app_home_tab() -> ChatTab {
        ChatTab {
            kind: TabKind::AppHome,
            title: "GLOBAL".into(),
            profile_name: "__app__".into(),
            has_unread: false,
            has_incoming: false,
            connected: false,
            closing: false,
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

    fn active_tab_is_group(&self) -> bool {
        self.active_tab()
            .map(|tab| tab.meta.kind == TabKind::Group)
            .unwrap_or(false)
    }

    fn active_group_ready_count(&self) -> usize {
        self.active_tab()
            .and_then(|tab| tab.group.as_ref())
            .map(|group| {
                group
                    .peers
                    .iter()
                    .filter(|peer| peer.ready && peer.authorized && peer.conn.is_some())
                    .count()
            })
            .unwrap_or(0)
    }

    fn is_profile_open_in_any_tab(&self, profile_name: &str) -> bool {
        self.opened_tabs
            .iter()
            .any(|tab| tab.meta.profile_name == profile_name)
    }

    fn is_group_open_in_any_tab(&self, group_key: &str) -> bool {
        let profile_name = format!("group:{group_key}");
        self.opened_tabs
            .iter()
            .any(|tab| tab.meta.kind == TabKind::Group && tab.meta.profile_name == profile_name)
    }

    fn update_open_group_roster(&mut self, group: &GroupMeta) {
        for tab in &mut self.opened_tabs {
            let Some(runtime) = tab.group.as_mut() else {
                continue;
            };

            if storage::group_storage_key(&runtime.meta) != storage::group_storage_key(group) {
                continue;
            }

            runtime.meta = group.clone();
            runtime.peers.retain(|peer| {
                group
                    .members
                    .iter()
                    .any(|member| member.b32.eq_ignore_ascii_case(&peer.member.b32))
            });

            for member in &group.members {
                if let Some(peer) = runtime
                    .peers
                    .iter_mut()
                    .find(|peer| peer.member.b32.eq_ignore_ascii_case(&member.b32))
                {
                    peer.member.name = member.name.clone();
                    continue;
                }

                runtime
                    .peers
                    .push(Self::new_group_peer_runtime(member.clone(), true));
            }
        }
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
        session.known_remote_next_send = offline
            .known_remote_next_send
            .max(offline.drop_recv_base);
        session.highest_authenticated_recv_index = offline.highest_authenticated_recv_index;
        session.missing_drop_recv = offline.missing_drop_recv.clone();
        session.skipped_drop_recv = offline.skipped_drop_recv.clone();
        session.forward_probe_index = offline.forward_probe_index;
        session
            .missing_drop_recv
            .sort_unstable_by_key(|entry| entry.index);
        session
            .skipped_drop_recv
            .sort_unstable_by_key(|entry| entry.index);
        session
            .missing_drop_recv
            .truncate(OFFLINE_RECOVERY_STATE_LIMIT);
        session
            .skipped_drop_recv
            .truncate(OFFLINE_RECOVERY_STATE_LIMIT);
    }

    fn offline_state_from_session(session: &SessionState) -> OfflineState {
        OfflineState {
            offline_shared_secret: session.offline_shared_secret.unwrap_or([0u8; 32]),
            drop_send_index: session.drop_send_index,
            drop_recv_base: session.drop_recv_base,
            drop_window: session.drop_window,
            consumed_drop_recv: session.consumed_drop_recv.clone(),
            known_remote_next_send: session.known_remote_next_send,
            highest_authenticated_recv_index: session.highest_authenticated_recv_index,
            missing_drop_recv: session.missing_drop_recv.clone(),
            skipped_drop_recv: session.skipped_drop_recv.clone(),
            forward_probe_index: session.forward_probe_index,
        }
    }

    fn save_offline_state_for_tab(tab: &mut OpenedTab, context: &str) {
        let Some(peer_b32) = tab.session.stored_peer.clone() else {
            return;
        };
        let offline = Self::offline_state_from_session(&tab.session);
        if let Err(err) =
            storage::save_offline_state(&tab.session.profile, &peer_b32, &offline)
        {
            tab.session.log_lines.push(format!("{context}: {err}"));
        }
    }

    fn has_real_offline_secret(&self) -> bool {
        self.session
            .offline_shared_secret
            .map(|s| s.iter().any(|b| *b != 0))
            .unwrap_or(false)
    }

    fn session_has_real_offline_secret(session: &SessionState) -> bool {
        session
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

    fn sync_offline_secret_if_needed_task(&mut self, tab_id: u64) -> Task<Message> {
        let Some(idx) = self.find_tab_index_by_id(tab_id) else {
            return Task::none();
        };

        if Self::should_initiate_offline_secret_for_session(&self.opened_tabs[idx].session) {
            self.send_offline_secret_if_needed_task(tab_id)
        } else {
            self.request_offline_secret_if_needed_task(tab_id)
        }
    }

    fn request_offline_secret_if_needed_task(&mut self, tab_id: u64) -> Task<Message> {
        let Some(idx) = self.find_tab_index_by_id(tab_id) else {
            return Task::none();
        };

        let Some(conn) = self.opened_tabs[idx].live_conn.clone() else {
            return Task::none();
        };

        let session = &self.opened_tabs[idx].session;
        if !session.live_ready
            || session.profile == "default"
            || session.stored_peer.is_none()
            || session.stored_peer_dest_b64.is_none()
            || session.deaddrop_servers.is_empty()
            || Self::session_has_real_offline_secret(session)
        {
            return Task::none();
        }

        self.opened_tabs[idx]
            .session
            .log_lines
            .push("Requesting offline secret sync from peer.".into());

        let frame = Frame {
            msg_type: MsgType::S,
            msg_id: self.generate_msg_id(),
            payload: OFFLINE_SECRET_REQUEST_SIGNAL.as_bytes().to_vec(),
        };

        Task::perform(
            async move { conn.send_frame(&frame).await.map_err(|e| e.to_string()) },
            move |result| Message::SendFinished(tab_id, result),
        )
    }

    fn send_offline_secret_if_needed_task(&mut self, tab_id: u64) -> Task<Message> {
        let Some(idx) = self.find_tab_index_by_id(tab_id) else {
            return Task::none();
        };

        let Some(conn) = self.opened_tabs[idx].live_conn.clone() else {
            return Task::none();
        };

        if !self.opened_tabs[idx].session.live_ready || !self.opened_tabs[idx].e2e.ready() {
            return Task::none();
        }

        if !(self.opened_tabs[idx].session.profile != "default"
            && self.opened_tabs[idx].session.stored_peer.is_some()
            && self.opened_tabs[idx].session.stored_peer_dest_b64.is_some()
            && !self.opened_tabs[idx].session.deaddrop_servers.is_empty())
        {
            return Task::none();
        }

        if !Self::should_initiate_offline_secret_for_session(&self.opened_tabs[idx].session) {
            return Task::none();
        }

        let secret = if Self::session_has_real_offline_secret(&self.opened_tabs[idx].session) {
            let Some(secret) = self.opened_tabs[idx].session.offline_shared_secret else {
                return Task::none();
            };
            secret
        } else {
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
            secret
        };

        let payload = match self.opened_tabs[idx].e2e.encrypt_strict(&secret) {
            Ok(payload) => payload,
            Err(err) => {
                self.opened_tabs[idx]
                    .session
                    .log_lines
                    .push(format!("Offline secret encryption failed: {err}"));
                return Task::none();
            }
        };

        self.opened_tabs[idx]
            .session
            .log_lines
            .push("Offline secret sync sent.".into());

        let frame = Frame {
            msg_type: MsgType::X,
            msg_id: self.generate_msg_id(),
            payload,
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

    fn is_valid_b32_address(address: &str) -> bool {
        let address = address.trim().to_lowercase();

        if address.is_empty() {
            return false;
        }

        if !address.ends_with(".b32.i2p") {
            return false;
        }

        let host = &address[..address.len() - 8];

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
        if !Self::is_persistent_contact_tab(tab) {
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
        if !Self::is_persistent_contact_tab(tab) {
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

        if !tab.session.live_ready
            || !tab.e2e.ready()
            || tab.session.profile == "default"
            || tab.session.stored_peer.is_none()
            || tab.session.stored_peer_dest_b64.is_none()
            || tab.session.deaddrop_servers.is_empty()
        {
            return Task::none();
        }

        let Some(conn) = tab.live_conn.clone() else {
            return Task::none();
        };

        let plaintext = tab.session.deaddrop_servers.join("\n").into_bytes();
        let Ok(payload) = tab.e2e.encrypt_strict(&plaintext) else {
            return Task::none();
        };

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

    fn can_send_offline_index_sync(tab: &OpenedTab) -> bool {
        let session = &tab.session;
        session.profile != "default"
            && session.live_ready
            && session.tofu_verified
            && tab.e2e.ready()
            && Self::session_has_real_offline_secret(session)
            && session.stored_peer.as_deref() == session.current_peer_addr.as_deref()
            && session.stored_peer_dest_b64.as_deref()
                == session.current_peer_dest_b64.as_deref()
            && tab.live_conn.is_some()
    }

    fn encode_offline_index_sync_payload(session: &SessionState) -> Vec<u8> {
        let mut payload = Vec::with_capacity(OFFLINE_INDEX_SYNC_PAYLOAD_LEN);
        payload.push(OFFLINE_INDEX_SYNC_VERSION);
        payload.extend_from_slice(&session.drop_send_index.to_be_bytes());
        payload.extend_from_slice(&session.drop_recv_base.to_be_bytes());
        payload
    }

    fn decode_offline_index_sync_payload(payload: &[u8]) -> Option<(u64, u64)> {
        if payload.len() != OFFLINE_INDEX_SYNC_PAYLOAD_LEN
            || payload[0] != OFFLINE_INDEX_SYNC_VERSION
        {
            return None;
        }

        let mut next_send_bytes = [0u8; 8];
        next_send_bytes.copy_from_slice(&payload[1..9]);
        let mut receive_base_bytes = [0u8; 8];
        receive_base_bytes.copy_from_slice(&payload[9..17]);
        Some((
            u64::from_be_bytes(next_send_bytes),
            u64::from_be_bytes(receive_base_bytes),
        ))
    }

    fn send_offline_index_sync_task(&mut self, tab_id: u64) -> Task<Message> {
        let Some(idx) = self.find_tab_index_by_id(tab_id) else {
            return Task::none();
        };
        if !Self::can_send_offline_index_sync(&self.opened_tabs[idx])
            || self.opened_tabs[idx].offline_index_sync_sent
        {
            return Task::none();
        }

        let Some(conn) = self.opened_tabs[idx].live_conn.clone() else {
            return Task::none();
        };
        let plaintext = Self::encode_offline_index_sync_payload(&self.opened_tabs[idx].session);
        let payload = match self.opened_tabs[idx].e2e.encrypt_strict(&plaintext) {
            Ok(payload) => payload,
            Err(err) => {
                self.opened_tabs[idx]
                    .session
                    .log_lines
                    .push(format!("Offline index sync encryption failed: {err}"));
                return Task::none();
            }
        };

        self.opened_tabs[idx].offline_index_sync_sent = true;
        let frame = Frame {
            msg_type: MsgType::I,
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
    ) -> Vec<OfflinePollTarget> {
        let mut out = Vec::new();

        let window = session.drop_window as u64;
        let window_end = session.drop_recv_base.saturating_add(window);
        for recv_index in session.drop_recv_base..window_end {
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

            out.push(OfflinePollTarget {
                index: recv_index,
                key: dd_key,
                kind: OfflinePollKind::Window,
            });
        }

        out
    }

    fn advance_drop_recv_base(session: &mut SessionState) {
        loop {
            if session
                .consumed_drop_recv
                .iter()
                .any(|n| *n == session.drop_recv_base)
                || session
                    .skipped_drop_recv
                    .iter()
                    .any(|entry| entry.index == session.drop_recv_base)
            {
                session.drop_recv_base = session.drop_recv_base.saturating_add(1);
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
            self.opened_tabs[idx].deaddrop_poll_round_misses.clear();
            self.opened_tabs[idx]
                .deaddrop_poll_round_authenticated
                .clear();
            self.opened_tabs[idx].deaddrop_last_poll_ms = Self::now_epoch_millis();
            return Task::none();
        }

        let Some(target) = self.opened_tabs[idx].deaddrop_poll_queue.first().cloned() else {
            Self::finalize_offline_poll_round(&mut self.opened_tabs[idx]);
            self.opened_tabs[idx].deaddrop_poll_in_flight = false;
            self.opened_tabs[idx].deaddrop_last_poll_ms = Self::now_epoch_millis();
            return Task::none();
        };

        self.opened_tabs[idx].deaddrop_poll_queue.remove(0);

        let dd = std::sync::Arc::clone(&self.opened_tabs[idx].deaddrop);
        let dd_key_for_task = target.key.clone();
        let recv_index = target.index;
        let poll_kind = target.kind;
        let dd_key = target.key;

        Task::perform(
            async move {
                let mut dd = dd.lock().await;
                dd.get_with_stats(&dd_key_for_task).await
            },
            move |(blobs, stats)| {
                Message::OfflinePollKeyFinished(
                    tab_id,
                    recv_index,
                    poll_kind,
                    dd_key,
                    blobs,
                    stats,
                )
            },
        )
    }

    fn handle_offline_poll_key_result(
        tab: &mut OpenedTab,
        recv_index: u64,
        poll_kind: OfflinePollKind,
        blobs: Vec<(String, Vec<u8>)>,
        stats: &[DeaddropOpStat],
        mark_unread: bool,
    ) {
        if blobs.is_empty() {
            Self::set_dd_status(&mut tab.session, "get_miss");
            let confirmed_miss = stats
                .iter()
                .any(|stat| stat.ok && stat.detail.eq_ignore_ascii_case("MISS"));
            if confirmed_miss && poll_kind != OfflinePollKind::RecoveryProbe {
                tab.deaddrop_poll_round_misses.push(recv_index);
            } else if !confirmed_miss {
                tab.session.log_lines.push(format!(
                    "Offline poll at index {} was indeterminate; no server confirmed MISS.",
                    recv_index
                ));
            }
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

            let frame_bytes = match tab.e2e.decrypt_offline_blob_strict(&blob, &blob_key) {
                Ok(frame_bytes) => frame_bytes,
                Err(err) => {
                    tab.session.log_lines.push(format!(
                        "Rejected unauthenticated offline blob at recv index {}: {}",
                        recv_index, err
                    ));
                    continue;
                }
            };

            match Frame::decode(&frame_bytes) {
                Ok(frame) => {
                    match frame.msg_type {
                        MsgType::U => {
                            let plain = tab.e2e.decrypt(&frame.payload);
                            match String::from_utf8(plain) {
                                Ok(text) => {
                                    tab.session.seen_drop_msgs.push(blob_hash);
                                    got_valid_blob = true;
                                    tab.session.bubbles.push(Bubble::peer_offline(text));
                                    if mark_unread {
                                        tab.meta.has_unread = true;
                                    }
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
            tab.deaddrop_poll_round_authenticated.push(recv_index);
            tab.session.known_remote_next_send = tab
                .session
                .known_remote_next_send
                .max(recv_index.saturating_add(1));
            tab.session.highest_authenticated_recv_index = Some(
                tab.session
                    .highest_authenticated_recv_index
                    .map(|current| current.max(recv_index))
                    .unwrap_or(recv_index),
            );
            tab.session
                .missing_drop_recv
                .retain(|entry| entry.index != recv_index);
            tab.session
                .skipped_drop_recv
                .retain(|entry| entry.index != recv_index);

            if recv_index >= tab.session.drop_recv_base
                && !tab
                    .session
                    .consumed_drop_recv
                    .iter()
                    .any(|n| *n == recv_index)
            {
                tab.session.consumed_drop_recv.push(recv_index);
            }

            Self::advance_drop_recv_base(&mut tab.session);
        } else {
            Self::set_dd_status(&mut tab.session, "get_miss");
            tab.session.log_lines.push(format!(
                "Offline blobs at recv index {} contained no authenticated message.",
                recv_index
            ));
        }
    }

    fn finalize_offline_poll_round(tab: &mut OpenedTab) {
        let now_ms = Self::now_epoch_millis();
        tab.deaddrop_poll_round_misses.sort_unstable();
        tab.deaddrop_poll_round_misses.dedup();
        tab.deaddrop_poll_round_authenticated.sort_unstable();
        tab.deaddrop_poll_round_authenticated.dedup();

        for index in &tab.deaddrop_poll_round_misses {
            if let Some(entry) = tab
                .session
                .missing_drop_recv
                .iter_mut()
                .find(|entry| entry.index == *index)
            {
                entry.confirmed_miss_rounds = entry.confirmed_miss_rounds.saturating_add(1);
                entry.last_miss_ms = now_ms;
            } else {
                tab.session.missing_drop_recv.push(OfflineMissingIndexState {
                    index: *index,
                    confirmed_miss_rounds: 1,
                    first_miss_ms: now_ms,
                    last_miss_ms: now_ms,
                });
            }
        }

        let skip_indexes = tab
            .session
            .missing_drop_recv
            .iter()
            .filter(|entry| {
                entry.confirmed_miss_rounds >= OFFLINE_GAP_MISS_ROUNDS
                    && entry.index < tab.session.known_remote_next_send
                    && entry.index >= tab.session.drop_recv_base
            })
            .map(|entry| entry.index)
            .collect::<Vec<_>>();

        for index in skip_indexes {
            if !tab
                .session
                .skipped_drop_recv
                .iter()
                .any(|entry| entry.index == index)
            {
                tab.session.skipped_drop_recv.push(OfflineSkippedIndexState {
                    index,
                    skipped_at_ms: now_ms,
                    last_recovery_probe_ms: 0,
                });
                tab.session.log_lines.push(format!(
                    "Skipped confirmed offline gap at recv index {}; late recovery remains active.",
                    index
                ));
            }
        }

        let skipped_indexes = tab
            .session
            .skipped_drop_recv
            .iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>();
        tab.session
            .missing_drop_recv
            .retain(|entry| !skipped_indexes.contains(&entry.index));
        tab.session.skipped_drop_recv.retain(|entry| {
            now_ms.saturating_sub(entry.skipped_at_ms) <= OFFLINE_SKIPPED_RETENTION_MS
        });
        tab.session
            .missing_drop_recv
            .sort_unstable_by_key(|entry| entry.index);
        tab.session
            .skipped_drop_recv
            .sort_unstable_by_key(|entry| entry.index);
        tab.session
            .missing_drop_recv
            .truncate(OFFLINE_RECOVERY_STATE_LIMIT);
        tab.session
            .skipped_drop_recv
            .truncate(OFFLINE_RECOVERY_STATE_LIMIT);

        let previous_base = tab.session.drop_recv_base;
        Self::advance_drop_recv_base(&mut tab.session);
        if previous_base != tab.session.drop_recv_base
            || !tab.deaddrop_poll_round_authenticated.is_empty()
        {
            tab.deaddrop_stalled_sweeps = 0;
            tab.session.forward_probe_index = tab
                .session
                .drop_recv_base
                .saturating_add(tab.session.drop_window as u64);
        } else {
            tab.deaddrop_stalled_sweeps = tab.deaddrop_stalled_sweeps.saturating_add(1);
        }

        tab.deaddrop_poll_round_misses.clear();
        tab.deaddrop_poll_round_authenticated.clear();
        Self::save_offline_state_for_tab(tab, "Failed to save offline recovery state");
    }
}

//fffff

fn bubble_timestamp_row<'a>(bubble: &'a Bubble) -> Element<'a, Message> {
    row![
        Space::new().width(Length::Fill),
        text(&bubble.timestamp_utc)
            .size(10)
            .color(Color::from_rgb8(140, 140, 140)),
        if let Some(mark) = bubble_delivery_mark(bubble) {
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

fn bubble_timestamp_inline<'a>(bubble: &'a Bubble) -> Element<'a, Message> {
    row![
        text(&bubble.timestamp_utc)
            .size(10)
            .color(Color::from_rgb8(140, 140, 140)),
        if let Some(mark) = bubble_delivery_mark(bubble) {
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
    .into()
}

fn bubble_delivery_mark(bubble: &Bubble) -> Option<String> {
    if !bubble.mine {
        return None;
    }

    if !bubble.group_expected_acks.is_empty() {
        return Some(format!(
            " ✓ {}/{}",
            bubble
                .group_received_acks
                .len()
                .min(bubble.group_expected_acks.len()),
            bubble.group_expected_acks.len()
        ));
    }

    if bubble.delivered {
        return Some(if bubble.offline { " ✓" } else { " ✓✓" }.into());
    }

    None
}

fn message_row<'a>(idx: usize, bubble: &'a Bubble) -> Element<'a, Message> {
    let (body, max_width): (Element<'a, Message>, f32) = match &bubble.content {
        BubbleContent::Text(value) => {
            let show_author = should_show_bubble_author(bubble);
            let mut body_width =
                text_bubble_body_width(value, bubble_delivery_mark(bubble).is_some());
            if show_author {
                let author_width = bubble.author.chars().count() as f32 * 7.0 + 4.0;
                body_width = body_width
                    .max(author_width)
                    .min(TEXT_BUBBLE_MAX_WIDTH - 24.0);
            }
            let message_body: Element<'a, Message> =
                if let Some(reply) = parse_reply_text(value.as_str()) {
                    column![
                        container(
                            column![
                                text(format!("Reply to {}", reply.author))
                                    .size(10)
                                    .color(Color::from_rgb8(155, 155, 164)),
                                text(reply.quote)
                                    .size(11)
                                    .color(Color::from_rgb8(178, 178, 186))
                                    .width(Length::Fill)
                                    .wrapping(iced::widget::text::Wrapping::WordOrGlyph),
                            ]
                            .spacing(2),
                        )
                        .width(Length::Fill)
                        .padding([5, 7])
                        .style(|_| quoted_reply_style()),
                        text(reply.body)
                            .size(12)
                            .width(Length::Fill)
                            .wrapping(iced::widget::text::Wrapping::WordOrGlyph),
                    ]
                    .spacing(6)
                    .into()
                } else {
                    text(value)
                        .size(12)
                        .width(Length::Fill)
                        .wrapping(iced::widget::text::Wrapping::WordOrGlyph)
                        .into()
                };
            let author_label: Element<'a, Message> = if show_author {
                text(&bubble.author)
                    .size(10)
                    .color(Color::from_rgb8(155, 155, 164))
                    .width(Length::Fill)
                    .into()
            } else {
                Space::new().height(0).into()
            };

            (
                column![
                    author_label,
                    message_body,
                    row![
                        button(
                            text("\u{e14d}")
                                .font(Font {
                                    family: font::Family::Name(APP_ICON_FONT_FAMILY),
                                    ..Font::default()
                                })
                                .size(12)
                        )
                        .width(20)
                        .height(16)
                        .padding(iced::Padding {
                            top: 0.0,
                            right: 2.0,
                            bottom: 2.0,
                            left: 4.0,
                        })
                        .style(copy_bubble_button_style)
                        .on_press(Message::CopyBubbleTextPressed(idx)),
                        button(
                            text("\u{e15e}")
                                .font(Font {
                                    family: font::Family::Name(APP_ICON_FONT_FAMILY),
                                    ..Font::default()
                                })
                                .size(12)
                        )
                        .width(20)
                        .height(16)
                        .padding(iced::Padding {
                            top: 0.0,
                            right: 2.0,
                            bottom: 2.0,
                            left: 4.0,
                        })
                        .style(copy_bubble_button_style)
                        .on_press(Message::ReplyBubblePressed(idx)),
                        Space::new().width(Length::Fill),
                        bubble_timestamp_inline(bubble),
                    ]
                    .spacing(6)
                    .align_y(Alignment::Center),
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
    let display_value = display_reply_text(value);
    let longest_line_chars = display_value
        .lines()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0) as f32;

    let estimated_text_width = longest_line_chars * 8.0 + 4.0;
    let footer_width = if delivered { 160.0 } else { 144.0 };
    let max_body_width = TEXT_BUBBLE_MAX_WIDTH - 24.0;

    estimated_text_width
        .max(footer_width)
        .clamp(TEXT_BUBBLE_MIN_BODY_WIDTH, max_body_width)
}

fn should_show_bubble_author(bubble: &Bubble) -> bool {
    if bubble.mine {
        return false;
    }

    !matches!(
        bubble.author.as_str(),
        "Peer" | "Peer-Offline" | "Me" | "Me-Offline"
    )
}

#[derive(Debug, Clone)]
struct ParsedReply {
    author: String,
    quote: String,
    body: String,
}

fn parse_reply_text(value: &str) -> Option<ParsedReply> {
    let rest = value.strip_prefix(REPLY_BEGIN_MARKER)?.strip_prefix('\n')?;
    let (author, rest) = rest.split_once('\n')?;
    let rest = rest.strip_prefix(REPLY_QUOTE_MARKER)?.strip_prefix('\n')?;
    let end_marker = format!("\n{REPLY_END_MARKER}\n");
    let (quote, body) = rest.split_once(end_marker.as_str())?;

    Some(ParsedReply {
        author: author.to_string(),
        quote: quote.to_string(),
        body: body.to_string(),
    })
}

fn display_reply_text(value: &str) -> String {
    if let Some(reply) = parse_reply_text(value) {
        format!("Reply to {}\n{}\n{}", reply.author, reply.quote, reply.body)
    } else {
        value.to_string()
    }
}

fn reply_source_text(value: &str) -> String {
    if let Some(reply) = parse_reply_text(value) {
        reply.body
    } else {
        value.to_string()
    }
}

fn compact_reply_preview(value: &str, max_chars: usize) -> String {
    let flattened = value.split_whitespace().collect::<Vec<_>>().join(" ");

    if flattened.chars().count() <= max_chars {
        return flattened;
    }

    let mut out = flattened.chars().take(max_chars).collect::<String>();
    out.push_str("...");
    out
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

fn group_sam_session_label(profile_name: &str) -> String {
    let clean_key = profile_name
        .strip_prefix("group:")
        .unwrap_or(profile_name)
        .trim()
        .trim_end_matches(".b32.i2p")
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();

    let compact = if clean_key.len() > 12 {
        format!("{}_{}", &clean_key[..6], &clean_key[clean_key.len() - 6..])
    } else {
        clean_key
    };

    format!("group_{}", compact)
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

fn owned_profile_indicator<'a>(profile: String) -> iced::widget::Container<'a, Message> {
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

    let age_ms = IcedCommApp::now_epoch_millis().saturating_sub(session.dd_status_at_ms);

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

fn status_address_container_style() -> container::Style {
    container::Style {
        background: None,
        border: border::Border {
            color: Color::from_rgb8(70, 70, 80),
            width: 1.5,
            radius: border::Radius::from(3.0),
        },
        ..Default::default()
    }
}

fn tab_status_marker<'a>(tab: &'a ChatTab, blink_on: bool, closing: bool) -> Element<'a, Message> {
    if tab.kind == TabKind::AppHome {
        return Space::new().width(Length::Shrink).into();
    }

    let connection_marker: Element<'a, Message> = if closing {
        text("...")
            .size(13)
            .color(APP_TAB_DISABLED_TEXT)
            .into()
    } else if tab.initializing {
        let frame = APP_TAB_SPINNER_FRAMES
            [((IcedCommApp::now_epoch_millis() / 120) as usize) % APP_TAB_SPINNER_FRAMES.len()];

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
    } else {
        Space::new().width(Length::Shrink).into()
    };

    if tab.has_unread && !closing {
        row![
            connection_marker,
            text("\u{e145}")
                .font(Font {
                    family: font::Family::Name(APP_ICON_FONT_FAMILY),
                    ..Font::default()
                })
                .size(13)
                .color(Color::from_rgb8(220, 220, 220)),
        ]
        .spacing(3)
        .align_y(Alignment::Center)
        .into()
    } else {
        connection_marker
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

fn sidebar_divider_style() -> container::Style {
    container::Style {
        background: Some(Background::Color(Color::from_rgb8(76, 76, 84))),
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

fn reply_preview_style() -> container::Style {
    container::Style {
        background: Some(Background::Color(Color::from_rgb8(20, 20, 25))),
        border: border::Border {
            color: Color::from_rgb8(74, 74, 84),
            width: 1.0,
            radius: border::Radius::from(4.0),
        },
        ..Default::default()
    }
}

fn quoted_reply_style() -> container::Style {
    container::Style {
        background: Some(Background::Color(Color::from_rgb8(18, 18, 22))),
        border: border::Border {
            color: Color::from_rgb8(82, 82, 92),
            width: 1.0,
            radius: border::Radius::from(4.0),
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

fn copy_bubble_button_style(
    _theme: &iced::Theme,
    status: iced::widget::button::Status,
) -> iced::widget::button::Style {
    let (bg, text_color, border_color) = match status {
        iced::widget::button::Status::Active => (
            Color::from_rgba8(80, 80, 88, 0.35),
            Color::from_rgb8(178, 178, 184),
            Color::from_rgba8(130, 130, 140, 0.35),
        ),
        iced::widget::button::Status::Hovered => (
            Color::from_rgba8(96, 96, 106, 0.55),
            Color::from_rgb8(210, 210, 216),
            Color::from_rgba8(160, 160, 170, 0.65),
        ),
        iced::widget::button::Status::Pressed => (
            Color::from_rgba8(70, 70, 78, 0.65),
            Color::from_rgb8(230, 230, 235),
            Color::from_rgba8(180, 180, 190, 0.75),
        ),
        iced::widget::button::Status::Disabled => (
            Color::from_rgba8(60, 60, 66, 0.20),
            Color::from_rgb8(120, 120, 126),
            Color::from_rgba8(100, 100, 108, 0.20),
        ),
    };

    iced::widget::button::Style {
        background: Some(Background::Color(bg)),
        text_color,
        border: border::Border {
            color: border_color,
            width: 1.0,
            radius: border::Radius::from(3.0),
        },
        ..Default::default()
    }
}
