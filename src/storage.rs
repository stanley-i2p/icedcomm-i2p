use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::constants::{DEFAULT_SAM_HOST, DEFAULT_SAM_PORT};

const DIR_MODE: u32 = 0o700;
const FILE_MODE: u32 = 0o600;

const DEFAULT_DEADDROP_SERVERS: [&str; 3] = [
    "62afc5yf2lcthx44okvavvmvgb55cee3weqeqhuapcclz6evwyrq.b32.i2p",
    "x75crc4lkcd3xcfrj5sox662mujngzrtmvmejaixutdozg35fgvq.b32.i2p",
    "xxbgj3dlw7fvwz3emqnvyzxrdj3vqd3fcdw6rutmvzoxidyhp7bq.b32.i2p",
];

#[derive(Debug)]
pub enum StorageError {
    Io(String),
    Serde(String),
    InvalidName(String),
    AlreadyRunning,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io(e) => write!(f, "io error: {e}"),
            StorageError::Serde(e) => write!(f, "serde error: {e}"),
            StorageError::InvalidName(e) => write!(f, "invalid name: {e}"),
            StorageError::AlreadyRunning => write!(f, "another GUI instance is already running"),
        }
    }
}

impl std::error::Error for StorageError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContactMeta {
    pub name: String,
    pub my_dest_b64: Option<String>,
    pub locked_peer: Option<String>,
    pub locked_peer_dest_b64: Option<String>,
    pub pq_enabled: bool,
    #[serde(default)]
    pub deaddrop_servers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeaddropServerStat {
    pub put_ok: u64,
    pub put_fail: u64,
    pub get_ok: u64,
    pub get_fail: u64,
    pub last_success_ts: f64,
    pub latency_ema_ms: f64,
    pub latency_samples: u64,
}

impl Default for DeaddropServerStat {
    fn default() -> Self {
        Self {
            put_ok: 0,
            put_fail: 0,
            get_ok: 0,
            get_fail: 0,
            last_success_ts: 0.0,
            latency_ema_ms: 0.0,
            latency_samples: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_sam_host")]
    pub sam_host: String,
    #[serde(default = "default_sam_port")]
    pub sam_port: u16,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            sam_host: DEFAULT_SAM_HOST.to_string(),
            sam_port: DEFAULT_SAM_PORT,
        }
    }
}

fn default_sam_host() -> String {
    DEFAULT_SAM_HOST.to_string()
}

fn default_sam_port() -> u16 {
    DEFAULT_SAM_PORT
}

#[derive(Debug, Clone)]
pub struct OfflineState {
    pub offline_shared_secret: [u8; 32],
    pub drop_send_index: u64,
    pub drop_recv_base: u64,
    pub drop_window: u32,
    pub consumed_drop_recv: Vec<u64>,
}

pub fn default_offline_state() -> OfflineState {
    OfflineState {
        offline_shared_secret: [0u8; 32],
        drop_send_index: 0,
        drop_recv_base: 0,
        drop_window: 8,
        consumed_drop_recv: vec![],
    }
}

impl ContactMeta {
    pub fn new(name: String) -> Self {
        Self {
            name,
            my_dest_b64: None,
            locked_peer: None,
            locked_peer_dest_b64: None,
            pq_enabled: false,
            deaddrop_servers: DEFAULT_DEADDROP_SERVERS
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }
    }
}

pub struct AppLock {
    file: File,
}

impl Drop for AppLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub fn base_dir() -> PathBuf {
    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".icedcomm-i2p")
}

pub fn contacts_dir() -> PathBuf {
    base_dir().join("profiles")
}

pub fn contact_dir(name: &str) -> PathBuf {
    contacts_dir().join(name)
}

pub fn contact_dat_path(name: &str) -> PathBuf {
    contact_dir(name).join(format!("{name}.dat"))
}

pub fn deaddrop_stats_path(name: &str) -> PathBuf {
    contact_dir(name).join("deaddrop_stats.json")
}

pub fn app_lock_path() -> PathBuf {
    PathBuf::from(format!("{}.app.lock", base_dir().to_string_lossy()))
}

pub fn app_config_path() -> PathBuf {
    base_dir().join("app_config.json")
}

pub fn ensure_base_layout() -> Result<(), StorageError> {
    create_dir_secure_all(&base_dir())?;
    create_dir_secure_all(&contacts_dir())?;
    create_dir_secure_all(&files_dir())?;
    Ok(())
}

pub fn load_app_config() -> Result<AppConfig, StorageError> {
    ensure_base_layout()?;

    let path = app_config_path();
    if !path.exists() {
        let config = AppConfig::default();
        save_app_config(&config)?;
        return Ok(config);
    }

    let text = fs::read_to_string(&path).map_err(|e| StorageError::Io(e.to_string()))?;
    let mut config: AppConfig =
        serde_json::from_str(&text).map_err(|e| StorageError::Serde(e.to_string()))?;
    if config.sam_host.trim().is_empty() {
        config.sam_host = DEFAULT_SAM_HOST.to_string();
    }
    Ok(config)
}

pub fn save_app_config(config: &AppConfig) -> Result<(), StorageError> {
    ensure_base_layout()?;

    if config.sam_host.trim().is_empty() {
        return Err(StorageError::InvalidName("SAM host is empty".into()));
    }
    if config.sam_port == 0 {
        return Err(StorageError::InvalidName("SAM port must be 1-65535".into()));
    }

    let text =
        serde_json::to_string_pretty(config).map_err(|e| StorageError::Serde(e.to_string()))?;
    atomic_write_text(&app_config_path(), &text)
}

pub fn acquire_app_lock() -> Result<AppLock, StorageError> {
    let path = app_lock_path();
    if let Some(parent) = path.parent() {
        create_dir_secure_all(parent)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|e| StorageError::Io(e.to_string()))?;

    file.try_lock_exclusive()
        .map_err(|_| StorageError::AlreadyRunning)?;

    set_file_mode(&path)?;
    Ok(AppLock { file })
}

pub fn load_contacts() -> Result<Vec<ContactMeta>, StorageError> {
    ensure_base_layout()?;

    let mut out = Vec::new();

    let entries = fs::read_dir(contacts_dir()).map_err(|e| StorageError::Io(e.to_string()))?;

    for entry in entries {
        let entry = entry.map_err(|e| StorageError::Io(e.to_string()))?;
        let path = entry.path();

        if !path.is_dir() {
            continue;
        }

        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };

        if name == "default" {
            continue;
        }

        let dat_path = contact_dat_path(name);
        if dat_path.exists() {
            out.push(load_contact_meta(name)?);
        } else {
            let meta = ContactMeta::new(name.to_string());
            save_contact_meta(&meta)?;
            out.push(meta);
        }
    }

    out.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()));
    Ok(out)
}

pub fn create_contact(name: &str) -> Result<ContactMeta, StorageError> {
    validate_contact_name(name)?;

    ensure_base_layout()?;

    let dir = contact_dir(name);
    if dir.exists() {
        return Err(StorageError::InvalidName("contact already exists".into()));
    }

    create_dir_secure_all(&dir)?;

    let meta = ContactMeta::new(name.to_string());
    save_contact_meta(&meta)?;
    Ok(meta)
}

pub fn delete_contact(name: &str) -> Result<(), StorageError> {
    validate_contact_name(name)?;

    let dir = contact_dir(name);
    if dir.exists() {
        fs::remove_dir_all(&dir).map_err(|e| StorageError::Io(e.to_string()))?;
    }
    Ok(())
}

pub fn reset_contact(name: &str) -> Result<ContactMeta, StorageError> {
    validate_contact_name(name)?;

    let old_meta = load_contact_meta(name)?;
    let dir = contact_dir(name);
    if dir.exists() {
        fs::remove_dir_all(&dir).map_err(|e| StorageError::Io(e.to_string()))?;
    }

    let mut new_meta = ContactMeta::new(name.to_string());
    new_meta.my_dest_b64 = old_meta.my_dest_b64;
    save_contact_meta(&new_meta)?;
    Ok(new_meta)
}

pub fn wipe_all_profiles_and_files() -> Result<(), StorageError> {
    let base = base_dir();
    if base.exists() {
        fs::remove_dir_all(&base).map_err(|e| StorageError::Io(e.to_string()))?;
    }

    let vault = PathBuf::from(format!("{}.vault", base.to_string_lossy()));
    if vault.exists() {
        fs::remove_file(&vault).map_err(|e| StorageError::Io(e.to_string()))?;
    }

    let vault_tmp = PathBuf::from(format!("{}.vault.tmp", base.to_string_lossy()));
    if vault_tmp.exists() {
        fs::remove_file(&vault_tmp).map_err(|e| StorageError::Io(e.to_string()))?;
    }

    Ok(())
}

pub fn load_contact_meta(name: &str) -> Result<ContactMeta, StorageError> {
    validate_contact_name(name)?;

    let path = contact_dat_path(name);
    let mut f = File::open(&path).map_err(|e| StorageError::Io(e.to_string()))?;

    let mut s = String::new();
    f.read_to_string(&mut s)
        .map_err(|e| StorageError::Io(e.to_string()))?;

    serde_json::from_str(&s).map_err(|e| StorageError::Serde(e.to_string()))
}

pub fn save_contact_meta(meta: &ContactMeta) -> Result<(), StorageError> {
    validate_contact_name(&meta.name)?;

    let dir = contact_dir(&meta.name);
    create_dir_secure_all(&dir)?;

    let path = contact_dat_path(&meta.name);
    let text =
        serde_json::to_string_pretty(meta).map_err(|e| StorageError::Serde(e.to_string()))?;

    atomic_write_text(&path, &text)
}

pub fn load_deaddrop_stats(
    name: &str,
) -> Result<HashMap<String, DeaddropServerStat>, StorageError> {
    validate_contact_name(name)?;

    let path = deaddrop_stats_path(name);
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let mut f = File::open(&path).map_err(|e| StorageError::Io(e.to_string()))?;
    let mut s = String::new();
    f.read_to_string(&mut s)
        .map_err(|e| StorageError::Io(e.to_string()))?;

    serde_json::from_str(&s).map_err(|e| StorageError::Serde(e.to_string()))
}

pub fn save_deaddrop_stats(
    name: &str,
    stats: &HashMap<String, DeaddropServerStat>,
) -> Result<(), StorageError> {
    validate_contact_name(name)?;

    let dir = contact_dir(name);
    create_dir_secure_all(&dir)?;

    let path = deaddrop_stats_path(name);
    let text =
        serde_json::to_string_pretty(stats).map_err(|e| StorageError::Serde(e.to_string()))?;

    atomic_write_text(&path, &text)
}

fn validate_contact_name(name: &str) -> Result<(), StorageError> {
    let trimmed = name.trim();

    if trimmed.is_empty() {
        return Err(StorageError::InvalidName("empty contact name".into()));
    }

    if trimmed.eq_ignore_ascii_case("default")
        || trimmed.eq_ignore_ascii_case("__app__")
        || trimmed.eq_ignore_ascii_case("global")
    {
        return Err(StorageError::InvalidName("reserved profile name".into()));
    }

    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(StorageError::InvalidName(
            "path separators are not allowed".into(),
        ));
    }

    if trimmed.starts_with('.') {
        return Err(StorageError::InvalidName(
            "leading '.' is not allowed".into(),
        ));
    }

    Ok(())
}

pub fn create_dir_secure_all(path: &Path) -> Result<(), StorageError> {
    fs::create_dir_all(path).map_err(|e| StorageError::Io(e.to_string()))?;

    let base = base_dir();

    if path.starts_with(&base) {
        set_dir_mode(&base)?;

        let mut cur = base.clone();

        if let Ok(relative) = path.strip_prefix(&base) {
            for part in relative.components() {
                cur.push(part.as_os_str());

                if cur.exists() && cur.is_dir() {
                    set_dir_mode(&cur)?;
                }
            }
        }
    } else {
        set_dir_mode(path)?;
    }

    Ok(())
}

pub fn atomic_write_text(path: &Path, text: &str) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        create_dir_secure_all(parent)?;
    }

    let tmp = path.with_extension("tmp");

    {
        let mut f = File::create(&tmp).map_err(|e| StorageError::Io(e.to_string()))?;
        f.write_all(text.as_bytes())
            .map_err(|e| StorageError::Io(e.to_string()))?;
        f.flush().map_err(|e| StorageError::Io(e.to_string()))?;
    }

    set_file_mode(&tmp)?;
    fs::rename(&tmp, path).map_err(|e| StorageError::Io(e.to_string()))?;
    set_file_mode(path)?;
    Ok(())
}

pub fn set_dir_mode(path: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    {
        let perms = fs::Permissions::from_mode(DIR_MODE);
        fs::set_permissions(path, perms).map_err(|e| StorageError::Io(e.to_string()))?;
    }
    Ok(())
}

pub fn set_file_mode(path: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    {
        let perms = fs::Permissions::from_mode(FILE_MODE);
        fs::set_permissions(path, perms).map_err(|e| StorageError::Io(e.to_string()))?;
    }
    Ok(())
}

pub fn files_dir() -> PathBuf {
    base_dir().join("files")
}

pub fn create_file_secure(path: &Path) -> Result<File, StorageError> {
    if let Some(parent) = path.parent() {
        create_dir_secure_all(parent)?;
    }

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|e| StorageError::Io(e.to_string()))?;

    set_file_mode(path)?;
    Ok(file)
}

pub fn sanitize_peer_for_filename(peer_b32: &str) -> String {
    peer_b32
        .trim()
        .to_lowercase()
        .replace(".b32.i2p", "")
        .replace("/", "_")
}

pub fn offline_state_filename(peer_b32: &str) -> String {
    format!("offline_{}.state", sanitize_peer_for_filename(peer_b32))
}

pub fn offline_state_path(name: &str, peer_b32: &str) -> PathBuf {
    contact_dir(name).join(offline_state_filename(peer_b32))
}

pub fn load_offline_state(name: &str, peer_b32: &str) -> Result<OfflineState, StorageError> {
    validate_contact_name(name)?;

    let path = offline_state_path(name, peer_b32);
    if !path.exists() {
        return Ok(default_offline_state());
    }

    let mut f = File::open(&path).map_err(|e| StorageError::Io(e.to_string()))?;

    let mut s = String::new();
    f.read_to_string(&mut s)
        .map_err(|e| StorageError::Io(e.to_string()))?;

    let mut state = default_offline_state();

    for raw_line in s.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        let Some((k, v)) = line.split_once('=') else {
            continue;
        };

        let key = k.trim();
        let val = v.trim();

        match key {
            "offline_shared_secret" => {
                if let Ok(bytes) = hex::decode(val) {
                    if bytes.len() == 32 {
                        let mut arr = [0u8; 32];
                        arr.copy_from_slice(&bytes);
                        state.offline_shared_secret = arr;
                    }
                }
            }
            "drop_send_index" => {
                if let Ok(n) = val.parse::<u64>() {
                    state.drop_send_index = n;
                }
            }
            "drop_recv_base" => {
                if let Ok(n) = val.parse::<u64>() {
                    state.drop_recv_base = n;
                }
            }
            "drop_window" => {
                if let Ok(n) = val.parse::<u32>() {
                    state.drop_window = n;
                }
            }
            "consumed_drop_recv" => {
                if !val.is_empty() {
                    state.consumed_drop_recv = val
                        .split(',')
                        .filter_map(|x| x.trim().parse::<u64>().ok())
                        .collect();
                }
            }
            _ => {}
        }
    }

    Ok(state)
}

pub fn save_offline_state(
    name: &str,
    peer_b32: &str,
    state: &OfflineState,
) -> Result<(), StorageError> {
    validate_contact_name(name)?;

    let dir = contact_dir(name);
    create_dir_secure_all(&dir)?;

    let path = offline_state_path(name, peer_b32);

    let mut text = String::new();
    text.push_str(&format!(
        "offline_shared_secret={}\n",
        hex::encode(state.offline_shared_secret)
    ));
    text.push_str(&format!("drop_send_index={}\n", state.drop_send_index));
    text.push_str(&format!("drop_recv_base={}\n", state.drop_recv_base));
    text.push_str(&format!("drop_window={}\n", state.drop_window));
    text.push_str("consumed_drop_recv=");
    text.push_str(
        &state
            .consumed_drop_recv
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(","),
    );
    text.push('\n');

    atomic_write_text(&path, &text)
}

pub fn delete_offline_state(name: &str, peer_b32: &str) -> Result<(), StorageError> {
    validate_contact_name(name)?;

    let path = offline_state_path(name, peer_b32);
    if path.exists() {
        fs::remove_file(&path).map_err(|e| StorageError::Io(e.to_string()))?;
    }

    Ok(())
}
