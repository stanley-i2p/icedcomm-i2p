use crate::storage;
use argon2::Argon2;
use crypto_secretbox::{
    Key, Nonce, XSalsa20Poly1305,
    aead::{Aead, KeyInit},
};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{Cursor, Write};
use std::path::{Component, Path, PathBuf};
use tar::{Archive, Builder};

const BACKUP_MAGIC: &[u8] = b"TERMCHAT-I2P-BACKUP-V2\n";
const BACKUP_VERSION: u32 = 2;
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 24;

#[derive(Debug)]
pub enum BackupError {
    Io(String),
    Crypto(String),
    Format(String),
    Storage(String),
    Serde(String),
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackupError::Io(e) => write!(f, "io error: {e}"),
            BackupError::Crypto(e) => write!(f, "crypto error: {e}"),
            BackupError::Format(e) => write!(f, "format error: {e}"),
            BackupError::Storage(e) => write!(f, "storage error: {e}"),
            BackupError::Serde(e) => write!(f, "serde error: {e}"),
        }
    }
}

impl std::error::Error for BackupError {}

#[derive(Debug, Serialize, Deserialize)]
struct BackupEnvelope {
    format: String,
    version: u32,
    kdf: String,
    cipher: String,
    compression: String,
    salt_hex: String,
    nonce_hex: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BackupManifest {
    format: String,
    version: u32,
    #[serde(default = "default_backup_scope")]
    scope: String,
    created_utc: String,
    profiles: Vec<String>,
}

struct TempTree {
    path: PathBuf,
}

impl TempTree {
    fn new(prefix: &str) -> Result<Self, BackupError> {
        let mut path = std::env::temp_dir();
        let mut random = [0u8; 8];
        OsRng.fill_bytes(&mut random);
        path.push(format!("{prefix}_{}", hex::encode(random)));
        fs::create_dir_all(&path).map_err(|e| BackupError::Io(e.to_string()))?;
        Ok(Self { path })
    }
}

impl Drop for TempTree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub fn export_backup(
    path: &Path,
    passphrase: &str,
    include_files: bool,
) -> Result<(), BackupError> {
    validate_passphrase(passphrase)?;
    storage::ensure_base_layout().map_err(|e| BackupError::Storage(e.to_string()))?;

    let temp = TempTree::new("termchat_backup_export")?;
    let root = temp.path.join("termchat-backup-v2");
    build_v2_tree(&root, include_files)?;

    let archive = build_tar_gz(&root)?;
    let encrypted = encrypt_payload(&archive, passphrase)?;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|e| BackupError::Io(e.to_string()))?;
        }
    }

    let mut f = File::create(path).map_err(|e| BackupError::Io(e.to_string()))?;
    f.write_all(&encrypted)
        .map_err(|e| BackupError::Io(e.to_string()))?;
    f.flush().map_err(|e| BackupError::Io(e.to_string()))?;
    storage::set_file_mode(path).map_err(|e| BackupError::Storage(e.to_string()))?;
    Ok(())
}

pub fn export_profile_backup(
    path: &Path,
    passphrase: &str,
    profile_name: &str,
) -> Result<(), BackupError> {
    validate_passphrase(passphrase)?;
    storage::ensure_base_layout().map_err(|e| BackupError::Storage(e.to_string()))?;

    let temp = TempTree::new("termchat_profile_backup_export")?;
    let root = temp.path.join("termchat-backup-v2");
    build_profile_v2_tree(&root, profile_name)?;

    let archive = build_tar_gz(&root)?;
    let encrypted = encrypt_payload(&archive, passphrase)?;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|e| BackupError::Io(e.to_string()))?;
        }
    }

    let mut f = File::create(path).map_err(|e| BackupError::Io(e.to_string()))?;
    f.write_all(&encrypted)
        .map_err(|e| BackupError::Io(e.to_string()))?;
    f.flush().map_err(|e| BackupError::Io(e.to_string()))?;
    storage::set_file_mode(path).map_err(|e| BackupError::Storage(e.to_string()))?;
    Ok(())
}

pub fn import_backup(
    path: &Path,
    passphrase: &str,
    restore_files: bool,
) -> Result<(), BackupError> {
    validate_passphrase(passphrase)?;
    ensure_import_target_empty()?;

    import_backup_unchecked(path, passphrase, restore_files)
}

pub fn import_backup_replace(
    path: &Path,
    passphrase: &str,
    restore_files: bool,
) -> Result<(), BackupError> {
    validate_passphrase(passphrase)?;

    let temp = load_backup_tree(path, passphrase)?;
    let root = temp.path.join("termchat-backup-v2");
    let manifest = read_manifest(&root)?;
    require_full_manifest(&manifest)?;
    wipe_import_target()?;
    install_v2_tree(&root, restore_files)?;
    Ok(())
}

pub fn inspect_profile_backup(path: &Path, passphrase: &str) -> Result<String, BackupError> {
    validate_passphrase(passphrase)?;

    let temp = load_backup_tree(path, passphrase)?;
    let root = temp.path.join("termchat-backup-v2");
    let manifest = read_manifest(&root)?;
    require_profile_manifest(&manifest)?;
    Ok(manifest.profiles[0].clone())
}

pub fn import_profile_backup(
    path: &Path,
    passphrase: &str,
    replace: bool,
) -> Result<String, BackupError> {
    validate_passphrase(passphrase)?;

    let temp = load_backup_tree(path, passphrase)?;
    let root = temp.path.join("termchat-backup-v2");
    let manifest = read_manifest(&root)?;
    require_profile_manifest(&manifest)?;

    let profile_name = manifest.profiles[0].clone();
    let profile_src = root.join("profiles").join(&profile_name);
    let meta: storage::ContactMeta = read_json(&profile_src.join("contact.json"))?;

    if meta.name != profile_name {
        return Err(BackupError::Format(
            "profile backup metadata name does not match manifest".into(),
        ));
    }

    let profile_exists = storage::contact_dir(&profile_name).exists();
    if profile_exists && !replace {
        return Err(BackupError::Format("profile already exists".into()));
    }

    if replace && profile_exists {
        storage::delete_contact(&profile_name).map_err(|e| BackupError::Storage(e.to_string()))?;
    }

    install_profile_dir(&profile_src)?;
    Ok(profile_name)
}

pub fn has_import_conflicts() -> Result<bool, BackupError> {
    storage::ensure_base_layout().map_err(|e| BackupError::Storage(e.to_string()))?;

    let contacts = storage::load_contacts().map_err(|e| BackupError::Storage(e.to_string()))?;
    if !contacts.is_empty() {
        return Ok(true);
    }

    if crate::vault::vault_path(&storage::base_dir()).exists() {
        return Ok(true);
    }

    dir_has_entries_except(&storage::files_dir(), &[])
}

fn import_backup_unchecked(
    path: &Path,
    passphrase: &str,
    restore_files: bool,
) -> Result<(), BackupError> {
    let temp = load_backup_tree(path, passphrase)?;
    let root = temp.path.join("termchat-backup-v2");
    let manifest = read_manifest(&root)?;
    require_full_manifest(&manifest)?;
    install_v2_tree(&root, restore_files)?;
    Ok(())
}

fn load_backup_tree(path: &Path, passphrase: &str) -> Result<TempTree, BackupError> {
    let encrypted = fs::read(path).map_err(|e| BackupError::Io(e.to_string()))?;
    let archive = decrypt_payload(&encrypted, passphrase)?;

    let temp = TempTree::new("termchat_backup_import")?;
    extract_tar_gz(&archive, &temp.path)?;
    let root = temp.path.join("termchat-backup-v2");
    validate_v2_tree(&root)?;
    Ok(temp)
}

fn validate_passphrase(passphrase: &str) -> Result<(), BackupError> {
    if passphrase.trim().is_empty() {
        return Err(BackupError::Format("backup passphrase is empty".into()));
    }
    Ok(())
}

fn build_v2_tree(root: &Path, include_files: bool) -> Result<(), BackupError> {
    fs::create_dir_all(root.join("profiles")).map_err(|e| BackupError::Io(e.to_string()))?;
    fs::create_dir_all(root.join("files")).map_err(|e| BackupError::Io(e.to_string()))?;

    let contacts = storage::load_contacts().map_err(|e| BackupError::Storage(e.to_string()))?;
    let mut profiles = Vec::new();

    for contact in contacts {
        let profile_root = root.join("profiles").join(&contact.name);
        fs::create_dir_all(&profile_root).map_err(|e| BackupError::Io(e.to_string()))?;

        write_json(&profile_root.join("contact.json"), &contact)?;
        profiles.push(contact.name.clone());

        let source_dir = storage::contact_dir(&contact.name);
        copy_matching_files(&source_dir, &profile_root, "offline_", ".state")?;
        copy_if_exists(
            &source_dir.join("deaddrop_stats.json"),
            &profile_root.join("deaddrop_stats.json"),
        )?;
    }

    if include_files {
        copy_dir_contents(&storage::files_dir(), &root.join("files"))?;
    }

    let manifest = BackupManifest {
        format: "termchat-i2p-backup".into(),
        version: BACKUP_VERSION,
        scope: "full".into(),
        created_utc: now_utc_timestamp(),
        profiles,
    };
    write_json(&root.join("manifest.json"), &manifest)?;

    Ok(())
}

fn build_profile_v2_tree(root: &Path, profile_name: &str) -> Result<(), BackupError> {
    fs::create_dir_all(root.join("profiles")).map_err(|e| BackupError::Io(e.to_string()))?;
    fs::create_dir_all(root.join("files")).map_err(|e| BackupError::Io(e.to_string()))?;

    let contact = storage::load_contact_meta(profile_name)
        .map_err(|e| BackupError::Storage(e.to_string()))?;
    let profile_root = root.join("profiles").join(&contact.name);
    fs::create_dir_all(&profile_root).map_err(|e| BackupError::Io(e.to_string()))?;

    write_json(&profile_root.join("contact.json"), &contact)?;

    let source_dir = storage::contact_dir(&contact.name);
    copy_matching_files(&source_dir, &profile_root, "offline_", ".state")?;
    copy_if_exists(
        &source_dir.join("deaddrop_stats.json"),
        &profile_root.join("deaddrop_stats.json"),
    )?;

    let manifest = BackupManifest {
        format: "termchat-i2p-backup".into(),
        version: BACKUP_VERSION,
        scope: "profile".into(),
        created_utc: now_utc_timestamp(),
        profiles: vec![contact.name],
    };
    write_json(&root.join("manifest.json"), &manifest)?;

    Ok(())
}

fn validate_v2_tree(root: &Path) -> Result<(), BackupError> {
    let manifest = read_manifest(root)?;

    if manifest.format != "termchat-i2p-backup" || manifest.version != BACKUP_VERSION {
        return Err(BackupError::Format("unsupported backup manifest".into()));
    }

    if !root.join("profiles").is_dir() {
        return Err(BackupError::Format(
            "backup is missing profiles directory".into(),
        ));
    }

    if !root.join("files").is_dir() {
        return Err(BackupError::Format(
            "backup is missing files directory".into(),
        ));
    }

    Ok(())
}

fn read_manifest(root: &Path) -> Result<BackupManifest, BackupError> {
    read_json(&root.join("manifest.json"))
}

fn default_backup_scope() -> String {
    "full".into()
}

fn require_profile_manifest(manifest: &BackupManifest) -> Result<(), BackupError> {
    if manifest.scope != "profile" {
        return Err(BackupError::Format("backup is not a profile backup".into()));
    }
    if manifest.profiles.len() != 1 {
        return Err(BackupError::Format(
            "profile backup must contain exactly one profile".into(),
        ));
    }
    Ok(())
}

fn require_full_manifest(manifest: &BackupManifest) -> Result<(), BackupError> {
    if manifest.scope != "full" {
        return Err(BackupError::Format("backup is not a full backup".into()));
    }
    Ok(())
}

fn install_v2_tree(root: &Path, restore_files: bool) -> Result<(), BackupError> {
    storage::ensure_base_layout().map_err(|e| BackupError::Storage(e.to_string()))?;

    let profiles_src = root.join("profiles");
    for entry in fs::read_dir(&profiles_src).map_err(|e| BackupError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| BackupError::Io(e.to_string()))?;
        let src = entry.path();
        if !src.is_dir() {
            continue;
        }

        install_profile_dir(&src)?;
    }

    if restore_files {
        copy_dir_contents(&root.join("files"), &storage::files_dir())?;
    }
    Ok(())
}

fn install_profile_dir(src: &Path) -> Result<(), BackupError> {
    let meta: storage::ContactMeta = read_json(&src.join("contact.json"))?;
    storage::save_contact_meta(&meta).map_err(|e| BackupError::Storage(e.to_string()))?;

    let dst = storage::contact_dir(&meta.name);
    copy_matching_files(src, &dst, "offline_", ".state")?;
    copy_if_exists(
        &src.join("deaddrop_stats.json"),
        &dst.join("deaddrop_stats.json"),
    )?;
    Ok(())
}

fn ensure_import_target_empty() -> Result<(), BackupError> {
    storage::ensure_base_layout().map_err(|e| BackupError::Storage(e.to_string()))?;

    let contacts = storage::load_contacts().map_err(|e| BackupError::Storage(e.to_string()))?;
    if !contacts.is_empty() {
        return Err(BackupError::Format(
            "import requires an empty profile list".into(),
        ));
    }

    if dir_has_entries_except(&storage::files_dir(), &[])? {
        return Err(BackupError::Format(
            "import requires an empty files directory".into(),
        ));
    }

    Ok(())
}

fn wipe_import_target() -> Result<(), BackupError> {
    storage::ensure_base_layout().map_err(|e| BackupError::Storage(e.to_string()))?;

    clear_dir_contents(&storage::contacts_dir())?;
    clear_dir_contents(&storage::files_dir())?;
    let vault = crate::vault::vault_path(&storage::base_dir());
    if vault.exists() {
        fs::remove_file(&vault).map_err(|e| BackupError::Io(e.to_string()))?;
    }

    storage::create_dir_secure_all(&storage::contacts_dir())
        .map_err(|e| BackupError::Storage(e.to_string()))?;
    storage::create_dir_secure_all(&storage::files_dir())
        .map_err(|e| BackupError::Storage(e.to_string()))?;
    Ok(())
}

fn build_tar_gz(root: &Path) -> Result<Vec<u8>, BackupError> {
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    {
        let mut tar = Builder::new(&mut gz);
        tar.append_dir_all("termchat-backup-v2", root)
            .map_err(|e| BackupError::Io(e.to_string()))?;
        tar.finish().map_err(|e| BackupError::Io(e.to_string()))?;
    }
    gz.finish().map_err(|e| BackupError::Io(e.to_string()))
}

fn extract_tar_gz(data: &[u8], target: &Path) -> Result<(), BackupError> {
    let gz = GzDecoder::new(Cursor::new(data));
    let mut archive = Archive::new(gz);
    for entry in archive
        .entries()
        .map_err(|e| BackupError::Io(e.to_string()))?
    {
        let mut entry = entry.map_err(|e| BackupError::Io(e.to_string()))?;
        let path = entry
            .path()
            .map_err(|e| BackupError::Io(e.to_string()))?
            .into_owned();

        if !safe_relative_path(&path) {
            return Err(BackupError::Format("backup contains unsafe path".into()));
        }

        let out_path = target.join(&path);
        entry
            .unpack(&out_path)
            .map_err(|e| BackupError::Io(e.to_string()))?;
    }
    Ok(())
}

fn encrypt_payload(payload: &[u8], passphrase: &str) -> Result<Vec<u8>, BackupError> {
    let mut salt = [0u8; SALT_LEN];
    let mut nonce = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut salt);
    OsRng.fill_bytes(&mut nonce);

    let key = derive_key(passphrase, &salt)?;
    let cipher = XSalsa20Poly1305::new(Key::from_slice(&key));
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), payload)
        .map_err(|_| BackupError::Crypto("backup encryption failed".into()))?;

    let envelope = BackupEnvelope {
        format: "termchat-i2p-encrypted-backup".into(),
        version: BACKUP_VERSION,
        kdf: "argon2id".into(),
        cipher: "xsalsa20poly1305".into(),
        compression: "tar.gz".into(),
        salt_hex: hex::encode(salt),
        nonce_hex: hex::encode(nonce),
    };

    let header = serde_json::to_vec(&envelope).map_err(|e| BackupError::Serde(e.to_string()))?;
    let mut out = Vec::new();
    out.extend_from_slice(BACKUP_MAGIC);
    out.extend_from_slice(&(header.len() as u32).to_be_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

fn decrypt_payload(data: &[u8], passphrase: &str) -> Result<Vec<u8>, BackupError> {
    if !data.starts_with(BACKUP_MAGIC) {
        return Err(BackupError::Format("not a termchat backup v2 file".into()));
    }

    let offset = BACKUP_MAGIC.len();
    if data.len() < offset + 4 {
        return Err(BackupError::Format("backup header is truncated".into()));
    }

    let header_len = u32::from_be_bytes(
        data[offset..offset + 4]
            .try_into()
            .map_err(|_| BackupError::Format("invalid backup header length".into()))?,
    ) as usize;
    let header_start = offset + 4;
    let header_end = header_start + header_len;

    if data.len() < header_end {
        return Err(BackupError::Format("backup header is incomplete".into()));
    }

    let envelope: BackupEnvelope = serde_json::from_slice(&data[header_start..header_end])
        .map_err(|e| BackupError::Serde(format!("backup header decode failed: {e}")))?;

    if envelope.format != "termchat-i2p-encrypted-backup"
        || envelope.version != BACKUP_VERSION
        || envelope.kdf != "argon2id"
        || envelope.cipher != "xsalsa20poly1305"
        || envelope.compression != "tar.gz"
    {
        return Err(BackupError::Format("unsupported backup envelope".into()));
    }

    let salt = hex::decode(envelope.salt_hex)
        .map_err(|e| BackupError::Format(format!("invalid salt: {e}")))?;
    let nonce = hex::decode(envelope.nonce_hex)
        .map_err(|e| BackupError::Format(format!("invalid nonce: {e}")))?;

    if nonce.len() != NONCE_LEN {
        return Err(BackupError::Format("invalid nonce length".into()));
    }

    let key = derive_key(passphrase, &salt)?;
    let cipher = XSalsa20Poly1305::new(Key::from_slice(&key));
    cipher
        .decrypt(Nonce::from_slice(&nonce), &data[header_end..])
        .map_err(|_| BackupError::Crypto("wrong passphrase or corrupted backup".into()))
}

fn derive_key(passphrase: &str, salt: &[u8]) -> Result<[u8; 32], BackupError> {
    let mut key = [0u8; 32];
    Argon2::default()
        .hash_password_into(passphrase.as_bytes(), salt, &mut key)
        .map_err(|e| BackupError::Crypto(format!("key derivation failed: {e}")))?;
    Ok(key)
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), BackupError> {
    let text =
        serde_json::to_string_pretty(value).map_err(|e| BackupError::Serde(e.to_string()))?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| BackupError::Io(e.to_string()))?;
    }
    fs::write(path, text).map_err(|e| BackupError::Io(e.to_string()))
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, BackupError> {
    let text = fs::read_to_string(path).map_err(|e| BackupError::Io(e.to_string()))?;
    serde_json::from_str(&text).map_err(|e| BackupError::Serde(e.to_string()))
}

fn copy_if_exists(src: &Path, dst: &Path) -> Result<(), BackupError> {
    if !src.exists() {
        return Ok(());
    }

    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).map_err(|e| BackupError::Io(e.to_string()))?;
        let _ = storage::set_dir_mode(parent);
    }
    fs::copy(src, dst).map_err(|e| BackupError::Io(e.to_string()))?;
    let _ = storage::set_file_mode(dst);
    Ok(())
}

fn copy_matching_files(
    src_dir: &Path,
    dst_dir: &Path,
    prefix: &str,
    suffix: &str,
) -> Result<(), BackupError> {
    if !src_dir.is_dir() {
        return Ok(());
    }

    fs::create_dir_all(dst_dir).map_err(|e| BackupError::Io(e.to_string()))?;
    for entry in fs::read_dir(src_dir).map_err(|e| BackupError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| BackupError::Io(e.to_string()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };

        if name.starts_with(prefix) && name.ends_with(suffix) {
            copy_if_exists(&path, &dst_dir.join(name))?;
        }
    }
    Ok(())
}

fn copy_dir_contents(src: &Path, dst: &Path) -> Result<(), BackupError> {
    fs::create_dir_all(dst).map_err(|e| BackupError::Io(e.to_string()))?;
    let _ = storage::set_dir_mode(dst);
    if !src.is_dir() {
        return Ok(());
    }

    for entry in fs::read_dir(src).map_err(|e| BackupError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| BackupError::Io(e.to_string()))?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_contents(&src_path, &dst_path)?;
        } else if src_path.is_file() {
            copy_if_exists(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

fn clear_dir_contents(path: &Path) -> Result<(), BackupError> {
    if !path.is_dir() {
        return Ok(());
    }

    for entry in fs::read_dir(path).map_err(|e| BackupError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| BackupError::Io(e.to_string()))?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            fs::remove_dir_all(&entry_path).map_err(|e| BackupError::Io(e.to_string()))?;
        } else {
            fs::remove_file(&entry_path).map_err(|e| BackupError::Io(e.to_string()))?;
        }
    }

    Ok(())
}

fn dir_has_entries_except(path: &Path, allowed: &[&str]) -> Result<bool, BackupError> {
    if !path.is_dir() {
        return Ok(false);
    }

    for entry in fs::read_dir(path).map_err(|e| BackupError::Io(e.to_string()))? {
        let entry = entry.map_err(|e| BackupError::Io(e.to_string()))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !allowed.iter().any(|allowed_name| *allowed_name == name) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn safe_relative_path(path: &Path) -> bool {
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn now_utc_timestamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{secs}")
}
