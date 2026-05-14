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
use thiserror::Error;

const VAULT_MAGIC: &[u8] = b"TERMCHAT-I2P-VAULT-V1\n";
const VAULT_VERSION: u32 = 1;
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 24;

#[derive(Debug, Error)]
pub enum VaultError {
    #[error("io error: {0}")]
    Io(String),
    #[error("crypto error: {0}")]
    Crypto(String),
    #[error("format error: {0}")]
    Format(String),
    #[error("serde error: {0}")]
    Serde(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct VaultEnvelope {
    format: String,
    version: u32,
    kdf: String,
    cipher: String,
    compression: String,
    salt_hex: String,
    nonce_hex: String,
}

pub fn vault_path(base_dir: &Path) -> PathBuf {
    PathBuf::from(format!("{}.vault", base_dir.to_string_lossy()))
}

pub fn fs_encrypt(base_dir: &str, passphrase: &str) -> Result<(), VaultError> {
    validate_passphrase(passphrase)?;

    let base = Path::new(base_dir);
    if !base.is_dir() {
        return Err(VaultError::Format(format!(
            "base dir does not exist: {}",
            base.display()
        )));
    }
    fs::create_dir_all(base.join("profiles")).map_err(|e| VaultError::Io(e.to_string()))?;
    fs::create_dir_all(base.join("files")).map_err(|e| VaultError::Io(e.to_string()))?;

    let archive = build_tar_gz(base)?;
    let encrypted = encrypt_payload(&archive, passphrase)?;

    let out_path = vault_path(base);
    let tmp_path = PathBuf::from(format!("{}.vault.tmp", base.to_string_lossy()));
    {
        let mut f = File::create(&tmp_path).map_err(|e| VaultError::Io(e.to_string()))?;
        f.write_all(&encrypted)
            .map_err(|e| VaultError::Io(e.to_string()))?;
        f.flush().map_err(|e| VaultError::Io(e.to_string()))?;
    }
    set_file_mode(&tmp_path)?;
    fs::rename(&tmp_path, &out_path).map_err(|e| VaultError::Io(e.to_string()))?;
    set_file_mode(&out_path)?;

    remove_if_exists(base)?;
    Ok(())
}

pub fn fs_decrypt(base_dir: &str, passphrase: &str) -> Result<(), VaultError> {
    validate_passphrase(passphrase)?;

    let base = Path::new(base_dir);
    let path = vault_path(base);
    if !path.exists() {
        fs::create_dir_all(base.join("profiles")).map_err(|e| VaultError::Io(e.to_string()))?;
        fs::create_dir_all(base.join("files")).map_err(|e| VaultError::Io(e.to_string()))?;
        return Ok(());
    }

    if plaintext_has_data(base)? {
        return Err(VaultError::Format(
            "plaintext storage is present next to encrypted vault; close recovery is required"
                .into(),
        ));
    }

    let encrypted = fs::read(&path).map_err(|e| VaultError::Io(e.to_string()))?;
    let archive = decrypt_payload(&encrypted, passphrase)?;

    let parent = base
        .parent()
        .ok_or_else(|| VaultError::Format("base dir has no parent".into()))?;
    remove_if_exists(base)?;
    extract_tar_gz(&archive, parent)?;
    validate_vault_tree(base)?;
    Ok(())
}

pub fn fs_verify_passphrase(base_dir: &str, passphrase: &str) -> Result<bool, VaultError> {
    validate_passphrase(passphrase)?;

    let base = Path::new(base_dir);
    let path = vault_path(base);
    if !path.exists() {
        return Ok(true);
    }

    let encrypted = fs::read(&path).map_err(|e| VaultError::Io(e.to_string()))?;
    match decrypt_payload(&encrypted, passphrase) {
        Ok(_) => Ok(true),
        Err(VaultError::Crypto(_)) => Ok(false),
        Err(err) => Err(err),
    }
}

fn validate_passphrase(passphrase: &str) -> Result<(), VaultError> {
    if passphrase.trim().is_empty() {
        return Err(VaultError::Format("vault passphrase is empty".into()));
    }
    Ok(())
}

fn plaintext_has_data(base: &Path) -> Result<bool, VaultError> {
    dir_has_entries(base)
}

fn remove_if_exists(path: &Path) -> Result<(), VaultError> {
    if !path.exists() {
        return Ok(());
    }
    if path.is_dir() {
        fs::remove_dir_all(path).map_err(|e| VaultError::Io(e.to_string()))
    } else {
        fs::remove_file(path).map_err(|e| VaultError::Io(e.to_string()))
    }
}

fn validate_vault_tree(root: &Path) -> Result<(), VaultError> {
    if !root.join("profiles").is_dir() {
        return Err(VaultError::Format(
            "vault is missing profiles directory".into(),
        ));
    }
    if !root.join("files").is_dir() {
        return Err(VaultError::Format(
            "vault is missing files directory".into(),
        ));
    }
    Ok(())
}

fn build_tar_gz(root: &Path) -> Result<Vec<u8>, VaultError> {
    let arcname = root
        .file_name()
        .ok_or_else(|| VaultError::Format("base dir has no name".into()))?;
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    {
        let mut tar = Builder::new(&mut gz);
        tar.append_dir_all(arcname, root)
            .map_err(|e| VaultError::Io(e.to_string()))?;
        tar.finish().map_err(|e| VaultError::Io(e.to_string()))?;
    }
    gz.finish().map_err(|e| VaultError::Io(e.to_string()))
}

fn extract_tar_gz(data: &[u8], target: &Path) -> Result<(), VaultError> {
    let gz = GzDecoder::new(Cursor::new(data));
    let mut archive = Archive::new(gz);
    for entry in archive
        .entries()
        .map_err(|e| VaultError::Io(e.to_string()))?
    {
        let mut entry = entry.map_err(|e| VaultError::Io(e.to_string()))?;
        let path = entry
            .path()
            .map_err(|e| VaultError::Io(e.to_string()))?
            .into_owned();

        if !safe_relative_path(&path) {
            return Err(VaultError::Format("vault contains unsafe path".into()));
        }

        entry
            .unpack(target.join(&path))
            .map_err(|e| VaultError::Io(e.to_string()))?;
    }
    Ok(())
}

fn encrypt_payload(payload: &[u8], passphrase: &str) -> Result<Vec<u8>, VaultError> {
    let mut salt = [0u8; SALT_LEN];
    let mut nonce = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut salt);
    OsRng.fill_bytes(&mut nonce);

    let key = derive_key(passphrase, &salt)?;
    let cipher = XSalsa20Poly1305::new(Key::from_slice(&key));
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), payload)
        .map_err(|_| VaultError::Crypto("vault encryption failed".into()))?;

    let envelope = VaultEnvelope {
        format: "termchat-i2p-encrypted-vault".into(),
        version: VAULT_VERSION,
        kdf: "argon2id".into(),
        cipher: "xsalsa20poly1305".into(),
        compression: "tar.gz".into(),
        salt_hex: hex::encode(salt),
        nonce_hex: hex::encode(nonce),
    };

    let header = serde_json::to_vec(&envelope).map_err(|e| VaultError::Serde(e.to_string()))?;
    let mut out = Vec::new();
    out.extend_from_slice(VAULT_MAGIC);
    out.extend_from_slice(&(header.len() as u32).to_be_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

fn decrypt_payload(data: &[u8], passphrase: &str) -> Result<Vec<u8>, VaultError> {
    if !data.starts_with(VAULT_MAGIC) {
        return Err(VaultError::Format("not a termchat vault file".into()));
    }

    let offset = VAULT_MAGIC.len();
    if data.len() < offset + 4 {
        return Err(VaultError::Format("vault header is truncated".into()));
    }

    let header_len = u32::from_be_bytes(
        data[offset..offset + 4]
            .try_into()
            .map_err(|_| VaultError::Format("invalid vault header length".into()))?,
    ) as usize;
    let header_start = offset + 4;
    let header_end = header_start + header_len;
    if data.len() < header_end {
        return Err(VaultError::Format("vault header is incomplete".into()));
    }

    let envelope: VaultEnvelope = serde_json::from_slice(&data[header_start..header_end])
        .map_err(|e| VaultError::Serde(format!("vault header decode failed: {e}")))?;

    if envelope.format != "termchat-i2p-encrypted-vault"
        || envelope.version != VAULT_VERSION
        || envelope.kdf != "argon2id"
        || envelope.cipher != "xsalsa20poly1305"
        || envelope.compression != "tar.gz"
    {
        return Err(VaultError::Format("unsupported vault envelope".into()));
    }

    let salt = hex::decode(envelope.salt_hex)
        .map_err(|e| VaultError::Format(format!("invalid salt: {e}")))?;
    let nonce = hex::decode(envelope.nonce_hex)
        .map_err(|e| VaultError::Format(format!("invalid nonce: {e}")))?;
    if nonce.len() != NONCE_LEN {
        return Err(VaultError::Format("invalid nonce length".into()));
    }

    let key = derive_key(passphrase, &salt)?;
    let cipher = XSalsa20Poly1305::new(Key::from_slice(&key));
    cipher
        .decrypt(Nonce::from_slice(&nonce), &data[header_end..])
        .map_err(|_| VaultError::Crypto("wrong passphrase or corrupted vault".into()))
}

fn derive_key(passphrase: &str, salt: &[u8]) -> Result<[u8; 32], VaultError> {
    let mut key = [0u8; 32];
    Argon2::default()
        .hash_password_into(passphrase.as_bytes(), salt, &mut key)
        .map_err(|e| VaultError::Crypto(format!("key derivation failed: {e}")))?;
    Ok(key)
}

fn dir_has_entries(path: &Path) -> Result<bool, VaultError> {
    if !path.is_dir() {
        return Ok(false);
    }

    for entry in fs::read_dir(path).map_err(|e| VaultError::Io(e.to_string()))? {
        entry.map_err(|e| VaultError::Io(e.to_string()))?;
        return Ok(true);
    }
    Ok(false)
}

fn safe_relative_path(path: &Path) -> bool {
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn set_file_mode(path: &Path) -> Result<(), VaultError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o600);
        fs::set_permissions(path, perms).map_err(|e| VaultError::Io(e.to_string()))?;
    }
    Ok(())
}
