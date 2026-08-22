use crate::storage;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

const HISTORY_FILENAME: &str = "history.jsonl";
const HISTORY_FORMAT_VERSION: u8 = 1;
const MAX_HISTORY_RECORDS: usize = 5_000;
const MAX_HISTORY_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_HISTORY_TEXT_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone)]
pub enum HistoryScope {
    Contact(String),
    Group(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryRecord {
    pub created_ms: u64,
    pub timestamp_utc: String,
    pub author: String,
    #[serde(default)]
    pub sender_b32: Option<String>,
    pub text: String,
    pub mine: bool,
    pub offline: bool,
    #[serde(default)]
    pub msg_id: Option<u64>,
    #[serde(default)]
    pub delivered: bool,
    #[serde(default)]
    pub group_expected_acks: Vec<String>,
    #[serde(default)]
    pub group_received_acks: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum HistoryEvent {
    Message {
        version: u8,
        record: HistoryRecord,
    },
    Delivery {
        version: u8,
        msg_id: u64,
        #[serde(default)]
        peer_b32: Option<String>,
    },
}

pub fn load(scope: &HistoryScope) -> Result<Vec<HistoryRecord>, String> {
    let path = history_path(scope);
    if !path.exists() {
        return Ok(Vec::new());
    }

    let file = fs::File::open(&path).map_err(|e| e.to_string())?;
    let mut records = VecDeque::new();
    let mut record_keys = HashSet::new();

    for line in BufReader::new(file).lines() {
        let Ok(line) = line else {
            continue;
        };
        if line.trim().is_empty() {
            continue;
        }

        let Ok(event) = serde_json::from_str::<HistoryEvent>(&line) else {
            continue;
        };

        match event {
            HistoryEvent::Message { version, record }
                if version == HISTORY_FORMAT_VERSION
                    && record.text.len() <= MAX_HISTORY_TEXT_BYTES =>
            {
                if let Some(key) = record_key(&record) {
                    if !record_keys.insert(key) {
                        continue;
                    }
                }

                records.push_back(record);
                while records.len() > MAX_HISTORY_RECORDS {
                    if let Some(removed) = records.pop_front() {
                        if let Some(key) = record_key(&removed) {
                            record_keys.remove(&key);
                        }
                    }
                }
            }
            HistoryEvent::Delivery {
                version,
                msg_id,
                peer_b32,
            } if version == HISTORY_FORMAT_VERSION => {
                apply_delivery(&mut records, msg_id, peer_b32.as_deref());
            }
            _ => {}
        }
    }

    Ok(records.into_iter().collect())
}

pub fn append_message(scope: &HistoryScope, record: &HistoryRecord) -> Result<(), String> {
    if record.text.len() > MAX_HISTORY_TEXT_BYTES {
        return Err("history text exceeds the storage limit".into());
    }

    append_event(
        scope,
        &HistoryEvent::Message {
            version: HISTORY_FORMAT_VERSION,
            record: record.clone(),
        },
    )
}

pub fn append_delivery(
    scope: &HistoryScope,
    msg_id: u64,
    peer_b32: Option<&str>,
) -> Result<(), String> {
    append_event(
        scope,
        &HistoryEvent::Delivery {
            version: HISTORY_FORMAT_VERSION,
            msg_id,
            peer_b32: peer_b32.map(str::to_string),
        },
    )
}

pub fn clear(scope: &HistoryScope) -> Result<(), String> {
    let path = history_path(scope);
    if path.exists() {
        fs::remove_file(path).map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn append_event(scope: &HistoryScope, event: &HistoryEvent) -> Result<(), String> {
    let path = history_path(scope);
    if let Some(parent) = path.parent() {
        storage::create_dir_secure_all(parent).map_err(|e| e.to_string())?;
    }

    let mut line = serde_json::to_string(event).map_err(|e| e.to_string())?;
    line.push('\n');

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| e.to_string())?;
    storage::set_file_mode(&path).map_err(|e| e.to_string())?;
    file.write_all(line.as_bytes()).map_err(|e| e.to_string())?;
    file.flush().map_err(|e| e.to_string())?;
    file.sync_data().map_err(|e| e.to_string())?;

    let compact_needed = file.metadata().map_err(|e| e.to_string())?.len() > MAX_HISTORY_FILE_BYTES;
    drop(file);
    if compact_needed {
        compact(scope)?;
    }

    Ok(())
}

fn compact(scope: &HistoryScope) -> Result<(), String> {
    let records = load(scope)?;
    let mut compacted = String::new();

    for record in records {
        let event = HistoryEvent::Message {
            version: HISTORY_FORMAT_VERSION,
            record,
        };
        compacted.push_str(&serde_json::to_string(&event).map_err(|e| e.to_string())?);
        compacted.push('\n');
    }

    storage::atomic_write_text(&history_path(scope), &compacted).map_err(|e| e.to_string())
}

fn apply_delivery(records: &mut VecDeque<HistoryRecord>, msg_id: u64, peer_b32: Option<&str>) {
    let Some(record) = records
        .iter_mut()
        .rev()
        .find(|record| record.mine && record.msg_id == Some(msg_id))
    else {
        return;
    };

    if let Some(peer_b32) = peer_b32 {
        if record
            .group_expected_acks
            .iter()
            .any(|expected| expected.eq_ignore_ascii_case(peer_b32))
            && !record
                .group_received_acks
                .iter()
                .any(|received| received.eq_ignore_ascii_case(peer_b32))
        {
            record
                .group_received_acks
                .push(peer_b32.to_ascii_lowercase());
        }
        record.delivered = !record.group_expected_acks.is_empty()
            && record.group_received_acks.len() >= record.group_expected_acks.len();
    } else {
        record.delivered = true;
    }
}

fn record_key(record: &HistoryRecord) -> Option<String> {
    let msg_id = record.msg_id?;
    Some(format!(
        "{}:{msg_id}:{}",
        if record.mine { "out" } else { "in" },
        record
            .sender_b32
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase()
    ))
}

fn history_path(scope: &HistoryScope) -> PathBuf {
    match scope {
        HistoryScope::Contact(name) => storage::contact_dir(name).join(HISTORY_FILENAME),
        HistoryScope::Group(key) => storage::group_dir(key).join(HISTORY_FILENAME),
    }
}
