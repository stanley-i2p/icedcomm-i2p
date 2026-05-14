use crate::constants::{DEFAULT_SAM_HOST, DEFAULT_SAM_PORT};
use crate::protocol::Frame;
use base64::{Engine as _, engine::general_purpose};
use data_encoding::BASE32_NOPAD;
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct SamClient {
    pub sam_host: String,
    pub sam_port: u16,
    pub session_id: Option<String>,
    ctrl: Option<Arc<Mutex<SamControl>>>,
}

#[derive(Debug)]
struct SamControl {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

#[derive(Debug, Clone)]
pub struct LiveConnection {
    writer: Arc<Mutex<tokio::net::tcp::OwnedWriteHalf>>,
    incoming: Arc<StdMutex<VecDeque<Frame>>>,
    closed: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Debug, Clone)]
pub struct SamInitResult {
    pub session_id: String,
    pub my_dest_b64: String,
    pub my_pub_dest_b64: String,
    pub my_b32: String,
}

#[derive(Debug, Clone)]
pub struct AcceptedIncoming {
    pub peer_dest_b64: String,
    pub peer_b32: String,
    pub conn: LiveConnection,
}

#[derive(Debug, Error)]
pub enum SamError {
    #[error("io error: {0}")]
    Io(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("base64 decode failed: {0}")]
    Base64(String),

    #[error("session is not initialized")]
    SessionNotInitialized,
}

impl Default for SamClient {
    fn default() -> Self {
        Self {
            sam_host: DEFAULT_SAM_HOST.to_string(),
            sam_port: DEFAULT_SAM_PORT,
            session_id: None,
            ctrl: None,
        }
    }
}

impl SamClient {
    pub fn new(sam_host: String, sam_port: u16) -> Self {
        Self {
            sam_host,
            sam_port,
            session_id: None,
            ctrl: None,
        }
    }

    pub async fn test_endpoint(sam_host: String, sam_port: u16) -> Result<String, SamError> {
        let stream = TcpStream::connect((sam_host.as_str(), sam_port))
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        write_half
            .write_all(b"HELLO VERSION MIN=3.0 MAX=3.2\n")
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let hello = read_line(&mut reader).await?;
        if !hello.contains("RESULT=OK") {
            return Err(SamError::Protocol(format!("HELLO failed: {hello}")));
        }

        Ok(hello)
    }

    pub async fn initialize_transient(
        &mut self,
        session_id: String,
    ) -> Result<SamInitResult, SamError> {
        let stream = TcpStream::connect((self.sam_host.as_str(), self.sam_port))
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let (read_half, write_half) = stream.into_split();

        let mut ctrl = SamControl {
            reader: BufReader::new(read_half),
            writer: write_half,
        };

        self.hello(&mut ctrl).await?;

        ctrl.writer
            .write_all(b"DEST GENERATE SIGNATURE_TYPE=7\n")
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let dest_resp = read_line(&mut ctrl.reader).await?;
        let my_dest_b64 =
            extract_field(&dest_resp, "PRIV").ok_or(SamError::MissingField("PRIV"))?;

        self.finish_session_create(ctrl, session_id, my_dest_b64)
            .await
    }

    pub async fn initialize_persistent(
        &mut self,
        session_id: String,
        my_dest_b64: String,
    ) -> Result<SamInitResult, SamError> {
        let stream = TcpStream::connect((self.sam_host.as_str(), self.sam_port))
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let (read_half, write_half) = stream.into_split();

        let mut ctrl = SamControl {
            reader: BufReader::new(read_half),
            writer: write_half,
        };

        self.hello(&mut ctrl).await?;
        self.finish_session_create(ctrl, session_id, my_dest_b64)
            .await
    }

    async fn hello(&self, ctrl: &mut SamControl) -> Result<(), SamError> {
        ctrl.writer
            .write_all(b"HELLO VERSION MIN=3.0 MAX=3.2\n")
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let hello = read_line(&mut ctrl.reader).await?;
        if !hello.contains("RESULT=OK") {
            return Err(SamError::Protocol(format!("HELLO failed: {hello}")));
        }

        Ok(())
    }

    async fn finish_session_create(
        &mut self,
        mut ctrl: SamControl,
        session_id: String,
        my_dest_b64: String,
    ) -> Result<SamInitResult, SamError> {
        let session_cmd = format!(
            "SESSION CREATE STYLE=STREAM ID={} DESTINATION={} SIGNATURE_TYPE=7 OPTION inbound.length=2 outbound.length=2 inbound.quantity=3 outbound.quantity=3\n",
            session_id, my_dest_b64
        );

        ctrl.writer
            .write_all(session_cmd.as_bytes())
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let session_resp = read_line(&mut ctrl.reader).await?;
        if !session_resp.contains("RESULT=OK") {
            return Err(SamError::Protocol(format!(
                "SESSION CREATE failed: {session_resp}"
            )));
        }

        ctrl.writer
            .write_all(b"NAMING LOOKUP NAME=ME\n")
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let lookup_resp = read_line(&mut ctrl.reader).await?;
        let result =
            extract_field(&lookup_resp, "RESULT").ok_or(SamError::MissingField("RESULT"))?;

        if result != "OK" {
            return Err(SamError::Protocol(format!(
                "NAMING LOOKUP failed: {lookup_resp}"
            )));
        }

        let my_pub_dest_b64 =
            extract_field(&lookup_resp, "VALUE").ok_or(SamError::MissingField("VALUE"))?;

        let my_b32 = Self::destination_to_b32(&my_pub_dest_b64)?;

        self.session_id = Some(session_id.clone());
        self.ctrl = Some(Arc::new(Mutex::new(ctrl)));

        Ok(SamInitResult {
            session_id,
            my_dest_b64,
            my_pub_dest_b64,
            my_b32,
        })
    }

    pub async fn stream_connect(&self, destination_b32: &str) -> Result<LiveConnection, SamError> {
        let session_id = self
            .session_id
            .as_ref()
            .ok_or(SamError::SessionNotInitialized)?
            .clone();

        let stream = TcpStream::connect((self.sam_host.as_str(), self.sam_port))
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        write_half
            .write_all(b"HELLO VERSION MIN=3.0 MAX=3.2\n")
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let hello = read_line(&mut reader).await?;
        if !hello.contains("RESULT=OK") {
            return Err(SamError::Protocol(format!("HELLO failed: {hello}")));
        }

        let connect_cmd = format!(
            "STREAM CONNECT ID={} DESTINATION={}\n",
            session_id, destination_b32
        );

        write_half
            .write_all(connect_cmd.as_bytes())
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let connect_resp = read_line(&mut reader).await?;
        if !connect_resp.contains("RESULT=OK") {
            return Err(SamError::Protocol(format!(
                "STREAM CONNECT failed: {connect_resp}"
            )));
        }

        let incoming = Arc::new(StdMutex::new(VecDeque::new()));
        let incoming_bg = incoming.clone();
        let closed = Arc::new(AtomicBool::new(false));
        let closed_bg = closed.clone();

        tokio::spawn(async move {
            let mut reader = reader;

            loop {
                match Frame::read_from(&mut reader).await {
                    Ok(frame) => {
                        if let Ok(mut q) = incoming_bg.lock() {
                            q.push_back(frame);
                        } else {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            closed_bg.store(true, Ordering::SeqCst);
        });

        Ok(LiveConnection {
            writer: Arc::new(Mutex::new(write_half)),
            incoming,
            closed,
        })
    }

    pub async fn stream_accept(&self) -> Result<AcceptedIncoming, SamError> {
        let session_id = self
            .session_id
            .as_ref()
            .ok_or(SamError::SessionNotInitialized)?
            .clone();

        let stream = TcpStream::connect((self.sam_host.as_str(), self.sam_port))
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        write_half
            .write_all(b"HELLO VERSION MIN=3.0 MAX=3.2\n")
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let hello = read_line(&mut reader).await?;
        if !hello.contains("RESULT=OK") {
            return Err(SamError::Protocol(format!("HELLO failed: {hello}")));
        }

        let accept_cmd = format!("STREAM ACCEPT ID={}\n", session_id);

        write_half
            .write_all(accept_cmd.as_bytes())
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let accept_resp = read_line(&mut reader).await?;
        if !accept_resp.contains("RESULT=OK") {
            return Err(SamError::Protocol(format!(
                "STREAM ACCEPT failed: {accept_resp}"
            )));
        }

        let peer_dest_b64 = read_line(&mut reader).await?;
        let peer_b32 = Self::destination_to_b32(&peer_dest_b64)?;

        let incoming = Arc::new(StdMutex::new(VecDeque::new()));

        let incoming_bg = incoming.clone();
        let closed = Arc::new(AtomicBool::new(false));
        let closed_bg = closed.clone();

        tokio::spawn(async move {
            let mut reader = reader;

            loop {
                match Frame::read_from(&mut reader).await {
                    Ok(frame) => {
                        if let Ok(mut q) = incoming_bg.lock() {
                            q.push_back(frame);
                        } else {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            closed_bg.store(true, Ordering::SeqCst);
        });

        Ok(AcceptedIncoming {
            peer_dest_b64,
            peer_b32,
            conn: LiveConnection {
                writer: Arc::new(Mutex::new(write_half)),
                incoming,
                closed,
            },
        })
    }

    pub async fn close(&mut self) -> Result<(), SamError> {
        if let Some(ctrl) = self.ctrl.take() {
            let mut ctrl = ctrl.lock().await;
            ctrl.writer
                .shutdown()
                .await
                .map_err(|e| SamError::Io(e.to_string()))?;
        }

        self.session_id = None;
        Ok(())
    }

    pub fn destination_to_b32(dest_b64: &str) -> Result<String, SamError> {
        let std_b64 = dest_b64.replace('-', "+").replace('~', "/");
        let raw = general_purpose::STANDARD
            .decode(std_b64.as_bytes())
            .map_err(|e| SamError::Base64(e.to_string()))?;

        let digest = Sha256::digest(raw);
        let b32 = BASE32_NOPAD.encode(&digest).to_lowercase();

        Ok(format!("{b32}.b32.i2p"))
    }
}

impl LiveConnection {
    pub async fn close(&self) -> Result<(), SamError> {
        let mut writer = self.writer.lock().await;
        let _ = writer.flush().await;
        writer
            .shutdown()
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub async fn send_raw_line(&self, line: &str) -> Result<(), SamError> {
        let mut writer = self.writer.lock().await;
        writer
            .write_all(line.as_bytes())
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;
        writer
            .flush()
            .await
            .map_err(|e| SamError::Io(e.to_string()))
    }

    pub async fn send_frame(&self, frame: &Frame) -> Result<(), SamError> {
        let mut writer = self.writer.lock().await;
        frame
            .write_to(&mut *writer)
            .await
            .map_err(|e| SamError::Protocol(e.to_string()))?;
        writer
            .flush()
            .await
            .map_err(|e| SamError::Io(e.to_string()))
    }

    pub fn try_recv_frame(&self) -> Option<Frame> {
        self.incoming.lock().ok()?.pop_front()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn is_dead(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn has_pending_frames(&self) -> bool {
        self.incoming.lock().map(|q| !q.is_empty()).unwrap_or(false)
    }
}

async fn read_line<R>(reader: &mut BufReader<R>) -> Result<String, SamError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut line = String::new();
    let n = reader
        .read_line(&mut line)
        .await
        .map_err(|e| SamError::Io(e.to_string()))?;

    if n == 0 {
        return Err(SamError::Protocol("unexpected EOF".into()));
    }

    Ok(line.trim().to_string())
}

fn extract_field(line: &str, key: &str) -> Option<String> {
    for part in line.split_whitespace() {
        if let Some(rest) = part.strip_prefix(&format!("{key}=")) {
            return Some(rest.to_string());
        }
    }
    None
}
