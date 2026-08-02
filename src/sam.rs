use crate::constants::{DEFAULT_SAM_HOST, DEFAULT_SAM_PORT};
use crate::protocol::Frame;
use base64::{Engine as _, engine::general_purpose};
use data_encoding::BASE32_NOPAD;
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const SAM_CLIENT_LIFECYCLE_DEBUG: bool = true;
const LIVE_CONNECTION_READER_JOIN_TIMEOUT_MS: u64 = 250;
const CANCELLED_STREAM_CONNECT_RESPONSE_GRACE_MS: u64 = 4_000;

fn sam_client_lifecycle_log(line: impl AsRef<str>) {
    if !SAM_CLIENT_LIFECYCLE_DEBUG {
        return;
    }

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let day = secs % 86_400;
    let h = day / 3_600;
    let m = (day % 3_600) / 60;
    let s = day % 60;

    eprintln!("[{:02}:{:02}:{:02} UTC][SAM-CLIENT] {}", h, m, s, line.as_ref());
}

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
    reader_task: Arc<StdMutex<Option<JoinHandle<()>>>>,
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

    pub async fn stream_connect_cancelled(
        &self,
        destination_b32: &str,
        cancelled: Arc<AtomicBool>,
    ) -> Result<LiveConnection, SamError> {
        let session_id = self
            .session_id
            .as_ref()
            .ok_or(SamError::SessionNotInitialized)?
            .clone();

        sam_client_lifecycle_log(format!(
            "stream connect start session={session_id} destination={destination_b32}"
        ));

        let stream = TcpStream::connect((self.sam_host.as_str(), self.sam_port))
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        write_half
            .write_all(b"HELLO VERSION MIN=3.0 MAX=3.2\n")
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let hello = match read_line_cancelled(&mut reader, cancelled.clone()).await {
            Ok(line) => line,
            Err(err) => {
                sam_client_lifecycle_log(format!(
                    "stream connect hello cancelled/error session={session_id} destination={destination_b32} err={err}"
                ));
                let _ = write_half.shutdown().await;
                return Err(err);
            }
        };
        if !hello.contains("RESULT=OK") {
            sam_client_lifecycle_log(format!(
                "stream connect hello failed session={session_id} destination={destination_b32} response={hello}"
            ));
            let _ = write_half.shutdown().await;
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

        sam_client_lifecycle_log(format!(
            "stream connect command sent session={session_id} destination={destination_b32}"
        ));

        let connect_resp = match read_line_cancelled(&mut reader, cancelled.clone()).await {
            Ok(line) => line,
            Err(err) => {
                if cancelled.load(Ordering::SeqCst) {
                    sam_client_lifecycle_log(format!(
                        "stream connect cancelled after command; draining response session={session_id} destination={destination_b32}"
                    ));

                    match timeout(
                        Duration::from_millis(CANCELLED_STREAM_CONNECT_RESPONSE_GRACE_MS),
                        read_line(&mut reader),
                    )
                    .await
                    {
                        Ok(Ok(line)) => {
                            sam_client_lifecycle_log(format!(
                                "stream connect cancelled drained response session={session_id} destination={destination_b32} response={line}"
                            ));
                            if line.contains("RESULT=OK") {
                                let conn = LiveConnection::new(reader, write_half);
                                let _ = conn.close().await;
                                return Err(SamError::Protocol("stream connect cancelled".into()));
                            }
                        }
                        Ok(Err(drain_err)) => {
                            sam_client_lifecycle_log(format!(
                                "stream connect cancelled drain error session={session_id} destination={destination_b32} err={drain_err}"
                            ));
                        }
                        Err(_) => {
                            sam_client_lifecycle_log(format!(
                                "stream connect cancelled drain timeout session={session_id} destination={destination_b32}"
                            ));
                        }
                    }
                }

                sam_client_lifecycle_log(format!(
                    "stream connect cancelled/error session={session_id} destination={destination_b32} err={err}"
                ));
                let _ = write_half.shutdown().await;
                return Err(err);
            }
        };
        if !connect_resp.contains("RESULT=OK") {
            sam_client_lifecycle_log(format!(
                "stream connect failed session={session_id} destination={destination_b32} response={connect_resp}"
            ));
            let _ = write_half.shutdown().await;
            return Err(SamError::Protocol(format!(
                "STREAM CONNECT failed: {connect_resp}"
            )));
        }

        sam_client_lifecycle_log(format!(
            "stream connect ok session={session_id} destination={destination_b32}"
        ));

        Ok(LiveConnection::new(reader, write_half))
    }

    pub async fn stream_accept_cancelled(
        &self,
        cancelled: Arc<AtomicBool>,
    ) -> Result<AcceptedIncoming, SamError> {
        let session_id = self
            .session_id
            .as_ref()
            .ok_or(SamError::SessionNotInitialized)?
            .clone();

        sam_client_lifecycle_log(format!("stream accept start session={session_id}"));

        let stream = TcpStream::connect((self.sam_host.as_str(), self.sam_port))
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        write_half
            .write_all(b"HELLO VERSION MIN=3.0 MAX=3.2\n")
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        let hello = match read_line_cancelled(&mut reader, cancelled.clone()).await {
            Ok(line) => line,
            Err(err) => {
                sam_client_lifecycle_log(format!(
                    "stream accept hello cancelled/error session={session_id} err={err}"
                ));
                let _ = write_half.shutdown().await;
                return Err(err);
            }
        };
        if !hello.contains("RESULT=OK") {
            sam_client_lifecycle_log(format!(
                "stream accept hello failed session={session_id} response={hello}"
            ));
            let _ = write_half.shutdown().await;
            return Err(SamError::Protocol(format!("HELLO failed: {hello}")));
        }

        let accept_cmd = format!("STREAM ACCEPT ID={}\n", session_id);

        write_half
            .write_all(accept_cmd.as_bytes())
            .await
            .map_err(|e| SamError::Io(e.to_string()))?;

        sam_client_lifecycle_log(format!("stream accept command sent session={session_id}"));

        let accept_resp = match read_line_cancelled(&mut reader, cancelled.clone()).await {
            Ok(line) => line,
            Err(err) => {
                sam_client_lifecycle_log(format!(
                    "stream accept cancelled/error session={session_id} err={err}"
                ));
                let _ = write_half.shutdown().await;
                return Err(err);
            }
        };
        if !accept_resp.contains("RESULT=OK") {
            sam_client_lifecycle_log(format!(
                "stream accept failed session={session_id} response={accept_resp}"
            ));
            let _ = write_half.shutdown().await;
            return Err(SamError::Protocol(format!(
                "STREAM ACCEPT failed: {accept_resp}"
            )));
        }

        let peer_dest_b64 = match read_line_cancelled(&mut reader, cancelled).await {
            Ok(line) => line,
            Err(err) => {
                sam_client_lifecycle_log(format!(
                    "stream accept peer read cancelled/error session={session_id} err={err}"
                ));
                let _ = write_half.shutdown().await;
                return Err(err);
            }
        };
        let peer_b32 = Self::destination_to_b32(&peer_dest_b64)?;

        sam_client_lifecycle_log(format!(
            "stream accept ok session={session_id} peer={peer_b32}"
        ));

        Ok(AcceptedIncoming {
            peer_dest_b64,
            peer_b32,
            conn: LiveConnection::new(reader, write_half),
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
    fn new(
        reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
        write_half: tokio::net::tcp::OwnedWriteHalf,
    ) -> Self {
        let incoming = Arc::new(StdMutex::new(VecDeque::new()));
        let incoming_bg = incoming.clone();
        let closed = Arc::new(AtomicBool::new(false));
        let closed_bg = closed.clone();

        let reader_task = tokio::spawn(async move {
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

        Self {
            writer: Arc::new(Mutex::new(write_half)),
            incoming,
            closed,
            reader_task: Arc::new(StdMutex::new(Some(reader_task))),
        }
    }

    pub async fn close(&self) -> Result<(), SamError> {
        let shutdown_result = {
            let mut writer = self.writer.lock().await;
            let _ = writer.flush().await;
            writer
                .shutdown()
                .await
                .map_err(|e| SamError::Io(e.to_string()))
        };

        let reader_task = self.reader_task.lock().ok().and_then(|mut task| task.take());

        if let Some(reader_task) = reader_task {
            reader_task.abort();
            let _ = timeout(
                Duration::from_millis(LIVE_CONNECTION_READER_JOIN_TIMEOUT_MS),
                reader_task,
            )
            .await;
        }

        self.closed.store(true, Ordering::SeqCst);
        shutdown_result
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

async fn read_line_cancelled<R>(
    reader: &mut BufReader<R>,
    cancelled: Arc<AtomicBool>,
) -> Result<String, SamError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut bytes = Vec::new();

    loop {
        if cancelled.load(Ordering::SeqCst) {
            return Err(SamError::Protocol("accept cancelled".into()));
        }

        match tokio::time::timeout(std::time::Duration::from_millis(50), reader.fill_buf()).await {
            Ok(Ok(buf)) => {
                if buf.is_empty() {
                    return Err(SamError::Protocol("unexpected EOF".into()));
                }

                if let Some(pos) = buf.iter().position(|byte| *byte == b'\n') {
                    bytes.extend_from_slice(&buf[..pos]);
                    reader.consume(pos + 1);
                    break;
                }

                let len = buf.len();
                bytes.extend_from_slice(buf);
                reader.consume(len);
            }
            Ok(Err(err)) => return Err(SamError::Io(err.to_string())),
            Err(_) => {}
        }
    }

    let line = String::from_utf8(bytes).map_err(|err| SamError::Protocol(err.to_string()))?;
    Ok(line.trim().to_string())
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
