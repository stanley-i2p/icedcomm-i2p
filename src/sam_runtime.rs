use crate::sam::{LiveConnection, SamClient};
use iced::{Task, task::Handle};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const SAM_RUNTIME_LIFECYCLE_DEBUG: bool = false;

fn runtime_lifecycle_log(line: impl AsRef<str>) {
    if !SAM_RUNTIME_LIFECYCLE_DEBUG {
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

    eprintln!("[{:02}:{:02}:{:02} UTC][SAM-RUNTIME] {}", h, m, s, line.as_ref());
}

#[derive(Debug, Clone)]
pub struct SamRuntime {
    pub client: SamClient,
    closing_client: Option<SamClient>,
    accept_cancelled: Arc<AtomicBool>,
    connect_cancelled: Arc<AtomicBool>,
    lookup_cancelled: Arc<AtomicBool>,
    accept_tokens: Arc<StdMutex<Vec<Weak<AtomicBool>>>>,
    connect_tokens: Arc<StdMutex<Vec<Weak<AtomicBool>>>>,
    lookup_tokens: Arc<StdMutex<Vec<Weak<AtomicBool>>>>,
    accept_task_handles: Arc<StdMutex<Vec<Handle>>>,
    connect_task_handles: Arc<StdMutex<Vec<Handle>>>,
    lookup_task_handles: Arc<StdMutex<Vec<Handle>>>,
    send_task_handles: Arc<StdMutex<Vec<Handle>>>,
    streams: Arc<StdMutex<Vec<LiveConnection>>>,
    closing: bool,
}

impl SamRuntime {
    pub fn new(sam_host: String, sam_port: u16) -> Self {
        Self {
            client: SamClient::new(sam_host, sam_port),
            closing_client: None,
            accept_cancelled: Arc::new(AtomicBool::new(false)),
            connect_cancelled: Arc::new(AtomicBool::new(false)),
            lookup_cancelled: Arc::new(AtomicBool::new(false)),
            accept_tokens: Arc::new(StdMutex::new(Vec::new())),
            connect_tokens: Arc::new(StdMutex::new(Vec::new())),
            lookup_tokens: Arc::new(StdMutex::new(Vec::new())),
            accept_task_handles: Arc::new(StdMutex::new(Vec::new())),
            connect_task_handles: Arc::new(StdMutex::new(Vec::new())),
            lookup_task_handles: Arc::new(StdMutex::new(Vec::new())),
            send_task_handles: Arc::new(StdMutex::new(Vec::new())),
            streams: Arc::new(StdMutex::new(Vec::new())),
            closing: false,
        }
    }

    pub fn is_closing(&self) -> bool {
        self.closing
    }

    pub fn begin_closing(&mut self) {
        runtime_lifecycle_log("begin closing");
        self.closing = true;
        self.cancel_sends();
        self.cancel_accept();
        self.cancel_connect();
        self.cancel_lookup();
    }

    pub fn shutdown_parts(&mut self) -> (Vec<LiveConnection>, SamClient) {
        self.begin_closing();
        let streams = self.registered_streams();

        if self.closing_client.is_none() {
            self.closing_client = Some(self.client.clone());
            let replacement = SamClient::new(self.client.sam_host.clone(), self.client.sam_port);
            let _ = std::mem::replace(&mut self.client, replacement);
        }

        let client = self
            .closing_client
            .clone()
            .unwrap_or_else(|| SamClient::new(self.client.sam_host.clone(), self.client.sam_port));

        (streams, client)
    }

    pub fn cancel_accept(&self) {
        self.accept_cancelled.store(true, Ordering::SeqCst);
        let mut cancelled_count = 0usize;
        if let Ok(mut tokens) = self.accept_tokens.lock() {
            tokens.retain(|token| {
                if let Some(token) = token.upgrade() {
                    token.store(true, Ordering::SeqCst);
                    cancelled_count += 1;
                    true
                } else {
                    false
                }
            });
        }
        if let Ok(mut handles) = self.accept_task_handles.lock() {
            runtime_lifecycle_log(format!(
                "accept cancel tokens={cancelled_count} released_handles={}",
                handles.len()
            ));
            handles.clear();
        }
    }

    pub fn cancel_connect(&self) {
        self.connect_cancelled.store(true, Ordering::SeqCst);
        let mut cancelled_count = 0usize;
        if let Ok(mut tokens) = self.connect_tokens.lock() {
            tokens.retain(|token| {
                if let Some(token) = token.upgrade() {
                    token.store(true, Ordering::SeqCst);
                    cancelled_count += 1;
                    true
                } else {
                    false
                }
            });
        }
        if let Ok(mut handles) = self.connect_task_handles.lock() {
            runtime_lifecycle_log(format!(
                "connect cancel tokens={cancelled_count} released_handles={}",
                handles.len()
            ));
            handles.clear();
        }
    }

    pub fn cancel_sends(&self) {
        if let Ok(mut handles) = self.send_task_handles.lock() {
            for handle in handles.iter() {
                handle.abort();
            }
            runtime_lifecycle_log(format!("send cancel handles={}", handles.len()));
            handles.clear();
        }
    }

    pub fn cancel_lookup(&self) {
        self.lookup_cancelled.store(true, Ordering::SeqCst);
        let mut cancelled_count = 0usize;
        if let Ok(mut tokens) = self.lookup_tokens.lock() {
            tokens.retain(|token| {
                if let Some(token) = token.upgrade() {
                    token.store(true, Ordering::SeqCst);
                    cancelled_count += 1;
                    true
                } else {
                    false
                }
            });
        }
        if let Ok(mut handles) = self.lookup_task_handles.lock() {
            runtime_lifecycle_log(format!(
                "lookup cancel tokens={cancelled_count} released_handles={}",
                handles.len()
            ));
            handles.clear();
        }
    }

    pub fn accept_parts(&self) -> Option<(SamClient, Arc<AtomicBool>)> {
        if self.closing {
            return None;
        }
        self.accept_cancelled.store(false, Ordering::SeqCst);
        let token = Arc::new(AtomicBool::new(false));
        if let Ok(mut tokens) = self.accept_tokens.lock() {
            tokens.push(Arc::downgrade(&token));
        }
        Some((self.client.clone(), token))
    }

    pub fn connect_parts(&self) -> Option<(SamClient, Arc<AtomicBool>)> {
        if self.closing {
            return None;
        }
        self.connect_cancelled.store(false, Ordering::SeqCst);
        let token = Arc::new(AtomicBool::new(false));
        if let Ok(mut tokens) = self.connect_tokens.lock() {
            tokens.push(Arc::downgrade(&token));
        }
        Some((self.client.clone(), token))
    }

    pub fn lookup_parts(&self) -> Option<(SamClient, Arc<AtomicBool>)> {
        if self.closing {
            return None;
        }
        self.lookup_cancelled.store(false, Ordering::SeqCst);
        let token = Arc::new(AtomicBool::new(false));
        if let Ok(mut tokens) = self.lookup_tokens.lock() {
            tokens.push(Arc::downgrade(&token));
        }
        Some((self.client.clone(), token))
    }

    pub fn accept_cancelled(&self) -> bool {
        self.accept_cancelled.load(Ordering::SeqCst)
    }

    pub fn track_accept_task<T: 'static>(&self, task: Task<T>) -> Task<T> {
        let (task, handle) = task.abortable();
        if let Ok(mut handles) = self.accept_task_handles.lock() {
            handles.push(handle);
        }
        task
    }

    pub fn track_connect_task<T: 'static>(&self, task: Task<T>) -> Task<T> {
        let (task, handle) = task.abortable();
        if let Ok(mut handles) = self.connect_task_handles.lock() {
            handles.push(handle);
        }
        task
    }

    pub fn track_lookup_task<T: 'static>(&self, task: Task<T>) -> Task<T> {
        let (task, handle) = task.abortable();
        if let Ok(mut handles) = self.lookup_task_handles.lock() {
            handles.push(handle);
        }
        task
    }

    pub fn track_send_task<T: 'static>(&self, task: Task<T>) -> Task<T> {
        let (task, handle) = task.abortable();
        if let Ok(mut handles) = self.send_task_handles.lock() {
            handles.push(handle);
        }
        task
    }

    pub fn register_stream(&self, conn: &LiveConnection) {
        if let Ok(mut streams) = self.streams.lock() {
            streams.push(conn.clone());
        }
    }

    pub fn registered_streams(&self) -> Vec<LiveConnection> {
        self.streams.lock().map(|streams| streams.clone()).unwrap_or_default()
    }

    pub fn clear_registered_streams(&self) {
        if let Ok(mut streams) = self.streams.lock() {
            streams.clear();
        }
    }

    pub fn clear_shutdown_state(&mut self) {
        self.clear_registered_streams();
        if let Ok(mut tokens) = self.accept_tokens.lock() {
            tokens.clear();
        }
        if let Ok(mut tokens) = self.connect_tokens.lock() {
            tokens.clear();
        }
        if let Ok(mut tokens) = self.lookup_tokens.lock() {
            tokens.clear();
        }
        if let Ok(mut handles) = self.accept_task_handles.lock() {
            handles.clear();
        }
        if let Ok(mut handles) = self.connect_task_handles.lock() {
            handles.clear();
        }
        if let Ok(mut handles) = self.lookup_task_handles.lock() {
            handles.clear();
        }
        if let Ok(mut handles) = self.send_task_handles.lock() {
            handles.clear();
        }
        self.closing_client = None;
    }
}
