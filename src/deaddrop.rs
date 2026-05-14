use crate::constants::{DEFAULT_SAM_HOST, DEFAULT_SAM_PORT};
use sha2::{Digest, Sha256};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};

const READY_PROBE_ATTEMPTS: usize = 3;
const READY_PROBE_DELAY_MS: u64 = 650;

#[derive(Debug, Clone)]
pub struct DeaddropOpStat {
    pub op: &'static str,
    pub drop: String,
    pub ok: bool,
    pub latency_ms: f64,
    pub detail: String,
}

#[derive(Debug)]
pub struct DeadDropClient {
    pub session_id: String,
    pub put_session_id: String,
    pub get_session_id: String,
    pub drops: Vec<String>,
    pub pow_zero_bits: u32,
    pub sam_host: String,
    pub sam_port: u16,

    put_ctrl_reader: Option<BufReader<OwnedReadHalf>>,
    put_ctrl_writer: Option<OwnedWriteHalf>,

    get_ctrl_reader: Option<BufReader<OwnedReadHalf>>,
    get_ctrl_writer: Option<OwnedWriteHalf>,
}

impl DeadDropClient {
    pub fn new(session_id: String, drops: Vec<String>) -> Self {
        Self::new_with_sam(
            session_id,
            drops,
            DEFAULT_SAM_HOST.to_string(),
            DEFAULT_SAM_PORT,
        )
    }

    pub fn new_with_sam(
        session_id: String,
        drops: Vec<String>,
        sam_host: String,
        sam_port: u16,
    ) -> Self {
        let put_session_id = format!("{}_put", session_id);
        let get_session_id = format!("{}_get", session_id);

        Self {
            session_id,
            put_session_id,
            get_session_id,
            drops,
            pow_zero_bits: 20,
            sam_host,
            sam_port,

            put_ctrl_reader: None,
            put_ctrl_writer: None,
            get_ctrl_reader: None,
            get_ctrl_writer: None,
        }
    }

    async fn sam_hello(
        reader: &mut BufReader<OwnedReadHalf>,
        writer: &mut OwnedWriteHalf,
    ) -> Result<(), String> {
        writer
            .write_all(b"HELLO VERSION MIN=3.0 MAX=3.2\n")
            .await
            .map_err(|e| e.to_string())?;

        writer.flush().await.map_err(|e| e.to_string())?;

        let mut line = String::new();
        let n = reader
            .read_line(&mut line)
            .await
            .map_err(|e| e.to_string())?;

        if n == 0 {
            return Err("SAM HELLO unexpected EOF".into());
        }

        if !line.contains("RESULT=OK") {
            return Err(format!("SAM HELLO failed: {}", line.trim()));
        }

        Ok(())
    }

    async fn connect_stream(
        &self,
        destination: &str,
        mode: &str,
    ) -> Result<(BufReader<OwnedReadHalf>, OwnedWriteHalf), String> {
        let stream = TcpStream::connect((self.sam_host.as_str(), self.sam_port))
            .await
            .map_err(|e| e.to_string())?;

        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        Self::sam_hello(&mut reader, &mut write_half).await?;

        let session_id = if mode == "put" {
            &self.put_session_id
        } else {
            &self.get_session_id
        };

        let cmd = format!(
            "STREAM CONNECT ID={} DESTINATION={}\n",
            session_id, destination
        );

        write_half
            .write_all(cmd.as_bytes())
            .await
            .map_err(|e| e.to_string())?;

        write_half.flush().await.map_err(|e| e.to_string())?;

        let mut resp = String::new();
        let n = reader
            .read_line(&mut resp)
            .await
            .map_err(|e| e.to_string())?;

        if n == 0 {
            return Err("STREAM CONNECT unexpected EOF".into());
        }

        if !resp.contains("RESULT=OK") {
            return Err(format!("SAM CONNECT FAILED: {}", resp.trim()));
        }

        Ok((reader, write_half))
    }

    async fn probe_ready_once(&self, drop: &str) -> Result<(), String> {
        let (_reader, mut writer) = self.connect_stream(drop, "put").await?;
        let _ = writer.shutdown().await;
        Ok(())
    }

    async fn wait_until_any_drop_ready(&self) -> Result<(), String> {
        if self.drops.is_empty() {
            return Err("No deaddrop servers configured".into());
        }

        let mut last_error = String::new();

        for attempt in 0..READY_PROBE_ATTEMPTS {
            for drop in &self.drops {
                match self.probe_ready_once(drop).await {
                    Ok(()) => return Ok(()),
                    Err(err) => {
                        last_error = format!("{drop}: {err}");
                    }
                }
            }

            if attempt + 1 < READY_PROBE_ATTEMPTS {
                sleep(Duration::from_millis(READY_PROBE_DELAY_MS)).await;
            }
        }

        Err(if last_error.is_empty() {
            "No deaddrop server accepted a readiness probe".into()
        } else {
            format!("No deaddrop server accepted a readiness probe ({last_error})")
        })
    }

    fn has_leading_zero_bits(hash: &[u8], bits: u32) -> bool {
        let full_bytes = (bits / 8) as usize;
        let rem_bits = (bits % 8) as u8;

        if hash.len() < full_bytes {
            return false;
        }

        if hash[..full_bytes].iter().any(|b| *b != 0) {
            return false;
        }

        if rem_bits == 0 {
            return true;
        }

        let mask = 0xFFu8 << (8 - rem_bits);
        (hash[full_bytes] & mask) == 0
    }

    pub fn find_pow_counter_for(pow_zero_bits: u32, key: &str, blob: &[u8]) -> u64 {
        let size_bytes = blob.len().to_string().into_bytes();

        let mut base = Sha256::new();
        base.update(b"POWv1");
        base.update(b"|");
        base.update(key.as_bytes());
        base.update(b"|");
        base.update(&size_bytes);
        base.update(b"|");
        base.update(blob);
        base.update(b"|");

        let mut counter: u64 = 0;

        loop {
            let mut h = base.clone();
            let counter_bytes = counter.to_string();
            h.update(counter_bytes.as_bytes());

            let digest = h.finalize();

            if Self::has_leading_zero_bits(&digest, pow_zero_bits) {
                return counter;
            }

            counter = counter.wrapping_add(1);
        }
    }

    fn find_pow_counter(&self, key: &str, blob: &[u8]) -> u64 {
        Self::find_pow_counter_for(self.pow_zero_bits, key, blob)
    }

    pub async fn start(&mut self) -> Result<(), String> {
        if self.put_ctrl_writer.is_some() && self.get_ctrl_writer.is_some() {
            return Ok(());
        }

        // If one side exists but the other does not, clear partial state first.
        self.close().await;

        let put_stream = TcpStream::connect((self.sam_host.as_str(), self.sam_port))
            .await
            .map_err(|e| e.to_string())?;
        let (put_read_half, mut put_write_half) = put_stream.into_split();
        let mut put_reader = BufReader::new(put_read_half);

        Self::sam_hello(&mut put_reader, &mut put_write_half).await?;

        let put_cmd = format!(
            "SESSION CREATE STYLE=STREAM ID={} DESTINATION=TRANSIENT SIGNATURE_TYPE=7 OPTION inbound.length=2 outbound.length=2 inbound.quantity=2 outbound.quantity=2\n",
            self.put_session_id
        );

        put_write_half
            .write_all(put_cmd.as_bytes())
            .await
            .map_err(|e| e.to_string())?;

        put_write_half.flush().await.map_err(|e| e.to_string())?;

        let mut put_resp = String::new();
        let n = put_reader
            .read_line(&mut put_resp)
            .await
            .map_err(|e| e.to_string())?;

        if n == 0 {
            return Err("PUT SESSION CREATE unexpected EOF".into());
        }

        if !put_resp.contains("RESULT=OK") {
            return Err(format!("PUT SESSION CREATE failed: {}", put_resp.trim()));
        }

        let get_stream = TcpStream::connect((self.sam_host.as_str(), self.sam_port))
            .await
            .map_err(|e| e.to_string())?;
        let (get_read_half, mut get_write_half) = get_stream.into_split();
        let mut get_reader = BufReader::new(get_read_half);

        Self::sam_hello(&mut get_reader, &mut get_write_half).await?;

        let get_cmd = format!(
            "SESSION CREATE STYLE=STREAM ID={} DESTINATION=TRANSIENT SIGNATURE_TYPE=7 OPTION inbound.length=2 outbound.length=2 inbound.quantity=2 outbound.quantity=2\n",
            self.get_session_id
        );

        get_write_half
            .write_all(get_cmd.as_bytes())
            .await
            .map_err(|e| e.to_string())?;

        get_write_half.flush().await.map_err(|e| e.to_string())?;

        let mut get_resp = String::new();
        let n = get_reader
            .read_line(&mut get_resp)
            .await
            .map_err(|e| e.to_string())?;

        if n == 0 {
            return Err("GET SESSION CREATE unexpected EOF".into());
        }

        if !get_resp.contains("RESULT=OK") {
            return Err(format!("GET SESSION CREATE failed: {}", get_resp.trim()));
        }

        self.put_ctrl_reader = Some(put_reader);
        self.put_ctrl_writer = Some(put_write_half);
        self.get_ctrl_reader = Some(get_reader);
        self.get_ctrl_writer = Some(get_write_half);

        if let Err(err) = self.wait_until_any_drop_ready().await {
            self.close().await;
            return Err(err);
        }

        Ok(())
    }

    async fn put_one_parallel(
        sam_host: String,
        sam_port: u16,
        put_session_id: String,
        drop: String,
        key: String,
        blob: Vec<u8>,
        pow_counter: u64,
    ) -> (String, String, DeaddropOpStat) {
        let started = std::time::Instant::now();

        let result = async {
            let stream = TcpStream::connect((sam_host.as_str(), sam_port))
                .await
                .map_err(|e| e.to_string())?;

            let (read_half, mut write_half) = stream.into_split();
            let mut reader = BufReader::new(read_half);

            write_half
                .write_all(b"HELLO VERSION MIN=3.0 MAX=3.2\n")
                .await
                .map_err(|e| e.to_string())?;
            write_half.flush().await.map_err(|e| e.to_string())?;

            let mut hello = String::new();
            let n = reader
                .read_line(&mut hello)
                .await
                .map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("STREAM CONNECT unexpected EOF during HELLO".into());
            }
            if !hello.contains("RESULT=OK") {
                return Err(format!("SAM HELLO failed: {}", hello.trim()));
            }

            let cmd = format!(
                "STREAM CONNECT ID={} DESTINATION={}\n",
                put_session_id, drop
            );

            write_half
                .write_all(cmd.as_bytes())
                .await
                .map_err(|e| e.to_string())?;
            write_half.flush().await.map_err(|e| e.to_string())?;

            let mut resp = String::new();
            let n = reader
                .read_line(&mut resp)
                .await
                .map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("STREAM CONNECT unexpected EOF".into());
            }
            if !resp.contains("RESULT=OK") {
                return Err(format!("SAM CONNECT FAILED: {}", resp.trim()));
            }

            write_half
                .write_all(format!("PUT {} {} {}\n", key, blob.len(), pow_counter).as_bytes())
                .await
                .map_err(|e| e.to_string())?;
            write_half
                .write_all(&blob)
                .await
                .map_err(|e| e.to_string())?;
            write_half.flush().await.map_err(|e| e.to_string())?;

            let mut put_resp = String::new();
            let n = reader
                .read_line(&mut put_resp)
                .await
                .map_err(|e| e.to_string())?;
            let _ = write_half.shutdown().await;

            if n == 0 {
                return Err("PUT unexpected EOF".into());
            }

            Ok::<String, String>(put_resp.trim().to_string())
        }
        .await;

        match result {
            Ok(resp) if resp == "OK" => {
                let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                let stat = DeaddropOpStat {
                    op: "put",
                    drop: drop.clone(),
                    ok: true,
                    latency_ms,
                    detail: "OK".into(),
                };
                (drop, "OK".into(), stat)
            }
            Ok(resp) if resp == "EXISTS" => {
                let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                let stat = DeaddropOpStat {
                    op: "put",
                    drop: drop.clone(),
                    ok: true,
                    latency_ms,
                    detail: "EXISTS".into(),
                };
                (drop, "EXISTS".into(), stat)
            }
            Ok(resp) => {
                eprintln!(
                    "[DD PUT unexpected] {} {} ({:?})",
                    drop,
                    resp,
                    started.elapsed()
                );
                let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                let stat = DeaddropOpStat {
                    op: "put",
                    drop: drop.clone(),
                    ok: false,
                    latency_ms,
                    detail: if resp.is_empty() { "FAIL".into() } else { resp },
                };
                (drop, "FAIL".into(), stat)
            }
            Err(err) => {
                eprintln!(
                    "[DROP PUT FAIL] {}: {} ({:?})",
                    drop,
                    err,
                    started.elapsed()
                );
                let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                let stat = DeaddropOpStat {
                    op: "put",
                    drop: drop.clone(),
                    ok: false,
                    latency_ms,
                    detail: err,
                };
                (drop, "FAIL".into(), stat)
            }
        }
    }

    pub async fn put(&mut self, key: &str, blob: &[u8]) -> (String, Vec<String>) {
        let pow_counter = self.find_pow_counter(key, blob);
        self.put_with_pow_counter(key, blob, pow_counter).await
    }

    pub async fn put_with_pow_counter(
        &mut self,
        key: &str,
        blob: &[u8],
        pow_counter: u64,
    ) -> (String, Vec<String>) {
        let (status, drops, _stats) = self
            .put_with_pow_counter_and_stats(key, blob, pow_counter)
            .await;
        (status, drops)
    }

    pub async fn put_with_pow_counter_and_stats(
        &mut self,
        key: &str,
        blob: &[u8],
        pow_counter: u64,
    ) -> (String, Vec<String>, Vec<DeaddropOpStat>) {
        if self.put_ctrl_writer.is_none() {
            if self.start().await.is_err() {
                return ("FAIL".into(), vec![], vec![]);
            }
        }

        let sam_host = self.sam_host.clone();

        let sam_port = self.sam_port;

        let put_session_id = self.put_session_id.clone();
        let key = key.to_string();
        let blob = blob.to_vec();

        let mut joinset = JoinSet::new();

        for drop in self.drops.clone() {
            let sam_host_cloned = sam_host.clone();
            let put_session_id_cloned = put_session_id.clone();
            let key_cloned = key.clone();
            let blob_cloned = blob.clone();

            joinset.spawn(async move {
                Self::put_one_parallel(
                    sam_host_cloned,
                    sam_port,
                    put_session_id_cloned,
                    drop,
                    key_cloned,
                    blob_cloned,
                    pow_counter,
                )
                .await
            });
        }

        let mut ok_drops = Vec::new();
        let mut exists_drops = Vec::new();
        let mut stats = Vec::new();

        while let Some(joined) = joinset.join_next().await {
            match joined {
                Ok((drop, status, stat)) if status == "OK" => {
                    stats.push(stat);
                    ok_drops.push(drop);
                }
                Ok((drop, status, stat)) if status == "EXISTS" => {
                    stats.push(stat);
                    exists_drops.push(drop);
                }
                Ok((_drop, _status, stat)) => {
                    stats.push(stat);
                }
                Err(err) => {
                    eprintln!("[DD PUT TASK FAIL] {}", err);
                }
            }
        }

        if !ok_drops.is_empty() {
            return ("OK".into(), ok_drops, stats);
        }

        if !exists_drops.is_empty() {
            return ("EXISTS".into(), exists_drops, stats);
        }

        ("FAIL".into(), vec![], stats)
    }

    pub async fn get(&mut self, key: &str) -> Vec<(String, Vec<u8>)> {
        let (blobs, _stats) = self.get_with_stats(key).await;
        blobs
    }

    pub async fn get_with_stats(
        &mut self,
        key: &str,
    ) -> (Vec<(String, Vec<u8>)>, Vec<DeaddropOpStat>) {
        if self.get_ctrl_writer.is_none() {
            if self.start().await.is_err() {
                return (vec![], vec![]);
            }
        }

        let mut good = Vec::new();
        let mut stats = Vec::new();

        for drop in self.drops.clone() {
            let started = std::time::Instant::now();

            let result = async {
                let (mut reader, mut writer) = self.connect_stream(&drop, "get").await?;
                writer
                    .write_all(format!("GET {}\n", key).as_bytes())
                    .await
                    .map_err(|e| e.to_string())?;
                writer.flush().await.map_err(|e| e.to_string())?;

                let mut header = String::new();
                let n = reader
                    .read_line(&mut header)
                    .await
                    .map_err(|e| e.to_string())?;

                if n == 0 {
                    let _ = writer.shutdown().await;
                    return Err("GET unexpected EOF".into());
                }

                let header = header.trim().to_string();

                if header == "MISS" {
                    let _ = writer.shutdown().await;
                    return Ok::<(Option<Vec<u8>>, String), String>((None, "MISS".into()));
                }

                if header == "ERR" {
                    let _ = writer.shutdown().await;
                    return Ok::<(Option<Vec<u8>>, String), String>((None, "ERR".into()));
                }

                let parts: Vec<&str> = header.split_whitespace().collect();
                if parts.len() == 2 && parts[0] == "OK" {
                    let size: usize = parts[1]
                        .parse()
                        .map_err(|_| format!("invalid GET size header: {}", header))?;

                    let mut buf = vec![0u8; size];

                    reader
                        .read_exact(&mut buf)
                        .await
                        .map_err(|e| e.to_string())?;

                    let _ = writer.shutdown().await;
                    return Ok((Some(buf), "OK".into()));
                }

                let _ = writer.shutdown().await;
                Err(format!("unexpected GET response: {}", header))
            }
            .await;

            match result {
                Ok((Some(data), detail)) => {
                    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                    stats.push(DeaddropOpStat {
                        op: "get",
                        drop: drop.clone(),
                        ok: true,
                        latency_ms,
                        detail,
                    });
                    good.push((drop, data));
                }
                Ok((None, detail)) if detail == "MISS" => {
                    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                    stats.push(DeaddropOpStat {
                        op: "get",
                        drop,
                        ok: true,
                        latency_ms,
                        detail,
                    });
                }
                Ok((None, detail)) => {
                    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                    stats.push(DeaddropOpStat {
                        op: "get",
                        drop,
                        ok: false,
                        latency_ms,
                        detail,
                    });
                }
                Err(err) => {
                    eprintln!(
                        "[DROP GET FAIL] {}: {} ({:?})",
                        drop,
                        err,
                        started.elapsed()
                    );
                    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
                    stats.push(DeaddropOpStat {
                        op: "get",
                        drop,
                        ok: false,
                        latency_ms,
                        detail: err,
                    });
                }
            }
        }

        (good, stats)
    }

    pub async fn close(&mut self) {
        if let Some(mut w) = self.put_ctrl_writer.take() {
            let _ = w.flush().await;
            let _ = w.shutdown().await;
        }
        self.put_ctrl_reader = None;

        if let Some(mut w) = self.get_ctrl_writer.take() {
            let _ = w.flush().await;
            let _ = w.shutdown().await;
        }
        self.get_ctrl_reader = None;
    }
}
