use base64::{Engine as _, engine::general_purpose};
use crypto_secretbox::{
    Key, Nonce, XSalsa20Poly1305,
    aead::{Aead, KeyInit},
};
use hmac::{Hmac, Mac};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

const REQUEST_PREFIX: &str = "COMMTOOLS-I2P-RENDEZVOUS-REQUEST-v1:";
const RESPONSE_PREFIX: &str = "COMMTOOLS-I2P-RENDEZVOUS-RESPONSE-v1:";
pub const AUTH_SIGNAL_PREFIX: &str = "__SIGNAL__:RENDEZVOUS-AUTH-v1:";
const REQUEST_FORMAT: &str = "COMMTOOLS-I2P-RENDEZVOUS-REQUEST-v1";
const RESPONSE_FORMAT: &str = "COMMTOOLS-I2P-RENDEZVOUS-RESPONSE-v1";
const RESPONSE_PAYLOAD_FORMAT: &str = "COMMTOOLS-I2P-RENDEZVOUS-PAYLOAD-v1";
const RESPONSE_KDF_DOMAIN: &[u8] = b"COMMTOOLS-I2P-RENDEZVOUS-RESPONSE-KDF-v1";
const AUTH_DOMAIN: &[u8] = b"COMMTOOLS-I2P-RENDEZVOUS-AUTH-v1";
const VALIDITY_MS: u64 = 15 * 60 * 1_000;
const MAX_CLOCK_SKEW_MS: u64 = 5 * 60 * 1_000;
const MAX_ENCODED_LEN: usize = 16 * 1_024;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    Request,
    Response,
    Unknown,
}

#[derive(Clone)]
pub struct PendingRequest {
    pub request_id: [u8; 16],
    private_key: [u8; 32],
    request_nonce: [u8; 24],
    pub expires_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IssuedState {
    Available,
    Reserved,
    Consumed,
    Revoked,
}

#[derive(Clone)]
pub struct IssuedAccess {
    pub request_id: [u8; 16],
    call_secret: [u8; 32],
    pub expires_ms: u64,
    pub state: IssuedState,
}

#[derive(Clone)]
pub struct OutgoingAccess {
    pub request_id: [u8; 16],
    call_secret: [u8; 32],
    pub destination_b32: String,
    pub expires_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestWire {
    format: String,
    request_id: String,
    public_key: String,
    created_ms: u64,
    expires_ms: u64,
    nonce: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResponseWire {
    format: String,
    request_id: String,
    public_key: String,
    nonce: String,
    ciphertext: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResponsePayload {
    format: String,
    request_id: String,
    destination_b32: String,
    call_secret: String,
    expires_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuthProof {
    request_id: String,
    nonce: String,
    mac: String,
}

impl std::fmt::Debug for PendingRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingRequest")
            .field("request_id", &encode(&self.request_id))
            .field("expires_ms", &self.expires_ms)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for IssuedAccess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IssuedAccess")
            .field("request_id", &encode(&self.request_id))
            .field("expires_ms", &self.expires_ms)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for OutgoingAccess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OutgoingAccess")
            .field("request_id", &encode(&self.request_id))
            .field("destination_b32", &self.destination_b32)
            .field("expires_ms", &self.expires_ms)
            .finish_non_exhaustive()
    }
}

pub fn input_kind(value: &str) -> InputKind {
    let trimmed = value.trim();
    if let Ok(wire) = decode_json::<RequestWire>(trimmed, REQUEST_PREFIX) {
        if wire.format == REQUEST_FORMAT {
            return InputKind::Request;
        }
    }
    if let Ok(wire) = decode_json::<ResponseWire>(trimmed, RESPONSE_PREFIX) {
        if wire.format == RESPONSE_FORMAT {
            return InputKind::Response;
        }
    }
    InputKind::Unknown
}

pub fn response_matches_pending(value: &str, pending: &PendingRequest) -> bool {
    let Ok(wire) = decode_json::<ResponseWire>(value, RESPONSE_PREFIX) else {
        return false;
    };
    if wire.format != RESPONSE_FORMAT {
        return false;
    }
    decode_array::<16>(&wire.request_id, "request id")
        .map(|request_id| request_id == pending.request_id)
        .unwrap_or(false)
}

pub fn generate_request(now_ms: u64) -> Result<(PendingRequest, String), String> {
    let private = StaticSecret::random_from_rng(OsRng);
    let public = PublicKey::from(&private);
    let mut request_id = [0u8; 16];
    let mut request_nonce = [0u8; 24];
    OsRng.fill_bytes(&mut request_id);
    OsRng.fill_bytes(&mut request_nonce);
    let expires_ms = now_ms.saturating_add(VALIDITY_MS);

    let wire = RequestWire {
        format: REQUEST_FORMAT.to_string(),
        request_id: encode(&request_id),
        public_key: encode(public.as_bytes()),
        created_ms: now_ms,
        expires_ms,
        nonce: encode(&request_nonce),
    };
    let encoded = encode_json(REQUEST_PREFIX, &wire)?;

    Ok((
        PendingRequest {
            request_id,
            private_key: private.to_bytes(),
            request_nonce,
            expires_ms,
        },
        encoded,
    ))
}

pub fn answer_request(
    encoded_request: &str,
    destination_b32: &str,
    now_ms: u64,
) -> Result<(IssuedAccess, String), String> {
    let wire: RequestWire = decode_json(encoded_request, REQUEST_PREFIX)?;
    if wire.format != REQUEST_FORMAT {
        return Err("unsupported rendezvous request format".into());
    }
    validate_times(wire.created_ms, wire.expires_ms, now_ms)?;

    let request_id = decode_array::<16>(&wire.request_id, "request id")?;
    let request_public = decode_array::<32>(&wire.public_key, "request public key")?;
    let request_nonce = decode_array::<24>(&wire.nonce, "request nonce")?;

    let response_private = StaticSecret::random_from_rng(OsRng);
    let response_public = PublicKey::from(&response_private);
    let shared = response_private.diffie_hellman(&PublicKey::from(request_public));
    let key = derive_response_key(shared.as_bytes(), &request_id, &request_nonce);
    let mut call_secret = [0u8; 32];
    let mut response_nonce = [0u8; 24];
    OsRng.fill_bytes(&mut call_secret);
    OsRng.fill_bytes(&mut response_nonce);
    let expires_ms = wire.expires_ms.min(now_ms.saturating_add(VALIDITY_MS));

    let payload = ResponsePayload {
        format: RESPONSE_PAYLOAD_FORMAT.to_string(),
        request_id: encode(&request_id),
        destination_b32: destination_b32.to_ascii_lowercase(),
        call_secret: encode(&call_secret),
        expires_ms,
    };
    let plaintext = serde_json::to_vec(&payload).map_err(|err| err.to_string())?;
    let cipher = XSalsa20Poly1305::new(Key::from_slice(&key));
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&response_nonce), plaintext.as_slice())
        .map_err(|_| "rendezvous response encryption failed".to_string())?;

    let response = ResponseWire {
        format: RESPONSE_FORMAT.to_string(),
        request_id: encode(&request_id),
        public_key: encode(response_public.as_bytes()),
        nonce: encode(&response_nonce),
        ciphertext: encode(&ciphertext),
    };

    Ok((
        IssuedAccess {
            request_id,
            call_secret,
            expires_ms,
            state: IssuedState::Available,
        },
        encode_json(RESPONSE_PREFIX, &response)?,
    ))
}

pub fn open_response(
    encoded_response: &str,
    pending: &PendingRequest,
    now_ms: u64,
) -> Result<OutgoingAccess, String> {
    if pending.expires_ms <= now_ms {
        return Err("rendezvous request expired".into());
    }

    let wire: ResponseWire = decode_json(encoded_response, RESPONSE_PREFIX)?;
    if wire.format != RESPONSE_FORMAT {
        return Err("unsupported rendezvous response format".into());
    }
    let request_id = decode_array::<16>(&wire.request_id, "request id")?;
    if request_id != pending.request_id {
        return Err("rendezvous response does not match this request".into());
    }

    let response_public = decode_array::<32>(&wire.public_key, "response public key")?;
    let response_nonce = decode_array::<24>(&wire.nonce, "response nonce")?;
    let ciphertext = decode_bytes(&wire.ciphertext, "response ciphertext")?;
    let private = StaticSecret::from(pending.private_key);
    let shared = private.diffie_hellman(&PublicKey::from(response_public));
    let key = derive_response_key(shared.as_bytes(), &request_id, &pending.request_nonce);
    let cipher = XSalsa20Poly1305::new(Key::from_slice(&key));
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&response_nonce), ciphertext.as_slice())
        .map_err(|_| "rendezvous response authentication failed".to_string())?;
    let payload: ResponsePayload = serde_json::from_slice(&plaintext)
        .map_err(|_| "invalid rendezvous response".to_string())?;

    if payload.format != RESPONSE_PAYLOAD_FORMAT {
        return Err("unsupported rendezvous response payload".into());
    }
    if decode_array::<16>(&payload.request_id, "payload request id")? != request_id {
        return Err("rendezvous response request id mismatch".into());
    }
    if payload.expires_ms <= now_ms || payload.expires_ms > pending.expires_ms {
        return Err("rendezvous response expired or has invalid lifetime".into());
    }

    Ok(OutgoingAccess {
        request_id,
        call_secret: decode_array::<32>(&payload.call_secret, "call secret")?,
        destination_b32: payload.destination_b32,
        expires_ms: payload.expires_ms,
    })
}

pub fn make_auth_signal(
    access: &OutgoingAccess,
    caller_b32: &str,
    receiver_b32: &str,
    now_ms: u64,
) -> Result<String, String> {
    if access.expires_ms <= now_ms {
        return Err("rendezvous response expired".into());
    }

    let mut nonce = [0u8; 16];
    OsRng.fill_bytes(&mut nonce);
    let mac = auth_mac(
        &access.call_secret,
        &access.request_id,
        &nonce,
        caller_b32,
        receiver_b32,
    )?;
    let proof = AuthProof {
        request_id: encode(&access.request_id),
        nonce: encode(&nonce),
        mac: encode(&mac),
    };
    Ok(format!(
        "{AUTH_SIGNAL_PREFIX}{}",
        encode(&serde_json::to_vec(&proof).map_err(|err| err.to_string())?)
    ))
}

pub fn verify_auth_signal(
    body: &str,
    issued: &IssuedAccess,
    caller_b32: &str,
    receiver_b32: &str,
    now_ms: u64,
) -> Result<(), String> {
    if issued.state != IssuedState::Available {
        return Err("rendezvous invitation is not available".into());
    }
    if issued.expires_ms <= now_ms {
        return Err("rendezvous invitation expired".into());
    }

    let encoded = body
        .strip_prefix(AUTH_SIGNAL_PREFIX)
        .ok_or_else(|| "not a rendezvous proof".to_string())?;
    if encoded.len() > MAX_ENCODED_LEN {
        return Err("rendezvous proof is too large".into());
    }
    let bytes = decode_bytes(encoded, "rendezvous proof")?;
    let proof: AuthProof =
        serde_json::from_slice(&bytes).map_err(|_| "invalid rendezvous proof".to_string())?;
    let request_id = decode_array::<16>(&proof.request_id, "proof request id")?;
    if request_id != issued.request_id {
        return Err("rendezvous proof request id mismatch".into());
    }
    let nonce = decode_array::<16>(&proof.nonce, "proof nonce")?;
    let received_mac = decode_array::<32>(&proof.mac, "proof authenticator")?;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(&issued.call_secret)
        .map_err(|_| "invalid rendezvous call secret".to_string())?;
    mac.update(&auth_transcript(
        &request_id,
        &nonce,
        caller_b32,
        receiver_b32,
    ));
    mac.verify_slice(&received_mac)
        .map_err(|_| "rendezvous proof authentication failed".to_string())
}

fn auth_mac(
    secret: &[u8; 32],
    request_id: &[u8; 16],
    nonce: &[u8; 16],
    caller_b32: &str,
    receiver_b32: &str,
) -> Result<[u8; 32], String> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret)
        .map_err(|_| "invalid rendezvous call secret".to_string())?;
    mac.update(&auth_transcript(
        request_id,
        nonce,
        caller_b32,
        receiver_b32,
    ));
    let bytes = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn auth_transcript(
    request_id: &[u8; 16],
    nonce: &[u8; 16],
    caller_b32: &str,
    receiver_b32: &str,
) -> Vec<u8> {
    let mut transcript = Vec::new();
    append_field(&mut transcript, AUTH_DOMAIN);
    append_field(&mut transcript, request_id);
    append_field(&mut transcript, nonce);
    append_field(&mut transcript, caller_b32.to_ascii_lowercase().as_bytes());
    append_field(
        &mut transcript,
        receiver_b32.to_ascii_lowercase().as_bytes(),
    );
    transcript
}

fn derive_response_key(
    shared: &[u8; 32],
    request_id: &[u8; 16],
    request_nonce: &[u8; 24],
) -> [u8; 32] {
    let mut material = Vec::new();
    append_field(&mut material, RESPONSE_KDF_DOMAIN);
    append_field(&mut material, shared);
    append_field(&mut material, request_id);
    append_field(&mut material, request_nonce);
    let digest = Sha256::digest(&material);
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest);
    key
}

fn append_field(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
}

fn validate_times(created_ms: u64, expires_ms: u64, now_ms: u64) -> Result<(), String> {
    if created_ms > now_ms.saturating_add(MAX_CLOCK_SKEW_MS) {
        return Err("rendezvous request creation time is too far in the future".into());
    }
    if expires_ms <= now_ms {
        return Err("rendezvous request expired".into());
    }
    if expires_ms.saturating_sub(created_ms) > VALIDITY_MS {
        return Err("rendezvous request lifetime is invalid".into());
    }
    Ok(())
}

fn encode_json<T: Serialize>(prefix: &str, value: &T) -> Result<String, String> {
    let json = serde_json::to_vec(value).map_err(|err| err.to_string())?;
    Ok(format!("{prefix}{}", encode(&json)))
}

fn decode_json<T: for<'de> Deserialize<'de>>(value: &str, prefix: &str) -> Result<T, String> {
    let trimmed = value.trim();
    if trimmed.len() > MAX_ENCODED_LEN {
        return Err("rendezvous value is too large".into());
    }
    let encoded = trimmed
        .strip_prefix(prefix)
        .ok_or_else(|| "unrecognized rendezvous value".to_string())?;
    let bytes = decode_bytes(encoded, "rendezvous value")?;
    serde_json::from_slice(&bytes).map_err(|_| "invalid rendezvous value".to_string())
}

fn encode(value: &[u8]) -> String {
    general_purpose::URL_SAFE_NO_PAD.encode(value)
}

fn decode_bytes(value: &str, label: &str) -> Result<Vec<u8>, String> {
    general_purpose::URL_SAFE_NO_PAD
        .decode(value.as_bytes())
        .map_err(|_| format!("invalid {label}"))
}

fn decode_array<const N: usize>(value: &str, label: &str) -> Result<[u8; N], String> {
    let bytes = decode_bytes(value, label)?;
    bytes
        .try_into()
        .map_err(|_| format!("invalid {label} length"))
}
