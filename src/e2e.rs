use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

use crypto_secretbox::{
    Key, Nonce, XSalsa20Poly1305,
    aead::{Aead, KeyInit},
};

use rand_core::{OsRng, RngCore};

#[derive(Clone)]
pub struct E2E {
    pub pq_enabled: bool,

    // Classical identity keys
    private_key: StaticSecret,
    public_key: PublicKey,

    peer_public: Option<PublicKey>,

    // Combined live session key
    session_key: Option<[u8; 32]>,

    // Hybrid-ready state
    classical_shared: Option<[u8; 32]>,
    pq_shared: Option<Vec<u8>>,
}

impl E2E {
    pub fn new(pq_enabled: bool) -> Self {
        let private_key = StaticSecret::random_from_rng(OsRng);
        let public_key = PublicKey::from(&private_key);

        Self {
            pq_enabled,
            private_key,
            public_key,
            peer_public: None,
            session_key: None,
            classical_shared: None,
            pq_shared: None,
        }
    }

    pub fn public_bytes(&self) -> Vec<u8> {
        self.public_key.as_bytes().to_vec()
    }

    fn finalize_session_key_if_ready(&mut self) {
        let Some(classical_shared) = self.classical_shared else {
            return;
        };

        if self.pq_enabled {
            let Some(ref pq_shared) = self.pq_shared else {
                return;
            };

            let mut material = Vec::new();
            material.extend_from_slice(b"TERMCHAT_HYBRID_V1");
            material.push(b'|');
            material.extend_from_slice(&classical_shared);
            material.push(b'|');
            material.extend_from_slice(pq_shared);

            let digest = Sha256::digest(&material);
            let mut key = [0u8; 32];
            key.copy_from_slice(&digest);
            self.session_key = Some(key);
        } else {
            let mut material = Vec::new();
            material.extend_from_slice(b"TERMCHAT_CLASSICAL_V1");
            material.push(b'|');
            material.extend_from_slice(&classical_shared);

            let digest = Sha256::digest(&material);
            let mut key = [0u8; 32];
            key.copy_from_slice(&digest);
            self.session_key = Some(key);
        }
    }

    pub fn receive_peer_key(&mut self, data: &[u8]) {
        if data.len() != 32 {
            return;
        }

        let mut arr = [0u8; 32];
        arr.copy_from_slice(data);

        let peer_public = PublicKey::from(arr);
        let shared = self.private_key.diffie_hellman(&peer_public);

        self.peer_public = Some(peer_public);
        self.classical_shared = Some(*shared.as_bytes());
        self.finalize_session_key_if_ready();
    }

    pub fn pq_public_bytes(&self) -> Option<Vec<u8>> {
        None
    }

    pub fn receive_peer_pq_public(&mut self, _peer_pq_public: &[u8]) -> Vec<u8> {
        Vec::new()
    }

    pub fn receive_peer_pq_ciphertext(&mut self, _ciphertext: &[u8]) {}

    pub fn ready(&self) -> bool {
        self.session_key.is_some()
    }

    pub fn encrypt(&self, payload: &[u8]) -> Vec<u8> {
        let Some(session_key) = self.session_key else {
            return payload.to_vec();
        };

        let cipher = XSalsa20Poly1305::new(Key::from_slice(&session_key));

        let mut nonce_bytes = [0u8; 24];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        match cipher.encrypt(nonce, payload) {
            Ok(ciphertext) => {
                let mut out = Vec::with_capacity(24 + ciphertext.len());
                out.extend_from_slice(&nonce_bytes);
                out.extend_from_slice(&ciphertext);
                out
            }
            Err(_) => payload.to_vec(),
        }
    }

    pub fn decrypt(&self, payload: &[u8]) -> Vec<u8> {
        let Some(session_key) = self.session_key else {
            return payload.to_vec();
        };

        if payload.len() < 24 {
            return payload.to_vec();
        }

        let cipher = XSalsa20Poly1305::new(Key::from_slice(&session_key));

        let (nonce_bytes, ciphertext) = payload.split_at(24);
        let nonce = Nonce::from_slice(nonce_bytes);

        match cipher.decrypt(nonce, ciphertext) {
            Ok(plain) => plain,
            Err(_) => payload.to_vec(),
        }
    }

    pub fn derive_offline_blob_key(
        &self,
        shared_secret: &[u8],
        my_b32: &str,
        peer_b32: &str,
    ) -> Vec<u8> {
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

        let mut ids = [my_id, peer_id];
        ids.sort();

        let mut material = Vec::new();
        material.extend_from_slice(b"OFFLINE_BLOB_V1");
        material.push(b'|');
        material.extend_from_slice(shared_secret);
        material.push(b'|');
        material.extend_from_slice(ids[0].as_bytes());
        material.push(b'|');
        material.extend_from_slice(ids[1].as_bytes());

        Sha256::digest(&material).to_vec()
    }

    pub fn encrypt_offline_blob(&self, frame: &[u8], blob_key: &[u8]) -> Vec<u8> {
        if blob_key.len() != 32 {
            return frame.to_vec();
        }

        let cipher = XSalsa20Poly1305::new(Key::from_slice(blob_key));

        let mut nonce_bytes = [0u8; 24];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        match cipher.encrypt(nonce, frame) {
            Ok(ciphertext) => {
                let mut out = Vec::with_capacity(24 + ciphertext.len());
                out.extend_from_slice(&nonce_bytes);
                out.extend_from_slice(&ciphertext);
                out
            }
            Err(_) => frame.to_vec(),
        }
    }

    pub fn decrypt_offline_blob(&self, blob: &[u8], blob_key: &[u8]) -> Vec<u8> {
        if blob_key.len() != 32 || blob.len() < 25 {
            return blob.to_vec();
        }

        let cipher = XSalsa20Poly1305::new(Key::from_slice(blob_key));
        let (nonce_bytes, ciphertext) = blob.split_at(24);
        let nonce = Nonce::from_slice(nonce_bytes);

        match cipher.decrypt(nonce, ciphertext) {
            Ok(plain) => plain,
            Err(_) => blob.to_vec(),
        }
    }
}
