use crate::constants::{MAGIC, MAX_FRAME_SIZE, PROTOCOL_VERSION};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgType {
    U,
    D,
    I,
    S,
    F,
    C,
    E,
    K,
    P,
    O,
    X,
    L,
    Q,
    Y,
    J,
    G,
    Z,
}

impl MsgType {
    pub fn as_u8(self) -> u8 {
        match self {
            MsgType::U => b'U',
            MsgType::D => b'D',
            MsgType::I => b'I',
            MsgType::S => b'S',
            MsgType::F => b'F',
            MsgType::C => b'C',
            MsgType::E => b'E',
            MsgType::K => b'K',
            MsgType::P => b'P',
            MsgType::O => b'O',
            MsgType::X => b'X',
            MsgType::L => b'L',
            MsgType::Q => b'Q',
            MsgType::Y => b'Y',
            MsgType::J => b'J',
            MsgType::G => b'G',
            MsgType::Z => b'Z',
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            b'U' => Some(MsgType::U),
            b'D' => Some(MsgType::D),
            b'I' => Some(MsgType::I),
            b'S' => Some(MsgType::S),
            b'F' => Some(MsgType::F),
            b'C' => Some(MsgType::C),
            b'E' => Some(MsgType::E),
            b'K' => Some(MsgType::K),
            b'P' => Some(MsgType::P),
            b'O' => Some(MsgType::O),
            b'X' => Some(MsgType::X),
            b'L' => Some(MsgType::L),
            b'Q' => Some(MsgType::Q),
            b'Y' => Some(MsgType::Y),
            b'J' => Some(MsgType::J),
            b'G' => Some(MsgType::G),
            b'Z' => Some(MsgType::Z),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub msg_type: MsgType,
    pub msg_id: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("io error: {0}")]
    Io(String),
    #[error("frame too short")]
    TooShort,
    #[error("invalid magic")]
    InvalidMagic,
    #[error("unsupported protocol version: {0}")]
    InvalidVersion(u8),
    #[error("unknown frame type: {0}")]
    UnknownType(u8),
    #[error("invalid frame size: {0}")]
    InvalidSize(usize),
    #[error("frame length mismatch")]
    LengthMismatch,
}

impl Frame {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        if self.payload.len() > MAX_FRAME_SIZE {
            return Err(ProtocolError::InvalidSize(self.payload.len()));
        }

        let mut out = Vec::with_capacity(18 + self.payload.len());
        out.extend_from_slice(&MAGIC);
        out.push(PROTOCOL_VERSION);
        out.push(self.msg_type.as_u8());
        out.extend_from_slice(&self.msg_id.to_be_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.payload);
        Ok(out)
    }

    pub fn decode(frame: &[u8]) -> Result<Self, ProtocolError> {
        if frame.len() < 18 {
            return Err(ProtocolError::TooShort);
        }

        if frame[0..4] != MAGIC {
            return Err(ProtocolError::InvalidMagic);
        }

        let version = frame[4];
        if version != PROTOCOL_VERSION {
            return Err(ProtocolError::InvalidVersion(version));
        }

        let msg_type = MsgType::from_u8(frame[5]).ok_or(ProtocolError::UnknownType(frame[5]))?;

        let msg_id = u64::from_be_bytes(frame[6..14].try_into().unwrap());
        let length = u32::from_be_bytes(frame[14..18].try_into().unwrap()) as usize;

        if length > MAX_FRAME_SIZE {
            return Err(ProtocolError::InvalidSize(length));
        }

        if frame.len() != 18 + length {
            return Err(ProtocolError::LengthMismatch);
        }

        Ok(Self {
            msg_type,
            msg_id,
            payload: frame[18..].to_vec(),
        })
    }

    pub async fn read_from<R>(reader: &mut R) -> Result<Self, ProtocolError>
    where
        R: AsyncRead + Unpin,
    {
        let mut buffer = Vec::new();

        loop {
            let mut one = [0u8; 1];
            reader
                .read_exact(&mut one)
                .await
                .map_err(|e| ProtocolError::Io(e.to_string()))?;

            buffer.push(one[0]);

            if buffer.ends_with(&MAGIC) {
                break;
            }

            if buffer.len() > 4 {
                let keep = buffer.split_off(buffer.len() - 4);
                buffer = keep;
            }
        }

        let mut header = [0u8; 14];
        reader
            .read_exact(&mut header)
            .await
            .map_err(|e| ProtocolError::Io(e.to_string()))?;

        let version = header[0];
        if version != PROTOCOL_VERSION {
            return Err(ProtocolError::InvalidVersion(version));
        }

        let msg_type = MsgType::from_u8(header[1]).ok_or(ProtocolError::UnknownType(header[1]))?;

        let msg_id = u64::from_be_bytes(header[2..10].try_into().unwrap());
        let length = u32::from_be_bytes(header[10..14].try_into().unwrap()) as usize;

        if length > MAX_FRAME_SIZE {
            return Err(ProtocolError::InvalidSize(length));
        }

        let mut payload = vec![0u8; length];
        reader
            .read_exact(&mut payload)
            .await
            .map_err(|e| ProtocolError::Io(e.to_string()))?;

        Ok(Self {
            msg_type,
            msg_id,
            payload,
        })
    }

    pub async fn write_to<W>(&self, writer: &mut W) -> Result<(), ProtocolError>
    where
        W: AsyncWrite + Unpin,
    {
        let bytes = self.encode()?;
        writer
            .write_all(&bytes)
            .await
            .map_err(|e| ProtocolError::Io(e.to_string()))
    }
}
