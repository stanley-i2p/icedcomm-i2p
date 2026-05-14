pub const APP_NAME: &str = "IcedComm-I2P";
pub const APP_VERSION: &str = "1.0.0-beta.1";

pub const MAGIC: [u8; 4] = [0x89, b'I', b'2', b'P'];
pub const PROTOCOL_VERSION: u8 = 3;

pub const MAX_FRAME_SIZE: usize = 256 * 1024;
pub const MAX_FILE_SIZE: usize = 50 * 1024 * 1024;
pub const MAX_IMAGE_LINES: usize = 2000;
pub const MAX_FILENAME: usize = 128;

pub const MAX_ACTIVE_DEADDROP_REPLICAS: usize = 3;

pub const DEFAULT_SAM_HOST: &str = "127.0.0.1";
pub const DEFAULT_SAM_PORT: u16 = 7656;
