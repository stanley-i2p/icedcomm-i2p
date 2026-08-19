# DeadDrop Server

This directory contains the Rust DeadDrop server used by CommTools-I2P clients
for replicated offline message storage and retrieval.

The server is intentionally content-agnostic. It stores opaque encrypted blobs
and does not participate in client identity, message encryption, or message
decryption.

## Project Layout

```text
SERVER/
|-- Cargo.toml
|-- README.md
`-- src/
    `-- main.rs
```

## Requirements

```bash
sudo apt install build-essential curl -y
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Use the default installation option when prompted, then load the Rust
environment:

```bash
source "$HOME/.cargo/env"
```

An I2P router with SAM enabled is required. The default SAM endpoint used by
the server is `127.0.0.1:7656`.

## Build

Run this command from the `SERVER` directory:

```bash
cargo build --release
```

The resulting binary is written to:

```text
target/release/deaddrop-server
```

## Run

```bash
./target/release/deaddrop-server
```

The server persists its SAM identities and stored data in its configured data
directory. Preserve that directory across restarts if the server should retain
the same I2P destinations and queued blobs.
