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

## Data Directory

The server stores its persistent SAM identities and opaque queued blobs under:

```text
~/.deaddrop-server/
|-- identities/
|   |-- drop_0.dat
|   |-- drop_1.dat
|   `-- drop_2.dat
`-- storage/
    |-- drop_0/
    |-- drop_1/
    `-- drop_2/
```

On Windows, the equivalent location is
`%USERPROFILE%\.deaddrop-server`. Preserve this complete directory across
restarts to retain the same I2P destinations and queued blobs.

## Migrating From `.termchat-server`

Older releases used `.termchat-server` as the data directory. The directory can
be renamed without changing the persistent B32 addresses or stored blobs.

Before migrating:

1. Stop the old server completely.
2. Confirm that no server process is still running.
3. Confirm that `.deaddrop-server` does not already exist.

On Linux or macOS, run:

```bash
mv "$HOME/.termchat-server" "$HOME/.deaddrop-server"
```

On Windows PowerShell, run:

```powershell
Rename-Item -Path "$env:USERPROFILE\.termchat-server" -NewName ".deaddrop-server"
```

Start the updated server only after the rename completes. Move the entire
directory as one unit; do not move only `identities` or only `storage`.
