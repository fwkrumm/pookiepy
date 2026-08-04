# Rust BaseServer Port

Async Rust implementation of BaseServer from pookiepy. Wire protocol stays compatible with Python clients because server uses same gRPC service and message schema from pookiepy/message.proto.

This Rust port was generated with AI assistance. Treat it as working draft code and plan human review before relying on it for long-term maintenance or production use.

## What this port includes

- Async bidirectional streaming server with tonic + tokio
- Welcome-first handshake compatible with Python BaseClient
- Requires-based fan-out routing with per-message subscriptions
- Metadata key x-schema-version handling
- Manual schema version check via ServerConfig.schema_version
- Graceful shutdown trigger
- Optional gzip compression on both directions

## Folder layout

- .rust/Cargo.toml
- .rust/build.rs
- .rust/src/lib.rs
- .rust/src/server.rs
- .rust/src/data_register.rs
- .rust/src/schema_version.rs
- .rust/src/main.rs
- .rust/.devcontainer/devcontainer.json
- .rust/.devcontainer/Dockerfile

## Workspace isolation

Open VS Code in .rust folder, not repository root. This keeps Rust devcontainer isolated from root Python devcontainer.

Linux or macOS:

```bash
cd .rust && code .
```

Windows PowerShell:

```powershell
Set-Location .rust; code .
```

## Install prerequisites

### Linux (Debian/Ubuntu)

1. Install system packages.
2. Install Rust toolchain.

Commands:

```bash
sudo apt-get update && sudo apt-get install -y build-essential pkg-config libssl-dev protobuf-compiler clang
curl https://sh.rustup.rs -sSf | sh -s -- -y
```

### Windows (PowerShell)

1. Install Rust toolchain.
2. Install protoc.
3. Ensure C++ build tools present.

Commands:

```powershell
winget install -e --id Rustlang.Rustup
winget install -e --id protobuf.protobuf
winget install -e --id Microsoft.VisualStudio.2022.BuildTools
```

For Visual Studio Build Tools, enable workload: Desktop development with C++.

## Build

From repository root:

```bash
cd .rust && cargo build
```

## Run server

Default bind address is [::]:50051.

```bash
cd .rust && cargo run
```

Custom bind address and server name:

```bash
cd .rust && pookiepy_RUST_ADDR='[::]:50051' pookiepy_RUST_NAME='server-rs' cargo run
```

On Windows PowerShell:

```powershell
Set-Location .rust; $env:pookiepy_RUST_ADDR='[::]:50051'; $env:pookiepy_RUST_NAME='server-rs'; cargo run
```

## Test with Python client

Start Rust server, then run any existing Python client example against same port.

Example:

```powershell
Set-Location ..; uv run python examples/interactive_streaming/run_text_client.py
```

## Notes on compatibility

- Package name is message.proto.v3, same as Python proto.
- First client message must contain metaInfo.clientInfo.
- Server sends welcome message with metaInfo.serverInfo before require registration side effects are visible.
- If both client and server schema strings are empty, connection continues and server logs cannot check schema because empty.
- If at least one side sets schema string, both values must match exactly or server rejects with FAILED_PRECONDITION.

## ToDos

- Extend test coverage with Python client examples.
