FROM mcr.microsoft.com/devcontainers/rust:1-1-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler clang pkg-config libssl-dev curl ca-certificates \
    && curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh \
    && rm -rf /var/lib/apt/lists/*
