FROM ubuntu:jammy

ARG DEBIAN_FRONTEND=noninteractive

# Install dependencies
# -o Acquire::http::No-Cache=True prevents stale CDN responses
# -o Acquire::http::Pipeline-Depth=0 prevents partial/inconsistent downloads
RUN apt-get update \
        -o Acquire::http::No-Cache=True \
        -o Acquire::http::Pipeline-Depth=0 \
    && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    build-essential \
    pkg-config \
    protobuf-compiler \
    libssl-dev \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/local/bin/python

# Install uv (fast Python package manager)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# Install Rust (stable toolchain: rustc/cargo/rustup)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Set the default shell to Bash
SHELL ["/bin/bash", "-c"]
