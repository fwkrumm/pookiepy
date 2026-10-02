FROM ubuntu:jammy

ARG DEBIAN_FRONTEND=noninteractive
ARG CMAKE_VERSION=3.31.12

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
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/local/bin/python

# Install the requested CMake release outside Ubuntu's older package repository.
RUN curl -fsSL "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz" -o /tmp/cmake.tar.gz && mkdir -p /opt/cmake && tar -xzf /tmp/cmake.tar.gz --strip-components=1 -C /opt/cmake && rm /tmp/cmake.tar.gz

# Install uv (fast Python package manager)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/opt/cmake/bin:/root/.local/bin:${PATH}"

# Set the default shell to Bash
SHELL ["/bin/bash", "-c"]
