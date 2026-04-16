# AMD ROCm Base Image
# Pre-installed with Jupyter Lab, Rust/Cargo, and OpenCode CLI
FROM docker.io/rocm/vllm-dev:rocm7.2.1_navi_ubuntu24.04_py3.12_pytorch_2.9_vllm_0.16.0

LABEL maintainer="AMD OneClick Team"
LABEL description="AMD ROCm base image with Jupyter Lab, Rust, and OpenCode"

ENV DEBIAN_FRONTEND=noninteractive
ENV SHELL=/bin/bash

WORKDIR /app

# Install system dependencies (Node.js 20.x, build tools)
RUN rm -rf /var/lib/apt/lists/* && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get update && apt-get install -y --no-install-recommends \
    nodejs \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust via rustup (available for all users)
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH="/usr/local/cargo/bin:${PATH}"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable && \
    chmod -R a+rw $RUSTUP_HOME $CARGO_HOME

# Install OpenCode CLI (try install script, fallback to npm)
RUN curl -fsSL https://raw.githubusercontent.com/sst/opencode/refs/heads/dev/install | bash || \
    npm install -g opencode-ai

# Install Jupyter and common data science packages
RUN pip install --no-cache-dir \
    jupyter \
    jupyterlab \
    ihighlight \
    ipywidgets \
    matplotlib \
    pandas \
    numpy \
    scipy \
    scikit-learn \
    seaborn \
    plotly \
    tqdm \
    && jupyter lab build --minimize=False 2>/dev/null || true

RUN mkdir -p /app/notebooks

WORKDIR /app/notebooks

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
