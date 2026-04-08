# AMD ROCm OneClick Base Image

Base image for all AMD OneClick instance types: Jupyter, OpenCode, and OpenCLAW.

## Included Tools

- **Jupyter Lab** — Interactive development environment
- **Rust / Cargo** — Rust toolchain via rustup
- **Node.js 20** — JavaScript runtime
- **OpenCode CLI** — AI coding agent for the terminal
- **Data Science** — matplotlib, pandas, numpy, scipy, scikit-learn, seaborn, plotly

## Base Image

`rocm/vllm-dev:rocm7.1.1_navi_ubuntu24.04_py3.12_pytorch_2.8_vllm_0.10.2rc1`

Includes ROCm 7.1.1, PyTorch 2.8, vLLM 0.10.2rc1, Python 3.12, Ubuntu 24.04.

## Build & Push

```bash
docker build -t crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick-base:latest .
docker push crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick-base:latest
```
