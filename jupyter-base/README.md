# AMD ROCm OneClick Base Image

Base image for all AMD OneClick instance types: Jupyter, OpenCode, and OpenCLAW.

## Included Tools

- **Jupyter Lab** — Interactive development environment
- **Rust / Cargo** — Rust toolchain via rustup
- **Node.js 20** — JavaScript runtime
- **OpenCode CLI** — AI coding agent for the terminal
- **Data Science** — matplotlib, pandas, numpy, scipy, scikit-learn, seaborn, plotly

## Base Image

`rocm/vllm-dev:rocm7.2.1_navi_ubuntu24.04_py3.12_pytorch_2.9_vllm_0.16.0`

Includes ROCm 7.2.1, PyTorch 2.9, vLLM 0.16.0, Python 3.12, Ubuntu 24.04.

## Build & Push

```bash
docker build -t radeon-cloud-registry.cn-shanghai.cr.aliyuncs.com/admin/amd-oneclick-base:rocm7.2.1-py3.12-v20260416 .
docker push radeon-cloud-registry.cn-shanghai.cr.aliyuncs.com/admin/amd-oneclick-base:rocm7.2.1-py3.12-v20260416
```
