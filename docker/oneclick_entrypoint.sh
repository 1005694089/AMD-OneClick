#!/usr/bin/env bash
set -euo pipefail

VLLM_PORT="${PADDLEX_ALL_IN_ONE_VLLM_SERVER_PORT:-8118}"
JUPYTER_PORT="${JUPYTER_PORT:-8888}"
NOTEBOOK_TOKEN="${NOTEBOOK_TOKEN:-amd-oneclick}"
MODEL_NAME="${MODEL_NAME:-PaddleOCR-VL-1.5-0.9B}"
GPU_MEMORY_UTILIZATION="${GPU_MEMORY_UTILIZATION:-0.85}"

echo "========================================"
echo "  PaddleOCR-VL OneClick Notebook"
echo "========================================"
echo "  vLLM Port: ${VLLM_PORT}"
echo "  Jupyter Port: ${JUPYTER_PORT}"
echo "  GPU Memory: ${GPU_MEMORY_UTILIZATION}"
echo "========================================"

mkdir -p /opt/PaddleX /app/notebooks /var/log

backend_config_path="/tmp/paddlex-vllm-backend.yaml"
printf 'gpu_memory_utilization: %s\n' "${GPU_MEMORY_UTILIZATION}" > "${backend_config_path}"

echo "[1/3] Starting vLLM server in background..."
nohup paddlex_genai_server \
    --model_name "${MODEL_NAME}" \
    --host 0.0.0.0 \
    --port "${VLLM_PORT}" \
    --backend vllm \
    --backend_config "${backend_config_path}" \
    > /var/log/paddlex_vllm_server.log 2>&1 &

VLLM_PID="$!"
echo "vLLM server started with PID: ${VLLM_PID}"
echo "vLLM logs: /var/log/paddlex_vllm_server.log"

NOTEBOOK_DIR="/workspace/PaddleX"

if [[ -n "${NOTEBOOK_URL:-}" ]]; then
    echo "[2/3] Downloading notebook from GitHub..."
    NOTEBOOK_DIR="/app/notebooks"
    if python - <<'PY'
import os, ssl
from urllib.parse import urlsplit
import urllib.request
url = os.environ["NOTEBOOK_URL"]
filename = os.path.basename(urlsplit(url).path) or "notebook.ipynb"
target = os.path.join("/app/notebooks", filename)
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE
with urllib.request.urlopen(url, context=ssl_ctx) as response, open(target, "wb") as f:
    f.write(response.read())
print(f"Downloaded: {target}")
PY
    then
        :
    else
        echo "Warning: failed to download notebook, falling back to /workspace/PaddleX"
        NOTEBOOK_DIR="/workspace/PaddleX"
    fi
else
    echo "[2/3] No notebook URL provided, using /workspace/PaddleX"
fi

echo "[3/3] Starting Jupyter Lab..."

if [[ -n "${INSTANCE_ID:-}" ]]; then
    BASE_URL="/instance/${INSTANCE_ID}/"
    echo "Jupyter base URL: ${BASE_URL}"
    exec jupyter lab \
        --ip=0.0.0.0 \
        --port="${JUPYTER_PORT}" \
        --no-browser \
        --allow-root \
        --ServerApp.token="${NOTEBOOK_TOKEN}" \
        --ServerApp.base_url="${BASE_URL}" \
        --notebook-dir="${NOTEBOOK_DIR}"
fi

exec jupyter lab \
    --ip=0.0.0.0 \
    --port="${JUPYTER_PORT}" \
    --no-browser \
    --allow-root \
    --ServerApp.token="${NOTEBOOK_TOKEN}" \
    --notebook-dir="${NOTEBOOK_DIR}"
