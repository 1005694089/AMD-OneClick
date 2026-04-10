#!/usr/bin/env bash
#
# verify_inference.sh - Runtime verification for PaddleOCR-VL ROCm inference
#
# Follows the procedures in PaddleOCR-VL-1.5 精度速度测试文档:
#   --mode quick            Single-image smoke test (native + vLLM)
#   --mode precision-native Full native precision (1355 images)
#   --mode precision-vllm   Full vLLM precision (1355 images)
#   --mode speed-vllm       vLLM speed benchmark
#   --mode all              All three full tests sequentially
#
# Requires: --device /dev/kfd --device /dev/dri --group-add video

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: verify_inference.sh [options]

Options:
  --mode MODE        quick|precision-native|precision-vllm|speed-vllm|all
                     Default: quick
  --output-dir DIR   Output directory. Default: /workspace/verify_output
  --server-port PORT vLLM server port. Default: 8118
  --images-dir DIR   Precision dataset directory.
                     Default: /opt/paddlex/datasets/images
  --pdfs-dir DIR     Speed dataset directory.
                     Default: /opt/paddlex/datasets/omni1_5_pdfs
  --benchmark-root   Benchmark root directory.
                     Default: /opt/paddlex/benchmarks
  --device DEVICE    PaddleOCR device (cpu|dcu|gpu). Default: cpu
  --batch-size N     Batch size for speed benchmark. Default: 512
  -h, --help         Show this help.
EOF
}

mode="quick"
output_dir="/workspace/verify_output"
server_port="${PADDLEX_ALL_IN_ONE_VLLM_SERVER_PORT:-8118}"
images_dir="/opt/paddlex/datasets/images"
pdfs_dir="/opt/paddlex/datasets/omni1_5_pdfs"
benchmark_root="/opt/paddlex/benchmarks"
client_device="dcu"
batch_size="512"
test_image="/opt/PaddleX/test/paddleocr_vl_demo.png"

overall_rc=0
native_status="not_run"
vllm_status="not_run"
speed_status="not_run"
server_pid=""

while (($# > 0)); do
    case "$1" in
        --mode)           mode="$2";           shift 2 ;;
        --output-dir)     output_dir="$2";     shift 2 ;;
        --server-port)    server_port="$2";    shift 2 ;;
        --images-dir)     images_dir="$2";     shift 2 ;;
        --pdfs-dir)       pdfs_dir="$2";       shift 2 ;;
        --benchmark-root) benchmark_root="$2"; shift 2 ;;
        --device)         client_device="$2";  shift 2 ;;
        --batch-size)     batch_size="$2";     shift 2 ;;
        -h|--help)        usage; exit 0 ;;
        *)                echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

server_url="http://127.0.0.1:${server_port}/v1"
mkdir -p "${output_dir}"

cleanup() {
    if [[ -n "${server_pid}" ]]; then
        kill "${server_pid}" 2>/dev/null || true
        server_pid=""
    fi
    echo ""
    echo "========================================"
    echo "  Verification Summary"
    echo "========================================"
    echo "  Native precision: ${native_status}"
    echo "  vLLM precision:   ${vllm_status}"
    echo "  Speed benchmark:  ${speed_status}"
    echo "  Overall:          $([ ${overall_rc} -eq 0 ] && echo PASS || echo FAIL)"
    echo "  Output dir:       ${output_dir}"
    echo "========================================"
}
trap cleanup EXIT

# ----------------------------------------------------------------
# Preflight: verify paddle ROCm support
# ----------------------------------------------------------------
preflight() {
    echo "[preflight] Checking PaddlePaddle ROCm support..."
    python -c "
import paddle
v = paddle.__version__
rocm = paddle.is_compiled_with_rocm()
print(f'  Paddle version: {v}')
print(f'  ROCm compiled:  {rocm}')
assert rocm, 'PaddlePaddle is NOT compiled with ROCm'
"
    echo "[preflight] Checking PaddleOCR-VL imports..."
    python -c "from paddleocr import PaddleOCRVL; print('  PaddleOCRVL OK')"
    echo "[preflight] All checks passed."
}

# ----------------------------------------------------------------
# vLLM server lifecycle
# ----------------------------------------------------------------
wait_for_server() {
    local max_wait=180
    local elapsed=0
    echo "[server] Waiting for vLLM server at ${server_url} (up to ${max_wait}s)..."
    while (( elapsed < max_wait )); do
        if curl -fsS "${server_url}/models" >/dev/null 2>&1; then
            echo "[server] vLLM server ready (${elapsed}s)."
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo "[server] ERROR: vLLM server did not become ready within ${max_wait}s" >&2
    return 1
}

start_server() {
    if curl -fsS "${server_url}/models" >/dev/null 2>&1; then
        echo "[server] vLLM server already running at ${server_url}"
        return 0
    fi

    local log="${output_dir}/vllm_server.log"
    local backend_cfg="/tmp/paddlex-vllm-backend.yaml"
    echo "gpu_memory_utilization: ${GPU_MEMORY_UTILIZATION:-0.85}" > "${backend_cfg}"

    echo "[server] Starting vLLM server on port ${server_port}..."
    nohup paddlex_genai_server \
        --model_name PaddleOCR-VL-1.5-0.9B \
        --host 0.0.0.0 \
        --port "${server_port}" \
        --backend vllm \
        --backend_config "${backend_cfg}" \
        > "${log}" 2>&1 &
    server_pid="$!"

    if ! wait_for_server; then
        echo "[server] Server log tail:" >&2
        tail -30 "${log}" >&2 || true
        return 1
    fi
}

# ----------------------------------------------------------------
# Artifact counting
# ----------------------------------------------------------------
count_artifacts() {
    local root="$1"
    if [[ ! -d "${root}" ]]; then
        echo 0; return
    fi
    find "${root}" -type f \( -name "*.json" -o -name "*.md" \) ! -path "*/imgs/*" | wc -l
}

# ----------------------------------------------------------------
# Mode: quick (single-image smoke test)
# ----------------------------------------------------------------
run_quick() {
    echo ""
    echo "========================================"
    echo "  Quick Smoke Test"
    echo "========================================"

    if [[ ! -f "${test_image}" ]]; then
        echo "[quick] ERROR: test image not found: ${test_image}" >&2
        overall_rc=1; return
    fi

    local quick_dir="${output_dir}/quick"
    mkdir -p "${quick_dir}/native" "${quick_dir}/vllm"

    echo "[quick] Testing native inference..."
    set +e
    python -c "
from paddleocr import PaddleOCRVL
pipeline = PaddleOCRVL(device='${client_device}')
output = pipeline.predict('${test_image}')
for res in output:
    res.save_to_json('${quick_dir}/native')
    res.save_to_markdown('${quick_dir}/native', pretty=False)
print('Native inference OK')
" > "${quick_dir}/native.log" 2>&1
    rc=$?
    set -e

    if [[ ${rc} -eq 0 ]]; then
        native_status="passed"
        echo "[quick] Native: PASSED"
    else
        native_status="failed"
        overall_rc=1
        echo "[quick] Native: FAILED (see ${quick_dir}/native.log)"
        tail -20 "${quick_dir}/native.log" || true
    fi

    echo "[quick] Testing vLLM inference..."
    if ! start_server; then
        vllm_status="failed-server"
        overall_rc=1
        echo "[quick] vLLM: FAILED (server did not start)"
        return
    fi

    set +e
    python -c "
from paddleocr import PaddleOCRVL
pipeline = PaddleOCRVL(
    vl_rec_backend='vllm-server',
    vl_rec_server_url='${server_url}',
    device='${client_device}',
)
output = pipeline.predict('${test_image}')
for res in output:
    res.save_to_json('${quick_dir}/vllm')
    res.save_to_markdown('${quick_dir}/vllm', pretty=False)
print('vLLM inference OK')
" > "${quick_dir}/vllm.log" 2>&1
    rc=$?
    set -e

    if [[ ${rc} -eq 0 ]]; then
        vllm_status="passed"
        echo "[quick] vLLM: PASSED"
    else
        vllm_status="failed"
        overall_rc=1
        echo "[quick] vLLM: FAILED (see ${quick_dir}/vllm.log)"
        tail -20 "${quick_dir}/vllm.log" || true
    fi
}

# ----------------------------------------------------------------
# Mode: precision-native (test doc section 2.1)
# ----------------------------------------------------------------
run_precision_native() {
    echo ""
    echo "========================================"
    echo "  Precision Test: Native Inference"
    echo "========================================"

    local out="${output_dir}/paddle_acc_output"
    local log="${output_dir}/precision_native.log"
    mkdir -p "${out}"

    if [[ ! -d "${images_dir}" ]]; then
        echo "[native] ERROR: images dir not found: ${images_dir}" >&2
        native_status="failed-no-data"; overall_rc=1; return
    fi

    echo "[native] Running PaddleOCRVL on ${images_dir}..."
    set +e
    python -c "
from paddleocr import PaddleOCRVL

pipeline = PaddleOCRVL(device='${client_device}')
output = pipeline.predict('${images_dir}')

for res in output:
    res.save_to_json('${out}')
    res.save_to_markdown('${out}', pretty=False)
" > "${log}" 2>&1
    rc=$?
    set -e

    local count
    count=$(count_artifacts "${out}")

    if [[ ${rc} -eq 0 ]]; then
        native_status="passed (${count} files)"
        echo "[native] PASSED: ${count} output files"
    else
        native_status="failed"
        overall_rc=1
        echo "[native] FAILED (see ${log})"
        tail -20 "${log}" || true
        return
    fi

    echo "[native] Packing results..."
    tar -czf "${output_dir}/paddle_acc_output.tar.gz" \
        --exclude="paddle_acc_output/imgs" \
        -C "${output_dir}" "paddle_acc_output"
    echo "[native] Archive: ${output_dir}/paddle_acc_output.tar.gz"
}

# ----------------------------------------------------------------
# Mode: precision-vllm (test doc section 2.2)
# ----------------------------------------------------------------
run_precision_vllm() {
    echo ""
    echo "========================================"
    echo "  Precision Test: vLLM Inference"
    echo "========================================"

    local out="${output_dir}/vllm_acc_output"
    local log="${output_dir}/precision_vllm.log"
    mkdir -p "${out}"

    if [[ ! -d "${images_dir}" ]]; then
        echo "[vllm-prec] ERROR: images dir not found: ${images_dir}" >&2
        vllm_status="failed-no-data"; overall_rc=1; return
    fi

    if ! start_server; then
        vllm_status="failed-server"; overall_rc=1; return
    fi

    echo "[vllm-prec] Running PaddleOCRVL with vLLM backend on ${images_dir}..."
    set +e
    python -c "
from paddleocr import PaddleOCRVL

pipeline = PaddleOCRVL(
    vl_rec_backend='vllm-server',
    vl_rec_server_url='${server_url}',
    device='${client_device}',
)
output = pipeline.predict('${images_dir}')

for res in output:
    res.save_to_json('${out}')
    res.save_to_markdown('${out}', pretty=False)
" > "${log}" 2>&1
    rc=$?
    set -e

    local count
    count=$(count_artifacts "${out}")

    if [[ ${rc} -eq 0 ]]; then
        vllm_status="passed (${count} files)"
        echo "[vllm-prec] PASSED: ${count} output files"
    else
        vllm_status="failed"
        overall_rc=1
        echo "[vllm-prec] FAILED (see ${log})"
        tail -20 "${log}" || true
        return
    fi

    echo "[vllm-prec] Packing results..."
    tar -czf "${output_dir}/vllm_acc_output.tar.gz" \
        --exclude="vllm_acc_output/imgs" \
        -C "${output_dir}" "vllm_acc_output"
    echo "[vllm-prec] Archive: ${output_dir}/vllm_acc_output.tar.gz"
}

# ----------------------------------------------------------------
# Mode: speed-vllm (test doc section 3)
# ----------------------------------------------------------------
run_speed_vllm() {
    echo ""
    echo "========================================"
    echo "  Speed Benchmark: vLLM"
    echo "========================================"

    local log="${output_dir}/speed_vllm.log"

    if ! start_server; then
        speed_status="failed-server"; overall_rc=1; return
    fi

    local benchmark_e2e_dir
    benchmark_e2e_dir="$(find "${benchmark_root}" -maxdepth 5 -type f -name "test_local.py" -path "*/e2e/*" | head -n1)"
    if [[ -z "${benchmark_e2e_dir}" ]]; then
        echo "[speed] ERROR: benchmark e2e/test_local.py not found under ${benchmark_root}" >&2
        speed_status="failed-no-benchmark"; overall_rc=1; return
    fi
    benchmark_e2e_dir="$(dirname "${benchmark_e2e_dir}")"

    # Resolve the actual PDFs path (may be nested)
    local resolved_pdfs="${pdfs_dir}"
    if [[ -d "${pdfs_dir}/omni1_5/pdfs" ]]; then
        resolved_pdfs="${pdfs_dir}/omni1_5/pdfs"
    elif [[ -d "${pdfs_dir}/pdfs" ]]; then
        resolved_pdfs="${pdfs_dir}/pdfs"
    fi

    # Patch the benchmark config to point at the local vLLM server
    local config_path="${benchmark_e2e_dir}/PaddleOCR-VL-1_5_vllm.yaml"
    python -c "
import yaml
src = '/opt/paddlex/configs/PaddleOCR-VL-1.5.vllm-server.local.yaml'
dst = '${config_path}'
with open(src, 'r') as f:
    payload = yaml.safe_load(f)
vl = payload['SubModules']['VLRecognition']
vl['genai_config']['server_url'] = '${server_url}'
payload['use_layout_detection'] = False
payload['use_doc_preprocessor'] = False
with open(dst, 'w') as f:
    yaml.safe_dump(payload, f, allow_unicode=False, sort_keys=False)
"

    echo "[speed] Running benchmark from ${benchmark_e2e_dir}..."
    echo "[speed] PDFs: ${resolved_pdfs}, batch_size: ${batch_size}, device: ${client_device}"

    set +e
    (
        cd "${benchmark_e2e_dir}" \
        && pip install -r requirements.txt > /dev/null 2>&1 \
        && python test_local.py "${resolved_pdfs}" \
            -b "${batch_size}" \
            --paddlex_config_path "${config_path}" \
            --device "${client_device}"
    ) > "${log}" 2>&1
    rc=$?
    set -e

    if [[ ${rc} -eq 0 ]]; then
        speed_status="passed"
        echo "[speed] PASSED"
        echo "[speed] Results:"
        grep -E "Throughput|latency|tokens" "${log}" || tail -10 "${log}"
    else
        speed_status="failed"
        overall_rc=1
        echo "[speed] FAILED (see ${log})"
        tail -20 "${log}" || true
    fi
}

# ----------------------------------------------------------------
# Main dispatch
# ----------------------------------------------------------------
preflight

case "${mode}" in
    quick)
        run_quick
        ;;
    precision-native)
        run_precision_native
        ;;
    precision-vllm)
        run_precision_vllm
        ;;
    speed-vllm)
        run_speed_vllm
        ;;
    all)
        run_precision_native
        run_precision_vllm
        run_speed_vllm
        ;;
    *)
        echo "Unknown mode: ${mode}" >&2
        usage >&2
        exit 2
        ;;
esac

exit "${overall_rc}"
