# PaddleOCR-VL Docker Images

本目录包含 PaddleOCR-VL OneClick 服务的所有 Docker 镜像构建文件。

## 镜像层级

```
┌─────────────────────────────────────────────────────────────────┐
│  rocm/vllm-dev:nightly_main_20260125  (ROCm 7.0 + vLLM)        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  vivienfanghua/vllm_paddle:base  (~26GB)                       │
│  Dockerfile.base                                                │
│  ─────────────────────────────────────────────────────────────  │
│  + PaddlePaddle DCU (ROCm 7.0)                                  │
│  + PaddleX + OCR 依赖                                           │
│  + 配置文件                                                      │
│  - 无 entrypoint (通用基础镜像)                                   │
│  - 无模型文件                                                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  vivienfanghua/vllm_paddle:ppocr-oneclick  (~38GB)             │
│  Dockerfile.ppocr-oneclick                                      │
│  ─────────────────────────────────────────────────────────────  │
│  + Jupyter Lab                                                  │
│  + 模型文件 (checkpoint-5000, layout_0116)                       │
│  + oneclick_entrypoint.sh                                       │
│  用于 K8s OneClick 服务                                          │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/... │
│  paddleocr-vl:latest-amd-all-in-one                             │
│  (预构建 All-in-One 基础镜像)                                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  vivienfanghua/vllm_paddle:all-in-one                           │
│  Dockerfile.all-in-one                                           │
│  ─────────────────────────────────────────────────────────────  │
│  + 复用 Aliyun all-in-one 全量环境                                │
│  + 预置 notebook: ppocr_vl_demo.ipynb                           │
│  + 默认启动 oneclick_entrypoint.sh (vLLM + Jupyter)             │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  rocm/vllm-dev:rocm7.2_navi  (ROCm 7.2 + vLLM + RDNA4)        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  vivienfanghua/vllm_paddle:rocm-all-in-one                      │
│  Dockerfile.rocm-all-in-one                                      │
│  ─────────────────────────────────────────────────────────────  │
│  + PaddlePaddle ROCm (3.4.0-dev, WITH_ROCM=ON, gfx1201)        │
│  + PaddleX + OCR + Jupyter + paddleocr                          │
│  + 测试数据集 (精度 1355 images + 速度 PDFs + benchmark)          │
│  + verify_inference.sh (native + vLLM + speed 验证)              │
│  + oneclick_entrypoint.sh (vLLM + Jupyter)                      │
│  推荐用于 AMD RDNA4 GPU                                          │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  vivienfanghua/amd-ppocr-vl-manager:latest  (~209MB)           │
│  Dockerfile.manager                                             │
│  ─────────────────────────────────────────────────────────────  │
│  FROM python:3.12-slim                                          │
│  + FastAPI + K8s 客户端                                          │
│  OneClick 管理服务                                               │
└─────────────────────────────────────────────────────────────────┘
```

## 构建镜像

### 使用构建脚本

```bash
# 构建所有镜像
./docker/build.sh all

# 只构建基础镜像
./docker/build.sh base

# 只构建 OneClick 镜像 (需要先构建 base)
./docker/build.sh oneclick

# 只构建 All-in-One 镜像
./docker/build.sh allinone

# 构建 ROCm All-in-One 镜像 (需要先放置 wheel)
cp /path/to/paddlepaddle_dcu-*.whl docker/
./docker/build.sh rocm-allinone

# 只构建 Manager 镜像
./docker/build.sh manager

# 推送所有镜像到 Docker Hub
./docker/build.sh push
```

### 手动构建

```bash
cd /path/to/AMD-OneClick

# 1. 构建基础镜像
docker build -f docker/Dockerfile.base -t vivienfanghua/vllm_paddle:base docker/

# 2. 构建 OneClick 镜像
docker build -f docker/Dockerfile.ppocr-oneclick -t vivienfanghua/vllm_paddle:ppocr-oneclick docker/

# 3. 构建 All-in-One 镜像 (基于 Aliyun 预构建镜像)
docker build -f docker/Dockerfile.all-in-one -t vivienfanghua/vllm_paddle:all-in-one .

# 4. 构建 ROCm All-in-One 镜像 (PaddlePaddle ROCm + 测试数据)
cp /path/to/paddlepaddle_dcu-*.whl docker/
docker build -f docker/Dockerfile.rocm-all-in-one -t vivienfanghua/vllm_paddle:rocm-all-in-one .

# 5. 构建 Manager 镜像
docker build -f docker/Dockerfile.manager -t vivienfanghua/amd-ppocr-vl-manager:latest .
```

## 目录结构

```
docker/
├── Dockerfile.base              # 基础镜像 (Paddle DCU + PaddleX)
├── Dockerfile.ppocr-oneclick    # OneClick 镜像 (+ Jupyter + 模型)
├── Dockerfile.all-in-one        # All-in-One 镜像 (+ 预置 notebook)
├── Dockerfile.rocm-all-in-one   # ROCm All-in-One 镜像 (ROCm wheel + 测试数据)
├── Dockerfile.manager           # Manager 服务镜像
├── build.sh                     # 构建脚本
├── verify_inference.sh          # 运行时推理验证脚本
├── models/                      # 预下载的模型文件
│   ├── checkpoint-5000/         # PaddleOCR-VL 模型
│   └── layout_0116/             # Layout 检测模型
├── paddlepaddle_dcu-*.whl       # PaddlePaddle ROCm/DCU wheel
└── README.md                    # 本文档
```

## 环境变量

### OneClick 镜像 (`ppocr-oneclick`)

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `GPU_MEMORY_UTILIZATION` | `0.85` | GPU 显存使用比例 |
| `VLLM_PORT` | `8118` | vLLM 服务端口 |
| `JUPYTER_PORT` | `8888` | Jupyter Lab 端口 |
| `NOTEBOOK_TOKEN` | `amd-oneclick` | Jupyter 访问 token |
| `NOTEBOOK_URL` | - | 自动下载的 notebook URL |
| `INSTANCE_ID` | - | K8s 实例 ID (用于 Nginx 代理) |

### All-in-One 镜像 (`all-in-one`)

`all-in-one` 默认继承并使用与 `ppocr-oneclick` 一致的运行时环境变量：
`GPU_MEMORY_UTILIZATION`、`PADDLEX_ALL_IN_ONE_VLLM_SERVER_PORT`、`JUPYTER_PORT`、`NOTEBOOK_TOKEN`、`NOTEBOOK_URL`、`INSTANCE_ID`。

预置 notebook 路径：

`/workspace/PaddleX/notebooks/ppocr_vl_demo.ipynb`

### ROCm All-in-One 镜像 (`rocm-all-in-one`)

与 `all-in-one` 使用相同运行时环境变量，另外支持：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `GPU_MEMORY_UTILIZATION` | `0.85` | GPU 显存使用比例 |
| `PADDLEX_ALL_IN_ONE_VLLM_SERVER_PORT` | `8118` | vLLM 服务端口 |
| `JUPYTER_PORT` | `8888` | Jupyter Lab 端口 |
| `NOTEBOOK_TOKEN` | `amd-oneclick` | Jupyter 访问 token |

内置数据与工具：
- 精度数据集：`/opt/paddlex/datasets/images` (1355 张)
- 速度数据集：`/opt/paddlex/datasets/omni1_5_pdfs`
- Benchmark 脚本：`/opt/paddlex/benchmarks`
- 验证脚本：`/opt/PaddleX/verify_inference.sh`

## 本地运行

```bash
# 运行 OneClick 镜像 (需要 AMD GPU)
docker run -it --rm \
  -p 8888:8888 \
  -p 8118:8118 \
  --device /dev/kfd \
  --device /dev/dri \
  --group-add video \
  -e GPU_MEMORY_UTILIZATION=0.85 \
  vivienfanghua/vllm_paddle:ppocr-oneclick

# 访问 Jupyter Lab: http://localhost:8888/?token=amd-oneclick
```

```bash
# 运行 All-in-One 镜像 (需要 AMD GPU)
docker run -it --rm \
  -p 8888:8888 \
  -p 8118:8118 \
  --device /dev/kfd \
  --device /dev/dri \
  --group-add video \
  -e GPU_MEMORY_UTILIZATION=0.85 \
  vivienfanghua/vllm_paddle:all-in-one

# Notebook: /workspace/PaddleX/notebooks/ppocr_vl_demo.ipynb
```

```bash
# 运行 ROCm All-in-One 镜像 (AMD RDNA4 GPU)
docker run -it --rm \
  -p 8888:8888 \
  -p 8118:8118 \
  --device /dev/kfd \
  --device /dev/dri \
  --group-add video \
  -e GPU_MEMORY_UTILIZATION=0.85 \
  vivienfanghua/vllm_paddle:rocm-all-in-one
```

## 推理验证 (ROCm All-in-One)

ROCm All-in-One 镜像内置了 `verify_inference.sh` 验证脚本，
按照 PaddleOCR-VL-1.5 精度速度测试文档的流程执行验证。

```bash
# 快速冒烟测试 (单张图片, ~2 min)
docker run --rm --device /dev/kfd --device /dev/dri --group-add video \
  vivienfanghua/vllm_paddle:rocm-all-in-one \
  /opt/PaddleX/verify_inference.sh --mode quick

# Native 精度测试 (1355 images)
docker run --rm --device /dev/kfd --device /dev/dri --group-add video \
  vivienfanghua/vllm_paddle:rocm-all-in-one \
  /opt/PaddleX/verify_inference.sh --mode precision-native

# vLLM 精度测试 (1355 images)
docker run --rm --device /dev/kfd --device /dev/dri --group-add video \
  vivienfanghua/vllm_paddle:rocm-all-in-one \
  /opt/PaddleX/verify_inference.sh --mode precision-vllm

# vLLM 速度测试
docker run --rm --device /dev/kfd --device /dev/dri --group-add video \
  vivienfanghua/vllm_paddle:rocm-all-in-one \
  /opt/PaddleX/verify_inference.sh --mode speed-vllm

# 全部测试 (精度 native + 精度 vLLM + 速度)
docker run --rm --device /dev/kfd --device /dev/dri --group-add video \
  -v /workspace/results:/workspace/verify_output \
  vivienfanghua/vllm_paddle:rocm-all-in-one \
  /opt/PaddleX/verify_inference.sh --mode all --output-dir /workspace/verify_output
```

预期输出产物：
- `paddle_acc_output.tar.gz` -- 2710 个文件 (1355 json + 1355 md)
- `vllm_acc_output.tar.gz` -- 2710 个文件
- 速度报告：throughput (files/s, pages/s, tokens/s)

## K8s 部署

请参考 `k8s-ppocr-deployment.yaml` 和 `nginx-proxy.yaml`。

