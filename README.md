# AMD OneClick Notebook Manager

Kubernetes-based Jupyter Notebook instance management with automatic lifecycle control.

## Features

- One-click notebook provisioning with email-based instance tracking
- AMD GPU support with automatic, GPU-proportional CPU/memory allocation (1, 2, or 4 GPUs)
- Admin panel for instance and image-catalog management
- Credit-based metering (1 credit per GPU-hour) with auto-destroy on exhaustion
- HuggingFace demo API for programmatic, token-authenticated launches
- Auto-cleanup: idle timeout and max-lifetime reaping (web and API instances)

## API Endpoints

### Web / Admin

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | User interface |
| POST | `/api/notebook/request` | Request notebook (session auth) |
| GET | `/api/notebook/status?email=xxx` | Check status |
| GET | `/admin` | Admin panel (user: `admin`) |
| GET | `/api/admin/instances` | List all instances |
| DELETE | `/api/admin/instance/{email}` | Destroy instance |
| DELETE | `/api/admin/instances/all` | Destroy all |
| GET | `/api/admin/images-list` | Enabled image catalog (admin Basic auth) |
| GET | `/api/admin/gpus` | Free vs total GPU capacity (admin Basic auth) |
| GET | `/health` | Health check |

### HuggingFace Demo API (Bearer token)

Programmatic surface for external callers. Full guide: [`docs/huggingface-demo-api.md`](docs/huggingface-demo-api.md).

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/huggingface/images` | List selectable images (name + ref) |
| GET | `/api/huggingface/gpus` | Free vs total GPUs reachable by launches |
| POST | `/api/huggingface/notebooks` | Launch a notebook |
| GET | `/api/huggingface/notebooks/current?user_name=xxx` | Poll status |
| DELETE | `/api/huggingface/notebooks/current?user_name=xxx` | Destroy |

Launch request highlights:

- `notebook_path` is optional — omit it for a blank JupyterLab; provide an `.ipynb` URL to pre-open it; or, with `pod_type` `workshop`, provide a `.git` repo (`org/repo.git`, optional `@branch`) to clone the repo and open JupyterLab at its root (no `.ipynb` needed).
- `image` accepts either the admin-panel **name** (e.g. `Huggingface`, case-insensitive) or the full registry ref; omit to use the API default.
- `gpu_count` is `1`, `2`, or `4` (default `1`). CPU/memory scale automatically with the GPU count.
- `pod_type` is an optional tag: `hackathon`, `workshop`, or `one-click`.

Notebook **templates** (admin/Gallery) may likewise point at a GitHub repo with no notebook path — the repo is cloned and JupyterLab opens at its root.

## Resource Sizing

CPU and memory scale with `gpu_count` (sized to the GPU nodes: 128 CPU / ~1007 GiB / 8 GPU each):

| `gpu_count` | CPU (request / limit) | Memory (request / limit) |
|-------------|-----------------------|--------------------------|
| 1 | 8 / 16 | 48Gi / 110Gi |
| 2 | 16 / 32 | 96Gi / 220Gi |
| 4 | 32 / 64 | 192Gi / 440Gi |

## Deployment

```bash
# Deploy to K8s
kubectl apply -f k8s-deployment.yaml

# Access
# User: http://<NODE_IP>:30080/
# Admin: http://<NODE_IP>:30080/admin
```

Manager deployments to the radeon-beta stack are recorded in [`docs/ops/deploy-ledger.md`](docs/ops/deploy-ledger.md).

## Local Development

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

## Configuration

Environment variables (set in the deployment manifest / ConfigMap):

| Variable | Description |
|----------|-------------|
| `SERVICE_HOST` | External IP for notebook URLs |
| `DEFAULT_IMAGE` | Default notebook container image (web UI) |
| `HUGGINGFACE_DEMO_DEFAULT_IMAGE` | Default image for API launches when `image` is omitted |
| `HUGGINGFACE_DEMO_API_TOKENS` | Comma-separated bearer tokens for the HuggingFace demo API |
| `HUGGINGFACE_DEMO_MIN_CREDITS` | One-time starting credit grant for new API users |
| `GPU_LIMIT` | GPUs per notebook |
| `IDLE_TIMEOUT_MINUTES` | Idle timeout before cleanup (web instances) |
| `MAX_LIFETIME_HOURS` | Maximum notebook lifetime |
| `API_IDLE_TIMEOUT_MINUTES` | Idle timeout for API-launched instances (default 480 = 8h) |
| `IDLE_REAPER_INTERVAL_MINUTES` | How often the idle reaper runs |
| `RUN_SCHEDULER` | Enable the billing + idle-reaper scheduler |
| `ADMIN_PASSWORD` | Admin panel password |
