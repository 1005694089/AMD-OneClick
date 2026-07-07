# Hugging Face Demo API Quick Guide

This guide is for frontend developers integrating with the Radeon Global Hugging Face demo notebook API.

## Base URL

```text
https://radeon-global.anruicloud.com
```

If public DNS is not live yet, backend smoke tests can temporarily resolve the hostname to:

```text
36.150.116.206
```

## Authentication

Every Hugging Face demo API call requires a Radeon Global API bearer token:

```http
Authorization: Bearer <HUGGINGFACE_DEMO_API_TOKEN>
```

Do not put this token in a public frontend bundle. The recommended frontend flow is:

1. Frontend calls your own backend.
2. Your backend attaches the Radeon Global API bearer token.
3. Your backend calls the Radeon Global API.
4. Your backend returns the safe response payload to the frontend.

This bearer token is separate from the upstream Hugging Face access token. The upstream `HF_TOKEN` is configured server-side in the deployment and is used only by notebook pods when downloading `.ipynb` files through the internal Hugging Face proxy. Frontend code should never send or know the upstream `HF_TOKEN`.

## Credits

Each demo user is granted a small starting credit balance the first time they launch (one-time grant; it is **not** refilled on later launches). In production the starting grant is **10** credits (`HUGGINGFACE_DEMO_MIN_CREDITS`). Running instances are metered at **1 credit per GPU per hour**. When a user's balance is exhausted, their running instance is automatically destroyed. A launch is rejected with `400 Insufficient credits` if the balance is below the requested `gpu_count`.

### Metered vs unlimited

| | Metered (default) | Unlimited (`unlimited_credits: true`) |
|---|---|---|
| Starting balance | One-time grant (10 in prod) | Same one-time grant |
| Billing | 1 credit / GPU / hour deducted | **No deduction** — balance stays frozen |
| Destroy on zero credits | Yes | **No** |
| Launch when balance < `gpu_count` | Rejected (`400 Insufficient credits`) | **Allowed** (once flagged) |
| Idle reaper (8h no activity) | Yes | **Yes** — still applies |
| Revoke via API | N/A | **No** — sticky; omitting the flag on a later launch does not turn billing back on |

Pass `"unlimited_credits": true` on **`POST /api/huggingface/notebooks`** to mark that `user_name` as unlimited. The flag is written only after launch validation succeeds (active-instance guard, credits gate, etc.), so a rejected launch does **not** leave the flag behind. Usage is still recorded server-side (0-credit billing rows) for audit/telemetry, but the user's balance never decreases and the instance is never torn down for insufficient credits.

**When to use:** pass `unlimited_credits: true` for demo users who should run without a credit cap (e.g. internal testers, workshop hosts). The flag is **caller-controlled** — any holder of the shared API bearer token can set it for any `user_name`. There is no separate privileged token.

**Sticky behavior:** the first successful launch with `unlimited_credits: true` permanently marks that `user_name`. Later launches that omit the field remain unlimited. There is no API to revoke unlimited status.

**What unlimited does not do:** it does not bypass GPU capacity limits, the one-active-instance rule, or the **8-hour idle reaper** (`API_IDLE_TIMEOUT_MINUTES=480`). Idle instances are still auto-destroyed after 8h with no activity.

## List Available Images

Discover the images a notebook can launch with. This is the same enabled catalog the admin panel shows.

```http
GET /api/huggingface/images
Authorization: Bearer <token>
```

Example response:

```json
{
  "images": [
    {
      "name": "AMD OneClick Base",
      "image": "<registry>/amd-oneclick-base:<tag>",
      "description": "Default ROCm Jupyter/OpenCode image"
    }
  ],
  "default_image": "<registry>/amd-oneclick-base:<tag>"
}
```

When launching, the `image` field accepts **either** the friendly `name` (e.g. `"Huggingface"`, as configured in the admin panel — case-insensitive) **or** the full `image` ref. Omit `image` to use `default_image` (the API default is the Hugging Face image built for AMD Radeon).

## Check GPU Availability

Report free vs total GPUs reachable by this service's launches (not the whole cluster).

```http
GET /api/huggingface/gpus
Authorization: Bearer <token>
```

Example response:

```json
{
  "total_gpus": 982,
  "free_gpus": 981,
  "nodes": [
    { "node": "<node-name>", "total": 8, "free": 7, "committed": 1, "quarantined": false }
  ]
}
```

`free` is schedulable capacity (`total - committed`), not live utilization. Check this before launching multi-GPU instances.

## Launch A Notebook

```http
POST /api/huggingface/notebooks
Content-Type: application/json
Authorization: Bearer <token>
```

Request body (metered — default):

```json
{
  "user_name": "user-001",
  "notebook_path": "https://huggingface.co/Qwen/Qwen3.6-27B.ipynb",
  "gpu_count": 1
}
```

Request body (unlimited credits — balance frozen, billing skipped):

```json
{
  "user_name": "vip-workshop-host",
  "notebook_path": "https://huggingface.co/Qwen/Qwen3.6-27B.ipynb",
  "gpu_count": 1,
  "unlimited_credits": true
}
```

Optional fields (may be combined with either body above):

```json
{
  "image": "<allowed-notebook-image>",
  "pod_type": "workshop",
  "notebook_path": "org/private-repo.git",
  "git_token": "<github-token>",
  "unlimited_credits": false
}
```
Rules:

- `user_name` is required and is used as the stable demo user identity.
- `notebook_path` is **optional** and accepts three shapes:
  - **Omitted / empty** — a blank notebook environment is started with no file pre-loaded; the returned `url` opens the JupyterLab root.
  - **An `.ipynb` file** (a `github/org/repo/blob/branch/path.ipynb` path or a `huggingface.co` notebook URL) — the notebook is fetched server-side and opened in the launched notebook.
  - **A `.git` repo** (workshop only — see below) — the repo is cloned and JupyterLab opens at the repo root.
- **Workshop `.git` repos:** when `pod_type` is `workshop`, `notebook_path` may be a GitHub repository instead of a notebook file: `org/repo.git`, a full `https://github.com/org/repo.git` URL, or the scp form `git@github.com:org/repo.git`. Append `@branch` to pick a branch (e.g. `org/repo.git@dev`); with no `@branch` the repo's default branch is cloned. The repo is cloned into the workspace and JupyterLab opens at its root — **no `.ipynb` is required**. A `.git` value with any non-workshop `pod_type` (or none) is rejected with `400 A .git repo can only be launched with pod_type='workshop'`. Only GitHub repositories are accepted.
- **Private repos (`git_token`):** to clone a **private** GitHub repo, pass `git_token` (a GitHub personal-access / fine-grained token with read access to the repo) alongside a `.git` workshop launch. The token is used only for the clone and is handled so it never reaches the user's notebook environment: the authenticated clone runs in a dedicated init container, so the token is **not** present in the notebook container's env, **not** written into the cloned repo's `.git/config`, and **not** placed on any command line. `git_token` is honored **only** for a `.git` workshop launch — passing it on any other launch (`.ipynb`, blank, or a non-`workshop` `pod_type`) is rejected with `400 git_token is only supported for a .git workshop launch (pod_type='workshop')`. For safety the token is **only accepted when the repo resolves to an HTTPS clone endpoint**; if this deployment resolves GitHub over plain HTTP the request is rejected with `400 git_token requires an HTTPS clone endpoint` (configure an HTTPS GitHub proxy to use private-repo tokens). Public repos need no `git_token`.
- Hugging Face notebook URLs are downloaded server-side through the configured internal Hugging Face proxy and server-side `HF_TOKEN`.
- `image` is optional. Pass either the friendly `name` or the full `image` ref from `GET /api/huggingface/images` (name match is case-insensitive); anything not in the enabled catalog is rejected with `400 Invalid image selected`. Defaults to `default_image`.
- `pod_type` is optional. When provided it must be one of `hackathon`, `workshop`, or `one-click` (case-insensitive; stored lowercase); any other value is rejected with `400 Invalid pod_type`. Use it to tag instances by program. Omit it for an untagged instance.
- `gpu_count` must be `1`, `2`, or `4`. Default is `1`. CPU and memory scale automatically with the GPU count (see **GPU Sizing** below). For **metered** users, each GPU costs 1 credit/hour, so a 4-GPU instance consumes credits 4x as fast. **Unlimited** users are not charged regardless of `gpu_count`. Check `GET /api/huggingface/gpus` for free capacity before requesting `2` or `4`.
- `unlimited_credits` is optional, defaults to `false`. When `true` on a **successful** launch, this `user_name` is marked unlimited and its credit balance is frozen going forward (see **Credits** above). Sticky — cannot be unset by a later launch that omits it. A launch rejected with `400` (e.g. `Each user can only have one active instance`) does **not** apply the flag.
- Each `user_name` can have only one active notebook.

Example success response:

```json
{
  "status": "allocating",
  "message": "Allocating resources for the Hugging Face demo notebook...",
  "url": "https://radeon-global.anruicloud.com/instances/hf-4-xxxx/lab/tree/Qwen3.6-27B.ipynb?token=amd-oneclick",
  "email": "hf-xxxx@huggingface.oneclick.local",
  "instance_id": "hf-4-xxxx"
}
```

## GPU Sizing

CPU and memory are allocated automatically in proportion to `gpu_count` (each GPU node has 128 CPU / ~1007 GiB / 8 GPU, so each GPU's fair share is ~16 CPU / ~125 GiB):

| `gpu_count` | CPU (request / limit) | Memory (request / limit) |
|-------------|-----------------------|--------------------------|
| 1           | 8 / 16                | 48Gi / 110Gi             |
| 2           | 16 / 32               | 96Gi / 220Gi             |
| 4           | 32 / 64               | 192Gi / 440Gi            |

You do not set CPU/memory directly; they follow `gpu_count`. A larger instance needs more free GPUs on a single node, so a `4` request can be rejected if no node has 4 free GPUs even when the cluster total is higher — check `GET /api/huggingface/gpus` (the per-node `free` field) first.

## Poll Notebook Status

```http
GET /api/huggingface/notebooks/current?user_name=user-001
Authorization: Bearer <token>
```

Recommended polling interval: every `3-5` seconds.

Possible `status` values:

```text
not_found
allocating
pending
initializing
loading
running
jupyter_starting
ready
failed
unknown
```

When `status` is `ready`, open the returned `url`.

Example ready response:

```json
{
  "status": "ready",
  "message": "The notebook is ready",
  "url": "https://radeon-global.anruicloud.com/instances/hf-4-xxxx/lab/tree/Qwen3.6-27B.ipynb?token=amd-oneclick",
  "email": "hf-xxxx@huggingface.oneclick.local",
  "instance_id": "hf-4-xxxx"
}
```

For `pod_type: "hackathon"` instances, once `status` is `ready` the response also carries a
`streamlit_url` field — see **Run A Streamlit App** below.

## Run A Streamlit App (Hackathon Pods Only)

Hackathon instances (`pod_type: "hackathon"` at launch) can also run a user-started Streamlit
app alongside Jupyter, reachable through the same reverse proxy ComfyUI/Gradio app instances use
(`/spaces/<instance_id>/<port>/`). This is opt-in from inside the notebook — the manager does not
start Streamlit for you, it only prepares the pod so a bare `streamlit run` works and, once the
app answers on its port, tells you the URL.

**From a JupyterLab terminal in a hackathon instance:**

```bash
pip install streamlit   # if not already present in the image
streamlit run app.py --server.port 8501 --server.headless true \
  --server.enableCORS false --server.enableXsrfProtection false --server.fileWatcherType poll
```

Notes:

- The app **must** listen on port **8501** — it is the only port the proxy and the status API
  recognize for Streamlit. A different `--server.port` will not be reachable or reported.
- Hackathon pods pre-set `STREAMLIT_SERVER_ADDRESS`, `STREAMLIT_SERVER_PORT`, and
  `STREAMLIT_SERVER_BASE_URL_PATH` in the environment, so the flags above are for
  belt-and-suspenders clarity — a bare `streamlit run app.py` already binds correctly and serves
  under the right base path.
- `--server.fileWatcherType poll` avoids inotify issues on the notebook's mounted filesystem;
  keep it if your app hot-reloads on file changes.

**From your integration:** poll `GET /api/huggingface/notebooks/current` as usual. Once Streamlit
is actually accepting connections, the response includes:

```json
{
  "status": "ready",
  "streamlit_url": "https://radeon-global.anruicloud.com/spaces/hf-15-1cd747c1/8501/"
}
```

- `streamlit_url` is `null`/absent until the app is actually listening on 8501 — it is a live
  check on every poll, not a static URL derived from the instance id. It can flip back to `null`
  if the app stops (e.g. the user kills it or an error crashes the process).
- `streamlit_url` is **only ever populated for `pod_type: "hackathon"`** instances. Workshop,
  one-click, and untagged instances never get this field, even if something happens to be
  listening on 8501 inside them.
- The trailing `/` matters — Streamlit's static assets and its live-update WebSocket are resolved
  relative to that base path. Always open/redirect to the URL exactly as returned.

## Delete A Notebook

```http
DELETE /api/huggingface/notebooks/current?user_name=user-001
Authorization: Bearer <token>
```

Example response:

```json
{
  "success": true,
  "message": "Instance hf-4-xxxx destroyed",
  "destroyed_count": 1
}
```

## Recommended Frontend Flow

1. User clicks **Launch Notebook**.
2. Frontend sends `user_name`, `notebook_path`, and (if needed) `unlimited_credits` to your backend.
3. Backend calls `POST /api/huggingface/notebooks` with the bearer token.
4. Frontend shows a loading state.
5. Frontend polls `GET /api/huggingface/notebooks/current?user_name=...`.
6. When `status === "ready"`, frontend shows or opens `url`.
7. When the user ends the session, backend calls `DELETE /api/huggingface/notebooks/current?user_name=...`.

For **unlimited** users your backend should pass `"unlimited_credits": true` on the **first** successful launch for that `user_name` (or on every launch — re-sending `true` is a no-op once already flagged). Do not expose the API bearer token to the browser; keep the unlimited decision on your backend.

## Minimal TypeScript Types

```ts
export type LaunchRequest = {
  user_name: string;
  notebook_path?: string; // omit/empty = blank; .ipynb path/URL = open notebook; org/repo.git[@branch] = clone repo (pod_type "workshop" only)
  gpu_count?: 1 | 2 | 4;
  image?: string; // a value from GET /api/huggingface/images
  pod_type?: "hackathon" | "workshop" | "one-click";
  git_token?: string; // private-repo clone token; ONLY valid with a .git workshop launch over an HTTPS endpoint (rejected otherwise). Never reaches the notebook container.
  unlimited_credits?: boolean; // default false; when true on a successful launch, sticky-freezes this user_name's balance (billing skipped; 8h idle reaper still applies)
};

export type NotebookStatus = {
  status: string;
  message: string;
  url?: string;
  email?: string;
  instance_id?: string;
  streamlit_url?: string; // pod_type "hackathon" only; set once the user's Streamlit app (port 8501) is live, else omitted/null
};

export type DestroyResponse = {
  success: boolean;
  message: string;
  destroyed_count: number;
};

export type ImageCatalog = {
  images: { name: string; image: string; description: string }[];
  default_image: string;
};

export type GpuAvailability = {
  total_gpus: number;
  free_gpus: number;
  nodes: {
    node: string;
    total: number;
    free: number;
    committed: number;
    quarantined: boolean;
  }[];
};
```

## UX Notes

- If launch returns `400` with `Each user can only have one active instance`, call the status endpoint and show the existing notebook.
- If launch returns `400` with `Insufficient credits`, the user is out of credits — show their balance and stop offering launch. **Unlimited** users never hit this gate once flagged; pass `unlimited_credits: true` on launch for users who should not be credit-limited.
- Unlimited users still need GPUs: check `GET /api/huggingface/gpus` before launch. Unlimited does not bypass cluster capacity.
- Unlimited instances are still destroyed after **8 hours idle** — warn long-running demo users or call `DELETE` when the session ends.
- To build a launch form, call `GET /api/huggingface/images` for the image dropdown and `GET /api/huggingface/gpus` to show available capacity before submitting.
- If status stays `initializing` or `jupyter_starting`, keep polling.
- If status is `failed`, show an error and offer retry or cleanup.
- If the notebook URL opens but the expected `.ipynb` is missing, report it as a backend download issue. Frontend clients should not retry direct Hugging Face downloads themselves.
- Always call delete when the user explicitly ends the demo session.
- `streamlit_url` only ever appears for `pod_type: "hackathon"` launches, and only once the user has actually started Streamlit on port 8501 inside the instance — keep polling status and show/hide a "Open Streamlit app" action based on whether the field is present, rather than assuming it will show up right after launch.
