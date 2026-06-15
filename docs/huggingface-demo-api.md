# Hugging Face Demo API Quick Guide

This guide is for frontend developers integrating with the Radeon Beta Hugging Face demo notebook API.

## Base URL

```text
https://radeon-beta.anruicloud.com
```

If public DNS is not live yet, backend smoke tests can temporarily resolve the hostname to:

```text
36.150.116.220
```

## Public DNS Setup

Create or update this DNS record in the DNS provider for `anruicloud.com`:

| Type | Host/Name | Value | TTL |
| --- | --- | --- | --- |
| `A` | `radeon-beta` | `36.150.116.220` | `300` seconds recommended |

The full public hostname should resolve as:

```text
radeon-beta.anruicloud.com -> 36.150.116.220
```

Do not use an underscore hostname such as `radeon_beta.anruicloud.com`.

After saving the DNS record, verify it from a terminal:

```bash
getent ahostsv4 radeon-beta.anruicloud.com
dig +short radeon-beta.anruicloud.com A
curl -k https://radeon-beta.anruicloud.com/health
```

Expected health response:

```json
{"status":"healthy"}
```

If DNS has not propagated yet, verify the edge route directly with a temporary local resolve:

```bash
curl -k --resolve radeon-beta.anruicloud.com:443:36.150.116.220 \
  https://radeon-beta.anruicloud.com/health
```

Once the plain `curl -k https://radeon-beta.anruicloud.com/health` command returns healthy without `--resolve`, public DNS is live.

## Authentication

Every Hugging Face demo API call requires a bearer token:

```http
Authorization: Bearer <HUGGINGFACE_DEMO_API_TOKEN>
```

Do not put this token in a public frontend bundle. The recommended frontend flow is:

1. Frontend calls your own backend.
2. Your backend attaches the bearer token.
3. Your backend calls the Radeon Beta API.
4. Your backend returns the safe response payload to the frontend.

## Launch A Notebook

```http
POST /api/huggingface/notebooks
Content-Type: application/json
Authorization: Bearer <token>
```

Request body:

```json
{
  "user_name": "user-001",
  "notebook_path": "https://huggingface.co/Qwen/Qwen3.6-27B.ipynb",
  "gpu_count": 1
}
```

Optional field:

```json
{
  "image": "<allowed-notebook-image>"
}
```

Rules:

- `user_name` is required and is used as the stable demo user identity.
- `notebook_path` must point to an `.ipynb` file.
- `gpu_count` must be `1`, `2`, or `4`. Default is `1`.
- Each `user_name` can have only one active notebook.

Example success response:

```json
{
  "status": "allocating",
  "message": "Allocating resources for the Hugging Face demo notebook...",
  "url": "https://radeon-beta.anruicloud.com/instances/hf-4-xxxx/lab/tree/Qwen3.6-27B.ipynb?token=amd-oneclick",
  "email": "hf-xxxx@huggingface.oneclick.local",
  "instance_id": "hf-4-xxxx"
}
```

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
  "url": "https://radeon-beta.anruicloud.com/instances/hf-4-xxxx/lab/tree/Qwen3.6-27B.ipynb?token=amd-oneclick",
  "email": "hf-xxxx@huggingface.oneclick.local",
  "instance_id": "hf-4-xxxx"
}
```

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
2. Frontend sends `user_name` and `notebook_path` to your backend.
3. Backend calls `POST /api/huggingface/notebooks` with the bearer token.
4. Frontend shows a loading state.
5. Frontend polls `GET /api/huggingface/notebooks/current?user_name=...`.
6. When `status === "ready"`, frontend shows or opens `url`.
7. When the user ends the session, backend calls `DELETE /api/huggingface/notebooks/current?user_name=...`.

## Minimal TypeScript Types

```ts
export type NotebookStatus = {
  status: string;
  message: string;
  url?: string;
  email?: string;
  instance_id?: string;
};

export type DestroyResponse = {
  success: boolean;
  message: string;
  destroyed_count: number;
};
```

## UX Notes

- If launch returns `400` with `Each user can only have one active instance`, call the status endpoint and show the existing notebook.
- If status stays `initializing` or `jupyter_starting`, keep polling.
- If status is `failed`, show an error and offer retry or cleanup.
- Always call delete when the user explicitly ends the demo session.
