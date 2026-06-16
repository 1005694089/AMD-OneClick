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

## Authentication

Every Hugging Face demo API call requires a Radeon Beta API bearer token:

```http
Authorization: Bearer <HUGGINGFACE_DEMO_API_TOKEN>
```

Do not put this token in a public frontend bundle. The recommended frontend flow is:

1. Frontend calls your own backend.
2. Your backend attaches the Radeon Beta API bearer token.
3. Your backend calls the Radeon Beta API.
4. Your backend returns the safe response payload to the frontend.

This bearer token is separate from the upstream Hugging Face access token. The upstream `HF_TOKEN` is configured server-side in the beta deployment and is used only by notebook pods when downloading `.ipynb` files through the internal Hugging Face proxy. Frontend code should never send or know the upstream `HF_TOKEN`.

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
- Hugging Face notebook URLs are downloaded server-side through the configured internal Hugging Face proxy and server-side `HF_TOKEN`.
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
- If the notebook URL opens but the expected `.ipynb` is missing, report it as a backend download issue. Frontend clients should not retry direct Hugging Face downloads themselves.
- Always call delete when the user explicitly ends the demo session.
