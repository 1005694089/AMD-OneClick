# 2026-06-12 v2 Manager Stability and Deployment Rules

## Current stable production v2

- Deployment: `default/amd-oneclick-manager-v2`
- Current image: `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-emptydir-workspace-20260612-1510`
- Rollback image: `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-nodeport-racefix-auth-20260611-1655`
- Service: `default/amd-oneclick-manager-v2`
- NodePort: `30088`
- Production edge IP: `36.150.116.200`
- Current edge topology: `amd-oneclick-edge-nginx` pinned to `wx-ms-w7900d-0004`

Validation at time of writing:

- `http://radeon.anruicloud.com/` -> `200`
- `https://radeon.anruicloud.com/` -> `200`
- `http://36.150.116.200:30088/` -> `200`
- `amd-oneclick-manager-v2` -> `1/1 ready`
- `amd-oneclick-manager-v2` endpoints -> only Ready pod published

## 2026-06-12 15:41 emptyDir workspace rollout

Goal: remove persistent hostPath `/workspace` for new instances and use Kubernetes-managed ephemeral storage instead.

## Change inventory

The permanent deployment summary table is maintained in `docs/ops/deployment-summary.md`.

Local manifest snapshot:

- Source manifest: `k8s-deployment-v2.local.yaml`
- Snapshot: `local-deploy-history/v2/2026-06-12-1541-v2-emptydir-workspace.local.yaml`
- Snapshot SHA256: `39e09ed35ff8124af20b34954ce66cdc5b7909b5361eeb1d3eadb77438c3ed09`
- Snapshot directory is intentionally git-ignored because it contains local Secret values.

Applied configuration:

- `WORKSPACE_VOLUME_TYPE=emptyDir`
- `WORKSPACE_EMPTYDIR_SIZE_LIMIT=100Gi`
- `EPHEMERAL_STORAGE_REQUEST=20Gi`
- `EPHEMERAL_STORAGE_LIMIT=100Gi`
- `publishNotReadyAddresses` must remain unset/false.

Validation:

- `https://radeon.anruicloud.com/` -> `200`
- `http://36.150.116.200:30088/` -> `200`
- `amd-oneclick-manager-v2` -> `1/1 ready`

## Incident summary

Frequent `502 Bad Gateway` was caused by the production v2 Service routing to an unhealthy manager pod, not by the PR1 edge proxy itself.

Contributing factors:

- `publishNotReadyAddresses=true` was temporarily added to `amd-oneclick-manager-v2`, causing nginx to hit NotReady endpoints.
- A bad v2 rollout image/config attempted to start with mismatched Postgres/default-image state and crashed.
- Generic/local manifest drift made it unclear which image and Secret/ConfigMap values were authoritative.

Immediate recovery performed:

- Disabled publishing NotReady endpoints on `amd-oneclick-manager-v2`.
- Rolled production v2 back to stable image `v2-nodeport-racefix-auth-20260611-1655`.
- Restored `amd-oneclick-postgres` Secret values from the healthy running pod environment.
- Restored service availability and verified repeated 200 responses.

## Rollback command

```bash
export KUBECONFIG=/home/hfang/AMD-OneClick/cluster.yaml
kubectl -n default set image deployment/amd-oneclick-manager-v2 \
  manager=crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-nodeport-racefix-auth-20260611-1655
kubectl -n default patch svc amd-oneclick-manager-v2 --type=merge -p '{"spec":{"publishNotReadyAddresses":false}}'
kubectl -n default rollout status deployment/amd-oneclick-manager-v2 --timeout=240s
```

## Mandatory future rules

- Update the relevant `.local.yaml` first. It is the deploy source of truth.
- Never apply generic manifests with placeholder Secret values.
- Never use `kubectl patch` or `kubectl set image` as the final deployment path, except emergency rollback.
- Every emergency command must be copied into a dated `docs/ops/` record.
- Before applying any manifest that touches Secret/ConfigMap, compare live values and explicitly list intended changes.
- Do not set `publishNotReadyAddresses=true` for production managers unless there is a documented and reviewed reason.
- For edge changes, document existing listeners and IP ownership before applying.
- Manager-only deploys must never apply or mutate `amd-oneclick-postgres` Deployment, Service, Secret, or data directory unless the user explicitly requests a database change. Use a manager-only manifest or resource-scoped apply.
