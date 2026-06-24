# Deploy Ledger (台账)

Authoritative, committed record of every manager deployment. This file MUST be
committed & pushed as part of every deploy (see
`.cursor/rules/deploy-ledger-commit.mdc`).

Raw manifests (`*.local.yaml`, `cluster.yaml`) and `local-deploy-history/` are
**git-ignored on purpose** — some embed raw Secrets (e.g. `GITHUB_CLIENT_SECRET`,
private keys). Do NOT commit them. Instead, each entry below pins the exact local
yaml version by `sha256` so it stays auditable/tamper-evident without leaking
secrets.

## Entry schema (every deploy adds one row per service)

| Field | Meaning |
|-------|---------|
| Date | Local time of the deploy (UTC+8) |
| Service / Port | e.g. `v2 prod` / 30088, `v2 test` / 30288, `pr1-zijun` / 30392 |
| Code commit | Git SHA the deployed image was built from (must be pushed) |
| Image | Full image ref `:tag` |
| Image digest | Registry manifest digest `sha256:…` |
| Image ID | Local image ID `sha256:…` |
| Local yaml | Manifest file applied + its `sha256` |
| Snapshot | Path under `local-deploy-history/` (git-ignored) |
| Notes | What changed / verification result |

---

## 2026-06-24 — Radeon beta oauth-credit-manager merge

**Code commit:** `e7df4328f0dce4ebde449ab7558bb9912c823505`
("Preserve beta node-local service overrides"), built on top of merge commit
`4ade5fcd8358c269cf913e6abfbaef31138a9f72`.

| Service / Port | Image (`:tag`) | Digest / ID | Local yaml + sha256 | Snapshot |
|----------------|----------------|-------------|---------------------|----------|
| radeon-beta / 30444 | `crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/radeon-cloud/amd-oneclick:rb-nodelocal-v2-052512` plus `amd-oneclick-radeon-beta-code-overrides` | registry digest N/A (image unchanged) / runtime image id `sha256:a28ec14b4f4b6f40dee8b6f8a61101ea2a85e97516da7f2cf29540f5138420ac` | generated code-overrides manifest sha256 `2c436b568f2ba0f532652de51940a7d108fd03df36ccef100344fcf708305b55` | `local-deploy-history/radeon-beta/2026-06-24-1710-beta-oauth-merge-code-overrides.local.yaml` |

**Process:** pulled `BETA-test`, merged `origin/feature/oauth-credit-manager`,
pushed `BETA-test`, then refreshed only
`amd-oneclick-radeon-beta-code-overrides` and rolled
`deployment/amd-oneclick-radeon-beta-manager`. Used `kubectl replace` for the
large ConfigMap to avoid the Kubernetes annotation size limit from
`kubectl apply`. No Postgres, Secret, builder, or production resources were
mutated.

**Verification:** rollout `1/1 ready`; `https://radeon-beta.anruicloud.com/health`
and `http://36.150.116.220:30444/health` returned 200; public homepage returned
200 and contains merged deploy-type UI; Playwright browser load had no console
errors or failed critical asset requests; mounted pod file hashes match local
`e7df432`; builder pod can reach its configured manager URL.

## 2026-06-24 — image prepull sync fix (IfNotPresent + ref normalization)

**Code commit:** `d8f0cea8ee9f830dda1e243eb1f0eb62198d47af`
("Fix image prepull sync: use IfNotPresent and normalize image refs")
built on top of `705fda2` (proxy-pool fix).

| Service / Port | Image (`:tag`) | Digest / ID | Local yaml + sha256 | Snapshot |
|----------------|----------------|-------------|---------------------|----------|
| v2 test / 30288 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-test-imgsync-fix-20260624-1500` | digest `sha256:de2ce10383d2611ee5509ad2b3fb5f19140c6889b33348d61003fe6b59377357` / id `sha256:73aa935bbe8a69c8984ca3bc87744803568ebd4c017e5f722a4b52ca2067d8ad` | `k8s-manager-v2-test.local.yaml` sha256 `913d2b91b94aa75447679fe527de6799a178ded137e44ff90dc50f6636500683` | `local-deploy-history/v2-test/2026-06-24-1500-v2-test-imgsync-prepull-fix.local.yaml` |
| v2 prod / 30088 | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick:v2-imgsync-prepull-fix-20260624-1535` | digest `sha256:de2ce10383d2611ee5509ad2b3fb5f19140c6889b33348d61003fe6b59377357` / id `sha256:73aa935bbe8a69c8984ca3bc87744803568ebd4c017e5f722a4b52ca2067d8ad` | `k8s-manager-v2-manager-only.local.yaml` sha256 `1d279c528691f1362154ff290d55a45bcd6186ef8b74d052c6d807d05fe0769d` | `local-deploy-history/v2-manager-only/2026-06-24-1538-v2-manager-imgsync-prepull-fix.local.yaml` |

**Process:** built from working tree → pushed to ACR → pre-imported on
`wx-ms-w7900d-0005` → `kubectl diff` confirmed image-only change vs live →
`kubectl apply` → bounded rollout (old pod kept serving until new ready).
Prod and test share the same image digest (prod tag is a retag of the validated
test image).

**Verification:** `/health=200` on both; image sync for catalog 52/53/54
(`rocm/atom-dev`, `vllm/vllm-openai-rocm`, `rocm/pytorch`) went `0/22` → `ready
21/22` (DaemonSet 25/27).

**Side task:** the two private images were node-to-node synced (ctr export/import
over cluster net) to the 5 missing nodes `0004/0005/0006/0008/0042`; `0008` used
a locally-cached base image with `imagePullPolicy: Never` (it cannot pull busybox
from ACR).
