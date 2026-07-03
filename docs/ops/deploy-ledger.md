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

## 2026-07-03 — new-prod NFS per-user REAL hard quota (route A: project quota + root_squash)

**Scope:** storage layer only (NFS servers in new-prod cluster
`new_cluster_prod.yaml`, ns `amd-oneclick-storage`). No manager image change.
These NFS servers back per-user workspaces for radeon / radeon-test via the
cross-cluster StorageClasses `oneclick-newprod-nfs-1..4`.

**Goal:** enforce a real ~10Gi per-user hard cap (previously the NFS-subdir
provisioner set no quota; PVC size was advisory only).

| Component | Image (`:tag`) | Digest / ID | Local yaml + sha256 |
|-----------|----------------|-------------|---------------------|
| NFS server (nfs-cpu-1/2) | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/nfs-server-crossmnt:rootsquash-20260703` | digest `sha256:2cd44e5ad1bbaa3f5be6538015392bc710b9f981e7ff25acadb518456009ffb2` / id `sha256:93adc95efc07bf8ffb63387afa125801bd2b0f32f5ba08a1032e42c148bac809` | `nfs-quota.local.yaml` sha256 `c89080112178cd7833d8a252a34f9caa40134d044018cbdcbc77957a1b945816` |
| quota tooling (init + enforcer sidecar) | `crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/nfs-quota-tools:20260703` | digest `sha256:980b60e7a874d25d2aadf16af6eaf9b525774a29e1ea3f95ff35cd941b2c13b9` / id `sha256:6d4d9037aef2e95f19d74663f439ccefa70c3ca7e31c8d2881c26f4e6b388095` | (same manifest) |

Dockerfiles: `nfs-quota-tools/Dockerfile` sha256
`48613e2ea4baa1af137e72908788de8b969b680ef25bffab4ef231ce84e47618`;
`nfs-server-crossmnt/Dockerfile.rootsquash` sha256
`3bc54a967f70486d906654aadfa9e5613be0823cc9e112e9c854846aa1af8c94`.

**Key design findings (why it works now):**
- `tune2fs -O quota,project` on an EXISTING ext4 does NOT yield working quota
  (kernel mounts `Quota mode: none`, `prjquota` dropped). Must `mkfs.ext4 -O
  quota,project`. XFS needs no reformat (project quota is enabled at first
  mount via `-o prjquota`).
- Nodes need the `quota_v2` kernel module (`CONFIG_QFMT_V2=m`); init runs
  `modprobe quota_v2`.
- **root bypasses project quota** (`CAP_SYS_RESOURCE`). Since Jupyter runs
  `--allow-root`, the export MUST be `root_squash` (root→nobody) or quota is
  a no-op. Hence the `rootsquash-*` server image.

**What was done:**
- Built `nfs-quota-tools` (Ubuntu + xfsprogs/quota/e2fsprogs) for the init
  (mount) and a `quota-enforcer` sidecar. Kept the alpine nfs-server image
  (its EOL apk repos can't install quota pkgs) and only flipped the export
  template to `root_squash`.
- Reformatted the 3 ext4 disks `mkfs.ext4 -O quota,project`
  (nfs-cpu-1 disk0+disk1, nfs-cpu-2 disk1); nfs-cpu-2 disk0 (xfs) kept, first
  re-mounted `-o prjquota`. All 4 disks now mount with `prjquota`.
  - Data wiped was operator test data / stale junk only (incl. a live but
    self-owned test instance `u-1-258c504b`, a 208G `opencompass.tar`, and
    stale containerd/buildkit dirs on nfs-cpu-2 disk1). xfs disk0 data
    (vllm test artifacts, 69G) preserved.
- `quota-enforcer` scans `/exports/diskN/pvc-*` every 30s and stamps each PVC
  subdir with a unique project id (= its inode) + `WORKSPACE_QUOTA_HARD=10G`
  hard block limit (xfs via `xfs_quota`, ext4 via `chattr +P` + `setquota`).

**Verification (end-to-end, real NFS client, running as root → squashed to
nobody):** on BOTH ext4 (nfs-cpu-1, 10.5.10.15) and xfs (nfs-cpu-2,
10.5.10.19): writing 50M into a 20M-limited dir → `du` capped at 20M and
`dd` (conv=fsync) exits rc=1 (EDQUOT surfaced); O_DIRECT write blocks
immediately at the limit. Enforcer confirmed auto-stamping 10G on `pvc-*`
dirs (`repquota`/`xfs_quota report`). Both NFS pods 2/2 Running.

**Gotcha for future restarts:** the lablab DaemonSet
`oneclick-workspace-localssd-prep` (ns `amd-oneclick-lablab`) bind-mounts host
`/` and captures stale copies of these disk mounts in its mount namespace,
which can block a re-mount/mkfs. The init only does a host-ns umount; the
one-time transition here required killing the holder pids. Steady state (and
node reboots, where the NFS init mounts the disks first) is fine; if you ever
need to re-enable xfs quota after it reverts to `noquota`, fully release the
device (kill holders) before re-mounting `-o prjquota`.

---

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
