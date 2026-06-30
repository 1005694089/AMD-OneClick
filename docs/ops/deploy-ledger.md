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

## 2026-06-30 (latest) — Radeon beta: select launch image by admin-panel name

**Commit:** `d7375a5` on `BETA-test` (pushed to origin). API callers can now pass
the friendly image NAME (e.g. `Huggingface`) in the `image` field, not just the
full registry ref.

**What shipped:** new `store.resolve_enabled_image(value)` matches an ENABLED
catalog row by ref (priority) or case-insensitive name (deterministic, ordered by
id to avoid ambiguity if a case-variant duplicate name ever exists). The HF launch
handler resolves the caller's value and launches with the real ref; unknown/disabled
→ `400 Invalid image selected`. Reviewed by a 2-dimension adversarial workflow
(correctness / ambiguity-security): 1 low finding (name-match determinism) fixed
before deploy.

**Deploy mechanism:** patched 2 keys (`store.py`, `main.py`) in
`amd-oneclick-radeon-beta-code-overrides` (`kubectl patch --type merge`) +
`rollout restart`. No config-CM or Postgres change.

| Service / Port | Image (`:tag`) | Code-overrides sha256 | Rollback snapshot (sha256, git-ignored) |
|----------------|----------------|-----------------------|------------------------------------------|
| radeon-beta / 30444 | `…:radeon-beta-image-service-20260625` (image unchanged) + CMs | `7c250eeff05a135f634b4f9e1a58312d14a01c9ad575710dfe32ff057a74a359` | `local-deploy-history/radeon-beta/20260630-1659-imgbyname-PRE-code-overrides.yaml` (`34ef4b45…`) |

**Verified live e2e:** clean startup; launch by name `Huggingface` resolved to
`…/huaggingface_for_amd_radeon:latest` and `comfy-ui` to `…/comfyui:latest` (DB
refs match catalog exactly); an unknown name → 400; both test instances destroyed,
0 residue. Full pytest suite: 149 passed.

---

## 2026-06-30 — Radeon beta: evidence-based multi-GPU resource sizing

**Commit:** `79b0f33` on `BETA-test` (pushed to origin). Re-sized the auto
resource profiles so API multi-GPU launches (gpu_count 1/2/4) get CPU/RAM matched
to the real node hardware.

**Evidence (measured live on GPU nodes wx-ms-w7900d-0043/0044):** 128 CPU,
~1007.5 GiB allocatable RAM, 8 amd.com/gpu, ~0 reserved overhead. GPU is the
binding constraint (8/node) → per-GPU fair share ≈ 16 CPU / 125 GiB.

**New profiles** (CPU unchanged from before; only RAM raised to use the node
fully with eviction margin — per-GPU 48Gi req / 110Gi limit = 88% of fair share):

| gpu_count | profile  | CPU req/lim | RAM req/lim   |
|-----------|----------|-------------|---------------|
| 1         | standard | 8 / 16      | 48Gi / 110Gi  |
| 2         | large    | 16 / 32     | 96Gi / 220Gi  |
| 4         | xlarge   | 32 / 64     | 192Gi / 440Gi |

Full node bin-packs by requests (8×48=384 ≤ 1007 GiB) and stays under allocatable
at limits (8×110=880 ≤ 1007 GiB, ~13% headroom). Reviewed by a 3-dimension
adversarial workflow (capacity-math / correctness / ops-safety): 0 real defects.

**Deploy mechanism:** patched 1 key (`k8s_client.py`) in
`amd-oneclick-radeon-beta-code-overrides` (`kubectl patch --type merge`) +
`rollout restart`. No config-CM or Postgres change.

| Service / Port | Image (`:tag`) | Code-overrides sha256 | Rollback snapshot (sha256, git-ignored) |
|----------------|----------------|-----------------------|------------------------------------------|
| radeon-beta / 30444 | `…:radeon-beta-image-service-20260625` (image unchanged) + CMs | `34ef4b45297ef16f1f1e76ece35c64d2e97f71373681801559a841cf6197cf61` | `local-deploy-history/radeon-beta/20260630-1646-gpusizing-PRE-code-overrides.yaml` (`fd7ef44f…`) |

**Verified live e2e:** clean startup; a 2-GPU launch (`simtest-gpu2-ed`) got
profile=large (CPU 16/32, RAM 96Gi/220Gi) and a 4-GPU launch (`simtest-gpu4-fi`)
got profile=xlarge (CPU 32/64, RAM 192Gi/440Gi); both reached Running 1/1
co-scheduled on one node (6 GPUs, no overcommit/eviction), then destroyed. 0
residue. Full pytest suite: 143 passed.

---

## 2026-06-30 — Radeon beta: API default image = Radeon HuggingFace

**Commit:** `502c12c` on `BETA-test` (pushed to origin). Adds
`HUGGINGFACE_DEMO_DEFAULT_IMAGE` (env-overridable, default
`crpi-ygzb1jbfyj9pjrm6.cn-shenzhen.personal.cr.aliyuncs.com/images_hana/huaggingface_for_amd_radeon:latest`).
The HF launch handler uses it when the caller omits `image`, and
`/api/huggingface/images` reports it as `default_image`. Independent of the web
UI `DEFAULT_IMAGE`. The image already exists enabled in the catalog (name
"Huggingface"), so launches validate.

**Deploy mechanism:** patched 2 keys (`config.py`, `main.py`) in
`amd-oneclick-radeon-beta-code-overrides` (`kubectl patch --type merge`) +
`rollout restart`. No config-CM or Postgres change.

| Service / Port | Image (`:tag`) | Code-overrides sha256 | Rollback snapshot (sha256, git-ignored) |
|----------------|----------------|-----------------------|------------------------------------------|
| radeon-beta / 30444 | `…:radeon-beta-image-service-20260625` (image unchanged) + CMs | `fd7ef44f63c4f9f2ec47ce0cf6b9de4906f738a79c41bc0b3ab82938f0ccde80` | `local-deploy-history/radeon-beta/20260630-1607-apidefault-PRE-code-overrides.yaml` (`570efae4…`) |

**Verified live:** clean startup; `/api/huggingface/images` `default_image` =
the Radeon HuggingFace ref; a launch omitting `image` (`simtest-apidefault-x`)
recorded instance `hf-51-4027fb8a` with that exact image, then destroyed. Full
pytest suite: 141 passed.

---

## 2026-06-30 — Radeon beta: fix hf_initial_grant ledger delta

**Commit:** `00282b3` on `BETA-test` (pushed to origin). Found by a live
multi-agent API-user simulation (4 personas, 28 steps): the once-only HF credit
grant wrote a `credit_ledger` row with `delta=0`, so the ledger did not reconcile
with `users.credits` (sum 0 vs balance 10). Fixed to record the real movement
(`amount - prior_balance`); marker presence still enforces once-only.

**Deploy mechanism:** patched 1 key (`store.py`) in
`amd-oneclick-radeon-beta-code-overrides` (`kubectl patch --type merge`) +
`rollout restart`. No config change. Postgres untouched.

| Service / Port | Image (`:tag`) | Code-overrides sha256 | Rollback snapshot (sha256, git-ignored) |
|----------------|----------------|-----------------------|------------------------------------------|
| radeon-beta / 30444 | `…:radeon-beta-image-service-20260625` (image unchanged) + CMs | `570efae441a333de9f4a4343e44800cbe89bd5eefd72fded32c23c9813dc7717` | `local-deploy-history/radeon-beta/20260630-1552-ledger-fix-PRE-code-overrides.yaml` (`037f994d…`) |

**Verified live:** fresh launch `simtest-ledgerfix-dan` → user 50 `credits=10`
with `hf_initial_grant` ledger `delta=10` (reconciles); instance destroyed. Full
pytest suite: 139 passed (grant-once test strengthened to assert ledger delta).
Note: pre-existing HF users (43–49) created before this fix still carry the
delta=0 grant row — balances are correct; only their historical ledger row is
understated. No backfill applied.

---

## 2026-06-30 — Radeon beta: scope billing to API instances, enable scheduler

**Commit:** `3eb3b1d` on `BETA-test` (pushed to origin). Makes credit metering
apply only to API-launched instances, then turns the scheduler on for beta.

**What shipped:**
- **Billing scoped to API instances.** New `instance_records.api_launched` column
  (BOOLEAN NOT NULL DEFAULT FALSE, additive ALTER); `list_active_instances()` (the
  billing loop's source) now filters `api_launched IS TRUE`. `record_instance`
  gained an `api_launched` param, set True only on the HF launch path. Existing
  web/template rows default FALSE, so enabling the scheduler does NOT retroactively
  bill or kill them.
- **Config flips on beta** (`amd-oneclick-radeon-beta-config`):
  `HUGGINGFACE_DEMO_MIN_CREDITS` 48→10, `RUN_SCHEDULER` false→true. Billing
  (`cleanup_job`) and the idle reaper (`idle_reaper_job`) now run every 1m/5m.

**Deploy mechanism:**
- **Manager:** patched 2 keys (`store.py`, `main.py`) in
  `amd-oneclick-radeon-beta-code-overrides` (`kubectl patch --type merge`); the
  live-only data_mounts integration in `config/k8s_client/models.py` was left
  intact (those keys not touched). Config CM patched for the two env flips.
  `rollout restart` + status wait. Postgres Deployment/Service/Secret untouched.

| Service / Port | Image (`:tag`) | Code-overrides sha256 | Config sha256 | Rollback snapshots (sha256, git-ignored) |
|----------------|----------------|-----------------------|---------------|------------------------------------------|
| radeon-beta / 30444 | `…/amd-oneclick:radeon-beta-image-service-20260625` (image unchanged) + CMs | `037f994d56810e0a75410986b4953b9fe917261c60d6bec18c5c111322dcb4d9` | `56cf0cc0ebe793b1f3bcf292f8957e83dcf7adc47237970e9dc2eb217449b57c` | `local-deploy-history/radeon-beta/20260630-1532-billing-scope-PRE-code-overrides.yaml` (`290f96c5…`), `…-PRE-config.yaml` (`4deaf162…`) |

**Verified live:** scheduler running (logs show "Bill running instances per
GPU-hour" + "Auto-destroy idle and expired instances" jobs executing every minute,
"No instances to clean up"); aditya's pre-existing web instance `u-11-3414db96`
(opencode, 4d) **not billed and still Running** (api_launched FALSE); a fresh HF
launch `billtest-aa` → user funded at exactly **10** credits with one
`hf_initial_grant` marker, instance `hf-45-0ea74586` recorded `api_launched=t`
`pod_type=hackathon` (so billing will meter it), then destroyed via the DELETE
endpoint. Full pytest suite: 139 passed.

---

## 2026-06-30 — Radeon beta: HuggingFace external-API features (6)

**Commit:** `9bbac49` on `BETA-test` (pushed to origin). Adds six external/HF-API
features so the demo API is self-serve and correctly metered.

**What shipped:**
- **Blank `notebook_path`** launches a bare Jupyter (no repo clone).
- **Image discovery** endpoints: `GET /api/huggingface/images` (HF bearer) +
  `GET /api/admin/images-list` (admin Basic), enabled catalog only.
- **HF credits stick:** the starting floor is granted exactly once via a
  `credit_ledger` marker (`hf_initial_grant`) — race-safe across concurrent
  first-launches, never re-topped after spend-down. Repo default floor 48→8
  (note: beta `amd-oneclick-radeon-beta-config` overrides
  `HUGGINGFACE_DEMO_MIN_CREDITS=48`, so the effective beta floor is still 48 —
  change the ConfigMap to apply 8 on beta). One-time backfill
  (`hf_backfill_cap_v1`) caps inflated `huggingface_demo` balances to the floor;
  on beta it ran as a no-op at cap=48 (29 users already at 48).
- **Idle reaper fixed** to key on the real instance id (not `nb-{md5(email)}`),
  wired into the scheduler as `idle_reaper_job`; API-launched pods auto-destroyed
  after 8h idle (`API_IDLE_TIMEOUT_MINUTES=480`), uptime fallback only on empty
  logs, transient log-read errors skip the tick. **Note:** beta runs
  `RUN_SCHEDULER=false`, so neither billing nor the reaper executes on beta until
  that flag is flipped.
- **pod_type tag** (`hackathon`/`workshop`/`one-click`) validated + persisted to
  `instance_records`/`instance_launch_events` (new `pod_type` column + ALTER) and
  set as the `amd-oneclick/pod-type` annotation, on both HF and standard paths.
- **GPU availability:** `GET /api/huggingface/gpus` + `GET /api/admin/gpus`,
  free/total over launch-eligible nodes.

**Deploy mechanism:**
- **Manager:** patched 6 keys (`config/k8s_client/main/models/scheduler/store.py`)
  in the existing `amd-oneclick-radeon-beta-code-overrides` ConfigMap (other 15
  keys byte-preserved), `kubectl patch --type merge` + `rollout restart`. Live CM
  was confirmed byte-identical to committed HEAD (27c8093) before the patch, so no
  drift was reconciled. The live-only `data_mounts.py` mount + `data-mounts-root`
  volume were left untouched. Postgres Deployment/Service/Secret untouched.

| Service / Port | Image (`:tag`) | Code-overrides sha256 (`kubectl get cm -o yaml \| sha256sum`) | Rollback snapshot (sha256, git-ignored) |
|----------------|----------------|-----------------------|------------------------------------------|
| radeon-beta / 30444 | `crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/radeon-cloud/amd-oneclick:radeon-beta-image-service-20260625` (image unchanged) + `code-overrides` ConfigMap | `ac9890dee3cb1d0baa09c2ee96646d63cad9d3fe466061f5dc6d65c0caf1e944` | `local-deploy-history/radeon-beta/20260630-1450-hf-api-PRE-code-overrides.yaml` (`90d5bafef26ee9104559d73f8654a3671eeec5728d087c09642eea544c054918`) |

**Verified live:** manager pod clean startup (no traceback); `pod_type` columns
migrated on beta Postgres; `hf_backfill_cap_v1` marker written; endpoints e2e:
`/api/huggingface/images` 200 + 401 unauth, `/api/huggingface/gpus` 200
(`total_gpus:16, free_gpus:15`), admin variants 200 + 401 unauth, bad `pod_type`
→ 400; a blank-path `pod_type=workshop` launch created pod `hf-43-96b719c3` with
`api-launched=true` + `pod-type=workshop` annotations and no github annotation,
DB row `pod_type=workshop`, exactly one `hf_initial_grant` marker — then destroyed
cleanly via the DELETE endpoint. Full pytest suite: 138 passed.

---

## 2026-06-29 — Radeon beta: remove prepull, deadlock-resistant image-service

**Commit:** `572d4b2` on `BETA-test` (pushed to origin). Fixes the containerd
per-layer-chain unpack-mutex deadlock (`unpack.lockSnChainID`) by making the
image-service the ONLY image-management system and hardening it.

**What shipped (4 parts):**
- **A. Prepull removed.** The legacy prepull-DaemonSet / pull-probe path is gone
  (`k8s_client.sync_image_to_nodes` now only enqueues a `distribute` job;
  custom-image delete enqueues an `evict` job; `IMAGE_PREPULL_ENABLED` /
  `IMAGE_PULL_PROBE_*` config + `image_pull_probe_enabled` stat removed). No
  manager process ever creates an image-pull DaemonSet, so a prepull pull can no
  longer co-tenant a node and race a same-digest image-service import.
- **B. Deadlock-resistant imports (agent.py).** Remote `ctr import` wrapped in
  `timeout -s KILL` (`REMOTE_IMPORT_TIMEOUT_SECONDS=1200`); containerd liveness
  probe before/after import; per-node import serialization (in-process lock + a
  queue-level guard in `claim_next_image_job`); `importing` `image_nodes` status.
- **C. Pod-safe recovery.** Automated `systemctl restart containerd` never runs on
  a node with >0 running user pods (live count via new
  `/api/internal/nodes/{node}/user-pods`; unknown count fails safe to
  "pods present"); default `AUTO_CONTAINERD_RESTART_ENABLED=0` (quarantine + alert).
- **D. No retry-into-wedged-node.** Wedged nodes are quarantined (additive
  `image_nodes.quarantined_until`); the reaper drops quarantined targets (fails the
  job if all are quarantined); `_select_target_gpu_node` routes around them;
  quarantine clears on a successful reload; a stale evict is superseded by a newer
  rebuild of the same ref.

**Deploy mechanism:**
- **Manager:** patched 7 keys (`main/store/k8s_client/config/models.py`,
  `admin/profile.html`) in the existing `amd-oneclick-radeon-beta-code-overrides`
  ConfigMap (other 14 keys byte-preserved), `kubectl replace` + `rollout restart`.
  The live CM was confirmed byte-identical to the committed working tree before the
  patch, so no drift was reconciled. The live-only `data_mounts.py` mount +
  `data-mounts-root` volume were left untouched.
- **Image-service agent:** new `agent.py` (md5 `bc86730b…`) copied to
  `wx-ms-w7900d-0042:/opt/amd-oneclick/image-service/agent.py` via a one-shot
  privileged `hostNetwork` pod (node 0042 has a broken flannel CNI, so normal pods
  can't get a sandbox there; `hostNetwork` bypasses it), `py_compile`-checked, then
  `systemctl restart image-service` via an `nsenter`-into-PID1 pod. Backup at
  `agent.py.bak-deploy` on the node.

| Service / Port | Image (`:tag`) | Code-overrides sha256 | Rollback snapshots (sha256, git-ignored) |
|----------------|----------------|-----------------------|------------------------------------------|
| radeon-beta / 30444 | `crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/radeon-cloud/amd-oneclick:radeon-beta-image-service-20260625` (image unchanged) + `code-overrides` ConfigMap | `38ef5ab035ac55f47fd6c13a08a0534ac72badf0f81a864b0c1a4062565a13bb` | `local-deploy-history/radeon-beta/2026-06-29-image-system-PRE-rollback-code-overrides.yaml` (`7272cdc0…`), `…-deployment.yaml` (`91b3cde5…`) |

**Verified live:** manager `/health` 200; new pod clean startup (no traceback);
`quarantined_until` column migrated on prod Postgres; new endpoints
`/api/internal/nodes/status` + `/nodes/{node}/user-pods` registered; launchable
image list unchanged (4 ready); `sync_image_to_nodes(1)` returns image-service
shape (`2/2 nodes loaded`); importing→loaded / quarantine→clear roundtrip works on
prod DB; **pod-safety gate sees `u-11` on 0043 (`count_user_pods_on_node`==1) so it
would refuse a restart there**; new agent on 0042 active + polling the new manager.
**`u-11` user instance undisturbed** (Running, 0 restarts, 3d13h). Offline: full
pytest suite green except 4 pre-existing baseline failures; prepull-only tests
removed; `test_image_system_hardening.py` added.

---

## 2026-06-29 (later) — Radeon beta: FULL BETA-test line + all 7 merge-blocker fixes

**SUPERSEDES the "5 P1/P2 fixes" entry below.** That earlier entry described a
*rebase-onto-live* approach (keep live `data_mounts`, skip the 2 frontend fixes).
Per explicit user decision, this deploy instead switched beta to the **full
BETA-test code line** (`e158438`) + all 7 fixes. NET EFFECT vs the previous live
pod: GAINED the SSH feature (`ssh_enabled`, `SshPublicKeyRequest`,
`set_user_ssh_public_key`, `ssh_command`), the shared `useLaunchFlow`/`launch_flow.js`
frontend, and both frontend fixes; **DROPPED the live-only `data_mounts` feature**
(catalog physical-disk mounts) and the k8s_client +338 line edits — the two code
lines had diverged and could not be cleanly merged.

**Code commit:** `e158438820d55c0468a72b75b64f64d86b9a0637` (BETA-test) + 7 uncommitted fixes.

| Service / Port | Image (`:tag`) | Code-overrides sha256 | Deployment sha256 | Rollback snapshots |
|----------------|----------------|-----------------------|-------------------|--------------------|
| radeon-beta / 30444 | `crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/radeon-cloud/amd-oneclick:radeon-beta-image-service-20260625` (image unchanged) + `code-overrides` ConfigMap | `bb9bf5dbce08249efbf5d22869d98a758a51d34dd44bed2a3a0120b80c55daff` | `655ada8ef5637d17d50974265d62c2e279c3dbfa2838d64d4ca8c91dbcc36103` | `local-deploy-history/radeon-beta/2026-06-29-PRE-7fix-rollback-{code-overrides,deployment}.yaml` |

**Deploy mechanism:** ConfigMap rebuilt from full working tree (13 app + 6 templates
+ **2 static JS** `launch_flow.js`/`template_form.js` — needed because the base image
`/app/static` has only `.gitkeep`). Deployment edited to (a) ADD subPath mounts for the
2 static files, (b) REMOVE the `data_mounts.py` mount + `data-mounts-root` hostPath
volume. `kubectl replace` ConfigMap (648K, avoids apply annotation limit) + `kubectl apply`
Deployment + `rollout restart`.

**All 7 fixes verified live:** P1-deadend (`launch_intents` table auto-created;
`_resume_distributing_launch`), P1-addimage (legacy source_ref fallback), P1-ghref
(`_derive_admin_image_ref` → real tag, validated `admin-acme-widgets:ce6cabe4`),
P2-readiness (real `live_status`), P3-hf-proxy (`_github_clone_url` in HF wrapper),
plus frontend #1 (profile `launchCustomImage` → `lf.startLaunch`) and #2 (homepage
SSH access card + isCustom/canOpenWeb).

**Verification:** rollout `1/1 ready` (pod `65d766c4bd-66vf7`, 0 restarts); `/health`
200 on NodePort `36.150.116.220:30444` + HTTPS `radeon-beta.anruicloud.com`; homepage
200; `/static/launch_flow.js` + `/static/template_form.js` 200 (the 404 risk); served
homepage inline JS passes `node --check`; all 6 changed files hash-match the pod;
`launch_intents` table exists; SSH symbols present in pod; active user instance
`u-11-3414db96` undisturbed (Running 3d6h, 0 restarts). Pre-existing 404s for orphaned
`hf-14/15/17` services (pods already gone before deploy) persist — NOT caused by this
deploy and NOT touched by my fixes (`_instance_service_base` unchanged).

**Rollback:** `kubectl -n amd-oneclick-radeon-beta replace -f local-deploy-history/radeon-beta/2026-06-29-PRE-7fix-rollback-code-overrides.yaml && kubectl -n amd-oneclick-radeon-beta apply -f local-deploy-history/radeon-beta/2026-06-29-PRE-7fix-rollback-deployment.yaml && kubectl -n amd-oneclick-radeon-beta rollout restart deployment/amd-oneclick-radeon-beta-manager` (restores the data_mounts line, drops SSH+frontend).

**Caveat:** working-tree fixes are NOT committed/pushed to BETA-test; the running pod is
the only place they exist besides the local-deploy-history snapshots. Commit+push BETA-test
to make this durable.

---

## 2026-06-29 — Radeon beta merge-blocker backend fixes (5 P1/P2 fixes) [SUPERSEDED — see entry above]

**Code base:** rebased onto the LIVE `code-overrides` ConfigMap (which is ahead of
git `e158438` — it carries un-committed live-only edits: the `data_mounts` feature
+ `data_mounts.py`, k8s_client/config/models/admin/template_sync live edits).
Only 3 backend override files changed: `app/main.py`, `app/store.py`,
`app/notebook_sources.py`. The 2 frontend fixes (launch ReferenceError, homepage
SSH card) were deliberately NOT applied — the live frontend is an older
implementation (no `useLaunchFlow`/`launch_flow.js`) that does not have those bugs.

| Service / Port | Image (`:tag`) | Code-overrides sha256 | Rollback snapshot | Snapshot |
|----------------|----------------|-----------------------|-------------------|----------|
| radeon-beta / 30444 | `crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/radeon-cloud/amd-oneclick:radeon-beta-image-service-20260625` plus `amd-oneclick-radeon-beta-code-overrides` (image unchanged) | new manifest sha256 `8b8af13f5e575210ba74d19b5cd533dd31af59b363db22ac7f3729e3c3e0484e` | pre-deploy ConfigMap sha256 `8b8fd90b7f25f8b407763c8fff1a04510651a46f9cf8311f1ba123387307caf7` | `local-deploy-history/radeon-beta/2026-06-29-merge-blocker-fixes-code-overrides.local.yaml` |

**Fixes (all verified present in the running pod):**
- **P1-deadend** — launches needing image distribution no longer dead-end at
  "No notebook instance found": added `launch_intents` table + `_resume_distributing_launch`
  (status poll resumes and creates the pod once the image lands), shared
  `_provision_notebook_instance`/`_provision_template_instance` helpers (thread `data_mounts`).
- **P1-addimage** — admin Add Image in default (image-service-off) mode no longer 400s;
  legacy POST/PUT fall back to `source_ref` when `image` is omitted.
- **P1-ghref** — admin github_build source stores a real registry tag (`_derive_admin_image_ref`),
  not the Dockerfile URL.
- **P2-readiness** — `_active_instance_context` passes real `live_status` (not hardcoded
  "ready") and gates opencode URL/creds on readiness; no pre-ready URL/cred exposure.
- **P3-hf-proxy** — HF-demo GitHub clone routed through `_github_clone_url` instead of a
  hardcoded `http://github.com` (notebook_sources.py).

**Process:** snapshotted live ConfigMap (rollback point), rebased the 3 backend files
onto the live override set (preserving `data_mounts.py` and all 17 other live-only keys
byte-identical), validated in-container (full unittest suite: only the 4 pre-existing
unrelated failures; 8 new blocker tests pass), regenerated the ConfigMap via
`kubectl create --from-file --dry-run | kubectl replace` (replace avoids the apply
annotation size limit), then `rollout restart`. No Postgres, Secret, builder, image, or
production resources mutated.

**Verification:** rollout `1/1 ready` (new pod `5f6f779c8c-pwmj4`, 0 restarts);
NodePort `:30444/health`, `https://radeon-beta.anruicloud.com/health`, and homepage `/`
all 200; `launch_intents` table created and `app.data_mounts` imports OK in the running
pod (live-only feature intact); all 5 fixes grep-confirmed in the running pod source;
active user instance `u-11-3414db96` preserved (3d5h, 1/1); only benign stale-browser
`/global/event` 404 noise in logs.

**Rollback:** `kubectl -n amd-oneclick-radeon-beta replace -f local-deploy-history/radeon-beta/2026-06-29-PRE-merge-blocker-fixes-rollback.local.yaml` then `kubectl -n amd-oneclick-radeon-beta rollout restart deployment/amd-oneclick-radeon-beta-manager`.

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
