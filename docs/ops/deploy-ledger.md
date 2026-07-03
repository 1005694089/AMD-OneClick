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

## 2026-07-03 (latest) — Radeon beta: revert image soft-affinity + enable Redis rate limiting (image rebuild)

**Code commit:** `48c3c7c` on `BETA-test` (pushed). Two coordinated changes on top of the
2026-07-02 oauth-credit-manager merge (`2bd2ae2`):

1. **Reverted image soft-affinity** (`app/config.py`): flipped `IMAGE_AFFINITY_ENABLED` default
   `true` → `false`. That feature (from `75f8953`) has the `image_sync_refresh_job` project
   per-image DB node-load state onto node labels via `core_v1.patch_node`, which needs
   `nodes:patch` RBAC. The per-service beta manager SA (`amd-oneclick-radeon-beta-manager`, bound
   to ClusterRole `amd-oneclick-radeon-beta-node-reader`) has only `get/list/watch` on nodes, so
   every reconcile cycle logged a 403 per node (~40 nodes × every 120s). The feature is also
   redundant under the Dragonfly P2P image-service model (every fleet node is warmed and pulls
   peer-to-peer; beta's fleet is 2 identically-warmed nodes 0043/0044), so warm-node scheduling
   bias buys nothing here. With the flag off, both gated paths go dormant:
   `reconcile_image_ready_labels()` early-returns before any `list_node`/`patch_node` (kills the
   403 spam) and `_image_affinity()` returns `None` (no affinity injected into pods). The
   `image_sync_refresh_job` still does its legitimate `update_image_sync_status` work. 245 tests
   pass. Re-enable only where the manager SA is granted `nodes:patch` (e.g. mirror the live v2
   `amd-oneclick-manager-v2-node-labeler` ClusterRole).

2. **Enabled Redis-backed rate limiting** — required rebuilding the manager image. The
   oauth-credit-manager merge added `redis>=5.0.0` to `requirements.txt` and `app/redis_client.py`
   (lazy `import redis` inside `get_redis()`, fails open when unreachable), but every radeon-beta
   deploy since 2026-06-24 ships code via the `code-overrides` ConfigMap with the image tag
   unchanged — so the `redis` pip package was never installed in the running image. Setting
   `REDIS_URL` alone would just trigger a lazy-import failure (still fail-open, rate limiting
   inert). So this deploy **rebuilds the image** to bake in `redis`, then wires `REDIS_URL`.

**New image:** built on host `zijun@10.161.176.9` (`docker build --no-cache --platform linux/amd64`
from `BETA-test`@`48c3c7c`), pushed to the same registry/namespace as prior beta images.

| Service / Port | Image (`:tag`) | Registry digest | Local image ID |
|----------------|----------------|-----------------|----------------|
| radeon-beta / 30444 | `crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/radeon-cloud/amd-oneclick:radeon-beta-redis-20260703-0026-48c3c7c` | `sha256:d2cfe1a916335618f46832379269b05e2e0b316e9b44b9582146eee55fb9077f` | `sha256:d2cfe1a916335618f46832379269b05e2e0b316e9b44b9582146eee55fb9077f` |

**Deploy mechanism (3 coordinated changes, one rollout):**
- Patched the `config.py` key in `amd-oneclick-radeon-beta-code-overrides` ConfigMap with the
  affinity-off version (`kubectl patch --type merge`). Confirmed the only diff vs the prior CM
  `config.py` was exactly the `IMAGE_AFFINITY_ENABLED` flip (comment + default) — no other drift.
- Added `REDIS_URL: redis://amd-oneclick-redis.default:6379/1` to the `amd-oneclick-radeon-beta-config`
  env ConfigMap. Uses the short-DNS form (beta pod DNS domain is `amd.gpu.dc`, not `cluster.local`,
  so the `.svc.cluster.local` FQDN does not resolve) and **DB index `/1`** to isolate beta's `rl:*`
  rate-limit counters from v2 prod's DB `/0` on the shared `amd-oneclick-redis` (default ns) cache.
- `kubectl set image deployment/amd-oneclick-radeon-beta-manager manager=...:radeon-beta-redis-20260703-0026-48c3c7c`.
- Default RollingUpdate (no `rollout restart` needed — the image + CM changes rolled a new pod).

| Artifact | PRE sha256 | APPLIED sha256 | Snapshot |
|----------|------------|-----------------|----------|
| code-overrides CM | `4734b434a167fa514df86501ac9a66f0de32bed3f57497127671dca1110f9752` | `adcdc1d5ff8c7eab6d5296be33a0822dbaf415f5b3db434b0fbdce18004cce77` | `local-deploy-history/radeon-beta/20260703-0026-redis-affinity-{PRE,APPLIED}-code-overrides.yaml` |
| deployment | `2abf783137dfa3d102cf2f3d161248357679d8e3ab6a0d8f3cc06135dc1120fe` | `62526ce12c3c03b805b12acc600b6dc6a1159ffc75a578b0075745ed0e9cb8dd` | `local-deploy-history/radeon-beta/20260703-0026-redis-affinity-{PRE,APPLIED}-deployment.yaml` |
| config CM | (PRE snapshot) | (APPLIED snapshot) | `local-deploy-history/radeon-beta/20260703-0026-redis-affinity-{PRE,APPLIED}-config-configmap.yaml` |

**Verification (live e2e):** rollout `1/1 ready` (pod `amd-oneclick-radeon-beta-manager-76dd5bbcdd-2j9km`,
0 restarts); image in use = the new redis tag. In-pod: `redis` pkg 8.0.1 importable;
`settings.REDIS_URL=redis://amd-oneclick-redis.default:6379/1`, `settings.RATE_LIMIT_ENABLED=True`,
`settings.IMAGE_AFFINITY_ENABLED=False`. **Redis functional test:** `get_redis()` returns a live
client, `ping()`→True, connected on DB index 1; `rate_limit_ok(key, limit=2, 60)` called 3× →
`[True, True, False]` (3rd correctly blocked — rate limiting actually enforcing, no longer just
fail-open). **403 spam gone:** across 2+ image-sync cycles (130s+) the pod logged **0** `403`s and
`reconcile_image_ready_labels` no longer executes at all (early-returns). `image_sync_refresh_job`
still runs its `update_image_sync_status` work. External: `https://radeon-beta.anruicloud.com/health`
and `/` both 200. All 6 pre-existing user instances (`u-13`, `u-18`, `u-20`, `u-57`, `u-58`, `u-59`)
undisturbed (Running, no new restarts).

**Rollback:** `kubectl -n amd-oneclick-radeon-beta replace -f local-deploy-history/radeon-beta/20260703-0026-redis-affinity-PRE-code-overrides.yaml && kubectl -n amd-oneclick-radeon-beta apply -f local-deploy-history/radeon-beta/20260703-0026-redis-affinity-PRE-config-configmap.yaml && kubectl -n amd-oneclick-radeon-beta apply -f local-deploy-history/radeon-beta/20260703-0026-redis-affinity-PRE-deployment.yaml && kubectl -n amd-oneclick-radeon-beta rollout restart deployment/amd-oneclick-radeon-beta-manager` (restores prior image tag `radeon-beta-image-service-20260625`, removes `REDIS_URL`, restores the affinity-on `config.py`). Note: reverting to affinity-on brings back the 403 spam.

---

## 2026-07-02 — Radeon beta: merge feature/oauth-credit-manager (reconciler crash-loop reclaim, scheduler off event loop, shared-DB delete safety, auth hardening)

**Code commit:** `2bd2ae2` on `BETA-test` (pushed), merging `feature/oauth-credit-manager`
(`75f8953`, `c9b2fc7`, `a106567`, `4e4e1b2`, `5b6bd18`, `a253597`) via intermediate branch
`merge-oauth-reconciler-fixes` (merge commit `3a75878`). Fixes "instance can't be deleted
properly": the bidirectional reconciler (`reconcile_job`) now also reclaims crash-looping /
high-restart pods (not just orphans and stuck-terminating pods), and before marking a DB
record deleted it confirms the pod is truly gone cluster-wide via `k8s_client._pod_exists()`
rather than only checking this manager's own label scope — preventing it from clobbering
another manager's instances in the shared Postgres DB. `cleanup_job` and `reconcile_job` run
as sync functions (dispatched to a worker thread by APScheduler's `AsyncIOExecutor`) instead
of coroutines, so their blocking k8s API calls no longer starve the event loop (previously
implicated in intermittent OAuth failures under load). Also brings in Risk1/2/3 hardening from
`75f8953`: Redis-backed login/signup rate limiting (fails open when Redis is absent, which is
the case here — `REDIS_URL` unset), admin session-epoch revocation, an authoritative
background-delete-then-poll-confirm pattern for instance deletion, a ModelScope login fix
(userinfo 404), and a stale-pod-reuse fix on launch.

**Merge conflicts** (across `app/config.py`, `app/scheduler.py`, `app/k8s_client.py`,
`app/store.py` — `main.py`/`requirements.txt` auto-merged clean): additive settings blocks
concatenated (zero name collisions, confirmed via `sort | uniq -d`); added the
`_skip_not_leader()` leader-election guard to `reconcile_job`/`image_sync_refresh_job`, which
had lacked it unlike every other scheduled job, for consistency; removed the feature branch's
now-superseded DaemonSet-based `get_image_sync_status`/`_pulled_nodes_for_image` in favor of
HEAD's DB-backed `get_image_sync_status(image_id, image=None)` (via
`store.list_nodes_for_image`), and rewired `reconcile_image_ready_labels()` accordingly.
Restored `_normalize_image_ref` (a static method still depended on by `_image_ready_label_key`)
after that dead-code removal — this was the one point where deleting "dead" code broke a live
caller; caught by the test suite (7 failures), fixed, then 245/245 passed. 245 tests passing
(unchanged count from before this merge; no new tests added, all pre-existing coverage green).

**Deploy mechanism:** regenerated `amd-oneclick-radeon-beta-code-overrides` ConfigMap (24 keys:
added new `redis_client.py`; refreshed `config.py`, `k8s_client.py`, `main.py`, `scheduler.py`,
`store.py`; 18 other keys unchanged) via `kubectl replace` (avoids the `apply`
last-applied-configuration annotation-size limit on this large manifest). Patched the
Deployment via `kubectl patch --type=json` to add the `redis_client.py` subPath volumeMount
(`/app/app/redis_client.py`), mirroring the existing `leader.py`/`purge_exec.py` mounts. Then
`rollout restart`. No image rebuild (image unchanged), no Postgres change, no config-CM
(env) change — all new settings (`RECONCILE_ENABLED`, `RATE_LIMIT_ENABLED`,
`IMAGE_AFFINITY_ENABLED`, etc.) run on their code defaults since `REDIS_URL` is unset (rate
limiting fails open, exactly as designed) and no override was added.

| Service / Port | Image (`:tag`) | Code-overrides sha256 (PRE) | Code-overrides sha256 (APPLIED) | Snapshot |
|----------------|----------------|------------------------------|-----------------------------------|----------|
| radeon-beta / 30444 | `crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/radeon-cloud/amd-oneclick:radeon-beta-image-service-20260625` (image unchanged) + `code-overrides` ConfigMap | `19f01b6671c4bfc3abc15de2d0bf47b96b45bca02e56cf9a3391dc3e195a8c6f` | `9f620add6b3aead12a4f15ae14d4aa16f4fbb258697bcb3e8fc0bb58543c0bab` | `local-deploy-history/radeon-beta/20260702-1051-oauth-merge-{PRE,APPLIED}-{code-overrides,deployment}.yaml` |

**Verification:** rollout `1/1 ready` (new pod `amd-oneclick-radeon-beta-manager-797545dc4b-z5f7c`,
0 restarts, old pod terminated cleanly). In-pod: `app.main`, `app.scheduler`, `app.k8s_client`,
`app.store`, `app.redis_client`, `app.config` all import cleanly; `k8s_client._normalize_image_ref`
and `k8s_client._pod_exists` both present; `settings.RECONCILE_ENABLED=True`,
`settings.RATE_LIMIT_ENABLED=True`, `settings.REDIS_URL=''`, `settings.IMAGE_AFFINITY_ENABLED=True`
(all code defaults, as intended — no config-CM override added). Scheduler log confirms all 8 jobs
registered including "Reconcile cluster instances against the database" (interval 60s) and
"Refresh image prepull status and node-ready labels" (interval 120s); reconcile job ran
successfully post-deploy: `Reconcile done: orphans=0 terminal=0 stuck=0 db_marked=0 cluster_pods=6
active_db=6` (steady state, no false reclaims). `https://radeon-beta.anruicloud.com/health` and
`/` both 200. All 6 pre-existing running user instances (`u-13`, `u-18`, `u-20`, `u-57`, `u-58`,
`u-59`) undisturbed (Running, no new restarts from this deploy).

**Rollback:** `kubectl -n amd-oneclick-radeon-beta replace -f local-deploy-history/radeon-beta/20260702-1051-oauth-merge-PRE-code-overrides.yaml && kubectl -n amd-oneclick-radeon-beta apply -f local-deploy-history/radeon-beta/20260702-1051-oauth-merge-PRE-deployment.yaml && kubectl -n amd-oneclick-radeon-beta rollout restart deployment/amd-oneclick-radeon-beta-manager` (restores the pre-merge code line and removes the `redis_client.py` mount).

---

## 2026-07-02 — Radeon beta: merge feat/dragonfly-p2p-100plus-nodes into BETA-test

**Code commit:** `7ce6e4c` (merge of `feat/dragonfly-p2p-100plus-nodes` into `BETA-test`,
merge-base `7909740`). Includes a review fix folded into the merge commit: `claim_next_image_job`'s
stale-evict supersede check (`app/store.py`) only matched newer `distribute`/`build` jobs for the
same ref, not `warm` — the P3+ primary transport once a LAN registry is wired up. A rebuild after a
delete would enqueue `warm`, invisible to that check, letting a stale evict race in and wipe the
freshly-warmed image. Fixed by adding `warm` to the supersede kinds tuple, with a regression test
(`test_evict_superseded_by_newer_warm`). 245 tests pass (was 244; +1 new).

Per `docs/DRAGONFLY_MIGRATION_RUNBOOK.md`, this merge's P0–P1 code was already live-deployed
**dormant→active** on radeon-beta prior to this session (LAN registry `10.5.10.43:5000` wired,
`leader.py`/`purge_exec.py` present in the running ConfigMap) — this deploy is a **manager-code
sync** to bring the running pod onto the merged, reviewed, tested `BETA-test` HEAD (was running
uncommitted/ahead-of-git working-tree edits), not a fresh activation. Node-0042 host-side pieces
(zot registry, Dragonfly Helm, `image-service.service` agent) are unchanged by this deploy — those
are `[YOU-0042]` steps outside this workspace host's network reach (no route to `10.5.10.x`).

| Service / Port | Image (`:tag`) | Code-overrides sha256 | Deployment sha256 | Snapshot |
|----------------|----------------|------------------------|--------------------|----------|
| radeon-beta / 30444 | `crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/radeon-cloud/amd-oneclick:radeon-beta-image-service-20260625` (image unchanged) + `code-overrides` ConfigMap | `231e848ff421afac2c10ebe4756f563c5d010439a2c99bd3b4774b9249334592` | `5fc86e36b3b43891773e63816b34443bcc1455eb750327302facc81138f226a1` | `local-deploy-history/radeon-beta/20260702-2248-dragonfly-merge-{PRE,APPLIED}-{code-overrides,deployment}.yaml` |

**Deploy mechanism:** regenerated `amd-oneclick-radeon-beta-code-overrides` ConfigMap from the
merged `BETA-test` working tree (23 keys: 13 `app/*.py` incl. new `leader.py`/`purge_exec.py`, 7
templates, 2 static JS) via `kubectl replace` (large manifest, avoids the `apply` annotation-size
limit). **Deliberately dropped the `data_mounts.py` key** — a live-only, never-committed-to-git
file: confirmed via grep it is not imported by live `main.py`/`k8s_client.py` (dead server-side
code already; only `models.py`'s `DataMountSelection` type and some frontend JS referenced the
concept), and Pydantic's default `extra="ignore"` means the stray `data_mounts` field in existing
frontend requests is now silently dropped rather than erroring. Removed the matching
`data_mounts.py` subPath volumeMount and the `data-mounts-root` hostPath volume from the Deployment
via `kubectl patch --type=json` (the stale `data_mounts.py` mount predates the `code-overrides`
key's introduction via `kubectl apply`'s last-applied-configuration annotation, so a plain `apply`
alone did not remove it — required an explicit JSON-patch `remove`). Then `rollout restart`.

**Verification:** rollout `1/1 ready` (pod `amd-oneclick-radeon-beta-manager-7d796fc4f5-nqwxq`, 0
restarts); NodePort `:30444/health`, HTTPS `radeon-beta.anruicloud.com/health`, and homepage `/` all
200; `app.main`/`app.leader`/`app.purge_exec` import cleanly in-pod; `/app/app/data_mounts.py`
confirmed absent; hackathon `--collaborative` code and the `distribute/warm/build` supersede fix
both grep-confirmed present in the running pod's `k8s_client.py`/`store.py`; `image_jobs` table
shows live `warm`/`distribute`/`push`/`evict`/`purge_*` activity (P3 Dragonfly transport actively
running, not dormant); all 6 pre-existing running user instances (`u-13`, `u-18`, `u-20`, `u-57`,
`u-58`, `u-59`) undisturbed (Running, 0 new restarts); `/api/internal/jobs/claim` and
`/api/internal/builds/claim` returning 200 (node-0042 agent actively polling the new pod).

**Rollback:** `kubectl -n amd-oneclick-radeon-beta replace -f local-deploy-history/radeon-beta/20260702-2248-dragonfly-merge-PRE-code-overrides.yaml && kubectl -n amd-oneclick-radeon-beta apply -f local-deploy-history/radeon-beta/20260702-2248-dragonfly-merge-PRE-deployment.yaml && kubectl -n amd-oneclick-radeon-beta rollout restart deployment/amd-oneclick-radeon-beta-manager` (restores the pre-merge code line + the `data_mounts.py` mount/volume).

---

## 2026-06-30 (latest) — Radeon beta: GPU nodes dashboard (admin, beta-only)

**Commit:** `807d286` on `BETA-test` (pushed). New admin "GPU Nodes" tab showing
per-node GPU status (health, used/total/free, CPU/mem, model) + cluster utilization
(counters, per-node bar chart, per-node running instances). Gated by a new
`GPU_DASHBOARD_ENABLED` flag (default false; set true only in the beta config CM).

**Design notes:** `gpu_cluster_status()` lists every GPU node this service owns
(incl cordoned/NotReady) via `list_node()` + `_node_belongs_to_service`. The beta
SA can list nodes cluster-wide but NOT pods across namespaces (403), so committed/
free + the instance list are scoped to this deployment's namespace and labeled as
such (`usage_scope=namespace`; "Schedulable free" excludes idle GPUs on
unhealthy nodes). `list_instances()` now carries `node_name` (getattr-safe).

**Review:** 3-dimension adversarial workflow (correctness/security/regression),
each finding verified. 3 confirmed, all fixed before deploy: (HIGH) `node_name`
access would break an existing opencode test mock → switched to
`getattr(pod.spec,"node_name",None)` (verified that test now passes); (low) headline
counters didn't visibly sum → relabeled "Schedulable free" + disclaimer; (low)
error-fallback object shape → mirror the default. Security pass: clean (admin auth
+ 404 gate; React escapes; tojson-safe).

**Deploy mechanism:** patched 4 keys (`config.py`, `k8s_client.py`, `main.py`,
`admin.html`) in `amd-oneclick-radeon-beta-code-overrides` (`kubectl patch --type
merge`) AND added `GPU_DASHBOARD_ENABLED: "true"` to `amd-oneclick-radeon-beta-config`
(envFrom source). `rollout restart`. No Postgres change.

| Service / Port | Image (`:tag`) | Code-overrides sha256 (post-patch, data) | Rollback snapshot (sha256, git-ignored) |
|----------------|----------------|------------------------------------------|------------------------------------------|
| radeon-beta / 30444 | image unchanged + CMs | `58c18cc5f07450a72b957427656c9ecd9ccaa18b00f25fa61b0ac3ae36705f67` | `local-deploy-history/radeon-beta/cm-snapshot-20260630-211322.yaml` (`26eeb8a5429b97e1344b4fa644cf20dd2282012dd4ea6982e9bdcfb16ae00c05`) |

(Rollback also requires removing `GPU_DASHBOARD_ENABLED` from the config CM, or the
tab stays visible even on old code — though the endpoint 404s without the new main.py.)

**Verified live e2e:** new pod runs new code, flag True. `gpu_cluster_status()`
returns 2 beta GPU nodes (model AMD_Radeon_Pro_W7900D, mem 1007.5 GiB, health,
used=1/16, usage_scope=namespace). `GET /api/admin/gpu-nodes` with admin auth → 200
total=16; no-auth → 401. `/admin` serves `gpuDashboardEnabled=true` + the "GPU Nodes"
tab. Tests: the at-risk opencode test passes with the getattr fix; 11 GPU tests green.

---

## 2026-06-30 — Radeon beta: clone a repo with no notebook file (prev)

**Commit:** `64b2ec3` on `BETA-test` (pushed to origin). Two ways to launch
JupyterLab from a cloned repo with no `.ipynb`: (A) the HF demo API accepts a
`.git` repo as `notebook_path` when `pod_type=workshop` (`org/repo.git`, optional
`@branch`, full URL or scp form, GitHub-only); (B) notebook-type templates may set
a `repo_url` with no `notebook_path`. Shared startup-script fix: omit `--branch`
when none given (follow remote HEAD), skip the notebook-not-found check when path
is empty, echo the shlex-quoted path.

**Review:** 3-dimension adversarial workflow (correctness/security/regression),
each finding verified by an independent skeptic. 4 findings raised: 1 HIGH was a
false positive (reviewer ran against a stale tree; the real sole caller at
`main.py:1287` gates on `_template_github_info(...) or None`, not notebook_path —
confirmed live on the deployed pod). 3 low findings fixed before deploy: scp-form
non-GitHub host now rejected, trailing slash/query/fragment tolerated, and the
`echo` now uses the quoted path (no shell-injection via a crafted `.ipynb` path).

**Deploy mechanism:** patched 3 keys (`main.py`, `k8s_client.py`,
`notebook_sources.py`) in `amd-oneclick-radeon-beta-code-overrides`
(`kubectl patch --type merge`) + `rollout restart`. No config-CM or Postgres
change. Verified live: pre-patch CM byte-identical to git baseline (no drift);
post-patch keys match committed code.

| Service / Port | Image (`:tag`) | Code-overrides sha256 (post-patch, data) | Rollback snapshot (sha256, git-ignored) |
|----------------|----------------|------------------------------------------|------------------------------------------|
| radeon-beta / 30444 | image unchanged + CM | `1d77c01f7a95a48c4c06b24179975a05eb935ffe7f7d01271db5a2988a06455d` | `local-deploy-history/radeon-beta/cm-snapshot-20260630-202406.yaml` (`7c250eeff05a135f634b4f9e1a58312d14a01c9ad575710dfe32ff057a74a359`) |

**Verified live e2e:** new pod runs new code. Negative: `.git`+`hackathon` → 400,
`.git`+no-pod_type → 400, `git@gitlab.com:…` → 400 (`Only GitHub …`). Positive:
workshop launch of `octocat/Hello-World.git` → 200, root URL (`/lab?token=`, no
`/tree/`), pod Running, logs show `Cloning … Repository cloned` with **no
"Notebook not found"**, annotations `pod-type=workshop` / `api-launched=true` /
`github-path=""`; instance destroyed, 0 residue. Part B: deployed
`_template_github_info` returns a clone-capable `github_info` (`path=""`,
`repo_url` set) for a repo-only notebook template, `{}` for image-only — same
clone path the workshop pod exercised. Tests: 53 passed in-pod.

---

## 2026-06-30 — Radeon beta: select launch image by admin-panel name

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

## 2026-07-02 — radeon-global (amd-oneclick-lablab) — BETA-test deploy, Image Service off, HF demo API enabled

**Code commit:** `a0fe4d8` (branch `prod/radeon-global`, tip of `BETA-test`).

**Target:** live namespace `amd-oneclick-lablab` on the 108-node prod cluster
(`/home/zijun/128-nodes-config.yml`), fronted by Azure Front Door at
`https://radeon-global.anruicloud.com` -> NodePort `36.150.116.206:30080` ->
manager pinned to `wx-k8s-prod-s-001`.

**Image build:** `docker build` from `prod/radeon-global` -> `lablab.local/amd-oneclick:a0fe4d8`
(442MB). Side-loaded node-local onto `wx-k8s-prod-s-001` via a one-shot privileged
`hostPath`-mounted Ubuntu pod (`ctr -n k8s.io images import`); no registry push, no pull secret.

**Config changes (ConfigMap `amd-oneclick-lablab-config`):** added
`HF_ENDPOINT=http://134.199.133.77`, `HF_HUB_DISABLE_XET=1`,
`HF_TOKEN_SECRET_NAME/KEY=amd-oneclick-lablab-secrets/HF_TOKEN`,
`HUGGINGFACE_DEMO_MIN_CREDITS=48`; confirmed `IMAGE_SERVICE_ENABLED=false` and
`IMAGE_AFFINITY_ENABLED=false` (already the working default on this cluster). All
other lablab keys (GitHub OAuth, PUBLIC_BASE_URL, SERVICE_HOST, NOTEBOOK_*, node
ports) preserved byte-identical.

**Secret changes (`amd-oneclick-lablab-secrets`):** added
`HUGGINGFACE_DEMO_API_TOKENS` (freshly generated random bearer token — no prior
value existed anywhere on the host or cluster). `ADMIN_PASSWORD`,
`GITHUB_CLIENT_SECRET`, `SESSION_SECRET` preserved from the pre-change snapshot.

**Incident during deploy:** the entire `amd-oneclick-lablab` namespace (including
live user pods `u-2`, `u-6`) was deleted by the user mid-verification (intentional,
not caused by this deploy — confirmed no app code path calls `delete_namespace`).
Rebuilt the full namespace (RBAC, ConfigMap, Secret, Deployment, Service) from the
PRE-change snapshots plus the new image/config, matching the original topology
exactly (nodeName `wx-k8s-prod-s-001`, hostPath `/var/lib/amd-oneclick-lablab/data`
for SQLite, NodePort 30080, same RBAC). Snapshots and rebuild manifest are not
retained in git (contain secrets).

**Verification:**
- `GET /health` (public + NodePort) -> `{"status":"healthy"}`, manager `1/1`, 0 restarts.
- `GET /api/huggingface/images` no-token -> `401` (was `503` pre-deploy: tokens were unset).
- With token -> `200` + image catalog.
- Full launch/status/destroy round trip via **public Front Door domain**
  (`https://radeon-global.anruicloud.com`) and direct NodePort: notebook pod
  scheduled by `default-scheduler` (no image-service node pinning) onto
  `wx-k8s-prod-s-033`; kubelet found/pulled the image itself (`Pulled` event:
  "already present on machine") — confirms the Image-Service-disabled path.
  Destroyed cleanly after.
- `python -m pytest tests/test_hf_api_features.py -q` -> **63 passed**.
- Pre-existing manager RBAC (SA + Role/RoleBinding + ClusterRole/ClusterRoleBinding,
  node get/list/watch only) recreated identically.

**Rollback:** re-apply `20260702-1736-PRE-{deploy,cm,secret}.yaml` snapshots
(local-only, not in git) to restore image `lablab.local/amd-oneclick:0706` and the
pre-HF ConfigMap/Secret state; note the original `u-2`/`u-6` user pods cannot be
restored (they were destroyed with the namespace, independent of this deploy).

## 2026-07-03 — radeon-global — HF startup credits 48 -> 10

**Change:** ConfigMap `amd-oneclick-lablab-config` key `HUGGINGFACE_DEMO_MIN_CREDITS`
`48` -> `10` (`kubectl patch` merge), then `rollout restart` the manager to pick up
the env change.

**Why:** new HuggingFace demo users were being granted 48 starting credits; desired
starting grant is 10. `HUGGINGFACE_DEMO_MIN_CREDITS` drives both the once-only
`grant_initial_credits_once(..., "hf_initial_grant")` call (`app/main.py:2510`) and
the `_backfill_hf_credit_cap` init-time cap (`app/store.py:520`). No code hardcodes
48/10 — config-only change.

**Scope:** affects NEW HF users only. Existing users keep their balance — the
per-user `hf_initial_grant` marker blocks re-grant, and the one-time
`hf_backfill_cap_v1` marker (already run at cap=48) blocks the cap from re-running.

**Verification:** launched a fresh HF user post-change; DB `users` row shows
`credits=10` for the new user (id 9), while pre-change test users (ids 7/8) retain
47/48 — confirms once-only grant and correct new default. Manager `1/1`, public
`/health` 200.

**Known open item (NOT changed):** OpenCode `opencode_url` still returns raw
`ip:port` and requires manual username/password. Root cause: `OPENCODE_PUBLIC_BASE_URL`
is unset, and enabling it needs a network path for OpenCode traffic that is separate
from the main app (the manager's origin-proxy middleware, `app/main.py:178`, hijacks
by hostname). radeon-global currently has NO such path: no tls-proxy pod, no NodePort
30450, and `radeon-global.anruicloud.com` resolves only to Azure Front Door (443).
Fixing requires either a Front Door route for a dedicated OpenCode subdomain -> manager
NodePort, or an in-cluster tls-proxy on a reachable edge NodePort (beta's pattern:
`radeon.anruicloud.com:30450` -> `36.150.116.200`). Pending infra decision.
