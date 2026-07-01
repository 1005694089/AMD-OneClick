# Dragonfly P2P + LAN registry migration — operator runbook (node 0042 + cluster)

Step-by-step deploy guide for migrating the AMD-OneClick image-service from single-source SSH push
to a self-hosted LAN registry + Dragonfly P2P mirror, sized for 100–300+ nodes.

- **Branch:** `feat/dragonfly-p2p-100plus-nodes` (worktree `/home/ziwei/dragonfly-worktree`, off
  `BETA-test` `7909740`).
- **Commits so far:** `a93dc40` (P0 control-plane + R2c), `6aa44a6` (leader-election hardening).
- **Tests:** 190 pass. **Review:** 3 adversarial reviewers, 0 blockers / 0 majors.
- **Cluster:** `KUBECONFIG=/home/zijun/7900_cluster_config`, namespace `amd-oneclick-radeon-beta`.

> **Hard constraint:** the workspace host `10.161.176.9` has **no route to the 10.5.10.x node LAN**.
> Only node 0042 is on that LAN. `kubectl` works from the workspace; anything that touches 0042
> directly (registry container, agent binary, systemd) must run from 0042 or via a jump host.

---

## Deploy mechanics (how this codebase ships — confirmed against the live cluster)

- **Manager** runs a fixed image and overlays live `.py` files via ConfigMap
  `amd-oneclick-radeon-beta-code-overrides` (flat keys such as `config.py`, `main.py`, mounted over
  `/app/app/`). Deploying manager code = regenerate that ConfigMap from the worktree `app/` and
  `kubectl rollout restart` the manager Deployment.
  **P0 adds one new key `leader.py`** (22 → 23 keys). Do not drop the existing non-`.py` keys
  (html/js) when regenerating.
- **Agent** is `/opt/amd-oneclick/image-service/agent.py` on 0042, run by the systemd unit
  `image-service.service` with `EnvironmentFile=/etc/amd-oneclick-image-service.env`. Deploying
  agent code = copy `agent.py` + `sudo systemctl restart image-service.service`. New job kinds must
  be added to `IMAGE_SERVICE_KINDS` in that env file (default
  `build,pull,acr_backup,distribute,evict`).

---

## Step 0 — Deploy P0 (safe; distribution transport unchanged)

P0 is transport-neutral: it hardens the control plane and fixes R2c on the *current* SSH push path.
Nothing about how images move changes yet, so this is low-risk.

From the workspace host (has `kubectl`):

1. Set `KUBECONFIG=/home/zijun/7900_cluster_config` for all `kubectl`.
2. **Back up** the current overrides ConfigMap:
   ```
   kubectl -n amd-oneclick-radeon-beta get cm amd-oneclick-radeon-beta-code-overrides -o yaml \
     > /tmp/code-overrides.bak.yaml
   ```
3. **Regenerate** the ConfigMap from `/home/ziwei/dragonfly-worktree/app/`, keeping all existing
   keys and **adding `leader.py`**. Use the same build step that produced the live ConfigMap (the
   23 keys = the `.py` modules + the html/js assets already present).
4. `kubectl apply` the regenerated ConfigMap, then:
   ```
   kubectl -n amd-oneclick-radeon-beta rollout restart deploy/amd-oneclick-radeon-beta-manager
   kubectl -n amd-oneclick-radeon-beta rollout status  deploy/amd-oneclick-radeon-beta-manager
   ```
5. **Verify the migration + config in-pod:**
   ```
   kubectl -n amd-oneclick-radeon-beta exec deploy/amd-oneclick-radeon-beta-manager -- \
     python -c "from app.config import settings; print(settings.CUSTOM_IMAGE_GC_LAUNCH_GRACE_SECONDS)"
   # expect 432000
   ```
   Confirm the `image_jobs.heartbeat_at` column now exists (the additive migration runs at startup).
6. **Deploy the P0 agent to 0042** (on 0042 / via jump host):
   ```
   sudo install -m 0755 agent.py /opt/amd-oneclick/image-service/agent.py
   sudo systemctl restart image-service.service
   journalctl -u image-service -f      # confirm clean start, no traceback
   ```
7. **Smoke test:** an admin image distribute still completes end-to-end (transport unchanged).
   Keep `LEADER_ELECTION_ENABLED=false` and manager `replicas=1` for now.

**Rollback:** `kubectl apply -f /tmp/code-overrides.bak.yaml` + rollout restart; restore the prior
`agent.py` + restart the unit. P0 makes no destructive schema change (only an additive column).

---

## Step 1 — Resolve the pre-P1 decisions (BLOCKS P1)

See the "Decisions required" section at the bottom. Concretely gather:

1. **Second routed-LAN host?** `kubectl get nodes -o wide` → is any other host on `10.5.10.x` that
   could run a standby registry + agent? Decides P6 (active/active vs rebuild-runbook).
2. **Live CNI probe.** Deploy a tiny DaemonSet that curls a peer pod's IP:port across nodes; confirm
   pod-to-pod works over the node network (Dragonfly P2P needs it), and confirm the **manager pod
   cannot** reach node InternalIPs (validates the B1 trigger choice).
3. **Registry engine:** registry:2 vs zot (see decisions). Code is written registry-v2-agnostic so
   this can be deferred to deploy time, but the GC-lock strategy differs.

---

## Step 2 — P1: LAN registry on 0042 + push / ready-after-push

**Code (worktree, unit-tested before any deploy):**
- Add `run_push` to `agent.py`: push the built image digest-pinned to the LAN registry, **keep** the
  `/disk/ssd2/image-tars` tarball (registry is truth, tarball is backup), report the digest.
- Chain `['push','warm']` for admin, `['push']` for custom.
- Move the custom "ready" flip out of the `build` branch into a new `push` branch of
  `_sync_image_job_lifecycle` (so "ready" means registry-durable, not "tarball on 0042").
- Add `images.digest` / `custom_images.digest` + setter.
- Agent-side registry HEAD digest check feeding `get_image_sync_status` (manager can't reach the
  registry, so the agent reports a `digest_present` flag).
- Add `push` to `IMAGE_JOB_KINDS` + DISPATCH + lifecycle. **Do NOT** add it to `SERIALIZED_KINDS`
  (it is 0042-local, not a per-node unpack).

**Deploy on 0042:**
1. `sudo install -d -o imagesvc -g imagesvc /disk/ssd2/registry`
2. Run the chosen registry (Step 1.3) as a systemd-managed container bound to the 0042 LAN IP:5000,
   TLS + htpasswd, data dir `/disk/ssd2/registry`.
3. Generate htpasswd + TLS cert; write registry creds into the agent's `DOCKER_CONFIG` dir and
   (later) into the Dragonfly `hosts.toml`.
4. Add `LAN_REGISTRY=<0042-LAN-IP>:5000` to `/etc/amd-oneclick-image-service.env` and to the manager
   ConfigMap; add `push` to `IMAGE_SERVICE_KINDS`.
5. Deploy P1 agent + manager code (as Step 0.3–0.6).

**Verify:** admin upload → `image_jobs` shows build→push→warm; `crane manifest <lan-ref>` resolves;
"ready" appears only after push; admin delete removes the registry tag.

---

## Step 3 — P2: Dragonfly cluster (dormant)

1. `helm install` Dragonfly chart 1.7.0 (manager/scheduler/seed ×3 + MySQL + Redis) into the
   namespace. Set every cache `storage.dir` to a `/disk/*` hostPath (default `/var/lib/dragonfly`
   would fill the OS root). Seed upstream = `LAN_REGISTRY`.
2. Write `/etc/containerd/certs.d/<lan-registry-host>/hosts.toml` → `http://127.0.0.1:4001` on the
   canary nodes first (`config_path=/etc/containerd/certs.d` is standard).
3. **Verify (no app dependency yet):** a manual `ctr pull` of a LAN-registry ref on a node is
   P2P-served — `back_to_source==1` on the seed only; Dragonfly pods healthy off the critical path.

---

## Step 4 — P3: transport cutover on a 2–3 node canary

**Code:**
- `run_warm` replaces the byte-stream: `ctr -n k8s.io images pull <lan-ref>` through the local
  mirror. Bytes arrive via P2P; 0042 only issues the trigger and records the row.
- Wire `warm` into DISPATCH + lifecycle + chain + `SERIALIZED_KINDS` (already pre-registered in P0).
  Keep `distribute` as a one-release alias to the same handler.
- Complete-delete fan-out: `purge_node` / `purge_p2p` / `purge_seed` / `registry_delete` /
  `purge_builder` / `purge_meta` (see plan for surface map).
- Seed-hot gate inside `run_warm` (block/backoff until the seed holds the task).
- Convergence reconciler, gated on `is_leader()`.

**Deploy:** label 2–3 canary nodes; point only their containerd at the mirror; route `warm` to the
canary while the fleet stays on `distribute`. **Verify** ready/delete/idle/self-heal on the canary.
**Revert** = remove the canary label (fleet is untouched).

---

## Step 5 — P4 / P5: fleet cutover + retire SSH push

1. Expand the mirror `hosts.toml` + `warm` to all eligible nodes; convergence reconciler live.
2. Once `warm` is proven fleet-wide, drop the `distribute` alias. SSH then remains only for
   triggers / purge / wedge-recovery.

---

## Step 6 — Enable HA (P6, recommended for 300)

1. Set `LEADER_ELECTION_ENABLED=true`, scale manager `replicas=3`. Confirm exactly one Lease holder:
   ```
   kubectl -n amd-oneclick-radeon-beta get lease amd-oneclick-manager-leader
   ```
   and that reapers/billing run once (not per replica).
2. Stage Postgres HA (leader election yields no availability if it fails over onto a dead single DB).
3. If a 2nd LAN host exists: standby registry (rsync `/disk/ssd2/registry`) + cold-standby agent
   behind the atomic claim. Else: keep the 0042 rebuild-from-tarball runbook and accept 0042 loss =
   a distribution outage (recoverable, in-flight P2P pulls survive a blip).

---

## Real end-to-end verification (run after the P3 canary; repeat after P4)

- **Admin upload:** image → all target nodes reach `loaded` & status `ready`; each node
  `ctr images ls` has the ref; dfdaemon shows P2P exchange (`back_to_source==1` seed only).
- **User custom:** build → `ready` only after push; launch → pod pinned to a node that has the
  image, `imagePullPolicy=IfNotPresent`, no pull secrets attached.
- **Complete delete:** admin delete → every node layer + P2P peer cache + seed cache + registry tag
  + 0042 tarball gone. Repeat with one node NotReady during delete → other nodes + seed + registry
  still purged; the down node is purged on rejoin.
- **Idle (R2c):** set a small grace in a test → an idle custom image full-purges incl. the registry
  tag, row `build_status='evicted'`; relaunch rebuilds from the stored Dockerfile.
- **Self-heal:** kill containerd mid-warm → quarantine + reconciler re-warm (no fleet re-stream);
  kill a seed → pulls continue; kill the agent mid-pull → heartbeat holds the lease, reaped only
  when truly dead.

---

## Decisions required before P1 (owner: you)

| # | Decision | Why it blocks | Options |
|---|---|---|---|
| 1 | **Registry engine** | Sets the delete/GC story and whether P1 needs a maintenance window | **registry:2** (battle-tested, per-manifest DELETE + offline `garbage-collect`; needs a push-pause during GC) vs **zot** (OCI-native, online/scheduled GC, no push-pause; slightly less ubiquitous) |
| 2 | **Second routed-LAN host?** | Decides whether 0042 can have real redundancy (P6) or only a rebuild runbook | Provision/identify a 2nd host on `10.5.10.x` → active standby; **or** accept 0042 as a single point kept recoverable via retained tarballs |
| 3 | **Registry sizing on `/disk/ssd2`** | Registry competes with build tarballs + containerd GC on 0042 NVMe | Confirm ~1–2 TB budget (Σ images 10–19 GB × versions × 1.3) and reconcile with the 85% disk-GC threshold |
| 4 | **Repo-path scoping in the LAN registry** | Prevents scoped delete from stranding/over-deleting shared blobs | Confirm `admin-<slug>` vs `user-<uid>` repo paths so admin and user images never share a manifest |
| 5 | **Pre-seed hot base layers to the seed?** | Only lever that speeds up the *custom cold-launch* (single-consumer) path | Decide after P3 canary latency numbers — pre-seed shared ROCm/PyTorch base layers, or leave as parity-with-today |

**Also confirm before flipping HA on (Step 6), not P1:** enabling `LEADER_ELECTION_ENABLED` +
manager `replicas=3` requires the Lease RBAC (get/create/update on `coordination.k8s.io/leases`) on
the manager service account, and Postgres HA staged alongside.
