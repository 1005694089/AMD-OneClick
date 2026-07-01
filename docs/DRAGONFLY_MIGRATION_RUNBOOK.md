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
> directly (zot registry service, agent binary, systemd) must run from 0042 or via a jump host.

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

## Ownership split

- **You (SSH to 0042):** everything host-side on node 0042 — the zot registry (native systemd
  service), its data dir, TLS/htpasswd, the containerd trust for zot's cert, and the agent
  `systemctl` restart when I hand you a new `agent.py`. You do **not** touch k8s or edit application
  code.
- **Me (kubectl via `KUBECONFIG=/home/zijun/7900_cluster_config`):** all cluster work — the manager
  `code-overrides` ConfigMap + rollout, the Dragonfly Helm install, DaemonSet/labels, Lease RBAC,
  and writing/committing all application + agent code on the branch.

Steps below are tagged **[YOU-0042]** (run over your SSH to 0042) or **[ME-k8s]** (I run these).

---

## Prerequisites — [YOU-0042] one-time host setup on node 0042

Run these over your SSH session to 0042. They stand up the zot LAN registry and prepare the host so
that when I ship the P1 agent code, the push/pull path just works. Nothing here depends on my k8s
steps — you can do it any time before P1.

> Values (confirmed from the cluster):
> - `LAN_IP = 10.5.10.43` (node `wx-ms-w7900d-0042`, its `10.5.10.x` InternalIP)
> - `REG_PORT = 5000`
> - `REGISTRY_DIR = /disk/ssd2/registry`
>
> The commands below use `10.5.10.43` directly. Set `<REGISTRY_PASSWORD>` to a value you choose.

**1. Confirm the disk budget and create the data dir**
```
df -h /disk/ssd2                 # expect ~3.5T available
sudo install -d -o imagesvc -g imagesvc -m 0750 /disk/ssd2/registry
```

**2. Generate TLS cert + basic-auth for the registry**
```
sudo install -d -m 0750 /etc/zot
# Self-signed cert with the LAN IP as a SAN (nodes connect by IP):
sudo openssl req -x509 -newkey rsa:4096 -nodes -days 3650 \
  -keyout /etc/zot/tls.key -out /etc/zot/tls.crt \
  -subj "/CN=amd-oneclick-lan-registry" \
  -addext "subjectAltName=IP:10.5.10.43"
# htpasswd (bcrypt). Pick a username/password; you'll give them to me for the k8s-side pull/push secret.
sudo sh -c 'htpasswd -Bbn imagesvc "<REGISTRY_PASSWORD>" > /etc/zot/htpasswd'
sudo chmod 0640 /etc/zot/tls.key /etc/zot/htpasswd
```
(If `htpasswd` is missing: `sudo apt-get install -y apache2-utils`.)

**3. Write the zot config** (`/etc/zot/config.json`) — online GC + dedupe + delete enabled, so no
push-pause is ever needed:
```
sudo tee /etc/zot/config.json >/dev/null <<'JSON'
{
  "distSpecVersion": "1.1.0",
  "storage": { "rootDirectory": "/disk/ssd2/registry", "dedupe": true,
    "gc": true, "gcDelay": "1h", "gcInterval": "24h" },
  "http": { "address": "0.0.0.0", "port": 5000,
    "tls": { "cert": "/etc/zot/tls.crt", "key": "/etc/zot/tls.key" },
    "auth": { "htpasswd": { "path": "/etc/zot/htpasswd" } } },
  "log": { "level": "info" },
  "extensions": { "scrub": { "interval": "24h" } }
}
JSON
```

**4. Run zot as a NATIVE systemd service (not a container).** zot is a single static Go binary, so
there is no runtime, no AppArmor/snap confinement, and no bind-mount question — it reads
`/disk/ssd2/registry` and `/etc/zot` directly. (Do **not** use snap Docker here: its AppArmor
profile blocks bind-mounts outside `$HOME`/`/media`, so `/disk/ssd2` and `/etc/zot` would fail or
mount empty. Native binary sidesteps all of that and matches the containerd/nerdctl-based cert-trust
+ smoke-test below.)
```
# Fetch the pinned static binary (verify the sha256 from the release page):
curl -fL -o /tmp/zot https://github.com/project-zot/zot/releases/download/v2.1.2/zot-linux-amd64
sudo install -m 0755 /tmp/zot /usr/local/bin/zot
zot --version                      # confirm v2.1.2

# systemd unit — runs as imagesvc (owns /disk/ssd2/registry), restart-always:
sudo tee /etc/systemd/system/zot.service >/dev/null <<'UNIT'
[Unit]
Description=zot OCI registry (AMD-OneClick LAN registry)
After=network-online.target
Wants=network-online.target

[Service]
User=imagesvc
Group=imagesvc
ExecStart=/usr/local/bin/zot serve /etc/zot/config.json
Restart=always
RestartSec=5
# Least-privilege hardening; registry data + config are the only writable paths it needs:
ReadWritePaths=/disk/ssd2/registry
ProtectSystem=strict
ProtectHome=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable --now zot.service
systemctl status zot.service --no-pager    # active (running)
journalctl -u zot -n 30 --no-pager         # confirm it bound :5000 with TLS
```
(`imagesvc` must be able to read `/etc/zot/tls.key` + `/etc/zot/htpasswd` — step 2 set mode 0640;
`chown imagesvc:imagesvc /etc/zot/tls.key /etc/zot/htpasswd` if they came out root-owned. With
`ProtectSystem=strict`, `/etc/zot` stays readable but `/disk/ssd2/registry` needs the explicit
`ReadWritePaths` above.)

**5. Trust the registry cert on 0042's containerd/nerdctl** (so the agent's push authenticates):
```
sudo install -d /etc/containerd/certs.d/10.5.10.43:5000
sudo tee /etc/containerd/certs.d/10.5.10.43:5000/hosts.toml >/dev/null <<TOML
server = "https://10.5.10.43:5000"
[host."https://10.5.10.43:5000"]
  capabilities = ["pull", "resolve", "push"]
  ca = "/etc/zot/tls.crt"
TOML
```

**6. Smoke-test the registry from 0042 (no app involvement)**
```
# login + round-trip a tiny image to prove push/pull/delete all work:
sudo nerdctl login 10.5.10.43:5000 -u imagesvc -p '<REGISTRY_PASSWORD>'
sudo nerdctl pull public.ecr.aws/docker/library/hello-world:latest || true
sudo nerdctl tag  hello-world:latest 10.5.10.43:5000/smoke/hello:1
sudo nerdctl push 10.5.10.43:5000/smoke/hello:1
curl -u imagesvc:'<REGISTRY_PASSWORD>' --cacert /etc/zot/tls.crt \
  https://10.5.10.43:5000/v2/_catalog          # expect {"repositories":["smoke/hello"]}
# delete round-trip (proves the delete extension is live):
DIGEST=$(curl -sI -u imagesvc:'<REGISTRY_PASSWORD>' --cacert /etc/zot/tls.crt \
  -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
  https://10.5.10.43:5000/v2/smoke/hello/manifests/1 | awk -F': ' '/docker-content-digest/{print $2}' | tr -d '\r')
curl -u imagesvc:'<REGISTRY_PASSWORD>' --cacert /etc/zot/tls.crt -X DELETE \
  https://10.5.10.43:5000/v2/smoke/hello/manifests/$DIGEST   # expect 202
```

**7. Hand me these values** (for the k8s-side secrets + the agent EnvironmentFile — I do the wiring):
- `LAN_IP` and confirmation zot is up (step 6 catalog worked).
- the registry `username` + `password` you set.
- the contents of `/etc/zot/tls.crt` (the public cert only — safe to share; I mount it so nodes and
  the manager trust zot).

> **Env file note:** the agent's `LAN_REGISTRY=10.5.10.43:5000`, `IMAGE_SERVICE_KINDS` (adds `push`,
> later `warm`/`purge_*`), and `DOCKER_CONFIG` login for zot all live in
> `/etc/amd-oneclick-image-service.env` on 0042. I'll give you the exact lines to add when the P1
> agent code is ready; you paste them in and `sudo systemctl restart image-service.service`.

---

## Step 0 — Deploy P0 (safe; distribution transport unchanged)  **[ME-k8s + one YOU-0042 restart]**

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
6. **[YOU-0042] Deploy the P0 agent.** I hand you the new `agent.py` (I can't reach 0042); you place
   it and restart the unit — no code editing on your side, just install + restart:
   ```
   # copy the file I give you to 0042 first (scp from wherever you received it), then:
   sudo install -m 0755 ~/agent.py /opt/amd-oneclick/image-service/agent.py
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
3. **Registry engine: zot (decided).** No GC-lock/push-pause needed. Push/delete code is written
   against the registry v2 API so it is engine-neutral; zot's online GC handles blob reclaim.

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
1. **[YOU-0042]** Stand up zot per the **Prerequisites** section above (native systemd service on
   `10.5.10.43:5000`, online GC + delete extension, cert-trust, smoke test). If already done, skip.
2. **[YOU-0042]** When I give you the P1 `agent.py` + the exact env lines, add
   `LAN_REGISTRY=10.5.10.43:5000` and `push` (→ `IMAGE_SERVICE_KINDS`) to
   `/etc/amd-oneclick-image-service.env`, install the new `agent.py`, and
   `sudo systemctl restart image-service.service`.
3. **[ME-k8s]** Create the registry pull/push secret + trust the zot cert cluster-side (from your
   handed-over creds + `tls.crt`); regenerate the manager `code-overrides` ConfigMap with the P1
   code and roll the manager.

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

## Decisions made (locked 2026-06-30)

| # | Decision | Choice | Consequence for the build |
|---|---|---|---|
| 1 | **Registry engine** | **zot** | OCI-native, online/scheduled GC → **no push-pause / GC-lock needed**. Delete = manifest DELETE via the registry v2 API; blob reclaim is zot's background GC. The `push ↔ registry_delete` mutual-exclusion in the plan is **dropped** for zot. |
| 2 | **0042 redundancy** | **Recoverable single point for now** (2nd LAN host coming later) | P6 ships the rebuild-from-tarball runbook, not active standby. Build tarballs are **retained** (already the P1 design). When the 2nd host arrives, add the standby registry (rsync `/disk/ssd2/registry`) + cold-standby agent then. |
| 3 | **Registry disk on `/disk/ssd2`** | **3.5 TB available** | Comfortable for Σ images 10–19 GB × versions × 1.3. Set zot's data dir under `/disk/ssd2/registry`; still reconcile with the 85% disk-GC threshold so registry growth doesn't trip node GC on 0042. |
| 4 | **Repo-path scoping** | **`admin-<slug>` vs `user-<uid>`** | Admin and user images never share a manifest, so scoped delete is safe. Keep the existing tag-prefix scheme. |
| 5 | **Pre-seed hot base layers** | **Deferred** | Revisit after P3 canary latency numbers. Custom cold-launch stays parity-with-today until then. |

**Still to confirm before flipping HA on (Step 6), not P1:** enabling `LEADER_ELECTION_ENABLED` +
manager `replicas=3` requires the Lease RBAC (get/create/update on `coordination.k8s.io/leases`) on
the manager service account, and Postgres HA staged alongside.
