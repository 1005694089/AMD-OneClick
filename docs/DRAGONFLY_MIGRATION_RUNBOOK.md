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
> The commands below use `10.5.10.43` directly. The registry password is **auto-generated**
> (`REGPW=$(openssl rand -hex 24)` in step 3) and handed back to me — you don't pick one.

> **Review applied (18-agent adversarial pass, 5 blockers + 6 majors confirmed).** The fixes are
> baked into the commands below. Key ones: zot `http.port` must be a **quoted string** and
> `http.address` must be the **LAN IP** (not `0.0.0.0`) — either mistake is a hard failure or an
> exposure on this shared node; the `/disk/ssd2` **mount is asserted** before seeding and depended on
> by systemd; the password is **hex** and passed via **stdin**; and the cert is added to the **system
> trust store** so the *rootless* imagesvc agent (the real P1 pusher) trusts zot — the root smoke
> test alone does not prove that.

**1. Assert the mount, confirm budget, create the data dir**
```
# HARD-STOP if /disk/ssd2 is not actually mounted — otherwise `install -d` would silently seed the
# registry onto the root filesystem.
findmnt -rno TARGET /disk/ssd2 >/dev/null || { echo 'FATAL: /disk/ssd2 not mounted'; exit 1; }
grep -q " /disk/ssd2 " /etc/fstab || echo 'WARN: /disk/ssd2 not in /etc/fstab — add it so it persists across reboot'
df -h /disk/ssd2                 # expect ~3.5T available
sudo install -d -o imagesvc -g imagesvc -m 0750 /disk/ssd2/registry
```

**2. Install the toolchain**
```
sudo apt-get update && sudo apt-get install -y apache2-utils   # provides htpasswd
```

**3. Generate TLS cert + basic-auth (auto-generated hex password, passed via stdin)**
```
sudo install -d -m 0750 /etc/zot
# Self-signed cert with the LAN IP as a SAN (nodes connect by IP):
sudo openssl req -x509 -newkey rsa:4096 -nodes -days 825 \
  -keyout /etc/zot/tls.key -out /etc/zot/tls.crt \
  -subj "/CN=amd-oneclick-lan-registry" \
  -addext "subjectAltName=IP:10.5.10.43"
# HEX password (no /,+,= — safe in htpasswd, nerdctl, curl, and the k8s dockerconfigjson at P1):
REGPW=$(openssl rand -hex 24)
# bcrypt htpasswd via STDIN so the password never appears in `ps`/bash history on this shared node:
printf '%s' "$REGPW" | sudo htpasswd -iBc /etc/zot/htpasswd imagesvc
# service runs as imagesvc → it must be able to READ these:
sudo chown root:imagesvc /etc/zot/tls.key /etc/zot/htpasswd /etc/zot/tls.crt
sudo chmod 0640 /etc/zot/tls.key /etc/zot/htpasswd
sudo chmod 0644 /etc/zot/tls.crt
sudo -u imagesvc test -r /etc/zot/htpasswd && echo "imagesvc can read htpasswd OK"
echo "REGISTRY PASSWORD (hand this back, then clear your scrollback): $REGPW"
```

**3b. Trust the zot CA in the SYSTEM trust store** — so the *rootless imagesvc* agent (the real P1
pusher, which does NOT read `/etc/containerd/certs.d`) trusts zot via Go's default cert pool. This is
the robust host-side action; do it now.
```
sudo cp /etc/zot/tls.crt /usr/local/share/ca-certificates/amd-oneclick-zot.crt
sudo update-ca-certificates          # adds it to /etc/ssl/certs used by Go/Python/curl for all users
```

**4. Write the zot config** (`/etc/zot/config.json`). **`port` is a quoted string** and **`address`
is the LAN IP** — both are load-bearing for v2.1.2 (`UnmarshalExact` rejects a bare-int port and any
unknown key; `0.0.0.0` would expose the registry on every interface of a shared node):
```
sudo tee /etc/zot/config.json >/dev/null <<'JSON'
{
  "distSpecVersion": "1.1.0",
  "storage": { "rootDirectory": "/disk/ssd2/registry", "dedupe": true,
    "gc": true, "gcDelay": "1h", "gcInterval": "24h" },
  "http": { "address": "10.5.10.43", "port": "5000",
    "tls": { "cert": "/etc/zot/tls.crt", "key": "/etc/zot/tls.key" },
    "auth": { "htpasswd": { "path": "/etc/zot/htpasswd" } } },
  "log": { "level": "info" },
  "extensions": { "scrub": { "enable": true, "interval": "24h" } }
}
JSON
sudo chown root:imagesvc /etc/zot/config.json && sudo chmod 0644 /etc/zot/config.json
```

**5. Install the pinned zot binary + run it as a NATIVE systemd service (not a container).** zot is a
single static Go binary — no runtime, no AppArmor/snap confinement, no bind-mount question. (Do
**not** use snap Docker: its AppArmor profile blocks bind-mounts outside `$HOME`/`/media`, so
`/disk/ssd2` and `/etc/zot` would fail/mount-empty.)
```
# Fetch the pinned static binary to a temp path, verify its checksum against the release, then install
# atomically (so a re-run never corrupts a running binary):
curl -fL --retry 3 -o /tmp/zot \
  https://github.com/project-zot/zot/releases/download/v2.1.2/zot-linux-amd64
curl -fL --retry 3 -o /tmp/zot.sums \
  https://github.com/project-zot/zot/releases/download/v2.1.2/checksums.sha256.txt
grep 'zot-linux-amd64$' /tmp/zot.sums | awk -v f=/tmp/zot '{print $1"  "f}' | sha256sum -c - \
  || { echo 'FATAL: zot checksum mismatch'; exit 1; }
sha256sum /tmp/zot          # record this digest for the handback
sudo systemctl stop zot.service 2>/dev/null || true   # safe if re-running; no-op on first install
sudo install -m 0755 /tmp/zot /usr/local/bin/zot
/usr/local/bin/zot --version                          # confirm v2.1.2

# VALIDATE the config BEFORE enabling the service — catches the strict-parse / port-type class of
# errors while nothing is running:
/usr/local/bin/zot verify /etc/zot/config.json && echo "config OK"

# systemd unit — runs as imagesvc; depends on the /disk/ssd2 mount so it can never start early and
# write registry data onto the root fs:
sudo tee /etc/systemd/system/zot.service >/dev/null <<'UNIT'
[Unit]
Description=zot OCI registry (AMD-OneClick LAN registry)
After=network-online.target
Wants=network-online.target
RequiresMountsFor=/disk/ssd2/registry

[Service]
User=imagesvc
Group=imagesvc
ExecStart=/usr/local/bin/zot serve /etc/zot/config.json
Restart=always
RestartSec=5
# Least-privilege hardening; registry data is the only writable path it needs:
ReadWritePaths=/disk/ssd2/registry
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
NoNewPrivileges=true
LimitNOFILE=524288

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable --now zot.service
systemctl status zot.service --no-pager    # active (running)
journalctl -u zot -n 30 --no-pager         # confirm it bound 10.5.10.43:5000 with TLS
```

**6. (Optional) rootful cert trust for the root smoke test only.** The production agent is *rootless*
and already covered by the system trust store (step 3b); this drop only lets the STEP 7 `sudo nerdctl`
sanity check resolve the cert. Skip it if you rely on the system trust store.
```
sudo install -d /etc/containerd/certs.d/10.5.10.43:5000
sudo tee /etc/containerd/certs.d/10.5.10.43:5000/hosts.toml >/dev/null <<TOML
server = "https://10.5.10.43:5000"
[host."https://10.5.10.43:5000"]
  capabilities = ["pull", "resolve", "push"]
  ca = "/etc/zot/tls.crt"
TOML
```

**7. Smoke-test the registry from 0042.** The pure-`curl` checks below prove push/catalog/delete
against zot without depending on any container client or external image pull.
```
# Build a tiny local image with NO external egress (only GitHub egress is confirmed on 0042), as
# imagesvc so it exercises the ROOTLESS stack the agent actually uses:
IMGSVC_UID=$(id -u imagesvc)
printf 'FROM scratch\nCOPY /etc/hostname /marker\n' | \
  sudo -u imagesvc env XDG_RUNTIME_DIR=/run/user/$IMGSVC_UID \
    XDG_CONFIG_HOME=/disk/ssd2/imagesvc/.config \
    CONTAINERD_ADDRESS=/run/user/$IMGSVC_UID/containerd/containerd.sock \
    BUILDKIT_HOST=unix:///run/user/$IMGSVC_UID/buildkit/buildkitd.sock \
    DOCKER_CONFIG=/etc/amd-oneclick/docker \
    nerdctl --namespace k8s.io build -t 10.5.10.43:5000/smoke/hello:1 -f - /tmp
# Log in AS imagesvc into the agent's DOCKER_CONFIG (NOT root's ~/.docker) via stdin, then push:
printf '%s' "$REGPW" | sudo -u imagesvc env XDG_RUNTIME_DIR=/run/user/$IMGSVC_UID \
    CONTAINERD_ADDRESS=/run/user/$IMGSVC_UID/containerd/containerd.sock \
    DOCKER_CONFIG=/etc/amd-oneclick/docker \
    nerdctl login 10.5.10.43:5000 -u imagesvc --password-stdin
sudo -u imagesvc env XDG_RUNTIME_DIR=/run/user/$IMGSVC_UID \
    CONTAINERD_ADDRESS=/run/user/$IMGSVC_UID/containerd/containerd.sock \
    DOCKER_CONFIG=/etc/amd-oneclick/docker \
    nerdctl push 10.5.10.43:5000/smoke/hello:1
```
> **If the rootless backend isn't provisioned yet** (setup.sh's rootless containerd/buildkit user
> services), the `sudo -u imagesvc nerdctl` commands will fail — that's expected pre-P1. In that case
> run the container step later; the pure-`curl` checks below still prove zot itself is healthy now.

```
# --- Pure-curl checks (no container client needed; system trust store covers the cert) ---
curl -fsS -u imagesvc:"$REGPW" https://10.5.10.43:5000/v2/_catalog    # {"repositories":["smoke/hello"]}
# delete round-trip proves the DELETE path (broad Accept; strip CR):
DIGEST=$(curl -fsSI -u imagesvc:"$REGPW" \
  -H 'Accept: application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json' \
  https://10.5.10.43:5000/v2/smoke/hello/manifests/1 \
  | awk -F': ' 'tolower($1)=="docker-content-digest"{print $2}' | tr -d '\r')
curl -fsS -u imagesvc:"$REGPW" -X DELETE \
  https://10.5.10.43:5000/v2/smoke/hello/manifests/$DIGEST -o /dev/null -w '%{http_code}\n'  # 202
```

**8. Hand me these values** (for the k8s-side secrets + the agent EnvironmentFile — I do the wiring):
- `LAN_IP=10.5.10.43` and confirmation zot is up (step 7 `_catalog` returned `smoke/hello`).
- the registry `username=imagesvc` + the generated `$REGPW`.
- the contents of `/etc/zot/tls.crt` (the public cert only — safe to share; I mount it so the
  manager + GPU nodes trust zot cluster-side).
- the recorded zot binary `sha256` (from step 5) for reproducibility.
Then clear the password from your shell: `unset REGPW; history -c` (and clear your terminal scrollback).

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
