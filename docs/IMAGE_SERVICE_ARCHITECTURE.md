# Image Service Architecture (Dragonfly P2P + self-hosted zot registry)

How the AMD-OneClick image-service distributes and deletes GPU-notebook images across the fleet,
and how each component connects to the Kubernetes cluster.

- **Goal:** distribute 10–19 GB images to 100 → 300+ GPU nodes, delete them completely on demand,
  auto-delete 5-day-idle custom images, keep all image data on nvme `/disk/*`, and leave the
  admin/user UI unchanged.
- **Approach:** build on node 0042 → push to a self-hosted **zot** LAN registry that 0042 runs →
  each GPU node **self-pulls by ref through its own per-node Dragonfly P2P mirror** (bytes move
  peer-to-peer over the LAN, never streamed N× from 0042).

Scope note: a manager instance is confined to nodes carrying its service taint
(`NOTEBOOK_TOLERATION_KEY`, e.g. `amd-oneclick/beta=radeon`). The beta service's fleet is exactly
its tainted nodes (currently 0043 + 0044); untainted production nodes are never targeted.

---

## Diagram 1 — Topology (components + connections)

```
                          ┌───────────────────────────── ADMIN / USER (browser) ───────────────────────────┐
                          │                         UI unchanged (upload / launch / delete)                  │
                          └───────────────────────────────────────┬───────────────────────────────────────┘
                                                                   │ HTTPS
        ══════════════════════════════════════ K8s CLUSTER ═══════▼══════════════════════════════════════════
        ║                                                                                                    ║
        ║  ns: amd-oneclick-radeon-beta                          ns: dragonfly-system  (pod overlay 10.232.x)║
        ║  ┌──────────────────────────────────┐                 ┌────────────────────────────────────────┐ ║
        ║  │  MANAGER  (FastAPI pod)           │                 │  Dragonfly CONTROL PLANE               │ ║
        ║  │  • has k8s API (kubectl)          │   schedules     │   manager ×3   scheduler ×3            │ ║
        ║  │  • owns job queue + chain seq     │◄───peers──────► │   + MySQL + Redis (Dragonfly's own)    │ ║
        ║  │  • resolves node targets          │                 │                                        │ ║
        ║  │  • leader-gated PURGE drain (15s) │                 │   SEED ×3 (StatefulSet)                │ ║
        ║  │    └─ kubectl-exec dfctl ─────────┼──────exec──────►│   dfdaemon :4000, cache /disk/*        │ ║
        ║  └───────┬───────────────────▲──────┘                 └──────────────▲─────────────────────────┘ ║
        ║          │ SQL                │ HTTP  /api/internal/jobs/claim                │ P2P (pull miss →   ║
        ║  ┌───────▼────────┐          │ (agent polls)                                 │  fetch from zot)   ║
        ║  │  Postgres      │          │                                               │                    ║
        ║  │  image_jobs    │          │        DaemonSet: dragonfly-client (hostNetwork)                   ║
        ║  │  images/custom │          │        only on nodes labeled dragonfly-client=enabled             ║
        ║  │  image_nodes   │          │        ┌───────────────────┐   ┌───────────────────┐              ║
        ║  └────────────────┘          │        │ GPU node 0043     │   │ GPU node 0044     │  ← beta fleet ║
        ║                              │        │  dfdaemon         │   │  dfdaemon         │    (tainted)  ║
        ║                              │        │  proxy 127.0.0.1: │   │  proxy 127.0.0.1: │              ║
        ║                              │        │       4001 ◄──────┼───┼─── containerd     │              ║
        ║                              │        │  (hosts.toml)     │   │   pulls via mirror│              ║
        ║                              │        └─────────▲─────────┘   └─────────▲─────────┘              ║
        ║   (production nodes 0066/0067… NOT labeled → no dfdaemon, never targeted by beta)                ║
        ═══════════════════════════════╪═════════════════════════╪═════════════════════════╪═══════════════
                                        │ SSH claim/report        │ node-SSH (build trigger,│ P2P LAN
                                        │                         │  purge_node, legacy     │ (bytes move
        ┌───────────────────────────────▼─────────────────────────▼──────────┐              │  peer→peer)
        │  NODE 0042  (host, LAN 10.5.10.43 — OUTSIDE pod network)            │              │
        │                                                                     │              │
        │   AGENT (image-service.service, systemd)   ◄── polls manager        │              │
        │    • build / pull / push / warm(trigger) / evict                    │              │
        │    • purge_node · registry_delete · purge_builder                   │              │
        │    • node-SSH key + zot push creds                                  │              │
        │                                                                     │              │
        │   zot REGISTRY (systemd, 10.5.10.43:5000, TLS+auth)  ◄──────────────┼──────────────┘
        │    • source of truth;  data on /disk/ssd2/registry                  │   seeds fetch here on cache miss
        │   build tarballs (backup) on /disk/ssd2/image-tars                  │
        └─────────────────────────────────────────────────────────────────────┘
```

### How it connects to the k8s cluster
- **Manager — inside the cluster (a pod).** The only component with the k8s API. It resolves which
  nodes to target (taint-scoped), sequences job chains, serves the agent's job-poll API, and drives
  seed/P2P-cache purges by `kubectl exec` into `dragonfly-system` pods. Reads/writes the app queue in
  Postgres.
- **Agent — outside the pod network,** on the 0042 host. Reaches the cluster only via (a) polling the
  manager's HTTP API (`/api/internal/jobs/claim`) and (b) SSH to GPU-node *hosts*. It cannot reach
  pod-overlay IPs (10.232.x) — which is why seed-cache purge must run on the manager, not the agent.
- **dfdaemon — the bridge.** A DaemonSet pod per labeled node, hostNetwork, so the node's containerd
  pulls through `127.0.0.1:4001` and image bytes arrive **peer-to-peer over the node LAN** instead of
  being streamed from 0042. A node participates in the P2P mesh only if it is labeled
  `dragonfly-client=enabled` AND has `/etc/containerd/certs.d/10.5.10.43:5000/hosts.toml`.
- **zot registry — on 0042 (systemd, not a pod).** Durable source of truth; seeds fetch from it once
  on a cache miss, then the mesh fans out. Build tarballs are retained on `/disk` as the rebuild
  backup.

---

## Diagram 2 — Data flows (upload vs. delete)

```
UPLOAD  (admin image / custom build)            "ready" = durable in zot + loaded rows
────────────────────────────────────────────────────────────────────────────────────
 manager enqueues chain ──► [ build|pull ] ─► [ push ] ─► [ warm ]        (per node)
                                 │              │            │
        agent builds/pulls ──────┘              │            │
        tarball on /disk ──────────► agent `nerdctl push --oci` ─► zot (10.5.10.43:5000)
                                                │            │
                                     digest+blob_list ──► manager records on images row
                                                             │
                        agent triggers on each node:  ctr pull <zot-ref> --hosts-dir
                                                             │
                        containerd → 127.0.0.1:4001 (dfdaemon) → P2P mesh / seed → zot(miss)
                                                             │
                        agent reports per-node result ─► manager writes image_nodes(loaded)
                                                             │
                        get_image_sync_status: ready when loaded_count == desired_count
   (legacy fallback: image with NO zot digest → chain tail = `distribute` = SSH byte-push from 0042)


DELETE  (admin/user delete OR 5-day idle)        6 independent, idempotent, reaper-retried surfaces
────────────────────────────────────────────────────────────────────────────────────
 manager enqueue_purge_fanout(ref, blob_ids = UNIQUE-to-this-image)
        │
        ├─ purge_node       [AGENT]    ctr images rm + content prune        (per node)
        ├─ registry_delete  [AGENT]    DELETE zot manifest by digest
        ├─ purge_builder    [AGENT]    rm tarball + buildkit prune
        ├─ purge_p2p        [MANAGER]  kubectl-exec `dfctl task rm <blob>`  → each node dfdaemon
        ├─ purge_seed       [MANAGER]  kubectl-exec `dfctl task rm <blob>`  → each seed
        └─ purge_meta       [MANAGER]  drop image_nodes rows + mark evicted
                                          ▲
                    GATE: purge_meta runs ONLY after the other 5 succeed
                    (else bytes would be orphaned).  Failed surface → requeued until it converges.
                    blob_ids are UNIQUE to the image → blobs shared with a live image survive.
```

### Notes
- **Transport is P2P by default (`warm`).** 0042 sends ~no image bytes; seeds fetch once from zot and
  the node mesh fans out — the 300-node scaling win. `distribute` (SSH byte-push from 0042) is
  **retired as the default** and kept only as the fallback for legacy images that have no zot copy
  (no recorded digest). It is safe to delete `run_distribute` once every image has a zot digest.
- **Delete is a split fan-out.** Node-local + registry + builder surfaces run on the **agent**
  (reachable from 0042); the P2P-cache and seed-cache surfaces run on the **manager** (only it can
  reach the overlay dfdaemons, via `dfctl task rm`). `purge_meta` gates last so DB handles (the only
  on-disk-byte handle) are never dropped while bytes remain. Gated behind `PURGE_FANOUT_ENABLED`.
- **task_id == blob-digest hex** in this Dragonfly build (`enableTaskIDBasedBlobDigest`), so an
  image's cache tasks are exactly its OCI config+layer digests (captured at push into `blob_list`);
  delete removes only the blobs unique to the image, leaving shared base layers for Dragonfly's
  `taskTTL` to reap.

---

## Job kinds (queue: `image_jobs`)

| Kind | Runs on | Purpose |
|---|---|---|
| `build` / `pull` | agent (0042) | build from Dockerfile / pull from source registry → tarball on /disk |
| `push` | agent (0042) | `nerdctl push --oci` to zot; record digest + blob_list |
| `warm` | agent trigger → node dfdaemon | per-node `ctr pull` of the zot ref through the P2P mirror |
| `distribute` | agent (0042) | **legacy fallback** — SSH byte-push of the tarball (`ctr import`) |
| `evict` | agent | pre-P4 layers-only removal (unused once fan-out enabled) |
| `purge_node` | agent | containerd layer removal per node |
| `purge_p2p` | **manager** | `dfctl task rm` into each node dfdaemon (P2P cache) |
| `purge_seed` | **manager** | `dfctl task rm` into each seed (seed cache) |
| `registry_delete` | agent | DELETE zot manifest by digest |
| `purge_builder` | agent | rm build tarball + buildkit prune |
| `purge_meta` | **manager** | drop `image_nodes` rows + mark custom image evicted (gated last) |

## Disk layout (`/disk/*` only)
- `/disk/ssd2/registry` — zot blobs (source of truth, on 0042)
- `/disk/ssd2/image-tars` — build tarballs (rebuild backup, on 0042)
- `/disk/*/dragonfly/{peer,seed}` — dfdaemon caches (per-node clients + seeds)
- `/etc/containerd/certs.d/10.5.10.43:5000/hosts.toml` — the only non-`/disk` artifact (per-node mirror config)
```
