# radeon-global — Scale Architecture (500–1000 concurrent active users)

Namespace: `amd-oneclick-lablab` · External: https://radeon-global.anruicloud.com

This document describes the production control-plane design that lets the AMD-OneClick
manager serve 500–1000 concurrent **active/polling** users on radeon-global, the
hardening that got it there, and the verified capacity limits.

---

## 1. Capacity summary (what "concurrent users" means here)

There are three distinct ceilings; do not conflate them.

| Dimension | Ceiling | Basis |
|---|---|---|
| **Active / polling users** (browsing, status-polling, proxied notebook traffic) | **500–1000+, verified headroom well beyond** | Load test #3 below: 400 concurrent in-flight requests → ~870 req/s at p99≈3s with manager pods **~87% idle** and Postgres at 12/100 connections. |
| **Running instances** (live GPU notebooks) | **~500–990** | Hardware: 990 GPUs total; one single-GPU instance per GPU. Control plane is not the limit here. |
| **Simultaneous launch burst** (N users clicking "launch" at the same instant) | **Latency-bound, NOT independently load-tested** | `create_instance` performs several sequential blocking k8s calls (~1–3s each), served by an 8-thread launch pool per worker process. A large simultaneous spike still queues. See §7. |

The headline figure: **500 concurrent active users is met with large margin; the control
plane runs mostly idle at that load.** The tested throughput plateau is a property of the
closed-loop test client (coordinated omission), not the servers.

---

## 2. Topology

```
                         Internet
                            │  https://radeon-global.anruicloud.com
                            ▼
                    NodePort 30080  (Service amd-oneclick-lablab-manager,
                            │        externalTrafficPolicy=Cluster)
        ┌───────────────────┴────────────────────┐
        ▼                                         ▼
  manager pod A (node s-00X)              manager pod B (node s-00Y)
  uvicorn --workers 2                     uvicorn --workers 2
  ├─ worker proc 1: event loop           ├─ worker proc 1: event loop
  │   + scheduler + leader elector        │   + scheduler + leader elector
  └─ worker proc 2: event loop           └─ worker proc 2: event loop
      + scheduler + leader elector            + scheduler + leader elector
        │        │                              │        │
        └────────┴──────────────┬───────────────┴────────┘
                                 │  (4 processes total; exactly ONE acts
                                 │   as leader via a coordination.k8s.io Lease)
             ┌───────────────────┼─────────────────────┐
             ▼                   ▼                     ▼
     Postgres (CPU node 0014)  Redis (CPU node 0018)  Kubernetes API
     - all app state           - rate-limit counters   - pod lifecycle
     - max_connections=100     - AUTH enabled          - Lease (leader election)
     - NetworkPolicy: ingress  - NetworkPolicy: ingress
       from manager pods only    from manager pods only
```

- **2 pods on 2 distinct nodes** (required pod anti-affinity by hostname) → real HA.
- **2 uvicorn workers per pod** → 2 event loops per pod, 4 total → CPU parallelism for the
  single-threaded asyncio request path.
- **Datastore isolated** on CPU-tainted nodes, brand-new, not shared with the v2 stack in
  `default`.

---

## 3. Manager Deployment (live spec)

| Field | Value | Rationale |
|---|---|---|
| `replicas` | 2 | Horizontal capacity + HA. |
| image | `10.5.10.89:1808/xinwei/amd-oneclick-manager:fence1-w2` | CMD bakes `uvicorn --workers 2`. |
| `strategy` | RollingUpdate `maxSurge=0 / maxUnavailable=1` | Caps concurrent pods at `replicas`, so Postgres connections never exceed `replicas × workers × 15`. No transient surge spike. |
| resources | limits `cpu=4 mem=2Gi`, requests `cpu=1 mem=512Mi` | 2 workers × ~1 core headroom; observed RSS ~250Mi (huge margin under 2Gi). |
| nodeAffinity | allowlist `[s-001, s-002, s-003]` by `kubernetes.io/hostname` | Explicit placement, off datastore/notebook-critical nodes. Not taint-reliant. |
| podAntiAffinity | required, `topologyKey=kubernetes.io/hostname` | Hard guarantee of 2-node spread (HA). |
| PodDisruptionBudget | `minAvailable: 1` | Survives coincident node drain/maintenance. |
| probes | readiness + liveness on `/health` | Pod only receives traffic once actually serving. |

There is **no `nodeName` pin** (removed during the replicas=2 rollout) and **no `--workers`
in the Deployment args** (it comes from the image CMD, single source of truth).

---

## 4. Datastore

### Postgres (`amd-oneclick-postgres`, CPU node wu-ms-w7900d-0014)
- Single instance, `strategy: Recreate`, NFS RWO PVC (`oneclick-cpu-nfs-1`, 20Gi).
- `securityContext runAsUser/runAsGroup: 65534` — required because the NFS export is
  root_squash; the postgres entrypoint's chown otherwise fails and CrashLoops.
- `PGDATA=/var/lib/postgresql/data/pgdata` (subdir, avoids NFS mount-root/lost+found collision).
- `pg_isready` readiness + liveness probes.
- `max_connections=100`, `superuser_reserved_connections=3` → **97 usable**.

### Connection budget
Engine (`app/store.py`) is created **per process** at import, pinned explicitly:
`pool_size=5, max_overflow=10, pool_recycle=1800` → **15 connections/process** ceiling.

| Config | Processes | Max conns | vs 97 |
|---|---|---|---|
| replicas=2 × workers=2 | 4 | 60 | 62% ✓ |
| (future) replicas=3 × workers=2 | 6 | 90 | 93% — raise `max_connections` to 200 first |
| (future) replicas=4 × workers=2 | 8 | 120 | **>97** — requires `max_connections` bump |

Observed live: idle floor ~8–12, never stressed under load (pool is lazy).

### Redis (`amd-oneclick-redis`, CPU node wx-ms-w7900d-0018)
- Ephemeral (no PVC), AUTH via secret, used for rate-limit counters with graceful fallback.

### Network isolation
Two ingress-only NetworkPolicies restrict PG:5432 and Redis:6379 to pods labeled
`app=amd-oneclick-lablab-manager`. All manager pods carry that label, so every replica is
admitted; nothing else in the cluster can reach the datastore.

---

## 5. Leader election & job fencing (the core correctness mechanism)

With 4 processes each running their own APScheduler + elector (FastAPI `lifespan()` runs
per process), the cluster-destructive background jobs (reconcile → force-deletes pods;
cleanup → billing) **must run on exactly one process**. This is enforced by a
`coordination.k8s.io/v1` Lease (`amd-oneclick-manager-leader`, duration 15s, renew 5s).

Each process's identity is `f"{hostname}-{pid}"`, so all 4 candidates are distinct and
genuinely contend.

### Fencing design (`app/leader.py`, `app/scheduler.py`)
The cached "am I leader" flag alone is **not** trusted, because under CPU starvation (a load
spike) the renew daemon thread may not run and the flag can go stale-True while a peer takes
over. Two independent guards close this:

1. **Time fence in `is_leader()`** — refuses to act as leader once the last successful lease
   renew is older than `LEADER_LEASE_RENEW_DEADLINE_SECONDS=10` (strictly < the 15s lease
   duration; the 5s gap absorbs clock skew + peer read/act latency). Measured on
   `time.monotonic()`, so an NTP / VM wall-clock step-back cannot hold the fence open.
   Mirrors client-go's `RenewDeadline < LeaseDuration`.
2. **Per-iteration recheck** — `reconcile_job` (stuck/orphan/terminal/gone loops) and
   `cleanup_job` (billing sweep) re-check `is_leader()` before each destructive action, so an
   O(N) loop cannot keep deleting pods / billing after losing the lease mid-cycle.

Lease acquisition uses `replace_namespaced_lease` on the read object (carries
`resourceVersion`) → the API server enforces optimistic concurrency; a losing racer gets
409 and never marks itself leader. On graceful shutdown `elector.stop()` releases the lease,
so failover is fast (~6s observed) rather than waiting for 15s expiry.

**Verified:** exactly one reconcile + one cleanup per cycle across all 4 processes; deleting
the entire leader pod fails over in ~6s with zero external downtime.

---

## 6. Request-path performance hardening

The event loop was the original bottleneck; blocking calls are offloaded (`app/main.py`,
`app/k8s_client.py`):

- **Launch path** — `create_instance` (all 4 provisioning sites) runs on a dedicated
  8-thread `_launch_executor` via `run_in_executor`, off the event loop.
- **Status path** — `get_instance_by_id` / `get_pod_status_details` / `get_startup_detail`
  via `asyncio.to_thread`.
- **Proxy hot path** — ClusterIP resolution cached in `k8s_client` (15s TTL, lock + monotonic
  epoch guard), invalidated at the universal `_delete_service` chokepoint so a delete+relaunch
  can't route to a stale IP (avoids cross-tenant misroute).
- **Billing sweep** — one `list_managed_pod_states()` LIST per cycle instead of a per-instance
  GET; absence still falls through to an authoritative GET (no TOCTOU mis-delete).
- **Memory profiles** — GPU-instance limits recalibrated to real node size
  (503.5GiB/8GPU ≈ 62.9GiB/GPU) to remove an OOM tail.
- **Reconcile safety** — delete cap (50/cycle) + proportional guard (refuse if orphans > 50%
  of cluster pods) + empty-active-set guard, so a DB/logic fault can't mass-delete live pods.

---

## 7. Verified limits & residual risks

**Load test #3 (replicas=2), safe control-plane probe (no real GPU launches):**
- 200 concurrent in-flight → ~987 req/s @ p99≈1.2s, 0 errors.
- 400 concurrent in-flight → ~871 req/s @ p99≈3s, 0 errors; manager CPU ~0.5 of 4 cores
  each (~87% idle); PG 12/100; single lease holder (0 flap); notebooks 0 restart.
- Traffic split near-evenly across both pods (Service load-balancing confirmed).

**Conclusion:** the plateau is coordinated omission + per-request uncached-Postgres-read
latency in the closed-loop client, **not** server saturation. True ceiling is above the
tested load.

**Residual risks / untested:**
1. **Launch burst** — `create_instance` write path not independently load-tested; a 500-
   simultaneous-launch spike stays latency-bound (8-thread pool per process).
2. **Long-lived streams** — with 2 pods behind a `Cluster`-policy NodePort and no session
   affinity, any in-memory manager↔notebook websocket/terminal session state (if present)
   could break on wrong-pod routing. Exercise streaming before relying on it.
3. **Redis connection ceiling** — not characterized under load.
4. **Accurate ceiling measurement** — a single closed-loop client caps ~500–600 req/s
   regardless of servers; use ≥2 parallel clients or an open-loop generator for a real number.

---

## 8. Scaling further (levers, in order)

1. **replicas 2→3–4** — anti-affinity + nodeAffinity allowlist already in place; add nodes to
   the allowlist and `kubectl scale`. **Raise Postgres `max_connections` 100→200 before
   exceeding ~6 total processes** (see §4).
2. **workers per pod** — bump CMD `--workers` and cpu limit proportionally.
3. **Postgres** — HA / read replicas only if the DB ever becomes the bottleneck (it is far
   from it today).
4. **Launch throughput** — widen `_launch_executor` and/or make provisioning fully async if
   launch-burst becomes a real requirement.

---

## 9. Rollback

| Change | Rollback |
|---|---|
| replicas=2 rollout | `kubectl apply -f local-deploy-history/radeon-global/*-replicas2-PRE.yaml` then `kubectl scale --replicas=1` (restores nodeName pin + Recreate). |
| workers=2 image | `kubectl set image ... manager=...:fence1` + resources cpu=2/mem=1Gi. |
| leader fencing | `kubectl set image ... manager=...:ecfbd23`. |
| Postgres cutover | remove the injected `AMD_ONECLICK_POSTGRES_SERVICE_HOST` env → store.py falls back to the untouched read-only SQLite. |

Datastore is the source of truth throughout; the old SQLite hostPath (`/data`, read-only,
unused) remains as a cold fallback.
