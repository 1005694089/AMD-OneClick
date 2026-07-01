"""Manager-side P4 purge handlers that must run IN-CLUSTER.

Why in the manager and not the 0042 agent:
  - The real Dragonfly v1.4.0 delete primitive is `dfctl task rm <task_id>` (dfget has NO delete;
    deletion is by 64-hex TASK ID, not URL). dfctl talks only to its LOCAL daemon socket, so it must
    run where each daemon is.
  - task_id == blob-digest-hex in this deployment (enableTaskIDBasedBlobDigest), so an image's tasks
    are exactly its OCI config+layer digests (captured at push time -> job payload `blob_ids`).
  - The per-node client dfdaemons are hostNetwork (reachable), but the SEED pods are overlay-only
    (10.232.x) and unreachable from the 0042 host. The manager pod is on the pod network and has the
    k8s API (exec) — so it drives both by `kubectl exec ... dfctl task rm` into the pods.

Idempotent + fail-loud: a "task not found" is success (already gone); any other non-zero dfctl exit
fails the job so requeue_failed_purges retries and the purge_meta gate stays closed until bytes are
truly gone.
"""
from __future__ import annotations

import logging
from typing import Optional

from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

from .config import settings

logger = logging.getLogger(__name__)

# dfctl prints this (case-insensitive substring) when the task is already absent -> treat as success.
_NOT_FOUND_MARKERS = ("task not found", "no such task", "task does not exist", "not found")


def _blob_ids_from_payload(payload: dict) -> list[str]:
    """The task-ids to delete for this ref (bare hex). Empty => nothing addressable on this surface."""
    ids = (payload or {}).get("blob_ids") or []
    out = []
    seen = set()
    for b in ids:
        if not b:
            continue
        s = str(b).strip()
        if ":" in s:
            s = s.split(":", 1)[1]
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _exec_dfctl_rm(core_v1, pod: str, container: str, task_id: str) -> tuple[bool, str]:
    """Run `dfctl task rm <task_id>` in one pod/container. Returns (ok, detail).

    ok=True on real deletion OR an already-gone task. ok=False on any other error (RBAC, exec
    transport, dfctl non-zero for a reason other than not-found)."""
    cmd = [settings.DRAGONFLY_DFCTL, "task", "rm", task_id]
    try:
        resp = stream(
            core_v1.connect_get_namespaced_pod_exec,
            pod,
            settings.DRAGONFLY_NAMESPACE,
            container=container,
            command=cmd,
            stderr=True,
            stdout=True,
            stdin=False,
            tty=False,
            _preload_content=True,
        )
    except ApiException as e:
        return False, f"exec_api_error:{e.status}"
    except Exception as e:  # transport / websocket errors
        return False, f"exec_error:{type(e).__name__}"
    text = (resp or "")
    low = text.lower()
    if any(m in low for m in _NOT_FOUND_MARKERS):
        return True, "already_gone"
    # dfctl exits 0 and prints nothing (or a success line) on a real delete. The stream API does not
    # surface the exit code with _preload_content=True, so we treat "no error marker" as success and
    # rely on an explicit error substring to fail. Known dfctl error phrases:
    for marker in ("error", "failed", "cannot", "refused", "no route", "connection"):
        if marker in low:
            return False, f"dfctl_error:{text.strip()[:200]}"
    return True, "deleted"


def _list_pods(core_v1, selector: str, field_selector: Optional[str] = None):
    return core_v1.list_namespaced_pod(
        settings.DRAGONFLY_NAMESPACE, label_selector=selector, field_selector=field_selector
    ).items


def run_purge_p2p_manager(core_v1, job: dict) -> dict:
    """Delete this image's blob tasks from each target node's client dfdaemon P2P cache.

    Targets come from the job payload (the nodes that warmed the ref). For each node we find its
    client dfdaemon pod (DaemonSet, one per node, matched by spec.nodeName) and `dfctl task rm` every
    blob id. Returns a result dict {ref, nodes:[{node, removed, error}]} mirroring the old agent
    handler so the lifecycle/reaper logic is unchanged."""
    ref = job.get("ref")
    payload = job.get("payload") or {}
    targets = payload.get("targets") or []
    blob_ids = _blob_ids_from_payload(payload)
    nodes_out = []

    if not blob_ids:
        # Nothing addressable (pre-P4 image w/o captured blobs). Report success so purge_meta isn't
        # blocked forever; the P2P cache is left to Dragonfly taskTTL. Logged for visibility.
        logger.warning("purge_p2p %s: no blob_ids in payload; skipping P2P cache purge (taskTTL will reap)", ref)
        return {"ref": ref, "nodes": [], "skipped": "no_blob_ids"}

    for target in targets:
        node = target.get("node") if isinstance(target, dict) else target
        entry = {"node": node, "removed": False, "error": None}
        if not node:
            entry["error"] = "missing_node"
            nodes_out.append(entry)
            continue
        pods = _list_pods(core_v1, settings.DRAGONFLY_CLIENT_SELECTOR, f"spec.nodeName={node}")
        if not pods:
            # No dfdaemon on this node (never labeled / drained). Nothing to purge there -> success:
            # if there's no daemon, there's no P2P cache for this ref on that node.
            entry["removed"] = True
            entry["error"] = None
            nodes_out.append(entry)
            continue
        pod = pods[0].metadata.name
        ok_all = True
        detail = None
        for tid in blob_ids:
            ok, d = _exec_dfctl_rm(core_v1, pod, settings.DRAGONFLY_CLIENT_CONTAINER, tid)
            if not ok:
                ok_all = False
                detail = d
                break
        entry["removed"] = ok_all
        if not ok_all:
            entry["error"] = detail or "purge_p2p_failed"
        nodes_out.append(entry)

    return {"ref": ref, "nodes": nodes_out}


def run_purge_seed_manager(core_v1, job: dict) -> dict:
    """Delete this image's blob tasks from every seed dfdaemon cache.

    Seeds are a StatefulSet (all 3 tried); a task absent on a seed is success. Returns
    {ref, seeds:[{pod, removed, error}], seed_deleted: bool}."""
    ref = job.get("ref")
    payload = job.get("payload") or {}
    blob_ids = _blob_ids_from_payload(payload)

    if not blob_ids:
        logger.warning("purge_seed %s: no blob_ids in payload; skipping seed cache purge (taskTTL will reap)", ref)
        return {"ref": ref, "seeds": [], "seed_deleted": False, "skipped": "no_blob_ids"}

    seeds = _list_pods(core_v1, settings.DRAGONFLY_SEED_SELECTOR)
    seeds_out = []
    all_ok = True
    for sp in seeds:
        pod = sp.metadata.name
        entry = {"pod": pod, "removed": False, "error": None}
        ok_all = True
        detail = None
        for tid in blob_ids:
            ok, d = _exec_dfctl_rm(core_v1, pod, settings.DRAGONFLY_SEED_CONTAINER, tid)
            if not ok:
                ok_all = False
                detail = d
                break
        entry["removed"] = ok_all
        if not ok_all:
            entry["error"] = detail or "purge_seed_failed"
            all_ok = False
        seeds_out.append(entry)

    return {"ref": ref, "seeds": seeds_out, "seed_deleted": all_ok}
