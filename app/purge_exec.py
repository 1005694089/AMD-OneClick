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
import re
from typing import Optional

from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

from .config import settings

logger = logging.getLogger(__name__)

# dfctl v1.4.0 prints (verified live against a real seed) on an already-absent task, rc=1:
#   Removing Task Failed! / Bad Code: Internal error / Message: task <64-hex> not found
# So the real phrase is "task <ID> not found", NOT the literal "task not found". Match it with a
# regex that requires the word "task" followed (allowing the id) by "not found" — deliberately
# TASK-SPECIFIC so a MISSING-BINARY shell error ("sh: dfctl: not found") is NOT accepted as success
# (that must fail so the purge retries instead of dropping the DB handle while bytes remain).
_NOT_FOUND_RE = re.compile(r"task\b.*\bnot found", re.IGNORECASE | re.DOTALL)
# Other daemon phrasings that also mean already-gone (kept as plain substrings).
_NOT_FOUND_MARKERS = ("no such task", "task does not exist", "content not found")


def _is_already_gone(text: str) -> bool:
    """True iff dfctl output indicates the task was already absent (a success for an idempotent
    delete). Requires a TASK-scoped not-found phrase; a bare 'not found' (e.g. missing binary) is
    intentionally NOT matched."""
    low = (text or "").lower()
    if _NOT_FOUND_RE.search(low):
        return True
    return any(m in low for m in _NOT_FOUND_MARKERS)

# Unique sentinels so we can recover the real exit code from the merged stdout+stderr stream
# (_preload_content=True does NOT surface the exec exit status; the shell wrapper carries it).
_RC_PREFIX = "__DFCTL_RC__:"


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

    The EXIT CODE is the source of truth (fail-loud for a delete): the stream API with
    _preload_content=True returns only merged stdout+stderr and drops the exec status, so we run the
    command through a shell that echoes a sentinel line `__DFCTL_RC__:<rc>` and parse it back.

      ok=True  iff rc==0 (deleted) OR rc!=0 with a TASK-SPECIFIC not-found phrase (already gone).
      ok=False on: any other non-zero rc (incl. 127 missing binary / 126 not-exec), a MISSING
                   sentinel (exec never ran the wrapper: OCI-runtime failure, timeout, truncated
                   stream), or a k8s transport error.

    This closes the false-success holes the review found (missing/misnamed dfctl, localized/gRPC
    error text, exec timeout) — none of which can now masquerade as a successful delete."""
    # Shell wrapper: run dfctl, capture rc, always print the sentinel LAST so a truncated/empty stream
    # is detectable (no sentinel => failure). shlex-free: task_id is a validated 64-hex id, but quote
    # defensively anyway.
    inner = f"{settings.DRAGONFLY_DFCTL} task rm {task_id}; rc=$?; echo {_RC_PREFIX}$rc"
    cmd = ["sh", "-c", inner]
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
    except Exception as e:  # transport / websocket errors / timeout raised by the client
        return False, f"exec_error:{type(e).__name__}"

    text = resp or ""
    # Recover rc from the LAST sentinel line. Absent sentinel => the wrapper never completed (OCI exec
    # failure like `exec: "dfctl": ... not found`, a mid-run timeout, or a truncated stream) => FAIL.
    rc = None
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith(_RC_PREFIX):
            tail = line[len(_RC_PREFIX):].strip()
            if tail.isdigit():
                rc = int(tail)
            break
    if rc is None:
        return False, f"exec_no_rc:{text.strip()[:200]}"
    if rc == 0:
        return True, "deleted"
    # Non-zero: success ONLY for a task-specific already-gone phrase; everything else fails loud.
    # (dfctl v1.4.0 exits 1 with "... task <id> not found" when the task is already gone.)
    if _is_already_gone(text):
        return True, "already_gone"
    return False, f"dfctl_rc_{rc}:{text.strip()[:200]}"


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

    # Distinguish "node genuinely has no dfdaemon" (success — no cache to purge) from "selector drift /
    # transient absence" (must FAIL, else we'd open purge_meta while bytes remain). If the client
    # selector matches at least one pod cluster-wide, the selector is valid, so an empty per-node
    # result means that node truly has no daemon. If it matches ZERO pods anywhere, the selector is
    # wrong/all daemons are down -> fail every node so requeue_failed_purges retries.
    if not targets:
        # No nodes recorded as holding this ref => nothing warmed it => no per-node P2P cache to
        # purge. Success (seeds are purged by purge_seed separately). Logged since a lost targets[]
        # would look identical — the seed purge + registry_delete still cover the durable copies.
        logger.info("purge_p2p %s: empty targets; no per-node P2P cache to purge", ref)
        return {"ref": ref, "nodes": []}

    selector_has_any = bool(_list_pods(core_v1, settings.DRAGONFLY_CLIENT_SELECTOR))

    for target in targets:
        node = target.get("node") if isinstance(target, dict) else target
        entry = {"node": node, "removed": False, "error": None}
        if not node:
            entry["error"] = "missing_node"
            nodes_out.append(entry)
            continue
        pods = _list_pods(core_v1, settings.DRAGONFLY_CLIENT_SELECTOR, f"spec.nodeName={node}")
        if not pods:
            if selector_has_any:
                # Selector is valid but no daemon on THIS node (never labeled / drained). The P2P
                # cache lives with the daemon, so no daemon => nothing to purge here => success.
                entry["removed"] = True
            else:
                # Zero client daemons match the selector anywhere: selector drift or a fleet-wide
                # daemon outage. Do NOT claim success — fail so this retries.
                entry["error"] = "no_client_daemons_match_selector"
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
    if not seeds:
        # Seeds are a fixed StatefulSet (>=1 replica). An empty list is a lookup failure (selector
        # drift, all seeds mid-rollout, or an API race), NOT proof the cache is empty. Fail-loud so
        # requeue_failed_purges retries — never open purge_meta on a vacuous success.
        logger.warning("purge_seed %s: zero seed pods matched %s; failing (will retry)", ref, settings.DRAGONFLY_SEED_SELECTOR)
        return {"ref": ref, "seeds": [], "seed_deleted": False, "error": "no_seed_pods"}

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
