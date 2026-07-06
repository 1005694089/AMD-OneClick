"""
Background scheduler for cleanup tasks
"""
import asyncio
import logging
import math
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.executors.pool import ThreadPoolExecutor as APThreadPoolExecutor

from .config import settings

logger = logging.getLogger(__name__)

# Run scheduler jobs on a DEDICATED thread pool, not the asyncio event loop's default executor.
# The jobs here are sync `def`s (billing, reconcile, image_sync_refresh_job → reconcile_preheat_ds
# which can block ~100s force-deleting a stuck preheat pod). Left on the default AsyncIOExecutor
# they would share the loop's default ThreadPoolExecutor with every `await asyncio.to_thread(...)`
# in main.py, so one slow reconcile could starve unrelated request-path to_thread work. A dedicated
# pool isolates them (max_instances=1 per job still prevents overlap of the same job).
scheduler = AsyncIOScheduler(executors={"default": APThreadPoolExecutor(max_workers=8)})

# Real image-service source_type keys (mirror rows use 'harbor_mirror', deliberately excluded).
# Kept in sync with main._ADMIN_SOURCE_HEAD_KIND; duplicated here to avoid an import cycle.
_IMAGE_SERVICE_SOURCE_TYPES = frozenset({"acr_pull", "dockerhub_pull", "github_build"})
# Dedicated source_type stamped on Harbor-mirror rows (kept in sync with main.HARBOR_MIRROR_SOURCE_TYPE).
_HARBOR_MIRROR_SOURCE_TYPE = "harbor_mirror"


def _mirror_row_is_stale(image: dict) -> bool:
    """True if a 'distributing' mirror row hasn't been touched in longer than a full mirror timeout
    plus buffer — i.e. its in-process background task almost certainly died (manager restart)."""
    ts = (image.get("updated_at") or image.get("sync_started_at") or "").strip()
    if not ts:
        return False
    try:
        started = datetime.fromisoformat(ts)
    except ValueError:
        return False
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    budget = int(getattr(settings, "HARBOR_MIRROR_TIMEOUT_SECONDS", 3600)) + 300
    return (datetime.now(timezone.utc) - started).total_seconds() > budget

# The asyncio loop the app runs on, captured at scheduler start. The heavy jobs
# below are SYNC functions so APScheduler's AsyncIOExecutor runs them in a
# thread pool (off the event loop); any async telemetry they emit is dispatched
# back onto this loop thread-safely so a slow job never blocks HTTP/OAuth.
_main_loop: "asyncio.AbstractEventLoop | None" = None

# Node-wedge detection: consecutive reconcile cycles each node has looked like a wedge suspect.
# A node is only quarantined once its streak reaches NODE_WEDGE_CONSECUTIVE_TICKS, so a transient
# one-cycle burst of stuck pods does not trigger a quarantine. Reset to 0 the moment a node stops
# looking wedged. Module-level (single-replica manager) — persists across reconcile ticks.
_node_wedge_streak: dict[str, int] = {}


def _detect_and_quarantine_wedged_nodes(pod_states: list, stuck_threshold: int) -> list:
    """Aggregate per-pod stuck signals by node; DB-quarantine nodes that stay wedged across ticks.

    A silently-wedged node reports Ready but its containerd can neither destroy pods (they pile up
    Terminating past stuck_threshold) nor create them (stuck ContainerCreating past the configured
    window). When a single node carries >= NODE_WEDGE_MIN_STUCK_PODS such pods for
    NODE_WEDGE_CONSECUTIVE_TICKS consecutive cycles, quarantine it (DB-only, via store.quarantine_node)
    so new placements route around it. No kubectl cordon — the manager SA has no node-patch RBAC.
    Returns the list of node names quarantined this cycle (for logging)."""
    if not settings.NODE_WEDGE_DETECT_ENABLED:
        return []
    from collections import defaultdict
    from .store import quarantine_node

    stuck_by_node: dict = defaultdict(int)
    for p in pod_states:
        node = p.get("node_name")
        if not node:
            continue
        term_stuck = p.get("terminating") and (p.get("terminating_seconds") or 0) >= stuck_threshold
        creating_stuck = (
            not p.get("terminating")
            and p.get("waiting_reason") == "ContainerCreating"
            and (p.get("age_seconds") or 0) >= settings.NODE_WEDGE_CREATING_SECONDS
        )
        if term_stuck or creating_stuck:
            stuck_by_node[node] += 1

    suspects = {n for n, c in stuck_by_node.items() if c >= settings.NODE_WEDGE_MIN_STUCK_PODS}
    # Decay streaks for nodes that recovered this cycle.
    for node in list(_node_wedge_streak.keys()):
        if node not in suspects:
            del _node_wedge_streak[node]

    quarantined_now = []
    for node in suspects:
        _node_wedge_streak[node] = _node_wedge_streak.get(node, 0) + 1
        streak = _node_wedge_streak[node]
        if streak >= settings.NODE_WEDGE_CONSECUTIVE_TICKS:
            try:
                quarantine_node(node, "", settings.NODE_QUARANTINE_SECONDS)
                quarantined_now.append(node)
                logger.warning(
                    "Reconcile: node %s looks WEDGED (%s stuck pods, streak=%s) — DB-quarantined for %ss "
                    "(new placements will avoid it; no kubectl cordon)",
                    node, stuck_by_node[node], streak, settings.NODE_QUARANTINE_SECONDS,
                )
            except Exception as e:
                logger.error("Reconcile: failed to quarantine wedged node %s: %s", node, e)
        else:
            logger.info(
                "Reconcile: node %s wedge-suspect (%s stuck pods, streak=%s/%s) — not quarantining yet",
                node, stuck_by_node[node], streak, settings.NODE_WEDGE_CONSECUTIVE_TICKS,
            )
    return quarantined_now


def _skip_not_leader(job_name: str) -> bool:
    """True if this replica is not the leader and should skip the job.

    With leader election disabled (default / single replica), is_leader() is always True so nothing
    is skipped. With it enabled and this replica not holding the Lease, the job is a no-op here and
    runs on the leader instead — preventing double-billing / duplicate reconciliation across
    replicas.
    """
    from .leader import is_leader

    if is_leader():
        return False
    logger.debug("Skipping %s on non-leader replica", job_name)
    return True


def _fire_and_forget(coro):
    """Schedule a coroutine (telemetry) on the main loop from a worker thread."""
    loop = _main_loop
    if loop is None:
        coro.close()
        return
    try:
        asyncio.run_coroutine_threadsafe(coro, loop)
    except Exception as e:
        logger.warning("telemetry dispatch failed: %s", e)
        try:
            coro.close()
        except Exception:
            pass


def cleanup_job():
    """Periodic job to cleanup idle and expired instances.

    SYNC on purpose: it performs many blocking k8s API + TCP readiness calls
    (one set per active instance). Running it as a coroutine on the event loop
    starved async HTTP (intermittent OAuth failures) and misfired other jobs at
    production scale; as a sync job it runs in a worker thread instead.
    """
    if _skip_not_leader("cleanup_job"):
        return
    from .k8s_client import k8s_client
    from .store import charge_usage_unit, list_active_instances, mark_instance_deleted, mark_instance_ready_for_billing, update_instance_charge_time

    logger.info("Running cleanup job...")
    try:
        cleaned = []
        now = datetime.now(timezone.utc)
        # Batch: one LIST of all managed pods instead of a per-instance GET just to
        # discover which pods are gone/terminating. At 500 instances this replaces up to
        # 500 blocking read_namespaced_pod calls per 60s cycle with a single list call.
        # The expensive, TCP-readiness-based get_pod_status_details is still called per
        # instance for pods that ARE present and not terminating, so the billing "ready"
        # gate is byte-for-byte unchanged; the LIST only lets us cheaply short-circuit the
        # terminating case (skip billing without a GET). Absence in the snapshot still falls
        # through to the authoritative GET, so a just-launched pod is never wrongly deleted.
        try:
            pod_index = {}
            for ps in k8s_client.list_managed_pod_states():
                iid = ps["instance_id"]
                prev = pod_index.get(iid)
                # On duplicate instance_id (pod-recreate race), prefer the live pod over a
                # terminating one so a live replacement is never shadowed by its dying predecessor.
                if prev is None or (prev.get("terminating") and not ps.get("terminating")):
                    pod_index[iid] = ps
        except Exception as e:
            # If the LIST fails, fall back to the original per-instance path (index empty
            # means every instance takes the GET branch) rather than mis-billing.
            logger.warning("cleanup_job: list_managed_pod_states failed (%s); per-instance fallback", e)
            pod_index = None
        from .leader import is_leader
        for record in list_active_instances():
            try:
                if not is_leader():
                    logger.warning("cleanup_job: lost leadership mid-cycle; stopping billing sweep")
                    break
                if pod_index is not None:
                    ps = pod_index.get(record["instance_id"])
                    # Only the terminating fast-path skips the authoritative GET (reconcile_job
                    # finalizes terminating pods and billing is skipped for them regardless).
                    # Absence in the single pre-loop snapshot is NOT proof of deletion — a pod
                    # launched during/after the LIST would be missing yet live — so we fall
                    # through to the fresh per-instance GET, which alone can mark_instance_deleted.
                    if ps is not None and ps.get("terminating"):
                        continue
                status_details = k8s_client.get_pod_status_details(record["email"], instance_id=record["instance_id"])
                if status_details is None:
                    mark_instance_deleted(record["instance_id"])
                    continue
                live_status = status_details.get("status")
                if live_status != "ready":
                    logger.info(
                        "Skipping billing for %s while pod status is %s reason=%s message=%s",
                        record["instance_id"],
                        live_status,
                        status_details.get("reason"),
                        status_details.get("message"),
                    )
                    continue
                if record.get("status") != "running":
                    record = mark_instance_ready_for_billing(record["instance_id"]) or record
                billing_started_at = record.get("billing_started_at") or record.get("created_at")
                started = datetime.fromisoformat(billing_started_at)
                elapsed_seconds = int((now - started).total_seconds())
                if elapsed_seconds < 60:
                    continue
                billable_units = max(1, math.ceil(elapsed_seconds / 3600))
                for unit in range(1, billable_units + 1):
                    billing_session_id = record.get("billing_session_id") or record["instance_id"]
                    result = charge_usage_unit(
                        record["user_id"],
                        record["instance_id"],
                        billing_session_id,
                        unit,
                        int(record["gpu_count"]),
                    )
                    if result == "charged":
                        from .telemetry import report_gpu_hour_charged_event

                        _fire_and_forget(report_gpu_hour_charged_event(
                            instance_id=record["instance_id"],
                            billing_session_id=billing_session_id,
                            billing_unit=unit,
                            user_id=record["user_id"],
                            gpu_count=int(record["gpu_count"]),
                        ))
                    if result == "insufficient":
                        if k8s_client.delete_instance_by_id(record["instance_id"]):
                            mark_instance_deleted(record["instance_id"])
                            cleaned.append({
                                "email": record["email"],
                                "reason": f"credits exhausted while charging billing unit {unit}",
                            })
                        break
                update_instance_charge_time(record["id"], now.isoformat())
            except Exception as e:
                logger.error(f"Credit billing failed for {record.get('instance_id')}: {e}")
        if cleaned:
            logger.info(f"Cleaned up {len(cleaned)} instances: {cleaned}")
        else:
            logger.info("No instances to clean up")
    except Exception as e:
        logger.error(f"Cleanup job failed: {e}")


async def template_preview_sync_job():
    """Periodic job to refresh notebook template preview caches."""
    if _skip_not_leader("template_preview_sync_job"):
        return
    from .template_sync import sync_due_template_previews

    logger.info("Running template preview sync job...")
    try:
        results = await sync_due_template_previews(limit=5)
        if results:
            logger.info("Synced %s template previews", len(results))
        else:
            logger.info("No template previews need syncing")
    except Exception as e:
        logger.error(f"Template preview sync job failed: {e}")


async def reap_stale_builds_job():
    """Fail custom image builds whose agent lease has expired (agent died/stalled)."""
    if _skip_not_leader("reap_stale_builds_job"):
        return
    from .store import reap_stale_builds

    try:
        reaped = reap_stale_builds(settings.CUSTOM_IMAGE_BUILD_LEASE_TIMEOUT_SECONDS)
        if reaped:
            logger.warning("Reaped %s stale custom image build(s)", reaped)
    except Exception as e:
        logger.error(f"Stale build reaper failed: {e}")


async def reap_stale_image_jobs_job():
    """Requeue (or fail) image_jobs whose agent lease has expired (daemon died/stalled).

    Harmless when RUN_SCHEDULER is off: the image-service daemon also reaps stale jobs.
    """
    if _skip_not_leader("reap_stale_image_jobs_job"):
        return
    from .store import reap_stale_image_jobs, requeue_failed_purges

    try:
        reaped = reap_stale_image_jobs(settings.JOB_LEASE_TIMEOUT_SECONDS)
        if reaped:
            logger.warning("Reaped %s stale image job(s)", reaped)
    except Exception as e:
        logger.error(f"Stale image job reaper failed: {e}")

    # P4 delete-completeness: the stale reaper above only requeues LEASED jobs. A purge-surface job
    # that REPORTED failure is terminal and would otherwise never retry, leaving orphaned bytes. Flip
    # such failed purges back to pending (bounded by max_attempts) so a delete converges to complete.
    try:
        requeued = requeue_failed_purges()
        if requeued:
            logger.warning("Re-enqueued %s failed purge job(s) for delete-completeness", requeued)
    except Exception as e:
        logger.error(f"Failed-purge requeue failed: {e}")


MANAGER_PURGE_AGENT_ID = "manager-purge"
# Kinds the manager itself drains in-cluster (the 0042 agent cannot reach overlay seeds and lacks the
# k8s API). purge_p2p/purge_seed exec `dfctl task rm`; purge_meta drops DB handles.
MANAGER_PURGE_KINDS = ("purge_p2p", "purge_seed", "purge_meta")


def _drain_manager_purges_sync() -> int:
    """Claim + execute manager-side purge jobs until none remain (bounded). Returns count processed.

    Runs the SAME path the agent result-report endpoint runs: finish_image_job(...) +
    _sync_image_job_lifecycle(...). purge_p2p/purge_seed call the in-cluster dfctl-exec handlers;
    purge_meta has no execution body of its own (the lifecycle branch drops the rows) so it finishes
    'succeeded' and the lifecycle does the DB cleanup + prereq re-check.

    Blocking (k8s exec + DB) — the caller runs it in a thread so it can't stall the event loop.
    """
    import json as _json

    from . import main as main_module
    from . import purge_exec
    from .k8s_client import k8s_client
    from .store import claim_next_image_job, finish_image_job

    processed = 0
    for _ in range(max(1, settings.PURGE_DRAIN_BATCH)):
        job = claim_next_image_job(MANAGER_PURGE_AGENT_ID, kinds=list(MANAGER_PURGE_KINDS))
        if not job:
            break
        kind = job.get("kind")
        # Normalize payload to a dict (claim returns the raw row; payload is a JSON string).
        payload = job.get("payload")
        if isinstance(payload, str):
            try:
                payload = _json.loads(payload) if payload else {}
            except (ValueError, TypeError):
                payload = {}
        job["payload"] = payload or {}

        status = "succeeded"
        result = {"ref": job.get("ref")}
        try:
            if kind == "purge_p2p":
                result = purge_exec.run_purge_p2p_manager(k8s_client.core_v1, job)
                if any(n.get("error") for n in result.get("nodes", [])):
                    status = "failed"
            elif kind == "purge_seed":
                result = purge_exec.run_purge_seed_manager(k8s_client.core_v1, job)
                if not result.get("seed_deleted") and not result.get("skipped"):
                    status = "failed"
            elif kind == "purge_meta":
                # No execution body — the lifecycle branch (re-checks prereqs) drops the DB rows.
                result = {"ref": job.get("ref")}
            else:
                continue
        except Exception as e:
            logger.error("Manager purge %s (job %s) raised: %s", kind, job.get("id"), e)
            status = "failed"
            result = {"ref": job.get("ref"), "error": f"manager_purge_exception:{type(e).__name__}"}

        finished = finish_image_job(job["id"], status, agent_id=MANAGER_PURGE_AGENT_ID, result=_json.dumps(result))
        if finished and status == "succeeded":
            try:
                main_module._sync_image_job_lifecycle(finished, result)
            except Exception as e:
                logger.error("Manager purge lifecycle sync failed for job %s (%s): %s", job.get("id"), kind, e)
        processed += 1
    return processed


async def drain_manager_purges_job():
    """Scheduler tick: drain manager-side purge jobs (leader-only). Inert unless the fan-out is on."""
    if not settings.PURGE_FANOUT_ENABLED:
        return
    if _skip_not_leader("drain_manager_purges_job"):
        return
    try:
        n = await asyncio.to_thread(_drain_manager_purges_sync)
        if n:
            logger.info("Manager drained %s purge job(s)", n)
    except Exception as e:
        logger.error("Manager purge drain failed: %s", e)


async def idle_reaper_job():
    """Auto-destroy idle/expired instances (API-launched pods after 8h idle)."""
    if _skip_not_leader("idle_reaper_job"):
        return
    from .k8s_client import k8s_client

    try:
        cleaned = k8s_client.cleanup_idle_instances()
        if cleaned:
            logger.info("Idle reaper destroyed %s instance(s): %s", len(cleaned), cleaned)
    except Exception as e:
        logger.error(f"Idle reaper job failed: {e}")

async def workspace_local_cache_reaper_job():
    """Delayed-local-delete: free node-local SSD workspace copies whose instance stopped longer than
    the TTL (durable NFS copy is kept forever). Leader-only; inert unless localcache is enabled."""
    if (settings.WORKSPACE_VOLUME_TYPE or "").strip().lower() != "localcache":
        return
    if _skip_not_leader("workspace_local_cache_reaper_job"):
        return
    from .k8s_client import k8s_client

    try:
        reaped = await asyncio.to_thread(k8s_client.reap_local_workspace_caches)
        if reaped:
            logger.info("Workspace local-cache reaper freed %s cache(s)", len(reaped))
    except Exception as e:
        logger.error("Workspace local-cache reaper failed: %s", e)


async def workspace_durable_trash_purge_job():
    """Nightly purge of <shard>/.trash entries older than the retention window (admin soft-deletes).
    Leader-only; inert unless localcache is enabled."""
    if (settings.WORKSPACE_VOLUME_TYPE or "").strip().lower() != "localcache":
        return
    if _skip_not_leader("workspace_durable_trash_purge_job"):
        return
    from .k8s_client import k8s_client

    try:
        n = await asyncio.to_thread(k8s_client.purge_durable_trash)
        if n:
            logger.info("Workspace durable trash purge processed %s shard(s)", n)
    except Exception as e:
        logger.error("Workspace durable trash purge failed: %s", e)


async def workspace_flush_retry_job():
    """Retry the out-of-pod local->durable flush for stopped instances whose flush never confirmed
    (node was NotReady at delete time, or the pod was lost out-of-band). Fallback for the single-node
    flush pin. Leader-only; inert unless localcache is enabled. DATA-SAFETY: without this an SSD copy
    stranded by a NotReady node would stay unflushed forever (the reaper correctly refuses to reap
    it), so the delta would never reach durable and the SSD would leak."""
    if (settings.WORKSPACE_VOLUME_TYPE or "").strip().lower() != "localcache":
        return
    if _skip_not_leader("workspace_flush_retry_job"):
        return
    from .k8s_client import k8s_client

    try:
        flushed = await asyncio.to_thread(k8s_client.retry_unflushed_workspaces)
        if flushed:
            logger.info("Workspace flush-retry sweep flushed %s workspace(s)", len(flushed))
    except Exception as e:
        logger.error("Workspace flush-retry sweep failed: %s", e)


async def workspace_durable_dirquota_reconcile_job():
    """Ensure every live instance's durable subdir has a per-user SFS dir-quota (best-effort backstop).

    SOLE applier of the quota (there is no inline create-hook: at create-time the durable subdir does
    not exist yet, so the SFS call would fail 100% of the time). Leader-only; inert unless localcache
    AND the dir-quota flag are on. Marker-gated + batch-capped so steady-state ticks are cheap no-ops
    and SFS API fan-out is bounded. Never raises; a per-instance failure just retries next tick."""
    if (settings.WORKSPACE_VOLUME_TYPE or "").strip().lower() != "localcache":
        return
    if not settings.WORKSPACE_DURABLE_DIRQUOTA_ENABLED:
        return
    if _skip_not_leader("workspace_durable_dirquota_reconcile_job"):
        return

    def _sweep() -> tuple:
        from . import workspace_dirquota
        from .store import list_active_instance_ids

        applied = 0
        attempted = 0
        active = list_active_instance_ids()
        # Keep the in-memory applied-marker set bounded by the live fleet: the delete-hook's
        # mark_unapplied usually runs on a non-leader process, so the leader's set would otherwise
        # accumulate ids of deleted instances across the freeze. Prune each sweep against live ids.
        workspace_dirquota.prune_applied(active)
        # Budget bounds ATTEMPTS, not successes: during an SFS incident (or an unbound shard) every
        # apply fails and is not marked, so a success-only cap would let the loop fan out over the
        # whole fleet (up to 2 SFS calls each) every tick. Charging attempts keeps per-tick SFS load
        # bounded regardless of failure rate; unreached instances just get their turn on a later tick.
        budget = max(1, int(settings.WORKSPACE_DURABLE_DIRQUOTA_BATCH_PER_TICK))
        for iid in sorted(active):
            if attempted >= budget:
                break
            if workspace_dirquota.is_applied(iid):
                continue  # already set this lifetime — don't spend the budget or an SFS call
            attempted += 1
            try:
                if workspace_dirquota.ensure_dir_quota_for_instance(iid):
                    applied += 1
            except Exception as e:
                logger.debug("dirquota reconcile for %s failed (non-fatal): %s", iid, e)
        return applied, attempted

    try:
        applied, attempted = await asyncio.to_thread(_sweep)
        if attempted:
            logger.info("Durable dir-quota reconcile: %s/%s instance(s) applied this tick",
                        applied, attempted)
    except Exception as e:
        logger.error("Durable dir-quota reconcile failed: %s", e)


def reconcile_job():
    """Bidirectional reconciliation between the cluster (source of truth) and
    the DB. Reclaims orphan/rogue pods (no owning DB record), force-finalizes
    pods stuck Terminating, and marks DB instances deleted when their pod is
    gone.

    SYNC on purpose (blocking k8s calls) so it runs in a worker thread and does
    not compete with / block the event loop, mirroring cleanup_job."""
    if not settings.RECONCILE_ENABLED:
        return
    if _skip_not_leader("reconcile_job"):
        return
    from .k8s_client import k8s_client
    from .store import list_active_instance_ids, mark_instance_deleted

    logger.info("Running reconcile job...")
    try:
        # Both sources must be obtained cleanly. list_managed_pod_states raises on
        # a K8s API error (rather than returning []), and a DB error raises here,
        # so any failure aborts the whole cycle below without destructive action.
        active_ids = list_active_instance_ids()
        pod_states = k8s_client.list_managed_pod_states()
        cluster_ids = {p["instance_id"] for p in pod_states}

        # --- classify (no side effects yet) ---
        # A localcache pod runs a preStop flush (local->durable, up to 100GB over NFS) and is given a
        # long terminationGracePeriodSeconds (WORKSPACE_TERMINATION_GRACE_SECONDS) for it. Force-deleting
        # it at the global 180s TERMINATING_GRACE_SECONDS would truncate that flush and lose the delta —
        # the exact data-loss the grace-period fix prevents on the direct delete path. So a Terminating
        # localcache pod is only "stuck" once it has exceeded its own grace (plus slack). Non-localcache
        # keeps the standard 180s threshold.
        is_localcache = (settings.WORKSPACE_VOLUME_TYPE or "").strip().lower() == "localcache"
        # When localcache is on, raise the stuck threshold to cover the pod's real termination grace so
        # a legitimately-flushing pod isn't force-killed early. app/api pods have no flush, so the
        # longer wait is merely a harmless delay before they're reclaimed (pod_states carries no
        # per-pod type, so we apply one threshold to all our pods rather than mis-key on a missing field).
        stuck_threshold = settings.TERMINATING_GRACE_SECONDS
        if is_localcache:
            stuck_threshold = max(stuck_threshold, int(settings.WORKSPACE_TERMINATION_GRACE_SECONDS) + 60)
        stuck = [p for p in pod_states if p["terminating"] and (p.get("terminating_seconds") or 0) >= stuck_threshold]
        orphan_candidates = [
            p for p in pod_states
            if not p["terminating"]
            and p["instance_id"] not in active_ids
            and p["age_seconds"] >= settings.ORPHAN_GRACE_SECONDS
        ]
        # DB-active records whose pod is not in THIS manager's label-scoped list.
        # The instance_records table may be shared with other managers that use a
        # different pod label prefix (e.g. a v2-test manager on the same cluster),
        # so a record missing from our label scope may still have a live pod owned
        # by another manager. Confirm the pod is truly absent BY NAME before
        # marking it deleted, otherwise we would wipe another manager's instances.
        gone = [
            iid for iid in (active_ids - cluster_ids)
            if not k8s_client._pod_exists(iid)
        ]

        # Terminal / broken pods: dead weight the billing loop never removes
        # (they are never "ready"). CrashLoopBackOff keeps holding its GPU while
        # restarting forever. Judged purely on the pod's own reported status, so
        # this is independent of the DB and not subject to the empty-active guard.
        terminal = []
        if settings.RECLAIM_TERMINAL_ENABLED:
            terminal = [
                p for p in pod_states
                if not p["terminating"] and (
                    p["phase"] in ("failed", "succeeded")
                    or (
                        # A broken pod is often sampled mid-restart (state Error,
                        # not the CrashLoopBackOff waiting window), so trigger on
                        # EITHER the CrashLoopBackOff signal OR a high cumulative
                        # restart count, once past the grace window.
                        p["age_seconds"] >= settings.ORPHAN_GRACE_SECONDS
                        and (
                            p.get("waiting_reason") == "CrashLoopBackOff"
                            or p.get("restart_count", 0) >= settings.CRASHLOOP_RESTART_THRESHOLD
                        )
                    )
                )
            ]

        # --- safety guards on orphan reclamation (the only cluster-destructive
        # action driven by DB comparison) ---
        if orphan_candidates and not active_ids:
            # DB reports zero active instances while pods exist: ambiguous (could
            # be a DB fault). Refuse to mass-reclaim on an unverifiable signal.
            logger.warning(
                "Reconcile: %d orphan candidate(s) but active DB set is EMPTY; skipping orphan reclamation (guard)",
                len(orphan_candidates),
            )
            orphan_candidates = []
        # Proportional guard: the empty-active_ids check above only catches TOTAL DB
        # blindness. A PARTIAL/stale active_ids read (replica lag, mis-scoped query)
        # can make a large fraction of live pods look orphaned while active_ids is
        # still non-empty — invisible to that guard and, since the delete cap was
        # raised, now able to reclaim many healthy pods. If orphans exceed half of all
        # managed cluster pods, treat it as a systemic fault and refuse the whole batch.
        if orphan_candidates and cluster_ids and len(orphan_candidates) > (len(cluster_ids) // 2):
            logger.error(
                "Reconcile: %d orphan candidate(s) exceed 50%% of %d cluster pods; refusing (systemic-fault guard). ids=%s",
                len(orphan_candidates), len(cluster_ids),
                [p["instance_id"] for p in orphan_candidates],
            )
            orphan_candidates = []
        if len(orphan_candidates) > settings.RECONCILE_MAX_DELETES_PER_CYCLE:
            # Circuit breaker: too many at once smells like a logic/DB fault.
            logger.error(
                "Reconcile: %d orphan candidates exceed cap %d; refusing to delete this cycle and alerting. ids=%s",
                len(orphan_candidates), settings.RECONCILE_MAX_DELETES_PER_CYCLE,
                [p["instance_id"] for p in orphan_candidates],
            )
            orphan_candidates = []

        from .leader import is_leader
        if not is_leader():
            logger.warning("Reconcile: not leader after classification; skipping all destructive actions this cycle")
            return

        orphans_removed = 0
        stuck_forced = 0
        for p in stuck:
            if not is_leader():
                logger.warning("Reconcile: lost leadership mid stuck-pod loop; stopping (forced=%d)", stuck_forced)
                break
            logger.warning("Reconcile: force-deleting stuck-terminating pod %s (%ss)", p["instance_id"], p.get("terminating_seconds"))
            k8s_client._delete_pod(p["instance_id"], grace_period_seconds=0)
            stuck_forced += 1
        for p in orphan_candidates:
            if not is_leader():
                logger.warning("Reconcile: lost leadership mid orphan loop; stopping (removed=%d)", orphans_removed)
                break
            logger.warning(
                "Reconcile: reclaiming orphan pod %s (email=%s age=%ss, no active DB record)",
                p["instance_id"], p.get("email"), p["age_seconds"],
            )
            # wait=False issues a background delete; the pod is typically still
            # Terminating on return, so count the reclamation as issued here (the
            # next cycle confirms it is gone).
            k8s_client.delete_instance_by_id(p["instance_id"], wait=False)
            orphans_removed += 1

        # Cap terminal reclamation too (defensive), though it is status-driven
        # and far less prone to misclassification than orphan detection.
        terminal_removed = 0
        if len(terminal) > settings.RECONCILE_MAX_DELETES_PER_CYCLE:
            logger.error(
                "Reconcile: %d terminal/broken candidates exceed cap %d; reclaiming only the first %d this cycle. ids=%s",
                len(terminal), settings.RECONCILE_MAX_DELETES_PER_CYCLE, settings.RECONCILE_MAX_DELETES_PER_CYCLE,
                [p["instance_id"] for p in terminal],
            )
            terminal = terminal[:settings.RECONCILE_MAX_DELETES_PER_CYCLE]
        for p in terminal:
            if not is_leader():
                logger.warning("Reconcile: lost leadership mid terminal loop; stopping (removed=%d)", terminal_removed)
                break
            logger.warning(
                "Reconcile: reclaiming terminal/broken pod %s (phase=%s waiting=%s restarts=%s age=%ss)",
                p["instance_id"], p["phase"], p.get("waiting_reason"), p.get("restart_count"), p["age_seconds"],
            )
            k8s_client.delete_instance_by_id(p["instance_id"], wait=False)
            mark_instance_deleted(p["instance_id"])
            terminal_removed += 1

        db_marked = 0
        for instance_id in gone:
            if not is_leader():
                logger.warning("Reconcile: lost leadership mid gone loop; stopping (marked=%d)", db_marked)
                break
            mark_instance_deleted(instance_id)
            # Out-of-band pod loss (node death / manual delete / eviction) bypasses
            # delete_instance_by_id, so workspace_cache_state would stay stuck at "running" and its
            # local SSD copy would never be reaped. Stamp it stopped (keeping the recorded node) so
            # the delayed-local reaper can eventually free it. Best-effort / localcache-only.
            if (settings.WORKSPACE_VOLUME_TYPE or "").strip().lower() == "localcache":
                try:
                    from .store import stamp_workspace_stopped_keep_node
                    stamp_workspace_stopped_keep_node(instance_id)
                except Exception as e:
                    logger.debug("reconcile: stamp_workspace_stopped_keep_node failed for %s: %s", instance_id, e)
            db_marked += 1

        # Silently-wedged-node detection: aggregate stuck pods by node and quarantine persistent
        # offenders so new placements avoid them. Non-destructive (DB-only), so it runs even when
        # the orphan/terminal reclaim guards above trip.
        wedged = _detect_and_quarantine_wedged_nodes(pod_states, stuck_threshold)

        logger.info(
            "Reconcile done: orphans=%s terminal=%s stuck=%s db_marked=%s wedged_quarantined=%s (candidates orphan=%s terminal=%s stuck=%s gone=%s) cluster_pods=%s active_db=%s",
            orphans_removed, terminal_removed, stuck_forced, db_marked, len(wedged),
            len(orphan_candidates), len(terminal), len(stuck), len(gone), len(cluster_ids), len(active_ids),
        )
    except Exception as e:
        logger.error("Reconcile job failed (no destructive action taken): %s", e)


def image_sync_refresh_job():
    """Refresh prepull warm-cache status into the DB and project per-image
    warmth onto node labels for the scheduling soft-affinity. Runs on a timer
    instead of only when an admin opens the images page (which left status
    stale)."""
    if _skip_not_leader("image_sync_refresh_job"):
        return
    from .k8s_client import k8s_client
    from .store import list_images, update_image_sync_status

    logger.info("Running image sync refresh job...")
    harbor_prefix = f"{settings.HARBOR_REGISTRY.strip().rstrip('/')}/{settings.HARBOR_PROJECT.strip().strip('/')}/"
    try:
        catalog = list_images(enabled_only=False)
        for image in catalog:
            try:
                # Branch per-ROW on how the row was actually distributed, NOT on the current global
                # HARBOR_MIRROR_ENABLED flag: a row mirrored+preheated earlier must keep being
                # reconciled via its DaemonSet even if the flag is later disabled (else it would fall
                # into the legacy image_nodes path — which mirror rows never populate — and be
                # misreported as 'pulling' forever).
                # Classify by the ROW's own source_type, not the live IMAGE_SERVICE_ENABLED flag:
                # an image-service-owned row must be left to that pipeline even if the flag is now
                # off, else it would fall into the legacy status read and get misreported.
                source_type = (image.get("source_type") or "").strip()
                is_mirror_row = source_type == _HARBOR_MIRROR_SOURCE_TYPE
                is_image_service_row = source_type in _IMAGE_SERVICE_SOURCE_TYPES
                if is_mirror_row:
                    status = image.get("sync_status")
                    # A row is "already mirrored" iff its `image` is a real Harbor ref. Such a row
                    # (even one now marked 'failed' by a transient re-sync error) has a live DS that
                    # must keep tracking node-set drift — reconcile it. A row whose image is NOT yet a
                    # Harbor ref ('pending' placeholder pre-copy, or a brand-new failed mirror storing
                    # the raw src) must NOT be preheated (would pin a DS to a nonexistent image).
                    image_in_harbor = (image.get("image") or "").startswith(harbor_prefix)
                    if status == "distributing":
                        # Mirror still copying (bg task owns it). But the bg task is in-process — a
                        # manager restart mid-mirror loses it, leaving the row stuck 'distributing'.
                        # Reap a row whose status hasn't advanced in > timeout+buffer to 'failed'.
                        if _mirror_row_is_stale(image):
                            update_image_sync_status(
                                image["id"], "failed", 0, 0,
                                "Mirror interrupted (manager restarted?); re-sync to retry", False)
                        continue
                    if not image_in_harbor:
                        # Not yet mirrored (pending placeholder, or a never-succeeded failed row).
                        # No valid Harbor image to preheat; leave it to the admin bg task / re-sync.
                        continue
                    if not settings.PREHEAT_DS_ENABLED:
                        # Preheat globally disabled: the image is durably in Harbor and launchable,
                        # so a mirror row is effectively ready. Actively flip any row still stuck in a
                        # non-terminal state (its DS was/will be torn down by the orphan sweep) to
                        # 'ready' instead of leaving it frozen at a stale 'pulling N/M' forever.
                        if image.get("sync_status") != "ready":
                            update_image_sync_status(
                                image["id"], "ready", 0, 0, "Mirrored to Harbor (preheat disabled)", True)
                        continue
                    # reconcile (not just read) so a newly-joined eligible node gets preheated and a
                    # stuck/stale DS self-heals, without waiting for a manual admin re-sync.
                    sync = k8s_client.reconcile_preheat_ds(image["id"], image.get("image"))
                elif is_image_service_row:
                    # Owned by the build/distribute pipeline — leave its status to that pipeline.
                    continue
                else:
                    # Everything else (legacy 'manual' rows, or non-mirror rows). These never get an
                    # image_nodes row (no distribute job), so the legacy get_image_sync_status counts
                    # 0/N forever. When NODE_IMAGE_SCAN_ENABLED, compute readiness from each node's
                    # kubelet image inventory (node.status.images) instead — reflecting that the image
                    # is genuinely resident. Pass image["image"] so the scan has a ref to match.
                    if settings.NODE_IMAGE_SCAN_ENABLED:
                        sync = k8s_client.get_image_node_scan_status(image["id"], image.get("image"))
                    else:
                        sync = k8s_client.get_image_sync_status(image["id"], image.get("image"))
                update_image_sync_status(
                    image["id"], sync["status"], sync["desired_count"],
                    sync["ready_count"], sync["message"], sync["completed"],
                )
            except Exception as e:
                logger.warning("Image sync refresh failed for image %s: %s", image.get("id"), e)
        # Sweep preheat DaemonSets that should no longer exist. Run UNCONDITIONALLY (not gated on
        # PREHEAT_DS_ENABLED) so leftover DaemonSets are cleaned up even after preheat is disabled.
        # "Should exist" = live catalog rows whose source_type is still harbor_mirror; anything else
        # with a preheat DS is orphaned (row deleted, source_type switched away, or preheat disabled
        # → empty expected set → all preheat DSs removed). Re-fetch HERE (not the pre-loop snapshot):
        # the loop can take seconds during which an admin create may have added a mirror row + DS.
        try:
            # A mirror row SHOULD keep its preheat DS iff its `image` is a real Harbor ref (mirror
            # succeeded at some point). Discriminate on the image ref, NOT sync_status: a row that
            # mirrored successfully then hit a TRANSIENT re-sync failure keeps its good Harbor `image`
            # (sync_status='failed' but DS still valid — must NOT be swept); a brand-new failed row
            # stores the raw un-mirrored src as `image` (never Harbor-prefixed → correctly swept).
            harbor_prefix = f"{settings.HARBOR_REGISTRY.strip().rstrip('/')}/{settings.HARBOR_PROJECT.strip().strip('/')}/"
            expected_ids = {
                img["id"] for img in list_images(enabled_only=False)
                if (img.get("source_type") or "").strip() == _HARBOR_MIRROR_SOURCE_TYPE
                and (img.get("image") or "").startswith(harbor_prefix)
                and settings.PREHEAT_DS_ENABLED
            }
            orphans = k8s_client.sweep_orphan_preheat_ds(expected_ids)
            if orphans:
                logger.info("Removed %s orphan preheat DaemonSet(s)", orphans)
        except Exception as e:
            logger.warning("Orphan preheat sweep failed: %s", e)
        result = k8s_client.reconcile_image_ready_labels(
            [{"id": img["id"], "image": img["image"]} for img in catalog]
        )
        logger.info("Image sync refresh done: %s images, node labels %s", len(catalog), result)
    except Exception as e:
        logger.error("Image sync refresh job failed: %s", e)

def start_scheduler():
    """Start the background scheduler"""
    global _main_loop
    try:
        _main_loop = asyncio.get_running_loop()
    except RuntimeError:
        _main_loop = asyncio.get_event_loop()
    # coalesce + generous misfire grace so a busy moment never silently drops a
    # cycle; max_instances=1 prevents overlapping runs of the same job.
    job_defaults = dict(coalesce=True, misfire_grace_time=120, max_instances=1)
    scheduler.add_job(
        cleanup_job,
        trigger=IntervalTrigger(minutes=1),
        id="cleanup_job",
        name="Bill running instances per GPU-hour",
        replace_existing=True,
        **job_defaults,
    )
    scheduler.add_job(
        idle_reaper_job,
        trigger=IntervalTrigger(minutes=settings.IDLE_REAPER_INTERVAL_MINUTES),
        id="idle_reaper_job",
        name="Auto-destroy idle and expired instances",
        replace_existing=True,
    )
    scheduler.add_job(
        template_preview_sync_job,
        trigger=IntervalTrigger(minutes=2),
        id="template_preview_sync_job",
        name="Sync notebook template preview cache",
        replace_existing=True,
        **job_defaults,
    )
    if settings.RECONCILE_ENABLED:
        scheduler.add_job(
            reconcile_job,
            trigger=IntervalTrigger(seconds=settings.RECONCILE_INTERVAL_SECONDS),
            id="reconcile_job",
            name="Reconcile cluster instances against the database",
            replace_existing=True,
            **job_defaults,
        )
    scheduler.add_job(
        image_sync_refresh_job,
        trigger=IntervalTrigger(seconds=settings.IMAGE_SYNC_REFRESH_INTERVAL_SECONDS),
        id="image_sync_refresh_job",
        name="Refresh image prepull status and node-ready labels",
        replace_existing=True,
        **job_defaults,
    )
    scheduler.add_job(
        reap_stale_builds_job,
        trigger=IntervalTrigger(minutes=5),
        id="reap_stale_builds_job",
        name="Reap stale custom image builds",
        replace_existing=True,
    )
    scheduler.add_job(
        reap_stale_image_jobs_job,
        trigger=IntervalTrigger(minutes=5),
        id="reap_stale_image_jobs_job",
        name="Reap stale image jobs",
        replace_existing=True,
    )
    scheduler.add_job(
        drain_manager_purges_job,
        trigger=IntervalTrigger(seconds=settings.PURGE_DRAIN_INTERVAL_SECONDS),
        id="drain_manager_purges_job",
        name="Drain manager-side purge jobs (purge_p2p/seed/meta)",
        replace_existing=True,
    )
    if (settings.WORKSPACE_VOLUME_TYPE or "").strip().lower() == "localcache":
        scheduler.add_job(
            workspace_local_cache_reaper_job,
            trigger=IntervalTrigger(minutes=settings.WORKSPACE_LOCAL_CACHE_REAPER_INTERVAL_MINUTES),
            id="workspace_local_cache_reaper_job",
            name="Free node-local SSD workspace caches past their TTL",
            replace_existing=True,
            **job_defaults,
        )
        scheduler.add_job(
            workspace_durable_trash_purge_job,
            trigger=IntervalTrigger(hours=24),
            id="workspace_durable_trash_purge_job",
            name="Purge durable workspace .trash past retention",
            replace_existing=True,
            **job_defaults,
        )
        scheduler.add_job(
            workspace_flush_retry_job,
            trigger=IntervalTrigger(minutes=settings.WORKSPACE_LOCAL_CACHE_REAPER_INTERVAL_MINUTES),
            id="workspace_flush_retry_job",
            name="Retry unconfirmed local->durable workspace flushes",
            replace_existing=True,
            **job_defaults,
        )
        if settings.WORKSPACE_DURABLE_DIRQUOTA_ENABLED:
            scheduler.add_job(
                workspace_durable_dirquota_reconcile_job,
                trigger=IntervalTrigger(minutes=settings.WORKSPACE_DURABLE_DIRQUOTA_RECONCILE_MINUTES),
                id="workspace_durable_dirquota_reconcile_job",
                name="Ensure per-user durable dir-quota (SFS Turbo backstop)",
                replace_existing=True,
                **job_defaults,
            )
    scheduler.start()
    logger.info(
        "Scheduler started; cleanup 1m, template preview 2m, reconcile %ss (%s), image sync %ss, telemetry %s",
        settings.RECONCILE_INTERVAL_SECONDS,
        "on" if settings.RECONCILE_ENABLED else "off",
        settings.IMAGE_SYNC_REFRESH_INTERVAL_SECONDS,
        "enabled" if settings.ONECLICK_TELEMETRY_ENABLED else "disabled",
    )


def stop_scheduler():
    """Stop the background scheduler"""
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")
