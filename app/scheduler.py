"""
Background scheduler for cleanup tasks
"""
import asyncio
import logging
import math
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .config import settings

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


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


async def cleanup_job():
    """Periodic job to cleanup idle and expired instances"""
    if _skip_not_leader("cleanup_job"):
        return
    from .k8s_client import k8s_client
    from .store import charge_usage_unit, list_active_instances, mark_instance_deleted, mark_instance_ready_for_billing, update_instance_charge_time

    logger.info("Running cleanup job...")
    try:
        cleaned = []
        now = datetime.now(timezone.utc)
        for record in list_active_instances():
            try:
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

                        await report_gpu_hour_charged_event(
                            instance_id=record["instance_id"],
                            billing_session_id=billing_session_id,
                            billing_unit=unit,
                            user_id=record["user_id"],
                            gpu_count=int(record["gpu_count"]),
                        )
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


def start_scheduler():
    """Start the background scheduler"""
    scheduler.add_job(
        cleanup_job,
        trigger=IntervalTrigger(minutes=1),
        id="cleanup_job",
        name="Bill running instances per GPU-hour",
        replace_existing=True
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
    scheduler.start()
    logger.info(
        "Scheduler started; cleanup runs every 1 minute, template preview sync every 2 minutes, telemetry events %s",
        "enabled" if settings.ONECLICK_TELEMETRY_ENABLED else "disabled",
    )


def stop_scheduler():
    """Stop the background scheduler"""
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")
