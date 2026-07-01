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


async def cleanup_job():
    """Periodic job to cleanup idle and expired instances"""
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


async def reconcile_job():
    """Bidirectional reconciliation between the cluster (source of truth) and
    the DB. Reclaims orphan/rogue pods (no owning DB record), force-finalizes
    pods stuck Terminating, and marks DB instances deleted when their pod is
    gone."""
    if not settings.RECONCILE_ENABLED:
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
        stuck = [p for p in pod_states if p["terminating"] and (p.get("terminating_seconds") or 0) >= settings.TERMINATING_GRACE_SECONDS]
        orphan_candidates = [
            p for p in pod_states
            if not p["terminating"]
            and p["instance_id"] not in active_ids
            and p["age_seconds"] >= settings.ORPHAN_GRACE_SECONDS
        ]
        gone = active_ids - cluster_ids  # DB active but pod absent

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
                        p.get("waiting_reason") == "CrashLoopBackOff"
                        and p.get("restart_count", 0) >= settings.CRASHLOOP_RESTART_THRESHOLD
                        and p["age_seconds"] >= settings.ORPHAN_GRACE_SECONDS
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
        if len(orphan_candidates) > settings.RECONCILE_MAX_DELETES_PER_CYCLE:
            # Circuit breaker: too many at once smells like a logic/DB fault.
            logger.error(
                "Reconcile: %d orphan candidates exceed cap %d; refusing to delete this cycle and alerting. ids=%s",
                len(orphan_candidates), settings.RECONCILE_MAX_DELETES_PER_CYCLE,
                [p["instance_id"] for p in orphan_candidates],
            )
            orphan_candidates = []

        orphans_removed = 0
        stuck_forced = 0
        for p in stuck:
            logger.warning("Reconcile: force-deleting stuck-terminating pod %s (%ss)", p["instance_id"], p.get("terminating_seconds"))
            k8s_client._delete_pod(p["instance_id"], grace_period_seconds=0)
            stuck_forced += 1
        for p in orphan_candidates:
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
            logger.warning(
                "Reconcile: reclaiming terminal/broken pod %s (phase=%s waiting=%s restarts=%s age=%ss)",
                p["instance_id"], p["phase"], p.get("waiting_reason"), p.get("restart_count"), p["age_seconds"],
            )
            k8s_client.delete_instance_by_id(p["instance_id"], wait=False)
            mark_instance_deleted(p["instance_id"])
            terminal_removed += 1

        db_marked = 0
        for instance_id in gone:
            mark_instance_deleted(instance_id)
            db_marked += 1

        logger.info(
            "Reconcile done: orphans=%s terminal=%s stuck=%s db_marked=%s (candidates orphan=%s terminal=%s stuck=%s gone=%s) cluster_pods=%s active_db=%s",
            orphans_removed, terminal_removed, stuck_forced, db_marked,
            len(orphan_candidates), len(terminal), len(stuck), len(gone), len(cluster_ids), len(active_ids),
        )
    except Exception as e:
        logger.error("Reconcile job failed (no destructive action taken): %s", e)


def image_sync_refresh_job():
    """Refresh prepull warm-cache status into the DB and project per-image
    warmth onto node labels for the scheduling soft-affinity. Runs on a timer
    instead of only when an admin opens the images page (which left status
    stale)."""
    from .k8s_client import k8s_client
    from .store import list_images, update_image_sync_status

    logger.info("Running image sync refresh job...")
    try:
        catalog = list_images(enabled_only=False)
        for image in catalog:
            try:
                sync = k8s_client.get_image_sync_status(image["id"])
                update_image_sync_status(
                    image["id"], sync["status"], sync["desired_count"],
                    sync["ready_count"], sync["message"], sync["completed"],
                )
            except Exception as e:
                logger.warning("Image sync refresh failed for image %s: %s", image.get("id"), e)
        result = k8s_client.reconcile_image_ready_labels(
            [{"id": img["id"], "image": img["image"]} for img in catalog]
        )
        logger.info("Image sync refresh done: %s images, node labels %s", len(catalog), result)
    except Exception as e:
        logger.error("Image sync refresh job failed: %s", e)


def start_scheduler():
    """Start the background scheduler"""
    scheduler.add_job(
        cleanup_job,
        trigger=IntervalTrigger(minutes=1),
        id="cleanup_job",
        name="Cleanup idle and expired instances",
        replace_existing=True
    )
    scheduler.add_job(
        template_preview_sync_job,
        trigger=IntervalTrigger(minutes=2),
        id="template_preview_sync_job",
        name="Sync notebook template preview cache",
        replace_existing=True,
    )
    if settings.RECONCILE_ENABLED:
        scheduler.add_job(
            reconcile_job,
            trigger=IntervalTrigger(seconds=settings.RECONCILE_INTERVAL_SECONDS),
            id="reconcile_job",
            name="Reconcile cluster instances against the database",
            replace_existing=True,
        )
    scheduler.add_job(
        image_sync_refresh_job,
        trigger=IntervalTrigger(seconds=settings.IMAGE_SYNC_REFRESH_INTERVAL_SECONDS),
        id="image_sync_refresh_job",
        name="Refresh image prepull status and node-ready labels",
        replace_existing=True,
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
