"""
Background scheduler for billing, lifecycle, build, and backup tasks.
"""
import asyncio
import logging
import math
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .config import settings

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


def _billing_cleanup_pass():
    from .k8s_client import k8s_client
    from .store import charge_usage_unit, list_active_instances, mark_instance_deleted, update_instance_charge_time

    cleaned = []
    telemetry_events = []
    now = datetime.now(timezone.utc)
    for record in list_active_instances():
        try:
            created = datetime.fromisoformat(record["created_at"])
            elapsed_seconds = int((now - created).total_seconds())
            if elapsed_seconds < 60:
                continue
            billable_units = max(1, math.ceil(elapsed_seconds / 3600))
            for unit in range(1, billable_units + 1):
                billing_session_id = record.get("billing_session_id") or record["instance_id"]
                gpu_count = int(record["gpu_count"])
                result = charge_usage_unit(
                    record["user_id"],
                    record["instance_id"],
                    billing_session_id,
                    unit,
                    gpu_count,
                )
                if result == "charged":
                    telemetry_events.append(
                        {
                            "instance_id": record["instance_id"],
                            "billing_session_id": billing_session_id,
                            "billing_unit": unit,
                            "user_id": record["user_id"],
                            "gpu_count": gpu_count,
                        }
                    )
                if result == "insufficient":
                    if k8s_client.delete_instance_by_id(record["instance_id"]):
                        mark_instance_deleted(record["instance_id"])
                        cleaned.append({
                            "email": record["email"],
                            "reason": f"credits exhausted while charging billing unit {unit}",
                        })
                    break
                if result == "inactive":
                    break
            update_instance_charge_time(record["id"], now.isoformat())
        except Exception as e:
            logger.error(f"Credit billing failed for {record.get('instance_id')}: {e}")
    return cleaned, telemetry_events


async def cleanup_job():
    """Charge active instances and tear down instances that exhaust credits."""
    logger.info("Running cleanup job...")
    try:
        cleaned, telemetry_events = await asyncio.to_thread(_billing_cleanup_pass)
        for event in telemetry_events:
            try:
                from .telemetry import report_gpu_hour_charged_event

                await report_gpu_hour_charged_event(**event)
            except Exception as e:
                logger.warning("Failed to report usage charge telemetry for %s: %s", event.get("instance_id"), e)
        if cleaned:
            logger.info(f"Cleaned up {len(cleaned)} instances: {cleaned}")
        else:
            logger.info("No instances to clean up")
    except Exception as e:
        logger.error(f"Cleanup job failed: {e}")


async def idle_reaper_job():
    """Periodic job to delete idle or max-lifetime-expired pods."""
    from .k8s_client import k8s_client
    from .store import mark_instance_deleted

    try:
        cleaned = await asyncio.to_thread(k8s_client.cleanup_idle_instances, mark_instance_deleted)
        if cleaned:
            logger.info("Idle/lifetime reaper cleaned %s instance(s): %s", len(cleaned), cleaned)
    except Exception as e:
        logger.error(f"Idle/lifetime reaper failed: {e}")


async def refresh_oss_sts_job():
    """Refresh per-instance OSS STS Secret volumes before tokens expire."""
    if not settings.OSS_ENABLED:
        return
    from .k8s_client import k8s_client
    from .oss import refresh_instance_secrets

    try:
        def refresh_current_pod_secrets():
            active_instances = [
                instance for instance in k8s_client.list_instances()
                if instance.get("user_id") and instance.get("oss_secret_name")
            ]
            return refresh_instance_secrets(
                k8s_client.core_v1,
                k8s_client.namespace,
                active_instances,
            )

        result = await asyncio.to_thread(refresh_current_pod_secrets)
        if result.get("refreshed") or result.get("failed"):
            logger.info("OSS STS refresh result: %s", result)
    except Exception as e:
        logger.error(f"OSS STS refresh job failed: {e}")


async def oss_idle_prefix_reaper_job():
    """Delete whole OSS user prefixes after configured no-running-instance idle age."""
    if not settings.OSS_ENABLED:
        return
    from .oss import delete_user_prefix
    from .store import users_idle_since

    cutoff = (datetime.now(timezone.utc) - timedelta(days=settings.OSS_IDLE_EXPIRY_DAYS)).isoformat()
    try:
        idle_users = await asyncio.to_thread(users_idle_since, cutoff)
        if not idle_users:
            return
        results = []
        for user in idle_users:
            result = await asyncio.to_thread(
                delete_user_prefix,
                int(user["user_id"]),
                settings.OSS_IDLE_REAPER_DRY_RUN,
            )
            result["idle_since"] = user["idle_since"]
            results.append(result)
        logger.info("OSS idle prefix reaper results: %s", results)
    except Exception as e:
        logger.error(f"OSS idle prefix reaper failed: {e}")


async def template_preview_sync_job():
    """Periodic job to refresh notebook template preview caches."""
    from .template_sync import sync_due_template_previews

    logger.info("Running template preview sync job...")
    try:
        results = await asyncio.to_thread(lambda: asyncio.run(sync_due_template_previews(limit=5)))
        if results:
            logger.info("Synced %s template previews", len(results))
        else:
            logger.info("No template previews need syncing")
    except Exception as e:
        logger.error(f"Template preview sync job failed: {e}")


async def reap_stale_builds_job():
    """Fail custom image builds whose agent lease has expired (agent died/stalled)."""
    from .store import reap_stale_builds

    try:
        reaped = await asyncio.to_thread(reap_stale_builds, settings.CUSTOM_IMAGE_BUILD_LEASE_TIMEOUT_SECONDS)
        if reaped:
            logger.warning("Reaped %s stale custom image build(s)", reaped)
    except Exception as e:
        logger.error(f"Stale build reaper failed: {e}")


def start_scheduler():
    """Start the background scheduler"""
    scheduler.add_job(
        cleanup_job,
        trigger=IntervalTrigger(minutes=1),
        id="cleanup_job",
        name="Bill active instances and stop credit-exhausted instances",
        replace_existing=True
    )
    scheduler.add_job(
        idle_reaper_job,
        trigger=IntervalTrigger(minutes=5),
        id="idle_reaper_job",
        name="Cleanup idle and max-lifetime-expired instances",
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
        refresh_oss_sts_job,
        trigger=IntervalTrigger(minutes=max(5, settings.OSS_STS_REFRESH_INTERVAL_MINUTES)),
        id="refresh_oss_sts_job",
        name="Refresh OSS STS Secrets",
        replace_existing=True,
    )
    scheduler.add_job(
        oss_idle_prefix_reaper_job,
        trigger=IntervalTrigger(hours=6),
        id="oss_idle_prefix_reaper_job",
        name="Reap idle OSS workspace prefixes",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "Scheduler started; billing every 1 minute, idle reaper every 5 minutes, template preview sync every 2 minutes, telemetry events %s",
        "enabled" if settings.ONECLICK_TELEMETRY_ENABLED else "disabled",
    )


def stop_scheduler():
    """Stop the background scheduler"""
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")
