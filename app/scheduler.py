"""
Background scheduler for cleanup tasks
"""
import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .config import settings

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def cleanup_job():
    """Periodic job to cleanup idle and expired instances"""
    from .k8s_client import k8s_client
    from .store import charge_user, list_active_instances, mark_instance_deleted, update_instance_charge_time
    
    logger.info("Running cleanup job...")
    try:
        cleaned = k8s_client.cleanup_idle_instances()
        now = datetime.now(timezone.utc)
        for record in list_active_instances():
            try:
                last = datetime.fromisoformat(record["last_charged_at"])
                elapsed_hours = int((now - last).total_seconds() // 3600)
                if elapsed_hours <= 0:
                    continue
                cost = elapsed_hours * int(record["gpu_count"])
                if int(record["credits"]) < cost:
                    if k8s_client.delete_instance_by_id(record["instance_id"]):
                        mark_instance_deleted(record["instance_id"])
                        cleaned.append({
                            "email": record["email"],
                            "reason": f"credits exhausted (balance {record['credits']}, need {cost})",
                        })
                    continue
                charge_user(record["user_id"], cost, f"{elapsed_hours}h gpu usage x {record['gpu_count']} GPU", record["instance_id"])
                update_instance_charge_time(record["id"], now.isoformat())
            except Exception as e:
                logger.error(f"Credit billing failed for {record.get('instance_id')}: {e}")
        if cleaned:
            logger.info(f"Cleaned up {len(cleaned)} instances: {cleaned}")
        else:
            logger.info("No instances to clean up")
    except Exception as e:
        logger.error(f"Cleanup job failed: {e}")


def start_scheduler():
    """Start the background scheduler"""
    scheduler.add_job(
        cleanup_job,
        trigger=IntervalTrigger(minutes=settings.IDLE_TIMEOUT_MINUTES),
        id="cleanup_job",
        name="Cleanup idle and expired instances",
        replace_existing=True
    )
    scheduler.start()
    logger.info(f"Scheduler started, cleanup runs every {settings.IDLE_TIMEOUT_MINUTES} minutes")


def stop_scheduler():
    """Stop the background scheduler"""
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")
