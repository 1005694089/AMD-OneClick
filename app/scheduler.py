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
    from .store import charge_usage_unit, list_active_instances, mark_instance_deleted, update_instance_charge_time
    
    logger.info("Running cleanup job...")
    try:
        cleaned = []
        now = datetime.now(timezone.utc)
        for record in list_active_instances():
            try:
                created = datetime.fromisoformat(record["created_at"])
                elapsed_seconds = int((now - created).total_seconds())
                if elapsed_seconds < 60:
                    continue
                billable_units = max(1, math.ceil(elapsed_seconds / 3600))
                for unit in range(1, billable_units + 1):
                    result = charge_usage_unit(
                        record["user_id"],
                        record["instance_id"],
                        unit,
                        int(record["gpu_count"]),
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


def start_scheduler():
    """Start the background scheduler"""
    scheduler.add_job(
        cleanup_job,
        trigger=IntervalTrigger(minutes=1),
        id="cleanup_job",
        name="Cleanup idle and expired instances",
        replace_existing=True
    )
    scheduler.start()
    logger.info("Scheduler started, cleanup and billing runs every 1 minute")


def stop_scheduler():
    """Stop the background scheduler"""
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")
