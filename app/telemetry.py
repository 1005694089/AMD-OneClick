"""AMD Telemetry reporting for Radeon Cloud atomic events."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from .config import settings

logger = logging.getLogger(__name__)

GPU_INSTANCE_CREATED_METRIC = "radeon_cloud_gpu_instance_created"
USER_REGISTERED_METRIC = "radeon_cloud_user_registered"
GPU_HOUR_CHARGED_METRIC = "radeon_cloud_gpu_hour_charged"
COUPON_ADP_USERS_DAILY_METRIC = "radeon_cloud_coupon_unique_adp_users_daily"
COUPON_ADP_USERS_TOTAL_DAILY_METRIC = "radeon_cloud_coupon_unique_adp_users_total_daily"


def telemetry_configured() -> bool:
    return bool(settings.ONECLICK_TELEMETRY_ENABLED and settings.TELEMETRY_API_URL and settings.METRICS_INGEST_API_KEY)


def _timestamp() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _base_labels() -> dict[str, str]:
    return {
        "product": settings.ONECLICK_TELEMETRY_PRODUCT,
        "source": settings.ONECLICK_TELEMETRY_SOURCE,
    }


async def _post_metric(name: str, labels: dict[str, Any], value: float = 1, timestamp: int | None = None) -> bool:
    if not telemetry_configured():
        return False

    metric = {
        "name": name,
        "value": value,
        "labels": {**_base_labels(), **{key: str(val) for key, val in labels.items() if val is not None}},
        "timestamp": timestamp or _timestamp(),
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            settings.TELEMETRY_API_URL,
            json={"metrics": [metric]},
            headers={"X-API-Key": settings.METRICS_INGEST_API_KEY},
        )
    resp.raise_for_status()
    return True


async def _post_metrics(metrics: list[dict[str, Any]]) -> bool:
    if not telemetry_configured() or not metrics:
        return False

    payload = []
    for metric in metrics:
        payload.append(
            {
                "name": metric["name"],
                "value": metric["value"],
                "labels": {**_base_labels(), **{key: str(val) for key, val in metric.get("labels", {}).items() if val is not None}},
                "timestamp": metric.get("timestamp") or _timestamp(),
            }
        )
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            settings.TELEMETRY_API_URL,
            json={"metrics": payload},
            headers={"X-API-Key": settings.METRICS_INGEST_API_KEY},
        )
    resp.raise_for_status()
    return True


def _day_timestamp(day: str) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())


async def report_user_registered_event(user: dict[str, Any]) -> None:
    try:
        reported = await _post_metric(
            USER_REGISTERED_METRIC,
            {
                "event": "user_registered",
                "provider": user.get("provider", ""),
                "user_id": user.get("id"),
            },
        )
        if reported:
            logger.info("Reported Radeon Cloud user registration event to AMD Telemetry")
    except Exception as exc:
        logger.warning("Failed to report user registration event to AMD Telemetry: %s", exc)


async def report_gpu_instance_created_event(
    *,
    instance_id: str,
    user_id: int,
    instance_type: str,
    gpu_count: int,
    template_id: int | None = None,
) -> None:
    try:
        reported = await _post_metric(
            GPU_INSTANCE_CREATED_METRIC,
            {
                "event": "gpu_instance_created",
                "instance_id": instance_id,
                "user_id": user_id,
                "instance_type": instance_type,
                "gpu_count": gpu_count,
                "gpu_type": "amd",
                "template_id": template_id,
            },
        )
        if reported:
            logger.info("Reported Radeon Cloud GPU instance creation event to AMD Telemetry")
    except Exception as exc:
        logger.warning("Failed to report GPU instance creation event to AMD Telemetry: %s", exc)


async def report_gpu_hour_charged_event(
    *,
    instance_id: str,
    billing_session_id: str,
    billing_unit: int,
    user_id: int,
    gpu_count: int,
    timestamp: int | None = None,
) -> None:
    try:
        reported = await _post_metric(
            GPU_HOUR_CHARGED_METRIC,
            {
                "event": "gpu_hour_charged",
                "instance_id": instance_id,
                "billing_session_id": billing_session_id,
                "billing_unit": billing_unit,
                "user_id": user_id,
                "gpu_count": gpu_count,
                "gpu_type": "amd",
            },
            value=float(gpu_count),
            timestamp=timestamp,
        )
        if reported:
            logger.info("Reported Radeon Cloud GPU hour charge event to AMD Telemetry")
    except Exception as exc:
        logger.warning("Failed to report GPU hour charge event to AMD Telemetry: %s", exc)


async def report_coupon_adp_user_daily_metrics() -> None:
    try:
        from .store import get_coupon_adp_user_daily_stats

        metrics = []
        for row in get_coupon_adp_user_daily_stats():
            labels = {
                "metric_type": "coupon_redeemed_unique_adp_users",
                "rollup": "daily",
            }
            timestamp = _day_timestamp(row["day"])
            metrics.append(
                {
                    "name": COUPON_ADP_USERS_DAILY_METRIC,
                    "labels": labels,
                    "value": float(row["new_unique_adp_user_ids"]),
                    "timestamp": timestamp,
                }
            )
            metrics.append(
                {
                    "name": COUPON_ADP_USERS_TOTAL_DAILY_METRIC,
                    "labels": labels,
                    "value": float(row["cumulative_unique_adp_user_ids"]),
                    "timestamp": timestamp,
                }
            )
        reported = await _post_metrics(metrics)
        if reported:
            logger.info("Reported %s Radeon Cloud coupon ADP user daily metrics", len(metrics))
    except Exception as exc:
        logger.warning("Failed to report Radeon Cloud coupon ADP user daily metrics: %s", exc)
