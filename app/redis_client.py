"""
Optional Redis integration for rate limiting and session-epoch caching.

Everything here degrades gracefully: when REDIS_URL is unset or Redis is
unreachable, rate limiting fails open (allows the request) and the caller falls
back to the database as the source of truth for the session epoch. This keeps
the manager fully functional without Redis while letting us harden abuse paths
when Redis is available.
"""
import logging
import threading
import time
from typing import Optional

from .config import settings

logger = logging.getLogger(__name__)

_client = None
_client_lock = threading.Lock()
_last_error_log = 0.0


def get_redis():
    """Return a shared redis client, or None if unavailable/unconfigured."""
    global _client
    if not settings.REDIS_URL:
        return None
    if _client is not None:
        return _client
    with _client_lock:
        if _client is not None:
            return _client
        try:
            import redis  # imported lazily so redis stays an optional dependency

            client = redis.Redis.from_url(
                settings.REDIS_URL,
                socket_connect_timeout=1.5,
                socket_timeout=1.5,
                decode_responses=True,
            )
            client.ping()
            _client = client
            logger.info("Connected to Redis")
        except Exception as e:  # pragma: no cover - infra dependent
            _log_error("Redis unavailable (%s); rate limiting fails open", e)
            _client = None
    return _client


def _log_error(fmt: str, *args):
    global _last_error_log
    now = time.monotonic()
    if now - _last_error_log > 30:
        logger.warning(fmt, *args)
        _last_error_log = now


def rate_limit_ok(key: str, limit: int, window_seconds: int) -> bool:
    """Fixed-window counter. Returns True when the request is allowed.

    Fails open (returns True) when Redis is not configured or unreachable.
    """
    if not settings.RATE_LIMIT_ENABLED or limit <= 0:
        return True
    client = get_redis()
    if client is None:
        return True
    redis_key = f"rl:{key}"
    try:
        count = client.incr(redis_key)
        if count == 1:
            client.expire(redis_key, window_seconds)
        return int(count) <= limit
    except Exception as e:  # pragma: no cover - infra dependent
        _log_error("Redis rate-limit check failed (%s); allowing request", e)
        return True


def rate_limit_at_capacity(key: str, limit: int) -> bool:
    """Non-consuming peek: True when the fixed-window counter for ``key`` is
    already at or above ``limit``.

    Unlike ``rate_limit_ok`` this does not increment the counter, so it is safe to
    call as an early guard before an expensive/destructive step. Fails open
    (returns False) when rate limiting is disabled or Redis is unavailable.
    """
    if not settings.RATE_LIMIT_ENABLED or limit <= 0:
        return False
    client = get_redis()
    if client is None:
        return False
    try:
        count = client.get(f"rl:{key}")
        return count is not None and int(count) >= limit
    except Exception as e:  # pragma: no cover - infra dependent
        _log_error("Redis rate-limit peek failed (%s); allowing request", e)
        return False
