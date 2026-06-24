"""Signed OpenCode proxy handoff/session helpers."""
from typing import Optional

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from .config import settings

_HANDOFF_SALT = "opencode-handoff"
_SESSION_SALT = "opencode-session"

HANDOFF_MAX_AGE_SECONDS = 300
SESSION_MAX_AGE_SECONDS = 12 * 3600
SESSION_COOKIE_NAME = "oc_session"


def _serializer(salt: str) -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(settings.SESSION_SECRET, salt=salt)


def mint_handoff_token(instance_id: str) -> str:
    return _serializer(_HANDOFF_SALT).dumps({"iid": instance_id})


def verify_handoff_token(token: str, max_age: int = HANDOFF_MAX_AGE_SECONDS) -> Optional[str]:
    try:
        data = _serializer(_HANDOFF_SALT).loads(token, max_age=max_age)
    except (BadSignature, SignatureExpired):
        return None
    iid = data.get("iid") if isinstance(data, dict) else None
    return iid or None


def mint_session_cookie(instance_id: str) -> str:
    return _serializer(_SESSION_SALT).dumps({"iid": instance_id})


def verify_session_cookie(value: str, max_age: int = SESSION_MAX_AGE_SECONDS) -> Optional[str]:
    try:
        data = _serializer(_SESSION_SALT).loads(value, max_age=max_age)
    except (BadSignature, SignatureExpired):
        return None
    iid = data.get("iid") if isinstance(data, dict) else None
    return iid or None
