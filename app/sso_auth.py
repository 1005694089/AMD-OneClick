"""SSO authentication helpers for developer-issued JWT cookies."""

from __future__ import annotations

import time
from typing import Optional
import httpx
import jwt
from fastapi import HTTPException, Request, Response
from redis import Redis

from .config import settings
from .store import get_or_create_sso_user

_redis_client: Optional[Redis] = None


def _redis() -> Optional[Redis]:
    global _redis_client
    if not settings.REDIS_URL:
        return None
    if _redis_client is None:
        _redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis_client


def _absolute_url(request: Request, value: str) -> str:
    if value.startswith("http://") or value.startswith("https://"):
        return value
    return str(request.base_url).rstrip("/") + "/" + value.lstrip("/")


def _decode_jwt(token: str) -> dict:
    options = {"require": ["exp", "token_version"]}
    claims = jwt.decode(
        token,
        settings.SSO_PUBLIC_KEY_PEM,
        algorithms=[settings.SSO_ALGORITHM],
        audience=settings.SSO_AUDIENCE,
        issuer=settings.SSO_ISSUER,
        options=options,
    )
    return _normalize_claims(claims)


def _normalize_claims(claims: dict) -> dict:
    normalized = dict(claims)
    if not normalized.get("sub") and normalized.get("Id"):
        normalized["sub"] = str(normalized.get("Id"))
    if not normalized.get("username") and normalized.get(settings.SSO_CLAIM_NAME_URI):
        normalized["username"] = str(normalized.get(settings.SSO_CLAIM_NAME_URI))
    if not normalized.get("email") and normalized.get(settings.SSO_CLAIM_EMAIL_URI):
        normalized["email"] = str(normalized.get(settings.SSO_CLAIM_EMAIL_URI))
    if not normalized.get("sub"):
        raise jwt.InvalidTokenError("JWT missing required subject claim")
    return normalized


def _read_token_version_from_redis(user_id: str) -> Optional[int]:
    client = _redis()
    if not client:
        return None
    key = f"{settings.REDIS_TOKEN_VERSION_KEY_PREFIX}:{user_id}:token_version"
    raw = client.get(key)
    if raw is None:
        return None
    return int(raw)


async def _refresh_tokens(request: Request, response: Response) -> Optional[str]:
    refresh_url = _absolute_url(request, settings.SSO_REFRESH_URL)
    timeout = httpx.Timeout(settings.OAUTH_READ_TIMEOUT_SECONDS, connect=settings.OAUTH_CONNECT_TIMEOUT_SECONDS)
    cookies = {}
    access_cookie = request.cookies.get(settings.SSO_ACCESS_COOKIE_NAME)
    refresh_cookie = request.cookies.get(settings.SSO_REFRESH_COOKIE_NAME)
    if access_cookie:
        cookies[settings.SSO_ACCESS_COOKIE_NAME] = access_cookie
    if refresh_cookie:
        cookies[settings.SSO_REFRESH_COOKIE_NAME] = refresh_cookie

    if not cookies:
        return None

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        try:
            resp = await client.post(refresh_url, cookies=cookies)
        except httpx.HTTPError:
            return None

    if resp.status_code >= 400:
        return None

    for header, value in resp.headers.multi_items():
        if header.lower() == "set-cookie":
            response.headers.append("set-cookie", value)

    return resp.cookies.get(settings.SSO_ACCESS_COOKIE_NAME) or request.cookies.get(settings.SSO_ACCESS_COOKIE_NAME)


def _email_from_claims(claims: dict) -> str:
    email = str(claims.get("email") or "").strip().lower()
    if email:
        return email
    username = str(claims.get("username") or "").strip().lower()
    if username and "@" in username:
        return username
    if username:
        return f"{username}@{settings.SSO_DEFAULT_USER_DOMAIN}"
    return f"{claims['sub']}@{settings.SSO_DEFAULT_USER_DOMAIN}"


def _build_user_from_claims(claims: dict) -> dict:
    user = get_or_create_sso_user(
        developer_user_id=str(claims["sub"]),
        email=_email_from_claims(claims),
        username=str(claims.get("username") or "").strip(),
        nickname=str(claims.get("nickname") or "").strip(),
    )
    user["sso_sub"] = str(claims["sub"])
    user["sso_username"] = str(claims.get("username") or "")
    user["sso_nickname"] = str(claims.get("nickname") or "")
    return user


def resolve_websocket_user_from_cookie_header(cookie_header: str) -> Optional[dict]:
    if not settings.SSO_ENABLED:
        return None
    if not cookie_header:
        return None

    cookies: dict[str, str] = {}
    for chunk in cookie_header.split(";"):
        part = chunk.strip()
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        cookies[name.strip()] = value.strip()

    access_token = cookies.get(settings.SSO_ACCESS_COOKIE_NAME)
    if not access_token:
        return None

    try:
        claims = _decode_jwt(access_token)
    except jwt.PyJWTError:
        return None

    token_version_claim = int(claims.get("token_version") or 0)
    redis_version = _read_token_version_from_redis(str(claims["sub"]))
    if redis_version is not None and redis_version != token_version_claim:
        return None

    return _build_user_from_claims(claims)


async def resolve_current_user(request: Request, response: Response, required: bool = True) -> Optional[dict]:
    if not settings.SSO_ENABLED:
        raise HTTPException(status_code=500, detail="SSO is not enabled")

    access_token = request.cookies.get(settings.SSO_ACCESS_COOKIE_NAME)
    if not access_token:
        if required:
            raise HTTPException(status_code=401, detail="Login required")
        return None

    claims: Optional[dict] = None
    try:
        claims = _decode_jwt(access_token)
    except jwt.PyJWTError:
        claims = None

    now = int(time.time())
    should_refresh = False
    if claims is None:
        should_refresh = True
    else:
        exp = int(claims.get("exp") or 0)
        should_refresh = (exp - now) <= settings.SSO_REFRESH_THRESHOLD_SECONDS

    if should_refresh:
        refreshed_token = await _refresh_tokens(request, response)
        if refreshed_token:
            try:
                claims = _decode_jwt(refreshed_token)
            except jwt.PyJWTError:
                claims = None

    if claims is None:
        if required:
            raise HTTPException(status_code=401, detail="Invalid or expired SSO token")
        return None

    token_version_claim = int(claims.get("token_version") or 0)
    redis_version = _read_token_version_from_redis(str(claims["sub"]))
    if redis_version is not None and redis_version != token_version_claim:
        if required:
            raise HTTPException(status_code=401, detail="Token has been revoked")
        return None

    return _build_user_from_claims(claims)


async def developer_logout(request: Request, response: Response) -> None:
    logout_url = _absolute_url(request, settings.SSO_LOGOUT_URL)
    timeout = httpx.Timeout(settings.OAUTH_READ_TIMEOUT_SECONDS, connect=settings.OAUTH_CONNECT_TIMEOUT_SECONDS)
    cookies = {}
    access_cookie = request.cookies.get(settings.SSO_ACCESS_COOKIE_NAME)
    refresh_cookie = request.cookies.get(settings.SSO_REFRESH_COOKIE_NAME)
    if access_cookie:
        cookies[settings.SSO_ACCESS_COOKIE_NAME] = access_cookie
    if refresh_cookie:
        cookies[settings.SSO_REFRESH_COOKIE_NAME] = refresh_cookie

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        try:
            await client.post(logout_url, cookies=cookies)
        except httpx.HTTPError:
            # Best effort: local cookies are still cleared.
            pass

    response.delete_cookie(settings.SSO_ACCESS_COOKIE_NAME, path="/")
    response.delete_cookie(settings.SSO_REFRESH_COOKIE_NAME, path="/")
