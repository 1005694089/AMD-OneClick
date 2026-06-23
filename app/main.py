"""
FastAPI main application for AMD OneClick Notebook Manager
"""
import hashlib
import json
import logging
import os
import base64
import asyncio
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote, urlencode, urlparse

from fastapi import FastAPI, HTTPException, Depends, Query, Request, Response, Cookie, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.background import BackgroundTask
from starlette.middleware.sessions import SessionMiddleware
import secrets
import httpx
import requests
import websockets

from .config import settings, INSTANCE_TYPES, APP_FRAMEWORK_PRESETS
from .models import (
    NotebookRequest, 
    NotebookStatus, 
    AdminListResponse, 
    NotebookListItem,
    DestroyResponse,
    ImageRequest,
    CreditGrantRequest,
    InstanceBulkDestroyRequest,
    CouponRedeemRequest,
    NotebookTemplateRequest,
    TemplateLaunchRequest,
)
from .k8s_client import AUTO_RESOURCE_PROFILE_BY_GPU, RESOURCE_PROFILES, k8s_client
from .scheduler import start_scheduler, stop_scheduler
from .template_sync import sync_template_preview
from .store import (
    clear_template_preview_cache,
    delete_image,
    delete_notebook_template,
    get_active_instance_for_user,
    get_admin_daily_stats,
    get_charged_credits_for_instance,
    get_image_by_value,
    get_notebook_template,
    get_or_create_user,
    get_template_preview_asset,
    get_template_preview_cache,
    get_user,
    ensure_user_min_credits,
    grant_user_credits,
    ensure_template_preview_cache,
    init_db,
    list_images,
    list_notebook_templates,
    list_users,
    mark_instance_deleted,
    mark_instance_ready_for_billing,
    record_instance,
    record_instance_launch_event,
    redeem_user_coupon,
    template_preview_fingerprint,
    upsert_notebook_template,
    update_image_sync_status,
    upsert_image,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting AMD OneClick Notebook Manager")
    init_db()
    if settings.RUN_SCHEDULER:
        start_scheduler()
    else:
        logger.info("Background scheduler disabled for this manager process")
    yield
    # Shutdown
    logger.info("Shutting down AMD OneClick Notebook Manager")
    if settings.RUN_SCHEDULER:
        stop_scheduler()


app = FastAPI(
    title="AMD OneClick Notebook Manager",
    description="Kubernetes-based Jupyter Notebook instance management",
    version="1.0.0",
    lifespan=lifespan
)
app.add_middleware(SessionMiddleware, secret_key=settings.SESSION_SECRET)


@app.middleware("http")
async def log_slow_requests(request: Request, call_next):
    start = time.monotonic()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        elapsed = time.monotonic() - start
        if elapsed >= settings.SLOW_REQUEST_THRESHOLD_SECONDS:
            logger.warning("Slow request method=%s path=%s status=%s duration=%.3fs", request.method, request.url.path, status_code, elapsed)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Templates
templates = Jinja2Templates(directory="templates")

# HTTP Basic Auth for admin
security = HTTPBasic()


def verify_admin(credentials: HTTPBasicCredentials = Depends(security)):
    """Verify admin credentials"""
    correct_password = secrets.compare_digest(
        credentials.password.encode("utf8"),
        settings.ADMIN_PASSWORD.encode("utf8")
    )
    if not (credentials.username == "admin" and correct_password):
        raise HTTPException(
            status_code=401,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


def current_user(request: Request) -> dict:
    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Login required")
    user = get_user(int(user_id))
    if not user:
        request.session.clear()
        raise HTTPException(status_code=401, detail="Login required")
    return user


def session_user(request: Request) -> Optional[dict]:
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    return get_user(int(user_id))

def _workshop_index_from_email(email: str) -> Optional[int]:
    raw = (email or "").strip().lower()
    if not raw.endswith("@amd.com") or not raw.startswith("workshop"):
        return None
    number = raw.removeprefix("workshop").removesuffix("@amd.com")
    if not number.isdigit():
        return None
    index = int(number)
    return index if 1 <= index <= settings.WORKSHOP_USER_COUNT else None


def current_editor(user: dict = Depends(current_user)) -> dict:
    if not user.get("is_editor"):
        raise HTTPException(status_code=403, detail="Editor permission required")
    return user


def _oauth_redirect_uri(request: Request, provider: str) -> str:
    configured = settings.GITHUB_REDIRECT_URI if provider == "github" else settings.MODELSCOPE_REDIRECT_URI
    if configured:
        return configured
    return str(request.url_for(f"{provider}_callback"))


def _oauth_state(request: Request, provider: str) -> str:
    state = secrets.token_urlsafe(24)
    request.session[f"{provider}_oauth_state"] = state
    return state


def _github_repo_parts(repo_url: str) -> tuple[str, str]:
    raw = repo_url.strip()
    if raw.startswith("git@github.com:"):
        path = raw.split(":", 1)[1]
    else:
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        host = parsed.netloc.lower()
        if host == "github":
            raise ValueError("GitHub repo URL must use github.com, e.g. https://github.com/org/repo")
        if host not in {"github.com", "www.github.com"}:
            raise ValueError("Only GitHub repository URLs are supported for templates")
        path = parsed.path.lstrip("/")

    path = path.removesuffix(".git").rstrip("/")
    parts = path.split("/")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError("Invalid GitHub repository URL, expected https://github.com/org/repo")
    return parts[0], parts[1]


def _github_clone_url(repo_url: str) -> str:
    org, repo = _github_repo_parts(repo_url)
    return f"http://github.com/{org}/{repo}.git"


def _github_raw_url(repo_url: str, branch: str, notebook_path: str) -> str:
    org, repo = _github_repo_parts(repo_url)
    return f"https://raw.githubusercontent.com/{org}/{repo}/{branch}/{notebook_path.lstrip('/')}"


def _github_raw_url_candidates(repo_url: str, branch: str, notebook_path: str) -> list[str]:
    org, repo = _github_repo_parts(repo_url)
    path = notebook_path.lstrip("/")
    return [
        f"https://raw.githubusercontent.com/{org}/{repo}/{branch}/{path}",
        f"https://github.com/{org}/{repo}/raw/{branch}/{path}",
        f"http://github.com/{org}/{repo}/raw/{branch}/{path}",
    ]


def _template_asset_base_path(template_id: int, notebook_path: str) -> str:
    parent = notebook_path.lstrip("/").rsplit("/", 1)
    directory = parent[0].strip("/") if len(parent) == 2 else ""
    if directory:
        return f"/templates/{template_id}/assets/{directory}/"
    return f"/templates/{template_id}/assets/"


def _template_github_info(template: dict) -> dict:
    if not template.get("repo_url") or not template.get("notebook_path"):
        return {}
    org, repo = _github_repo_parts(template["repo_url"])
    return {
        "org": org,
        "repo": repo,
        "branch": template["branch"],
        "path": template["notebook_path"].lstrip("/"),
        "raw_url": _github_raw_url(template["repo_url"], template["branch"], template["notebook_path"]),
        "repo_url": _github_clone_url(template["repo_url"]),
        "template_id": str(template["id"]),
        "template_title": template["title"],
    }


def _save_notebook_template(
    req: NotebookTemplateRequest,
    template_id: Optional[int] = None,
    owner_user_id: Optional[int] = None,
    sort_order: Optional[int] = None,
    enabled_override: Optional[bool] = None,
) -> dict:
    if not get_image_by_value(req.image):
        raise ValueError("Template image must be an enabled image catalog entry")
    has_repo = bool((req.repo_url or "").strip())
    has_notebook = bool((req.notebook_path or "").strip())
    if has_repo != has_notebook:
        raise ValueError("GitHub repo URL and notebook path must be provided together, or both left empty for an image-only template")
    if has_repo:
        _github_repo_parts(req.repo_url or "")
    template_instance_type = (req.instance_type or "").strip() or None
    if template_instance_type is not None:
        type_cfg = INSTANCE_TYPES.get(template_instance_type)
        if not type_cfg or not type_cfg.get("enabled"):
            raise ValueError("Invalid or disabled instance type for template")
    template_app_port = req.app_port if req.app_port else None
    if template_app_port is not None and int(template_app_port) not in set(settings.APP_PORTS.values()):
        raise ValueError(f"app_port must be one of the curated app ports: {sorted(set(settings.APP_PORTS.values()))}")
    template = upsert_notebook_template(
        req.title,
        req.slug or "",
        req.description or "",
        req.category or "",
        req.tags or "",
        req.image,
        req.repo_url or "",
        req.branch,
        req.notebook_path or "",
        req.cover_url or "",
        req.enabled if enabled_override is None else enabled_override,
        req.sort_order if sort_order is None else sort_order,
        template_id=template_id,
        owner_user_id=owner_user_id,
        upsert_on_slug_conflict=owner_user_id is None,
        instance_type=template_instance_type,
        start_command=req.start_command,
        app_port=template_app_port,
    )
    if template.get("repo_url") and template.get("notebook_path"):
        ensure_template_preview_cache(template, force=True)
        _schedule_template_preview_sync(template["id"], force=True)
    else:
        clear_template_preview_cache(template["id"])
    return template


def _template_accessible_to_user(template_id: int, user: Optional[dict]) -> Optional[dict]:
    template = get_notebook_template(template_id, enabled_only=True)
    if template:
        return template
    if user:
        return get_notebook_template(template_id, owner_user_id=user["id"])
    return None


def _request_public_origin(request: Request) -> str:
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"{proto}://{host}".rstrip("/")


def _instance_public_url(request: Request, instance_id: str, notebook_path: Optional[str] = None) -> str:
    path = f"/instances/{instance_id}/lab"
    if notebook_path:
        path += f"/tree/{quote(notebook_path.lstrip('/'), safe='/')}"
    return f"{_request_public_origin(request)}{path}?token={settings.NOTEBOOK_TOKEN}"


def _ready_instance_url(request: Request, instance: Optional[dict], status: Optional[str]) -> Optional[str]:
    if status != "ready" or not instance:
        return None
    itype = (instance.get("instance_type") or "").strip()
    if itype in APP_FRAMEWORK_PRESETS:
        port = instance.get("app_port") or APP_FRAMEWORK_PRESETS[itype].get("port")
        origin = _request_public_origin(request) if request else (settings.PUBLIC_BASE_URL or "").rstrip("/")
        return f"{origin}{settings.SPACES_PATH_PREFIX}/{instance['id']}/{port}/"
    return _instance_public_url(request, instance["id"], instance.get("github_path"))


def _instance_api_info(request: Request, instance: Optional[dict], status: Optional[str]) -> tuple:
    """For API-kind instances, return (base_url, api_key) once ready, else (None, None).
    base_url is the OpenAI-compatible base, e.g. .../spaces/<id>/8000/v1"""
    if status != "ready" or not instance or not instance.get("api_kind"):
        return None, None
    itype = (instance.get("instance_type") or "").strip()
    port = instance.get("app_port") or APP_FRAMEWORK_PRESETS.get(itype, {}).get("port")
    suffix = instance.get("api_base_suffix") or APP_FRAMEWORK_PRESETS.get(itype, {}).get("api_base_suffix", "")
    origin = _request_public_origin(request) if request else (settings.PUBLIC_BASE_URL or "").rstrip("/")
    base = f"{origin}{settings.SPACES_PATH_PREFIX}/{instance['id']}/{port}{suffix}"
    return base, instance.get("api_key")


def _notebook_status_message(status_details: Optional[dict]) -> str:
    status = (status_details or {}).get("status") or "unknown"
    detail = (status_details or {}).get("message") or ""
    defaults = {
        "ready": "Your notebook is ready!",
        "running": "Container is running, waiting for readiness...",
        "jupyter_starting": "Jupyter is starting up...",
        "pending": "Waiting for resources...",
        "initializing": "Initializing notebook environment...",
        "loading": "Loading notebook image...",
        "failed": "Notebook creation failed",
        "unknown": "Checking status...",
    }
    if detail and status != "ready":
        return detail
    return defaults.get(status, "Checking status...")


def _validate_resource_profile(profile: Optional[str]) -> str:
    value = (profile or "auto").strip().lower()
    if value == "auto" or value in RESOURCE_PROFILES:
        return value
    allowed = ", ".join(["auto", *RESOURCE_PROFILES.keys()])
    raise HTTPException(status_code=400, detail=f"Invalid resource profile. Allowed values: {allowed}")


def _active_instance_context(user: Optional[dict], request: Optional[Request] = None) -> Optional[dict]:
    if not user:
        return None
    active_instance = get_active_instance_for_user(user["id"])
    if not active_instance:
        return None

    live_instance = k8s_client.get_instance_by_id(active_instance["instance_id"])
    if not live_instance:
        mark_instance_deleted(active_instance["instance_id"])
        return None

    status_details = k8s_client.get_pod_status_details(user["email"].lower(), instance_id=active_instance["instance_id"]) or {}
    live_status = status_details.get("status")
    if live_status == "ready" and active_instance.get("status") != "running":
        active_instance = mark_instance_ready_for_billing(active_instance["instance_id"]) or active_instance

    created_at = datetime.fromisoformat(active_instance["created_at"])
    now = datetime.now(timezone.utc)
    runtime_seconds = max(0, int((now - created_at).total_seconds())) if active_instance.get("status") == "running" else 0
    active_instance["runtime_minutes"] = runtime_seconds // 60
    active_instance["runtime_hours_display"] = round(runtime_seconds / 3600, 2)
    active_instance["credits_consumed"] = get_charged_credits_for_instance(
        active_instance["instance_id"],
        active_instance.get("billing_session_id"),
    )
    active_instance["live_status"] = live_status or "unknown"
    active_instance["live_reason"] = status_details.get("reason")
    active_instance["live_message"] = status_details.get("message")
    active_instance["url"] = _ready_instance_url(request, live_instance, live_status) if request else (live_instance.get("url") if live_status == "ready" else None)
    active_instance["github_path"] = live_instance.get("github_path")
    active_instance["template_id"] = live_instance.get("template_id")
    active_instance["template_title"] = live_instance.get("template_title")
    active_instance["instance_type"] = live_instance.get("instance_type")
    active_instance["app_port"] = live_instance.get("app_port")
    api_base_url, api_key = _instance_api_info(request, live_instance, live_status)
    active_instance["api_base_url"] = api_base_url
    active_instance["api_key"] = api_key
    return active_instance


def _can_manage_template(template: dict, user: Optional[dict]) -> bool:
    if not template or not user:
        return False
    return template.get("owner_user_id") == user["id"]


def _schedule_template_preview_sync(template_id: int, force: bool = False):
    try:
        asyncio.create_task(sync_template_preview(template_id, force=force))
    except RuntimeError:
        logger.warning("No running event loop available to schedule template preview sync")


def _preview_cache_public(cache: Optional[dict]) -> dict:
    if not cache:
        return {}
    return {
        key: cache.get(key)
        for key in [
            "template_id",
            "repo_url",
            "branch",
            "notebook_path",
            "source_fingerprint",
            "status",
            "error_message",
            "last_synced_at",
            "next_sync_at",
            "created_at",
            "updated_at",
        ]
    }


def _validate_oauth_state(request: Request, provider: str, state: str):
    expected = request.session.pop(f"{provider}_oauth_state", None)
    if not expected or not secrets.compare_digest(expected, state or ""):
        raise HTTPException(status_code=400, detail="Invalid OAuth state")


def _instance_service_base(instance_id: str) -> str:
    try:
        svc = k8s_client.core_v1.read_namespaced_service(
            name=f"{instance_id}-svc",
            namespace=k8s_client.namespace,
        )
    except Exception:
        raise HTTPException(status_code=404, detail="Instance service not found")
    return f"http://{svc.spec.cluster_ip}:{settings.NOTEBOOK_PORT}"


def _proxy_headers(headers) -> dict:
    skip = {
        "host", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailers", "transfer-encoding", "upgrade", "content-length",
    }
    return {k: v for k, v in headers.items() if k.lower() not in skip}


def _rewrite_location(location: str, instance_id: str, target_base: str) -> str:
    public_prefix = f"/instances/{instance_id}/"
    if location.startswith(target_base):
        return location.replace(target_base, public_prefix.rstrip("/"), 1)
    if location.startswith("/"):
        return location
    return location


async def _close_httpx_stream(upstream: httpx.Response, client: httpx.AsyncClient):
    await upstream.aclose()
    await client.aclose()


def _request_with_retries(method: str, url: str, retries: int = 3, **kwargs) -> requests.Response:
    last_error = None
    timeout = kwargs.pop("timeout", 30)
    for attempt in range(1, retries + 1):
        try:
            return requests.request(method, url, timeout=timeout, **kwargs)
        except requests.RequestException as e:
            last_error = e
            logger.warning("HTTP %s %s failed on attempt %s/%s: %s", method, url, attempt, retries, e)
            if attempt < retries:
                import time
                time.sleep(min(2 * attempt, 5))
    raise last_error


def _oauth_timeout() -> httpx.Timeout:
    return httpx.Timeout(settings.OAUTH_READ_TIMEOUT_SECONDS, connect=settings.OAUTH_CONNECT_TIMEOUT_SECONDS)


def _parse_coupon_datetime(value: str) -> datetime:
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _decode_credit_coupon(encrypted_coupon_b64: str) -> dict:
    if not settings.COUPON_PRIVATE_KEY_PEM:
        raise HTTPException(status_code=500, detail="Coupon redemption is not configured")
    try:
        from Crypto.Cipher import PKCS1_OAEP
        from Crypto.PublicKey import RSA

        encrypted = base64.b64decode(encrypted_coupon_b64.strip(), validate=True)
        rsa_private_key = RSA.import_key(settings.COUPON_PRIVATE_KEY_PEM)
        cipher_rsa = PKCS1_OAEP.new(rsa_private_key)
        decrypted_json = cipher_rsa.decrypt(encrypted)
        coupon = json.loads(decrypted_json.decode("utf-8"))

        required = {"coupon_id", "user_id", "card_hours", "issued_at", "expires_at"}
        missing = required - set(coupon)
        if missing:
            raise ValueError(f"missing fields: {', '.join(sorted(missing))}")
        if not isinstance(coupon["card_hours"], int) or coupon["card_hours"] <= 0:
            raise ValueError("invalid card_hours")
        if datetime.utcnow() > _parse_coupon_datetime(coupon["expires_at"]):
            raise ValueError("coupon expired")
        return coupon
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid coupon: {e}")


# =============================================================================
# User Endpoints
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the main request page"""
    user = get_user(int(request.session["user_id"])) if request.session.get("user_id") else None
    images = list_images(enabled_only=True)
    notebook_templates = list_notebook_templates(enabled_only=True)
    active_instance = _active_instance_context(user, request)
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "images": [img["image"] for img in images],
            "image_catalog_json": json.dumps(images),
            "notebook_templates_json": json.dumps(notebook_templates),
            "default_image": settings.DEFAULT_IMAGE,
            "instance_types_json": json.dumps(INSTANCE_TYPES),
            "user_json": json.dumps(user or {}),
            "active_instance_json": json.dumps(active_instance or {}),
            "workshop_login_enabled": settings.WORKSHOP_LOGIN_ENABLED,
            "admin_login_enabled": settings.ADMIN_LOGIN_ENABLED,
            "resource_profiles_json": json.dumps(RESOURCE_PROFILES),
            "auto_resource_profile_by_gpu_json": json.dumps(AUTO_RESOURCE_PROFILE_BY_GPU),
        },
    )


@app.get("/profile", response_class=HTMLResponse)
async def profile_page(request: Request):
    """Render user profile and login page."""
    user = get_user(int(request.session["user_id"])) if request.session.get("user_id") else None
    active_instance = _active_instance_context(user, request)
    return templates.TemplateResponse(
        request,
        "profile.html",
        {
            "user": user,
            "active_instance": active_instance,
            "github_enabled": bool(settings.GITHUB_CLIENT_ID),
            "modelscope_enabled": bool(settings.MODELSCOPE_CLIENT_ID),
            "coupon_redeem_enabled": settings.COUPON_REDEEM_ENABLED,
            "coupon_redeem_disabled_message": settings.COUPON_REDEEM_DISABLED_MESSAGE,
        },
    )


@app.get("/auth/github/login")
async def github_login(request: Request):
    if not settings.GITHUB_CLIENT_ID:
        raise HTTPException(status_code=500, detail="GitHub OAuth is not configured")
    params = {
        "client_id": settings.GITHUB_CLIENT_ID,
        "redirect_uri": _oauth_redirect_uri(request, "github"),
        "scope": "read:user user:email",
        "state": _oauth_state(request, "github"),
    }
    return RedirectResponse("https://github.com/login/oauth/authorize?" + urlencode(params))


@app.get("/auth/github/callback", name="github_callback")
async def github_callback(request: Request, code: str = Query(...), state: str = Query("")):
    _validate_oauth_state(request, "github", state)
    try:
        async with httpx.AsyncClient(timeout=_oauth_timeout()) as client:
            token_resp = await client.post(
                "https://github.com/login/oauth/access_token",
                headers={"Accept": "application/json"},
                data={
                    "client_id": settings.GITHUB_CLIENT_ID,
                    "client_secret": settings.GITHUB_CLIENT_SECRET,
                    "code": code,
                    "redirect_uri": _oauth_redirect_uri(request, "github"),
                },
            )
            token = token_resp.json().get("access_token")
            if not token:
                raise HTTPException(status_code=400, detail="GitHub OAuth token exchange failed")
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            profile_resp, emails_resp = await asyncio.gather(
                client.get("https://api.github.com/user", headers=headers),
                client.get("https://api.github.com/user/emails", headers=headers),
            )
    except httpx.RequestError as e:
        logger.error("GitHub OAuth request failed: %s", e)
        raise HTTPException(status_code=502, detail="GitHub OAuth request failed")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("GitHub OAuth response handling failed: %s", e)
        raise HTTPException(status_code=400, detail="GitHub OAuth token exchange failed")
    profile = profile_resp.json()
    emails = emails_resp.json()
    email = profile.get("email") or next((e["email"] for e in emails if e.get("primary")), None)
    if not email:
        raise HTTPException(status_code=400, detail="GitHub account has no accessible email")
    user = get_or_create_user("github", str(profile["id"]), email, profile.get("name") or profile.get("login") or "", profile.get("avatar_url") or "")
    if user.get("_created"):
        from .telemetry import report_user_registered_event

        await report_user_registered_event(user)
    request.session["user_id"] = user["id"]
    return RedirectResponse("/")


@app.get("/auth/modelscope/login")
async def modelscope_login(request: Request):
    if not settings.MODELSCOPE_CLIENT_ID:
        raise HTTPException(status_code=500, detail="ModelScope OAuth is not configured")
    params = {
        "client_id": settings.MODELSCOPE_CLIENT_ID,
        "redirect_uri": _oauth_redirect_uri(request, "modelscope"),
        "response_type": "code",
        "scope": "openid profile read-repos api-inference",
        "state": _oauth_state(request, "modelscope"),
    }
    return RedirectResponse(settings.MODELSCOPE_AUTH_URL + "?" + urlencode(params))


@app.get("/auth/modelscope/callback", name="modelscope_callback")
async def modelscope_callback(request: Request, code: str = Query(...), state: str = Query("")):
    expected_state = request.session.pop("modelscope_oauth_state", None)
    if expected_state and state and not secrets.compare_digest(expected_state, state):
        logger.warning("ModelScope OAuth state mismatch: expected=%s got=%s; continuing because ModelScope may not echo state", expected_state, state)
    try:
        async with httpx.AsyncClient(timeout=_oauth_timeout()) as client:
            token_resp = await client.post(
                settings.MODELSCOPE_TOKEN_URL,
                data={
                    "grant_type": "authorization_code",
                    "client_id": settings.MODELSCOPE_CLIENT_ID,
                    "client_secret": settings.MODELSCOPE_CLIENT_SECRET,
                    "code": code,
                    "redirect_uri": _oauth_redirect_uri(request, "modelscope"),
                },
            )
            try:
                token_data = token_resp.json()
            except Exception:
                logger.error("ModelScope token response is not JSON: status=%s body=%s", token_resp.status_code, token_resp.text[:500])
                raise HTTPException(status_code=400, detail="ModelScope OAuth token exchange failed")
            token = token_data.get("access_token")
            if not token:
                raise HTTPException(status_code=400, detail="ModelScope OAuth token exchange failed")
            try:
                profile_resp = await client.get(settings.MODELSCOPE_USERINFO_URL, headers={"Authorization": f"Bearer {token}"})
            except httpx.RequestError as e:
                logger.warning("ModelScope userinfo request failed: %s", e)
                profile_resp = None
    except httpx.RequestError as e:
        logger.error("ModelScope OAuth token request failed: %s", e)
        raise HTTPException(status_code=502, detail="ModelScope OAuth token request failed")
    except HTTPException:
        raise
    try:
        profile = profile_resp.json() if profile_resp else {}
    except Exception:
        logger.warning("ModelScope userinfo response is not JSON: status=%s body=%s", profile_resp.status_code, profile_resp.text[:500])
        profile = {}
    if not profile and token_data.get("id_token"):
        try:
            payload = token_data["id_token"].split(".")[1]
            payload += "=" * (-len(payload) % 4)
            profile = json.loads(base64.urlsafe_b64decode(payload))
        except Exception as e:
            logger.warning("Failed to decode ModelScope id_token: %s", e)
    provider_id = str(profile.get("id") or profile.get("sub") or profile.get("username") or profile.get("email") or token_data.get("uid") or token[:12])
    email = profile.get("email") or f"{provider_id}@modelscope.local"
    user = get_or_create_user("modelscope", provider_id, email, profile.get("name") or profile.get("username") or provider_id, profile.get("avatar_url") or profile.get("avatar") or "")
    if user.get("_created"):
        from .telemetry import report_user_registered_event

        await report_user_registered_event(user)
    request.session["user_id"] = user["id"]
    return RedirectResponse("/")


@app.get("/auth/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/")


@app.post("/auth/workshop/login")
async def workshop_login(request: Request):
    if not settings.WORKSHOP_LOGIN_ENABLED:
        raise HTTPException(status_code=404, detail="Workshop login is not enabled")
    payload = await request.json()
    index = _workshop_index_from_email(payload.get("email", ""))
    password = str(payload.get("password", ""))
    if not index or not secrets.compare_digest(password, f"amdyes{index}"):
        raise HTTPException(status_code=401, detail="Invalid workshop credentials")
    user = get_or_create_user("workshop", f"workshop{index}", f"workshop{index}@amd.com", f"WORKSHOP{index}", "")
    user = ensure_user_min_credits(user["id"], settings.WORKSHOP_CREDITS) or user
    request.session.clear()
    request.session["user_id"] = user["id"]
    return {"user": user}


@app.post("/auth/admin/login")
async def admin_login(request: Request):
    if not settings.ADMIN_LOGIN_ENABLED:
        raise HTTPException(status_code=404, detail="Admin login is not enabled")
    payload = await request.json()
    username = str(payload.get("username") or "")
    password = str(payload.get("password") or "")
    if username != "admin" or not secrets.compare_digest(password, settings.ADMIN_PASSWORD):
        raise HTTPException(status_code=401, detail="Invalid admin credentials")
    user = get_or_create_user("admin", "admin", "admin@radeon.local", "Radeon Cloud Admin", "")
    user = ensure_user_min_credits(user["id"], settings.ADMIN_LOGIN_CREDITS) or user
    request.session.clear()
    request.session["user_id"] = user["id"]
    return {"user": user}


@app.get("/api/me")
async def api_me(user: dict = Depends(current_user)):
    return user


@app.post("/api/credits/redeem")
async def redeem_credits(req: CouponRedeemRequest, user: dict = Depends(current_user)):
    if not settings.COUPON_REDEEM_ENABLED:
        raise HTTPException(status_code=503, detail=settings.COUPON_REDEEM_DISABLED_MESSAGE)
    coupon = _decode_credit_coupon(req.coupon)
    try:
        return redeem_user_coupon(user["id"], coupon)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/notebook/request", response_model=NotebookStatus)
async def request_notebook(request: Request, req: NotebookRequest, user: dict = Depends(current_user)):
    """Request a notebook instance"""
    email = user["email"].lower()
    image = req.image or settings.DEFAULT_IMAGE
    instance_type = req.instance_type or "jupyter"
    gpu_count = req.gpu_count or 1
    resource_profile = _validate_resource_profile(req.resource_profile)

    if gpu_count not in [1, 2, 4]:
        raise HTTPException(status_code=400, detail="GPU count must be 1, 2, or 4")

    if not get_image_by_value(image):
        raise HTTPException(status_code=400, detail="Invalid image selected")

    type_cfg = INSTANCE_TYPES.get(instance_type)
    if not type_cfg or not type_cfg.get("enabled"):
        raise HTTPException(status_code=400, detail="Invalid or disabled instance type")

    active = get_active_instance_for_user(user["id"])
    if active:
        if k8s_client.get_instance_by_id(active["instance_id"]):
            raise HTTPException(status_code=400, detail="Each user can only have one active instance")
        mark_instance_deleted(active["instance_id"])

    if int(user["credits"]) < gpu_count:
        raise HTTPException(status_code=400, detail="Insufficient credits")

    try:
        instance_id = f"u-{user['id']}-{hashlib.md5(email.encode()).hexdigest()[:8]}"
        instance = k8s_client.create_instance(
            email, image,
            instance_type=instance_type,
            gpu_count=gpu_count,
            custom_instance_id=instance_id,
            resource_profile=resource_profile,
        )
        record_instance(
            user["id"], email, instance["id"], image, instance_type, gpu_count, instance.get("node_port")
        )
        record_instance_launch_event(user["id"], email, instance["id"], image, instance_type, gpu_count)
        from .telemetry import report_gpu_instance_created_event

        await report_gpu_instance_created_event(
            instance_id=instance["id"],
            user_id=user["id"],
            instance_type=instance_type,
            gpu_count=gpu_count,
        )

        return NotebookStatus(
            status="allocating",
            message="Allocating resources for your instance...",
            url=None,
            email=email
        )

    except Exception as e:
        logger.error(f"Error creating instance for {email}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/notebook/status", response_model=NotebookStatus)
async def check_status(request: Request, email: Optional[str] = Query(None, description="User email")):
    """Check the status of a notebook instance"""
    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Login required")
    user = get_user(int(user_id))
    if not user:
        raise HTTPException(status_code=401, detail="Login required")
    email = user["email"].lower()
    
    try:
        active = get_active_instance_for_user(user["id"])
        if not active:
            return NotebookStatus(
                status="not_found",
                message="No notebook instance found for this user",
                email=email
            )
        instance = k8s_client.get_instance_by_id(active["instance_id"])
        
        if not instance:
            mark_instance_deleted(active["instance_id"])
            return NotebookStatus(
                status="not_found",
                message="No notebook instance found for this user",
                email=email
            )
        
        status_details = k8s_client.get_pod_status_details(email, instance_id=active["instance_id"]) or {}
        status = status_details.get("status")
        if status == "ready" and active.get("status") != "running":
            active = mark_instance_ready_for_billing(active["instance_id"]) or active
        
        api_base_url, api_key = _instance_api_info(request, instance, status)
        return NotebookStatus(
            status=status or "unknown",
            message=_notebook_status_message(status_details),
            url=_ready_instance_url(request, instance, status),
            email=email,
            instance_id=active["instance_id"],
            phase=status_details.get("phase"),
            reason=status_details.get("reason"),
            detail=status_details.get("message"),
            ready=bool(status_details.get("ready")),
            instance_type=(instance.get("instance_type") if instance else None),
            app_port=(instance.get("app_port") if instance else None),
            api_base_url=api_base_url,
            api_key=api_key,
        )
        
    except Exception as e:
        logger.error(f"Error checking status for {email}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/notebook/current", response_model=DestroyResponse)
async def destroy_current_notebook(user: dict = Depends(current_user)):
    """Destroy the current user's active notebook instance."""
    active = get_active_instance_for_user(user["id"])
    if not active:
        return DestroyResponse(
            success=False,
            message="No active instance found",
            destroyed_count=0,
        )

    instance_id = active["instance_id"]
    try:
        deleted = k8s_client.delete_instance_by_id(instance_id)
        mark_instance_deleted(instance_id)
        return DestroyResponse(
            success=True,
            message=f"Instance {instance_id} {'destroyed' if deleted else 'marked deleted'}",
            destroyed_count=1 if deleted else 0,
        )
    except Exception as e:
        logger.error(f"Error destroying current user instance {instance_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Notebook Template Endpoints
# =============================================================================

@app.get("/api/templates")
async def list_public_templates():
    return {"templates": list_notebook_templates(enabled_only=True)}


@app.get("/api/profile/templates")
@app.get("/api/editor/templates")
async def profile_list_templates(user: dict = Depends(current_user)):
    return {
        "templates": list_notebook_templates(enabled_only=False, owner_user_id=user["id"]),
        "images": list_images(enabled_only=True),
    }


@app.post("/api/profile/templates")
@app.post("/api/editor/templates")
async def profile_create_template(req: NotebookTemplateRequest, user: dict = Depends(current_user)):
    try:
        return _save_notebook_template(req, owner_user_id=user["id"], sort_order=0, enabled_override=bool(user.get("is_editor")))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/api/profile/templates/{template_id}")
@app.put("/api/editor/templates/{template_id}")
async def profile_update_template(template_id: int, req: NotebookTemplateRequest, user: dict = Depends(current_user)):
    if not get_notebook_template(template_id, owner_user_id=user["id"]):
        raise HTTPException(status_code=404, detail="Template not found")
    try:
        return _save_notebook_template(req, template_id=template_id, owner_user_id=user["id"], sort_order=0, enabled_override=req.enabled if user.get("is_editor") else False)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/profile/templates/{template_id}")
@app.delete("/api/editor/templates/{template_id}")
async def profile_delete_template(template_id: int, user: dict = Depends(current_user)):
    if not delete_notebook_template(template_id, owner_user_id=user["id"]):
        raise HTTPException(status_code=404, detail="Template not found")
    return {"success": True}


@app.get("/api/templates/{template_id}/preview-status")
async def template_preview_status(request: Request, template_id: int):
    template = _template_accessible_to_user(template_id, session_user(request))
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    if not template.get("repo_url") or not template.get("notebook_path"):
        return {"template_id": template_id, "preview": {"template_id": template_id, "status": "image_only"}}
    cache = get_template_preview_cache(template_id)
    if not cache or cache.get("source_fingerprint") != template_preview_fingerprint(template):
        cache = ensure_template_preview_cache(template, force=True)
        _schedule_template_preview_sync(template_id, force=True)
    return {"template_id": template_id, "preview": _preview_cache_public(cache)}


@app.post("/api/profile/templates/{template_id}/sync-preview")
async def profile_sync_template_preview(template_id: int, user: dict = Depends(current_user)):
    template = get_notebook_template(template_id, owner_user_id=user["id"])
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    if not template.get("repo_url") or not template.get("notebook_path"):
        return {"template_id": template_id, "preview": {"template_id": template_id, "status": "image_only"}}
    cache = ensure_template_preview_cache(template, force=True)
    _schedule_template_preview_sync(template_id, force=True)
    return {"template_id": template_id, "preview": _preview_cache_public(cache)}


@app.get("/templates/{template_id}/preview", response_class=HTMLResponse)
async def preview_notebook_template(request: Request, template_id: int):
    user = session_user(request)
    template = _template_accessible_to_user(template_id, user)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    if not template.get("repo_url") or not template.get("notebook_path"):
        return templates.TemplateResponse(
            request,
            "notebook_preview_status.html",
            {
                "template_json": json.dumps(template),
                "preview_json": json.dumps({"template_id": template_id, "status": "image_only"}),
                "can_retry": False,
            },
            status_code=200,
        )

    cache = get_template_preview_cache(template_id)
    if not cache or cache.get("source_fingerprint") != template_preview_fingerprint(template):
        cache = ensure_template_preview_cache(template, force=True)
        _schedule_template_preview_sync(template_id, force=True)

    notebook = None
    if cache and cache.get("notebook_json") and cache.get("status") in {"ready", "stale"}:
        try:
            notebook = json.loads(cache["notebook_json"])
        except Exception as e:
            logger.error("Cached notebook template %s is invalid JSON: %s", template_id, e)

    if notebook is None:
        return templates.TemplateResponse(
            request,
            "notebook_preview_status.html",
            {
                "template_json": json.dumps(template),
                "preview_json": json.dumps(cache or {}),
                "can_retry": _can_manage_template(template, user),
            },
            status_code=200,
        )

    return templates.TemplateResponse(
        request,
        "notebook_preview.html",
        {
            "template_json": json.dumps(template),
            "notebook_json": json.dumps(notebook),
            "asset_base_url": _template_asset_base_path(template_id, template["notebook_path"]),
            "preview_json": json.dumps(cache or {}),
        },
    )


@app.get("/templates/{template_id}/assets/{asset_path:path}")
async def preview_notebook_template_asset(request: Request, template_id: int, asset_path: str):
    template = _template_accessible_to_user(template_id, session_user(request))
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    if not template.get("repo_url") or not template.get("notebook_path"):
        raise HTTPException(status_code=404, detail="Image-only templates do not have preview assets")

    asset = get_template_preview_asset(template_id, asset_path)
    if asset:
        return Response(
            content=asset["content"],
            media_type=asset.get("content_type") or "application/octet-stream",
            headers={"Cache-Control": "public, max-age=300"},
        )
    _schedule_template_preview_sync(template_id, force=True)
    raise HTTPException(status_code=404, detail="Template asset not found")


@app.post("/api/templates/{template_id}/launch", response_model=NotebookStatus)
async def launch_notebook_template(template_id: int, request: Request, req: TemplateLaunchRequest, user: dict = Depends(current_user)):
    template = _template_accessible_to_user(template_id, user)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")

    gpu_count = req.gpu_count or 1
    if gpu_count not in [1, 2, 4]:
        raise HTTPException(status_code=400, detail="GPU count must be 1, 2, or 4")
    if not get_image_by_value(template["image"]):
        raise HTTPException(status_code=400, detail="Template image is disabled or not in the image catalog")

    active = get_active_instance_for_user(user["id"])
    if active:
        if k8s_client.get_instance_by_id(active["instance_id"]):
            raise HTTPException(status_code=400, detail="Each user can only have one active instance")
        mark_instance_deleted(active["instance_id"])

    if int(user["credits"]) < gpu_count:
        raise HTTPException(status_code=400, detail="Insufficient credits")

    email = user["email"].lower()
    try:
        github_info = _template_github_info(template) if template.get("repo_url") and template.get("notebook_path") else None
        template_instance_type = (template.get("instance_type") or "").strip() or "opencode"
        instance_id = f"u-{user['id']}-{hashlib.md5(email.encode()).hexdigest()[:8]}"
        instance = k8s_client.create_instance(
            email,
            template["image"],
            instance_type=template_instance_type,
            gpu_count=gpu_count,
            github_info=github_info,
            custom_instance_id=instance_id,
            resource_profile="auto",
            start_command=template.get("start_command"),
            app_port=template.get("app_port"),
        )
        record_instance(user["id"], email, instance["id"], template["image"], template_instance_type, gpu_count, instance.get("node_port"))
        record_instance_launch_event(
            user["id"],
            email,
            instance["id"],
            template["image"],
            template_instance_type,
            gpu_count,
            template_id=template["id"],
            template_title=template["title"],
        )
        from .telemetry import report_gpu_instance_created_event

        await report_gpu_instance_created_event(
            instance_id=instance["id"],
            user_id=user["id"],
            instance_type=template_instance_type,
            gpu_count=gpu_count,
            template_id=template["id"],
        )
        return NotebookStatus(
            status="allocating",
            message="Allocating resources for your notebook template...",
            url=None,
            email=email,
            instance_id=instance["id"],
        )
    except Exception as e:
        logger.error("Error launching template %s for %s: %s", template_id, email, e)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# GitHub Notebook Endpoints
# =============================================================================

def _generate_github_instance_id(org: str, repo: str, path: str, user_session: str = "") -> str:
    """Generate a unique instance ID from GitHub path and user session"""
    key = f"{org}/{repo}/{path}/{user_session}".lower()
    hash_str = hashlib.md5(key.encode()).hexdigest()[:8]
    return f"gh-{hash_str}"


def _generate_user_session() -> str:
    """Generate a random user session ID"""
    import uuid
    return uuid.uuid4().hex[:12]


def _parse_github_path(full_path: str) -> dict:
    """Parse GitHub path like org/repo/blob/branch/path/to/notebook.ipynb"""
    parts = full_path.split("/")
    if len(parts) < 5:
        raise ValueError("Invalid GitHub path format")
    
    org = parts[0]
    repo = parts[1]
    # parts[2] should be 'blob'
    branch = parts[3]
    path = "/".join(parts[4:])
    
    # Construct raw GitHub URL
    raw_url = f"https://raw.githubusercontent.com/{org}/{repo}/{branch}/{path}"
    
    return {
        "org": org,
        "repo": repo,
        "branch": branch,
        "path": path,
        "raw_url": raw_url
    }


@app.get("/github/{full_path:path}", response_class=HTMLResponse)
async def github_notebook(
    request: Request,
    full_path: str,
    response: Response,
    instance_id: Optional[str] = Cookie(None, alias="amd_oneclick_gh_instance"),
    user_session: Optional[str] = Cookie(None, alias="amd_oneclick_session")
):
    """Handle GitHub notebook request"""
    try:
        github_info = _parse_github_path(full_path)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    # Generate or use existing user session
    if not user_session:
        user_session = _generate_user_session()
    
    # Generate instance ID from GitHub path + user session
    generated_instance_id = _generate_github_instance_id(
        github_info["org"], 
        github_info["repo"], 
        github_info["path"],
        user_session
    )
    
    # Check if user already has an instance for this notebook (via cookie)
    if instance_id == generated_instance_id:
        # Check if instance exists and is ready
        existing = k8s_client.get_instance_by_id(generated_instance_id)
        if existing:
            status = k8s_client.get_pod_status("", instance_id=generated_instance_id)
            if status == "ready":
                # Redirect directly to notebook
                return RedirectResponse(url=existing["url"], status_code=302)
    
    # Set user session cookie if new
    resp = templates.TemplateResponse(
        request,
        "github_landing.html",
        {
            "github_org": github_info["org"],
            "github_repo": github_info["repo"],
            "github_path": github_info["path"],
            "github_branch": github_info["branch"],
            "instance_id": generated_instance_id,
            "full_path": full_path,
            "user_session": user_session,
        },
    )
    
    # Set session cookie for this user
    resp.set_cookie(
        key="amd_oneclick_session",
        value=user_session,
        max_age=86400 * 30,  # 30 days
        httponly=True
    )
    
    return resp


@app.post("/api/github/notebook/create")
async def create_github_notebook(
    request: Request,
    response: Response,
    org: str = Query(...),
    repo: str = Query(...),
    branch: str = Query(...),
    path: str = Query(...),
    user_session: Optional[str] = Cookie(None, alias="amd_oneclick_session")
):
    """Create a notebook instance for a GitHub notebook"""
    github_info = {
        "org": org,
        "repo": repo,
        "branch": branch,
        "path": path,
        "raw_url": f"https://raw.githubusercontent.com/{org}/{repo}/{branch}/{path}"
    }
    
    # Use user session to generate unique instance ID per user
    if not user_session:
        user_session = _generate_user_session()
    
    instance_id = _generate_github_instance_id(org, repo, path, user_session)
    
    # Check if instance already exists for THIS user
    existing = k8s_client.get_instance_by_id(instance_id)
    if existing:
        response.set_cookie(
            key="amd_oneclick_gh_instance",
            value=instance_id,
            max_age=86400 * 7,  # 7 days
            httponly=True
        )
        return NotebookStatus(
            status="exists",
            message="Instance already exists",
            url=None,
            instance_id=instance_id
        )
    
    try:
        # Use a placeholder email for GitHub notebooks
        email = f"github-{instance_id}@oneclick.local"
        
        instance = k8s_client.create_instance(
            email=email,
            image=settings.DEFAULT_IMAGE,
            github_info=github_info,
            custom_instance_id=instance_id
        )
        
        # Set cookie to remember this instance
        response.set_cookie(
            key="amd_oneclick_gh_instance",
            value=instance_id,
            max_age=86400 * 7,  # 7 days
            httponly=True
        )
        
        return NotebookStatus(
            status="allocating",
            message="Allocating resources for your notebook...",
            url=None,
            instance_id=instance_id
        )
        
    except Exception as e:
        logger.error(f"Error creating GitHub notebook: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/github/notebook/status")
async def check_github_status(instance_id: str = Query(...)):
    """Check the status of a GitHub notebook instance"""
    try:
        instance = k8s_client.get_instance_by_id(instance_id)
        
        if not instance:
            return NotebookStatus(
                status="not_found",
                message="No notebook instance found",
                instance_id=instance_id
            )
        
        status_details = k8s_client.get_pod_status_details("", instance_id=instance_id) or {}
        status = status_details.get("status")
        
        # Normalize status for frontend - 'ready' means 'running' and ready to use
        ready = status == "ready"
        if status == "ready":
            status = "running"
        
        return NotebookStatus(
            status=status or "unknown",
            message="Your notebook is ready!" if ready else _notebook_status_message(status_details),
            url=instance.get("url") if ready else None,
            instance_id=instance_id,
            phase=status_details.get("phase"),
            reason=status_details.get("reason"),
            detail=status_details.get("message"),
            ready=ready,
        )
        
    except Exception as e:
        logger.error(f"Error checking GitHub status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Instance Path Proxy
# =============================================================================

@app.api_route("/instances/{instance_id}/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def proxy_instance_http(instance_id: str, path: str, request: Request):
    """Proxy HTTP traffic to a Jupyter instance using its path-based base_url."""
    target_base = _instance_service_base(instance_id)
    target_url = f"{target_base}/instances/{instance_id}/{path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    body = await request.body()
    timeout = httpx.Timeout(3600.0, connect=10.0)
    client = httpx.AsyncClient(timeout=timeout, follow_redirects=False)
    upstream = await client.send(
        client.build_request(
            request.method,
            target_url,
            headers=_proxy_headers(request.headers),
            content=body,
        ),
        stream=True,
    )

    response_headers = {
        k: v for k, v in upstream.headers.items()
        if k.lower() not in {"content-encoding", "transfer-encoding", "connection", "content-length", "set-cookie"}
    }
    if "location" in response_headers:
        response_headers["location"] = _rewrite_location(response_headers["location"], instance_id, target_base)
    response = StreamingResponse(
        upstream.aiter_raw(),
        status_code=upstream.status_code,
        headers=response_headers,
        background=BackgroundTask(_close_httpx_stream, upstream, client),
    )
    for cookie in upstream.headers.get_list("set-cookie"):
        response.raw_headers.append((b"set-cookie", cookie.encode("latin-1")))
    return response


@app.websocket("/instances/{instance_id}/{path:path}")
async def proxy_instance_websocket(websocket: WebSocket, instance_id: str, path: str):
    """Proxy WebSocket traffic for Jupyter terminals/kernels under /instances/<id>/."""
    await websocket.accept()
    try:
        target_base = _instance_service_base(instance_id).replace("http://", "ws://")
        target_url = f"{target_base}/instances/{instance_id}/{path}"
        if websocket.url.query:
            target_url += f"?{websocket.url.query}"

        headers = []
        if websocket.headers.get("cookie"):
            headers.append(("cookie", websocket.headers["cookie"]))

        async with websockets.connect(target_url, additional_headers=headers, open_timeout=10, max_size=16 * 1024 * 1024, max_queue=4) as upstream:
            async def client_to_upstream():
                while True:
                    msg = await websocket.receive()
                    if msg.get("type") == "websocket.disconnect":
                        await upstream.close()
                        break
                    if msg.get("text") is not None:
                        await upstream.send(msg["text"])
                    elif msg.get("bytes") is not None:
                        await upstream.send(msg["bytes"])

            async def upstream_to_client():
                async for msg in upstream:
                    if isinstance(msg, bytes):
                        await websocket.send_bytes(msg)
                    else:
                        await websocket.send_text(msg)

            await asyncio.gather(client_to_upstream(), upstream_to_client())
    except WebSocketDisconnect:
        return
    except Exception as e:
        logger.error(f"WebSocket proxy failed for {instance_id}/{path}: {e}")
        try:
            await websocket.close(code=1011)
        except Exception:
            pass


# =============================================================================
# Spaces app-port proxy (user apps like Gradio/Streamlit on curated ports)
# =============================================================================

def _instance_pod_ip(instance_id: str) -> str:
    try:
        pod = k8s_client.core_v1.read_namespaced_pod(name=instance_id, namespace=k8s_client.namespace)
    except Exception:
        raise HTTPException(status_code=404, detail="Instance pod not found")
    ip = pod.status.pod_ip if pod and pod.status else None
    if not ip:
        raise HTTPException(status_code=503, detail="Instance is still starting")
    return ip


def _validate_app_port(port: str) -> int:
    try:
        p = int(port)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid app port")
    if p not in set(settings.APP_PORTS.values()):
        raise HTTPException(status_code=403, detail="App port is not in the allowed set")
    return p


def _app_port_proxy_mode(app_port: int) -> str:
    """preserve = keep /spaces/<id>/<port> prefix (base-path-aware apps like Gradio);
    strip = remove the prefix before forwarding (apps like ComfyUI that can't run
    under a sub-path)."""
    from .config import APP_FRAMEWORK_PRESETS
    for name, preset in APP_FRAMEWORK_PRESETS.items():
        if int(preset.get("port") or 0) == app_port:
            return preset.get("proxy_mode", "preserve")
    return "preserve"


def _space_upstream_path(request_or_ws, instance_id: str, app_port: int, mode: str) -> str:
    """Compute the upstream path. For strip mode use the raw (still-encoded) path
    so %2F is preserved (ComfyUI workflow saves depend on this)."""
    prefix = f"{settings.SPACES_PATH_PREFIX}/{instance_id}/{app_port}"
    if mode == "strip":
        raw = request_or_ws.scope.get("raw_path") or request_or_ws.url.path.encode()
        raw_str = raw.decode("latin-1") if isinstance(raw, (bytes, bytearray)) else str(raw)
        stripped = raw_str[len(prefix):] if raw_str.startswith(prefix) else raw_str
        if not stripped.startswith("/"):
            stripped = "/" + stripped
        return stripped
    return request_or_ws.url.path


@app.get(f"{settings.SPACES_PATH_PREFIX}/{{instance_id}}/{{port}}")
async def proxy_space_root_redirect(instance_id: str, port: str):
    _validate_app_port(port)
    return RedirectResponse(url=f"{settings.SPACES_PATH_PREFIX}/{instance_id}/{port}/")


@app.api_route(f"{settings.SPACES_PATH_PREFIX}/{{instance_id}}/{{port}}/{{path:path}}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def proxy_space_http(instance_id: str, port: str, path: str, request: Request):
    """Proxy HTTP traffic to a user app on a curated port. Path is preserved so a
    base-path-aware app (Gradio root_path / Streamlit baseUrlPath) resolves correctly."""
    app_port = _validate_app_port(port)
    mode = _app_port_proxy_mode(app_port)
    target_base = f"http://{_instance_pod_ip(instance_id)}:{app_port}"
    upstream_path = _space_upstream_path(request, instance_id, app_port, mode)
    target_url = f"{target_base}{upstream_path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    # Preserve the public Host + forwarded info so apps (e.g. Gradio) build
    # correct external URLs instead of using the internal pod IP they receive.
    fwd_headers = _proxy_headers(request.headers)
    public_host = request.headers.get("host") or request.url.netloc
    fwd_headers["host"] = public_host
    fwd_headers["X-Forwarded-Host"] = public_host
    fwd_headers["X-Forwarded-Proto"] = request.headers.get("x-forwarded-proto", request.url.scheme)
    fwd_headers["X-Forwarded-Prefix"] = f"{settings.SPACES_PATH_PREFIX}/{instance_id}/{app_port}"

    body = await request.body()
    timeout = httpx.Timeout(3600.0, connect=10.0)
    client = httpx.AsyncClient(timeout=timeout, follow_redirects=False)
    upstream = await client.send(
        client.build_request(
            request.method,
            target_url,
            headers=fwd_headers,
            content=body,
        ),
        stream=True,
    )
    # Keep content-encoding (we stream raw/compressed bytes); drop only hop-by-hop
    # and length/cookie headers we re-add separately.
    response_headers = {
        k: v for k, v in upstream.headers.items()
        if k.lower() not in {"transfer-encoding", "connection", "content-length", "set-cookie"}
    }
    response = StreamingResponse(
        upstream.aiter_raw(),
        status_code=upstream.status_code,
        headers=response_headers,
        background=BackgroundTask(_close_httpx_stream, upstream, client),
    )
    for cookie in upstream.headers.get_list("set-cookie"):
        response.raw_headers.append((b"set-cookie", cookie.encode("latin-1")))
    return response


@app.websocket(f"{settings.SPACES_PATH_PREFIX}/{{instance_id}}/{{port}}/{{path:path}}")
async def proxy_space_websocket(websocket: WebSocket, instance_id: str, port: str, path: str):
    """Proxy WebSocket traffic for user apps (Gradio/Streamlit live updates)."""
    await websocket.accept()
    try:
        app_port = _validate_app_port(port)
        mode = _app_port_proxy_mode(app_port)
        target_base = f"http://{_instance_pod_ip(instance_id)}:{app_port}".replace("http://", "ws://")
        upstream_path = _space_upstream_path(websocket, instance_id, app_port, mode)
        target_url = f"{target_base}{upstream_path}"
        if websocket.url.query:
            target_url += f"?{websocket.url.query}"

        headers = []
        if websocket.headers.get("cookie"):
            headers.append(("cookie", websocket.headers["cookie"]))

        async with websockets.connect(target_url, additional_headers=headers, open_timeout=10, max_size=16 * 1024 * 1024, max_queue=4) as upstream:
            async def client_to_upstream():
                while True:
                    msg = await websocket.receive()
                    if msg.get("type") == "websocket.disconnect":
                        await upstream.close()
                        break
                    if msg.get("text") is not None:
                        await upstream.send(msg["text"])
                    elif msg.get("bytes") is not None:
                        await upstream.send(msg["bytes"])

            async def upstream_to_client():
                async for msg in upstream:
                    if isinstance(msg, bytes):
                        await websocket.send_bytes(msg)
                    else:
                        await websocket.send_text(msg)

            await asyncio.gather(client_to_upstream(), upstream_to_client())
    except WebSocketDisconnect:
        return
    except Exception as e:
        logger.error(f"Space WS proxy failed for {instance_id}:{port}/{path}: {e}")
        try:
            await websocket.close(code=1011)
        except Exception:
            pass


# =============================================================================
# Admin Endpoints
# =============================================================================

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request, username: str = Depends(verify_admin)):
    """Render the admin management page"""
    return templates.TemplateResponse(
        request,
        "admin.html",
        {"username": username},
    )


@app.get("/api/admin/instances", response_model=AdminListResponse)
async def list_instances(username: str = Depends(verify_admin)):
    """List all notebook instances"""
    try:
        instances = k8s_client.list_instances()
        
        items = [
            NotebookListItem(
                id=inst["id"],
                email=inst["email"],
                pod_name=inst["pod_name"],
                url=inst.get("url") or "",
                status=inst["status"],
                created_at=inst.get("created_at", ""),
                last_activity=inst.get("last_activity"),
                uptime_minutes=inst.get("uptime_minutes", 0),
                instance_type=inst.get("instance_type", "jupyter"),
                gpu_count=inst.get("gpu_count", 1),
                github_org=inst.get("github_org"),
                github_repo=inst.get("github_repo"),
                github_path=inst.get("github_path")
            )
            for inst in instances
        ]
        
        return AdminListResponse(
            instances=items,
            total_count=len(items)
        )
        
    except Exception as e:
        logger.error(f"Error listing instances: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/users")
async def admin_list_users(username: str = Depends(verify_admin)):
    return {"users": list_users()}


@app.get("/api/admin/stats")
async def admin_stats(username: str = Depends(verify_admin)):
    return get_admin_daily_stats()


@app.post("/api/admin/users/{user_id}/credits")
async def admin_grant_credits(user_id: int, req: CreditGrantRequest, username: str = Depends(verify_admin)):
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be positive")

    user = grant_user_credits(user_id, req.amount, req.reason or "manual admin grant")
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {"user": user}


@app.get("/api/admin/templates")
async def admin_list_templates(username: str = Depends(verify_admin)):
    return {"templates": list_notebook_templates(enabled_only=False)}


@app.post("/api/admin/templates")
async def admin_create_template(req: NotebookTemplateRequest, username: str = Depends(verify_admin)):
    try:
        return _save_notebook_template(req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/api/admin/templates/{template_id}")
async def admin_update_template(template_id: int, req: NotebookTemplateRequest, username: str = Depends(verify_admin)):
    if not get_notebook_template(template_id):
        raise HTTPException(status_code=404, detail="Template not found")
    try:
        return _save_notebook_template(req, template_id=template_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/admin/templates/{template_id}")
async def admin_delete_template(template_id: int, username: str = Depends(verify_admin)):
    if not delete_notebook_template(template_id):
        raise HTTPException(status_code=404, detail="Template not found")
    return {"success": True}


@app.post("/api/admin/templates/{template_id}/sync-preview")
async def admin_sync_template_preview(template_id: int, username: str = Depends(verify_admin)):
    template = get_notebook_template(template_id, enabled_only=False)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    if not template.get("repo_url") or not template.get("notebook_path"):
        return {"template_id": template_id, "preview": {"template_id": template_id, "status": "image_only"}}
    cache = ensure_template_preview_cache(template, force=True)
    _schedule_template_preview_sync(template_id, force=True)
    return {"template_id": template_id, "preview": _preview_cache_public(cache)}


@app.get("/api/admin/images")
async def admin_list_images(username: str = Depends(verify_admin)):
    for image in list_images(enabled_only=False):
        sync = k8s_client.get_image_sync_status(image["id"])
        if (
            image.get("sync_status") != sync["status"]
            or image.get("desired_count") != sync["desired_count"]
            or image.get("ready_count") != sync["ready_count"]
            or image.get("sync_message") != sync["message"]
        ):
            update_image_sync_status(
                image["id"],
                sync["status"],
                sync["desired_count"],
                sync["ready_count"],
                sync["message"],
                sync["completed"],
            )
    return {"images": list_images(enabled_only=False)}


@app.post("/api/admin/images")
async def admin_create_image(req: ImageRequest, username: str = Depends(verify_admin)):
    image = upsert_image(req.name, req.image, req.description or "", req.enabled)
    sync = k8s_client.sync_image_to_nodes(image["id"], image["image"])
    return update_image_sync_status(
        image["id"],
        sync["status"],
        sync["desired_count"],
        sync["ready_count"],
        sync["message"],
        sync["completed"],
    )


@app.put("/api/admin/images/{image_id}")
async def admin_update_image(image_id: int, req: ImageRequest, username: str = Depends(verify_admin)):
    image = upsert_image(req.name, req.image, req.description or "", req.enabled, image_id=image_id)
    if not image:
        raise HTTPException(status_code=404, detail="Image not found")
    sync = k8s_client.sync_image_to_nodes(image["id"], image["image"])
    return update_image_sync_status(
        image["id"],
        sync["status"],
        sync["desired_count"],
        sync["ready_count"],
        sync["message"],
        sync["completed"],
    )


@app.post("/api/admin/images/{image_id}/sync")
async def admin_sync_image(image_id: int, username: str = Depends(verify_admin)):
    image = next((img for img in list_images(enabled_only=False) if img["id"] == image_id), None)
    if not image:
        raise HTTPException(status_code=404, detail="Image not found")
    sync = k8s_client.sync_image_to_nodes(image["id"], image["image"])
    return update_image_sync_status(
        image["id"],
        sync["status"],
        sync["desired_count"],
        sync["ready_count"],
        sync["message"],
        sync["completed"],
    )


@app.delete("/api/admin/images/{image_id}")
async def admin_delete_image(image_id: int, username: str = Depends(verify_admin)):
    k8s_client.delete_image_sync(image_id)
    if not delete_image(image_id):
        raise HTTPException(status_code=404, detail="Image not found")
    return {"success": True}


@app.delete("/api/admin/instance/{instance_id}", response_model=DestroyResponse)
async def destroy_instance(instance_id: str, username: str = Depends(verify_admin)):
    """Destroy a specific notebook instance by ID"""
    try:
        success = k8s_client.delete_instance_by_id(instance_id)
        if success:
            mark_instance_deleted(instance_id)
        
        return DestroyResponse(
            success=success,
            message=f"Instance {instance_id} {'destroyed' if success else 'not found'}",
            destroyed_count=1 if success else 0
        )
        
    except Exception as e:
        logger.error(f"Error destroying instance {instance_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/admin/instances/all", response_model=DestroyResponse)
async def destroy_all_instances(username: str = Depends(verify_admin)):
    """Destroy all notebook instances"""
    try:
        count = k8s_client.delete_all_instances()
        for inst in k8s_client.list_instances():
            mark_instance_deleted(inst["id"])
        
        return DestroyResponse(
            success=True,
            message=f"Destroyed {count} instances",
            destroyed_count=count
        )
        
    except Exception as e:
        logger.error(f"Error destroying all instances: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/instances/bulk-destroy")
async def bulk_destroy_instances(req: InstanceBulkDestroyRequest, username: str = Depends(verify_admin)):
    """Destroy running notebook instances whose email matches the input, or all instances for ALL."""
    matcher = (req.matcher or "").strip()
    if not matcher:
        raise HTTPException(status_code=400, detail="matcher is required")

    destroy_all = matcher == "ALL"
    needle = matcher.lower()
    instances = k8s_client.list_instances()
    matched = [
        inst for inst in instances
        if destroy_all or needle in (inst.get("email") or "").lower()
    ]
    destroyed = []
    failed = []
    for inst in matched:
        instance_id = inst.get("id")
        if not instance_id:
            continue
        try:
            if k8s_client.delete_instance_by_id(instance_id):
                mark_instance_deleted(instance_id)
                destroyed.append({"id": instance_id, "email": inst.get("email")})
            else:
                failed.append({"id": instance_id, "email": inst.get("email"), "reason": "not found"})
        except Exception as e:
            logger.error("Bulk destroy failed for %s: %s", instance_id, e)
            failed.append({"id": instance_id, "email": inst.get("email"), "reason": str(e)})

    return {
        "success": not failed,
        "matcher": matcher,
        "matched_count": len(matched),
        "destroyed_count": len(destroyed),
        "failed_count": len(failed),
        "destroyed": destroyed,
        "failed": failed,
    }


@app.post("/api/admin/cleanup")
async def trigger_cleanup(username: str = Depends(verify_admin)):
    """Manually trigger cleanup of idle instances"""
    try:
        cleaned = k8s_client.cleanup_idle_instances()
        
        return {
            "success": True,
            "message": f"Cleaned up {len(cleaned)} instances",
            "cleaned": cleaned
        }
        
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Health Check
# =============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


@app.get("/api/config")
async def get_config():
    """Get public configuration"""
    return {
        "available_images": [img["image"] for img in list_images(enabled_only=True)],
        "default_image": settings.DEFAULT_IMAGE,
        "max_lifetime_hours": settings.MAX_LIFETIME_HOURS,
        "idle_timeout_minutes": settings.IDLE_TIMEOUT_MINUTES,
        "instance_types": INSTANCE_TYPES,
    }
