"""
FastAPI main application for AMD OneClick Notebook Manager
"""
import hashlib
import json
import logging
import os
import re
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
from kubernetes.client.rest import ApiException

from .config import settings, INSTANCE_TYPES, APP_FRAMEWORK_PRESETS
from .redis_client import rate_limit_ok
from .models import (
    NotebookRequest, 
    NotebookStatus, 
    AdminListResponse, 
    NotebookListItem,
    DestroyResponse,
    ImageRequest,
    CreditGrantRequest,
    EditorGrantRequest,
    SshPublicKeyRequest,
    InstanceBulkDestroyRequest,
    CouponRedeemRequest,
    NotebookTemplateRequest,
    TemplateLaunchRequest,
    HuggingFaceNotebookLaunchRequest,
    CustomImageBuildRequest,
    BuildClaimRequest,
    BuildLogRequest,
    BuildResultRequest,
    BuildEvictRequest,
    ImageJobClaimRequest,
    ImageJobLogRequest,
    ImageJobHeartbeatRequest,
    ImageJobResultRequest,
    ImageNodeStatusRequest,
)
from .k8s_client import AUTO_RESOURCE_PROFILE_BY_GPU, RESOURCE_PROFILES, k8s_client
from .notebook_sources import (
    parse_github_path,
    parse_huggingface_demo_git_path,
    parse_huggingface_demo_notebook_path,
    parse_huggingface_notebook_url,
)
from . import store
from .email_service import send_notebook_url_email
from .scheduler import start_scheduler, stop_scheduler
from .template_sync import sync_template_preview
from .store import (
    clear_template_preview_cache,
    delete_image,
    delete_notebook_template,
    get_active_instance_for_user,
    upsert_launch_intent,
    get_launch_intent,
    delete_launch_intent,
    get_admin_daily_stats,
    get_charged_credits_for_instance,
    get_image_by_value,
    resolve_enabled_image,
    get_notebook_template,
    get_or_create_user,
    get_or_create_external_user,
    get_template_preview_asset,
    get_template_preview_cache,
    get_user_by_provider,
    get_user,
    bump_user_token_version,
    ensure_user_min_credits,
    grant_initial_credits_once,
    grant_user_credits,
    ensure_template_preview_cache,
    init_db,
    list_images,
    list_notebook_templates,
    list_users,
    set_user_editor,
    set_user_ssh_public_key,
    mark_instance_deleted,
    mark_instance_ready_for_billing,
    record_instance,
    record_instance_launch_event,
    redeem_user_coupon,
    template_preview_fingerprint,
    upsert_notebook_template,
    update_image_sync_status,
    upsert_image,
    list_custom_images,
    get_custom_image,
    create_custom_image,
    claim_next_build,
    append_custom_image_log,
    update_custom_image_status,
    delete_custom_image,
    get_ready_custom_image_by_value,
    get_custom_image_by_value,
    list_gc_candidates,
    mark_custom_image_evicted,
    mark_custom_image_launched,
    requeue_custom_image_build,
    enqueue_image_job,
    claim_next_image_job,
    append_image_job_log,
    heartbeat_image_job,
    finish_image_job,
    upsert_image_node,
    image_loaded_on_node,
    clear_image_node,
    touch_image_node,
    list_outdated_images,
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
    if settings.OPENCODE_PASSWORD_SECRET == "change-me-for-production":
        logger.warning(
            "OPENCODE_PASSWORD_SECRET is unset and SESSION_SECRET is the insecure default "
            "'change-me-for-production'. OpenCode per-instance passwords are then derivable by "
            "anyone, since the HMAC key is a well-known constant. Set OPENCODE_PASSWORD_SECRET "
            "(or SESSION_SECRET) to a strong server-only value in production."
        )
    init_db()
    # Bootstrap the durable workspace shards (idempotent) when the two-tier localcache workspace
    # is enabled. Best-effort: a storage hiccup must never block manager startup — launches will
    # re-ensure defensively, and the shards can also be created out-of-band.
    if (settings.WORKSPACE_VOLUME_TYPE or "").strip().lower() == "localcache":
        # Run in the background so a slow/degraded NFS backend (each shard can poll up to ~60s to
        # Bound, x5 = up to 5 min) never blocks app startup / readiness probes. Launches defensively
        # re-ensure (create-if-missing) anyway, so serving before shards are confirmed Bound is safe.
        async def _bootstrap_durable_shards():
            try:
                bound = await asyncio.to_thread(k8s_client._ensure_durable_shards)
                logger.info("Durable workspace shards ready: %s", bound)
            except Exception as e:
                logger.error("Durable workspace shard bootstrap failed (continuing): %s", e)
        asyncio.create_task(_bootstrap_durable_shards())
    if settings.RUN_SCHEDULER:
        from .leader import elector
        elector.start()
        start_scheduler()
    else:
        logger.info("Background scheduler disabled for this manager process")
    yield
    # Shutdown
    logger.info("Shutting down AMD OneClick Notebook Manager")
    if settings.RUN_SCHEDULER:
        stop_scheduler()
        from .leader import elector
        elector.stop()


app = FastAPI(
    title="AMD OneClick Notebook Manager",
    description="Kubernetes-based Jupyter Notebook instance management",
    version="1.0.0",
    lifespan=lifespan
)
app.add_middleware(SessionMiddleware, secret_key=settings.SESSION_SECRET)


@app.middleware("http")
async def opencode_origin_proxy(request: Request, call_next):
    """Serve OpenCode through the manager when OPENCODE_PUBLIC_BASE_URL matches this origin."""
    opencode_host = settings.OPENCODE_PUBLIC_HOST
    if not opencode_host:
        return await call_next(request)
    req_host = (request.headers.get("host") or "").split(":", 1)[0].lower()
    if req_host != opencode_host.lower():
        return await call_next(request)
    opencode_port = settings.OPENCODE_PUBLIC_PORT
    if opencode_port is not None:
        try:
            req_port = int(request.headers.get("x-forwarded-port") or 0)
        except (TypeError, ValueError):
            req_port = 0
        if req_port != opencode_port:
            return await call_next(request)
    return await _handle_opencode_request(request)


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
HF_DEMO_PROVIDER = "huggingface_demo"

# Caller-supplied pod tags accepted by the API. Empty/None means untagged interactive.
ALLOWED_POD_TYPES = {"hackathon", "workshop", "one-click"}


def _normalize_pod_type(value: Optional[str]) -> Optional[str]:
    """Validate an optional caller-supplied pod tag; raise 400 if non-empty and unknown."""
    normalized = (value or "").strip().lower()
    if not normalized:
        return None
    if normalized not in ALLOWED_POD_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid pod_type. Allowed: {', '.join(sorted(ALLOWED_POD_TYPES))}",
        )
    return normalized


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


def _session_epoch_valid(request: Request, user: dict) -> bool:
    """A session is only valid while its embedded epoch matches the user's
    current token_version. Bumping token_version (admin revocation) invalidates
    every outstanding cookie for that user."""
    return int(request.session.get("sv", 0)) == int(user.get("token_version", 0))


def current_user(request: Request) -> dict:
    user_id = request.session.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Login required")
    user = get_user(int(user_id))
    if not user or not _session_epoch_valid(request, user):
        request.session.clear()
        raise HTTPException(status_code=401, detail="Login required")
    return user


def session_user(request: Request) -> Optional[dict]:
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    user = get_user(int(user_id))
    if user and not _session_epoch_valid(request, user):
        request.session.clear()
        return None
    return user


def _client_ip(request: Request) -> str:
    """Best-effort client IP, honoring the ingress X-Forwarded-For chain."""
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _enforce_login_rate_limit(request: Request):
    ip = _client_ip(request)
    if not rate_limit_ok(f"login:{ip}", settings.LOGIN_RATE_LIMIT_PER_MINUTE, 60):
        raise HTTPException(status_code=429, detail="Too many login attempts, please retry later")


def _establish_session(request: Request, user: dict):
    """Bind the browser session to a user and stamp the current session epoch."""
    request.session.clear()
    request.session["user_id"] = user["id"]
    request.session["sv"] = int(user.get("token_version", 0))


def _enforce_signup_quota(request: Request, user: dict):
    """When a login just created a new account, cap new accounts per IP per day."""
    if not user.get("_created"):
        return
    ip = _client_ip(request)
    if not rate_limit_ok(f"signup:{ip}", settings.SIGNUP_RATE_LIMIT_PER_DAY, 86400):
        logger.warning("Signup quota exceeded for ip=%s (new user_id=%s)", ip, user.get("id"))
        raise HTTPException(status_code=429, detail="Too many new accounts from this network today")


def verify_huggingface_demo_api(request: Request) -> None:
    tokens = [token.strip() for token in settings.HUGGINGFACE_DEMO_API_TOKENS.split(",") if token.strip()]
    if not tokens:
        raise HTTPException(status_code=503, detail="Hugging Face demo API is not configured")

    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    candidate = token.strip()
    if scheme.lower() != "bearer" or not candidate:
        raise HTTPException(
            status_code=401,
            detail="Bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not any(secrets.compare_digest(candidate, allowed) for allowed in tokens):
        raise HTTPException(
            status_code=401,
            detail="Invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


def _huggingface_demo_user_identity(user_name: str) -> tuple[str, str, str]:
    display_name = (user_name or "").strip()
    provider_id = display_name.lower()
    if not provider_id:
        raise HTTPException(status_code=400, detail="user_name is required")
    if len(provider_id) > 255:
        raise HTTPException(status_code=400, detail="user_name is too long")

    digest = hashlib.sha256(provider_id.encode("utf-8")).hexdigest()[:16]
    email = f"hf-{digest}@huggingface.oneclick.local"
    return provider_id, display_name[:255], email


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


_GITHUB_DEFAULT_BASES = ("https://github.com", "http://github.com")


def _github_is_proxy_base() -> bool:
    """True when GITHUB_WEB_BASE points at a real proxy (not plain github.com)."""
    return settings.GITHUB_WEB_BASE not in _GITHUB_DEFAULT_BASES


def _github_clone_url(repo_url: str) -> str:
    """Clone URL used INSIDE instance pods (git).

    For github.com the pod reaches it via the `github.com -> 36.151.243.83`
    hostAlias; that IP only serves git correctly over plain HTTP (its HTTPS does
    not present a valid github.com cert, so an HTTPS clone fails verification).
    Use HTTP for github.com (the long-standing working behaviour). When a real
    proxy with a valid TLS cert is configured (e.g. gh-test), use it as-is over
    HTTPS — that path is proven on the test manager.
    """
    org, repo = _github_repo_parts(repo_url)
    if _github_is_proxy_base():
        return f"{settings.GITHUB_WEB_BASE}/{org}/{repo}.git"
    return f"http://github.com/{org}/{repo}.git"


def _github_raw_url(repo_url: str, branch: str, notebook_path: str) -> str:
    """Raw download URL used INSIDE instance pods (curl, which has the system CA).

    For github.com keep the original raw.githubusercontent.com (HTTPS) form; for
    a proxy base use its mirrored /raw/ path.
    """
    org, repo = _github_repo_parts(repo_url)
    path = notebook_path.lstrip("/")
    if _github_is_proxy_base():
        return f"{settings.GITHUB_WEB_BASE}/{org}/{repo}/raw/{branch}/{path}"
    return f"https://raw.githubusercontent.com/{org}/{repo}/{branch}/{path}"


def _github_raw_url_candidates(repo_url: str, branch: str, notebook_path: str) -> list[str]:
    org, repo = _github_repo_parts(repo_url)
    path = notebook_path.lstrip("/")
    candidates = [
        f"{settings.GITHUB_WEB_BASE}/{org}/{repo}/raw/{branch}/{path}",
        f"https://raw.githubusercontent.com/{org}/{repo}/{branch}/{path}",
        f"https://github.com/{org}/{repo}/raw/{branch}/{path}",
        f"http://github.com/{org}/{repo}/raw/{branch}/{path}",
    ]
    # De-dupe while preserving order (proxy base may equal github.com by default).
    seen = set()
    return [u for u in candidates if not (u in seen or seen.add(u))]


def _template_asset_base_path(template_id: int, notebook_path: str) -> str:
    parent = notebook_path.lstrip("/").rsplit("/", 1)
    directory = parent[0].strip("/") if len(parent) == 2 else ""
    if directory:
        return f"/templates/{template_id}/assets/{directory}/"
    return f"/templates/{template_id}/assets/"


def _template_github_info(template: dict) -> dict:
    # Any template with a repo_url clones it. A notebook_path (when present) is opened in the
    # launched notebook; without one, JupyterLab opens at the repo root. No repo = image-only.
    if not template.get("repo_url"):
        return {}
    org, repo = _github_repo_parts(template["repo_url"])
    notebook_path = (template.get("notebook_path") or "").lstrip("/")
    return {
        "org": org,
        "repo": repo,
        "branch": template["branch"],
        "path": notebook_path,
        "raw_url": _github_raw_url(template["repo_url"], template["branch"], template["notebook_path"]) if notebook_path else "",
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
    is_catalog_image = bool(get_image_by_value(req.image))
    if not is_catalog_image and not (owner_user_id and get_ready_custom_image_by_value(owner_user_id, req.image)):
        raise ValueError("Template image must be an enabled catalog image or one of your build-ready custom images")
    # A template backed by a user custom image must never be publicly listed, even when an
    # editor creates it: the image is private to its owner, so other users would see a gallery
    # entry they can never launch. Force it Profile-only until the image is promoted to a
    # global catalog image. Catalog-image templates keep whatever enabled value was requested.
    if not is_catalog_image:
        enabled_override = False
    has_repo = bool((req.repo_url or "").strip())
    has_notebook = bool((req.notebook_path or "").strip())
    _req_itype = (req.instance_type or "").strip()
    _is_app_type = _req_itype in APP_FRAMEWORK_PRESETS
    # A repo is always optional (no repo = image-only / blank workspace). A notebook path,
    # however, needs a repo to clone it from. App types never use a notebook path.
    if not _is_app_type and has_notebook and not has_repo:
        raise ValueError("A notebook path needs a GitHub repo URL to clone from; provide a repo URL, or leave both empty for an image-only template")
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
        model_source=(req.model_source or "").strip().lower() or None,
        ssh_enabled=bool(getattr(req, "ssh_enabled", False)),
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


def _fetch_api_model(instance: dict, port: int, suffix: str) -> Optional[str]:
    """Best-effort: query the running OpenAI-compatible endpoint for its served
    model id so the UI can show a real value in the curl example."""
    try:
        pod_ip = None
        try:
            pod = k8s_client.core_v1.read_namespaced_pod(name=instance["id"], namespace=k8s_client.namespace)
            pod_ip = pod.status.pod_ip if pod and pod.status else None
        except Exception:
            return None
        if not pod_ip:
            return None
        headers = {}
        if instance.get("api_key"):
            headers["Authorization"] = f"Bearer {instance['api_key']}"
        resp = requests.get(f"http://{pod_ip}:{port}{suffix}/models", headers=headers, timeout=2)
        if resp.status_code == 200:
            data = resp.json().get("data") or []
            if data:
                return data[0].get("id")
    except Exception:
        return None
    return None


def _instance_api_info(request: Request, instance: Optional[dict], status: Optional[str]) -> tuple:
    """For API-kind instances, return (base_url, api_key, model) once ready, else (None, None, None).
    base_url is the OpenAI-compatible base, e.g. .../spaces/<id>/8000/v1"""
    if status != "ready" or not instance or not instance.get("api_kind"):
        return None, None, None
    itype = (instance.get("instance_type") or "").strip()
    port = instance.get("app_port") or APP_FRAMEWORK_PRESETS.get(itype, {}).get("port")
    suffix = instance.get("api_base_suffix") or APP_FRAMEWORK_PRESETS.get(itype, {}).get("api_base_suffix", "")
    origin = _request_public_origin(request) if request else (settings.PUBLIC_BASE_URL or "").rstrip("/")
    base = f"{origin}{settings.SPACES_PATH_PREFIX}/{instance['id']}/{port}{suffix}"
    model = _fetch_api_model(instance, port, suffix)
    return base, instance.get("api_key"), model


def _notebook_status_message(status_details: Optional[dict]) -> str:
    status = (status_details or {}).get("status") or "unknown"
    detail = (status_details or {}).get("message") or ""
    defaults = {
        "ready": "Your notebook is ready!",
        "running": "Container is running, waiting for readiness...",
        "jupyter_starting": "Jupyter is starting up...",
        "pending": "Waiting for resources...",
        "distributing": "Loading image onto the GPU node…",
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
    active_instance["url"] = (
        _ready_instance_url(request, live_instance, live_status)
        if request
        else (live_instance.get("url") if live_status == "ready" else None)
    )
    # Only surface access URL/credentials once the pod is actually ready; otherwise the profile
    # JSON would hand out endpoints that 404 (or leak credentials) while the pod is still pending.
    notebook_like = (live_instance.get("instance_type") or "").strip() in {"jupyter", "opencode"}
    opencode_ready = notebook_like and live_status == "ready"
    active_instance["opencode_url"] = live_instance.get("opencode_url") if opencode_ready else None
    active_instance["opencode_username"] = live_instance.get("opencode_username") if opencode_ready else None
    active_instance["opencode_password"] = live_instance.get("opencode_password") if opencode_ready else None
    active_instance["github_path"] = live_instance.get("github_path")
    active_instance["template_id"] = live_instance.get("template_id")
    active_instance["template_title"] = live_instance.get("template_title")
    active_instance["instance_type"] = live_instance.get("instance_type")
    active_instance["app_port"] = live_instance.get("app_port")
    api_base_url, api_key, api_model = _instance_api_info(request, live_instance, live_status)
    active_instance["api_base_url"] = api_base_url
    active_instance["api_key"] = api_key
    active_instance["api_model"] = api_model
    # SSH is reachable as soon as the pod is Running (sshd boots with the pod),
    # independent of app readiness. Only surface it once the pod is Running.
    # get_pod_status_details lowercases the phase, so compare against "running".
    pod_running = (status_details.get("phase") or "").lower() == "running"
    active_instance["ssh_host"] = live_instance.get("ssh_host") if pod_running else None
    active_instance["ssh_port"] = live_instance.get("ssh_port") if pod_running else None
    active_instance["ssh_username"] = live_instance.get("ssh_username") if pod_running else None
    active_instance["ssh_command"] = live_instance.get("ssh_command") if pod_running else None
    return active_instance


CUSTOM_IMAGE_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,38}$")


def _custom_image_public(record: dict) -> dict:
    """Strip heavy/internal fields from a custom image row for API responses."""
    if not record:
        return {}
    return {
        "id": record.get("id"),
        "name": record.get("name"),
        "image": record.get("image"),
        "build_status": record.get("build_status"),
        "build_log": record.get("build_log") or "",
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
    }


def _resolve_launchable_image(user: dict, image: str) -> bool:
    """An image is launchable if it is catalog-backed or the caller's own ready/rebuildable image."""
    if get_image_by_value(image):
        return True
    if get_ready_custom_image_by_value(user["id"], image):
        return True
    row = get_custom_image_by_value(user["id"], image)
    return bool(row and row["build_status"] == "evicted")


def _prepare_custom_image_for_launch(user: dict, image: str) -> Optional[str]:
    """Ensure a custom image is present for launch, requeueing evicted node-local images."""
    row = get_custom_image_by_value(user["id"], image)
    if not row:
        return None
    if row["build_status"] == "evicted":
        requeue_custom_image_build(row["id"], user["id"])
        return "rebuilding"
    mark_custom_image_launched(row["id"])
    return row["build_status"]


def _stamp_launch(user: dict, image: str, node_name: Optional[str]) -> None:
    """Record a launch against an image so neither catalog nor custom rows are wrongly reaped.

    Custom images stamp last_launched_at via mark_custom_image_launched (used by GC). Every image
    (catalog or custom) refreshes its image_nodes row so a freshly-launched ref on a node is not
    evicted by the outdated reaper. Best-effort: a stamping failure must never fail a launch.
    """
    try:
        row = get_custom_image_by_value(user["id"], image)
        if row:
            mark_custom_image_launched(row["id"])
        if node_name:
            touch_image_node(image, node_name)
    except Exception as e:
        logger.warning("Failed to stamp launch for image %s on node %s: %s", image, node_name, e)


def _ensure_image_on_node(image: str, gpu_count: int) -> Optional[str]:
    """Resolve the GPU node a launch will land on, distributing the image first if needed.

    Return contract:
      - a node name: the image is already loaded on that node; launch may proceed (pinned there).
      - None: distribution was enqueued; the caller must return a 'distributing' status instead of
        calling create_instance — the off-cluster daemon never runs inside the request handler
        (the single uvicorn worker would block all clients).
      - "" (empty string): no gating — proceed on the normal scheduling path. Returned when
        IMAGE_SERVICE_ENABLED is off (legacy DaemonSet/ACR path untouched) or no target node could
        be resolved.
    """
    if not settings.IMAGE_SERVICE_ENABLED:
        return ""
    node = k8s_client._select_target_gpu_node(gpu_count)
    if not node:
        return ""
    if image_loaded_on_node(image, node):
        return node
    # The Manager resolves the single node's target now; the daemon never expands scope.
    targets = k8s_client.resolve_node_targets([node])
    # P5 transport: use warm (P2P self-pull of the zot ref) when this image is durable in the LAN
    # registry (digest recorded by a prior push); otherwise fall back to the SSH byte-push. warm needs
    # the zot ref, so carry lan_target_ref. Legacy images never pushed to zot keep using distribute.
    lan_target_ref = _lan_registry_target_ref(image)
    use_warm = bool(lan_target_ref) and store.image_digest_present(image)
    payload = {"scope": f"node:{node}", "targets": targets}
    if use_warm:
        payload["lan_target_ref"] = lan_target_ref
    enqueue_image_job(
        kind="warm" if use_warm else "distribute",
        ref=image,
        payload=payload,
    )
    return None


def _normalize_github_raw_url(url: str) -> str:
    """Normalize a github.com/<o>/<r>/blob/<ref>/<path> URL to its raw.githubusercontent.com form."""
    parsed = urlparse(url)
    if parsed.hostname in ("github.com", "www.github.com"):
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 5 and parts[2] == "blob":
            owner, repo, _, ref = parts[0], parts[1], parts[2], parts[3]
            path = "/".join(parts[4:])
            return f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{path}"
    return url


def _fetch_github_dockerfile(url: str) -> str:
    """Fetch a single raw Dockerfile from an allowlisted GitHub host with an SSRF guard.

    Hardening: https only; host must be in GITHUB_RAW_ALLOWED_HOSTS; every resolved IP is rejected
    if it is private/loopback/link-local/reserved (blocks DNS rebinding); redirects are treated as
    errors (follow_redirects=False) so a 3xx can't bounce us to an internal host; the response is
    capped at CUSTOM_IMAGE_MAX_DOCKERFILE_BYTES. Any failure raises HTTPException(400).
    """
    import ipaddress
    import socket
    import time

    raw_url = _normalize_github_raw_url((url or "").strip())
    # The user-supplied URL must be a raw github URL (validated before proxying), so a proxy can't
    # be abused to fetch arbitrary hosts. Then route the actual fetch through GITHUB_RAW_PROXY
    # (gh-proxy.org) to avoid raw.githubusercontent.com throttling from cn-shanghai.
    src = urlparse(raw_url)
    if src.scheme != "https" or (src.hostname or "") != "raw.githubusercontent.com":
        raise HTTPException(
            status_code=400,
            detail="Dockerfile URL host is not allowed; use a github.com/...blob or raw.githubusercontent.com URL",
        )
    fetch_url = (settings.GITHUB_RAW_PROXY + raw_url) if settings.GITHUB_RAW_PROXY else raw_url
    parsed = urlparse(fetch_url)
    if parsed.scheme != "https":
        raise HTTPException(status_code=400, detail="Dockerfile URL must use https")
    host = parsed.hostname or ""
    if host not in settings.GITHUB_RAW_ALLOWED_HOSTS:
        raise HTTPException(
            status_code=400,
            detail="Dockerfile fetch host is not allowed",
        )
    port = parsed.port or 443
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except Exception:
        raise HTTPException(status_code=400, detail="Could not resolve Dockerfile URL host")
    if not infos:
        raise HTTPException(status_code=400, detail="Could not resolve Dockerfile URL host")
    for info in infos:
        ip = info[4][0]
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            raise HTTPException(status_code=400, detail="Could not resolve Dockerfile URL host")
        if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved or addr.is_multicast or addr.is_unspecified:
            raise HTTPException(status_code=400, detail="Dockerfile URL resolves to a disallowed address")

    max_bytes = settings.CUSTOM_IMAGE_MAX_DOCKERFILE_BYTES
    # Retry transient fetch failures (timeouts / connreset) — even via the proxy, github can blip.
    attempts = max(1, settings.GITHUB_DOCKERFILE_FETCH_RETRIES)
    resp = None
    last_err = None
    for i in range(attempts):
        try:
            with httpx.Client(
                follow_redirects=False,
                timeout=settings.GITHUB_DOCKERFILE_FETCH_TIMEOUT_SECONDS,
            ) as client:
                resp = client.get(fetch_url)
            if resp.status_code < 300:
                break
            last_err = f"HTTP {resp.status_code}"
            resp = None
        except Exception as e:
            last_err = type(e).__name__
            resp = None
        if i < attempts - 1:
            time.sleep(1)
    if resp is None:
        raise HTTPException(status_code=400, detail=f"Failed to fetch Dockerfile ({last_err or 'unknown error'})")
    content = resp.content[: max_bytes + 1]
    if len(content) > max_bytes:
        raise HTTPException(status_code=400, detail=f"Dockerfile exceeds {max_bytes} bytes")
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="Dockerfile is not valid UTF-8 text")


def verify_build_agent(request: Request):
    """Authenticate the R9700 build-agent for internal endpoints.

    Fails closed: if no BUILD_AGENT_TOKEN is configured, internal endpoints are disabled.
    The optional IP allowlist checks only the real TCP peer (request.client.host); the
    client-supplied X-Forwarded-For header is NOT trusted, since these endpoints are reached
    directly over a NodePort with no header-sanitizing reverse proxy in front, so trusting it
    would let any token-holder spoof an allowed IP. The bearer token is the primary gate.
    """
    token = settings.BUILD_AGENT_TOKEN
    if not token:
        raise HTTPException(status_code=404, detail="Not found")
    header = request.headers.get("authorization", "")
    expected = f"Bearer {token}"
    if not secrets.compare_digest(header, expected):
        raise HTTPException(status_code=401, detail="Invalid build-agent token")
    allowed = settings.BUILD_AGENT_ALLOWED_IPS
    if allowed:
        client_ip = request.client.host if request.client else ""
        if client_ip not in allowed:
            raise HTTPException(status_code=403, detail="Source IP not allowed")
    return True


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


def _instance_service_base(instance_id: str, port: Optional[int] = None) -> str:
    try:
        svc = k8s_client.core_v1.read_namespaced_service(
            name=f"{instance_id}-svc",
            namespace=k8s_client.namespace,
        )
    except Exception:
        raise HTTPException(status_code=404, detail="Instance service not found")
    return f"http://{svc.spec.cluster_ip}:{port or settings.NOTEBOOK_PORT}"


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


# Shared pooled client for the instance/app (spaces) HTTP proxies. A new client
# per request (with a 1-hour timeout) leaked connections and overwhelmed
# single-threaded backends (e.g. ComfyUI aiohttp) under the browser's burst of
# concurrent asset requests. A bounded shared pool reuses/limits connections.
_proxy_client: Optional[httpx.AsyncClient] = None


def _get_proxy_client() -> httpx.AsyncClient:
    global _proxy_client
    if _proxy_client is None or _proxy_client.is_closed:
        _proxy_client = httpx.AsyncClient(
            follow_redirects=False,
            timeout=httpx.Timeout(connect=10.0, read=120.0, write=120.0, pool=30.0),
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32, keepalive_expiry=30.0),
        )
    return _proxy_client


async def _close_upstream_only(upstream: httpx.Response):
    await upstream.aclose()


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
    # Also surface the logged-in user's own templates (including profile-only ones, e.g.
    # templates built on their custom images) in their gallery view, deduped by id.
    if user:
        existing_ids = {t["id"] for t in notebook_templates}
        for t in list_notebook_templates(enabled_only=False, owner_user_id=user["id"]):
            if t["id"] not in existing_ids:
                notebook_templates.append(t)
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
            "custom_images_json": json.dumps(
                [_custom_image_public(ci) for ci in list_custom_images(user["id"])] if user else []
            ),
            "workshop_login_enabled": settings.WORKSHOP_LOGIN_ENABLED,
            "admin_login_enabled": settings.ADMIN_LOGIN_ENABLED,
            "resource_profiles_json": json.dumps(RESOURCE_PROFILES),
            "auto_resource_profile_by_gpu_json": json.dumps(AUTO_RESOURCE_PROFILE_BY_GPU),
            "disk_size_min_gb": settings.DISK_SIZE_MIN_GB,
            "disk_size_max_by_gpu_json": json.dumps(settings.DISK_SIZE_MAX_BY_GPU),
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
    _enforce_login_rate_limit(request)
    if not settings.GITHUB_CLIENT_ID:
        raise HTTPException(status_code=500, detail="GitHub OAuth is not configured")
    params = {
        "client_id": settings.GITHUB_CLIENT_ID,
        "redirect_uri": _oauth_redirect_uri(request, "github"),
        "scope": "read:user user:email",
        "state": _oauth_state(request, "github"),
    }
    return RedirectResponse(f"{settings.GITHUB_AUTHORIZE_BASE}/login/oauth/authorize?" + urlencode(params))


@app.get("/auth/github/callback", name="github_callback")
async def github_callback(request: Request, code: str = Query(...), state: str = Query("")):
    _enforce_login_rate_limit(request)
    _validate_oauth_state(request, "github", state)
    try:
        async with httpx.AsyncClient(timeout=_oauth_timeout()) as client:
            token_resp = await client.post(
                f"{settings.GITHUB_WEB_BASE}/login/oauth/access_token",
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
                client.get(f"{settings.GITHUB_API_BASE}/user", headers=headers),
                client.get(f"{settings.GITHUB_API_BASE}/user/emails", headers=headers),
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
    _enforce_signup_quota(request, user)
    if user.get("_created"):
        from .telemetry import report_user_registered_event

        await report_user_registered_event(user)
    _establish_session(request, user)
    return RedirectResponse("/")


@app.get("/auth/modelscope/login")
async def modelscope_login(request: Request):
    _enforce_login_rate_limit(request)
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
    _enforce_login_rate_limit(request)
    # Fail closed on OAuth state, matching the GitHub path. A missing/mismatched
    # state is a CSRF/login-fixation signal and must not be allowed through.
    _validate_oauth_state(request, "modelscope", state)
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
    # Identity comes from server-verified sources only: the userinfo response,
    # or the id_token / uid from the token endpoint. The token endpoint is a
    # direct server-to-server HTTPS exchange authenticated with our
    # client_secret, so reading the id_token's claims is trustworthy even
    # without a separate JWT signature check (the TLS channel authenticates the
    # source). Some ModelScope apps' userinfo endpoint 404s, so the id_token is
    # the primary identity there. We still refuse to derive identity from the
    # raw access token (not a stable account id).
    claims = dict(profile) if isinstance(profile, dict) else {}
    if not (claims.get("id") or claims.get("sub") or claims.get("username")):
        id_token = token_data.get("id_token")
        if id_token:
            try:
                payload = id_token.split(".")[1]
                payload += "=" * (-len(payload) % 4)
                decoded = json.loads(base64.urlsafe_b64decode(payload))
                # userinfo values win over id_token when both present
                claims = {**decoded, **claims}
            except Exception as e:
                logger.warning("Failed to decode ModelScope id_token: %s", e)
    provider_id = str(
        claims.get("id") or claims.get("sub") or claims.get("username")
        or claims.get("email") or token_data.get("uid") or ""
    ).strip()
    if not provider_id:
        logger.warning(
            "ModelScope login: no verifiable identity. userinfo_keys=%s token_keys=%s",
            list(profile.keys()) if isinstance(profile, dict) else None, list(token_data.keys()),
        )
        raise HTTPException(status_code=400, detail="ModelScope login failed: no verifiable account identity")
    email = claims.get("email") or f"{provider_id}@modelscope.local"
    user = get_or_create_user("modelscope", provider_id, email, claims.get("name") or claims.get("username") or provider_id, claims.get("avatar_url") or claims.get("avatar") or "")
    _enforce_signup_quota(request, user)
    if user.get("_created"):
        from .telemetry import report_user_registered_event

        await report_user_registered_event(user)
    _establish_session(request, user)
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
    _establish_session(request, user)
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
    _establish_session(request, user)
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


async def _provision_notebook_instance(user: dict, email: str, image: str, params: dict, target_node: Optional[str]) -> dict:
    """Create + record a plain notebook/opencode/custom instance. Shared by the request handler
    and the distribute-resume path so both produce an identical instance + DB row + telemetry."""
    instance_type = params["instance_type"]
    gpu_count = params["gpu_count"]
    pod_type = params.get("pod_type")
    instance_id = f"u-{user['id']}-{hashlib.md5(email.encode()).hexdigest()[:8]}"
    instance = k8s_client.create_instance(
        email, image,
        instance_type=instance_type,
        gpu_count=gpu_count,
        custom_instance_id=instance_id,
        resource_profile=params.get("resource_profile", "auto"),
        disk_size_gb=params.get("disk_size_gb"),
        pod_type=pod_type,
    )
    _stamp_launch(user, image, target_node or None)
    record_instance(
        user["id"], email, instance["id"], image, instance_type, gpu_count,
        instance.get("node_port"), instance.get("opencode_node_port"),
        pod_type=pod_type,
    )
    record_instance_launch_event(user["id"], email, instance["id"], image, instance_type, gpu_count,
                                 pod_type=pod_type)
    from .telemetry import report_gpu_instance_created_event

    await report_gpu_instance_created_event(
        instance_id=instance["id"],
        user_id=user["id"],
        instance_type=instance_type,
        gpu_count=gpu_count,
    )
    return instance


async def _provision_template_instance(user: dict, email: str, template: dict, params: dict, target_node: Optional[str]) -> dict:
    """Create + record a template instance. Shared by the launch handler and the resume path."""
    gpu_count = params["gpu_count"]
    github_info = _template_github_info(template) or None
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
        template_id=str(template["id"]),
        template_title=template["title"],
        start_command=template.get("start_command"),
        app_port=template.get("app_port"),
        model_source=template.get("model_source"),
        ssh_enabled=bool(template.get("ssh_enabled")),
        ssh_public_key=user.get("ssh_public_key"),
    )
    _stamp_launch(user, template["image"], target_node or None)
    record_instance(
        user["id"], email, instance["id"], template["image"], template_instance_type, gpu_count,
        instance.get("node_port"), instance.get("opencode_node_port"),
    )
    record_instance_launch_event(
        user["id"], email, instance["id"], template["image"], template_instance_type, gpu_count,
        template_id=template["id"], template_title=template["title"],
    )
    from .telemetry import report_gpu_instance_created_event

    await report_gpu_instance_created_event(
        instance_id=instance["id"],
        user_id=user["id"],
        instance_type=template_instance_type,
        gpu_count=gpu_count,
        template_id=template["id"],
    )
    return instance


async def _resume_distributing_launch(user: dict) -> Optional[str]:
    """Resume a launch that was waiting on image distribution.

    Returns "ready" (instance created — caller should report allocating/pending), "distributing"
    (image still not on a node — keep waiting), or None (no pending intent / unrecoverable, intent
    cleared). Called from the status poll, which the frontend already drives every few seconds.
    """
    intent = get_launch_intent(user["id"])
    if not intent:
        return None
    email = user["email"].lower()
    image = intent["image"]
    params = intent.get("params") or {}
    gpu_count = params.get("gpu_count", 1)
    # An instance may already exist (the user raced or a prior resume succeeded).
    if get_active_instance_for_user(user["id"]):
        delete_launch_intent(user["id"])
        return None
    if int(user["credits"]) < gpu_count:
        delete_launch_intent(user["id"])
        return None
    target_node = _ensure_image_on_node(image, gpu_count)
    if target_node is None:
        return "distributing"  # still copying; re-enqueue is deduped by enqueue_image_job
    try:
        if intent["kind"] == "template":
            template_id = params.get("template_id")
            template = _template_accessible_to_user(int(template_id), user) if template_id else None
            if not template:
                delete_launch_intent(user["id"])
                return None
            await _provision_template_instance(user, email, template, params, target_node)
        else:
            await _provision_notebook_instance(user, email, image, params, target_node)
    except Exception as e:
        logger.error("Failed to resume distributing launch for %s: %s", email, e)
        return "distributing"  # transient; let the next poll retry rather than fail hard
    delete_launch_intent(user["id"])
    return "ready"


@app.post("/api/notebook/request", response_model=NotebookStatus)
async def request_notebook(request: Request, req: NotebookRequest, user: dict = Depends(current_user)):
    """Request a notebook instance"""
    email = user["email"].lower()
    image = req.image or settings.DEFAULT_IMAGE
    instance_type = req.instance_type or "jupyter"
    gpu_count = req.gpu_count or 1
    resource_profile = _validate_resource_profile(req.resource_profile)
    pod_type = _normalize_pod_type(req.pod_type)

    if gpu_count not in [1, 2, 4]:
        raise HTTPException(status_code=400, detail="GPU count must be 1, 2, or 4")

    disk_max = settings.DISK_SIZE_MAX_BY_GPU.get(gpu_count, settings.DISK_SIZE_MIN_GB)
    disk_size_gb = req.disk_size_gb if req.disk_size_gb else disk_max
    if disk_size_gb < settings.DISK_SIZE_MIN_GB or disk_size_gb > disk_max:
        raise HTTPException(status_code=400, detail=f"Disk size must be between {settings.DISK_SIZE_MIN_GB}G and {disk_max}G for this instance scale")

    if not _resolve_launchable_image(user, image):
        raise HTTPException(status_code=400, detail="Invalid image selected")
    prep = _prepare_custom_image_for_launch(user, image)
    if prep == "rebuilding":
        raise HTTPException(
            status_code=409,
            detail="This custom image was reclaimed under disk pressure and is being rebuilt. Retry when its status is ready.",
        )

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

    target_node = _ensure_image_on_node(image, gpu_count)
    if target_node is None:
        # Image is being copied to the node; persist the launch so /api/notebook/status can
        # resume it once distribution finishes (otherwise it dead-ends with no instance row).
        upsert_launch_intent(user["id"], email, image, "notebook", {
            "instance_type": instance_type,
            "gpu_count": gpu_count,
            "resource_profile": resource_profile,
            "disk_size_gb": disk_size_gb,
            "pod_type": pod_type,
        })
        return NotebookStatus(
            status="distributing",
            message="Loading image onto the GPU node…",
            url=None,
            email=email,
        )

    try:
        instance = await _provision_notebook_instance(user, email, image, {
            "instance_type": instance_type,
            "gpu_count": gpu_count,
            "resource_profile": resource_profile,
            "disk_size_gb": disk_size_gb,
            "pod_type": pod_type,
        }, target_node)
        return NotebookStatus(
            status="allocating",
            message="Allocating resources for your instance...",
            url=None,
            email=email,
            instance_id=instance["id"],
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
            # A launch waiting on image distribution has no instance row yet. Resume it (create the
            # pod once the image lands) instead of reporting not_found, which the UI treats as a
            # terminal failure and would strand the launch forever.
            resume = await _resume_distributing_launch(user)
            if resume == "ready":
                active = get_active_instance_for_user(user["id"])
            elif resume == "distributing":
                return NotebookStatus(
                    status="distributing",
                    message="Loading image onto the GPU node…",
                    email=email,
                )
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
        
        if status not in ("ready", "failed") and not status_details.get("message"):
            try:
                detail = k8s_client.get_startup_detail(active["instance_id"])
            except Exception as e:
                logger.debug("get_startup_detail failed for %s: %s", active["instance_id"], e)
                detail = None
            if detail:
                status_details["message"] = detail

        api_base_url, api_key, api_model = _instance_api_info(request, instance, status)
        _pod_running = (status_details.get("phase") or "").lower() == "running"
        return NotebookStatus(
            status=status or "unknown",
            message=_notebook_status_message(status_details),
            url=_ready_instance_url(request, instance, status),
            opencode_url=instance.get("opencode_url") if status == "ready" else None,
            opencode_username=instance.get("opencode_username") if status == "ready" else None,
            opencode_password=instance.get("opencode_password") if status == "ready" else None,
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
            api_model=api_model,
            ssh_host=(instance.get("ssh_host") if (instance and _pod_running) else None),
            ssh_port=(instance.get("ssh_port") if (instance and _pod_running) else None),
            ssh_username=(instance.get("ssh_username") if (instance and _pod_running) else None),
            ssh_command=(instance.get("ssh_command") if (instance and _pod_running) else None),
        )

    except Exception as e:
        logger.error(f"Error checking status for {email}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/notebook/logs")
async def notebook_logs(user: dict = Depends(current_user)):
    """Pod events + container stdout for the user's active instance, for the live startup view."""
    active = get_active_instance_for_user(user["id"])
    if not active:
        return {"events": [], "container": "", "status": "not_found"}
    try:
        return k8s_client.get_pod_logs(active["instance_id"])
    except Exception as e:
        logger.error("Error fetching pod logs for %s: %s", active["instance_id"], e)
        return {"events": [], "container": "", "status": "error"}


@app.delete("/api/notebook/current", response_model=DestroyResponse)
async def destroy_current_notebook(user: dict = Depends(current_user)):
    """Destroy the current user's active notebook instance."""
    # Cancelling during image distribution: drop the pending intent so a later status poll does
    # not resurrect a pod the user just cancelled.
    delete_launch_intent(user["id"])
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
# Custom Image Endpoints (user-facing)
# =============================================================================

@app.get("/api/custom-images")
async def list_my_custom_images(user: dict = Depends(current_user)):
    return {
        "custom_images": [_custom_image_public(ci) for ci in list_custom_images(user["id"])],
        "max_per_user": settings.CUSTOM_IMAGE_MAX_PER_USER,
    }


@app.post("/api/custom-images/build")
async def build_custom_image(req: CustomImageBuildRequest, user: dict = Depends(current_user)):
    name = (req.name or "").strip().lower()
    if not CUSTOM_IMAGE_NAME_RE.match(name):
        raise HTTPException(
            status_code=400,
            detail="Name must be 1-39 chars: lowercase letters, digits, hyphens; must start with a letter or digit.",
        )
    dockerfile = req.dockerfile or ""
    github_url = (getattr(req, "github_url", None) or "").strip()
    if github_url and not dockerfile.strip():
        dockerfile = _fetch_github_dockerfile(github_url)
    if not dockerfile.strip():
        raise HTTPException(status_code=400, detail="Dockerfile must not be empty")
    if len(dockerfile.encode("utf-8")) > settings.CUSTOM_IMAGE_MAX_DOCKERFILE_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"Dockerfile exceeds {settings.CUSTOM_IMAGE_MAX_DOCKERFILE_BYTES} bytes",
        )

    image_tag = f"{settings.CUSTOM_IMAGE_LOCAL_TAG_PREFIX}/user-{user['id']}:{name}"
    try:
        record = create_custom_image(
            user["id"], name, image_tag, dockerfile, settings.CUSTOM_IMAGE_MAX_PER_USER
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if settings.IMAGE_SERVICE_ENABLED:
        # Build on the image-service host (node 0042) so run_build exports a docker tarball; the
        # launch-time distribute (_ensure_image_on_node) then ships it via `cat tar | ctr import`.
        # Without this the row is only built by the LEGACY build-agent, which writes no tarball, so
        # distribute falls back to `nerdctl save` (nonexistent on 0042) and the launch never lands.
        # Enqueue the RAW dockerfile — claim_image_job appends DOCKERFILE_SUFFIX at claim time
        # (do NOT append it here or it would be duplicated).
        # When the LAN registry is wired, chain a `push` after build so a custom image is durable in
        # zot (its "ready" gate). The build branch defers the ready flip to push in that case.
        build_payload = {"dockerfile": dockerfile}
        lan_target_ref = _lan_registry_target_ref(image_tag)
        if lan_target_ref:
            build_payload["chain"] = ["push"]
            build_payload["lan_target_ref"] = lan_target_ref
        enqueue_image_job(
            kind="build",
            ref=image_tag,
            custom_image_id=record["id"],
            payload=build_payload,
        )
    return _custom_image_public(record)


@app.get("/api/custom-images/{image_id}/status")
async def custom_image_status(image_id: int, user: dict = Depends(current_user)):
    record = get_custom_image(image_id, user_id=user["id"])
    if not record:
        raise HTTPException(status_code=404, detail="Custom image not found")
    return _custom_image_public(record)


@app.delete("/api/custom-images/{image_id}")
async def delete_my_custom_image(image_id: int, user: dict = Depends(current_user)):
    # Compute blobs UNIQUE to this image BEFORE deleting its row (the row's blob_list is one of the
    # inputs; after delete_custom_image it's gone). Blobs shared with a live image are excluded so the
    # P2P/seed cache purge never removes a still-referenced blob.
    _pre = get_custom_image(image_id, user_id=user["id"])
    unique_blobs = store.unique_blob_ids_for_ref(_pre.get("image")) if _pre and _pre.get("image") else []
    try:
        record = delete_custom_image(image_id, user["id"])
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if not record:
        raise HTTPException(status_code=404, detail="Custom image not found")
    # Image-service is the only image-management system: removing the catalog row must also evict
    # the image's layers from the nodes it was distributed to (the legacy prepull cleanup that used
    # to run here is gone). The Manager resolves evict targets (the daemon has no kubectl): prefer
    # the nodes recorded as holding this ref, falling back to all eligible nodes when none recorded.
    image_ref = record.get("image")
    if image_ref:
        try:
            recorded = store.list_nodes_for_image(image_ref)
            targets = k8s_client.resolve_node_targets(recorded) if recorded else k8s_client.resolve_node_targets(None)
            # P4 complete-delete fan-out — GATED (see admin_delete_image). Until PURGE_FANOUT_ENABLED,
            # keep the single `evict`. Do NOT link custom_image_id in either branch: the row was
            # already deleted above, and image_jobs has a plain FK to custom_images — a link to a
            # missing row would raise ForeignKeyViolation.
            if settings.PURGE_FANOUT_ENABLED:
                enqueue_purge_fanout(
                    image_ref,
                    targets,
                    digest=record.get("digest"),
                    lan_target_ref=_lan_registry_target_ref(image_ref),
                    image_id=None,
                    blob_ids=unique_blobs,
                )
            else:
                enqueue_image_job(kind="evict", ref=image_ref,
                                  payload={"scope": "all", "targets": targets})
        except Exception as e:
            logger.warning("Failed to enqueue purge fan-out for custom image %s (%s): %s", image_id, image_ref, e)
    return {"success": True}


# =============================================================================
# Internal Build-Agent Endpoints (R9700 polling worker; token-guarded)
# =============================================================================

@app.post("/api/internal/builds/claim")
async def claim_build(req: BuildClaimRequest, _agent: bool = Depends(verify_build_agent)):
    # When the image-service is on, custom builds are owned by the new kind=build image_job path
    # (which exports a distributable tarball). The legacy node-local build-agent must NOT also claim
    # the same pending custom_images row — that double-build marks the row 'ready' with no tarball,
    # so launch-time distribute fails. Starve the legacy claimer in image-service mode.
    if settings.IMAGE_SERVICE_ENABLED:
        return {"job": None}
    job = claim_next_build(req.agent_id)
    if not job:
        return {"job": None}
    # The agent always builds with the Jupyter + OpenCode suffix appended.
    dockerfile = job["dockerfile"] + "\n" + settings.DOCKERFILE_SUFFIX
    return {"job": {"id": job["id"], "tag": job["image"], "dockerfile": dockerfile}}


@app.post("/api/internal/builds/{image_id}/log")
async def push_build_log(image_id: int, req: BuildLogRequest, _agent: bool = Depends(verify_build_agent)):
    # Only the agent that leased this build (and only while it is still building) may log to it.
    if not append_custom_image_log(image_id, req.log or "", agent_id=req.agent_id):
        raise HTTPException(status_code=409, detail="Build not claimed by this agent or not building")
    return {"ok": True}


@app.post("/api/internal/builds/{image_id}/result")
async def report_build_result(image_id: int, req: BuildResultRequest, _agent: bool = Depends(verify_build_agent)):
    status = (req.status or "").strip().lower()
    if status not in ("ready", "failed"):
        raise HTTPException(status_code=400, detail="status must be 'ready' or 'failed'")
    # Only the agent that leased this build (and only while it is still building) may finalize it.
    record = update_custom_image_status(image_id, status=status, require_claimed_by=req.agent_id)
    if not record:
        raise HTTPException(status_code=409, detail="Build not claimed by this agent or not building")
    # The node-local builder writes the image straight into the GPU node's containerd `k8s.io`
    # namespace. Ready means the launch path can use imagePullPolicy=IfNotPresent without pulling.
    return {"ok": True}


@app.post("/api/internal/builds/gc-candidates")
async def build_gc_candidates(req: BuildClaimRequest, mode: str = "idle", _agent: bool = Depends(verify_build_agent)):
    """Custom-image GC candidates. mode='idle' (default) returns only images past the 5-day idle
    window; mode='disk_pressure' returns all ready node-local images coldest-first so an
    over-threshold disk can reclaim the coldest image regardless of the idle window."""
    gc_mode = "disk_pressure" if mode == "disk_pressure" else "idle"
    candidates = [
        {"id": row["id"], "tag": row["image"], "last_launched_at": row.get("last_launched_at")}
        for row in list_gc_candidates(mode=gc_mode)
    ]
    return {"candidates": candidates}


@app.post("/api/internal/builds/evicted")
async def report_evicted(req: BuildEvictRequest, _agent: bool = Depends(verify_build_agent)):
    evicted = []
    for image_id in req.image_ids:
        if mark_custom_image_evicted(image_id):
            evicted.append(image_id)
    return {"evicted": evicted}


# =============================================================================
# Internal Image-Service Job API (off-cluster daemon; token-guarded)
# =============================================================================

def _resolve_chain_targets(scope: str) -> list[dict]:
    """Resolve distribute targets for a chain step from its scope. The Manager owns target
    resolution (the daemon has no kubectl): scope 'all' -> every eligible node; 'node:<name>'
    -> just that node; anything else -> empty (daemon will fail fast on no_targets)."""
    scope = (scope or "").strip()
    if scope == "node:" or scope.startswith("node:"):
        node_name = scope.split(":", 1)[1].strip()
        return k8s_client.resolve_node_targets([node_name]) if node_name else []
    if scope == "all":
        return k8s_client.resolve_node_targets(None)
    return k8s_client.resolve_node_targets(None)


def _enqueue_next_chain_step(job: dict, payload: dict) -> None:
    """Manager-driven chaining: pop the next kind off payload['chain'] and enqueue it, carrying
    the remaining chain forward. Targets for a 'distribute' step are resolved NOW (the daemon
    never resolves node IPs); 'acr_backup' carries acr_target_ref."""
    chain = list(payload.get("chain") or [])
    if not chain:
        return
    next_kind = chain.pop(0)
    ref = job.get("ref")
    next_payload: dict = {"chain": chain}
    if payload.get("scope"):
        next_payload["scope"] = payload["scope"]
    if next_kind in ("distribute", "warm"):
        # warm forks distribute's target model exactly — Manager resolves node IPs (the daemon has
        # no kubectl); the daemon triggers a per-node pull through the P2P mirror.
        next_payload["targets"] = _resolve_chain_targets(payload.get("scope") or "all")
    elif next_kind == "acr_backup":
        acr_target_ref = payload.get("acr_target_ref")
        if acr_target_ref:
            next_payload["acr_target_ref"] = acr_target_ref
    elif next_kind == "push":
        lan_target_ref = payload.get("lan_target_ref")
        if lan_target_ref:
            next_payload["lan_target_ref"] = lan_target_ref
    # Carry the LAN ref forward through non-push steps too, so a later push step in the same chain
    # (or the lifecycle's digest recording) still sees it.
    if payload.get("lan_target_ref") and "lan_target_ref" not in next_payload:
        next_payload["lan_target_ref"] = payload["lan_target_ref"]
    enqueue_image_job(
        kind=next_kind,
        ref=ref,
        image_id=job.get("image_id"),
        custom_image_id=job.get("custom_image_id"),
        payload=next_payload,
    )


def _sync_image_job_lifecycle(job: dict, result: Optional[dict]) -> None:
    """Apply a succeeded job's effect to the linked image/node lifecycle tables."""
    kind = job.get("kind")
    ref = job.get("ref")
    result = result or {}
    payload = job.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload) if payload else {}
        except (ValueError, TypeError):
            payload = {}
    payload = payload or {}

    if kind == "build":
        custom_image_id = job.get("custom_image_id")
        if custom_image_id and "push" not in (payload.get("chain") or []):
            # Flip the custom image to ready. In IMAGE_SERVICE_ENABLED mode the custom_images row is
            # never 'building'/claimed (the kind=build image_job owns the lifecycle; the legacy
            # claim_build path that set 'building' is starved), so guarding on
            # require_claimed_by=<job agent> would match 0 rows and leave it stuck 'pending'.
            # finish_image_job already verified the image_job's own ownership before we get here.
            #
            # When a `push` step follows in the chain (LAN registry wired), DEFER the ready flip to
            # the push branch so "ready" means the image is durable in the registry, not merely
            # built on 0042. The row stays 'pending' (UI renders "Building...") until push succeeds.
            update_custom_image_status(custom_image_id, status="ready", require_claimed_by=None)
    elif kind in ("pull",):
        # Source bytes now exist on the Image-Service host; distribution follows as its own job.
        pass
    elif kind == "push":
        # Registry-durable: record the manifest digest and flip readiness. digest is agent-reported
        # (the manager has no route to the LAN registry). A null digest still counts as pushed but
        # can't gate on digest for this ref (logged agent-side).
        digest = result.get("digest")
        blob_ids = result.get("blob_ids") or []
        image_id = job.get("image_id")
        if image_id is not None:
            store.set_image_digest(image_id, digest)
            if blob_ids:
                store.set_image_blob_list(image_id, blob_ids)
        custom_image_id = job.get("custom_image_id")
        if custom_image_id:
            store.set_custom_image_digest(custom_image_id, digest)
            if blob_ids:
                store.set_custom_image_blob_list(custom_image_id, blob_ids)
            update_custom_image_status(custom_image_id, status="ready", require_claimed_by=None)
    elif kind in ("distribute", "warm"):
        # warm writes the SAME 'loaded' rows distribute did — the per-node results contract is
        # identical, so upload->ready-on-all counting is unchanged whichever transport ran.
        for node in result.get("nodes", []) or []:
            node_name = node.get("node") if isinstance(node, dict) else node
            if node and (not isinstance(node, dict) or node.get("loaded")):
                if node_name:
                    upsert_image_node(node_name=node_name, image_ref=ref, status="loaded", size_bytes=(node.get("size_bytes") if isinstance(node, dict) else None))
    elif kind == "evict":
        for node in result.get("nodes", []) or []:
            node_name = node.get("node") if isinstance(node, dict) else node
            if node and (not isinstance(node, dict) or node.get("removed")):
                if node_name:
                    clear_image_node(ref, node_name)
        custom_image_id = job.get("custom_image_id")
        if custom_image_id:
            mark_custom_image_evicted(custom_image_id)
    elif kind == "purge_node":
        # P4: clear the image_nodes row for each node whose layers were actually removed. A node
        # that failed keeps its row (the durable retry handle); requeue_failed_purges retries it.
        for node in result.get("nodes", []) or []:
            node_name = node.get("node") if isinstance(node, dict) else node
            if node and (not isinstance(node, dict) or node.get("removed")):
                if node_name:
                    clear_image_node(ref, node_name)
    elif kind in ("purge_p2p", "purge_seed", "registry_delete", "purge_builder"):
        # P4 byte-surface purges with no image_nodes-row effect (P2P cache / seed / registry tag /
        # 0042 tarball). Success is recorded by the job's terminal status; the purge_meta claim-gate
        # reads that. Nothing to write here.
        pass
    elif kind == "purge_meta":
        # P4 FINAL step — drops the remaining DB handles (image_nodes rows + custom_images mark).
        # RE-CHECK prereqs at execution time, not just at claim: requeue_failed_purges can flip a
        # prereq failed->pending AFTER purge_meta was claimed but before this result lands. Dropping
        # the rows then would orphan that surface's bytes. If any prereq regressed, abort by
        # re-enqueuing purge_meta (it will defer again at claim until prereqs re-succeed).
        if not store.purge_prereqs_satisfied(ref):
            logger.warning("purge_meta for %s aborted: a byte-surface prereq regressed; re-enqueuing", ref)
            enqueue_image_job(kind="purge_meta", ref=ref, custom_image_id=job.get("custom_image_id"),
                              payload=payload)
        else:
            store.delete_orphan_image_nodes(ref)
            custom_image_id = job.get("custom_image_id")
            if custom_image_id:
                mark_custom_image_evicted(custom_image_id)
    elif kind == "acr_backup":
        image_id = job.get("image_id")
        if image_id is not None:
            store.set_image_acr_backup(image_id, result.get("acr_backup_ref") or ref, "ok")

    # Manager-driven chaining: any successful job carrying a non-empty chain enqueues the next
    # step, resolving distribute targets here (the daemon has no kubectl).
    _enqueue_next_chain_step(job, payload)


@app.post("/api/internal/jobs/claim")
async def claim_image_job(req: ImageJobClaimRequest, _agent: bool = Depends(verify_build_agent)):
    job = claim_next_image_job(req.agent_id, kinds=req.kinds)
    if not job:
        return {"job": None}
    payload = job.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload) if payload else {}
        except (ValueError, TypeError):
            payload = {}
    payload = payload or {}
    # A build job always builds with the Jupyter + OpenCode suffix appended, exactly as claim_build does.
    if job.get("kind") == "build" and payload.get("dockerfile"):
        payload["dockerfile"] = payload["dockerfile"] + "\n" + settings.DOCKERFILE_SUFFIX
    return {
        "job": {
            "id": job["id"],
            "kind": job.get("kind"),
            "ref": job.get("ref"),
            "image_id": job.get("image_id"),
            "custom_image_id": job.get("custom_image_id"),
            "payload": payload,
        }
    }


@app.post("/api/internal/jobs/{job_id}/log")
async def push_image_job_log(job_id: int, req: ImageJobLogRequest, _agent: bool = Depends(verify_build_agent)):
    if not append_image_job_log(job_id, req.log or "", agent_id=req.agent_id):
        raise HTTPException(status_code=409, detail="Job not claimed by this agent or not running")
    return {"ok": True}


@app.post("/api/internal/jobs/{job_id}/heartbeat")
async def push_image_job_heartbeat(job_id: int, req: ImageJobHeartbeatRequest, _agent: bool = Depends(verify_build_agent)):
    """Refresh a long-running job's lease so the reaper does not requeue it mid-pull."""
    if not heartbeat_image_job(job_id, agent_id=req.agent_id):
        raise HTTPException(status_code=409, detail="Job not claimed by this agent or not running")
    return {"ok": True}


@app.post("/api/internal/jobs/{job_id}/result")
async def report_image_job_result(job_id: int, req: ImageJobResultRequest, _agent: bool = Depends(verify_build_agent)):
    status = (req.status or "").strip().lower()
    if status not in ("succeeded", "failed"):
        raise HTTPException(status_code=400, detail="status must be 'succeeded' or 'failed'")
    result_json = json.dumps(req.result) if req.result is not None else None
    job = finish_image_job(job_id, status, agent_id=req.agent_id, result=result_json)
    if not job:
        raise HTTPException(status_code=409, detail="Job not claimed by this agent or not running")
    # Quarantine any node the daemon reported as containerd_wedged, regardless of overall job status
    # (a distribute can partially succeed while one node wedged). The agent also quarantines inline,
    # but doing it here too is the durable backstop and covers agents that die before reporting.
    if isinstance(req.result, dict):
        ref = job.get("ref")
        for node in req.result.get("nodes", []) or []:
            if isinstance(node, dict) and node.get("error") == "containerd_wedged" and node.get("node"):
                try:
                    store.quarantine_node(node["node"], ref, settings.NODE_QUARANTINE_SECONDS)
                    logger.warning("Quarantined node %s (containerd_wedged) for %ss", node["node"], settings.NODE_QUARANTINE_SECONDS)
                except Exception as e:
                    logger.warning("Failed to quarantine node %s: %s", node.get("node"), e)
    if status == "succeeded":
        try:
            _sync_image_job_lifecycle(job, req.result)
        except Exception as e:
            logger.error("Failed to sync lifecycle for image job %s (%s): %s", job_id, job.get("kind"), e)
    else:
        # A failed distribute must not leave nodes stuck in the transient `importing` state (which
        # reads as "not loaded" forever). Clear any importing rows for this ref so status reflects
        # reality. Fix-1 guarantees a previously-loaded row was never downgraded to importing, so
        # this only drops genuinely-incomplete imports.
        if job.get("kind") == "distribute" and job.get("ref"):
            try:
                store.clear_importing_nodes(job["ref"])
            except Exception as e:
                logger.warning("Failed to clear importing rows for %s: %s", job.get("ref"), e)
        # A failed step aborts the chain (no next step is enqueued). Surface the failure on the
        # linked catalog image so it shows 'failed' instead of spinning in 'pulling'/'distributing'.
        image_id = job.get("image_id")
        if image_id is not None:
            err = req.result.get("error") if isinstance(req.result, dict) else None
            update_image_sync_status(
                image_id, "failed", 0, 0,
                f"{job.get('kind')} job {job_id} failed: {err or 'see job log'}", False,
            )
        custom_image_id = job.get("custom_image_id")
        if custom_image_id is not None:
            try:
                update_custom_image_status(custom_image_id, status="failed", require_claimed_by=None)
            except Exception as e:
                logger.warning("Could not mark custom image %s failed: %s", custom_image_id, e)
    return {"ok": True}


@app.post("/api/internal/nodes/status")
async def report_image_node_status(req: ImageNodeStatusRequest, _agent: bool = Depends(verify_build_agent)):
    """Image-service reports a node's status for an image ref (importing | quarantined | loaded).

    `quarantined` additionally stamps quarantined_until = now + (quarantine_seconds or the configured
    NODE_QUARANTINE_SECONDS) so the reaper routes around the wedged node (Part D)."""
    status = (req.status or "").strip().lower()
    if status not in ("importing", "quarantined", "loaded"):
        raise HTTPException(status_code=400, detail="status must be importing|quarantined|loaded")
    try:
        if status == "quarantined":
            seconds = req.quarantine_seconds if req.quarantine_seconds is not None else settings.NODE_QUARANTINE_SECONDS
            store.quarantine_node(req.node, req.ref, seconds)
        else:
            store.set_image_node_status(req.node, req.ref, status)
    except Exception as e:
        logger.warning("Failed to record node status %s for %s/%s: %s", status, req.node, req.ref, e)
        raise HTTPException(status_code=500, detail="failed to record node status")
    return {"ok": True}


@app.get("/api/internal/nodes/{node}/user-pods")
async def node_user_pod_count(node: str, _agent: bool = Depends(verify_build_agent)):
    """Return the count of running (non-terminal) USER notebook pods on `node`.

    The image-service consults this before any automated containerd restart: a restart is only ever
    issued to a node with zero user pods (pod-safety gate, Part C). Fail-safe: on any kube error the
    count is reported as unknown so the agent treats the node as "pods present" and refuses to
    restart — automated recovery must never destroy a user pod."""
    try:
        count = k8s_client.count_user_pods_on_node(node)
        return {"node": node, "user_pods": count, "ok": True}
    except Exception as e:
        logger.warning("Failed to count user pods on %s: %s", node, e)
        return {"node": node, "user_pods": None, "ok": False}


@app.post("/api/internal/images/outdated")
async def list_outdated_image_targets(req: ImageJobClaimRequest, _agent: bool = Depends(verify_build_agent)):
    """Compute outdated (ref,node) pairs and enqueue one evict job per ref server-side.

    The daemon only triggers this pass; the Manager resolves targets (it has kubectl) and
    enqueues. enqueue_image_job's non-terminal (kind, ref) guard dedupes repeated polls.
    """
    outdated = list_outdated_images()
    by_ref: dict[str, list[str]] = {}
    cid_by_ref: dict[str, int] = {}
    for row in outdated:
        ref = row.get("image_ref")
        node = row.get("node_name")
        if not ref or not node:
            continue
        by_ref.setdefault(ref, []).append(node)
        # Carry the custom_image_id (idle custom images) so the evict job flips the row to
        # 'evicted' via mark_custom_image_evicted; catalog rows have none.
        cid = row.get("custom_image_id")
        if cid is not None:
            cid_by_ref[ref] = cid
    enqueued = 0
    for ref, node_names in by_ref.items():
        targets = k8s_client.resolve_node_targets(node_names)
        # P4: idle-delete must be a COMPLETE purge (node layers + P2P caches + seed + registry tag +
        # 0042 tarball + rows) — the same hard "remove every byte" contract as an explicit delete,
        # not the layers-only `evict`. Relaunch rebuilds from the stored Dockerfile. digest is looked
        # up so registry_delete can DELETE the zot manifest. custom_image_id rides on purge_meta so
        # mark_custom_image_evicted flips the idle custom row to 'evicted'.
        cid = cid_by_ref.get(ref)
        if settings.PURGE_FANOUT_ENABLED:
            enqueue_purge_fanout(
                ref,
                targets,
                digest=store.get_digest_for_ref(ref),
                lan_target_ref=_lan_registry_target_ref(ref),
                image_id=None,
                blob_ids=store.unique_blob_ids_for_ref(ref),
            )
            if cid is not None:
                # Re-stamp custom_image_id onto the purge_meta job (fan-out enqueued it with
                # image_id=None; the custom mark needs the link). Safe: same-ref purge_meta is unique.
                store.link_purge_meta_custom_image(ref, cid)
        else:
            # Pre-cutover: the original layers-only evict, carrying custom_image_id so an idle custom
            # image is still marked 'evicted' (the P0/R2c behavior). Unchanged from before P4.
            enqueue_image_job(kind="evict", ref=ref, custom_image_id=cid,
                              payload={"targets": targets, "scope": "outdated"})
        enqueued += 1
    return {"images": outdated, "enqueued": enqueued}


# =============================================================================
# Notebook Template Endpoints
# =============================================================================

@app.get("/api/templates")
async def list_public_templates():
    return {"templates": list_notebook_templates(enabled_only=True)}


def _validate_ssh_public_key(value: str) -> str:
    """Lightly validate an SSH public key (single line, known type prefix)."""
    key = (value or "").strip()
    if not key:
        return ""
    if "\n" in key or "\r" in key:
        raise ValueError("SSH public key must be a single line")
    if not key.startswith(("ssh-rsa ", "ssh-ed25519 ", "ecdsa-sha2-", "ssh-dss ", "sk-ssh-ed25519@", "sk-ecdsa-sha2-")):
        raise ValueError("Not a valid SSH public key (expected e.g. 'ssh-ed25519 AAAA... comment')")
    if len(key) > 8192:
        raise ValueError("SSH public key is too long")
    return key


@app.post("/api/profile/ssh-key")
async def profile_set_ssh_key(req: SshPublicKeyRequest, user: dict = Depends(current_user)):
    """Save (or clear) the user's SSH public key used for key-based pod access."""
    try:
        key = _validate_ssh_public_key(req.ssh_public_key)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    updated = set_user_ssh_public_key(user["id"], key)
    if not updated:
        raise HTTPException(status_code=404, detail="User not found")
    return {"ssh_public_key": updated.get("ssh_public_key") or ""}


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
        # Editors choose Public/Private (default Private via the form); everyone
        # else can only create Private (Profile-only) templates.
        enabled_override = (bool(req.enabled) if user.get("is_editor") else False)
        return _save_notebook_template(req, owner_user_id=user["id"], sort_order=0, enabled_override=enabled_override)
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
    if not _resolve_launchable_image(user, template["image"]):
        raise HTTPException(status_code=400, detail="Template image is not available (catalog image disabled, or custom image not ready/owned by you)")
    if _prepare_custom_image_for_launch(user, template["image"]) == "rebuilding":
        raise HTTPException(
            status_code=409,
            detail="This custom image was reclaimed under disk pressure and is being rebuilt. Retry when its status is ready.",
        )

    active = get_active_instance_for_user(user["id"])
    if active:
        if k8s_client.get_instance_by_id(active["instance_id"]):
            raise HTTPException(status_code=400, detail="Each user can only have one active instance")
        mark_instance_deleted(active["instance_id"])

    if int(user["credits"]) < gpu_count:
        raise HTTPException(status_code=400, detail="Insufficient credits")

    email = user["email"].lower()
    target_node = _ensure_image_on_node(template["image"], gpu_count)
    if target_node is None:
        # Persist so /api/notebook/status can resume the template launch once the image lands.
        upsert_launch_intent(user["id"], email, template["image"], "template", {
            "gpu_count": gpu_count,
            "template_id": template["id"],
        })
        return NotebookStatus(
            status="distributing",
            message="Loading image onto the GPU node…",
            url=None,
            email=email,
        )
    try:
        instance = await _provision_template_instance(
            user, email, template, {"gpu_count": gpu_count}, target_node
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
    github_info = parse_github_path(full_path)
    repo_url = f"https://github.com/{github_info['org']}/{github_info['repo']}"
    github_info["raw_url"] = _github_raw_url(repo_url, github_info["branch"], github_info["path"])
    github_info["repo_url"] = _github_clone_url(repo_url)
    return github_info


def _parse_huggingface_notebook_url(raw_url: str) -> Optional[dict]:
    return parse_huggingface_notebook_url(raw_url)


def _parse_huggingface_demo_notebook_path(notebook_path: str) -> dict:
    github_info = parse_huggingface_demo_notebook_path(notebook_path)
    # GitHub-backed demos clone the repo inside the pod, so the clone URL must honor the
    # configured GitHub proxy (GITHUB_WEB_BASE) exactly like _parse_github_path. The native
    # huggingface.co path downloads over HTTP and needs no git clone URL.
    if (github_info.get("org") or "") != "huggingface":
        repo_url = f"https://github.com/{github_info['org']}/{github_info['repo']}"
        github_info["repo_url"] = _github_clone_url(repo_url)
    return github_info


def _parse_huggingface_demo_git_path(value: str) -> Optional[dict]:
    """Parse a workshop `.git` repo reference into a clone-ready github_info, or None when the
    value is not a `.git` reference (caller falls back to the .ipynb/HF parser)."""
    git_info = parse_huggingface_demo_git_path(value)
    if not git_info:
        return None
    repo_url = f"https://github.com/{git_info['org']}/{git_info['repo']}"
    git_info["repo_url"] = _github_clone_url(repo_url)
    return git_info


@app.post("/api/huggingface/notebooks", response_model=NotebookStatus)
async def launch_huggingface_demo_notebook(
    request: Request,
    req: HuggingFaceNotebookLaunchRequest,
    _auth: None = Depends(verify_huggingface_demo_api),
):
    pod_type = _normalize_pod_type(req.pod_type)

    # notebook_path can be: blank (bare Jupyter), a `.git` repo (workshop-only: clone + open
    # JupyterLab at repo root), or an .ipynb / HF URL (download or clone + open the notebook).
    if (req.notebook_path or "").strip():
        try:
            git_info = _parse_huggingface_demo_git_path(req.notebook_path)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if git_info is not None:
            if pod_type != "workshop":
                raise HTTPException(
                    status_code=400,
                    detail="A .git repo can only be launched with pod_type='workshop'",
                )
            github_info = git_info
        else:
            try:
                github_info = _parse_huggingface_demo_notebook_path(req.notebook_path)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
    else:
        github_info = None

    provider_id, display_name, email = _huggingface_demo_user_identity(req.user_name)
    gpu_count = req.gpu_count or 1
    requested_image = req.image or settings.HUGGINGFACE_DEMO_DEFAULT_IMAGE

    if gpu_count not in [1, 2, 4]:
        raise HTTPException(status_code=400, detail="GPU count must be 1, 2, or 4")
    # Callers may pass either the full image ref or the admin-panel name (e.g. "Huggingface").
    resolved = resolve_enabled_image(requested_image)
    if not resolved:
        raise HTTPException(status_code=400, detail="Invalid image selected")
    image = resolved["image"]

    user = get_or_create_external_user(
        HF_DEMO_PROVIDER,
        provider_id,
        email,
        name=display_name,
        initial_credits=0,
    )
    # Grant the starting credit floor exactly once (tracked by a ledger marker), never re-topping
    # on later launches — otherwise usage the billing loop charged would be refunded and credits
    # would never decrease. The marker is race-safe across concurrent first-launches.
    user = grant_initial_credits_once(
        user["id"], settings.HUGGINGFACE_DEMO_MIN_CREDITS, "hf_initial_grant"
    ) or user

    active = get_active_instance_for_user(user["id"])
    if active:
        if k8s_client.get_instance_by_id(active["instance_id"]):
            raise HTTPException(status_code=400, detail="Each user can only have one active instance")
        mark_instance_deleted(active["instance_id"])

    if int(user["credits"]) < gpu_count:
        raise HTTPException(status_code=400, detail="Insufficient credits")

    try:
        instance_id = f"hf-{user['id']}-{hashlib.md5(email.encode()).hexdigest()[:8]}"
        instance = k8s_client.create_instance(
            email,
            image,
            instance_type="jupyter",
            gpu_count=gpu_count,
            github_info=github_info,
            custom_instance_id=instance_id,
            resource_profile="auto",
            pod_type=pod_type,
            api_launched=True,
        )
        _stamp_launch(user, image, k8s_client._select_target_gpu_node(gpu_count) if settings.IMAGE_SERVICE_ENABLED else None)
        record_instance(user["id"], email, instance["id"], image, "jupyter", gpu_count,
                        instance.get("node_port"), instance.get("opencode_node_port"),
                        pod_type=pod_type, api_launched=True)
        record_instance_launch_event(user["id"], email, instance["id"], image, "jupyter", gpu_count,
                                     pod_type=pod_type)
        from .telemetry import report_gpu_instance_created_event

        await report_gpu_instance_created_event(
            instance_id=instance["id"],
            user_id=user["id"],
            instance_type="jupyter",
            gpu_count=gpu_count,
        )

        return NotebookStatus(
            status="allocating",
            message="Allocating resources for the Hugging Face demo notebook...",
            url=_instance_public_url(request, instance["id"], github_info.get("path") if github_info else None),
            opencode_url=instance.get("opencode_url"),
            opencode_username=instance.get("opencode_username"),
            opencode_password=instance.get("opencode_password"),
            email=email,
            instance_id=instance["id"],
        )
    except Exception as e:
        logger.error("Error launching Hugging Face demo notebook for %s: %s", email, e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/huggingface/notebooks/current", response_model=NotebookStatus)
async def huggingface_demo_notebook_status(
    request: Request,
    user_name: str = Query(...),
    _auth: None = Depends(verify_huggingface_demo_api),
):
    provider_id, _, email = _huggingface_demo_user_identity(user_name)
    user = get_user_by_provider(HF_DEMO_PROVIDER, provider_id)
    if not user:
        return NotebookStatus(
            status="not_found",
            message="No notebook instance found for this user",
            email=email,
        )

    try:
        active = get_active_instance_for_user(user["id"])
        if not active:
            return NotebookStatus(
                status="not_found",
                message="No notebook instance found for this user",
                email=email,
            )

        instance = k8s_client.get_instance_by_id(active["instance_id"])
        if not instance:
            mark_instance_deleted(active["instance_id"])
            return NotebookStatus(
                status="not_found",
                message="No notebook instance found for this user",
                email=email,
                instance_id=active["instance_id"],
            )

        status = k8s_client.get_pod_status(email, instance_id=active["instance_id"])
        status_messages = {
            "ready": "The notebook is ready",
            "running": "Container is running, starting Jupyter...",
            "jupyter_starting": "Jupyter is starting up...",
            "pending": "Waiting for resources...",
            "initializing": "Initializing notebook environment...",
            "loading": "Loading notebook image...",
            "failed": "Notebook creation failed",
            "unknown": "Checking status...",
        }

        message = status_messages.get(status, "Checking status...")
        if status not in ("ready", "failed"):
            try:
                detail = k8s_client.get_startup_detail(active["instance_id"])
            except Exception as e:
                logger.debug("get_startup_detail failed for %s: %s", active["instance_id"], e)
                detail = None
            if detail:
                message = detail

        return NotebookStatus(
            status=status or "unknown",
            message=message,
            url=_instance_public_url(request, instance["id"], instance.get("github_path")),
            opencode_url=instance.get("opencode_url"),
            opencode_username=instance.get("opencode_username"),
            opencode_password=instance.get("opencode_password"),
            email=email,
            instance_id=instance["id"],
        )
    except Exception as e:
        logger.error("Error checking Hugging Face demo notebook for %s: %s", email, e)
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/huggingface/notebooks/current", response_model=DestroyResponse)
async def destroy_huggingface_demo_notebook(
    user_name: str = Query(...),
    _auth: None = Depends(verify_huggingface_demo_api),
):
    provider_id, _, _ = _huggingface_demo_user_identity(user_name)
    user = get_user_by_provider(HF_DEMO_PROVIDER, provider_id)
    if not user:
        return DestroyResponse(success=False, message="No active instance found", destroyed_count=0)

    active = get_active_instance_for_user(user["id"])
    if not active:
        return DestroyResponse(success=False, message="No active instance found", destroyed_count=0)

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
        logger.error("Error destroying Hugging Face demo notebook %s: %s", instance_id, e)
        raise HTTPException(status_code=500, detail=str(e))


def _image_catalog_payload(default_image: Optional[str] = None) -> dict:
    """The enabled image catalog (same set the launch validator accepts)."""
    return {
        "images": [
            {
                "name": img["name"],
                "image": img["image"],
                "description": img.get("description") or "",
            }
            for img in list_images(enabled_only=True)
        ],
        "default_image": default_image or settings.DEFAULT_IMAGE,
    }


@app.get("/api/huggingface/images")
async def huggingface_demo_images(_auth: None = Depends(verify_huggingface_demo_api)):
    """Discover the selectable image catalog (HF bearer auth)."""
    return _image_catalog_payload(default_image=settings.HUGGINGFACE_DEMO_DEFAULT_IMAGE)


@app.get("/api/admin/images-list")
async def admin_images_list(_username: str = Depends(verify_admin)):
    """Same image catalog as the HF endpoint, under admin Basic auth."""
    return _image_catalog_payload()


@app.get("/api/huggingface/gpus")
async def huggingface_demo_gpus(_auth: None = Depends(verify_huggingface_demo_api)):
    """Free vs total GPUs reachable by this service's launches (HF bearer auth).

    Scoped to launch-eligible nodes, not the entire cluster — see gpu_capacity_summary."""
    return k8s_client.gpu_capacity_summary()


@app.get("/api/admin/gpus")
async def admin_gpus(_username: str = Depends(verify_admin)):
    """Free vs total GPUs reachable by this service's launches (admin Basic auth).

    Scoped to launch-eligible nodes, not the entire cluster — see gpu_capacity_summary."""
    return k8s_client.gpu_capacity_summary()


@app.get("/api/admin/gpu-nodes")
async def admin_gpu_nodes(_username: str = Depends(verify_admin)):
    """Per-node GPU status + cluster-utilization summary for the admin GPU dashboard.

    Beta-only: 404 unless GPU_DASHBOARD_ENABLED. Shows every GPU node of this service
    (including cordoned/NotReady); committed/free reflect this deployment's namespace."""
    if not settings.GPU_DASHBOARD_ENABLED:
        raise HTTPException(status_code=404, detail="Not found")
    return k8s_client.gpu_cluster_status()


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
    repo_url = f"https://github.com/{org}/{repo}"
    github_info = {
        "org": org,
        "repo": repo,
        "branch": branch,
        "path": path,
        "raw_url": _github_raw_url(repo_url, branch, path),
        "repo_url": _github_clone_url(repo_url),
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
        if settings.IMAGE_SERVICE_ENABLED:
            try:
                node_name = k8s_client._select_target_gpu_node()
                if node_name:
                    touch_image_node(settings.DEFAULT_IMAGE, node_name)
            except Exception as e:
                logger.warning("Failed to stamp launch for github notebook image: %s", e)

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
async def check_github_status(
    instance_id: str = Query(...),
    gh_instance: Optional[str] = Cookie(None, alias="amd_oneclick_gh_instance"),
):
    """Check the status of a GitHub notebook instance.

    The GitHub flow is anonymous: the caller's identity is the httponly
    `amd_oneclick_gh_instance` cookie set at create time. The response embeds
    credential-bearing URLs (Jupyter ?token= and the OpenCode Basic-auth URL), so we must
    only return them for the caller's OWN instance. The instance_id is a low-entropy md5[:8]
    that an attacker could brute-force, so trusting the query param alone would let anyone
    harvest another instance's credentials. Require the cookie to match.
    """
    if not gh_instance or gh_instance != instance_id:
        raise HTTPException(status_code=403, detail="Not your notebook instance")
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
            opencode_url=instance.get("opencode_url") if ready else None,
            opencode_username=instance.get("opencode_username") if ready else None,
            opencode_password=instance.get("opencode_password") if ready else None,
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

async def _handle_opencode_request(request: Request) -> Response:
    from .opencode_proxy import (
        SESSION_COOKIE_NAME,
        SESSION_MAX_AGE_SECONDS,
        mint_session_cookie,
        verify_handoff_token,
        verify_session_cookie,
    )

    if request.url.path == "/__opencode_auth":
        token = request.query_params.get("token", "")
        instance_id = verify_handoff_token(token) if token else None
        if not instance_id:
            return Response(
                "Invalid or expired OpenCode link. Reopen it from your dashboard.",
                status_code=403,
                media_type="text/plain",
            )
        response = RedirectResponse(url="/", status_code=302)
        response.set_cookie(
            SESSION_COOKIE_NAME,
            mint_session_cookie(instance_id),
            max_age=SESSION_MAX_AGE_SECONDS,
            httponly=True,
            secure=True,
            samesite="lax",
            path="/",
        )
        return response

    if request.url.path == "/site.webmanifest" and request.method in {"GET", "HEAD"}:
        return Response(
            json.dumps({
                "name": "OpenCode",
                "short_name": "OpenCode",
                "theme_color": "#ffffff",
                "background_color": "#ffffff",
                "display": "standalone",
            }),
            media_type="application/manifest+json",
            headers={"Cache-Control": "public, max-age=3600"},
        )

    cookie = request.cookies.get(SESSION_COOKIE_NAME, "")
    instance_id = verify_session_cookie(cookie) if cookie else None
    if not instance_id:
        return Response(
            "OpenCode session expired. Reopen OpenCode from your dashboard.",
            status_code=401,
            media_type="text/plain",
        )

    target_base = _instance_service_base(instance_id, port=settings.OPENCODE_WEB_PORT)
    target_url = f"{target_base}{request.url.path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    raw_user = settings.OPENCODE_WEB_USERNAME
    raw_password = k8s_client._opencode_password(instance_id)
    basic = base64.b64encode(f"{raw_user}:{raw_password}".encode("utf-8")).decode("ascii")
    body = await request.body()
    headers = _proxy_headers(request.headers)
    headers = {k: v for k, v in headers.items() if k.lower() != "cookie"}
    headers["authorization"] = f"Basic {basic}"

    client = _get_proxy_client()
    upstream = await client.send(
        client.build_request(request.method, target_url, headers=headers, content=body),
        stream=True,
    )
    response_headers = {
        k: v for k, v in upstream.headers.items()
        if k.lower() not in {
            "transfer-encoding",
            "connection",
            "content-length",
            "set-cookie",
            "www-authenticate",
            "proxy-authenticate",
        }
    }
    response = StreamingResponse(
        upstream.aiter_raw(),
        status_code=upstream.status_code,
        headers=response_headers,
        background=BackgroundTask(_close_upstream_only, upstream),
    )
    csp = response.headers.get("content-security-policy", "")
    theme_script_hash = "'sha256-QI23YWMJrD/tljM6/82tpL8EwqdBoptwZfycFHA9IiQ='"
    if csp and theme_script_hash not in csp:
        response.headers["content-security-policy"] = csp.replace(
            "script-src 'self' 'wasm-unsafe-eval'",
            f"script-src 'self' 'wasm-unsafe-eval' {theme_script_hash}",
        )
    return response


@app.api_route("/instances/{instance_id}/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def proxy_instance_http(instance_id: str, path: str, request: Request):
    """Proxy HTTP traffic to a Jupyter instance using its path-based base_url."""
    target_base = _instance_service_base(instance_id)
    target_url = f"{target_base}/instances/{instance_id}/{path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    body = await request.body()
    client = _get_proxy_client()
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
        background=BackgroundTask(_close_upstream_only, upstream),
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
    client = _get_proxy_client()
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
        background=BackgroundTask(_close_upstream_only, upstream),
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
        {"username": username, "gpu_dashboard_enabled": settings.GPU_DASHBOARD_ENABLED},
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


@app.post("/api/admin/users/{user_id}/revoke-sessions")
async def admin_revoke_sessions(user_id: int, username: str = Depends(verify_admin)):
    """Invalidate every outstanding session for a user (e.g. to kick an abusive
    or compromised account). Bumps the user's session epoch so all cookies that
    were issued before now are rejected on the next request."""
    new_version = bump_user_token_version(user_id)
    if new_version is None:
        raise HTTPException(status_code=404, detail="User not found")
    return {"success": True, "user_id": user_id, "token_version": new_version}


@app.post("/api/admin/users/{user_id}/credits")
async def admin_grant_credits(user_id: int, req: CreditGrantRequest, username: str = Depends(verify_admin)):
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be positive")

    user = grant_user_credits(user_id, req.amount, req.reason or "manual admin grant")
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {"user": user}


@app.post("/api/admin/users/{user_id}/editor")
async def admin_set_editor(user_id: int, req: EditorGrantRequest, username: str = Depends(verify_admin)):
    user = set_user_editor(user_id, req.is_editor)
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
        try:
            sync = k8s_client.get_image_sync_status(image["id"], image["image"])
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
        except Exception:
            logger.warning(
                "Failed to refresh sync status for image %s; using stale data",
                image["id"],
                exc_info=True,
            )
    return {"images": list_images(enabled_only=False)}


# Admin source types map to the head verb of the distribution chain.
_ADMIN_SOURCE_HEAD_KIND = {
    "acr_pull": "pull",
    "dockerhub_pull": "pull",
    "github_build": "build",
}

# The admin UI Select uses short labels ("registry", "github"); map them to the
# backend source kinds. "registry" -> acr_pull (same head verb + chain as a
# Docker Hub pull). Already-canonical values pass through unchanged.
_UI_SOURCE_TYPE_ALIASES = {
    "registry": "acr_pull",
    "github": "github_build",
}


def _normalize_source_type(source_type: str) -> str:
    return _UI_SOURCE_TYPE_ALIASES.get(source_type, source_type)


def _derive_admin_image_ref(source_type: str, source_ref: str, explicit_image: Optional[str]) -> str:
    """Resolve the runnable registry tag stored in images.image for an admin source.

    For pull sources source_ref IS a registry reference, so it is a valid tag. For github_build
    source_ref is a Dockerfile URL, which is NOT a runnable image tag — storing it would make the
    catalog row un-launchable and feed a URL to the build/distribute job as its ref. Derive a real
    tag under the custom-image registry instead (mirrors the user build path at build_custom_image).
    An explicit image always wins when provided.
    """
    explicit = (explicit_image or "").strip()
    if explicit:
        return explicit
    ref = (source_ref or "").strip()
    if source_type != "github_build":
        return ref
    # source_ref is a github.com/<org>/<repo>/blob/... Dockerfile URL; _github_repo_parts reads
    # org/repo straight from that path. raise 400 (not 500) if it is not a parseable GitHub URL.
    try:
        org, repo = _github_repo_parts(ref)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    slug = re.sub(r"[^a-z0-9-]+", "-", f"{org}-{repo}".lower()).strip("-") or "admin-build"
    digest = hashlib.md5(ref.encode()).hexdigest()[:8]
    return f"{settings.CUSTOM_IMAGE_LOCAL_TAG_PREFIX}/admin-{slug}:{digest}"


def _upsert_image_with_source(name, image, description, enabled, image_id, source_type, source_ref):
    """upsert_image carrying source_type/source_ref so the admin flow persists the source columns."""
    return upsert_image(
        name, image, description, enabled, image_id=image_id,
        source_type=source_type, source_ref=source_ref,
    )


def _acr_backup_target_ref(ref: str) -> Optional[str]:
    """Compute the ACR Enterprise backup ref for a catalog image: <registry>/<repo:tag>.

    Returns None when ACR_ENTERPRISE_REGISTRY is unset (the acr_backup job then fails fast)."""
    registry = (settings.ACR_ENTERPRISE_REGISTRY or "").strip().rstrip("/")
    if not registry:
        return None
    # Strip any existing registry host from the ref so we re-home it under the backup registry.
    repo = ref.strip()
    first = repo.split("/", 1)[0]
    if "." in first or ":" in first or first == "localhost":
        repo = repo.split("/", 1)[1] if "/" in repo else repo
    return f"{registry}/{repo}"


def enqueue_purge_fanout(ref: str, targets: list, *, digest: Optional[str] = None,
                         lan_target_ref: Optional[str] = None, image_id: Optional[int] = None,
                         blob_ids: Optional[list] = None) -> None:
    """P4 complete-delete: enqueue one INDEPENDENT job per byte-surface (§ six surfaces).

    Each is idempotent and — critically — its FAILURE is re-enqueued by requeue_failed_purges until
    it succeeds (the stale reaper only requeues LEASED jobs, so a reported failure would otherwise
    never retry and leave orphaned bytes). purge_meta (the DB-row cleanup) is enqueued too but the
    claim-gate holds it until every byte-surface for this ref has SUCCEEDED.

    digest + lan_target_ref ride in the payload (captured NOW) because the DB row may be deleted
    before/while the purge runs — the ref string + payload are the durable handles, not the row.
    Per-node surfaces carry the full targets[] LIST (the proven distribute/evict model) so the
    (kind,ref) dedup can't collapse them. image_id is linked only where the caller keeps the row
    until after enqueue (admin path); custom path passes image_id=None (row already deleted).

    blob_ids are this image's dfdaemon task-ids UNIQUE to it (blobs shared with a live image are
    excluded by the caller so we never delete a still-referenced blob). They ride on purge_p2p +
    purge_seed so the manager's dfctl-exec handlers know exactly what to `task rm`. Empty/None =>
    those surfaces skip the cache purge (nothing addressable; Dragonfly taskTTL reaps the tail)."""
    base = {"ref": ref, "digest": digest, "lan_target_ref": lan_target_ref}
    cache_base = {**base, "blob_ids": list(blob_ids or [])}
    enqueue_image_job(kind="purge_node", ref=ref, image_id=image_id, payload={**base, "scope": "all", "targets": targets})
    enqueue_image_job(kind="purge_p2p", ref=ref, image_id=image_id, payload={**cache_base, "scope": "all", "targets": targets})
    enqueue_image_job(kind="purge_seed", ref=ref, image_id=image_id, payload={**cache_base})
    enqueue_image_job(kind="registry_delete", ref=ref, image_id=image_id, payload={**base})
    enqueue_image_job(kind="purge_builder", ref=ref, image_id=image_id, payload={**base})
    enqueue_image_job(kind="purge_meta", ref=ref, image_id=image_id, payload={**base, "scope": "all", "targets": targets})


def _lan_registry_target_ref(ref: str) -> Optional[str]:
    """Compute the LAN-registry (zot) ref for an image: <LAN_REGISTRY>/<repo:tag>.

    Returns None when LAN_REGISTRY is unset, which is the signal that P1 push is dormant (no push
    step is chained and readiness gates on node-loaded rows only, exactly as before P1)."""
    registry = (settings.LAN_REGISTRY or "").strip().rstrip("/")
    if not registry:
        return None
    repo = ref.strip()
    first = repo.split("/", 1)[0]
    # Strip an existing registry host (has a dot/port or is localhost) so we re-home under zot.
    if "." in first or ":" in first or first == "localhost":
        repo = repo.split("/", 1)[1] if "/" in repo else repo
    return f"{registry}/{repo}"


def _enqueue_admin_image_chain(image_row: dict, source_type: str, source_ref: str) -> None:
    """Enqueue the non-blocking distribution chain for a source_type-backed admin image.

    Head verb: pull (acr_pull/dockerhub_pull) or build (github_build, Dockerfile fetched
    server-side). The chain then runs acr_backup and finally distribute scope=all. The chain is
    sequenced by the off-cluster daemon from the head job's payload `chain`; the Manager only
    enqueues the head and returns immediately so the single uvicorn worker never blocks.
    """
    head_kind = _ADMIN_SOURCE_HEAD_KIND[source_type]
    ref = image_row["image"]
    # acr_backup is OPTIONAL: only chain it when an enterprise backup registry is configured.
    # Otherwise (the common case — the source is already in ACR) it would hard-fail with
    # acr_registry_unset and abort the whole chain, leaving the image stuck "pulling".
    acr_target_ref = _acr_backup_target_ref(ref)
    # push is OPTIONAL too: only chain it when the LAN registry is wired. When set, push runs before
    # distribute so "ready" means the image is durable in zot. Until P3, distribute stays the
    # transport (it becomes warm in P3).
    lan_target_ref = _lan_registry_target_ref(ref)
    # P5 transport: when the LAN registry is wired, `push` puts the image in zot and the tail step is
    # `warm` (each node self-pulls the zot ref through its local Dragonfly dfdaemon → P2P). Without a
    # LAN registry (dormant/pre-P1), fall back to the SSH byte-push `distribute`. warm forks
    # distribute's exact target/results contract, so readiness counting is unchanged either way.
    transport = "warm" if lan_target_ref else "distribute"
    chain = (
        (["acr_backup"] if acr_target_ref else [])
        + (["push"] if lan_target_ref else [])
        + [transport]
    )
    payload = {
        "image_id": image_row["id"],
        "acr_target_ref": acr_target_ref,
        "lan_target_ref": lan_target_ref,
        "chain": chain,
        "scope": "all",
    }
    if head_kind == "build":
        payload["dockerfile"] = _fetch_github_dockerfile(source_ref)
    else:
        payload["source_ref"] = source_ref
    enqueue_image_job(kind=head_kind, ref=ref, image_id=image_row["id"], payload=payload)


@app.post("/api/admin/images")
async def admin_create_image(req: ImageRequest, username: str = Depends(verify_admin)):
    source_type = _normalize_source_type((getattr(req, "source_type", None) or "").strip())
    if source_type and settings.IMAGE_SERVICE_ENABLED:
        if source_type not in _ADMIN_SOURCE_HEAD_KIND:
            raise HTTPException(status_code=400, detail="Invalid source_type")
        source_ref = (getattr(req, "source_ref", None) or req.image or "").strip()
        if not source_ref:
            raise HTTPException(status_code=400, detail="source_ref is required")
        image_ref = _derive_admin_image_ref(source_type, source_ref, req.image)
        image = _upsert_image_with_source(
            req.name, image_ref, req.description or "", req.enabled, None,
            source_type, source_ref,
        )
        _enqueue_admin_image_chain(image, source_type, source_ref)
        return update_image_sync_status(image["id"], "distributing", 0, 0, "Distribution enqueued", False)

    # Legacy mode (image-service off): the admin form sends source_ref (the registry reference)
    # but no separate `image` field, so fall back to source_ref before requiring a value.
    image_ref = (req.image or getattr(req, "source_ref", None) or "").strip()
    if not image_ref:
        raise HTTPException(status_code=400, detail="image is required")
    image = upsert_image(req.name, image_ref, req.description or "", req.enabled)
    try:
        sync = k8s_client.sync_image_to_nodes(image["id"], image["image"])
    except ApiException as e:
        raise HTTPException(status_code=e.status or 502, detail=f"Image sync failed: {e.reason or str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
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
    source_type = _normalize_source_type((getattr(req, "source_type", None) or "").strip())
    if source_type and settings.IMAGE_SERVICE_ENABLED:
        if source_type not in _ADMIN_SOURCE_HEAD_KIND:
            raise HTTPException(status_code=400, detail="Invalid source_type")
        source_ref = (getattr(req, "source_ref", None) or req.image or "").strip()
        if not source_ref:
            raise HTTPException(status_code=400, detail="source_ref is required")
        image_ref = _derive_admin_image_ref(source_type, source_ref, req.image)
        image = _upsert_image_with_source(
            req.name, image_ref, req.description or "", req.enabled, image_id,
            source_type, source_ref,
        )
        if not image:
            raise HTTPException(status_code=404, detail="Image not found")
        _enqueue_admin_image_chain(image, source_type, source_ref)
        return update_image_sync_status(image["id"], "distributing", 0, 0, "Distribution enqueued", False)

    image_ref = (req.image or getattr(req, "source_ref", None) or "").strip()
    if not image_ref:
        raise HTTPException(status_code=400, detail="image is required")
    image = upsert_image(req.name, image_ref, req.description or "", req.enabled, image_id=image_id)
    if not image:
        raise HTTPException(status_code=404, detail="Image not found")
    try:
        sync = k8s_client.sync_image_to_nodes(image["id"], image["image"])
    except ApiException as e:
        raise HTTPException(status_code=e.status or 502, detail=f"Image sync failed: {e.reason or str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
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
    try:
        sync = k8s_client.sync_image_to_nodes(image["id"], image["image"])
    except ApiException as e:
        raise HTTPException(status_code=e.status or 502, detail=f"Image sync failed: {e.reason or str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
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
    if settings.IMAGE_SERVICE_ENABLED:
        existing = next((img for img in list_images(enabled_only=False) if img["id"] == image_id), None)
        if not existing:
            raise HTTPException(status_code=404, detail="Image not found")
        # The Manager resolves evict targets now (the daemon has no kubectl). Prefer the nodes
        # currently recorded as holding this ref; fall back to all eligible nodes when none.
        recorded = store.list_nodes_for_image(existing["image"])
        targets = k8s_client.resolve_node_targets(recorded) if recorded else k8s_client.resolve_node_targets(None)
        # P4 complete-delete fan-out (all six byte-surfaces) — GATED. Until PURGE_FANOUT_ENABLED is
        # flipped on (post-cutover, when nodes/seed run dfdaemon), keep the pre-P4 single `evict` so
        # production delete behaves exactly as before. digest read NOW; the subsequent delete_image()
        # NULLs image_jobs.image_id so the images DELETE won't FK-violate either way.
        if settings.PURGE_FANOUT_ENABLED:
            # Blobs unique to this image (row still present here — computed before delete_image below).
            unique_blobs = store.unique_blob_ids_for_ref(existing["image"])
            enqueue_purge_fanout(
                existing["image"],
                targets,
                digest=existing.get("digest"),
                lan_target_ref=_lan_registry_target_ref(existing["image"]),
                image_id=image_id,
                blob_ids=unique_blobs,
            )
        else:
            enqueue_image_job(kind="evict", ref=existing["image"], image_id=image_id,
                              payload={"scope": "all", "targets": targets})
        try:
            k8s_client.delete_image_sync(image_id)
        except Exception as e:
            logger.warning("delete_image_sync failed for image %s: %s", image_id, e)
        if not delete_image(image_id):
            raise HTTPException(status_code=404, detail="Image not found")
        return {"success": True}

    try:
        k8s_client.delete_image_sync(image_id)
    except ApiException as e:
        raise HTTPException(status_code=e.status or 502, detail=f"Image delete failed: {e.reason or str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
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
