"""
FastAPI main application for AMD OneClick Notebook Manager
"""
import hashlib
import json
import logging
import os
import base64
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Depends, Query, Request, Response, Cookie, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.middleware.sessions import SessionMiddleware
import secrets
import httpx
import requests
import websockets

from .config import settings, INSTANCE_TYPES
from .models import (
    NotebookRequest, 
    NotebookStatus, 
    AdminListResponse, 
    NotebookListItem,
    DestroyResponse,
    ImageRequest,
)
from .k8s_client import k8s_client
from .email_service import send_notebook_url_email
from .scheduler import start_scheduler, stop_scheduler
from .store import (
    delete_image,
    get_active_instance_for_user,
    get_charged_credits_for_instance,
    get_image_by_value,
    get_or_create_user,
    get_user,
    init_db,
    list_images,
    mark_instance_deleted,
    record_instance,
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
    start_scheduler()
    yield
    # Shutdown
    logger.info("Shutting down AMD OneClick Notebook Manager")
    stop_scheduler()


app = FastAPI(
    title="AMD OneClick Notebook Manager",
    description="Kubernetes-based Jupyter Notebook instance management",
    version="1.0.0",
    lifespan=lifespan
)
app.add_middleware(SessionMiddleware, secret_key=settings.SESSION_SECRET)

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


def _oauth_redirect_uri(request: Request, provider: str) -> str:
    configured = settings.GITHUB_REDIRECT_URI if provider == "github" else settings.MODELSCOPE_REDIRECT_URI
    if configured:
        return configured
    return str(request.url_for(f"{provider}_callback"))


def _oauth_state(request: Request, provider: str) -> str:
    state = secrets.token_urlsafe(24)
    request.session[f"{provider}_oauth_state"] = state
    return state


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


# =============================================================================
# User Endpoints
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the main request page"""
    user = get_user(int(request.session["user_id"])) if request.session.get("user_id") else None
    images = list_images(enabled_only=True)
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "images": [img["image"] for img in images],
            "image_catalog_json": json.dumps(images),
            "default_image": settings.DEFAULT_IMAGE,
            "instance_types_json": json.dumps(INSTANCE_TYPES),
            "user_json": json.dumps(user or {}),
        },
    )


@app.get("/profile", response_class=HTMLResponse)
async def profile_page(request: Request):
    """Render user profile and login page."""
    user = get_user(int(request.session["user_id"])) if request.session.get("user_id") else None
    active_instance = get_active_instance_for_user(user["id"]) if user else None
    if active_instance:
        created_at = datetime.fromisoformat(active_instance["created_at"])
        now = datetime.now(timezone.utc)
        runtime_seconds = max(0, int((now - created_at).total_seconds()))
        active_instance["runtime_minutes"] = runtime_seconds // 60
        active_instance["runtime_hours_display"] = round(runtime_seconds / 3600, 2)
        active_instance["credits_consumed"] = get_charged_credits_for_instance(active_instance["instance_id"])
    return templates.TemplateResponse(
        request,
        "profile.html",
        {
            "user": user,
            "active_instance": active_instance,
            "github_enabled": bool(settings.GITHUB_CLIENT_ID),
            "modelscope_enabled": bool(settings.MODELSCOPE_CLIENT_ID),
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
    token_resp = requests.post(
        "https://github.com/login/oauth/access_token",
        headers={"Accept": "application/json"},
        data={
            "client_id": settings.GITHUB_CLIENT_ID,
            "client_secret": settings.GITHUB_CLIENT_SECRET,
            "code": code,
            "redirect_uri": _oauth_redirect_uri(request, "github"),
        },
        timeout=20,
    )
    token = token_resp.json().get("access_token")
    if not token:
        raise HTTPException(status_code=400, detail="GitHub OAuth token exchange failed")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    profile = requests.get("https://api.github.com/user", headers=headers, timeout=20).json()
    emails = requests.get("https://api.github.com/user/emails", headers=headers, timeout=20).json()
    email = profile.get("email") or next((e["email"] for e in emails if e.get("primary")), None)
    if not email:
        raise HTTPException(status_code=400, detail="GitHub account has no accessible email")
    user = get_or_create_user("github", str(profile["id"]), email, profile.get("name") or profile.get("login") or "", profile.get("avatar_url") or "")
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
        token_resp = requests.post(
            settings.MODELSCOPE_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "client_id": settings.MODELSCOPE_CLIENT_ID,
                "client_secret": settings.MODELSCOPE_CLIENT_SECRET,
                "code": code,
                "redirect_uri": _oauth_redirect_uri(request, "modelscope"),
            },
            timeout=20,
        )
    except requests.RequestException as e:
        logger.error("ModelScope OAuth token request failed: %s", e)
        raise HTTPException(status_code=502, detail="ModelScope OAuth token request failed")
    try:
        token_data = token_resp.json()
    except Exception:
        logger.error("ModelScope token response is not JSON: status=%s body=%s", token_resp.status_code, token_resp.text[:500])
        raise HTTPException(status_code=400, detail="ModelScope OAuth token exchange failed")
    token = token_data.get("access_token")
    if not token:
        raise HTTPException(status_code=400, detail="ModelScope OAuth token exchange failed")
    try:
        profile_resp = requests.get(settings.MODELSCOPE_USERINFO_URL, headers={"Authorization": f"Bearer {token}"}, timeout=20)
    except requests.RequestException as e:
        logger.warning("ModelScope userinfo request failed: %s", e)
        profile_resp = None
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
    request.session["user_id"] = user["id"]
    return RedirectResponse("/")


@app.get("/auth/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/")


@app.get("/api/me")
async def api_me(user: dict = Depends(current_user)):
    return user


@app.post("/api/notebook/request", response_model=NotebookStatus)
async def request_notebook(req: NotebookRequest, user: dict = Depends(current_user)):
    """Request a notebook instance"""
    email = user["email"].lower()
    image = req.image or settings.DEFAULT_IMAGE
    instance_type = req.instance_type or "jupyter"
    gpu_count = req.gpu_count or 1

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
        )
        record_instance(
            user["id"], email, instance["id"], image, instance_type, gpu_count, instance.get("node_port")
        )

        if instance.get("url"):
            send_notebook_url_email(email, instance["url"])

        return NotebookStatus(
            status="allocating",
            message="Allocating resources for your instance...",
            url=instance.get("url"),
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
        
        status = k8s_client.get_pod_status(email, instance_id=active["instance_id"])
        
        status_messages = {
            "ready": "Your notebook is ready!",
            "running": "Container is running, starting Jupyter...",
            "jupyter_starting": "Jupyter is starting up...",
            "pending": "Waiting for resources...",
            "initializing": "Initializing notebook environment...",
            "loading": "Loading notebook image...",
            "failed": "Notebook creation failed",
            "unknown": "Checking status..."
        }
        
        return NotebookStatus(
            status=status or "unknown",
            message=status_messages.get(status, "Checking status..."),
            url=instance.get("url"),
            email=email
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
            url=existing.get("url"),
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
            url=instance.get("url"),
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
        
        status = k8s_client.get_pod_status("", instance_id=instance_id)
        
        # Normalize status for frontend - 'ready' means 'running' and ready to use
        if status == "ready":
            status = "running"
        
        status_messages = {
            "running": "Your notebook is ready!",
            "jupyter_starting": "Jupyter is starting up...",
            "pending": "Waiting for resources...",
            "initializing": "Initializing notebook environment...",
            "loading": "Loading notebook image...",
            "failed": "Notebook creation failed",
            "unknown": "Checking status..."
        }
        
        return NotebookStatus(
            status=status or "unknown",
            message=status_messages.get(status, "Checking status..."),
            url=instance.get("url"),
            instance_id=instance_id
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
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
        upstream = await client.request(
            request.method,
            target_url,
            headers=_proxy_headers(request.headers),
            content=body,
        )

    response_headers = {
        k: v for k, v in upstream.headers.items()
        if k.lower() not in {"content-encoding", "transfer-encoding", "connection", "content-length"}
    }
    if "location" in response_headers:
        response_headers["location"] = _rewrite_location(response_headers["location"], instance_id, target_base)
    return Response(content=upstream.content, status_code=upstream.status_code, headers=response_headers)


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

        async with websockets.connect(target_url, additional_headers=headers, open_timeout=10) as upstream:
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
                url=inst.get("url", ""),
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


@app.get("/api/admin/images")
async def admin_list_images(username: str = Depends(verify_admin)):
    for image in list_images(enabled_only=False):
        sync = k8s_client.get_image_sync_status(image["id"])
        if image.get("sync_status") != sync["status"] or image.get("ready_count") != sync["ready_count"]:
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
