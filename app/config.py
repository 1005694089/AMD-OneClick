"""
Configuration settings for AMD OneClick Notebook Manager
"""
import os
from typing import Optional
from urllib.parse import urlparse


INSTANCE_TYPES = {
    "jupyter": {
        "name": "Jupyter Notebook",
        "description": "GPU-powered Jupyter Lab for data science and AI development",
        "icon": "📓",
        "enabled": True,
        "max_lifetime_hours": None,
        "idle_timeout_minutes": None,
    },
    "opencode": {
        "name": "OpenCode",
        "description": "AI coding agent in terminal — launch opencode from Jupyter",
        "icon": "🤖",
        "enabled": True,
        "max_lifetime_hours": 2160,
        "idle_timeout_minutes": 0,
    },
    "openclaw": {
        "name": "OpenCLAW",
        "description": "Coming soon",
        "icon": "🔬",
        "enabled": False,
        "max_lifetime_hours": None,
        "idle_timeout_minutes": None,
    },
    "custom": {
        "name": "Custom Image",
        "description": "Run any image that serves on port 8888 using the image's own start command",
        "icon": "📦",
        "enabled": True,
        "max_lifetime_hours": None,
        "idle_timeout_minutes": None,
        # When true, the manager does NOT inject a start command; the image's own
        # ENTRYPOINT/CMD runs and is expected to listen on NOTEBOOK_PORT (8888).
        "image_defined_command": True,
    },
    "gradio": {
        "name": "Gradio App",
        "description": "Launch a Gradio app from a prepared image; opens at the app URL",
        "icon": "🎨",
        "enabled": True,
        "max_lifetime_hours": None,
        "idle_timeout_minutes": None,
        "app_kind": True,
    },
    "streamlit": {
        "name": "Streamlit App",
        "description": "Launch a Streamlit app from a prepared image; opens at the app URL",
        "icon": "📊",
        "enabled": True,
        "max_lifetime_hours": None,
        "idle_timeout_minutes": None,
        "app_kind": True,
    },
    "comfyui": {
        "name": "ComfyUI",
        "description": "Launch ComfyUI from a prepared image; opens at the app URL",
        "icon": "🧩",
        "enabled": True,
        "max_lifetime_hours": None,
        "idle_timeout_minutes": None,
        "app_kind": True,
    },
    "vllm": {
        "name": "vLLM API",
        "description": "Serve an OpenAI-compatible model API with vLLM",
        "icon": "🚀",
        "enabled": True,
        "max_lifetime_hours": None,
        "idle_timeout_minutes": None,
        "app_kind": True,
        "api_kind": True,
    },
    "sglang": {
        "name": "SGLang API",
        "description": "Serve an OpenAI-compatible model API with SGLang",
        "icon": "⚡",
        "enabled": True,
        "max_lifetime_hours": None,
        "idle_timeout_minutes": None,
        "app_kind": True,
        "api_kind": True,
    },
}

# Framework presets for "app" instance types. Each declares the default start
# command, the port the app listens on, the reverse-proxy mode, and which
# default app file is expected. Admins can override start_command / app_port
# per template; everything else is derived from the framework here.
#   proxy_mode: "preserve" keeps the /spaces/<id>/<port> prefix and the app is
#     made base-path-aware (Gradio root_path / Streamlit baseUrlPath via env).
#     "strip" removes the prefix before forwarding (for apps like ComfyUI that
#     cannot run under a sub-path); requires raw-path forwarding to keep %2F.
APP_FRAMEWORK_PRESETS = {
    "gradio": {
        "port": 7860,
        "proxy_mode": "preserve",
        "start_command": "python app.py",
    },
    "streamlit": {
        "port": 8501,
        "proxy_mode": "preserve",
        "start_command": "streamlit run app.py",
    },
    "comfyui": {
        "port": 8188,
        "proxy_mode": "strip",
        # ComfyUI is conventionally installed at /workspace/ComfyUI in prepared
        # images; run it there on the curated app port. Admins can override.
        "start_command": "bash -lc 'cd /workspace/ComfyUI 2>/dev/null || cd \"$WORKSPACE_DIR\"; exec python main.py --listen 0.0.0.0 --port 8188'",
    },
    # API (model-serving) kinds. The deliverable is an OpenAI-compatible endpoint,
    # not a UI: the ready screen shows base_url + api key + curl. The per-instance
    # API key is injected via api_key_env at launch. proxy_mode "strip" forwards
    # /spaces/<id>/<port>/v1/... to the server's /v1/... (API clients use the full
    # base_url we hand them, so there is no browser-absolute-URL problem).
    "vllm": {
        "port": 8000,
        "proxy_mode": "strip",
        "start_command": "vllm serve --host 0.0.0.0 --port 8000",
        "api_kind": True,
        "api_base_suffix": "/v1",
        "api_key_env": "VLLM_API_KEY",
    },
    "sglang": {
        "port": 30000,
        "proxy_mode": "strip",
        "start_command": "python -m sglang.launch_server --host 0.0.0.0 --port 30000",
        "api_kind": True,
        "api_base_suffix": "/v1",
        "api_key_env": "SGLANG_API_KEY",
    },
}


# Appended to every user-supplied custom Dockerfile (and usable to rebuild the base
# image) so that Jupyter + OpenCode are always present and on PATH. Kept as a module
# constant so the build-agent and the API share one definition.
# NOTE: the RUN --mount=type=cache directives below persist package caches (pip wheels and the
# npm cache) across builds on the node's buildkit store. Cache mounts are build-time only, so
# download caches are reused without baking cache directories into the image.
DOCKERFILE_SUFFIX = """
# --- AMD OneClick: auto-appended (Jupyter + OpenCode) ---
# JupyterLab provides the Jupyter server + Lab UI the workspace launches with. Try each pip
# variant in turn, but DO NOT swallow the final failure: the workspace cannot start without
# Jupyter, so a build that can't install it must fail here rather than be pushed as "ready"
# and then crash-loop at launch. Drop --no-cache-dir and mount a persistent pip cache so wheels
# are reused across builds.
RUN --mount=type=cache,target=/root/.cache/pip pip3 install jupyterlab || pip install jupyterlab || python3 -m pip install jupyterlab
# Hard gate: the image is only usable if `jupyter lab` is actually on PATH and runnable.
# This converts a silently-incomplete base (no working pip, missing deps) into a build failure.
RUN jupyter lab --version
# OpenCode is an opt-in convenience (launched from a terminal), NOT the workspace server, so it is
# installed BEST-EFFORT. The opencode.ai installer downloads its binary from github.com/Fastly,
# which has no domestic mirror and is intermittently throttled from the cn-shanghai build region —
# and the bare `curl | bash` had no timeout, so a stalled connect consumed the entire build
# watchdog budget (and the `|| npm` fallback never ran because curl never returned). Bound every
# network call: download to a file (so curl's exit code isn't masked by the pipe), hard curl
# timeouts + retries, an OUTER `timeout` ceiling per branch, real fallthrough to the npm mirror
# (Cloudflare, reachable), then a no-op so a transient outage degrades to "image without OpenCode"
# rather than failing the whole build. Version stays pinned on both paths.
# Note: Dockerfile RUN uses /bin/sh (dash), which lacks bash pipefail — we download the
# installer to a file (no pipe) so dash plain and/or chaining is sufficient and correct.
RUN --mount=type=cache,target=/root/.npm \
      ( timeout 300 sh -c 'curl -4 -fsSL --connect-timeout 10 --max-time 180 --retry 3 --retry-connrefused --retry-delay 2 -o /tmp/opencode-install.sh https://opencode.ai/install \
          && bash /tmp/opencode-install.sh --version 1.4.6' ) \
      || timeout 300 npm i -g opencode-ai@1.4.6 \
      || echo 'WARNING: OpenCode install failed; image will ship without it.'
RUN [ -x /root/.opencode/bin/opencode ] && ln -sf /root/.opencode/bin/opencode /usr/local/bin/opencode || true
# Best-effort verify — do NOT hard-fail the build on OpenCode (Jupyter above is the hard gate).
RUN opencode --version || echo 'WARNING: opencode not on PATH; continuing.'
ENV PATH="/root/.opencode/bin:${PATH}"
"""


class Settings:
    K8S_NAMESPACE: str = os.getenv("K8S_NAMESPACE", "default")

    DEFAULT_IMAGE: str = os.getenv(
        "DEFAULT_IMAGE",
        "crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick-base:rocm7.2.1-py3.12-v20260416"
    )
    IMAGE_PULL_SECRET_NAME: str = os.getenv("IMAGE_PULL_SECRET_NAME", "")

    @property
    def AVAILABLE_IMAGES(self) -> list:
        return [self.DEFAULT_IMAGE]

    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "/data/amd-oneclick.db")
    SESSION_SECRET: str = os.getenv("SESSION_SECRET", "change-me-for-production")

    GITHUB_CLIENT_ID: Optional[str] = os.getenv("GITHUB_CLIENT_ID")
    GITHUB_CLIENT_SECRET: Optional[str] = os.getenv("GITHUB_CLIENT_SECRET")
    GITHUB_REDIRECT_URI: Optional[str] = os.getenv("GITHUB_REDIRECT_URI")

    MODELSCOPE_CLIENT_ID: Optional[str] = os.getenv("MODELSCOPE_CLIENT_ID")
    MODELSCOPE_CLIENT_SECRET: Optional[str] = os.getenv("MODELSCOPE_CLIENT_SECRET")
    MODELSCOPE_AUTH_URL: str = os.getenv("MODELSCOPE_AUTH_URL", "https://modelscope.cn/oauth/authorize")
    MODELSCOPE_TOKEN_URL: str = os.getenv("MODELSCOPE_TOKEN_URL", "https://modelscope.cn/oauth/token")
    MODELSCOPE_USERINFO_URL: str = os.getenv("MODELSCOPE_USERINFO_URL", "https://modelscope.cn/api/v1/user")
    MODELSCOPE_REDIRECT_URI: Optional[str] = os.getenv("MODELSCOPE_REDIRECT_URI")

    NOTEBOOK_TOKEN: str = os.getenv("NOTEBOOK_TOKEN", "amd-oneclick")
    NOTEBOOK_PORT: int = 8888
    NOTEBOOK_LABEL_PREFIX: str = os.getenv("NOTEBOOK_LABEL_PREFIX", "amd-oneclick")
    NOTEBOOK_NODE_NAME: str = os.getenv("NOTEBOOK_NODE_NAME", "")
    NOTEBOOK_TOLERATION_KEY: str = os.getenv("NOTEBOOK_TOLERATION_KEY", "")
    NOTEBOOK_TOLERATION_VALUE: str = os.getenv("NOTEBOOK_TOLERATION_VALUE", "")
    NOTEBOOK_TOLERATION_EFFECT: str = os.getenv("NOTEBOOK_TOLERATION_EFFECT", "NoSchedule")

    # Spaces-style user app port forwarding. Each instance also exposes these
    # curated app ports; the manager proxies https://<host>/spaces/<id>/<port>/...
    # to the instance pod on that port. Gradio/Streamlit are auto-configured
    # (via env) to serve under that base path so `gradio app.py` / `streamlit run`
    # just work like local. Map name -> container port.
    SPACES_PATH_PREFIX: str = os.getenv("SPACES_PATH_PREFIX", "/spaces")
    APP_PORTS: dict = {"gradio": 7860, "streamlit": 8501, "comfyui": 8188, "app": 8000, "sglang": 30000}

    CPU_LIMIT: str = os.getenv("CPU_LIMIT", "16")
    MEMORY_LIMIT: str = os.getenv("MEMORY_LIMIT", "64Gi")
    GPU_LIMIT: str = os.getenv("GPU_LIMIT", "1")
    CPU_REQUEST: str = os.getenv("CPU_REQUEST", "8")
    MEMORY_REQUEST: str = os.getenv("MEMORY_REQUEST", "32Gi")

    # supplementalGroups for AMD GPU device access (video + render)
    GPU_SUPPLEMENTAL_GROUPS: list = [44, 109]

    # Host-level Hugging Face cache shared by model-sync jobs and notebook instances.
    HF_CACHE_HOST_PATH: str = os.getenv("HF_CACHE_HOST_PATH", "/var/lib/amd-oneclick/hf-cache")
    HF_CACHE_MOUNT_PATH: str = os.getenv("HF_CACHE_MOUNT_PATH", "/root/.cache/huggingface")
    # "emptyDir" (per-instance, reclaimed on pod deletion) or "hostPath" (shared node-local cache, persists).
    # Default emptyDir so the HF cache no longer leaks onto node local disk after instances are deleted.
    HF_CACHE_VOLUME_TYPE: str = os.getenv("HF_CACHE_VOLUME_TYPE", "emptyDir")
    HF_CACHE_EMPTYDIR_SIZE_LIMIT: str = os.getenv("HF_CACHE_EMPTYDIR_SIZE_LIMIT", "")
    HF_HUB_DISABLE_XET: str = os.getenv("HF_HUB_DISABLE_XET", "1")
    # HuggingFace mirror endpoint; pods have no direct egress to huggingface.co.
    HF_ENDPOINT: str = os.getenv("HF_ENDPOINT", "http://134.199.133.77")
    HF_TOKEN: str = os.getenv("HF_TOKEN", "")
    HF_TOKEN_SECRET_NAME: str = os.getenv("HF_TOKEN_SECRET_NAME", "")
    HF_TOKEN_SECRET_KEY: str = os.getenv("HF_TOKEN_SECRET_KEY", "HF_TOKEN")
    # PyPI mirror for in-pod pip installs (e.g. app requirements.txt at startup).
    PIP_INDEX_URL: str = os.getenv("PIP_INDEX_URL", "https://pypi.tuna.tsinghua.edu.cn/simple")
    WORKSPACE_HOST_ROOT: str = os.getenv("WORKSPACE_HOST_ROOT", "/workspace/amd-oneclick")
    WORKSPACE_MOUNT_PATH: str = os.getenv("WORKSPACE_MOUNT_PATH", "/workspace")
    WORKSPACE_VOLUME_TYPE: str = os.getenv("WORKSPACE_VOLUME_TYPE", "hostPath")
    WORKSPACE_EMPTYDIR_SIZE_LIMIT: str = os.getenv("WORKSPACE_EMPTYDIR_SIZE_LIMIT", "")
    # User-selectable workspace disk size (GiB) for the blank notebook launch.
    # The max scales with instance size; min is always DISK_SIZE_MIN_GB.
    DISK_SIZE_MIN_GB: int = int(os.getenv("DISK_SIZE_MIN_GB", "100"))
    DISK_SIZE_MAX_BY_GPU: dict = {1: 100, 2: 150, 4: 200}
    WORKSPACE_QUOTA_ENABLED: bool = os.getenv("WORKSPACE_QUOTA_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    WORKSPACE_QUOTA_SIZE_GI: int = int(os.getenv("WORKSPACE_QUOTA_SIZE_GI", "20"))
    WORKSPACE_QUOTA_NODE_NAME: str = os.getenv("WORKSPACE_QUOTA_NODE_NAME", "")
    WORKSPACE_QUOTA_IMAGE_ROOT: str = os.getenv("WORKSPACE_QUOTA_IMAGE_ROOT", "/workspace/amd-oneclick-quota-images")
    EPHEMERAL_STORAGE_REQUEST: str = os.getenv("EPHEMERAL_STORAGE_REQUEST", "")
    EPHEMERAL_STORAGE_LIMIT: str = os.getenv("EPHEMERAL_STORAGE_LIMIT", "")
    NETWORK_DISK_ENABLED: bool = os.getenv("NETWORK_DISK_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    NETWORK_DISK_PVC_NAME: str = os.getenv("NETWORK_DISK_PVC_NAME", "")
    NETWORK_DISK_MOUNT_PATH: str = os.getenv("NETWORK_DISK_MOUNT_PATH", "/network-workspace")
    NETWORK_DISK_SUBPATH_PREFIX: str = os.getenv("NETWORK_DISK_SUBPATH_PREFIX", "instances")
    NETWORK_DISK_SIZE_GI: int = int(os.getenv("NETWORK_DISK_SIZE_GI", "20"))
    NETWORK_DISK_PVC_PREFIX: str = os.getenv("NETWORK_DISK_PVC_PREFIX", "oneclick-network-disk")
    NETWORK_DISK_NFS_SERVER: str = os.getenv("NETWORK_DISK_NFS_SERVER", "")
    NETWORK_DISK_NFS_PATH_PREFIX: str = os.getenv("NETWORK_DISK_NFS_PATH_PREFIX", "/instances")
    NETWORK_DISK_SERVER_NODE_NAME: str = os.getenv("NETWORK_DISK_SERVER_NODE_NAME", "")
    NETWORK_DISK_IMAGE_HOST_ROOT: str = os.getenv("NETWORK_DISK_IMAGE_HOST_ROOT", "/workspace/oneclick-network-disk/images")
    NETWORK_DISK_EXPORT_HOST_ROOT: str = os.getenv("NETWORK_DISK_EXPORT_HOST_ROOT", "/workspace/oneclick-network-disk/export")

    IDLE_TIMEOUT_MINUTES: int = int(os.getenv("IDLE_TIMEOUT_MINUTES", "10"))
    MAX_LIFETIME_HOURS: int = int(os.getenv("MAX_LIFETIME_HOURS", "6"))

    SMTP_HOST: Optional[str] = os.getenv("SMTP_HOST")
    SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER: Optional[str] = os.getenv("SMTP_USER")
    SMTP_PASSWORD: Optional[str] = os.getenv("SMTP_PASSWORD")
    SMTP_FROM: str = os.getenv("SMTP_FROM", "noreply@amd-oneclick.local")

    SERVICE_HOST: str = os.getenv("SERVICE_HOST", "localhost")
    PUBLIC_BASE_URL: str = os.getenv("PUBLIC_BASE_URL", "")
    NODE_PORT_BASE: int = int(os.getenv("NODE_PORT_BASE", "30000"))
    NODE_PORT_MAX: int = int(os.getenv("NODE_PORT_MAX", "32767"))
    NODE_PORT_CLUSTER_SCAN_ENABLED: bool = os.getenv(
        "NODE_PORT_CLUSTER_SCAN_ENABLED",
        "true",
    ).lower() in {"1", "true", "yes", "on"}

    # Opt-in SSH access: only templates with ssh_enabled expose a second NodePort
    # -> pod:22. Auth is key-only (the launching user's public key from Profile is
    # injected; password login is disabled). The image must ship sshd.
    SSH_PORT: int = int(os.getenv("SSH_PORT", "22"))
    SSH_USERNAME: str = os.getenv("SSH_USERNAME", "root")
    # Host advertised in the ssh command; falls back to SERVICE_HOST when empty.
    SSH_HOST: str = os.getenv("SSH_HOST", "")

    # GitHub access goes through an in-cluster proxy on nodes that cannot reach
    # github.com directly. Defaults are the real GitHub hosts; on proxied envs set
    # these to the proxy (e.g. https://gh-test.anruicloud.com / https://gh-api-test.anruicloud.com).
    # Used for server-side OAuth token exchange, the GitHub REST API, git clone,
    # and raw file fetches (template preview). The browser-facing OAuth authorize
    # stays on github.com so the user's github.com session cookies are sent.
    GITHUB_API_BASE: str = os.getenv("GITHUB_API_BASE", "https://api.github.com").rstrip("/")
    GITHUB_WEB_BASE: str = os.getenv("GITHUB_WEB_BASE", "https://github.com").rstrip("/")
    GITHUB_AUTHORIZE_BASE: str = os.getenv("GITHUB_AUTHORIZE_BASE", "https://github.com").rstrip("/")

    PYPI_MIRROR: str = "https://pypi.tuna.tsinghua.edu.cn/simple"
    PYPI_HOST: str = "pypi.tuna.tsinghua.edu.cn"
    PYPI_HOST_IP: str = "101.6.15.130"

    ADMIN_PASSWORD: str = os.getenv("ADMIN_PASSWORD", "admin123")
    ADMIN_LOGIN_ENABLED: bool = os.getenv("ADMIN_LOGIN_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    ADMIN_LOGIN_CREDITS: int = int(os.getenv("ADMIN_LOGIN_CREDITS", "10000"))
    COUPON_PRIVATE_KEY_PEM: Optional[str] = os.getenv("COUPON_PRIVATE_KEY_PEM")
    COUPON_REDEEM_ENABLED: bool = os.getenv("COUPON_REDEEM_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    COUPON_REDEEM_DISABLED_MESSAGE: str = os.getenv(
        "COUPON_REDEEM_DISABLED_MESSAGE",
        "System maintenance is in progress. Credit redemption is temporarily unavailable. Please contact the administrator if you need credits.",
    )

    RUN_SCHEDULER: bool = os.getenv("RUN_SCHEDULER", "true").lower() in {"1", "true", "yes", "on"}
    OAUTH_CONNECT_TIMEOUT_SECONDS: float = float(os.getenv("OAUTH_CONNECT_TIMEOUT_SECONDS", "5"))
    OAUTH_READ_TIMEOUT_SECONDS: float = float(os.getenv("OAUTH_READ_TIMEOUT_SECONDS", "15"))
    SLOW_REQUEST_THRESHOLD_SECONDS: float = float(os.getenv("SLOW_REQUEST_THRESHOLD_SECONDS", "2"))
    WORKSHOP_LOGIN_ENABLED: bool = os.getenv("WORKSHOP_LOGIN_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    WORKSHOP_USER_COUNT: int = int(os.getenv("WORKSHOP_USER_COUNT", "150"))
    WORKSHOP_CREDITS: int = int(os.getenv("WORKSHOP_CREDITS", "10000"))

    HUGGINGFACE_DEMO_API_TOKENS: str = os.getenv("HUGGINGFACE_DEMO_API_TOKENS", os.getenv("HUGGINGFACE_API_TOKENS", ""))
    HUGGINGFACE_DEMO_MIN_CREDITS: int = int(
        os.getenv("HUGGINGFACE_DEMO_MIN_CREDITS", os.getenv("HUGGINGFACE_MIN_CREDITS", "48"))
    )
    TELEMETRY_API_URL: str = os.getenv("TELEMETRY_API_URL", "")
    METRICS_INGEST_API_KEY: str = os.getenv("METRICS_INGEST_API_KEY", "")
    ONECLICK_TELEMETRY_ENABLED: bool = os.getenv("ONECLICK_TELEMETRY_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    ONECLICK_TELEMETRY_SOURCE: str = os.getenv("ONECLICK_TELEMETRY_SOURCE", "amd_oneclick")
    ONECLICK_TELEMETRY_PRODUCT: str = os.getenv("ONECLICK_TELEMETRY_PRODUCT", "radeon_cloud")

    # OpenCode web (second in-pod service alongside Jupyter). The HTTP basic-auth password is a
    # per-instance HMAC so the web UI is never exposed unauthenticated on a NodePort.
    OPENCODE_WEB_PORT: int = int(os.getenv("OPENCODE_WEB_PORT", "4096"))
    OPENCODE_VERSION: str = os.getenv("OPENCODE_VERSION", "1.4.6")
    OPENCODE_WEB_USERNAME: str = os.getenv("OPENCODE_WEB_USERNAME", "opencode")
    # HMAC key for deriving per-instance OpenCode passwords. MUST be server-only: NOTEBOOK_TOKEN
    # is unusable here because it is embedded in user-facing Jupyter URLs, so any user could
    # re-derive every instance's password. Falls back to SESSION_SECRET (also server-only); set a
    # dedicated value in production so it does not share fate with the cookie-signing key.
    OPENCODE_PASSWORD_SECRET: str = os.getenv("OPENCODE_PASSWORD_SECRET") or SESSION_SECRET
    # Optional HTTPS origin for OpenCode. When set, OpenCode links go through the manager's
    # reverse proxy with signed handoff/session cookies instead of direct cleartext NodePorts.
    # When set, OpenCode is served through the manager proxy at this base URL (e.g. an AFD/ingress
    # host on 443). LEAVE EMPTY to serve OpenCode directly on its per-instance NodePort via
    # OPENCODE_NODEPORT_HOST/SERVICE_HOST — required when the public domain is fronted by Azure
    # Front Door, which only serves 443 and cannot reach the tls-proxy NodePort (e.g. :30450).
    OPENCODE_PUBLIC_BASE_URL: str = os.getenv("OPENCODE_PUBLIC_BASE_URL", "").strip().rstrip("/")
    # Host for direct OpenCode NodePort URLs. Defaults to SERVICE_HOST (the edge that forwards
    # raw NodePorts, same as the working Jupyter URLs). Override only if OpenCode's edge differs.
    OPENCODE_NODEPORT_HOST: str = os.getenv("OPENCODE_NODEPORT_HOST", "").strip()

    @property
    def OPENCODE_PUBLIC_HOST(self) -> str:
        if not self.OPENCODE_PUBLIC_BASE_URL:
            return ""
        return urlparse(self.OPENCODE_PUBLIC_BASE_URL).hostname or ""

    @property
    def OPENCODE_PUBLIC_PORT(self) -> Optional[int]:
        if not self.OPENCODE_PUBLIC_BASE_URL:
            return None
        return urlparse(self.OPENCODE_PUBLIC_BASE_URL).port
    # Appended to every custom-image build. The default uses upstream `curl | bash` installers
    # (opencode.ai, nousresearch.com) — a third-party supply-chain dependency. Operators who
    # want to remove that exposure can set DOCKERFILE_SUFFIX to a vendored, checksum-pinned
    # equivalent (e.g. COPY a verified installer from the build context) via the env var.
    DOCKERFILE_SUFFIX: str = os.getenv("DOCKERFILE_SUFFIX", "").strip() or DOCKERFILE_SUFFIX

    # Custom user image builds. Built on 0042 into containerd, then distributed to the GPU node.
    CUSTOM_IMAGE_REGISTRY: str = os.getenv(
        "CUSTOM_IMAGE_REGISTRY",
        "crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/radeon-cloud-user",
    )
    # The local tag prefix MUST be registry-qualified (host with a dot/port before the first "/").
    # A bare prefix like "amd-oneclick-custom" makes nerdctl/containerd normalize the ref to
    # docker.io/amd-oneclick-custom/...; `nerdctl save` then can't find the locally-built image
    # ("docker.io/...: not found") and the piped `ctr import` fails ("unrecognized image format").
    # Anchoring it under CUSTOM_IMAGE_REGISTRY (ACR host) keeps build/save/import/launch on one
    # exact, un-normalized ref. The image is node-local (never pushed to ACR); the host prefix is
    # only there to defeat docker.io normalization.
    CUSTOM_IMAGE_LOCAL_TAG_PREFIX: str = os.getenv(
        "CUSTOM_IMAGE_LOCAL_TAG_PREFIX",
        f"{CUSTOM_IMAGE_REGISTRY}/amd-oneclick-custom",
    ).strip("/")
    CUSTOM_IMAGE_MAX_PER_USER: int = int(os.getenv("CUSTOM_IMAGE_MAX_PER_USER", "1"))
    CUSTOM_IMAGE_BUILD_TIMEOUT_SECONDS: int = int(os.getenv("CUSTOM_IMAGE_BUILD_TIMEOUT_SECONDS", "1800"))
    CUSTOM_IMAGE_MAX_DOCKERFILE_BYTES: int = int(os.getenv("CUSTOM_IMAGE_MAX_DOCKERFILE_BYTES", "65536"))
    # Optional registry pull secret for legacy custom-registry rows. Node-local images need none.
    CUSTOM_IMAGE_PULL_SECRET_NAME: str = os.getenv("CUSTOM_IMAGE_PULL_SECRET_NAME", "")
    # Name of the dockerconfigjson secret the legacy ACR agent used to push (referenced for docs).
    ACR_PUSH_SECRET_NAME: str = os.getenv("ACR_PUSH_SECRET_NAME", "acr-push-secret")
    CUSTOM_IMAGE_GC_ENABLED: bool = os.getenv("CUSTOM_IMAGE_GC_ENABLED", "true").lower() in {"1", "true", "yes"}
    CUSTOM_IMAGE_GC_DISK_PATH: str = os.getenv("CUSTOM_IMAGE_GC_DISK_PATH", "/disk/ssd1/containerd")
    CUSTOM_IMAGE_GC_DISK_THRESHOLD_PERCENT: float = float(os.getenv("CUSTOM_IMAGE_GC_DISK_THRESHOLD_PERCENT", "85"))
    CUSTOM_IMAGE_GC_INTERVAL_SECONDS: int = int(os.getenv("CUSTOM_IMAGE_GC_INTERVAL_SECONDS", "300"))
    CUSTOM_IMAGE_GC_LAUNCH_GRACE_SECONDS: int = int(os.getenv("CUSTOM_IMAGE_GC_LAUNCH_GRACE_SECONDS", "21600"))

    # Internal build-agent channel (R9700 -> manager). Token guards /api/internal/builds/*.
    BUILD_AGENT_TOKEN: str = os.getenv("BUILD_AGENT_TOKEN", "")
    BUILD_AGENT_ALLOWED_IPS: list = [
        ip.strip() for ip in os.getenv("BUILD_AGENT_ALLOWED_IPS", "").split(",") if ip.strip()
    ]
    # Builds whose lease is older than this are reaped back to "failed" by the scheduler.
    CUSTOM_IMAGE_BUILD_LEASE_TIMEOUT_SECONDS: int = int(
        os.getenv("CUSTOM_IMAGE_BUILD_LEASE_TIMEOUT_SECONDS", "3600")
    )

    # Isolated Image Service — the sole image-management system. Image distribution goes through the
    # image_jobs queue consumed by the off-cluster daemon (save | ssh ctr import). The legacy prepull
    # DaemonSet / pull-probe path has been removed, so this is effectively always-on; the setting is
    # retained for one release as a safety toggle and defaults true.
    IMAGE_SERVICE_ENABLED: bool = os.getenv("IMAGE_SERVICE_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    # The Image-Service host is itself a labelled prepull node; node-target resolution must drop it
    # so it never receives distributions. Must exactly match its `kubectl get nodes` name.
    IMAGE_SERVICE_NODE_NAME: str = os.getenv("IMAGE_SERVICE_NODE_NAME", "")
    # ACR Enterprise registry used as the admin-image backup source of truth.
    ACR_ENTERPRISE_REGISTRY: str = os.getenv("ACR_ENTERPRISE_REGISTRY", "")
    # Optional Docker Hub pull secret for dockerhub_pull source images.
    DOCKERHUB_PULL_SECRET_NAME: str = os.getenv("DOCKERHUB_PULL_SECRET_NAME", "")
    # Optional proxy prefix for raw GitHub fetches. raw.githubusercontent.com is intermittently
    # throttled from the cn-shanghai region (~1/3 of fetches time out), so route through a GitHub
    # mirror: the fetch URL becomes f"{GITHUB_RAW_PROXY}{raw_url}". Default gh-proxy.org (Cloudflare,
    # returns 200 directly, no redirect). Set to "" to fetch raw.githubusercontent.com directly.
    GITHUB_RAW_PROXY: str = os.getenv("GITHUB_RAW_PROXY", "https://gh-proxy.org/").strip()
    # Hosts the server may fetch a raw Dockerfile from for github_build sources (SSRF allowlist).
    # Includes the proxy host so the SSRF guard accepts the proxied URL.
    GITHUB_RAW_ALLOWED_HOSTS: set = {
        h.strip() for h in os.getenv(
            "GITHUB_RAW_ALLOWED_HOSTS", "raw.githubusercontent.com,gh-proxy.org"
        ).split(",") if h.strip()
    }
    GITHUB_DOCKERFILE_FETCH_TIMEOUT_SECONDS: int = int(os.getenv("GITHUB_DOCKERFILE_FETCH_TIMEOUT_SECONDS", "10"))
    # Retries for the raw Dockerfile fetch (the proxy/github can still blip intermittently).
    GITHUB_DOCKERFILE_FETCH_RETRIES: int = int(os.getenv("GITHUB_DOCKERFILE_FETCH_RETRIES", "3"))
    # An image is considered outdated (eligible for node eviction) this many days after its last launch.
    IMAGE_OUTDATED_DAYS: int = int(os.getenv("IMAGE_OUTDATED_DAYS", "5"))
    # image_jobs whose lease is older than this are reaped back to pending (or failed) by the scheduler.
    JOB_LEASE_TIMEOUT_SECONDS: int = int(os.getenv("JOB_LEASE_TIMEOUT_SECONDS", "3600"))
    # The daemon refuses a distribute job when the target node's containerd-root free space is below this.
    IMAGE_NODE_MIN_FREE_DISK_GB: int = int(os.getenv("IMAGE_NODE_MIN_FREE_DISK_GB", "50"))
    # A node whose containerd wedged (import timed out / liveness probe failed) is quarantined for this
    # long: the reaper will not requeue distribute/evict onto it and launches route around it. After the
    # window expires it becomes eligible again (recovery may have cleared the wedge in the meantime).
    NODE_QUARANTINE_SECONDS: int = int(os.getenv("NODE_QUARANTINE_SECONDS", "1800"))


settings = Settings()
