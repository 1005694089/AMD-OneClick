"""
Configuration settings for AMD OneClick Notebook Manager
"""
import os
from typing import Optional


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


class Settings:
    K8S_NAMESPACE: str = os.getenv("K8S_NAMESPACE", "default")

    DEFAULT_IMAGE: str = os.getenv(
        "DEFAULT_IMAGE",
        "crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick-base:rocm7.2.1-py3.12-v20260416"
    )

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
    WORKSPACE_HOST_ROOT: str = os.getenv("WORKSPACE_HOST_ROOT", "/workspace/amd-oneclick")
    WORKSPACE_MOUNT_PATH: str = os.getenv("WORKSPACE_MOUNT_PATH", "/workspace")
    WORKSPACE_VOLUME_TYPE: str = os.getenv("WORKSPACE_VOLUME_TYPE", "hostPath")
    WORKSPACE_EMPTYDIR_SIZE_LIMIT: str = os.getenv("WORKSPACE_EMPTYDIR_SIZE_LIMIT", "")
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

    TELEMETRY_API_URL: str = os.getenv("TELEMETRY_API_URL", "")
    METRICS_INGEST_API_KEY: str = os.getenv("METRICS_INGEST_API_KEY", "")
    ONECLICK_TELEMETRY_ENABLED: bool = os.getenv("ONECLICK_TELEMETRY_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    ONECLICK_TELEMETRY_SOURCE: str = os.getenv("ONECLICK_TELEMETRY_SOURCE", "amd_oneclick")
    ONECLICK_TELEMETRY_PRODUCT: str = os.getenv("ONECLICK_TELEMETRY_PRODUCT", "radeon_cloud")


settings = Settings()
