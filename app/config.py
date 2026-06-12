"""
Configuration settings for AMD OneClick Notebook Manager
"""
import os
from typing import Optional
from urllib.parse import urlparse


def _normalize_path_prefix(value: str) -> str:
    prefix = (value or "").strip()
    if not prefix or prefix == "/":
        return ""
    return "/" + prefix.strip("/")


def _path_prefix_from_public_base_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith("/"):
        return urlparse(raw).path
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    return parsed.path


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
}


# Appended to every user-supplied custom Dockerfile (and usable to rebuild the base
# image) so that OpenCode + Hermes are always present and on PATH. Kept as a module
# constant so the build-agent and the API share one definition.
DOCKERFILE_SUFFIX = """
# --- AMD OneClick: auto-appended (Jupyter + OpenCode + Hermes) ---
# JupyterLab provides the Jupyter server + Lab UI the workspace launches with. Best-effort
# across pip variants; harmless/idempotent if the base image already ships Jupyter.
RUN pip3 install --no-cache-dir jupyterlab || pip install --no-cache-dir jupyterlab || python3 -m pip install --no-cache-dir jupyterlab || true
# Pin the OpenCode version so the installer skips its "fetch latest version" network call,
# which intermittently fails in the build sandbox and silently left OpenCode uninstalled.
# Symlink into /usr/local/bin so `opencode` is on the default PATH (survives login shells).
RUN curl -fsSL https://opencode.ai/install | bash -s -- --version 1.16.2 || npm i -g opencode-ai@latest || true
RUN ln -sf /root/.opencode/bin/opencode /usr/local/bin/opencode 2>/dev/null || true
RUN curl -fsSL https://hermes-agent.nousresearch.com/install.sh | bash || true
ENV PATH="/root/.opencode/bin:/root/.hermes/bin:${PATH}"
"""


class Settings:
    K8S_NAMESPACE: str = os.getenv("K8S_NAMESPACE", "default")

    ENTERPRISE_REGISTRY_HOST: str = os.getenv(
        "ENTERPRISE_REGISTRY_HOST",
        "radeon-cloud-registry.cn-shanghai.cr.aliyuncs.com",
    ).rstrip("/")
    ADMIN_IMAGE_REGISTRY: str = os.getenv("ADMIN_IMAGE_REGISTRY", f"{ENTERPRISE_REGISTRY_HOST}/admin").rstrip("/")
    ADMIN_IMAGE_UPLOAD_REPOSITORY: str = os.getenv(
        "ADMIN_IMAGE_UPLOAD_REPOSITORY",
        f"{ENTERPRISE_REGISTRY_HOST}/admin/radeon-admin-upload",
    ).rstrip("/")

    DEFAULT_IMAGE: str = os.getenv(
        "DEFAULT_IMAGE",
        f"{ENTERPRISE_REGISTRY_HOST}/admin/amd-oneclick-base:rocm7.2.1-py3.12-v20260416",
    )

    @property
    def AVAILABLE_IMAGES(self) -> list:
        return [self.DEFAULT_IMAGE]

    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "/data/amd-oneclick.db")
    DATABASE_POOL_SIZE: int = int(os.getenv("DATABASE_POOL_SIZE", "10"))
    DATABASE_MAX_OVERFLOW: int = int(os.getenv("DATABASE_MAX_OVERFLOW", "10"))
    DATABASE_POOL_TIMEOUT_SECONDS: int = int(os.getenv("DATABASE_POOL_TIMEOUT_SECONDS", "30"))
    DATABASE_POOL_RECYCLE_SECONDS: int = int(os.getenv("DATABASE_POOL_RECYCLE_SECONDS", "1800"))
    SESSION_SECRET: str = os.getenv("SESSION_SECRET", "change-me-for-production")
    SSO_ENABLED: bool = os.getenv("SSO_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    SSO_PUBLIC_KEY_PEM: Optional[str] = os.getenv("SSO_PUBLIC_KEY_PEM")
    SSO_ISSUER: str = os.getenv("SSO_ISSUER", "")
    SSO_AUDIENCE: str = os.getenv("SSO_AUDIENCE", "")
    SSO_ALGORITHM: str = os.getenv("SSO_ALGORITHM", "RS256")
    SSO_ACCESS_COOKIE_NAME: str = os.getenv("SSO_ACCESS_COOKIE_NAME", "sso_access_token")
    SSO_REFRESH_COOKIE_NAME: str = os.getenv("SSO_REFRESH_COOKIE_NAME", "sso_refresh_token")
    SSO_REFRESH_THRESHOLD_SECONDS: int = int(os.getenv("SSO_REFRESH_THRESHOLD_SECONDS", "300"))
    SSO_REFRESH_URL: str = os.getenv("SSO_REFRESH_URL", "/apitest/api/auth/refresh")
    SSO_LOGOUT_URL: str = os.getenv("SSO_LOGOUT_URL", "/apitest/api/auth/logout")
    SSO_BIND_ENTRY_URL: str = os.getenv("SSO_BIND_ENTRY_URL", "https://aideveloperportal.anruicloud.com/login?returnUrl=")
    SSO_BIND_RETURN_QUERY_KEY: str = os.getenv("SSO_BIND_RETURN_QUERY_KEY", "bind")
    SSO_AUTO_CREATE_USER: bool = os.getenv("SSO_AUTO_CREATE_USER", "true").lower() in {"1", "true", "yes", "on"}
    SSO_DEFAULT_USER_DOMAIN: str = os.getenv("SSO_DEFAULT_USER_DOMAIN", "developer.local")
    SSO_CLAIM_NAME_URI: str = os.getenv(
        "SSO_CLAIM_NAME_URI",
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name",
    )
    SSO_CLAIM_EMAIL_URI: str = os.getenv(
        "SSO_CLAIM_EMAIL_URI",
        "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress",
    )

    REDIS_URL: str = os.getenv("REDIS_URL", "")
    REDIS_TOKEN_VERSION_KEY_PREFIX: str = os.getenv("REDIS_TOKEN_VERSION_KEY_PREFIX", "auth:user")

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
    HF_HUB_DISABLE_XET: str = os.getenv("HF_HUB_DISABLE_XET", "1")
    WORKSPACE_HOST_ROOT: str = os.getenv("WORKSPACE_HOST_ROOT", "/workspace/amd-oneclick")
    WORKSPACE_MOUNT_PATH: str = os.getenv("WORKSPACE_MOUNT_PATH", "/workspace")
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
    PUBLIC_PATH_PREFIX: str = _normalize_path_prefix(
        os.getenv("PUBLIC_PATH_PREFIX", "")
        or _path_prefix_from_public_base_url(os.getenv("PUBLIC_BASE_URL", ""))
    )
    NODE_PORT_BASE: int = int(os.getenv("NODE_PORT_BASE", "30000"))
    POD_CREATE_RETRY_ATTEMPTS: int = int(os.getenv("POD_CREATE_RETRY_ATTEMPTS", "30"))
    POD_CREATE_RETRY_DELAY_SECONDS: int = int(os.getenv("POD_CREATE_RETRY_DELAY_SECONDS", "5"))

    PYPI_MIRROR: str = "https://pypi.tuna.tsinghua.edu.cn/simple"
    PYPI_HOST: str = "pypi.tuna.tsinghua.edu.cn"
    PYPI_HOST_IP: str = "101.6.15.130"

    ADMIN_PASSWORD: str = os.getenv("ADMIN_PASSWORD", "admin123")
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
    IMAGE_CACHE_NODE_AFFINITY_ENABLED: bool = os.getenv("IMAGE_CACHE_NODE_AFFINITY_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    K8S_READ_TIMEOUT_SECONDS: float = float(os.getenv("K8S_READ_TIMEOUT_SECONDS", "5"))
    WORKSHOP_LOGIN_ENABLED: bool = os.getenv("WORKSHOP_LOGIN_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    WORKSHOP_USER_COUNT: int = int(os.getenv("WORKSHOP_USER_COUNT", "150"))
    WORKSHOP_CREDITS: int = int(os.getenv("WORKSHOP_CREDITS", "10000"))

    TELEMETRY_API_URL: str = os.getenv("TELEMETRY_API_URL", "")
    METRICS_INGEST_API_KEY: str = os.getenv("METRICS_INGEST_API_KEY", "")
    ONECLICK_TELEMETRY_ENABLED: bool = os.getenv("ONECLICK_TELEMETRY_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    ONECLICK_TELEMETRY_SOURCE: str = os.getenv("ONECLICK_TELEMETRY_SOURCE", "amd_oneclick")
    ONECLICK_TELEMETRY_PRODUCT: str = os.getenv("ONECLICK_TELEMETRY_PRODUCT", "radeon_cloud")

    # OpenCode web (second in-pod service alongside Jupyter). Reuses NOTEBOOK_TOKEN as the
    # HTTP basic-auth password so the web UI is never exposed unauthenticated on a NodePort.
    OPENCODE_WEB_PORT: int = int(os.getenv("OPENCODE_WEB_PORT", "4096"))
    OPENCODE_WEB_USERNAME: str = os.getenv("OPENCODE_WEB_USERNAME", "opencode")
    DOCKERFILE_SUFFIX: str = DOCKERFILE_SUFFIX

    # Custom user image builds (built off-cluster by the R9700 build-agent, pushed to ACR).
    CUSTOM_IMAGE_REGISTRY: str = os.getenv(
        "CUSTOM_IMAGE_REGISTRY",
        f"{ENTERPRISE_REGISTRY_HOST}/cloud_user",
    ).rstrip("/")
    # Legacy compatibility: no longer used for new user build tags. New user
    # builds always use CUSTOM_IMAGE_REGISTRY/user-XX-<name>:latest.
    CUSTOM_IMAGE_REPOSITORY: str = os.getenv("CUSTOM_IMAGE_REPOSITORY", "").rstrip("/")

    CUSTOM_IMAGE_MAX_PER_USER: int = int(os.getenv("CUSTOM_IMAGE_MAX_PER_USER", "2"))
    CUSTOM_IMAGE_BUILD_TIMEOUT_SECONDS: int = int(os.getenv("CUSTOM_IMAGE_BUILD_TIMEOUT_SECONDS", "1800"))
    CUSTOM_IMAGE_MAX_DOCKERFILE_BYTES: int = int(os.getenv("CUSTOM_IMAGE_MAX_DOCKERFILE_BYTES", "65536"))
    # Registry pull secret referenced by manager, notebook, and prepull pods for the
    # enterprise ACR host. Empty means "assume nodes can already pull".
    CUSTOM_IMAGE_PULL_SECRET_NAME: str = os.getenv("CUSTOM_IMAGE_PULL_SECRET_NAME", "acr-enterprise-pull")
    IMAGE_PULL_SECRET_REGISTRY_HOSTS: list = [
        host.strip().strip("/")
        for host in os.getenv("IMAGE_PULL_SECRET_REGISTRY_HOSTS", ENTERPRISE_REGISTRY_HOST).split(",")
        if host.strip()
    ]
    # Name of the dockerconfigjson secret the R9700 agent uses to push (referenced for docs).
    ACR_PUSH_SECRET_NAME: str = os.getenv("ACR_PUSH_SECRET_NAME", "acr-push-secret")

    # Internal build-agent channel (R9700 -> manager). Token guards /api/internal/builds/*.
    BUILD_AGENT_TOKEN: str = os.getenv("BUILD_AGENT_TOKEN", "")
    BUILD_AGENT_ALLOWED_IPS: list = [
        ip.strip() for ip in os.getenv("BUILD_AGENT_ALLOWED_IPS", "").split(",") if ip.strip()
    ]
    # Builds whose lease is older than this are reaped back to "failed" by the scheduler.
    CUSTOM_IMAGE_BUILD_LEASE_TIMEOUT_SECONDS: int = int(
        os.getenv("CUSTOM_IMAGE_BUILD_LEASE_TIMEOUT_SECONDS", "3600")
    )

    OSS_ENABLED: bool = os.getenv("OSS_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    OSS_BUCKET: str = os.getenv("OSS_BUCKET", "").strip()
    OSS_ENDPOINT: str = os.getenv("OSS_ENDPOINT", "oss-cn-shanghai-internal.aliyuncs.com").strip()
    OSS_REGION: str = os.getenv("OSS_REGION", "cn-shanghai").strip()
    OSS_BACKUP_PREFIX: str = os.getenv("OSS_BACKUP_PREFIX", "cloud_user").strip().strip("/")
    OSS_INSTANCE_QUOTA_GB: int = int(os.getenv("OSS_INSTANCE_QUOTA_GB", "10"))
    OSS_BACKUP_INTERVAL_MINUTES: int = int(os.getenv("OSS_BACKUP_INTERVAL_MINUTES", "10"))
    OSS_IDLE_EXPIRY_DAYS: int = int(os.getenv("OSS_IDLE_EXPIRY_DAYS", "15"))
    OSS_IDLE_REAPER_DRY_RUN: bool = os.getenv("OSS_IDLE_REAPER_DRY_RUN", "true").lower() in {"1", "true", "yes", "on"}
    OSS_STS_ROLE_ARN: str = os.getenv("OSS_STS_ROLE_ARN", "").strip()
    OSS_STS_DURATION_SECONDS: int = int(os.getenv("OSS_STS_DURATION_SECONDS", "7200"))
    OSS_STS_REFRESH_INTERVAL_MINUTES: int = int(os.getenv("OSS_STS_REFRESH_INTERVAL_MINUTES", "45"))
    OSS_RAM_ACCESS_KEY_ID: str = os.getenv("OSS_RAM_ACCESS_KEY_ID", "").strip()
    OSS_RAM_ACCESS_KEY_SECRET: str = os.getenv("OSS_RAM_ACCESS_KEY_SECRET", "").strip()
    OSSUTIL_IMAGE: str = os.getenv("OSSUTIL_IMAGE", f"{ADMIN_IMAGE_REGISTRY}/ossutil:1.7.19")
    OSS_TERMINATION_GRACE_PERIOD_SECONDS: int = int(os.getenv("OSS_TERMINATION_GRACE_PERIOD_SECONDS", "45"))
    OSS_SECRET_DELETE_WAIT_SECONDS: int = int(
        os.getenv("OSS_SECRET_DELETE_WAIT_SECONDS", str(OSS_TERMINATION_GRACE_PERIOD_SECONDS + 30))
    )
    OSS_EXCLUDES: list = [
        item.strip()
        for item in os.getenv(
            "OSS_EXCLUDES",
            ".git,.cache,__pycache__,node_modules,.venv,venv,env,datasets,*.pt,*.pth,*.safetensors,*.bin,*.gguf",
        ).split(",")
        if item.strip()
    ]


settings = Settings()


def validate_settings() -> None:
    if not settings.SSO_ENABLED:
        return
    missing = []
    if not settings.SSO_PUBLIC_KEY_PEM:
        missing.append("SSO_PUBLIC_KEY_PEM")
    if not settings.SSO_ISSUER:
        missing.append("SSO_ISSUER")
    if not settings.SSO_AUDIENCE:
        missing.append("SSO_AUDIENCE")
    if missing:
        raise RuntimeError(f"SSO is enabled but missing required env vars: {', '.join(missing)}")
