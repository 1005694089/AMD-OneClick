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
    HF_ENDPOINT: str = os.getenv("HF_ENDPOINT", "")
    HF_TOKEN: str = os.getenv("HF_TOKEN", "")
    HF_TOKEN_SECRET_NAME: str = os.getenv("HF_TOKEN_SECRET_NAME", "")
    HF_TOKEN_SECRET_KEY: str = os.getenv("HF_TOKEN_SECRET_KEY", "HF_TOKEN")
    HF_HUB_DISABLE_XET: str = os.getenv("HF_HUB_DISABLE_XET", "1")
    IMAGE_PREPULL_ENABLED: bool = os.getenv("IMAGE_PREPULL_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    IMAGE_PULL_PROBE_ENABLED: bool = os.getenv("IMAGE_PULL_PROBE_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    IMAGE_PULL_PROBE_DEADLINE_SECONDS: int = int(os.getenv("IMAGE_PULL_PROBE_DEADLINE_SECONDS", "7200"))
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
    NODE_PORT_MAX: int = int(os.getenv("NODE_PORT_MAX", "32767"))
    NODE_PORT_CLUSTER_SCAN_ENABLED: bool = os.getenv(
        "NODE_PORT_CLUSTER_SCAN_ENABLED",
        "true",
    ).lower() in {"1", "true", "yes", "on"}

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

    # OpenCode web (second in-pod service alongside Jupyter). Reuses NOTEBOOK_TOKEN as the
    # HTTP basic-auth password so the web UI is never exposed unauthenticated on a NodePort.
    OPENCODE_WEB_PORT: int = int(os.getenv("OPENCODE_WEB_PORT", "4096"))
    OPENCODE_WEB_USERNAME: str = os.getenv("OPENCODE_WEB_USERNAME", "opencode")
    # Appended to every custom-image build. The default uses upstream `curl | bash` installers
    # (opencode.ai, nousresearch.com) — a third-party supply-chain dependency. Operators who
    # want to remove that exposure can set DOCKERFILE_SUFFIX to a vendored, checksum-pinned
    # equivalent (e.g. COPY a verified installer from the build context) via the env var.
    DOCKERFILE_SUFFIX: str = os.getenv("DOCKERFILE_SUFFIX", "").strip() or DOCKERFILE_SUFFIX

    # Custom user image builds (built off-cluster by the R9700 build-agent, pushed to ACR).
    CUSTOM_IMAGE_REGISTRY: str = os.getenv(
        "CUSTOM_IMAGE_REGISTRY",
        "crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com/radeon-cloud-user",
    )
    CUSTOM_IMAGE_MAX_PER_USER: int = int(os.getenv("CUSTOM_IMAGE_MAX_PER_USER", "1"))
    CUSTOM_IMAGE_BUILD_TIMEOUT_SECONDS: int = int(os.getenv("CUSTOM_IMAGE_BUILD_TIMEOUT_SECONDS", "1800"))
    CUSTOM_IMAGE_MAX_DOCKERFILE_BYTES: int = int(os.getenv("CUSTOM_IMAGE_MAX_DOCKERFILE_BYTES", "65536"))
    # Optional registry pull secret referenced by notebook + prepull pods for the custom
    # registry. Empty means "assume nodes can already pull" (no imagePullSecrets injected).
    CUSTOM_IMAGE_PULL_SECRET_NAME: str = os.getenv("CUSTOM_IMAGE_PULL_SECRET_NAME", "")
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


settings = Settings()
