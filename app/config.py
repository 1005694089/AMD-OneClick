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

    # --- Two-tier persistent /workspace (localcache): node-local SSD working copy backed by a
    # durable, sharded NFS canonical copy. See the plan (ultracode-workspace-robust-dragon).
    # WORKSPACE_VOLUME_TYPE="localcache" turns this on; "hostPath"/"emptyDir" keep legacy behavior.
    # The pod mounts /workspace from a node-local SSD dir (WORKSPACE_LOCAL_CACHE_ROOT/<instance_id>);
    # an initContainer hydrates it from the durable NFS shard, a preStop hook flushes it back, so data
    # survives pod deletion and node loss (only the delta since the last flush is ever at risk).
    WORKSPACE_LOCAL_CACHE_ROOT: str = os.getenv("WORKSPACE_LOCAL_CACHE_ROOT", "/nvme0/data/workspace")
    # Durable tier = one shared RWX PVC per StorageClass below (created once at bootstrap). Each
    # instance gets an isolated subdirectory on its shard, chosen by md5(instance_id) % len(list).
    # APPEND-ONLY: never reorder or remove entries — the modulo maps existing instances to a shard by
    # position, so changing the list silently strands live data on the wrong (empty) shard.
    # NOTE (2026-07-03): managed-nfs-storage-1 was decommissioned out-of-band (its SFS-Turbo backend
    # denies mounts), so it was removed here BEFORE any real durable data existed — the only safe time
    # to change this list. From now the append-only rule stands: 4 shards over the healthy backends.
    WORKSPACE_DURABLE_STORAGE_CLASSES: list = [
        s.strip() for s in os.getenv(
            "WORKSPACE_DURABLE_STORAGE_CLASSES",
            "managed-nfs-storage-2,managed-nfs-storage-3,managed-nfs-storage-4,managed-nfs-storage-5",
        ).split(",") if s.strip()
    ]
    WORKSPACE_DURABLE_PVC_PREFIX: str = os.getenv("WORKSPACE_DURABLE_PVC_PREFIX", "oneclick-durable")
    # Per-shard durable PVC capacity requested at bootstrap (the SFS-Turbo backend is ~101 TiB; this
    # is the PVC request, not a hard cap — the per-instance 100GB cap is enforced by the ext4 loop image).
    WORKSPACE_DURABLE_PVC_SIZE_GI: int = int(os.getenv("WORKSPACE_DURABLE_PVC_SIZE_GI", "10240"))
    # Where the durable shard PVC is mounted inside the pod (hidden from the user; /workspace is the SSD copy).
    WORKSPACE_DURABLE_MOUNT_PATH: str = os.getenv("WORKSPACE_DURABLE_MOUNT_PATH", "/mnt/workspace-durable")
    # Privileged helper image for node-exec (nsenter) + init/preStop sync containers. Must be warm on
    # nodes (public registries are blocked). Empty => k8s_client falls back to DEFAULT_IMAGE (the
    # platform base image, already on nodes, ships bash+rsync).
    WORKSPACE_SYNC_IMAGE: str = os.getenv("WORKSPACE_SYNC_IMAGE", "")
    # After a pod stops, its node-local SSD copy is kept for this long so a fast relaunch lands on the
    # same node (soft affinity) and rehydrates near-instantly. After the window a reaper frees the SSD;
    # the next relaunch rehydrates from durable NFS onto whatever node it lands on.
    WORKSPACE_LOCAL_CACHE_TTL_MINUTES: int = int(os.getenv("WORKSPACE_LOCAL_CACHE_TTL_MINUTES", "120"))
    # Soft (preferred) nodeAffinity toward the instance's last node, for warm-relaunch cache hits.
    WORKSPACE_SOFT_AFFINITY_ENABLED: bool = os.getenv("WORKSPACE_SOFT_AFFINITY_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    WORKSPACE_SOFT_AFFINITY_WEIGHT: int = int(os.getenv("WORKSPACE_SOFT_AFFINITY_WEIGHT", "50"))
    # Manual (admin) durable delete soft-deletes to <shard>/.trash/<id>-<ts>/; a nightly reaper purges
    # trash older than this. User data is otherwise kept forever until an admin deletes it.
    WORKSPACE_DURABLE_TRASH_RETENTION_DAYS: int = int(os.getenv("WORKSPACE_DURABLE_TRASH_RETENTION_DAYS", "7"))
    # How often the delayed-local-delete reaper runs.
    WORKSPACE_LOCAL_CACHE_REAPER_INTERVAL_MINUTES: int = int(os.getenv("WORKSPACE_LOCAL_CACHE_REAPER_INTERVAL_MINUTES", "10"))
    # Pod termination grace for localcache instances. The preStop hook flushes local->durable (up to
    # 100GB over NFS) on graceful stop; the K8s default 30s would SIGKILL a large flush mid-copy. Give
    # it real headroom. delete_instance_by_id also uses this (not DELETE_CONFIRM_TIMEOUT_SECONDS) as its
    # confirm window for localcache pods so the manager never force-deletes before the flush finishes.
    WORKSPACE_TERMINATION_GRACE_SECONDS: int = int(os.getenv("WORKSPACE_TERMINATION_GRACE_SECONDS", "600"))
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

    # API-launched (e.g. HuggingFace) pods get a longer budget: auto-destroy after 8h idle.
    API_IDLE_TIMEOUT_MINUTES: int = int(os.getenv("API_IDLE_TIMEOUT_MINUTES", "480"))
    API_MAX_LIFETIME_HOURS: Optional[int] = (
        int(os.getenv("API_MAX_LIFETIME_HOURS")) if os.getenv("API_MAX_LIFETIME_HOURS") else None
    )
    IDLE_REAPER_INTERVAL_MINUTES: int = int(os.getenv("IDLE_REAPER_INTERVAL_MINUTES", "5"))

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
    # Beta-only admin GPU-nodes dashboard. Off by default; set true only in the beta config CM.
    GPU_DASHBOARD_ENABLED: bool = os.getenv("GPU_DASHBOARD_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    ADMIN_LOGIN_CREDITS: int = int(os.getenv("ADMIN_LOGIN_CREDITS", "10000"))
    COUPON_PRIVATE_KEY_PEM: Optional[str] = os.getenv("COUPON_PRIVATE_KEY_PEM")
    COUPON_REDEEM_ENABLED: bool = os.getenv("COUPON_REDEEM_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    COUPON_REDEEM_DISABLED_MESSAGE: str = os.getenv(
        "COUPON_REDEEM_DISABLED_MESSAGE",
        "System maintenance is in progress. Credit redemption is temporarily unavailable. Please contact the administrator if you need credits.",
    )

    RUN_SCHEDULER: bool = os.getenv("RUN_SCHEDULER", "true").lower() in {"1", "true", "yes", "on"}
    # Leader election: when manager runs >1 replica, only the Lease holder runs the scheduled jobs
    # (billing, reapers, reconciler) so they don't double-run. Uses a coordination.k8s.io Lease.
    # Disabled by default to preserve single-replica behaviour; enable when scaling replicas.
    LEADER_ELECTION_ENABLED: bool = os.getenv("LEADER_ELECTION_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    LEADER_LEASE_NAME: str = os.getenv("LEADER_LEASE_NAME", "amd-oneclick-manager-leader")
    LEADER_LEASE_DURATION_SECONDS: int = int(os.getenv("LEADER_LEASE_DURATION_SECONDS", "15"))
    LEADER_LEASE_RENEW_SECONDS: float = float(os.getenv("LEADER_LEASE_RENEW_SECONDS", "5"))
    OAUTH_CONNECT_TIMEOUT_SECONDS: float = float(os.getenv("OAUTH_CONNECT_TIMEOUT_SECONDS", "5"))
    OAUTH_READ_TIMEOUT_SECONDS: float = float(os.getenv("OAUTH_READ_TIMEOUT_SECONDS", "15"))
    SLOW_REQUEST_THRESHOLD_SECONDS: float = float(os.getenv("SLOW_REQUEST_THRESHOLD_SECONDS", "2"))
    WORKSHOP_LOGIN_ENABLED: bool = os.getenv("WORKSHOP_LOGIN_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    WORKSHOP_USER_COUNT: int = int(os.getenv("WORKSHOP_USER_COUNT", "150"))
    WORKSHOP_CREDITS: int = int(os.getenv("WORKSHOP_CREDITS", "10000"))

    HUGGINGFACE_DEMO_API_TOKENS: str = os.getenv("HUGGINGFACE_DEMO_API_TOKENS", os.getenv("HUGGINGFACE_API_TOKENS", ""))
    HUGGINGFACE_DEMO_MIN_CREDITS: int = int(
        os.getenv("HUGGINGFACE_DEMO_MIN_CREDITS", os.getenv("HUGGINGFACE_MIN_CREDITS", "8"))
    )
    # Default image for API launches when the caller omits `image`. Must exist in the enabled
    # catalog. Independent of DEFAULT_IMAGE (the web UI default).
    HUGGINGFACE_DEMO_DEFAULT_IMAGE: str = os.getenv(
        "HUGGINGFACE_DEMO_DEFAULT_IMAGE",
        "crpi-ygzb1jbfyj9pjrm6.cn-shenzhen.personal.cr.aliyuncs.com/images_hana/huaggingface_for_amd_radeon:latest",
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
    # Appended to every custom-image build. The default uses an upstream `curl | bash` installer
    # (opencode.ai) — a third-party supply-chain dependency. Operators who
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
    # Idle window after a custom image's last launch before it becomes eligible for full delete
    # (node layers + P2P caches + registry tag). Default 5 days per the auto-delete contract.
    CUSTOM_IMAGE_GC_LAUNCH_GRACE_SECONDS: int = int(os.getenv("CUSTOM_IMAGE_GC_LAUNCH_GRACE_SECONDS", "432000"))

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
    # DaemonSet / pull-probe path has been removed.
    # Default FALSE: when enabled, create_instance resolves a target GPU node and HARD-PINS the pod via
    # spec.nodeName so the image can be preloaded there — which also disables the pod's soft nodeAffinity
    # (affinity is only applied to un-pinned pods). Off by default so launches stay un-pinned and the
    # workspace warm-relaunch soft-affinity actually takes effect; set true only where the off-cluster
    # image daemon is running and image preloading is required.
    IMAGE_SERVICE_ENABLED: bool = os.getenv("IMAGE_SERVICE_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    # The Image-Service host is itself a labelled prepull node; node-target resolution must drop it
    # so it never receives distributions. Must exactly match its `kubectl get nodes` name.
    IMAGE_SERVICE_NODE_NAME: str = os.getenv("IMAGE_SERVICE_NODE_NAME", "")
    # Nodes permanently excluded from image distribution regardless of their labels/Ready state
    # (e.g. off-LAN nodes the daemon cannot reach over the 10.5.10.0/24 routed LAN). Comma-separated
    # node names. Defaults to the known off-LAN node so a relabel can never make it a target.
    IMAGE_TARGET_NODE_DENYLIST: list = [
        n.strip() for n in os.getenv("IMAGE_TARGET_NODE_DENYLIST", "wx-ms-w7900d-0027").split(",") if n.strip()
    ]
    # Short TTL (seconds) for the cached list_node() result used in image target resolution.
    NODE_LIST_CACHE_TTL_SECONDS: float = float(os.getenv("NODE_LIST_CACHE_TTL_SECONDS", "5"))
    # Legacy 'manual' catalog rows (base images pre-mirrored into Harbor by an admin, no distribute
    # job ever created) have no image_nodes rows, so the old get_image_sync_status counts 0/N forever.
    # When enabled, their readiness is instead computed by scanning each eligible node's kubelet image
    # inventory (node.status.images) — free per-node data already in the cached list_node() snapshot —
    # so the admin catalog reflects reality (the image IS on the nodes; instances launch from it).
    # Default TRUE: this only affects the previously-broken 0/N manual rows. Mirror rows (preheat DS)
    # and image-service rows are unaffected — they keep their own status paths.
    NODE_IMAGE_SCAN_ENABLED: bool = os.getenv("NODE_IMAGE_SCAN_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    # ACR Enterprise registry used as the admin-image backup source of truth.
    ACR_ENTERPRISE_REGISTRY: str = os.getenv("ACR_ENTERPRISE_REGISTRY", "")
    # Self-hosted LAN registry (zot) on node 0042 — the durable source of truth for the P2P
    # transport (host:port, e.g. "10.5.10.43:5000"). EMPTY => P1 push behavior is dormant: the
    # `push` chain step is not inserted and readiness still gates on node-loaded rows only, so
    # deploying P1 code before the registry env is wired is a no-op (safe/revertible). When set,
    # admin/custom builds push here and "ready" gates on the pushed digest being recorded.
    LAN_REGISTRY: str = os.getenv("LAN_REGISTRY", "").strip().rstrip("/")
    # --- Harbor auto-mirror + node preheat (in-cluster image distribution) ---
    # Admin Add Image mirrors the external source ref into this Harbor registry via skopeo, rewrites
    # the catalog launch ref to the Harbor ref, and preheats it onto eligible GPU nodes with an
    # unprivileged prepull DaemonSet. This removes the launch-time dependency on public registries/DNS.
    HARBOR_REGISTRY: str = os.getenv("HARBOR_REGISTRY", "10.5.10.89:1808").strip().rstrip("/")
    HARBOR_PROJECT: str = os.getenv("HARBOR_PROJECT", "xinwei").strip().strip("/")
    # Default FALSE so deploying this code is INERT (admin Add Image keeps the legacy path) until an
    # operator explicitly opts in per environment — a controlled/dark rollout, matching the dormant
    # defaults used by LAN_REGISTRY / PURGE_FANOUT_ENABLED elsewhere in this file.
    HARBOR_MIRROR_ENABLED: bool = os.getenv("HARBOR_MIRROR_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    # Docker config.json (from secret kaniko-harbor-auth) mounted into the manager for skopeo dest auth.
    HARBOR_AUTH_CONFIG_PATH: str = os.getenv("HARBOR_AUTH_CONFIG_PATH", "/harbor/config.json").strip()
    # skopeo copy retry count — mandatory: large multi-GB layers flake with transient Harbor 502s.
    HARBOR_MIRROR_RETRY_TIMES: int = int(os.getenv("HARBOR_MIRROR_RETRY_TIMES", "8"))
    # Verify the SOURCE registry's TLS cert (default true — only disable for a known plaintext src).
    HARBOR_MIRROR_SRC_TLS_VERIFY: bool = os.getenv("HARBOR_MIRROR_SRC_TLS_VERIFY", "true").lower() in {"1", "true", "yes", "on"}
    HARBOR_MIRROR_TIMEOUT_SECONDS: int = int(os.getenv("HARBOR_MIRROR_TIMEOUT_SECONDS", "3600"))
    # SSRF guard for the mirror source. Admin is an app-level shared password, not network-level
    # trust, so an admin must not be able to point skopeo at arbitrary internal hosts. By default,
    # reject source hosts that resolve to private/loopback/link-local addresses. An optional
    # allowlist (comma-separated host[:port] or host suffixes) whitelists specific internal
    # registries (e.g. the LAN Harbor itself for re-mirror). Empty allowlist = only public hosts.
    HARBOR_MIRROR_BLOCK_PRIVATE_SRC: bool = os.getenv("HARBOR_MIRROR_BLOCK_PRIVATE_SRC", "true").lower() in {"1", "true", "yes", "on"}
    HARBOR_MIRROR_SRC_HOST_ALLOWLIST: list = [
        h.strip().lower() for h in os.getenv("HARBOR_MIRROR_SRC_HOST_ALLOWLIST", "").split(",") if h.strip()
    ]
    # Node preheat via unprivileged prepull DaemonSet (pulls the Harbor image onto every eligible
    # node). Default FALSE — inert until opted in alongside HARBOR_MIRROR_ENABLED.
    PREHEAT_DS_ENABLED: bool = os.getenv("PREHEAT_DS_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    # ephemeral-storage request for the preheat pod. Image LAYERS live in containerd's image store,
    # NOT the pod's ephemeral-storage (which only counts the writable container layer + logs). A
    # `sleep infinity` container writes nothing, so this only needs to cover logs. Keep it tiny —
    # a large per-image reservation would stack across images and exhaust node scheduling capacity.
    PREHEAT_EPHEMERAL_STORAGE: str = os.getenv("PREHEAT_EPHEMERAL_STORAGE", "64Mi").strip()
    # Low, non-preempting priority class for preheat pods (created once; yields to real workloads).
    PREHEAT_PRIORITY_CLASS: str = os.getenv("PREHEAT_PRIORITY_CLASS", "oneclick-preheat-low").strip()
    # Master switch to disable user-submitted custom image builds (admin-curated catalog only).
    # INTENTIONALLY defaults FALSE (disabled) — unlike the dark-rollout HARBOR_MIRROR_ENABLED /
    # PREHEAT_DS_ENABLED flags, disabling custom builds is a REQUESTED behavior change to ship, not
    # an inert code drop. Deploying this flips POST /api/custom-images/build to 403 immediately and
    # hides the build UI; existing built images stay launchable/deletable. Set to true to re-enable.
    USER_CUSTOM_BUILDS_ENABLED: bool = os.getenv("USER_CUSTOM_BUILDS_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    # P4 complete-delete fan-out gate. FALSE (default) => delete/idle paths keep the pre-P4 single
    # `evict` (node-layer removal), so shipping P4 code is INERT for delete and production delete
    # keeps working exactly as before. TRUE => the 6-surface purge fan-out (purge_node/p2p/seed/
    # registry_delete/purge_builder/purge_meta). Flip to TRUE only AFTER the P2P cutover, once every
    # node/seed actually runs a dfdaemon (else purge_p2p/purge_seed have nothing to talk to and would
    # stick purge_meta). This makes P3/P4 code shippable to prod with zero delete-behavior change.
    PURGE_FANOUT_ENABLED: bool = os.getenv("PURGE_FANOUT_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    # P4 manager-executed purge (purge_p2p/purge_seed run via `kubectl exec dfctl task rm` into the
    # in-cluster dfdaemon/seed pods — the 0042 agent cannot reach the overlay-only seeds, and the real
    # v1.4.0 delete CLI is `dfctl task rm <task_id>`, socket-local). The manager drains these kinds on
    # the scheduler leader. Namespace + workload names of the Dragonfly install:
    DRAGONFLY_NAMESPACE: str = os.getenv("DRAGONFLY_NAMESPACE", "dragonfly-system")
    # DaemonSet pod name prefix for the per-node client dfdaemon (label app selection is used; this is
    # the container name to exec into).
    DRAGONFLY_CLIENT_CONTAINER: str = os.getenv("DRAGONFLY_CLIENT_CONTAINER", "client")
    DRAGONFLY_SEED_CONTAINER: str = os.getenv("DRAGONFLY_SEED_CONTAINER", "seed-client")
    # Label selector to find the per-node client dfdaemon pods (DaemonSet).
    DRAGONFLY_CLIENT_SELECTOR: str = os.getenv("DRAGONFLY_CLIENT_SELECTOR", "app=dragonfly,component=client")
    DRAGONFLY_SEED_SELECTOR: str = os.getenv("DRAGONFLY_SEED_SELECTOR", "app=dragonfly,component=seed-client")
    # dfctl binary path + daemon socket inside the pods (v1.4.0 defaults).
    DRAGONFLY_DFCTL: str = os.getenv("DRAGONFLY_DFCTL", "dfctl")
    # How many manager-side purge jobs to drain per scheduler tick (bounded so one tick can't run away).
    PURGE_DRAIN_BATCH: int = int(os.getenv("PURGE_DRAIN_BATCH", "20"))
    # How often the manager drains purge_p2p/purge_seed/purge_meta. Deletes are interactive, so keep
    # this brisk. Inert unless PURGE_FANOUT_ENABLED.
    PURGE_DRAIN_INTERVAL_SECONDS: int = int(os.getenv("PURGE_DRAIN_INTERVAL_SECONDS", "15"))
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
    # --- Risk 1: signup abuse / session revocation / rate limiting ---
    # Credits granted to a brand-new account on first login. Kept low so a
    # scripted throwaway OAuth account is not worth farming for GPU time.
    SIGNUP_BONUS_CREDITS: int = int(os.getenv("SIGNUP_BONUS_CREDITS", "2"))
    # Redis is used for login/registration rate limiting and (optionally) as a
    # fast cache for the per-user session epoch. Everything degrades gracefully
    # when REDIS_URL is empty or Redis is unreachable.
    REDIS_URL: str = os.getenv("REDIS_URL", "")
    RATE_LIMIT_ENABLED: bool = os.getenv("RATE_LIMIT_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    # Per client IP. login = OAuth start/callback bursts; signup = new-account creation.
    LOGIN_RATE_LIMIT_PER_MINUTE: int = int(os.getenv("LOGIN_RATE_LIMIT_PER_MINUTE", "20"))
    SIGNUP_RATE_LIMIT_PER_DAY: int = int(os.getenv("SIGNUP_RATE_LIMIT_PER_DAY", "5"))

    # --- Risk 2: authoritative deletion & reconciliation ---
    # After issuing a graceful delete we poll until the pod is truly gone; if it
    # is still present after this window we escalate to a force delete (grace 0).
    DELETE_CONFIRM_TIMEOUT_SECONDS: int = int(os.getenv("DELETE_CONFIRM_TIMEOUT_SECONDS", "30"))
    DELETE_POLL_INTERVAL_SECONDS: float = float(os.getenv("DELETE_POLL_INTERVAL_SECONDS", "2"))
    # Reconciler: adopt cluster as source of truth for instance lifecycle.
    RECONCILE_ENABLED: bool = os.getenv("RECONCILE_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    RECONCILE_INTERVAL_SECONDS: int = int(os.getenv("RECONCILE_INTERVAL_SECONDS", "60"))
    # Circuit breaker: if a single cycle would reclaim more orphans than this,
    # refuse and alert instead of mass-deleting (protects against a logic/DB
    # fault that misclassifies healthy instances as orphans).
    RECONCILE_MAX_DELETES_PER_CYCLE: int = int(os.getenv("RECONCILE_MAX_DELETES_PER_CYCLE", "10"))
    # A pod carrying our label but with no matching active DB record for at least
    # this long is treated as an orphan (e.g. a rogue/miner pod) and reclaimed.
    ORPHAN_GRACE_SECONDS: int = int(os.getenv("ORPHAN_GRACE_SECONDS", "300"))
    # A pod stuck Terminating past this window is force-deleted.
    TERMINATING_GRACE_SECONDS: int = int(os.getenv("TERMINATING_GRACE_SECONDS", "180"))
    # Terminal / broken instance reclamation. Failed & Succeeded pods are dead
    # weight; CrashLoopBackOff pods keep holding their GPU while restarting
    # forever. The billing loop never removes these (they are never "ready"), so
    # the reconciler reclaims them. CrashLoop is only reclaimed once it has
    # restarted at least this many times AND run past ORPHAN_GRACE_SECONDS, so a
    # brief startup crash-loop that recovers is not killed prematurely.
    RECLAIM_TERMINAL_ENABLED: bool = os.getenv("RECLAIM_TERMINAL_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    CRASHLOOP_RESTART_THRESHOLD: int = int(os.getenv("CRASHLOOP_RESTART_THRESHOLD", "10"))
    # Silently-wedged-node detection. A node whose containerd CRI lifecycle path hangs still reports
    # Ready with fresh heartbeats, so k8s never marks it NotReady; meanwhile its pods pile up stuck
    # Terminating (can't kill) and/or ContainerCreating (can't create). The reconciler aggregates its
    # per-pod stuck signals by node and, when a node accumulates enough simultaneous stuck pods across
    # consecutive cycles, DB-quarantines it (via store.quarantine_node) so new placements route around
    # it. This is app-internal only (no kubectl cordon — the manager SA lacks node RBAC by design).
    NODE_WEDGE_DETECT_ENABLED: bool = os.getenv("NODE_WEDGE_DETECT_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    # Min distinct stuck pods (Terminating past TERMINATING_GRACE_SECONDS, or ContainerCreating past
    # NODE_WEDGE_CREATING_SECONDS) on ONE node before it is a wedge suspect. >1 avoids flagging a node
    # for a single slow pod.
    NODE_WEDGE_MIN_STUCK_PODS: int = int(os.getenv("NODE_WEDGE_MIN_STUCK_PODS", "2"))
    # A pod ContainerCreating longer than this counts as stuck for wedge detection (normal creates are
    # seconds; a large cold image pull can legitimately take minutes, so keep this generous).
    NODE_WEDGE_CREATING_SECONDS: int = int(os.getenv("NODE_WEDGE_CREATING_SECONDS", "600"))
    # Consecutive reconcile cycles a node must remain a suspect before it is actually quarantined.
    # Requiring persistence guards against a transient burst being misread as a wedge.
    NODE_WEDGE_CONSECUTIVE_TICKS: int = int(os.getenv("NODE_WEDGE_CONSECUTIVE_TICKS", "2"))

    # --- Risk 3: image availability vs. prepull warmth ---
    # Prepull is a best-effort warm cache, NOT an availability gate. An
    # admin-enabled image is always offered to users; kubelet pulls on demand
    # (IfNotPresent) when a chosen node has not been pre-warmed.
    # Soft node affinity biases scheduling toward already-warmed nodes.
    # Default OFF: under the Dragonfly P2P image-service model every fleet node is
    # warmed and pulls peer-to-peer, so warm-node bias is redundant; enabling it also
    # requires nodes:patch RBAC (the label writer), which the per-service SA lacks by
    # default. Turn on only where the manager SA is granted nodes:patch.
    IMAGE_AFFINITY_ENABLED: bool = os.getenv("IMAGE_AFFINITY_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    IMAGE_READY_NODE_LABEL_PREFIX: str = os.getenv("IMAGE_READY_NODE_LABEL_PREFIX", "amd-oneclick.io/image-ready-")
    IMAGE_AFFINITY_WEIGHT: int = int(os.getenv("IMAGE_AFFINITY_WEIGHT", "80"))
    # Fraction of eligible prepull nodes that must have pulled before the admin
    # UI shows the image as "ready" (display only; does not hide the image).
    PREPULL_READY_THRESHOLD: float = float(os.getenv("PREPULL_READY_THRESHOLD", "0.8"))
    IMAGE_SYNC_REFRESH_INTERVAL_SECONDS: int = int(os.getenv("IMAGE_SYNC_REFRESH_INTERVAL_SECONDS", "120"))


settings = Settings()
