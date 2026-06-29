"""
Kubernetes client for managing notebook instances
"""
import hashlib
import hmac
import logging
import os
import re
import secrets
import shlex
import socket
import time
import random
import threading
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote, urlparse, urlunparse

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from . import store
from .config import settings, INSTANCE_TYPES, APP_FRAMEWORK_PRESETS

logger = logging.getLogger(__name__)

_node_port_lock = threading.Lock()

RESOURCE_PROFILES = {
    "standard": {
        "label": "16 CPU / 64Gi memory",
        "cpu_request": "8",
        "cpu_limit": "16",
        "memory_request": "32Gi",
        "memory_limit": "64Gi",
    },
    "large": {
        "label": "32 CPU / 128Gi memory",
        "cpu_request": "16",
        "cpu_limit": "32",
        "memory_request": "64Gi",
        "memory_limit": "128Gi",
    },
    "xlarge": {
        "label": "64 CPU / 256Gi memory",
        "cpu_request": "32",
        "cpu_limit": "64",
        "memory_request": "128Gi",
        "memory_limit": "256Gi",
    },
}

AUTO_RESOURCE_PROFILE_BY_GPU = {
    1: "standard",
    2: "large",
    4: "xlarge",
}


class K8sClient:
    """Kubernetes client for notebook management"""
    
    def __init__(self):
        """Initialize K8s client"""
        try:
            # Try in-cluster config first (when running inside K8s)
            config.load_incluster_config()
            logger.info("Loaded in-cluster K8s config")
        except config.ConfigException:
            # Fall back to kubeconfig file
            config.load_kube_config()
            logger.info("Loaded kubeconfig file")

        api_client = self._authenticated_api_client()

        self.core_v1 = client.CoreV1Api(api_client)
        self.apps_v1 = client.AppsV1Api(api_client)
        self.namespace = settings.K8S_NAMESPACE

    def _authenticated_api_client(self):
        token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        cfg = client.Configuration.get_default_copy()
        if not os.path.exists(token_path) or cfg.auth_settings():
            return client.ApiClient(cfg)
        token = open(token_path, encoding="utf-8").read().strip()
        cfg.api_key["BearerToken"] = f"Bearer {token}"
        client.Configuration.set_default(cfg)
        return client.ApiClient(cfg)
    
    def _generate_instance_id(self, email: str) -> str:
        """Generate a unique instance ID from email"""
        hash_str = hashlib.md5(email.lower().encode()).hexdigest()[:8]
        return f"nb-{hash_str}"
    
    def _get_labels(self, email: str, instance_id: str) -> dict:
        """Generate labels for K8s resources"""
        return {
            "app": settings.NOTEBOOK_LABEL_PREFIX,
            "instance-id": instance_id,
            "email-hash": hashlib.md5(email.lower().encode()).hexdigest()[:16],
        }
    
    def _jupyter_base_url(self, instance_id: str) -> str:
        return f"/instances/{instance_id}/"

    def _workspace_host_path(self, instance_id: str) -> str:
        safe_id = re.sub(r"[^a-zA-Z0-9_.-]", "-", instance_id)
        return f"{settings.WORKSPACE_HOST_ROOT.rstrip('/')}/{safe_id}"

    def _safe_storage_segment(self, value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_.-]", "-", value).strip("-") or "default"

    def _network_disk_sub_path(self, instance_id: str) -> str:
        prefix = settings.NETWORK_DISK_SUBPATH_PREFIX.strip("/")
        safe_id = self._safe_storage_segment(instance_id)
        return f"{prefix}/{safe_id}" if prefix else safe_id

    def _network_disk_dynamic_enabled(self) -> bool:
        return bool(
            settings.NETWORK_DISK_ENABLED
            and settings.NETWORK_DISK_NFS_SERVER.strip()
            and settings.NETWORK_DISK_SERVER_NODE_NAME.strip()
        )

    def _network_disk_claim_name(self, instance_id: str) -> str:
        prefix = self._safe_storage_segment(settings.NETWORK_DISK_PVC_PREFIX).lower()
        safe_id = self._safe_storage_segment(instance_id).lower()
        return f"{prefix}-{safe_id}"[:63].rstrip("-")

    def _network_disk_nfs_path(self, instance_id: str) -> str:
        prefix = "/" + settings.NETWORK_DISK_NFS_PATH_PREFIX.strip("/")
        return f"{prefix}/{self._safe_storage_segment(instance_id)}"

    def _workspace_quota_enabled(self) -> bool:
        return bool(settings.WORKSPACE_QUOTA_ENABLED)

    def _ensure_workspace_quota(self, instance_id: str) -> Optional[str]:
        if not settings.WORKSPACE_QUOTA_ENABLED:
            return None
        return settings.WORKSPACE_QUOTA_NODE_NAME.strip() or None

    def _resolve_notebook_node_name(self, workspace_quota_node_name: Optional[str] = None) -> Optional[str]:
        notebook_node_name = settings.NOTEBOOK_NODE_NAME.strip()
        quota_node_name = (workspace_quota_node_name or "").strip()
        if notebook_node_name and quota_node_name and notebook_node_name != quota_node_name:
            raise RuntimeError(
                "NOTEBOOK_NODE_NAME must match WORKSPACE_QUOTA_NODE_NAME when workspace quota is enabled"
            )
        return notebook_node_name or quota_node_name or None

    def _notebook_tolerations(self) -> list[dict]:
        tolerations = [
            {
                "key": "amd.com/gpu",
                "operator": "Exists",
                "effect": "NoSchedule"
            }
        ]
        toleration_key = settings.NOTEBOOK_TOLERATION_KEY.strip()
        if toleration_key:
            toleration = {
                "key": toleration_key,
                "operator": "Equal",
                "value": settings.NOTEBOOK_TOLERATION_VALUE.strip(),
                "effect": settings.NOTEBOOK_TOLERATION_EFFECT.strip() or "NoSchedule",
            }
            tolerations.append(toleration)
        return tolerations

    def _ensure_network_disk(self, instance_id: str) -> Optional[str]:
        if not self._network_disk_dynamic_enabled():
            return None

        claim_name = self._network_disk_claim_name(instance_id)
        nfs_path = self._network_disk_nfs_path(instance_id)
        self._provision_network_disk_image(instance_id)
        self._ensure_network_disk_pv_pvc(claim_name, nfs_path)
        return claim_name

    def _provision_network_disk_image(self, instance_id: str):
        safe_id = self._safe_storage_segment(instance_id)
        pod_name = f"netdisk-prov-{safe_id.lower()}"[:63].rstrip("-")
        image_path = f"{settings.NETWORK_DISK_IMAGE_HOST_ROOT.rstrip('/')}/{safe_id}.img"
        mount_path = f"{settings.NETWORK_DISK_EXPORT_HOST_ROOT.rstrip('/')}{self._network_disk_nfs_path(instance_id)}"
        size_gi = int(settings.NETWORK_DISK_SIZE_GI)
        command = f"""
set -eux
nsenter -t 1 -m -- /bin/bash -lc {shlex.quote(f'''
set -eux
img={shlex.quote(image_path)}
mnt={shlex.quote(mount_path)}
mkdir -p {shlex.quote(settings.NETWORK_DISK_IMAGE_HOST_ROOT)} "$mnt"
if [ ! -f "$img" ]; then
  truncate -s {size_gi}G "$img"
  mkfs.ext4 -F "$img"
fi
if ! mountpoint -q "$mnt"; then
  mount -o loop "$img" "$mnt"
fi
df -h "$mnt"
findmnt "$mnt"
''')}
"""
        body = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name, "namespace": self.namespace, "labels": {"app": "oneclick-network-disk-provisioner", "instance-id": safe_id}},
            "spec": {
                "nodeName": settings.NETWORK_DISK_SERVER_NODE_NAME,
                "hostPID": True,
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": "provision",
                        "image": "docker.m.daocloud.io/library/ubuntu:24.04",
                        "securityContext": {"privileged": True},
                        "command": ["/bin/bash", "-lc"],
                        "args": [command],
                        "resources": {
                            "requests": {"cpu": "50m", "memory": "64Mi"},
                            "limits": {"cpu": "1", "memory": "512Mi"},
                        },
                        "volumeMounts": [{"name": "host", "mountPath": "/host"}],
                    }
                ],
                "volumes": [{"name": "host", "hostPath": {"path": "/", "type": "Directory"}}],
            },
        }
        try:
            self.core_v1.delete_namespaced_pod(name=pod_name, namespace=self.namespace)
            for _ in range(30):
                try:
                    self.core_v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
                    time.sleep(1)
                except ApiException as e:
                    if e.status == 404:
                        break
                    raise
        except ApiException as e:
            if e.status != 404:
                raise

        self.core_v1.create_namespaced_pod(namespace=self.namespace, body=body)
        last_phase = ""
        for _ in range(120):
            pod = self.core_v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
            last_phase = pod.status.phase
            if last_phase == "Succeeded":
                return
            if last_phase == "Failed":
                raise RuntimeError(f"network disk provisioner {pod_name} failed")
            time.sleep(1)
        raise RuntimeError(f"network disk provisioner {pod_name} timed out in phase {last_phase}")

    def _ensure_network_disk_pv_pvc(self, claim_name: str, nfs_path: str):
        size = f"{int(settings.NETWORK_DISK_SIZE_GI)}Gi"
        pv_body = {
            "apiVersion": "v1",
            "kind": "PersistentVolume",
            "metadata": {"name": claim_name, "labels": {"app": "oneclick-network-disk", "claim": claim_name}},
            "spec": {
                "capacity": {"storage": size},
                "accessModes": ["ReadWriteMany"],
                "persistentVolumeReclaimPolicy": "Retain",
                "storageClassName": "",
                "nfs": {"server": settings.NETWORK_DISK_NFS_SERVER.strip(), "path": nfs_path},
            },
        }
        pvc_body = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": claim_name, "namespace": self.namespace, "labels": {"app": "oneclick-network-disk"}},
            "spec": {
                "accessModes": ["ReadWriteMany"],
                "resources": {"requests": {"storage": size}},
                "volumeName": claim_name,
                "storageClassName": "",
            },
        }
        try:
            self.core_v1.read_persistent_volume(name=claim_name)
        except ApiException as e:
            if e.status == 404:
                self.core_v1.create_persistent_volume(body=pv_body)
            else:
                raise

        try:
            self.core_v1.read_namespaced_persistent_volume_claim(name=claim_name, namespace=self.namespace)
        except ApiException as e:
            if e.status == 404:
                self.core_v1.create_namespaced_persistent_volume_claim(namespace=self.namespace, body=pvc_body)
            else:
                raise

        for _ in range(60):
            pvc = self.core_v1.read_namespaced_persistent_volume_claim(name=claim_name, namespace=self.namespace)
            if pvc.status.phase == "Bound":
                return
            time.sleep(1)
        raise RuntimeError(f"network disk PVC {claim_name} did not bind")

    def _resolve_resource_profile(self, gpu_count: int, resource_profile: Optional[str] = None) -> tuple[str, dict]:
        profile = (resource_profile or "auto").strip().lower()
        if profile == "auto":
            profile = AUTO_RESOURCE_PROFILE_BY_GPU.get(gpu_count, "standard")
        if profile not in RESOURCE_PROFILES:
            allowed = ", ".join(["auto", *RESOURCE_PROFILES.keys()])
            raise ValueError(f"Invalid resource profile '{resource_profile}'. Allowed values: {allowed}")
        return profile, RESOURCE_PROFILES[profile]

    def _service_launch_snippet(self, instance_id: str, notebook_dir: str) -> str:
        """Launch Jupyter Lab and OpenCode web side by side.

        Security model: both services are on NodePorts and both require a credential.
        Jupyter uses NOTEBOOK_TOKEN in its URL; OpenCode web enforces HTTP Basic auth via
        OPENCODE_SERVER_USERNAME/OPENCODE_SERVER_PASSWORD (injected into the pod env; the
        password is a per-instance HMAC keyed on the server-only OPENCODE_PASSWORD_SECRET, see
        _opencode_password) so the NodePort is never unauthenticated. errexit is
        disabled so OpenCode (the optional service) failing to start can never crash the pod
        before Jupyter is up.

        Jupyter is the REQUIRED process: we wait on its PID specifically (not a bare `wait`,
        which would block on OpenCode too). If Jupyter exits -- crash or clean shutdown -- we
        tear OpenCode down and exit the container with Jupyter's code, so a dead notebook can
        never masquerade as a healthy, still-billable pod kept alive by a lingering OpenCode.
        """
        base_url = self._jupyter_base_url(instance_id)
        return (
            "set +e\n"
            "export PATH=\"/usr/local/bin:/usr/bin:/root/.opencode/bin:$PATH\"\n"
            ": > /tmp/opencode-web.log\n"
            # CRITICAL: start Jupyter FIRST and never block it on OpenCode. OpenCode is opt-in and
            # its installer fetches from opencode.ai/github (intermittently throttled from
            # cn-shanghai); a blocking, un-timed install there used to hang the whole startup so
            # Jupyter never launched and the instance was stuck "JupyterStarting". Jupyter is the
            # required process — launch it immediately; reconcile + run OpenCode in the background.
            f"jupyter lab --ip=0.0.0.0 --port={settings.NOTEBOOK_PORT} --no-browser --allow-root "
            f"--ServerApp.token='{settings.NOTEBOOK_TOKEN}' --ServerApp.base_url='{base_url}' "
            f"--notebook-dir={notebook_dir} &\n"
            "JUPYTER_PID=$!\n"
            # OpenCode setup runs entirely in a backgrounded subshell, fully bounded so it can never
            # delay Jupyter. The version normally matches the baked image (no reinstall); if it ever
            # mismatches, the install is timeout-capped and best-effort. opencode web only starts if
            # the binary is present.
            "(\n"
            f"  OPENCODE_REQUIRED_VERSION='{settings.OPENCODE_VERSION}'\n"
            "  OPENCODE_CURRENT_VERSION=\"$(opencode --version 2>/dev/null | tr -d '[:space:]' || true)\"\n"
            "  if [ \"$OPENCODE_CURRENT_VERSION\" != \"$OPENCODE_REQUIRED_VERSION\" ]; then\n"
            "    echo \"Installing OpenCode ${OPENCODE_REQUIRED_VERSION} (current: ${OPENCODE_CURRENT_VERSION:-missing})\" >>/tmp/opencode-web.log\n"
            "    ( timeout 300 sh -c 'curl -4 -fsSL --connect-timeout 10 --max-time 180 --retry 2 -o /tmp/oc-install.sh https://opencode.ai/install && bash /tmp/oc-install.sh --version \"'\"$OPENCODE_REQUIRED_VERSION\"'\"' ) >>/tmp/opencode-web.log 2>&1 || timeout 300 npm i -g \"opencode-ai@$OPENCODE_REQUIRED_VERSION\" >>/tmp/opencode-web.log 2>&1 || echo 'OpenCode install failed; continuing without it.' >>/tmp/opencode-web.log\n"
            "    if [ \"$(/usr/bin/opencode --version 2>/dev/null | tr -d '[:space:]')\" = \"$OPENCODE_REQUIRED_VERSION\" ]; then ln -sf /usr/bin/opencode /usr/local/bin/opencode 2>/dev/null || true; fi\n"
            "    if [ \"$(/root/.opencode/bin/opencode --version 2>/dev/null | tr -d '[:space:]')\" = \"$OPENCODE_REQUIRED_VERSION\" ]; then ln -sf /root/.opencode/bin/opencode /usr/local/bin/opencode 2>/dev/null || true; fi\n"
            "  fi\n"
            "  opencode --version >>/tmp/opencode-web.log 2>&1 || true\n"
            "  if command -v opencode >/dev/null 2>&1; then\n"
            f"    opencode web --port {settings.OPENCODE_WEB_PORT} --hostname 0.0.0.0 >>/tmp/opencode-web.log 2>&1\n"
            "  fi\n"
            ") &\n"
            "OPENCODE_PID=$!\n"
            'wait "$JUPYTER_PID"\n'
            "JUPYTER_RC=$?\n"
            'echo "Jupyter exited with code $JUPYTER_RC; stopping container."\n'
            'kill "$OPENCODE_PID" 2>/dev/null\n'
            'exit "$JUPYTER_RC"\n'
        )

    def _build_startup_script(self, instance_id: str,
                              instance_type: str = "jupyter",
                              github_info: Optional[dict] = None) -> str:
        """Build startup script based on instance type"""
        workspace = shlex.quote(settings.WORKSPACE_MOUNT_PATH)
        model_link_script = f"""
mkdir -p /app
mkdir -p {workspace}
if [ -d {shlex.quote(settings.HF_CACHE_MOUNT_PATH)}/Qwen3-8B ] && [ ! -e /app/Qwen3-8B ]; then
    ln -s {shlex.quote(settings.HF_CACHE_MOUNT_PATH)}/Qwen3-8B /app/Qwen3-8B
fi
if [ -d {shlex.quote(settings.HF_CACHE_MOUNT_PATH)}/Qwen3-8B ] && [ ! -e {workspace}/Qwen3-8B ]; then
    ln -s {shlex.quote(settings.HF_CACHE_MOUNT_PATH)}/Qwen3-8B {workspace}/Qwen3-8B
fi
"""
        # Notebook-type images don't always ship Jupyter. Detect it and, if
        # missing, install jupyterlab from the Tsinghua PyPI mirror so the
        # Notebook deploy type works on any image instead of failing at launch.
        jupyter_ensure = f"""
if ! command -v jupyter >/dev/null 2>&1; then
    echo "[oneclick] Jupyter not found in image; installing jupyterlab via Tsinghua mirror..."
    pip install --no-cache-dir -i {settings.PIP_INDEX_URL} --trusted-host {settings.PYPI_HOST} jupyterlab 2>&1 | tail -8 || pip3 install --no-cache-dir -i {settings.PIP_INDEX_URL} --trusted-host {settings.PYPI_HOST} jupyterlab 2>&1 | tail -8
    export PATH="$HOME/.local/bin:/usr/local/bin:$PATH"
    hash -r 2>/dev/null || true
fi
"""
        if github_info:
            notebook_path = github_info["path"].lstrip("/")
            notebook_filename = notebook_path.split("/")[-1]
            repo_url = github_info.get("repo_url") or github_info.get("clone_url")
            if repo_url:
                repo_url_q = shlex.quote(repo_url)
                branch_q = shlex.quote(github_info.get("branch") or "main")
                notebook_path_q = shlex.quote(notebook_path)
                return f"""
set -e
export PATH="/root/.opencode/bin:$PATH"
{model_link_script}
mkdir -p {workspace}

if [ ! -e {workspace}/repo ]; then
    echo "Cloning {repo_url}..."
    for i in 1 2 3; do
        rm -rf {workspace}/.repo-tmp
        if timeout 240 git -c http.lowSpeedLimit=1000 -c http.lowSpeedTime=30 clone --depth 1 --branch {branch_q} {repo_url_q} {workspace}/.repo-tmp; then
            mv {workspace}/.repo-tmp {workspace}/repo
            echo "Repository cloned"
            break
        fi
        echo "Git clone attempt $i failed, retrying..."
        sleep $((i * 3))
    done
else
    echo "Using existing persistent workspace at {settings.WORKSPACE_MOUNT_PATH}/repo"
fi

cd {workspace}/repo
if [ ! -f {notebook_path_q} ]; then
    echo "Notebook not found: {notebook_path}"
    find . -maxdepth 4 -name '*.ipynb' | sed 's#^./##' | head -50
fi

{jupyter_ensure}
{self._service_launch_snippet(instance_id, f"{workspace}/repo")}"""
            return f"""
{model_link_script}
mkdir -p {workspace}/notebooks
cd {workspace}/notebooks

download_notebook() {{
    output_path="$1"
    source_url="$2"
    if [ -n "${{HF_TOKEN:-}}" ]; then
        curl -fsSL --connect-timeout 30 --max-time 120 -H "Authorization: Bearer ${{HF_TOKEN}}" -o "$output_path" "$source_url"
    else
        curl -fsSL --connect-timeout 30 --max-time 120 -o "$output_path" "$source_url"
    fi
}}

if [ ! -f {shlex.quote(notebook_filename)} ]; then
    echo "Downloading {notebook_filename}..."
    for i in 1 2 3; do
        if download_notebook {shlex.quote(notebook_filename)} {shlex.quote(self._notebook_download_url(github_info["raw_url"]))}; then
            echo "Downloaded: {notebook_filename}"
            break
        else
            echo "Attempt $i failed, retrying..."
            sleep 2
        fi
    done
else
    echo "Using existing persistent notebook {notebook_filename}"
fi

if [ ! -f {shlex.quote(notebook_filename)} ]; then
    echo "Warning: Failed to download notebook, starting with empty directory"
fi

{jupyter_ensure}
{self._service_launch_snippet(instance_id, f"{workspace}/notebooks")}"""

        if instance_type == "opencode":
            return f"""
export PATH="/root/.opencode/bin:$PATH"
{model_link_script}
cd {workspace}
{jupyter_ensure}
{self._service_launch_snippet(instance_id, workspace)}"""

        # Default: jupyter
        return f"""
export PATH="/root/.opencode/bin:$PATH"
{model_link_script}
cd {workspace}
{jupyter_ensure}
{self._service_launch_snippet(instance_id, workspace)}"""

    def _notebook_download_url(self, raw_url: str) -> str:
        endpoint = settings.HF_ENDPOINT.strip().rstrip("/")
        if not endpoint:
            return raw_url

        parsed = urlparse(raw_url)
        if parsed.netloc.lower() not in {"huggingface.co", "www.huggingface.co"}:
            return raw_url

        endpoint_parsed = urlparse(endpoint if "://" in endpoint else f"https://{endpoint}")
        endpoint_path = endpoint_parsed.path.rstrip("/")
        rewritten_path = f"{endpoint_path}{parsed.path}" if endpoint_path else parsed.path
        return urlunparse((
            endpoint_parsed.scheme,
            endpoint_parsed.netloc,
            rewritten_path,
            "",
            parsed.query,
            parsed.fragment,
        ))

    def _resolve_app_command(self, instance_type: str, start_command: Optional[str], app_port: Optional[int]) -> tuple:
        """Resolve the effective start command + port for an app-type instance.
        Template override wins; otherwise the framework preset default is used."""
        preset = APP_FRAMEWORK_PRESETS.get(instance_type, {})
        cmd = (start_command or "").strip() or preset.get("start_command", "")
        port = int(app_port) if app_port else int(preset.get("port") or settings.APP_PORTS.get(instance_type) or 8000)
        return cmd, port

    def _build_app_startup_script(self, instance_id: str, instance_type: str,
                                  github_info: Optional[dict] = None,
                                  start_command: Optional[str] = None,
                                  app_port: Optional[int] = None) -> str:
        """Build the startup script for an app-type instance (Gradio/Streamlit/ComfyUI).
        Clones the template repo if provided, then runs the resolved start command.
        The app must listen on its app port; the manager proxies it under
        /spaces/<id>/<port>/ (Gradio/Streamlit are base-path-aware via injected env)."""
        workspace = shlex.quote(settings.WORKSPACE_MOUNT_PATH)
        cmd, _port = self._resolve_app_command(instance_type, start_command, app_port)
        clone_block = ""
        run_dir = settings.WORKSPACE_MOUNT_PATH
        if github_info and (github_info.get("repo_url") or github_info.get("clone_url")):
            repo_url = github_info.get("repo_url") or github_info.get("clone_url")
            branch_q = shlex.quote(github_info.get("branch") or "main")
            repo_url_q = shlex.quote(repo_url)
            clone_block = f"""
if [ ! -e {workspace}/repo ]; then
    echo "Cloning {repo_url}..."
    for i in 1 2 3; do
        rm -rf {workspace}/.repo-tmp
        if timeout 240 git -c http.lowSpeedLimit=1000 -c http.lowSpeedTime=30 clone --depth 1 --branch {branch_q} {repo_url_q} {workspace}/.repo-tmp; then
            mv {workspace}/.repo-tmp {workspace}/repo
            echo "Repository cloned"
            break
        fi
        echo "Git clone attempt $i failed, retrying..."
        sleep $((i * 3))
    done
fi
"""
            run_dir = f"{settings.WORKSPACE_MOUNT_PATH}/repo"
        pip_index = settings.PIP_INDEX_URL.strip()
        pip_flag = f"-i {shlex.quote(pip_index)} " if pip_index else ""
        return f"""
set -e
export PATH="/root/.opencode/bin:$PATH"
mkdir -p {workspace}
{clone_block}
cd {shlex.quote(run_dir)} 2>/dev/null || cd {workspace}
if [ -f requirements.txt ]; then
    echo "Installing requirements.txt..."
    pip install {pip_flag}-r requirements.txt || echo "WARN: pip install -r requirements.txt failed"
fi
if [ -n "$VLLM_USE_MODELSCOPE" ] && ! python -c "import modelscope" 2>/dev/null; then
    echo "Installing modelscope..."
    pip install {pip_flag}modelscope || echo "WARN: pip install modelscope failed"
fi
echo "Starting {instance_type} app: {cmd}"
exec {cmd}
"""

    def _get_pod_manifest(self, email: str, instance_id: str, image: str,
                          instance_type: str = "jupyter",
                          gpu_count: int = 1,
                          github_info: Optional[dict] = None,
                          resource_profile: Optional[str] = None,
                          network_disk_claim_name: Optional[str] = None,
                          workspace_quota_node_name: Optional[str] = None,
                          notebook_node_name: Optional[str] = None,
                          template_id: Optional[str] = None,
                          template_title: Optional[str] = None,
                          start_command: Optional[str] = None,
                          app_port: Optional[int] = None,
                          disk_size_gb: Optional[int] = None,
                          model_source: Optional[str] = None,
                          ssh_enabled: bool = False,
                          ssh_public_key: Optional[str] = None) -> dict:
        """Generate Pod manifest"""
        labels = self._get_labels(email, instance_id)
        profile_name, resources = self._resolve_resource_profile(gpu_count, resource_profile)

        annotations = {
            "amd-oneclick/email": email,
            "amd-oneclick/created-at": datetime.now(timezone.utc).isoformat(),
            "amd-oneclick/instance-type": instance_type,
            "amd-oneclick/path-proxy": "true",
            "amd-oneclick/resource-profile": profile_name,
            "amd-oneclick/cpu-limit": resources["cpu_limit"],
            "amd-oneclick/memory-limit": resources["memory_limit"],
            "amd-oneclick/workspace-host-path": self._workspace_host_path(instance_id),
        }
        network_disk_pvc_name = network_disk_claim_name or settings.NETWORK_DISK_PVC_NAME.strip()
        network_disk_enabled = bool(settings.NETWORK_DISK_ENABLED and network_disk_pvc_name)
        use_static_network_disk_subpath = bool(network_disk_enabled and not network_disk_claim_name)
        network_disk_sub_path = self._network_disk_sub_path(instance_id) if use_static_network_disk_subpath else ""
        if network_disk_enabled:
            annotations["amd-oneclick/network-disk-pvc"] = network_disk_pvc_name
            if network_disk_sub_path:
                annotations["amd-oneclick/network-disk-sub-path"] = network_disk_sub_path
        if workspace_quota_node_name:
            annotations["amd-oneclick/workspace-quota"] = f"{settings.WORKSPACE_QUOTA_SIZE_GI}Gi"
            annotations["amd-oneclick/workspace-quota-node"] = workspace_quota_node_name
        if notebook_node_name:
            annotations["amd-oneclick/notebook-node"] = notebook_node_name

        if github_info:
            annotations["amd-oneclick/github-org"] = github_info.get("org", "")
            annotations["amd-oneclick/github-repo"] = github_info.get("repo", "")
            annotations["amd-oneclick/github-branch"] = github_info.get("branch", "")
            annotations["amd-oneclick/github-path"] = github_info.get("path", "")
            annotations["amd-oneclick/github-raw-url"] = github_info.get("raw_url", "")
            annotations["amd-oneclick/github-repo-url"] = github_info.get("repo_url", "")
            annotations["amd-oneclick/template-id"] = github_info.get("template_id", "")
            annotations["amd-oneclick/template-title"] = github_info.get("template_title", "")

        # Tag the instance with its source template even for image-only templates (no
        # github_info), so the active instance is attributable to the template rather
        # than just its underlying image.
        if template_id:
            annotations["amd-oneclick/template-id"] = str(template_id)
        if template_title:
            annotations["amd-oneclick/template-title"] = template_title

        image_defined_command = bool(INSTANCE_TYPES.get(instance_type, {}).get("image_defined_command"))
        app_preset = APP_FRAMEWORK_PRESETS.get(instance_type)
        is_app_type = app_preset is not None
        api_key_value = None
        if is_app_type:
            _eff_cmd, _eff_port = self._resolve_app_command(instance_type, start_command, app_port)
            annotations["amd-oneclick/app-port"] = str(_eff_port)
            if app_preset.get("api_kind"):
                annotations["amd-oneclick/api-kind"] = "true"
                annotations["amd-oneclick/api-base-suffix"] = app_preset.get("api_base_suffix", "")
                # Per-instance API key so the exposed model endpoint is not open to anyone.
                api_key_value = f"sk-{secrets.token_hex(20)}"
                annotations["amd-oneclick/api-key"] = api_key_value
            startup_script = self._build_app_startup_script(
                instance_id, instance_type, github_info,
                start_command=start_command, app_port=app_port,
            )
        else:
            startup_script = self._build_startup_script(instance_id, instance_type, github_info)

        ssh_enabled = bool(ssh_enabled)
        if ssh_enabled:
            annotations["amd-oneclick/ssh-enabled"] = "true"

        workspace_volume_type = (settings.WORKSPACE_VOLUME_TYPE or "hostPath").strip().lower()
        workspace_uses_empty_dir = workspace_volume_type == "emptydir"
        hf_cache_volume_type = (settings.HF_CACHE_VOLUME_TYPE or "emptyDir").strip().lower()
        hf_cache_uses_empty_dir = hf_cache_volume_type == "emptydir"

        # App-type images often bake their app under /workspace (e.g. ComfyUI at
        # /workspace/ComfyUI). Mounting our workspace volume there would hide the
        # image's files, so app types do not get the /workspace overmount.
        volume_mounts = [
            {"name": "shm", "mountPath": "/dev/shm"},
            {"name": "hf-cache", "mountPath": settings.HF_CACHE_MOUNT_PATH},
        ]
        if not is_app_type:
            volume_mounts.append({"name": "workspace", "mountPath": settings.WORKSPACE_MOUNT_PATH})
        volumes = [
            {
                "name": "shm",
                "emptyDir": {
                    "medium": "Memory",
                    "sizeLimit": "64Gi"
                }
            },
        ]
        if hf_cache_uses_empty_dir:
            hf_cache_empty_dir = {}
            if settings.HF_CACHE_EMPTYDIR_SIZE_LIMIT.strip():
                hf_cache_empty_dir["sizeLimit"] = settings.HF_CACHE_EMPTYDIR_SIZE_LIMIT.strip()
            volumes.append({"name": "hf-cache", "emptyDir": hf_cache_empty_dir})
        else:
            volumes.append({
                "name": "hf-cache",
                "hostPath": {
                    "path": settings.HF_CACHE_HOST_PATH,
                    "type": "DirectoryOrCreate"
                }
            })
        if is_app_type:
            pass  # no workspace volume; the app lives in the image
        elif workspace_uses_empty_dir:
            workspace_empty_dir = {}
            if disk_size_gb:
                workspace_empty_dir["sizeLimit"] = f"{int(disk_size_gb)}Gi"
            elif settings.WORKSPACE_EMPTYDIR_SIZE_LIMIT.strip():
                workspace_empty_dir["sizeLimit"] = settings.WORKSPACE_EMPTYDIR_SIZE_LIMIT.strip()
            volumes.append({"name": "workspace", "emptyDir": workspace_empty_dir})
        else:
            volumes.append({
                "name": "workspace",
                "hostPath": {
                    "path": self._workspace_host_path(instance_id),
                    "type": "DirectoryOrCreate"
                }
            })
        env = [
            {"name": "SHELL", "value": "/bin/bash"},
            {"name": "USER_EMAIL", "value": email},
            {"name": "INSTANCE_TYPE", "value": instance_type},
            {"name": "WORKSPACE_DIR", "value": settings.WORKSPACE_MOUNT_PATH},
            {"name": "HF_HOME", "value": settings.HF_CACHE_MOUNT_PATH},
            {"name": "HUGGINGFACE_HUB_CACHE", "value": settings.HF_CACHE_MOUNT_PATH},
            {"name": "HF_HUB_DISABLE_XET", "value": settings.HF_HUB_DISABLE_XET},
            # Protect OpenCode web (bound to 0.0.0.0 on a NodePort) with HTTP Basic auth.
            # OpenCode reads these for both `serve` and `web`. The password is per-instance
            # (HMAC keyed on the server-only OPENCODE_PASSWORD_SECRET + instance_id) so it can't
            # be reused against another owner's NodePort; the same value is embedded in this
            # owner's opencode_url.
            {"name": "OPENCODE_SERVER_USERNAME", "value": settings.OPENCODE_WEB_USERNAME},
            {"name": "OPENCODE_SERVER_PASSWORD", "value": self._opencode_password(instance_id)},
        ]
        use_modelscope = (model_source or "").strip().lower() == "modelscope"
        if use_modelscope:
            # vLLM/SGLang download the model from ModelScope instead of HuggingFace.
            env.append({"name": "VLLM_USE_MODELSCOPE", "value": "True"})
            env.append({"name": "SGLANG_USE_MODELSCOPE", "value": "True"})
            env.append({"name": "MODELSCOPE_CACHE", "value": settings.HF_CACHE_MOUNT_PATH})
        elif settings.HF_ENDPOINT.strip():
            env.append({"name": "HF_ENDPOINT", "value": settings.HF_ENDPOINT.strip()})
        hf_token_secret_name = settings.HF_TOKEN_SECRET_NAME.strip()
        if hf_token_secret_name:
            env.append({
                "name": "HF_TOKEN",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": hf_token_secret_name,
                        "key": settings.HF_TOKEN_SECRET_KEY.strip() or "HF_TOKEN",
                        "optional": True,
                    }
                },
            })
        elif settings.HF_TOKEN.strip():
            env.append({"name": "HF_TOKEN", "value": settings.HF_TOKEN.strip()})
        if settings.PIP_INDEX_URL.strip():
            env.append({"name": "PIP_INDEX_URL", "value": settings.PIP_INDEX_URL.strip()})
        # Auto-configure common app frameworks so they serve under the Spaces
        # proxy base path and bind 0.0.0.0:<curated port>. This lets a user run
        # `gradio app.py` / `streamlit run app.py` from the notebook terminal and
        # get a working forwarded URL with no extra flags.
        spaces_prefix = settings.SPACES_PATH_PREFIX.rstrip("/")
        gradio_port = settings.APP_PORTS.get("gradio")
        streamlit_port = settings.APP_PORTS.get("streamlit")
        if gradio_port:
            env += [
                {"name": "GRADIO_SERVER_NAME", "value": "0.0.0.0"},
                {"name": "GRADIO_SERVER_PORT", "value": str(gradio_port)},
                {"name": "GRADIO_ROOT_PATH", "value": f"{spaces_prefix}/{instance_id}/{gradio_port}"},
            ]
        if streamlit_port:
            env += [
                {"name": "STREAMLIT_SERVER_ADDRESS", "value": "0.0.0.0"},
                {"name": "STREAMLIT_SERVER_PORT", "value": str(streamlit_port)},
                {"name": "STREAMLIT_SERVER_BASE_URL_PATH", "value": f"{spaces_prefix}/{instance_id}/{streamlit_port}"},
                {"name": "STREAMLIT_SERVER_HEADLESS", "value": "true"},
                {"name": "STREAMLIT_SERVER_ENABLE_CORS", "value": "false"},
                {"name": "STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION", "value": "false"},
            ]
        # API-kind instances: inject the per-instance API key under the framework's
        # expected env var (e.g. VLLM_API_KEY) so the served endpoint requires it.
        if api_key_value and app_preset and app_preset.get("api_key_env"):
            env.append({"name": app_preset["api_key_env"], "value": api_key_value})
            env.append({"name": "AMD_ONECLICK_API_KEY", "value": api_key_value})
        if network_disk_enabled:
            network_disk_mount = {
                "name": "network-disk",
                "mountPath": settings.NETWORK_DISK_MOUNT_PATH,
            }
            if network_disk_sub_path:
                network_disk_mount["subPath"] = network_disk_sub_path
            volume_mounts.append(network_disk_mount)
            volumes.append({
                "name": "network-disk",
                "persistentVolumeClaim": {
                    "claimName": network_disk_pvc_name
                }
            })
            env.append({"name": "NETWORK_DISK_DIR", "value": settings.NETWORK_DISK_MOUNT_PATH})

        init_containers = []
        if settings.WORKSPACE_QUOTA_ENABLED and not workspace_uses_empty_dir and not is_app_type:
            safe_id = self._safe_storage_segment(instance_id)
            quota_image_path = f"/quota-images/{safe_id}.img"
            init_containers.append({
                "name": "workspace-quota",
                "image": "docker.m.daocloud.io/library/ubuntu:24.04",
                "imagePullPolicy": "IfNotPresent",
                "securityContext": {"privileged": True},
                "command": ["/bin/bash", "-lc"],
                "args": [f"""
set -eux
img={shlex.quote(quota_image_path)}
mnt={shlex.quote(settings.WORKSPACE_MOUNT_PATH)}
mkdir -p /quota-images "$mnt"
current_source="$(findmnt -n -o SOURCE "$mnt" || true)"
if printf '%s' "$current_source" | grep -Fq "$img"; then
  df -h "$mnt"
  findmnt "$mnt"
  exit 0
fi
if [ ! -f "$img" ]; then
  truncate -s {int(settings.WORKSPACE_QUOTA_SIZE_GI)}G "$img"
  mkfs.ext4 -F "$img"
fi
mount -o loop "$img" "$mnt"
chmod 0777 "$mnt"
df -h "$mnt"
findmnt "$mnt"
"""],
                "volumeMounts": [
                    {"name": "workspace", "mountPath": settings.WORKSPACE_MOUNT_PATH, "mountPropagation": "Bidirectional"},
                    {"name": "workspace-quota-images", "mountPath": "/quota-images"},
                ],
            })
            volume_mounts[2]["mountPropagation"] = "HostToContainer"
            volumes.append({
                "name": "workspace-quota-images",
                "hostPath": {
                    "path": settings.WORKSPACE_QUOTA_IMAGE_ROOT,
                    "type": "DirectoryOrCreate"
                }
            })

        container_limits = {
            "cpu": resources["cpu_limit"],
            "memory": resources["memory_limit"],
            "amd.com/gpu": str(gpu_count)
        }
        container_requests = {
            "cpu": resources["cpu_request"],
            "memory": resources["memory_request"],
            "amd.com/gpu": str(gpu_count)
        }
        if disk_size_gb:
            # Ephemeral-storage limit must cover the workspace emptyDir plus image
            # writable layer/logs, so add a small buffer above the chosen disk size.
            container_limits["ephemeral-storage"] = f"{int(disk_size_gb) + 20}Gi"
            if settings.EPHEMERAL_STORAGE_REQUEST.strip():
                container_requests["ephemeral-storage"] = settings.EPHEMERAL_STORAGE_REQUEST.strip()
        else:
            if settings.EPHEMERAL_STORAGE_LIMIT.strip():
                container_limits["ephemeral-storage"] = settings.EPHEMERAL_STORAGE_LIMIT.strip()
            if settings.EPHEMERAL_STORAGE_REQUEST.strip():
                container_requests["ephemeral-storage"] = settings.EPHEMERAL_STORAGE_REQUEST.strip()

        container_ports = [
            {"containerPort": settings.NOTEBOOK_PORT, "name": "jupyter"},
            {"containerPort": settings.OPENCODE_WEB_PORT, "name": "opencode"},
        ]
        for _app_name, _app_port in settings.APP_PORTS.items():
            container_ports.append({"containerPort": int(_app_port), "name": _app_name[:15]})
        if ssh_enabled:
            container_ports.append({"containerPort": int(settings.SSH_PORT), "name": "ssh"})
            env = list(env) + [{"name": "ONECLICK_SSH_PUBLIC_KEY", "value": (ssh_public_key or "").strip()}]

        notebook_container = {
            "name": "notebook",
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "ports": container_ports,
            "resources": {
                "limits": container_limits,
                "requests": container_requests,
            },
            "env": env,
            "volumeMounts": volume_mounts
        }
        # For image-defined instance types the manager does not assemble a start
        # command; the image's own ENTRYPOINT/CMD runs and must listen on NOTEBOOK_PORT.
        if not image_defined_command:
            notebook_container["command"] = ["/bin/bash", "-c"]
            notebook_container["args"] = [startup_script]

        # When the Image Service has already imported this ref into containerd on the
        # resolved target node, the image is present locally with no registry behind it:
        # pull IfNotPresent and drop the registry pull secrets entirely.
        image_preloaded = bool(
            settings.IMAGE_SERVICE_ENABLED
            and notebook_node_name
            and store.image_loaded_on_node(image, notebook_node_name)
        )

        # Custom images reuse the tag user-{id}:{name} across rebuilds, so IfNotPresent could
        # launch a stale cached layer on the node after a delete+rebuild. Force Always for the
        # custom registry so the freshly pushed image is always pulled.
        is_custom_image = bool(
            settings.CUSTOM_IMAGE_REGISTRY and image.startswith(settings.CUSTOM_IMAGE_REGISTRY)
        )
        if image_preloaded:
            notebook_pull_policy = "IfNotPresent"
        else:
            notebook_pull_policy = "Always" if is_custom_image else "IfNotPresent"
        notebook_container["imagePullPolicy"] = notebook_pull_policy

        # Opt-in SSH: inject the launching user's public key and force key-only
        # auth via a postStart hook. This runs regardless of the container's main
        # command (works for custom image-defined types too) so the image's sshd
        # accepts the user's key and never a password.
        if ssh_enabled:
            notebook_container["lifecycle"] = {
                "postStart": {"exec": {"command": ["/bin/sh", "-c", self._ssh_poststart_script()]}}
            }

        spec = {
            "securityContext": {
                "supplementalGroups": settings.GPU_SUPPLEMENTAL_GROUPS
            },
            # Notebook pods don't call the K8s API; dropping the token reduces blast
            # radius if a user (root in their pod) tries to reach the apiserver.
            "automountServiceAccountToken": False,
            "dnsPolicy": "None",
            "dnsConfig": {
                "nameservers": ["8.8.8.8", "8.8.4.4"],
                "searches": ["default.svc.cluster.local", "svc.cluster.local", "cluster.local"],
                "options": [
                    {"name": "ndots", "value": "5"}
                ]
            },
            "hostAliases": [
                {
                    "ip": "36.151.243.83",
                    "hostnames": ["github.com"]
                }
            ],
            "tolerations": self._notebook_tolerations(),
            "containers": [
                notebook_container
            ],
            "volumes": volumes,
            "restartPolicy": "Always"
        }
        if init_containers:
            spec["initContainers"] = init_containers
        if notebook_node_name:
            spec["nodeName"] = notebook_node_name

        # A preloaded ref is served from the node's local containerd store, so no
        # registry credentials are needed (and attaching them is wrong — the ref has
        # no registry). Only attach pull secrets when a registry pull may happen.
        if not image_preloaded:
            image_pull_secrets = []
            image_pull_secret_name = settings.IMAGE_PULL_SECRET_NAME.strip()
            if image_pull_secret_name:
                image_pull_secrets.append({"name": image_pull_secret_name})
            # Additionally attach the custom-registry pull secret for images from the custom
            # registry, when one is configured. Other images keep relying on node-level credentials.
            if settings.CUSTOM_IMAGE_PULL_SECRET_NAME and image.startswith(settings.CUSTOM_IMAGE_REGISTRY):
                image_pull_secrets.append({"name": settings.CUSTOM_IMAGE_PULL_SECRET_NAME})
            if image_pull_secrets:
                spec["imagePullSecrets"] = image_pull_secrets

        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": instance_id,
                "namespace": self.namespace,
                "labels": labels,
                "annotations": annotations
            },
            "spec": spec
        }

    def _get_service_manifest(self, email: str, instance_id: str, node_port: int,
                              opencode_node_port: Optional[int] = None,
                              ssh_node_port: Optional[int] = None) -> dict:
        """Generate Service manifest exposing Jupyter, OpenCode web, and optional SSH."""
        labels = self._get_labels(email, instance_id)

        ports = [
            {
                "name": "jupyter",
                "port": settings.NOTEBOOK_PORT,
                "targetPort": settings.NOTEBOOK_PORT,
                "nodePort": node_port
            }
        ]
        if opencode_node_port:
            ports.append({
                "name": "opencode",
                "port": settings.OPENCODE_WEB_PORT,
                "targetPort": settings.OPENCODE_WEB_PORT,
                "nodePort": int(opencode_node_port)
            })
        if ssh_node_port:
            ports.append({
                "name": "ssh",
                "port": int(settings.SSH_PORT),
                "targetPort": int(settings.SSH_PORT),
                "nodePort": int(ssh_node_port)
            })

        return {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": f"{instance_id}-svc",
                "namespace": self.namespace,
                "labels": labels,
            },
            "spec": {
                "selector": labels,
                "type": "NodePort",
                "ports": ports
            }
        }

    @staticmethod
    def _svc_node_ports_by_name(svc) -> dict:
        """Map service port name -> nodePort for a Service object."""
        result: dict = {}
        for p in (svc.spec.ports or []):
            if p.node_port:
                result[p.name] = int(p.node_port)
        return result

    @staticmethod
    def _ssh_poststart_script() -> str:
        """postStart hook: install the user's public key, force key-only auth.

        Runs after the container starts regardless of its main command. Writes
        $ONECLICK_SSH_PUBLIC_KEY to root's authorized_keys, disables password
        login (drop-in + main config + locks the account password), ensures host
        keys exist, and (re)starts/reloads sshd if the image ships one. Every
        step is best-effort so it never crashes the container.
        """
        return r"""
set +e
KEY="${ONECLICK_SSH_PUBLIC_KEY:-}"
if [ -n "$KEY" ]; then
  mkdir -p /root/.ssh && chmod 700 /root/.ssh
  printf '%s\n' "$KEY" > /root/.ssh/authorized_keys
  chmod 600 /root/.ssh/authorized_keys
fi
# Force key-only auth (drop-in wins if Include is present; also patch main config).
mkdir -p /etc/ssh/sshd_config.d 2>/dev/null
printf 'PasswordAuthentication no\nPermitRootLogin prohibit-password\nPubkeyAuthentication yes\n' > /etc/ssh/sshd_config.d/00-oneclick.conf 2>/dev/null
if [ -f /etc/ssh/sshd_config ]; then
  sed -i 's/^[#[:space:]]*PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config 2>/dev/null
  sed -i 's/^[#[:space:]]*PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config 2>/dev/null
fi
# Lock any baked-in root password so only the injected key can log in.
passwd -l root 2>/dev/null
# Ensure host keys + (re)start/reload sshd if the image provides it.
if command -v sshd >/dev/null 2>&1 || [ -x /usr/sbin/sshd ]; then
  mkdir -p /run/sshd 2>/dev/null
  ssh-keygen -A 2>/dev/null
  service ssh reload 2>/dev/null || /usr/sbin/sshd 2>/dev/null || sshd 2>/dev/null
fi
exit 0
"""

    def _ssh_access(self, ssh_node_port: Optional[int]) -> dict:
        """Build the SSH access info surfaced to the user, when SSH is enabled."""
        if not ssh_node_port:
            return {}
        host = settings.SSH_HOST or settings.SERVICE_HOST
        user = settings.SSH_USERNAME
        return {
            "ssh_host": host,
            "ssh_port": int(ssh_node_port),
            "ssh_username": user,
            "ssh_command": f"ssh {user}@{host} -p {ssh_node_port}",
        }
    
    def _node_port_bounds(self) -> tuple[int, int]:
        lower = settings.NODE_PORT_BASE
        upper = settings.NODE_PORT_MAX
        if lower > upper:
            raise RuntimeError(f"Invalid NodePort range: {lower}-{upper}")
        return lower, upper

    def _used_node_ports(self) -> set[int]:
        if not settings.NODE_PORT_CLUSTER_SCAN_ENABLED:
            return set()

        used_ports: set[int] = set()
        services = self.core_v1.list_service_for_all_namespaces()
        for svc in services.items:
            for port in svc.spec.ports or []:
                if port.node_port:
                    used_ports.add(int(port.node_port))
        return used_ports

    def _allocate_node_port(self, used_ports: Optional[set[int]] = None, start_port: Optional[int] = None) -> int:
        """Allocate an available NodePort candidate."""
        lower, upper = self._node_port_bounds()
        used_ports = used_ports if used_ports is not None else self._used_node_ports()
        port = start_port or lower
        if port < lower:
            port = lower
        while port in used_ports and port <= upper:
            port += 1
        if port <= upper:
            return port
        for port in range(lower, upper + 1):
            if port not in used_ports:
                return port
        raise RuntimeError("No available NodePort in configured range")

    def _allocate_node_port_pair(self, used_ports: Optional[set[int]] = None,
                                 start_port: Optional[int] = None) -> tuple[int, int]:
        """Allocate two distinct available NodePorts (jupyter + opencode)."""
        used_ports = set(used_ports) if used_ports is not None else self._used_node_ports()
        jupyter_port = self._allocate_node_port(used_ports, start_port=start_port)
        opencode_port = self._allocate_node_port(used_ports | {jupyter_port},
                                                 start_port=jupyter_port + 1)
        return jupyter_port, opencode_port

    def _is_node_port_conflict(self, exc: ApiException) -> bool:
        message = str(exc).lower()
        return exc.status in {409, 422} and (
            "already allocated" in message or "provided port" in message
        )

    def _allocate_instance_node_ports(self, used_ports: Optional[set[int]] = None,
                                      start_port: Optional[int] = None,
                                      ssh_enabled: bool = False) -> tuple[int, int, Optional[int]]:
        """Allocate distinct NodePorts for Jupyter, OpenCode, and optional SSH."""
        used_ports = set(used_ports) if used_ports is not None else self._used_node_ports()
        jupyter_port, opencode_port = self._allocate_node_port_pair(used_ports, start_port=start_port)
        ssh_node_port = None
        if ssh_enabled:
            ssh_node_port = self._allocate_node_port(
                used_ports | {jupyter_port, opencode_port},
                start_port=opencode_port + 1,
            )
        return jupyter_port, opencode_port, ssh_node_port

    def _create_service_with_nodeport_retry(self, email: str, instance_id: str,
                                            ssh_enabled: bool = False) -> tuple:
        """Create the instance Service.

        Returns (jupyter_node_port, opencode_node_port, created) by default to
        preserve the existing internal contract. When ssh_enabled is true,
        returns (jupyter_node_port, opencode_node_port, ssh_node_port, created).

        When ssh_enabled the Service exposes a second NodePort -> pod:22 so the
        user can SSH into the pod. Ports are (re)allocated together on conflict.
        """
        try:
            existing = self.core_v1.read_namespaced_service(name=f"{instance_id}-svc", namespace=self.namespace)
            by_name = self._svc_node_ports_by_name(existing)
            jupyter_port = by_name.get("jupyter") or (existing.spec.ports[0].node_port if existing.spec.ports else None)
            if jupyter_port:
                if ssh_enabled:
                    return int(jupyter_port), by_name.get("opencode"), by_name.get("ssh"), False
                return int(jupyter_port), by_name.get("opencode"), False
        except ApiException as e:
            if e.status != 404:
                raise

        with _node_port_lock:
            lower, upper = self._node_port_bounds()
            used_ports = self._used_node_ports()
            start = lower + random.randint(0, min(200, max(0, upper - lower)))
            node_port, opencode_port, ssh_node_port = self._allocate_instance_node_ports(
                used_ports,
                start_port=start,
                ssh_enabled=ssh_enabled,
            )
            max_attempts = min(512, upper - lower + 1)
            for _ in range(max_attempts):
                try:
                    self.core_v1.create_namespaced_service(
                        namespace=self.namespace,
                        body=self._get_service_manifest(email, instance_id, node_port, opencode_port, ssh_node_port),
                    )
                    logger.info(
                        "Created service %s-svc with NodePorts jupyter=%s opencode=%s ssh=%s",
                        instance_id, node_port, opencode_port, ssh_node_port,
                    )
                    if ssh_enabled:
                        return node_port, opencode_port, ssh_node_port, True
                    return node_port, opencode_port, True
                except ApiException as e:
                    if e.status == 409 and not self._is_node_port_conflict(e):
                        existing = self.core_v1.read_namespaced_service(name=f"{instance_id}-svc", namespace=self.namespace)
                        by_name = self._svc_node_ports_by_name(existing)
                        existing_port = by_name.get("jupyter") or (existing.spec.ports[0].node_port if existing.spec.ports else None)
                        if existing_port:
                            if ssh_enabled:
                                return int(existing_port), by_name.get("opencode"), by_name.get("ssh"), False
                            return int(existing_port), by_name.get("opencode"), False
                        raise
                    if self._is_node_port_conflict(e):
                        if settings.NODE_PORT_CLUSTER_SCAN_ENABLED:
                            used_ports = self._used_node_ports()
                        used_ports.update(port for port in (node_port, opencode_port, ssh_node_port) if port)
                        node_port, opencode_port, ssh_node_port = self._allocate_instance_node_ports(
                            used_ports,
                            start_port=node_port + 1,
                            ssh_enabled=ssh_enabled,
                        )
                        continue
                    raise
        raise RuntimeError("Unable to allocate NodePort for service")

    @staticmethod
    def _normalize_image_ref(ref: str) -> str:
        """Canonicalize a docker image reference for reliable comparison.

        kubelet reports container status images in fully-qualified form
        (e.g. ``docker.io/library/nginx:latest``) while the catalog may store
        short names (e.g. ``nginx`` or ``rocm/atom-dev:tag``). Normalize both
        sides so the sync counter matches regardless of how it was entered.
        """
        if not ref:
            return ref
        ref = ref.strip()
        # Separate digest if present (keep it as-is, it is already canonical).
        digest = ""
        if "@" in ref:
            ref, digest = ref.split("@", 1)
            digest = "@" + digest
        first = ref.split("/", 1)[0]
        has_registry = "." in first or ":" in first or first == "localhost"
        if not has_registry:
            if "/" not in ref:
                ref = "library/" + ref
            ref = "docker.io/" + ref
        if not digest and ":" not in ref.rsplit("/", 1)[-1]:
            ref = ref + ":latest"
        return ref + digest

    def _prepull_name(self, image_id: int) -> str:
        return f"image-prepull-catalog-{image_id}"

    def _prepull_labels(self, image_id: int) -> dict:
        return {
            "app": "amd-oneclick-image-prepull",
            "image-id": str(image_id),
        }

    def _pull_probe_name(self, image_id: int) -> str:
        return f"image-pull-catalog-{image_id}"

    def _pull_probe_labels(self, image_id: int) -> dict:
        return {
            "app": "amd-oneclick-image-pull-check",
            "managed-by": "amd-oneclick-manager",
            "image-id": str(image_id),
        }

    def _image_pull_probe_enabled(self) -> bool:
        return (
            not settings.IMAGE_PREPULL_ENABLED
            and bool(settings.IMAGE_PULL_PROBE_ENABLED)
            and bool(settings.NOTEBOOK_NODE_NAME.strip())
        )

    def _pull_probe_admin_auth_block_message(self) -> Optional[str]:
        if settings.ADMIN_PASSWORD == "admin123":
            return "Image pull probe disabled until ADMIN_PASSWORD is set to a non-default beta secret"
        return None

    def _pull_probe_desired_count(self) -> int:
        return 1 if settings.NOTEBOOK_NODE_NAME.strip() else 0

    def _pull_probe_status(
        self,
        status: str,
        ready_count: int = 0,
        message: str = "",
        completed: bool = False,
    ) -> dict:
        return {
            "status": status,
            "desired_count": self._pull_probe_desired_count(),
            "ready_count": ready_count,
            "message": message,
            "completed": completed,
        }

    def _pull_probe_node_block_message(self) -> Optional[str]:
        node_name = settings.NOTEBOOK_NODE_NAME.strip()
        if not node_name:
            return "IMAGE_PULL_PROBE_ENABLED requires NOTEBOOK_NODE_NAME"
        try:
            node = self.core_v1.read_node(name=node_name)
        except ApiException as e:
            reason = e.reason or str(e)
            return f"Cannot inspect pull node {node_name}: {reason}"

        conditions = {cond.type: cond for cond in (node.status.conditions or [])}
        disk_pressure = conditions.get("DiskPressure")
        if disk_pressure and disk_pressure.status == "True":
            detail = disk_pressure.message or disk_pressure.reason or "DiskPressure=True"
            return f"{node_name} DiskPressure=True: {detail}"

        ready = conditions.get("Ready")
        if not ready or ready.status != "True":
            detail = (ready.message or ready.reason) if ready else "Ready condition missing"
            return f"{node_name} is not Ready: {detail}"
        return None

    def _image_cached_on_pull_node(self, image: str) -> bool:
        image = (image or "").strip()
        node_name = settings.NOTEBOOK_NODE_NAME.strip()
        if not image or not node_name:
            return False
        try:
            node = self.core_v1.read_node(name=node_name)
        except ApiException:
            return False
        for item in node.status.images or []:
            if image in (item.names or []):
                return True
        return False

    def _is_managed_pull_probe(self, pod, image_id: int) -> bool:
        labels = pod.metadata.labels or {}
        expected = self._pull_probe_labels(image_id)
        return all(labels.get(key) == value for key, value in expected.items())

    def _delete_image_pull_probe(self, image_id: int, wait: bool = False):
        name = self._pull_probe_name(image_id)
        try:
            pod = self.core_v1.read_namespaced_pod(name=name, namespace=self.namespace)
        except ApiException as e:
            if e.status == 404:
                return
            raise

        if not self._is_managed_pull_probe(pod, image_id):
            raise RuntimeError(f"Refusing to delete unmanaged pull probe pod {name}")

        self.core_v1.delete_namespaced_pod(name=name, namespace=self.namespace)
        if not wait:
            return
        for _ in range(30):
            try:
                self.core_v1.read_namespaced_pod(name=name, namespace=self.namespace)
                time.sleep(1)
            except ApiException as e:
                if e.status == 404:
                    return
                raise

    def _active_pull_probe_name(self, exclude_name: Optional[str] = None) -> Optional[str]:
        pods = self.core_v1.list_namespaced_pod(
            namespace=self.namespace,
            label_selector="app=amd-oneclick-image-pull-check,managed-by=amd-oneclick-manager",
        )
        for pod in pods.items:
            name = pod.metadata.name
            if exclude_name and name == exclude_name:
                continue
            phase = getattr(pod.status, "phase", "") or ""
            if phase in {"Succeeded", "Failed"}:
                continue
            # A terminating probe still holds containerd image-pull work and disk on the
            # node until it is fully gone, so it must keep counting as active. The caller
            # only soft-queues on this (user retries Sync), so a draining pod can't wedge
            # future pulls -- it disappears once deletion completes.
            return name
        return None

    def _eligible_prepull_nodes(self) -> set[str]:
        """Nodes that should count toward image availability."""
        eligible: set[str] = set()
        try:
            nodes = self.core_v1.list_node()
        except ApiException as e:
            if e.status == 403:
                logger.warning("Cannot list nodes for image pre-pull status; returning best-effort status")
                return eligible
            raise
        for node in nodes.items:
            labels = node.metadata.labels or {}
            conditions = {cond.type: cond.status for cond in node.status.conditions or []}
            if labels.get("amd-oneclick-prepull") != "enabled":
                continue
            if getattr(node.spec, "unschedulable", False):
                continue
            if conditions.get("Ready") != "True":
                continue
            if conditions.get("DiskPressure") == "True":
                continue
            eligible.add(node.metadata.name)
        return eligible

    def _node_belongs_to_service(self, node) -> bool:
        """True if `node` belongs to THIS manager's service (taint-wise).

        The `amd-oneclick-prepull=enabled` label is shared cluster-wide (production +
        every beta), so it alone is too broad a target set for a single service. A
        service is identified by the dedicated taint its notebooks carry as a toleration
        (NOTEBOOK_TOLERATION_KEY/VALUE, e.g. amd-oneclick/beta=radeon). Membership is
        SYMMETRIC, not mere tolerance:
          - A scoped service (toleration key set) targets ONLY nodes that carry that
            exact key=value taint — so beta never lands on production/other-beta nodes
            even though its pods would *tolerate* an untainted node.
          - The default service (no toleration key) targets ONLY nodes with no
            service taint (it must still tolerate infra taints like amd.com/gpu).
        In both cases every NoSchedule/NoExecute taint on the node must be covered by
        this manager's notebook tolerations (else the pod couldn't schedule there)."""
        tolerations = self._notebook_tolerations()
        service_key = settings.NOTEBOOK_TOLERATION_KEY.strip()
        service_value = settings.NOTEBOOK_TOLERATION_VALUE.strip()

        def _tolerated(taint) -> bool:
            for tol in tolerations:
                if tol.get("effect") and tol["effect"] != taint.effect:
                    continue
                op = tol.get("operator", "Equal")
                if op == "Exists":
                    if not tol.get("key") or tol["key"] == taint.key:
                        return True
                else:
                    if tol.get("key") == taint.key and tol.get("value", "") == (taint.value or ""):
                        return True
            return False

        node_taints = node.spec.taints or []
        # Every blocking taint must be tolerated (necessary for the pod to schedule).
        for taint in node_taints:
            if taint.effect in ("NoSchedule", "NoExecute") and not _tolerated(taint):
                return False
        # Symmetric membership: a scoped service requires its taint be present; the
        # default service requires NO foreign service taint be present.
        has_service_taint = any(
            t.key == service_key and (t.value or "") == service_value
            for t in node_taints
        ) if service_key else False
        if service_key:
            return has_service_taint
        # Default service: reject nodes carrying any non-infra (service) taint.
        for taint in node_taints:
            if taint.effect in ("NoSchedule", "NoExecute") and taint.key != "amd.com/gpu":
                return False
        return True

    def _eligible_target_nodes(self) -> list[dict]:
        """Nodes the Image Service should distribute images to.

        Predicate: prepull label, schedulable, Ready, no DiskPressure, AND the node's
        taints are tolerated by this manager's notebook pods (so a service only targets
        its own nodes — see `_node_tolerated_by_notebooks`). Resolves each node's
        InternalIP and excludes the Image-Service host itself (it carries the prepull
        label). Nodes without an InternalIP are skipped (the daemon reaches them over it)."""
        targets: list[dict] = []
        image_service_node = settings.IMAGE_SERVICE_NODE_NAME.strip()
        try:
            nodes = self.core_v1.list_node()
        except ApiException as e:
            if e.status == 403:
                logger.warning("Cannot list nodes for image distribution; returning best-effort targets")
                return targets
            raise
        for node in nodes.items:
            name = node.metadata.name
            if image_service_node and name == image_service_node:
                continue
            labels = node.metadata.labels or {}
            conditions = {cond.type: cond.status for cond in node.status.conditions or []}
            if labels.get("amd-oneclick-prepull") != "enabled":
                continue
            if getattr(node.spec, "unschedulable", False):
                continue
            if conditions.get("Ready") != "True":
                continue
            if conditions.get("DiskPressure") == "True":
                continue
            if not self._node_belongs_to_service(node):
                continue
            internal_ip = next(
                (addr.address for addr in (node.status.addresses or []) if addr.type == "InternalIP"),
                None,
            )
            if not internal_ip:
                continue
            targets.append({"node": name, "ip": internal_ip})
        return targets

    def resolve_node_targets(self, node_names: Optional[list[str]] = None) -> list[dict]:
        """Resolve distribute/evict targets ({"node","ip"}) for the daemon.

        The Manager owns target resolution (it has kubectl; the daemon does not). Returns
        every eligible target when node_names is None, else the eligible targets whose node
        name is in node_names (silently dropping names that are not eligible/resolvable)."""
        targets = self._eligible_target_nodes()
        if node_names is None:
            return targets
        wanted = {n for n in node_names if n}
        return [t for t in targets if t["node"] in wanted]

    def _select_target_gpu_node(self, gpu_count: int = 1) -> Optional[str]:
        """Pick an eligible node with enough free GPUs for a node-pinned launch.

        Reads each node's LIVE `amd.com/gpu` allocatable and subtracts the GPUs
        already committed by non-terminal pods on that node. Returns the first
        node with `allocatable - committed >= gpu_count`, else None."""
        for target in self._eligible_target_nodes():
            name = target["node"]
            try:
                node = self.core_v1.read_node(name=name)
            except ApiException:
                continue
            allocatable = node.status.allocatable or {}
            try:
                node_gpus = int(allocatable.get("amd.com/gpu", 0))
            except (TypeError, ValueError):
                continue
            if node_gpus < gpu_count:
                continue
            try:
                pods = self.core_v1.list_namespaced_pod(
                    namespace=self.namespace,
                    field_selector=f"spec.nodeName={name},status.phase!=Succeeded,status.phase!=Failed",
                )
            except ApiException:
                continue
            committed = 0
            for pod in pods.items:
                for container in pod.spec.containers or []:
                    requests = getattr(container.resources, "requests", None) or {}
                    try:
                        committed += int(requests.get("amd.com/gpu", 0))
                    except (TypeError, ValueError):
                        continue
            if node_gpus - committed >= gpu_count:
                return name
        return None

    def _configured_notebook_node_count(self) -> int:
        """Return the configured runtime node count when pre-pull is intentionally off."""
        return 1 if settings.NOTEBOOK_NODE_NAME else 0

    def _create_image_pull_probe(self, image_id: int, image: str):
        name = self._pull_probe_name(image_id)
        labels = self._pull_probe_labels(image_id)
        node_name = settings.NOTEBOOK_NODE_NAME.strip()
        spec = {
            "nodeName": node_name,
            "restartPolicy": "Never",
            "activeDeadlineSeconds": settings.IMAGE_PULL_PROBE_DEADLINE_SECONDS,
            "automountServiceAccountToken": False,
            "tolerations": self._notebook_tolerations(),
            "containers": [
                {
                    "name": "pull",
                    "image": image,
                    "imagePullPolicy": "Always",
                    "command": ["sh", "-c"],
                    "args": ["echo image pull probe ready on $(hostname)"],
                    "resources": {
                        "requests": {"cpu": "10m", "memory": "16Mi"},
                        "limits": {"cpu": "100m", "memory": "64Mi"},
                    },
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "readOnlyRootFilesystem": True,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                }
            ],
        }
        image_pull_secret_name = settings.IMAGE_PULL_SECRET_NAME.strip()
        if image_pull_secret_name:
            spec["imagePullSecrets"] = [{"name": image_pull_secret_name}]
        body = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": self.namespace,
                "labels": labels,
                "annotations": {
                    "amd-oneclick/image": image,
                    "amd-oneclick/pull-node": node_name,
                },
            },
            "spec": spec,
        }
        self.core_v1.create_namespaced_pod(namespace=self.namespace, body=body)

    def _sync_image_pull_probe(self, image_id: int, image: str) -> dict:
        image = image.strip()
        if not image:
            raise ValueError("image must not be empty")

        auth_message = self._pull_probe_admin_auth_block_message()
        if auth_message:
            return self._pull_probe_status("failed", 0, auth_message, False)

        blocking_message = self._pull_probe_node_block_message()
        if blocking_message:
            return self._pull_probe_status("failed", 0, blocking_message, False)

        probe_name = self._pull_probe_name(image_id)
        active_probe = self._active_pull_probe_name(exclude_name=probe_name)
        if active_probe:
            return self._pull_probe_status(
                "queued",
                0,
                f"Another image pull is active ({active_probe}); click Sync after it finishes",
                False,
            )

        self._delete_image_pull_probe(image_id, wait=True)
        node_name = settings.NOTEBOOK_NODE_NAME.strip()
        for attempt in range(1, 7):
            try:
                self._create_image_pull_probe(image_id, image)
                break
            except ApiException as e:
                if e.status == 409 and attempt < 6:
                    logger.warning(
                        "Pull probe %s still terminating while creating; retry %s",
                        probe_name, attempt,
                    )
                    time.sleep(5)
                    continue
                if e.status == 409:
                    return self._pull_probe_status(
                        "queued",
                        0,
                        f"Previous pull probe still terminating on {node_name}; click Sync again shortly",
                        False,
                    )
                raise
        return self.get_image_sync_status(image_id, image)

    def sync_image_to_nodes(self, image_id: int, image: str) -> dict:
        """Distribute a catalog image to every eligible GPU node.

        With the Image Service enabled this enqueues a `distribute` job (the
        out-of-cluster daemon does the actual `save | ssh ctr import`) and
        synthesizes status from `image_nodes`. With it disabled the legacy
        prepull-DaemonSet / pull-probe path is used unchanged."""
        if settings.IMAGE_SERVICE_ENABLED:
            image = image.strip()
            if not image:
                raise ValueError("image must not be empty")
            targets = self._eligible_target_nodes()
            store.enqueue_image_job(
                kind="distribute",
                ref=image,
                image_id=image_id,
                payload={"targets": targets, "concurrency": 2},
            )
            return self.get_image_sync_status(image_id, image)

        if not settings.IMAGE_PREPULL_ENABLED:
            if self._image_pull_probe_enabled():
                return self._sync_image_pull_probe(image_id, image)
            return self.get_image_sync_status(image_id)

        image = image.strip()
        if not image:
            raise ValueError("image must not be empty")

        name = self._prepull_name(image_id)
        labels = self._prepull_labels(image_id)

        try:
            self.apps_v1.delete_namespaced_daemon_set(name=name, namespace=self.namespace)
        except ApiException as e:
            if e.status != 404:
                raise

        body = {
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": {
                "name": name,
                "namespace": self.namespace,
                "labels": labels,
            },
            "spec": {
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "nodeSelector": {"amd-oneclick-prepull": "enabled"},
                        "tolerations": [{"operator": "Exists"}],
                        "containers": [
                            {
                                "name": "pull",
                                "image": image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": [
                                    "sh",
                                    "-c",
                                    "echo image ready on $(hostname); while true; do sleep 86400; done",
                                ],
                                "resources": {
                                    "requests": {"cpu": "10m", "memory": "16Mi"},
                                    "limits": {"cpu": "100m", "memory": "64Mi"},
                                },
                            }
                        ],
                    },
                },
            },
        }
        self.apps_v1.create_namespaced_daemon_set(namespace=self.namespace, body=body)
        return self.get_image_sync_status(image_id)

    def _pull_probe_message_from_waiting(self, waiting) -> str:
        reason = waiting.reason or "waiting"
        message = waiting.message or ""
        node_name = settings.NOTEBOOK_NODE_NAME.strip()
        suffix = f": {message}" if message else ""
        return f"{node_name} {reason}{suffix}"

    def _with_elapsed(self, pod, message: str) -> str:
        start = getattr(pod.status, "start_time", None) or getattr(pod.metadata, "creation_timestamp", None)
        if not start:
            return message
        elapsed = datetime.now(timezone.utc) - start
        elapsed_min = int(elapsed.total_seconds() // 60)
        deadline_min = int(settings.IMAGE_PULL_PROBE_DEADLINE_SECONDS // 60)
        return f"{message} ({elapsed_min}m / {deadline_min}m deadline)"

    def _get_image_pull_probe_status(self, image_id: int, image: Optional[str] = None) -> dict:
        auth_message = self._pull_probe_admin_auth_block_message()
        if auth_message:
            return self._pull_probe_status("failed", 0, auth_message, False)

        node_name = settings.NOTEBOOK_NODE_NAME.strip()
        image = (image or "").strip()
        name = self._pull_probe_name(image_id)
        try:
            pod = self.core_v1.read_namespaced_pod(name=name, namespace=self.namespace)
        except ApiException as e:
            if e.status == 404:
                if image and self._image_cached_on_pull_node(image):
                    return self._pull_probe_status("ready", 1, f"Image cached on {node_name}", True)
                return self._pull_probe_status("pending", 0, f"not pulled on {node_name}", False)
            raise

        if not self._is_managed_pull_probe(pod, image_id):
            return self._pull_probe_status("failed", 0, f"Probe pod {name} has unexpected labels", False)

        # The probe pod is named by image_id, not by image, so a pod left over from a previous
        # image value for this catalog entry can still be present (or terminating) after the admin
        # edits the image. Its imageID then proves the OLD image is pulled, not the requested one --
        # never treat that as readiness for the new image. The probe records the image it pulled in
        # an annotation; the imageID is only trustworthy as proof for the requested image when that
        # annotation confirms it. If it disagrees -- OR is absent (a legacy pod from before this
        # annotation shipped, where we cannot prove which image was pulled) -- force a re-Sync
        # rather than trusting the digest.
        if image:
            probe_annotations = getattr(pod.metadata, "annotations", None) or {}
            probe_image = (probe_annotations.get("amd-oneclick/image") or "").strip()
            if probe_image != image:
                detail = (
                    f"last pulled {probe_image}" if probe_image
                    else "cannot confirm which image this probe pulled"
                )
                return self._pull_probe_status(
                    "pending",
                    0,
                    f"{node_name} {detail}; click Sync to pull {image}",
                    False,
                )

        pod_status = pod.status
        phase = getattr(pod_status, "phase", "") or "Pending"
        pod_reason = getattr(pod_status, "reason", "") or ""
        pod_message = getattr(pod_status, "message", "") or ""
        statuses = getattr(pod_status, "container_statuses", None) or []
        container_status = next((status for status in statuses if getattr(status, "name", "") == "pull"), None)
        container_status = container_status or (statuses[0] if statuses else None)

        if container_status:
            image_id_value = getattr(container_status, "image_id", "") or ""
            if image_id_value:
                state = getattr(container_status, "state", None)
                terminated = getattr(state, "terminated", None) if state else None
                if terminated and phase == "Failed":
                    reason = terminated.reason or "terminated"
                    return self._pull_probe_status(
                        "ready",
                        1,
                        f"Image pulled on {node_name}; probe command ended with {reason}",
                        True,
                    )
                return self._pull_probe_status("ready", 1, f"Image pulled on {node_name}", True)

            # After activeDeadlineSeconds, Kubernetes marks the pod Failed/DeadlineExceeded while
            # the container can still report state.waiting (e.g. ContainerCreating) because it never
            # started. That must read as a terminal timeout, not an indefinite "pulling" -- check it
            # before the waiting branch below, but after the image-pulled success above so a probe
            # that actually pulled then hit the deadline still counts as ready.
            if phase == "Failed" or pod_reason == "DeadlineExceeded":
                detail = pod_message or pod_reason or "probe deadline exceeded"
                return self._pull_probe_status("failed", 0, f"{node_name} {detail}", False)

            if pod.metadata.deletion_timestamp:
                return self._pull_probe_status(
                    "failed",
                    0,
                    f"{node_name} previous pull stuck terminating; retry Sync once it clears",
                    False,
                )

            state = getattr(container_status, "state", None)
            waiting = getattr(state, "waiting", None) if state else None
            if waiting:
                reason = waiting.reason or "waiting"
                message = self._pull_probe_message_from_waiting(waiting)
                if reason in {"ErrImagePull", "ImagePullBackOff", "InvalidImageName", "CreateContainerConfigError", "CreateContainerError", "RunContainerError"}:
                    return self._pull_probe_status("failed", 0, message, False)
                return self._pull_probe_status("pulling", 0, self._with_elapsed(pod, message), False)

            terminated = getattr(state, "terminated", None) if state else None
            if terminated:
                reason = terminated.reason or "terminated"
                detail = terminated.message or ""
                suffix = f": {detail}" if detail else ""
                return self._pull_probe_status("failed", 0, f"{node_name} probe {reason}{suffix}", False)

        if pod.metadata.deletion_timestamp:
            return self._pull_probe_status(
                "failed",
                0,
                f"{node_name} previous pull stuck terminating; retry Sync once it clears",
                False,
            )

        if phase == "Succeeded":
            return self._pull_probe_status("failed", 0, f"{node_name} probe succeeded but image id was not reported", False)
        if phase == "Failed" or pod_reason == "DeadlineExceeded":
            detail = pod_message or pod_reason or "probe pod failed"
            return self._pull_probe_status("failed", 0, f"{node_name} {detail}", False)
        return self._pull_probe_status("pulling", 0, self._with_elapsed(pod, f"{node_name} probe phase {phase}"), False)

    def get_image_sync_status(self, image_id: int, image: Optional[str] = None) -> dict:
        """Return distribution status for an image catalog entry."""
        if settings.IMAGE_SERVICE_ENABLED:
            ref = (image or "").strip()
            targets = self._eligible_target_nodes()
            target_names = {t["node"] for t in targets}
            desired = len(target_names)
            loaded_nodes = set(store.list_nodes_for_image(ref)) if ref else set()
            ready = len(loaded_nodes & target_names) if target_names else len(loaded_nodes)
            if desired > 0 and ready >= desired:
                status = "ready"
            elif desired == 0:
                status = "pending"
            else:
                status = "pulling"
            return {
                "status": status,
                "desired_count": desired,
                "ready_count": ready,
                "message": f"{ready}/{desired} nodes loaded",
                "completed": status == "ready",
            }

        if not settings.IMAGE_PREPULL_ENABLED:
            if self._image_pull_probe_enabled():
                return self._get_image_pull_probe_status(image_id, image)
            desired = self._configured_notebook_node_count()
            return {
                "status": "skipped",
                "desired_count": desired,
                "ready_count": 0,
                "message": "Image pre-pull disabled; catalog image remains launchable on configured runtime nodes",
                "completed": False,
            }

        name = self._prepull_name(image_id)
        try:
            ds = self.apps_v1.read_namespaced_daemon_set(name=name, namespace=self.namespace)
            eligible_nodes = self._eligible_prepull_nodes()
            desired = len(eligible_nodes) or (ds.status.desired_number_scheduled or 0)
            ds_uid = ds.metadata.uid
            image = ds.spec.template.spec.containers[0].image
            image_norm = self._normalize_image_ref(image)
            pulled_nodes = set()
            pods = self.core_v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"app=amd-oneclick-image-prepull,image-id={image_id}",
            )
            for pod in pods.items:
                if not pod.spec.node_name:
                    continue
                if not any(ref.uid == ds_uid for ref in (pod.metadata.owner_references or [])):
                    continue
                statuses = pod.status.container_statuses or []
                if not statuses:
                    continue
                status = statuses[0]
                if status.image_id and self._normalize_image_ref(status.image) == image_norm:
                    pulled_nodes.add(pod.spec.node_name)

            effective_pulled_nodes = pulled_nodes & eligible_nodes if eligible_nodes else pulled_nodes
            ready = len(effective_pulled_nodes)
            target = max(1, int(desired * 0.8 + 0.999)) if desired else 0
            status = "ready" if target > 0 and ready >= target else "pulling"
            message = f"{ready}/{desired} eligible nodes pulled (threshold {target}, 80%)"
            return {
                "status": status,
                "desired_count": desired,
                "ready_count": ready,
                "message": message,
                "completed": status == "ready",
            }
        except ApiException as e:
            if e.status == 404:
                return {
                    "status": "pending",
                    "desired_count": 0,
                    "ready_count": 0,
                    "message": "not synced",
                    "completed": False,
                }
            raise

    def delete_image_sync(self, image_id: int):
        # With the Image Service enabled there is no prepull DaemonSet/probe to tear
        # down; node eviction (image_nodes cleanup + ssh ctr rm) is orchestrated by
        # the DELETE handler via an `evict` job. Keep the legacy cleanup for off.
        if settings.IMAGE_SERVICE_ENABLED:
            return
        try:
            self.apps_v1.delete_namespaced_daemon_set(name=self._prepull_name(image_id), namespace=self.namespace)
        except ApiException as e:
            if e.status != 404:
                raise
        if settings.IMAGE_PULL_PROBE_ENABLED:
            self._delete_image_pull_probe(image_id)

    def _custom_prepull_name(self, custom_image_id: int) -> str:
        return f"image-prepull-custom-{custom_image_id}"

    def delete_custom_image_sync(self, custom_image_id: int):
        # Custom user images are never prepulled (pulled lazily by the notebook pod on launch).
        # This remains a best-effort cleanup of any pre-existing custom-prepull DaemonSet from
        # an older deployment that did auto-prepull.
        try:
            self.apps_v1.delete_namespaced_daemon_set(name=self._custom_prepull_name(custom_image_id), namespace=self.namespace)
        except ApiException as e:
            if e.status != 404:
                raise

    def get_instance_by_email(self, email: str) -> Optional[dict]:
        """Get existing notebook instance for an email"""
        instance_id = self._generate_instance_id(email)
        
        try:
            pod = self.core_v1.read_namespaced_pod(
                name=instance_id,
                namespace=self.namespace
            )
            
            # Get associated service
            ssh_node_port = None
            opencode_node_port = None
            try:
                svc = self.core_v1.read_namespaced_service(
                    name=f"{instance_id}-svc",
                    namespace=self.namespace
                )
                by_name = self._svc_node_ports_by_name(svc)
                node_port = by_name.get("jupyter") or (svc.spec.ports[0].node_port if svc.spec.ports else None)
                opencode_node_port = by_name.get("opencode")
                ssh_node_port = by_name.get("ssh")
            except ApiException:
                node_port = None
                opencode_node_port = None
            
            return {
                "id": instance_id,
                "email": email,
                "pod_name": pod.metadata.name,
                "service_name": f"{instance_id}-svc",
                "image": pod.spec.containers[0].image,
                "status": pod.status.phase.lower(),
                "created_at": pod.metadata.creation_timestamp,
                "node_port": node_port,
                "opencode_node_port": opencode_node_port,
                "ssh_node_port": ssh_node_port,
                "url": self._build_url(node_port, instance_id=instance_id, use_path_proxy=pod.metadata.annotations.get("amd-oneclick/path-proxy") == "true") if node_port else None,
                "opencode_url": self._build_opencode_url(opencode_node_port, instance_id),
                **self._opencode_auth(opencode_node_port, instance_id),
                **self._ssh_access(ssh_node_port),
            }
        except ApiException as e:
            if e.status == 404:
                return None
            raise
    
    def _build_url(self, node_port: int, notebook_path: Optional[str] = None, instance_id: Optional[str] = None, use_path_proxy: bool = False) -> str:
        """Build notebook URL"""
        if use_path_proxy and settings.PUBLIC_BASE_URL and instance_id:
            base = settings.PUBLIC_BASE_URL.rstrip("/")
            if notebook_path:
                encoded_path = quote(notebook_path.lstrip("/"), safe="/")
                return f"{base}/instances/{instance_id}/lab/tree/{encoded_path}?token={settings.NOTEBOOK_TOKEN}"
            return f"{base}/instances/{instance_id}/lab?token={settings.NOTEBOOK_TOKEN}"
        base_url = f"http://{settings.SERVICE_HOST}:{node_port}/lab?token={settings.NOTEBOOK_TOKEN}"
        if notebook_path:
            encoded_path = quote(notebook_path.lstrip("/"), safe="/")
            return f"http://{settings.SERVICE_HOST}:{node_port}/lab/tree/{encoded_path}?token={settings.NOTEBOOK_TOKEN}"
        return base_url

    def _opencode_host(self) -> str:
        """Host for direct OpenCode NodePort access.

        Use the SAME host the working Jupyter NodePort URLs use: SERVICE_HOST
        (the cluster edge that forwards raw NodePorts directly, e.g. 36.150.116.220).
        PUBLIC_BASE_URL's hostname is the Azure Front Door domain, which only serves
        443 and does NOT forward arbitrary NodePorts — so building a direct
        host:nodeport URL on it times out. OPENCODE_NODEPORT_HOST overrides if the
        OpenCode edge ever differs from the Jupyter edge.
        """
        override = getattr(settings, "OPENCODE_NODEPORT_HOST", "").strip()
        if override:
            return override
        return settings.SERVICE_HOST

    def _opencode_password(self, instance_id: str) -> str:
        """Derive the OpenCode Basic-auth password for one instance.

        Per-instance (NOT a single shared secret): HMAC-SHA256(OPENCODE_PASSWORD_SECRET,
        instance_id). Both the pod env injection and the owner URL recompute this from
        instance_id, so the value never needs to be persisted, yet it differs for every pod.
        This closes the horizontal-reuse hole where a user who saw one OpenCode URL held the
        password for every other instance's NodePort.

        The key is OPENCODE_PASSWORD_SECRET, which is server-only. NOTEBOOK_TOKEN must NOT be
        used here: it is embedded in user-facing Jupyter URLs (?token=...), so any user holds it
        and could re-derive every other instance's password from the (predictable) instance_id.
        """
        return hmac.new(
            settings.OPENCODE_PASSWORD_SECRET.encode("utf-8"),
            instance_id.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _build_opencode_url(self, opencode_node_port: Optional[int], instance_id: str) -> Optional[str]:
        """Build the OpenCode web URL for the instance owner.

        OpenCode web enforces HTTP Basic auth (OPENCODE_SERVER_USERNAME/PASSWORD injected
        into the pod env), so the NodePort is not exposed unauthenticated. The password is
        per-instance (see _opencode_password) so it cannot be reused against another owner's
        NodePort. Do not embed credentials in the URL: OpenCode's current web app can break
        when loaded as http://user:password@host/.
        """
        if not opencode_node_port:
            return None
        if settings.OPENCODE_PUBLIC_BASE_URL:
            from .opencode_proxy import mint_handoff_token

            token = mint_handoff_token(instance_id)
            return f"{settings.OPENCODE_PUBLIC_BASE_URL}/__opencode_auth?token={quote(token, safe='')}"
        return f"http://{self._opencode_host()}:{opencode_node_port}/"

    def _opencode_auth(self, opencode_node_port: Optional[int], instance_id: str) -> dict:
        if not opencode_node_port or settings.OPENCODE_PUBLIC_BASE_URL:
            return {"opencode_username": None, "opencode_password": None}
        return {
            "opencode_username": settings.OPENCODE_WEB_USERNAME,
            "opencode_password": self._opencode_password(instance_id),
        }

    def _extract_node_ports(self, svc) -> tuple:
        """Return (jupyter_node_port, opencode_node_port) from a Service object."""
        jupyter_port = None
        opencode_port = None
        for port in (svc.spec.ports or []):
            if port.name == "opencode":
                opencode_port = port.node_port
            elif port.name == "jupyter" or jupyter_port is None:
                jupyter_port = port.node_port
        return jupyter_port, opencode_port
    
    def create_instance(self, email: str, image: Optional[str] = None,
                        instance_type: str = "jupyter",
                        gpu_count: int = 1,
                        github_info: Optional[dict] = None,
                        custom_instance_id: Optional[str] = None,
                        resource_profile: Optional[str] = None,
                        template_id: Optional[str] = None,
                        template_title: Optional[str] = None,
                        start_command: Optional[str] = None,
                        app_port: Optional[int] = None,
                        disk_size_gb: Optional[int] = None,
                        model_source: Optional[str] = None,
                        ssh_enabled: bool = False,
                        ssh_public_key: Optional[str] = None) -> dict:
        """Create a new notebook instance"""
        instance_id = custom_instance_id or self._generate_instance_id(email)
        image = image or settings.DEFAULT_IMAGE

        # Reuse an existing pod only if it is NOT being deleted. A pod that is
        # Terminating (deletionTimestamp set) is a stale instance from a prior
        # launch; reusing it would return the old type/command. Wait for it to
        # fully disappear so we can create a fresh pod.
        try:
            existing_pod = self.core_v1.read_namespaced_pod(name=instance_id, namespace=self.namespace)
        except ApiException as e:
            if e.status == 404:
                existing_pod = None
            else:
                raise
        if existing_pod is not None and existing_pod.metadata.deletion_timestamp is None:
            return self.get_instance_by_id(instance_id)
        if existing_pod is not None:
            logger.info("Pod %s is terminating; waiting for deletion before recreate", instance_id)
            for _ in range(30):
                time.sleep(2)
                try:
                    self.core_v1.read_namespaced_pod(name=instance_id, namespace=self.namespace)
                except ApiException as e:
                    if e.status == 404:
                        break
                    raise

        workspace_quota_node_name = self._ensure_workspace_quota(instance_id)
        notebook_node_name = self._resolve_notebook_node_name(workspace_quota_node_name)
        # When no node is pinned by config/quota, let the Image Service pick a GPU node
        # with free capacity so its image can be preloaded onto that same node.
        if not notebook_node_name and settings.IMAGE_SERVICE_ENABLED:
            notebook_node_name = self._select_target_gpu_node(gpu_count)
        network_disk_claim_name = self._ensure_network_disk(instance_id)
        pod_manifest = self._get_pod_manifest(
            email, instance_id, image,
            instance_type=instance_type,
            gpu_count=gpu_count,
            github_info=github_info,
            resource_profile=resource_profile,
            network_disk_claim_name=network_disk_claim_name,
            workspace_quota_node_name=workspace_quota_node_name,
            notebook_node_name=notebook_node_name,
            template_id=template_id,
            template_title=template_title,
            start_command=start_command,
            app_port=app_port,
            disk_size_gb=disk_size_gb,
            model_source=model_source,
            ssh_enabled=ssh_enabled,
            ssh_public_key=ssh_public_key,
        )
        for attempt in range(1, 7):
            try:
                self.core_v1.create_namespaced_pod(
                    namespace=self.namespace,
                    body=pod_manifest
                )
                logger.info(f"Created pod {instance_id} for {email} (type={instance_type})")
                break
            except ApiException as e:
                if e.status == 409 and attempt < 6:
                    logger.warning("Pod %s still exists while creating; waiting for deletion before retry %s", instance_id, attempt)
                    time.sleep(5)
                    continue
                logger.error(f"Failed to create pod: {e}")
                raise

        try:
            service_result = self._create_service_with_nodeport_retry(
                email,
                instance_id,
                ssh_enabled=ssh_enabled,
            )
            if ssh_enabled:
                node_port, opencode_node_port, ssh_node_port, service_created = service_result
            else:
                node_port, opencode_node_port, service_created = service_result
                ssh_node_port = None
        except Exception as e:
            logger.error("Failed to create service for %s: %s", instance_id, e)
            self.delete_instance_by_id(instance_id)
            raise

        notebook_path = github_info.get("path") if github_info else None

        return {
            "id": instance_id,
            "email": email,
            "pod_name": instance_id,
            "service_name": f"{instance_id}-svc",
            "image": image,
            "instance_type": instance_type,
            "gpu_count": gpu_count,
            "resource_profile": self._resolve_resource_profile(gpu_count, resource_profile)[0],
            "status": "pending",
            "created_at": datetime.now(timezone.utc),
            "node_port": node_port,
            "opencode_node_port": opencode_node_port,
            "ssh_node_port": ssh_node_port,
            "url": self._build_url(node_port, notebook_path, instance_id, use_path_proxy=True),
            "opencode_url": self._build_opencode_url(opencode_node_port, instance_id),
            **self._opencode_auth(opencode_node_port, instance_id),
            "github_info": github_info,
            **self._ssh_access(ssh_node_port),
        }
    
    def get_instance_by_id(self, instance_id: str) -> Optional[dict]:
        """Get existing notebook instance by instance ID"""
        try:
            pod = self.core_v1.read_namespaced_pod(
                name=instance_id,
                namespace=self.namespace
            )
            if pod.metadata.deletion_timestamp:
                return None

            ssh_node_port = None
            opencode_node_port = None
            try:
                svc = self.core_v1.read_namespaced_service(
                    name=f"{instance_id}-svc",
                    namespace=self.namespace
                )
                by_name = self._svc_node_ports_by_name(svc)
                node_port = by_name.get("jupyter") or (svc.spec.ports[0].node_port if svc.spec.ports else None)
                opencode_node_port = by_name.get("opencode")
                ssh_node_port = by_name.get("ssh")
            except ApiException:
                node_port = None
                opencode_node_port = None

            email = pod.metadata.annotations.get("amd-oneclick/email", "unknown")
            github_path = pod.metadata.annotations.get("amd-oneclick/github-path")
            instance_type = pod.metadata.annotations.get("amd-oneclick/instance-type", "jupyter")
            gpu_count = 1
            try:
                gpu_count = int(pod.spec.containers[0].resources.requests.get("amd.com/gpu", 1))
            except Exception:
                pass

            return {
                "id": instance_id,
                "email": email,
                "pod_name": pod.metadata.name,
                "service_name": f"{instance_id}-svc",
                "image": pod.spec.containers[0].image,
                "status": pod.status.phase.lower(),
                "created_at": pod.metadata.creation_timestamp,
                "node_port": node_port,
                "opencode_node_port": opencode_node_port,
                "ssh_node_port": ssh_node_port,
                **self._ssh_access(ssh_node_port),
                "url": self._build_url(node_port, github_path, instance_id, use_path_proxy=pod.metadata.annotations.get("amd-oneclick/path-proxy") == "true") if node_port else None,
                "opencode_url": self._build_opencode_url(opencode_node_port, instance_id),
                **self._opencode_auth(opencode_node_port, instance_id),
                "instance_type": instance_type,
                "app_port": int(pod.metadata.annotations.get("amd-oneclick/app-port")) if pod.metadata.annotations.get("amd-oneclick/app-port") else None,
                "api_kind": pod.metadata.annotations.get("amd-oneclick/api-kind") == "true",
                "api_key": pod.metadata.annotations.get("amd-oneclick/api-key"),
                "api_base_suffix": pod.metadata.annotations.get("amd-oneclick/api-base-suffix") or "",
                "gpu_count": gpu_count,
                "resource_profile": pod.metadata.annotations.get("amd-oneclick/resource-profile"),
                "cpu_limit": pod.metadata.annotations.get("amd-oneclick/cpu-limit"),
                "memory_limit": pod.metadata.annotations.get("amd-oneclick/memory-limit"),
                "github_org": pod.metadata.annotations.get("amd-oneclick/github-org"),
                "github_repo": pod.metadata.annotations.get("amd-oneclick/github-repo"),
                "github_path": github_path,
                "template_id": pod.metadata.annotations.get("amd-oneclick/template-id"),
                "template_title": pod.metadata.annotations.get("amd-oneclick/template-title"),
            }
        except ApiException as e:
            if e.status == 404:
                return None
            raise
    
    def delete_instance_by_id(self, instance_id: str) -> bool:
        """Delete a notebook instance by instance ID"""
        deleted = False
        
        # Delete Service
        try:
            self.core_v1.delete_namespaced_service(
                name=f"{instance_id}-svc",
                namespace=self.namespace
            )
            logger.info(f"Deleted service {instance_id}-svc")
            deleted = True
        except ApiException as e:
            if e.status != 404:
                logger.warning(f"Error deleting service: {e}")
        
        # Delete Pod
        try:
            self.core_v1.delete_namespaced_pod(
                name=instance_id,
                namespace=self.namespace
            )
            logger.info(f"Deleted pod {instance_id}")
            deleted = True
        except ApiException as e:
            if e.status != 404:
                logger.warning(f"Error deleting pod: {e}")
        
        return deleted
    
    def delete_instance(self, email: str) -> bool:
        """Delete a notebook instance"""
        instance_id = self._generate_instance_id(email)
        return self.delete_instance_by_id(instance_id)
    
    def list_instances(self) -> list:
        """List all notebook instances"""
        instances = []
        
        try:
            pods = self.core_v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"app={settings.NOTEBOOK_LABEL_PREFIX}"
            )
            
            for pod in pods.items:
                instance_id = pod.metadata.labels.get("instance-id", "unknown")
                email = pod.metadata.annotations.get("amd-oneclick/email", "unknown")
                created_at = pod.metadata.creation_timestamp
                
                # Get GitHub info from annotations
                github_org = pod.metadata.annotations.get("amd-oneclick/github-org")
                github_repo = pod.metadata.annotations.get("amd-oneclick/github-repo")
                github_path = pod.metadata.annotations.get("amd-oneclick/github-path")
                
                # Get NodePort from service
                node_port = None
                opencode_node_port = None
                try:
                    svc = self.core_v1.read_namespaced_service(
                        name=f"{instance_id}-svc",
                        namespace=self.namespace
                    )
                    node_port, opencode_node_port = self._extract_node_ports(svc)
                except ApiException:
                    pass
                
                # Calculate uptime
                uptime_minutes = 0
                if created_at:
                    uptime_delta = datetime.now(timezone.utc) - created_at.replace(tzinfo=timezone.utc)
                    uptime_minutes = int(uptime_delta.total_seconds() / 60)
                
                instance_type = pod.metadata.annotations.get("amd-oneclick/instance-type", "jupyter")
                gpu_count = 1
                try:
                    gpu_count = int(pod.spec.containers[0].resources.requests.get("amd.com/gpu", 1))
                except Exception:
                    pass

                instances.append({
                    "id": instance_id,
                    "email": email,
                    "pod_name": pod.metadata.name,
                    "service_name": f"{instance_id}-svc",
                    "image": pod.spec.containers[0].image if pod.spec.containers else "unknown",
                    "status": pod.status.phase.lower() if pod.status.phase else "unknown",
                    "created_at": created_at.isoformat() if created_at else None,
                    "node_port": node_port,
                    "opencode_node_port": opencode_node_port,
                    "url": self._build_url(node_port, github_path, instance_id, use_path_proxy=pod.metadata.annotations.get("amd-oneclick/path-proxy") == "true") if node_port else None,
                    "opencode_url": self._build_opencode_url(opencode_node_port, instance_id),
                    **self._opencode_auth(opencode_node_port, instance_id),
                    "uptime_minutes": uptime_minutes,
                    "instance_type": instance_type,
                    "gpu_count": gpu_count,
                    "github_org": github_org,
                    "github_repo": github_repo,
                    "github_path": github_path,
                })
        except ApiException as e:
            logger.error(f"Error listing pods: {e}")
        
        return instances
    
    def delete_all_instances(self) -> int:
        """Delete all notebook instances"""
        instances = self.list_instances()
        deleted_count = 0
        
        for instance in instances:
            if self.delete_instance(instance["email"]):
                deleted_count += 1
        
        return deleted_count
    
    def get_pod_status(self, email: str, instance_id: Optional[str] = None) -> Optional[str]:
        """Get the current status of a pod"""
        details = self.get_pod_status_details(email, instance_id=instance_id)
        return details.get("status") if details else None

    def get_pod_status_details(self, email: str, instance_id: Optional[str] = None) -> Optional[dict]:
        """Return structured pod readiness and failure details for UI and billing gates."""
        if not instance_id:
            instance_id = self._generate_instance_id(email)
        
        try:
            pod = self.core_v1.read_namespaced_pod(
                name=instance_id,
                namespace=self.namespace
            )
            
            phase = pod.status.phase.lower() if pod.status.phase else "unknown"
            reason = pod.status.reason or ""
            message = pod.status.message or ""
            pod_scheduled = True

            for condition in pod.status.conditions or []:
                if condition.type == "PodScheduled" and condition.status != "True":
                    pod_scheduled = False
                    reason = condition.reason or reason or "Unschedulable"
                    message = condition.message or message or "Pod is not scheduled"
                    return {
                        "status": "pending",
                        "phase": phase,
                        "reason": reason,
                        "message": message,
                        "ready": False,
                        "pod_scheduled": pod_scheduled,
                        "jupyter_ready": False,
                    }
            
            # Check container statuses for more detail
            if pod.status.container_statuses:
                container_status = pod.status.container_statuses[0]
                if container_status.ready:
                    # For app-type instances, readiness = the app port answering on the
                    # pod IP (not Jupyter 8888 on the node port).
                    annos = pod.metadata.annotations or {}
                    app_port_anno = annos.get("amd-oneclick/app-port")
                    if app_port_anno:
                        pod_ip = pod.status.pod_ip
                        if pod_ip and self._check_tcp_ready(pod_ip, int(app_port_anno)):
                            return {
                                "status": "ready", "phase": phase, "reason": "",
                                "message": "App is ready", "ready": True,
                                "pod_scheduled": pod_scheduled, "jupyter_ready": False,
                            }
                        return {
                            "status": "jupyter_starting", "phase": phase, "reason": "AppStarting",
                            "message": "Container is ready but the app is not responding yet",
                            "ready": False, "pod_scheduled": pod_scheduled, "jupyter_ready": False,
                        }
                    # Container is ready. Decide readiness by the relevant signal:
                    #  - notebook (jupyter/opencode): Jupyter answering on 8888
                    #  - SSH-enabled: sshd answering on the SSH port
                    #  - custom (image-defined command): the image may serve
                    #    something other than Jupyter, so a Running container is ready
                    instance = self.get_instance_by_id(instance_id)
                    inst_type = annos.get("amd-oneclick/instance-type", "jupyter")
                    is_custom = bool(INSTANCE_TYPES.get(inst_type, {}).get("image_defined_command"))
                    ssh_enabled = (annos.get("amd-oneclick/ssh-enabled") == "true")
                    pod_ip = pod.status.pod_ip
                    if instance and instance.get("node_port") and self._check_jupyter_ready(instance["node_port"]):
                        return {
                            "status": "ready", "phase": phase, "reason": "",
                            "message": "Notebook is ready", "ready": True,
                            "pod_scheduled": pod_scheduled, "jupyter_ready": True,
                        }
                    if ssh_enabled and pod_ip and self._check_tcp_ready(pod_ip, int(settings.SSH_PORT)):
                        return {
                            "status": "ready", "phase": phase, "reason": "",
                            "message": "SSH is ready", "ready": True,
                            "pod_scheduled": pod_scheduled, "jupyter_ready": False,
                        }
                    if is_custom:
                        return {
                            "status": "ready", "phase": phase, "reason": "",
                            "message": "Container is ready", "ready": True,
                            "pod_scheduled": pod_scheduled, "jupyter_ready": False,
                        }
                    if instance and instance.get("node_port"):
                        return {
                            "status": "jupyter_starting",
                            "phase": phase,
                            "reason": "JupyterStarting",
                            "message": "Container is ready but Jupyter is not responding yet",
                            "ready": False,
                            "pod_scheduled": pod_scheduled,
                            "jupyter_ready": False,
                        }
                    return {
                        "status": "running",
                        "phase": phase,
                        "reason": "ServicePending",
                        "message": "Container is ready but service endpoint is not available yet",
                        "ready": False,
                        "pod_scheduled": pod_scheduled,
                        "jupyter_ready": False,
                    }
                elif container_status.state.waiting:
                    reason = container_status.state.waiting.reason or "waiting"
                    message = container_status.state.waiting.message or ""
                    failed_reasons = {
                        "ImagePullBackOff",
                        "ErrImagePull",
                        "CrashLoopBackOff",
                        "CreateContainerConfigError",
                        "CreateContainerError",
                        "InvalidImageName",
                    }
                    status = "failed" if reason in failed_reasons else ("initializing" if reason in ["ContainerCreating", "PodInitializing"] else "loading")
                    return {
                        "status": status,
                        "phase": phase,
                        "reason": reason,
                        "message": message or reason,
                        "ready": False,
                        "pod_scheduled": pod_scheduled,
                        "jupyter_ready": False,
                    }
                elif container_status.state.terminated:
                    reason = container_status.state.terminated.reason or "Terminated"
                    message = container_status.state.terminated.message or reason
                    return {
                        "status": "failed",
                        "phase": phase,
                        "reason": reason,
                        "message": message,
                        "ready": False,
                        "pod_scheduled": pod_scheduled,
                        "jupyter_ready": False,
                    }
                elif container_status.state.running:
                    # Container is running but not ready yet
                    return {
                        "status": "running",
                        "phase": phase,
                        "reason": "ContainerNotReady",
                        "message": "Container is running but readiness probe has not passed",
                        "ready": False,
                        "pod_scheduled": pod_scheduled,
                        "jupyter_ready": False,
                    }
            
            if phase == "failed":
                return {
                    "status": "failed",
                    "phase": phase,
                    "reason": reason or "PodFailed",
                    "message": message or "Pod failed",
                    "ready": False,
                    "pod_scheduled": pod_scheduled,
                    "jupyter_ready": False,
                }

            return {
                "status": phase,
                "phase": phase,
                "reason": reason or phase,
                "message": message or f"Pod phase is {phase}",
                "ready": False,
                "pod_scheduled": pod_scheduled,
                "jupyter_ready": False,
            }
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def _pod_events(self, instance_id: str, limit: int = 50) -> list:
        """Return pod events (oldest→newest) as plain dicts. Best-effort: [] on any failure."""
        try:
            resp = self.core_v1.list_namespaced_event(
                namespace=self.namespace,
                field_selector=f"involvedObject.name={instance_id}",
            )
        except Exception:
            return []
        items = list(getattr(resp, "items", None) or [])

        def _ts(ev):
            return (
                getattr(ev, "last_timestamp", None)
                or getattr(ev, "event_time", None)
                or getattr(ev, "first_timestamp", None)
            )

        # Stable decorate-sort: events without a usable timestamp keep their original
        # (API-returned, roughly chronological) order and sort after timestamped ones.
        def _sort_key(pair):
            i, ev = pair
            t = _ts(ev)
            return (0, t.timestamp(), i) if t is not None else (1, 0.0, i)

        try:
            items = [ev for _, ev in sorted(enumerate(items), key=_sort_key)]
        except Exception:
            pass
        out = []
        for ev in items[-limit:]:
            t = _ts(ev)
            out.append(
                {
                    "time": t.isoformat() if t is not None else None,
                    "reason": getattr(ev, "reason", None) or "",
                    "message": getattr(ev, "message", None) or "",
                }
            )
        return out

    def get_startup_detail(self, instance_id: str) -> Optional[str]:
        """Best-effort human-readable detail of why an instance is still starting.

        Returns a specific message (image pulling with elapsed time, image-pull failure,
        scheduling blocked by resources, …) or None when nothing useful can be derived
        (caller falls back to a static status message).
        """
        try:
            pod = self.core_v1.read_namespaced_pod(name=instance_id, namespace=self.namespace)
        except ApiException:
            return None

        status = getattr(pod, "status", None)
        phase = (getattr(status, "phase", None) or "").lower() if status else ""
        container_statuses = getattr(status, "container_statuses", None) if status else None

        cs = container_statuses[0] if container_statuses else None
        waiting = getattr(getattr(cs, "state", None), "waiting", None) if cs else None
        waiting_reason = getattr(waiting, "reason", None) if waiting else None

        if waiting_reason in ("ImagePullBackOff", "ErrImagePull"):
            msg = getattr(waiting, "message", None) or "image could not be pulled"
            return f"Image pull failed: {msg}"

        events = self._pod_events(instance_id)

        if waiting_reason in ("ContainerCreating", "PodInitializing") or (
            cs is None and phase in ("pending", "")
        ):
            # Look for the most recent image-pull progress event.
            pulling = None
            for ev in reversed(events):
                if ev["reason"] in ("Pulling", "Pulled"):
                    pulling = ev
                    break
            if pulling is not None and pulling["reason"] == "Pulling":
                image = self._image_from_pull_message(pulling["message"])
                elapsed = self._elapsed_label(pulling["time"]) or self._elapsed_label(
                    getattr(status, "start_time", None) if status else None
                )
                label = image or "image"
                if elapsed:
                    return f"Pulling image {label} ({elapsed})…"
                return f"Pulling image {label}…"

            # No pull yet — maybe scheduling is blocked by resources.
            for ev in reversed(events):
                if ev["reason"] in ("FailedScheduling", "FailedCreate") and ev["message"]:
                    return f"Waiting: {ev['message']}"

            if waiting_reason in ("ContainerCreating", "PodInitializing"):
                return "Preparing container…"
            if phase in ("pending", ""):
                return "Waiting for resources…"

        # Pending with no container status and no useful event → resource wait.
        if phase == "pending":
            for ev in reversed(events):
                if ev["reason"] in ("FailedScheduling", "FailedCreate") and ev["message"]:
                    return f"Waiting: {ev['message']}"
            return "Waiting for resources…"

        return None

    @staticmethod
    def _image_from_pull_message(message: Optional[str]) -> Optional[str]:
        """Extract the image ref from a kubelet 'Pulling image "repo:tag"' event message."""
        if not message:
            return None
        if '"' in message:
            parts = message.split('"')
            if len(parts) >= 2 and parts[1].strip():
                return parts[1].strip()
        return None

    @staticmethod
    def _elapsed_label(start) -> Optional[str]:
        """Return a compact elapsed label (e.g. '4m', '45s') since an ISO timestamp/datetime."""
        if start is None:
            return None
        if isinstance(start, str):
            try:
                start = datetime.fromisoformat(start.replace("Z", "+00:00"))
            except ValueError:
                return None
        if getattr(start, "tzinfo", None) is None:
            start = start.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - start
        secs = int(delta.total_seconds())
        if secs < 0:
            return None
        if secs < 60:
            return f"{secs}s"
        return f"{secs // 60}m"

    def get_pod_logs(self, instance_id: str, tail_lines: int = 200) -> dict:
        """Return pod events plus container stdout for the live log view during startup.

        Shape: {"events": [{"time","reason","message"}...], "container": "<stdout or ''>"}.
        During image pull / pending the container has not started, so 'container' is "" and
        the events carry the useful signal. Missing pod → empty payload.
        """
        try:
            self.core_v1.read_namespaced_pod(name=instance_id, namespace=self.namespace)
        except ApiException as e:
            if e.status == 404:
                return {"events": [], "container": "", "status": "not_found"}
            return {"events": [], "container": ""}

        events = self._pod_events(instance_id)
        container = ""
        try:
            container = self.core_v1.read_namespaced_pod_log(
                name=instance_id,
                namespace=self.namespace,
                tail_lines=tail_lines,
                limit_bytes=262144,
            ) or ""
        except ApiException:
            container = ""
        return {"events": events, "container": container}

    def _check_jupyter_ready(self, node_port: int, timeout: float = 2.0) -> bool:
        """Check if Jupyter is responding on the given port"""
        return self._check_tcp_ready(settings.SERVICE_HOST, node_port, timeout)

    def _check_tcp_ready(self, host: str, port: int, timeout: float = 2.0) -> bool:
        """Check if a TCP port accepts connections on the given host."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            result = sock.connect_ex((host, port))
            sock.close()
            return result == 0
        except Exception as e:
            logger.debug(f"TCP health check failed for {host}:{port}: {e}")
            return False
    
    def check_pod_activity(self, email: str) -> Optional[datetime]:
        """Check last activity of a pod by examining logs"""
        instance_id = self._generate_instance_id(email)
        
        try:
            # Get recent logs
            logs = self.core_v1.read_namespaced_pod_log(
                name=instance_id,
                namespace=self.namespace,
                tail_lines=10,
                timestamps=True
            )
            
            if logs:
                # Parse last log timestamp
                lines = logs.strip().split('\n')
                if lines:
                    last_line = lines[-1]
                    # Kubernetes log format: 2024-01-01T00:00:00.000000000Z ...
                    timestamp_str = last_line.split(' ')[0]
                    try:
                        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    except ValueError:
                        pass
            
            return None
        except ApiException:
            return None
    
    def cleanup_idle_instances(self) -> list:
        """Cleanup idle and expired instances"""
        cleaned = []
        instances = self.list_instances()
        now = datetime.now(timezone.utc)

        for instance in instances:
            should_delete = False
            reason = ""

            itype = instance.get("instance_type", "jupyter")
            type_cfg = INSTANCE_TYPES.get(itype, {})
            raw_lifetime = type_cfg.get("max_lifetime_hours")
            max_lifetime = raw_lifetime if raw_lifetime is not None else settings.MAX_LIFETIME_HOURS
            raw_idle = type_cfg.get("idle_timeout_minutes")
            idle_timeout = raw_idle if raw_idle is not None else settings.IDLE_TIMEOUT_MINUTES

            uptime_hours = instance["uptime_minutes"] / 60
            if uptime_hours >= max_lifetime:
                should_delete = True
                reason = f"exceeded max lifetime ({max_lifetime}h)"

            elif instance["status"] == "running" and idle_timeout > 0:
                last_activity = self.check_pod_activity(instance["email"])
                if last_activity:
                    idle_minutes = (now - last_activity).total_seconds() / 60
                    if idle_minutes >= idle_timeout:
                        should_delete = True
                        reason = f"idle for {int(idle_minutes)} minutes (limit {idle_timeout}m)"

            if should_delete:
                if self.delete_instance(instance["email"]):
                    cleaned.append({
                        "email": instance["email"],
                        "reason": reason
                    })
                    logger.info(f"Cleaned up instance for {instance['email']}: {reason}")

        return cleaned


# Global K8s client instance
k8s_client = K8sClient()
