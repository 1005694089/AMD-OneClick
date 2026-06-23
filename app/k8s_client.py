"""
Kubernetes client for managing notebook instances
"""
import hashlib
import logging
import os
import re
import shlex
import socket
import time
import random
import threading
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from .config import settings, INSTANCE_TYPES, APP_FRAMEWORK_PRESETS

logger = logging.getLogger(__name__)

NODE_PORT_MAX = 32767
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

jupyter lab --ip=0.0.0.0 --port={settings.NOTEBOOK_PORT} --no-browser --allow-root --ServerApp.token='{settings.NOTEBOOK_TOKEN}' --ServerApp.base_url='{self._jupyter_base_url(instance_id)}' --notebook-dir={workspace}/repo
"""
            return f"""
{model_link_script}
mkdir -p {workspace}/notebooks
cd {workspace}/notebooks

if [ ! -f {shlex.quote(notebook_filename)} ]; then
    echo "Downloading {notebook_filename}..."
    for i in 1 2 3; do
        if curl -fsSL --connect-timeout 30 --max-time 120 -o {shlex.quote(notebook_filename)} {shlex.quote(github_info["raw_url"])}; then
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

jupyter lab --ip=0.0.0.0 --port={settings.NOTEBOOK_PORT} --no-browser --allow-root --ServerApp.token='{settings.NOTEBOOK_TOKEN}' --ServerApp.base_url='{self._jupyter_base_url(instance_id)}' --notebook-dir={workspace}/notebooks
"""

        if instance_type == "opencode":
            return f"""
export PATH="/root/.opencode/bin:$PATH"
{model_link_script}
cd {workspace}
jupyter lab --ip=0.0.0.0 --port={settings.NOTEBOOK_PORT} --no-browser --allow-root --ServerApp.token='{settings.NOTEBOOK_TOKEN}' --ServerApp.base_url='{self._jupyter_base_url(instance_id)}' --notebook-dir={workspace}
"""

        # Default: jupyter
        return f"""
export PATH="/root/.opencode/bin:$PATH"
{model_link_script}
cd {workspace}
jupyter lab --ip=0.0.0.0 --port={settings.NOTEBOOK_PORT} --no-browser --allow-root --ServerApp.token='{settings.NOTEBOOK_TOKEN}' --ServerApp.base_url='{self._jupyter_base_url(instance_id)}' --notebook-dir={workspace}
"""

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
        return f"""
set -e
export PATH="/root/.opencode/bin:$PATH"
mkdir -p {workspace}
{clone_block}
cd {shlex.quote(run_dir)} 2>/dev/null || cd {workspace}
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
                          start_command: Optional[str] = None,
                          app_port: Optional[int] = None) -> dict:
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

        if github_info:
            annotations["amd-oneclick/github-org"] = github_info.get("org", "")
            annotations["amd-oneclick/github-repo"] = github_info.get("repo", "")
            annotations["amd-oneclick/github-branch"] = github_info.get("branch", "")
            annotations["amd-oneclick/github-path"] = github_info.get("path", "")
            annotations["amd-oneclick/github-raw-url"] = github_info.get("raw_url", "")
            annotations["amd-oneclick/github-repo-url"] = github_info.get("repo_url", "")
            annotations["amd-oneclick/template-id"] = github_info.get("template_id", "")
            annotations["amd-oneclick/template-title"] = github_info.get("template_title", "")

        image_defined_command = bool(INSTANCE_TYPES.get(instance_type, {}).get("image_defined_command"))
        app_preset = APP_FRAMEWORK_PRESETS.get(instance_type)
        is_app_type = app_preset is not None
        if is_app_type:
            _eff_cmd, _eff_port = self._resolve_app_command(instance_type, start_command, app_port)
            annotations["amd-oneclick/app-port"] = str(_eff_port)
            startup_script = self._build_app_startup_script(
                instance_id, instance_type, github_info,
                start_command=start_command, app_port=app_port,
            )
        else:
            startup_script = self._build_startup_script(instance_id, instance_type, github_info)

        workspace_volume_type = (settings.WORKSPACE_VOLUME_TYPE or "hostPath").strip().lower()
        workspace_uses_empty_dir = workspace_volume_type == "emptydir"
        hf_cache_volume_type = (settings.HF_CACHE_VOLUME_TYPE or "emptyDir").strip().lower()
        hf_cache_uses_empty_dir = hf_cache_volume_type == "emptydir"

        volume_mounts = [
            {"name": "shm", "mountPath": "/dev/shm"},
            {"name": "hf-cache", "mountPath": settings.HF_CACHE_MOUNT_PATH},
            {"name": "workspace", "mountPath": settings.WORKSPACE_MOUNT_PATH},
        ]
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
        if workspace_uses_empty_dir:
            workspace_empty_dir = {}
            if settings.WORKSPACE_EMPTYDIR_SIZE_LIMIT.strip():
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
        ]
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
        if settings.WORKSPACE_QUOTA_ENABLED and not workspace_uses_empty_dir:
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
        if settings.EPHEMERAL_STORAGE_LIMIT.strip():
            container_limits["ephemeral-storage"] = settings.EPHEMERAL_STORAGE_LIMIT.strip()
        if settings.EPHEMERAL_STORAGE_REQUEST.strip():
            container_requests["ephemeral-storage"] = settings.EPHEMERAL_STORAGE_REQUEST.strip()

        container_ports = [
            {"containerPort": settings.NOTEBOOK_PORT, "name": "jupyter"}
        ]
        for _app_name, _app_port in settings.APP_PORTS.items():
            container_ports.append({"containerPort": int(_app_port), "name": _app_name[:15]})

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

        spec = {
            "securityContext": {
                "supplementalGroups": settings.GPU_SUPPLEMENTAL_GROUPS
            },
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
            "tolerations": [
                {
                    "key": "amd.com/gpu",
                    "operator": "Exists",
                    "effect": "NoSchedule"
                }
            ],
            "containers": [
                notebook_container
            ],
            "volumes": volumes,
            "restartPolicy": "Always"
        }
        if init_containers:
            spec["initContainers"] = init_containers
        if workspace_quota_node_name:
            spec["nodeName"] = workspace_quota_node_name

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
    
    def _get_service_manifest(self, email: str, instance_id: str, node_port: int) -> dict:
        """Generate Service manifest"""
        labels = self._get_labels(email, instance_id)
        
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
                "ports": [
                    {
                        "name": "jupyter",
                        "port": settings.NOTEBOOK_PORT,
                        "targetPort": settings.NOTEBOOK_PORT,
                        "nodePort": node_port
                    }
                ]
            }
        }
    
    def _used_node_ports(self) -> set[int]:
        used_ports: set[int] = set()
        services = self.core_v1.list_service_for_all_namespaces()
        for svc in services.items:
            for port in svc.spec.ports or []:
                if port.node_port:
                    used_ports.add(int(port.node_port))
        return used_ports

    def _allocate_node_port(self, used_ports: Optional[set[int]] = None, start_port: Optional[int] = None) -> int:
        """Allocate an available NodePort candidate."""
        used_ports = used_ports if used_ports is not None else self._used_node_ports()
        port = start_port or settings.NODE_PORT_BASE
        while port in used_ports and port <= NODE_PORT_MAX:
            port += 1
        if port <= NODE_PORT_MAX:
            return port
        for port in range(settings.NODE_PORT_BASE, NODE_PORT_MAX + 1):
            if port not in used_ports:
                return port
        raise RuntimeError("No available NodePort in configured range")

    def _create_service_with_nodeport_retry(self, email: str, instance_id: str) -> tuple[int, bool]:
        try:
            existing = self.core_v1.read_namespaced_service(name=f"{instance_id}-svc", namespace=self.namespace)
            node_port = existing.spec.ports[0].node_port if existing.spec.ports else None
            if node_port:
                return int(node_port), False
        except ApiException as e:
            if e.status != 404:
                raise

        with _node_port_lock:
            used_ports = self._used_node_ports()
            start = settings.NODE_PORT_BASE + random.randint(0, min(200, max(0, NODE_PORT_MAX - settings.NODE_PORT_BASE)))
            node_port = self._allocate_node_port(used_ports, start_port=start)
            for _ in range(512):
                try:
                    self.core_v1.create_namespaced_service(namespace=self.namespace, body=self._get_service_manifest(email, instance_id, node_port))
                    logger.info("Created service %s-svc with NodePort %s", instance_id, node_port)
                    return node_port, True
                except ApiException as e:
                    message = str(e)
                    if e.status == 409:
                        existing = self.core_v1.read_namespaced_service(name=f"{instance_id}-svc", namespace=self.namespace)
                        existing_port = existing.spec.ports[0].node_port if existing.spec.ports else None
                        if existing_port:
                            return int(existing_port), False
                        raise
                    if e.status == 422 and ("provided port is already allocated" in message or "invalid" in message.lower()):
                        used_ports = self._used_node_ports()
                        used_ports.add(node_port)
                        node_port = self._allocate_node_port(used_ports, start_port=node_port + 1)
                        continue
                    raise
        raise RuntimeError("Unable to allocate NodePort for service")

    def _prepull_name(self, image_id: int) -> str:
        return f"image-prepull-catalog-{image_id}"

    def _prepull_labels(self, image_id: int) -> dict:
        return {
            "app": "amd-oneclick-image-prepull",
            "image-id": str(image_id),
        }

    def _eligible_prepull_nodes(self) -> set[str]:
        """Nodes that should count toward image availability."""
        eligible: set[str] = set()
        nodes = self.core_v1.list_node()
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

    def sync_image_to_nodes(self, image_id: int, image: str) -> dict:
        """Create or replace a DaemonSet that pulls the image on every node."""
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
                                "imagePullPolicy": "Always",
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

    def get_image_sync_status(self, image_id: int) -> dict:
        """Return DaemonSet sync status for an image catalog entry."""
        name = self._prepull_name(image_id)
        try:
            ds = self.apps_v1.read_namespaced_daemon_set(name=name, namespace=self.namespace)
            eligible_nodes = self._eligible_prepull_nodes()
            desired = len(eligible_nodes) or (ds.status.desired_number_scheduled or 0)
            ds_uid = ds.metadata.uid
            image = ds.spec.template.spec.containers[0].image
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
                if status.image == image and status.image_id:
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
        try:
            self.apps_v1.delete_namespaced_daemon_set(name=self._prepull_name(image_id), namespace=self.namespace)
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
            try:
                svc = self.core_v1.read_namespaced_service(
                    name=f"{instance_id}-svc",
                    namespace=self.namespace
                )
                node_port = svc.spec.ports[0].node_port if svc.spec.ports else None
            except ApiException:
                node_port = None
            
            return {
                "id": instance_id,
                "email": email,
                "pod_name": pod.metadata.name,
                "service_name": f"{instance_id}-svc",
                "image": pod.spec.containers[0].image,
                "status": pod.status.phase.lower(),
                "created_at": pod.metadata.creation_timestamp,
                "node_port": node_port,
                "url": self._build_url(node_port, instance_id=instance_id, use_path_proxy=pod.metadata.annotations.get("amd-oneclick/path-proxy") == "true") if node_port else None
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
    
    def create_instance(self, email: str, image: Optional[str] = None,
                        instance_type: str = "jupyter",
                        gpu_count: int = 1,
                        github_info: Optional[dict] = None,
                        custom_instance_id: Optional[str] = None,
                        resource_profile: Optional[str] = None,
                        start_command: Optional[str] = None,
                        app_port: Optional[int] = None) -> dict:
        """Create a new notebook instance"""
        instance_id = custom_instance_id or self._generate_instance_id(email)
        image = image or settings.DEFAULT_IMAGE

        existing = self.get_instance_by_id(instance_id)
        if existing:
            return existing

        workspace_quota_node_name = self._ensure_workspace_quota(instance_id)
        network_disk_claim_name = self._ensure_network_disk(instance_id)
        pod_manifest = self._get_pod_manifest(
            email, instance_id, image,
            instance_type=instance_type,
            gpu_count=gpu_count,
            github_info=github_info,
            resource_profile=resource_profile,
            network_disk_claim_name=network_disk_claim_name,
            workspace_quota_node_name=workspace_quota_node_name,
            start_command=start_command,
            app_port=app_port,
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
            node_port, service_created = self._create_service_with_nodeport_retry(email, instance_id)
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
            "url": self._build_url(node_port, notebook_path, instance_id, use_path_proxy=True),
            "github_info": github_info
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

            try:
                svc = self.core_v1.read_namespaced_service(
                    name=f"{instance_id}-svc",
                    namespace=self.namespace
                )
                node_port = svc.spec.ports[0].node_port if svc.spec.ports else None
            except ApiException:
                node_port = None

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
                "url": self._build_url(node_port, github_path, instance_id, use_path_proxy=pod.metadata.annotations.get("amd-oneclick/path-proxy") == "true") if node_port else None,
                "instance_type": instance_type,
                "app_port": int(pod.metadata.annotations.get("amd-oneclick/app-port")) if pod.metadata.annotations.get("amd-oneclick/app-port") else None,
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
                try:
                    svc = self.core_v1.read_namespaced_service(
                        name=f"{instance_id}-svc",
                        namespace=self.namespace
                    )
                    node_port = svc.spec.ports[0].node_port if svc.spec.ports else None
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
                    "url": self._build_url(node_port, github_path, instance_id, use_path_proxy=pod.metadata.annotations.get("amd-oneclick/path-proxy") == "true") if node_port else None,
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
                    # Container is ready, but we need to verify Jupyter is actually responding
                    instance = self.get_instance_by_id(instance_id)
                    if instance and instance.get("node_port"):
                        if self._check_jupyter_ready(instance["node_port"]):
                            return {
                                "status": "ready",
                                "phase": phase,
                                "reason": "",
                                "message": "Notebook is ready",
                                "ready": True,
                                "pod_scheduled": pod_scheduled,
                                "jupyter_ready": True,
                            }
                        else:
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
