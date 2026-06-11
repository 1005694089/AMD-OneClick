"""
Kubernetes client for managing notebook instances
"""
import hashlib
import logging
import os
import re
import shlex
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote, urlparse

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from .config import settings, INSTANCE_TYPES

logger = logging.getLogger(__name__)

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

        # kubernetes>=35 generated clients use the BearerToken auth name, while
        # load_incluster_config may populate the older "authorization" key.
        self._ensure_bearer_token_auth()
        
        self.core_v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.namespace = settings.K8S_NAMESPACE
        self._node_port_lock = threading.Lock()

    def _ensure_bearer_token_auth(self):
        cfg = client.Configuration.get_default_copy()
        if cfg.api_key.get("BearerToken"):
            return
        if cfg.api_key.get("authorization"):
            cfg.api_key["BearerToken"] = cfg.api_key["authorization"]
            client.Configuration.set_default(cfg)
            return

        token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        if not os.path.exists(token_path):
            return
        token = open(token_path, encoding="utf-8").read().strip()
        cfg.api_key["BearerToken"] = f"bearer {token}"
        client.Configuration.set_default(cfg)
    
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

    def _image_requires_pull_secret(self, image: Optional[str]) -> bool:
        if not image:
            return False
        prefixes = {
            settings.DEFAULT_IMAGE,
            settings.ADMIN_IMAGE_REGISTRY,
            settings.CUSTOM_IMAGE_REGISTRY,
            settings.OSSUTIL_IMAGE,
            *settings.IMAGE_PULL_SECRET_REGISTRY_HOSTS,
        }
        for prefix in prefixes:
            prefix = (prefix or "").strip().rstrip("/")
            if prefix and (image == prefix or image.startswith(f"{prefix}/")):
                return True
        return False

    def _image_pull_secrets(self, *images: Optional[str]) -> list:
        if not settings.CUSTOM_IMAGE_PULL_SECRET_NAME:
            return []
        if any(self._image_requires_pull_secret(image) for image in images):
            return [{"name": settings.CUSTOM_IMAGE_PULL_SECRET_NAME}]
        return []

    def _node_is_ready_for_scheduling(self, node) -> bool:
        if getattr(node.spec, "unschedulable", False):
            return False
        for condition in node.status.conditions or []:
            if condition.type == "Ready":
                return condition.status == "True"
        return False

    def _cached_image_node_names(self, image: str) -> list[str]:
        if not settings.IMAGE_CACHE_NODE_AFFINITY_ENABLED or not image:
            return []
        try:
            nodes = self.core_v1.list_node(_request_timeout=settings.K8S_READ_TIMEOUT_SECONDS).items
        except Exception as e:
            logger.warning("Unable to list nodes for image-cache affinity; scheduling normally: %s", e)
            return []

        node_names = []
        for node in nodes:
            if not self._node_is_ready_for_scheduling(node):
                continue
            for cached_image in node.status.images or []:
                if image in (cached_image.names or []):
                    node_names.append(node.metadata.name)
                    break
        return node_names

    def _image_cache_node_affinity(self, image: str) -> Optional[dict]:
        node_names = self._cached_image_node_names(image)
        if not node_names:
            return None
        logger.info("Restricting notebook image %s to %s cached nodes", image, len(node_names))
        return {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchFields": [
                                {"key": "metadata.name", "operator": "In", "values": node_names}
                            ]
                        }
                    ]
                }
            }
        }

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

        Security model: both services are on NodePorts whose URLs are only returned
        to the authenticated instance owner. This matches the existing Jupyter trust
        model (shared NOTEBOOK_TOKEN visible in every URL). OpenCode web runs without
        HTTP basic auth because Chrome blocks embedded credentials in URLs loaded from
        web pages. errexit is disabled so a failed optional service can never
        crash-loop the pod; `wait` keeps the container alive while Jupyter runs.
        """
        base_url = self._jupyter_base_url(instance_id)
        return (
            "set +e\n"
            f"jupyter lab --ip=0.0.0.0 --port={settings.NOTEBOOK_PORT} --no-browser --allow-root "
            f"--ServerApp.token='{settings.NOTEBOOK_TOKEN}' --ServerApp.base_url='{base_url}' "
            f"--notebook-dir={notebook_dir} &\n"
            f"opencode web --port {settings.OPENCODE_WEB_PORT} --hostname 0.0.0.0 "
            ">/tmp/opencode-web.log 2>&1 &\n"
            "wait\n"
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

{self._service_launch_snippet(instance_id, f"{workspace}/repo")}"""
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

{self._service_launch_snippet(instance_id, f"{workspace}/notebooks")}"""

        if instance_type == "opencode":
            return f"""
export PATH="/root/.opencode/bin:$PATH"
{model_link_script}
cd {workspace}
{self._service_launch_snippet(instance_id, workspace)}"""

        # Default: jupyter
        return f"""
export PATH="/root/.opencode/bin:$PATH"
{model_link_script}
cd {workspace}
{self._service_launch_snippet(instance_id, workspace)}"""

    def _oss_remote_uri(self, user_id: int) -> str:
        prefix = settings.OSS_BACKUP_PREFIX.strip("/")
        return f"oss://{settings.OSS_BUCKET}/{prefix}/{int(user_id)}/workspace/"

    def _oss_exclude_args(self) -> str:
        return "set -- " + " ".join(shlex.quote(f"--exclude={pattern}") for pattern in settings.OSS_EXCLUDES)

    def _oss_credential_shell(self) -> str:
        endpoint = shlex.quote(settings.OSS_ENDPOINT)
        return f"""
export OSS_ENDPOINT={endpoint}
resolve_ossutil() {{
  if command -v ossutil >/dev/null 2>&1; then echo ossutil; return 0; fi
  if command -v ossutil64 >/dev/null 2>&1; then echo ossutil64; return 0; fi
  echo "ossutil binary not found" >&2
  return 1
}}
read_creds() {{
  OSS_AK_ID="$(cat /etc/oss-creds/access_key_id 2>/dev/null || true)"
  OSS_AK_SECRET="$(cat /etc/oss-creds/access_key_secret 2>/dev/null || true)"
  OSS_STS_TOKEN="$(cat /etc/oss-creds/security_token 2>/dev/null || true)"
  if [ -z "$OSS_AK_ID" ] || [ -z "$OSS_AK_SECRET" ] || [ -z "$OSS_STS_TOKEN" ]; then
    echo "OSS STS credentials are missing" >&2
    return 1
  fi
}}
ossutil_with_creds() {{
  read_creds || return 1
  OSSUTIL_BIN="$(resolve_ossutil)" || return 1
  "$OSSUTIL_BIN" -e "$OSS_ENDPOINT" -i "$OSS_AK_ID" -k "$OSS_AK_SECRET" -t "$OSS_STS_TOKEN" "$@"
}}
"""

    def _oss_restore_script(self, user_id: int) -> str:
        remote = shlex.quote(self._oss_remote_uri(user_id))
        workspace = shlex.quote(settings.WORKSPACE_MOUNT_PATH)
        return f"""
set -u
{self._oss_credential_shell()}
WORKSPACE={workspace}
REMOTE={remote}
mkdir -p "$WORKSPACE"
rm -f "$WORKSPACE/.oss-restore-ok"
if ! ossutil_with_creds ls "$REMOTE" > /tmp/oss-list.txt 2> /tmp/oss-list.err; then
  echo "OSS restore failed: unable to list $REMOTE" >&2
  cat /tmp/oss-list.err >&2 || true
  exit 1
fi
if grep -Eq 'Object Number is: 0|Object Number is 0' /tmp/oss-list.txt || ! grep -q "oss://" /tmp/oss-list.txt; then
  echo "OSS restore skipped: no prior backup at $REMOTE"
  date -Iseconds > "$WORKSPACE/.oss-restore-ok"
  exit 0
fi
if ossutil_with_creds sync --delete "$REMOTE" "$WORKSPACE/"; then
  date -Iseconds > "$WORKSPACE/.oss-restore-ok"
else
  echo "OSS restore failed" >&2
  exit 1
fi
exit 0
"""

    def _oss_backup_script(self, user_id: int) -> str:
        remote = shlex.quote(self._oss_remote_uri(user_id))
        workspace = shlex.quote(settings.WORKSPACE_MOUNT_PATH)
        interval_seconds = max(60, int(settings.OSS_BACKUP_INTERVAL_MINUTES) * 60)
        quota_bytes = int(settings.OSS_INSTANCE_QUOTA_GB) * 1024 * 1024 * 1024
        exclude_set_args = self._oss_exclude_args()
        return f"""
set -u
{self._oss_credential_shell()}
WORKSPACE={workspace}
REMOTE={remote}
INTERVAL_SECONDS={interval_seconds}
QUOTA_BYTES={quota_bytes}
{exclude_set_args}
backup_once() {{
  if [ ! -f "$WORKSPACE/.oss-restore-ok" ]; then
    echo "OSS backup skipped: restore sentinel missing"
    return 0
  fi
  mkdir -p "$WORKSPACE"
  (
    if command -v flock >/dev/null 2>&1; then flock -n 9 || exit 0; fi
    LOCAL_BYTES="$(du -sb "$@" "$WORKSPACE" 2>/dev/null | awk '{{print $1}}' || true)"
    if [ -n "$LOCAL_BYTES" ] && [ "$LOCAL_BYTES" -gt "$QUOTA_BYTES" ]; then
      printf 'Workspace backup skipped: %s bytes exceeds %s byte quota.\\n' "$LOCAL_BYTES" "$QUOTA_BYTES" > "$WORKSPACE/.oss-backup-warning"
      exit 0
    fi
    if ossutil_with_creds sync --delete "$@" "$WORKSPACE/" "$REMOTE"; then
      date -Iseconds > "$WORKSPACE/.oss-last-backup-at"
      rm -f "$WORKSPACE/.oss-backup-warning"
      ossutil_with_creds du "$REMOTE" > "$WORKSPACE/.oss-remote-du" 2>/tmp/oss-du.err || true
    else
      echo "OSS backup failed" >&2
      exit 1
    fi
  ) 9>/tmp/oss-backup.lock
}}
finish_backup() {{
  echo "OSS final backup requested"
  backup_once || echo "OSS final backup failed" >&2
  exit 0
}}
trap finish_backup TERM INT
while true; do
  backup_once || true
  slept=0
  while [ "$slept" -lt "$INTERVAL_SECONDS" ]; do
    if [ -f "$WORKSPACE/.oss-backup-now" ]; then
      rm -f "$WORKSPACE/.oss-backup-now"
      break
    fi
    sleep 15
    slept=$((slept + 15))
  done
done
"""

    def _get_pod_manifest(self, email: str, instance_id: str, image: str,
                          instance_type: str = "jupyter",
                          gpu_count: int = 1,
                          github_info: Optional[dict] = None,
                          resource_profile: Optional[str] = None,
                          network_disk_claim_name: Optional[str] = None,
                          template_id: Optional[str] = None,
                          template_title: Optional[str] = None,
                          user_id: Optional[int] = None,
                          oss_secret_name: Optional[str] = None,
                          oss_launch_id: Optional[str] = None) -> dict:
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
        if user_id is not None:
            annotations["amd-oneclick/user-id"] = str(int(user_id))
        if oss_secret_name:
            annotations["amd-oneclick/oss-sts-secret"] = oss_secret_name
        if oss_launch_id:
            annotations["amd-oneclick/oss-launch-id"] = oss_launch_id
        network_disk_pvc_name = network_disk_claim_name or settings.NETWORK_DISK_PVC_NAME.strip()
        network_disk_enabled = bool(settings.NETWORK_DISK_ENABLED and network_disk_pvc_name)
        use_static_network_disk_subpath = bool(network_disk_enabled and not network_disk_claim_name)
        network_disk_sub_path = self._network_disk_sub_path(instance_id) if use_static_network_disk_subpath else ""
        if network_disk_enabled:
            annotations["amd-oneclick/network-disk-pvc"] = network_disk_pvc_name
            if network_disk_sub_path:
                annotations["amd-oneclick/network-disk-sub-path"] = network_disk_sub_path

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

        startup_script = self._build_startup_script(instance_id, instance_type, github_info)

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
            {
                "name": "hf-cache",
                "hostPath": {
                    "path": settings.HF_CACHE_HOST_PATH,
                    "type": "DirectoryOrCreate"
                }
            },
            {
                "name": "workspace",
                "hostPath": {
                    "path": self._workspace_host_path(instance_id),
                    "type": "DirectoryOrCreate"
                }
            },
        ]
        env = [
            {"name": "SHELL", "value": "/bin/bash"},
            {"name": "USER_EMAIL", "value": email},
            {"name": "INSTANCE_TYPE", "value": instance_type},
            {"name": "WORKSPACE_DIR", "value": settings.WORKSPACE_MOUNT_PATH},
            {"name": "HF_HOME", "value": settings.HF_CACHE_MOUNT_PATH},
            {"name": "HUGGINGFACE_HUB_CACHE", "value": settings.HF_CACHE_MOUNT_PATH},
            {"name": "HF_HUB_DISABLE_XET", "value": settings.HF_HUB_DISABLE_XET},
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

        oss_pod_enabled = bool(settings.OSS_ENABLED and user_id and oss_secret_name and settings.OSS_BUCKET)
        init_containers = []
        sidecars = []
        pod_images = [image]
        if oss_pod_enabled:
            pod_images.append(settings.OSSUTIL_IMAGE)
            volumes.append({
                "name": "oss-creds",
                "secret": {
                    "secretName": oss_secret_name,
                    "optional": False,
                },
            })
            oss_volume_mounts = [
                {"name": "workspace", "mountPath": settings.WORKSPACE_MOUNT_PATH},
                {"name": "oss-creds", "mountPath": "/etc/oss-creds", "readOnly": True},
            ]
            init_containers.append({
                "name": "restore-workspace",
                "image": settings.OSSUTIL_IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "command": ["/bin/sh", "-lc"],
                "args": [self._oss_restore_script(int(user_id))],
                "volumeMounts": oss_volume_mounts,
                "resources": {
                    "requests": {"cpu": "50m", "memory": "64Mi"},
                    "limits": {"cpu": "1", "memory": "512Mi"},
                },
            })
            sidecars.append({
                "name": "workspace-backup",
                "image": settings.OSSUTIL_IMAGE,
                "imagePullPolicy": "IfNotPresent",
                "restartPolicy": "Always",
                "command": ["/bin/sh", "-lc"],
                "args": [self._oss_backup_script(int(user_id))],
                "volumeMounts": oss_volume_mounts,
                "resources": {
                    "requests": {"cpu": "50m", "memory": "64Mi"},
                    "limits": {"cpu": "1", "memory": "512Mi"},
                },
            })

        image_pull_secrets = self._image_pull_secrets(*pod_images)

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
            "tolerations": [
                {
                    "key": "amd.com/gpu",
                    "operator": "Exists",
                    "effect": "NoSchedule"
                }
            ],
            "containers": [
                {
                    "name": "notebook",
                    "image": image,
                    "imagePullPolicy": "IfNotPresent",
                    "command": ["/bin/bash", "-c"],
                    "args": [startup_script],
                    "ports": [
                        {
                            "containerPort": settings.NOTEBOOK_PORT,
                            "name": "jupyter"
                        },
                        {
                            "containerPort": settings.OPENCODE_WEB_PORT,
                            "name": "opencode"
                        }
                    ],
                    "resources": {
                        "limits": {
                            "cpu": resources["cpu_limit"],
                            "memory": resources["memory_limit"],
                            "amd.com/gpu": str(gpu_count)
                        },
                        "requests": {
                            "cpu": resources["cpu_request"],
                            "memory": resources["memory_request"],
                            "amd.com/gpu": str(gpu_count)
                        }
                    },
                    "env": env,
                    "volumeMounts": volume_mounts
                },
            ],
            "volumes": volumes,
            "restartPolicy": "Always",
        }
        if image_pull_secrets:
            spec["imagePullSecrets"] = image_pull_secrets
        image_affinity = self._image_cache_node_affinity(image)
        if image_affinity:
            spec["affinity"] = image_affinity
        init_containers.extend(sidecars)
        if init_containers:
            spec["initContainers"] = init_containers
            spec["terminationGracePeriodSeconds"] = settings.OSS_TERMINATION_GRACE_PERIOD_SECONDS

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
    
    def _get_service_manifest(self, email: str, instance_id: str, node_port: int, opencode_node_port: int) -> dict:
        """Generate Service manifest exposing both Jupyter and OpenCode web."""
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
                    },
                    {
                        "name": "opencode",
                        "port": settings.OPENCODE_WEB_PORT,
                        "targetPort": settings.OPENCODE_WEB_PORT,
                        "nodePort": opencode_node_port
                    }
                ]
            }
        }
    
    def _used_node_ports(self) -> set:
        used_ports = set()
        try:
            services = self.core_v1.list_namespaced_service(namespace=self.namespace)
            for svc in services.items:
                for port in svc.spec.ports or []:
                    if port.node_port:
                        used_ports.add(port.node_port)
        except ApiException as e:
            logger.warning(f"Error listing services: {e}")
        return used_ports

    def _allocate_node_port(self) -> int:
        """Allocate a single available NodePort."""
        used_ports = self._used_node_ports()
        port = settings.NODE_PORT_BASE
        while port in used_ports and port < 32767:
            port += 1
        return port

    def _allocate_node_port_pair(self, extra_used_ports: Optional[set] = None) -> tuple:
        """Allocate two distinct available NodePorts (jupyter + opencode)."""
        used_ports = self._used_node_ports()
        if extra_used_ports:
            used_ports.update(extra_used_ports)
        port = settings.NODE_PORT_BASE
        while port < 32766:
            if port not in used_ports and (port + 1) not in used_ports:
                return port, port + 1
            port += 1
        raise RuntimeError("Unable to allocate a free NodePort pair for service")

    def _prepull_name(self, image_id: int) -> str:
        return f"image-prepull-catalog-{image_id}"

    def _prepull_labels(self, image_id: int) -> dict:
        return {
            "app": "amd-oneclick-image-prepull",
            "image-id": str(image_id),
        }

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
        image_pull_secrets = self._image_pull_secrets(image)
        if image_pull_secrets:
            body["spec"]["template"]["spec"]["imagePullSecrets"] = image_pull_secrets
        self.apps_v1.create_namespaced_daemon_set(namespace=self.namespace, body=body)
        return self.get_image_sync_status(image_id)

    def get_image_sync_status(self, image_id: int) -> dict:
        """Return DaemonSet sync status for an image catalog entry."""
        name = self._prepull_name(image_id)
        try:
            ds = self.apps_v1.read_namespaced_daemon_set(name=name, namespace=self.namespace)
            desired = ds.status.desired_number_scheduled or 0
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

            ready = len(pulled_nodes)
            target = max(1, int(desired * 0.8 + 0.999)) if desired else 0
            status = "ready" if target > 0 and ready >= target else "pulling"
            message = f"{ready}/{desired} nodes pulled (threshold {target}, 80%)"
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

    def _custom_prepull_name(self, custom_image_id: int) -> str:
        return f"image-prepull-custom-{custom_image_id}"

    def sync_custom_image_to_nodes(self, custom_image_id: int, image: str) -> None:
        """Prepull a user's custom image onto every node (best-effort).

        Uses a distinct DaemonSet name from the catalog prepull and attaches the
        custom registry pull secret when configured.
        """
        image = image.strip()
        if not image:
            return
        name = self._custom_prepull_name(custom_image_id)
        labels = {"app": "amd-oneclick-custom-prepull", "custom-image-id": str(custom_image_id)}

        try:
            self.apps_v1.delete_namespaced_daemon_set(name=name, namespace=self.namespace)
        except ApiException as e:
            if e.status != 404:
                raise

        pod_spec = {
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
        }
        image_pull_secrets = self._image_pull_secrets(image)
        if image_pull_secrets:
            pod_spec["imagePullSecrets"] = image_pull_secrets

        body = {
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": {"name": name, "namespace": self.namespace, "labels": labels},
            "spec": {
                "selector": {"matchLabels": labels},
                "template": {"metadata": {"labels": labels}, "spec": pod_spec},
            },
        }
        self.apps_v1.create_namespaced_daemon_set(namespace=self.namespace, body=body)

    def delete_custom_image_sync(self, custom_image_id: int):
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
            try:
                svc = self.core_v1.read_namespaced_service(
                    name=f"{instance_id}-svc",
                    namespace=self.namespace
                )
                node_port, opencode_node_port = self._extract_node_ports(svc)
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
                "url": self._build_url(node_port, instance_id=instance_id, use_path_proxy=pod.metadata.annotations.get("amd-oneclick/path-proxy") == "true") if node_port else None,
                "opencode_url": self._build_opencode_url(opencode_node_port),
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

        Must be the host users actually reach the cluster on (the hostname in
        PUBLIC_BASE_URL, e.g. 36.150.116.200), NOT SERVICE_HOST (36.151.243.69),
        which is only used for the path-proxied Jupyter URLs and is not routable
        for direct NodePort access from the browser.
        """
        if settings.PUBLIC_BASE_URL:
            host = urlparse(settings.PUBLIC_BASE_URL).hostname
            if host:
                return host
        return settings.SERVICE_HOST

    def _build_opencode_url(self, opencode_node_port: Optional[int]) -> Optional[str]:
        """Build the OpenCode web URL for the instance owner.

        No credentials are embedded because Chrome blocks http://user:pass@host/ URLs
        loaded from web pages. The URL is only returned to the authenticated owner,
        matching the existing Jupyter trust model (shared token in query string).
        """
        if not opencode_node_port:
            return None
        return f"http://{self._opencode_host()}:{opencode_node_port}/"

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

    def _wait_for_existing_instance(self, instance_id: str, timeout_seconds: int = 45) -> Optional[dict]:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            instance = self.get_instance_by_id(instance_id)
            if not instance:
                return None
            if instance.get("node_port"):
                return instance
            time.sleep(1)
        return None
    
    def create_instance(self, email: str, image: Optional[str] = None,
                        instance_type: str = "jupyter",
                        gpu_count: int = 1,
                        github_info: Optional[dict] = None,
                        custom_instance_id: Optional[str] = None,
                        resource_profile: Optional[str] = None,
                        template_id: Optional[str] = None,
                        template_title: Optional[str] = None,
                        user_id: Optional[int] = None) -> dict:
        """Create a new notebook instance"""
        instance_id = custom_instance_id or self._generate_instance_id(email)
        image = image or settings.DEFAULT_IMAGE

        existing = self._wait_for_existing_instance(instance_id, timeout_seconds=3)
        if existing:
            return existing

        oss_secret_name = None
        oss_launch_id = None
        pod_created = False
        service_created = False
        try:
            if user_id and settings.OSS_ENABLED:
                from .oss import ensure_instance_secret, oss_instance_enabled, sts_secret_name

                if oss_instance_enabled(user_id):
                    oss_launch_id = uuid.uuid4().hex[:12]
                    oss_secret_name = sts_secret_name(instance_id, oss_launch_id)
                    ensure_instance_secret(
                        self.core_v1,
                        self.namespace,
                        instance_id,
                        int(user_id),
                        secret_name=oss_secret_name,
                        launch_id=oss_launch_id,
                    )
                else:
                    raise RuntimeError("OSS is enabled but OSS/STS runtime configuration is incomplete")

            network_disk_claim_name = self._ensure_network_disk(instance_id)

            pod_manifest = self._get_pod_manifest(
                email, instance_id, image,
                instance_type=instance_type,
                gpu_count=gpu_count,
                github_info=github_info,
                resource_profile=resource_profile,
                network_disk_claim_name=network_disk_claim_name,
                template_id=template_id,
                template_title=template_title,
                user_id=user_id,
                oss_secret_name=oss_secret_name,
                oss_launch_id=oss_launch_id,
            )
            pod_retry_attempts = max(1, settings.POD_CREATE_RETRY_ATTEMPTS)
            for attempt in range(1, pod_retry_attempts + 1):
                try:
                    self.core_v1.create_namespaced_pod(
                        namespace=self.namespace,
                        body=pod_manifest
                    )
                    pod_created = True
                    logger.info(f"Created pod {instance_id} for {email} (type={instance_type})")
                    break
                except ApiException as e:
                    if e.status == 409:
                        existing = self._wait_for_existing_instance(instance_id)
                        if existing:
                            if oss_secret_name:
                                self._delete_oss_secret(instance_id, secret_name=oss_secret_name)
                            return existing
                    if e.status == 409 and attempt < pod_retry_attempts:
                        logger.warning("Pod %s still exists while creating; waiting for deletion before retry %s", instance_id, attempt)
                        time.sleep(max(1, settings.POD_CREATE_RETRY_DELAY_SECONDS))
                        continue
                    logger.error(f"Failed to create pod: {e}")
                    raise

            node_port = None
            opencode_node_port = None
            rejected_ports = set()
            with self._node_port_lock:
                for _ in range(20):
                    node_port, opencode_node_port = self._allocate_node_port_pair(rejected_ports)
                    svc_manifest = self._get_service_manifest(email, instance_id, node_port, opencode_node_port)
                    try:
                        self.core_v1.create_namespaced_service(
                            namespace=self.namespace,
                            body=svc_manifest
                        )
                        logger.info(f"Created service {instance_id}-svc with NodePorts {node_port}/{opencode_node_port}")
                        service_created = True
                        break
                    except ApiException as e:
                        if e.status == 422:
                            logger.warning(f"NodePort pair {node_port}/{opencode_node_port} rejected by API server, re-listing ports: {e}")
                            rejected_ports.update({node_port, opencode_node_port})
                            continue
                        logger.error(f"Failed to create service: {e}")
                        raise
            if not service_created:
                raise RuntimeError("Unable to allocate NodePort for service")
        except Exception:
            if pod_created:
                self.delete_instance_by_id(instance_id)
            elif oss_secret_name:
                try:
                    from .oss import delete_instance_secret

                    delete_instance_secret(self.core_v1, self.namespace, instance_id, secret_name=oss_secret_name)
                except Exception as secret_error:
                    logger.warning("Failed to clean up OSS STS Secret for %s after launch failure: %s", instance_id, secret_error)
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
            "url": self._build_url(node_port, notebook_path, instance_id, use_path_proxy=True),
            "opencode_url": self._build_opencode_url(opencode_node_port),
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
                node_port, opencode_node_port = self._extract_node_ports(svc)
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
                "url": self._build_url(node_port, github_path, instance_id, use_path_proxy=pod.metadata.annotations.get("amd-oneclick/path-proxy") == "true") if node_port else None,
                "opencode_url": self._build_opencode_url(opencode_node_port),
                "instance_type": instance_type,
                "gpu_count": gpu_count,
                "resource_profile": pod.metadata.annotations.get("amd-oneclick/resource-profile"),
                "cpu_limit": pod.metadata.annotations.get("amd-oneclick/cpu-limit"),
                "memory_limit": pod.metadata.annotations.get("amd-oneclick/memory-limit"),
                "github_org": pod.metadata.annotations.get("amd-oneclick/github-org"),
                "github_repo": pod.metadata.annotations.get("amd-oneclick/github-repo"),
                "github_path": github_path,
                "template_id": pod.metadata.annotations.get("amd-oneclick/template-id"),
                "template_title": pod.metadata.annotations.get("amd-oneclick/template-title"),
                "user_id": pod.metadata.annotations.get("amd-oneclick/user-id"),
                "oss_secret_name": pod.metadata.annotations.get("amd-oneclick/oss-sts-secret"),
            }
        except ApiException as e:
            if e.status == 404:
                return None
            raise
    
    def delete_instance_by_id(self, instance_id: str) -> bool:
        """Delete a notebook instance by instance ID"""
        pod_delete_succeeded = False
        pod_delete_requested = False
        pod_already_gone = False
        oss_secret_name = None

        # Delete Service
        try:
            self.core_v1.delete_namespaced_service(
                name=f"{instance_id}-svc",
                namespace=self.namespace
            )
            logger.info(f"Deleted service {instance_id}-svc")
        except ApiException as e:
            if e.status != 404:
                logger.warning(f"Error deleting service: {e}")

        try:
            pod = self.core_v1.read_namespaced_pod(name=instance_id, namespace=self.namespace)
            annotations = pod.metadata.annotations or {}
            oss_secret_name = annotations.get("amd-oneclick/oss-sts-secret")
        except ApiException as e:
            if e.status == 404:
                pod_already_gone = True
                pod_delete_succeeded = True
            else:
                logger.warning(f"Error reading pod before delete: {e}")
                return False

        # Delete Pod
        if not pod_already_gone:
            try:
                self.core_v1.delete_namespaced_pod(
                    name=instance_id,
                    namespace=self.namespace
                )
                logger.info(f"Deleted pod {instance_id}")
                pod_delete_succeeded = True
                pod_delete_requested = True
            except ApiException as e:
                if e.status == 404:
                    pod_already_gone = True
                    pod_delete_succeeded = True
                else:
                    logger.warning(f"Error deleting pod: {e}")
                    return False

        if pod_delete_requested:
            threading.Thread(
                target=self._delete_oss_secret_after_pod_gone,
                args=(instance_id, oss_secret_name),
                daemon=True,
            ).start()
        elif pod_already_gone:
            self._delete_oss_secret(instance_id)
        
        return pod_delete_succeeded

    def _delete_oss_secret(self, instance_id: str, secret_name: Optional[str] = None):
        try:
            from .oss import delete_instance_secret

            delete_instance_secret(self.core_v1, self.namespace, instance_id, secret_name=secret_name)
        except Exception as e:
            logger.warning("Error deleting OSS STS Secret for %s: %s", instance_id, e)

    def _delete_oss_secret_after_pod_gone(self, instance_id: str, secret_name: Optional[str]):
        deadline = time.time() + max(1, settings.OSS_SECRET_DELETE_WAIT_SECONDS)
        while time.time() < deadline:
            try:
                self.core_v1.read_namespaced_pod(name=instance_id, namespace=self.namespace)
                time.sleep(2)
            except ApiException as e:
                if e.status == 404:
                    self._delete_oss_secret(instance_id, secret_name=secret_name)
                    return
                logger.warning("Error waiting for pod %s to terminate before OSS Secret cleanup: %s", instance_id, e)
                break
        logger.warning("Deferring OSS STS Secret cleanup for %s; pod still exists or status is unknown", instance_id)
    
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
                    "user_id": pod.metadata.annotations.get("amd-oneclick/user-id"),
                    "oss_secret_name": pod.metadata.annotations.get("amd-oneclick/oss-sts-secret"),
                })
        except ApiException as e:
            logger.error(f"Error listing pods: {e}")
        
        return instances
    
    def delete_all_instances(self) -> int:
        """Delete all notebook instances"""
        instances = self.list_instances()
        deleted_count = 0
        
        for instance in instances:
            if self.delete_instance_by_id(instance["id"]):
                deleted_count += 1
        
        return deleted_count
    
    def get_pod_status(self, email: str, instance_id: Optional[str] = None) -> Optional[str]:
        """Get the current status of a pod"""
        if not instance_id:
            instance_id = self._generate_instance_id(email)
        
        try:
            pod = self.core_v1.read_namespaced_pod(
                name=instance_id,
                namespace=self.namespace
            )
            
            phase = pod.status.phase.lower() if pod.status.phase else "unknown"
            
            # Check container statuses for more detail
            if pod.status.container_statuses:
                container_status = pod.status.container_statuses[0]
                if container_status.ready:
                    # Container is ready, but we need to verify Jupyter is actually responding
                    instance = self.get_instance_by_id(instance_id)
                    if instance and instance.get("node_port"):
                        if self._check_jupyter_ready(instance["node_port"]):
                            return "ready"
                        else:
                            return "jupyter_starting"
                    return "running"
                elif container_status.state.waiting:
                    reason = container_status.state.waiting.reason or "waiting"
                    if reason in ["ContainerCreating", "PodInitializing"]:
                        return "initializing"
                    elif reason == "ImagePullBackOff":
                        return "failed"
                    return "loading"
                elif container_status.state.running:
                    # Container is running but not ready yet
                    return "running"
            
            return phase
        except ApiException as e:
            if e.status == 404:
                return None
            raise
    
    def _check_jupyter_ready(self, node_port: int, timeout: float = 2.0) -> bool:
        """Check if Jupyter is responding on the given port"""
        try:
            # Try to connect to the Jupyter server
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            # Connect to any node in the cluster
            result = sock.connect_ex((settings.SERVICE_HOST, node_port))
            sock.close()
            return result == 0
        except Exception as e:
            logger.debug(f"Jupyter health check failed: {e}")
            return False
    
    def check_pod_activity(self, email: str, instance_id: Optional[str] = None) -> Optional[datetime]:
        """Check last activity of a pod by examining logs"""
        if not instance_id:
            instance_id = self._generate_instance_id(email)
        
        try:
            # Get recent logs
            logs = self.core_v1.read_namespaced_pod_log(
                name=instance_id,
                namespace=self.namespace,
                container="notebook",
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
        except ApiException as e:
            if e.status != 404:
                logger.warning("Unable to read notebook logs for idle check on %s: %s", instance_id, e)
            return None
    
    def cleanup_idle_instances(self, on_deleted=None) -> list:
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
                last_activity = self.check_pod_activity(instance["email"], instance_id=instance["id"])
                if last_activity:
                    idle_minutes = (now - last_activity).total_seconds() / 60
                    if idle_minutes >= idle_timeout:
                        should_delete = True
                        reason = f"idle for {int(idle_minutes)} minutes (limit {idle_timeout}m)"

            if should_delete:
                if self.delete_instance_by_id(instance["id"]):
                    if on_deleted:
                        on_deleted(instance["id"])
                    cleaned.append({
                        "email": instance["email"],
                        "id": instance["id"],
                        "reason": reason
                    })
                    logger.info(f"Cleaned up instance for {instance['email']}: {reason}")

        return cleaned


# Global K8s client instance
k8s_client = K8sClient()
