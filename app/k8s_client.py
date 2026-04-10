"""
Kubernetes client for managing notebook instances
"""
import hashlib
import logging
import socket
from datetime import datetime, timezone
from typing import Optional

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from .config import settings

logger = logging.getLogger(__name__)


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
        
        self.core_v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.namespace = settings.K8S_NAMESPACE
    
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

    def _build_standard_startup_script(self, github_info: Optional[dict] = None) -> str:
        """Startup script for default notebook types."""
        if github_info:
            notebook_filename = github_info["path"].split("/")[-1]
            return f"""
echo "{settings.PYPI_HOST_IP} {settings.PYPI_HOST}" >> /etc/hosts
mkdir -p ~/.pip
cat > ~/.pip/pip.conf << EOF
[global]
index-url = {settings.PYPI_MIRROR}
trusted-host = {settings.PYPI_HOST}
EOF
pip install --no-cache-dir jupyter ihighlight
mkdir -p /app/notebooks
cd /app/notebooks
python -c "
import urllib.request
import ssl
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE
url = '{github_info["raw_url"]}'
urllib.request.urlretrieve(url, '{notebook_filename}')
print('Downloaded: {notebook_filename}')
"
jupyter lab --ip=0.0.0.0 --port={settings.NOTEBOOK_PORT} --no-browser --allow-root --ServerApp.token='{settings.NOTEBOOK_TOKEN}' --notebook-dir=/app/notebooks
"""

        return f"""
echo "{settings.PYPI_HOST_IP} {settings.PYPI_HOST}" >> /etc/hosts
mkdir -p ~/.pip
cat > ~/.pip/pip.conf << EOF
[global]
index-url = {settings.PYPI_MIRROR}
trusted-host = {settings.PYPI_HOST}
EOF
pip install --no-cache-dir jupyter ihighlight
cd /app
jupyter lab --ip=0.0.0.0 --port={settings.NOTEBOOK_PORT} --no-browser --allow-root --ServerApp.token='{settings.NOTEBOOK_TOKEN}'
"""

    def _build_paddleocr_startup_script(
        self,
        instance_id: str,
        github_info: Optional[dict] = None,
    ) -> str:
        """Startup script for PaddleOCR-VL image using oneclick entrypoint."""
        notebook_url_export = ""
        if github_info:
            notebook_url_export = f"export NOTEBOOK_URL='{github_info['raw_url']}'\n"

        return f"""
{notebook_url_export}export NOTEBOOK_TOKEN='{settings.NOTEBOOK_TOKEN}'
export JUPYTER_PORT={settings.NOTEBOOK_PORT}
export INSTANCE_ID='{instance_id}'
export GPU_MEMORY_UTILIZATION='{self._get_gpu_memory_utilization("paddleocr_vl")}'
exec /opt/PaddleX/oneclick_entrypoint.sh
"""

    def _get_resources_for_instance_type(self, instance_type: str) -> dict:
        """Resolve resource requests/limits for an instance type."""
        defaults = {
            "cpu_limit": settings.CPU_LIMIT,
            "memory_limit": settings.MEMORY_LIMIT,
            "gpu_limit": settings.GPU_LIMIT,
            "cpu_request": settings.CPU_REQUEST,
            "memory_request": settings.MEMORY_REQUEST,
        }
        overrides = settings.INSTANCE_TYPE_RESOURCES.get(instance_type, {})
        defaults.update({k: v for k, v in overrides.items() if k in defaults})
        return defaults

    def _get_gpu_memory_utilization(self, instance_type: str) -> str:
        """Resolve GPU memory utilization env for an instance type."""
        overrides = settings.INSTANCE_TYPE_RESOURCES.get(instance_type, {})
        return overrides.get("gpu_memory_utilization", "0.85")
    
    def _get_pod_manifest(
        self,
        email: str,
        instance_id: str,
        image: str,
        github_info: Optional[dict] = None,
        instance_type: str = "jupyter",
    ) -> dict:
        """Generate Pod manifest"""
        labels = self._get_labels(email, instance_id)
        resources = self._get_resources_for_instance_type(instance_type)
        
        annotations = {
            "amd-oneclick/email": email,
            "amd-oneclick/created-at": datetime.now(timezone.utc).isoformat(),
            "amd-oneclick/instance-type": instance_type,
        }
        
        # Add GitHub info to annotations if provided
        if github_info:
            annotations["amd-oneclick/github-org"] = github_info.get("org", "")
            annotations["amd-oneclick/github-repo"] = github_info.get("repo", "")
            annotations["amd-oneclick/github-branch"] = github_info.get("branch", "")
            annotations["amd-oneclick/github-path"] = github_info.get("path", "")
            annotations["amd-oneclick/github-raw-url"] = github_info.get("raw_url", "")
        
        # Build startup command by instance type.
        if instance_type == "paddleocr_vl":
            startup_script = self._build_paddleocr_startup_script(instance_id, github_info)
        else:
            startup_script = self._build_standard_startup_script(github_info)
        
        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": instance_id,
                "namespace": self.namespace,
                "labels": labels,
                "annotations": annotations
            },
            "spec": {
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
                            }
                        ],
                        "resources": {
                            "limits": {
                                "cpu": resources["cpu_limit"],
                                "memory": resources["memory_limit"],
                                "amd.com/gpu": resources["gpu_limit"],
                            },
                            "requests": {
                                "cpu": resources["cpu_request"],
                                "memory": resources["memory_request"],
                                "amd.com/gpu": resources["gpu_limit"],
                            }
                        },
                        "env": [
                            {"name": "SHELL", "value": "/bin/bash"},
                            {"name": "USER_EMAIL", "value": email},
                            {"name": "INSTANCE_TYPE", "value": instance_type},
                            {
                                "name": "GPU_MEMORY_UTILIZATION",
                                "value": self._get_gpu_memory_utilization(instance_type),
                            },
                        ],
                        "volumeMounts": [
                            {"name": "shm", "mountPath": "/dev/shm"}
                        ]
                    }
                ],
                "volumes": [
                    {
                        "name": "shm",
                        "emptyDir": {
                            "medium": "Memory",
                            "sizeLimit": "64Gi"
                        }
                    }
                ],
                "restartPolicy": "Always"
            }
        }

        pull_secret_name = (settings.NOTEBOOK_IMAGE_PULL_SECRET or "").strip()
        if pull_secret_name:
            pod_manifest["spec"]["imagePullSecrets"] = [{"name": pull_secret_name}]

        return pod_manifest
    
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
    
    def _cleanup_orphaned_services(self) -> list[str]:
        """Delete services labeled as notebook instances whose pods no longer exist."""
        cleaned: list[str] = []
        try:
            services = self.core_v1.list_namespaced_service(
                namespace=self.namespace,
                label_selector=f"app={settings.NOTEBOOK_LABEL_PREFIX}",
            )
        except ApiException as e:
            logger.warning(f"Error listing services for orphan cleanup: {e}")
            return cleaned

        for svc in services.items:
            instance_id = svc.metadata.labels.get("instance-id")
            if not instance_id:
                continue
            try:
                self.core_v1.read_namespaced_pod(
                    name=instance_id, namespace=self.namespace
                )
            except ApiException as e:
                if e.status == 404:
                    try:
                        self.core_v1.delete_namespaced_service(
                            name=svc.metadata.name, namespace=self.namespace
                        )
                        cleaned.append(svc.metadata.name)
                        logger.info(
                            f"Cleaned orphaned service {svc.metadata.name} "
                            f"(pod {instance_id} not found)"
                        )
                    except ApiException as del_err:
                        logger.warning(f"Failed to delete orphaned service {svc.metadata.name}: {del_err}")
        return cleaned

    def _allocate_node_port(self) -> int:
        """Allocate an available NodePort, scanning all services in the namespace."""
        used_ports = set()

        try:
            services = self.core_v1.list_namespaced_service(
                namespace=self.namespace,
            )
            for svc in services.items:
                for port in svc.spec.ports or []:
                    if port.node_port:
                        used_ports.add(port.node_port)
        except ApiException as e:
            logger.warning(f"Error listing services: {e}")
        
        # Find available port starting from base
        port = settings.NODE_PORT_BASE
        while port in used_ports and port < 32767:
            port += 1
        
        return port
    
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
                "instance_type": pod.metadata.annotations.get("amd-oneclick/instance-type", "jupyter"),
                "status": pod.status.phase.lower(),
                "created_at": pod.metadata.creation_timestamp,
                "node_port": node_port,
                "url": self._build_url(
                    node_port,
                    instance_id=instance_id,
                    instance_type=pod.metadata.annotations.get("amd-oneclick/instance-type", "jupyter"),
                ) if node_port else None,
            }
        except ApiException as e:
            if e.status == 404:
                return None
            raise
    
    def _build_url(
        self,
        node_port: int,
        notebook_path: Optional[str] = None,
        instance_id: Optional[str] = None,
        instance_type: str = "jupyter",
    ) -> str:
        """Build notebook URL."""
        base_lab_path = "/lab"
        if instance_type == "paddleocr_vl" and instance_id:
            base_lab_path = f"/instance/{instance_id}/lab"
            notebook_host = (settings.NOTEBOOK_PROXY_HOST or settings.SERVICE_HOST).strip()
            base_url = f"http://{notebook_host}{base_lab_path}?token={settings.NOTEBOOK_TOKEN}"
            if notebook_path:
                notebook_filename = notebook_path.split("/")[-1]
                return (
                    f"http://{notebook_host}"
                    f"{base_lab_path}/tree/{notebook_filename}?token={settings.NOTEBOOK_TOKEN}"
                )
            return base_url

        base_url = f"http://{settings.SERVICE_HOST}:{node_port}{base_lab_path}?token={settings.NOTEBOOK_TOKEN}"
        if notebook_path:
            # Add notebook path to URL for direct open
            notebook_filename = notebook_path.split("/")[-1]
            return (
                f"http://{settings.SERVICE_HOST}:{node_port}"
                f"{base_lab_path}/tree/{notebook_filename}?token={settings.NOTEBOOK_TOKEN}"
            )
        return base_url
    
    def create_instance(self, email: str, image: Optional[str] = None, 
                        github_info: Optional[dict] = None,
                        custom_instance_id: Optional[str] = None,
                        instance_type: str = "jupyter") -> dict:
        """Create a new notebook instance"""
        instance_id = custom_instance_id or self._generate_instance_id(email)
        image = image or settings.DEFAULT_IMAGE
        
        # Check if instance already exists
        existing = self.get_instance_by_id(instance_id)
        if existing:
            return existing

        # Clean up orphaned services before allocating a port
        self._cleanup_orphaned_services()
        
        # Allocate NodePort
        node_port = self._allocate_node_port()
        
        # Create Pod
        pod_manifest = self._get_pod_manifest(
            email,
            instance_id,
            image,
            github_info,
            instance_type=instance_type,
        )
        try:
            self.core_v1.create_namespaced_pod(
                namespace=self.namespace,
                body=pod_manifest
            )
            logger.info(f"Created pod {instance_id} for {email}")
        except ApiException as e:
            logger.error(f"Failed to create pod: {e}")
            raise
        
        # Create Service (replace stale one on 409 conflict)
        svc_manifest = self._get_service_manifest(email, instance_id, node_port)
        try:
            self.core_v1.create_namespaced_service(
                namespace=self.namespace,
                body=svc_manifest
            )
            logger.info(f"Created service {instance_id}-svc with NodePort {node_port}")
        except ApiException as e:
            if e.status == 409:
                logger.warning(f"Service {instance_id}-svc already exists, replacing it")
                try:
                    self.core_v1.delete_namespaced_service(
                        name=f"{instance_id}-svc", namespace=self.namespace
                    )
                    self.core_v1.create_namespaced_service(
                        namespace=self.namespace, body=svc_manifest
                    )
                    logger.info(f"Replaced service {instance_id}-svc with NodePort {node_port}")
                except ApiException as retry_err:
                    logger.error(f"Failed to replace service: {retry_err}")
                    self.delete_instance_by_id(instance_id)
                    raise retry_err
            else:
                logger.error(f"Failed to create service: {e}")
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
            "status": "pending",
            "created_at": datetime.now(timezone.utc),
            "node_port": node_port,
            "url": self._build_url(
                node_port,
                notebook_path=notebook_path,
                instance_id=instance_id,
                instance_type=instance_type,
            ),
            "github_info": github_info
        }
    
    def get_instance_by_id(self, instance_id: str) -> Optional[dict]:
        """Get existing notebook instance by instance ID"""
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
            
            email = pod.metadata.annotations.get("amd-oneclick/email", "unknown")
            github_path = pod.metadata.annotations.get("amd-oneclick/github-path")
            
            return {
                "id": instance_id,
                "email": email,
                "pod_name": pod.metadata.name,
                "service_name": f"{instance_id}-svc",
                "image": pod.spec.containers[0].image,
                "instance_type": pod.metadata.annotations.get("amd-oneclick/instance-type", "jupyter"),
                "status": pod.status.phase.lower(),
                "created_at": pod.metadata.creation_timestamp,
                "node_port": node_port,
                "url": self._build_url(
                    node_port,
                    notebook_path=github_path,
                    instance_id=instance_id,
                    instance_type=pod.metadata.annotations.get("amd-oneclick/instance-type", "jupyter"),
                ) if node_port else None,
                "github_org": pod.metadata.annotations.get("amd-oneclick/github-org"),
                "github_repo": pod.metadata.annotations.get("amd-oneclick/github-repo"),
                "github_path": github_path,
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
                
                instances.append({
                    "id": instance_id,
                    "email": email,
                    "pod_name": pod.metadata.name,
                    "service_name": f"{instance_id}-svc",
                    "image": pod.spec.containers[0].image if pod.spec.containers else "unknown",
                    "instance_type": pod.metadata.annotations.get("amd-oneclick/instance-type", "jupyter"),
                    "status": pod.status.phase.lower() if pod.status.phase else "unknown",
                    "created_at": created_at.isoformat() if created_at else None,
                    "node_port": node_port,
                    "url": self._build_url(
                        node_port,
                        notebook_path=github_path,
                        instance_id=instance_id,
                        instance_type=pod.metadata.annotations.get("amd-oneclick/instance-type", "jupyter"),
                    ) if node_port else None,
                    "uptime_minutes": uptime_minutes,
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
            
            # Check max lifetime
            uptime_hours = instance["uptime_minutes"] / 60
            if uptime_hours >= settings.MAX_LIFETIME_HOURS:
                should_delete = True
                reason = f"exceeded max lifetime ({settings.MAX_LIFETIME_HOURS}h)"
            
            # Check idle timeout (only for running instances)
            elif instance["status"] == "running":
                last_activity = self.check_pod_activity(instance["email"])
                if last_activity:
                    idle_minutes = (now - last_activity).total_seconds() / 60
                    if idle_minutes >= settings.IDLE_TIMEOUT_MINUTES:
                        should_delete = True
                        reason = f"idle for {int(idle_minutes)} minutes"
            
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
