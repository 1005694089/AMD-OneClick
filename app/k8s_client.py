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

# Sized to the GPU nodes' real capacity (measured: 128 CPU / ~1007 GiB / 8 GPU per node,
# ~0 reserved overhead). GPU is the binding constraint (8/node), so each profile takes a
# GPU-proportional share: CPU limit = 16/GPU (exact fair share), memory limit ~110 GiB/GPU
# (88% of the 125 GiB/GPU fair share, leaving margin for kernel/page cache/daemonsets so a
# spike does not trigger node memory-pressure eviction). Requests sit well below limits so a
# full node of single-GPU pods still bin-packs (8x8 CPU = 64 <= 128; 8x48 GiB = 384 <= 1007).
RESOURCE_PROFILES = {
    "standard": {
        "label": "16 CPU / 55Gi memory",
        "cpu_request": "8",
        "cpu_limit": "16",
        "memory_request": "48Gi",
        # Real GPU nodes are 503.5GiB / 8 GPU = 62.9GiB/GPU. 8 x 110Gi (old) = 880Gi >> 503Gi
        # -> ~1.75x memory oversubscription and an OOM tail under GPU-packed load. Limit set to
        # ~88% of the real per-GPU share so 8 single-GPU pods fit node RAM with system headroom.
        # Requests are unchanged so scheduler bin-packing (GPU-bound) is unaffected.
        "memory_limit": "55Gi",
    },
    "large": {
        "label": "32 CPU / 110Gi memory",
        "cpu_request": "16",
        "cpu_limit": "32",
        "memory_request": "96Gi",
        # 2 GPU -> 2 x 55Gi
        "memory_limit": "110Gi",
    },
    "xlarge": {
        "label": "64 CPU / 220Gi memory",
        "cpu_request": "32",
        "cpu_limit": "64",
        "memory_request": "192Gi",
        # 4 GPU -> 4 x 55Gi
        "memory_limit": "220Gi",
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
        # Proxy hot-path cache: ClusterIP per instance_id, short TTL. Invalidated at the
        # single delete chokepoint (_delete_service) so EVERY delete path -- admin, bulk,
        # idle-cleanup, and scheduler reconcile force-delete -- drops the entry, preventing
        # a reallocated ClusterIP from routing a tenant's proxied traffic to another pod.
        # _svc_ip_epoch guards the check-then-set race: an in-flight read that started before
        # an invalidation must not repopulate a stale IP.
        self._svc_ip_cache = {}
        self._svc_ip_epoch = {}
        self._svc_ip_ttl = 15.0
        self._svc_ip_lock = threading.Lock()
        # Short-TTL cache for list_node(): target resolution + node selection call
        # _eligible_target_nodes on a hot path (every admin status poll, every launch). Caching the
        # API result for a few seconds removes the per-call apiserver hit that bites at 300 nodes.
        self._node_list_cache = None
        self._node_list_cache_ts = 0.0
        self._node_list_cache_ttl = float(getattr(settings, "NODE_LIST_CACHE_TTL_SECONDS", 5.0))
        self._node_list_cache_lock = threading.Lock()
        # Per-(instance,node) flush serialization: delete-path flush and the reconciler retry sweep
        # must never run concurrent opposite-direction rsyncs for the same copy. A single process holds
        # both, so an in-process lock suffices (leader-only scheduler + single manager replica).
        self._flush_locks = {}
        self._flush_locks_guard = threading.Lock()
        # Per-image locks serializing preheat DaemonSet mutation (create/replace/remove) so two
        # admin actions on the same image can't race the delete+recreate into an uncaught 409.
        self._preheat_locks: dict[int, threading.Lock] = {}
        self._preheat_locks_guard = threading.Lock()

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

    def _image_ready_label_key(self, image_ref: str) -> str:
        """Node label key marking that a specific image is warm on the node.

        Derived from a hash of the normalized image ref so the affinity injector
        (which only knows the image string) and the label reconciler compute the
        same key without a DB round-trip. Prefix carries the domain part, the
        suffix stays well under the 63-char label-name limit.
        """
        digest = hashlib.md5(self._normalize_image_ref(image_ref).encode()).hexdigest()[:16]
        return f"{settings.IMAGE_READY_NODE_LABEL_PREFIX}{digest}"

    def _image_affinity(self, image_ref: str) -> Optional[dict]:
        """Soft (preferred) node affinity biasing scheduling toward nodes that
        already have the image pulled. Soft is intentional: when no node is
        pre-warmed the pod still schedules anywhere and kubelet pulls on demand.
        """
        if not settings.IMAGE_AFFINITY_ENABLED or not image_ref:
            return None
        return {
            "nodeAffinity": {
                "preferredDuringSchedulingIgnoredDuringExecution": [
                    {
                        "weight": max(1, min(100, settings.IMAGE_AFFINITY_WEIGHT)),
                        "preference": {
                            "matchExpressions": [
                                {
                                    "key": self._image_ready_label_key(image_ref),
                                    "operator": "In",
                                    "values": ["true"],
                                }
                            ]
                        },
                    }
                ]
            }
        }
    
    def _workspace_node_affinity(self, last_node: Optional[str]) -> Optional[dict]:
        """Soft (preferred) node affinity toward the instance's last node, so a fast relaunch lands
        where the warm local-SSD cache still lives (near-instant rehydrate). Soft only: if that node
        is full/gone the pod schedules elsewhere and rehydrates from durable NFS."""
        if not (settings.WORKSPACE_SOFT_AFFINITY_ENABLED and last_node):
            return None
        return {
            "nodeAffinity": {
                "preferredDuringSchedulingIgnoredDuringExecution": [
                    {
                        "weight": max(1, min(100, settings.WORKSPACE_SOFT_AFFINITY_WEIGHT)),
                        "preference": {
                            "matchExpressions": [
                                {"key": "kubernetes.io/hostname", "operator": "In", "values": [last_node]}
                            ]
                        },
                    }
                ]
            }
        }

    @staticmethod
    def _merge_node_affinity(*affinities) -> Optional[dict]:
        """Merge several {nodeAffinity:{preferredDuringScheduling...:[...]}} dicts into one by
        concatenating their preferred terms. Ignores None. Returns None if nothing to merge."""
        terms = []
        for aff in affinities:
            if not aff:
                continue
            na = aff.get("nodeAffinity", {})
            terms.extend(na.get("preferredDuringSchedulingIgnoredDuringExecution", []) or [])
        if not terms:
            return None
        return {"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": terms}}

    def _jupyter_base_url(self, instance_id: str) -> str:
        return f"/instances/{instance_id}/"

    def _workspace_host_path(self, instance_id: str) -> str:
        safe_id = re.sub(r"[^a-zA-Z0-9_.-]", "-", instance_id)
        return f"{settings.WORKSPACE_HOST_ROOT.rstrip('/')}/{safe_id}"

    def _safe_storage_segment(self, value: str) -> str:
        seg = re.sub(r"[^a-zA-Z0-9_.-]", "-", value).strip("-")
        # Reject dot-only results (".", "..", "...") — they survive the char filter (dot is allowed,
        # no dashes to strip) but are path-traversal segments: a subpath like "<bucket>/.." collapses
        # to the parent, escaping the per-instance dir and (for the destructive reaper/admin-delete
        # paths) potentially targeting a shared root. Every filesystem/subPath name in this module
        # flows through here, so this single guard closes that class of escape centrally.
        if not seg or set(seg) <= {"."}:
            return "default"
        return seg

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

    # --- Two-tier localcache workspace: sharded durable NFS + node-local SSD cache ---

    def _workspace_localcache_enabled(self) -> bool:
        return (settings.WORKSPACE_VOLUME_TYPE or "").strip().lower() == "localcache"

    def _durable_shard_classes(self) -> list:
        """The append-only list of durable StorageClasses (one shared PVC per class)."""
        return list(settings.WORKSPACE_DURABLE_STORAGE_CLASSES or [])

    def _durable_shard_index(self, instance_id: str) -> int:
        """Stable shard for an instance: md5(instance_id) % number_of_shards.

        APPEND-ONLY invariant: the modulo maps by list position, so callers must never
        reorder/remove WORKSPACE_DURABLE_STORAGE_CLASSES (doing so silently remaps existing
        instances onto a different, empty shard = data loss). Guarded by a CI test.
        """
        classes = self._durable_shard_classes()
        if not classes:
            raise RuntimeError("WORKSPACE_DURABLE_STORAGE_CLASSES is empty; cannot shard localcache workspace")
        digest = hashlib.md5(instance_id.encode()).hexdigest()
        return int(digest, 16) % len(classes)

    def _durable_shard_class(self, instance_id: str) -> str:
        return self._durable_shard_classes()[self._durable_shard_index(instance_id)]

    def _durable_shard_pvc(self, instance_id: str) -> str:
        """Name of the shared durable PVC this instance lives on: <prefix>-shard-<i>."""
        prefix = self._safe_storage_segment(settings.WORKSPACE_DURABLE_PVC_PREFIX).lower()
        return f"{prefix}-shard-{self._durable_shard_index(instance_id)}"[:63].rstrip("-")

    def _durable_shard_pvc_for_index(self, index: int) -> str:
        prefix = self._safe_storage_segment(settings.WORKSPACE_DURABLE_PVC_PREFIX).lower()
        return f"{prefix}-shard-{index}"[:63].rstrip("-")

    def _durable_subpath(self, instance_id: str) -> str:
        """Per-instance subdirectory on the shard PVC: <hh>/<safe_instance_id>.

        Two-level (first 2 md5 hex chars as a bucket) so no single shard directory holds
        tens of thousands of flat entries.
        """
        safe_id = self._safe_storage_segment(instance_id)
        bucket = hashlib.md5(instance_id.encode()).hexdigest()[:2]
        return f"{bucket}/{safe_id}"

    def _workspace_local_cache_path(self, instance_id: str) -> str:
        """Node-local SSD directory backing /workspace for this instance (the loop mountpoint when
        quota is enabled, or the data dir itself when quota is off)."""
        safe_id = self._safe_storage_segment(instance_id)
        return f"{settings.WORKSPACE_LOCAL_CACHE_ROOT.rstrip('/')}/{safe_id}"

    def _workspace_quota_image_host_root(self) -> str:
        """Host dir holding per-instance ext4 quota images. For localcache this is SSD-resident
        (sibling of the cache root on /nvme0) so the 100GB image consumes SSD, not the root LVM.
        Legacy (non-localcache) keeps the configured WORKSPACE_QUOTA_IMAGE_ROOT."""
        if self._workspace_localcache_enabled():
            return f"{settings.WORKSPACE_LOCAL_CACHE_ROOT.rstrip('/')}-quota"
        return settings.WORKSPACE_QUOTA_IMAGE_ROOT

    def _workspace_quota_image_path(self, instance_id: str) -> str:
        """Host path of this instance's ext4 quota image (used by cleanup to free SSD)."""
        safe_id = self._safe_storage_segment(instance_id)
        return f"{self._workspace_quota_image_host_root().rstrip('/')}/{safe_id}.img"

    def _workspace_sync_image(self) -> str:
        return (settings.WORKSPACE_SYNC_IMAGE or "").strip() or settings.DEFAULT_IMAGE

    def _run_node_command_pod(self, pod_name_stem: str, node_name: str, script: str,
                              timeout_seconds: int = 120):
        """Run a privileged one-shot pod on a specific node that nsenters the host mount
        namespace and executes `script` as root on the host. Mirrors the pattern in
        _provision_network_disk_image. Blocks until the pod Succeeds (raises on Failed/timeout).

        Used for durable soft-delete (admin) and local-cache cleanup — both host-fs operations
        that must run on the node, not from the manager. Never operates on NFS deletion of live data.
        """
        safe = self._safe_storage_segment(pod_name_stem).lower()
        pod_name = f"ws-nodeop-{safe}"[:63].rstrip("-")
        wrapped = f"set -eux\nnsenter -t 1 -m -- /bin/bash -lc {shlex.quote(script)}\n"
        body = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": self.namespace,
                "labels": {"app": "oneclick-workspace-nodeop"},
            },
            "spec": {
                "nodeName": node_name,
                "hostPID": True,
                "restartPolicy": "Never",
                "automountServiceAccountToken": False,
                "tolerations": [{"operator": "Exists"}],
                "containers": [
                    {
                        "name": "nodeop",
                        "image": self._workspace_sync_image(),
                        "imagePullPolicy": "IfNotPresent",
                        "securityContext": {"privileged": True},
                        "command": ["/bin/bash", "-lc"],
                        "args": [wrapped],
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
        # Clear any stale pod of the same name first.
        try:
            self.core_v1.delete_namespaced_pod(name=pod_name, namespace=self.namespace, grace_period_seconds=0)
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
        for _ in range(max(1, timeout_seconds)):
            pod = self.core_v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
            last_phase = pod.status.phase
            if last_phase == "Succeeded":
                # Best-effort cleanup of the completed pod.
                try:
                    self.core_v1.delete_namespaced_pod(name=pod_name, namespace=self.namespace, grace_period_seconds=0)
                except ApiException:
                    pass
                return
            if last_phase == "Failed":
                raise RuntimeError(f"workspace node-op {pod_name} failed on {node_name}")
            time.sleep(1)
        raise RuntimeError(f"workspace node-op {pod_name} timed out in phase {last_phase}")

    def _cleanup_local_workspace_cache(self, instance_id: str, node_name: str) -> bool:
        """Free the node-local SSD copy for an instance (durable NFS copy is untouched).

        When the quota loop-mount is enabled its mount is created in the HOST mount namespace
        (Bidirectional propagation), so kubelet does NOT unmount it on pod deletion. The cleanup
        therefore: (1) lazily unmounts the cache dir if it is a mountpoint (else rm -rf would
        recurse into the live loop fs and fail to free space), (2) removes the cache dir, and
        (3) removes the SSD-resident ext4 quota image. Guarded to paths under the cache/quota roots.

        No-op returning False when node_name is unknown. Best-effort: logs and returns False on
        failure so a stuck node never blocks the reaper.
        """
        if not node_name:
            return False
        # TOCTOU guard: the caller checked _pod_exists before enqueuing us, but scheduling the node-op
        # pod + waiting for it can take up to ~150s, during which a relaunch may recreate the pod. Only
        # bail if the live pod is on the SAME node we are about to clean (its hydrate could be
        # repopulating that exact dir). If the relaunch landed on a different node, cleaning THIS node's
        # now-orphaned copy is safe and desired.
        try:
            pod = self.core_v1.read_namespaced_pod(name=instance_id, namespace=self.namespace)
            live_node = getattr(pod.spec, "node_name", None)
            if live_node == node_name:
                logger.info("Skipping local cache cleanup for %s on %s: pod live on same node (relaunch)",
                            instance_id, node_name)
                return False
        except ApiException as e:
            if e.status != 404:
                logger.debug("pod re-check failed for %s before cleanup: %s", instance_id, e)
        cache_path = self._workspace_local_cache_path(instance_id)
        image_path = self._workspace_quota_image_path(instance_id)
        cache_root = settings.WORKSPACE_LOCAL_CACHE_ROOT.rstrip("/")
        image_root = self._workspace_quota_image_host_root().rstrip("/")
        # Guard: only ever act on paths strictly under the cache root and the quota-image root.
        if not cache_path.startswith(cache_root + "/") or cache_path == cache_root:
            logger.error("Refusing to clean suspicious local cache path %s", cache_path)
            return False
        if not image_path.startswith(image_root + "/") or image_path == image_root:
            logger.error("Refusing to clean suspicious quota image path %s", image_path)
            return False
        # _run_node_command_pod wraps the script in `nsenter -t 1 -m`, so it executes in the HOST
        # mount namespace where the real paths are bare (/nvme0/...), NOT under the container's /host
        # bind mount. Use the real host paths directly, matching _provision_network_disk_image. A
        # sentinel line ("WS_CLEANUP_OK") is printed so the caller can assert the script actually ran
        # to completion (a path typo would otherwise silently "succeed" as a no-op).
        script = (
            f"target={shlex.quote(cache_path)}\n"
            f"img={shlex.quote(image_path)}\n"
            f"if mountpoint -q \"$target\"; then umount -l \"$target\" || true; echo unmounted \"$target\"; fi\n"
            f"if [ -d \"$target\" ]; then rm -rf \"$target\"; echo removed \"$target\"; else echo \"no local cache at $target\"; fi\n"
            f"if [ -f \"$img\" ]; then rm -f \"$img\"; echo removed \"$img\"; else echo \"no quota image at $img\"; fi\n"
            f"echo WS_CLEANUP_OK\n"
        )
        try:
            self._run_node_command_pod(f"lc-{instance_id}", node_name, script, timeout_seconds=120)
            logger.info("Cleaned local workspace cache for %s on node %s", instance_id, node_name)
            return True
        except Exception as e:
            logger.error("Local workspace cache cleanup failed for %s on %s: %s", instance_id, node_name, e)
            return False

    def _nfs_op_candidate_nodes(self, limit: int = 6) -> list:
        """Ordered candidate nodes for a durable-op pod (which must mount an SFS-Turbo NFS PVC).

        Not every node can mount the NFS backend (missing nfs-common, off-LAN, transient issues) —
        an op pod that lands on such a node hangs in ContainerCreating on `mount.nfs` (exit 32) and
        times out. So instead of scheduling anywhere (tolerations: Exists, which even allows masters),
        we hand the caller a short ordered list to try in turn:
          1) FIRST, nodes that ALREADY run a Running workspace pod with a durable NFS mount — those
             have provably mounted the backend, so they are the safest bet.
          2) THEN, eligible nodes that ALREADY have the sync image warm (node.status.images) — the
             op pod uses the ~90GB base image, and on a cold node the pull alone exceeds the not-
             started deadline, so an unwarmed node looks like a mount failure and wastes the budget.
             Image-warm nodes start in seconds.
          3) THEN, remaining eligible nodes (this service's own, Ready, schedulable) as last resort.
        Eligibility reuses _node_belongs_to_service (the same symmetric taint/tenant scoping used by
        _eligible_target_nodes) rather than a hand-rolled taint check — so an op pinned via nodeName
        (which bypasses scheduler taint admission) can never land on a master or ANOTHER tenant's
        tainted node pool. Best-effort: on API error return [] and let the caller fall back to one
        unpinned attempt."""
        try:
            nodes = self._list_node_cached()
        except ApiException as e:
            logger.debug("candidate-node listing failed: %s", e)
            return []
        sync_image = self._normalize_image_ref(self._workspace_sync_image())
        eligible = set()
        warm = set()
        for node in nodes.items:
            name = node.metadata.name
            if getattr(node.spec, "unschedulable", False):
                continue
            conds = {c.type: c.status for c in (node.status.conditions or [])}
            if conds.get("Ready") != "True" or conds.get("DiskPressure") == "True":
                continue
            # Symmetric service/tenant scoping (excludes masters + other tenants' tainted pools +
            # any node whose NoSchedule/NoExecute taints this service doesn't tolerate).
            if not self._node_belongs_to_service(node):
                continue
            eligible.add(name)
            # Is the sync image already present on this node? node.status.images[].names holds every
            # tag/digest the node has pulled; match against the normalized sync image ref.
            for img in (node.status.images or []):
                if any(self._normalize_image_ref(n2) == sync_image for n2 in (img.names or [])):
                    warm.add(name)
                    break
        # Nodes already running a durable-mounted workspace pod (proven NFS-capable) rank first.
        proven: list = []
        try:
            pods = self.core_v1.list_namespaced_pod(
                namespace=self.namespace, label_selector=f"app={settings.NOTEBOOK_LABEL_PREFIX}",
                field_selector="status.phase=Running",
            )
            durable_prefix = self._safe_storage_segment(settings.WORKSPACE_DURABLE_PVC_PREFIX).lower()
            for p in pods.items:
                n = getattr(p.spec, "node_name", None)
                if not n or n not in eligible:
                    continue
                has_durable = any(
                    (v.persistent_volume_claim and
                     str(v.persistent_volume_claim.claim_name or "").startswith(durable_prefix))
                    for v in (p.spec.volumes or [])
                )
                if has_durable and n not in proven:
                    proven.append(n)
        except ApiException as e:
            logger.debug("proven-node discovery failed: %s", e)
        proven_set = set(proven)
        warm_only = sorted(n for n in warm if n not in proven_set)
        cold = sorted(n for n in eligible if n not in proven_set and n not in warm)
        ordered = proven + warm_only + cold
        return ordered[:max(1, limit)]

    def _run_durable_shard_command(self, pod_name_stem: str, shard_pvc: str, script: str,
                                   timeout_seconds: int = 300):
        """Run a one-shot pod that mounts a durable shard PVC (RWX) at /durable and executes
        `script`. Used for admin soft-delete (mv into .trash) and the nightly trash purge —
        filesystem ops on the NFS share itself, so they run in a pod that mounts the PVC rather
        than via host nsenter. Blocks until Succeeded (raises on Failed/exhausted-retries).

        Because not every node can mount the SFS-Turbo backend, this tries a short ordered list of
        NFS-capable candidate nodes (see _nfs_op_candidate_nodes). If a pinned attempt stalls before
        the container starts (mount/schedule failure — the exit-32 case), it deletes that pod and
        tries the next node. A pod that actually STARTED and then Fails is a real script error and
        raises immediately (no pointless retries). timeout_seconds is an OVERALL wall-clock budget for
        the whole call (across all candidate attempts), so a post-start hang can't multiply it.
        Candidates is empty only when no eligible node exists — then one unpinned fallback attempt."""
        safe = self._safe_storage_segment(pod_name_stem).lower()
        pod_name = f"ws-durable-{safe}"[:63].rstrip("-")
        # Overall deadline for the ENTIRE call (all attempts share this budget), so total wall-clock
        # is bounded by timeout_seconds regardless of how many candidates stall or hang post-start.
        overall_deadline = time.monotonic() + max(1, timeout_seconds)
        # Per-attempt not-started grace: if the container hasn't started by now the node likely can't
        # mount — abandon and try the next. Kept small so several attempts fit in the overall budget.
        start_deadline = max(20, min(60, timeout_seconds // 4))

        def _read_pod():
            """Read the op pod, retrying a couple of times on a transient (non-404) API error so a
            single apiserver hiccup doesn't get misread as a stall and force-kill a healthy pod.
            Returns the pod, None if 404 (gone), or raises 'transient' sentinel after retries."""
            last = None
            for _ in range(3):
                try:
                    return self.core_v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
                except ApiException as e:
                    if e.status == 404:
                        return None
                    last = e
                    time.sleep(1)
            raise last if last else RuntimeError("read failed")

        def _cleanup_pod():
            try:
                self.core_v1.delete_namespaced_pod(name=pod_name, namespace=self.namespace, grace_period_seconds=0)
            except ApiException as e:
                if e.status != 404:
                    logger.debug("durable-op cleanup delete of %s: %s", pod_name, e)
                    return
            for _ in range(30):
                try:
                    self.core_v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
                    time.sleep(1)
                except ApiException as e:
                    if e.status == 404:
                        return
                    logger.debug("durable-op cleanup poll of %s: %s", pod_name, e)
                    return

        def _body(node_name):
            spec = {
                "restartPolicy": "Never",
                "automountServiceAccountToken": False,
                # Notebook tolerations (not tolerations: Exists) so an op never lands on a master or
                # a node this service doesn't own — belt-and-suspenders with candidate scoping since a
                # nodeName pin bypasses scheduler taint admission.
                "tolerations": self._notebook_tolerations(),
                "containers": [
                    {
                        "name": "durableop",
                        "image": self._workspace_sync_image(),
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["/bin/bash", "-lc"],
                        "args": [f"set -eux\n{script}\n"],
                        "resources": {
                            "requests": {"cpu": "50m", "memory": "64Mi"},
                            "limits": {"cpu": "1", "memory": "512Mi"},
                        },
                        "volumeMounts": [{"name": "durable", "mountPath": "/durable"}],
                    }
                ],
                "volumes": [{"name": "durable", "persistentVolumeClaim": {"claimName": shard_pvc}}],
            }
            if node_name:
                spec["nodeName"] = node_name
            return {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": self.namespace,
                    "labels": {"app": "oneclick-workspace-durable-op"},
                },
                "spec": spec,
            }

        candidates = self._nfs_op_candidate_nodes() or [None]  # None => one unpinned fallback attempt
        last_err = "no candidate nodes"
        for node_name in candidates:
            if time.monotonic() >= overall_deadline:
                last_err = "overall timeout budget exhausted before all candidates tried"
                break
            _cleanup_pod()
            try:
                self.core_v1.create_namespaced_pod(namespace=self.namespace, body=_body(node_name))
            except ApiException as e:
                if e.status == 409:
                    # A prior pod of this name is still Terminating (stuck on a bad node). Skip this
                    # attempt rather than aborting the whole loop; the next iteration re-cleans.
                    last_err = f"create 409 (stale pod terminating) targeting node {node_name}"
                    logger.warning("durable-op %s create hit 409; will retry after cleanup", pod_name)
                    continue
                raise
            started = False
            attempt_start = time.monotonic()
            while True:
                if time.monotonic() >= overall_deadline:
                    last_err = f"overall timeout in phase (started={started}) on node {node_name}"
                    _cleanup_pod()
                    break
                try:
                    pod = _read_pod()
                except ApiException as e:
                    last_err = f"read failed after retries: {e}"
                    _cleanup_pod()
                    break
                if pod is None:  # pod vanished unexpectedly (evicted before completion) → try next
                    last_err = f"pod disappeared on node {node_name}"
                    break
                last_phase = pod.status.phase
                cst = (pod.status.container_statuses or [])
                if last_phase == "Running" or (cst and (cst[0].state.running or cst[0].state.terminated)):
                    started = True
                if last_phase == "Succeeded":
                    _cleanup_pod()
                    return
                if last_phase == "Failed":
                    if started:
                        # Container actually ran and failed → real script error, don't retry nodes.
                        _cleanup_pod()
                        raise RuntimeError(f"workspace durable-op {pod_name} failed on shard {shard_pvc} (node {node_name})")
                    # Failed before the container ever started (e.g. evicted / DiskPressure) → not a
                    # script error; treat like a stall and try the next candidate.
                    last_err = f"pod Failed pre-start on node {node_name}"
                    _cleanup_pod()
                    break
                if not started and (time.monotonic() - attempt_start) >= start_deadline:
                    last_err = f"did not start on node {node_name} within {start_deadline}s (likely NFS mount failure)"
                    logger.warning("durable-op %s did not start on %s within %ss; retrying next node",
                                   pod_name, node_name, start_deadline)
                    _cleanup_pod()
                    break
                time.sleep(1)
        raise RuntimeError(f"workspace durable-op {pod_name} exhausted candidates for shard {shard_pvc}: {last_err}")

    def _flush_lock_for(self, key: str):
        """Return a per-(instance,node) lock, created on first use. Serializes the delete-path flush
        and the reconciler retry sweep so they never run concurrent rsyncs for the same copy."""
        with self._flush_locks_guard:
            lk = self._flush_locks.get(key)
            if lk is None:
                lk = threading.Lock()
                self._flush_locks[key] = lk
            return lk

    def _flush_workspace_to_durable(self, instance_id: str, node_name: str, session_token: str,
                                    timeout_seconds: int = 600) -> bool:
        """Flush an instance's node-local SSD /workspace copy up to its durable NFS shard.

        Replaces the old in-pod preStop hook: the notebook container no longer mounts the durable
        shard, so the local -> durable flush is driven from the manager (delete path + reconciler
        retry sweep) by a one-shot pod that mounts BOTH the durable shard PVC (at /durable, RW,
        subPath per instance) AND the host-resident SSD copy (at /local, hostPath). Because the SSD
        copy survives pod teardown (the loop mount lives in the host mount namespace, Bidirectional
        propagation), this fires regardless of how the pod died — graceful delete, OOM, eviction, or
        force-delete — closing the ungraceful-kill data-loss gap the preStop hook had.

        PINNED to node_name unconditionally: the data exists on exactly ONE node, so unlike
        _run_durable_shard_command there is no candidate list to fall back to. If the node is
        NotReady/gone the pod can't start and this returns False WITHOUT marking flushed, so the
        reaper won't reap the unflushed SSD copy and the reconciler sweep retries once the node
        returns.

        session_token FENCES the confirmation (store.mark_workspace_flushed only marks the local-copy
        row whose token still matches), so a straggler flush that confirms after a relaunch/re-stop
        can never falsely certify a session it didn't flush.

        Uses `rsync -a --update` (newer-wins, NO --delete) — identical semantics to the retired
        preStop and symmetric with the hydrate init, so flush and hydrate can never fight."""
        if not node_name or not session_token:
            logger.info("flush skip for %s: missing node/session token (nothing to flush)", instance_id)
            return True
        if not self._workspace_localcache_enabled():
            return True
        shard_pvc = self._durable_shard_pvc(instance_id)
        durable_subpath = self._durable_subpath(instance_id)
        local_path = self._workspace_local_cache_path(instance_id)
        cache_root = settings.WORKSPACE_LOCAL_CACHE_ROOT.rstrip("/")
        # Guard: only ever hostPath-mount a path strictly under the cache root.
        if not local_path.startswith(cache_root + "/") or local_path == cache_root:
            logger.error("Refusing to flush suspicious local cache path %s", local_path)
            return False

        lock = self._flush_lock_for(f"{instance_id}\x00{node_name}")
        if not lock.acquire(blocking=False):
            # Another flush for this exact copy is already running (delete path vs retry sweep). Skip;
            # the in-flight one will mark flushed, or the sweep retries later. Not a failure.
            logger.info("flush for %s on %s already in progress; skipping duplicate", instance_id, node_name)
            return False
        try:
            # A live pod means the instance was (re)launched on this node; its hydrate-init may be
            # rsyncing durable->local right now. Running our local->durable rsync concurrently against
            # the same host dir would race. Abort — the copy is the active working set, not a stopped
            # copy to flush; a later stop will re-record it.
            if self._pod_exists(instance_id):
                logger.info("flush for %s aborted: pod is live (relaunched); not flushing over active session",
                            instance_id)
                return False

            # Unique per-invocation pod name so the delete-path flush and the retry sweep can never
            # delete each other's in-flight pod (their _cleanup only targets their own name).
            run_id = secrets.token_hex(4)
            safe = self._safe_storage_segment(f"{instance_id}-{run_id}").lower()
            pod_name = f"ws-flush-{safe}"[:63].rstrip("-")

            # DATA-SAFETY (finding 4): under quota, /local is an ext4 loop image mounted out-of-band in
            # the host mount namespace. If that mount is ABSENT when we run (node rebooted, host agent
            # hasn't re-provisioned), hostPath sees an EMPTY plain dir — which must NOT be mistaken for
            # "nothing to flush" (that would mark flushed and let the reaper destroy the real, still
            # loop-resident data). The prep DaemonSet drops a sentinel file (.oneclick-ssd-root) at the
            # cache-root; inside a per-instance loop mount that sentinel is NOT visible. So: if the
            # instance dir is empty AND the root sentinel IS visible from inside /local's parent, the
            # loop is not mounted (or there's genuinely no data) — treat an empty dir cautiously:
            #   * quota ON  -> require the loop to be an actual mountpoint before declaring no-op; if
            #                  /local exists, is empty, and is NOT a mountpoint => FAIL (exit 3) so we
            #                  do not mark flushed.
            #   * quota OFF -> /local is a plain dir; empty legitimately means no data => no-op OK.
            quota_on = self._workspace_quota_enabled()
            if quota_on:
                guard = (
                    "if [ -n \"$(ls -A /local 2>/dev/null)\" ]; then\n"
                    "  rsync -a --update /local/ /durable/\n"
                    "  echo WS_FLUSH_OK\n"
                    "elif mountpoint -q /local; then\n"
                    "  echo \"loop mounted but empty; nothing to flush\"; echo WS_FLUSH_OK\n"
                    "else\n"
                    "  echo \"ERROR: /local empty and NOT a mountpoint under quota — loop image not \"\n"
                    "  echo \"mounted; refusing to mark flushed\" >&2\n"
                    "  exit 3\n"
                    "fi\n"
                )
            else:
                guard = (
                    "if [ -n \"$(ls -A /local 2>/dev/null)\" ]; then\n"
                    "  rsync -a --update /local/ /durable/\n"
                    "fi\n"
                    "echo WS_FLUSH_OK\n"
                )
            script = "set -eux\n" + guard

            overall_deadline = time.monotonic() + max(1, timeout_seconds)
            start_deadline = max(30, min(90, timeout_seconds // 4))

            def _read_pod():
                last = None
                for _ in range(3):
                    try:
                        return self.core_v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
                    except ApiException as e:
                        if e.status == 404:
                            return None
                        last = e
                        time.sleep(1)
                raise last if last else RuntimeError("read failed")

            def _cleanup_pod():
                try:
                    self.core_v1.delete_namespaced_pod(name=pod_name, namespace=self.namespace, grace_period_seconds=0)
                except ApiException as e:
                    if e.status != 404:
                        logger.debug("flush cleanup delete of %s: %s", pod_name, e)
                        return
                for _ in range(30):
                    try:
                        self.core_v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
                        time.sleep(1)
                    except ApiException as e:
                        if e.status == 404:
                            return
                        logger.debug("flush cleanup poll of %s: %s", pod_name, e)
                        return

            body = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": self.namespace,
                    "labels": {"app": "oneclick-workspace-flush", "instance-id": instance_id},
                },
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "nodeName": node_name,
                    "tolerations": self._notebook_tolerations(),
                    "securityContext": {
                        "runAsNonRoot": False,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "flush",
                            "image": self._workspace_sync_image(),
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/bin/bash", "-lc"],
                            "args": [script],
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "64Mi"},
                                "limits": {"cpu": "1", "memory": "512Mi"},
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "volumeMounts": [
                                {"name": "durable", "mountPath": "/durable", "subPath": durable_subpath},
                                # HostToContainer so the loop-image contents (mounted in the host mount
                                # namespace) are visible, mirroring the hydrate init's /workspace mount.
                                {"name": "local", "mountPath": "/local",
                                 "mountPropagation": "HostToContainer"},
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "durable", "persistentVolumeClaim": {"claimName": shard_pvc}},
                        # DirectoryOrCreate so a genuinely-reaped dir mounts empty (quota-off no-op).
                        # Under quota the in-container mountpoint check guards the empty-but-unmounted
                        # case before we ever mark flushed.
                        {"name": "local", "hostPath": {"path": local_path, "type": "DirectoryOrCreate"}},
                    ],
                },
            }
            _cleanup_pod()
            try:
                self.core_v1.create_namespaced_pod(namespace=self.namespace, body=body)
            except ApiException as e:
                logger.error("flush pod create failed for %s on %s: %s", instance_id, node_name, e)
                return False
            started = False
            attempt_start = time.monotonic()
            try:
                while True:
                    if time.monotonic() >= overall_deadline:
                        logger.error("flush for %s on %s timed out (started=%s)", instance_id, node_name, started)
                        return False
                    try:
                        pod = _read_pod()
                    except ApiException as e:
                        logger.error("flush read failed for %s: %s", instance_id, e)
                        return False
                    if pod is None:
                        logger.error("flush pod for %s vanished on %s before completion", instance_id, node_name)
                        return False
                    phase = pod.status.phase
                    cst = (pod.status.container_statuses or [])
                    if phase == "Running" or (cst and (cst[0].state.running or cst[0].state.terminated)):
                        started = True
                    if phase == "Succeeded":
                        marked = store.mark_workspace_flushed(instance_id, node_name, session_token)
                        if marked:
                            logger.info("Flushed workspace %s -> durable shard %s (node %s, session %s)",
                                        instance_id, shard_pvc, node_name, session_token)
                        else:
                            # Session advanced (relaunch/re-stop) while we flushed — our copy was the
                            # OLD session's; the current row keeps its own (correct) unflushed state.
                            logger.info("flush for %s on %s succeeded but session token stale (%s); "
                                        "not marking current row", instance_id, node_name, session_token)
                        return marked
                    if phase == "Failed":
                        logger.error("flush pod for %s FAILED on %s (shard %s) — not marked",
                                     instance_id, node_name, shard_pvc)
                        return False
                    if not started and (time.monotonic() - attempt_start) >= start_deadline:
                        logger.error("flush for %s did not start on %s within %ss (node may be NotReady/"
                                     "cannot mount shard) — not marked", instance_id, node_name, start_deadline)
                        return False
                    time.sleep(1)
            finally:
                _cleanup_pod()
        finally:
            lock.release()

    def delete_workspace_durable(self, instance_id: str) -> bool:
        """ADMIN ONLY: soft-delete an instance's durable workspace by moving its subdir into
        <shard>/.trash/<id>-<epoch>/. Reversible until the nightly trash purge. Returns True on
        success. Nothing automatic calls this — user data is kept forever until an admin acts."""
        if not self._workspace_localcache_enabled():
            logger.info("delete_workspace_durable no-op: localcache not enabled")
            return False
        # Refuse to trash a live instance's durable subdir: the running pod's subPath bind-mount would
        # survive the mv (writes keep landing in the now-hidden .trash copy), and the trash purge would
        # later delete a still-live workspace. Require the pod to be gone first.
        if self._pod_exists(instance_id):
            logger.error("Refusing durable delete for %s: pod still running (stop it first)", instance_id)
            return False
        shard_pvc = self._durable_shard_pvc(instance_id)
        subpath = self._durable_subpath(instance_id)
        # Defense-in-depth containment: this is an admin-triggerable destructive op against a SHARED
        # multi-tenant shard PVC. Require the subpath to be exactly "<2-hex-bucket>/<non-dot segment>"
        # so a crafted/typo'd instance_id can never collapse `src` to the shard root (which would mv the
        # whole shard into .trash). _safe_storage_segment already rejects dot-only segments; this is a
        # belt-and-suspenders check right before the mv.
        parts = subpath.split("/")
        if (len(parts) != 2 or not re.fullmatch(r"[0-9a-f]{2}", parts[0])
                or parts[1] in (".", "..", "") or "/" in parts[1]):
            logger.error("Refusing durable delete for %s: unsafe subpath %r", instance_id, subpath)
            return False
        # Atomic + idempotent soft-delete, safe to re-run if a prior attempt was interrupted:
        #   - The trash dir name ENCODES the delete epoch (`<id>-<epoch>`). purge_durable_trash parses
        #     that epoch for the retention window instead of relying on filesystem mtime, so retention
        #     is correct regardless of what mtime the moved dir carries or whether any `touch` ran.
        #     This removes the mv/touch-truncation data-loss risk (a kill between mv and touch used to
        #     leave a stale-mtime dir that the next purge would delete far too early).
        #   - The move is a SINGLE `mv "$src" "$dst"` = one same-fs rename() syscall, which is atomic:
        #     it either fully happened (src gone, dst present) or not at all (src intact). There is no
        #     half-moved state to split across a retry.
        #   - Retry idempotency: if a prior attempt already completed the rename, `$src` is gone and the
        #     `[ -d "$src" ]` guard makes this a clean no-op success (the correctly-named dst already
        #     exists). If the rename never ran, we do it now. Either way no data is lost or duplicated.
        safe_id = self._safe_storage_segment(instance_id)
        script = (
            f"src=/durable/{shlex.quote(subpath)}\n"
            f"trash_dir=/durable/.trash\n"
            f"mkdir -p \"$trash_dir\"\n"
            f"if [ -d \"$src\" ]; then dst=\"$trash_dir/{shlex.quote(safe_id)}-$(date +%s)\"; "
            f"mv \"$src\" \"$dst\"; echo trashed \"$src\" '->' \"$dst\"; "
            f"else echo \"no live durable data at $src (already trashed or never existed)\"; fi\n"
        )
        # Best-effort: drop the SFS dir-quota BEFORE the trash-move, while the subdir still exists at
        # its original path. The move is an mv (rename) — afterward the path is GONE (not "emptied"),
        # and SFS refuses delete_fs_dir_quota on a non-empty dir, so this is the only correct ordering.
        # Never raises / never blocks the delete; an orphan rule on a vanished path costs nothing.
        try:
            from . import workspace_dirquota
            workspace_dirquota.delete_dir_quota_for_instance(instance_id)
            workspace_dirquota.mark_unapplied(instance_id)
        except Exception as e:
            logger.debug("dirquota delete hook for %s failed (non-fatal): %s", instance_id, e)
        try:
            self._run_durable_shard_command(f"del-{instance_id}", shard_pvc, script, timeout_seconds=180)
            logger.info("Soft-deleted durable workspace for %s on shard %s", instance_id, shard_pvc)
            # The SSD copy (if any) is now stale AND would be resurrected by the retry sweep (which
            # would flush it back up, undoing the trash). The pod is already gone (guarded above), so:
            # (1) free the SSD copy on the last node, then (2) drop all copy rows so neither the retry
            # sweep nor the reaper acts on it again.
            try:
                last_node = store.get_workspace_last_node(instance_id)
                if last_node:
                    self._cleanup_local_workspace_cache(instance_id, last_node)
            except Exception as e:
                logger.debug("SSD cleanup after durable delete of %s: %s", instance_id, e)
            try:
                store.clear_all_local_copies(instance_id)
            except Exception as e:
                logger.debug("clear_all_local_copies after durable delete of %s: %s", instance_id, e)
            return True
        except Exception as e:
            logger.error("delete_workspace_durable failed for %s: %s", instance_id, e)
            return False

    def purge_durable_trash(self, retention_days: Optional[int] = None) -> int:
        """Purge <shard>/.trash entries older than retention across all shards. Returns the number
        of shards successfully processed. Best-effort per shard."""
        if not self._workspace_localcache_enabled():
            return 0
        days = retention_days if retention_days is not None else settings.WORKSPACE_DURABLE_TRASH_RETENTION_DAYS
        max_age_secs = max(1, int(days) * 24 * 60 * 60)
        processed = 0
        for index in range(len(self._durable_shard_classes())):
            shard_pvc = self._durable_shard_pvc_for_index(index)
            # Retention is derived from the delete epoch ENCODED in each trash entry's name
            # (`<id>-<epoch>`), not filesystem mtime — so a same-fs mv that preserved an old mtime
            # can't cause premature deletion. Entries whose trailing -<epoch> is older than the
            # window are removed; names without a parseable epoch are left alone (never blindly rm'd).
            script = (
                f"trash_dir=/durable/.trash\n"
                f"now=$(date +%s); max_age={max_age_secs}\n"
                f"if [ -d \"$trash_dir\" ]; then "
                f"for e in \"$trash_dir\"/*; do "
                f"[ -e \"$e\" ] || continue; "
                f"base=$(basename \"$e\"); ep=${{base##*-}}; "
                f"case \"$ep\" in ''|*[!0-9]*) echo \"skip (no epoch): $base\"; continue;; esac; "
                f"age=$((now - ep)); "
                f"if [ \"$age\" -gt \"$max_age\" ]; then rm -rf \"$e\" && echo \"purged $base (age ${{age}}s)\"; "
                f"else echo \"keep $base (age ${{age}}s)\"; fi; "
                f"done; echo purged-done \"$trash_dir\"; else echo 'no trash dir'; fi\n"
            )
            try:
                self._run_durable_shard_command(f"trash-{index}", shard_pvc, script, timeout_seconds=300)
                processed += 1
            except Exception as e:
                logger.error("purge_durable_trash failed on shard %s: %s", shard_pvc, e)
        return processed

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

    def _ensure_durable_shards(self, wait_bound: bool = True) -> list:
        """Bootstrap the durable tier: one shared RWX PVC per configured StorageClass, created
        once (idempotent). Unlike the static network-disk PV/PVC, these use the CSI dynamic
        provisioner (nfs.csi.k8s.io) — we create only the PVC with storageClassName=<shard class>
        and the provisioner creates the backing PV on the corresponding SFS-Turbo filesystem.

        wait_bound=True (startup): poll each PVC to Bound (up to ~60s each). wait_bound=False
        (launch hot path): create-if-missing only, no Bound-poll — keeps the per-launch cost to a
        few fast idempotent reads and never blocks the event loop for a minute per shard.

        Once all shards have been observed Bound, a cached flag short-circuits subsequent calls so the
        launch path does zero API work in the steady state. Best-effort: logs and continues on a
        per-shard failure so a single unavailable backend does not block launches. Returns the list of
        PVC names confirmed present (Bound when wait_bound, else created/existing)."""
        if not self._workspace_localcache_enabled():
            return []
        classes = self._durable_shard_classes()
        # Steady-state fast path: once every shard was seen Bound, don't re-hit the API on launches.
        if getattr(self, "_durable_shards_ready", False):
            return [self._durable_shard_pvc_for_index(i) for i in range(len(classes))]
        size = f"{int(settings.WORKSPACE_DURABLE_PVC_SIZE_GI)}Gi"
        bound = []
        for index, storage_class in enumerate(classes):
            pvc_name = self._durable_shard_pvc_for_index(index)
            pvc_body = {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {
                    "name": pvc_name,
                    "namespace": self.namespace,
                    "labels": {"app": "oneclick-workspace-durable", "shard": str(index)},
                },
                "spec": {
                    "accessModes": ["ReadWriteMany"],
                    "resources": {"requests": {"storage": size}},
                    "storageClassName": storage_class,
                },
            }
            try:
                self.core_v1.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=self.namespace)
            except ApiException as e:
                if e.status == 404:
                    try:
                        self.core_v1.create_namespaced_persistent_volume_claim(namespace=self.namespace, body=pvc_body)
                        logger.info("Created durable shard PVC %s (class %s)", pvc_name, storage_class)
                    except ApiException as ce:
                        logger.error("Failed creating durable shard PVC %s (class %s): %s", pvc_name, storage_class, ce)
                        continue
                else:
                    logger.error("Failed reading durable shard PVC %s: %s", pvc_name, e)
                    continue
            if not wait_bound:
                # Launch hot path: PVC exists (or was just created); don't block polling for Bound.
                bound.append(pvc_name)
                continue
            # Startup: confirm Bound (Immediate binding provisions right away).
            is_bound = False
            for _ in range(60):
                try:
                    pvc = self.core_v1.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=self.namespace)
                except ApiException as e:
                    logger.error("Failed polling durable shard PVC %s: %s", pvc_name, e)
                    break
                if pvc.status.phase == "Bound":
                    is_bound = True
                    break
                time.sleep(1)
            if is_bound:
                bound.append(pvc_name)
            else:
                logger.error("Durable shard PVC %s did not bind (class %s)", pvc_name, storage_class)
        # Cache readiness only when we actually confirmed all shards Bound (startup path).
        if wait_bound and len(bound) == len(classes):
            self._durable_shards_ready = True
        if len(bound) != len(classes):
            logger.warning("Durable shards ready %s/%s: %s", len(bound), len(classes), bound)
        return bound

    def _resolve_resource_profile(self, gpu_count: int, resource_profile: Optional[str] = None) -> tuple[str, dict]:
        profile = (resource_profile or "auto").strip().lower()
        if profile == "auto":
            profile = AUTO_RESOURCE_PROFILE_BY_GPU.get(gpu_count, "standard")
        if profile not in RESOURCE_PROFILES:
            allowed = ", ".join(["auto", *RESOURCE_PROFILES.keys()])
            raise ValueError(f"Invalid resource profile '{resource_profile}'. Allowed values: {allowed}")
        return profile, RESOURCE_PROFILES[profile]

    def _service_launch_snippet(self, instance_id: str, notebook_dir: str,
                                pod_type: Optional[str] = None) -> str:
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
        # Hackathon pods enable JupyterLab real-time collaboration so participants sharing a pod
        # see each other's edits live. The flag is `--collaborative` (JupyterLab 4.x) — NOT
        # `--collaborate`, which jupyter-lab rejects with a usage error (exit 2).
        #
        # CRITICAL (verified on the base image): `--collaborative` is NOT a graceful no-op when the
        # `jupyter_collaboration` extension is missing — jupyter-lab HARD-EXITS with code 1
        # ("cannot start, because jupyter_collaboration was configured but cannot be imported").
        # _build_startup_script best-effort pip-installs the extension, but that fetch can fail
        # (mirror throttling / DNS). So we must NOT pass the flag unconditionally: we gate it at
        # launch on the extension actually being importable, computed here in shell. If the install
        # failed, COLLAB_FLAG stays empty and Jupyter launches normally (RTC just off) instead of
        # crash-looping the pod.
        if pod_type == "hackathon":
            # Gate the flag on the extension being visible to JUPYTER's own environment, checked via
            # `jupyter labextension list` — NOT a bare `python3 -c import`. The export PATH above
            # puts /usr/bin ahead of the venv, so `python3` can resolve to the system interpreter
            # while `jupyter` runs from /opt/venv; a bare python import then reports the extension
            # missing even after a successful venv install, silently disabling collaboration.
            # `jupyter labextension list` always runs under the same interpreter `jupyter lab` will,
            # so it reflects exactly what the server will see at startup. NOTE: it prints its listing
            # to STDERR, not stdout — so we merge 2>&1 before grep; piping 2>/dev/null would discard
            # the very lines we match on and the gate would always fail.
            collab_gate = (
                "COLLAB_FLAG=\"\"\n"
                "if jupyter labextension list 2>&1 | grep -qi collaboration; then\n"
                "    COLLAB_FLAG=\"--collaborative\"\n"
                "    echo \"[oneclick] jupyter_collaboration present; enabling real-time collaboration.\"\n"
                "else\n"
                "    echo \"[oneclick] jupyter_collaboration missing; starting Jupyter WITHOUT collaboration.\"\n"
                "fi\n"
            )
            collab_flag_ref = "$COLLAB_FLAG "
        else:
            collab_gate = ""
            collab_flag_ref = ""
        return (
            "set +e\n"
            "export PATH=\"/usr/local/bin:/usr/bin:/root/.opencode/bin:$PATH\"\n"
            ": > /tmp/opencode-web.log\n"
            f"{collab_gate}"
            # CRITICAL: start Jupyter FIRST and never block it on OpenCode. OpenCode is opt-in and
            # its installer fetches from opencode.ai/github (intermittently throttled from
            # cn-shanghai); a blocking, un-timed install there used to hang the whole startup so
            # Jupyter never launched and the instance was stuck "JupyterStarting". Jupyter is the
            # required process — launch it immediately; reconcile + run OpenCode in the background.
            f"jupyter lab --ip=0.0.0.0 --port={settings.NOTEBOOK_PORT} --no-browser --allow-root "
            f"{collab_flag_ref}"
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

    def _git_clone_init_container(self, image: str, github_info: dict,
                                  workspace_propagation: Optional[str],
                                  git_token: str) -> dict:
        """Init container that clones a PRIVATE repo using a token, isolated from the user.

        Why an init container: the notebook container gives the user a root shell, so any secret in
        its env is readable (os.environ, /proc/1/environ). Init containers are not user-exec
        reachable and their /proc is gone once they exit, so the token — placed only in THIS
        container's env — never reaches a user-readable surface. We clone into {workspace}/repo
        before the notebook container starts; its startup script then skips its own clone.

        Auth: the token is sent as the HTTP Basic *password* via a GIT_ASKPASS helper that reads it
        from the env at runtime (never on argv, never in .git/config). The username is the fixed,
        non-secret literal `x-access-token` (GitHub's documented token-auth convention), baked into
        the clone URL so git only prompts for the password. GIT_TERMINAL_PROMPT=0 fails fast instead
        of hanging if auth is rejected. main.py has already guaranteed repo_url is https:// before a
        token is allowed; this method independently re-asserts that (raises ValueError otherwise), so
        the credential is only ever sent over TLS regardless of the caller.
        """
        workspace = shlex.quote(settings.WORKSPACE_MOUNT_PATH)
        repo_url = github_info.get("repo_url") or github_info.get("clone_url") or ""
        # Fail CLOSED on transport, independently of the endpoint gate: a token is HTTP Basic auth,
        # so we must never arm GIT_ASKPASS for a non-https clone (that would put the PAT on the wire
        # in cleartext). main.py already 400s a token+http launch; this is the belt-and-suspenders
        # invariant so any future caller of this helper cannot silently downgrade the secret.
        if not repo_url.lower().startswith("https://"):
            raise ValueError("refusing to use a git token over a non-HTTPS clone URL")
        # Insert the x-access-token username into the https:// URL (GitHub token-auth convention);
        # the token itself is supplied only via GIT_ASKPASS, never in this URL.
        auth_repo_url = "https://x-access-token@" + repo_url[len("https://"):]
        repo_url_q = shlex.quote(auth_repo_url)
        branch = (github_info.get("branch") or "").strip()
        branch_opt = f"--branch {shlex.quote(branch)} " if branch else ""
        askpass_path = "/tmp/oneclick-git-askpass.sh"
        script = f"""
set -e
umask 077
mkdir -p {workspace}
if [ -e {workspace}/repo ]; then
    echo "workspace already populated; skipping authenticated clone"
    exit 0
fi
cat > {askpass_path} <<'ONECLICK_ASKPASS_EOF'
#!/bin/sh
printf '%s' "$GIT_CLONE_TOKEN"
ONECLICK_ASKPASS_EOF
chmod 700 {askpass_path}
export GIT_ASKPASS={askpass_path}
export GIT_TERMINAL_PROMPT=0
cloned=0
for i in 1 2 3; do
    rm -rf {workspace}/.repo-tmp
    if timeout 240 git -c http.lowSpeedLimit=1000 -c http.lowSpeedTime=30 clone --depth 1 {branch_opt}{repo_url_q} {workspace}/.repo-tmp; then
        mv {workspace}/.repo-tmp {workspace}/repo
        cloned=1
        echo "Private repository cloned"
        break
    fi
    echo "Authenticated clone attempt $i failed, retrying..."
    sleep $((i * 3))
done
rm -f {askpass_path}
if [ "$cloned" != "1" ]; then
    echo "Authenticated clone failed after retries" >&2
    exit 1
fi
"""
        workspace_mount = {"name": "workspace", "mountPath": settings.WORKSPACE_MOUNT_PATH}
        if workspace_propagation:
            workspace_mount["mountPropagation"] = workspace_propagation
        return {
            "name": "git-clone",
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "command": ["/bin/sh", "-c"],
            "args": [script],
            # Token lives ONLY on this init container (not the user's notebook container). It is still
            # a literal in the pod spec, so an operator with pod-read RBAC / etcd access can see it;
            # eliminating that too would require a per-pod Secret, which the manager's RBAC does not
            # currently permit creating. This closes the user-facing exposure, which is the exploit.
            "env": [{"name": "GIT_CLONE_TOKEN", "value": git_token}],
            "volumeMounts": [workspace_mount],
        }

    def _build_startup_script(self, instance_id: str,
                              instance_type: str = "jupyter",
                              github_info: Optional[dict] = None,
                              pod_type: Optional[str] = None) -> str:
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
        # Hackathon pods run JupyterLab with --collaborative (real-time collaboration), which
        # requires the `jupyter-collaboration` server extension that the base image does not ship.
        # Install it here, best-effort, before launch. Scoped to hackathon pods so non-hackathon
        # startups are unchanged. If this install fails (mirror/DNS), the launch snippet's runtime
        # import gate drops the flag so Jupyter still starts instead of crash-looping.
        collaboration_ensure = ""
        if pod_type == "hackathon":
            collaboration_ensure = f"""
if ! jupyter labextension list 2>&1 | grep -qi collaboration; then
    echo "[oneclick] Installing jupyter-collaboration for hackathon RTC via Tsinghua mirror..."
    pip install --no-cache-dir -i {settings.PIP_INDEX_URL} --trusted-host {settings.PYPI_HOST} jupyter-collaboration 2>&1 | tail -8 || pip3 install --no-cache-dir -i {settings.PIP_INDEX_URL} --trusted-host {settings.PYPI_HOST} jupyter-collaboration 2>&1 | tail -8 || echo "[oneclick] jupyter-collaboration install failed; collaboration disabled."
    hash -r 2>/dev/null || true
fi
"""
        # Hackathon pods launched via the HF API also need jupyter-server-proxy so
        # proxied services work. Install it here, best-effort, before launch. Scoped to
        # hackathon pods so non-hackathon startups are unchanged; a failed fetch (mirror/DNS)
        # must never abort startup (jupyter_ensure runs under `set -e` on the clone path).
        server_proxy_ensure = ""
        if pod_type == "hackathon":
            server_proxy_ensure = f"""
if ! jupyter server extension list 2>&1 | grep -qi server.proxy; then
    echo "[oneclick] Installing jupyter-server-proxy for hackathon via Tsinghua mirror..."
    pip install --no-cache-dir -i {settings.PIP_INDEX_URL} --trusted-host {settings.PYPI_HOST} jupyter-server-proxy 2>&1 | tail -8 || pip3 install --no-cache-dir -i {settings.PIP_INDEX_URL} --trusted-host {settings.PYPI_HOST} jupyter-server-proxy 2>&1 | tail -8 || echo "[oneclick] jupyter-server-proxy install failed; server proxy disabled."
    hash -r 2>/dev/null || true
fi
"""
        jupyter_ensure = jupyter_ensure + collaboration_ensure + server_proxy_ensure
        if github_info:
            notebook_path = github_info["path"].lstrip("/")
            notebook_filename = notebook_path.split("/")[-1]
            repo_url = github_info.get("repo_url") or github_info.get("clone_url")
            if repo_url:
                repo_url_q = shlex.quote(repo_url)
                notebook_path_q = shlex.quote(notebook_path)
                # Pin a branch only when one is given; otherwise follow the remote's default
                # HEAD so master-default (or any non-main) repos clone too. Templates default
                # branch to "main", so their behaviour is unchanged.
                branch = (github_info.get("branch") or "").strip()
                branch_opt = f"--branch {shlex.quote(branch)} " if branch else ""
                # Only look for / warn about a missing notebook when a path was requested. A
                # repo-only clone (no notebook_path) just opens JupyterLab at the repo root.
                notebook_check = ""
                if notebook_path:
                    # Echo the shlex-quoted path, never the raw value — a notebook_path can
                    # contain shell metacharacters (the .ipynb parser does not sanitize the
                    # path segments), which unquoted would break out of the echo.
                    notebook_check = f"""if [ ! -f {notebook_path_q} ]; then
    echo "Notebook not found:" {notebook_path_q}
    find . -maxdepth 4 -name '*.ipynb' | sed 's#^./##' | head -50
fi
"""
                # NOTE: a PRIVATE-repo clone that needs a token does NOT happen here. The token must
                # never enter the notebook container (the user has a root shell in it: PID 1's
                # /proc/1/environ, `os.environ`, and any child would expose it). Instead the manager
                # runs the authenticated clone in a dedicated init container (see
                # _git_clone_init_container) that populates {workspace}/repo before this container
                # starts; the block below then finds the repo already present and skips cloning. This
                # public path is unchanged and only clones when no token flow pre-populated the repo.
                return f"""
set -e
export PATH="/root/.opencode/bin:$PATH"
{model_link_script}
mkdir -p {workspace}

if [ ! -e {workspace}/repo ]; then
    echo "Cloning {repo_url}..."
    for i in 1 2 3; do
        rm -rf {workspace}/.repo-tmp
        if timeout 240 git -c http.lowSpeedLimit=1000 -c http.lowSpeedTime=30 clone --depth 1 {branch_opt}{repo_url_q} {workspace}/.repo-tmp; then
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
{notebook_check}
{jupyter_ensure}
{self._service_launch_snippet(instance_id, f"{workspace}/repo", pod_type=pod_type)}"""
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
{self._service_launch_snippet(instance_id, f"{workspace}/notebooks", pod_type=pod_type)}"""

        if instance_type == "opencode":
            return f"""
export PATH="/root/.opencode/bin:$PATH"
{model_link_script}
cd {workspace}
{jupyter_ensure}
{self._service_launch_snippet(instance_id, workspace, pod_type=pod_type)}"""

        # Default: jupyter
        return f"""
export PATH="/root/.opencode/bin:$PATH"
{model_link_script}
cd {workspace}
{jupyter_ensure}
{self._service_launch_snippet(instance_id, workspace, pod_type=pod_type)}"""

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
                          ssh_public_key: Optional[str] = None,
                          pod_type: Optional[str] = None,
                          api_launched: bool = False,
                          workspace_last_node: Optional[str] = None,
                          git_token: Optional[str] = None) -> dict:
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
        if pod_type:
            annotations["amd-oneclick/pod-type"] = pod_type
        if api_launched:
            annotations["amd-oneclick/api-launched"] = "true"

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
            startup_script = self._build_startup_script(instance_id, instance_type, github_info, pod_type=pod_type)

        ssh_enabled = bool(ssh_enabled)
        if ssh_enabled:
            annotations["amd-oneclick/ssh-enabled"] = "true"

        workspace_volume_type = (settings.WORKSPACE_VOLUME_TYPE or "hostPath").strip().lower()
        workspace_uses_empty_dir = workspace_volume_type == "emptydir"
        # Two-tier persistent workspace: node-local SSD /workspace backed by durable NFS shard.
        # Only for non-app instances (app types keep their image's own /workspace).
        workspace_is_localcache = workspace_volume_type == "localcache" and not is_app_type
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
        elif workspace_is_localcache:
            # /workspace lives on the node-local SSD (fast working copy). Durable NFS shard is
            # mounted separately below and synced in/out by init + preStop.
            volumes.append({
                "name": "workspace",
                "hostPath": {
                    "path": self._workspace_local_cache_path(instance_id),
                    "type": "DirectoryOrCreate"
                }
            })
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
        # NOTE: a private-repo git_token is deliberately NOT injected into this (notebook) container's
        # env. The user holds a root shell here, so any env var is readable via os.environ /
        # /proc/1/environ. The authenticated clone runs in a separate init container instead (see
        # _git_clone_init_container), keeping the token out of every user-reachable surface.
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
            # Use the warm base image (ships e2fsprogs/mount/rsync) instead of a public-registry
            # ubuntu, which is blocked from the nodes. Legacy behavior preserved when localcache is
            # off only if the warm image is still reachable; the base image is always node-local.
            quota_image = self._workspace_sync_image() if workspace_is_localcache else "docker.m.daocloud.io/library/ubuntu:24.04"
            init_containers.append({
                "name": "workspace-quota",
                "image": quota_image,
                "imagePullPolicy": "IfNotPresent",
                "securityContext": {"privileged": True},
                "command": ["/bin/bash", "-lc"],
                "args": [f"""
set -eux
img={shlex.quote(quota_image_path)}
mnt={shlex.quote(settings.WORKSPACE_MOUNT_PATH)}
mkdir -p /quota-images "$mnt"
# Idempotency guard (robust). IMPORTANT: $mnt is ALWAYS a mountpoint here — kubelet bind-mounts the
# SSD dir in via the "workspace" volume (source shows as /dev/nvme0n1p1[/workspace/<id>]). The loop
# image is meant to be STACKED on top of that bind (the original code did `mount -o loop` directly on
# it, and the notebook container's HostToContainer propagation then sees the topmost = the loop). So
# we must NOT unmount the bind base on a normal start.
#
# The bug the old guard had: `findmnt -o SOURCE` on a loop mount returns the loop DEVICE (/dev/loopN),
# never the image path, so its `grep -Fq "$img"` test was dead. On a restart where OUR loop is already
# the top mount it fell through and re-ran `mount -o loop` => "already mounted/busy" => set -e =>
# CrashLoopBackOff. Correct logic keyed on the CURRENT top source:
#   * top source is OUR loop (a /dev/loopN backing $img)  -> already mounted, skip (idempotent).
#   * top source is a FOREIGN loop (/dev/loopN NOT backing $img, e.g. a leaked prior-instance mount)
#     -> unmount it, then stack ours.
#   * top source is the bind base (not a loop device at all) -> normal first start; DO NOT unmount,
#     just stack our loop on top (original behavior).
src="$(findmnt -n -o SOURCE "$mnt" 2>/dev/null || true)"
# strip any findmnt "[/subpath]" suffix to get the bare device
dev_only="${{src%%[*}}"
if printf '%s' "$dev_only" | grep -q '^/dev/loop'; then
  if losetup -j "$img" 2>/dev/null | cut -d: -f1 | grep -qx "$dev_only"; then
    echo "quota loop already mounted at $mnt from $img; nothing to do"
    df -h "$mnt"; findmnt "$mnt"; exit 0
  fi
  echo "foreign loop mounted at $mnt (source=${{src:-none}}); unmounting before remounting ours"
  umount -l "$mnt" || true
fi
# Detach any loop devices still bound to $img but no longer mounted anywhere, so leaked devices
# don't accumulate across restarts (and we never remount a stale one).
for dev in $(losetup -j "$img" 2>/dev/null | cut -d: -f1); do
  if ! findmnt -n -S "$dev" >/dev/null 2>&1; then losetup -d "$dev" 2>/dev/null || true; fi
done
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
                    "path": self._workspace_quota_image_host_root(),
                    "type": "DirectoryOrCreate"
                }
            })

        # Two-tier localcache: mount the durable shard (subPath per instance) ONLY on the hydrate
        # init container to seed the node-local /workspace at startup. The durable shard is NOT
        # mounted on the main (user) container — the flush local -> durable runs out-of-pod from the
        # manager delete path (_flush_workspace_to_durable) against the host-resident SSD copy. This
        # keeps the durable NFS invisible/unwritable from inside the user's notebook (they can't
        # bypass the 100GB quota cap by writing straight to the uncapped shared NFS) and also closes
        # the ungraceful-kill gap for free (OOM/eviction/force-delete no longer skip the flush).
        # Independent of the quota loop-mount: with quota on, /workspace is the loop image (so the
        # hydrate mount uses HostToContainer propagation to see it); with quota off, /workspace is
        # the plain SSD dir.
        if workspace_is_localcache:
            shard_pvc = self._durable_shard_pvc(instance_id)
            durable_subpath = self._durable_subpath(instance_id)
            durable_mount_path = settings.WORKSPACE_DURABLE_MOUNT_PATH.rstrip("/")
            annotations["amd-oneclick/workspace-durable-pvc"] = shard_pvc
            annotations["amd-oneclick/workspace-durable-subpath"] = durable_subpath
            annotations["amd-oneclick/workspace-local-cache"] = self._workspace_local_cache_path(instance_id)
            # Hydrate init container: merge durable INTO local using `rsync -a --update` (newer-mtime
            # wins), NEVER `--delete`. This is the load-bearing data-safety choice:
            #   * An ungraceful kill (OOM/eviction/force-delete) skips the preStop flush, so durable
            #     is stale. If soft-affinity lands the relaunch on the same node, the warm local copy
            #     is NEWER than durable. `--update` keeps those newer local files instead of clobbering
            #     them (a plain `--delete` mirror would destroy every change since the last flush).
            #   * `--update` also means an empty/partial durable can never wipe a good local copy.
            # Trade-off: a file deleted on one side is not propagated as a deletion to the other (it
            # reappears from whichever side still has it). For a "keep user data forever" system this
            # is the correct, conservative direction — accumulate, never silently destroy. rsync writes
            # via a temp file + atomic rename, so a mid-copy SIGKILL leaves a stray temp file, not a
            # corrupted destination file. Runs after the quota loop-mount when quota is enabled.
            init_containers.append({
                "name": "workspace-hydrate",
                "image": self._workspace_sync_image(),
                "imagePullPolicy": "IfNotPresent",
                "command": ["/bin/bash", "-lc"],
                "args": [f"""
set -eux
src={shlex.quote(durable_mount_path + '/')}
dst={shlex.quote(settings.WORKSPACE_MOUNT_PATH.rstrip('/') + '/')}
mkdir -p "$src" "$dst"
if [ -n "$(ls -A "$src" 2>/dev/null)" ]; then
  rsync -a --update "$src" "$dst"
  echo "hydrated $dst from durable (newer-wins merge)"
else
  echo "durable empty; keeping local cache as-is (will seed durable on flush)"
fi
"""],
                "volumeMounts": [
                    {"name": "workspace", "mountPath": settings.WORKSPACE_MOUNT_PATH,
                     "mountPropagation": "HostToContainer"},
                    {"name": "workspace-durable", "mountPath": durable_mount_path, "subPath": durable_subpath},
                ],
            })
            # The durable shard volume is declared once for the whole pod; only the hydrate init
            # container mounts it (above). Init containers are not user-exec-reachable, so this does
            # not expose the shard to the notebook user. NO mount on the main container and NO preStop
            # flush — the flush is driven out-of-pod by the manager (_flush_workspace_to_durable).
            volumes.append({
                "name": "workspace-durable",
                "persistentVolumeClaim": {"claimName": shard_pvc},
            })

        # Private-repo clone runs in a dedicated init container (NOT the user's notebook container).
        # The token lives only in this init container's env; init containers are not user-exec
        # reachable and their process table / /proc is gone once they complete, so the PAT never
        # touches a surface the (root-in-pod) notebook user can read. It clones into {workspace}/repo
        # before the notebook container starts; the notebook startup script then finds the repo
        # present and skips its own (public, unauthenticated) clone. Gated on github_info + repo_url +
        # git_token so public/.ipynb/bare launches are completely unaffected.
        git_clone_repo_url = (github_info or {}).get("repo_url") if github_info else None
        if git_token and git_clone_repo_url:
            # Match the notebook container's workspace propagation: when the quota loop is mounted on
            # the host by the privileged quota init container, HostToContainer lets this init see it.
            clone_workspace_propagation = "HostToContainer" if (
                settings.WORKSPACE_QUOTA_ENABLED and not workspace_uses_empty_dir and not is_app_type
            ) else None
            init_containers.append(self._git_clone_init_container(
                image, github_info, clone_workspace_propagation, git_token.strip(),
            ))

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
            notebook_container.setdefault("lifecycle", {})["postStart"] = {
                "exec": {"command": ["/bin/sh", "-c", self._ssh_poststart_script()]}
            }

        # NOTE: no localcache preStop flush on the notebook container — the local -> durable flush is
        # driven out-of-pod by the manager (_flush_workspace_to_durable) reading the host-resident
        # SSD copy after the pod is gone. This makes the flush fire regardless of how the pod died
        # (graceful delete, OOM, eviction, force-delete) and keeps the durable NFS out of the pod.

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
        # No terminationGracePeriodSeconds override for localcache: with no in-pod flush there is
        # nothing that needs a long grace window. The out-of-pod flush runs after the pod is gone and
        # reads the host-resident SSD copy, which survives pod teardown (delayed-local-reaper design).
        if init_containers:
            spec["initContainers"] = init_containers
        if notebook_node_name:
            spec["nodeName"] = notebook_node_name
        else:
            # Soft-prefer (a) nodes that already have this image warm, and (b) the node holding this
            # instance's warm workspace cache. Both are preferred (soft) so scheduling stays free;
            # skipped entirely when the pod is pinned via nodeName. Merged so neither overwrites the
            # other (a plain dict assignment would drop whichever ran second).
            affinity = self._merge_node_affinity(
                self._image_affinity(image),
                self._workspace_node_affinity(workspace_last_node),
            )
            if affinity:
                spec["affinity"] = affinity

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
                              ssh_node_port: Optional[int] = None,
                              owner_uid: Optional[str] = None) -> dict:
        """Generate Service manifest exposing Jupyter, OpenCode web, and optional SSH."""
        labels = self._get_labels(email, instance_id)
        metadata = {
            "name": f"{instance_id}-svc",
            "namespace": self.namespace,
            "labels": labels,
        }
        # Own the Service by the Pod so Kubernetes garbage-collects it whenever
        # the Pod is deleted (including the reconciler's force-delete path,
        # which only targets the Pod). Prevents leaked Services/NodePorts.
        if owner_uid:
            metadata["ownerReferences"] = [{
                "apiVersion": "v1",
                "kind": "Pod",
                "name": instance_id,
                "uid": owner_uid,
                "blockOwnerDeletion": False,
                "controller": False,
            }]

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
            "metadata": metadata,
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
                                            ssh_enabled: bool = False,
                                            owner_uid: Optional[str] = None) -> tuple:
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
                        body=self._get_service_manifest(email, instance_id, node_port, opencode_port, ssh_node_port, owner_uid=owner_uid),
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

    def _list_node_cached(self):
        """list_node() with a short TTL cache shared across target-resolution call sites.

        The eligibility filtering still runs per call against the cached snapshot; only the
        apiserver round-trip is cached. TTL is small (default 5s) so node churn (NotReady, drain,
        rejoin) is reflected within one cache window — fresh enough for image targeting."""
        now = time.monotonic()
        with self._node_list_cache_lock:
            if (
                self._node_list_cache is not None
                and (now - self._node_list_cache_ts) < self._node_list_cache_ttl
            ):
                return self._node_list_cache
            nodes = self.core_v1.list_node()
            self._node_list_cache = nodes
            self._node_list_cache_ts = now
            return nodes

    def _eligible_target_nodes(self, raise_on_error: bool = False) -> list[dict]:
        """Nodes the Image Service should distribute images to.

        Predicate: prepull label, schedulable, Ready, no DiskPressure, AND the node's
        taints are tolerated by this manager's notebook pods (so a service only targets
        its own nodes — see `_node_tolerated_by_notebooks`). Resolves each node's
        InternalIP and excludes the Image-Service host itself (it carries the prepull
        label). Nodes without an InternalIP are skipped (the daemon reaches them over it).

        By default a 403 listing nodes returns an empty best-effort list. Destructive callers
        (preheat DS create/reconcile, which delete the DS when the set is empty) pass
        raise_on_error=True so a transient 403 does NOT masquerade as 'zero eligible nodes' and
        tear down a healthy DaemonSet."""
        targets: list[dict] = []
        image_service_node = settings.IMAGE_SERVICE_NODE_NAME.strip()
        try:
            nodes = self._list_node_cached()
        except ApiException as e:
            if e.status == 403 and not raise_on_error:
                logger.warning("Cannot list nodes for image distribution; returning best-effort targets")
                return targets
            raise
        denylist = set(getattr(settings, "IMAGE_TARGET_NODE_DENYLIST", []) or [])
        for node in nodes.items:
            name = node.metadata.name
            if image_service_node and name == image_service_node:
                continue
            if name in denylist:
                continue
            labels = node.metadata.labels or {}
            conditions = {cond.type: cond.status for cond in node.status.conditions or []}
            if labels.get("amd-oneclick-prepull") != "enabled":
                continue
            if getattr(node.spec, "unschedulable", False):
                continue
            if conditions.get("Ready") != "True":
                continue
            # Exclude nodes under resource pressure. kubelet auto-taints these (DiskPressure /
            # MemoryPressure / PIDPressure via TaintNodesByCondition) with NoSchedule, and the
            # preheat pod deliberately does NOT tolerate them — so a pressured node left in the set
            # would be pinned into the DS nodeAffinity but never actually scheduled, silently
            # dropping out of desiredNumberScheduled and producing a false "ready" report.
            if conditions.get("DiskPressure") == "True":
                continue
            if conditions.get("MemoryPressure") == "True":
                continue
            if conditions.get("PIDPressure") == "True":
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
        node with `allocatable - committed >= gpu_count`, else None. Quarantined
        nodes (containerd wedged — Part D) are skipped so launches route around
        the wedge instead of into it."""
        quarantined = store.quarantined_nodes()
        for target in self._eligible_target_nodes():
            name = target["node"]
            if name in quarantined:
                continue
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

    def gpu_capacity_summary(self) -> dict:
        """Report free vs total GPUs over the nodes this service can launch onto.

        Scope is the launch-eligible node set (_eligible_target_nodes): GPU nodes carrying the
        prepull label whose taints this manager tolerates, excluding the image-service host. This
        is intentionally NOT the whole cluster — it is the capacity reachable by this service's
        launches, so free_gpus matches where _select_target_gpu_node actually routes pods. Reuses
        the launch-time accounting (allocatable amd.com/gpu minus GPUs committed by non-terminal
        pods). Quarantined nodes count toward total but contribute 0 to free. Kube errors are
        swallowed per-node so the endpoint returns partial results instead of failing."""
        quarantined = set()
        try:
            quarantined = store.quarantined_nodes()
        except Exception as e:
            logger.debug("quarantined_nodes lookup failed: %s", e)

        nodes_out = []
        total_gpus = 0
        free_gpus = 0
        try:
            targets = self._eligible_target_nodes()
        except Exception as e:
            logger.error("gpu_capacity_summary: cannot list nodes: %s", e)
            return {"total_gpus": 0, "free_gpus": 0, "nodes": []}

        for target in targets:
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
            if node_gpus <= 0:
                continue
            committed = 0
            try:
                pods = self.core_v1.list_namespaced_pod(
                    namespace=self.namespace,
                    field_selector=f"spec.nodeName={name},status.phase!=Succeeded,status.phase!=Failed",
                )
                for pod in pods.items:
                    for container in pod.spec.containers or []:
                        requests = getattr(container.resources, "requests", None) or {}
                        try:
                            committed += int(requests.get("amd.com/gpu", 0))
                        except (TypeError, ValueError):
                            continue
            except ApiException:
                pass
            is_quarantined = name in quarantined
            node_free = max(0, node_gpus - committed)
            total_gpus += node_gpus
            free_gpus += 0 if is_quarantined else node_free
            nodes_out.append({
                "node": name,
                "total": node_gpus,
                "free": node_free,
                "committed": committed,
                "quarantined": is_quarantined,
            })
        return {"total_gpus": total_gpus, "free_gpus": free_gpus, "nodes": nodes_out}

    @staticmethod
    def _quantity_to_int(value) -> int:
        """Best-effort parse of a Kubernetes quantity to a plain integer (count of GPUs etc.).
        Returns 0 on anything unparseable."""
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _memory_to_gib(value) -> float:
        """Convert a Kubernetes memory quantity string (e.g. '1056403496Ki', '256Gi', '64Mi')
        to GiB as a float, rounded to 1 decimal. Returns 0.0 on anything unparseable."""
        if value is None:
            return 0.0
        s = str(value).strip()
        units = {
            "Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3, "Ti": 1024 ** 4,
            "K": 1000, "M": 1000 ** 2, "G": 1000 ** 3, "T": 1000 ** 4,
        }
        try:
            for suffix, mult in units.items():
                if s.endswith(suffix):
                    return round(float(s[: -len(suffix)]) * mult / (1024 ** 3), 1)
            return round(float(s) / (1024 ** 3), 1)
        except (TypeError, ValueError):
            return 0.0

    def gpu_cluster_status(self) -> dict:
        """Per-node GPU status + cluster-utilization summary for the admin dashboard.

        Unlike gpu_capacity_summary (which is scoped to launch-eligible nodes and used by the
        scheduler), this reports EVERY GPU node belonging to this service — including cordoned /
        NotReady ones — so the dashboard shows real cluster health, not just schedulable capacity.

        IMPORTANT — usage scope: 'committed' GPUs and the per-node instance list are derived only
        from pods in THIS deployment's namespace (the service account is not granted cluster-wide
        pod read). So on shared GPU hardware the committed/free figures reflect THIS deployment's
        own usage, not every tenant's. node capacity/health (total GPUs, Ready/cordon, CPU/mem,
        model) is accurate for all nodes. The payload sets usage_scope='namespace' so the UI can
        label this honestly. Kube errors degrade gracefully to a partial/empty result."""
        # One namespaced pod list (allowed); group GPU requests + instance rows by node.
        committed_by_node: dict[str, int] = {}
        instances_by_node: dict[str, list] = {}
        try:
            pods = self.core_v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"app={settings.NOTEBOOK_LABEL_PREFIX}",
            )
            for pod in pods.items:
                phase = (pod.status.phase or "") if pod.status else ""
                if phase in ("Succeeded", "Failed"):
                    continue
                node_name = getattr(pod.spec, "node_name", None)
                if not node_name:
                    continue
                gpus = 0
                for container in pod.spec.containers or []:
                    requests = getattr(container.resources, "requests", None) or {}
                    gpus += self._quantity_to_int(requests.get("amd.com/gpu", 0))
                committed_by_node[node_name] = committed_by_node.get(node_name, 0) + gpus
                instances_by_node.setdefault(node_name, []).append({
                    "id": pod.metadata.labels.get("instance-id", pod.metadata.name),
                    "email": pod.metadata.annotations.get("amd-oneclick/email", ""),
                    "gpu_count": gpus,
                    "status": phase.lower() or "unknown",
                    "pod_type": pod.metadata.annotations.get("amd-oneclick/pod-type"),
                })
        except ApiException as e:
            logger.warning("gpu_cluster_status: cannot list pods: %s", e)

        quarantined = set()
        try:
            quarantined = store.quarantined_nodes()
        except Exception as e:
            logger.debug("quarantined_nodes lookup failed: %s", e)

        try:
            nodes = self.core_v1.list_node()
        except ApiException as e:
            logger.error("gpu_cluster_status: cannot list nodes: %s", e)
            return {"total_gpus": 0, "free_gpus": 0, "used_gpus": 0,
                    "nodes": [], "usage_scope": "namespace"}

        nodes_out = []
        total_gpus = 0
        free_gpus = 0
        used_gpus = 0
        for node in nodes.items:
            if not self._node_belongs_to_service(node):
                continue
            allocatable = node.status.allocatable or {}
            node_gpus = self._quantity_to_int(allocatable.get("amd.com/gpu", 0))
            if node_gpus <= 0:
                continue  # not a GPU node (or GPUs not advertised) — skip from a GPU dashboard
            name = node.metadata.name
            labels = node.metadata.labels or {}
            conditions = {c.type: c.status for c in (node.status.conditions or [])}
            ready = conditions.get("Ready") == "True"
            cordoned = bool(getattr(node.spec, "unschedulable", False))
            is_quarantined = name in quarantined
            committed = min(committed_by_node.get(name, 0), node_gpus)
            node_free = max(0, node_gpus - committed)
            total_gpus += node_gpus
            used_gpus += committed
            # A cordoned/NotReady/quarantined node cannot take new work — its capacity is not "free".
            free_gpus += node_free if (ready and not cordoned and not is_quarantined) else 0
            nodes_out.append({
                "node": name,
                "gpu_model": labels.get("amd.com/gpu.product-name") or "",
                "total": node_gpus,
                "committed": committed,
                "free": node_free,
                "ready": ready,
                "cordoned": cordoned,
                "quarantined": is_quarantined,
                "cpu_allocatable": str(allocatable.get("cpu", "")),
                "memory_allocatable_gib": self._memory_to_gib(allocatable.get("memory")),
                "instances": instances_by_node.get(name, []),
            })
        nodes_out.sort(key=lambda n: n["node"])
        return {
            "total_gpus": total_gpus,
            "free_gpus": free_gpus,
            "used_gpus": used_gpus,
            "nodes": nodes_out,
            "usage_scope": "namespace",
        }

    def count_user_pods_on_node(self, node_name: str) -> int:
        """Count running (non-terminal) user notebook pods on a node.

        Selects pods by the notebook app label and the node, excluding Succeeded/Failed. Raises on a
        kube API error so the caller (the pod-safety gate before an automated containerd restart)
        fails safe — an uncertain count must be treated as "pods present", never as zero."""
        pods = self.core_v1.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=f"app={settings.NOTEBOOK_LABEL_PREFIX}",
            field_selector=f"spec.nodeName={node_name},status.phase!=Succeeded,status.phase!=Failed",
        )
        return len(pods.items)

    def sync_image_to_nodes(self, image_id: int, image: str) -> dict:
        """Distribute a catalog image to every eligible GPU node.

        Image-service is the sole image-management system: this enqueues a
        `distribute` job (the out-of-cluster daemon does the actual
        `save | ssh ctr import`) and synthesizes status from `image_nodes`.
        The legacy prepull-DaemonSet / pull-probe path has been removed — no
        manager process ever creates an image-pull DaemonSet on a node, so a
        prepull pull can never co-tenant a node and race a same-digest
        image-service import on containerd's chain-ID unpack mutex."""
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

    def get_image_sync_status(self, image_id: int, image: Optional[str] = None) -> dict:
        """Return distribution status for an image catalog entry (image-service only)."""
        ref = (image or "").strip()
        targets = self._eligible_target_nodes()
        target_names = {t["node"] for t in targets}
        desired = len(target_names)
        loaded_nodes = set(store.list_nodes_for_image(ref)) if ref else set()
        ready = len(loaded_nodes & target_names) if target_names else len(loaded_nodes)
        # Readiness = image loaded on every eligible node. Registry durability is guaranteed
        # STRUCTURALLY, not via a digest gate here: the chain runs push BEFORE distribute, so a node
        # can only reach "loaded" after the push succeeded. Gating additionally on a recorded digest
        # would (a) flip every pre-P1 image — which has digest=NULL — from ready to pulling the
        # instant LAN_REGISTRY is set, and (b) strand an image whose digest couldn't be parsed even
        # though it pushed fine. The digest column is still recorded (P5 delete-by-digest) but must
        # NOT gate readiness. See tests/test_p1_registry_push.py.
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

    def get_image_node_scan_status(self, image_id: int, image: Optional[str] = None) -> dict:
        """Readiness for a 'manual' catalog row by scanning kubelet's own image inventory.

        Legacy manual rows (base images pre-mirrored into Harbor by hand, no distribute job) never
        populate the image_nodes table, so get_image_sync_status counts 0/N forever even though the
        image is genuinely on the nodes (instances launch from it). Instead of a DB table nothing
        writes, count eligible nodes whose node.status.images already lists this ref. This is free
        data: it comes from the same cached list_node() snapshot used for target resolution — no
        extra apiserver calls, no per-node reads, no dependency on the dead ctr-import daemon.

        A node's status.images entries each carry `names` (repo:tag + repoDigests). We match on the
        normalized ref so 'nginx' and 'docker.io/library/nginx:latest' compare equal. Best-effort:
        on a node-list error, fall back to the legacy DB read so status is never worse than before."""
        ref = (image or "").strip()
        if not ref:
            return self.get_image_sync_status(image_id, image)
        try:
            targets = self._eligible_target_nodes(raise_on_error=True)
        except ApiException as e:
            logger.warning("get_image_node_scan_status: node list failed (%s); using legacy read", e)
            return self.get_image_sync_status(image_id, image)
        target_names = {t["node"] for t in targets}
        desired = len(target_names)
        want = self._normalize_image_ref(ref)
        # Build node_name -> set(normalized image names) from the cached snapshot. _eligible_target_nodes
        # already consumed _list_node_cached(), so this hits the same cache window (no new API call).
        try:
            nodes = self._list_node_cached()
        except ApiException as e:
            logger.warning("get_image_node_scan_status: node images read failed (%s); using legacy read", e)
            return self.get_image_sync_status(image_id, image)
        # kubelet caps node.status.images at NodeStatusMaxImages (default 50), sorting by size DESC and
        # keeping the largest — so truncation only ever drops the SMALLEST images. The catalog images
        # this scans are multi-GB container images (base/model images, ~17-20GB — the largest on any
        # node), so they sort to the very top of that list and are not the ones a cap would drop. No
        # DB fallback for truncation: for 'manual' rows (the only callers) the image_nodes table is
        # empty by definition (that omission is the P1 root cause), so such a fallback would be dead
        # code. Worst case, if a large image ever WERE truncated, the count under-reports (node shows
        # 'pulling' though ready) — a visible, safe-side error, never a false 'ready'.
        ready = 0
        for node in nodes.items:
            name = node.metadata.name
            if name not in target_names:
                continue
            present = False
            for img in (node.status.images or []):
                for n in (img.names or []):
                    if self._normalize_image_ref(n) == want:
                        present = True
                        break
                if present:
                    break
            if present:
                ready += 1
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
            "message": f"{ready}/{desired} nodes have the image",
            "completed": status == "ready",
        }

    # --- Harbor preheat via unprivileged prepull DaemonSet -------------------------------------
    # Replaces the dead `distribute` job path: instead of an off-cluster daemon side-loading bytes,
    # a DaemonSet whose container IS the Harbor image makes each node's kubelet pull it. Proven live.

    def _preheat_ds_name(self, image_id: int) -> str:
        return f"oneclick-preheat-{int(image_id)}"

    def _preheat_tolerations(self) -> list[dict]:
        """Scoped tolerations for the preheat pod.

        The GPU-node taint(s) the notebooks tolerate (so preheat lands on exactly this service's
        nodes) PLUS bounded not-ready/unreachable. Deliberately does NOT tolerate
        DiskPressure/MemoryPressure — a preheat pull must never pile onto a pressured node."""
        # NOTE: keys are the snake_case kwargs of client.V1Toleration (built via V1Toleration(**t)),
        # NOT the camelCase manifest field names — 'toleration_seconds', not 'tolerationSeconds',
        # or V1Toleration.__init__ raises TypeError.
        tolerations = list(self._notebook_tolerations())
        tolerations.append({
            "key": "node.kubernetes.io/not-ready", "operator": "Exists",
            "effect": "NoExecute", "toleration_seconds": 300,
        })
        tolerations.append({
            "key": "node.kubernetes.io/unreachable", "operator": "Exists",
            "effect": "NoExecute", "toleration_seconds": 300,
        })
        return tolerations

    def _ensure_preheat_priority_class(self) -> Optional[str]:
        """Create the low, non-preempting PriorityClass once; return its name (or None on failure).

        A preheat pull is best-effort background work: it must yield to real workloads and must
        never preempt them. Missing RBAC / any error → return None so the DS is created without a
        priorityClassName rather than failing the whole preheat."""
        name = (settings.PREHEAT_PRIORITY_CLASS or "").strip()
        if not name:
            return None
        scheduling = client.SchedulingV1Api(self.core_v1.api_client)
        try:
            scheduling.read_priority_class(name=name)
            return name
        except ApiException as e:
            if e.status != 404:
                logger.warning("Cannot read PriorityClass %s: %s; preheat runs without one", name, e)
                return None
        body = client.V1PriorityClass(
            metadata=client.V1ObjectMeta(name=name),
            value=-10,
            global_default=False,
            preemption_policy="Never",
            description="Low, non-preempting priority for OneClick image preheat pods.",
        )
        try:
            scheduling.create_priority_class(body=body)
            return name
        except ApiException as e:
            if e.status == 409:  # created concurrently
                return name
            logger.warning("Cannot create PriorityClass %s: %s; preheat runs without one", name, e)
            return None

    def _preheat_ds_body(self, image_id: int, image: str, node_names: list[str]) -> "client.V1DaemonSet":
        name = self._preheat_ds_name(image_id)
        labels = {"app": "oneclick-preheat", "oneclick-preheat-image-id": str(int(image_id))}
        container = client.V1Container(
            name="preheat",
            image=image,
            command=["/bin/sh", "-c", "sleep infinity"],
            # Always, not IfNotPresent: the admin re-sync path re-mirrors the SAME mutable tag
            # (e.g. :latest) to pick up new upstream bits, and the DS is recreated to force a fresh
            # pull. IfNotPresent would let a node that cached the old bytes under that tag skip the
            # pull entirely, silently serving stale layers. The pull is a no-op when the digest is
            # already current, so Always is cheap for a throwaway sleep pod.
            image_pull_policy="Always",
            resources=client.V1ResourceRequirements(
                requests={
                    "cpu": "10m",
                    "memory": "32Mi",
                    "ephemeral-storage": settings.PREHEAT_EPHEMERAL_STORAGE,
                },
                limits={
                    "cpu": "100m",
                    "memory": "128Mi",
                    "ephemeral-storage": settings.PREHEAT_EPHEMERAL_STORAGE,
                },
            ),
        )
        # Pin to the EXACT eligible node set resolved by _eligible_target_nodes (which already
        # applies tenant/service scoping via _node_belongs_to_service, the IMAGE_SERVICE_NODE_NAME
        # exclusion, and IMAGE_TARGET_NODE_DENYLIST). A label-only affinity (prepull=enabled) would
        # be too broad — the prepull label is shared cluster-wide across tenants — so a preheat pod
        # could land on another service's / a denylisted node. hostname-pinning enforces the same
        # symmetric membership every other node-targeting path in this file uses.
        affinity = client.V1Affinity(
            node_affinity=client.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=client.V1NodeSelector(
                    node_selector_terms=[client.V1NodeSelectorTerm(
                        match_expressions=[
                            client.V1NodeSelectorRequirement(
                                key="kubernetes.io/hostname", operator="In", values=list(node_names)),
                            client.V1NodeSelectorRequirement(
                                key="node-role.kubernetes.io/control-plane", operator="DoesNotExist"),
                        ]
                    )]
                )
            )
        )
        pod_spec = client.V1PodSpec(
            containers=[container],
            affinity=affinity,
            tolerations=[client.V1Toleration(**t) for t in self._preheat_tolerations()],
            restart_policy="Always",
            termination_grace_period_seconds=0,
            automount_service_account_token=False,
            priority_class_name=self._ensure_preheat_priority_class(),
        )
        return client.V1DaemonSet(
            metadata=client.V1ObjectMeta(name=name, namespace=self.namespace, labels=labels),
            spec=client.V1DaemonSetSpec(
                selector=client.V1LabelSelector(match_labels={"app": "oneclick-preheat",
                                                              "oneclick-preheat-image-id": str(int(image_id))}),
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(labels=labels),
                    spec=pod_spec,
                ),
                update_strategy=client.V1DaemonSetUpdateStrategy(type="RollingUpdate"),
            ),
        )

    def preheat_image_to_nodes(self, image_id: int, image: str) -> dict:
        """Create/replace the preheat DaemonSet so every eligible node pulls `image` from Harbor.

        The manager SA can create/delete/get DaemonSets but NOT patch → replace = delete + create.
        Serialized per image_id so a concurrent create/update/sync cannot race the delete+recreate.
        Returns the initial preheat status."""
        image = (image or "").strip()
        if not image:
            raise ValueError("image must not be empty")
        # raise_on_error: a transient 403 must NOT look like "zero eligible nodes" and trigger a
        # teardown of a healthy DS. Let the exception propagate so the caller leaves the DS intact.
        # Exclude quarantined (containerd-wedged) nodes — the same routing every other node-targeting
        # path applies (see _select_target_gpu_node); pinning the DS to a wedged node would keep the
        # image forever "pulling" since that node can never complete the pull.
        quarantined = store.quarantined_nodes()
        node_names = [t["node"] for t in self._eligible_target_nodes(raise_on_error=True)
                      if t["node"] not in quarantined]
        name = self._preheat_ds_name(image_id)
        # Hold the per-image lock across BOTH the empty-set teardown and the create/replace so a
        # transient empty node set (e.g. the sole eligible node briefly quarantined) in one caller
        # can't delete a healthy DS a concurrent caller just created for the same image_id.
        with self._preheat_lock_for(image_id):
            if not node_names:
                # Genuinely no eligible nodes: don't create a DS with an empty hostname-In (apiserver
                # rejects an empty values list). Remove any stale DS and report pending.
                self._delete_preheat_ds_unlocked(image_id)
                return {"status": "pending", "desired_count": 0, "ready_count": 0,
                        "message": "no eligible nodes", "completed": False}
            # Re-check under lock that the catalog row still exists: a concurrent admin_delete_image
            # may have removed it (and torn down its DS) after our caller read a pre-delete snapshot.
            # Without this, we would resurrect a DaemonSet for a deleted image.
            if not store.image_row_exists(image_id):
                logger.info("preheat skipped: image %s was deleted", image_id)
                return {"status": "pending", "desired_count": 0, "ready_count": 0,
                        "message": "image deleted", "completed": False}
            body = self._preheat_ds_body(image_id, image, node_names)
            self._create_or_replace_preheat_ds(image_id, name, body)
        # Report status but floor desired_count at the node set we just pinned: the DS controller
        # populates status.desiredNumberScheduled asynchronously, so an immediate read often sees 0
        # and would misreport a fresh preheat as "pending"/"no nodes". min_desired makes the sync
        # response say "distributing 0/N" instead of a misleading "pending 0/0".
        return self.get_preheat_status(image_id, image, min_desired=len(node_names))

    def _preheat_lock_for(self, image_id: int) -> threading.Lock:
        with self._preheat_locks_guard:
            lock = self._preheat_locks.get(image_id)
            if lock is None:
                lock = threading.Lock()
                self._preheat_locks[image_id] = lock
            return lock

    def _create_or_replace_preheat_ds(self, image_id: int, name: str, body) -> None:
        try:
            self.apps_v1.create_namespaced_daemon_set(namespace=self.namespace, body=body)
            return
        except ApiException as e:
            if e.status != 409:
                raise
        # Already exists: delete then recreate (no patch RBAC). Wait for teardown, then create with
        # bounded retries so a slow Foreground GC (pod stuck on an unreachable node) doesn't surface
        # a bare 409 to the caller.
        try:
            self.apps_v1.delete_namespaced_daemon_set(
                name=name, namespace=self.namespace,
                body=client.V1DeleteOptions(propagation_policy="Foreground"),
            )
        except ApiException as e:
            if e.status != 404:
                raise
        gone = False
        for attempt in range(60):
            try:
                self.apps_v1.read_namespaced_daemon_set(name=name, namespace=self.namespace)
            except ApiException as read_err:
                if read_err.status == 404:
                    gone = True
                    break
                raise
            time.sleep(1)
        if not gone:
            # Foreground GC is blocked because a child pod is stuck Terminating on an unreachable
            # node. A second DS delete is a no-op once the foregroundDeletion finalizer is set, and
            # we have no patch RBAC to clear it. The manager DOES have pod-delete RBAC, so
            # force-delete the DS's child pods (grace 0) directly — that lets the GC controller see
            # zero dependents and clear the finalizer. Then wait a final window for the DS to vanish.
            self._force_delete_preheat_pods(image_id)
            for attempt in range(20):
                try:
                    self.apps_v1.read_namespaced_daemon_set(name=name, namespace=self.namespace)
                except ApiException as read_err:
                    if read_err.status == 404:
                        gone = True
                        break
                    raise
                time.sleep(1)
            if not gone:
                # The old DS still won't die (a child pod is wedged Terminating on an unreachable
                # node and we lack patch RBAC to strip its finalizer). Don't burn ~20s on 10 futile
                # 409 create attempts every call — surface a clear error so the caller/scheduler logs
                # it and moves on; the DS will clear once the node recovers or is drained.
                raise RuntimeError(
                    f"preheat DaemonSet {name} is stuck terminating (a pod is wedged on an "
                    f"unreachable node); skipping recreate until it clears")
        # Re-check row existence right before recreating: the delete+wait above can take ~100s, and a
        # concurrent admin_delete_image may have removed the catalog row (and its DS) during that
        # window. Recreating now would resurrect a DaemonSet for a deleted image. Abort instead — the
        # earlier check (before the wait) can't see a delete that lands mid-wait.
        if not store.image_row_exists(image_id):
            logger.info("preheat recreate aborted: image %s was deleted during teardown", image_id)
            self._delete_preheat_ds_unlocked(image_id)
            return
        for attempt in range(10):
            try:
                self.apps_v1.create_namespaced_daemon_set(namespace=self.namespace, body=body)
                return
            except ApiException as e:
                if e.status == 409 and attempt < 9:
                    time.sleep(2)
                    continue
                raise

    # Container waiting reasons that are only reachable AFTER the image is fully pulled: the runtime
    # got as far as creating/starting a container (and failed for a non-pull reason). A shell-less
    # image (distroless/scratch) with our `/bin/sh -c sleep infinity` command fails here with
    # CreateContainerError/RunContainerError — never running/terminated — but the bytes ARE on disk.
    _PREHEAT_POST_PULL_WAIT_REASONS = frozenset({
        "CreateContainerError", "RunContainerError", "CreateContainerConfigError",
        "CrashLoopBackOff", "PostStartHookError", "StartError",
    })

    @staticmethod
    def _preheat_pod_image_pulled(pod) -> bool:
        """True if this preheat pod's image has been pulled onto its node.

        Preheat's goal is the image ON DISK, not a running container. Signals that the image is
        present: the container is running, is/was terminated (started then exited), OR is waiting for
        a POST-PULL reason (CreateContainerError etc.) — all of which are only reachable once kubelet
        finished pulling. This correctly counts a shell-less image (distroless/scratch) whose exec of
        `/bin/sh` fails during container creation (CreateContainerError), which never yields a
        running/terminated state. It does NOT count a container waiting with a pull-phase reason
        (ContainerCreating/PodInitializing/ImagePullBackOff/ErrImagePull) — that is before the bytes
        land, so counting it would flip status to "ready" prematurely."""
        statuses = (pod.status.container_statuses or []) if pod.status else []
        if not statuses:
            return False
        for cs in statuses:
            state = cs.state
            last_state = getattr(cs, "last_state", None)
            waiting = getattr(state, "waiting", None)
            waiting_reason = (getattr(waiting, "reason", "") or "") if waiting is not None else ""
            pulled = (
                getattr(state, "running", None) is not None
                or getattr(state, "terminated", None) is not None
                or (last_state is not None and getattr(last_state, "terminated", None) is not None)
                or waiting_reason in K8sClient._PREHEAT_POST_PULL_WAIT_REASONS
            )
            if not pulled:
                return False
        return True

    def get_preheat_status(self, image_id: int, image: Optional[str] = None,
                           min_desired: int = 0) -> dict:
        """Return preheat progress for the DaemonSet.

        "Ready" = the image is PULLED on every scheduled node (not that the container runs), so a
        shell-less image whose `sleep infinity` exec fails still counts once its layers are down.
        Falls back to numberReady if pods can't be listed. Missing DS => pending. `min_desired`
        floors desired_count (used right after create, when the DS controller hasn't populated
        status.desiredNumberScheduled yet, to avoid a misleading 0/0 'pending')."""
        name = self._preheat_ds_name(image_id)
        try:
            ds = self.apps_v1.read_namespaced_daemon_set(name=name, namespace=self.namespace)
        except ApiException as e:
            if e.status == 404:
                return {"status": "pending", "desired_count": 0, "ready_count": 0,
                        "message": "preheat not started", "completed": False}
            raise
        st = ds.status
        desired = max(int(getattr(st, "desired_number_scheduled", 0) or 0), int(min_desired or 0))
        pulled = int(getattr(st, "number_ready", 0) or 0)
        try:
            pods = self.core_v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"oneclick-preheat-image-id={int(image_id)}",
            )
            pulled = sum(1 for p in pods.items if self._preheat_pod_image_pulled(p))
        except ApiException as e:
            logger.warning("Cannot list preheat pods for image %s; using numberReady: %s", image_id, e)
        if desired > 0 and pulled >= desired:
            status, completed = "ready", True
        elif desired == 0:
            status, completed = "pending", False
        else:
            status, completed = "pulling", False
        return {
            "status": status,
            "desired_count": desired,
            "ready_count": pulled,
            "message": f"{pulled}/{desired} nodes preheated",
            "completed": completed,
        }

    def sweep_orphan_preheat_ds(self, expected_image_ids) -> int:
        """Delete every preheat DaemonSet whose image_id is NOT in `expected_image_ids`.

        `expected_image_ids` must be the set of image_ids that SHOULD currently own a preheat DS —
        i.e. the live catalog rows whose source_type is 'harbor_mirror'. Anything else with a
        oneclick-preheat-* DS is orphaned and removed. This covers three cases with one rule:
          - the catalog row was deleted (delete/sync TOCTOU backstop),
          - the row's source_type was switched away from harbor_mirror (row still exists), and
          - the row is a mirror row but preheat was globally disabled (empty expected set).
        Returns the count deleted."""
        expected = {int(i) for i in expected_image_ids}
        removed = 0
        try:
            dss = self.apps_v1.list_namespaced_daemon_set(
                namespace=self.namespace, label_selector="app=oneclick-preheat")
        except ApiException as e:
            logger.warning("Cannot list preheat DaemonSets for orphan sweep: %s", e)
            return 0
        for ds in dss.items:
            labels = ds.metadata.labels or {}
            raw = labels.get("oneclick-preheat-image-id")
            try:
                image_id = int(raw)
            except (TypeError, ValueError):
                continue
            if image_id not in expected:
                if self.remove_preheat_ds(image_id):
                    removed += 1
        return removed

    def _preheat_ds_pinned_nodes(self, ds) -> set:
        """Extract the hostname set baked into a preheat DS's nodeAffinity In-list (or empty)."""
        try:
            terms = ds.spec.template.spec.affinity.node_affinity \
                .required_during_scheduling_ignored_during_execution.node_selector_terms
        except AttributeError:
            return set()
        for term in terms or []:
            for expr in term.match_expressions or []:
                if expr.key == "kubernetes.io/hostname" and expr.operator == "In":
                    return set(expr.values or [])
        return set()

    def _preheat_ds_image(self, ds) -> Optional[str]:
        """The container image the preheat DS is actually running (or None)."""
        try:
            return ds.spec.template.spec.containers[0].image
        except (AttributeError, IndexError):
            return None

    def reconcile_preheat_ds(self, image_id: int, image: str) -> dict:
        """Ensure the preheat DS for `image` targets the CURRENT eligible node set.

        Recreates the DS only when the eligible hostname set has drifted from what the DS pins
        (a new/reimaged node joined, or one dropped) — otherwise it's a cheap read. This is what
        the periodic scheduler calls so newly-eligible nodes get preheated without a manual sync,
        and so a stuck/stale DS self-heals. Returns the resulting preheat status."""
        image = (image or "").strip()
        if not image:
            return self.get_preheat_status(image_id)
        # raise_on_error: a transient 403 must not collapse `current` to empty and drive a
        # spurious drift-triggered teardown/recreate. Let it propagate so the periodic caller
        # (which swallows/logs) simply retries next tick, leaving the DS intact. Exclude quarantined
        # nodes here too so `current` matches what preheat_image_to_nodes would actually pin (else
        # drift is detected every tick and the DS is needlessly recreated).
        quarantined = store.quarantined_nodes()
        current = {t["node"] for t in self._eligible_target_nodes(raise_on_error=True)
                   if t["node"] not in quarantined}
        name = self._preheat_ds_name(image_id)
        try:
            ds = self.apps_v1.read_namespaced_daemon_set(name=name, namespace=self.namespace)
            pinned = self._preheat_ds_pinned_nodes(ds)
            ds_image = self._preheat_ds_image(ds)
        except ApiException as e:
            if e.status != 404:
                raise
            ds, pinned, ds_image = None, None, None
        # No drift only if BOTH the pinned node set AND the running image match the desired ones.
        # Checking the image too is essential: after an admin edits a row to a new image, if the
        # initial preheat call failed, the old DS (same fixed name, old image) lingers; without the
        # image check reconcile would report the OLD DS as "ready" for the NEW image and never fix it.
        if ds is not None and pinned == current and ds_image == image:
            return self.get_preheat_status(image_id, image)
        # Drifted (node set or image changed) or missing: re-drive preheat onto the current set.
        return self.preheat_image_to_nodes(image_id, image)

    def remove_preheat_ds(self, image_id: int) -> bool:
        """Delete the preheat DaemonSet (delete-parity). The image stays in Harbor.

        Deleting the DS stops PINNING the image on nodes but does NOT immediately free disk: the
        pulled layers linger in containerd's content store until kubelet's image GC evicts them
        opportunistically (only once node disk crosses imageGCHighThresholdPercent, ~85% default).
        Returns True if deleted, False if it did not exist.

        Does NOT hold the per-image preheat lock: a delete must stay responsive and must never wait
        out an in-flight _create_or_replace_preheat_ds's ~100s stuck-teardown loop for the same
        image_id. The DS delete is idempotent, and if a concurrent create finishes AFTER this delete
        (recreating the DS), the scheduler's orphan sweep removes it on the next tick (the row is
        gone by the time delete-parity calls this). So racing create/delete converges safely without
        serializing on the heavy create lock."""
        return self._delete_preheat_ds_unlocked(image_id)

    def _force_delete_preheat_pods(self, image_id: int) -> None:
        """Force-delete (grace 0) the child pods of a preheat DS so a stuck Foreground deletion can
        complete. The manager has pod-delete but not patch RBAC, so clearing the pods lets the GC
        controller drop the DS's foregroundDeletion finalizer even when a pod is wedged Terminating
        on an unreachable node. Best-effort; logs and swallows per-pod errors."""
        try:
            pods = self.core_v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"oneclick-preheat-image-id={int(image_id)}",
            )
        except ApiException as e:
            logger.warning("listing stuck preheat pods for image %s failed: %s", image_id, e)
            return
        for pod in pods.items:
            try:
                self.core_v1.delete_namespaced_pod(
                    name=pod.metadata.name, namespace=self.namespace,
                    grace_period_seconds=0,
                    body=client.V1DeleteOptions(grace_period_seconds=0, propagation_policy="Background"),
                )
            except ApiException as pod_err:
                if pod_err.status != 404:
                    logger.warning("force-delete preheat pod %s failed: %s", pod.metadata.name, pod_err)

    def _delete_preheat_ds_unlocked(self, image_id: int) -> bool:
        """Delete the preheat DS by name. Caller decides whether to hold the per-image lock.

        Issues a Foreground delete, then briefly waits; if the DS is still present (a child pod
        wedged Terminating on an unreachable node blocks GC), force-deletes the child pods so the
        finalizer can clear. Without this, delete-parity and the orphan sweep would return a false
        'deleted' and the DS would leak until the node recovers."""
        name = self._preheat_ds_name(image_id)
        try:
            self.apps_v1.delete_namespaced_daemon_set(
                name=name, namespace=self.namespace,
                body=client.V1DeleteOptions(propagation_policy="Foreground"),
            )
        except ApiException as e:
            if e.status == 404:
                return False
            raise
        # Short wait for normal teardown; if it lingers, force-delete the child pods to unblock GC,
        # then poll again so we don't return a false 'deleted' while the DS is still present.
        for attempt in range(5):
            try:
                self.apps_v1.read_namespaced_daemon_set(name=name, namespace=self.namespace)
            except ApiException as read_err:
                if read_err.status == 404:
                    return True
                raise
            time.sleep(1)
        self._force_delete_preheat_pods(image_id)
        for attempt in range(20):
            try:
                self.apps_v1.read_namespaced_daemon_set(name=name, namespace=self.namespace)
            except ApiException as read_err:
                if read_err.status == 404:
                    return True
                raise
            time.sleep(1)
        # Still present (wedged pod / GC lag). Report not-fully-deleted so callers/sweep don't treat
        # it as reclaimed; the next sweep tick will retry.
        logger.warning("preheat DaemonSet %s still present after force-delete; will retry next sweep", name)
        return False


    def reconcile_image_ready_labels(self, catalog: list[dict]) -> dict:
        """Project per-image prepull warmth onto node labels so the pod-level
        soft affinity can steer scheduling toward already-warmed nodes.

        `catalog` is a list of {"id", "image"} dicts. For each node we add the
        image-ready label where the image is warm and remove it where it is not.
        Requires nodes:patch RBAC; failures are logged and skipped.
        """
        if not settings.IMAGE_AFFINITY_ENABLED:
            return {"updated_nodes": 0, "images": 0}
        try:
            nodes = self.core_v1.list_node()
        except ApiException as e:
            logger.warning("reconcile_image_ready_labels: cannot list nodes: %s", e)
            return {"updated_nodes": 0, "images": 0, "error": str(e)}

        # Desired label -> set(node names) it should be present on.
        desired_by_key: dict[str, set[str]] = {}
        managed_keys: set[str] = set()
        for entry in catalog:
            image_ref = entry.get("image")
            image_id = entry.get("id")
            if not image_ref or image_id is None:
                continue
            key = self._image_ready_label_key(image_ref)
            managed_keys.add(key)
            try:
                desired_by_key[key] = set(store.list_nodes_for_image(image_ref))
            except Exception as e:
                logger.warning("reconcile_image_ready_labels: pulled nodes lookup failed for image %s: %s", image_id, e)
                desired_by_key[key] = set()

        updated = 0
        for node in nodes.items:
            node_name = node.metadata.name
            current = node.metadata.labels or {}
            patch_labels: dict[str, Optional[str]] = {}
            for key in managed_keys:
                should_have = node_name in desired_by_key.get(key, set())
                has = current.get(key) == "true"
                if should_have and not has:
                    patch_labels[key] = "true"
                elif has and not should_have:
                    patch_labels[key] = None  # merge-patch null removes the label
            if patch_labels:
                try:
                    self.core_v1.patch_node(node_name, {"metadata": {"labels": patch_labels}})
                    updated += 1
                except ApiException as e:
                    logger.warning("reconcile_image_ready_labels: patch node %s failed: %s", node_name, e)
        return {"updated_nodes": updated, "images": len(managed_keys)}

    def delete_image_sync(self, image_id: int):
        # Image-service mode: there is no prepull DaemonSet/probe to tear down. Node
        # eviction (image_nodes cleanup + ssh ctr rm) is orchestrated by the DELETE
        # handler via an `evict` job, so this is a no-op kept for call-site stability.
        return

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
                        ssh_public_key: Optional[str] = None,
                        pod_type: Optional[str] = None,
                        api_launched: bool = False,
                        git_token: Optional[str] = None) -> dict:
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
            # A pod already exists for this (per-user) instance_id. Only reuse it
            # if it matches the requested launch (idempotent double-submit of the
            # SAME launch). If it differs (e.g. the user picked a different
            # template/type, or it is a stale pod left over from a delete that
            # did not fully sync), REPLACE it — otherwise we would silently hand
            # back the old instance instead of the one the user just requested.
            try:
                existing_image = existing_pod.spec.containers[0].image
            except Exception:
                existing_image = None
            existing_type = (existing_pod.metadata.annotations or {}).get("amd-oneclick/instance-type", "jupyter")
            if existing_image == image and existing_type == instance_type:
                logger.info("Reusing matching existing pod %s (image/type identical)", instance_id)
                return self.get_instance_by_id(instance_id)
            logger.info(
                "Existing pod %s differs from requested launch (image %s->%s, type %s->%s); replacing",
                instance_id, existing_image, image, existing_type, instance_type,
            )
            # Issue the delete without a long blocking wait; the terminating-wait
            # loop below (breaks on 404) handles confirming removal before we
            # recreate the pod with the requested spec.
            self.delete_instance_by_id(instance_id, wait=False)
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
        # Two-tier localcache: ensure the durable shards exist (idempotent; startup already does this,
        # this covers a shard that failed to bind then), and look up the instance's last node for the
        # warm-cache soft affinity. Best-effort: never block a launch on either.
        workspace_last_node = None
        if self._workspace_localcache_enabled():
            try:
                # Launch hot path: create-if-missing only, no Bound-poll (startup already bound them
                # and caches _durable_shards_ready). Keeps launches off the minute-long poll loop.
                self._ensure_durable_shards(wait_bound=False)
            except Exception as e:
                logger.error("ensure durable shards at launch failed for %s (continuing): %s", instance_id, e)
            try:
                workspace_last_node = store.get_workspace_last_node(instance_id)
            except Exception as e:
                logger.debug("workspace last-node lookup failed for %s: %s", instance_id, e)
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
            pod_type=pod_type,
            api_launched=api_launched,
            workspace_last_node=workspace_last_node,
            git_token=git_token,
        )
        pod_uid = None
        for attempt in range(1, 7):
            try:
                created_pod = self.core_v1.create_namespaced_pod(
                    namespace=self.namespace,
                    body=pod_manifest
                )
                pod_uid = created_pod.metadata.uid
                logger.info(f"Created pod {instance_id} for {email} (type={instance_type})")
                # Two-tier localcache: mark running so the delayed-local-delete reaper won't reap the
                # SSD copy while the pod is alive. The node it actually lands on is captured at delete
                # time (soft-affinity means it usually returns to workspace_last_node anyway).
                if self._workspace_localcache_enabled():
                    try:
                        store.stamp_workspace_running(instance_id, notebook_node_name)
                    except Exception as e:
                        logger.debug("stamp_workspace_running failed for %s: %s", instance_id, e)
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
                owner_uid=pod_uid,
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
    
    def _pod_exists(self, instance_id: str) -> bool:
        try:
            self.core_v1.read_namespaced_pod(name=instance_id, namespace=self.namespace)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            raise

    def invalidate_service_ip(self, instance_id: str) -> None:
        """Drop cached ClusterIP + bump epoch so any in-flight resolve cannot repopulate it.

        The epoch must stay monotonic (never reset) so a slow read that snapshotted an
        older generation can never match again and re-cache a stale IP. The epoch map is
        naturally bounded: instance_ids are deterministic per user (u-<uid>-<hash>), so its
        size tracks the distinct-user count, not request volume.
        """
        with self._svc_ip_lock:
            self._svc_ip_cache.pop(instance_id, None)
            self._svc_ip_epoch[instance_id] = self._svc_ip_epoch.get(instance_id, 0) + 1

    def resolve_service_ip(self, instance_id: str):
        """Return the ClusterIP for an instance Service, cached for a short TTL.

        Blocking (reads the Service on a miss) -- callers on the event loop must
        dispatch via asyncio.to_thread. Returns None if the Service is absent. The lock
        makes the snapshot-epoch and the compare-then-write atomic against concurrent
        invalidation from delete paths / other worker threads (the blocking k8s read
        itself runs OUTSIDE the lock so it never serializes proxy traffic).
        """
        import time as _time
        with self._svc_ip_lock:
            hit = self._svc_ip_cache.get(instance_id)
            if hit and hit[1] > _time.monotonic():
                return hit[0]
            epoch_at_read = self._svc_ip_epoch.get(instance_id, 0)
        try:
            svc = self.core_v1.read_namespaced_service(
                name=f"{instance_id}-svc", namespace=self.namespace)
        except Exception:
            return None
        ip = svc.spec.cluster_ip
        with self._svc_ip_lock:
            if self._svc_ip_epoch.get(instance_id, 0) == epoch_at_read:
                self._svc_ip_cache[instance_id] = (ip, _time.monotonic() + self._svc_ip_ttl)
        return ip

    def _delete_service(self, instance_id: str, grace_period_seconds: Optional[int] = None):
        self.invalidate_service_ip(instance_id)
        body = None
        if grace_period_seconds is not None:
            body = client.V1DeleteOptions(grace_period_seconds=grace_period_seconds, propagation_policy="Background")
        else:
            body = client.V1DeleteOptions(propagation_policy="Background")
        try:
            self.core_v1.delete_namespaced_service(name=f"{instance_id}-svc", namespace=self.namespace, body=body)
            logger.info("Requested delete of service %s-svc", instance_id)
        except ApiException as e:
            if e.status != 404:
                logger.warning("Error deleting service %s-svc: %s", instance_id, e)

    def _delete_pod(self, instance_id: str, grace_period_seconds: Optional[int] = None):
        body = client.V1DeleteOptions(propagation_policy="Background")
        if grace_period_seconds is not None:
            body.grace_period_seconds = grace_period_seconds
        try:
            self.core_v1.delete_namespaced_pod(name=instance_id, namespace=self.namespace, body=body)
            logger.info("Requested delete of pod %s (grace=%s)", instance_id, grace_period_seconds)
        except ApiException as e:
            if e.status != 404:
                logger.warning("Error deleting pod %s: %s", instance_id, e)

    def delete_instance_by_id(self, instance_id: str, wait: bool = True) -> bool:
        """Authoritatively delete a notebook instance.

        Issues a graceful delete of the Service and Pod, then polls until the
        Pod is truly gone. If the Pod is still present after
        DELETE_CONFIRM_TIMEOUT_SECONDS (e.g. wedged on a NotReady node or a
        finalizer), it escalates to a force delete (grace period 0).

        Returns True only when the Pod is confirmed absent, so callers can rely
        on it before marking the instance deleted in the database. A stuck Pod
        returns False and is left for the reconciler to finalize, keeping the DB
        consistent with cluster reality.
        """
        # Two-tier localcache: BEFORE deleting, record the node the pod ran on + stop time. The flush
        # (local -> durable) is NOT an in-pod preStop hook anymore — it runs out-of-pod AFTER the pod
        # is confirmed gone (see below), driven by the manager against the host-resident SSD copy. The
        # stamp lets a fast relaunch soft-affine back to this node's warm cache and lets the delayed
        # reaper free the local SSD copy after the TTL (only once the flush has confirmed). The local
        # copy is intentionally kept here (not deleted) — durable persists forever regardless.
        is_localcache = self._workspace_localcache_enabled()
        node_name = None
        session_token = None
        if is_localcache:
            try:
                pod = self.core_v1.read_namespaced_pod(name=instance_id, namespace=self.namespace)
                node_name = getattr(pod.spec, "node_name", None)
            except ApiException as e:
                if e.status != 404:
                    logger.debug("could not read node for %s before delete: %s", instance_id, e)
            try:
                # Records the (instance, node) local-copy row + returns its session fence token.
                session_token = store.stamp_workspace_stopped(instance_id, node_name)
            except Exception as e:
                logger.debug("stamp_workspace_stopped failed for %s: %s", instance_id, e)

        self._delete_service(instance_id)
        self._delete_pod(instance_id)

        if not wait:
            gone = not self._pod_exists(instance_id)
            # Even on the fire-and-forget path, kick the flush if the pod is already gone; otherwise the
            # reconciler retry sweep will pick up the unflushed copy row later.
            if gone:
                self._flush_after_delete(instance_id, node_name, session_token, is_localcache)
            return gone

        # No in-pod flush to wait out anymore — the out-of-pod flush reads the host SSD copy after the
        # pod is gone, so the normal fast confirm window applies to both localcache and legacy.
        confirm_window = settings.DELETE_CONFIRM_TIMEOUT_SECONDS
        deadline = time.monotonic() + max(0, confirm_window)
        interval = max(0.5, settings.DELETE_POLL_INTERVAL_SECONDS)
        while time.monotonic() < deadline:
            if not self._pod_exists(instance_id):
                logger.info("Confirmed pod %s is gone", instance_id)
                self._flush_after_delete(instance_id, node_name, session_token, is_localcache)
                return True
            time.sleep(interval)

        # Escalate: force delete and give it one more short confirmation window.
        logger.warning("Pod %s still present after graceful delete; force deleting", instance_id)
        self._delete_pod(instance_id, grace_period_seconds=0)
        force_deadline = time.monotonic() + max(5, int(interval * 5))
        while time.monotonic() < force_deadline:
            if not self._pod_exists(instance_id):
                logger.info("Confirmed pod %s is gone after force delete", instance_id)
                self._flush_after_delete(instance_id, node_name, session_token, is_localcache)
                return True
            time.sleep(interval)

        logger.error("Pod %s still present after force delete; leaving for reconciler", instance_id)
        return False

    def _flush_after_delete(self, instance_id: str, node_name: Optional[str],
                            session_token: Optional[str], is_localcache: bool):
        """Run the out-of-pod local->durable flush once a delete has confirmed the pod is gone.
        Best-effort: a failed flush leaves the local-copy row's flushed_at NULL, so (a) the reaper
        won't reap the unflushed SSD copy and (b) the reconciler sweep retries it later. Never raises
        — a flush hiccup must not turn a successful delete into a failure."""
        if not is_localcache or not node_name or not session_token:
            return
        try:
            self._flush_workspace_to_durable(instance_id, node_name, session_token)
        except Exception as e:
            logger.error("post-delete flush for %s on %s failed (will retry via reconciler): %s",
                         instance_id, node_name, e)
    
    def reap_local_workspace_caches(self) -> list:
        """Delayed-local-delete: free node-local SSD copies of instances stopped longer than
        WORKSPACE_LOCAL_CACHE_TTL_MINUTES (durable NFS copy untouched). Skips any instance whose
        pod is currently present (a relaunch within the window). Returns the reaped instance ids."""
        if not self._workspace_localcache_enabled():
            return []
        reaped = []
        try:
            # Each candidate is a (instance, node) copy that is past its TTL AND confirmed-flushed.
            candidates = store.list_workspace_cache_to_reap(settings.WORKSPACE_LOCAL_CACHE_TTL_MINUTES)
        except Exception as e:
            logger.error("reap_local_workspace_caches: candidate lookup failed: %s", e)
            return []
        for row in candidates:
            instance_id = row["instance_id"]
            node_name = row.get("node_name")
            if not node_name:
                continue
            # A live pod ON THIS NODE means the instance was relaunched here and /workspace is the
            # ACTIVE working set — never rm it. (A live pod on a DIFFERENT node means THIS node's copy
            # is genuinely a stopped, flushed leftover and is safe to reap.) _cleanup_local_workspace_
            # cache also re-checks pod-on-same-node as a TOCTOU guard, but skip early to avoid the op.
            if self._pod_exists(instance_id):
                live_node = None
                try:
                    pod = self.core_v1.read_namespaced_pod(name=instance_id, namespace=self.namespace)
                    live_node = getattr(pod.spec, "node_name", None)
                except ApiException:
                    pass
                if live_node == node_name:
                    continue
            if self._cleanup_local_workspace_cache(instance_id, node_name):
                # Drop this (instance, node) copy row. Leave cache_state (affinity hint) alone — it is
                # one bounded row per instance and gets overwritten on the next stop.
                try:
                    store.clear_local_copy_node(instance_id, node_name)
                except Exception as e:
                    logger.debug("clear_local_copy_node failed for %s/%s: %s", instance_id, node_name, e)
                reaped.append(f"{instance_id}@{node_name}")
        if reaped:
            logger.info("Reaped %s local workspace cache(s): %s", len(reaped), reaped)
        return reaped

    def retry_unflushed_workspaces(self) -> list:
        """Reconciler retry sweep: re-run the out-of-pod flush for every (instance, node) local copy
        whose flush never confirmed (flushed_at NULL). Fallback for the single-node flush pin: if the
        node was NotReady when delete_instance_by_id fired the flush (or the pod was lost out-of-band
        before any delete), the flush couldn't run and the copy is unflushed. Once the node is Ready
        again this retries it, fenced by the copy's session_token so it marks exactly that session.
        Returns the "instance@node" keys successfully flushed this sweep."""
        if not self._workspace_localcache_enabled():
            return []
        try:
            rows = store.list_workspace_unflushed_copies()
        except Exception as e:
            logger.error("retry_unflushed_workspaces: lookup failed: %s", e)
            return []
        # Only flush onto Ready nodes — a pin to a NotReady node just wastes the start_deadline.
        ready_nodes = set()
        try:
            for node in self._list_node_cached().items:
                conds = {c.type: c.status for c in (node.status.conditions or [])}
                if conds.get("Ready") == "True":
                    ready_nodes.add(node.metadata.name)
        except ApiException as e:
            logger.debug("retry_unflushed_workspaces: node listing failed: %s", e)
            return []
        flushed = []
        for row in rows:
            instance_id = row["instance_id"]
            node_name = row.get("node_name")
            session_token = row.get("session_token")
            if not node_name or node_name not in ready_nodes or not session_token:
                continue
            # A live pod ON THIS NODE means the instance was relaunched here; its /workspace is the
            # active working set and its hydrate-init may be rsyncing — don't flush over it (the flush
            # method also guards this, but skip early). A live pod on ANOTHER node doesn't affect this
            # node's stopped copy.
            if self._pod_exists(instance_id):
                live_node = None
                try:
                    pod = self.core_v1.read_namespaced_pod(name=instance_id, namespace=self.namespace)
                    live_node = getattr(pod.spec, "node_name", None)
                except ApiException:
                    pass
                if live_node == node_name:
                    continue
            try:
                if self._flush_workspace_to_durable(instance_id, node_name, session_token):
                    flushed.append(f"{instance_id}@{node_name}")
            except Exception as e:
                logger.error("retry flush for %s on %s failed: %s", instance_id, node_name, e)
        if flushed:
            logger.info("Reconciler flushed %s previously-unflushed workspace(s): %s", len(flushed), flushed)
        return flushed

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
                    "pod_type": pod.metadata.annotations.get("amd-oneclick/pod-type"),
                    "api_launched": pod.metadata.annotations.get("amd-oneclick/api-launched") == "true",
                    "node_name": getattr(pod.spec, "node_name", None),
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

    def list_managed_pod_states(self) -> list[dict]:
        """Lightweight lifecycle view of every pod carrying our app label.

        Used by the reconciler to reconcile cluster truth against the DB
        (orphan detection, stuck-Terminating detection) without the per-pod
        Service reads that list_instances() performs.
        """
        states: list[dict] = []
        now = datetime.now(timezone.utc)
        # Intentionally NOT swallowing errors: a failed listing must propagate so
        # the reconciler aborts the cycle rather than mistaking an API failure
        # for "no pods exist" and marking every instance deleted.
        pods = self.core_v1.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=f"app={settings.NOTEBOOK_LABEL_PREFIX}",
        )
        for pod in pods.items:
            instance_id = (pod.metadata.labels or {}).get("instance-id") or pod.metadata.name
            created = pod.metadata.creation_timestamp
            age = int((now - created.replace(tzinfo=timezone.utc)).total_seconds()) if created else 0
            deletion_ts = pod.metadata.deletion_timestamp
            terminating_seconds = None
            if deletion_ts:
                terminating_seconds = int((now - deletion_ts.replace(tzinfo=timezone.utc)).total_seconds())
            # Container-level signals for terminal / broken detection.
            restart_count = 0
            waiting_reason = ""
            for cs in (pod.status.container_statuses or []):
                restart_count += int(cs.restart_count or 0)
                w = getattr(cs.state, "waiting", None) if cs.state else None
                if w and w.reason:
                    waiting_reason = w.reason
            states.append({
                "instance_id": instance_id,
                "pod_name": pod.metadata.name,
                # node the pod is bound to — used by wedge detection to aggregate stuck pods per node.
                "node_name": getattr(pod.spec, "node_name", None),
                "email": (pod.metadata.annotations or {}).get("amd-oneclick/email", "unknown"),
                "phase": (pod.status.phase or "unknown").lower(),
                "terminating": deletion_ts is not None,
                "terminating_seconds": terminating_seconds,
                "age_seconds": age,
                "restart_count": restart_count,
                "waiting_reason": waiting_reason,
            })
        return states
    
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

            # A pod being deleted keeps reporting its last phase (often Running) and its container
            # may still show ready=True until the kubelet finishes teardown — so without this guard
            # a Terminating pod is indistinguishable from a healthy one and the UI shows "ready".
            # Short-circuit to a terminating status so the delete never looks like a no-op.
            if pod.metadata.deletion_timestamp is not None:
                return {
                    "status": "terminating",
                    "phase": (pod.status.phase or "unknown").lower(),
                    "reason": "Terminating",
                    "message": "Instance is shutting down",
                    "ready": False,
                    "pod_scheduled": True,
                    "jupyter_ready": False,
                }

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

    def is_pod_port_live(self, instance_id: str, port: int, timeout: float = 0.5) -> bool:
        """True if something is accepting TCP connections on the pod's <port> right now.

        Used to detect a user-started app (e.g. `streamlit run` on the curated 8501 port)
        that the manager never launched itself, so there is no readiness probe for it —
        this is a point-in-time check, not a cached/gated status. Short default timeout
        since this is called from a request path (status polling) and a closed port
        returns immediately (RST); the timeout only bounds a dropped-packet edge case.
        """
        try:
            pod = self.core_v1.read_namespaced_pod(name=instance_id, namespace=self.namespace)
        except ApiException:
            return False
        pod_ip = pod.status.pod_ip if pod and pod.status else None
        if not pod_ip:
            return False
        return self._check_tcp_ready(pod_ip, port, timeout=timeout)

    def check_pod_activity(self, email: str, instance_id: Optional[str] = None) -> Optional[datetime]:
        """Check last activity of a pod by examining logs.

        Pods launched via the API use custom instance IDs (e.g. hf-<id>-<hash>) that do not match
        _generate_instance_id(email), so callers must pass instance_id to target the right pod.

        Returns None ONLY when the pod has no parseable activity timestamp. A transient kube API
        error (read_namespaced_pod_log raising) is re-raised so the caller can tell "no activity"
        apart from "could not check", and avoid reaping an active pod on a flaky tick.
        """
        if not instance_id:
            instance_id = self._generate_instance_id(email)

        # Get recent logs (ApiException propagates: "unknown", not "idle").
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
    
    def cleanup_idle_instances(self) -> list:
        """Cleanup idle and expired instances.

        Targets pods by their real instance id (from list_instances), so custom-id pods
        (hf-*, u-*) are matched. API-launched pods follow the longer API idle/lifetime budget;
        all other pods keep their existing per-type behavior.
        """
        cleaned = []
        instances = self.list_instances()
        now = datetime.now(timezone.utc)

        for instance in instances:
            should_delete = False
            reason = ""
            instance_id = instance.get("id")
            if not instance_id or instance_id == "unknown":
                continue

            if instance.get("api_launched"):
                idle_timeout = settings.API_IDLE_TIMEOUT_MINUTES
                max_lifetime = settings.API_MAX_LIFETIME_HOURS or settings.MAX_LIFETIME_HOURS
            else:
                itype = instance.get("instance_type", "jupyter")
                type_cfg = INSTANCE_TYPES.get(itype, {})
                raw_lifetime = type_cfg.get("max_lifetime_hours")
                max_lifetime = raw_lifetime if raw_lifetime is not None else settings.MAX_LIFETIME_HOURS
                raw_idle = type_cfg.get("idle_timeout_minutes")
                idle_timeout = raw_idle if raw_idle is not None else settings.IDLE_TIMEOUT_MINUTES

            uptime_minutes = instance["uptime_minutes"]
            if max_lifetime and uptime_minutes / 60 >= max_lifetime:
                should_delete = True
                reason = f"exceeded max lifetime ({max_lifetime}h)"

            elif instance["status"] == "running" and idle_timeout and idle_timeout > 0:
                try:
                    last_activity = self.check_pod_activity(instance["email"], instance_id=instance_id)
                except ApiException as e:
                    # Could not read logs this tick; treat as "unknown" and skip, so a flaky
                    # API call never reaps an active pod via the uptime fallback.
                    logger.debug("check_pod_activity failed for %s; skipping idle check: %s", instance_id, e)
                    continue
                if last_activity:
                    idle_minutes = (now - last_activity).total_seconds() / 60
                    if idle_minutes >= idle_timeout:
                        should_delete = True
                        reason = f"idle for {int(idle_minutes)} minutes (limit {idle_timeout}m)"
                elif instance.get("api_launched") and uptime_minutes >= idle_timeout:
                    # No parseable log timestamps: fall back to pod age as the idle proxy.
                    should_delete = True
                    reason = f"no activity logs; up {int(uptime_minutes)} minutes (limit {idle_timeout}m)"

            if should_delete:
                if self.delete_instance_by_id(instance_id):
                    store.mark_instance_deleted(instance_id)
                    cleaned.append({
                        "email": instance["email"],
                        "instance_id": instance_id,
                        "reason": reason,
                    })
                    logger.info("Cleaned up instance %s for %s: %s", instance_id, instance["email"], reason)

        return cleaned


# Global K8s client instance
k8s_client = K8sClient()
