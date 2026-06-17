import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from tests.kube_stub import install

install()

from kubernetes.client.rest import ApiException

from app import k8s_client as k8s_module


class FakeCoreV1:
    def __init__(self):
        self.created_ports = []

    def list_service_for_all_namespaces(self):
        raise AssertionError("cluster-wide service scan should be disabled")

    def read_namespaced_service(self, name, namespace):
        raise ApiException(status=404, reason="Not Found")

    def create_namespaced_service(self, namespace, body):
        node_port = body["spec"]["ports"][0]["nodePort"]
        self.created_ports.append(node_port)
        if node_port == 32500:
            raise ApiException(status=422, reason="provided port is already allocated")
        return SimpleNamespace()


class NodePortAllocationTests(unittest.TestCase):
    def setUp(self):
        self.original = (
            k8s_module.settings.NODE_PORT_BASE,
            k8s_module.settings.NODE_PORT_MAX,
            k8s_module.settings.NODE_PORT_CLUSTER_SCAN_ENABLED,
        )
        k8s_module.settings.NODE_PORT_BASE = 32500
        k8s_module.settings.NODE_PORT_MAX = 32699
        k8s_module.settings.NODE_PORT_CLUSTER_SCAN_ENABLED = False

    def tearDown(self):
        (
            k8s_module.settings.NODE_PORT_BASE,
            k8s_module.settings.NODE_PORT_MAX,
            k8s_module.settings.NODE_PORT_CLUSTER_SCAN_ENABLED,
        ) = self.original

    def test_used_node_ports_does_not_scan_cluster_when_disabled(self):
        client = object.__new__(k8s_module.K8sClient)
        client.core_v1 = FakeCoreV1()

        self.assertEqual(client._used_node_ports(), set())

    def test_create_service_retries_conflict_without_cluster_scan(self):
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeCoreV1()

        with patch.object(k8s_module.random, "randint", return_value=0):
            node_port, opencode_node_port, created = client._create_service_with_nodeport_retry(
                "hf-user@example.test",
                "hf-demo",
            )

        # First attempt allocates the pair (32500, 32501); the stub rejects the
        # jupyter port 32500 with a 422, so the retry adds both to the used set
        # and reallocates the next free pair (32502, 32503).
        self.assertTrue(created)
        self.assertEqual(node_port, 32502)
        self.assertEqual(opencode_node_port, 32503)
        self.assertEqual(client.core_v1.created_ports, [32500, 32502])

    def test_allocation_respects_configured_upper_bound(self):
        client = object.__new__(k8s_module.K8sClient)
        k8s_module.settings.NODE_PORT_BASE = 32500
        k8s_module.settings.NODE_PORT_MAX = 32501

        self.assertEqual(client._allocate_node_port({32500}, start_port=32500), 32501)
        with self.assertRaises(RuntimeError):
            client._allocate_node_port({32500, 32501}, start_port=32500)


class NotebookNodePinningTests(unittest.TestCase):
    def setUp(self):
        self.original = (
            k8s_module.settings.IMAGE_PULL_SECRET_NAME,
            k8s_module.settings.NOTEBOOK_NODE_NAME,
            k8s_module.settings.NOTEBOOK_TOLERATION_KEY,
            k8s_module.settings.NOTEBOOK_TOLERATION_VALUE,
            k8s_module.settings.NOTEBOOK_TOLERATION_EFFECT,
            k8s_module.settings.WORKSPACE_QUOTA_ENABLED,
            k8s_module.settings.WORKSPACE_QUOTA_NODE_NAME,
        )

    def tearDown(self):
        (
            k8s_module.settings.IMAGE_PULL_SECRET_NAME,
            k8s_module.settings.NOTEBOOK_NODE_NAME,
            k8s_module.settings.NOTEBOOK_TOLERATION_KEY,
            k8s_module.settings.NOTEBOOK_TOLERATION_VALUE,
            k8s_module.settings.NOTEBOOK_TOLERATION_EFFECT,
            k8s_module.settings.WORKSPACE_QUOTA_ENABLED,
            k8s_module.settings.WORKSPACE_QUOTA_NODE_NAME,
        ) = self.original

    def test_notebook_node_name_pins_pod_without_workspace_quota(self):
        k8s_module.settings.NOTEBOOK_NODE_NAME = "wx-ms-w7900d-0044"
        k8s_module.settings.IMAGE_PULL_SECRET_NAME = ""
        k8s_module.settings.WORKSPACE_QUOTA_ENABLED = False
        k8s_module.settings.WORKSPACE_QUOTA_NODE_NAME = ""
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"

        manifest = client._get_pod_manifest(
            "hf-user@example.test",
            "hf-demo",
            "notebook-image",
            notebook_node_name=client._resolve_notebook_node_name(None),
        )

        self.assertEqual(manifest["spec"]["nodeName"], "wx-ms-w7900d-0044")
        self.assertEqual(
            manifest["metadata"]["annotations"]["amd-oneclick/notebook-node"],
            "wx-ms-w7900d-0044",
        )
        self.assertNotIn("amd-oneclick/workspace-quota", manifest["metadata"]["annotations"])

    def test_image_pull_secret_config_is_added(self):
        k8s_module.settings.IMAGE_PULL_SECRET_NAME = "amd-oneclick-radeon-beta-regcred"
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"

        manifest = client._get_pod_manifest(
            "hf-user@example.test",
            "hf-demo",
            "notebook-image",
        )

        self.assertEqual(
            manifest["spec"]["imagePullSecrets"],
            [{"name": "amd-oneclick-radeon-beta-regcred"}],
        )

    def test_notebook_toleration_config_is_added(self):
        k8s_module.settings.NOTEBOOK_TOLERATION_KEY = "amd-oneclick/beta"
        k8s_module.settings.NOTEBOOK_TOLERATION_VALUE = "radeon"
        k8s_module.settings.NOTEBOOK_TOLERATION_EFFECT = "NoSchedule"
        client = object.__new__(k8s_module.K8sClient)

        tolerations = client._notebook_tolerations()

        self.assertIn(
            {
                "key": "amd-oneclick/beta",
                "operator": "Equal",
                "value": "radeon",
                "effect": "NoSchedule",
            },
            tolerations,
        )

    def test_conflicting_notebook_and_quota_nodes_fail(self):
        k8s_module.settings.NOTEBOOK_NODE_NAME = "wx-ms-w7900d-0044"
        k8s_module.settings.WORKSPACE_QUOTA_ENABLED = True
        k8s_module.settings.WORKSPACE_QUOTA_NODE_NAME = "wx-ms-w7900d-0042"
        client = object.__new__(k8s_module.K8sClient)

        with self.assertRaises(RuntimeError):
            client._resolve_notebook_node_name("wx-ms-w7900d-0042")


class HuggingFaceEndpointTests(unittest.TestCase):
    def setUp(self):
        self.original = (
            k8s_module.settings.HF_ENDPOINT,
            k8s_module.settings.HF_TOKEN,
            k8s_module.settings.HF_TOKEN_SECRET_NAME,
            k8s_module.settings.HF_TOKEN_SECRET_KEY,
        )

    def tearDown(self):
        (
            k8s_module.settings.HF_ENDPOINT,
            k8s_module.settings.HF_TOKEN,
            k8s_module.settings.HF_TOKEN_SECRET_NAME,
            k8s_module.settings.HF_TOKEN_SECRET_KEY,
        ) = self.original

    def test_huggingface_download_url_uses_configured_endpoint(self):
        k8s_module.settings.HF_ENDPOINT = "http://134.199.133.77"
        client = object.__new__(k8s_module.K8sClient)

        self.assertEqual(
            client._notebook_download_url("https://huggingface.co/Qwen/Qwen3.6-27B.ipynb"),
            "http://134.199.133.77/Qwen/Qwen3.6-27B.ipynb",
        )
        self.assertEqual(
            client._notebook_download_url("https://huggingface.co/org/model/resolve/main/notebooks/demo.ipynb"),
            "http://134.199.133.77/org/model/resolve/main/notebooks/demo.ipynb",
        )

    def test_non_huggingface_download_url_is_not_rewritten(self):
        k8s_module.settings.HF_ENDPOINT = "http://134.199.133.77"
        client = object.__new__(k8s_module.K8sClient)

        self.assertEqual(
            client._notebook_download_url("https://raw.githubusercontent.com/org/repo/main/demo.ipynb"),
            "https://raw.githubusercontent.com/org/repo/main/demo.ipynb",
        )

    def test_notebook_pod_exposes_huggingface_endpoint(self):
        k8s_module.settings.HF_ENDPOINT = "http://134.199.133.77"
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"

        manifest = client._get_pod_manifest(
            "hf-user@example.test",
            "hf-demo",
            "notebook-image",
        )
        env = manifest["spec"]["containers"][0]["env"]

        self.assertIn({"name": "HF_ENDPOINT", "value": "http://134.199.133.77"}, env)

    def test_notebook_pod_reads_huggingface_token_from_secret(self):
        k8s_module.settings.HF_TOKEN_SECRET_NAME = "amd-oneclick-radeon-beta-secrets"
        k8s_module.settings.HF_TOKEN_SECRET_KEY = "HF_TOKEN"
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"

        manifest = client._get_pod_manifest(
            "hf-user@example.test",
            "hf-demo",
            "notebook-image",
        )
        env = manifest["spec"]["containers"][0]["env"]

        self.assertIn(
            {
                "name": "HF_TOKEN",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": "amd-oneclick-radeon-beta-secrets",
                        "key": "HF_TOKEN",
                        "optional": True,
                    }
                },
            },
            env,
        )

    def test_huggingface_download_script_sends_token_when_available(self):
        client = object.__new__(k8s_module.K8sClient)

        script = client._build_startup_script(
            "hf-demo",
            github_info={
                "path": "Qwen3.6-27B.ipynb",
                "raw_url": "https://huggingface.co/Qwen/Qwen3.6-27B.ipynb",
            },
        )

        self.assertIn('Authorization: Bearer ${HF_TOKEN}', script)
        self.assertIn('download_notebook Qwen3.6-27B.ipynb', script)


class FakeAppsV1:
    def __init__(self):
        self.daemonset_body = None
        self.deleted_daemonsets = []

    def create_namespaced_daemon_set(self, namespace, body):
        self.daemonset_body = body
        return SimpleNamespace()

    def read_namespaced_daemon_set(self, name, namespace):
        raise ApiException(status=404, reason="Not Found")

    def delete_namespaced_daemon_set(self, name, namespace):
        self.deleted_daemonsets.append(name)
        raise ApiException(status=404, reason="Not Found")


def node_with_conditions(ready="True", disk_pressure="False", images=None):
    return SimpleNamespace(
        status=SimpleNamespace(
            conditions=[
                SimpleNamespace(type="Ready", status=ready, reason="KubeletReady", message="ready"),
                SimpleNamespace(type="DiskPressure", status=disk_pressure, reason="KubeletHasNoDiskPressure", message="disk ok"),
            ],
            images=images or [],
        )
    )


def pull_probe_pod(image_id=1, phase="Pending", labels=None, container_statuses=None,
                   reason="", message="", name=None,
                   deletion_timestamp=None, start_time=None, creation_timestamp=None):
    labels = labels if labels is not None else {
        "app": "amd-oneclick-image-pull-check",
        "managed-by": "amd-oneclick-manager",
        "image-id": str(image_id),
    }
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name or f"image-pull-catalog-{image_id}",
            labels=labels,
            deletion_timestamp=deletion_timestamp,
            creation_timestamp=creation_timestamp,
        ),
        status=SimpleNamespace(
            phase=phase,
            reason=reason,
            message=message,
            container_statuses=container_statuses or [],
            start_time=start_time,
        ),
    )


class FakeProbeCoreV1:
    def __init__(self, read_pod=None, listed_pods=None, node=None):
        self.read_pod = read_pod
        self.listed_pods = listed_pods or []
        self.node = node or node_with_conditions()
        self.created_pod_body = None
        self.deleted_pods = []

    def read_node(self, name):
        return self.node

    def list_namespaced_pod(self, namespace, label_selector=None):
        return SimpleNamespace(items=self.listed_pods)

    def read_namespaced_pod(self, name, namespace):
        if self.read_pod is None:
            raise ApiException(status=404, reason="Not Found")
        return self.read_pod

    def create_namespaced_pod(self, namespace, body):
        self.created_pod_body = body
        self.read_pod = pull_probe_pod(
            image_id=int(body["metadata"]["labels"]["image-id"]),
            phase="Pending",
            name=body["metadata"]["name"],
        )
        return SimpleNamespace()

    def delete_namespaced_pod(self, name, namespace):
        self.deleted_pods.append(name)
        self.read_pod = None
        return SimpleNamespace()


class ImagePrepullTests(unittest.TestCase):
    def setUp(self):
        self.original_prepull_enabled = k8s_module.settings.IMAGE_PREPULL_ENABLED
        self.original_probe_enabled = k8s_module.settings.IMAGE_PULL_PROBE_ENABLED
        self.original_notebook_node_name = k8s_module.settings.NOTEBOOK_NODE_NAME
        self.original_pull_secret = k8s_module.settings.IMAGE_PULL_SECRET_NAME
        self.original_admin_password = k8s_module.settings.ADMIN_PASSWORD
        self.original_deadline = k8s_module.settings.IMAGE_PULL_PROBE_DEADLINE_SECONDS
        self.original_toleration = (
            k8s_module.settings.NOTEBOOK_TOLERATION_KEY,
            k8s_module.settings.NOTEBOOK_TOLERATION_VALUE,
            k8s_module.settings.NOTEBOOK_TOLERATION_EFFECT,
        )
        k8s_module.settings.ADMIN_PASSWORD = "test-admin-password"

    def tearDown(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = self.original_prepull_enabled
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = self.original_probe_enabled
        k8s_module.settings.NOTEBOOK_NODE_NAME = self.original_notebook_node_name
        k8s_module.settings.IMAGE_PULL_SECRET_NAME = self.original_pull_secret
        k8s_module.settings.ADMIN_PASSWORD = self.original_admin_password
        k8s_module.settings.IMAGE_PULL_PROBE_DEADLINE_SECONDS = self.original_deadline
        (
            k8s_module.settings.NOTEBOOK_TOLERATION_KEY,
            k8s_module.settings.NOTEBOOK_TOLERATION_VALUE,
            k8s_module.settings.NOTEBOOK_TOLERATION_EFFECT,
        ) = self.original_toleration

    def test_disabled_image_prepull_without_probe_returns_skipped_without_daemonset(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = False
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        client = object.__new__(k8s_module.K8sClient)

        status = client.sync_image_to_nodes(1, "notebook-image")

        self.assertEqual(status["status"], "skipped")
        self.assertEqual(status["desired_count"], 1)
        self.assertEqual(status["ready_count"], 0)

    def test_production_prepull_still_creates_daemonset_when_probe_enabled(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = True
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "default"
        client.apps_v1 = FakeAppsV1()
        client.core_v1 = FakeProbeCoreV1()

        status = client.sync_image_to_nodes(7, "notebook-image")

        self.assertEqual(status["status"], "pending")
        self.assertEqual(client.apps_v1.daemonset_body["kind"], "DaemonSet")
        self.assertEqual(client.apps_v1.daemonset_body["spec"]["template"]["spec"]["containers"][0]["image"], "notebook-image")

    def test_beta_probe_path_creates_single_hardened_pull_pod(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        k8s_module.settings.IMAGE_PULL_SECRET_NAME = "regcred"
        k8s_module.settings.NOTEBOOK_TOLERATION_KEY = "amd-oneclick/beta"
        k8s_module.settings.NOTEBOOK_TOLERATION_VALUE = "radeon"
        k8s_module.settings.NOTEBOOK_TOLERATION_EFFECT = "NoSchedule"
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1()

        status = client.sync_image_to_nodes(2, "registry/image:tag")

        body = client.core_v1.created_pod_body
        self.assertEqual(status["status"], "pulling")
        self.assertEqual(body["metadata"]["name"], "image-pull-catalog-2")
        self.assertEqual(body["metadata"]["labels"]["app"], "amd-oneclick-image-pull-check")
        self.assertEqual(body["spec"]["nodeName"], "beta-node")
        self.assertEqual(body["spec"]["restartPolicy"], "Never")
        self.assertFalse(body["spec"]["automountServiceAccountToken"])
        self.assertEqual(body["spec"]["imagePullSecrets"], [{"name": "regcred"}])
        container = body["spec"]["containers"][0]
        self.assertEqual(container["image"], "registry/image:tag")
        self.assertEqual(container["imagePullPolicy"], "Always")
        self.assertNotIn("amd.com/gpu", container["resources"]["requests"])
        self.assertFalse(container["securityContext"]["allowPrivilegeEscalation"])
        self.assertEqual(container["securityContext"]["capabilities"]["drop"], ["ALL"])
        self.assertTrue(container["securityContext"]["readOnlyRootFilesystem"])

    def test_probe_status_ready_when_container_reports_image_id(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        container_status = SimpleNamespace(
            name="pull",
            image_id="registry/image@sha256:abc",
            state=SimpleNamespace(running=SimpleNamespace()),
        )
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1(read_pod=pull_probe_pod(2, "Running", container_statuses=[container_status]))

        status = client.get_image_sync_status(2, "registry/image:tag")

        self.assertEqual(status["status"], "ready")
        self.assertEqual(status["ready_count"], 1)

    def test_probe_status_failed_when_image_pull_backoff(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        waiting = SimpleNamespace(reason="ImagePullBackOff", message="pull access denied")
        container_status = SimpleNamespace(name="pull", image_id="", state=SimpleNamespace(waiting=waiting))
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1(read_pod=pull_probe_pod(2, "Pending", container_statuses=[container_status]))

        status = client.get_image_sync_status(2, "registry/image:tag")

        self.assertEqual(status["status"], "failed")
        self.assertIn("ImagePullBackOff", status["message"])

    def test_probe_status_pending_when_no_probe_pod(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1()

        status = client.get_image_sync_status(2, "registry/image:tag")

        self.assertEqual(status["status"], "pending")

    def test_probe_status_ready_from_node_cache_when_no_probe_pod(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        node = node_with_conditions(images=[SimpleNamespace(names=["registry/image:tag"])])
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1(node=node)

        status = client.get_image_sync_status(2, "registry/image:tag")

        self.assertEqual(status["status"], "ready")
        self.assertEqual(status["ready_count"], 1)

    def test_probe_status_prefers_active_probe_over_node_cache(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        node = node_with_conditions(images=[SimpleNamespace(names=["registry/image:tag"])])
        waiting = SimpleNamespace(reason="ContainerCreating", message="")
        container_status = SimpleNamespace(name="pull", image_id="", state=SimpleNamespace(waiting=waiting))
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1(
            read_pod=pull_probe_pod(2, "Pending", container_statuses=[container_status]),
            node=node,
        )

        status = client.get_image_sync_status(2, "registry/image:tag")

        self.assertEqual(status["status"], "pulling")
        self.assertEqual(status["ready_count"], 0)

    def test_probe_sync_queues_when_another_probe_is_active(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        active = pull_probe_pod(1, "Pending")
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1(listed_pods=[active])

        status = client.sync_image_to_nodes(2, "registry/image:tag")

        self.assertEqual(status["status"], "queued")
        self.assertIsNone(client.core_v1.created_pod_body)

    def test_probe_sync_fails_before_create_when_node_has_disk_pressure(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1(node=node_with_conditions(disk_pressure="True"))

        status = client.sync_image_to_nodes(2, "registry/image:tag")

        self.assertEqual(status["status"], "failed")
        self.assertIn("DiskPressure", status["message"])
        self.assertIsNone(client.core_v1.created_pod_body)

    def test_probe_sync_refuses_default_admin_password(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        k8s_module.settings.ADMIN_PASSWORD = "admin123"
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1()

        status = client.sync_image_to_nodes(2, "registry/image:tag")

        self.assertEqual(status["status"], "failed")
        self.assertIn("ADMIN_PASSWORD", status["message"])
        self.assertIsNone(client.core_v1.created_pod_body)

    def test_delete_probe_removes_only_verified_exact_pod(self):
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.apps_v1 = FakeAppsV1()
        client.core_v1 = FakeProbeCoreV1(read_pod=pull_probe_pod(2))

        client.delete_image_sync(2)

        self.assertEqual(client.core_v1.deleted_pods, ["image-pull-catalog-2"])

    def test_delete_probe_refuses_unmanaged_same_name_pod(self):
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.apps_v1 = FakeAppsV1()
        client.core_v1 = FakeProbeCoreV1(read_pod=pull_probe_pod(2, labels={"app": "other"}))

        with self.assertRaises(RuntimeError):
            client.delete_image_sync(2)

        self.assertEqual(client.core_v1.deleted_pods, [])

    def test_probe_status_failed_when_pod_terminating(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        waiting = SimpleNamespace(reason="ContainerCreating", message="")
        container_status = SimpleNamespace(name="pull", image_id="", state=SimpleNamespace(waiting=waiting))
        pod = pull_probe_pod(2, "Pending", container_statuses=[container_status],
                             deletion_timestamp="2026-06-16T00:00:00Z")
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1(read_pod=pod)

        status = client.get_image_sync_status(2, "registry/image:tag")

        self.assertEqual(status["status"], "failed")
        self.assertIn("terminating", status["message"])

    def test_probe_status_ready_when_terminating_but_image_pulled(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        container_status = SimpleNamespace(
            name="pull",
            image_id="registry/image@sha256:abc",
            state=SimpleNamespace(running=SimpleNamespace()),
        )
        pod = pull_probe_pod(2, "Running", container_statuses=[container_status],
                             deletion_timestamp="2026-06-16T00:00:00Z")
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1(read_pod=pod)

        status = client.get_image_sync_status(2, "registry/image:tag")

        self.assertEqual(status["status"], "ready")
        self.assertEqual(status["ready_count"], 1)

    def test_active_probe_name_skips_terminating_pod(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        terminating = pull_probe_pod(1, "Running", deletion_timestamp="2026-06-16T00:00:00Z")
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1(listed_pods=[terminating])

        self.assertIsNone(client._active_pull_probe_name())

    @patch("app.k8s_client.time.sleep", return_value=None)
    def test_probe_sync_retries_then_queues_when_create_conflicts(self, _sleep):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"

        class ConflictCoreV1(FakeProbeCoreV1):
            def create_namespaced_pod(self, namespace, body):
                raise ApiException(status=409, reason="object is being deleted")

        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = ConflictCoreV1()

        status = client.sync_image_to_nodes(2, "registry/image:tag")

        self.assertEqual(status["status"], "queued")
        self.assertIn("terminating", status["message"])

    @patch("app.k8s_client.time.sleep", return_value=None)
    def test_probe_sync_succeeds_after_transient_conflict(self, _sleep):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"

        class TransientConflictCoreV1(FakeProbeCoreV1):
            def __init__(self, *a, **kw):
                super().__init__(*a, **kw)
                self.create_calls = 0

            def create_namespaced_pod(self, namespace, body):
                self.create_calls += 1
                if self.create_calls == 1:
                    raise ApiException(status=409, reason="object is being deleted")
                return super().create_namespaced_pod(namespace, body)

        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = TransientConflictCoreV1()

        status = client.sync_image_to_nodes(2, "registry/image:tag")

        self.assertEqual(client.core_v1.create_calls, 2)
        self.assertIsNotNone(client.core_v1.created_pod_body)
        self.assertEqual(status["status"], "pulling")

    def test_probe_pod_uses_configured_deadline(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        k8s_module.settings.IMAGE_PULL_PROBE_DEADLINE_SECONDS = 1800
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1()

        client.sync_image_to_nodes(2, "registry/image:tag")

        self.assertEqual(client.core_v1.created_pod_body["spec"]["activeDeadlineSeconds"], 1800)

    def test_probe_pulling_message_includes_elapsed(self):
        k8s_module.settings.IMAGE_PREPULL_ENABLED = False
        k8s_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        k8s_module.settings.NOTEBOOK_NODE_NAME = "beta-node"
        k8s_module.settings.IMAGE_PULL_PROBE_DEADLINE_SECONDS = 7200
        start = datetime(2026, 6, 16, 0, 0, 0, tzinfo=timezone.utc)
        waiting = SimpleNamespace(reason="ContainerCreating", message="")
        container_status = SimpleNamespace(name="pull", image_id="", state=SimpleNamespace(waiting=waiting))
        pod = pull_probe_pod(2, "Pending", container_statuses=[container_status], start_time=start)
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = FakeProbeCoreV1(read_pod=pod)

        class FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                fixed = start + timedelta(minutes=8)
                return fixed.astimezone(tz) if tz else fixed

        with patch("app.k8s_client.datetime", FixedDatetime):
            status = client.get_image_sync_status(2, "registry/image:tag")

        self.assertEqual(status["status"], "pulling")
        self.assertIn("8m", status["message"])
        self.assertIn("120m deadline", status["message"])

    def test_forbidden_node_list_returns_best_effort_empty_eligible_set(self):
        class ForbiddenCoreV1:
            def list_node(self):
                raise ApiException(status=403, reason="Forbidden")

        client = object.__new__(k8s_module.K8sClient)
        client.core_v1 = ForbiddenCoreV1()

        self.assertEqual(client._eligible_prepull_nodes(), set())


def _event(reason, message, ts):
    return SimpleNamespace(
        reason=reason,
        message=message,
        last_timestamp=ts,
        first_timestamp=ts,
        event_time=None,
    )


def _starting_pod(phase="Pending", waiting_reason="ContainerCreating", waiting_message="",
                  start_time=None, container_statuses=None):
    cs = container_statuses
    if cs is None and waiting_reason is not None:
        cs = [SimpleNamespace(
            name="notebook",
            ready=False,
            state=SimpleNamespace(
                waiting=SimpleNamespace(reason=waiting_reason, message=waiting_message),
                running=None,
            ),
        )]
    return SimpleNamespace(
        metadata=SimpleNamespace(name="nb-1"),
        status=SimpleNamespace(
            phase=phase,
            container_statuses=cs,
            start_time=start_time,
        ),
    )


class StartupDetailCoreV1:
    def __init__(self, pod=None, events=None, log="", log_exc=None, read_exc=None):
        self._pod = pod
        self._events = events or []
        self._log = log
        self._log_exc = log_exc
        self._read_exc = read_exc

    def read_namespaced_pod(self, name, namespace):
        if self._read_exc is not None:
            raise self._read_exc
        if self._pod is None:
            raise ApiException(status=404, reason="Not Found")
        return self._pod

    def list_namespaced_event(self, namespace, field_selector=None):
        return SimpleNamespace(items=list(self._events))

    def read_namespaced_pod_log(self, name, namespace, tail_lines=None, limit_bytes=None):
        if self._log_exc is not None:
            raise self._log_exc
        return self._log


class StartupDetailTests(unittest.TestCase):
    def test_startup_detail_reports_pulling_with_elapsed(self):
        start = datetime(2026, 6, 16, 0, 0, 0, tzinfo=timezone.utc)
        events = [_event("Pulling", 'Pulling image "user-7:llm"', start)]
        pod = _starting_pod(waiting_reason="ContainerCreating", start_time=start)
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "ns"
        client.core_v1 = StartupDetailCoreV1(pod=pod, events=events)

        class FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                fixed = start + timedelta(minutes=4)
                return fixed.astimezone(tz) if tz else fixed

        with patch("app.k8s_client.datetime", FixedDatetime):
            msg = client.get_startup_detail("nb-1")

        self.assertIsNotNone(msg)
        self.assertIn("user-7:llm", msg)
        self.assertIn("4m", msg)

    def test_startup_detail_reports_scheduling_block(self):
        when = datetime(2026, 6, 16, 0, 0, 0, tzinfo=timezone.utc)
        events = [_event("FailedScheduling", "0/1 nodes are available: 1 Insufficient amd.com/gpu", when)]
        pod = _starting_pod(phase="Pending", waiting_reason=None, container_statuses=None)
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "ns"
        client.core_v1 = StartupDetailCoreV1(pod=pod, events=events)

        msg = client.get_startup_detail("nb-1")
        self.assertIsNotNone(msg)
        self.assertIn("Insufficient amd.com/gpu", msg)

    def test_startup_detail_reports_image_pull_failure(self):
        pod = _starting_pod(waiting_reason="ImagePullBackOff", waiting_message="pull access denied")
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "ns"
        client.core_v1 = StartupDetailCoreV1(pod=pod, events=[])

        msg = client.get_startup_detail("nb-1")
        self.assertIsNotNone(msg)
        self.assertIn("pull access denied", msg)

    def test_startup_detail_none_when_pod_missing(self):
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "ns"
        client.core_v1 = StartupDetailCoreV1(pod=None)
        self.assertIsNone(client.get_startup_detail("nb-1"))

    def test_get_pod_logs_returns_events_and_container(self):
        when = datetime(2026, 6, 16, 0, 0, 0, tzinfo=timezone.utc)
        events = [_event("Pulling", "Pulling image", when), _event("Started", "Started container", when)]
        pod = _starting_pod(phase="Running", waiting_reason=None,
                            container_statuses=[SimpleNamespace(name="nb", ready=True,
                                                                state=SimpleNamespace(waiting=None, running=SimpleNamespace()))])
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "ns"
        client.core_v1 = StartupDetailCoreV1(pod=pod, events=events, log="hello world\n")

        out = client.get_pod_logs("nb-1")
        self.assertEqual(out["container"], "hello world\n")
        self.assertEqual(len(out["events"]), 2)
        self.assertEqual(out["events"][0]["reason"], "Pulling")

    def test_get_pod_logs_empty_container_during_pull(self):
        when = datetime(2026, 6, 16, 0, 0, 0, tzinfo=timezone.utc)
        events = [_event("Pulling", "Pulling image", when)]
        pod = _starting_pod(waiting_reason="ContainerCreating")
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "ns"
        client.core_v1 = StartupDetailCoreV1(
            pod=pod, events=events, log_exc=ApiException(status=400, reason="ContainerCreating"))

        out = client.get_pod_logs("nb-1")
        self.assertEqual(out["container"], "")
        self.assertEqual(len(out["events"]), 1)

    def test_pod_events_swallows_non_api_exception(self):
        class BoomCoreV1:
            def list_namespaced_event(self, namespace, field_selector=None):
                raise RuntimeError("connection reset")

        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "ns"
        client.core_v1 = BoomCoreV1()
        self.assertEqual(client._pod_events("nb-1"), [])

    def test_get_pod_logs_not_found(self):
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "ns"
        client.core_v1 = StartupDetailCoreV1(pod=None)
        out = client.get_pod_logs("nb-1")
        self.assertEqual(out, {"events": [], "container": "", "status": "not_found"})


if __name__ == "__main__":
    unittest.main()
