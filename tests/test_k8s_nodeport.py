import unittest
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
            node_port, created = client._create_service_with_nodeport_retry(
                "hf-user@example.test",
                "hf-demo",
            )

        self.assertTrue(created)
        self.assertEqual(node_port, 32501)
        self.assertEqual(client.core_v1.created_ports, [32500, 32501])

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
        self.original = k8s_module.settings.HF_ENDPOINT

    def tearDown(self):
        k8s_module.settings.HF_ENDPOINT = self.original

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


if __name__ == "__main__":
    unittest.main()
