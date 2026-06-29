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

    def test_custom_image_notebook_uses_always_pull_policy(self):
        # Custom images reuse the user-{id}:{name} tag across rebuilds, so the notebook pod
        # must pull Always to avoid serving a stale cached layer after delete+rebuild.
        orig = k8s_module.settings.CUSTOM_IMAGE_REGISTRY
        try:
            k8s_module.settings.CUSTOM_IMAGE_REGISTRY = "reg.example/custom"
            client = object.__new__(k8s_module.K8sClient)
            client.namespace = "amd-oneclick-radeon-beta"
            manifest = client._get_pod_manifest(
                "hf-user@example.test",
                "hf-demo",
                "reg.example/custom/user-1:demo",
            )
        finally:
            k8s_module.settings.CUSTOM_IMAGE_REGISTRY = orig

        container = manifest["spec"]["containers"][0]
        self.assertEqual(container["imagePullPolicy"], "Always")

    def test_standard_image_notebook_keeps_ifnotpresent(self):
        orig = k8s_module.settings.CUSTOM_IMAGE_REGISTRY
        try:
            k8s_module.settings.CUSTOM_IMAGE_REGISTRY = "reg.example/custom"
            client = object.__new__(k8s_module.K8sClient)
            client.namespace = "amd-oneclick-radeon-beta"
            manifest = client._get_pod_manifest(
                "hf-user@example.test",
                "hf-demo",
                "docker.io/library/standard-notebook:1.0",
            )
        finally:
            k8s_module.settings.CUSTOM_IMAGE_REGISTRY = orig

        container = manifest["spec"]["containers"][0]
        self.assertEqual(container["imagePullPolicy"], "IfNotPresent")

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

    def test_service_launch_waits_on_jupyter_not_all_jobs(self):
        # Regression: a bare `wait` keeps the pod alive as long as ANY backgrounded job runs,
        # so a crashed Jupyter is masked by a still-running OpenCode. We must wait on Jupyter's
        # PID specifically, then tear OpenCode down and exit with Jupyter's code.
        client = object.__new__(k8s_module.K8sClient)
        snippet = client._service_launch_snippet("nb-1", "/work")

        self.assertIn("JUPYTER_PID=$!", snippet)
        self.assertIn("OPENCODE_REQUIRED_VERSION", snippet)
        self.assertIn('npm i -g "opencode-ai@$OPENCODE_REQUIRED_VERSION"', snippet)
        self.assertIn("opencode --version", snippet)
        self.assertIn('wait "$JUPYTER_PID"', snippet)
        self.assertIn('kill "$OPENCODE_PID"', snippet)
        self.assertIn('exit "$JUPYTER_RC"', snippet)
        # The old unconditional `wait` (no PID) must be gone.
        self.assertNotIn("\nwait\n", snippet)


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
