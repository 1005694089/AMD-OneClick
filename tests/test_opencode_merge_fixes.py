import hashlib
import hmac
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests.kube_stub import install

install()

from kubernetes.client.rest import ApiException  # noqa: E402

from app import k8s_client as k8s_module  # noqa: E402


def _svc(jupyter_port=None, opencode_port=None):
    ports = []
    if jupyter_port is not None:
        ports.append(SimpleNamespace(name="jupyter", node_port=jupyter_port))
    if opencode_port is not None:
        ports.append(SimpleNamespace(name="opencode", node_port=opencode_port))
    return SimpleNamespace(spec=SimpleNamespace(ports=ports))


class OpenCodeAuthTests(unittest.TestCase):
    """OpenCode web must not be exposed unauthenticated on its NodePort."""

    def setUp(self):
        self._orig = (
            k8s_module.settings.NOTEBOOK_TOKEN,
            k8s_module.settings.OPENCODE_PASSWORD_SECRET,
            k8s_module.settings.OPENCODE_WEB_USERNAME,
            k8s_module.settings.PUBLIC_BASE_URL,
            k8s_module.settings.SERVICE_HOST,
        )
        # NOTEBOOK_TOKEN is deliberately DIFFERENT from the password secret: the OpenCode
        # password must derive from the server-only secret, never from the user-visible token.
        k8s_module.settings.NOTEBOOK_TOKEN = "user-visible-tok"
        k8s_module.settings.OPENCODE_PASSWORD_SECRET = "server-only-secret"
        k8s_module.settings.OPENCODE_WEB_USERNAME = "opencode"

    def tearDown(self):
        (
            k8s_module.settings.NOTEBOOK_TOKEN,
            k8s_module.settings.OPENCODE_PASSWORD_SECRET,
            k8s_module.settings.OPENCODE_WEB_USERNAME,
            k8s_module.settings.PUBLIC_BASE_URL,
            k8s_module.settings.SERVICE_HOST,
        ) = self._orig

    @staticmethod
    def _expected_pw(instance_id):
        return hmac.new(
            k8s_module.settings.OPENCODE_PASSWORD_SECRET.encode("utf-8"),
            instance_id.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def test_pod_env_injects_opencode_basic_auth_credentials(self):
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"

        manifest = client._get_pod_manifest(
            "hf-user@example.test", "hf-demo", "notebook-image",
        )
        env = manifest["spec"]["containers"][0]["env"]

        self.assertIn({"name": "OPENCODE_SERVER_USERNAME", "value": "opencode"}, env)
        # Password is the per-instance HMAC keyed on the server-only secret.
        self.assertIn(
            {"name": "OPENCODE_SERVER_PASSWORD", "value": self._expected_pw("hf-demo")}, env
        )
        # NOT the raw user-visible NOTEBOOK_TOKEN, and NOT an HMAC keyed on it.
        self.assertNotIn(
            {"name": "OPENCODE_SERVER_PASSWORD", "value": k8s_module.settings.NOTEBOOK_TOKEN},
            env,
        )
        forged = hmac.new(
            k8s_module.settings.NOTEBOOK_TOKEN.encode("utf-8"),
            b"hf-demo",
            hashlib.sha256,
        ).hexdigest()
        self.assertNotIn(
            {"name": "OPENCODE_SERVER_PASSWORD", "value": forged}, env
        )

    def test_opencode_password_is_per_instance(self):
        # Two different instances must get two different OpenCode passwords, so a credential
        # leaked from one cannot authenticate against another instance's NodePort.
        client = object.__new__(k8s_module.K8sClient)
        pw_a = client._opencode_password("inst-a")
        pw_b = client._opencode_password("inst-b")
        self.assertNotEqual(pw_a, pw_b)
        self.assertNotEqual(pw_a, k8s_module.settings.NOTEBOOK_TOKEN)
        # Deterministic: same instance recomputes the same value (pod env vs URL must agree).
        self.assertEqual(pw_a, client._opencode_password("inst-a"))

    def test_opencode_password_not_derivable_from_notebook_token(self):
        # NOTEBOOK_TOKEN is embedded in every user's Jupyter URL, so a user holds it. The
        # OpenCode password MUST be keyed on the server-only OPENCODE_PASSWORD_SECRET instead,
        # so a user cannot re-derive another instance's password from the token + instance_id.
        client = object.__new__(k8s_module.K8sClient)
        pw = client._opencode_password("inst-a")
        forged = hmac.new(
            k8s_module.settings.NOTEBOOK_TOKEN.encode("utf-8"),
            b"inst-a",
            hashlib.sha256,
        ).hexdigest()
        self.assertNotEqual(pw, forged)
        # And it does track the server-only secret.
        self.assertEqual(pw, self._expected_pw("inst-a"))

    def test_pod_env_password_matches_url_password(self):
        # End-to-end invariant: the password baked into the pod env MUST equal the password
        # embedded in the owner's URL for the SAME instance, or Basic auth fails at runtime.
        k8s_module.settings.PUBLIC_BASE_URL = "http://1.2.3.4:8080"
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"

        manifest = client._get_pod_manifest(
            "hf-user@example.test", "hf-demo", "notebook-image",
        )
        env = manifest["spec"]["containers"][0]["env"]
        env_pw = next(
            e["value"] for e in env if e["name"] == "OPENCODE_SERVER_PASSWORD"
        )
        url = client._build_opencode_url(31000, "hf-demo")
        self.assertEqual(url, f"http://opencode:{env_pw}@1.2.3.4:31000/")

    def test_opencode_url_embeds_credentials(self):
        k8s_module.settings.PUBLIC_BASE_URL = "http://1.2.3.4:8080"
        client = object.__new__(k8s_module.K8sClient)

        url = client._build_opencode_url(31000, "inst-a")

        self.assertEqual(
            url, f"http://opencode:{self._expected_pw('inst-a')}@1.2.3.4:31000/"
        )

    def test_opencode_url_percent_encodes_username(self):
        # The derived password is hex (URL-safe), but the username may contain reserved chars.
        k8s_module.settings.PUBLIC_BASE_URL = "http://1.2.3.4:8080"
        k8s_module.settings.OPENCODE_WEB_USERNAME = "a/b@c:d"
        client = object.__new__(k8s_module.K8sClient)

        url = client._build_opencode_url(31000, "inst-a")

        self.assertIn("a%2Fb%40c%3Ad:", url)
        self.assertNotIn("a/b@c:d:", url)

    def test_opencode_url_none_when_no_port(self):
        client = object.__new__(k8s_module.K8sClient)
        self.assertIsNone(client._build_opencode_url(None, "inst-a"))


class CustomImageNoPrepullTests(unittest.TestCase):
    """Req 1: custom user images are pulled lazily by the notebook pod, never prepulled."""

    def test_sync_custom_image_to_nodes_removed(self):
        # The auto-prepull creator must no longer exist on the client; nothing should create
        # a custom-prepull DaemonSet.
        self.assertFalse(
            hasattr(k8s_module.K8sClient, "sync_custom_image_to_nodes"),
            "sync_custom_image_to_nodes must be removed so custom images are never prepulled",
        )

    def test_delete_custom_image_sync_still_cleans_up(self):
        # Best-effort cleanup of any pre-existing DaemonSet (from older deployments) must remain,
        # and must swallow a 404 (nothing to delete is the normal case now).
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        deleted = []

        class _Apps:
            def delete_namespaced_daemon_set(self, name, namespace):
                deleted.append(name)
                raise ApiException(status=404, reason="Not Found")

        client.apps_v1 = _Apps()
        client.delete_custom_image_sync(7)  # must not raise on 404
        self.assertEqual(deleted, ["image-prepull-custom-7"])


class DualPortAllocationTests(unittest.TestCase):
    def setUp(self):
        self._orig = (
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
        ) = self._orig

    def test_allocate_pair_returns_distinct_ports(self):
        client = object.__new__(k8s_module.K8sClient)
        jupyter, opencode = client._allocate_node_port_pair(set(), start_port=32500)
        self.assertEqual((jupyter, opencode), (32500, 32501))

    def test_allocate_pair_skips_used_ports(self):
        client = object.__new__(k8s_module.K8sClient)
        jupyter, opencode = client._allocate_node_port_pair({32500, 32502}, start_port=32500)
        self.assertEqual(jupyter, 32501)
        self.assertEqual(opencode, 32503)
        self.assertNotEqual(jupyter, opencode)

    def test_create_service_retries_when_opencode_port_conflicts(self):
        # Stub rejects only the opencode port (ports[1]) of the first pair, forcing the
        # retry loop to reallocate BOTH ports and succeed on the next pair.
        class _Core:
            def __init__(self):
                self.created = []

            def read_namespaced_service(self, name, namespace):
                raise ApiException(status=404, reason="Not Found")

            def create_namespaced_service(self, namespace, body):
                opencode_port = body["spec"]["ports"][1]["nodePort"]
                self.created.append((body["spec"]["ports"][0]["nodePort"], opencode_port))
                if opencode_port == 32501:
                    raise ApiException(status=422, reason="provided port is already allocated")
                return SimpleNamespace()

        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = _Core()

        with patch.object(k8s_module.random, "randint", return_value=0):
            node_port, opencode_node_port, created = client._create_service_with_nodeport_retry(
                "u@example.test", "nb-1",
            )

        self.assertTrue(created)
        self.assertNotEqual(node_port, opencode_node_port)
        # first attempt (32500, 32501) rejected on opencode port; retry from 32502
        self.assertEqual(client.core_v1.created[0], (32500, 32501))
        self.assertEqual((node_port, opencode_node_port), (32502, 32503))

    def test_list_instances_returns_opencode_fields(self):
        class _Core:
            def list_namespaced_pod(self, namespace, label_selector=None):
                pod = SimpleNamespace(
                    metadata=SimpleNamespace(
                        labels={"instance-id": "nb-1"},
                        annotations={"amd-oneclick/instance-type": "jupyter",
                                     "amd-oneclick/path-proxy": "true"},
                        name="nb-1",
                        creation_timestamp=None,
                    ),
                    status=SimpleNamespace(phase="Running"),
                    spec=SimpleNamespace(containers=[SimpleNamespace(
                        image="img",
                        resources=SimpleNamespace(requests={"amd.com/gpu": 1}),
                    )]),
                )
                return SimpleNamespace(items=[pod])

            def read_namespaced_service(self, name, namespace):
                return _svc(jupyter_port=30001, opencode_port=30002)

        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "amd-oneclick-radeon-beta"
        client.core_v1 = _Core()

        instances = client.list_instances()
        self.assertEqual(len(instances), 1)
        self.assertEqual(instances[0]["node_port"], 30001)
        self.assertEqual(instances[0]["opencode_node_port"], 30002)
        self.assertIsNotNone(instances[0]["opencode_url"])


if __name__ == "__main__":
    unittest.main()
