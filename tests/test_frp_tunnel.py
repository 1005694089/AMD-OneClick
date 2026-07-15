import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import httpx

from tests.kube_stub import install

install()

from app import frp_tunnel  # noqa: E402
from app import k8s_client as k8s_module  # noqa: E402
from app.frp_control import FrpControlClient, FrpControlError  # noqa: E402


class FrpControlClientTests(unittest.TestCase):
    def test_create_sends_backend_auth_and_idempotency_key(self):
        captured = {}

        def handler(request):
            captured["authorization"] = request.headers.get("Authorization")
            captured["idempotency"] = request.headers.get("Idempotency-Key")
            captured["body"] = request.read().decode("utf-8")
            return httpx.Response(
                201,
                headers={"X-Request-ID": "req-1"},
                json={"tunnel": {"tunnel_id": "tun-1"}},
            )

        client = FrpControlClient(
            base_url="https://control.example.test",
            token="t" * 48,
            transport=httpx.MockTransport(handler),
        )
        result = client.create_tunnel({"owner_id": "user-1"}, "oneclick-request-1")

        self.assertEqual(result["tunnel"]["tunnel_id"], "tun-1")
        self.assertEqual(captured["authorization"], f"Bearer {'t' * 48}")
        self.assertEqual(captured["idempotency"], "oneclick-request-1")
        self.assertIn('"owner_id":"user-1"', captured["body"])

    def test_control_error_is_sanitized_and_structured(self):
        def handler(_request):
            return httpx.Response(
                409,
                json={
                    "code": "domain_conflict",
                    "message": "domain prefix is already active",
                    "request_id": "req-2",
                },
            )

        client = FrpControlClient(
            base_url="https://control.example.test",
            token="t" * 48,
            transport=httpx.MockTransport(handler),
        )
        with self.assertRaises(FrpControlError) as caught:
            client.create_tunnel({"owner_id": "user-1"}, "oneclick-request-2")
        self.assertEqual(caught.exception.status_code, 409)
        self.assertEqual(caught.exception.code, "domain_conflict")
        self.assertEqual(caught.exception.request_id, "req-2")


class FrpManifestTests(unittest.TestCase):
    def setUp(self):
        self.original = {
            name: getattr(k8s_module.settings, name)
            for name in (
                "FRP_TUNNEL_ENABLED",
                "FRP_AGENT_IMAGE",
                "FRP_AGENT_IMAGE_PULL_POLICY",
                "FRP_AGENT_IMAGE_PULL_SECRET_NAME",
                "FRP_PLATFORM_SECRET_NAME",
                "FRP_PLATFORM_TOKEN_KEY",
                "FRP_LOG_INGEST_URL",
                "FRP_DOMAIN_SUFFIX",
                "FRP_BANDWIDTH_LIMIT",
                "WORKSPACE_QUOTA_ENABLED",
            )
        }
        k8s_module.settings.WORKSPACE_QUOTA_ENABLED = False

    def tearDown(self):
        for name, value in self.original.items():
            setattr(k8s_module.settings, name, value)

    def test_enabled_manifest_contains_dormant_least_privilege_agent(self):
        k8s_module.settings.FRP_TUNNEL_ENABLED = True
        k8s_module.settings.FRP_AGENT_IMAGE = "registry.example/frp-agent:v1"
        k8s_module.settings.FRP_AGENT_IMAGE_PULL_POLICY = "IfNotPresent"
        k8s_module.settings.FRP_AGENT_IMAGE_PULL_SECRET_NAME = "frp-regcred"
        k8s_module.settings.FRP_PLATFORM_SECRET_NAME = "frp-platform"
        k8s_module.settings.FRP_PLATFORM_TOKEN_KEY = "global-token"
        k8s_module.settings.FRP_LOG_INGEST_URL = "https://origin.example:7443/__frp_logs/v1/logs"
        k8s_module.settings.FRP_DOMAIN_SUFFIX = "example.test"
        k8s_module.settings.FRP_BANDWIDTH_LIMIT = "2500KB"

        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "user-workloads"
        manifest = client._get_pod_manifest("user@example.test", "u-1-demo", "notebook:v1")

        containers = {item["name"]: item for item in manifest["spec"]["containers"]}
        self.assertEqual(set(containers), {"notebook", "frpc"})
        self.assertTrue(manifest["spec"]["automountServiceAccountToken"] is False)
        self.assertTrue(containers["frpc"]["securityContext"]["runAsNonRoot"])
        self.assertTrue(containers["frpc"]["securityContext"]["readOnlyRootFilesystem"])
        self.assertEqual(containers["frpc"]["securityContext"]["capabilities"]["drop"], ["ALL"])
        notebook_env_names = {item["name"] for item in containers["notebook"]["env"]}
        self.assertNotIn("FRP_CLIENT_SECRET", notebook_env_names)
        volumes = {item["name"]: item for item in manifest["spec"]["volumes"]}
        self.assertTrue(volumes["frp-tunnel-credentials"]["secret"]["optional"])
        self.assertEqual(volumes["frp-platform"]["secret"]["secretName"], "frp-platform")
        self.assertEqual(volumes["frp-platform"]["secret"]["defaultMode"], 0o444)
        self.assertNotIn("fsGroup", manifest["spec"]["securityContext"])
        init_containers = {item["name"]: item for item in manifest["spec"]["initContainers"]}
        self.assertIn("frp-agent-init", init_containers)
        self.assertEqual(
            init_containers["frp-agent-init"]["args"],
            ["chown 10001:10001 /var/lib/frp-agent /var/log/frpc"],
        )
        self.assertEqual(
            init_containers["frp-agent-init"]["securityContext"]["capabilities"]["add"],
            ["CHOWN"],
        )
        self.assertIn({"name": "frp-regcred"}, manifest["spec"]["imagePullSecrets"])

    def test_disabled_manifest_is_unchanged_single_container(self):
        k8s_module.settings.FRP_TUNNEL_ENABLED = False
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "user-workloads"
        manifest = client._get_pod_manifest("user@example.test", "u-1-demo", "notebook:v1")
        self.assertEqual([item["name"] for item in manifest["spec"]["containers"]], ["notebook"])
        self.assertNotIn("amd-oneclick/frp-agent", manifest["metadata"]["annotations"])


class FakeK8s:
    def __init__(self):
        self.secret = None
        self.secret_exists = False
        self.deleted = False
        self.has_agent = True

    def pod_has_container(self, _instance_id, container_name):
        return self.has_agent and container_name == "frpc"

    def get_pod_identity(self, instance_id):
        return {
            "namespace": "user-workloads",
            "pod_name": instance_id,
            "pod_uid": "695057eb-4dae-4acd-bad6-d285a07f3068",
        }

    def tunnel_secret_exists(self, _instance_id):
        return self.secret_exists

    def upsert_tunnel_secret(self, instance_id, tunnel, credentials):
        self.secret = (instance_id, copy.deepcopy(tunnel), copy.deepcopy(credentials))
        self.secret_exists = True

    def delete_tunnel_secret(self, _instance_id):
        self.deleted = True
        self.secret_exists = False
        return True

    def disable_tunnel_secret(self, _instance_id):
        self.deleted = True
        self.secret_exists = True
        return True


class FakeControl:
    def __init__(self):
        self.deleted = []

    @staticmethod
    def _tunnel():
        return {
            "tunnel_id": "tun-1",
            "owner_id": "user-7",
            "cluster_id": "host",
            "namespace": "user-workloads",
            "pod_name": "u-7-demo",
            "pod_uid": "695057eb-4dae-4acd-bad6-d285a07f3068",
            "domain_prefix": "demo-user",
            "fqdn": "demo-user.radeon.firstdg.ai",
            "local_port": 8888,
            "status": "pending",
        }

    def create_tunnel(self, request, _key):
        tunnel = self._tunnel()
        tunnel.update({key: request[key] for key in ("owner_id", "cluster_id", "namespace", "pod_name", "pod_uid", "domain_prefix", "local_port")})
        tunnel["fqdn"] = f"{request['domain_prefix']}.radeon.firstdg.ai"
        return {
            "tunnel": tunnel,
            "agent_credentials": {
                "client_id": "rc-client-1",
                "client_secret": "s" * 48,
                "server_address": "radeon.firstdg.ai",
                "server_port": 7000,
            },
        }

    def get_tunnel(self, _tunnel_id):
        return {"tunnel": self._tunnel()}

    def list_tunnels(self, _owner_id):
        return [self._tunnel()]

    def rotate_credentials(self, _tunnel_id):
        return self.create_tunnel(
            {
                "owner_id": "user-7",
                "cluster_id": "host",
                "namespace": "user-workloads",
                "pod_name": "u-7-demo",
                "pod_uid": "695057eb-4dae-4acd-bad6-d285a07f3068",
                "domain_prefix": "demo-user",
                "local_port": 8888,
            },
            "rotate",
        )

    def delete_tunnel(self, tunnel_id):
        self.deleted.append(tunnel_id)
        tunnel = self._tunnel()
        tunnel["status"] = "quarantine"
        tunnel["quarantine_until"] = "2026-07-16T00:00:00Z"
        return {"tunnel": tunnel}


class FrpTunnelLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.old_enabled = frp_tunnel.settings.FRP_TUNNEL_ENABLED
        frp_tunnel.settings.FRP_TUNNEL_ENABLED = True
        self.user = {"id": 7, "email": "user@example.test"}
        self.record = {
            "user_id": 7,
            "instance_id": "u-7-demo",
            "frp_tunnel_id": None,
            "frp_domain_prefix": None,
            "frp_fqdn": None,
            "frp_local_port": None,
            "frp_tunnel_status": None,
        }

    def tearDown(self):
        frp_tunnel.settings.FRP_TUNNEL_ENABLED = self.old_enabled

    @patch.object(frp_tunnel.store, "set_instance_tunnel")
    def test_create_writes_one_time_secret_but_only_persists_metadata(self, persist):
        k8s = FakeK8s()
        control = FakeControl()
        result = frp_tunnel.create_tunnel(
            self.user, self.record, "Demo-User", 8888, k8s, client=control
        )

        self.assertEqual(result["fqdn"], "demo-user.radeon.firstdg.ai")
        self.assertEqual(k8s.secret[2]["client_secret"], "s" * 48)
        persisted = persist.call_args.args
        self.assertNotIn("s" * 48, [str(value) for value in persisted])
        self.assertEqual(persisted[1], "tun-1")
        self.assertEqual(persisted[2], "demo-user")

    @patch.object(frp_tunnel.store, "set_instance_tunnel")
    def test_lost_create_response_recovers_by_rotating_pod_credentials(self, _persist):
        class LostResponseControl(FakeControl):
            def __init__(self):
                super().__init__()
                self.rotated = False

            def create_tunnel(self, _request, _key):
                raise FrpControlError(
                    "domain prefix is already active",
                    status_code=409,
                    code="domain_conflict",
                )

            def rotate_credentials(self, tunnel_id):
                self.rotated = tunnel_id == "tun-1"
                return FakeControl.create_tunnel(
                    self,
                    {
                        "owner_id": "user-7",
                        "cluster_id": "host",
                        "namespace": "user-workloads",
                        "pod_name": "u-7-demo",
                        "pod_uid": "695057eb-4dae-4acd-bad6-d285a07f3068",
                        "domain_prefix": "demo-user",
                        "local_port": 8888,
                    },
                    "rotate",
                )

        control = LostResponseControl()
        k8s = FakeK8s()
        result = frp_tunnel.create_tunnel(
            self.user, self.record, "demo-user", 8888, k8s, client=control
        )
        self.assertTrue(control.rotated)
        self.assertEqual(result["tunnel_id"], "tun-1")
        self.assertTrue(k8s.secret_exists)

    @patch.object(frp_tunnel.store, "update_instance_tunnel_status")
    def test_delete_stops_secret_before_remote_revoke(self, update_status):
        k8s = FakeK8s()
        k8s.secret_exists = True
        control = FakeControl()
        record = {
            **self.record,
            "frp_tunnel_id": "tun-1",
            "frp_domain_prefix": "demo-user",
            "frp_fqdn": "demo-user.radeon.firstdg.ai",
            "frp_local_port": 8888,
            "frp_tunnel_status": "active",
        }

        result = frp_tunnel.delete_tunnel(self.user, record, k8s, client=control)

        self.assertTrue(k8s.deleted)
        self.assertEqual(control.deleted, ["tun-1"])
        self.assertEqual(result["status"], "quarantine")
        update_status.assert_called_once_with("u-7-demo", "quarantine")

    def test_agent_missing_requires_instance_restart(self):
        k8s = FakeK8s()
        k8s.has_agent = False
        with self.assertRaises(frp_tunnel.FrpTunnelError) as caught:
            frp_tunnel.create_tunnel(
                self.user, self.record, "demo-user", 8888, k8s, client=FakeControl()
            )
        self.assertEqual(caught.exception.code, "agent_missing")


class FrpSecretTests(unittest.TestCase):
    def test_secret_is_owned_by_pod_and_contains_no_global_token(self):
        core = SimpleNamespace()
        core.read_namespaced_pod = lambda **_kwargs: SimpleNamespace(
            metadata=SimpleNamespace(
                name="u-7-demo",
                uid="695057eb-4dae-4acd-bad6-d285a07f3068",
                deletion_timestamp=None,
            )
        )
        captured = {}
        core.create_namespaced_secret = lambda namespace, body: captured.update(
            {"namespace": namespace, "body": body}
        )
        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "user-workloads"
        client.core_v1 = core

        client.upsert_tunnel_secret(
            "u-7-demo",
            {"domain_prefix": "demo-user", "local_port": 8888},
            {
                "client_id": "rc-client-1",
                "client_secret": "s" * 48,
                "server_address": "radeon.firstdg.ai",
                "server_port": 7000,
            },
        )

        body = captured["body"]
        self.assertEqual(captured["namespace"], "user-workloads")
        self.assertEqual(body["metadata"]["ownerReferences"][0]["uid"], "695057eb-4dae-4acd-bad6-d285a07f3068")
        self.assertNotIn("global-token", body["stringData"])
        self.assertNotIn("control-api-token", body["stringData"])

    def test_disable_replaces_secret_data_with_empty_map(self):
        captured = {}

        def patch_secret(**kwargs):
            captured.update(kwargs)

        client = object.__new__(k8s_module.K8sClient)
        client.namespace = "user-workloads"
        client.core_v1 = SimpleNamespace(patch_namespaced_secret=patch_secret)

        self.assertTrue(client.disable_tunnel_secret("u-7-demo"))
        self.assertEqual(captured["body"], [{"op": "add", "path": "/data", "value": {}}])
        self.assertEqual(captured["_content_type"], "application/json-patch+json")


if __name__ == "__main__":
    unittest.main()
