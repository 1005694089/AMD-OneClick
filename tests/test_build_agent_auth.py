import os
import tempfile
import unittest

# (a) Set env before any app import.
_DB_FD, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.close(_DB_FD)
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["ADMIN_PASSWORD"] = "testpass"
os.environ["NOTEBOOK_NODE_NAME"] = "fake-node"
os.environ["BUILD_AGENT_TOKEN"] = "agent-secret"
# These tests exercise the LEGACY node-local build-agent endpoints (claim_build/log/result), which
# are intentionally starved when the image-service is on. Pin it off so the legacy path stays live.
os.environ["IMAGE_SERVICE_ENABLED"] = "false"

# (b) Install the kube stub before importing app.main.
from tests.kube_stub import install  # noqa: E402

install()

from fastapi.testclient import TestClient  # noqa: E402

from app import main as main_module  # noqa: E402
from app import store  # noqa: E402

AGENT_AUTH = {"Authorization": "Bearer agent-secret"}


class BuildAgentOwnershipTests(unittest.TestCase):
    """Build-agent log/result endpoints must only mutate builds the caller leased."""

    def setUp(self):
        self._orig = (
            main_module.settings.BUILD_AGENT_TOKEN,
            main_module.settings.BUILD_AGENT_ALLOWED_IPS,
            main_module.settings.CUSTOM_IMAGE_MAX_PER_USER,
            main_module.settings.IMAGE_SERVICE_ENABLED,
        )
        main_module.settings.BUILD_AGENT_TOKEN = "agent-secret"
        main_module.settings.BUILD_AGENT_ALLOWED_IPS = []
        main_module.settings.CUSTOM_IMAGE_MAX_PER_USER = 5
        # Legacy build-agent path under test; keep image-service off so claim_build is not starved.
        main_module.settings.IMAGE_SERVICE_ENABLED = False
        store.init_db()
        self.client = TestClient(main_module.app)
        self.img = store.create_custom_image(1, "demo", "registry/demo:1", "FROM scratch", 5)

    def tearDown(self):
        (
            main_module.settings.BUILD_AGENT_TOKEN,
            main_module.settings.BUILD_AGENT_ALLOWED_IPS,
            main_module.settings.CUSTOM_IMAGE_MAX_PER_USER,
            main_module.settings.IMAGE_SERVICE_ENABLED,
        ) = self._orig
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM custom_images")

    @classmethod
    def tearDownClass(cls):
        # Do not os.remove(_DB_PATH): app.store.engine is a process-wide singleton, and other
        # test classes/modules sharing this DB would hit "readonly database" after deletion.
        pass

    def _claim(self, agent_id="agent-A"):
        return self.client.post(
            "/api/internal/builds/claim", headers=AGENT_AUTH, json={"agent_id": agent_id}
        )

    def test_result_rejected_for_unclaimed_pending_build(self):
        # No claim performed: build is still 'pending'. A rogue result must be rejected.
        res = self.client.post(
            f"/api/internal/builds/{self.img['id']}/result",
            headers=AGENT_AUTH,
            json={"agent_id": "agent-evil", "status": "ready"},
        )
        self.assertEqual(res.status_code, 409)
        row = store.get_custom_image(self.img["id"])
        self.assertEqual(row["build_status"], "pending")

    def test_result_rejected_for_other_agent(self):
        self._claim("agent-A")
        res = self.client.post(
            f"/api/internal/builds/{self.img['id']}/result",
            headers=AGENT_AUTH,
            json={"agent_id": "agent-B", "status": "ready"},
        )
        self.assertEqual(res.status_code, 409)
        self.assertEqual(store.get_custom_image(self.img["id"])["build_status"], "building")

    def test_result_accepted_for_owning_agent(self):
        self._claim("agent-A")
        res = self.client.post(
            f"/api/internal/builds/{self.img['id']}/result",
            headers=AGENT_AUTH,
            json={"agent_id": "agent-A", "status": "ready"},
        )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(store.get_custom_image(self.img["id"])["build_status"], "ready")

    def test_build_ready_does_not_prepull(self):
        # Req 1: custom images are pulled lazily on launch, never prepulled to nodes.
        # Finalizing a build must NOT create any DaemonSet on the cluster — assert on the
        # underlying apps_v1 client (catches a regression regardless of method name).
        created = []
        self._claim("agent-A")

        class _Apps:
            def create_namespaced_daemon_set(self, namespace, body):
                created.append(body)
                return None

            def delete_namespaced_daemon_set(self, name, namespace):
                return None

        orig_apps = getattr(main_module.k8s_client, "apps_v1", None)
        main_module.k8s_client.apps_v1 = _Apps()
        try:
            res = self.client.post(
                f"/api/internal/builds/{self.img['id']}/result",
                headers=AGENT_AUTH,
                json={"agent_id": "agent-A", "status": "ready"},
            )
        finally:
            main_module.k8s_client.apps_v1 = orig_apps
        self.assertEqual(res.status_code, 200)
        self.assertEqual(created, [])
        self.assertEqual(store.get_custom_image(self.img["id"])["build_status"], "ready")

    def test_log_rejected_for_other_agent(self):
        self._claim("agent-A")
        res = self.client.post(
            f"/api/internal/builds/{self.img['id']}/log",
            headers=AGENT_AUTH,
            json={"agent_id": "agent-B", "log": "hello"},
        )
        self.assertEqual(res.status_code, 409)
        self.assertEqual(store.get_custom_image(self.img["id"])["build_log"], "")

    def test_log_accepted_for_owning_agent(self):
        self._claim("agent-A")
        res = self.client.post(
            f"/api/internal/builds/{self.img['id']}/log",
            headers=AGENT_AUTH,
            json={"agent_id": "agent-A", "log": "building...\n"},
        )
        self.assertEqual(res.status_code, 200)
        self.assertIn("building...", store.get_custom_image(self.img["id"])["build_log"])

    def test_double_finalize_is_rejected(self):
        # After the owning agent finalizes once (build leaves "building"), a second result
        # must be rejected by the in-WHERE-clause ownership/state guard.
        self._claim("agent-A")
        first = self.client.post(
            f"/api/internal/builds/{self.img['id']}/result",
            headers=AGENT_AUTH,
            json={"agent_id": "agent-A", "status": "ready"},
        )
        self.assertEqual(first.status_code, 200)
        second = self.client.post(
            f"/api/internal/builds/{self.img['id']}/result",
            headers=AGENT_AUTH,
            json={"agent_id": "agent-A", "status": "failed"},
        )
        self.assertEqual(second.status_code, 409)
        # status stays at the first terminal value, not overwritten
        self.assertEqual(store.get_custom_image(self.img["id"])["build_status"], "ready")

    def test_log_after_finalize_is_rejected(self):
        # Once a build is no longer "building", even the owning agent cannot append logs.
        self._claim("agent-A")
        self.client.post(
            f"/api/internal/builds/{self.img['id']}/result",
            headers=AGENT_AUTH,
            json={"agent_id": "agent-A", "status": "ready"},
        )
        res = self.client.post(
            f"/api/internal/builds/{self.img['id']}/log",
            headers=AGENT_AUTH,
            json={"agent_id": "agent-A", "log": "late log"},
        )
        self.assertEqual(res.status_code, 409)

    def test_missing_token_returns_401(self):
        res = self.client.post(
            f"/api/internal/builds/{self.img['id']}/log",
            json={"agent_id": "agent-A", "log": "x"},
        )
        self.assertEqual(res.status_code, 401)

    def test_ip_allowlist_ignores_x_forwarded_for(self):
        main_module.settings.BUILD_AGENT_ALLOWED_IPS = ["10.0.0.5"]
        try:
            # TestClient peer is "testclient", not in the allowlist; a spoofed XFF must NOT help.
            res = self.client.post(
                "/api/internal/builds/claim",
                headers={**AGENT_AUTH, "X-Forwarded-For": "10.0.0.5"},
                json={"agent_id": "agent-A"},
            )
            self.assertEqual(res.status_code, 403)
        finally:
            main_module.settings.BUILD_AGENT_ALLOWED_IPS = []


class BuildStoreGuardTests(unittest.TestCase):
    """Store-level guards for the build state machine (TOCTOU hardening)."""

    def setUp(self):
        store.init_db()
        self.img = store.create_custom_image(2, "guard", "registry/guard:1", "FROM scratch", 5)

    def tearDown(self):
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM custom_images")

    def test_reaper_does_not_clobber_finalized_build(self):
        # Agent claims and finalizes; the reaper (timeout=0 => everything stale) must NOT
        # flip the already-ready build back to failed, because it is no longer "building".
        store.claim_next_build("agent-A")
        store.update_custom_image_status(self.img["id"], status="ready", require_claimed_by="agent-A")
        reaped = store.reap_stale_builds(0)
        self.assertEqual(reaped, 0)
        self.assertEqual(store.get_custom_image(self.img["id"])["build_status"], "ready")

    def test_reaper_fails_genuinely_stale_building_build(self):
        store.claim_next_build("agent-A")  # leaves it in "building"
        reaped = store.reap_stale_builds(0)
        self.assertEqual(reaped, 1)
        self.assertEqual(store.get_custom_image(self.img["id"])["build_status"], "failed")

    def test_status_update_rejected_when_not_building(self):
        # require_claimed_by guard: a 'pending' (never-claimed) build cannot be driven to ready.
        result = store.update_custom_image_status(self.img["id"], status="ready", require_claimed_by="agent-A")
        self.assertIsNone(result)
        self.assertEqual(store.get_custom_image(self.img["id"])["build_status"], "pending")

    def test_claim_is_exclusive(self):
        first = store.claim_next_build("agent-A")
        self.assertIsNotNone(first)
        # No more pending builds: second claim returns None (does not hand back agent-A's row).
        second = store.claim_next_build("agent-B")
        self.assertIsNone(second)
        self.assertEqual(store.get_custom_image(self.img["id"])["claimed_by"], "agent-A")

    def test_claim_skips_already_claimed_and_returns_next_pending(self):
        # Two pending builds; the first is pre-claimed. A new claim must still lease the second
        # (liveness): it walks past the already-claimed row rather than returning None.
        second_img = store.create_custom_image(2, "guard2", "registry/guard2:1", "FROM scratch", 5)
        # Simulate the first build already claimed by another agent (no longer "pending").
        store.claim_next_build("other")
        claimed = store.claim_next_build("agent-A")
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["id"], second_img["id"])
        self.assertEqual(claimed["claimed_by"], "agent-A")


if __name__ == "__main__":
    unittest.main()
