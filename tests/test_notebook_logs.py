import os
import tempfile
import unittest

# (a) Set env before any app import.
_DB_FD, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.close(_DB_FD)
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["ADMIN_PASSWORD"] = "testpass"
os.environ["NOTEBOOK_NODE_NAME"] = "fake-node"

# (b) Install the kube stub before importing app.main.
from tests.kube_stub import install  # noqa: E402

install()

from fastapi.testclient import TestClient  # noqa: E402

from app import main as main_module  # noqa: E402
from app import store  # noqa: E402


class NotebookLogsRouteTests(unittest.TestCase):
    """GET /api/notebook/logs returns events + container stdout for the active instance."""

    def setUp(self):
        store.init_db()
        # Auth: override current_user so the route sees a logged-in user.
        self._fake_user = {"id": 1, "email": "u@example.test", "name": "U"}
        main_module.app.dependency_overrides[main_module.current_user] = lambda: self._fake_user
        self.client = TestClient(main_module.app)
        self._orig_active = main_module.get_active_instance_for_user
        self._orig_logs = main_module.k8s_client.get_pod_logs

    def tearDown(self):
        main_module.app.dependency_overrides.pop(main_module.current_user, None)
        main_module.get_active_instance_for_user = self._orig_active
        main_module.k8s_client.get_pod_logs = self._orig_logs

    def test_logs_returns_payload_for_active_instance(self):
        main_module.get_active_instance_for_user = lambda uid: {"instance_id": "nb-1"}
        payload = {
            "events": [{"time": "2026-06-16T00:00:00+00:00", "reason": "Pulling", "message": "Pulling image"}],
            "container": "hello\n",
        }
        captured = {}
        def fake_logs(instance_id, *a, **k):
            captured["id"] = instance_id
            return payload
        main_module.k8s_client.get_pod_logs = fake_logs

        res = self.client.get("/api/notebook/logs")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), payload)
        self.assertEqual(captured["id"], "nb-1")

    def test_logs_not_found_without_active_instance(self):
        main_module.get_active_instance_for_user = lambda uid: None
        res = self.client.get("/api/notebook/logs")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), {"events": [], "container": "", "status": "not_found"})

    def test_logs_error_is_swallowed(self):
        main_module.get_active_instance_for_user = lambda uid: {"instance_id": "nb-1"}
        def boom(instance_id, *a, **k):
            raise RuntimeError("kube down")
        main_module.k8s_client.get_pod_logs = boom
        res = self.client.get("/api/notebook/logs")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["status"], "error")


class CustomImageCapTests(unittest.TestCase):
    """Req 2: per-user custom-build cap. create_custom_image enforces max_per_user."""

    def setUp(self):
        store.init_db()

    def tearDown(self):
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM custom_images")

    def test_cap_of_one_rejects_second_active_build(self):
        store.create_custom_image(42, "first", "registry/first:1", "FROM scratch", 1)
        with self.assertRaises(ValueError):
            store.create_custom_image(42, "second", "registry/second:1", "FROM scratch", 1)

    def test_default_cap_is_one(self):
        # The shipped default must be 1.
        self.assertEqual(main_module.settings.CUSTOM_IMAGE_MAX_PER_USER, 1)

    def test_concurrent_different_name_builds_respect_cap(self):
        # Two concurrent requests with different names must not both slip past a cap of 1.
        # The in-process lock around count-and-insert serializes them so exactly one wins.
        import threading

        barrier = threading.Barrier(2)
        outcomes = []
        lock = threading.Lock()

        def attempt(name):
            barrier.wait()
            try:
                store.create_custom_image(99, name, f"registry/{name}:1", "FROM scratch", 1)
                result = "ok"
            except ValueError:
                result = "rejected"
            with lock:
                outcomes.append(result)

        threads = [threading.Thread(target=attempt, args=(n,)) for n in ("alpha", "beta")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(sorted(outcomes), ["ok", "rejected"])
        self.assertEqual(store.count_active_custom_images(99), 1)


class GithubStatusAuthTests(unittest.TestCase):
    """GET /api/github/notebook/status returns credential-bearing fields. The instance_id is
    a low-entropy md5[:8], so the endpoint must only return them for the caller's OWN instance,
    identified by the httponly `amd_oneclick_gh_instance` cookie set at create time."""

    def setUp(self):
        self.client = TestClient(main_module.app)
        self._orig = main_module.k8s_client.get_instance_by_id
        self._orig_status = main_module.k8s_client.get_pod_status
        self._orig_details = main_module.k8s_client.get_pod_status_details
        main_module.k8s_client.get_instance_by_id = lambda iid: {
            "url": "http://h:30000/lab?token=user-visible-tok",
            "opencode_url": "http://h:30001/",
            "opencode_username": "opencode",
            "opencode_password": "deadbeef",
            "instance_id": iid,
        }
        main_module.k8s_client.get_pod_status = lambda email, instance_id=None: "ready"
        # The status handler reads get_pod_status_details (not get_pod_status); stub it so the
        # test exercises the auth gate, not the kube stub's missing read_namespaced_pod.
        main_module.k8s_client.get_pod_status_details = lambda email, instance_id=None: {"status": "ready"}

    def tearDown(self):
        main_module.k8s_client.get_instance_by_id = self._orig
        main_module.k8s_client.get_pod_status = self._orig_status
        main_module.k8s_client.get_pod_status_details = self._orig_details

    def test_status_rejected_without_matching_cookie(self):
        # No cookie at all: cannot harvest another instance's credentials by guessing the id.
        res = self.client.get("/api/github/notebook/status?instance_id=gh-deadbeef")
        self.assertEqual(res.status_code, 403)

    def test_status_rejected_when_cookie_mismatches(self):
        # Caller owns gh-aaaa but asks about gh-deadbeef.
        self.client.cookies.set("amd_oneclick_gh_instance", "gh-aaaa")
        res = self.client.get("/api/github/notebook/status?instance_id=gh-deadbeef")
        self.assertEqual(res.status_code, 403)
        self.client.cookies.clear()

    def test_status_allowed_for_own_instance(self):
        self.client.cookies.set("amd_oneclick_gh_instance", "gh-deadbeef")
        res = self.client.get("/api/github/notebook/status?instance_id=gh-deadbeef")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["opencode_url"], "http://h:30001/")
        self.assertEqual(res.json()["opencode_username"], "opencode")
        self.assertEqual(res.json()["opencode_password"], "deadbeef")
        self.client.cookies.clear()


if __name__ == "__main__":
    unittest.main()
