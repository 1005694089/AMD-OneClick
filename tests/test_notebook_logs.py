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


if __name__ == "__main__":
    unittest.main()
