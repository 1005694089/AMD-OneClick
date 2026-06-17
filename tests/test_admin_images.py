import os
import tempfile
import unittest

# (a) Set env before any app import.
_DB_FD, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.close(_DB_FD)
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["ADMIN_PASSWORD"] = "testpass"
os.environ["NOTEBOOK_NODE_NAME"] = "fake-node"
os.environ["IMAGE_PULL_PROBE_ENABLED"] = "true"
os.environ["IMAGE_PREPULL_ENABLED"] = "false"

# (b) Install the kube stub before importing app.main.
from tests.kube_stub import install

install()

from kubernetes.client.rest import ApiException  # noqa: E402

# (c) Import the app only after env + stub are in place. main.py uses a relative
# StaticFiles(directory="static"), so tests must run from the project root.
from fastapi.testclient import TestClient  # noqa: E402

from app import main as main_module  # noqa: E402
from app import store  # noqa: E402

AUTH = ("admin", "testpass")


class AdminImageRouteTests(unittest.TestCase):
    def setUp(self):
        # settings is a shared singleton that may have been imported (with
        # different env) by another test module first, so pin the values here.
        self._orig = (
            main_module.settings.ADMIN_PASSWORD,
            main_module.settings.IMAGE_PULL_PROBE_ENABLED,
            main_module.settings.IMAGE_PREPULL_ENABLED,
            main_module.settings.NOTEBOOK_NODE_NAME,
        )
        main_module.settings.ADMIN_PASSWORD = "testpass"
        main_module.settings.IMAGE_PULL_PROBE_ENABLED = True
        main_module.settings.IMAGE_PREPULL_ENABLED = False
        main_module.settings.NOTEBOOK_NODE_NAME = "fake-node"
        store.init_db()
        self.client = TestClient(main_module.app)
        self.image = store.upsert_image("demo", "registry/image:tag", "", True)

    def tearDown(self):
        (
            main_module.settings.ADMIN_PASSWORD,
            main_module.settings.IMAGE_PULL_PROBE_ENABLED,
            main_module.settings.IMAGE_PREPULL_ENABLED,
            main_module.settings.NOTEBOOK_NODE_NAME,
        ) = self._orig
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM images")

    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(_DB_PATH)
        except OSError:
            pass

    def test_sync_route_maps_apiexception_status(self):
        def boom(image_id, image):
            raise ApiException(status=409, reason="being deleted")

        orig = main_module.k8s_client.sync_image_to_nodes
        main_module.k8s_client.sync_image_to_nodes = boom
        try:
            res = self.client.post(f"/api/admin/images/{self.image['id']}/sync", auth=AUTH)
        finally:
            main_module.k8s_client.sync_image_to_nodes = orig

        self.assertEqual(res.status_code, 409)
        self.assertIn("being deleted", res.json()["detail"])

    def test_delete_route_handles_runtime_error(self):
        def boom(image_id):
            raise RuntimeError("Refusing to delete unmanaged pull probe pod")

        orig = main_module.k8s_client.delete_image_sync
        main_module.k8s_client.delete_image_sync = boom
        try:
            res = self.client.delete(f"/api/admin/images/{self.image['id']}", auth=AUTH)
        finally:
            main_module.k8s_client.delete_image_sync = orig

        self.assertEqual(res.status_code, 500)
        self.assertIn("unmanaged", res.json()["detail"])

    def test_list_images_survives_single_probe_failure(self):
        def boom(image_id, image):
            raise ApiException(status=500, reason="kube down")

        orig = main_module.k8s_client.get_image_sync_status
        main_module.k8s_client.get_image_sync_status = boom
        try:
            res = self.client.get("/api/admin/images", auth=AUTH)
        finally:
            main_module.k8s_client.get_image_sync_status = orig

        self.assertEqual(res.status_code, 200)
        images = res.json()["images"]
        self.assertTrue(any(img["id"] == self.image["id"] for img in images))


if __name__ == "__main__":
    unittest.main()
