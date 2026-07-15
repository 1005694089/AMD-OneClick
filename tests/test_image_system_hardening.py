"""Tests for image-system hardening that survive the image-service removal.

Covers:
  - Part A: the prepull/probe surface is gone; removed config keys are absent;
    get_image_sync_status is image-service shaped (Tier C preheat/scan reporting).
  - Part C/D: set_image_node_status / quarantine_node / quarantined_nodes and the
    importing-vs-loaded node-health bookkeeping (shared node-health, kept).
"""
import os
import tempfile
import unittest

_DB_FD, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.close(_DB_FD)
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["ADMIN_PASSWORD"] = "testpass"
os.environ["NOTEBOOK_NODE_NAME"] = "fake-node"

from tests.kube_stub import install  # noqa: E402

install()

from app import k8s_client as k8s_module  # noqa: E402
from app import store  # noqa: E402
from app.config import settings  # noqa: E402


class PrepullRemovedTests(unittest.TestCase):
    def test_prepull_probe_methods_removed(self):
        for name in (
            "_prepull_name", "_pull_probe_name", "_sync_image_pull_probe",
            "_get_image_pull_probe_status", "_eligible_prepull_nodes",
            "delete_custom_image_sync", "_create_image_pull_probe",
        ):
            self.assertFalse(hasattr(k8s_module.K8sClient, name), f"{name} must be removed")

    def test_prepull_config_keys_removed(self):
        for key in ("IMAGE_PREPULL_ENABLED", "IMAGE_PULL_PROBE_ENABLED", "IMAGE_PULL_PROBE_DEADLINE_SECONDS"):
            self.assertFalse(hasattr(settings, key), f"{key} must be removed from settings")

    def test_quarantine_config_present(self):
        self.assertTrue(hasattr(settings, "NODE_QUARANTINE_SECONDS"))


class ImageSyncStatusTests(unittest.TestCase):
    def setUp(self):
        store.init_db()
        self.client = object.__new__(k8s_module.K8sClient)
        self._orig_eligible = k8s_module.K8sClient._eligible_target_nodes
        k8s_module.K8sClient._eligible_target_nodes = lambda self: [
            {"node": "n1", "ip": "10.0.0.1"}, {"node": "n2", "ip": "10.0.0.2"}
        ]

    def tearDown(self):
        k8s_module.K8sClient._eligible_target_nodes = self._orig_eligible
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM image_nodes")

    def test_get_image_sync_status_is_image_service_shaped(self):
        st = self.client.get_image_sync_status(1, "registry/img:1")
        self.assertEqual(st["desired_count"], 2)
        self.assertIn("status", st)
        self.assertIn("ready_count", st)


class NodeStatusQuarantineTests(unittest.TestCase):
    def setUp(self):
        store.init_db()
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM image_nodes")

    def tearDown(self):
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM image_nodes")

    def test_importing_not_counted_as_loaded(self):
        store.set_image_node_status("nodeA", "img:x", "importing")
        self.assertFalse(store.image_loaded_on_node("img:x", "nodeA"))
        self.assertEqual(store.list_nodes_for_image("img:x"), [])

    def test_importing_does_not_downgrade_loaded(self):
        # A re-distribute of an already-loaded image marks the node importing before streaming.
        # That must NOT clobber the loaded row — otherwise a failed re-import leaves the image
        # (still on the node) reading as unavailable. Regression for the base-image 0/2 incident.
        store.upsert_image_node("nodeB", "img:base", status="loaded")
        self.assertTrue(store.image_loaded_on_node("img:base", "nodeB"))
        store.set_image_node_status("nodeB", "img:base", "importing")  # re-distribute begins
        self.assertTrue(store.image_loaded_on_node("img:base", "nodeB"))  # still loaded
        self.assertEqual(store.list_nodes_for_image("img:base"), ["nodeB"])

    def test_clear_importing_nodes_after_failed_distribute(self):
        # A fresh (never-loaded) node marked importing, then the distribute fails -> the importing
        # row is cleared so status reflects reality (not stuck importing forever).
        store.set_image_node_status("nodeC", "img:new", "importing")
        self.assertEqual(store.list_nodes_for_image("img:new"), [])  # importing != loaded
        removed = store.clear_importing_nodes("img:new")
        self.assertEqual(removed, 1)
        # A loaded row is never cleared by this.
        store.upsert_image_node("nodeD", "img:keep", status="loaded")
        self.assertEqual(store.clear_importing_nodes("img:keep"), 0)
        self.assertTrue(store.image_loaded_on_node("img:keep", "nodeD"))

    def test_quarantine_sets_and_lists(self):
        store.quarantine_node("nodeB", "img:y", 1800)
        self.assertIn("nodeB", store.quarantined_nodes())

    def test_quarantine_anchor_when_no_prior_rows(self):
        # First-import wedge: node has no image_nodes rows and the caller passes an empty ref.
        # An anchor row must still be written so the node is reported as quarantined.
        store.quarantine_node("freshNode", "", 1800)
        self.assertIn("freshNode", store.quarantined_nodes())

    def test_successful_load_clears_quarantine(self):
        store.quarantine_node("recovers", "img:q", 1800)
        self.assertIn("recovers", store.quarantined_nodes())
        # A genuine successful (re)load proves the daemon is healthy again -> quarantine cleared.
        store.upsert_image_node("recovers", "img:q", status="loaded")
        self.assertNotIn("recovers", store.quarantined_nodes())


if __name__ == "__main__":
    unittest.main()
