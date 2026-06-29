"""Tests for the image-system hardening (prepull removal + deadlock-resistant image-service).

Covers:
  - Part A: the prepull/probe surface is gone; sync_image_to_nodes only enqueues a distribute job;
    get_image_sync_status is image-service shaped; removed config keys are absent.
  - Part B: claim_next_image_job serializes distribute/evict per target node.
  - Part C/D: set_image_node_status / quarantine_node / quarantined_nodes; the reaper drops
    quarantined targets and fails a job whose every target is quarantined.
"""
import os
import tempfile
import unittest

_DB_FD, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.close(_DB_FD)
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["ADMIN_PASSWORD"] = "testpass"
os.environ["NOTEBOOK_NODE_NAME"] = "fake-node"
os.environ["IMAGE_SERVICE_ENABLED"] = "true"

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


class SyncEnqueuesDistributeTests(unittest.TestCase):
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
            conn.exec_driver_sql("DELETE FROM image_jobs")
            conn.exec_driver_sql("DELETE FROM image_nodes")

    def test_sync_enqueues_single_distribute_job(self):
        before = store.claim_next_image_job("probe")  # drain
        while before:
            store.finish_image_job(before["id"], "succeeded", "probe")
            before = store.claim_next_image_job("probe")
        status = self.client.sync_image_to_nodes(1, "registry/img:1")
        self.assertIn(status["status"], ("pending", "pulling", "ready"))
        job = store.claim_next_image_job("agent-A")
        self.assertIsNotNone(job)
        self.assertEqual(job["kind"], "distribute")
        self.assertEqual(job["ref"], "registry/img:1")

    def test_get_image_sync_status_is_image_service_shaped(self):
        st = self.client.get_image_sync_status(1, "registry/img:1")
        self.assertEqual(st["desired_count"], 2)
        self.assertIn("status", st)
        self.assertIn("ready_count", st)


class ClaimSerializationTests(unittest.TestCase):
    def setUp(self):
        store.init_db()
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM image_jobs")

    def tearDown(self):
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM image_jobs")

    def test_distribute_serialized_per_node(self):
        t = [{"node": "nodeX", "ip": "10.0.0.9"}]
        store.enqueue_image_job(kind="distribute", ref="img:a", payload={"targets": t})
        store.enqueue_image_job(kind="distribute", ref="img:b", payload={"targets": t})
        first = store.claim_next_image_job("agent-1")
        self.assertIsNotNone(first)
        # Second claim for the same node must be deferred while the first is in flight.
        second = store.claim_next_image_job("agent-2")
        self.assertIsNone(second)
        # Once the first finishes, the second becomes claimable.
        store.finish_image_job(first["id"], "succeeded", "agent-1")
        third = store.claim_next_image_job("agent-2")
        self.assertIsNotNone(third)
        self.assertEqual(third["ref"], "img:b")

    def test_evict_superseded_by_newer_distribute(self):
        # delete -> evict pending -> rebuild same tag -> distribute enqueued -> claiming the evict
        # must drop it (superseded), never wipe the freshly-distributed image.
        store.enqueue_image_job(kind="evict", ref="user-1:demo", payload={"targets": [{"node": "A", "ip": "1"}]})
        store.enqueue_image_job(kind="distribute", ref="user-1:demo", payload={"targets": [{"node": "A", "ip": "1"}]})
        claimed = store.claim_next_image_job("agent-1")
        # The evict (lower id) is superseded and skipped; the distribute is what gets claimed.
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["kind"], "distribute")

    def test_cross_node_distribute_runs_concurrently(self):
        store.enqueue_image_job(kind="distribute", ref="img:a", payload={"targets": [{"node": "A", "ip": "1"}]})
        store.enqueue_image_job(kind="distribute", ref="img:b", payload={"targets": [{"node": "B", "ip": "2"}]})
        first = store.claim_next_image_job("agent-1")
        second = store.claim_next_image_job("agent-2")
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertNotEqual(first["id"], second["id"])


class NodeStatusQuarantineTests(unittest.TestCase):
    def setUp(self):
        store.init_db()
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM image_nodes")
            conn.exec_driver_sql("DELETE FROM image_jobs")

    def tearDown(self):
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM image_nodes")
            conn.exec_driver_sql("DELETE FROM image_jobs")

    def test_importing_not_counted_as_loaded(self):
        store.set_image_node_status("nodeA", "img:x", "importing")
        self.assertFalse(store.image_loaded_on_node("img:x", "nodeA"))
        self.assertEqual(store.list_nodes_for_image("img:x"), [])

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

    def test_reaper_drops_quarantined_target_and_fails_when_all_quarantined(self):
        store.quarantine_node("wedged", "img:z", 1800)
        store.enqueue_image_job(kind="distribute", ref="img:z", payload={"targets": [{"node": "wedged", "ip": "9"}]})
        job = store.claim_next_image_job("agent-1")
        self.assertIsNotNone(job)
        # Force the lease to look stale so the reaper acts; timeout_seconds=0 makes any lease stale.
        store.reap_stale_image_jobs(0)
        # Re-read the job: every target quarantined => failed, not requeued.
        from sqlalchemy import select as _select
        with store.engine.begin() as conn:
            status = conn.execute(
                _select(store.image_jobs.c.status).where(store.image_jobs.c.id == job["id"])
            ).first()
        self.assertEqual(status[0], "failed")

    def test_reaper_strips_quarantined_but_keeps_surviving_target(self):
        store.quarantine_node("wedged", "img:m", 1800)
        store.enqueue_image_job(
            kind="distribute", ref="img:m",
            payload={"targets": [{"node": "wedged", "ip": "9"}, {"node": "ok", "ip": "10"}]},
        )
        job = store.claim_next_image_job("agent-1")
        store.reap_stale_image_jobs(0)
        # Surviving target => requeued to pending; claim again and assert the wedged node is gone.
        again = store.claim_next_image_job("agent-2")
        self.assertIsNotNone(again)
        nodes = store._job_target_nodes(again.get("payload"))
        self.assertIn("ok", nodes)
        self.assertNotIn("wedged", nodes)


if __name__ == "__main__":
    unittest.main()
