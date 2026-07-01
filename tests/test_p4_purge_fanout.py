"""P4 complete-delete purge fan-out tests.

The load-bearing guarantees (from the adversarial design review):
  - Six independent surface jobs are enqueued (not one evict).
  - purge_meta is NOT claimable until EVERY byte-surface purge for the ref has SUCCEEDED
    (in flight OR failed => deferred), even after a failed-then-re-enqueued-then-succeeded prereq.
  - requeue_failed_purges flips terminally-failed surface jobs back to pending (the stale reaper
    only requeues leased jobs, so failures would otherwise never retry => orphaned bytes).
  - Lifecycle: purge_node clears only removed nodes; purge_meta drops remaining rows.
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

from app import store  # noqa: E402
from app import main as main_module  # noqa: E402

REF = "10.5.10.43:5000/repo:tag"
T = [{"node": "A", "ip": "1"}, {"node": "B", "ip": "2"}]


def _reset():
    store.init_db()
    with store.engine.begin() as conn:
        for tbl in (store.image_jobs, store.image_nodes):
            conn.execute(tbl.delete())


def _finish_all(kind, ref, status):
    """Finish every claimable job of a kind for a ref (claim as an agent, then report)."""
    while True:
        j = store.claim_next_image_job("agent-x", kinds=[kind])
        if not j:
            break
        store.finish_image_job(j["id"], status, "agent-x")


class KindsTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_all_purge_kinds_registered(self):
        for k in ("purge_node", "purge_p2p", "purge_seed", "registry_delete", "purge_builder", "purge_meta"):
            self.assertIn(k, store.IMAGE_JOB_KINDS)
        # Per-node containerd ops serialized; 0042/manager-local ones not.
        self.assertIn("purge_node", store.SERIALIZED_KINDS)
        self.assertIn("purge_p2p", store.SERIALIZED_KINDS)
        for k in ("purge_seed", "registry_delete", "purge_builder", "purge_meta"):
            self.assertNotIn(k, store.SERIALIZED_KINDS)


class FanoutShapeTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_fanout_enqueues_six_surfaces_with_digest(self):
        main_module.enqueue_purge_fanout(REF, T, digest="sha256:abc", lan_target_ref=REF)
        import json
        with store.engine.begin() as conn:
            rows = conn.execute(
                store.image_jobs.select().where(store.image_jobs.c.ref == REF)
            ).mappings().all()
        kinds = sorted(r["kind"] for r in rows)
        self.assertEqual(kinds, sorted(["purge_node", "purge_p2p", "purge_seed",
                                        "registry_delete", "purge_builder", "purge_meta"]))
        # digest rides in every payload (row may be deleted before purge runs).
        for r in rows:
            self.assertEqual(json.loads(r["payload"])["digest"], "sha256:abc")


class MetaGateTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_meta_deferred_until_all_prereqs_succeed(self):
        main_module.enqueue_purge_fanout(REF, T, digest="d", lan_target_ref=REF)
        # Initially purge_meta must NOT be claimable (prereqs all pending).
        self.assertIsNone(store.claim_next_image_job("m", kinds=["purge_meta"]))
        # Finish 4 of 5 prereqs; meta still blocked.
        for k in ("purge_node", "purge_p2p", "purge_seed", "registry_delete"):
            _finish_all(k, REF, "succeeded")
        self.assertIsNone(store.claim_next_image_job("m", kinds=["purge_meta"]))
        # Finish the last one; now meta is claimable.
        _finish_all("purge_builder", REF, "succeeded")
        j = store.claim_next_image_job("m", kinds=["purge_meta"])
        self.assertIsNotNone(j)
        self.assertEqual(j["kind"], "purge_meta")

    def test_meta_blocked_while_a_prereq_failed(self):
        main_module.enqueue_purge_fanout(REF, T, digest="d", lan_target_ref=REF)
        for k in ("purge_node", "purge_p2p", "purge_seed", "registry_delete"):
            _finish_all(k, REF, "succeeded")
        # purge_builder FAILS -> meta must stay blocked (bytes may remain).
        _finish_all("purge_builder", REF, "failed")
        self.assertIsNone(store.claim_next_image_job("m", kinds=["purge_meta"]))

    def test_meta_unblocks_after_failed_then_requeued_then_succeeded(self):
        main_module.enqueue_purge_fanout(REF, T, digest="d", lan_target_ref=REF)
        for k in ("purge_node", "purge_p2p", "purge_seed", "registry_delete"):
            _finish_all(k, REF, "succeeded")
        _finish_all("purge_builder", REF, "failed")
        # Convergence: requeue the failed purge, then it succeeds -> newest row per kind is succeeded.
        n = store.requeue_failed_purges(REF)
        self.assertGreaterEqual(n, 1)
        _finish_all("purge_builder", REF, "succeeded")
        j = store.claim_next_image_job("m", kinds=["purge_meta"])
        self.assertIsNotNone(j)  # latest-per-kind gate lets it through despite the stale failed row


class RequeueFailedTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_requeue_only_failed_with_attempts_left(self):
        store.enqueue_image_job(kind="registry_delete", ref=REF, payload={"ref": REF})
        _finish_all("registry_delete", REF, "failed")
        # A failed purge is terminal; requeue flips it back to pending.
        self.assertEqual(store.requeue_failed_purges(REF), 1)
        with store.engine.begin() as conn:
            row = conn.execute(
                store.image_jobs.select().where(store.image_jobs.c.ref == REF,
                                                store.image_jobs.c.kind == "registry_delete")
                .order_by(store.image_jobs.c.id.desc())
            ).mappings().first()
        self.assertEqual(row["status"], "pending")

    def test_requeue_noop_when_succeeded(self):
        store.enqueue_image_job(kind="purge_seed", ref=REF, payload={"ref": REF})
        _finish_all("purge_seed", REF, "succeeded")
        self.assertEqual(store.requeue_failed_purges(REF), 0)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_purge_node_clears_only_removed_nodes(self):
        store.upsert_image_node(node_name="A", image_ref=REF, status="loaded")
        store.upsert_image_node(node_name="B", image_ref=REF, status="loaded")
        job = {"kind": "purge_node", "ref": REF, "payload": {}}
        result = {"ref": REF, "nodes": [
            {"node": "A", "removed": True},
            {"node": "B", "removed": False, "error": "purge_node_failed"},
        ]}
        main_module._sync_image_job_lifecycle(job, result)
        remaining = set(store.list_nodes_for_image(REF))
        self.assertNotIn("A", remaining)  # removed -> row cleared
        self.assertIn("B", remaining)     # failed -> row kept (retry handle)

    def test_purge_meta_drops_remaining_rows(self):
        store.upsert_image_node(node_name="A", image_ref=REF, status="loaded")
        # purge_meta re-checks prereqs at execution (C5): all 5 byte-surfaces must be succeeded or it
        # aborts+re-enqueues. Satisfy them first, then meta drops the remaining rows.
        main_module.enqueue_purge_fanout(REF, T, digest="d", lan_target_ref=REF)
        for k in ("purge_node", "purge_p2p", "purge_seed", "registry_delete", "purge_builder"):
            _finish_all(k, REF, "succeeded")
        job = {"kind": "purge_meta", "ref": REF, "payload": {}}
        main_module._sync_image_job_lifecycle(job, {"ref": REF})
        self.assertEqual(store.list_nodes_for_image(REF), [])

    def test_purge_meta_aborts_when_prereq_not_satisfied(self):
        # C5 guard: if prereqs are NOT all-succeeded at execution, meta must NOT drop rows.
        store.upsert_image_node(node_name="A", image_ref=REF, status="loaded")
        main_module.enqueue_purge_fanout(REF, T, digest="d", lan_target_ref=REF)
        # Only some prereqs done -> meta must abort and leave rows intact.
        _finish_all("purge_node", REF, "succeeded")
        job = {"kind": "purge_meta", "ref": REF, "payload": {}}
        main_module._sync_image_job_lifecycle(job, {"ref": REF})
        self.assertIn("A", set(store.list_nodes_for_image(REF)))


if __name__ == "__main__":
    unittest.main()
