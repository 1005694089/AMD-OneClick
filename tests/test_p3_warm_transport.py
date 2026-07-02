"""P3 warm-transport tests for the Dragonfly migration.

Covers:
  - `warm` is a claimable job kind and is serialized per-node (in SERIALIZED_KINDS from P0).
  - The warm lifecycle branch writes the SAME image_nodes 'loaded' rows distribute writes
    (upload->ready-on-all counting is transport-agnostic).
  - _enqueue_next_chain_step resolves warm targets like distribute AND carries lan_target_ref.
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


def _reset():
    store.init_db()
    with store.engine.begin() as conn:
        for tbl in (store.image_jobs, store.image_nodes):
            conn.execute(tbl.delete())


class WarmKindTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_warm_is_a_job_kind(self):
        self.assertIn("warm", store.IMAGE_JOB_KINDS)

    def test_warm_serialized_per_node(self):
        # From P0: warm is in SERIALIZED_KINDS, so two warms to the same node don't run concurrently.
        self.assertIn("warm", store.SERIALIZED_KINDS)
        t = [{"node": "A", "ip": "1"}]
        store.enqueue_image_job(kind="warm", ref="img:a", payload={"targets": t})
        store.enqueue_image_job(kind="warm", ref="img:b", payload={"targets": t})
        first = store.claim_next_image_job("agent-1")
        self.assertEqual(first["ref"], "img:a")
        second = store.claim_next_image_job("agent-2")
        self.assertIsNone(second)  # same node busy -> deferred
        store.finish_image_job(first["id"], "succeeded", "agent-1")
        third = store.claim_next_image_job("agent-2")
        self.assertEqual(third["ref"], "img:b")


class WarmLifecycleTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_warm_writes_loaded_rows_like_distribute(self):
        ref = "10.5.10.43:5000/repo:tag"
        job = {"kind": "warm", "ref": ref, "payload": {}}
        result = {"ref": ref, "nodes": [
            {"node": "nodeA", "ip": "1", "loaded": True, "size_bytes": 123},
            {"node": "nodeB", "ip": "2", "loaded": False, "error": "pull_failed"},
        ]}
        main_module._sync_image_job_lifecycle(job, result)
        loaded = set(store.list_nodes_for_image(ref))
        self.assertIn("nodeA", loaded)          # loaded node recorded
        self.assertNotIn("nodeB", loaded)       # failed node not recorded


class WarmChainStepTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_next_step_warm_resolves_targets_and_carries_lan_ref(self):
        # A push job whose chain's next step is warm must get targets resolved (via warm branch) AND
        # lan_target_ref carried. Stub _resolve_chain_targets (real one needs kubectl) to isolate the
        # chain-wiring logic under test.
        ref = "reg/image:tag"
        lan = "10.5.10.43:5000/reg/image:tag"
        orig = main_module._resolve_chain_targets
        main_module._resolve_chain_targets = lambda scope: [{"node": "nodeA", "ip": "1"}]
        try:
            job = {
                "kind": "push", "ref": ref, "image_id": None, "custom_image_id": None,
                "payload": {"chain": ["warm"], "scope": "all", "lan_target_ref": lan},
            }
            main_module._enqueue_next_chain_step(job, job["payload"])
        finally:
            main_module._resolve_chain_targets = orig
        import json
        with store.engine.begin() as conn:
            row = conn.execute(
                store.image_jobs.select()
                .where(store.image_jobs.c.kind == "warm", store.image_jobs.c.ref == ref)
            ).mappings().first()
        self.assertIsNotNone(row)
        payload = json.loads(row["payload"])
        # warm went through the target-resolving branch (targets present) + lan ref carried forward.
        self.assertEqual(payload["targets"], [{"node": "nodeA", "ip": "1"}])
        self.assertEqual(payload["lan_target_ref"], lan)


if __name__ == "__main__":
    unittest.main()
