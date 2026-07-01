"""P4b: manager-executed purge (dfctl-exec) + blob-list capture + unique-blob computation.

Covers the fixes made after the live investigation overturned the guessed delete primitive:
  - blob_list capture at push time (store setters + push lifecycle) and unique-blob computation
    (this image's blobs minus blobs still referenced by other live images).
  - purge_p2p / purge_seed run on the MANAGER via `dfctl task rm <task_id>` (task_id == blob hex),
    NOT on the 0042 agent (which can't reach overlay seeds; dfget has no delete).
  - the manager drain loop claims [purge_p2p, purge_seed, purge_meta], executes, and finishes.
"""
import json
import os
import tempfile
import unittest
from types import SimpleNamespace

_DB_FD, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.close(_DB_FD)
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["ADMIN_PASSWORD"] = "testpass"
os.environ["NOTEBOOK_NODE_NAME"] = "fake-node"
os.environ["IMAGE_SERVICE_ENABLED"] = "true"
os.environ["PURGE_FANOUT_ENABLED"] = "true"

from tests.kube_stub import install  # noqa: E402

install()

from app import store  # noqa: E402
from app import purge_exec  # noqa: E402

REF = "10.5.10.43:5000/radeon-cloud/user-1:tag"
BLOBS = ["aaaa", "bbbb", "cccc"]  # bare hex task-ids (== blob digests)


def _reset():
    store.init_db()
    with store.engine.begin() as conn:
        for tbl in (store.image_jobs, store.image_nodes, store.images, store.custom_images):
            conn.execute(tbl.delete())


# ---- Fake k8s core_v1 for exec tests -------------------------------------------------------------

class FakePod:
    def __init__(self, name):
        self.metadata = SimpleNamespace(name=name)


class FakeCoreV1:
    """Records exec calls; returns scripted dfctl output per (pod, task_id)."""

    def __init__(self, pods_by_selector, exec_output=""):
        self._pods = pods_by_selector  # {selector: [FakePod,...]} ; field_selector filters by suffix
        self.exec_output = exec_output  # str OR callable(pod, container, cmd)->str
        self.calls = []

    def list_namespaced_pod(self, ns, label_selector=None, field_selector=None):
        pods = list(self._pods.get(label_selector, []))
        if field_selector and field_selector.startswith("spec.nodeName="):
            node = field_selector.split("=", 1)[1]
            pods = [p for p in pods if getattr(p, "_node", None) == node or node in p.metadata.name]
        return SimpleNamespace(items=pods)

    def connect_get_namespaced_pod_exec(self, pod, ns, container=None, command=None, **kw):
        self.calls.append({"pod": pod, "container": container, "command": command})
        if callable(self.exec_output):
            return self.exec_output(pod, container, command)
        return self.exec_output


class BlobCaptureTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_set_and_normalize_image_blob_list(self):
        with store.engine.begin() as conn:
            conn.execute(store.images.insert().values(
                name="i", image=REF, created_at=store.utc_now(), updated_at=store.utc_now()))
            iid = conn.execute(store.select(store.images.c.id).where(store.images.c.image == REF)).scalar()
        # digests with sha256: prefix must be normalized to bare hex
        store.set_image_blob_list(iid, ["sha256:aaaa", "bbbb", "sha256:aaaa"])  # dup collapses
        with store.engine.begin() as conn:
            raw = conn.execute(store.select(store.images.c.blob_list).where(store.images.c.id == iid)).scalar()
        self.assertEqual(json.loads(raw), ["aaaa", "bbbb"])

    def test_unique_blob_computation_excludes_shared(self):
        # image X (REF) has aaaa,bbbb,cccc ; image Y shares bbbb -> unique(X) = aaaa,cccc
        now = store.utc_now()
        with store.engine.begin() as conn:
            conn.execute(store.images.insert().values(name="x", image=REF, blob_list=json.dumps(["aaaa", "bbbb", "cccc"]), created_at=now, updated_at=now))
            conn.execute(store.images.insert().values(name="y", image="other:tag", blob_list=json.dumps(["bbbb", "dddd"]), created_at=now, updated_at=now))
        self.assertEqual(sorted(store.unique_blob_ids_for_ref(REF)), ["aaaa", "cccc"])

    def test_unique_blob_empty_when_no_blob_list(self):
        now = store.utc_now()
        with store.engine.begin() as conn:
            conn.execute(store.images.insert().values(name="x", image=REF, created_at=now, updated_at=now))
        self.assertEqual(store.unique_blob_ids_for_ref(REF), [])

    def test_unique_blob_custom_image_side(self):
        now = store.utc_now()
        with store.engine.begin() as conn:
            conn.execute(store.custom_images.insert().values(
                user_id=1, name="c", image=REF, dockerfile="FROM x", build_status="ready",
                blob_list=json.dumps(["aaaa", "bbbb"]), created_at=now, updated_at=now))
            conn.execute(store.images.insert().values(
                name="shared", image="z:tag", blob_list=json.dumps(["bbbb"]), created_at=now, updated_at=now))
        self.assertEqual(store.unique_blob_ids_for_ref(REF), ["aaaa"])


class ManagerPurgeP2PTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_p2p_execs_dfctl_per_blob_per_node(self):
        core = FakeCoreV1({"app=dragonfly,component=client": [FakePod("dragonfly-client-A"), FakePod("dragonfly-client-B")]},
                          exec_output="")  # empty => success
        job = {"id": 1, "ref": REF, "payload": {"targets": [{"node": "A"}, {"node": "B"}], "blob_ids": BLOBS}}
        res = purge_exec.run_purge_p2p_manager(core, job)
        self.assertTrue(all(n["removed"] for n in res["nodes"]))
        # 2 nodes x 3 blobs = 6 dfctl task rm calls
        self.assertEqual(len(core.calls), 6)
        self.assertTrue(all(c["command"][:3] == ["dfctl", "task", "rm"] for c in core.calls))

    def test_p2p_task_not_found_is_success(self):
        core = FakeCoreV1({"app=dragonfly,component=client": [FakePod("dragonfly-client-A")]},
                          exec_output="task not found")
        job = {"id": 1, "ref": REF, "payload": {"targets": [{"node": "A"}], "blob_ids": ["aaaa"]}}
        res = purge_exec.run_purge_p2p_manager(core, job)
        self.assertTrue(res["nodes"][0]["removed"])

    def test_p2p_real_error_fails(self):
        core = FakeCoreV1({"app=dragonfly,component=client": [FakePod("dragonfly-client-A")]},
                          exec_output="error: connection refused")
        job = {"id": 1, "ref": REF, "payload": {"targets": [{"node": "A"}], "blob_ids": ["aaaa"]}}
        res = purge_exec.run_purge_p2p_manager(core, job)
        self.assertFalse(res["nodes"][0]["removed"])
        self.assertIsNotNone(res["nodes"][0]["error"])

    def test_p2p_no_daemon_on_node_is_success(self):
        # node with no dfdaemon pod => nothing to purge there => removed True
        core = FakeCoreV1({"app=dragonfly,component=client": []}, exec_output="")
        job = {"id": 1, "ref": REF, "payload": {"targets": [{"node": "Z"}], "blob_ids": ["aaaa"]}}
        res = purge_exec.run_purge_p2p_manager(core, job)
        self.assertTrue(res["nodes"][0]["removed"])
        self.assertEqual(len(core.calls), 0)

    def test_p2p_no_blob_ids_skips(self):
        core = FakeCoreV1({"app=dragonfly,component=client": [FakePod("dragonfly-client-A")]}, exec_output="")
        job = {"id": 1, "ref": REF, "payload": {"targets": [{"node": "A"}], "blob_ids": []}}
        res = purge_exec.run_purge_p2p_manager(core, job)
        self.assertEqual(res.get("skipped"), "no_blob_ids")
        self.assertEqual(len(core.calls), 0)


class ManagerPurgeSeedTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_seed_execs_all_seeds(self):
        core = FakeCoreV1({"app=dragonfly,component=seed-client": [FakePod("dragonfly-seed-client-0"), FakePod("dragonfly-seed-client-1"), FakePod("dragonfly-seed-client-2")]}, exec_output="")
        job = {"id": 1, "ref": REF, "payload": {"blob_ids": BLOBS}}
        res = purge_exec.run_purge_seed_manager(core, job)
        self.assertTrue(res["seed_deleted"])
        self.assertEqual(len(core.calls), 9)  # 3 seeds x 3 blobs
        self.assertTrue(all(c["container"] == "seed-client" for c in core.calls))

    def test_seed_one_failure_fails_job(self):
        def out(pod, container, command):
            return "error: boom" if pod.endswith("-1") else ""
        core = FakeCoreV1({"app=dragonfly,component=seed-client": [FakePod("dragonfly-seed-client-0"), FakePod("dragonfly-seed-client-1")]}, exec_output=out)
        job = {"id": 1, "ref": REF, "payload": {"blob_ids": ["aaaa"]}}
        res = purge_exec.run_purge_seed_manager(core, job)
        self.assertFalse(res["seed_deleted"])

    def test_seed_no_blob_ids_skips(self):
        core = FakeCoreV1({"app=dragonfly,component=seed-client": [FakePod("dragonfly-seed-client-0")]}, exec_output="")
        job = {"id": 1, "ref": REF, "payload": {"blob_ids": []}}
        res = purge_exec.run_purge_seed_manager(core, job)
        self.assertEqual(res.get("skipped"), "no_blob_ids")
        self.assertEqual(len(core.calls), 0)


class DrainLoopTests(unittest.TestCase):
    """End-to-end: enqueue the fan-out, run agent-side surfaces, then the manager drain loop finishes
    purge_p2p/seed/meta and the delete converges (rows dropped)."""

    def setUp(self):
        _reset()

    def test_drain_finishes_manager_surfaces_and_purge_meta(self):
        from app import scheduler
        # Seed an image_nodes row so purge_meta has something to drop.
        store.upsert_image_node(node_name="A", image_ref=REF, status="loaded")
        # Enqueue the 6-surface fan-out with blob_ids on the cache surfaces.
        from app import main as main_module
        main_module.enqueue_purge_fanout(REF, [{"node": "A", "ip": "1"}], digest="sha256:d",
                                         lan_target_ref=REF, image_id=None, blob_ids=BLOBS)
        # Agent-owned surfaces succeed (purge_node/registry_delete/purge_builder).
        for kind in ("purge_node", "registry_delete", "purge_builder"):
            j = store.claim_next_image_job("agent-x", kinds=[kind])
            self.assertIsNotNone(j, f"{kind} should be claimable by agent")
            store.finish_image_job(j["id"], "succeeded", "agent-x")

        # Patch the manager drain to use a fake core_v1 (all execs succeed).
        core = FakeCoreV1({
            "app=dragonfly,component=client": [FakePod("dragonfly-client-A")],
            "app=dragonfly,component=seed-client": [FakePod("dragonfly-seed-client-0")],
        }, exec_output="")
        import app.k8s_client as kc
        orig = kc.k8s_client.core_v1
        kc.k8s_client.core_v1 = core
        try:
            processed = scheduler._drain_manager_purges_sync()
        finally:
            kc.k8s_client.core_v1 = orig

        # purge_p2p + purge_seed + purge_meta => 3 processed (purge_meta only after the two succeed).
        self.assertGreaterEqual(processed, 3)
        # All purge jobs terminal-succeeded.
        with store.engine.begin() as conn:
            rows = conn.execute(store.select(store.image_jobs.c.kind, store.image_jobs.c.status)).all()
        by_kind = {k: s for k, s in rows}
        for k in ("purge_node", "purge_p2p", "purge_seed", "registry_delete", "purge_builder", "purge_meta"):
            self.assertEqual(by_kind.get(k), "succeeded", f"{k} not succeeded: {by_kind.get(k)}")
        # purge_meta dropped the image_nodes row.
        self.assertEqual(store.list_nodes_for_image(REF), [])

    def test_purge_meta_deferred_until_manager_surfaces_succeed(self):
        from app import scheduler, main as main_module
        store.upsert_image_node(node_name="A", image_ref=REF, status="loaded")
        main_module.enqueue_purge_fanout(REF, [{"node": "A", "ip": "1"}], digest="sha256:d",
                                         lan_target_ref=REF, image_id=None, blob_ids=BLOBS)
        # Only agent surfaces succeed; p2p/seed NOT yet run.
        for kind in ("purge_node", "registry_delete", "purge_builder"):
            j = store.claim_next_image_job("agent-x", kinds=[kind])
            store.finish_image_job(j["id"], "succeeded", "agent-x")
        # purge_meta must NOT be claimable yet (p2p/seed pending).
        self.assertIsNone(store.claim_next_image_job("m", kinds=["purge_meta"]))
        # Row still present.
        self.assertEqual(store.list_nodes_for_image(REF), ["A"])


if __name__ == "__main__":
    unittest.main()
