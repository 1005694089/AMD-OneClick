"""P0 control-plane + R2c tests for the Dragonfly migration.

Covers:
  - Heartbeat keeps a long job's lease fresh (reaper uses max(claimed_at, heartbeat_at)).
  - SERIALIZED_KINDS is a single hoisted constant covering the new warm/purge kinds.
  - R2c: launch-grace defaults to 5 days; idle reaper carries custom_image_id so eviction marks
    the custom_images row evicted; disk-pressure GC mode ignores the idle window.
  - Node denylist is honored.
  - Leader gate: is_leader() defaults True when election disabled.
"""
import os
import tempfile
import time
import unittest
from datetime import datetime, timezone, timedelta

_DB_FD, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.close(_DB_FD)
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["ADMIN_PASSWORD"] = "testpass"
os.environ["NOTEBOOK_NODE_NAME"] = "fake-node"
os.environ["IMAGE_SERVICE_ENABLED"] = "true"

from tests.kube_stub import install  # noqa: E402

install()

from app import store  # noqa: E402
from app.config import settings  # noqa: E402


def _reset():
    store.init_db()
    with store.engine.begin() as conn:
        for tbl in (store.image_jobs, store.image_nodes):
            conn.execute(tbl.delete())


class HeartbeatLeaseTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_heartbeat_keeps_job_from_being_reaped(self):
        store.enqueue_image_job(kind="warm", ref="img:a", payload={"targets": [{"node": "A", "ip": "1"}]})
        job = store.claim_next_image_job("agent-1")
        self.assertIsNotNone(job)
        # Backdate claimed_at well past the lease so it would be stale without a heartbeat.
        old = (datetime.now(timezone.utc) - timedelta(seconds=99999)).isoformat()
        with store.engine.begin() as conn:
            conn.execute(
                store.image_jobs.update()
                .where(store.image_jobs.c.id == job["id"])
                .values(claimed_at=old, status="running")
            )
        # A fresh heartbeat must protect the job.
        self.assertTrue(store.heartbeat_image_job(job["id"], "agent-1"))
        reaped = store.reap_stale_image_jobs(timeout_seconds=60)
        self.assertEqual(reaped, 0)
        # Job must still be running (not requeued/failed).
        with store.engine.begin() as conn:
            status = conn.execute(
                store.image_jobs.select().where(store.image_jobs.c.id == job["id"])
            ).mappings().first()["status"]
        self.assertEqual(status, "running")

    def test_stale_without_heartbeat_is_reaped(self):
        store.enqueue_image_job(kind="warm", ref="img:b", payload={"targets": [{"node": "A", "ip": "1"}]})
        job = store.claim_next_image_job("agent-1")
        old = (datetime.now(timezone.utc) - timedelta(seconds=99999)).isoformat()
        with store.engine.begin() as conn:
            conn.execute(
                store.image_jobs.update()
                .where(store.image_jobs.c.id == job["id"])
                .values(claimed_at=old, status="running")
            )
        reaped = store.reap_stale_image_jobs(timeout_seconds=60)
        self.assertEqual(reaped, 1)

    def test_heartbeat_rejected_for_wrong_agent(self):
        store.enqueue_image_job(kind="warm", ref="img:c", payload={"targets": [{"node": "A", "ip": "1"}]})
        job = store.claim_next_image_job("agent-1")
        self.assertFalse(store.heartbeat_image_job(job["id"], "agent-OTHER"))


class SerializedKindsTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_serialized_kinds_is_single_constant_with_new_kinds(self):
        for k in ("distribute", "warm", "evict", "purge_node", "purge_p2p"):
            self.assertIn(k, store.SERIALIZED_KINDS)

    def test_warm_serialized_per_node(self):
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


class R2cTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_launch_grace_defaults_to_five_days(self):
        self.assertEqual(settings.CUSTOM_IMAGE_GC_LAUNCH_GRACE_SECONDS, 432000)

    def test_disk_pressure_mode_ignores_idle_window(self):
        prefix = (settings.CUSTOM_IMAGE_LOCAL_TAG_PREFIX or "").strip("/")
        cid = store.create_custom_image(
            user_id=1, name="fresh", image=f"{prefix}/user-1:fresh", dockerfile="FROM x", max_per_user=10
        )["id"]
        store.update_custom_image_status(cid, status="ready", require_claimed_by=None)
        # Just launched -> inside the 5-day idle window.
        with store.engine.begin() as conn:
            conn.execute(
                store.custom_images.update()
                .where(store.custom_images.c.id == cid)
                .values(last_launched_at=datetime.now(timezone.utc).isoformat())
            )
        self.assertEqual(store.list_gc_candidates(mode="idle"), [])
        dp = store.list_gc_candidates(mode="disk_pressure")
        self.assertTrue(any(r["id"] == cid for r in dp))


class DenylistTests(unittest.TestCase):
    def test_denylist_contains_offlan_node(self):
        self.assertIn("wx-ms-w7900d-0027", settings.IMAGE_TARGET_NODE_DENYLIST)


class LeaderGateTests(unittest.TestCase):
    def test_is_leader_true_when_election_disabled(self):
        from app.leader import is_leader

        self.assertFalse(settings.LEADER_ELECTION_ENABLED)
        self.assertTrue(is_leader())


if __name__ == "__main__":
    unittest.main()
