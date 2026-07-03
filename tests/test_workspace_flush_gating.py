"""DATA-LOSS-CRITICAL regression for the per-(instance,node) workspace_local_copy flush ledger:
- the reaper must NEVER select a copy whose flush hasn't confirmed (flushed_at NULL), even past TTL;
- a stale-token flush confirm must NOT falsely certify a newer session;
- a cross-node relaunch must NOT orphan the old node's unflushed copy.
Uses a throwaway SQLite DB."""
import os
import tempfile
import unittest


class LocalCopyFlushTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        cls._tmp.close()
        from app import store
        from sqlalchemy import create_engine
        store.engine = create_engine(f"sqlite:///{cls._tmp.name}", future=True)
        cls.store = store
        store.metadata.create_all(store.engine)

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink(cls._tmp.name)
        except OSError:
            pass

    def setUp(self):
        with self.store.engine.begin() as conn:
            conn.execute(self.store.workspace_cache_state.delete())
            conn.execute(self.store.workspace_local_copy.delete())

    def _backdate_copy(self, instance_id, node_name, minutes_ago):
        from datetime import datetime, timezone, timedelta
        ts = (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()
        with self.store.engine.begin() as conn:
            conn.execute(
                self.store.update(self.store.workspace_local_copy)
                .where(
                    self.store.workspace_local_copy.c.instance_id == instance_id,
                    self.store.workspace_local_copy.c.node_name == node_name,
                )
                .values(session_token=ts)
            )
        return ts

    def test_unflushed_copy_never_reaped_past_ttl(self):
        s = self.store
        s.stamp_workspace_stopped("nb-unflushed", "node-1")   # flushed_at stays NULL
        self._backdate_copy("nb-unflushed", "node-1", 999)     # way past any TTL
        ids = {(r["instance_id"], r["node_name"]) for r in s.list_workspace_cache_to_reap(1)}
        self.assertNotIn(("nb-unflushed", "node-1"), ids,
                         "UNFLUSHED copy must NOT be reaped — that would destroy unflushed user data")

    def test_flushed_copy_reaped_after_ttl(self):
        s = self.store
        token = s.stamp_workspace_stopped("nb-flushed", "node-1")
        self.assertTrue(s.mark_workspace_flushed("nb-flushed", "node-1", token))
        self._backdate_copy("nb-flushed", "node-1", 999)
        ids = {(r["instance_id"], r["node_name"]) for r in s.list_workspace_cache_to_reap(1)}
        self.assertIn(("nb-flushed", "node-1"), ids, "a flushed, past-TTL copy SHOULD be reapable")

    def test_flushed_within_ttl_not_reaped(self):
        s = self.store
        token = s.stamp_workspace_stopped("nb-recent", "node-1")
        s.mark_workspace_flushed("nb-recent", "node-1", token)
        ids = {(r["instance_id"], r["node_name"]) for r in s.list_workspace_cache_to_reap(120)}
        self.assertNotIn(("nb-recent", "node-1"), ids)

    def test_stale_token_mark_is_noop(self):
        # A straggler flush from an OLD session (old token) must not mark the current (re-stopped) copy.
        s = self.store
        old_token = s.stamp_workspace_stopped("nb-x", "node-1")
        # Relaunch (clears stopped_at) then stop again → new token, flushed_at reset to NULL.
        s.stamp_workspace_running("nb-x", "node-1")
        new_token = s.stamp_workspace_stopped("nb-x", "node-1")
        self.assertNotEqual(old_token, new_token)
        # The straggler tries to confirm with the OLD token → must be a no-op.
        self.assertFalse(s.mark_workspace_flushed("nb-x", "node-1", old_token),
                         "stale-token confirm must NOT mark the current session flushed")
        # Backdate the current copy past the TTL. session_token doubles as the TTL clock AND the fence
        # token (in prod both are the stop timestamp), so capture the new value to confirm against.
        aged_token = self._backdate_copy("nb-x", "node-1", 999)
        ids = {(r["instance_id"], r["node_name"]) for r in s.list_workspace_cache_to_reap(1)}
        self.assertNotIn(("nb-x", "node-1"), ids,
                         "current session still unflushed → must not be reapable after a stale confirm")
        # An even-older stale token still must not mark it.
        self.assertFalse(s.mark_workspace_flushed("nb-x", "node-1", old_token))
        # The correct (current) confirm marks it → now reapable.
        self.assertTrue(s.mark_workspace_flushed("nb-x", "node-1", aged_token))
        ids = {(r["instance_id"], r["node_name"]) for r in s.list_workspace_cache_to_reap(1)}
        self.assertIn(("nb-x", "node-1"), ids)

    def test_cross_node_relaunch_keeps_old_node_copy(self):
        # Session stops on node A (unflushed), relaunch lands on node B and stops there.
        s = self.store
        s.stamp_workspace_stopped("nb-move", "node-A")   # A: unflushed
        s.stamp_workspace_running("nb-move", None)         # relaunch (node unknown at create)
        tokenB = s.stamp_workspace_stopped("nb-move", "node-B")  # B: unflushed
        pending = {(r["instance_id"], r["node_name"]) for r in s.list_workspace_unflushed_copies()}
        self.assertIn(("nb-move", "node-A"), pending, "old node's unflushed copy must NOT be orphaned")
        self.assertIn(("nb-move", "node-B"), pending)
        # Flushing B must not reap A.
        s.mark_workspace_flushed("nb-move", "node-B", tokenB)
        self._backdate_copy("nb-move", "node-A", 999)
        self._backdate_copy("nb-move", "node-B", 999)
        ids = {(r["instance_id"], r["node_name"]) for r in s.list_workspace_cache_to_reap(1)}
        self.assertIn(("nb-move", "node-B"), ids)
        self.assertNotIn(("nb-move", "node-A"), ids, "A is still unflushed → must not be reapable")

    def test_relaunch_resets_flushed_state_same_node(self):
        # Same-node relaunch after a flush: the new session's copy must start UNFLUSHED.
        s = self.store
        t1 = s.stamp_workspace_stopped("nb-r", "node-1")
        s.mark_workspace_flushed("nb-r", "node-1", t1)
        s.stamp_workspace_running("nb-r", "node-1")
        t2 = s.stamp_workspace_stopped("nb-r", "node-1")
        self._backdate_copy("nb-r", "node-1", 999)
        ids = {(r["instance_id"], r["node_name"]) for r in s.list_workspace_cache_to_reap(1)}
        self.assertNotIn(("nb-r", "node-1"), ids,
                         "a fresh session's copy must be unflushed until its own flush confirms")
        self.assertIn(("nb-r", "node-1"),
                      {(r["instance_id"], r["node_name"]) for r in s.list_workspace_unflushed_copies()})

    def test_unflushed_listed_for_retry(self):
        s = self.store
        s.stamp_workspace_stopped("nb-retry", "node-9")
        td = s.stamp_workspace_stopped("nb-done", "node-9")
        s.mark_workspace_flushed("nb-done", "node-9", td)
        pending = {r["instance_id"] for r in s.list_workspace_unflushed_copies()}
        self.assertIn("nb-retry", pending)
        self.assertNotIn("nb-done", pending)


if __name__ == "__main__":
    unittest.main()
