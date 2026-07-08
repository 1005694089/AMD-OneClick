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


class GenerationLedgerTests(unittest.TestCase):
    """Fenced-deletion generation model (Part C): the durable_generation / synced_generation columns,
    the authoritative-flush bump, superseded discard, and that the reaper stays flushed-only."""

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

    def test_fresh_instance_generation_zero(self):
        # An unmigrated / never-flushed instance reads generation 0 (accumulate-only, today's behavior).
        self.assertEqual(self.store.get_durable_generation("nb-new"), 0)

    def test_authoritative_flush_bumps_and_marks(self):
        s = self.store
        t = s.stamp_workspace_stopped("nb-a", "node-1")
        self.assertEqual(s.get_durable_generation("nb-a"), 0)
        self.assertTrue(s.mark_workspace_flushed_authoritative("nb-a", "node-1", t, 1))
        self.assertEqual(s.get_durable_generation("nb-a"), 1)
        # Marked flushed + synced=1 → out of the unflushed list.
        self.assertNotIn("nb-a", {r["instance_id"] for r in s.list_workspace_unflushed_copies()})

    def test_authoritative_stale_token_is_noop(self):
        # A straggler authoritative confirm with the WRONG token must NOT bump the generation.
        s = self.store
        s.stamp_workspace_stopped("nb-b", "node-1")
        self.assertFalse(s.mark_workspace_flushed_authoritative("nb-b", "node-1", "OLD-TOKEN", 1))
        self.assertEqual(s.get_durable_generation("nb-b"), 0, "stale token must not bump durable_generation")

    def test_authoritative_never_regresses_generation(self):
        # If durable is already ahead (5), a confirm computing a lower new_gen must not lower it.
        s = self.store
        t = s.stamp_workspace_stopped("nb-c", "node-1")
        with s.engine.begin() as conn:
            conn.execute(
                s.update(s.workspace_cache_state)
                .where(s.workspace_cache_state.c.instance_id == "nb-c")
                .values(durable_generation=5)
            )
        self.assertTrue(s.mark_workspace_flushed_authoritative("nb-c", "node-1", t, 3))
        self.assertEqual(s.get_durable_generation("nb-c"), 5, "durable_generation must never regress")

    def test_superseded_discard_drops_row_token_fenced(self):
        s = self.store
        t = s.stamp_workspace_stopped("nb-d", "node-1")
        # Wrong token → no-op (row stays).
        self.assertFalse(s.discard_superseded_copy("nb-d", "node-1", "OLD"))
        self.assertIn(("nb-d", "node-1"),
                      {(r["instance_id"], r["node_name"]) for r in s.list_workspace_unflushed_copies()})
        # Correct token → dropped.
        self.assertTrue(s.discard_superseded_copy("nb-d", "node-1", t))
        self.assertNotIn(("nb-d", "node-1"),
                         {(r["instance_id"], r["node_name"]) for r in s.list_workspace_unflushed_copies()})

    def test_unflushed_list_carries_generation_context(self):
        # The retry sweep needs synced/durable generations to observe a superseded copy.
        s = self.store
        t = s.stamp_workspace_stopped("nb-e", "node-1")
        with s.engine.begin() as conn:
            conn.execute(
                s.update(s.workspace_cache_state)
                .where(s.workspace_cache_state.c.instance_id == "nb-e")
                .values(durable_generation=4)
            )
        rows = [r for r in s.list_workspace_unflushed_copies() if r["instance_id"] == "nb-e"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["synced_generation"], 0)   # NULL → 0
        self.assertEqual(rows[0]["durable_generation"], 4)  # from the joined cache_state

    def test_superseded_unflushed_copy_never_reaped(self):
        # A superseded stranded copy is UNFLUSHED (flushed_at NULL), so the reaper must never select it
        # even past TTL — reaping it would destroy data before it is discarded/routed.
        s = self.store
        s.stamp_workspace_stopped("nb-f", "node-1")
        with s.engine.begin() as conn:
            conn.execute(
                s.update(s.workspace_cache_state)
                .where(s.workspace_cache_state.c.instance_id == "nb-f")
                .values(durable_generation=9)
            )
        self._backdate_copy("nb-f", "node-1", 999)
        ids = {(r["instance_id"], r["node_name"]) for r in s.list_workspace_cache_to_reap(1)}
        self.assertNotIn(("nb-f", "node-1"), ids,
                         "an unflushed superseded copy must never be reapable (never-reap-unflushed)")

    def test_has_newer_local_copy_distinguishes_stranded_from_latest(self):
        # Distinguishes a genuinely-superseded stranded copy (a newer session ran on another node)
        # from a race-desynced LATEST copy (no newer copy) — the guard that prevents discarding a
        # live session whose marker was clobbered below durable_generation.
        s = self.store
        tA = s.stamp_workspace_stopped("nb-h", "node-A")
        # Only A exists → A is the latest → not superseded.
        self.assertFalse(s.has_newer_local_copy("nb-h", "node-A", tA))
        s.stamp_workspace_running("nb-h", None)
        tB = s.stamp_workspace_stopped("nb-h", "node-B")   # newer session on a different node
        self.assertTrue(s.has_newer_local_copy("nb-h", "node-A", tA),
                        "A now has a strictly-newer copy on B → genuinely superseded")
        self.assertFalse(s.has_newer_local_copy("nb-h", "node-B", tB),
                         "B is the latest → not superseded")

    def test_clear_cache_state_resets_generation(self):
        # Admin durable delete calls clear_workspace_cache_state → generation resets so a re-created
        # workspace under the same id starts at 0 (won't MIRROR-wipe against empty durable).
        s = self.store
        t = s.stamp_workspace_stopped("nb-g", "node-1")
        s.mark_workspace_flushed_authoritative("nb-g", "node-1", t, 7)
        self.assertEqual(s.get_durable_generation("nb-g"), 7)
        s.clear_workspace_cache_state("nb-g")
        self.assertEqual(s.get_durable_generation("nb-g"), 0)


class GenerationMigrationTests(unittest.TestCase):
    """The generation columns MUST be added by an explicit ALTER on a pre-existing table — create_all
    does not add columns to an existing table. Simulate an old DB (tables without the columns) and
    assert ensure_workspace_generation_columns adds them, idempotently."""

    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._tmp.close()
        from app import store
        from sqlalchemy import create_engine, text
        self._text = text
        self.eng = create_engine(f"sqlite:///{self._tmp.name}", future=True)
        self.store = store
        # OLD schema: the two workspace tables WITHOUT the generation columns.
        with self.eng.begin() as conn:
            conn.execute(text(
                "CREATE TABLE workspace_cache_state (instance_id VARCHAR(255) PRIMARY KEY, "
                "node_name VARCHAR(255), stopped_at VARCHAR(64), updated_at VARCHAR(64) NOT NULL)"))
            conn.execute(text(
                "CREATE TABLE workspace_local_copy (instance_id VARCHAR(255) NOT NULL, "
                "node_name VARCHAR(255) NOT NULL, session_token VARCHAR(64) NOT NULL, "
                "flushed_at VARCHAR(64), created_at VARCHAR(64) NOT NULL, updated_at VARCHAR(64) NOT NULL, "
                "PRIMARY KEY (instance_id, node_name))"))

    def tearDown(self):
        try:
            os.unlink(self._tmp.name)
        except OSError:
            pass

    def _columns(self, conn, table):
        from sqlalchemy import inspect
        return {c["name"] for c in inspect(conn).get_columns(table)}

    def test_alter_adds_generation_columns_idempotently(self):
        with self.eng.begin() as conn:
            self.assertNotIn("durable_generation", self._columns(conn, "workspace_cache_state"))
            self.assertNotIn("synced_generation", self._columns(conn, "workspace_local_copy"))
            self.store.ensure_workspace_generation_columns(conn)
            self.assertIn("durable_generation", self._columns(conn, "workspace_cache_state"))
            self.assertIn("synced_generation", self._columns(conn, "workspace_local_copy"))
            # Idempotent: a second run must not raise (column already present).
            self.store.ensure_workspace_generation_columns(conn)
            self.assertIn("durable_generation", self._columns(conn, "workspace_cache_state"))


if __name__ == "__main__":
    unittest.main()
