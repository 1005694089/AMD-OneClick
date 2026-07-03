"""Tests for the reliability fixes: node-wedge detection/quarantine (P2), the Terminating status
short-circuit in get_pod_status_details (P3), and the non-blocking-delete plumbing (P1).

Pure-logic where possible: the wedge detector operates on a list of pod-state dicts and a module
`settings`, so it is exercised directly with fakes; the status short-circuit is exercised against a
fake core_v1. No real cluster or DB required."""
import os
import tempfile
import unittest
from types import SimpleNamespace

# Point the DB at a throwaway sqlite file BEFORE importing app modules (app.store creates the
# engine at import time and otherwise tries to mkdir /data). Mirrors tests/test_admin_images.py.
_DB_FD, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ.setdefault("NOTEBOOK_NODE_NAME", "fake-node")

from tests.kube_stub import install

install()

from app import scheduler as sched
from app import k8s_client as k8s_module


class _FakeDatetime:
    pass


def _pod(node, terminating=False, terminating_seconds=0, waiting_reason="", age=0):
    return {
        "instance_id": f"u-{node}-{age}-{terminating_seconds}-{waiting_reason}",
        "node_name": node,
        "terminating": terminating,
        "terminating_seconds": terminating_seconds,
        "waiting_reason": waiting_reason,
        "age_seconds": age,
    }


class NodeWedgeDetectionTests(unittest.TestCase):
    def setUp(self):
        # Snapshot + set deterministic thresholds.
        self._s = sched.settings
        self._orig = {
            k: getattr(self._s, k)
            for k in (
                "NODE_WEDGE_DETECT_ENABLED",
                "NODE_WEDGE_MIN_STUCK_PODS",
                "NODE_WEDGE_CREATING_SECONDS",
                "NODE_WEDGE_CONSECUTIVE_TICKS",
                "NODE_QUARANTINE_SECONDS",
            )
        }
        self._s.NODE_WEDGE_DETECT_ENABLED = True
        self._s.NODE_WEDGE_MIN_STUCK_PODS = 2
        self._s.NODE_WEDGE_CREATING_SECONDS = 600
        self._s.NODE_WEDGE_CONSECUTIVE_TICKS = 2
        self._s.NODE_QUARANTINE_SECONDS = 1800
        # Capture quarantine calls instead of hitting the DB.
        self._q = []
        import app.store as store_mod
        self._orig_q = store_mod.quarantine_node
        store_mod.quarantine_node = lambda node, ref, secs: self._q.append((node, ref, secs))
        sched._node_wedge_streak.clear()

    def tearDown(self):
        for k, v in self._orig.items():
            setattr(self._s, k, v)
        import app.store as store_mod
        store_mod.quarantine_node = self._orig_q
        sched._node_wedge_streak.clear()

    def test_flag_off_is_noop(self):
        self._s.NODE_WEDGE_DETECT_ENABLED = False
        pods = [_pod("n1", terminating=True, terminating_seconds=9999) for _ in range(5)]
        out = sched._detect_and_quarantine_wedged_nodes(pods, stuck_threshold=180)
        self.assertEqual(out, [])
        self.assertEqual(self._q, [])

    def test_single_stuck_pod_below_min_not_flagged(self):
        pods = [_pod("n1", terminating=True, terminating_seconds=9999)]  # only 1, min is 2
        for _ in range(5):
            out = sched._detect_and_quarantine_wedged_nodes(pods, stuck_threshold=180)
            self.assertEqual(out, [])
        self.assertEqual(self._q, [])

    def test_requires_consecutive_ticks_before_quarantine(self):
        pods = [
            _pod("n1", terminating=True, terminating_seconds=9999),
            _pod("n1", terminating=True, terminating_seconds=9999),
        ]
        # tick 1: suspect but streak=1 < 2 -> no quarantine
        out1 = sched._detect_and_quarantine_wedged_nodes(pods, stuck_threshold=180)
        self.assertEqual(out1, [])
        self.assertEqual(self._q, [])
        # tick 2: streak=2 -> quarantine
        out2 = sched._detect_and_quarantine_wedged_nodes(pods, stuck_threshold=180)
        self.assertEqual(out2, ["n1"])
        self.assertEqual(len(self._q), 1)
        self.assertEqual(self._q[0][0], "n1")
        self.assertEqual(self._q[0][2], 1800)  # node-wide (empty ref), full quarantine window

    def test_containercreating_stuck_counts_as_wedge(self):
        pods = [
            _pod("n2", waiting_reason="ContainerCreating", age=700),
            _pod("n2", waiting_reason="ContainerCreating", age=700),
        ]
        sched._detect_and_quarantine_wedged_nodes(pods, stuck_threshold=180)
        out = sched._detect_and_quarantine_wedged_nodes(pods, stuck_threshold=180)
        self.assertEqual(out, ["n2"])

    def test_recovery_resets_streak(self):
        pods = [
            _pod("n1", terminating=True, terminating_seconds=9999),
            _pod("n1", terminating=True, terminating_seconds=9999),
        ]
        sched._detect_and_quarantine_wedged_nodes(pods, stuck_threshold=180)  # streak=1
        # node recovered this cycle (no stuck pods) -> streak cleared
        sched._detect_and_quarantine_wedged_nodes([], stuck_threshold=180)
        self.assertNotIn("n1", sched._node_wedge_streak)
        # a single fresh suspect tick must not immediately quarantine
        out = sched._detect_and_quarantine_wedged_nodes(pods, stuck_threshold=180)
        self.assertEqual(out, [])

    def test_fresh_terminating_below_threshold_ignored(self):
        # Pods terminating only 10s (< stuck_threshold 180) are normal, not a wedge.
        pods = [
            _pod("n1", terminating=True, terminating_seconds=10),
            _pod("n1", terminating=True, terminating_seconds=10),
        ]
        for _ in range(3):
            out = sched._detect_and_quarantine_wedged_nodes(pods, stuck_threshold=180)
            self.assertEqual(out, [])


class TerminatingStatusShortCircuitTests(unittest.TestCase):
    """get_pod_status_details must report a pod with deletion_timestamp set as 'terminating',
    never 'ready', even if the container still reports ready=True."""

    def _client_with_pod(self, pod):
        client = k8s_module.K8sClient.__new__(k8s_module.K8sClient)
        client.namespace = "test-ns"
        client.core_v1 = SimpleNamespace(read_namespaced_pod=lambda name, namespace: pod)
        return client

    def test_deleting_pod_reports_terminating(self):
        pod = SimpleNamespace(
            metadata=SimpleNamespace(deletion_timestamp="2026-07-03T12:00:00Z", annotations={}),
            status=SimpleNamespace(
                phase="Running",
                reason=None,
                message=None,
                conditions=[],
                container_statuses=[SimpleNamespace(ready=True, state=None)],
            ),
        )
        client = self._client_with_pod(pod)
        out = client.get_pod_status_details("user@example.com", instance_id="u-1-abc")
        self.assertEqual(out["status"], "terminating")
        self.assertFalse(out["ready"])

    def test_live_pod_not_affected(self):
        pod = SimpleNamespace(
            metadata=SimpleNamespace(deletion_timestamp=None, annotations={}),
            status=SimpleNamespace(
                phase="Pending",
                reason=None,
                message=None,
                conditions=[],
                container_statuses=None,
            ),
        )
        client = self._client_with_pod(pod)
        out = client.get_pod_status_details("user@example.com", instance_id="u-1-abc")
        self.assertNotEqual(out["status"], "terminating")


class PodStatesNodeNameTests(unittest.TestCase):
    """list_managed_pod_states must expose node_name so wedge detection can aggregate by node."""

    def test_node_name_present(self):
        pod = SimpleNamespace(
            metadata=SimpleNamespace(
                name="u-1-abc",
                labels={"instance-id": "u-1-abc"},
                annotations={},
                creation_timestamp=None,
                deletion_timestamp=None,
            ),
            spec=SimpleNamespace(node_name="wx-k8s-prod-s-064"),
            status=SimpleNamespace(phase="Running", container_statuses=[]),
        )
        client = k8s_module.K8sClient.__new__(k8s_module.K8sClient)
        client.namespace = "test-ns"
        client.core_v1 = SimpleNamespace(
            list_namespaced_pod=lambda namespace, label_selector: SimpleNamespace(items=[pod])
        )
        states = client.list_managed_pod_states()
        self.assertEqual(len(states), 1)
        self.assertEqual(states[0]["node_name"], "wx-k8s-prod-s-064")


class MarkInstanceDeletingTests(unittest.TestCase):
    """DB-backed: mark_instance_deleting must transition a live row to 'deleting' without touching
    columns that don't exist (regression for the 'Unconsumed column names: updated_at' bug), leave
    deleted_at NULL, and skip already-deleted rows."""

    @classmethod
    def setUpClass(cls):
        from app import store
        store.init_db()

    def _insert(self, instance_id, status="running", deleted_at=None):
        from app import store
        now = store.utc_now()
        with store.engine.begin() as conn:
            conn.execute(
                store.instance_records.insert().values(
                    user_id=1, email="e@x.com", instance_id=instance_id, image="img",
                    instance_type="opencode", gpu_count=1, status=status,
                    created_at=now, last_charged_at=now, billing_session_id="b-" + instance_id,
                    deleted_at=deleted_at,
                )
            )

    def _row(self, instance_id):
        from app import store
        from sqlalchemy import select
        with store.engine.begin() as conn:
            return conn.execute(
                select(store.instance_records).where(
                    store.instance_records.c.instance_id == instance_id
                )
            ).mappings().first()

    def test_running_row_transitions_to_deleting_and_keeps_deleted_at_null(self):
        from app import store
        self._insert("u-1-mkdel-a", status="running")
        store.mark_instance_deleting("u-1-mkdel-a")  # must not raise (regression: updated_at)
        row = self._row("u-1-mkdel-a")
        self.assertEqual(row["status"], "deleting")
        self.assertIsNone(row["deleted_at"])

    def test_deleting_row_excluded_from_active_but_present(self):
        from app import store
        self._insert("u-1-mkdel-b", status="running")
        store.mark_instance_deleting("u-1-mkdel-b")
        # A deleting row must NOT block a new launch (not "active")...
        self.assertIsNone(store.get_active_instance_for_user(1))
        # ...but the row still exists (visible to status polling).
        self.assertEqual(self._row("u-1-mkdel-b")["status"], "deleting")

    def test_already_deleted_row_not_resurrected(self):
        from app import store
        self._insert("u-1-mkdel-c", status="deleted", deleted_at=store.utc_now())
        store.mark_instance_deleting("u-1-mkdel-c")
        self.assertEqual(self._row("u-1-mkdel-c")["status"], "deleted")


if __name__ == "__main__":
    unittest.main()
