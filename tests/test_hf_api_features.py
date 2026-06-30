"""Regression tests for the 6 HuggingFace / external-API features.

Covers:
  - F1: blank notebook_path launches a bare Jupyter (github_info=None, root URL).
  - F2: image discovery endpoints (HF bearer + admin) return the enabled catalog.
  - F3: credit grant happens once on creation (no per-launch re-top) + one-time backfill cap.
  - F4: cleanup_idle_instances targets custom-id API pods and leaves non-API pods alone.
  - F5: pod_type validation (restricted set) + persistence to instance_records.
  - F6: GPU availability endpoint returns free/total shape.
"""
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

_DB_FD, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.close(_DB_FD)
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["ADMIN_PASSWORD"] = "testpass"
os.environ["NOTEBOOK_NODE_NAME"] = "fake-node"
os.environ["IMAGE_SERVICE_ENABLED"] = "false"
os.environ["HUGGINGFACE_DEMO_API_TOKENS"] = "tok-test"

from tests.kube_stub import install  # noqa: E402

install()

from fastapi.testclient import TestClient  # noqa: E402

from app import main as main_module  # noqa: E402
from app import store  # noqa: E402

BEARER = {"Authorization": "Bearer tok-test"}
ADMIN = ("admin", "testpass")


def _set_credits(user_id, value):
    with store.engine.begin() as conn:
        conn.exec_driver_sql("UPDATE users SET credits=? WHERE id=?", (value, user_id))


def _pin_settings():
    main_module.settings.ADMIN_PASSWORD = "testpass"
    main_module.settings.HUGGINGFACE_DEMO_API_TOKENS = "tok-test"
    main_module.settings.IMAGE_SERVICE_ENABLED = False


class _FakeK8s:
    """Minimal stand-in for k8s_client used by the launch handler."""

    def __init__(self):
        self.created = []
        self.existing_ids = {}

    def get_instance_by_id(self, instance_id):
        return self.existing_ids.get(instance_id)

    def create_instance(self, email, image, **kwargs):
        self.created.append({"email": email, "image": image, **kwargs})
        return {"id": kwargs.get("custom_instance_id", "fake-id"), "node_port": 30001,
                "opencode_node_port": None}

    def _select_target_gpu_node(self, gpu_count):
        return "fake-node"

    def delete_instance_by_id(self, instance_id):
        return True


class HFLaunchTestBase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        _pin_settings()
        store.init_db()
        self.client = TestClient(main_module.app)
        self.image = store.upsert_image("demo", "registry/image:tag", "", True)
        self._orig_k8s = main_module.k8s_client
        self.fake = _FakeK8s()
        main_module.k8s_client = self.fake

    def tearDown(self):
        main_module.k8s_client = self._orig_k8s
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM instance_records")
            conn.exec_driver_sql("DELETE FROM instance_launch_events")
            conn.exec_driver_sql("DELETE FROM images")
            conn.exec_driver_sql("DELETE FROM credit_ledger")
            conn.exec_driver_sql("DELETE FROM users")


class Feature1BlankPath(HFLaunchTestBase):
    def test_blank_notebook_path_launches_bare_jupyter(self):
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "alice", "image": "registry/image:tag"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(len(self.fake.created), 1)
        self.assertIsNone(self.fake.created[0]["github_info"])

    def test_empty_string_path_also_bare(self):
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "bob", "notebook_path": "   ", "image": "registry/image:tag"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertIsNone(self.fake.created[0]["github_info"])


class Feature3Credits(HFLaunchTestBase):
    def test_grant_once_on_creation_no_retop(self):
        cap = main_module.settings.HUGGINGFACE_DEMO_MIN_CREDITS
        # First launch creates the user and grants the floor.
        r1 = self.client.post("/api/huggingface/notebooks",
                              json={"user_name": "carol", "image": "registry/image:tag"},
                              headers=BEARER)
        self.assertEqual(r1.status_code, 200, r1.text)
        user = store.get_user_by_provider(main_module.HF_DEMO_PROVIDER, "carol")
        self.assertEqual(user["credits"], cap)

        # Simulate usage spending some credits.
        _set_credits(user["id"], cap - 3)
        self.assertEqual(store.get_user(user["id"])["credits"], cap - 3)

        # Destroy the active instance, then relaunch: must NOT re-top to the floor.
        store.mark_instance_deleted(self.fake.created[-1]["custom_instance_id"])
        r2 = self.client.post("/api/huggingface/notebooks",
                              json={"user_name": "carol", "image": "registry/image:tag"},
                              headers=BEARER)
        self.assertEqual(r2.status_code, 200, r2.text)
        self.assertEqual(store.get_user(user["id"])["credits"], cap - 3)

    def test_grant_marker_written_once_and_no_retop_after_spend_to_zero(self):
        cap = main_module.settings.HUGGINGFACE_DEMO_MIN_CREDITS
        u = store.get_or_create_external_user(main_module.HF_DEMO_PROVIDER, "zoe", "zoe@hf.local")
        # New user starts at 0 credits, so the grant delta must equal the full floor.
        _set_credits(u["id"], 0)
        store.grant_initial_credits_once(u["id"], cap, "hf_initial_grant")
        self.assertEqual(store.get_user(u["id"])["credits"], cap)
        # Ledger must reconcile: the grant row records the real +cap movement, not 0.
        with store.engine.begin() as conn:
            rows = conn.exec_driver_sql(
                "SELECT delta FROM credit_ledger WHERE user_id=? AND reason='hf_initial_grant'",
                (u["id"],),
            ).fetchall()
        self.assertEqual([r[0] for r in rows], [cap])
        # Spend to zero, then call grant again: marker present -> no re-top, no new ledger row.
        _set_credits(u["id"], 0)
        store.grant_initial_credits_once(u["id"], cap, "hf_initial_grant")
        self.assertEqual(store.get_user(u["id"])["credits"], 0)
        with store.engine.begin() as conn:
            n = conn.exec_driver_sql(
                "SELECT COUNT(*) FROM credit_ledger WHERE user_id=? AND reason='hf_initial_grant'",
                (u["id"],),
            ).scalar()
        self.assertEqual(n, 1)

    def test_backfill_noop_on_empty_db_writes_no_marker(self):
        # No users at all: backfill must defer (no marker), so it can still run once a user exists.
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM credit_ledger")
            conn.exec_driver_sql("DELETE FROM users")
            store._backfill_hf_credit_cap(conn)
            markers = conn.exec_driver_sql(
                "SELECT COUNT(*) FROM credit_ledger WHERE reason='hf_backfill_cap_v1'"
            ).scalar()
        self.assertEqual(markers, 0)

    def test_backfill_caps_inflated_hf_balances(self):
        cap = main_module.settings.HUGGINGFACE_DEMO_MIN_CREDITS
        # Inflated HF user, a non-HF user at 48, and an HF user already below the cap.
        hf_hi = store.get_or_create_external_user(main_module.HF_DEMO_PROVIDER, "rich", "rich@hf.local")
        _set_credits(hf_hi["id"], 48)
        google = store.get_or_create_user("google", "g1", "g1@example.com", "G", None)
        _set_credits(google["id"], 48)
        hf_lo = store.get_or_create_external_user(main_module.HF_DEMO_PROVIDER, "poor", "poor@hf.local")
        _set_credits(hf_lo["id"], 5)

        with store.engine.begin() as conn:
            store._backfill_hf_credit_cap(conn)

        self.assertEqual(store.get_user(hf_hi["id"])["credits"], cap)   # capped down
        self.assertEqual(store.get_user(google["id"])["credits"], 48)   # untouched (non-HF)
        self.assertEqual(store.get_user(hf_lo["id"])["credits"], 5)     # untouched (already low)

    def test_backfill_is_idempotent(self):
        hf = store.get_or_create_external_user(main_module.HF_DEMO_PROVIDER, "u", "u@hf.local")
        _set_credits(hf["id"], 48)
        with store.engine.begin() as conn:
            store._backfill_hf_credit_cap(conn)
        # Re-inflate and run again: guard row should prevent a second cap.
        _set_credits(hf["id"], 40)
        before = store.get_user(hf["id"])["credits"]
        with store.engine.begin() as conn:
            store._backfill_hf_credit_cap(conn)
        self.assertEqual(store.get_user(hf["id"])["credits"], before)
        with store.engine.begin() as conn:
            markers = conn.exec_driver_sql(
                "SELECT COUNT(*) FROM credit_ledger WHERE reason='hf_backfill_cap_v1'"
            ).scalar()
        self.assertEqual(markers, 1)


class Feature5PodType(HFLaunchTestBase):
    def test_valid_pod_type_persists(self):
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "dora", "image": "registry/image:tag", "pod_type": "Workshop"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        # Normalized to lowercase and forwarded to k8s create_instance.
        self.assertEqual(self.fake.created[0]["pod_type"], "workshop")
        self.assertTrue(self.fake.created[0]["api_launched"])
        with store.engine.begin() as conn:
            rec = conn.exec_driver_sql(
                "SELECT pod_type FROM instance_records ORDER BY id DESC LIMIT 1"
            ).scalar()
            evt = conn.exec_driver_sql(
                "SELECT pod_type FROM instance_launch_events ORDER BY id DESC LIMIT 1"
            ).scalar()
        self.assertEqual(rec, "workshop")
        self.assertEqual(evt, "workshop")

    def test_invalid_pod_type_rejected(self):
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "eve", "image": "registry/image:tag", "pod_type": "bogus"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 400, resp.text)

    def test_no_pod_type_is_allowed(self):
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "frank", "image": "registry/image:tag"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertIsNone(self.fake.created[0]["pod_type"])

    async def test_standard_path_threads_pod_type(self):
        # _provision_notebook_instance must forward pod_type to k8s + both DB writes.
        user = store.get_or_create_user("google", "p1", "p1@example.com", "P", None)
        _set_credits(user["id"], 50)
        user = store.get_user(user["id"])
        await main_module._provision_notebook_instance(
            user, user["email"], "registry/image:tag",
            {"instance_type": "jupyter", "gpu_count": 1, "resource_profile": "auto",
             "disk_size_gb": None, "pod_type": "hackathon"},
            "fake-node",
        )
        self.assertEqual(self.fake.created[-1]["pod_type"], "hackathon")
        with store.engine.begin() as conn:
            rec = conn.exec_driver_sql(
                "SELECT pod_type FROM instance_records ORDER BY id DESC LIMIT 1").scalar()
        self.assertEqual(rec, "hackathon")

    def test_normalize_pod_type_rejects_unknown(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException):
            main_module._normalize_pod_type("nope")
        self.assertIsNone(main_module._normalize_pod_type(""))
        self.assertEqual(main_module._normalize_pod_type(" Workshop "), "workshop")


class Feature2And6Endpoints(unittest.TestCase):
    def setUp(self):
        _pin_settings()
        store.init_db()
        self.client = TestClient(main_module.app)
        self.image = store.upsert_image("demo2", "registry/image:tag2", "desc", True)
        store.upsert_image("disabled", "registry/off:tag", "", False)
        self._orig_k8s = main_module.k8s_client

    def tearDown(self):
        main_module.k8s_client = self._orig_k8s
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM images")

    def test_hf_images_lists_enabled_only(self):
        resp = self.client.get("/api/huggingface/images", headers=BEARER)
        self.assertEqual(resp.status_code, 200, resp.text)
        imgs = {i["image"] for i in resp.json()["images"]}
        self.assertIn("registry/image:tag2", imgs)
        self.assertNotIn("registry/off:tag", imgs)

    def test_admin_images_list_matches(self):
        resp = self.client.get("/api/admin/images-list", auth=ADMIN)
        self.assertEqual(resp.status_code, 200, resp.text)
        imgs = {i["image"] for i in resp.json()["images"]}
        self.assertIn("registry/image:tag2", imgs)
        self.assertNotIn("registry/off:tag", imgs)

    def test_images_requires_auth(self):
        self.assertEqual(self.client.get("/api/huggingface/images").status_code, 401)

    def test_gpu_endpoint_shape(self):
        class _GpuK8s:
            def gpu_capacity_summary(self):
                return {"total_gpus": 8, "free_gpus": 3,
                        "nodes": [{"node": "n1", "total": 8, "free": 3,
                                   "committed": 5, "quarantined": False}]}
        main_module.k8s_client = _GpuK8s()
        r1 = self.client.get("/api/huggingface/gpus", headers=BEARER)
        r2 = self.client.get("/api/admin/gpus", auth=ADMIN)
        self.assertEqual(r1.status_code, 200, r1.text)
        self.assertEqual(r2.status_code, 200, r2.text)
        self.assertEqual(r1.json()["total_gpus"], 8)
        self.assertEqual(r1.json()["free_gpus"], 3)
        self.assertEqual(r2.json(), r1.json())


class Feature4IdleReaper(unittest.TestCase):
    """cleanup_idle_instances must key on instance id and scope 8h to API pods."""

    def setUp(self):
        _pin_settings()
        store.init_db()
        from app.k8s_client import K8sClient
        self.k = K8sClient.__new__(K8sClient)  # bypass __init__ (no real kube)
        self.deleted = []
        self.marked = []

    def _make_instance(self, instance_id, api_launched, uptime_minutes, status="running"):
        return {
            "id": instance_id, "email": f"{instance_id}@x.local", "status": status,
            "uptime_minutes": uptime_minutes, "instance_type": "jupyter",
            "api_launched": api_launched,
        }

    def _patch(self, instances, last_activity=None):
        self.k.list_instances = lambda: instances
        self.k.check_pod_activity = lambda email, instance_id=None: last_activity
        self.k.delete_instance_by_id = lambda iid: (self.deleted.append(iid) or True)
        import app.k8s_client as kc
        self._orig_mark = kc.store.mark_instance_deleted
        kc.store.mark_instance_deleted = lambda iid: self.marked.append(iid)
        self._kc = kc

    def tearDown(self):
        if hasattr(self, "_kc"):
            self._kc.store.mark_instance_deleted = self._orig_mark

    def test_api_pod_idle_past_8h_reaped_by_custom_id(self):
        old = datetime.now(timezone.utc) - timedelta(minutes=481)
        self._patch([self._make_instance("hf-1-abcd", True, 481)], last_activity=old)
        cleaned = self.k.cleanup_idle_instances()
        self.assertEqual(self.deleted, ["hf-1-abcd"])
        self.assertEqual(self.marked, ["hf-1-abcd"])
        self.assertEqual(len(cleaned), 1)

    def test_api_pod_no_logs_falls_back_to_uptime(self):
        self._patch([self._make_instance("hf-2-efgh", True, 500)], last_activity=None)
        self.k.cleanup_idle_instances()
        self.assertEqual(self.deleted, ["hf-2-efgh"])

    def test_api_pod_recent_activity_not_reaped(self):
        recent = datetime.now(timezone.utc) - timedelta(minutes=10)
        self._patch([self._make_instance("hf-3-ijkl", True, 60)], last_activity=recent)
        self.k.cleanup_idle_instances()
        self.assertEqual(self.deleted, [])

    def test_non_api_pod_uses_short_default_not_8h(self):
        # Non-API jupyter pod idle 20m: under the API budget but the default idle is short,
        # so with no activity logs it should NOT be reaped by uptime (no fallback for non-API).
        self._patch([self._make_instance("u-9-zzzz", False, 20)], last_activity=None)
        self.k.cleanup_idle_instances()
        self.assertEqual(self.deleted, [])


class BillingScope(unittest.TestCase):
    """Billing (list_active_instances) must include only API-launched instances."""

    def setUp(self):
        store.init_db()
        self.user = store.get_or_create_user("google", "b1", "b1@example.com", "B", None)
        _set_credits(self.user["id"], 50)

    def tearDown(self):
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM instance_records")
            conn.exec_driver_sql("DELETE FROM users WHERE id=?", (self.user["id"],))

    def test_web_instance_excluded_api_instance_included(self):
        store.record_instance(self.user["id"], "b1@example.com", "u-b1-web", "img",
                              "opencode", 1, 30001, api_launched=False)
        store.record_instance(self.user["id"], "b1@example.com", "hf-b1-api", "img",
                              "jupyter", 1, 30002, api_launched=True)
        ids = {r["instance_id"] for r in store.list_active_instances()}
        self.assertIn("hf-b1-api", ids)
        self.assertNotIn("u-b1-web", ids)


if __name__ == "__main__":
    unittest.main()
