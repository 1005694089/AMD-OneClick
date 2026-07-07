"""Regression tests for the 7 HuggingFace / external-API features.

Covers:
  - F1: blank notebook_path launches a bare Jupyter (github_info=None, root URL).
  - F2: image discovery endpoints (HF bearer + admin) return the enabled catalog.
  - F3: credit grant happens once on creation (no per-launch re-top) + one-time backfill cap.
  - F4: cleanup_idle_instances targets custom-id API pods and leaves non-API pods alone.
  - F5: pod_type validation (restricted set) + persistence to instance_records.
  - F6: GPU availability endpoint returns free/total shape.
  - F7: unlimited_credits launch flag freezes the user's balance (billing loop skips deduction).
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
        # Status-poll stand-ins (huggingface_demo_notebook_status).
        self.pod_status = "ready"
        self.startup_detail = None
        self.live_ports = set()  # instance_ids for which is_pod_port_live() returns True
        self.port_probes = []  # (instance_id, port) log of every is_pod_port_live() call

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

    def get_pod_status(self, email, instance_id=None):
        return self.pod_status

    def get_startup_detail(self, instance_id):
        return self.startup_detail

    def is_pod_port_live(self, instance_id, port, timeout=0.5):
        self.port_probes.append((instance_id, port))
        return instance_id in self.live_ports


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

    def test_omitting_image_uses_api_default(self):
        # The API default must exist in the enabled catalog and be selected when image is omitted.
        store.upsert_image("Huggingface", main_module.settings.HUGGINGFACE_DEMO_DEFAULT_IMAGE, "", True)
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "carl-default"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(self.fake.created[0]["image"],
                         main_module.settings.HUGGINGFACE_DEMO_DEFAULT_IMAGE)


class ImageSelectionByName(HFLaunchTestBase):
    """API callers may pass the admin-panel image NAME or the full ref."""

    def test_launch_by_image_name_resolves_to_ref(self):
        # Fixture registers name "demo" -> ref "registry/image:tag".
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "byname-1", "image": "demo"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(self.fake.created[0]["image"], "registry/image:tag")

    def test_launch_by_image_name_is_case_insensitive(self):
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "byname-2", "image": "DEMO"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(self.fake.created[0]["image"], "registry/image:tag")

    def test_launch_by_full_ref_still_works(self):
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "byref-1", "image": "registry/image:tag"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(self.fake.created[0]["image"], "registry/image:tag")

    def test_unknown_name_or_ref_rejected(self):
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "byname-bad", "image": "no-such-image"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 400, resp.text)

    def test_disabled_image_name_not_selectable(self):
        store.upsert_image("OffImage", "registry/off:tag", "", False)
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "byname-off", "image": "OffImage"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 400, resp.text)

    def test_case_folded_name_collision_is_deterministic(self):
        # If two enabled rows share a case-folded name, resolve picks the lowest id deterministically.
        a = store.upsert_image("Dup", "registry/dup-a:tag", "", True)
        store.upsert_image("dup", "registry/dup-b:tag", "", True)
        for _ in range(3):
            r = store.resolve_enabled_image("DUP")
            self.assertEqual(r["id"], a["id"])
            self.assertEqual(r["image"], "registry/dup-a:tag")


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


class Feature7UnlimitedCredits(HFLaunchTestBase):
    def test_launch_with_flag_marks_user_unlimited_and_freezes_at_grant(self):
        cap = main_module.settings.HUGGINGFACE_DEMO_MIN_CREDITS
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "uli", "image": "registry/image:tag", "unlimited_credits": True},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        user = store.get_user_by_provider(main_module.HF_DEMO_PROVIDER, "uli")
        self.assertTrue(user["unlimited_credits"])
        self.assertEqual(user["credits"], cap)

    def test_launch_without_flag_stays_metered(self):
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "metered-uli", "image": "registry/image:tag"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        user = store.get_user_by_provider(main_module.HF_DEMO_PROVIDER, "metered-uli")
        self.assertFalse(user["unlimited_credits"])

    def test_charge_usage_unit_skips_deduction_for_unlimited_user(self):
        u = store.get_or_create_external_user(main_module.HF_DEMO_PROVIDER, "unl-charge", "unl-charge@hf.local")
        _set_credits(u["id"], 10)
        store.set_user_unlimited(u["id"], True)
        result = store.charge_usage_unit(u["id"], "hf-instance-unl", "session-unl", 1, 4)
        self.assertEqual(result, "charged")
        self.assertEqual(store.get_user(u["id"])["credits"], 10)
        with store.engine.begin() as conn:
            charged = conn.exec_driver_sql(
                "SELECT credits FROM usage_charges WHERE billing_session_id=? AND billing_unit=1",
                ("session-unl",),
            ).scalar()
        self.assertEqual(charged, 0)

    def test_charge_usage_unit_still_deducts_for_metered_user(self):
        # Regression: the unlimited short-circuit must not affect normal users.
        u = store.get_or_create_external_user(main_module.HF_DEMO_PROVIDER, "reg-charge", "reg-charge@hf.local")
        _set_credits(u["id"], 10)
        result = store.charge_usage_unit(u["id"], "hf-instance-reg", "session-reg", 1, 2)
        self.assertEqual(result, "charged")
        self.assertEqual(store.get_user(u["id"])["credits"], 8)

    def test_launch_bypasses_insufficient_credits_gate_when_unlimited(self):
        cap = main_module.settings.HUGGINGFACE_DEMO_MIN_CREDITS
        u = store.get_or_create_external_user(main_module.HF_DEMO_PROVIDER, "brokeuser", "brokeuser@hf.local")
        # Set the grant marker first so the launch below does not re-top the balance, then force
        # credits under gpu_count to isolate the gate-bypass behavior from the grant itself.
        store.grant_initial_credits_once(u["id"], cap, "hf_initial_grant")
        store.set_user_unlimited(u["id"], True)
        _set_credits(u["id"], 0)
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "brokeuser", "image": "registry/image:tag"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)

    def test_rejected_launch_does_not_persist_unlimited_flag(self):
        # A launch rejected by the "one active instance" guard must not leave the sticky unlimited
        # flag behind — otherwise the already-running pod silently stops being billed.
        r1 = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "stale", "image": "registry/image:tag"},
            headers=BEARER,
        )
        self.assertEqual(r1.status_code, 200, r1.text)
        iid = self.fake.created[-1]["custom_instance_id"]
        # Make the cluster report the instance as still live so the relaunch is rejected.
        self.fake.existing_ids[iid] = {"id": iid}
        r2 = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "stale", "image": "registry/image:tag", "unlimited_credits": True},
            headers=BEARER,
        )
        self.assertEqual(r2.status_code, 400, r2.text)
        user = store.get_user_by_provider(main_module.HF_DEMO_PROVIDER, "stale")
        self.assertFalse(user["unlimited_credits"])

    def test_launch_still_rejected_when_metered_user_has_no_credits(self):
        # Contrast case: without the flag, the pre-existing gate is unchanged.
        cap = main_module.settings.HUGGINGFACE_DEMO_MIN_CREDITS
        u = store.get_or_create_external_user(main_module.HF_DEMO_PROVIDER, "brokeuser2", "brokeuser2@hf.local")
        store.grant_initial_credits_once(u["id"], cap, "hf_initial_grant")
        _set_credits(u["id"], 0)
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "brokeuser2", "image": "registry/image:tag"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 400, resp.text)


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


class FeatureStreamlitUrl(HFLaunchTestBase):
    """Status poll surfaces streamlit_url only for pod_type=hackathon once 8501 is live."""

    def _launch_and_activate(self, user_name, pod_type=None):
        body = {"user_name": user_name, "image": "registry/image:tag"}
        if pod_type is not None:
            body["pod_type"] = pod_type
        resp = self.client.post("/api/huggingface/notebooks", json=body, headers=BEARER)
        self.assertEqual(resp.status_code, 200, resp.text)
        instance_id = self.fake.created[-1]["custom_instance_id"]
        # get_instance_by_id must resolve the instance for the status poll to proceed.
        self.fake.existing_ids[instance_id] = {"id": instance_id}
        return instance_id

    def _poll(self, user_name):
        resp = self.client.get(
            "/api/huggingface/notebooks/current",
            params={"user_name": user_name}, headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        return resp.json()

    def test_hackathon_ready_and_live_returns_streamlit_url(self):
        instance_id = self._launch_and_activate("sl-hack-live", pod_type="hackathon")
        self.fake.pod_status = "ready"
        self.fake.live_ports = {instance_id}
        body = self._poll("sl-hack-live")
        self.assertEqual(body["streamlit_url"], f"http://testserver/spaces/{instance_id}/8501/")
        self.assertIn((instance_id, main_module.STREAMLIT_APP_PORT), self.fake.port_probes)
        # The jupyter url is unaffected by the new field.
        self.assertIn(f"/instances/{instance_id}/lab", body["url"])

    def test_hackathon_ready_but_not_live_returns_none(self):
        instance_id = self._launch_and_activate("sl-hack-notlive", pod_type="hackathon")
        self.fake.pod_status = "ready"
        self.fake.live_ports = set()  # nothing listening on 8501 yet
        body = self._poll("sl-hack-notlive")
        self.assertIsNone(body["streamlit_url"])
        self.assertIn((instance_id, main_module.STREAMLIT_APP_PORT), self.fake.port_probes)

    def test_workshop_pod_type_never_gets_streamlit_url(self):
        instance_id = self._launch_and_activate("sl-workshop", pod_type="workshop")
        self.fake.pod_status = "ready"
        self.fake.live_ports = {instance_id}  # even with something listening on 8501...
        body = self._poll("sl-workshop")
        self.assertIsNone(body["streamlit_url"])  # ...the non-hackathon gate wins
        self.assertEqual(self.fake.port_probes, [])  # and the probe is never even attempted

    def test_no_pod_type_never_gets_streamlit_url(self):
        instance_id = self._launch_and_activate("sl-notag", pod_type=None)
        self.fake.pod_status = "ready"
        self.fake.live_ports = {instance_id}
        body = self._poll("sl-notag")
        self.assertIsNone(body["streamlit_url"])
        self.assertEqual(self.fake.port_probes, [])

    def test_hackathon_not_ready_skips_probe(self):
        instance_id = self._launch_and_activate("sl-hack-pending", pod_type="hackathon")
        self.fake.pod_status = "pending"
        self.fake.live_ports = {instance_id}  # would be live, but status isn't ready yet
        body = self._poll("sl-hack-pending")
        self.assertIsNone(body["streamlit_url"])
        self.assertEqual(self.fake.port_probes, [])


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

    def test_hf_images_reports_api_default_image(self):
        resp = self.client.get("/api/huggingface/images", headers=BEARER)
        self.assertEqual(resp.json()["default_image"],
                         main_module.settings.HUGGINGFACE_DEMO_DEFAULT_IMAGE)

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


class MultiGpuResourceProfiles(unittest.TestCase):
    """gpu_count -> auto resource profile sizing (CPU/RAM scale with GPUs)."""

    def setUp(self):
        from app.k8s_client import K8sClient
        self.k = K8sClient.__new__(K8sClient)

    def test_auto_profile_scales_with_gpu_count(self):
        cases = {
            1: ("standard", "16", "110Gi"),
            2: ("large", "32", "220Gi"),
            4: ("xlarge", "64", "440Gi"),
        }
        for gpu, (name, cpu_lim, mem_lim) in cases.items():
            pname, res = self.k._resolve_resource_profile(gpu, "auto")
            self.assertEqual(pname, name, f"gpu={gpu}")
            self.assertEqual(res["cpu_limit"], cpu_lim, f"gpu={gpu}")
            self.assertEqual(res["memory_limit"], mem_lim, f"gpu={gpu}")

    def test_profiles_fit_within_node_capacity(self):
        # Node hardware (measured): 128 CPU, ~1007 GiB, 8 GPU. A full node of same-size pods
        # must fit by REQUESTS (what the scheduler bin-packs on).
        from app.k8s_client import RESOURCE_PROFILES, AUTO_RESOURCE_PROFILE_BY_GPU
        NODE_CPU, NODE_GIB, NODE_GPU = 128, 1007, 8
        for gpu, pname in AUTO_RESOURCE_PROFILE_BY_GPU.items():
            res = RESOURCE_PROFILES[pname]
            pods_per_node = NODE_GPU // gpu
            cpu_req = int(res["cpu_request"]) * pods_per_node
            mem_req = int(res["memory_request"].rstrip("Gi")) * pods_per_node
            self.assertLessEqual(cpu_req, NODE_CPU, f"gpu={gpu} cpu requests overcommit")
            self.assertLessEqual(mem_req, NODE_GIB, f"gpu={gpu} mem requests overcommit")
            # A single pod's LIMIT must not exceed the node's GPU-proportional share.
            self.assertLessEqual(int(res["memory_limit"].rstrip("Gi")), (NODE_GIB // NODE_GPU) * gpu + 1)


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


class GitRepoParser(unittest.TestCase):
    """parse_huggingface_demo_git_path: .git shorthand/URL/scp + optional @branch."""

    def setUp(self):
        from app.notebook_sources import parse_huggingface_demo_git_path
        self.parse = parse_huggingface_demo_git_path

    def test_shorthand_no_branch(self):
        info = self.parse("org/repo.git")
        self.assertEqual(info["org"], "org")
        self.assertEqual(info["repo"], "repo")
        self.assertIsNone(info["branch"])
        self.assertEqual(info["path"], "")
        self.assertEqual(info["raw_url"], "")

    def test_shorthand_with_branch(self):
        info = self.parse("org/repo.git@dev")
        self.assertEqual((info["org"], info["repo"], info["branch"]), ("org", "repo", "dev"))

    def test_full_github_url(self):
        info = self.parse("https://github.com/org/repo.git")
        self.assertEqual((info["org"], info["repo"]), ("org", "repo"))

    def test_full_github_url_with_branch(self):
        info = self.parse("https://github.com/org/repo.git@feature/x")
        self.assertEqual(info["branch"], "feature/x")

    def test_scp_form(self):
        info = self.parse("git@github.com:org/repo.git")
        self.assertEqual((info["org"], info["repo"]), ("org", "repo"))

    def test_non_git_returns_none(self):
        self.assertIsNone(self.parse("org/repo"))
        self.assertIsNone(self.parse("https://huggingface.co/x/y/blob/main/a.ipynb"))
        self.assertIsNone(self.parse(""))

    def test_non_github_host_rejected(self):
        with self.assertRaises(ValueError):
            self.parse("https://gitlab.com/o/r.git")

    def test_non_github_scp_rejected(self):
        # scp form must enforce the GitHub host too (not silently accepted as a corrupt org).
        with self.assertRaises(ValueError):
            self.parse("git@gitlab.com:org/repo.git")
        with self.assertRaises(ValueError):
            self.parse("git@bitbucket.org:o/r.git")

    def test_trailing_slash_query_fragment_tolerated(self):
        self.assertEqual(self.parse("org/repo.git/")["repo"], "repo")
        self.assertEqual(self.parse("https://github.com/org/repo.git?x=1")["repo"], "repo")
        self.assertEqual(self.parse("https://github.com/org/repo.git#frag")["org"], "org")

    def test_malformed_rejected(self):
        with self.assertRaises(ValueError):
            self.parse("org/x/y.git")
        with self.assertRaises(ValueError):
            self.parse("just-one.git")


class GitWorkshopLaunch(HFLaunchTestBase):
    """HF launch: a .git repo is workshop-only, builds a clone-ready github_info."""

    def test_workshop_git_launch_builds_clone_info(self):
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "wsa", "image": "registry/image:tag",
                  "pod_type": "workshop", "notebook_path": "org/repo.git"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        gi = self.fake.created[0]["github_info"]
        self.assertEqual(gi["org"], "org")
        self.assertEqual(gi["repo"], "repo")
        self.assertEqual(gi["path"], "")
        self.assertTrue(gi.get("repo_url"))
        self.assertTrue(self.fake.created[0]["api_launched"])
        # Root URL (no notebook path appended).
        self.assertNotIn("/tree/", resp.json()["url"])

    def test_workshop_git_branch_threaded(self):
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "wsb", "image": "registry/image:tag",
                  "pod_type": "workshop", "notebook_path": "org/repo.git@dev"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(self.fake.created[0]["github_info"]["branch"], "dev")

    def test_git_requires_workshop_pod_type(self):
        for body in (
            {"user_name": "wsc", "image": "registry/image:tag",
             "pod_type": "hackathon", "notebook_path": "org/repo.git"},
            {"user_name": "wsd", "image": "registry/image:tag",
             "notebook_path": "org/repo.git"},  # no pod_type
        ):
            resp = self.client.post("/api/huggingface/notebooks", json=body, headers=BEARER)
            self.assertEqual(resp.status_code, 400, resp.text)
            self.assertIn("workshop", resp.json()["detail"])

    def test_blank_path_still_none(self):
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "wse", "image": "registry/image:tag", "pod_type": "workshop"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertIsNone(self.fake.created[0]["github_info"])

    def test_non_git_non_workshop_still_parses_ipynb(self):
        # A normal .ipynb HF URL must NOT be forced into workshop-only.
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "wsf", "image": "registry/image:tag",
                  "notebook_path": "https://huggingface.co/org/repo/blob/main/x.ipynb"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        gi = self.fake.created[0]["github_info"]
        self.assertEqual(gi["path"], "x.ipynb")

    def test_git_token_threaded_to_create_instance_over_https(self):
        # A workshop .git launch whose clone URL resolves to https:// forwards the token.
        main_module.settings.GITHUB_WEB_BASE = "https://gh-proxy.example.com"
        try:
            resp = self.client.post(
                "/api/huggingface/notebooks",
                json={"user_name": "wtok", "image": "registry/image:tag",
                      "pod_type": "workshop", "notebook_path": "org/repo.git",
                      "git_token": "ghp_secrettoken"},
                headers=BEARER,
            )
            self.assertEqual(resp.status_code, 200, resp.text)
            self.assertEqual(self.fake.created[0]["git_token"], "ghp_secrettoken")
        finally:
            main_module.settings.GITHUB_WEB_BASE = "https://github.com"

    def test_git_token_rejected_over_plain_http(self):
        # Default config resolves github.com to a plain-http clone URL; a token must be refused
        # (fail closed) so the credential is never sent in cleartext on the wire.
        main_module.settings.GITHUB_WEB_BASE = "https://github.com"  # not a proxy -> http clone
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "wtokhttp", "image": "registry/image:tag",
                  "pod_type": "workshop", "notebook_path": "org/repo.git",
                  "git_token": "ghp_secrettoken"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 400, resp.text)
        self.assertIn("HTTPS", resp.json()["detail"])
        self.assertEqual(self.fake.created, [])

    def test_blank_git_token_forwarded_as_none(self):
        # A whitespace/blank git_token is normalized to None, not an empty string (and the
        # HTTPS gate does not trip because there is effectively no token).
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "wtok2", "image": "registry/image:tag",
                  "pod_type": "workshop", "notebook_path": "org/repo.git",
                  "git_token": "   "},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertIsNone(self.fake.created[0]["git_token"])

    def test_git_token_rejected_for_non_git_launch(self):
        # A git_token on an .ipynb (non-git) launch is a 400, never silently dropped.
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "wtok3", "image": "registry/image:tag",
                  "notebook_path": "https://huggingface.co/org/repo/blob/main/x.ipynb",
                  "git_token": "ghp_secrettoken"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 400, resp.text)
        self.assertIn("git_token", resp.json()["detail"])
        self.assertEqual(self.fake.created, [])

    def test_git_token_rejected_for_bare_launch(self):
        # A git_token with no repo at all (bare Jupyter) is also rejected.
        resp = self.client.post(
            "/api/huggingface/notebooks",
            json={"user_name": "wtok4", "image": "registry/image:tag",
                  "pod_type": "workshop", "git_token": "ghp_secrettoken"},
            headers=BEARER,
        )
        self.assertEqual(resp.status_code, 400, resp.text)
        self.assertIn("git_token", resp.json()["detail"])


class GitTokenInitContainer(unittest.TestCase):
    """_get_pod_manifest runs the authenticated clone in an isolated init container, and keeps
    the token off the user's notebook container entirely."""

    def setUp(self):
        _pin_settings()
        from app import k8s_client as k8s_mod
        self.builder = k8s_mod.K8sClient.__new__(k8s_mod.K8sClient)
        self.builder.namespace = "test-ns"
        # Neutralize node/quota/localcache/image-service helpers so _get_pod_manifest is pure.
        self.builder._get_labels = lambda email, iid: {"app": iid}
        self.builder._resolve_resource_profile = lambda gpu, prof: (
            "standard", {"cpu_limit": "16", "cpu_request": "8", "memory_limit": "110Gi",
                         "memory_request": "55Gi"})
        self.builder._workspace_localcache_enabled = lambda: False
        self.builder._notebook_tolerations = lambda: []

    def _gi(self):
        return {"org": "org", "repo": "repo", "branch": "main", "path": "", "raw_url": "",
                "repo_url": "https://gh-proxy.example.com/org/repo.git"}

    def _manifest(self, git_token):
        return self.builder._get_pod_manifest(
            "u@example.com", "hf-1-abc", "registry/image:tag",
            instance_type="jupyter", gpu_count=1, github_info=self._gi(),
            pod_type="workshop", api_launched=True, git_token=git_token)

    def _notebook_container(self, manifest):
        return manifest["spec"]["containers"][0]

    def _init_by_name(self, manifest, name):
        return next((c for c in manifest["spec"].get("initContainers", []) if c["name"] == name), None)

    def test_token_launch_adds_isolated_clone_init_container(self):
        m = self._manifest("ghp_secrettoken")
        clone = self._init_by_name(m, "git-clone")
        self.assertIsNotNone(clone, "git-clone init container missing")
        # Token is on the init container env (the ONLY place it lives).
        env_names = {e["name"]: e for e in clone["env"]}
        self.assertIn("GIT_CLONE_TOKEN", env_names)
        self.assertEqual(env_names["GIT_CLONE_TOKEN"]["value"], "ghp_secrettoken")
        # Auth uses GIT_ASKPASS reading the env var; token not on argv / not literal in script.
        script = clone["args"][0]
        self.assertIn("GIT_ASKPASS", script)
        self.assertIn("x-access-token", script)  # username baked in; token is the password
        self.assertNotIn("ghp_secrettoken", script)
        self.assertIn("GIT_TERMINAL_PROMPT=0", script)

    def test_token_never_reaches_notebook_container(self):
        m = self._manifest("ghp_secrettoken")
        nb = self._notebook_container(m)
        env_names = {e["name"] for e in nb["env"]}
        self.assertNotIn("GIT_CLONE_TOKEN", env_names)
        # The token value appears nowhere in the notebook container (env or startup script).
        import json as _json
        self.assertNotIn("ghp_secrettoken", _json.dumps(nb))

    def test_non_token_launch_has_no_clone_init_container(self):
        m = self._manifest(None)
        self.assertIsNone(self._init_by_name(m, "git-clone"))
        # Public clone still happens in the notebook container's startup script.
        nb = self._notebook_container(m)
        self.assertIn("clone --depth 1", nb["args"][0])

    def test_clone_init_container_fails_closed_on_non_https(self):
        # Belt-and-suspenders: the helper itself refuses a token over a non-https clone URL,
        # independently of the main.py gate, so no caller can downgrade the secret.
        gi = {"org": "org", "repo": "repo", "branch": "main", "path": "", "raw_url": "",
              "repo_url": "http://github.com/org/repo.git"}
        with self.assertRaises(ValueError):
            self.builder._git_clone_init_container("registry/image:tag", gi, None, "ghp_secrettoken")


class TemplateRepoOnly(unittest.TestCase):
    """Notebook templates may clone a repo with no notebook path."""

    def setUp(self):
        _pin_settings()

    def test_save_allows_repo_without_notebook(self):
        from app.models import NotebookTemplateRequest
        store.init_db()
        store.upsert_image("ti", "registry/image:tag", "", True)
        req = NotebookTemplateRequest(
            title="repo-only", slug="repo-only", image="registry/image:tag",
            repo_url="https://github.com/org/repo", branch="main",
            notebook_path="", instance_type="jupyter",
        )
        tmpl = main_module._save_notebook_template(req)
        self.assertEqual(tmpl["repo_url"], "https://github.com/org/repo")
        self.assertFalse((tmpl.get("notebook_path") or ""))

    def test_save_rejects_notebook_without_repo(self):
        from app.models import NotebookTemplateRequest
        store.init_db()
        store.upsert_image("ti2", "registry/image:tag", "", True)
        req = NotebookTemplateRequest(
            title="no-repo", slug="no-repo", image="registry/image:tag",
            repo_url="", branch="main",
            notebook_path="notebooks/x.ipynb", instance_type="jupyter",
        )
        with self.assertRaises(ValueError):
            main_module._save_notebook_template(req)

    def test_github_info_built_for_repo_only(self):
        gi = main_module._template_github_info({
            "instance_type": "jupyter", "repo_url": "https://github.com/org/repo",
            "branch": "main", "notebook_path": "", "id": 1, "title": "t",
        })
        self.assertTrue(gi)  # not {}
        self.assertEqual(gi["path"], "")
        self.assertEqual(gi["raw_url"], "")
        self.assertTrue(gi["repo_url"])

    def test_github_info_empty_without_repo(self):
        gi = main_module._template_github_info({
            "instance_type": "jupyter", "repo_url": "", "branch": "main",
            "notebook_path": "", "id": 1, "title": "t",
        })
        self.assertEqual(gi, {})


class StartupScriptClone(unittest.TestCase):
    """_build_startup_script clone branch: repo-only skips notebook-not-found; branch optional."""

    def setUp(self):
        from app.k8s_client import K8sClient
        self.k = K8sClient.__new__(K8sClient)

    def _script(self, github_info):
        return self.k._build_startup_script("inst-1", "jupyter", github_info)

    def test_repo_only_no_notebook_check_no_branch_flag(self):
        gi = {"org": "o", "repo": "r", "branch": None, "path": "",
              "raw_url": "", "repo_url": "http://github.com/o/r.git"}
        s = self._script(gi)
        self.assertIn("clone", s)
        self.assertNotIn("Notebook not found", s)
        self.assertNotIn("--branch", s)
        self.assertIn("/repo", s)

    def test_repo_with_path_keeps_notebook_check(self):
        gi = {"org": "o", "repo": "r", "branch": "main", "path": "nb/x.ipynb",
              "raw_url": "", "repo_url": "http://github.com/o/r.git"}
        s = self._script(gi)
        self.assertIn("Notebook not found", s)
        self.assertIn("--branch main", s)

    def test_repo_only_with_branch(self):
        gi = {"org": "o", "repo": "r", "branch": "dev", "path": "",
              "raw_url": "", "repo_url": "http://github.com/o/r.git"}
        s = self._script(gi)
        self.assertIn("--branch dev", s)
        self.assertNotIn("Notebook not found", s)

    def test_hackathon_installs_jupyter_server_proxy_before_launch(self):
        gi = {"org": "o", "repo": "r", "branch": None, "path": "",
              "raw_url": "", "repo_url": "http://github.com/o/r.git"}
        s = self.k._build_startup_script("inst-1", "jupyter", gi, pod_type="hackathon")
        self.assertIn("jupyter-server-proxy", s)
        self.assertIn(
            f"pip install --no-cache-dir -i {main_module.settings.PIP_INDEX_URL} "
            f"--trusted-host {main_module.settings.PYPI_HOST} jupyter-server-proxy",
            s,
        )
        proxy_idx = s.index("jupyter-server-proxy")
        launch_idx = s.index("jupyter lab --ip=")
        self.assertLess(proxy_idx, launch_idx)

    def test_non_hackathon_skips_jupyter_server_proxy(self):
        gi = {"org": "o", "repo": "r", "branch": None, "path": "",
              "raw_url": "", "repo_url": "http://github.com/o/r.git"}
        s_none = self.k._build_startup_script("inst-1", "jupyter", gi, pod_type=None)
        self.assertNotIn("jupyter-server-proxy", s_none)

        s_workshop = self.k._build_startup_script("inst-1", "jupyter", gi, pod_type="workshop")
        self.assertNotIn("jupyter-server-proxy", s_workshop)


class GpuClusterStatus(unittest.TestCase):
    """gpu_cluster_status: all service GPU nodes incl unhealthy; committed/free from own ns."""

    def setUp(self):
        _pin_settings()
        from app.k8s_client import K8sClient
        self.k = K8sClient.__new__(K8sClient)
        self.k.namespace = "test-ns"

    def _node(self, name, gpus, ready=True, cordoned=False, cpu="128", mem="1056403496Ki",
              model="AMD_Radeon_Pro_W7900D"):
        from types import SimpleNamespace as NS
        labels = {}
        if model:
            labels["amd.com/gpu.product-name"] = model
        allocatable = {"cpu": cpu, "memory": mem}
        if gpus is not None:
            allocatable["amd.com/gpu"] = str(gpus)
        return NS(
            metadata=NS(name=name, labels=labels),
            spec=NS(unschedulable=cordoned, taints=[]),
            status=NS(
                allocatable=allocatable,
                conditions=[NS(type="Ready", status="True" if ready else "False")],
            ),
        )

    def _pod(self, instance_id, node_name, gpus, phase="Running", pod_type=None):
        from types import SimpleNamespace as NS
        return NS(
            metadata=NS(name=f"{instance_id}-pod", labels={"instance-id": instance_id},
                        annotations={"amd-oneclick/email": f"{instance_id}@x.local",
                                     "amd-oneclick/pod-type": pod_type}),
            spec=NS(node_name=node_name, containers=[
                NS(resources=NS(requests={"amd.com/gpu": str(gpus)}))]),
            status=NS(phase=phase),
        )

    def _patch(self, nodes, pods, raise_pods=False, raise_nodes=False):
        from kubernetes.client.rest import ApiException
        from types import SimpleNamespace as NS

        def list_node():
            if raise_nodes:
                raise ApiException(status=403)
            return NS(items=nodes)

        def list_namespaced_pod(namespace=None, label_selector=None, **kw):
            if raise_pods:
                raise ApiException(status=403)
            return NS(items=pods)

        self.k.core_v1 = NS(list_node=list_node, list_namespaced_pod=list_namespaced_pod)
        # default service (no toleration key) -> every untainted node belongs
        main_module.settings.NOTEBOOK_TOLERATION_KEY = ""
        main_module.settings.NOTEBOOK_TOLERATION_VALUE = ""
        import app.k8s_client as kc
        kc.settings.NOTEBOOK_TOLERATION_KEY = ""
        kc.settings.NOTEBOOK_TOLERATION_VALUE = ""

    def test_all_gpu_nodes_listed_incl_unhealthy(self):
        nodes = [
            self._node("n-ok", 8),
            self._node("n-cordon", 8, cordoned=True),
            self._node("n-notready", 8, ready=False),
            self._node("n-nogpu", 0),       # skipped: not a GPU node
            self._node("n-nogpukey", None), # skipped: no amd.com/gpu
        ]
        self._patch(nodes, [])
        out = self.k.gpu_cluster_status()
        names = [n["node"] for n in out["nodes"]]
        self.assertEqual(names, ["n-cordon", "n-notready", "n-ok"])  # sorted, GPU-only
        self.assertEqual(out["total_gpus"], 24)
        # free only counts the healthy schedulable node
        self.assertEqual(out["free_gpus"], 8)
        self.assertEqual(out["usage_scope"], "namespace")

    def test_committed_and_free_from_namespace_pods(self):
        nodes = [self._node("n1", 8)]
        pods = [self._pod("i1", "n1", 2), self._pod("i2", "n1", 3),
                self._pod("done", "n1", 4, phase="Succeeded")]  # terminal excluded
        self._patch(nodes, pods)
        out = self.k.gpu_cluster_status()
        n = out["nodes"][0]
        self.assertEqual(n["committed"], 5)
        self.assertEqual(n["free"], 3)
        self.assertEqual(out["used_gpus"], 5)
        self.assertEqual(len(n["instances"]), 2)

    def test_committed_capped_at_node_total(self):
        nodes = [self._node("n1", 4)]
        pods = [self._pod("i1", "n1", 8)]  # over-subscribed reading
        self._patch(nodes, pods)
        out = self.k.gpu_cluster_status()
        self.assertEqual(out["nodes"][0]["committed"], 4)
        self.assertEqual(out["nodes"][0]["free"], 0)

    def test_node_metadata_surfaced(self):
        self._patch([self._node("n1", 8)], [])
        n = self.k.gpu_cluster_status()["nodes"][0]
        self.assertEqual(n["gpu_model"], "AMD_Radeon_Pro_W7900D")
        self.assertEqual(n["cpu_allocatable"], "128")
        self.assertGreater(n["memory_allocatable_gib"], 900)
        self.assertTrue(n["ready"])
        self.assertFalse(n["cordoned"])

    def test_list_nodes_403_degrades(self):
        self._patch([], [], raise_nodes=True)
        out = self.k.gpu_cluster_status()
        self.assertEqual(out, {"total_gpus": 0, "free_gpus": 0, "used_gpus": 0,
                               "nodes": [], "usage_scope": "namespace"})

    def test_list_pods_403_still_returns_capacity(self):
        self._patch([self._node("n1", 8)], [], raise_pods=True)
        out = self.k.gpu_cluster_status()
        self.assertEqual(out["nodes"][0]["committed"], 0)
        self.assertEqual(out["nodes"][0]["free"], 8)
        self.assertEqual(out["total_gpus"], 8)

    def test_memory_to_gib_parsing(self):
        from app.k8s_client import K8sClient
        self.assertEqual(K8sClient._memory_to_gib("1073741824"), 1.0)  # 1 GiB in bytes
        self.assertEqual(K8sClient._memory_to_gib("1048576Ki"), 1.0)
        self.assertEqual(K8sClient._memory_to_gib("1024Mi"), 1.0)
        self.assertEqual(K8sClient._memory_to_gib("2Gi"), 2.0)
        self.assertEqual(K8sClient._memory_to_gib(None), 0.0)
        self.assertEqual(K8sClient._memory_to_gib("garbage"), 0.0)


class GpuDashboardEndpoint(unittest.TestCase):
    """/api/admin/gpu-nodes is admin-gated and 404 unless GPU_DASHBOARD_ENABLED."""

    def setUp(self):
        _pin_settings()
        store.init_db()
        self.client = TestClient(main_module.app)
        self._orig = main_module.k8s_client
        from types import SimpleNamespace as NS
        main_module.k8s_client = NS(gpu_cluster_status=lambda: {
            "total_gpus": 8, "free_gpus": 5, "used_gpus": 3, "nodes": [], "usage_scope": "namespace"})

    def tearDown(self):
        main_module.k8s_client = self._orig
        main_module.settings.GPU_DASHBOARD_ENABLED = False

    def test_404_when_disabled(self):
        main_module.settings.GPU_DASHBOARD_ENABLED = False
        r = self.client.get("/api/admin/gpu-nodes", auth=ADMIN)
        self.assertEqual(r.status_code, 404, r.text)

    def test_200_when_enabled(self):
        main_module.settings.GPU_DASHBOARD_ENABLED = True
        r = self.client.get("/api/admin/gpu-nodes", auth=ADMIN)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["total_gpus"], 8)

    def test_requires_admin_auth(self):
        main_module.settings.GPU_DASHBOARD_ENABLED = True
        r = self.client.get("/api/admin/gpu-nodes")
        self.assertEqual(r.status_code, 401)


if __name__ == "__main__":
    unittest.main()
