"""P1 LAN-registry-push tests for the Dragonfly migration.

Covers:
  - digest column + setters + migration (images / custom_images).
  - image_digest_present gate helper.
  - push kind is a claimable job kind.
  - Chain wiring: admin/custom chains include `push` only when LAN_REGISTRY is set (dormant when
    empty, so deploying P1 before wiring the env is a no-op).
  - Lifecycle: build defers the custom-image ready flip when a push step follows; the push branch
    records the digest and flips ready.
  - get_image_sync_status: digest-gated only when LAN_REGISTRY is set.
  - agent._parse_repo_digest parses the registry manifest digest from inspect output.
"""
import os
import sys
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
from app.config import settings  # noqa: E402


def _reset():
    store.init_db()
    with store.engine.begin() as conn:
        for tbl in (store.image_jobs, store.image_nodes, store.images, store.custom_images):
            conn.execute(tbl.delete())


class DigestColumnTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_set_and_read_image_digest(self):
        img = store.upsert_image("demo", "reg/image:tag", "", True)
        self.assertFalse(store.image_digest_present("reg/image:tag"))
        store.set_image_digest(img["id"], "sha256:abc")
        self.assertTrue(store.image_digest_present("reg/image:tag"))

    def test_set_and_read_custom_image_digest(self):
        rec = store.create_custom_image(
            user_id=1, name="c", image="reg/user-1:c", dockerfile="FROM x", max_per_user=10
        )
        self.assertFalse(store.image_digest_present("reg/user-1:c"))
        store.set_custom_image_digest(rec["id"], "sha256:def")
        self.assertTrue(store.image_digest_present("reg/user-1:c"))

    def test_digest_present_false_for_unknown_ref(self):
        self.assertFalse(store.image_digest_present("nope/none:0"))
        self.assertFalse(store.image_digest_present(""))

    def test_push_is_a_job_kind(self):
        self.assertIn("push", store.IMAGE_JOB_KINDS)


class ChainWiringTests(unittest.TestCase):
    def setUp(self):
        _reset()
        self._orig = settings.LAN_REGISTRY

    def tearDown(self):
        settings.LAN_REGISTRY = self._orig

    def test_admin_chain_omits_push_when_registry_unset(self):
        settings.LAN_REGISTRY = ""
        self.assertIsNone(main_module._lan_registry_target_ref("reg/image:tag"))

    def test_admin_chain_includes_push_when_registry_set(self):
        settings.LAN_REGISTRY = "10.5.10.43:5000"
        ref = main_module._lan_registry_target_ref("crpi.example.com/radeon/image:tag")
        # Existing registry host stripped, re-homed under the LAN registry.
        self.assertEqual(ref, "10.5.10.43:5000/radeon/image:tag")

    def test_admin_enqueue_chains_push_before_distribute(self):
        settings.LAN_REGISTRY = "10.5.10.43:5000"
        img = store.upsert_image("demo", "reg/image:tag", "", True)
        main_module._enqueue_admin_image_chain(
            {"id": img["id"], "image": "reg/image:tag"}, "acr_pull", "reg/image:tag"
        )
        # Head job is pull; its payload chain must contain push then distribute.
        with store.engine.begin() as conn:
            row = conn.execute(
                store.image_jobs.select().where(store.image_jobs.c.ref == "reg/image:tag")
            ).mappings().first()
        import json
        payload = json.loads(row["payload"])
        self.assertEqual(payload["chain"], ["push", "distribute"])
        self.assertEqual(payload["lan_target_ref"], "10.5.10.43:5000/reg/image:tag")


class LifecycleReadyFlipTests(unittest.TestCase):
    def setUp(self):
        _reset()

    def test_build_defers_ready_when_push_follows(self):
        rec = store.create_custom_image(
            user_id=1, name="c", image="reg/user-1:c", dockerfile="FROM x", max_per_user=10
        )
        job = {
            "kind": "build",
            "ref": "reg/user-1:c",
            "custom_image_id": rec["id"],
            "payload": {"chain": ["push"], "lan_target_ref": "10.5.10.43:5000/user-1:c"},
        }
        main_module._sync_image_job_lifecycle(job, {"ref": "reg/user-1:c", "built": True})
        row = store.get_custom_image(rec["id"], user_id=1)
        self.assertNotEqual(row["build_status"], "ready")  # deferred to push

    def test_build_flips_ready_when_no_push(self):
        rec = store.create_custom_image(
            user_id=1, name="c2", image="reg/user-1:c2", dockerfile="FROM x", max_per_user=10
        )
        job = {"kind": "build", "ref": "reg/user-1:c2", "custom_image_id": rec["id"], "payload": {}}
        main_module._sync_image_job_lifecycle(job, {"ref": "reg/user-1:c2", "built": True})
        row = store.get_custom_image(rec["id"], user_id=1)
        self.assertEqual(row["build_status"], "ready")

    def test_push_branch_records_digest_and_flips_ready(self):
        rec = store.create_custom_image(
            user_id=1, name="c3", image="reg/user-1:c3", dockerfile="FROM x", max_per_user=10
        )
        job = {
            "kind": "push",
            "ref": "reg/user-1:c3",
            "custom_image_id": rec["id"],
            "payload": {"lan_target_ref": "10.5.10.43:5000/user-1:c3"},
        }
        main_module._sync_image_job_lifecycle(
            job, {"ref": "reg/user-1:c3", "digest": "sha256:deadbeef"}
        )
        row = store.get_custom_image(rec["id"], user_id=1)
        self.assertEqual(row["build_status"], "ready")
        self.assertTrue(store.image_digest_present("reg/user-1:c3"))


class ReadinessNotDigestGatedTests(unittest.TestCase):
    """Readiness must NOT depend on a recorded digest. Durability is guaranteed structurally
    (push runs before distribute), so gating on digest would strand pre-P1 images (digest=NULL)
    and images whose digest couldn't be parsed. The digest column is recorded only for P5
    delete-by-digest. This encodes the backward-compat fix from the P1 adversarial review."""

    def setUp(self):
        _reset()
        self._orig = settings.LAN_REGISTRY

    def tearDown(self):
        settings.LAN_REGISTRY = self._orig

    def test_sync_status_source_has_no_digest_gate(self):
        # Guard against a regression that reintroduces the digest gate into get_image_sync_status.
        import inspect
        src = inspect.getsource(
            type(main_module.k8s_client).get_image_sync_status
        )
        self.assertNotIn("digest_ok", src)
        self.assertNotIn("image_digest_present", src)

    def test_digest_helper_still_available_for_p5(self):
        # image_digest_present is retained (P5 delete-by-digest) even though it no longer gates ready.
        img = store.upsert_image("demo2", "reg/image2:tag", "", True)
        self.assertFalse(store.image_digest_present("reg/image2:tag"))
        store.set_image_digest(img["id"], "sha256:cafe")
        self.assertTrue(store.image_digest_present("reg/image2:tag"))


class ParseRepoDigestTests(unittest.TestCase):
    def _agent(self):
        import importlib.util
        path = os.path.join(os.path.dirname(__file__), "..", "image-service", "agent.py")
        # agent.py reads env at import; provide the minimum so module import succeeds.
        os.environ.setdefault("MANAGER_URL", "http://localhost")
        os.environ.setdefault("BUILD_AGENT_TOKEN", "t")
        spec = importlib.util.spec_from_file_location("_dfwt_agent", os.path.abspath(path))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_parse_matches_repo(self):
        agent = self._agent()
        lan_ref = "10.5.10.43:5000/radeon/image:tag"
        inspect = (
            '{"RepoDigests": ["10.5.10.43:5000/radeon/image@sha256:aaaa", '
            '"other/repo@sha256:bbbb"]}'
        )
        self.assertEqual(agent._parse_repo_digest(inspect, lan_ref), "sha256:aaaa")

    def test_parse_fallback_first_digest(self):
        agent = self._agent()
        lan_ref = "10.5.10.43:5000/radeon/image:tag"
        inspect = '{"RepoDigests": ["mismatch/repo@sha256:cccc"]}'
        self.assertEqual(agent._parse_repo_digest(inspect, lan_ref), "sha256:cccc")

    def test_parse_none_when_empty(self):
        agent = self._agent()
        self.assertIsNone(agent._parse_repo_digest('{"RepoDigests": []}', "r/i:t"))
        self.assertIsNone(agent._parse_repo_digest("not json", "r/i:t"))


if __name__ == "__main__":
    unittest.main()
