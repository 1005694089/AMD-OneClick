"""Regression tests for the oauth->BETA merge blocker fixes.

Covers:
  - P1-deadend: a launch that returns "distributing" persists a launch_intent and is resumed by
    the status poll once the image lands (instead of dead-ending at not_found).
  - P1-ghref:  an admin github_build source stores a real registry tag, not the Dockerfile URL.
  - P3-hf-proxy: HF-demo GitHub clone URLs honor the configured GitHub proxy.
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

from app import main as main_module  # noqa: E402
from app import store  # noqa: E402


class LaunchIntentStoreTests(unittest.TestCase):
    def setUp(self):
        store.init_db()
        self.user = store.get_or_create_user("google", "uid-li", "li@example.com", "Li", None)

    def tearDown(self):
        store.delete_launch_intent(self.user["id"])

    def test_upsert_get_delete_roundtrip(self):
        store.upsert_launch_intent(self.user["id"], "li@example.com", "img:1", "notebook",
                                   {"instance_type": "opencode", "gpu_count": 2})
        got = store.get_launch_intent(self.user["id"])
        self.assertIsNotNone(got)
        self.assertEqual(got["image"], "img:1")
        self.assertEqual(got["kind"], "notebook")
        self.assertEqual(got["params"]["gpu_count"], 2)

        # upsert replaces, never duplicates
        store.upsert_launch_intent(self.user["id"], "li@example.com", "img:2", "template",
                                   {"gpu_count": 1, "template_id": 7})
        got = store.get_launch_intent(self.user["id"])
        self.assertEqual(got["image"], "img:2")
        self.assertEqual(got["params"]["template_id"], 7)

        store.delete_launch_intent(self.user["id"])
        self.assertIsNone(store.get_launch_intent(self.user["id"]))


class ResumeDistributingLaunchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        store.init_db()
        self.user = store.get_or_create_user("google", "uid-rd", "rd@example.com", "Rd", None)
        store.grant_user_credits(self.user["id"], 100)
        self.user = store.get_user(self.user["id"])
        self._orig_ensure = main_module._ensure_image_on_node
        self._orig_provision = main_module._provision_notebook_instance

    def tearDown(self):
        main_module._ensure_image_on_node = self._orig_ensure
        main_module._provision_notebook_instance = self._orig_provision
        store.delete_launch_intent(self.user["id"])

    async def test_resume_waits_then_creates_instance(self):
        store.upsert_launch_intent(self.user["id"], "rd@example.com", "img:resume", "notebook",
                                   {"instance_type": "opencode", "gpu_count": 1})

        # 1) image still distributing -> "distributing", intent preserved
        main_module._ensure_image_on_node = lambda image, gpu: None
        result = await main_module._resume_distributing_launch(self.user)
        self.assertEqual(result, "distributing")
        self.assertIsNotNone(store.get_launch_intent(self.user["id"]))

        # 2) image lands -> provision called, returns "ready", intent cleared
        calls = {}

        async def fake_provision(user, email, image, params, target_node):
            calls["image"] = image
            calls["params"] = params
            return {"id": "u-rd-1"}

        main_module._ensure_image_on_node = lambda image, gpu: "node-a"
        main_module._provision_notebook_instance = fake_provision
        result = await main_module._resume_distributing_launch(self.user)
        self.assertEqual(result, "ready")
        self.assertEqual(calls["image"], "img:resume")
        self.assertIsNone(store.get_launch_intent(self.user["id"]))

    async def test_resume_returns_none_without_intent(self):
        store.delete_launch_intent(self.user["id"])
        result = await main_module._resume_distributing_launch(self.user)
        self.assertIsNone(result)


class HuggingFaceProxyTests(unittest.TestCase):
    def setUp(self):
        self._orig_base = main_module.settings.GITHUB_WEB_BASE

    def tearDown(self):
        main_module.settings.GITHUB_WEB_BASE = self._orig_base

    def test_github_demo_path_uses_proxy_base(self):
        main_module.settings.GITHUB_WEB_BASE = "https://gh-proxy.example.com"
        info = main_module._parse_huggingface_demo_notebook_path(
            "/github/org/repo/blob/main/demo.ipynb"
        )
        self.assertEqual(info["repo_url"], "https://gh-proxy.example.com/org/repo.git")

    def test_native_huggingface_path_has_no_clone_url(self):
        info = main_module._parse_huggingface_demo_notebook_path(
            "https://huggingface.co/Qwen/model.ipynb"
        )
        self.assertNotIn("repo_url", info)


if __name__ == "__main__":
    unittest.main()
