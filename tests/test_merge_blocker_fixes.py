"""Regression tests for the oauth->BETA merge blocker fixes.

Covers:
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

from tests.kube_stub import install  # noqa: E402

install()

from app import main as main_module  # noqa: E402


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
