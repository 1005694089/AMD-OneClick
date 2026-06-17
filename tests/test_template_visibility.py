import os
import tempfile
import unittest

# (a) Env before any app import.
_DB_FD, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.close(_DB_FD)
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["ADMIN_PASSWORD"] = "testpass"
os.environ["NOTEBOOK_NODE_NAME"] = "fake-node"

# (b) Kube stub before importing app.main.
from tests.kube_stub import install  # noqa: E402

install()

from app import main as main_module  # noqa: E402
from app import store  # noqa: E402
from app.models import NotebookTemplateRequest  # noqa: E402


class CustomImageTemplateVisibilityTests(unittest.TestCase):
    """A template backed by a user custom image must stay Profile-only (enabled=False),
    even when an editor creates it, so it never surfaces in the public gallery."""

    def setUp(self):
        store.init_db()
        self.user_id = 4242
        # A build-ready custom image owned by this user.
        self.custom_image = f"reg.example/custom/user-{self.user_id}:demo"
        store.create_custom_image(self.user_id, "demo", self.custom_image, "FROM scratch", 5)
        ready = store.update_custom_image_status(
            store.list_custom_images(self.user_id)[0]["id"], status="ready"
        )
        self.assertIsNotNone(ready)

    def tearDown(self):
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM custom_images")
            conn.exec_driver_sql("DELETE FROM notebook_templates")

    def _req(self, image):
        return NotebookTemplateRequest(title="My T", image=image, enabled=True)

    def test_editor_custom_image_template_forced_private(self):
        # Editor explicitly asks enabled=True, but the backing image is a private custom image.
        tmpl = main_module._save_notebook_template(
            self._req(self.custom_image),
            owner_user_id=self.user_id,
            enabled_override=True,
        )
        self.assertFalse(tmpl["enabled"])
        # And it must not appear in the public (enabled_only) list.
        public = store.list_notebook_templates(enabled_only=True)
        self.assertNotIn(tmpl["id"], [t["id"] for t in public])

    def test_catalog_image_template_keeps_requested_enabled(self):
        # A real catalog image template still honors the editor's enabled choice.
        store.upsert_image("base", "reg.example/catalog/base:1", "", True)
        tmpl = main_module._save_notebook_template(
            self._req("reg.example/catalog/base:1"),
            owner_user_id=self.user_id,
            enabled_override=True,
        )
        self.assertTrue(tmpl["enabled"])


if __name__ == "__main__":
    unittest.main()
