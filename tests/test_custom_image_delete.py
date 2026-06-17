import os
import tempfile
import unittest

# Env before any app import.
_DB_FD, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.close(_DB_FD)
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ.setdefault("ADMIN_PASSWORD", "testpass")

from app import store  # noqa: E402


class DeleteCustomImageGuardTests(unittest.TestCase):
    """delete_custom_image must refuse in-flight (pending/building) builds: the image tag is
    mutable and reused (user-{id}:{name}), so deleting a running build lets the user recreate
    the same name while the old agent is still building and later pushes stale content onto the
    new tag."""

    def setUp(self):
        store.init_db()
        self.user_id = 9001

    def tearDown(self):
        with store.engine.begin() as conn:
            conn.exec_driver_sql("DELETE FROM custom_images")

    def _make(self, name="demo"):
        store.create_custom_image(
            self.user_id, name, f"reg.example/user-{self.user_id}:{name}", "FROM scratch", 5
        )
        return store.list_custom_images(self.user_id)[0]["id"]

    def test_delete_rejected_while_pending(self):
        image_id = self._make()  # create_custom_image starts in 'pending'
        with self.assertRaises(ValueError):
            store.delete_custom_image(image_id, self.user_id)
        # Row must still exist.
        self.assertIsNotNone(store.get_custom_image(image_id, user_id=self.user_id))

    def test_delete_rejected_while_building(self):
        image_id = self._make()
        store.update_custom_image_status(image_id, status="building")
        with self.assertRaises(ValueError):
            store.delete_custom_image(image_id, self.user_id)
        self.assertIsNotNone(store.get_custom_image(image_id, user_id=self.user_id))

    def test_delete_allowed_when_ready(self):
        image_id = self._make()
        store.update_custom_image_status(image_id, status="ready")
        deleted = store.delete_custom_image(image_id, self.user_id)
        self.assertIsNotNone(deleted)
        self.assertIsNone(store.get_custom_image(image_id, user_id=self.user_id))

    def test_delete_allowed_when_failed(self):
        image_id = self._make()
        store.update_custom_image_status(image_id, status="failed")
        deleted = store.delete_custom_image(image_id, self.user_id)
        self.assertIsNotNone(deleted)

    def test_delete_missing_returns_none(self):
        # A non-existent id is not an in-flight build: returns None, does not raise.
        self.assertIsNone(store.delete_custom_image(424242, self.user_id))


if __name__ == "__main__":
    unittest.main()
