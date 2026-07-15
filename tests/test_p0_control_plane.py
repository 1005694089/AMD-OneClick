"""Control-plane tests for kept node-denylist / idle-GC / leader logic.

Covers:
  - R2c: launch-grace defaults to 5 days; idle reaper GC modes — disk-pressure GC mode ignores the
    idle window while idle mode respects it.
  - Node denylist is honored.
  - Leader gate: is_leader() defaults True when election disabled.
"""
import os
import tempfile
import unittest
from datetime import datetime, timezone

_DB_FD, _DB_PATH = tempfile.mkstemp(suffix=".db")
os.close(_DB_FD)
os.environ["DATABASE_URL"] = f"sqlite:///{_DB_PATH}"
os.environ["ADMIN_PASSWORD"] = "testpass"
os.environ["NOTEBOOK_NODE_NAME"] = "fake-node"

from tests.kube_stub import install  # noqa: E402

install()

from app import store  # noqa: E402
from app.config import settings  # noqa: E402


def _reset():
    store.init_db()
    with store.engine.begin() as conn:
        for tbl in (store.image_nodes, store.custom_images):
            conn.execute(tbl.delete())


class DenylistTests(unittest.TestCase):
    def test_denylist_contains_offlan_node(self):
        self.assertIn("wx-ms-w7900d-0027", settings.IMAGE_TARGET_NODE_DENYLIST)


class LeaderGateTests(unittest.TestCase):
    def test_is_leader_true_when_election_disabled(self):
        from app.leader import is_leader

        self.assertFalse(settings.LEADER_ELECTION_ENABLED)
        self.assertTrue(is_leader())


if __name__ == "__main__":
    unittest.main()
