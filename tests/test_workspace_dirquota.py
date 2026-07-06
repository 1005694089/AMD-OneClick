"""Tests for the durable dir-quota backstop (app/workspace_dirquota.py).

These assert the load-bearing safety contract WITHOUT requiring the Huawei SDK to be installed:
  * fail-open: a missing SDK / missing creds / unresolvable shard returns False, never raises;
  * path resolution: quota_path = /<subdir>/<hh>/<safe_id>, share_id from the parallel-indexed
    static backend config;
  * marker gating: ensure_* is a cheap no-op once applied, and mark_unapplied re-arms it.
"""
import unittest
from types import SimpleNamespace

from tests.kube_stub import install

install()

from app import k8s_client as k8s_module
from app import workspace_dirquota as dq


FROZEN_SHARD_CLASSES = [
    "managed-nfs-storage-2",
    "managed-nfs-storage-3",
    "managed-nfs-storage-4",
    "managed-nfs-storage-5",
    "managed-nfs-storage-1",
]

BACKENDS = [
    {"share_id": "share-0", "subdir": "pvc-aaa"},
    {"share_id": "share-1", "subdir": "pvc-bbb"},
    {"share_id": "share-2", "subdir": "pvc-ccc"},
    {"share_id": "share-3", "subdir": "pvc-ddd"},
    {"share_id": "share-4", "subdir": "pvc-eee"},
]


class DirQuotaTests(unittest.TestCase):
    def setUp(self):
        self.s = dq.settings
        self._saved = {k: getattr(self.s, k, None) for k in (
            "WORKSPACE_VOLUME_TYPE", "WORKSPACE_DURABLE_DIRQUOTA_ENABLED",
            "WORKSPACE_DURABLE_STORAGE_CLASSES", "WORKSPACE_DURABLE_SHARD_SFS_BACKENDS",
            "WORKSPACE_DURABLE_PVC_PREFIX", "SFS_TURBO_AK", "SFS_TURBO_SK",
            "WORKSPACE_DURABLE_DIRQUOTA_CAPACITY_MB", "WORKSPACE_DURABLE_DIRQUOTA_INODE_COUNT",
        )}
        self.s.WORKSPACE_VOLUME_TYPE = "localcache"
        self.s.WORKSPACE_DURABLE_DIRQUOTA_ENABLED = True
        self.s.WORKSPACE_DURABLE_STORAGE_CLASSES = list(FROZEN_SHARD_CLASSES)
        self.s.WORKSPACE_DURABLE_SHARD_SFS_BACKENDS = [dict(b) for b in BACKENDS]
        self.s.WORKSPACE_DURABLE_PVC_PREFIX = "oneclick-durable"
        self.s.WORKSPACE_DURABLE_DIRQUOTA_CAPACITY_MB = 102400
        self.s.WORKSPACE_DURABLE_DIRQUOTA_INODE_COUNT = 2000000
        self.s.SFS_TURBO_AK = ""
        self.s.SFS_TURBO_SK = ""
        # k8s_client singleton is used by _resolve_path for shard math; align its view of the classes.
        self._k_saved = k8s_module.settings.WORKSPACE_DURABLE_STORAGE_CLASSES
        k8s_module.settings.WORKSPACE_DURABLE_STORAGE_CLASSES = list(FROZEN_SHARD_CLASSES)
        with dq._applied_lock:
            dq._applied.clear()

    def tearDown(self):
        for k, v in self._saved.items():
            setattr(self.s, k, v)
        k8s_module.settings.WORKSPACE_DURABLE_STORAGE_CLASSES = self._k_saved
        with dq._applied_lock:
            dq._applied.clear()

    def test_disabled_when_flag_off(self):
        self.s.WORKSPACE_DURABLE_DIRQUOTA_ENABLED = False
        self.assertFalse(dq._enabled())
        self.assertFalse(dq.apply_dir_quota_for_instance("u-1-abcd"))

    def test_disabled_when_not_localcache(self):
        self.s.WORKSPACE_VOLUME_TYPE = "hostPath"
        self.assertFalse(dq._enabled())

    def test_resolve_path_matches_shard_math(self):
        from app.k8s_client import k8s_client
        iid = "u-42-9f8e"
        idx = k8s_client._durable_shard_index(iid)
        subpath = k8s_client._durable_subpath(iid)
        resolved = dq._resolve_path(iid)
        self.assertIsNotNone(resolved)
        share_id, quota_path = resolved
        self.assertEqual(share_id, BACKENDS[idx]["share_id"])
        self.assertEqual(quota_path, f"/{BACKENDS[idx]['subdir']}/{subpath}")
        # Path is absolute, single-rooted, no traversal.
        self.assertTrue(quota_path.startswith("/"))
        self.assertNotIn("..", quota_path)

    @staticmethod
    def _id_for_shard(target_index: int) -> str:
        from app.k8s_client import k8s_client
        for n in range(100000):
            iid = f"probe-{n}"
            if k8s_client._durable_shard_index(iid) == target_index:
                return iid
        raise AssertionError(f"no probe id hashed to shard {target_index}")

    def test_resolve_path_none_when_backend_missing(self):
        # Fewer backends than shards -> a shard with no backend resolves to None (skip, not crash).
        self.s.WORKSPACE_DURABLE_SHARD_SFS_BACKENDS = BACKENDS[:1]
        iid = self._id_for_shard(2)  # shard 2 has no backend in the truncated list
        self.assertIsNone(dq._resolve_path(iid))

    def test_resolve_path_none_when_subdir_placeholder_blank(self):
        b = [dict(x) for x in BACKENDS]
        b[0]["subdir"] = ""
        self.s.WORKSPACE_DURABLE_SHARD_SFS_BACKENDS = b
        iid = self._id_for_shard(0)
        self.assertIsNone(dq._resolve_path(iid))

    def test_resolve_path_none_when_subdir_pending_sentinel(self):
        # A not-yet-bound shard ships a "*_PENDING" placeholder — must be treated as unresolvable so
        # the reconciler skips it (cheap no-op) rather than hammering a bogus path every tick.
        b = [dict(x) for x in BACKENDS]
        b[4]["subdir"] = "SHARD4_SUBDIR_PENDING"
        self.s.WORKSPACE_DURABLE_SHARD_SFS_BACKENDS = b
        iid = self._id_for_shard(4)
        self.assertIsNone(dq._resolve_path(iid))

    def test_apply_fail_open_when_no_creds(self):
        # Creds blank -> _build_client returns None -> apply returns False, never raises.
        self.assertFalse(dq.apply_dir_quota_for_instance("u-1-abcd"))

    def test_delete_fail_open_when_no_creds(self):
        self.assertFalse(dq.delete_dir_quota_for_instance("u-1-abcd"))

    def test_marker_gating(self):
        iid = "u-7-marker"
        self.assertFalse(dq.is_applied(iid))
        with dq._applied_lock:
            dq._applied.add(iid)
        # ensure_* short-circuits to True without any SFS work once marked.
        self.assertTrue(dq.ensure_dir_quota_for_instance(iid))
        self.assertTrue(dq.is_applied(iid))
        dq.mark_unapplied(iid)
        self.assertFalse(dq.is_applied(iid))

    def test_ensure_does_not_mark_on_failure(self):
        # No creds -> apply fails -> instance must NOT be marked applied (so it retries next tick).
        iid = "u-9-fail"
        self.assertFalse(dq.ensure_dir_quota_for_instance(iid))
        self.assertFalse(dq.is_applied(iid))

    def test_resolve_path_none_on_non_dict_backend(self):
        # A valid-JSON-but-wrong-shape override (list of strings) must not raise; resolve to None.
        self.s.WORKSPACE_DURABLE_SHARD_SFS_BACKENDS = ["a", "b", "c", "d", "e"]
        iid = self._id_for_shard(0)
        self.assertIsNone(dq._resolve_path(iid))  # must not raise AttributeError

    def test_int_env_failsafe(self):
        # A malformed numeric env must fall back to the default, never raise at import time.
        import os
        from app import config as cfg
        for bad in ("100Gi", "15s", "", "  "):
            os.environ["X_TEST_INT"] = bad
            try:
                self.assertEqual(cfg._int_env("X_TEST_INT", 42), 42)
            finally:
                os.environ.pop("X_TEST_INT", None)
        os.environ["X_TEST_INT"] = "7"
        try:
            self.assertEqual(cfg._int_env("X_TEST_INT", 42), 7)
        finally:
            os.environ.pop("X_TEST_INT", None)
        self.assertEqual(cfg._int_env("X_TEST_INT_UNSET", 99), 99)

    def test_prune_applied_bounds_to_live(self):
        with dq._applied_lock:
            dq._applied.update({"u-1", "u-2", "u-3"})
        removed = dq.prune_applied({"u-2"})  # only u-2 is live
        self.assertEqual(removed, 2)
        self.assertTrue(dq.is_applied("u-2"))
        self.assertFalse(dq.is_applied("u-1"))
        self.assertFalse(dq.is_applied("u-3"))


if __name__ == "__main__":
    unittest.main()
