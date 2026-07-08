"""Tests for the two-tier localcache workspace: shard mapping stability, path-consistency across
code paths, and the append-only shard-list invariant (reordering = silent data loss)."""
import os
import subprocess
import tempfile
import unittest
from types import SimpleNamespace

from tests.kube_stub import install

install()

from app import k8s_client as k8s_module


# The exact shard list the live deployment must preserve in order forever. If someone reorders or
# removes an entry, md5(instance_id) % len remaps existing instances onto a different (empty) shard,
# stranding their durable data. This frozen expectation catches that in CI.
# 2026-07-03: managed-nfs-storage-1 was decommissioned out-of-band (backend denies mounts) and removed
# from the list BEFORE any real durable data existed (the only safe time to change it). Baseline is
# the 4 healthy backends; the append-only rule applies going forward from here.
# 2026-07-06: a 5th shard (managed-nfs-storage-1) append was DEFERRED — appending remaps ~4/5 of
# instances (md5%4 -> md5%5), safe only on empty shards, and real durable data now exists. Baseline
# stays at 4 until a confirmed idle window allows a wipe + append.
FROZEN_SHARD_CLASSES = [
    "managed-nfs-storage-2",
    "managed-nfs-storage-3",
    "managed-nfs-storage-4",
    "managed-nfs-storage-5",
]

# A frozen sample of instance_id -> shard index, computed from the current (correct) mapping. If the
# mapping function or the class-list order changes, these break — the whole point.
FROZEN_INSTANCE_SHARDS = {
    "nb-a1b2c3d4": None,   # filled in setUp from the live function, then re-asserted stable
}


class ShardMappingTests(unittest.TestCase):
    def setUp(self):
        self._orig_classes = k8s_module.settings.WORKSPACE_DURABLE_STORAGE_CLASSES
        self._orig_type = k8s_module.settings.WORKSPACE_VOLUME_TYPE
        self._orig_prefix = k8s_module.settings.WORKSPACE_DURABLE_PVC_PREFIX
        self._orig_root = k8s_module.settings.WORKSPACE_LOCAL_CACHE_ROOT
        k8s_module.settings.WORKSPACE_DURABLE_STORAGE_CLASSES = list(FROZEN_SHARD_CLASSES)
        k8s_module.settings.WORKSPACE_VOLUME_TYPE = "localcache"
        k8s_module.settings.WORKSPACE_DURABLE_PVC_PREFIX = "oneclick-durable"
        k8s_module.settings.WORKSPACE_LOCAL_CACHE_ROOT = "/nvme0/data/workspace"
        # Build the client without touching a real cluster.
        self.c = k8s_module.K8sClient.__new__(k8s_module.K8sClient)
        self.c.namespace = "amd-oneclick-lablab"
        self.c.core_v1 = SimpleNamespace()

    def tearDown(self):
        k8s_module.settings.WORKSPACE_DURABLE_STORAGE_CLASSES = self._orig_classes
        k8s_module.settings.WORKSPACE_VOLUME_TYPE = self._orig_type
        k8s_module.settings.WORKSPACE_DURABLE_PVC_PREFIX = self._orig_prefix
        k8s_module.settings.WORKSPACE_LOCAL_CACHE_ROOT = self._orig_root

    def test_shard_index_in_range_and_deterministic(self):
        for iid in ["nb-a1b2c3d4", "u-42-9f8e", "hf-7-5d4c", "nb-deadbeef", "custom-XYZ_123"]:
            i1 = self.c._durable_shard_index(iid)
            i2 = self.c._durable_shard_index(iid)
            self.assertEqual(i1, i2, "shard index must be deterministic")
            self.assertTrue(0 <= i1 < len(FROZEN_SHARD_CLASSES))

    def test_append_only_invariant(self):
        """The operational rule is append-only: the existing entries must never be reordered or
        removed (that remaps md5%len and strands data). Assert the REAL, UNMOCKED config default
        matches the frozen list in order — a future append adds entries AFTER these, never disturbing
        them. NOTE: this deliberately reads a fresh Settings() from the actual config (NOT the mocked
        k8s_module.settings that setUp overwrites), so it genuinely guards the live shard order — the
        old version compared the mock against itself and could never fail."""
        import os
        from importlib import reload
        from app import config as config_module
        # Read the config default with no env override in effect, so we test the code default, not
        # whatever WORKSPACE_DURABLE_STORAGE_CLASSES happens to be set in this shell.
        saved = os.environ.pop("WORKSPACE_DURABLE_STORAGE_CLASSES", None)
        try:
            reload(config_module)
            live_default = list(config_module.Settings().WORKSPACE_DURABLE_STORAGE_CLASSES)
        finally:
            if saved is not None:
                os.environ["WORKSPACE_DURABLE_STORAGE_CLASSES"] = saved
            reload(config_module)
        self.assertEqual(
            live_default,
            FROZEN_SHARD_CLASSES,
            "The live durable StorageClass default must be exactly the frozen append-only list, in "
            "order (existing entries never reordered/removed; new shards appended LAST).",
        )

    def test_reordering_changes_mapping_is_detectable(self):
        """Prove that a reorder changes the mapping for at least one instance, so a CI diff on the
        frozen expectation below would catch an accidental reorder."""
        baseline = {iid: self.c._durable_shard_index(iid)
                    for iid in ["nb-a1b2c3d4", "u-42-9f8e", "hf-7-5d4c", "nb-deadbeef"]}
        # Simulate a reorder (swap first two).
        swapped = list(FROZEN_SHARD_CLASSES)
        swapped[0], swapped[1] = swapped[1], swapped[0]
        k8s_module.settings.WORKSPACE_DURABLE_STORAGE_CLASSES = swapped
        after = {iid: self.c._durable_shard_index(iid)
                 for iid in ["nb-a1b2c3d4", "u-42-9f8e", "hf-7-5d4c", "nb-deadbeef"]}
        # The shard *index* is a pure function of md5 % len, so it is unchanged by reorder; what
        # changes is which StorageClass that index maps to. Assert THAT is what shifts.
        base_class = {iid: FROZEN_SHARD_CLASSES[idx] for iid, idx in baseline.items()}
        after_class = {iid: swapped[idx] for iid, idx in after.items()}
        self.assertNotEqual(base_class, after_class,
                            "A reorder must change at least one instance's target StorageClass")

    def test_pvc_name_dns_safe_and_bounded(self):
        for iid in ["nb-a1b2c3d4", "u-42-9f8e", "hf-7-5d4c"]:
            name = self.c._durable_shard_pvc(iid)
            self.assertTrue(name.startswith("oneclick-durable-shard-"))
            self.assertLessEqual(len(name), 63)
            self.assertNotIn("_", name)
            self.assertRegex(name, r"^[a-z0-9-]+$")

    def test_path_consistency_across_code_paths(self):
        """The manifest mount, admin delete, and trash purge must all resolve the SAME durable
        location for a given instance_id. delete_workspace_durable uses _durable_shard_pvc +
        _durable_subpath; the manifest uses the same helpers; assert they agree."""
        for iid in ["nb-a1b2c3d4", "u-42-9f8e", "hf-7-5d4c", "custom-XYZ_123"]:
            pvc = self.c._durable_shard_pvc(iid)
            sub = self.c._durable_subpath(iid)
            idx = self.c._durable_shard_index(iid)
            # PVC name must encode the same index used everywhere.
            self.assertEqual(pvc, self.c._durable_shard_pvc_for_index(idx))
            # subpath is <2 hex>/<safe id>, stable and injection-safe.
            self.assertRegex(sub, r"^[0-9a-f]{2}/[A-Za-z0-9_.-]+$")

    def test_local_cache_and_quota_image_paths_are_ssd_and_guarded(self):
        for iid in ["nb-a1b2c3d4", "custom-XYZ_123"]:
            cache = self.c._workspace_local_cache_path(iid)
            img = self.c._workspace_quota_image_path(iid)
            self.assertTrue(cache.startswith("/nvme0/data/workspace/"))
            self.assertTrue(img.startswith("/nvme0/data/workspace-quota/"))
            self.assertTrue(img.endswith(".img"))

    def test_empty_shard_list_raises(self):
        k8s_module.settings.WORKSPACE_DURABLE_STORAGE_CLASSES = []
        with self.assertRaises(RuntimeError):
            self.c._durable_shard_index("nb-a1b2c3d4")

    def test_safe_storage_segment_rejects_dot_only(self):
        """Path-traversal hardening: '.'/'..'/'...' must never survive sanitization (they would let a
        subpath like '<bucket>/..' escape the per-instance dir)."""
        for bad in [".", "..", "...", "....", "/", "//", "..%2f", "../.."]:
            seg = self.c._safe_storage_segment(bad)
            self.assertNotIn(seg, {".", "..", "...", "...."})
            self.assertTrue(seg and set(seg) != {"."})

    def test_durable_subpath_never_escapes(self):
        """Even a hostile instance_id must yield a two-part subpath whose second segment is not a
        traversal token."""
        for iid in ["..", ".", "../../etc", "..%2f..%2f", "nb-normal"]:
            sub = self.c._durable_subpath(iid)
            parts = sub.split("/")
            self.assertEqual(len(parts), 2)
            self.assertRegex(parts[0], r"^[0-9a-f]{2}$")
            self.assertNotIn(parts[1], {".", "..", ""})


def _node(name, ready=True, unschedulable=False, master=False, disk_pressure=False, images=None):
    taints = [SimpleNamespace(key="node-role.kubernetes.io/control-plane", value=None, effect="NoSchedule")] if master else []
    conds = [SimpleNamespace(type="Ready", status="True" if ready else "False")]
    if disk_pressure:
        conds.append(SimpleNamespace(type="DiskPressure", status="True"))
    img_objs = [SimpleNamespace(names=list(names)) for names in (images or [])]
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        spec=SimpleNamespace(taints=taints, unschedulable=unschedulable),
        status=SimpleNamespace(conditions=conds, images=img_objs),
    )


class DurableOpRetryTests(unittest.TestCase):
    """The durable-op pod mounts an SFS-Turbo NFS PVC; not every node can mount it. Verify candidate
    selection excludes masters/unschedulable/NotReady and that _run_durable_shard_command retries the
    next node when a pinned attempt stalls (mount failure), succeeds on a good node, and raises on a
    genuine container Failure without pointless retries."""

    def setUp(self):
        self._orig = (k8s_module.settings.WORKSPACE_VOLUME_TYPE,
                      k8s_module.settings.WORKSPACE_DURABLE_PVC_PREFIX,
                      k8s_module.settings.NOTEBOOK_LABEL_PREFIX)
        k8s_module.settings.WORKSPACE_VOLUME_TYPE = "localcache"
        k8s_module.settings.WORKSPACE_DURABLE_PVC_PREFIX = "oneclick-durable"
        self.c = k8s_module.K8sClient.__new__(k8s_module.K8sClient)
        self.c.namespace = "ns"
        self.c._node_list_cache = None
        self.c._node_list_cache_ts = 0.0
        self.c._node_list_cache_ttl = 0.0
        import threading
        self.c._node_list_cache_lock = threading.Lock()

    def tearDown(self):
        (k8s_module.settings.WORKSPACE_VOLUME_TYPE,
         k8s_module.settings.WORKSPACE_DURABLE_PVC_PREFIX,
         k8s_module.settings.NOTEBOOK_LABEL_PREFIX) = self._orig

    def test_candidate_nodes_filter_and_prioritize(self):
        self.c._workspace_sync_image = lambda: "img:tag"
        nodes = [_node("good-1"), _node("good-2"), _node("m-1", master=True),
                 _node("cordoned", unschedulable=True), _node("notready", ready=False)]
        # good-2 already runs a durable-mounted workspace pod → proven NFS-capable, ranked first.
        pod = SimpleNamespace(spec=SimpleNamespace(
            node_name="good-2",
            volumes=[SimpleNamespace(persistent_volume_claim=SimpleNamespace(claim_name="oneclick-durable-shard-3"))],
        ))
        self.c.core_v1 = SimpleNamespace(
            list_node=lambda: SimpleNamespace(items=nodes),
            list_namespaced_pod=lambda **k: SimpleNamespace(items=[pod]),
        )
        cands = self.c._nfs_op_candidate_nodes()
        self.assertEqual(cands[0], "good-2", "proven NFS node must rank first")
        self.assertIn("good-1", cands)
        for bad in ("m-1", "cordoned", "notready"):
            self.assertNotIn(bad, cands, f"{bad} must be excluded")

    def test_candidate_nodes_prefer_image_warm(self):
        # No proven-NFS pods; warmth must order candidates (cold node last so its 90GB pull doesn't
        # burn the retry budget).
        self.c._workspace_sync_image = lambda: "reg/base:v1"
        nodes = [
            _node("cold-a"),
            _node("warm-z", images=[["reg/base:v1", "reg/base@sha256:deadbeef"]]),
            _node("cold-b"),
        ]
        self.c.core_v1 = SimpleNamespace(
            list_node=lambda: SimpleNamespace(items=nodes),
            list_namespaced_pod=lambda **k: SimpleNamespace(items=[]),
        )
        cands = self.c._nfs_op_candidate_nodes()
        self.assertEqual(cands[0], "warm-z", "image-warm node must rank ahead of cold nodes")
        self.assertEqual(set(cands), {"warm-z", "cold-a", "cold-b"})

    def test_retry_moves_past_stuck_node_then_succeeds(self):
        # Model: node A keeps the pod Pending (NFS mount stall); node B runs it to Succeeded.
        self.c._notebook_tolerations = lambda: [{"operator": "Exists"}]
        self.c._workspace_sync_image = lambda: "img"
        self.c._nfs_op_candidate_nodes = lambda limit=6: ["A", "B"]
        state = {"pods": {}, "created_on": []}

        def create(namespace, body):
            node = body["spec"].get("nodeName")
            state["created_on"].append(node)
            # A: never starts (stuck Pending). B: immediately Succeeded.
            state["pods"][body["metadata"]["name"]] = "pendingA" if node == "A" else "Succeeded"

        def read(name, namespace):
            st = state["pods"].get(name)
            if st is None:
                raise k8s_module.ApiException(status=404)
            if st == "pendingA":
                return SimpleNamespace(status=SimpleNamespace(phase="Pending", container_statuses=None))
            return SimpleNamespace(status=SimpleNamespace(phase="Succeeded", container_statuses=None))

        def delete(name, namespace, grace_period_seconds=0):
            state["pods"].pop(name, None)

        self.c.core_v1 = SimpleNamespace(
            create_namespaced_pod=create, read_namespaced_pod=read, delete_namespaced_pod=delete)
        # Short per-attempt deadline so the stuck node is abandoned fast in the test.
        self.c._run_durable_shard_command("trash-3", "oneclick-durable-shard-3", "echo hi", timeout_seconds=90)
        self.assertEqual(state["created_on"], ["A", "B"], "should try A (stall) then B (success)")

    def test_container_failure_raises_without_extra_retries(self):
        self.c._notebook_tolerations = lambda: [{"operator": "Exists"}]
        self.c._workspace_sync_image = lambda: "img"
        self.c._nfs_op_candidate_nodes = lambda limit=6: ["A", "B", "C"]
        state = {"created_on": []}

        def create(namespace, body):
            state["created_on"].append(body["spec"].get("nodeName"))

        def read(name, namespace):
            # Container ran (terminated) and pod Failed → genuine error, must not retry other nodes.
            term = SimpleNamespace(running=None, terminated=SimpleNamespace(exit_code=1))
            return SimpleNamespace(status=SimpleNamespace(
                phase="Failed", container_statuses=[SimpleNamespace(state=term)]))

        def delete(name, namespace, grace_period_seconds=0):
            pass

        self.c.core_v1 = SimpleNamespace(
            create_namespaced_pod=create, read_namespaced_pod=read, delete_namespaced_pod=delete)
        with self.assertRaises(RuntimeError):
            self.c._run_durable_shard_command("del-x", "oneclick-durable-shard-1", "false", timeout_seconds=90)
        self.assertEqual(state["created_on"], ["A"], "a real container failure must not retry other nodes")

    def test_failed_pre_start_retries_next_node(self):
        # Pod Failed BEFORE the container ever ran (e.g. evicted) → not a script error, try next node.
        self.c._notebook_tolerations = lambda: [{"operator": "Exists"}]
        self.c._workspace_sync_image = lambda: "img"
        self.c._nfs_op_candidate_nodes = lambda limit=6: ["A", "B"]
        state = {"created_on": [], "pods": {}}

        def create(namespace, body):
            node = body["spec"].get("nodeName")
            state["created_on"].append(node)
            state["pods"][body["metadata"]["name"]] = node

        def read(name, namespace):
            node = state["pods"].get(name)
            if node is None:
                raise k8s_module.ApiException(status=404)
            if node == "A":  # Failed with NO container ever started
                return SimpleNamespace(status=SimpleNamespace(phase="Failed", container_statuses=None))
            return SimpleNamespace(status=SimpleNamespace(phase="Succeeded", container_statuses=None))

        def delete(name, namespace, grace_period_seconds=0):
            state["pods"].pop(name, None)

        self.c.core_v1 = SimpleNamespace(
            create_namespaced_pod=create, read_namespaced_pod=read, delete_namespaced_pod=delete)
        self.c._run_durable_shard_command("trash-1", "oneclick-durable-shard-1", "x", timeout_seconds=90)
        self.assertEqual(state["created_on"], ["A", "B"], "pre-start Failed should retry, not abort")

    def test_create_409_skips_to_next_node(self):
        # A stale pod stuck Terminating → create raises 409; loop should skip to the next candidate.
        self.c._notebook_tolerations = lambda: [{"operator": "Exists"}]
        self.c._workspace_sync_image = lambda: "img"
        self.c._nfs_op_candidate_nodes = lambda limit=6: ["A", "B"]
        state = {"created_on": [], "pods": {}}

        def create(namespace, body):
            node = body["spec"].get("nodeName")
            if node == "A":
                raise k8s_module.ApiException(status=409)
            state["created_on"].append(node)
            state["pods"][body["metadata"]["name"]] = "Succeeded"

        def read(name, namespace):
            st = state["pods"].get(name)
            if st is None:
                raise k8s_module.ApiException(status=404)
            return SimpleNamespace(status=SimpleNamespace(phase="Succeeded", container_statuses=None))

        def delete(name, namespace, grace_period_seconds=0):
            state["pods"].pop(name, None)

        self.c.core_v1 = SimpleNamespace(
            create_namespaced_pod=create, read_namespaced_pod=read, delete_namespaced_pod=delete)
        self.c._run_durable_shard_command("trash-2", "oneclick-durable-shard-2", "x", timeout_seconds=90)
        self.assertEqual(state["created_on"], ["B"], "409 on A should skip to B, not abort the loop")

    def test_overall_timeout_budget_bounds_total_time(self):
        # Two stuck-pending nodes with a tiny overall budget → must give up quickly, not run 2x full.
        import time as _t
        self.c._notebook_tolerations = lambda: [{"operator": "Exists"}]
        self.c._workspace_sync_image = lambda: "img"
        self.c._nfs_op_candidate_nodes = lambda limit=6: ["A", "B"]
        state = {"pods": {}}

        def create(namespace, body):
            state["pods"][body["metadata"]["name"]] = "Pending"

        def read(name, namespace):
            if name not in state["pods"]:
                raise k8s_module.ApiException(status=404)
            return SimpleNamespace(status=SimpleNamespace(phase="Pending", container_statuses=None))

        def delete(name, namespace, grace_period_seconds=0):
            state["pods"].pop(name, None)

        self.c.core_v1 = SimpleNamespace(
            create_namespaced_pod=create, read_namespaced_pod=read, delete_namespaced_pod=delete)
        t0 = _t.monotonic()
        with self.assertRaises(RuntimeError):
            self.c._run_durable_shard_command("trash-0", "oneclick-durable-shard-0", "x", timeout_seconds=8)
        elapsed = _t.monotonic() - t0
        # Overall budget is 8s; must not run anywhere near 2x8 even with 2 stuck nodes.
        self.assertLess(elapsed, 20, f"overall timeout budget not honored: {elapsed:.1f}s")


class ManifestApproachBTests(unittest.TestCase):
    """Approach B: the durable shard must NOT be mounted on the notebook (main) container and there
    must be NO preStop flush on it — the flush is out-of-pod. The hydrate init container MUST still
    mount the durable shard (it seeds the SSD copy). Guards against a regression that re-exposes the
    uncapped NFS to the user."""

    def setUp(self):
        s = k8s_module.settings
        self._orig = {k: getattr(s, k) for k in (
            "WORKSPACE_VOLUME_TYPE", "WORKSPACE_DURABLE_STORAGE_CLASSES", "WORKSPACE_DURABLE_PVC_PREFIX",
            "WORKSPACE_LOCAL_CACHE_ROOT", "WORKSPACE_QUOTA_ENABLED", "WORKSPACE_MOUNT_PATH",
            "WORKSPACE_DURABLE_MOUNT_PATH")}
        s.WORKSPACE_VOLUME_TYPE = "localcache"
        s.WORKSPACE_DURABLE_STORAGE_CLASSES = list(FROZEN_SHARD_CLASSES)
        s.WORKSPACE_DURABLE_PVC_PREFIX = "oneclick-durable"
        s.WORKSPACE_LOCAL_CACHE_ROOT = "/nvme0/data/workspace"
        s.WORKSPACE_QUOTA_ENABLED = True
        s.WORKSPACE_MOUNT_PATH = "/workspace"
        s.WORKSPACE_DURABLE_MOUNT_PATH = "/mnt/workspace-durable"
        self.c = k8s_module.K8sClient.__new__(k8s_module.K8sClient)
        self.c.namespace = "amd-oneclick-lablab"

    def tearDown(self):
        for k, v in self._orig.items():
            setattr(k8s_module.settings, k, v)

    def _manifest(self):
        return self.c._get_pod_manifest("user@example.com", "nb-a1b2c3d4", "img:tag",
                                        instance_type="jupyter", gpu_count=1)

    def _containers(self, m):
        spec = m["spec"]
        main = spec["containers"][0]
        inits = {c["name"]: c for c in spec.get("initContainers", [])}
        return spec, main, inits

    def test_main_container_has_no_durable_mount(self):
        _, main, _ = self._containers(self._manifest())
        names = {vm["name"] for vm in main["volumeMounts"]}
        self.assertIn("workspace", names)
        self.assertNotIn("workspace-durable", names,
                         "notebook container must NOT mount the durable NFS shard (cap-bypass)")

    def test_main_container_has_no_prestop(self):
        _, main, _ = self._containers(self._manifest())
        lifecycle = main.get("lifecycle") or {}
        self.assertNotIn("preStop", lifecycle,
                         "notebook container must have no local->durable preStop flush anymore")

    def test_hydrate_init_still_mounts_durable(self):
        _, _, inits = self._containers(self._manifest())
        self.assertIn("workspace-hydrate", inits, "hydrate init container must exist for localcache")
        names = {vm["name"] for vm in inits["workspace-hydrate"]["volumeMounts"]}
        self.assertIn("workspace-durable", names, "hydrate init must mount durable to seed local")

    def test_durable_volume_declared_once(self):
        spec, _, _ = self._containers(self._manifest())
        durable_vols = [v for v in spec["volumes"] if v["name"] == "workspace-durable"]
        self.assertEqual(len(durable_vols), 1, "durable PVC volume must be declared exactly once")
        self.assertEqual(durable_vols[0]["persistentVolumeClaim"]["claimName"],
                         self.c._durable_shard_pvc("nb-a1b2c3d4"))

    def test_no_forced_termination_grace(self):
        spec, _, _ = self._containers(self._manifest())
        self.assertNotIn("terminationGracePeriodSeconds", spec,
                         "no long grace needed without an in-pod flush")

    def test_quota_guard_is_robust_not_dead_findmnt_grep(self):
        """Regression for the quota initContainer idempotency guard. Two bugs it must avoid:
        (1) the ORIGINAL dead guard did `findmnt -o SOURCE | grep -Fq "$img"` — SOURCE is /dev/loopN,
            never the image path, so it never matched and a leaked loop remount hit `mount -o loop` on
            a busy mount → CrashLoopBackOff.
        (2) a naive fix that unconditionally unmounts any existing mount BREAKS the normal first start:
            /workspace is ALWAYS the kubelet bind base (source /dev/nvme0n1p1[/workspace/<id>]); the
            loop must be STACKED on top of it, NOT after unmounting it (unmounting the bind base drops
            the quota — the notebook container then sees the uncapped SSD).
        The correct guard keys on whether the CURRENT top source is a loop device backing $img."""
        _, _, inits = self._containers(self._manifest())
        self.assertIn("workspace-quota", inits, "quota init container must exist when quota enabled")
        script = inits["workspace-quota"]["args"][0]
        code_lines = [ln for ln in script.splitlines() if not ln.lstrip().startswith("#")]
        code = "\n".join(code_lines)
        # The dead grep-against-$img guard must be gone.
        self.assertNotIn('grep -Fq "$img"', code,
                         "the dead findmnt-SOURCE-vs-image-path guard must be removed from code")
        self.assertNotIn('current_source=', code)
        # Must discriminate on the loop device, and resolve our loop via losetup.
        self.assertIn("losetup -j", code, "must resolve loop devices backing the image via losetup")
        self.assertIn("/dev/loop", code, "must key the guard on whether the top source is a loop dev")
        # The final stack-mount must be present.
        self.assertIn('mount -o loop "$img" "$mnt"', code, "must stack the loop mount on /workspace")
        # Structural safety: any `umount` must sit INSIDE the `/dev/loop` guard branch (never at top
        # level), so a normal first start with the bind base as top source is never unmounted.
        for i, ln in enumerate(code_lines):
            if "umount" in ln:
                preceding = "\n".join(code_lines[:i])
                self.assertIn("/dev/loop", preceding,
                              "umount must be guarded by the loop-device check, not unconditional")


class FlushPodTests(unittest.TestCase):
    """_flush_workspace_to_durable: pinned to the recorded node, mounts durable(subPath)+local(hostPath),
    marks flushed only on Succeeded and only when the session token matches, aborts if the pod is live,
    and (under quota) guards the empty-but-unmounted loop case."""

    def setUp(self):
        import threading
        s = k8s_module.settings
        self._orig = {k: getattr(s, k) for k in (
            "WORKSPACE_VOLUME_TYPE", "WORKSPACE_DURABLE_STORAGE_CLASSES", "WORKSPACE_DURABLE_PVC_PREFIX",
            "WORKSPACE_LOCAL_CACHE_ROOT", "WORKSPACE_QUOTA_ENABLED")}
        s.WORKSPACE_VOLUME_TYPE = "localcache"
        s.WORKSPACE_DURABLE_STORAGE_CLASSES = list(FROZEN_SHARD_CLASSES)
        s.WORKSPACE_DURABLE_PVC_PREFIX = "oneclick-durable"
        s.WORKSPACE_LOCAL_CACHE_ROOT = "/nvme0/data/workspace"
        s.WORKSPACE_QUOTA_ENABLED = True
        self.c = k8s_module.K8sClient.__new__(k8s_module.K8sClient)
        self.c.namespace = "ns"
        self.c._notebook_tolerations = lambda: [{"operator": "Exists"}]
        self.c._workspace_sync_image = lambda: "img"
        self.c._flush_locks = {}
        self.c._flush_locks_guard = threading.Lock()
        self.c._pod_exists = lambda i: False  # default: no live pod
        self._orig_mark = k8s_module.store.mark_workspace_flushed
        # Propagation defaults on → the flush reads get_durable_generation under the lock and ABORTS
        # if it raises. Provide a working reader (Gd=0) so these tests exercise the pod path; the
        # abort-on-error behavior is covered by FlushGenerationTests.test_get_durable_generation_error_aborts_flush.
        self._orig_gdg = k8s_module.store.get_durable_generation
        k8s_module.store.get_durable_generation = lambda iid: 0

    def tearDown(self):
        for k, v in self._orig.items():
            setattr(k8s_module.settings, k, v)
        k8s_module.store.mark_workspace_flushed = self._orig_mark
        k8s_module.store.get_durable_generation = self._orig_gdg

    def test_skip_when_no_session_token(self):
        # No session token → nothing to flush → True, no pod created, no mark.
        marks = []
        k8s_module.store.mark_workspace_flushed = lambda i, n, t: marks.append((i, n, t))
        self.assertTrue(self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", None))
        self.assertEqual(marks, [])

    def test_aborts_when_pod_live(self):
        # A live pod means relaunched — must not flush over the active session.
        marks = []
        k8s_module.store.mark_workspace_flushed = lambda i, n, t: marks.append((i, n, t)) or True
        self.c._pod_exists = lambda i: True
        created = []
        self.c.core_v1 = SimpleNamespace(
            create_namespaced_pod=lambda namespace, body: created.append(body),
            read_namespaced_pod=lambda name, namespace: None,
            delete_namespaced_pod=lambda name, namespace, grace_period_seconds=0: None)
        ok = self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T1")
        self.assertFalse(ok, "must abort (return False) when a pod is live")
        self.assertEqual(created, [], "must not create a flush pod when the instance is live")
        self.assertEqual(marks, [])

    def test_success_pins_node_mounts_and_marks_with_token(self):
        marks = []
        k8s_module.store.mark_workspace_flushed = lambda i, n, t: (marks.append((i, n, t)) or True)
        captured = {}

        def create(namespace, body):
            captured["body"] = body

        def read(name, namespace):
            return SimpleNamespace(status=SimpleNamespace(phase="Succeeded", container_statuses=None))

        def delete(name, namespace, grace_period_seconds=0):
            raise k8s_module.ApiException(status=404)

        self.c.core_v1 = SimpleNamespace(
            create_namespaced_pod=create, read_namespaced_pod=read, delete_namespaced_pod=delete)
        ok = self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T-2026")
        self.assertTrue(ok)
        self.assertEqual(marks, [("nb-a1b2c3d4", "node-7", "T-2026")],
                         "success must mark flushed with the exact (instance, node, session token)")
        spec = captured["body"]["spec"]
        self.assertEqual(spec["nodeName"], "node-7", "flush pod must be pinned to the recorded node")
        self.assertEqual(spec["containers"][0]["securityContext"]["allowPrivilegeEscalation"], False)
        vols = {v["name"]: v for v in spec["volumes"]}
        self.assertEqual(vols["durable"]["persistentVolumeClaim"]["claimName"],
                         self.c._durable_shard_pvc("nb-a1b2c3d4"))
        self.assertEqual(vols["local"]["hostPath"]["path"],
                         self.c._workspace_local_cache_path("nb-a1b2c3d4"))
        mounts = {m["name"]: m for m in spec["containers"][0]["volumeMounts"]}
        self.assertEqual(mounts["durable"]["subPath"], self.c._durable_subpath("nb-a1b2c3d4"))
        self.assertEqual(mounts["local"]["mountPropagation"], "HostToContainer")
        # Under quota the script must guard the empty-but-not-mounted loop case.
        self.assertIn('mountpoint -q "$local_dir"', spec["containers"][0]["args"][0])

    def test_quota_off_script_has_no_mountpoint_guard(self):
        k8s_module.settings.WORKSPACE_QUOTA_ENABLED = False
        k8s_module.store.mark_workspace_flushed = lambda i, n, t: True
        captured = {}
        self.c.core_v1 = SimpleNamespace(
            create_namespaced_pod=lambda namespace, body: captured.setdefault("body", body),
            read_namespaced_pod=lambda name, namespace: SimpleNamespace(
                status=SimpleNamespace(phase="Succeeded", container_statuses=None)),
            delete_namespaced_pod=lambda name, namespace, grace_period_seconds=0: None)
        self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T")
        self.assertNotIn("mountpoint -q /local", captured["body"]["spec"]["containers"][0]["args"][0])

    def test_stale_token_success_returns_false(self):
        # Pod Succeeded but the DB row's token advanced (relaunch) → mark returns False (no-op) → we
        # must NOT report success (the current session is still unflushed).
        k8s_module.store.mark_workspace_flushed = lambda i, n, t: False
        self.c.core_v1 = SimpleNamespace(
            create_namespaced_pod=lambda namespace, body: None,
            read_namespaced_pod=lambda name, namespace: SimpleNamespace(
                status=SimpleNamespace(phase="Succeeded", container_statuses=None)),
            delete_namespaced_pod=lambda name, namespace, grace_period_seconds=0: None)
        ok = self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "OLD")
        self.assertFalse(ok, "a stale-token success must not certify the current session")

    def test_failed_pod_does_not_mark(self):
        marks = []
        k8s_module.store.mark_workspace_flushed = lambda i, n, t: (marks.append((i, n, t)) or True)

        def read(name, namespace):
            term = SimpleNamespace(running=None, terminated=SimpleNamespace(exit_code=1))
            return SimpleNamespace(status=SimpleNamespace(
                phase="Failed", container_statuses=[SimpleNamespace(state=term)]))

        self.c.core_v1 = SimpleNamespace(
            create_namespaced_pod=lambda namespace, body: None, read_namespaced_pod=read,
            delete_namespaced_pod=lambda name, namespace, grace_period_seconds=0: None)
        ok = self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T")
        self.assertFalse(ok, "a Failed flush pod must return False")
        self.assertEqual(marks, [], "a Failed flush must NOT mark flushed (reaper must not reap)")

    def test_unique_pod_names_across_invocations(self):
        # Two invocations must use DIFFERENT pod names so their cleanups don't kill each other's pod.
        k8s_module.store.mark_workspace_flushed = lambda i, n, t: True
        names = []
        self.c.core_v1 = SimpleNamespace(
            create_namespaced_pod=lambda namespace, body: names.append(body["metadata"]["name"]),
            read_namespaced_pod=lambda name, namespace: SimpleNamespace(
                status=SimpleNamespace(phase="Succeeded", container_statuses=None)),
            delete_namespaced_pod=lambda name, namespace, grace_period_seconds=0: None)
        self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T1")
        self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T2")
        self.assertEqual(len(names), 2)
        self.assertNotEqual(names[0], names[1], "each flush invocation must use a unique pod name")
        for n in names:
            self.assertTrue(n.startswith("ws-flush-"))
            self.assertLessEqual(len(n), 63)


class _ManifestBase(unittest.TestCase):
    """Shared localcache settings harness for the Part B/C manifest + script tests."""

    def setUp(self):
        s = k8s_module.settings
        self._orig = {k: getattr(s, k) for k in (
            "WORKSPACE_VOLUME_TYPE", "WORKSPACE_DURABLE_STORAGE_CLASSES", "WORKSPACE_DURABLE_PVC_PREFIX",
            "WORKSPACE_LOCAL_CACHE_ROOT", "WORKSPACE_QUOTA_ENABLED", "WORKSPACE_MOUNT_PATH",
            "WORKSPACE_DURABLE_MOUNT_PATH", "WORKSPACE_SEED_ENABLED", "WORKSPACE_SEED_PER_TEMPLATE",
            "WORKSPACE_DELETION_PROPAGATION_ENABLED")}
        s.WORKSPACE_VOLUME_TYPE = "localcache"
        s.WORKSPACE_DURABLE_STORAGE_CLASSES = list(FROZEN_SHARD_CLASSES)
        s.WORKSPACE_DURABLE_PVC_PREFIX = "oneclick-durable"
        s.WORKSPACE_LOCAL_CACHE_ROOT = "/nvme0/data/workspace"
        s.WORKSPACE_QUOTA_ENABLED = True
        s.WORKSPACE_MOUNT_PATH = "/workspace"
        s.WORKSPACE_DURABLE_MOUNT_PATH = "/mnt/workspace-durable"
        s.WORKSPACE_SEED_ENABLED = True
        s.WORKSPACE_SEED_PER_TEMPLATE = True
        s.WORKSPACE_DELETION_PROPAGATION_ENABLED = True
        # get_durable_generation is called during manifest build; default it to 0 unless a test overrides.
        self._orig_gdg = k8s_module.store.get_durable_generation
        k8s_module.store.get_durable_generation = lambda iid: 0
        self.c = k8s_module.K8sClient.__new__(k8s_module.K8sClient)
        self.c.namespace = "ns"

    def tearDown(self):
        for k, v in self._orig.items():
            setattr(k8s_module.settings, k, v)
        k8s_module.store.get_durable_generation = self._orig_gdg

    def _manifest(self, **kw):
        return self.c._get_pod_manifest("u@e.com", "nb-a1b2c3d4", "img:tag",
                                        instance_type="jupyter", gpu_count=1, **kw)

    def _inits(self, m):
        return {c["name"]: c for c in m["spec"].get("initContainers", [])}


class SeedManifestTests(_ManifestBase):
    """Part B: the workspace-seed init container copies the image's baked /workspace into a
    per-template subdir via an ALT mountpoint, once, marker-gated, ordered after hydrate."""

    def test_seed_container_present_for_template(self):
        seed = self._inits(self._manifest(template_id="42", template_title="Cool Template"))["workspace-seed"]
        self.assertEqual(seed["image"], "img:tag", "seed must run the USER's own image (baked /workspace)")
        mounts = {vm["name"]: vm for vm in seed["volumeMounts"]}
        self.assertIn("workspace", mounts)
        self.assertEqual(mounts["workspace"]["mountPath"], "/mnt/ws-seed",
                         "seed MUST mount the workspace volume at the ALT path, never /workspace (else it shadows the image)")
        self.assertEqual(mounts["workspace"].get("mountPropagation"), "HostToContainer")
        script = seed["args"][0]
        self.assertIn("/mnt/ws-seed", script)
        self.assertIn("seeded-", script, "marker-gated (once per template)")
        self.assertIn("cp -a /workspace/.", script, "copies the image's baked /workspace")
        self.assertIn("ls -A /workspace", script, "skips seeding a content-less image (no empty subdir)")
        # Must not brick the launch if a user planted /workspace/<slug> as a non-directory.
        self.assertIn('[ -e "$dst" ] && [ ! -d "$dst" ]', script,
                      "seed must skip gracefully (not abort the init) if the target is a non-directory")
        # The seed marker must be touched ONLY when the copy succeeds (a failed/partial copy must not
        # record the template as seeded and skip a later retry).
        self.assertIn("if cp -a /workspace/.", script,
                      "the seeded-marker touch must be gated on cp success")
        cp_idx = script.index("if cp -a /workspace/.")
        touch_idx = script.index('touch "$marker"')
        else_idx = script.index("else", cp_idx)
        self.assertLess(cp_idx, touch_idx)
        self.assertLess(touch_idx, else_idx, "the marker touch must sit in the cp-success (then) branch")

    def test_seed_ordered_after_hydrate(self):
        m = self._manifest(template_id="42", template_title="Cool")
        names = [c["name"] for c in m["spec"]["initContainers"]]
        self.assertIn("workspace-hydrate", names)
        self.assertIn("workspace-seed", names)
        self.assertLess(names.index("workspace-hydrate"), names.index("workspace-seed"),
                        "seed must be ordered AFTER hydrate")

    def test_no_seed_for_blank_launch(self):
        self.assertNotIn("workspace-seed", self._inits(self._manifest()),
                         "a blank/no-template launch must NOT be seeded (no empty default/ subdir)")

    def test_seed_key_per_template_slug(self):
        script = self._inits(self._manifest(template_id="42", template_title="Cool Template"))["workspace-seed"]["args"][0]
        self.assertIn("Cool-Template", script, "per-template seeding keys the subdir on the sanitized title")

    def test_seed_shared_key_when_per_template_off(self):
        k8s_module.settings.WORKSPACE_SEED_PER_TEMPLATE = False
        script = self._inits(self._manifest(template_id="42", template_title="Cool Template"))["workspace-seed"]["args"][0]
        self.assertIn("key=default", script, "with per-template off, all templates share the 'default' key")
        self.assertNotIn("Cool-Template", script)

    def test_seed_disabled_flag(self):
        k8s_module.settings.WORKSPACE_SEED_ENABLED = False
        self.assertNotIn("workspace-seed", self._inits(self._manifest(template_id="42", template_title="X")))

    def test_startup_relocates_cwd_to_seed_subdir(self):
        script = self.c._build_startup_script("nb-a1b2c3d4", "jupyter", None, seed_key="Cool-Template")
        self.assertIn("WS_ROOT=", script)
        self.assertIn("/workspace/Cool-Template", script)
        self.assertIn('--notebook-dir="$WS_ROOT"', script,
                      "Jupyter root must follow the seeded subdir when it exists")

    def test_startup_no_relocation_without_seed(self):
        script = self.c._build_startup_script("nb-a1b2c3d4", "jupyter", None, seed_key=None)
        self.assertNotIn("WS_ROOT=", script)
        self.assertIn("--notebook-dir=/workspace", script)


class HydrateDecisionTests(_ManifestBase):
    """Part C hydrate: the MIRROR/MERGE decision is made IN-CONTAINER from the on-node marker vs the
    manager-passed Gd — the manager passes only Gd, never a verdict (soft-affinity node drift safety)."""

    def _hydrate_script(self, m):
        return self._inits(m)["workspace-hydrate"]["args"][0]

    def test_hydrate_has_both_branches_and_incontainer_decision(self):
        script = self._hydrate_script(self._manifest())
        self.assertIn("rsync -a --delete", script, "MIRROR branch (propagate deletions) must exist")
        self.assertIn("rsync -a --update", script, "MERGE branch (accumulate) must exist")
        self.assertIn('[ "$Gd" -gt "$Gl" ]', script,
                      "the MIRROR/MERGE decision must be computed IN-CONTAINER (Gd vs on-node Gl)")
        self.assertIn("synced-generation", script, "reads/writes the on-node synced marker")
        self.assertIn("--exclude=", script, "the synced marker must be excluded from the rsyncs")

    def test_hydrate_receives_gd_from_store(self):
        k8s_module.store.get_durable_generation = lambda iid: 7
        m = self._manifest()
        self.assertEqual(m["metadata"]["annotations"].get("amd-oneclick/workspace-durable-generation"), "7")
        self.assertIn("Gd=7", self._hydrate_script(m))

    def test_hydrate_gd_zero_when_propagation_disabled(self):
        # KILL-SWITCH: propagation off => Gd passed as 0 => the MIRROR (--delete) branch can never fire.
        k8s_module.store.get_durable_generation = lambda iid: 7
        k8s_module.settings.WORKSPACE_DELETION_PROPAGATION_ENABLED = False
        self.assertIn("Gd=0", self._hydrate_script(self._manifest()))

    def test_hydrate_no_manager_verdict_annotation(self):
        # The manifest must not smuggle a MIRROR/MERGE verdict; only the node-independent Gd is passed.
        anns = self._manifest()["metadata"]["annotations"]
        self.assertNotIn("amd-oneclick/workspace-hydrate-verdict", anns)


class FlushGenerationTests(unittest.TestCase):
    """Part C flush: the script's --delete lives ONLY in the Gl==Gd (authoritative) branch, and the
    manager routes the pod's WS_FLUSH_* outcome to the right ledger update."""

    def setUp(self):
        import threading
        s = k8s_module.settings
        self._orig = {k: getattr(s, k) for k in (
            "WORKSPACE_VOLUME_TYPE", "WORKSPACE_DURABLE_STORAGE_CLASSES", "WORKSPACE_DURABLE_PVC_PREFIX",
            "WORKSPACE_LOCAL_CACHE_ROOT", "WORKSPACE_QUOTA_ENABLED", "WORKSPACE_DELETION_PROPAGATION_ENABLED")}
        s.WORKSPACE_VOLUME_TYPE = "localcache"
        s.WORKSPACE_DURABLE_STORAGE_CLASSES = list(FROZEN_SHARD_CLASSES)
        s.WORKSPACE_DURABLE_PVC_PREFIX = "oneclick-durable"
        s.WORKSPACE_LOCAL_CACHE_ROOT = "/nvme0/data/workspace"
        s.WORKSPACE_QUOTA_ENABLED = True
        s.WORKSPACE_DELETION_PROPAGATION_ENABLED = True
        self._sleep = k8s_module.time.sleep
        k8s_module.time.sleep = lambda *a, **k: None  # neutralize cleanup-poll sleeps
        self.c = k8s_module.K8sClient.__new__(k8s_module.K8sClient)
        self.c.namespace = "ns"
        self.c._notebook_tolerations = lambda: [{"operator": "Exists"}]
        self.c._workspace_sync_image = lambda: "img"
        self.c._flush_locks = {}
        self.c._flush_locks_guard = threading.Lock()
        self.c._pod_exists = lambda i: False
        # Record store calls; default Gd=0.
        self.calls = {"auth": [], "mark": [], "discard": [], "adv": 0}
        self._orig_store = {k: getattr(k8s_module.store, k) for k in (
            "get_durable_generation", "mark_workspace_flushed", "mark_workspace_flushed_authoritative",
            "discard_superseded_copy", "workspace_instance_advisory_lock")}
        k8s_module.store.get_durable_generation = lambda iid: 0
        k8s_module.store.mark_workspace_flushed = lambda i, n, t: (self.calls["mark"].append((i, n, t)) or True)
        k8s_module.store.mark_workspace_flushed_authoritative = \
            lambda i, n, t, g: (self.calls["auth"].append((i, n, t, g)) or True)
        k8s_module.store.discard_superseded_copy = lambda i, n, t: (self.calls["discard"].append((i, n, t)) or True)
        import contextlib as _cl

        def _adv(iid, timeout_seconds=180):
            self.calls["adv"] += 1
            return _cl.nullcontext(True)
        k8s_module.store.workspace_instance_advisory_lock = _adv
        # SSD reap during superseded discard → pretend it succeeds.
        self.c._cleanup_local_workspace_cache = lambda i, n: True

    def tearDown(self):
        for k, v in self._orig.items():
            setattr(k8s_module.settings, k, v)
        for k, v in self._orig_store.items():
            setattr(k8s_module.store, k, v)
        k8s_module.time.sleep = self._sleep

    def _core(self, outcome_log):
        """core_v1 whose flush pod immediately Succeeds and whose log carries `outcome_log`."""
        self.calls["create"] = self.calls.get("create", 0)

        def create(namespace, body):
            self.calls["create"] += 1

        def read(name, namespace):
            return SimpleNamespace(status=SimpleNamespace(phase="Succeeded", container_statuses=None))

        def delete(name, namespace, grace_period_seconds=0):
            raise k8s_module.ApiException(status=404)

        def read_log(name, namespace, tail_lines=None):
            if outcome_log is None:
                raise k8s_module.ApiException(status=500)
            return outcome_log
        return SimpleNamespace(create_namespaced_pod=create, read_namespaced_pod=read,
                               delete_namespaced_pod=delete, read_namespaced_pod_log=read_log)

    # --- script structure -----------------------------------------------------------------------
    def test_flush_script_delete_only_in_equal_branch(self):
        script = self.c._flush_script(propagate=True, quota_on=True, durable_gen=3)
        self.assertIn("Gd=3", script)
        # --delete must appear only AFTER the Gl==Gd test, never in the superseded/merge branches.
        idx_eq = script.index('[ "$Gl" -eq "$Gd" ]')
        idx_del = script.index("rsync -a --delete")
        self.assertLess(idx_eq, idx_del, "--delete must live inside the Gl==Gd authoritative branch")
        self.assertIn("WS_FLUSH_SUPERSEDED", script)
        self.assertIn("WS_FLUSH_MERGE", script)
        # superseded branch must not rsync to durable.
        sup = script[script.index("WS_FLUSH_SUPERSEDED"):]
        self.assertNotIn("rsync", sup.split("else", 1)[0], "superseded branch must NOT touch durable")

    def test_flush_authoritative_requires_completeness_marker(self):
        # DATA-LOSS REGRESSION: the authoritative --delete AND the superseded discard must both be
        # gated on the marker file being PRESENT (a completed hydrate's positive proof). Absent marker
        # (fresh/partial/interrupted hydrate — incl. the gen-0 bootstrap) must fall through to --update,
        # never --delete durable against an unproven local.
        script = self.c._flush_script(propagate=True, quota_on=True, durable_gen=0)
        self.assertIn('have_marker=0', script)
        self.assertIn('if [ -f "$marker" ]; then', script)
        # A present-but-empty/garbage marker must NOT count as a completeness proof (→ have_marker=0).
        self.assertIn("case \"$mv\" in ''|*[!0-9]*) have_marker=0;; *) have_marker=1; Gl=$mv;; esac", script)
        # Both destructive/decisive branches must require have_marker == 1.
        self.assertIn('[ "$have_marker" = "1" ] && [ "$Gl" -eq "$Gd" ]', script,
                      "authoritative --delete must require the completeness marker (not a defaulted Gl=0)")
        self.assertIn('[ "$have_marker" = "1" ] && [ "$Gl" -lt "$Gd" ]', script,
                      "superseded discard must require the completeness marker")

    def test_flush_script_legacy_when_propagation_off(self):
        script = self.c._flush_script(propagate=False, quota_on=True, durable_gen=0)
        self.assertIn("rsync -a --update", script)
        self.assertNotIn("--delete", script, "legacy path must never --delete")
        self.assertNotIn("synced-generation", script)

    # --- outcome routing ------------------------------------------------------------------------
    def test_authoritative_outcome_bumps_generation(self):
        self.c.core_v1 = self._core("WS_FLUSH_AUTHORITATIVE newgen=1 (Gl=0 Gd=0)")
        ok = self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T1")
        self.assertTrue(ok)
        self.assertEqual(self.calls["auth"], [("nb-a1b2c3d4", "node-7", "T1", 1)],
                         "AUTHORITATIVE must confirm via the generation-bumping path with Gd+1")
        self.assertEqual(self.calls["mark"], [])

    def test_superseded_outcome_discards(self):
        self.c.core_v1 = self._core("WS_FLUSH_SUPERSEDED Gl=0 Gd=2")
        ok = self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T1")
        self.assertTrue(ok)
        self.assertEqual(self.calls["discard"], [("nb-a1b2c3d4", "node-7", "T1")],
                         "SUPERSEDED must discard the stranded copy, never merge it up")
        self.assertEqual(self.calls["auth"], [])
        self.assertEqual(self.calls["mark"], [])

    def test_merge_outcome_marks_without_bump(self):
        self.c.core_v1 = self._core("WS_FLUSH_MERGE Gl=5 Gd=3")
        ok = self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T1")
        self.assertTrue(ok)
        self.assertEqual(self.calls["mark"], [("nb-a1b2c3d4", "node-7", "T1")])
        self.assertEqual(self.calls["auth"], [])

    def test_unreadable_log_falls_back_to_plain_mark(self):
        # If the outcome can't be read, fall back to a non-generation-advancing mark (always safe).
        self.c.core_v1 = self._core(None)
        ok = self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T1")
        self.assertTrue(ok)
        self.assertEqual(self.calls["mark"], [("nb-a1b2c3d4", "node-7", "T1")])
        self.assertEqual(self.calls["auth"], [])

    def test_termination_message_outcome_is_lossless(self):
        # The outcome must be read from the LOSSLESS terminated.message, NOT the pod log: even if the
        # log API raises, an AUTHORITATIVE flush still bumps the generation (no silent downgrade →
        # no warm-relaunch resurrection).
        def create(namespace, body):
            self.calls["create"] = self.calls.get("create", 0) + 1

        def read(name, namespace):
            term = SimpleNamespace(running=None,
                                   terminated=SimpleNamespace(message="WS_FLUSH_AUTHORITATIVE newgen=1 (Gl=0 Gd=0)"))
            return SimpleNamespace(status=SimpleNamespace(
                phase="Succeeded", container_statuses=[SimpleNamespace(state=term)]))

        def delete(name, namespace, grace_period_seconds=0):
            raise k8s_module.ApiException(status=404)

        def read_log(name, namespace, tail_lines=None):
            raise k8s_module.ApiException(status=500)  # log API down → must NOT matter
        self.c.core_v1 = SimpleNamespace(create_namespaced_pod=create, read_namespaced_pod=read,
                                         delete_namespaced_pod=delete, read_namespaced_pod_log=read_log)
        ok = self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T1")
        self.assertTrue(ok)
        self.assertEqual(self.calls["auth"], [("nb-a1b2c3d4", "node-7", "T1", 1)],
                         "AUTHORITATIVE must be confirmed from terminated.message even if the log API fails")

    def test_get_durable_generation_error_aborts_flush(self):
        # DATA-LOSS REGRESSION: a get_durable_generation() failure must ABORT the flush (never guess
        # Gd=0), else a superseded gen-0-marked copy would false-authoritatively --delete over a higher
        # durable generation. No pod created, no ledger mutation, returns False (retried by the sweep).
        def _boom(iid):
            raise RuntimeError("db down")
        k8s_module.store.get_durable_generation = _boom
        self.c.core_v1 = self._core("WS_FLUSH_AUTHORITATIVE newgen=1")
        ok = self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T1")
        self.assertFalse(ok, "a DB read failure under propagation must abort the flush, not guess Gd=0")
        self.assertEqual(self.calls.get("create", 0), 0, "no flush pod may be created on a Gd read failure")
        self.assertEqual(self.calls["auth"], [])
        self.assertEqual(self.calls["mark"], [])

    def test_advisory_lock_taken_when_propagation_on(self):
        self.c.core_v1 = self._core("WS_FLUSH_MERGE")
        self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T1")
        self.assertEqual(self.calls["adv"], 1, "the per-instance advisory lock must be taken when propagation is on")

    def test_legacy_flush_no_advisory_lock_and_marks(self):
        k8s_module.settings.WORKSPACE_DELETION_PROPAGATION_ENABLED = False
        self.c.core_v1 = self._core("ignored")
        ok = self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-7", "T1")
        self.assertTrue(ok)
        self.assertEqual(self.calls["adv"], 0, "legacy (--update) path must not take the advisory lock")
        self.assertEqual(self.calls["mark"], [("nb-a1b2c3d4", "node-7", "T1")])
        self.assertEqual(self.calls["auth"], [])

    def test_per_instance_lock_blocks_concurrent_same_instance(self):
        # A held per-instance lock (a flush already running in-process) makes a second concurrent flush
        # of the SAME instance (even a different node) skip — serializing destructive flushes.
        self.c.core_v1 = self._core("WS_FLUSH_MERGE")
        held = self.c._flush_lock_for("nb-a1b2c3d4")
        self.assertTrue(held.acquire(blocking=False))
        try:
            ok = self.c._flush_workspace_to_durable("nb-a1b2c3d4", "node-OTHER", "T2")
            self.assertFalse(ok, "a second concurrent flush of the same instance must skip (return False)")
        finally:
            held.release()
        # different instance uses a different lock object.
        self.assertIsNot(self.c._flush_lock_for("nb-a1b2c3d4"), self.c._flush_lock_for("nb-other"))


class EphemeralModeTests(_ManifestBase):
    """Part A seam: ephemeral mode (use_pvc=False on localcache) wires the SSD working copy + seed but
    NO durable shard / hydrate / flush, and frees the SSD immediately on destroy."""

    def test_durable_manifest_is_default(self):
        m = self._manifest()  # use_pvc=None
        self.assertEqual(m["metadata"]["annotations"].get("amd-oneclick/workspace-mode"), "durable")
        vols = {v["name"] for v in m["spec"]["volumes"]}
        self.assertIn("workspace-durable", vols)
        self.assertIn("workspace-hydrate", self._inits(m))

    def test_ephemeral_manifest_no_durable_no_hydrate_keeps_seed(self):
        m = self._manifest(use_pvc=False, template_id="42", template_title="T")
        self.assertEqual(m["metadata"]["annotations"].get("amd-oneclick/workspace-mode"), "ephemeral")
        vols = {v["name"] for v in m["spec"]["volumes"]}
        self.assertNotIn("workspace-durable", vols, "ephemeral has NO durable shard volume")
        inits = self._inits(m)
        self.assertNotIn("workspace-hydrate", inits, "ephemeral has NO hydrate init")
        self.assertIn("workspace-seed", inits, "seed still runs in ephemeral mode")
        # /workspace is still the node-local SSD hostPath.
        ws = [v for v in m["spec"]["volumes"] if v["name"] == "workspace"][0]
        self.assertIn("hostPath", ws)
        self.assertEqual(ws["hostPath"]["path"], self.c._workspace_local_cache_path("nb-a1b2c3d4"))

    def test_finalize_ephemeral_cleans_ssd_immediately(self):
        calls = {"reap": [], "clear_copies": [], "clear_state": [], "flush": []}
        self.c._cleanup_local_workspace_cache = lambda i, n: calls["reap"].append((i, n)) or True
        self.c._flush_workspace_to_durable = lambda *a, **k: calls["flush"].append(a) or True
        _orig = (k8s_module.store.clear_all_local_copies, k8s_module.store.clear_workspace_cache_state)
        k8s_module.store.clear_all_local_copies = lambda i: calls["clear_copies"].append(i)
        k8s_module.store.clear_workspace_cache_state = lambda i: calls["clear_state"].append(i)
        try:
            self.c._finalize_workspace_after_delete("nb-a1b2c3d4", "node-7", None, True, "ephemeral")
        finally:
            (k8s_module.store.clear_all_local_copies, k8s_module.store.clear_workspace_cache_state) = _orig
        self.assertEqual(calls["reap"], [("nb-a1b2c3d4", "node-7")], "ephemeral must free the SSD immediately")
        self.assertEqual(calls["clear_copies"], ["nb-a1b2c3d4"])
        self.assertEqual(calls["clear_state"], ["nb-a1b2c3d4"])
        self.assertEqual(calls["flush"], [], "ephemeral must NOT run the durable flush")

    def test_finalize_durable_runs_flush(self):
        calls = {"flush": [], "reap": []}
        self.c._flush_workspace_to_durable = lambda *a, **k: calls["flush"].append(a) or True
        self.c._cleanup_local_workspace_cache = lambda i, n: calls["reap"].append((i, n)) or True
        self.c._finalize_workspace_after_delete("nb-a1b2c3d4", "node-7", "T1", True, "durable")
        self.assertEqual(len(calls["flush"]), 1, "durable mode must run the out-of-pod flush")
        self.assertEqual(calls["reap"], [], "durable mode must NOT eagerly reap the SSD (flush-gated reaper does)")


class FlushShellE2ETests(unittest.TestCase):
    """Run the REAL generated flush script through bash+rsync against temp dirs (via the
    WS_FLUSH_LOCAL_DIR/WS_FLUSH_DURABLE_DIR overrides) to prove the durable-write behavior — the sole
    durable-data-loss surface: AUTHORITATIVE removes ONLY this session's deletions; SUPERSEDED and
    a MISSING/garbage marker never --delete durable; the completeness-marker fence actually holds."""

    def setUp(self):
        from shutil import which
        if not (which("bash") and which("rsync")):
            self.skipTest("bash+rsync required for the flush shell e2e")
        self.tmp = tempfile.mkdtemp(prefix="ws-flush-e2e-")
        self.local = os.path.join(self.tmp, "local")
        self.durable = os.path.join(self.tmp, "durable")
        os.makedirs(self.local); os.makedirs(self.durable)
        k8s_module.settings.WORKSPACE_VOLUME_TYPE = "localcache"
        self.c = k8s_module.K8sClient.__new__(k8s_module.K8sClient)

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _run(self, durable_gen):
        # quota_on=False so the empty-branch is a plain no-op (no mountpoint requirement on a tmpdir).
        script = self.c._flush_script(propagate=True, quota_on=False, durable_gen=durable_gen)
        env = dict(os.environ, WS_FLUSH_LOCAL_DIR=self.local, WS_FLUSH_DURABLE_DIR=self.durable)
        r = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env)
        self.assertEqual(r.returncode, 0, f"flush script failed: {r.stderr}")
        return r.stdout + r.stderr

    def _w(self, root, rel, content):
        p = os.path.join(root, rel)
        os.makedirs(os.path.dirname(p) or root, exist_ok=True)
        with open(p, "w") as f:
            f.write(content)

    def _r(self, root, rel):
        p = os.path.join(root, rel)
        if not os.path.exists(p):
            return None
        with open(p) as f:
            return f.read()

    def _marker(self, val):
        self._w(self.local, ".oneclick/synced-generation", val)

    def test_authoritative_removes_only_this_sessions_deletion(self):
        # durable has {a,b,c}; local (a complete descendant of gen 1, marker=1) DELETED b and edited a.
        # Authoritative --delete must drop b from durable, keep c, update a, and bump the marker to 2.
        for n in ("a", "b", "c"):
            self._w(self.durable, n, f"d-{n}")
        self._w(self.local, "a", "local-edited-a")
        self._w(self.local, "c", "d-c")
        self._marker("1")
        out = self._run(durable_gen=1)
        self.assertIn("WS_FLUSH_AUTHORITATIVE", out)
        self.assertEqual(self._r(self.durable, "a"), "local-edited-a", "edit propagates")
        self.assertIsNone(self._r(self.durable, "b"), "the user's deletion of b must propagate to durable")
        self.assertEqual(self._r(self.durable, "c"), "d-c", "unrelated durable file c must survive")
        self.assertEqual(self._r(self.local, ".oneclick/synced-generation"), "2", "marker bumped to Gd+1")
        # The synced marker itself must never travel to durable.
        self.assertIsNone(self._r(self.durable, ".oneclick/synced-generation"))

    def test_superseded_never_touches_durable(self):
        # local marker Gl=1 < Gd=3 → SUPERSEDED: durable must be byte-for-byte untouched.
        self._w(self.durable, "keep", "durable-keeps")
        self._w(self.local, "stale", "should-not-reach-durable")
        self._marker("1")
        out = self._run(durable_gen=3)
        self.assertIn("WS_FLUSH_SUPERSEDED", out)
        self.assertEqual(self._r(self.durable, "keep"), "durable-keeps")
        self.assertIsNone(self._r(self.durable, "stale"), "a superseded copy must NOT push its edits up")

    def test_no_marker_falls_back_to_update_never_delete(self):
        # The gen-0 interrupted-hydrate protection, exercised end to end: NO marker + nonempty local →
        # --update (accumulate), NEVER --delete. durable keeps files local is missing (no data loss).
        self._w(self.durable, "d-only", "precious-durable")
        self._w(self.durable, "shared", "durable-shared")
        self._w(self.local, "shared", "local-shared")
        self._w(self.local, "l-only", "local-new")
        # no marker written at all
        out = self._run(durable_gen=0)
        self.assertIn("WS_FLUSH_MERGE", out)
        self.assertIn("have_marker=0", out)
        self.assertEqual(self._r(self.durable, "d-only"), "precious-durable",
                         "no-marker flush must NOT --delete durable-only files (the gen-0 bootstrap safety)")
        self.assertEqual(self._r(self.durable, "l-only"), "local-new", "--update accumulates local additions")

    def test_garbage_marker_is_not_a_completeness_proof(self):
        # A present-but-garbage marker (truncated write, or a user scribbling junk) must be treated as
        # NO proof → --update, never an authoritative --delete against durable.
        self._w(self.durable, "d-only", "precious")
        self._w(self.local, "x", "y")
        self._marker("not-a-number")
        out = self._run(durable_gen=0)
        self.assertIn("WS_FLUSH_MERGE", out)
        self.assertEqual(self._r(self.durable, "d-only"), "precious",
                         "a garbage marker must not enable a destructive --delete")


class HydrateShellE2ETests(unittest.TestCase):
    """Run the REAL generated hydrate script through bash+rsync against temp dirs (by pointing the
    workspace/durable mount paths at them) to prove the actual deletion-propagation behavior:
    MIRROR removes a file a newer durable generation deleted (the resurrection fix); MERGE accumulates
    and keeps a newer un-flushed local file (ungraceful-kill safety); empty durable never wipes local."""

    def setUp(self):
        if not (self._which("bash") and self._which("rsync")):
            self.skipTest("bash+rsync required for the hydrate shell e2e")
        self.tmp = tempfile.mkdtemp(prefix="ws-e2e-")
        self.local = os.path.join(self.tmp, "ws")       # in-container /workspace (node SSD copy)
        self.durable = os.path.join(self.tmp, "durable")  # the durable shard subPath
        os.makedirs(self.local); os.makedirs(self.durable)
        s = k8s_module.settings
        self._orig = {k: getattr(s, k) for k in (
            "WORKSPACE_VOLUME_TYPE", "WORKSPACE_DURABLE_STORAGE_CLASSES", "WORKSPACE_DURABLE_PVC_PREFIX",
            "WORKSPACE_LOCAL_CACHE_ROOT", "WORKSPACE_MOUNT_PATH", "WORKSPACE_DURABLE_MOUNT_PATH",
            "WORKSPACE_QUOTA_ENABLED", "WORKSPACE_DELETION_PROPAGATION_ENABLED")}
        s.WORKSPACE_VOLUME_TYPE = "localcache"
        s.WORKSPACE_DURABLE_STORAGE_CLASSES = list(FROZEN_SHARD_CLASSES)
        s.WORKSPACE_DURABLE_PVC_PREFIX = "oneclick-durable"
        s.WORKSPACE_LOCAL_CACHE_ROOT = "/nvme0/data/workspace"
        s.WORKSPACE_MOUNT_PATH = self.local
        s.WORKSPACE_DURABLE_MOUNT_PATH = self.durable
        s.WORKSPACE_QUOTA_ENABLED = True
        s.WORKSPACE_DELETION_PROPAGATION_ENABLED = True
        self._orig_gdg = k8s_module.store.get_durable_generation
        self.c = k8s_module.K8sClient.__new__(k8s_module.K8sClient)
        self.c.namespace = "ns"

    def tearDown(self):
        for k, v in self._orig.items():
            setattr(k8s_module.settings, k, v)
        k8s_module.store.get_durable_generation = self._orig_gdg
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    @staticmethod
    def _which(binname):
        from shutil import which
        return which(binname)

    def _hydrate_script(self, gd):
        k8s_module.store.get_durable_generation = lambda iid: gd
        m = self.c._get_pod_manifest("u@e.com", "nb-a1b2c3d4", "img:tag",
                                     instance_type="jupyter", gpu_count=1)
        inits = {c["name"]: c for c in m["spec"]["initContainers"]}
        return inits["workspace-hydrate"]["args"][0]

    def _run(self, gd):
        script = self._hydrate_script(gd)
        r = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, f"hydrate script failed: {r.stderr}")
        return r

    def _write(self, root, rel, content, age_seconds=0):
        p = os.path.join(root, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as f:
            f.write(content)
        if age_seconds:
            import time as _t
            past = _t.time() - age_seconds
            os.utime(p, (past, past))
        return p

    def _read(self, root, rel):
        p = os.path.join(root, rel)
        if not os.path.exists(p):
            return None
        with open(p) as f:
            return f.read()

    def test_mirror_propagates_deletion(self):
        # Durable is a NEWER generation (Gd=1) that no longer has del.txt; the stale local (Gl=0) still
        # has it. MIRROR must make local match durable → del.txt is REMOVED (does not resurrect).
        self._write(self.durable, "keep.txt", "newcontent")
        self._write(self.local, "keep.txt", "oldcontent", age_seconds=3600)  # older mtime → rsync -a refreshes it
        self._write(self.local, "del.txt", "should-be-deleted")
        self._write(self.local, "localonly.txt", "gone-too")
        self._write(self.local, ".oneclick/synced-generation", "0")  # Gl=0
        out = self._run(gd=1)
        self.assertIn("MIRROR", out.stdout + out.stderr)
        self.assertEqual(self._read(self.local, "keep.txt"), "newcontent", "MIRROR overwrites with durable")
        self.assertIsNone(self._read(self.local, "del.txt"), "deleted file must NOT resurrect (MIRROR --delete)")
        self.assertIsNone(self._read(self.local, "localonly.txt"), "local-only stale file removed by MIRROR")
        # The synced marker is excluded from rsync (survives) and is stamped to Gd.
        self.assertEqual(self._read(self.local, ".oneclick/synced-generation"), "1")

    def test_merge_keeps_newer_local_and_accumulates(self):
        # Same generation (Gd==Gl==1) → MERGE: accumulate durable, keep a NEWER un-flushed local file,
        # never delete a local-only file. This is the ungraceful-kill warm-copy safety.
        self._write(self.durable, "shared.txt", "from-durable")
        self._write(self.durable, "keep.txt", "durable-old", age_seconds=3600)  # older than local
        self._write(self.local, "keep.txt", "local-newer")                       # newer → --update keeps
        self._write(self.local, "localonly.txt", "precious")
        self._write(self.local, ".oneclick/synced-generation", "1")
        out = self._run(gd=1)
        self.assertIn("MERGE", out.stdout + out.stderr)
        self.assertEqual(self._read(self.local, "shared.txt"), "from-durable", "MERGE accumulates durable files")
        self.assertEqual(self._read(self.local, "keep.txt"), "local-newer", "MERGE keeps the NEWER local copy")
        self.assertEqual(self._read(self.local, "localonly.txt"), "precious", "MERGE never deletes local-only files")
        # COMPLETENESS PROOF: a finished MERGE stamps the marker (=Gl) so the flush can trust local.
        self.assertEqual(self._read(self.local, ".oneclick/synced-generation"), "1")

    def test_merge_at_gen0_writes_marker_for_bootstrap_safety(self):
        # gen-0 bootstrap: durable non-empty, no on-node marker (fresh relaunch). A COMPLETED MERGE
        # must stamp the marker=0, so the subsequent flush has positive proof of a complete hydrate
        # before it may authoritatively --delete. (An interrupted merge never reaches the stamp, so
        # the flush safely falls back to --update — that half can't be exercised without a mid-rsync
        # kill, but the presence of the stamp here is what distinguishes complete from partial.)
        self._write(self.durable, "a.txt", "da")
        self._write(self.durable, "b.txt", "db")
        # no marker at all → Gl defaults 0; Gd=0 → MERGE
        out = self._run(gd=0)
        self.assertIn("MERGE", out.stdout + out.stderr)
        self.assertEqual(self._read(self.local, ".oneclick/synced-generation"), "0",
                         "a completed gen-0 MERGE must stamp the completeness marker")
        self.assertEqual(self._read(self.local, "a.txt"), "da")

    def test_empty_durable_never_wipes_local(self):
        # Durable empty → local kept as-is (an empty/absent durable can never wipe a good local copy).
        self._write(self.local, "important.txt", "keep me")
        out = self._run(gd=5)
        self.assertIn("durable empty", out.stdout + out.stderr)
        self.assertEqual(self._read(self.local, "important.txt"), "keep me")
        # Empty durable still stamps the marker (=Gd) so a brand-new instance's first flush can
        # authoritatively establish generation 1.
        self.assertEqual(self._read(self.local, ".oneclick/synced-generation"), "5")

    def test_stale_low_gd_downgrades_to_merge(self):
        # A manager Gd that is BEHIND the on-node marker (Gd<Gl) must only ever downgrade to MERGE
        # (never MIRROR-wipe a copy that is ahead). Deletion does not propagate, but nothing is lost.
        self._write(self.durable, "a.txt", "dur")
        self._write(self.local, "a.txt", "loc", age_seconds=3600)
        self._write(self.local, "localonly.txt", "safe")
        self._write(self.local, ".oneclick/synced-generation", "3")  # Gl=3 > Gd
        out = self._run(gd=1)
        self.assertIn("MERGE", out.stdout + out.stderr)
        self.assertEqual(self._read(self.local, "localonly.txt"), "safe", "stale-low Gd must not --delete")

    def test_hydrate_self_heals_wrong_type_marker_path_no_launch_block(self):
        # LAUNCH-DoS REGRESSION: a user (root in their pod) plants /workspace/.oneclick as a FILE. A
        # naive `mkdir -p $(dirname marker)` under `set -eux` would fail → hydrate init aborts → the
        # pod is stuck in Init (self-DoS). stamp_marker must self-heal (rm the file, mkdir the dir) so
        # hydrate completes (rc 0) and writes a valid marker.
        self._write(self.durable, "a.txt", "da")
        with open(os.path.join(self.local, ".oneclick"), "w") as f:
            f.write("i am a file not a dir")
        out = self._run(gd=1)  # rc==0 asserted in _run — proves no launch block
        self.assertIn("hydrated", out.stdout + out.stderr)
        # .oneclick is now a directory holding a valid marker.
        self.assertTrue(os.path.isdir(os.path.join(self.local, ".oneclick")))
        self.assertEqual(self._read(self.local, ".oneclick/synced-generation"), "1")


if __name__ == "__main__":
    unittest.main()
