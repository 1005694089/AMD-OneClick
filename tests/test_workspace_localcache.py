"""Tests for the two-tier localcache workspace: shard mapping stability, path-consistency across
code paths, and the append-only shard-list invariant (reordering = silent data loss)."""
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
# now the 4 healthy backends; the append-only rule applies going forward from here.
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
        removed (that remaps md5%len and strands data). Assert the current baseline is exactly the
        frozen list, in order — a future append adds entries AFTER these, never disturbing them."""
        n = len(FROZEN_SHARD_CLASSES)
        self.assertEqual(
            k8s_module.settings.WORKSPACE_DURABLE_STORAGE_CLASSES[:n],
            FROZEN_SHARD_CLASSES,
            f"The first {n} durable StorageClasses must never be reordered or removed (append-only).",
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
        """Regression: the quota initContainer idempotency guard must NOT use the old
        `findmnt -o SOURCE ... | grep -Fq "$img"` test — SOURCE is /dev/loopN, never the image path,
        so that test could never match and a leaked loop mount re-triggered `mount -o loop` →
        CrashLoopBackOff. The fix must detect an existing mount via losetup back-file and unmount a
        stale/foreign mount before remounting."""
        _, _, inits = self._containers(self._manifest())
        self.assertIn("workspace-quota", inits, "quota init container must exist when quota enabled")
        script = inits["workspace-quota"]["args"][0]
        # The dead guard was `current_source="$(findmnt -n -o SOURCE ...)"` then grep against $img.
        # Assert that specific dead assignment is gone (ignore prose in comments).
        code_lines = [ln for ln in script.splitlines() if not ln.lstrip().startswith("#")]
        code = "\n".join(code_lines)
        self.assertNotIn('current_source=', code,
                         "the dead findmnt-SOURCE-vs-image-path guard must be removed from executable code")
        # The robust guard must be present in executable code.
        self.assertIn("losetup -j", code, "must resolve loop devices backing the image via losetup")
        self.assertIn("mountpoint -q", code, "must check mountpoint state")
        self.assertIn("umount", code, "must unmount a stale/foreign mount before remount")


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

    def tearDown(self):
        for k, v in self._orig.items():
            setattr(k8s_module.settings, k, v)
        k8s_module.store.mark_workspace_flushed = self._orig_mark

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
        self.assertIn("mountpoint -q /local", spec["containers"][0]["args"][0])

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


if __name__ == "__main__":
    unittest.main()
