"""Best-effort per-user durable-tier cap via Huawei SFS Turbo directory quotas.

The durable workspace tier is a set of shared RWX SFS-Turbo shards; each instance lives in an
isolated subdir (md5-sharded, see k8s_client._durable_shard_*). Without a per-dir cap one user can
fill an entire 10Ti shard. SFS Turbo's directory-quota API hard-enforces a per-path capacity/inode
cap (writing past it -> "Disk quota exceeded"), so we set a quota on each instance's durable subdir.

DESIGN CONTRACT (do not weaken — this runs unattended through a change freeze):
  * EVERY SFS SDK call is wrapped: it logs (NEVER the AK/SK), returns a bool, and NEVER raises. A
    quota failure must never wedge instance create/delete — the feature is a backstop, not a gate.
  * The SDK is imported LAZILY inside each function (never at module top-level). A packaging problem
    degrades only this feature instead of crash-looping the whole manager with no hotfix window.
  * Shard math is resolved through the live K8sClient helpers (single source of truth for the
    append-only md5%len mapping) — resolved lazily to avoid an import cycle with k8s_client.
  * The {share_id, subdir} backend for each shard comes from STATIC config
    (settings.WORKSPACE_DURABLE_SHARD_SFS_BACKENDS), parallel-indexed to
    WORKSPACE_DURABLE_STORAGE_CLASSES — NOT a live PV read (the manager ServiceAccount has no RBAC
    on persistentvolumes; a live read would 403 and, fail-open, silently disable the feature).
"""
import logging
import threading

from .config import settings

logger = logging.getLogger(__name__)

# Process-local set of instance_ids whose quota this replica has already confirmed set, so
# steady-state reconciler ticks are cheap no-ops (no SFS API fan-out once everything is applied).
# Bounded by the number of live instances. Reset on process restart / leader change is harmless:
# re-applying is idempotent (create -> update fallback). NOT a correctness store — just a rate damper.
_applied_lock = threading.Lock()
_applied: set = set()


def mark_unapplied(instance_id: str) -> None:
    """Drop an instance from the applied-marker set so the next reconciler tick re-applies it."""
    with _applied_lock:
        _applied.discard(instance_id)


def is_applied(instance_id: str) -> bool:
    """True if this replica already confirmed the quota for instance_id this process-lifetime."""
    with _applied_lock:
        return instance_id in _applied


def prune_applied(live_ids) -> int:
    """Drop marker entries for instances no longer live, bounding the set to the live fleet.

    mark_unapplied fires in whichever of the N manager processes handles the delete HTTP request,
    which is usually NOT the leader that owns this _applied set — so ~(N-1)/N of deletes never prune
    it. The reconciler (leader) calls this each sweep with the current active-instance set to keep the
    marker bounded by live instances rather than leaking cumulative-applied ids across the freeze.
    """
    live = set(live_ids)
    with _applied_lock:
        stale = _applied - live
        _applied.difference_update(stale)
    return len(stale)


def ensure_dir_quota_for_instance(instance_id: str) -> bool:
    """Idempotent, marker-gated apply for the reconciler. Returns True if applied (now or already).

    Cheap no-op if this replica already confirmed the quota this process-lifetime. Otherwise applies
    and, on success, records the marker. A failed apply is NOT marked, so it retries next tick.
    """
    if not _enabled():
        return False
    with _applied_lock:
        if instance_id in _applied:
            return True
    ok = apply_dir_quota_for_instance(instance_id)
    if ok:
        with _applied_lock:
            _applied.add(instance_id)
    return ok


def _enabled() -> bool:
    """True only when localcache is on AND the dir-quota backstop is enabled."""
    if (settings.WORKSPACE_VOLUME_TYPE or "").strip().lower() != "localcache":
        return False
    return bool(settings.WORKSPACE_DURABLE_DIRQUOTA_ENABLED)


def _resolve_path(instance_id: str):
    """Resolve (share_id, quota_path) for an instance's durable subdir, or None if unresolvable.

    quota_path = /<shard-subdir>/<hh>/<safe_id> — the path SFS Turbo enforces the quota on. The
    shard index and per-instance subpath come from the live K8sClient helpers so the append-only
    md5%len mapping stays single-sourced; the {share_id, subdir} backend comes from static config.
    """
    # Whole body guarded: this is called by apply/delete which MUST never raise, and the inputs
    # (static config backends) are hand-edited, so treat any unexpected shape as "unresolved".
    try:
        from .k8s_client import k8s_client  # lazy: avoid import cycle (k8s_client imports this module)

        index = k8s_client._durable_shard_index(instance_id)

        backends = settings.WORKSPACE_DURABLE_SHARD_SFS_BACKENDS or []
        if index >= len(backends):
            logger.warning(
                "dirquota: no SFS backend configured for shard index %s (instance %s); "
                "WORKSPACE_DURABLE_SHARD_SFS_BACKENDS has %s entries — skipping quota",
                index, instance_id, len(backends),
            )
            return None
        backend = backends[index]
        # A valid-JSON-but-wrong-shape override (e.g. a list of strings) would make .get() raise.
        if not isinstance(backend, dict):
            logger.warning("dirquota: SFS backend for shard %s is not an object (%r); skipping quota for %s",
                           index, backend, instance_id)
            return None
        share_id = (backend.get("share_id") or "").strip()
        subdir = (backend.get("subdir") or "").strip().strip("/")
        if not share_id or not subdir:
            logger.warning("dirquota: incomplete SFS backend for shard %s (instance %s): %r",
                           index, instance_id, backend)
            return None
        # A not-yet-bound shard ships a placeholder subdir (e.g. "SHARD4_SUBDIR_PENDING") until an
        # operator pastes the real pvc-<uuid>. It is non-blank, so it would otherwise resolve to a
        # bogus path and make create/update fail on EVERY reconcile tick for ~1/N of users. Treat any
        # *_PENDING marker as unresolvable so the reconciler cleanly SKIPS that shard (cheap no-op).
        if subdir.endswith("_PENDING"):
            logger.warning("dirquota: shard %s subdir not yet bound (%r); skipping quota for %s",
                           index, subdir, instance_id)
            return None

        subpath = k8s_client._durable_subpath(instance_id)  # "<hh>/<safe_id>"
        quota_path = f"/{subdir}/{subpath}"
        return share_id, quota_path
    except Exception as e:
        logger.warning("dirquota: path resolution failed for %s (non-fatal): %s", instance_id, e)
        return None


def _build_client():
    """Build an SFS Turbo client from env-supplied AK/SK, or None on any problem (never raises).

    AK/SK are read ONLY from the environment (populated from a k8s Secret) and are never logged.
    """
    ak = (settings.SFS_TURBO_AK or "").strip()
    sk = (settings.SFS_TURBO_SK or "").strip()
    if not ak or not sk:
        logger.warning("dirquota: SFS_TURBO_AK/SK not set — skipping quota op")
        return None
    try:
        from huaweicloudsdkcore.auth.credentials import BasicCredentials
        from huaweicloudsdkcore.http.http_config import HttpConfig
        from huaweicloudsdkcore.region.region import Region
        from huaweicloudsdksfsturbo.v1 import SFSTurboClient
    except ImportError as e:
        logger.error("dirquota: huaweicloudsdksfsturbo not importable (%s) — feature disabled", e)
        return None
    try:
        cred = BasicCredentials(ak=ak, sk=sk, project_id=(settings.SFS_TURBO_PROJECT_ID or "").strip())
        region = Region(id=settings.SFS_TURBO_REGION, endpoint=settings.SFS_TURBO_ENDPOINT)
        # Short bounded timeout: the delete-hook runs INLINE on the admin-delete path, and the SDK
        # default is 120s. A blackholed/throttled SFS endpoint would otherwise stall the delete for up
        # to 2 min before fail-open catches it. Cap it so an SFS outage adds seconds, not minutes.
        http_config = HttpConfig.get_default_config()
        http_config.timeout = int(settings.SFS_TURBO_TIMEOUT_SECONDS)
        return (
            SFSTurboClient.new_builder()
            .with_credentials(cred)
            .with_region(region)
            .with_http_config(http_config)
            .build()
        )
    except Exception as e:  # never leak creds; message carries only the exception text
        logger.error("dirquota: failed building SFS client: %s", e)
        return None


def apply_dir_quota_for_instance(instance_id: str) -> bool:
    """Ensure the instance's durable subdir has a per-user quota. Best-effort; never raises.

    Tries create; on any client error (e.g. quota already exists) falls back to update so the cap is
    corrected idempotently. Returns True only if the quota is confirmed set (created or updated).
    The subdir must already exist (created by the workspace-hydrate init container) — the reconciler
    is the sole caller and only runs against live/hydrated instances, so the path is present.
    """
    if not _enabled():
        return False
    resolved = _resolve_path(instance_id)
    if not resolved:
        return False
    share_id, quota_path = resolved
    client = _build_client()
    if client is None:
        return False

    capacity_mb = int(settings.WORKSPACE_DURABLE_DIRQUOTA_CAPACITY_MB)
    inode_count = int(settings.WORKSPACE_DURABLE_DIRQUOTA_INODE_COUNT)
    try:
        from huaweicloudsdksfsturbo.v1 import (
            CreateFsDirQuotaRequest, CreateFsDirQuotaRequestBody,
            UpdateFsDirQuotaRequest, UpdateFsDirQuotaRequestBody,
        )
    except ImportError as e:
        logger.error("dirquota: SDK model import failed (%s)", e)
        return False

    try:
        client.create_fs_dir_quota(CreateFsDirQuotaRequest(
            share_id=share_id,
            body=CreateFsDirQuotaRequestBody(path=quota_path, capacity=capacity_mb, inode=inode_count),
        ))
        logger.info("dirquota: created quota for %s at %s (%sMB, %s inodes)",
                    instance_id, quota_path, capacity_mb, inode_count)
        return True
    except Exception as create_err:
        # Most commonly "quota already exists" — fall back to update so the cap is idempotently
        # corrected. Any other error also lands here and is logged; we never raise.
        try:
            client.update_fs_dir_quota(UpdateFsDirQuotaRequest(
                share_id=share_id,
                body=UpdateFsDirQuotaRequestBody(path=quota_path, capacity=capacity_mb, inode=inode_count),
            ))
            logger.info("dirquota: updated existing quota for %s at %s (%sMB, %s inodes)",
                        instance_id, quota_path, capacity_mb, inode_count)
            return True
        except Exception as update_err:
            logger.error("dirquota: create AND update failed for %s at %s (create=%s / update=%s)",
                         instance_id, quota_path, create_err, update_err)
            return False


def delete_dir_quota_for_instance(instance_id: str) -> bool:
    """Best-effort remove of an instance's durable dir-quota. Never raises.

    Called from the admin durable-delete path. Ordering vs the trash-move (rename) is subtle — SFS
    refuses delete_fs_dir_quota on a non-empty dir, and after the rename the original path is gone —
    so this is invoked while the subdir still exists and is verified empirically during smoke test.
    A failure here is harmless: an orphan quota rule on a vanished path consumes nothing.
    """
    if not _enabled():
        return False
    resolved = _resolve_path(instance_id)
    if not resolved:
        return False
    share_id, quota_path = resolved
    client = _build_client()
    if client is None:
        return False
    try:
        from huaweicloudsdksfsturbo.v1 import DeleteFsDirQuotaRequest, DeleteFsDirQuotaRequestBody
    except ImportError as e:
        logger.error("dirquota: SDK model import failed on delete (%s)", e)
        return False
    try:
        client.delete_fs_dir_quota(DeleteFsDirQuotaRequest(
            share_id=share_id, body=DeleteFsDirQuotaRequestBody(path=quota_path),
        ))
        logger.info("dirquota: deleted quota for %s at %s", instance_id, quota_path)
        return True
    except Exception as e:
        logger.warning("dirquota: delete quota for %s at %s failed (non-fatal): %s",
                       instance_id, quota_path, e)
        return False
