"""
Persistent store for users, image catalog, and credit accounting.

Uses PostgreSQL when DATABASE_URL is set; falls back to local SQLite for dev.
"""
import hashlib
import json
import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    inspect,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.exc import IntegrityError

from .config import settings


DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL and os.getenv("AMD_ONECLICK_POSTGRES_SERVICE_HOST"):
    pg_host = os.getenv("AMD_ONECLICK_POSTGRES_SERVICE_HOST")
    pg_port = os.getenv("AMD_ONECLICK_POSTGRES_SERVICE_PORT", "5432")
    pg_user = os.getenv("POSTGRES_USER", "amd_oneclick")
    pg_password = os.getenv("POSTGRES_PASSWORD", "")
    pg_db = os.getenv("POSTGRES_DB", "amd_oneclick")
    DATABASE_URL = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
if not DATABASE_URL:
    DATABASE_URL = f"sqlite:///{os.getenv('DATABASE_PATH', '/data/amd-oneclick.db')}"
if DATABASE_URL.startswith("sqlite:///"):
    db_path = DATABASE_URL.removeprefix("sqlite:///")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True, pool_size=5, max_overflow=10, pool_recycle=1800)
metadata = MetaData()

# Row-level locking (FOR UPDATE SKIP LOCKED) lets concurrent agents claim different pending jobs
# without contending on the same row or scanning each other's in-flight work. Only Postgres/MySQL
# support SKIP LOCKED; SQLite (dev/tests) does not, so gate on the dialect.
_SUPPORTS_SKIP_LOCKED = engine.dialect.name in ("postgresql", "mysql", "mariadb")

# Serializes the count-and-insert in create_custom_image within a single process so the
# per-user cap can't be bypassed by concurrent different-name requests. Across multiple
# Postgres workers a transaction-level advisory lock (taken inside that function) provides
# the cross-process guarantee; SQLite is single-writer so this in-process lock suffices.
_custom_image_cap_lock = threading.Lock()
# Arbitrary fixed namespace for the two-int pg_advisory_xact_lock keyspace; pairs with
# user_id so different users never contend.
_CUSTOM_IMAGE_CAP_LOCK_NS = 0x0A3D_C0DE
# Single-bigint advisory-lock key that serializes startup schema migration across workers/replicas.
# Kept well below the two-int (NS<<32 | user_id) range above so the two lock spaces never collide.
_SCHEMA_MIGRATION_LOCK_KEY = 0x5CEA_0001
# Namespace for the per-instance workspace-flush advisory lock (two-int form: (NS, instance_hash)).
# Postgres keeps single-int and two-int advisory locks in SEPARATE spaces, and this NS differs from
# the custom-image-cap NS above, so the three lock spaces never collide.
_WORKSPACE_FLUSH_LOCK_NS = 0x5CEA_0002

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("provider", String(64), nullable=False),
    Column("provider_id", String(255), nullable=False),
    Column("email", String(255), nullable=False, unique=True),
    Column("name", String(255)),
    Column("avatar_url", Text),
    Column("credits", Integer, nullable=False, default=100),
    Column("is_editor", Boolean, nullable=False, default=False),
    # Sticky flag: when true, charge_usage_unit skips deduction so the balance freezes at
    # whatever it was when the flag was set. Does not exempt the instance from the idle reaper.
    Column("unlimited_credits", Boolean, nullable=False, default=False),
    Column("ssh_public_key", Text),
    # Session epoch. Every issued session cookie embeds the value that was
    # current at login; bumping this invalidates all outstanding sessions for
    # the user (used for admin revocation / kicking abusive accounts).
    Column("token_version", Integer, nullable=False, default=0),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("provider", "provider_id", name="uq_users_provider_id"),
)

images = Table(
    "images",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(255), nullable=False, unique=True),
    Column("image", Text, nullable=False, unique=True),
    Column("description", Text),
    Column("enabled", Boolean, nullable=False, default=True),
    Column("sync_status", String(32), nullable=False, default="ready"),
    Column("desired_count", Integer, nullable=False, default=0),
    Column("ready_count", Integer, nullable=False, default=0),
    Column("sync_message", Text),
    Column("sync_started_at", String(64)),
    Column("sync_completed_at", String(64)),
    Column("source_type", String(32), nullable=False, server_default="manual"),
    Column("source_ref", Text),
    Column("acr_backup_ref", Text),
    Column("acr_backup_status", String(32)),
    Column("last_launched_at", String(64)),
    # Manifest digest of the copy pushed to the LAN registry (zot). Set by the `push` job; used as
    # the registry-durable "ready" gate (the manager has no route to the LAN registry, so presence
    # is agent-reported: a non-NULL digest means the image is durable in the registry).
    Column("digest", String(255)),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)

notebook_templates = Table(
    "notebook_templates",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", String(255), nullable=False),
    Column("slug", String(255), nullable=False, unique=True),
    Column("description", Text),
    Column("category", String(128)),
    Column("tags", Text),
    Column("image", Text, nullable=False),
    Column("repo_url", Text, nullable=False),
    Column("branch", String(255), nullable=False, default="main"),
    Column("notebook_path", Text, nullable=False),
    Column("cover_url", Text),
    Column("instance_type", String(64)),
    Column("start_command", Text),
    Column("app_port", Integer),
    Column("model_source", String(32)),
    Column("ssh_enabled", Boolean, nullable=False, default=False),
    Column("use_pvc", Boolean),
    Column("model_mount", String(64)),
    Column("enabled", Boolean, nullable=False, default=True),
    Column("sort_order", Integer, nullable=False, default=0),
    Column("owner_user_id", Integer, ForeignKey("users.id")),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)

template_preview_cache = Table(
    "template_preview_cache",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("template_id", Integer, ForeignKey("notebook_templates.id"), nullable=False, unique=True),
    Column("repo_url", Text, nullable=False),
    Column("branch", String(255), nullable=False),
    Column("notebook_path", Text, nullable=False),
    Column("source_fingerprint", String(64), nullable=False),
    Column("notebook_json", Text),
    Column("status", String(32), nullable=False, default="pending"),
    Column("error_message", Text),
    Column("last_synced_at", String(64)),
    Column("next_sync_at", String(64)),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
)

template_preview_assets = Table(
    "template_preview_assets",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("template_id", Integer, ForeignKey("notebook_templates.id"), nullable=False),
    Column("asset_path", Text, nullable=False),
    Column("content_type", String(255)),
    Column("content", LargeBinary, nullable=False),
    Column("status", String(32), nullable=False, default="ready"),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("template_id", "asset_path", name="uq_template_preview_asset_path"),
)

instance_records = Table(
    "instance_records",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, ForeignKey("users.id"), nullable=False),
    Column("email", String(255), nullable=False),
    Column("instance_id", String(255), nullable=False, unique=True),
    Column("image", Text, nullable=False),
    Column("instance_type", String(64), nullable=False),
    Column("gpu_count", Integer, nullable=False),
    Column("node_port", Integer),
    Column("opencode_node_port", Integer),
    Column("status", String(64), nullable=False, default="running"),
    Column("created_at", String(64), nullable=False),
    Column("last_charged_at", String(64), nullable=False),
    Column("billing_session_id", String(255), nullable=False),
    Column("ready_at", String(64)),
    Column("billing_started_at", String(64)),
    Column("deleted_at", String(64)),
    Column("pod_type", String(64)),
    Column("api_launched", Boolean, nullable=False, default=False),
)

instance_launch_events = Table(
    "instance_launch_events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, ForeignKey("users.id"), nullable=False),
    Column("email", String(255), nullable=False),
    Column("instance_id", String(255), nullable=False),
    Column("image", Text, nullable=False),
    Column("instance_type", String(64), nullable=False),
    Column("gpu_count", Integer, nullable=False),
    Column("template_id", Integer),
    Column("template_title", String(255)),
    Column("created_at", String(64), nullable=False),
    Column("pod_type", String(64)),
)

credit_ledger = Table(
    "credit_ledger",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, ForeignKey("users.id"), nullable=False),
    Column("delta", Integer, nullable=False),
    Column("reason", Text, nullable=False),
    Column("instance_id", String(255)),
    Column("created_at", String(64), nullable=False),
)

coupon_redemptions = Table(
    "coupon_redemptions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("coupon_id", String(255), nullable=False, unique=True),
    Column("user_id", Integer, ForeignKey("users.id"), nullable=False),
    Column("external_user_id", String(255)),
    Column("card_hours", Integer, nullable=False),
    Column("credits", Integer, nullable=False),
    Column("issued_at", String(64)),
    Column("expires_at", String(64)),
    Column("redeemed_at", String(64), nullable=False),
)

usage_charges = Table(
    "usage_charges",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, ForeignKey("users.id"), nullable=False),
    Column("instance_id", String(255), nullable=False),
    Column("billing_session_id", String(255), nullable=False),
    Column("billing_unit", Integer, nullable=False),
    Column("gpu_count", Integer, nullable=False),
    Column("credits", Integer, nullable=False),
    Column("created_at", String(64), nullable=False),
    UniqueConstraint("billing_session_id", "billing_unit", name="uq_usage_charge_session_unit"),
)

# Per-user custom images. This is deliberately separate from the global `images`
# catalog so a user's custom image never appears in other users' launch dropdowns.
# It also doubles as the build queue consumed by the R9700 build-agent.
custom_images = Table(
    "custom_images",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("user_id", Integer, ForeignKey("users.id"), nullable=False, index=True),
    Column("name", String(64), nullable=False),
    Column("image", Text, nullable=False),
    Column("dockerfile", Text, nullable=False),
    Column("source_type", String(32), nullable=False, server_default="dockerfile"),
    Column("github_url", Text),
    Column("build_status", String(32), nullable=False, default="pending"),
    Column("build_log", Text),
    Column("claimed_by", String(128)),
    Column("claimed_at", String(64)),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    Column("last_launched_at", String(64)),
    # Manifest digest of the copy pushed to the LAN registry (zot); see images.digest.
    Column("digest", String(255)),
    UniqueConstraint("user_id", "name", name="uq_custom_image_user_name"),
)

# Build queue states that count against a user's quota and block re-use of a name.
CUSTOM_IMAGE_ACTIVE_STATUSES = ("pending", "building", "ready", "evicted")
CUSTOM_IMAGE_LOG_MAX_CHARS = 60000

# Tracks which image ref is loaded on which GPU node (distribute/evict bookkeeping).
image_nodes = Table(
    "image_nodes",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("image_ref", Text, nullable=False, index=True),
    Column("node_name", String(255), nullable=False),
    # status: loaded | importing | quarantined. Only "loaded" counts as available (see
    # image_loaded_on_node / list_nodes_for_image). "importing" marks an in-flight distribute so a
    # hung import is distinguishable from "never started"; "quarantined" marks a wedged node.
    Column("status", String(32), nullable=False, server_default="loaded"),
    Column("size_bytes", Integer),
    Column("loaded_at", String(64)),
    Column("last_seen_at", String(64)),
    # When set (ISO-8601 UTC), this node is quarantined until the timestamp: the reaper will not
    # requeue distribute/evict onto it and launches route around it (Part D).
    Column("quarantined_until", String(64)),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("image_ref", "node_name", name="uq_image_node"),
)

# Two-tier localcache workspace: tracks, per instance, the node whose local SSD holds the warm
# working copy and when the instance last stopped. Drives (a) warm-relaunch soft affinity and
# (b) the delayed-local-delete reaper. Independent of instance_records so it survives soft-delete
# of the instance row and is keyed purely by instance_id.
workspace_cache_state = Table(
    "workspace_cache_state",
    metadata,
    Column("instance_id", String(255), primary_key=True),
    Column("node_name", String(255)),
    Column("stopped_at", String(64)),   # ISO ts when the pod was last deleted; NULL while running
    Column("updated_at", String(64), nullable=False),
    # Fenced-deletion generation (Part C): the per-instance canonical durable version. NULL reads as
    # 0. Bumped to Gd+1 ONLY by a confirmed authoritative flush (`rsync --delete`); the single fence
    # is "only --delete when a copy's synced_generation == this durable_generation". Reset when the
    # durable workspace is admin-deleted so a re-created workspace never inherits a stale-high value.
    Column("durable_generation", Integer),
    # The reaper query filters on (stopped_at IS NOT NULL, stopped_at < cutoff, node_name IS NOT NULL);
    # index those so it stays cheap as the table grows instead of full-scanning every 10 min.
    Index("ix_workspace_cache_reap", "stopped_at", "node_name"),
)

# Per-(instance, node) SSD-copy ledger for the two-tier localcache workspace. A row means "instance_id
# has a node-local SSD copy on node_name from a stopped session"; flushed_at records whether that copy
# has been confirmed-flushed to the durable NFS shard. This is the single authority for BOTH the flush
# retry sweep and the reaper. DATA-LOSS-CRITICAL design:
#   * Keyed by (instance_id, node_name), NOT just instance_id — a cross-node relaunch (session ends on
#     node A, next session runs on node B) keeps node A's row so the retry sweep still flushes it and
#     the reaper still gates on node A. A single node_name field on cache_state would orphan it.
#   * `session_token` (the stopped_at snapshot) fences stale flushes: a straggler flush pod still
#     running from a prior stop can only set flushed_at on the row whose token it actually flushed;
#     every new stop rewrites the token and resets flushed_at to NULL, so a late confirm can never
#     falsely certify a newer, unflushed session.
#   * The reaper frees a node's SSD copy ONLY when flushed_at IS NOT NULL for that (instance, node).
workspace_local_copy = Table(
    "workspace_local_copy",
    metadata,
    Column("instance_id", String(255), nullable=False),
    Column("node_name", String(255), nullable=False),
    # stopped_at snapshot of the session that produced this copy; the flush fence token AND TTL clock.
    Column("session_token", String(64), nullable=False),
    # ISO ts when the local->durable flush for THIS session/copy confirmed; NULL = unflushed.
    Column("flushed_at", String(64)),
    # Fenced-deletion generation (Part C): the durable generation this SSD copy is a confirmed
    # DESCENDANT of. NULL reads as 0. Advances ONLY on a confirmed authoritative flush from this copy
    # (set to the new durable_generation, token-fenced) — never speculatively. The on-node marker
    # (.oneclick/synced-generation) is this value's node-local mirror consulted by the in-container
    # hydrate/flush; this column is the manager-side record for retry-sweep routing/observability.
    Column("synced_generation", Integer),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    PrimaryKeyConstraint("instance_id", "node_name", name="pk_workspace_local_copy"),
    Index("ix_workspace_local_copy_node", "node_name"),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def row_to_dict(row) -> Optional[dict]:
    return dict(row) if row else None


def init_db():
    metadata.create_all(engine)
    with engine.begin() as conn:
        ensure_schema_columns(conn)
        ensure_default_image(conn)
        ensure_default_blank_template(conn)


def ensure_schema_columns(conn):
    # Serialize concurrent migrators. With uvicorn --workers N (and/or multiple replicas) every
    # worker runs this at startup; a brand-new additive ALTER would otherwise race — all workers
    # inspect, all see the column missing, all issue the ALTER, one wins and the rest crash with
    # DuplicateColumn (observed under --workers 2). A transaction-scoped Postgres advisory lock makes
    # the first migrator apply the DDL and commit while the others block, then re-inspect (READ
    # COMMITTED sees the committed columns) and no-op. SQLite is single-writer, so it needs no lock.
    if conn.dialect.name == "postgresql":
        conn.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": _SCHEMA_MIGRATION_LOCK_KEY})
    inspector = inspect(conn)
    user_columns = {col["name"] for col in inspector.get_columns("users")}
    if "is_editor" not in user_columns:
        conn.execute(text("ALTER TABLE users ADD COLUMN is_editor BOOLEAN NOT NULL DEFAULT FALSE"))
    if "ssh_public_key" not in user_columns:
        conn.execute(text("ALTER TABLE users ADD COLUMN ssh_public_key TEXT"))
    if "token_version" not in user_columns:
        conn.execute(text("ALTER TABLE users ADD COLUMN token_version INTEGER NOT NULL DEFAULT 0"))
    if "unlimited_credits" not in user_columns:
        conn.execute(text("ALTER TABLE users ADD COLUMN unlimited_credits BOOLEAN NOT NULL DEFAULT FALSE"))

    instance_columns = {col["name"] for col in inspector.get_columns("instance_records")}
    if "billing_session_id" not in instance_columns:
        conn.execute(text("ALTER TABLE instance_records ADD COLUMN billing_session_id VARCHAR(255)"))
        conn.execute(text("UPDATE instance_records SET billing_session_id = instance_id WHERE billing_session_id IS NULL"))
    if "opencode_node_port" not in instance_columns:
        conn.execute(text("ALTER TABLE instance_records ADD COLUMN opencode_node_port INTEGER"))
    if "ready_at" not in instance_columns:
        conn.execute(text("ALTER TABLE instance_records ADD COLUMN ready_at VARCHAR(64)"))
    if "billing_started_at" not in instance_columns:
        conn.execute(text("ALTER TABLE instance_records ADD COLUMN billing_started_at VARCHAR(64)"))

    usage_columns = {col["name"] for col in inspector.get_columns("usage_charges")}
    if "billing_session_id" not in usage_columns:
        conn.execute(text("ALTER TABLE usage_charges ADD COLUMN billing_session_id VARCHAR(255)"))
        conn.execute(text("UPDATE usage_charges SET billing_session_id = instance_id WHERE billing_session_id IS NULL"))

    template_columns = {col["name"] for col in inspector.get_columns("notebook_templates")}
    if "instance_type" not in template_columns:
        conn.execute(text("ALTER TABLE notebook_templates ADD COLUMN instance_type VARCHAR(64)"))
    if "start_command" not in template_columns:
        conn.execute(text("ALTER TABLE notebook_templates ADD COLUMN start_command TEXT"))
    if "app_port" not in template_columns:
        conn.execute(text("ALTER TABLE notebook_templates ADD COLUMN app_port INTEGER"))
    if "model_source" not in template_columns:
        conn.execute(text("ALTER TABLE notebook_templates ADD COLUMN model_source VARCHAR(32)"))
    if "ssh_enabled" not in template_columns:
        conn.execute(text("ALTER TABLE notebook_templates ADD COLUMN ssh_enabled BOOLEAN NOT NULL DEFAULT FALSE"))
    if "use_pvc" not in template_columns:
        conn.execute(text("ALTER TABLE notebook_templates ADD COLUMN use_pvc BOOLEAN"))
    if "model_mount" not in template_columns:
        conn.execute(text("ALTER TABLE notebook_templates ADD COLUMN model_mount VARCHAR(64)"))

    # New launches reuse the same Kubernetes instance_id, so billing idempotency must be scoped
    # to a launch session instead of the stable instance id.
    if conn.dialect.name == "postgresql":
        conn.execute(text("ALTER TABLE usage_charges DROP CONSTRAINT IF EXISTS uq_usage_charge_instance_unit"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_usage_charge_session_unit ON usage_charges (billing_session_id, billing_unit)"))
    elif conn.dialect.name == "sqlite":
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_usage_charge_session_unit ON usage_charges (billing_session_id, billing_unit)"))

    template_columns = {col["name"] for col in inspector.get_columns("notebook_templates")}
    if "owner_user_id" not in template_columns:
        conn.execute(text("ALTER TABLE notebook_templates ADD COLUMN owner_user_id INTEGER"))

    image_columns = {col["name"] for col in inspector.get_columns("images")}
    if "source_type" not in image_columns:
        conn.execute(text("ALTER TABLE images ADD COLUMN source_type VARCHAR(32) NOT NULL DEFAULT 'manual'"))
    if "source_ref" not in image_columns:
        conn.execute(text("ALTER TABLE images ADD COLUMN source_ref TEXT"))
    if "acr_backup_ref" not in image_columns:
        conn.execute(text("ALTER TABLE images ADD COLUMN acr_backup_ref TEXT"))
    if "acr_backup_status" not in image_columns:
        conn.execute(text("ALTER TABLE images ADD COLUMN acr_backup_status VARCHAR(32)"))
    if "last_launched_at" not in image_columns:
        conn.execute(text("ALTER TABLE images ADD COLUMN last_launched_at VARCHAR(64)"))

    if inspector.has_table("custom_images"):
        custom_image_columns = {col["name"] for col in inspector.get_columns("custom_images")}
        if "last_launched_at" not in custom_image_columns:
            conn.execute(text("ALTER TABLE custom_images ADD COLUMN last_launched_at VARCHAR(64)"))
        if "source_type" not in custom_image_columns:
            conn.execute(text("ALTER TABLE custom_images ADD COLUMN source_type VARCHAR(32) NOT NULL DEFAULT 'dockerfile'"))
        if "github_url" not in custom_image_columns:
            conn.execute(text("ALTER TABLE custom_images ADD COLUMN github_url TEXT"))

    # Backward-compatible creation for databases initialized before these tables.
    metadata.create_all(bind=conn, tables=[template_preview_cache, template_preview_assets, coupon_redemptions, instance_launch_events, custom_images, image_nodes])

    # Additive: a node whose containerd wedged is quarantined until this timestamp (ISO-8601 UTC).
    # Mirrors the acr_backup_status additive-column pattern above so old databases upgrade in place.
    if inspector.has_table("image_nodes"):
        image_node_columns = {col["name"] for col in inspector.get_columns("image_nodes")}
        if "quarantined_until" not in image_node_columns:
            conn.execute(text("ALTER TABLE image_nodes ADD COLUMN quarantined_until VARCHAR(64)"))

    # Additive: LAN-registry manifest digest (P1). NULL on old rows => not-yet-pushed, so the
    # digest-gated readiness check treats them exactly like today (loaded-row count only) until a
    # push job records a digest. Mirrors the acr_backup_status/heartbeat_at additive pattern.
    if inspector.has_table("images"):
        image_columns = {col["name"] for col in inspector.get_columns("images")}
        if "digest" not in image_columns:
            conn.execute(text("ALTER TABLE images ADD COLUMN digest VARCHAR(255)"))
    if inspector.has_table("custom_images"):
        custom_image_columns = {col["name"] for col in inspector.get_columns("custom_images")}
        if "digest" not in custom_image_columns:
            conn.execute(text("ALTER TABLE custom_images ADD COLUMN digest VARCHAR(255)"))

    # Caller-supplied pod tag (hackathon/workshop/one-click). Nullable, no default -> old rows = NULL.
    instance_columns = {col["name"] for col in inspector.get_columns("instance_records")}
    if "pod_type" not in instance_columns:
        conn.execute(text("ALTER TABLE instance_records ADD COLUMN pod_type VARCHAR(64)"))
    # Billing is scoped to API-launched instances. Existing rows default FALSE so web/template
    # instances are never retroactively billed when the scheduler is enabled.
    if "api_launched" not in instance_columns:
        conn.execute(text("ALTER TABLE instance_records ADD COLUMN api_launched BOOLEAN NOT NULL DEFAULT FALSE"))
    launch_event_columns = {col["name"] for col in inspector.get_columns("instance_launch_events")}
    if "pod_type" not in launch_event_columns:
        conn.execute(text("ALTER TABLE instance_launch_events ADD COLUMN pod_type VARCHAR(64)"))

    # Drop the legacy image-service tables (removed with the off-cluster image_jobs distribution +
    # Dragonfly P2P purge system). They are no longer in the ORM metadata, so create_all() will not
    # recreate them; but a pre-existing production DB still has image_jobs with NO ACTION foreign
    # keys onto images/custom_images. Leaving it would make every catalog/custom-image DELETE raise
    # ForeignKeyViolation. Dropping it (child table -> safe) removes those constraints. Idempotent
    # (IF EXISTS): a no-op on fresh DBs that never had these tables. image_jobs first (it FKs the
    # image tables), then the standalone launch_intents resume ledger.
    conn.execute(text("DROP TABLE IF EXISTS image_jobs"))
    conn.execute(text("DROP TABLE IF EXISTS launch_intents"))

    # The per-(instance,node) workspace_local_copy ledger (out-of-pod flush tracking) is created by
    # metadata.create_all above when absent — no ALTER needed. A pre-existing durable_flush_at column
    # from an earlier iteration of this feature (single-column design) is harmless if it lingers; the
    # new code reads only workspace_local_copy, so we leave any stray column in place rather than risk
    # a destructive DROP on a live table.

    # Fenced-deletion generation columns (workspace_persistence_fix). create_all does NOT add columns
    # to a pre-existing table, so add them explicitly (mirrors every other additive column above).
    ensure_workspace_generation_columns(conn)

    _backfill_hf_credit_cap(conn)


def ensure_workspace_generation_columns(conn):
    """Additive migration for the fenced-deletion generation columns (workspace_persistence_fix).

    create_all does NOT add columns to a pre-existing table, so add them explicitly (idempotent,
    guarded by inspect — mirrors the other additive column migrations). NULL on old rows reads as
    generation 0 (today's accumulate-only behavior), so every existing instance starts at 0 and only
    reaches generation 1 on its first authoritative flush — no risk to existing durable data."""
    inspector = inspect(conn)
    if inspector.has_table("workspace_cache_state"):
        wcs_columns = {col["name"] for col in inspector.get_columns("workspace_cache_state")}
        if "durable_generation" not in wcs_columns:
            conn.execute(text("ALTER TABLE workspace_cache_state ADD COLUMN durable_generation INTEGER"))
    if inspector.has_table("workspace_local_copy"):
        wlc_columns = {col["name"] for col in inspector.get_columns("workspace_local_copy")}
        if "synced_generation" not in wlc_columns:
            conn.execute(text("ALTER TABLE workspace_local_copy ADD COLUMN synced_generation INTEGER"))


def _backfill_hf_credit_cap(conn):
    """One-time cap of inflated HuggingFace demo balances to the current grant floor.

    Earlier code re-topped HF users to the floor on every launch while billing never decremented
    in production, leaving balances stuck high. Runs exactly once, guarded by a marker ledger row.
    """
    cap = int(settings.HUGGINGFACE_DEMO_MIN_CREDITS)
    marker = "hf_backfill_cap_v1"
    already = conn.execute(
        select(credit_ledger.c.id).where(credit_ledger.c.reason == marker).limit(1)
    ).first()
    if already:
        return
    # The marker row requires a user_id (NOT NULL). Defer the whole backfill until at least one
    # user exists, so the cap UPDATE and the marker always commit together — otherwise on an empty
    # DB the UPDATE would run with no marker and re-run on every later startup.
    anchor = conn.execute(select(users.c.id).order_by(users.c.id.asc()).limit(1)).scalar()
    if anchor is None:
        return
    conn.execute(
        update(users)
        .where(users.c.provider == "huggingface_demo", users.c.credits > cap)
        .values(credits=cap)
    )
    conn.execute(
        credit_ledger.insert().values(
            user_id=anchor,
            delta=0,
            reason=marker,
            instance_id=None,
            created_at=utc_now(),
        )
    )


def ensure_default_image(conn):
    now = utc_now()
    exists = conn.execute(select(images.c.id).where(images.c.image == settings.DEFAULT_IMAGE)).first()
    if exists:
        return
    existing_by_name = conn.execute(select(images.c.id).where(images.c.name == "AMD OneClick Base")).first()
    if existing_by_name:
        conn.execute(
            update(images)
            .where(images.c.id == existing_by_name.id)
            .values(
                image=settings.DEFAULT_IMAGE,
                description="Default ROCm Jupyter/OpenCode image",
                enabled=True,
                sync_status="ready",
                updated_at=now,
            )
        )
        return
    conn.execute(
        images.insert().values(
            name="AMD OneClick Base",
            image=settings.DEFAULT_IMAGE,
            description="Default ROCm Jupyter/OpenCode image",
            enabled=True,
            sync_status="ready",
            created_at=now,
            updated_at=now,
        )
    )


def ensure_default_blank_template(conn):
    now = utc_now()
    exists = conn.execute(select(notebook_templates.c.id).where(notebook_templates.c.slug == "blank-opencode-workspace")).first()
    if exists:
        return
    conn.execute(
        notebook_templates.insert().values(
            title="Blank OpenCode Workspace",
            slug="blank-opencode-workspace",
            description="Start an empty JupyterLab workspace with OpenCode and your selected AMD GPU image.",
            category="Workspace",
            tags="workspace,opencode",
            image=settings.DEFAULT_IMAGE,
            repo_url="",
            branch="main",
            notebook_path="",
            cover_url="",
            enabled=True,
            sort_order=-100,
            owner_user_id=None,
            created_at=now,
            updated_at=now,
        )
    )


def get_or_create_user(provider: str, provider_id: str, email: str, name: str = "", avatar_url: str = "") -> dict:
    now = utc_now()
    try:
        with engine.begin() as conn:
            existing = conn.execute(
                select(users).where(users.c.provider == provider, users.c.provider_id == provider_id)
            ).mappings().first()
            if existing:
                conn.execute(
                    update(users)
                    .where(users.c.id == existing["id"])
                    .values(email=email, name=name, avatar_url=avatar_url, updated_at=now)
                )
                user = row_to_dict(conn.execute(select(users).where(users.c.id == existing["id"])).mappings().first())
                user["_created"] = False
                return user

            by_email = conn.execute(select(users).where(func.lower(users.c.email) == (email or "").lower())).mappings().first()
            if by_email:
                conn.execute(
                    update(users)
                    .where(users.c.id == by_email["id"])
                    .values(provider=provider, provider_id=provider_id, name=name, avatar_url=avatar_url, updated_at=now)
                )
                user = row_to_dict(conn.execute(select(users).where(users.c.id == by_email["id"])).mappings().first())
                user["_created"] = False
                return user

            signup_bonus = int(settings.SIGNUP_BONUS_CREDITS)
            result = conn.execute(
                users.insert().values(
                    provider=provider,
                    provider_id=provider_id,
                    email=email,
                    name=name,
                    avatar_url=avatar_url,
                    credits=signup_bonus,
                    created_at=now,
                    updated_at=now,
                )
            )
            user_id = result.inserted_primary_key[0]
            if signup_bonus:
                conn.execute(
                    credit_ledger.insert().values(user_id=user_id, delta=signup_bonus, reason="signup_bonus", created_at=now)
                )
            user = row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first())
            user["_created"] = True
            return user
    except IntegrityError:
        with engine.begin() as conn:
            provider_match = conn.execute(
                select(users).where(users.c.provider == provider, users.c.provider_id == provider_id)
            ).mappings().first()
            if provider_match:
                user = row_to_dict(provider_match)
                user["_created"] = False
                return user
            email_match = conn.execute(select(users).where(func.lower(users.c.email) == (email or "").lower())).mappings().first()
            if not email_match:
                raise
            # Mirror the normal by_email branch: link the provider the user just
            # authenticated with instead of returning stale provider metadata.
            conn.execute(
                update(users)
                .where(users.c.id == email_match["id"])
                .values(provider=provider, provider_id=provider_id, name=name, avatar_url=avatar_url, updated_at=now)
            )
            user = row_to_dict(conn.execute(select(users).where(users.c.id == email_match["id"])).mappings().first())
            user["_created"] = False
            return user


def get_user_by_email(email: str) -> Optional[dict]:
    # Case-insensitive: email login normalizes addresses to lowercase while OAuth
    # providers may return mixed case. Matching on lower(email) avoids missing an
    # existing account, which would otherwise skip linking and create a duplicate.
    with engine.begin() as conn:
        return row_to_dict(
            conn.execute(select(users).where(func.lower(users.c.email) == (email or "").lower())).mappings().first()
        )


def get_user_by_provider(provider: str, provider_id: str) -> Optional[dict]:
    with engine.begin() as conn:
        return row_to_dict(
            conn.execute(select(users).where(users.c.provider == provider, users.c.provider_id == provider_id))
            .mappings()
            .first()
        )


def get_or_create_external_user(
    provider: str,
    provider_id: str,
    email: str,
    name: str = "",
    avatar_url: str = "",
    initial_credits: int = 0,
) -> dict:
    """Create an API-backed user without relinking an existing email-owned OAuth user."""
    now = utc_now()
    try:
        with engine.begin() as conn:
            existing = conn.execute(
                select(users).where(users.c.provider == provider, users.c.provider_id == provider_id)
            ).mappings().first()
            if existing:
                conn.execute(
                    update(users)
                    .where(users.c.id == existing["id"])
                    .values(email=email, name=name, avatar_url=avatar_url, updated_at=now)
                )
                user = row_to_dict(conn.execute(select(users).where(users.c.id == existing["id"])).mappings().first())
                user["_created"] = False
                return user

            result = conn.execute(
                users.insert().values(
                    provider=provider,
                    provider_id=provider_id,
                    email=email,
                    name=name,
                    avatar_url=avatar_url,
                    credits=initial_credits,
                    created_at=now,
                    updated_at=now,
                )
            )
            user_id = result.inserted_primary_key[0]
            user = row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first())
            user["_created"] = True
            return user
    except IntegrityError:
        user = get_user_by_provider(provider, provider_id)
        if user:
            user["_created"] = False
            return user
        raise


def get_user(user_id: int) -> Optional[dict]:
    with engine.begin() as conn:
        return row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first())


def get_user_token_version(user_id: int) -> Optional[int]:
    with engine.begin() as conn:
        row = conn.execute(select(users.c.token_version).where(users.c.id == user_id)).first()
        return int(row[0]) if row else None


def bump_user_token_version(user_id: int) -> Optional[int]:
    """Invalidate all outstanding sessions for a user (admin revocation)."""
    now = utc_now()
    with engine.begin() as conn:
        user = conn.execute(select(users.c.token_version).where(users.c.id == user_id)).first()
        if not user:
            return None
        new_version = int(user[0]) + 1
        conn.execute(update(users).where(users.c.id == user_id).values(token_version=new_version, updated_at=now))
        return new_version


def ensure_user_min_credits(user_id: int, minimum_credits: int) -> Optional[dict]:
    with engine.begin() as conn:
        user = conn.execute(select(users).where(users.c.id == user_id)).mappings().first()
        if not user:
            return None
        if int(user["credits"]) < minimum_credits:
            conn.execute(update(users).where(users.c.id == user_id).values(credits=minimum_credits, updated_at=utc_now()))
        return row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first())


def grant_initial_credits_once(user_id: int, amount: int, marker: str) -> Optional[dict]:
    """Grant a starting credit floor exactly once per user, tracked by a ledger marker.

    Unlike a _created flag, this survives the get_or_create race: if two concurrent first-launches
    hit the same brand-new user, only one writes the marker (and the grant), the other is a no-op.
    It also never re-tops after the user legitimately spends down, because the marker persists.
    """
    with engine.begin() as conn:
        user = conn.execute(select(users).where(users.c.id == user_id)).mappings().first()
        if not user:
            return None
        already = conn.execute(
            select(credit_ledger.c.id).where(
                credit_ledger.c.user_id == user_id, credit_ledger.c.reason == marker
            ).limit(1)
        ).first()
        if already:
            return row_to_dict(user)
        now = utc_now()
        before = int(user["credits"])
        # Record the actual credit movement so the ledger reconciles with the balance. The marker
        # is the (user_id, reason) pair — its presence, not the delta, is what enforces once-only.
        delta = amount - before if before < amount else 0
        if delta:
            conn.execute(update(users).where(users.c.id == user_id).values(credits=amount, updated_at=now))
        conn.execute(
            credit_ledger.insert().values(
                user_id=user_id, delta=delta, reason=marker, instance_id=None, created_at=now,
            )
        )
        return row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first())


def set_user_unlimited(user_id: int, unlimited: bool = True) -> Optional[dict]:
    """Stick the unlimited-credits flag on a user.

    Sticky and one-directional in normal use: it does not itself change the credit balance, it
    only tells charge_usage_unit to stop decrementing it — so the balance freezes at whatever it
    is at the moment this is called (typically right after grant_initial_credits_once).
    """
    with engine.begin() as conn:
        user = conn.execute(select(users).where(users.c.id == user_id)).mappings().first()
        if not user:
            return None
        conn.execute(
            update(users).where(users.c.id == user_id)
            .values(unlimited_credits=unlimited, updated_at=utc_now())
        )
        return row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first())


def list_users() -> list[dict]:
    stmt = select(users).order_by(users.c.id.desc())
    with engine.begin() as conn:
        return [dict(r) for r in conn.execute(stmt).mappings().all()]


def set_user_editor(user_id: int, is_editor: bool) -> Optional[dict]:
    now = utc_now()
    with engine.begin() as conn:
        user = conn.execute(select(users).where(users.c.id == user_id)).mappings().first()
        if not user:
            return None
        conn.execute(update(users).where(users.c.id == user_id).values(is_editor=bool(is_editor), updated_at=now))
        return row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first())


def set_user_ssh_public_key(user_id: int, ssh_public_key: str) -> Optional[dict]:
    now = utc_now()
    value = (ssh_public_key or "").strip() or None
    with engine.begin() as conn:
        user = conn.execute(select(users).where(users.c.id == user_id)).mappings().first()
        if not user:
            return None
        conn.execute(update(users).where(users.c.id == user_id).values(ssh_public_key=value, updated_at=now))
        return row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first())


def get_admin_daily_stats() -> dict:
    def day_key(value: str) -> str:
        return datetime.fromisoformat(value).date().isoformat()

    with engine.begin() as conn:
        user_rows = conn.execute(select(users.c.created_at)).all()
        active_user_rows = conn.execute(select(instance_records.c.created_at)).all()
        launch_rows = conn.execute(select(instance_launch_events.c.created_at)).all()

    user_counts: dict[str, int] = {}
    active_user_counts: dict[str, int] = {}
    launch_counts: dict[str, int] = {}
    for (created_at,) in user_rows:
        day = day_key(created_at)
        user_counts[day] = user_counts.get(day, 0) + 1
    for (created_at,) in active_user_rows:
        day = day_key(created_at)
        active_user_counts[day] = active_user_counts.get(day, 0) + 1
    for (created_at,) in launch_rows:
        day = day_key(created_at)
        launch_counts[day] = launch_counts.get(day, 0) + 1

    days = sorted(set(user_counts) | set(active_user_counts) | set(launch_counts))
    tracking_started_at = min((created_at for (created_at,) in launch_rows), default=None)
    return {
        "days": days,
        "daily_users": [user_counts.get(day, 0) for day in days],
        "daily_instance_launches": [launch_counts.get(day, 0) for day in days],
        "daily_active_users": [active_user_counts.get(day, 0) for day in days],
        # Backward-compatible aliases for older frontend code.
        "daily_instances": [launch_counts.get(day, 0) for day in days],
        "total_users": len(user_rows),
        "total_instance_launches": len(launch_rows),
        "total_active_users": len(active_user_rows),
        "total_instances": len(launch_rows),
        "legacy_total_instance_records": len(active_user_rows),
        "tracking_started_at": tracking_started_at,
    }


def grant_user_credits(user_id: int, amount: int, reason: str = "manual admin grant") -> Optional[dict]:
    if amount <= 0:
        raise ValueError("amount must be positive")

    now = utc_now()
    reason = (reason or "manual admin grant").strip() or "manual admin grant"
    with engine.begin() as conn:
        user = conn.execute(select(users).where(users.c.id == user_id).with_for_update()).mappings().first()
        if not user:
            return None

        conn.execute(update(users).where(users.c.id == user_id).values(credits=users.c.credits + amount, updated_at=now))
        conn.execute(
            credit_ledger.insert().values(
                user_id=user_id,
                delta=amount,
                reason=reason,
                instance_id=None,
                created_at=now,
            )
        )
        return row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first())


def redeem_user_coupon(user_id: int, coupon: dict) -> dict:
    coupon_id = str(coupon.get("coupon_id") or "").strip()
    if not coupon_id:
        raise ValueError("coupon_id is required")

    card_hours = coupon.get("card_hours")
    if not isinstance(card_hours, int) or card_hours <= 0:
        raise ValueError("card_hours must be a positive integer")

    now = utc_now()
    with engine.begin() as conn:
        user = conn.execute(select(users).where(users.c.id == user_id).with_for_update()).mappings().first()
        if not user:
            raise ValueError("user not found")

        try:
            conn.execute(
                coupon_redemptions.insert().values(
                    coupon_id=coupon_id,
                    user_id=user_id,
                    external_user_id=str(coupon.get("user_id") or ""),
                    card_hours=card_hours,
                    credits=card_hours,
                    issued_at=str(coupon.get("issued_at") or ""),
                    expires_at=str(coupon.get("expires_at") or ""),
                    redeemed_at=now,
                )
            )
        except IntegrityError:
            raise ValueError("coupon has already been redeemed")

        conn.execute(update(users).where(users.c.id == user_id).values(credits=users.c.credits + card_hours, updated_at=now))
        conn.execute(
            credit_ledger.insert().values(
                user_id=user_id,
                delta=card_hours,
                reason=f"coupon redemption {coupon_id}",
                instance_id=None,
                created_at=now,
            )
        )
        return {
            "user": row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first()),
            "credits_added": card_hours,
            "coupon_id": coupon_id,
        }


def list_images(enabled_only: bool = False) -> list[dict]:
    # Catalog images are gated only on `enabled`. There is no sync-readiness gate on the launchable
    # list (the legacy IMAGE_PREPULL_ENABLED branch that filtered sync_status=="ready" was a no-op
    # in production, where prepull was off, so removing it preserves current live behavior).
    # Kubelet pulls the image on demand (IfNotPresent) when a chosen node hasn't been pre-warmed.
    stmt = select(images).order_by(images.c.id)
    if enabled_only:
        stmt = stmt.where(images.c.enabled == True)  # noqa: E712
    with engine.begin() as conn:
        return [dict(r) for r in conn.execute(stmt).mappings().all()]


def _template_row_to_dict(row) -> Optional[dict]:
    item = row_to_dict(row)
    if not item:
        return None
    tags = item.get("tags") or ""
    item["tags"] = [tag.strip() for tag in tags.split(",") if tag.strip()]
    return item


def _normalize_tags(tags) -> str:
    if isinstance(tags, list):
        return ",".join(str(tag).strip() for tag in tags if str(tag).strip())
    return str(tags or "").strip()


def _normalize_slug(slug: str, fallback: str) -> str:
    raw = (slug or fallback or "template").strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
    return normalized or "template"


def _unique_template_slug(conn, slug: str, template_id: Optional[int] = None) -> str:
    base = slug
    candidate = base
    suffix = 2
    while True:
        stmt = select(notebook_templates.c.id).where(notebook_templates.c.slug == candidate)
        if template_id:
            stmt = stmt.where(notebook_templates.c.id != template_id)
        exists = conn.execute(stmt).first()
        if not exists:
            return candidate
        candidate = f"{base}-{suffix}"
        suffix += 1


def list_notebook_templates(enabled_only: bool = False, owner_user_id: Optional[int] = None) -> list[dict]:
    stmt = select(notebook_templates).order_by(notebook_templates.c.sort_order, notebook_templates.c.id.desc())
    if enabled_only:
        stmt = stmt.where(notebook_templates.c.enabled == True)  # noqa: E712
    if owner_user_id is not None:
        stmt = stmt.where(notebook_templates.c.owner_user_id == owner_user_id)
    with engine.begin() as conn:
        return [_template_row_to_dict(r) for r in conn.execute(stmt).mappings().all()]


def get_notebook_template(template_id: int, enabled_only: bool = False, owner_user_id: Optional[int] = None) -> Optional[dict]:
    stmt = select(notebook_templates).where(notebook_templates.c.id == template_id)
    if enabled_only:
        stmt = stmt.where(notebook_templates.c.enabled == True)  # noqa: E712
    if owner_user_id is not None:
        stmt = stmt.where(notebook_templates.c.owner_user_id == owner_user_id)
    with engine.begin() as conn:
        return _template_row_to_dict(conn.execute(stmt).mappings().first())


def upsert_notebook_template(
    title: str,
    slug: str,
    description: str,
    category: str,
    tags,
    image: str,
    repo_url: str,
    branch: str,
    notebook_path: str,
    cover_url: str = "",
    enabled: bool = True,
    sort_order: int = 0,
    template_id: Optional[int] = None,
    owner_user_id: Optional[int] = None,
    upsert_on_slug_conflict: bool = True,
    instance_type: Optional[str] = None,
    start_command: Optional[str] = None,
    app_port: Optional[int] = None,
    model_source: Optional[str] = None,
    ssh_enabled: Optional[bool] = None,
    use_pvc: Optional[bool] = None,
    model_mount: Optional[str] = None,
) -> dict:
    now = utc_now()
    title = title.strip()
    slug = _normalize_slug(slug, title)
    image = image.strip()
    repo_url = (repo_url or "").strip()
    branch = (branch or "main").strip()
    notebook_path = (notebook_path or "").strip().lstrip("/")
    values = dict(
        title=title,
        slug=slug,
        description=(description or "").strip(),
        category=(category or "").strip(),
        tags=_normalize_tags(tags),
        image=image,
        repo_url=repo_url,
        branch=branch,
        notebook_path=notebook_path,
        cover_url=(cover_url or "").strip(),
        enabled=enabled,
        sort_order=int(sort_order or 0),
        updated_at=now,
    )
    if instance_type is not None:
        values["instance_type"] = (instance_type or "").strip() or None
    if start_command is not None:
        values["start_command"] = (start_command or "").strip() or None
    if app_port is not None:
        values["app_port"] = int(app_port) if app_port else None
    if model_source is not None:
        values["model_source"] = (model_source or "").strip() or None
    if ssh_enabled is not None:
        values["ssh_enabled"] = bool(ssh_enabled)
    if use_pvc is not None:
        values["use_pvc"] = bool(use_pvc)
    if model_mount is not None:
        values["model_mount"] = (model_mount or "").strip().lower() or None
    if owner_user_id is not None:
        values["owner_user_id"] = owner_user_id
    if not title:
        raise ValueError("template title must not be empty")
    if not image:
        raise ValueError("template image must not be empty")
    # A notebook path requires a repo; a repo without a notebook path is allowed
    # (app types clone the repo and start the app).
    if notebook_path and not repo_url:
        raise ValueError("notebook path requires a GitHub repo URL")
    if notebook_path and not notebook_path.endswith(".ipynb"):
        raise ValueError("template notebook_path must point to an .ipynb file")

    with engine.begin() as conn:
        # Avoid relying on catching IntegrityError inside the transaction. In
        # PostgreSQL a failed insert aborts the transaction, so duplicate slugs
        # must be handled before writing.
        values["slug"] = _unique_template_slug(conn, slug, template_id)
        if template_id:
            conn.execute(update(notebook_templates).where(notebook_templates.c.id == template_id).values(**values))
            return _template_row_to_dict(conn.execute(select(notebook_templates).where(notebook_templates.c.id == template_id)).mappings().first())
        result = conn.execute(notebook_templates.insert().values(**values, created_at=now))
        new_id = result.inserted_primary_key[0]
        return _template_row_to_dict(conn.execute(select(notebook_templates).where(notebook_templates.c.id == new_id)).mappings().first())


def delete_notebook_template(template_id: int, owner_user_id: Optional[int] = None) -> bool:
    with engine.begin() as conn:
        exists_stmt = select(notebook_templates.c.id).where(notebook_templates.c.id == template_id)
        if owner_user_id is not None:
            exists_stmt = exists_stmt.where(notebook_templates.c.owner_user_id == owner_user_id)
        if not conn.execute(exists_stmt).first():
            return False
        conn.execute(template_preview_assets.delete().where(template_preview_assets.c.template_id == template_id))
        conn.execute(template_preview_cache.delete().where(template_preview_cache.c.template_id == template_id))
        stmt = notebook_templates.delete().where(notebook_templates.c.id == template_id)
        if owner_user_id is not None:
            stmt = stmt.where(notebook_templates.c.owner_user_id == owner_user_id)
        result = conn.execute(stmt)
        return result.rowcount > 0


def template_preview_fingerprint(template: dict) -> str:
    raw = "|".join(
        [
            str(template.get("repo_url") or "").strip(),
            str(template.get("branch") or "").strip(),
            str(template.get("notebook_path") or "").strip().lstrip("/"),
        ]
    )
    import hashlib

    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_template_preview_cache(template_id: int) -> Optional[dict]:
    with engine.begin() as conn:
        return row_to_dict(
            conn.execute(select(template_preview_cache).where(template_preview_cache.c.template_id == template_id))
            .mappings()
            .first()
        )


def clear_template_preview_cache(template_id: int) -> None:
    with engine.begin() as conn:
        conn.execute(template_preview_assets.delete().where(template_preview_assets.c.template_id == template_id))
        conn.execute(template_preview_cache.delete().where(template_preview_cache.c.template_id == template_id))


def ensure_template_preview_cache(template: dict, force: bool = False) -> dict:
    now = utc_now()
    fingerprint = template_preview_fingerprint(template)
    values = dict(
        template_id=template["id"],
        repo_url=template["repo_url"],
        branch=template["branch"],
        notebook_path=template["notebook_path"],
        source_fingerprint=fingerprint,
        updated_at=now,
    )
    with engine.begin() as conn:
        existing = (
            conn.execute(select(template_preview_cache).where(template_preview_cache.c.template_id == template["id"]))
            .mappings()
            .first()
        )
        if existing:
            should_resync = force or existing["source_fingerprint"] != fingerprint or existing["status"] in {"failed"}
            update_values = dict(values)
            if should_resync:
                update_values.update(status="pending", error_message=None, next_sync_at=now)
            conn.execute(
                update(template_preview_cache)
                .where(template_preview_cache.c.template_id == template["id"])
                .values(**update_values)
            )
        else:
            conn.execute(
                template_preview_cache.insert().values(
                    **values,
                    status="pending",
                    error_message=None,
                    next_sync_at=now,
                    created_at=now,
                )
            )
        return row_to_dict(
            conn.execute(select(template_preview_cache).where(template_preview_cache.c.template_id == template["id"]))
            .mappings()
            .first()
        )


def mark_template_preview_syncing(template_id: int) -> Optional[dict]:
    now = utc_now()
    with engine.begin() as conn:
        existing = (
            conn.execute(select(template_preview_cache).where(template_preview_cache.c.template_id == template_id))
            .mappings()
            .first()
        )
        if not existing:
            return None
        conn.execute(
            update(template_preview_cache)
            .where(template_preview_cache.c.template_id == template_id)
            .values(status="syncing", error_message=None, updated_at=now)
        )
        return row_to_dict(
            conn.execute(select(template_preview_cache).where(template_preview_cache.c.template_id == template_id))
            .mappings()
            .first()
        )


def update_template_preview_success(template: dict, notebook_json: str, next_sync_at: str) -> dict:
    now = utc_now()
    fingerprint = template_preview_fingerprint(template)
    with engine.begin() as conn:
        conn.execute(
            update(template_preview_cache)
            .where(template_preview_cache.c.template_id == template["id"])
            .values(
                repo_url=template["repo_url"],
                branch=template["branch"],
                notebook_path=template["notebook_path"],
                source_fingerprint=fingerprint,
                notebook_json=notebook_json,
                status="ready",
                error_message=None,
                last_synced_at=now,
                next_sync_at=next_sync_at,
                updated_at=now,
            )
        )
        return row_to_dict(
            conn.execute(select(template_preview_cache).where(template_preview_cache.c.template_id == template["id"]))
            .mappings()
            .first()
        )


def update_template_preview_success_with_assets(template: dict, notebook_json: str, assets: list[dict], next_sync_at: str) -> dict:
    now = utc_now()
    fingerprint = template_preview_fingerprint(template)
    with engine.begin() as conn:
        conn.execute(
            update(template_preview_cache)
            .where(template_preview_cache.c.template_id == template["id"])
            .values(
                repo_url=template["repo_url"],
                branch=template["branch"],
                notebook_path=template["notebook_path"],
                source_fingerprint=fingerprint,
                notebook_json=notebook_json,
                status="ready",
                error_message=None,
                last_synced_at=now,
                next_sync_at=next_sync_at,
                updated_at=now,
            )
        )
        conn.execute(template_preview_assets.delete().where(template_preview_assets.c.template_id == template["id"]))
        for asset in assets:
            conn.execute(
                template_preview_assets.insert().values(
                    template_id=template["id"],
                    asset_path=asset["asset_path"],
                    content_type=asset.get("content_type") or "application/octet-stream",
                    content=asset["content"],
                    status=asset.get("status") or "ready",
                    created_at=now,
                    updated_at=now,
                )
            )
        return row_to_dict(
            conn.execute(select(template_preview_cache).where(template_preview_cache.c.template_id == template["id"]))
            .mappings()
            .first()
        )


def update_template_preview_failure(template_id: int, error_message: str, next_sync_at: str) -> Optional[dict]:
    now = utc_now()
    with engine.begin() as conn:
        existing = (
            conn.execute(select(template_preview_cache).where(template_preview_cache.c.template_id == template_id))
            .mappings()
            .first()
        )
        if not existing:
            return None
        status = "stale" if existing.get("notebook_json") else "failed"
        conn.execute(
            update(template_preview_cache)
            .where(template_preview_cache.c.template_id == template_id)
            .values(status=status, error_message=error_message[:4000], next_sync_at=next_sync_at, updated_at=now)
        )
        return row_to_dict(
            conn.execute(select(template_preview_cache).where(template_preview_cache.c.template_id == template_id))
            .mappings()
            .first()
        )


def list_template_preview_sync_candidates(now_iso: str, limit: int = 10) -> list[dict]:
    stmt = select(template_preview_cache).order_by(template_preview_cache.c.updated_at).limit(max(limit * 3, limit))
    with engine.begin() as conn:
        rows = [dict(r) for r in conn.execute(stmt).mappings().all()]
    candidates = [
        row for row in rows
        if row["status"] in {"pending", "failed", "stale"} or (row.get("next_sync_at") and row["next_sync_at"] <= now_iso)
    ]
    return candidates[:limit]


def replace_template_preview_assets(template_id: int, assets: list[dict]) -> None:
    now = utc_now()
    with engine.begin() as conn:
        conn.execute(template_preview_assets.delete().where(template_preview_assets.c.template_id == template_id))
        for asset in assets:
            conn.execute(
                template_preview_assets.insert().values(
                    template_id=template_id,
                    asset_path=asset["asset_path"],
                    content_type=asset.get("content_type") or "application/octet-stream",
                    content=asset["content"],
                    status=asset.get("status") or "ready",
                    created_at=now,
                    updated_at=now,
                )
            )


def get_template_preview_asset(template_id: int, asset_path: str) -> Optional[dict]:
    clean_path = asset_path.strip().lstrip("/")
    with engine.begin() as conn:
        return row_to_dict(
            conn.execute(
                select(template_preview_assets).where(
                    template_preview_assets.c.template_id == template_id,
                    template_preview_assets.c.asset_path == clean_path,
                )
            )
            .mappings()
            .first()
        )


def upsert_image(name: str, image: str, description: str = "", enabled: bool = True, image_id: Optional[int] = None,
                 source_type: Optional[str] = None, source_ref: Optional[str] = None) -> dict:
    name = name.strip()
    image = image.strip()
    description = (description or "").strip()
    if not name:
        raise ValueError("image name must not be empty")
    if not image:
        raise ValueError("image must not be empty")

    now = utc_now()
    with engine.begin() as conn:
        values = dict(
            name=name,
            image=image,
            description=description,
            enabled=enabled,
            sync_status="pending",
            desired_count=0,
            ready_count=0,
            sync_message=None,
            sync_started_at=None,
            sync_completed_at=None,
            updated_at=now,
        )
        # Only write the source columns when provided so legacy positional callers leave the
        # server_default (and any existing value) untouched.
        if source_type is not None:
            values["source_type"] = source_type
        if source_ref is not None:
            values["source_ref"] = source_ref
        if image_id:
            conn.execute(update(images).where(images.c.id == image_id).values(**values))
            return row_to_dict(conn.execute(select(images).where(images.c.id == image_id)).mappings().first())
        # Wrap the INSERT in a SAVEPOINT: on PostgreSQL an IntegrityError aborts the WHOLE
        # transaction, so the recovery SELECT/UPDATE below would raise InFailedSqlTransaction (which
        # is exactly what surfaced as a 500 when an admin re-added an existing image ref/name). A
        # nested transaction rolls back only the failed INSERT and leaves the outer tx usable, so a
        # duplicate ref/name cleanly UPDATES the existing row (upsert) instead of erroring.
        try:
            with conn.begin_nested():
                result = conn.execute(images.insert().values(**values, created_at=now))
                new_id = result.inserted_primary_key[0]
        except IntegrityError:
            existing = conn.execute(select(images).where(images.c.image == image)).mappings().first()
            if not existing:
                existing = conn.execute(select(images).where(images.c.name == name)).mappings().first()
            if not existing:
                raise
            conn.execute(update(images).where(images.c.id == existing["id"]).values(**values))
            new_id = existing["id"]
        return row_to_dict(conn.execute(select(images).where(images.c.id == new_id)).mappings().first())


def update_image_sync_status(
    image_id: int,
    status: str,
    desired_count: int = 0,
    ready_count: int = 0,
    message: str = "",
    completed: bool = False,
) -> Optional[dict]:
    now = utc_now()
    with engine.begin() as conn:
        current = conn.execute(select(images).where(images.c.id == image_id)).mappings().first()
        if not current:
            return None
        conn.execute(
            update(images)
            .where(images.c.id == image_id)
            .values(
                sync_status=status,
                desired_count=desired_count,
                ready_count=ready_count,
                sync_message=message,
                sync_started_at=current.get("sync_started_at") or now,
                sync_completed_at=now if completed else current.get("sync_completed_at"),
                updated_at=now,
            )
        )
        return row_to_dict(conn.execute(select(images).where(images.c.id == image_id)).mappings().first())


def set_image_acr_backup(image_id: int, ref: Optional[str], status: str) -> Optional[dict]:
    """Record the ACR Enterprise backup ref/status for a catalog image (guarded UPDATE)."""
    now = utc_now()
    with engine.begin() as conn:
        current = conn.execute(select(images.c.id).where(images.c.id == image_id)).first()
        if not current:
            return None
        conn.execute(
            update(images)
            .where(images.c.id == image_id)
            .values(acr_backup_ref=ref, acr_backup_status=status, updated_at=now)
        )
        return row_to_dict(conn.execute(select(images).where(images.c.id == image_id)).mappings().first())


def delete_image(image_id: int) -> bool:
    with engine.begin() as conn:
        result = conn.execute(images.delete().where(images.c.id == image_id))
        return result.rowcount > 0


def get_image_by_value(image: str) -> Optional[dict]:
    with engine.begin() as conn:
        return row_to_dict(
            conn.execute(select(images).where(images.c.image == image, images.c.enabled == True)).mappings().first()  # noqa: E712
        )


def image_row_exists(image_id: int) -> bool:
    """True if a catalog row with this id still exists (regardless of enabled state).

    Used by the preheat path to re-check, under lock, that an image was not concurrently deleted
    before (re)creating its DaemonSet — closing the delete/reconcile resurrection race."""
    with engine.begin() as conn:
        return conn.execute(
            select(images.c.id).where(images.c.id == image_id)
        ).first() is not None


def resolve_enabled_image(value: str) -> Optional[dict]:
    """Resolve an enabled catalog image by its full ref OR its admin-panel name.

    Lets API callers pass the friendly name (e.g. "Huggingface") instead of the full
    registry ref. Ref match takes priority; name match is case-insensitive. Returns the
    catalog row (with the real `image` ref) or None if no enabled image matches.
    """
    candidate = (value or "").strip()
    if not candidate:
        return None
    with engine.begin() as conn:
        by_ref = conn.execute(
            select(images).where(images.c.image == candidate, images.c.enabled == True)  # noqa: E712
        ).mappings().first()
        if by_ref:
            return row_to_dict(by_ref)
        # Fall back to a case-insensitive name match (admin-panel friendly name). Order by id so
        # the result is deterministic if two enabled rows ever share a case-folded name (the
        # unique-name constraint is case-sensitive, so "Huggingface"/"huggingface" could coexist).
        rows = conn.execute(
            select(images).where(images.c.enabled == True).order_by(images.c.id)  # noqa: E712
        ).mappings().all()
        target = candidate.lower()
        for r in rows:
            if (r["name"] or "").strip().lower() == target:
                return row_to_dict(r)
        return None


# =============================================================================
# Custom images (per-user) + build queue
# =============================================================================

def list_custom_images(user_id: int) -> list[dict]:
    stmt = select(custom_images).where(custom_images.c.user_id == user_id).order_by(custom_images.c.id.desc())
    with engine.begin() as conn:
        return [dict(r) for r in conn.execute(stmt).mappings().all()]


def get_custom_image(image_id: int, user_id: Optional[int] = None) -> Optional[dict]:
    stmt = select(custom_images).where(custom_images.c.id == image_id)
    if user_id is not None:
        stmt = stmt.where(custom_images.c.user_id == user_id)
    with engine.begin() as conn:
        return row_to_dict(conn.execute(stmt).mappings().first())


def count_active_custom_images(user_id: int) -> int:
    with engine.begin() as conn:
        rows = conn.execute(
            select(custom_images.c.id).where(
                custom_images.c.user_id == user_id,
                custom_images.c.build_status.in_(CUSTOM_IMAGE_ACTIVE_STATUSES),
            )
        ).all()
        return len(rows)


def create_custom_image(user_id: int, name: str, image: str, dockerfile: str, max_per_user: int) -> dict:
    """Enqueue a custom image build. Enforces the per-user cap and unique name.

    Raises ValueError on cap exceeded or duplicate name.
    """
    name = name.strip()
    image = image.strip()
    now = utc_now()
    with _custom_image_cap_lock, engine.begin() as conn:
        # On Postgres, also take a transaction-scoped advisory lock keyed on the user so
        # concurrent workers (separate processes, unaffected by the in-process lock above)
        # serialize on the same count-and-insert. Released automatically at txn end.
        if conn.dialect.name == "postgresql":
            conn.execute(
                text("SELECT pg_advisory_xact_lock(:k1, :k2)"),
                {"k1": _CUSTOM_IMAGE_CAP_LOCK_NS, "k2": int(user_id)},
            )
        active = conn.execute(
            select(custom_images.c.id).where(
                custom_images.c.user_id == user_id,
                custom_images.c.build_status.in_(CUSTOM_IMAGE_ACTIVE_STATUSES),
            )
        ).all()
        if len(active) >= max_per_user:
            raise ValueError(f"You can have at most {max_per_user} custom images. Delete one first.")
        existing = conn.execute(
            select(custom_images.c.id).where(
                custom_images.c.user_id == user_id,
                custom_images.c.name == name,
            )
        ).first()
        if existing:
            raise ValueError(f"You already have a custom image named '{name}'.")
        try:
            result = conn.execute(
                custom_images.insert().values(
                    user_id=user_id,
                    name=name,
                    image=image,
                    dockerfile=dockerfile,
                    build_status="pending",
                    build_log="",
                    claimed_by=None,
                    claimed_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )
        except IntegrityError:
            raise ValueError(f"You already have a custom image named '{name}'.")
        new_id = result.inserted_primary_key[0]
        return row_to_dict(conn.execute(select(custom_images).where(custom_images.c.id == new_id)).mappings().first())


def claim_next_build(agent_id: str) -> Optional[dict]:
    """Atomically lease the oldest pending build for the agent.

    Each iteration finds the oldest pending row and tries a conditional claim. The claim
    only succeeds if the row is still "pending" — under concurrent agents (PostgreSQL READ
    COMMITTED) another agent may have claimed it after our SELECT, in which case rowcount==0
    and we retry the *next* pending row rather than returning None while claimable work
    remains. Returns None only when no pending build is left.
    """
    with engine.begin() as conn:
        # Walk pending rows in id order, skipping any we lose the claim race for. We track
        # already-tried ids so a row another agent grabbed (still momentarily visible as the
        # "oldest pending" in a snapshot) cannot make us loop forever; the not-in filter
        # advances us to genuinely different candidates each iteration.
        tried: list[int] = []
        while True:
            query = select(custom_images).where(custom_images.c.build_status == "pending")
            if tried:
                query = query.where(custom_images.c.id.notin_(tried))
            pending = conn.execute(query.order_by(custom_images.c.id).limit(1)).mappings().first()
            if not pending:
                return None
            tried.append(pending["id"])
            now = utc_now()
            result = conn.execute(
                update(custom_images)
                .where(custom_images.c.id == pending["id"], custom_images.c.build_status == "pending")
                .values(build_status="building", claimed_by=agent_id, claimed_at=now, updated_at=now)
            )
            if result.rowcount == 0:
                continue  # lost the race for this row; try the next pending one
            return row_to_dict(conn.execute(select(custom_images).where(custom_images.c.id == pending["id"])).mappings().first())


def append_custom_image_log(image_id: int, chunk: str, agent_id: Optional[str] = None) -> bool:
    """Append a log chunk to a build. Returns True if applied.

    When agent_id is given, the write only applies to a build currently in 'building'
    state and claimed by that same agent, so a token-holder cannot scribble on builds it
    did not lease or on already-finished builds.
    """
    if not chunk:
        return False
    now = utc_now()
    with engine.begin() as conn:
        row = conn.execute(
            select(custom_images.c.build_log, custom_images.c.build_status, custom_images.c.claimed_by)
            .where(custom_images.c.id == image_id)
        ).first()
        if not row:
            return False
        if agent_id is not None and (row[1] != "building" or row[2] != agent_id):
            return False
        combined = (row[0] or "") + chunk
        if len(combined) > CUSTOM_IMAGE_LOG_MAX_CHARS:
            combined = combined[-CUSTOM_IMAGE_LOG_MAX_CHARS:]
        # Re-assert the ownership guard in the WHERE clause so the write cannot land on a row
        # that another transaction transitioned out of "building" after our SELECT (PostgreSQL
        # READ COMMITTED). rowcount==0 => the build was finalized/reclaimed concurrently.
        conditions = [custom_images.c.id == image_id]
        if agent_id is not None:
            conditions.append(custom_images.c.build_status == "building")
            conditions.append(custom_images.c.claimed_by == agent_id)
        result = conn.execute(
            update(custom_images).where(*conditions).values(build_log=combined, updated_at=now)
        )
        return result.rowcount > 0


def update_custom_image_status(image_id: int, status: Optional[str] = None, image: Optional[str] = None,
                               require_claimed_by: Optional[str] = None) -> Optional[dict]:
    """Update a build's status/image. Returns the updated row, or None if not applied.

    When require_claimed_by is given, the update only applies to a build currently in
    'building' state and claimed by that agent — preventing a token-holder from driving a
    build it never leased (e.g. flipping a never-claimed 'pending' image to 'ready').
    """
    now = utc_now()
    values = {"updated_at": now}
    if status is not None:
        values["build_status"] = status
    if image is not None:
        values["image"] = image
    with engine.begin() as conn:
        # Push the ownership guard into the UPDATE's WHERE clause so the check and the write
        # are a single atomic statement. A SELECT-then-UPDATE would race under PostgreSQL
        # READ COMMITTED (e.g. the stale-build reaper flipping status between the two), letting
        # an update slip past the guard. rowcount==0 means the guard (or the row) did not match.
        conditions = [custom_images.c.id == image_id]
        if require_claimed_by is not None:
            conditions.append(custom_images.c.build_status == "building")
            conditions.append(custom_images.c.claimed_by == require_claimed_by)
        result = conn.execute(update(custom_images).where(*conditions).values(**values))
        if result.rowcount == 0:
            return None
        return row_to_dict(conn.execute(select(custom_images).where(custom_images.c.id == image_id)).mappings().first())


def delete_custom_image(image_id: int, user_id: int) -> Optional[dict]:
    """Delete a user's custom image; returns the deleted row (for cleanup) or None.

    Refuses to delete a build that is still pending or building: the image tag is mutable and
    reused (user-{id}:{name}), so deleting an in-flight build lets the user recreate the same
    name while the old agent is still running and later docker-pushes stale content onto the
    new tag — leaving a fresh-looking 'ready' row pointing at the wrong image. The build must
    reach a terminal state (ready/failed, incl. the stale-build reaper) before it can be
    removed. Raises ValueError if the build is in-flight.
    """
    with engine.begin() as conn:
        row = conn.execute(
            select(custom_images).where(custom_images.c.id == image_id, custom_images.c.user_id == user_id)
        ).mappings().first()
        if not row:
            return None
        if row["build_status"] in ("pending", "building"):
            raise ValueError("Cannot delete a build that is still pending or building; wait for it to finish or fail")
        # Guard the DELETE on the same non-in-flight statuses so a build that gets claimed
        # between the SELECT and the DELETE (PostgreSQL READ COMMITTED) is not removed mid-flight.
        result = conn.execute(
            custom_images.delete().where(
                custom_images.c.id == image_id,
                custom_images.c.user_id == user_id,
                custom_images.c.build_status.notin_(("pending", "building")),
            )
        )
        if result.rowcount == 0:
            raise ValueError("Cannot delete a build that is still pending or building; wait for it to finish or fail")
        return dict(row)


def get_ready_custom_image_by_value(user_id: int, image: str) -> Optional[dict]:
    """Resolve a user-owned, build-ready custom image by its full image string."""
    with engine.begin() as conn:
        return row_to_dict(
            conn.execute(
                select(custom_images).where(
                    custom_images.c.user_id == user_id,
                    custom_images.c.image == image,
                    custom_images.c.build_status == "ready",
                )
            ).mappings().first()
        )


def get_custom_image_by_value(user_id: int, image: str) -> Optional[dict]:
    """Resolve a user-owned custom image by its full image string, in any build state."""
    with engine.begin() as conn:
        return row_to_dict(
            conn.execute(
                select(custom_images).where(
                    custom_images.c.user_id == user_id,
                    custom_images.c.image == image,
                )
            ).mappings().first()
        )


def mark_custom_image_launched(image_id: int) -> None:
    """Stamp launch time so disk-pressure GC can evict the coldest node-local images first."""
    now = utc_now()
    with engine.begin() as conn:
        conn.execute(
            update(custom_images)
            .where(custom_images.c.id == image_id)
            .values(last_launched_at=now, updated_at=now)
        )


def requeue_custom_image_build(image_id: int, user_id: int) -> Optional[dict]:
    """Re-enqueue an evicted or failed custom image from its stored Dockerfile."""
    now = utc_now()
    with engine.begin() as conn:
        row = conn.execute(
            select(custom_images).where(
                custom_images.c.id == image_id,
                custom_images.c.user_id == user_id,
            )
        ).mappings().first()
        if not row:
            return None
        if row["build_status"] in ("evicted", "failed"):
            conn.execute(
                update(custom_images)
                .where(
                    custom_images.c.id == image_id,
                    custom_images.c.build_status.in_(("evicted", "failed")),
                )
                .values(build_status="pending", claimed_by=None, claimed_at=None, build_log="", updated_at=now)
            )
            return row_to_dict(
                conn.execute(select(custom_images).where(custom_images.c.id == image_id)).mappings().first()
            )
        return dict(row)


def reap_stale_builds(timeout_seconds: int) -> int:
    """Fail builds whose lease is older than timeout_seconds. Returns count reaped."""
    now = datetime.now(timezone.utc)
    reaped = 0
    with engine.begin() as conn:
        building = conn.execute(
            select(custom_images.c.id, custom_images.c.claimed_at).where(custom_images.c.build_status == "building")
        ).all()
        for row in building:
            claimed_at = row[1]
            stale = True
            if claimed_at:
                try:
                    stale = (now - datetime.fromisoformat(claimed_at)).total_seconds() > timeout_seconds
                except ValueError:
                    stale = True
            if stale:
                # Guard on build_status=="building" in the WHERE clause: if the agent finalized
                # this build (ready/failed) between our SELECT and this UPDATE (PostgreSQL READ
                # COMMITTED), rowcount==0 and we must not clobber the agent's terminal status.
                result = conn.execute(
                    update(custom_images)
                    .where(custom_images.c.id == row[0], custom_images.c.build_status == "building")
                    .values(build_status="failed", updated_at=utc_now())
                )
                if result.rowcount > 0:
                    reaped += 1
    return reaped


# =============================================================================
# Image node tracking
# =============================================================================

def upsert_image_node(node_name: str, image_ref: str, status: str = "loaded", size_bytes: Optional[int] = None) -> dict:
    """Record that image_ref is loaded on node_name, upserting on (image_ref, node_name)."""
    now = utc_now()
    with engine.begin() as conn:
        values = dict(
            status=status,
            size_bytes=size_bytes,
            loaded_at=now,
            last_seen_at=now,
            updated_at=now,
        )
        # A genuine successful (re)load proves containerd on this node is healthy again, so clear any
        # quarantine on it — otherwise a recovered node keeps being routed around until the wall-clock
        # NODE_QUARANTINE_SECONDS window expires. Clearing is node-wide (quarantine is node-wide).
        if status == "loaded":
            conn.execute(
                update(image_nodes)
                .where(image_nodes.c.node_name == node_name)
                .values(quarantined_until=None, updated_at=now)
            )
            values["quarantined_until"] = None
        try:
            with conn.begin_nested():
                result = conn.execute(
                    image_nodes.insert().values(
                        image_ref=image_ref,
                        node_name=node_name,
                        created_at=now,
                        **values,
                    )
                )
            new_id = result.inserted_primary_key[0]
        except IntegrityError:
            conn.execute(
                update(image_nodes)
                .where(image_nodes.c.image_ref == image_ref, image_nodes.c.node_name == node_name)
                .values(**values)
            )
            existing = conn.execute(
                select(image_nodes.c.id).where(
                    image_nodes.c.image_ref == image_ref,
                    image_nodes.c.node_name == node_name,
                )
            ).first()
            new_id = existing[0]
        return row_to_dict(conn.execute(select(image_nodes).where(image_nodes.c.id == new_id)).mappings().first())


def set_image_node_status(node_name: str, image_ref: str, status: str, size_bytes: Optional[int] = None) -> dict:
    """Set the (image_ref, node_name) row's status, upserting the row if it does not exist yet.

    Used to record `importing` (in-flight distribute) and `quarantined` (wedged node) states so a
    hung import is distinguishable from "never started". Only `loaded` counts as available elsewhere.

    `importing` is a NON-DOWNGRADING marker: it must never overwrite an existing `loaded` row. A
    re-distribute of an already-loaded image marks the node `importing` before streaming; if that
    import then FAILS the row would be left stuck `importing` and the image — still physically on the
    node — would read as unavailable (0/N). So an `importing` write is applied only to rows that are
    not already `loaded`; a genuine success still flips the row to `loaded` via upsert_image_node.
    """
    now = utc_now()
    with engine.begin() as conn:
        values = dict(status=status, last_seen_at=now, updated_at=now)
        if status == "loaded":
            values["loaded_at"] = now
        if size_bytes is not None:
            values["size_bytes"] = size_bytes
        # Existence-check then UPDATE-or-INSERT. We avoid catching IntegrityError inside the txn: on
        # PostgreSQL a failed INSERT aborts the whole transaction, so a fallback statement on the
        # same connection would raise InFailedSqlTransaction (see notebook-template note above).
        where = [image_nodes.c.image_ref == image_ref, image_nodes.c.node_name == node_name]
        if status == "importing":
            # Do not clobber a known-good loaded row with a transient importing marker.
            where.append(image_nodes.c.status != "loaded")
        result = conn.execute(
            update(image_nodes)
            .where(*where)
            .values(**values)
        )
        if status == "importing" and result.rowcount == 0:
            # Either the row is already loaded (leave it) or it does not exist yet. Distinguish: only
            # insert a fresh importing row when no row exists at all.
            exists = conn.execute(
                select(image_nodes.c.status).where(
                    image_nodes.c.image_ref == image_ref,
                    image_nodes.c.node_name == node_name,
                )
            ).first()
            if exists is not None:
                # Row exists and is loaded -> intentionally leave it loaded; return it as-is.
                row = conn.execute(
                    select(image_nodes).where(
                        image_nodes.c.image_ref == image_ref,
                        image_nodes.c.node_name == node_name,
                    )
                ).mappings().first()
                return row_to_dict(row)
        if result.rowcount == 0:
            # Insert in a SAVEPOINT so a concurrent inserter winning the UNIQUE race doesn't poison
            # the outer txn; on conflict fall back to UPDATE.
            try:
                with conn.begin_nested():
                    conn.execute(
                        image_nodes.insert().values(
                            image_ref=image_ref,
                            node_name=node_name,
                            created_at=now,
                            **values,
                        )
                    )
            except IntegrityError:
                conn.execute(
                    update(image_nodes)
                    .where(image_nodes.c.image_ref == image_ref, image_nodes.c.node_name == node_name)
                    .values(**values)
                )
        row = conn.execute(
            select(image_nodes).where(
                image_nodes.c.image_ref == image_ref,
                image_nodes.c.node_name == node_name,
            )
        ).mappings().first()
        return row_to_dict(row)


# Sentinel image_ref for a node-level quarantine anchor row when the wedge happened on a node with
# no prior image_nodes rows (a first-import wedge). status='quarantined' keeps it out of the
# loaded/list views; it exists only so quarantined_nodes() reports the node.
_QUARANTINE_ANCHOR_REF = "__node_quarantine__"


def quarantine_node(node_name: str, image_ref: str, seconds: int) -> None:
    """Quarantine a node for `seconds`: stamp quarantined_until across ALL of its image_nodes rows.

    Quarantine is a node-wide fact (containerd wedged), so it is applied to every row the node has.
    If the node has NO rows yet (first-import wedge), an anchor row is inserted under a sentinel ref
    so the quarantine is never silently lost — even when the caller passes an empty image_ref.
    """
    until = (datetime.now(timezone.utc) + timedelta(seconds=int(seconds))).isoformat()
    now = utc_now()
    anchor_ref = image_ref or _QUARANTINE_ANCHOR_REF
    with engine.begin() as conn:
        updated = conn.execute(
            update(image_nodes)
            .where(image_nodes.c.node_name == node_name)
            .values(quarantined_until=until, updated_at=now)
        )
        if updated.rowcount == 0:
            # No existing rows for this node — insert an anchor (SAVEPOINT-guarded against a racing
            # inserter; on conflict the other writer created rows, so re-stamp them).
            try:
                with conn.begin_nested():
                    conn.execute(
                        image_nodes.insert().values(
                            image_ref=anchor_ref,
                            node_name=node_name,
                            status="quarantined",
                            quarantined_until=until,
                            last_seen_at=now,
                            created_at=now,
                            updated_at=now,
                        )
                    )
            except IntegrityError:
                conn.execute(
                    update(image_nodes)
                    .where(image_nodes.c.node_name == node_name)
                    .values(quarantined_until=until, updated_at=now)
                )


def quarantined_nodes() -> set[str]:
    """Return the set of node names currently quarantined (quarantined_until in the future)."""
    now_iso = datetime.now(timezone.utc).isoformat()
    with engine.begin() as conn:
        rows = conn.execute(
            select(image_nodes.c.node_name).where(
                image_nodes.c.quarantined_until.isnot(None),
                image_nodes.c.quarantined_until > now_iso,
            )
        ).all()
        return {r[0] for r in rows}


def clear_importing_nodes(image_ref: str) -> int:
    """Delete rows left in the transient `importing` state for a ref (after a failed distribute).

    Only touches `importing` rows, never `loaded` ones, so a still-resident image keeps its
    availability. Returns the number of rows removed.
    """
    with engine.begin() as conn:
        result = conn.execute(
            image_nodes.delete().where(
                image_nodes.c.image_ref == image_ref,
                image_nodes.c.status == "importing",
            )
        )
        return result.rowcount


def image_loaded_on_node(image_ref: str, node_name: str) -> bool:
    with engine.begin() as conn:
        row = conn.execute(
            select(image_nodes.c.id).where(
                image_nodes.c.image_ref == image_ref,
                image_nodes.c.node_name == node_name,
                image_nodes.c.status == "loaded",
            )
        ).first()
        return row is not None


def list_nodes_for_image(image_ref: str) -> list[str]:
    with engine.begin() as conn:
        rows = conn.execute(
            select(image_nodes.c.node_name).where(
                image_nodes.c.image_ref == image_ref,
                image_nodes.c.status == "loaded",
            )
        ).all()
        return [r[0] for r in rows]


def clear_image_node(image_ref: str, node_name: str) -> bool:
    with engine.begin() as conn:
        result = conn.execute(
            image_nodes.delete().where(
                image_nodes.c.image_ref == image_ref,
                image_nodes.c.node_name == node_name,
            )
        )
        return result.rowcount > 0


def touch_image_node(image_ref: str, node_name: str) -> None:
    """Refresh last_seen_at/loaded_at so a recently-launched image isn't reaped as outdated."""
    now = utc_now()
    with engine.begin() as conn:
        conn.execute(
            update(image_nodes)
            .where(image_nodes.c.image_ref == image_ref, image_nodes.c.node_name == node_name)
            .values(last_seen_at=now, loaded_at=now, updated_at=now)
        )


def get_active_instance_for_user(user_id: int) -> Optional[dict]:
    with engine.begin() as conn:
        return row_to_dict(
            conn.execute(
                select(instance_records)
                .where(
                    instance_records.c.user_id == user_id,
                    instance_records.c.deleted_at.is_(None),
                    instance_records.c.status.in_(["pending", "running"]),
                )
                .order_by(instance_records.c.id.desc())
                .limit(1)
            ).mappings().first()
        )


def record_instance(user_id: int, email: str, instance_id: str, image: str, instance_type: str, gpu_count: int, node_port: int, opencode_node_port: Optional[int] = None, pod_type: Optional[str] = None, api_launched: bool = False):
    now = utc_now()
    billing_session_id = f"{instance_id}:{uuid.uuid4().hex[:12]}"
    with engine.begin() as conn:
        existing = conn.execute(
            select(instance_records).where(instance_records.c.instance_id == instance_id)
        ).mappings().first()
        values = dict(
            user_id=user_id,
            email=email,
            image=image,
            instance_type=instance_type,
            gpu_count=gpu_count,
            node_port=node_port,
            opencode_node_port=opencode_node_port,
            status="pending",
            created_at=now,
            last_charged_at=now,
            billing_session_id=billing_session_id,
            ready_at=None,
            billing_started_at=None,
            deleted_at=None,
            pod_type=pod_type,
            api_launched=bool(api_launched),
        )
        if existing:
            conn.execute(update(instance_records).where(instance_records.c.id == existing["id"]).values(**values))
        else:
            conn.execute(instance_records.insert().values(**values, instance_id=instance_id))


def record_instance_launch_event(
    user_id: int,
    email: str,
    instance_id: str,
    image: str,
    instance_type: str,
    gpu_count: int,
    template_id: Optional[int] = None,
    template_title: Optional[str] = None,
    pod_type: Optional[str] = None,
):
    with engine.begin() as conn:
        conn.execute(
            instance_launch_events.insert().values(
                user_id=user_id,
                email=email,
                instance_id=instance_id,
                image=image,
                instance_type=instance_type,
                gpu_count=gpu_count,
                template_id=template_id,
                template_title=template_title,
                created_at=utc_now(),
                pod_type=pod_type,
            )
        )


def mark_instance_deleting(instance_id: str):
    """Optimistically flag an instance as deleting the instant a delete is requested.

    Written synchronously before the (now non-blocking) k8s delete so the intent is durable
    even if the process dies mid-request. Distinct from 'deleted': the row is NOT stamped
    deleted_at yet (reconcile still tracks it until the pod is confirmed gone), but 'deleting'
    is excluded from get_active_instance_for_user's (pending/running) filter so the user can
    immediately launch a replacement, while get_instance_by_id still surfaces it so the status
    endpoint can show 'Terminating…'. Only transitions live rows; never resurrects a deleted one.
    """
    with engine.begin() as conn:
        conn.execute(
            update(instance_records)
            .where(
                instance_records.c.instance_id == instance_id,
                instance_records.c.deleted_at.is_(None),
                instance_records.c.status.in_(["pending", "running"]),
            )
            .values(status="deleting")
        )


def mark_instance_deleted(instance_id: str):
    with engine.begin() as conn:
        conn.execute(
            update(instance_records)
            .where(instance_records.c.instance_id == instance_id, instance_records.c.deleted_at.is_(None))
            .values(status="deleted", deleted_at=utc_now())
        )


def _upsert_workspace_cache_state(instance_id: str, values: dict):
    """Insert-or-update a workspace_cache_state row (portable across SQLite/Postgres).

    Uses try-insert / on-conflict-update rather than check-then-insert: two concurrent stampers for
    the same instance_id (e.g. a relaunch racing an idle-reap delete) would both see "no row" and
    both INSERT, and the loser's write would be lost to a swallowed IntegrityError. Catching the
    conflict and falling back to UPDATE makes the last writer win deterministically."""
    now = utc_now()
    try:
        with engine.begin() as conn:
            conn.execute(
                workspace_cache_state.insert().values(
                    instance_id=instance_id, updated_at=now, **values
                )
            )
        return
    except IntegrityError:
        pass  # row already exists — fall through to update
    with engine.begin() as conn:
        conn.execute(
            update(workspace_cache_state)
            .where(workspace_cache_state.c.instance_id == instance_id)
            .values(updated_at=now, **values)
        )


def stamp_workspace_running(instance_id: str, node_name: Optional[str] = None):
    """Mark an instance as running (clears stopped_at so the local-delete reaper won't reap it).
    node_name is usually None at create time (scheduler picks the node); the delete path records the
    actual node it ran on.

    Does NOT touch workspace_pending_flush: a pending-flush row is keyed by (instance, node) and
    fenced by the session's stopped_at token, so a fresh session on the SAME node is a distinct
    working copy and the reaper's per-(instance,node) gate already protects it; a prior session's
    unflushed copy on ANOTHER node keeps its own pending-flush row until its retry flush confirms."""
    vals = {"stopped_at": None}
    if node_name:
        vals["node_name"] = node_name
    _upsert_workspace_cache_state(instance_id, vals)


def _record_local_copy(instance_id: str, node_name: str, session_token: str):
    """Insert-or-refresh the (instance, node) local-copy row with the session's fence token and
    flushed_at reset to NULL (a new stopped session's copy starts unflushed). Called on every stop
    transition. Portable upsert (try-insert / on-conflict-update)."""
    if not node_name or not session_token:
        return
    now = utc_now()
    try:
        with engine.begin() as conn:
            conn.execute(
                workspace_local_copy.insert().values(
                    instance_id=instance_id, node_name=node_name,
                    session_token=session_token, flushed_at=None,
                    created_at=now, updated_at=now,
                )
            )
        return
    except IntegrityError:
        pass  # a copy row for this (instance, node) already exists — new session: retoken + unflush
    with engine.begin() as conn:
        conn.execute(
            update(workspace_local_copy)
            .where(
                workspace_local_copy.c.instance_id == instance_id,
                workspace_local_copy.c.node_name == node_name,
            )
            .values(session_token=session_token, flushed_at=None, updated_at=now)
        )


def stamp_workspace_stopped(instance_id: str, node_name: Optional[str]) -> Optional[str]:
    """Record the node the instance last ran on and the stop time, so a fast relaunch can soft-affine
    back to it and the delayed reaper can free the local SSD copy after the TTL. ALSO records a
    per-(instance,node) local-copy row fenced by this stop's timestamp. Returns the session_token
    (the stop timestamp) so the caller passes it to the flush and marks exactly this session's copy."""
    token = utc_now()
    _upsert_workspace_cache_state(instance_id, {"node_name": node_name, "stopped_at": token})
    if node_name:
        _record_local_copy(instance_id, node_name, token)
    return token if node_name else None


def stamp_workspace_stopped_keep_node(instance_id: str) -> Optional[str]:
    """Mark an existing workspace_cache_state row stopped WITHOUT changing node_name. For out-of-band
    pod loss (node death / kubectl delete / eviction) caught by the reconciler, where the pod is
    already gone so its node can't be read — we still want stopped_at set so the delayed-local reaper
    eventually frees the SSD copy on the node recorded at create/last-run. Also records a local-copy
    row on the recorded node so the retry sweep flushes it. No-op if no row exists. Returns the
    session token (stop ts) when a node was recorded, else None."""
    now = utc_now()
    with engine.begin() as conn:
        conn.execute(
            update(workspace_cache_state)
            .where(
                workspace_cache_state.c.instance_id == instance_id,
                workspace_cache_state.c.stopped_at.is_(None),
            )
            .values(stopped_at=now, updated_at=now)
        )
        node = conn.execute(
            select(workspace_cache_state.c.node_name).where(
                workspace_cache_state.c.instance_id == instance_id
            )
        ).scalar()
    if node:
        _record_local_copy(instance_id, node, now)
        return now
    return None


def mark_workspace_flushed(instance_id: str, node_name: str, session_token: str) -> bool:
    """Confirm a flush: set flushed_at on the (instance, node) local-copy row ONLY if its session_token
    still matches the token the flush actually flushed. DATA-LOSS-CRITICAL fence: a straggler flush pod
    that confirms AFTER the instance was re-stopped (new token + flushed_at reset) will NOT match, so
    it can never falsely certify a session it didn't flush. Returns True if a row was marked."""
    if not node_name or not session_token:
        return False
    now = utc_now()
    with engine.begin() as conn:
        res = conn.execute(
            update(workspace_local_copy)
            .where(
                workspace_local_copy.c.instance_id == instance_id,
                workspace_local_copy.c.node_name == node_name,
                workspace_local_copy.c.session_token == session_token,
            )
            .values(flushed_at=now, updated_at=now)
        )
    return (res.rowcount or 0) > 0


def get_durable_generation(instance_id: str) -> int:
    """Current per-instance durable generation Gd (NULL/absent reads as 0). Node-independent; the
    manager passes this to the hydrate/flush init so the container can compare it against its own
    on-node synced marker. Cheap PK lookup on workspace_cache_state."""
    with engine.begin() as conn:
        val = conn.execute(
            select(workspace_cache_state.c.durable_generation).where(
                workspace_cache_state.c.instance_id == instance_id
            )
        ).scalar()
    return int(val or 0)


def mark_workspace_flushed_authoritative(instance_id: str, node_name: str, session_token: str,
                                         new_generation: int) -> bool:
    """Confirm an AUTHORITATIVE flush (`rsync --delete` ran and succeeded): in ONE token-fenced
    transaction, set the copy's flushed_at + synced_generation=new_generation AND bump the instance's
    durable_generation to new_generation. DATA-LOSS-CRITICAL: this is called ONLY AFTER the in-pod
    --delete confirmed, so durable is never certified at a higher generation than it actually holds.

    Token fence (identical to mark_workspace_flushed): the copy row is updated only if its
    session_token still matches, so a straggler flush from a superseded session can't certify a newer
    one. If the fence fails (rowcount 0) the durable_generation is NOT bumped and False is returned.
    Serialized across replicas by the per-instance advisory lock the caller holds, so the
    read-modify-write of durable_generation cannot race a concurrent flush of the same instance."""
    if not node_name or not session_token:
        return False
    now = utc_now()
    with engine.begin() as conn:
        res = conn.execute(
            update(workspace_local_copy)
            .where(
                workspace_local_copy.c.instance_id == instance_id,
                workspace_local_copy.c.node_name == node_name,
                workspace_local_copy.c.session_token == session_token,
            )
            .values(flushed_at=now, synced_generation=int(new_generation), updated_at=now)
        )
        if (res.rowcount or 0) <= 0:
            return False  # stale token → do NOT bump durable_generation
        # Bump the canonical durable generation. Never regress it (a stale caller must not lower it).
        cur = conn.execute(
            select(workspace_cache_state.c.durable_generation).where(
                workspace_cache_state.c.instance_id == instance_id
            )
        ).scalar()
        target = max(int(cur or 0), int(new_generation))
        upd = conn.execute(
            update(workspace_cache_state)
            .where(workspace_cache_state.c.instance_id == instance_id)
            .values(durable_generation=target, updated_at=now)
        )
        if (upd.rowcount or 0) <= 0:
            # No cache_state row yet (e.g. the affinity-hint row was reaped) — create one so the
            # generation sticks. stopped_at = the session_token (the stop timestamp) rather than NULL:
            # this row is created during a post-stop flush, so it is NOT running.
            conn.execute(
                workspace_cache_state.insert().values(
                    instance_id=instance_id, node_name=node_name, stopped_at=session_token,
                    durable_generation=target, updated_at=now,
                )
            )
    return True


def has_newer_local_copy(instance_id: str, node_name: str, session_token: str) -> bool:
    """True if a STRICTLY-NEWER copy (larger session_token) exists for this instance on a DIFFERENT
    node — i.e. a later session genuinely superseded this one. Distinguishes a truly-stranded copy
    (a newer session ran elsewhere → safe to discard, B1) from a race-desynced LIVE copy (this is the
    latest session but its on-node generation marker was clobbered below durable_generation by a
    flush-vs-relaunch race → must NOT be discarded). session_token is the stop timestamp (ISO), so a
    lexicographic '>' is a chronological '>'."""
    lc = workspace_local_copy.c
    with engine.begin() as conn:
        row = conn.execute(
            select(lc.instance_id).where(
                lc.instance_id == instance_id,
                lc.node_name != node_name,
                lc.session_token > session_token,
            ).limit(1)
        ).first()
    return row is not None


def discard_superseded_copy(instance_id: str, node_name: str, session_token: str) -> bool:
    """Drop a SUPERSEDED stranded local-copy row (synced_generation < durable_generation) so neither
    the retry sweep nor the reaper acts on it again. Token-fenced so a re-stopped session's fresh copy
    is never dropped by a straggler. Caller frees the SSD separately. Returns True if a row was
    dropped. This is the intentional generation-wins-over-mtime data-drop of that node's un-flushed
    edits (merging them up would re-inject files a newer session deleted, poisoning durable)."""
    if not node_name or not session_token:
        return False
    with engine.begin() as conn:
        res = conn.execute(
            workspace_local_copy.delete().where(
                workspace_local_copy.c.instance_id == instance_id,
                workspace_local_copy.c.node_name == node_name,
                workspace_local_copy.c.session_token == session_token,
            )
        )
    return (res.rowcount or 0) > 0


@contextmanager
def workspace_instance_advisory_lock(instance_id: str, timeout_seconds: int = 180):
    """Cross-process/-replica per-INSTANCE lock serializing durable flushes. Two DIFFERENT-node
    unflushed copies of one instance can otherwise flush concurrently into the SAME durable subpath,
    and with `--delete` in play that interleaving is destructive. The existing per-instance in-process
    lock only covers one manager process; this Postgres session-scoped advisory lock covers all
    replicas. No-op on SQLite (single-writer; tests are single-process). Yields True if held (or
    lock-free backend), False if it could not be acquired within the timeout."""
    if engine.dialect.name != "postgresql":
        yield True
        return
    key32 = int(hashlib.md5(instance_id.encode()).hexdigest(), 16) % (2 ** 31)
    conn = engine.connect()
    acquired = False
    try:
        deadline = time.monotonic() + max(1, timeout_seconds)
        while True:
            acquired = bool(conn.execute(
                text("SELECT pg_try_advisory_lock(:ns, :k)"),
                {"ns": _WORKSPACE_FLUSH_LOCK_NS, "k": key32},
            ).scalar())
            if acquired or time.monotonic() >= deadline:
                break
            time.sleep(1)
        yield acquired
    finally:
        if acquired:
            try:
                conn.execute(
                    text("SELECT pg_advisory_unlock(:ns, :k)"),
                    {"ns": _WORKSPACE_FLUSH_LOCK_NS, "k": key32},
                )
            except Exception:
                # A session-scoped advisory lock is NOT released by returning a POOLED connection to
                # the pool — only by an explicit unlock or a real disconnect. If unlock failed, force
                # a disconnect (invalidate) so the lock can't linger on a reused pooled connection and
                # deadlock this instance's future flushes.
                try:
                    conn.invalidate()
                except Exception:
                    pass
        conn.close()


def clear_local_copy_node(instance_id: str, node_name: str):
    """Drop the local-copy row for one (instance, node) — used after the reaper frees that node's SSD
    copy (and the durable copy is current)."""
    with engine.begin() as conn:
        conn.execute(
            workspace_local_copy.delete().where(
                workspace_local_copy.c.instance_id == instance_id,
                workspace_local_copy.c.node_name == node_name,
            )
        )


def clear_all_local_copies(instance_id: str):
    """Drop every local-copy row for an instance (admin durable delete / full teardown)."""
    with engine.begin() as conn:
        conn.execute(
            workspace_local_copy.delete().where(
                workspace_local_copy.c.instance_id == instance_id
            )
        )


def list_workspace_unflushed_copies() -> list[dict]:
    """All (instance, node) copies awaiting a confirmed flush (flushed_at IS NULL). The reconciler
    retry sweep re-runs the flush for these once the node is Ready. Returns
    [{instance_id, node_name, session_token, synced_generation, durable_generation}].

    The generation context (LEFT JOINed from workspace_cache_state) lets the retry sweep observe
    whether a copy is a current descendant (synced == durable) or SUPERSEDED (synced < durable) —
    routing/observability context; the in-container flush re-derives the authoritative decision from
    the on-node marker. NULL generations read as 0 (an unmigrated/first-gen instance)."""
    lc = workspace_local_copy.c
    cs = workspace_cache_state.c
    with engine.begin() as conn:
        rows = conn.execute(
            select(
                lc.instance_id,
                lc.node_name,
                lc.session_token,
                lc.synced_generation,
                cs.durable_generation,
            ).select_from(
                workspace_local_copy.outerjoin(
                    workspace_cache_state, lc.instance_id == cs.instance_id
                )
            ).where(lc.flushed_at.is_(None))
        ).mappings().all()
    out = []
    for r in rows:
        d = dict(r)
        d["synced_generation"] = int(d.get("synced_generation") or 0)
        d["durable_generation"] = int(d.get("durable_generation") or 0)
        out.append(d)
    return out


def get_workspace_last_node(instance_id: str) -> Optional[str]:
    with engine.begin() as conn:
        row = conn.execute(
            select(workspace_cache_state.c.node_name).where(
                workspace_cache_state.c.instance_id == instance_id
            )
        ).first()
    return row[0] if row and row[0] else None


def list_workspace_cache_to_reap(ttl_minutes: int) -> list[dict]:
    """(instance, node) SSD copies safe to free: a local-copy row whose session stopped more than
    ttl_minutes ago AND flushed_at IS NOT NULL (confirmed-flushed to durable). Returns
    [{instance_id, node_name, stopped_at}] (stopped_at = the copy's session_token).

    DATA-LOSS-CRITICAL: the flushed_at gate is mandatory. The flush is decoupled from pod teardown
    (out-of-pod, manager-driven), so a copy can be past its TTL with the pod long gone yet still
    UNFLUSHED (node was NotReady at delete time). rm -rf'ing it would destroy the only current copy of
    the user's data. Only reap a (instance, node) whose flushed_at is set. Keyed per-node, so a
    cross-node relaunch never orphans the old node's copy — it is reaped once its own flush confirms."""
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=max(0, ttl_minutes))).isoformat()
    lc = workspace_local_copy.c
    with engine.begin() as conn:
        rows = conn.execute(
            select(
                lc.instance_id,
                lc.node_name,
                lc.session_token.label("stopped_at"),
            ).where(
                lc.session_token < cutoff,
                lc.flushed_at.isnot(None),
            )
        ).mappings().all()
    return [dict(r) for r in rows]


def clear_workspace_cache_state(instance_id: str):
    """Remove the row once the local copy is reaped (or on manual durable delete)."""
    with engine.begin() as conn:
        conn.execute(
            workspace_cache_state.delete().where(
                workspace_cache_state.c.instance_id == instance_id
            )
        )


def mark_instance_ready_for_billing(instance_id: str) -> Optional[dict]:
    """Transition an instance to billable running state exactly when it is truly ready."""
    now = utc_now()
    with engine.begin() as conn:
        current = conn.execute(
            select(instance_records)
            .where(instance_records.c.instance_id == instance_id, instance_records.c.deleted_at.is_(None))
            .with_for_update()
        ).mappings().first()
        if not current:
            return None
        if current["status"] != "running":
            conn.execute(
                update(instance_records)
                .where(instance_records.c.id == current["id"])
                .values(status="running", ready_at=now, billing_started_at=now, last_charged_at=now)
            )
        return row_to_dict(conn.execute(select(instance_records).where(instance_records.c.id == current["id"])).mappings().first())


def charge_user(user_id: int, amount: int, reason: str, instance_id: str):
    now = utc_now()
    with engine.begin() as conn:
        conn.execute(update(users).where(users.c.id == user_id).values(credits=users.c.credits - amount, updated_at=now))
        conn.execute(
            credit_ledger.insert().values(user_id=user_id, delta=-amount, reason=reason, instance_id=instance_id, created_at=now)
        )


def get_charged_credits_for_instance(instance_id: str, billing_session_id: Optional[str] = None) -> int:
    with engine.begin() as conn:
        if billing_session_id:
            rows = conn.execute(select(usage_charges.c.credits).where(usage_charges.c.billing_session_id == billing_session_id)).all()
        else:
            rows = conn.execute(select(usage_charges.c.credits).where(usage_charges.c.instance_id == instance_id)).all()
        return sum(int(r[0]) for r in rows)


def charge_usage_unit(user_id: int, instance_id: str, billing_session_id: str, billing_unit: int, gpu_count: int) -> str:
    """Idempotently charge one billing unit. Returns charged/existing/insufficient."""
    now = utc_now()
    credits = int(gpu_count)
    with engine.begin() as conn:
        existing = conn.execute(
            select(usage_charges.c.id).where(
                usage_charges.c.billing_session_id == billing_session_id,
                usage_charges.c.billing_unit == billing_unit,
            )
        ).first()
        if existing:
            return "existing"

        user = conn.execute(select(users).where(users.c.id == user_id).with_for_update()).mappings().first()
        if not user:
            raise ValueError(f"User {user_id} not found")

        # Unlimited users are never charged: record the usage unit for audit/telemetry (and to
        # satisfy the (billing_session_id, billing_unit) idempotency key) but skip the deduction,
        # so the balance stays frozen at whatever it was when unlimited_credits was set.
        if user["unlimited_credits"]:
            conn.execute(
                usage_charges.insert().values(
                    user_id=user_id,
                    instance_id=instance_id,
                    billing_session_id=billing_session_id,
                    billing_unit=billing_unit,
                    gpu_count=gpu_count,
                    credits=0,
                    created_at=now,
                )
            )
            conn.execute(
                credit_ledger.insert().values(
                    user_id=user_id,
                    delta=0,
                    reason=f"unlimited unit {billing_unit} x {gpu_count} GPU",
                    instance_id=instance_id,
                    created_at=now,
                )
            )
            return "charged"

        if int(user["credits"]) < credits:
            return "insufficient"

        conn.execute(
            usage_charges.insert().values(
                user_id=user_id,
                instance_id=instance_id,
                billing_session_id=billing_session_id,
                billing_unit=billing_unit,
                gpu_count=gpu_count,
                credits=credits,
                created_at=now,
            )
        )
        conn.execute(update(users).where(users.c.id == user_id).values(credits=users.c.credits - credits, updated_at=now))
        conn.execute(
            credit_ledger.insert().values(
                user_id=user_id,
                delta=-credits,
                reason=f"usage_charge unit {billing_unit} x {gpu_count} GPU",
                instance_id=instance_id,
                created_at=now,
            )
        )
        return "charged"


def update_instance_charge_time(record_id: int, charged_at: str):
    with engine.begin() as conn:
        conn.execute(update(instance_records).where(instance_records.c.id == record_id).values(last_charged_at=charged_at))


def list_active_instance_ids() -> set[str]:
    """instance_ids of all records that are not soft-deleted (any live status).

    Used by the reconciler to distinguish cluster orphans (pods with our label
    but no owning DB record) from legitimately managed instances.
    """
    stmt = select(instance_records.c.instance_id).where(instance_records.c.deleted_at.is_(None))
    with engine.begin() as conn:
        return {r[0] for r in conn.execute(stmt).all()}


def list_active_instances() -> list[dict]:
    # Billing is scoped to API-launched instances only; web/template launches are not metered.
    stmt = (
        select(instance_records, users.c.credits)
        .join(users, users.c.id == instance_records.c.user_id)
        .where(
            instance_records.c.deleted_at.is_(None),
            instance_records.c.status.in_(["pending", "running"]),
            instance_records.c.api_launched.is_(True),
        )
    )
    with engine.begin() as conn:
        return [dict(r) for r in conn.execute(stmt).mappings().all()]
