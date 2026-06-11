"""
Persistent store for users, image catalog, and credit accounting.

Uses PostgreSQL when DATABASE_URL is set; falls back to local SQLite for dev.
"""
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    inspect,
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

engine_kwargs = {"future": True, "pool_pre_ping": True}
if not DATABASE_URL.startswith("sqlite"):
    engine_kwargs.update(
        pool_size=settings.DATABASE_POOL_SIZE,
        max_overflow=settings.DATABASE_MAX_OVERFLOW,
        pool_timeout=settings.DATABASE_POOL_TIMEOUT_SECONDS,
        pool_recycle=settings.DATABASE_POOL_RECYCLE_SECONDS,
    )
engine = create_engine(DATABASE_URL, **engine_kwargs)
metadata = MetaData()

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
    Column("deleted_at", String(64)),
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
    Column("build_status", String(32), nullable=False, default="pending"),
    Column("build_log", Text),
    Column("claimed_by", String(128)),
    Column("claimed_at", String(64)),
    Column("created_at", String(64), nullable=False),
    Column("updated_at", String(64), nullable=False),
    UniqueConstraint("user_id", "name", name="uq_custom_image_user_name"),
)

# Build queue states that count against a user's quota and block re-use of a name.
CUSTOM_IMAGE_ACTIVE_STATUSES = ("pending", "building", "ready")
CUSTOM_IMAGE_LOG_MAX_CHARS = 60000


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def row_to_dict(row) -> Optional[dict]:
    return dict(row) if row else None


def _sqlite_usage_has_legacy_instance_unit_unique(conn) -> bool:
    indexes = conn.execute(text("PRAGMA index_list('usage_charges')")).mappings().all()
    for index in indexes:
        if not index.get("unique"):
            continue
        index_name = index["name"]
        columns = [
            row["name"]
            for row in conn.execute(text(f"PRAGMA index_info('{index_name}')")).mappings().all()
        ]
        if columns == ["instance_id", "billing_unit"]:
            return True
    return False


def _rebuild_sqlite_usage_charges(conn):
    conn.execute(text("ALTER TABLE usage_charges RENAME TO usage_charges_legacy"))
    metadata.create_all(bind=conn, tables=[usage_charges])
    conn.execute(
        text(
            """
            INSERT INTO usage_charges (
                id, user_id, instance_id, billing_session_id, billing_unit,
                gpu_count, credits, created_at
            )
            SELECT
                id, user_id, instance_id, billing_session_id, billing_unit,
                gpu_count, credits, created_at
            FROM usage_charges_legacy
            """
        )
    )
    conn.execute(text("DROP TABLE usage_charges_legacy"))


def init_db():
    metadata.create_all(engine)
    with engine.begin() as conn:
        ensure_schema_columns(conn)
        ensure_default_image(conn)
        ensure_default_blank_template(conn)


def ensure_schema_columns(conn):
    inspector = inspect(conn)
    user_columns = {col["name"] for col in inspector.get_columns("users")}
    if "is_editor" not in user_columns:
        conn.execute(text("ALTER TABLE users ADD COLUMN is_editor BOOLEAN NOT NULL DEFAULT FALSE"))

    instance_columns = {col["name"] for col in inspector.get_columns("instance_records")}
    if "billing_session_id" not in instance_columns:
        conn.execute(text("ALTER TABLE instance_records ADD COLUMN billing_session_id VARCHAR(255)"))
        conn.execute(text("UPDATE instance_records SET billing_session_id = instance_id WHERE billing_session_id IS NULL"))
    if "opencode_node_port" not in instance_columns:
        conn.execute(text("ALTER TABLE instance_records ADD COLUMN opencode_node_port INTEGER"))

    usage_columns = {col["name"] for col in inspector.get_columns("usage_charges")}
    if "billing_session_id" not in usage_columns:
        conn.execute(text("ALTER TABLE usage_charges ADD COLUMN billing_session_id VARCHAR(255)"))
        conn.execute(text("UPDATE usage_charges SET billing_session_id = instance_id WHERE billing_session_id IS NULL"))

    # New launches reuse the same Kubernetes instance_id, so billing idempotency must be scoped
    # to a launch session instead of the stable instance id.
    if conn.dialect.name == "postgresql":
        conn.execute(text("ALTER TABLE usage_charges DROP CONSTRAINT IF EXISTS uq_usage_charge_instance_unit"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_usage_charge_session_unit ON usage_charges (billing_session_id, billing_unit)"))
    elif conn.dialect.name == "sqlite":
        if _sqlite_usage_has_legacy_instance_unit_unique(conn):
            _rebuild_sqlite_usage_charges(conn)
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_usage_charge_session_unit ON usage_charges (billing_session_id, billing_unit)"))

    template_columns = {col["name"] for col in inspector.get_columns("notebook_templates")}
    if "owner_user_id" not in template_columns:
        conn.execute(text("ALTER TABLE notebook_templates ADD COLUMN owner_user_id INTEGER"))

    # Backward-compatible creation for databases initialized before these tables.
    metadata.create_all(bind=conn, tables=[template_preview_cache, template_preview_assets, coupon_redemptions, instance_launch_events, custom_images])


def ensure_default_image(conn):
    now = utc_now()
    exists = conn.execute(select(images.c.id).where(images.c.image == settings.DEFAULT_IMAGE)).first()
    if exists:
        return
    existing_named = conn.execute(select(images.c.id).where(images.c.name == "AMD OneClick Base")).first()
    if existing_named:
        conn.execute(
            update(images)
            .where(images.c.id == existing_named.id)
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
    exists = conn.execute(
        select(notebook_templates.c.id, notebook_templates.c.image)
        .where(notebook_templates.c.slug == "blank-opencode-workspace")
    ).first()
    if exists:
        if exists.image != settings.DEFAULT_IMAGE:
            conn.execute(
                update(notebook_templates)
                .where(notebook_templates.c.id == exists.id)
                .values(image=settings.DEFAULT_IMAGE, updated_at=now)
            )
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

        by_email = conn.execute(select(users).where(users.c.email == email)).mappings().first()
        if by_email:
            conn.execute(
                update(users)
                .where(users.c.id == by_email["id"])
                .values(provider=provider, provider_id=provider_id, name=name, avatar_url=avatar_url, updated_at=now)
            )
            user = row_to_dict(conn.execute(select(users).where(users.c.id == by_email["id"])).mappings().first())
            user["_created"] = False
            return user

        result = conn.execute(
            users.insert().values(
                provider=provider,
                provider_id=provider_id,
                email=email,
                name=name,
                avatar_url=avatar_url,
                credits=10,
                created_at=now,
                updated_at=now,
            )
        )
        user_id = result.inserted_primary_key[0]
        conn.execute(
            credit_ledger.insert().values(user_id=user_id, delta=10, reason="signup_bonus", created_at=now)
        )
        user = row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first())
        user["_created"] = True
        return user


def get_user(user_id: int) -> Optional[dict]:
    with engine.begin() as conn:
        return row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first())


def ensure_user_min_credits(user_id: int, minimum_credits: int) -> Optional[dict]:
    with engine.begin() as conn:
        user = conn.execute(select(users).where(users.c.id == user_id)).mappings().first()
        if not user:
            return None
        if int(user["credits"]) < minimum_credits:
            conn.execute(update(users).where(users.c.id == user_id).values(credits=minimum_credits, updated_at=utc_now()))
        return row_to_dict(conn.execute(select(users).where(users.c.id == user_id)).mappings().first())


def list_users() -> list[dict]:
    stmt = select(users).order_by(users.c.id.desc())
    with engine.begin() as conn:
        return [dict(r) for r in conn.execute(stmt).mappings().all()]


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
    stmt = select(images).order_by(images.c.id)
    if enabled_only:
        stmt = stmt.where(images.c.enabled == True, images.c.sync_status == "ready")  # noqa: E712
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
    if owner_user_id is not None:
        values["owner_user_id"] = owner_user_id
    if not title:
        raise ValueError("template title must not be empty")
    if not image:
        raise ValueError("template image must not be empty")
    if bool(repo_url) != bool(notebook_path):
        raise ValueError("repo_url and notebook_path must be provided together, or both left empty for an image-only template")
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


def upsert_image(name: str, image: str, description: str = "", enabled: bool = True, image_id: Optional[int] = None) -> dict:
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
        if image_id:
            conn.execute(update(images).where(images.c.id == image_id).values(**values))
            return row_to_dict(conn.execute(select(images).where(images.c.id == image_id)).mappings().first())
        try:
            result = conn.execute(images.insert().values(**values, created_at=now))
            new_id = result.inserted_primary_key[0]
        except IntegrityError:
            existing = conn.execute(select(images).where(images.c.image == image)).mappings().first()
            if not existing:
                existing = conn.execute(select(images).where(images.c.name == name)).mappings().first()
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


def delete_image(image_id: int) -> bool:
    with engine.begin() as conn:
        result = conn.execute(images.delete().where(images.c.id == image_id))
        return result.rowcount > 0


def get_image_by_value(image: str) -> Optional[dict]:
    with engine.begin() as conn:
        return row_to_dict(
            conn.execute(select(images).where(images.c.image == image, images.c.enabled == True)).mappings().first()  # noqa: E712
        )


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
    with engine.begin() as conn:
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
    """Atomically lease the oldest pending build for the agent."""
    now = utc_now()
    with engine.begin() as conn:
        pending = conn.execute(
            select(custom_images)
            .where(custom_images.c.build_status == "pending")
            .order_by(custom_images.c.id)
            .limit(1)
        ).mappings().first()
        if not pending:
            return None
        conn.execute(
            update(custom_images)
            .where(custom_images.c.id == pending["id"], custom_images.c.build_status == "pending")
            .values(build_status="building", claimed_by=agent_id, claimed_at=now, updated_at=now)
        )
        return row_to_dict(conn.execute(select(custom_images).where(custom_images.c.id == pending["id"])).mappings().first())


def append_custom_image_log(image_id: int, chunk: str) -> None:
    if not chunk:
        return
    now = utc_now()
    with engine.begin() as conn:
        current = conn.execute(select(custom_images.c.build_log).where(custom_images.c.id == image_id)).first()
        if not current:
            return
        combined = (current[0] or "") + chunk
        if len(combined) > CUSTOM_IMAGE_LOG_MAX_CHARS:
            combined = combined[-CUSTOM_IMAGE_LOG_MAX_CHARS:]
        conn.execute(
            update(custom_images).where(custom_images.c.id == image_id).values(build_log=combined, updated_at=now)
        )


def update_custom_image_status(image_id: int, status: Optional[str] = None, image: Optional[str] = None) -> Optional[dict]:
    now = utc_now()
    values = {"updated_at": now}
    if status is not None:
        values["build_status"] = status
    if image is not None:
        values["image"] = image
    with engine.begin() as conn:
        current = conn.execute(select(custom_images.c.id).where(custom_images.c.id == image_id)).first()
        if not current:
            return None
        conn.execute(update(custom_images).where(custom_images.c.id == image_id).values(**values))
        return row_to_dict(conn.execute(select(custom_images).where(custom_images.c.id == image_id)).mappings().first())


def delete_custom_image(image_id: int, user_id: int) -> Optional[dict]:
    """Delete a user's custom image; returns the deleted row (for cleanup) or None."""
    with engine.begin() as conn:
        row = conn.execute(
            select(custom_images).where(custom_images.c.id == image_id, custom_images.c.user_id == user_id)
        ).mappings().first()
        if not row:
            return None
        conn.execute(custom_images.delete().where(custom_images.c.id == image_id, custom_images.c.user_id == user_id))
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
                conn.execute(
                    update(custom_images)
                    .where(custom_images.c.id == row[0])
                    .values(build_status="failed", updated_at=utc_now())
                )
                reaped += 1
    return reaped


def get_active_instance_for_user(user_id: int) -> Optional[dict]:
    with engine.begin() as conn:
        return row_to_dict(
            conn.execute(
                select(instance_records)
                .where(
                    instance_records.c.user_id == user_id,
                    instance_records.c.deleted_at.is_(None),
                    instance_records.c.status == "running",
                )
                .order_by(instance_records.c.id.desc())
                .limit(1)
            ).mappings().first()
        )


def _record_instance_once(conn, user_id: int, email: str, instance_id: str, image: str, instance_type: str, gpu_count: int, node_port: int, opencode_node_port: Optional[int] = None) -> dict:
    now = utc_now()
    billing_session_id = f"{instance_id}:{uuid.uuid4().hex[:12]}"
    existing = conn.execute(
        select(instance_records).where(instance_records.c.instance_id == instance_id).with_for_update()
    ).mappings().first()

    if existing and existing["status"] == "running" and not existing["deleted_at"]:
        conn.execute(
            update(instance_records)
            .where(instance_records.c.id == existing["id"])
            .values(
                node_port=node_port,
                opencode_node_port=opencode_node_port,
            )
        )
        return {
            "id": existing["id"],
            "billing_session_id": existing["billing_session_id"],
            "new_session": False,
            "created": False,
        }

    values = dict(
        user_id=user_id,
        email=email,
        image=image,
        instance_type=instance_type,
        gpu_count=gpu_count,
        node_port=node_port,
        opencode_node_port=opencode_node_port,
        status="running",
        created_at=now,
        last_charged_at=now,
        billing_session_id=billing_session_id,
        deleted_at=None,
    )
    if existing:
        result = conn.execute(
            update(instance_records)
            .where(
                instance_records.c.id == existing["id"],
                instance_records.c.status != "running",
            )
            .values(**values)
        )
        if result.rowcount == 0:
            current = conn.execute(
                select(instance_records).where(instance_records.c.id == existing["id"])
            ).mappings().first()
            if current and current["status"] == "running" and not current["deleted_at"]:
                return {
                    "id": current["id"],
                    "billing_session_id": current["billing_session_id"],
                    "new_session": False,
                    "created": False,
                }
            raise RuntimeError(f"Unable to record launch for {instance_id}; instance row changed concurrently")
        return {
            "id": existing["id"],
            "billing_session_id": billing_session_id,
            "new_session": True,
            "created": False,
        }

    result = conn.execute(instance_records.insert().values(**values, instance_id=instance_id))
    return {
        "id": result.inserted_primary_key[0] if result.inserted_primary_key else None,
        "billing_session_id": billing_session_id,
        "new_session": True,
        "created": True,
    }


def record_instance(user_id: int, email: str, instance_id: str, image: str, instance_type: str, gpu_count: int, node_port: int, opencode_node_port: Optional[int] = None) -> dict:
    try:
        with engine.begin() as conn:
            return _record_instance_once(conn, user_id, email, instance_id, image, instance_type, gpu_count, node_port, opencode_node_port)
    except IntegrityError:
        # Another launch request may have inserted the reusable instance_id between
        # our read and insert. Re-read under lock and treat the live row as canonical.
        with engine.begin() as conn:
            return _record_instance_once(conn, user_id, email, instance_id, image, instance_type, gpu_count, node_port, opencode_node_port)


def record_instance_launch_event(
    user_id: int,
    email: str,
    instance_id: str,
    image: str,
    instance_type: str,
    gpu_count: int,
    template_id: Optional[int] = None,
    template_title: Optional[str] = None,
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
            )
        )


def mark_instance_deleted(instance_id: str):
    with engine.begin() as conn:
        conn.execute(
            update(instance_records)
            .where(instance_records.c.instance_id == instance_id, instance_records.c.deleted_at.is_(None))
            .values(status="deleted", deleted_at=utc_now())
        )


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
        def duplicate_charge_exists() -> bool:
            duplicate = conn.execute(
                select(usage_charges.c.id).where(
                    usage_charges.c.billing_session_id == billing_session_id,
                    usage_charges.c.billing_unit == billing_unit,
                )
            ).first()
            return duplicate is not None

        if duplicate_charge_exists():
            return "existing"

        active_instance = conn.execute(
            select(instance_records.c.id).where(
                instance_records.c.instance_id == instance_id,
                instance_records.c.billing_session_id == billing_session_id,
                instance_records.c.deleted_at.is_(None),
                instance_records.c.status == "running",
            ).with_for_update()
        ).first()
        if not active_instance:
            return "inactive"

        if duplicate_charge_exists():
            return "existing"

        user = conn.execute(select(users).where(users.c.id == user_id).with_for_update()).mappings().first()
        if not user:
            raise ValueError(f"User {user_id} not found")

        try:
            with conn.begin_nested():
                credit_update = conn.execute(
                    update(users)
                    .where(users.c.id == user_id, users.c.credits >= credits)
                    .values(credits=users.c.credits - credits, updated_at=now)
                )
                if credit_update.rowcount == 0:
                    if duplicate_charge_exists():
                        return "existing"
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
        except IntegrityError:
            if duplicate_charge_exists():
                return "existing"
            raise
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


def list_active_instances() -> list[dict]:
    stmt = (
        select(instance_records, users.c.credits)
        .join(users, users.c.id == instance_records.c.user_id)
        .where(instance_records.c.deleted_at.is_(None), instance_records.c.status == "running")
    )
    with engine.begin() as conn:
        return [dict(r) for r in conn.execute(stmt).mappings().all()]


def users_idle_since(cutoff_iso: str) -> list[dict]:
    """Users with no running instance and last instance activity at or before cutoff."""
    with engine.begin() as conn:
        rows = conn.execute(
            select(
                instance_records.c.user_id,
                instance_records.c.deleted_at,
                instance_records.c.last_charged_at,
                instance_records.c.status,
            )
        ).mappings().all()

    active_users = set()
    latest_activity: dict[int, str] = {}
    for row in rows:
        user_id = int(row["user_id"])
        if row["status"] == "running" and not row["deleted_at"]:
            active_users.add(user_id)
        activity = row["deleted_at"] or row["last_charged_at"]
        if activity and (user_id not in latest_activity or activity > latest_activity[user_id]):
            latest_activity[user_id] = activity

    return [
        {"user_id": user_id, "idle_since": idle_since}
        for user_id, idle_since in latest_activity.items()
        if user_id not in active_users and idle_since <= cutoff_iso
    ]
