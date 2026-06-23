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

engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)
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
    Column("instance_type", String(64)),
    Column("start_command", Text),
    Column("app_port", Integer),
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
    Column("status", String(64), nullable=False, default="running"),
    Column("created_at", String(64), nullable=False),
    Column("last_charged_at", String(64), nullable=False),
    Column("billing_session_id", String(255), nullable=False),
    Column("ready_at", String(64)),
    Column("billing_started_at", String(64)),
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
    inspector = inspect(conn)
    user_columns = {col["name"] for col in inspector.get_columns("users")}
    if "is_editor" not in user_columns:
        conn.execute(text("ALTER TABLE users ADD COLUMN is_editor BOOLEAN NOT NULL DEFAULT FALSE"))

    instance_columns = {col["name"] for col in inspector.get_columns("instance_records")}
    if "billing_session_id" not in instance_columns:
        conn.execute(text("ALTER TABLE instance_records ADD COLUMN billing_session_id VARCHAR(255)"))
        conn.execute(text("UPDATE instance_records SET billing_session_id = instance_id WHERE billing_session_id IS NULL"))
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

    # Backward-compatible creation for databases initialized before these tables.
    metadata.create_all(bind=conn, tables=[template_preview_cache, template_preview_assets, coupon_redemptions, instance_launch_events])


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


def set_user_editor(user_id: int, is_editor: bool) -> Optional[dict]:
    now = utc_now()
    with engine.begin() as conn:
        user = conn.execute(select(users).where(users.c.id == user_id)).mappings().first()
        if not user:
            return None
        conn.execute(update(users).where(users.c.id == user_id).values(is_editor=bool(is_editor), updated_at=now))
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
    instance_type: Optional[str] = None,
    start_command: Optional[str] = None,
    app_port: Optional[int] = None,
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


def record_instance(user_id: int, email: str, instance_id: str, image: str, instance_type: str, gpu_count: int, node_port: int):
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
            status="pending",
            created_at=now,
            last_charged_at=now,
            billing_session_id=billing_session_id,
            ready_at=None,
            billing_started_at=None,
            deleted_at=None,
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


def list_active_instances() -> list[dict]:
    stmt = (
        select(instance_records, users.c.credits)
        .join(users, users.c.id == instance_records.c.user_id)
        .where(instance_records.c.deleted_at.is_(None), instance_records.c.status.in_(["pending", "running"]))
    )
    with engine.begin() as conn:
        return [dict(r) for r in conn.execute(stmt).mappings().all()]
