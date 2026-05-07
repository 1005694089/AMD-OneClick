"""
Persistent store for users, image catalog, and credit accounting.
"""
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from .config import settings


DB_PATH = os.getenv("DATABASE_PATH", "/data/amd-oneclick.db")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                email TEXT NOT NULL,
                name TEXT,
                avatar_url TEXT,
                credits INTEGER NOT NULL DEFAULT 100,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(provider, provider_id),
                UNIQUE(email)
            );

            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                image TEXT NOT NULL UNIQUE,
                description TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS instance_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                instance_id TEXT NOT NULL UNIQUE,
                image TEXT NOT NULL,
                instance_type TEXT NOT NULL,
                gpu_count INTEGER NOT NULL,
                node_port INTEGER,
                status TEXT NOT NULL DEFAULT 'running',
                created_at TEXT NOT NULL,
                last_charged_at TEXT NOT NULL,
                deleted_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS credit_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                delta INTEGER NOT NULL,
                reason TEXT NOT NULL,
                instance_id TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );
            """
        )
        ensure_default_image(conn)


def ensure_default_image(conn: sqlite3.Connection):
    now = utc_now()
    conn.execute(
        """
        INSERT OR IGNORE INTO images (name, image, description, enabled, created_at, updated_at)
        VALUES (?, ?, ?, 1, ?, ?)
        """,
        ("AMD OneClick Base", settings.DEFAULT_IMAGE, "Default ROCm Jupyter/OpenCode image", now, now),
    )


def row_to_dict(row: Optional[sqlite3.Row]) -> Optional[dict]:
    return dict(row) if row else None


def get_or_create_user(provider: str, provider_id: str, email: str, name: str = "", avatar_url: str = "") -> dict:
    now = utc_now()
    with db() as conn:
        existing = conn.execute("SELECT * FROM users WHERE provider=? AND provider_id=?", (provider, provider_id)).fetchone()
        if existing:
            conn.execute(
                "UPDATE users SET email=?, name=?, avatar_url=?, updated_at=? WHERE id=?",
                (email, name, avatar_url, now, existing["id"]),
            )
            return row_to_dict(conn.execute("SELECT * FROM users WHERE id=?", (existing["id"],)).fetchone())
        by_email = conn.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
        if by_email:
            conn.execute(
                "UPDATE users SET provider=?, provider_id=?, name=?, avatar_url=?, updated_at=? WHERE id=?",
                (provider, provider_id, name, avatar_url, now, by_email["id"]),
            )
            return row_to_dict(conn.execute("SELECT * FROM users WHERE id=?", (by_email["id"],)).fetchone())
        cur = conn.execute(
            """
            INSERT INTO users (provider, provider_id, email, name, avatar_url, credits, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 100, ?, ?)
            """,
            (provider, provider_id, email, name, avatar_url, now, now),
        )
        user_id = cur.lastrowid
        conn.execute(
            "INSERT INTO credit_ledger (user_id, delta, reason, created_at) VALUES (?, 100, 'signup_bonus', ?)",
            (user_id, now),
        )
        return row_to_dict(conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone())


def get_user(user_id: int) -> Optional[dict]:
    with db() as conn:
        return row_to_dict(conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone())


def list_images(enabled_only: bool = False) -> list[dict]:
    query = "SELECT * FROM images"
    if enabled_only:
        query += " WHERE enabled=1"
    query += " ORDER BY id"
    with db() as conn:
        return [dict(r) for r in conn.execute(query).fetchall()]


def upsert_image(name: str, image: str, description: str = "", enabled: bool = True, image_id: Optional[int] = None) -> dict:
    now = utc_now()
    with db() as conn:
        if image_id:
            conn.execute(
                "UPDATE images SET name=?, image=?, description=?, enabled=?, updated_at=? WHERE id=?",
                (name, image, description, int(enabled), now, image_id),
            )
            return row_to_dict(conn.execute("SELECT * FROM images WHERE id=?", (image_id,)).fetchone())
        cur = conn.execute(
            """
            INSERT INTO images (name, image, description, enabled, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (name, image, description, int(enabled), now, now),
        )
        return row_to_dict(conn.execute("SELECT * FROM images WHERE id=?", (cur.lastrowid,)).fetchone())


def delete_image(image_id: int) -> bool:
    with db() as conn:
        cur = conn.execute("DELETE FROM images WHERE id=?", (image_id,))
        return cur.rowcount > 0


def get_image_by_value(image: str) -> Optional[dict]:
    with db() as conn:
        return row_to_dict(conn.execute("SELECT * FROM images WHERE image=? AND enabled=1", (image,)).fetchone())


def get_active_instance_for_user(user_id: int) -> Optional[dict]:
    with db() as conn:
        return row_to_dict(
            conn.execute(
                "SELECT * FROM instance_records WHERE user_id=? AND deleted_at IS NULL AND status='running' ORDER BY id DESC LIMIT 1",
                (user_id,),
            ).fetchone()
        )


def record_instance(user_id: int, email: str, instance_id: str, image: str, instance_type: str, gpu_count: int, node_port: int):
    now = utc_now()
    with db() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO instance_records
            (user_id, email, instance_id, image, instance_type, gpu_count, node_port, status, created_at, last_charged_at, deleted_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'running', ?, ?, NULL)
            """,
            (user_id, email, instance_id, image, instance_type, gpu_count, node_port, now, now),
        )


def mark_instance_deleted(instance_id: str):
    with db() as conn:
        conn.execute(
            "UPDATE instance_records SET status='deleted', deleted_at=? WHERE instance_id=? AND deleted_at IS NULL",
            (utc_now(), instance_id),
        )


def charge_user(user_id: int, amount: int, reason: str, instance_id: str):
    now = utc_now()
    with db() as conn:
        conn.execute("UPDATE users SET credits=credits-?, updated_at=? WHERE id=?", (amount, now, user_id))
        conn.execute(
            "INSERT INTO credit_ledger (user_id, delta, reason, instance_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, -amount, reason, instance_id, now),
        )


def update_instance_charge_time(record_id: int, charged_at: str):
    with db() as conn:
        conn.execute("UPDATE instance_records SET last_charged_at=? WHERE id=?", (charged_at, record_id))


def list_active_instances() -> list[dict]:
    with db() as conn:
        return [
            dict(r)
            for r in conn.execute(
                """
                SELECT ir.*, u.credits
                FROM instance_records ir
                JOIN users u ON u.id=ir.user_id
                WHERE ir.deleted_at IS NULL AND ir.status='running'
                """
            ).fetchall()
        ]
