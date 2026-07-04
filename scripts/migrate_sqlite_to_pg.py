"""One-off SQLite -> PostgreSQL data migration for the AMD-OneClick manager.

Runs from inside the manager image so app.store metadata is byte-identical to the
schema the app will serve. Copies every table in FK-safe order with EXPLICIT primary
keys, resets Postgres sequences so future autoincrement never collides, and asserts
row-count + per-table checksum parity plus survival of the known running instances.

Everything (schema create, all copies, sequence resets, verification) runs inside a
SINGLE transaction; without --commit it ROLLS BACK (dry run) leaving PG untouched.

Usage:
  SQLITE_PATH=/data/amd-oneclick.db  DATABASE_URL=postgresql+psycopg2://...  \
  RUNNING_GUARD="u-16-...,u-18-...,u-20-..."  python scripts/migrate_sqlite_to_pg.py [--commit]
"""
import hashlib
import os
import sys

from sqlalchemy import create_engine, MetaData, select, text

DRY = "--commit" not in sys.argv
SQLITE_PATH = os.environ["SQLITE_PATH"]
PG_URL = os.environ["DATABASE_URL"]
RUNNING_GUARD = [s for s in os.environ.get("RUNNING_GUARD", "").split(",") if s]

sqlite_engine = create_engine(f"sqlite:///{SQLITE_PATH}")
pg_engine = create_engine(PG_URL, future=True)

# Reflect the SOURCE schema straight from the live SQLite file (authoritative for
# what data actually exists), and use the app metadata to build the destination.
src_md = MetaData()
src_md.reflect(bind=sqlite_engine)

os.environ.setdefault("DATABASE_PATH", "/tmp/ignore.db")
sys.path.insert(0, os.getcwd())
from app import store  # noqa: E402

app_md = store.metadata


def checksum(conn, table, cols):
    """Order-independent content hash: sha256 over sorted per-row hashes."""
    rows = conn.execute(select(table)).fetchall()
    row_hashes = []
    for r in rows:
        m = r._mapping
        payload = "|".join(f"{c}={m[c]!r}" for c in cols)
        row_hashes.append(hashlib.sha256(payload.encode()).hexdigest())
    row_hashes.sort()
    h = hashlib.sha256()
    for rh in row_hashes:
        h.update(rh.encode())
    return len(rows), h.hexdigest()


def preflight_column_diff():
    """MUST-FIX: hard-fail if the live SQLite has any column the destination model
    lacks (its data would be silently dropped and the checksum, computed only over
    intersected columns, could never notice)."""
    problems = []
    for t in app_md.sorted_tables:
        if t.name not in src_md.tables:
            continue
        src_cols = set(src_md.tables[t.name].columns.keys())
        dst_cols = set(c.name for c in t.columns)
        source_only = src_cols - dst_cols
        if source_only:
            problems.append(f"{t.name}: source-only columns would be DROPPED: {sorted(source_only)}")
    # Also flag any SQLite table entirely absent from the destination model.
    orphan_tables = set(src_md.tables) - set(t.name for t in app_md.sorted_tables)
    # ignore sqlite internal + alembic-style bookkeeping if any
    orphan_tables = {x for x in orphan_tables if not x.startswith("sqlite_")}
    for ot in sorted(orphan_tables):
        nrows = sqlite_engine.connect().execute(
            text(f"SELECT count(*) FROM {ot}")).scalar()
        if nrows:
            problems.append(f"table {ot} ({nrows} rows) exists in SQLite but NOT in the app model")
    if problems:
        print("PRE-FLIGHT COLUMN/TABLE DIFF FAILURES:")
        for p in problems:
            print("  !!", p)
        raise SystemExit("ABORT: schema drift would lose data; reconcile before migrating.")
    print("pre-flight column/table diff: OK (no source-only columns/tables with data)")


def main():
    preflight_column_diff()

    order = [t for t in app_md.sorted_tables if t.name in src_md.tables]

    # SINGLE transaction: DDL + copies + setval + verify all roll back together.
    with pg_engine.begin() as pg:
        app_md.create_all(bind=pg)  # bound to THIS connection, inside the txn

        # Re-run safety: destination must be empty (plain INSERTs, explicit PKs).
        for t in app_md.sorted_tables:
            if pg.execute(t.select().limit(1)).first() is not None:
                raise SystemExit(
                    f"ABORT: destination table {t.name} is non-empty; "
                    f"TRUNCATE/DROP the PG schema before (re-)running."
                )

        for t in order:
            src_t = src_md.tables[t.name]
            with sqlite_engine.connect() as sc:
                rows = [dict(r._mapping) for r in sc.execute(select(src_t)).fetchall()]
            if not rows:
                continue
            common = [c.name for c in t.columns if c.name in src_t.columns]
            payload = [{k: r.get(k) for k in common} for r in rows]
            pg.execute(t.insert(), payload)
            print(f"  copied {t.name}: {len(rows)} rows ({len(common)} cols)")

        # Reset sequences for single-int-PK tables (derive PK name from schema).
        for t in order:
            pkcols = list(t.primary_key.columns)
            if len(pkcols) != 1:
                continue
            pk = pkcols[0]
            try:
                is_int = pk.type.python_type is int
            except Exception:
                is_int = False
            if not is_int:
                continue
            seq = pg.execute(
                text("SELECT pg_get_serial_sequence(:tbl, :col)"),
                {"tbl": t.name, "col": pk.name},
            ).scalar()
            if seq is None:
                raise SystemExit(
                    f"ABORT: no serial sequence for {t.name}.{pk.name}; "
                    f"future inserts could collide. Investigate before commit."
                )
            res = pg.execute(
                text(
                    "SELECT setval(:seq, "
                    f"COALESCE((SELECT MAX({pk.name}) FROM {t.name}), 1), "
                    f"(SELECT MAX({pk.name}) IS NOT NULL FROM {t.name}))"
                ),
                {"seq": seq},
            ).scalar()
            print(f"  setval {t.name}.{pk.name} ({seq}) -> {res}")

        # ---- parity assertions (inside txn) ----
        print("--- verifying parity ---")
        ok = True
        sconn = sqlite_engine.connect()
        for t in order:
            src_t = src_md.tables[t.name]
            common = [c.name for c in t.columns if c.name in src_t.columns]
            sn, sh = checksum(sconn, src_t, common)
            pn, ph = checksum(pg, t, common)
            status = "OK" if (sn == pn and sh == ph) else "MISMATCH"
            if status != "OK":
                ok = False
            print(f"  {t.name:26s} sqlite={sn} pg={pn} {status}")

        if RUNNING_GUARD:
            got = [r[0] for r in pg.execute(text(
                "SELECT instance_id FROM instance_records WHERE status='running'"
            )).fetchall()]
            missing = [g for g in RUNNING_GUARD if g not in got]
            print(f"  running in PG: {sorted(got)}")
            if missing:
                ok = False
                print(f"  !! MISSING running instances: {missing}")

        if not ok:
            raise SystemExit("PARITY CHECK FAILED - aborting (txn rolled back)")

        if DRY:
            print("DRY RUN OK - rolling back (pass --commit to persist)")
            raise _Rollback()
    print("COMMIT OK - migration persisted")


class _Rollback(Exception):
    pass


if __name__ == "__main__":
    try:
        main()
    except _Rollback:
        print("(rolled back as intended)")
        sys.exit(0)
