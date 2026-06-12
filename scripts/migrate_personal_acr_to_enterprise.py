#!/usr/bin/env python3
"""Migrate workshop/notebook image references from personal ACR to enterprise ACR.

Copies missing images with skopeo (when available), then updates the manager DB and
optionally triggers catalog prepull sync via the admin API.

Usage:
  python scripts/migrate_personal_acr_to_enterprise.py --dry-run
  python scripts/migrate_personal_acr_to_enterprise.py --apply
  python scripts/migrate_personal_acr_to_enterprise.py --apply --sync-images
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from typing import Iterable

from sqlalchemy import text

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.config import settings  # noqa: E402
from app.store import engine, images, notebook_templates  # noqa: E402

ENTERPRISE = settings.ENTERPRISE_REGISTRY_HOST

PERSONAL_TO_ENTERPRISE = {
    "crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick-base:rocm7.2.1-py3.12-v20260416": f"{ENTERPRISE}/admin/amd-oneclick-base:rocm7.2.1-py3.12-v20260416",
    "crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick-base:rocm7.2.1-py3.12-v20260416-deepseek-flash-default-20260518": f"{ENTERPRISE}/admin/amd-oneclick-base:rocm7.2.1-py3.12-v20260416-deepseek-flash-default-20260518",
    "crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick-base:git-proxy-test-20260528-1125": f"{ENTERPRISE}/admin/amd-oneclick-base:git-proxy-test-20260528-1125",
    "crpi-755cgxj597677gbs.cn-shanghai.personal.cr.aliyuncs.com/sdg_workshop/sdg_workshop:rocm7.2_w7900_kitchen_v3": f"{ENTERPRISE}/admin/amd-oneclick:sdg_workshop-rocm7.2_w7900_kitchen_v3",
}

DEFAULT_ENTERPRISE_IMAGE = PERSONAL_TO_ENTERPRISE[
    "crpi-xhg6joi134vrkpzq.cn-shanghai.personal.cr.aliyuncs.com/vivienfanghua/amd-oneclick-base:rocm7.2.1-py3.12-v20260416"
]


def _skopeo_inspect(ref: str) -> bool:
    result = subprocess.run(
        ["skopeo", "inspect", f"docker://{ref}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def _skopeo_copy(src: str, dst: str) -> None:
    print(f"copy {src} -> {dst}")
    subprocess.run(["skopeo", "copy", f"docker://{src}", f"docker://{dst}"], check=True)


def ensure_enterprise_images(personal_to_enterprise: dict[str, str], dry_run: bool) -> None:
    for src, dst in personal_to_enterprise.items():
        if _skopeo_inspect(dst):
            print(f"ok  {dst}")
            continue
        if dry_run:
            print(f"would copy {src} -> {dst}")
            continue
        _skopeo_copy(src, dst)
        if not _skopeo_inspect(dst):
            raise RuntimeError(f"enterprise image missing after copy: {dst}")


def _replace_sql(column: str, mapping: dict[str, str]) -> tuple[str, dict[str, str]]:
    params: dict[str, str] = {}
    case = " ".join(
        f"WHEN :src_{idx} THEN :dst_{idx}"
        for idx, (src, dst) in enumerate(mapping.items())
    )
    in_list = ", ".join(f":src_{idx}" for idx in range(len(mapping)))
    for idx, (src, dst) in enumerate(mapping.items()):
        params[f"src_{idx}"] = src
        params[f"dst_{idx}"] = dst
    sql = f"UPDATE {{table}} SET {column} = CASE {column} {case} ELSE {column} END WHERE {column} IN ({in_list})"
    return sql, params


def apply_db_updates(mapping: dict[str, str], dry_run: bool) -> dict[str, int]:
    counts: dict[str, int] = {}
    with engine.begin() as conn:
        for table in ("images", "notebook_templates", "custom_images", "instance_records", "instance_launch_events"):
            sql, params = _replace_sql("image", mapping)
            stmt = text(sql.format(table=table))
            if dry_run:
                row = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table} WHERE image IN ({', '.join(f':src_{i}' for i in range(len(mapping)))})"),
                    params,
                ).scalar()
                counts[table] = int(row or 0)
                continue
            result = conn.execute(stmt, params)
            counts[table] = int(result.rowcount or 0)
    return counts


def list_remaining_personal_refs() -> Iterable[tuple[str, str, str]]:
    personal_like = "%personal.cr.aliyuncs.com%"
    with engine.connect() as conn:
        for table in ("images", "notebook_templates", "custom_images", "instance_records", "instance_launch_events"):
            rows = conn.execute(
                text(f"SELECT id, image FROM {table} WHERE image LIKE :pattern"),
                {"pattern": personal_like},
            ).fetchall()
            for row in rows:
                yield table, str(row[0]), str(row[1])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--skip-copy", action="store_true", help="Only update DB references")
    args = parser.parse_args()
    if args.dry_run == args.apply:
        parser.error("Specify exactly one of --dry-run or --apply")

    dry_run = args.dry_run
    print(f"DEFAULT_IMAGE target: {DEFAULT_ENTERPRISE_IMAGE}")
    if not args.skip_copy:
        ensure_enterprise_images(PERSONAL_TO_ENTERPRISE, dry_run=dry_run)

    counts = apply_db_updates(PERSONAL_TO_ENTERPRISE, dry_run=dry_run)
    print("updated rows:", counts)
    remaining = list(list_remaining_personal_refs())
    if remaining:
        print("remaining personal ACR refs:")
        for table, row_id, image in remaining:
            print(f"  {table}#{row_id}: {image}")
        if not dry_run:
            raise SystemExit(1)
    else:
        print("no personal ACR image refs remain in DB")


if __name__ == "__main__":
    main()
