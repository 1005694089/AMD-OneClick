"""Copy public catalog data from production DB into the isolated workshop DB."""

import os

from sqlalchemy import create_engine, select, text

from .store import engine as destination_engine
from .store import images, notebook_templates


def main() -> None:
    source_url = os.environ.get("SOURCE_DATABASE_URL")
    if not source_url:
        raise SystemExit("SOURCE_DATABASE_URL is required")

    source_engine = create_engine(source_url, pool_pre_ping=True)

    with source_engine.begin() as src, destination_engine.begin() as dst:
        image_rows = [dict(row) for row in src.execute(select(images)).mappings().all()]
        template_rows = []
        for row in src.execute(select(notebook_templates).where(notebook_templates.c.owner_user_id.is_(None))).mappings().all():
            data = dict(row)
            data["owner_user_id"] = None
            template_rows.append(data)

        dst.execute(text("TRUNCATE TABLE template_preview_assets, template_preview_cache, notebook_templates, images RESTART IDENTITY CASCADE"))
        if image_rows:
            dst.execute(images.insert(), image_rows)
        if template_rows:
            dst.execute(notebook_templates.insert(), template_rows)

    print({"images": len(image_rows), "templates": len(template_rows)})


if __name__ == "__main__":
    main()
