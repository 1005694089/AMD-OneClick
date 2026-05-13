"""
Offline sync for notebook template previews.

Preview requests should serve cached notebook JSON/assets instead of depending on
live GitHub fetches in the user request path.
"""
import asyncio
import json
import logging
import posixpath
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlparse

import httpx

from .store import (
    ensure_template_preview_cache,
    get_notebook_template,
    list_notebook_templates,
    list_template_preview_sync_candidates,
    mark_template_preview_syncing,
    replace_template_preview_assets,
    update_template_preview_failure,
    update_template_preview_success,
)

logger = logging.getLogger(__name__)

REFRESH_INTERVAL_MINUTES = 60
RETRY_INTERVAL_MINUTES = 5
MAX_ASSET_BYTES = 8 * 1024 * 1024
MARKDOWN_IMAGE_RE = re.compile(r"!\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
HTML_IMAGE_RE = re.compile(r"<img\b[^>]*\bsrc=[\"']([^\"']+)[\"']", re.IGNORECASE)


def _github_repo_parts(repo_url: str) -> tuple[str, str]:
    parsed = urlparse(repo_url.strip() if "://" in repo_url else f"https://{repo_url.strip()}")
    if parsed.netloc.lower() not in {"github.com", "www.github.com"}:
        raise ValueError("Only GitHub repository URLs are supported for templates")
    path = parsed.path.lstrip("/").removesuffix(".git").rstrip("/")
    org, repo, *_ = path.split("/") + ["", ""]
    if not org or not repo:
        raise ValueError("Invalid GitHub repository URL")
    return org, repo


def _github_raw_url_candidates(repo_url: str, branch: str, path: str) -> list[str]:
    org, repo = _github_repo_parts(repo_url)
    clean_path = quote(path.lstrip("/"), safe="/")
    clean_branch = quote(branch or "main", safe="")
    return [
        f"https://raw.githubusercontent.com/{org}/{repo}/{clean_branch}/{clean_path}",
        f"https://github.com/{org}/{repo}/raw/{clean_branch}/{clean_path}",
        f"http://github.com/{org}/{repo}/raw/{clean_branch}/{clean_path}",
    ]


def _next_sync(minutes: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


def _source_text(source) -> str:
    if isinstance(source, list):
        return "".join(str(part) for part in source)
    return str(source or "")


def _is_relative_asset(url: str) -> bool:
    clean = (url or "").strip()
    return bool(clean) and not re.match(r"^(https?:|data:|/|#)", clean, re.IGNORECASE)


def _resolve_asset_path(notebook_path: str, asset_url: str) -> str:
    clean = asset_url.strip().split("#", 1)[0].split("?", 1)[0]
    base_dir = posixpath.dirname(notebook_path.lstrip("/"))
    resolved = posixpath.normpath(posixpath.join(base_dir, clean))
    return "" if resolved.startswith("../") else resolved.lstrip("/")


def extract_markdown_asset_paths(notebook: dict, notebook_path: str) -> list[str]:
    paths: set[str] = set()
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "markdown":
            continue
        source = _source_text(cell.get("source"))
        for match in MARKDOWN_IMAGE_RE.finditer(source):
            url = match.group(1)
            if _is_relative_asset(url):
                resolved = _resolve_asset_path(notebook_path, url)
                if resolved:
                    paths.add(resolved)
        for match in HTML_IMAGE_RE.finditer(source):
            url = match.group(1)
            if _is_relative_asset(url):
                resolved = _resolve_asset_path(notebook_path, url)
                if resolved:
                    paths.add(resolved)
    return sorted(paths)


async def _fetch_bytes(client: httpx.AsyncClient, urls: list[str], max_bytes: int | None = None) -> tuple[bytes, str, str]:
    errors = []
    for url in urls:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            content = resp.content
            if max_bytes is not None and len(content) > max_bytes:
                raise ValueError(f"asset exceeds {max_bytes} bytes")
            return content, resp.headers.get("content-type") or "application/octet-stream", url
        except Exception as e:
            errors.append(f"{url}: {type(e).__name__}: {e}")
    raise RuntimeError(" | ".join(errors[-4:]))


async def sync_template_preview(template_id: int, force: bool = False) -> dict:
    template = get_notebook_template(template_id, enabled_only=False)
    if not template:
        raise ValueError(f"Template {template_id} not found")

    ensure_template_preview_cache(template, force=force)
    mark_template_preview_syncing(template_id)

    timeout = httpx.Timeout(connect=5.0, read=20.0, write=5.0, pool=5.0)
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            notebook_bytes, _, source_url = await _fetch_bytes(
                client,
                _github_raw_url_candidates(template["repo_url"], template["branch"], template["notebook_path"]),
            )
            notebook = json.loads(notebook_bytes.decode("utf-8"))
            assets = []
            for asset_path in extract_markdown_asset_paths(notebook, template["notebook_path"]):
                try:
                    content, content_type, _ = await _fetch_bytes(
                        client,
                        _github_raw_url_candidates(template["repo_url"], template["branch"], asset_path),
                        max_bytes=MAX_ASSET_BYTES,
                    )
                    assets.append({"asset_path": asset_path, "content_type": content_type, "content": content})
                except Exception as e:
                    logger.warning("Template %s asset sync failed for %s: %s", template_id, asset_path, e)

        replace_template_preview_assets(template_id, assets)
        cache = update_template_preview_success(
            template,
            json.dumps(notebook, ensure_ascii=False),
            _next_sync(REFRESH_INTERVAL_MINUTES),
        )
        logger.info("Template %s preview synced from %s with %s assets", template_id, source_url, len(assets))
        return cache
    except Exception as e:
        cache = update_template_preview_failure(template_id, str(e), _next_sync(RETRY_INTERVAL_MINUTES))
        logger.error("Template %s preview sync failed: %s", template_id, e)
        return cache or {"template_id": template_id, "status": "failed", "error_message": str(e)}


async def sync_due_template_previews(limit: int = 5) -> list[dict]:
    for template in list_notebook_templates(enabled_only=False):
        ensure_template_preview_cache(template)

    now = datetime.now(timezone.utc).isoformat()
    results = []
    for cache in list_template_preview_sync_candidates(now, limit=limit):
        results.append(await sync_template_preview(int(cache["template_id"])))
        await asyncio.sleep(0.1)
    return results

