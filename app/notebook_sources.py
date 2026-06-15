"""
Notebook source path parsing helpers.
"""
from typing import Optional
from urllib.parse import quote, unquote, urlparse, urlunparse


def parse_github_path(full_path: str) -> dict:
    """Parse GitHub path like org/repo/blob/branch/path/to/notebook.ipynb."""
    raw = (full_path or "").strip().lstrip("/")
    parts = raw.split("/")
    if len(parts) < 5 or any(not part for part in parts[:4]) or parts[2] != "blob":
        raise ValueError("Invalid GitHub path format")
    if not parts[-1].lower().endswith(".ipynb"):
        raise ValueError("GitHub path must point to an .ipynb file")

    org = parts[0]
    repo = parts[1]
    branch = parts[3]
    path = "/".join(parts[4:])
    raw_url = f"https://raw.githubusercontent.com/{org}/{repo}/{branch}/{path}"

    return {
        "org": org,
        "repo": repo,
        "branch": branch,
        "path": path,
        "raw_url": raw_url,
    }


def parse_huggingface_notebook_url(raw_url: str) -> Optional[dict]:
    parsed = urlparse(raw_url)
    host = parsed.netloc.lower()
    if host not in {"huggingface.co", "www.huggingface.co"}:
        return None

    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) < 2:
        raise ValueError("Hugging Face notebook URL must include an .ipynb file")

    marker_index = next((idx for idx, part in enumerate(parts) if part in {"blob", "resolve"}), None)
    if marker_index is not None:
        if marker_index < 1 or len(parts) <= marker_index + 2:
            raise ValueError("Hugging Face notebook URL must look like /repo/blob/revision/path.ipynb")
        repo_parts = parts[:marker_index]
        branch = parts[marker_index + 1]
        file_parts = parts[marker_index + 2:]
        if not file_parts[-1].lower().endswith(".ipynb"):
            raise ValueError("Hugging Face notebook URL must point to an .ipynb file")
        repo_id = "/".join(repo_parts)
        file_path = "/".join(file_parts)
        download_path = f"/{repo_id}/resolve/{branch}/{quote(file_path, safe='/')}"
        download_url = urlunparse((parsed.scheme or "https", parsed.netloc, download_path, "", parsed.query, ""))
    else:
        if not parts[-1].lower().endswith(".ipynb"):
            raise ValueError("Hugging Face notebook URL must point to an .ipynb file")
        repo_parts = parts[:-1]
        branch = "main"
        file_parts = [parts[-1]]
        download_url = urlunparse((parsed.scheme or "https", parsed.netloc, parsed.path, "", parsed.query, ""))

    filename = unquote(file_parts[-1])
    repo_id = "/".join(repo_parts) if repo_parts else "huggingface"
    return {
        "org": "huggingface",
        "repo": repo_id,
        "branch": branch,
        "path": filename,
        "raw_url": download_url,
    }


def parse_huggingface_demo_notebook_path(notebook_path: str) -> dict:
    raw = (notebook_path or "").strip()
    if not raw:
        raise ValueError("notebook_path is required")

    parsed = urlparse(raw)
    huggingface_info = parse_huggingface_notebook_url(raw) if parsed.scheme or parsed.netloc else None
    if huggingface_info:
        return huggingface_info

    path = parsed.path.lstrip("/") if parsed.scheme or parsed.netloc else raw.lstrip("/")
    if path.startswith("github/"):
        path = path[len("github/"):]

    parts = path.split("/")
    if len(parts) < 5 or any(not part for part in parts[:4]) or parts[2] != "blob":
        raise ValueError("notebook_path must look like /github/org/repo/blob/branch/path.ipynb")
    if not parts[-1].lower().endswith(".ipynb"):
        raise ValueError("notebook_path must point to an .ipynb file")

    github_info = parse_github_path(path)
    github_info["repo_url"] = f"http://github.com/{github_info['org']}/{github_info['repo']}.git"
    return github_info
