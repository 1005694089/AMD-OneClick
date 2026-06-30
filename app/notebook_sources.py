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
    return github_info


def parse_huggingface_demo_git_path(value: str) -> Optional[dict]:
    """Parse a `.git` repo reference for a workshop launch (clone, no notebook).

    Accepts a bare shorthand (`org/repo.git`), a full GitHub URL
    (`https://github.com/org/repo.git`), or the scp form (`git@github.com:org/repo.git`),
    each with an optional trailing `@branch` (e.g. `org/repo.git@dev`). Returns None when
    the value is not a `.git` reference (so the caller can fall back to the `.ipynb` parser).
    Raises ValueError when it looks like a `.git` reference but is malformed or non-GitHub.
    """
    raw = (value or "").strip()
    if not raw:
        return None

    # Split an optional trailing @branch on the LAST ".git@" so branch names with no dots are
    # handled and the ".git" suffix detection below still works on the base.
    base = raw
    branch: Optional[str] = None
    marker = raw.rfind(".git@")
    if marker != -1:
        base = raw[: marker + len(".git")]
        branch = raw[marker + len(".git@") :].strip() or None

    # Tolerate a trailing slash / query / fragment after ".git" (common copy/paste shapes).
    base = base.rstrip("/")
    for sep in ("?", "#"):
        cut = base.find(sep)
        if cut != -1:
            base = base[:cut]
    if not base.endswith(".git"):
        return None

    if base.startswith("git@"):
        # scp form: git@HOST:org/repo.git — enforce the GitHub host like the URL branch.
        host, _, scp_path = base[len("git@"):].partition(":")
        if host.lower() not in {"github.com", "www.github.com"}:
            raise ValueError("Only GitHub .git repositories are supported")
        path = scp_path
    elif "://" in base:
        # An explicit URL: enforce GitHub host.
        parsed = urlparse(base)
        host = (parsed.netloc or "").lower()
        if host not in {"github.com", "www.github.com"}:
            raise ValueError("Only GitHub .git repositories are supported")
        path = parsed.path.lstrip("/")
    else:
        # Bare shorthand like "org/repo.git" (no scheme): use it directly. Do NOT prepend a
        # scheme — that would make urlparse treat "org" as the host.
        path = base.lstrip("/")

    path = path.removesuffix(".git").strip("/")
    parts = path.split("/")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("git repo must look like org/repo.git")
    # Org/repo are interpolated into a github.com URL and an in-pod clone; reject any stray
    # URL/scp metacharacters that survived parsing so they can never reach git as a wrong host.
    if any(c in parts[0] + parts[1] for c in ("@", ":", "\\", " ")):
        raise ValueError("git repo must look like org/repo.git")

    return {
        "org": parts[0],
        "repo": parts[1],
        "branch": branch,
        "path": "",
        "raw_url": "",
    }
