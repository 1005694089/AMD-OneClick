"""Harbor auto-mirror: copy an external source image into the LAN Harbor registry via skopeo.

Admin Add Image mirrors any external ref (personal ACR, Docker Hub, etc.) into Harbor so that
instance launches never depend on public registries or public DNS at pull time. The rewritten
Harbor ref is what gets stored as the catalog launch ref and preheated onto the GPU nodes.

The rewrite PRESERVES the source registry host (sanitized) as the first path segment and re-homes
the whole thing under ``<HARBOR_REGISTRY>/<HARBOR_PROJECT>/<sanitized-host>/<repo>:<tag>``. Keeping
the host matters: two different source registries can expose the same trailing ``repo:tag``
(``team1.acr/foo:latest`` vs ``team2.acr/foo:latest``); stripping the host would collapse them onto
one Harbor ref and silently serve the wrong bits. Keeping the host also matches the existing
base-image naming convention (``10.5.10.89:1808/xinwei/<acr-host>/.../amd-oneclick-base:...``).
"""
import hashlib
import ipaddress
import logging
import os
import re
import socket
import subprocess
from typing import Optional

from .config import settings

logger = logging.getLogger(__name__)


class MirrorError(RuntimeError):
    """Raised when the skopeo copy into Harbor fails (or the source ref is unusable)."""


def _source_host(src_ref: str) -> str:
    """Extract the registry host (no port) from a source ref, defaulting to docker.io."""
    ref = (src_ref or "").strip()
    first = ref.split("/", 1)[0] if "/" in ref else ""
    if first and ("." in first or ":" in first or first.lower() == "localhost"):
        return first.split(":", 1)[0].lower()
    return "docker.io"


def _source_host_port(src_ref: str) -> str:
    """Extract the registry host[:port] (with port if present) from a source ref, else ''."""
    ref = (src_ref or "").strip()
    first = ref.split("/", 1)[0] if "/" in ref else ""
    if first and ("." in first or ":" in first or first.lower() == "localhost"):
        return first.lower()
    return ""


def _assert_source_host_allowed(src_ref: str) -> None:
    """Best-effort SSRF guard: reject a source host that resolves to a private/loopback/link-local
    address at check time. A host on HARBOR_MIRROR_SRC_HOST_ALLOWLIST (exact or suffix match) or the
    configured Harbor registry is always permitted.

    LIMITATIONS (this is defense-in-depth, NOT a complete boundary): skopeo does its own DNS
    resolution and follows registry blob redirects, so it can still reach an internal host via
    (a) DNS rebinding (public IP at check time, internal IP when skopeo connects) or (b) a
    302/307 blob redirect from a malicious/compromised source registry to an internal URL. Fully
    closing those requires a network egress policy on the manager pod (allow public + Harbor only) —
    an infra control this function cannot provide. This check stops the direct/common case (a source
    ref pointing straight at an internal IP or an internal-resolving host); the egress policy is the
    real boundary for the adversarial-DNS / malicious-registry cases."""
    if not settings.HARBOR_MIRROR_BLOCK_PRIVATE_SRC:
        return
    host = _source_host(src_ref)
    # Always allow the configured Harbor registry itself (a LAN/private endpoint by design):
    # re-mirroring an image already in Harbor under a different project is a legitimate workflow. Match
    # the FULL host:port — a bare-host match would exempt every OTHER service colocated on Harbor's IP
    # (e.g. 10.5.10.89:9999), defeating the guard for arbitrary internal ports on that box.
    harbor_hostport = settings.HARBOR_REGISTRY.strip().rstrip("/").split("/", 1)[0].lower()
    if _source_host_port(src_ref) == harbor_hostport:
        return
    allow = settings.HARBOR_MIRROR_SRC_HOST_ALLOWLIST or []
    if any(host == a or host.endswith("." + a.lstrip(".")) or host == a.lstrip(".") for a in allow):
        return
    # Resolve and check every returned address. A literal IP resolves to itself.
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror as e:
        raise MirrorError(f"cannot resolve source host {host!r}: {e}") from e
    for info in infos:
        addr = info[4][0]
        try:
            ip = ipaddress.ip_address(addr.split("%", 1)[0])
        except ValueError:
            continue
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_unspecified:
            raise MirrorError(
                f"source host {host!r} resolves to a non-public address ({ip}); "
                f"mirroring from internal hosts is blocked (add it to "
                f"HARBOR_MIRROR_SRC_HOST_ALLOWLIST to override)")


def _sanitize_host(host: str) -> str:
    """Turn a registry host[:port] into a valid, INJECTIVE single repo path component.

    Docker repo path components disallow ':' and uppercase; dots are allowed. Lowercasing and
    collapsing illegal chars to '-' alone is lossy — 'registry:5000' and a real host literally named
    'registry-5000' would both map to 'registry-5000' and collide onto the same Harbor object. To
    keep distinct source hosts distinct, append a short hash so the mapping can't collapse two
    different hosts together. The hash is over the LOWERCASED host: DNS hostnames are
    case-insensitive, so 'MyACR.io' and 'myacr.io' are the same registry and must hash identically,
    else re-adding a differently-cased spelling would mirror to a divergent dest."""
    normalized = host.lower()
    cleaned = re.sub(r"[^a-z0-9._-]+", "-", normalized).strip("-._") or "src"
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:8]
    return f"{cleaned}-{digest}"


def _split_ref(ref: str) -> tuple[Optional[str], str, str]:
    """Split a full image ref into (sanitized_host_or_None, repo, tag).

    The registry host (first path segment containing a '.'/':' or equal to 'localhost') is
    PRESERVED as a sanitized path component so two sources with the same trailing repo:tag do not
    collide. A digest ('@sha256:...') is rejected — the admin catalog stores tag refs."""
    ref = (ref or "").strip()
    if not ref:
        raise MirrorError("source image ref is empty")
    if "@" in ref:
        raise MirrorError(f"digest refs are not supported for mirroring: {ref!r}")

    # A ref with NO '/' is ALWAYS a Docker Hub image (per Docker's reference grammar: registry-host
    # detection only kicks in when a '/' is present). So 'nginx', 'my.tool:v1', even 'localhost:5000'
    # (no slash) all resolve to docker.io/library/<name> — we must NOT reject these as "bare hosts".
    # The bare-host rejection below applies only to WITH-slash refs whose repo part is empty.

    # Separate an optional tag. A ':' only introduces a tag when it appears in the LAST path
    # segment (a ':' in the first segment is the registry host:port, not a tag).
    repo, tag = ref, "latest"
    last_slash = ref.rfind("/")
    last_colon = ref.rfind(":")
    if last_colon > last_slash:
        repo, tag = ref[:last_colon], ref[last_colon + 1:]
        if not tag:
            raise MirrorError(f"empty tag in ref: {ref!r}")

    # Peel off a registry host prefix (has a dot or colon, or is localhost). Compare case-insensitively
    # (DNS hostnames are case-insensitive) so 'Localhost'/'Docker.io' are detected as hosts too.
    first = repo.split("/", 1)[0]
    first_lower = first.lower()
    has_explicit_host = "/" in repo and ("." in first or ":" in first or first_lower == "localhost")
    if has_explicit_host:
        raw_host = first_lower
        repo = repo.split("/", 1)[1]
    else:
        # No explicit registry → Docker Hub.
        raw_host = "docker.io"

    # Canonicalize Docker Hub refs so all equivalent spellings converge on ONE Harbor dest and don't
    # produce duplicate mirrors: normalize the host aliases to 'docker.io' and prepend the implicit
    # 'library/' namespace for a bare single-segment image. Thus 'nginx', 'library/nginx',
    # 'docker.io/nginx', 'Docker.io/nginx', 'docker.io:443/nginx', and 'docker.io/library/nginx' all
    # map to the same host+repo. Strip any port before the alias check so 'docker.io:443' matches.
    if raw_host.split(":", 1)[0] in {"docker.io", "index.docker.io", "registry-1.docker.io", "registry.hub.docker.com"}:
        raw_host = "docker.io"
        if "/" not in repo.strip("/"):
            repo = f"library/{repo.strip('/')}"

    repo = repo.strip("/")
    if not repo:
        raise MirrorError(f"could not derive a repo path from ref: {ref!r}")
    return _sanitize_host(raw_host), repo, tag


def harbor_ref(src_ref: str) -> str:
    """Return the Harbor ref an external source image will be mirrored to.

    ``<HARBOR_REGISTRY>/<HARBOR_PROJECT>/<sanitized-host>/<repo>:<tag>``. Idempotent: a ref already
    under the Harbor registry is returned unchanged (re-mirroring keeps the same dest)."""
    ref = (src_ref or "").strip()
    registry = settings.HARBOR_REGISTRY.strip().rstrip("/")
    project = settings.HARBOR_PROJECT.strip().strip("/")
    if not registry or not project:
        raise MirrorError("HARBOR_REGISTRY / HARBOR_PROJECT are not configured")
    prefix = f"{registry}/{project}/"
    if ref.startswith(prefix):
        # Already under the Harbor prefix. Still enforce the same invariants as the rewrite path so
        # an already-Harbor ref can't smuggle in a digest (unsupported) or a missing tag: reject a
        # digest, and default a missing tag to ':latest' so the catalog always stores a tag ref.
        if "@" in ref:
            raise MirrorError(f"digest refs are not supported for mirroring: {ref!r}")
        rest = ref[len(prefix):]
        last_slash = rest.rfind("/")
        last_colon = rest.rfind(":")
        if last_colon <= last_slash:  # no tag on the final path segment
            return f"{ref}:latest"
        return ref
    host, repo, tag = _split_ref(ref)
    path = f"{host}/{repo}" if host else repo
    return f"{prefix}{path}:{tag}"


def mirror_to_harbor(src_ref: str) -> str:
    """Copy ``src_ref`` (any external registry) into Harbor via skopeo; return the Harbor ref.

    Blocking (subprocess) — callers MUST invoke via ``asyncio.to_thread`` so the single uvicorn
    worker is not stalled. ``--retry-times`` is mandatory: large multi-GB layers flake with
    transient Harbor 502s on the first chunked push and only succeed on retry (proven live).

    Raises ``MirrorError`` on any non-zero skopeo exit or timeout so the caller surfaces the
    failure on the catalog row instead of flipping it to ready."""
    dest = harbor_ref(src_ref)
    # Self-copy short-circuit: if the source is already the Harbor ref (a row whose stored image is
    # already in Harbor, e.g. a legacy row with no source_ref), there is nothing to mirror. Running
    # skopeo would attempt a src TLS handshake against Harbor's plain-HTTP port and fail every time.
    authfile = settings.HARBOR_AUTH_CONFIG_PATH.strip()
    if not authfile or not os.path.exists(authfile):
        raise MirrorError(f"Harbor auth config not found at {authfile!r}")
    if dest == (src_ref or "").strip():
        # Self-copy: the source is already the Harbor ref (a row whose stored image is already in
        # Harbor). There's nothing to copy, but VERIFY the image actually exists at that Harbor path
        # — otherwise a mistyped/guessed Harbor-prefixed ref would be accepted as "mirrored" and then
        # ImagePullBackOff on every node while status shows a misleading "pulling" forever.
        logger.info("Source %s is already the Harbor ref; verifying existence (no copy)", dest)
        inspect = subprocess.run(
            ["skopeo", "inspect", "--tls-verify=false", "--authfile", authfile,
             f"docker://{dest}"],
            capture_output=True, text=True, timeout=120,
        )
        if inspect.returncode != 0:
            tail = (inspect.stderr or inspect.stdout or "").strip()[-800:]
            raise MirrorError(f"image not found in Harbor at {dest} (rc={inspect.returncode}): {tail}")
        return dest
    # SSRF guard: block pulling from internal/private hosts (checked AFTER the self-copy
    # short-circuit, so re-mirroring the LAN Harbor ref itself is still allowed).
    _assert_source_host_allowed(src_ref)

    # Source TLS verification stays ON by default — external registries (ACR, Docker Hub) present
    # valid certs, and disabling it would let an on-path attacker MITM the pull and inject an
    # arbitrary image that then propagates to every node. Only the Harbor DEST is plain HTTP, so its
    # verification is disabled unconditionally. EXCEPTION: if the SOURCE also lives on the Harbor
    # host (same-host, different project — the self-copy short-circuit above only catches the exact
    # canonical prefix), it too is plain HTTP, so a TLS handshake would always fail; disable src
    # verification in that case.
    registry = settings.HARBOR_REGISTRY.strip().rstrip("/")
    src_on_harbor = (src_ref or "").strip().startswith(f"{registry}/")
    src_tls = "false" if src_on_harbor or not settings.HARBOR_MIRROR_SRC_TLS_VERIFY else "true"
    cmd = [
        "skopeo", "copy",
        "--retry-times", str(int(settings.HARBOR_MIRROR_RETRY_TIMES)),
        f"--src-tls-verify={src_tls}",
        "--dest-tls-verify=false",
        "--dest-authfile", authfile,
        f"docker://{src_ref.strip()}",
        f"docker://{dest}",
    ]
    logger.info("Mirroring %s -> %s via skopeo", src_ref, dest)
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=int(settings.HARBOR_MIRROR_TIMEOUT_SECONDS),
        )
    except subprocess.TimeoutExpired as e:
        raise MirrorError(f"skopeo copy timed out after {settings.HARBOR_MIRROR_TIMEOUT_SECONDS}s") from e
    except FileNotFoundError as e:
        raise MirrorError("skopeo binary not found in the manager image") from e
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "").strip()[-800:]
        raise MirrorError(f"skopeo copy failed (rc={proc.returncode}): {tail}")
    logger.info("Mirrored %s -> %s", src_ref, dest)
    return dest
