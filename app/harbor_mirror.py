"""Harbor image resolver: verify that an image exists in one of the configured Harbor sources.

Admin Add Image assumes the image is ALREADY pushed to a Harbor registry (by an external process).
The resolver normalizes user input, builds candidate refs across ``HARBOR_IMAGE_SOURCES``, and
verifies existence via ``skopeo inspect``. The first source that reports the image is returned as
the canonical launch ref and preheated onto GPU nodes.

Priority order (default):
  1. ``10.5.10.12:1808/radeon-cloud-global``
  2. ``10.5.10.12:1808/radeon-cloud-user``
  3. ``10.5.10.89:1808/xinwei``
"""
import logging
import os
import subprocess
from typing import Optional

from .config import settings

logger = logging.getLogger(__name__)

_KNOWN_HARBOR_PROJECTS = frozenset({"radeon-cloud-global", "radeon-cloud-user", "xinwei"})


class MirrorError(RuntimeError):
    """Raised when no Harbor source contains the requested image."""


def _normalize_input(user_input: str) -> str:
    """Strip leading Harbor host and/or known project prefix; default missing tag to :latest.

    Examples (with default config):
      ``crpi-xxx.aliyuncs.com/ns/img:tag``                -> ``crpi-xxx.aliyuncs.com/ns/img:tag``
      ``radeon-cloud-global/crpi-xxx.aliyuncs.com/ns/img:tag`` -> ``crpi-xxx.aliyuncs.com/ns/img:tag``
      ``10.5.10.12:1808/radeon-cloud-global/crpi-xxx/ns/img:tag`` -> ``crpi-xxx/ns/img:tag``
    """
    ref = (user_input or "").strip()
    if not ref:
        raise MirrorError("image ref is empty")
    if "@" in ref:
        raise MirrorError(f"digest refs are not supported: {ref!r}")

    for source in settings.HARBOR_IMAGE_SOURCES:
        prefix = f"{source}/"
        if ref.startswith(prefix):
            ref = ref[len(prefix):]
            break

    first_segment = ref.split("/", 1)[0] if "/" in ref else ""
    if first_segment in _KNOWN_HARBOR_PROJECTS:
        ref = ref.split("/", 1)[1]

    if not ref:
        raise MirrorError(f"could not derive a repo path from: {user_input!r}")

    last_slash = ref.rfind("/")
    last_colon = ref.rfind(":")
    if last_colon <= last_slash:
        ref = f"{ref}:latest"

    return ref


def candidate_harbor_refs(remainder: str) -> list[str]:
    """Build candidate refs from each configured Harbor source."""
    return [f"{source}/{remainder}" for source in settings.HARBOR_IMAGE_SOURCES]


def resolve_existing_harbor_ref(user_input: str) -> str:
    """Verify which Harbor source contains the image; return the full ref.

    Tries each ``HARBOR_IMAGE_SOURCES`` candidate in order via ``skopeo inspect``.
    Returns the first candidate that exists. Raises ``MirrorError`` if none found.

    Blocking (subprocess) -- callers MUST invoke via ``asyncio.to_thread``."""
    remainder = _normalize_input(user_input)
    candidates = candidate_harbor_refs(remainder)

    authfile = settings.HARBOR_AUTH_CONFIG_PATH.strip()
    if not authfile or not os.path.exists(authfile):
        raise MirrorError(f"Harbor auth config not found at {authfile!r}")

    timeout = settings.HARBOR_RESOLVE_TIMEOUT_SECONDS
    errors: list[str] = []

    for ref in candidates:
        try:
            proc = subprocess.run(
                ["skopeo", "inspect", "--tls-verify=false", "--authfile", authfile,
                 f"docker://{ref}"],
                capture_output=True, text=True, timeout=timeout,
            )
            if proc.returncode == 0:
                logger.info("Resolved image to %s", ref)
                return ref
            tail = (proc.stderr or proc.stdout or "").strip()[-300:]
            errors.append(f"{ref}: rc={proc.returncode} {tail}")
        except subprocess.TimeoutExpired:
            errors.append(f"{ref}: timed out after {timeout}s")
        except FileNotFoundError:
            raise MirrorError("skopeo binary not found in the manager image")

    raise MirrorError(
        f"Image not found in any Harbor source. Tried:\n" +
        "\n".join(f"  - {e}" for e in errors)
    )


def image_matches_harbor_source(image_ref: str) -> bool:
    """True if ``image_ref`` starts with any configured Harbor source prefix."""
    ref = (image_ref or "").strip()
    return any(ref.startswith(f"{source}/") for source in settings.HARBOR_IMAGE_SOURCES)
