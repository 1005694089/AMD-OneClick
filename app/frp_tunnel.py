"""Managed FRP tunnel lifecycle for AMD OneClick notebook Pods."""
from __future__ import annotations

import logging
import re
import secrets
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

from . import store
from .config import settings
from .frp_control import FrpControlClient, FrpControlError, frp_control_client


logger = logging.getLogger(__name__)

PREFIX_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{1,30}[a-z0-9])$")
ACTIVE_STATUSES = {"pending", "connecting", "active", "degraded"}


class FrpTunnelError(RuntimeError):
    def __init__(self, message: str, *, status_code: int = 400, code: str = "invalid_tunnel"):
        super().__init__(message)
        self.status_code = status_code
        self.code = code


def owner_id(user_id: int) -> str:
    return f"user-{int(user_id)}"


def _csv_set(raw: str) -> set[str]:
    return {part.strip().lower() for part in (raw or "").split(",") if part.strip()}


def _excluded_ports() -> set[int]:
    ports: set[int] = set()
    for value in _csv_set(settings.FRP_EXCLUDED_PORTS):
        try:
            ports.add(int(value))
        except ValueError:
            logger.error("Ignoring malformed FRP_EXCLUDED_PORTS value %r", value)
    return ports


def normalize_prefix(prefix: str) -> str:
    normalized = (prefix or "").strip().lower()
    if not PREFIX_PATTERN.fullmatch(normalized):
        raise FrpTunnelError("Domain prefix must be 3-32 lowercase letters, digits, or hyphens")
    if normalized in _csv_set(settings.FRP_RESERVED_PREFIXES):
        raise FrpTunnelError("Domain prefix is reserved", code="reserved_prefix")
    return normalized


def normalize_request(prefix: str, local_port: int) -> tuple[str, int]:
    normalized = normalize_prefix(prefix)
    try:
        port = int(local_port)
    except (TypeError, ValueError) as exc:
        raise FrpTunnelError("Local port must be an integer") from exc
    if port < 1024 or port > 65535 or port in _excluded_ports():
        raise FrpTunnelError("Local port is not allowed", code="invalid_port")
    return normalized, port


def validate_configuration() -> None:
    """Fail closed when operators enable an incomplete production integration."""
    if not settings.FRP_TUNNEL_ENABLED:
        return
    missing = []
    for name in (
        "FRP_CONTROL_API_URL",
        "FRP_AGENT_IMAGE",
        "FRP_PLATFORM_SECRET_NAME",
        "FRP_PLATFORM_TOKEN_KEY",
        "FRP_CLUSTER_ID",
        "FRP_DOMAIN_SUFFIX",
    ):
        if not getattr(settings, name, ""):
            missing.append(name)
    if missing:
        raise RuntimeError(f"FRP tunnel configuration is incomplete: {', '.join(missing)}")
    parsed = urlparse(settings.FRP_CONTROL_API_URL)
    if parsed.scheme != "https" or not parsed.hostname:
        raise RuntimeError("FRP_CONTROL_API_URL must be an HTTPS URL")
    if settings.FRP_AGENT_IMAGE_PULL_POLICY not in {"Always", "IfNotPresent", "Never"}:
        raise RuntimeError("FRP_AGENT_IMAGE_PULL_POLICY is invalid")
    if "@sha256:" not in settings.FRP_AGENT_IMAGE:
        raise RuntimeError("FRP_AGENT_IMAGE must be pinned by sha256 digest")
    if not settings.FRP_CONTROL_API_TOKEN:
        token_path = Path(settings.FRP_CONTROL_API_TOKEN_FILE)
        if not token_path.is_file():
            raise RuntimeError("FRP Control API token file is missing")
        if token_path.stat().st_size > 4096:
            raise RuntimeError("FRP Control API token file is invalid")


def _control_error(exc: FrpControlError) -> FrpTunnelError:
    if exc.code in {
        "domain_conflict",
        "domain_quarantined",
        "pod_tunnel_limit",
        "idempotency_conflict",
    }:
        return FrpTunnelError(str(exc), status_code=409, code=exc.code)
    if exc.status_code == 400:
        return FrpTunnelError(str(exc), status_code=400, code=exc.code)
    if exc.status_code == 401:
        return FrpTunnelError(
            "Tunnel service authentication failed", status_code=502, code="control_auth_failed"
        )
    return FrpTunnelError(
        "Tunnel service is temporarily unavailable", status_code=502, code=exc.code
    )


def _unwrap_tunnel(payload: dict[str, Any]) -> dict[str, Any]:
    tunnel = payload.get("tunnel")
    if not isinstance(tunnel, dict) or not tunnel.get("tunnel_id"):
        raise FrpTunnelError(
            "Tunnel service returned an invalid response", status_code=502, code="invalid_control_response"
        )
    return tunnel


def _public(tunnel: Optional[dict[str, Any]], *, control_available: bool = True) -> dict[str, Any]:
    if not tunnel:
        return {
            "enabled": bool(settings.FRP_TUNNEL_ENABLED),
            "configured": False,
            "domain_suffix": settings.FRP_DOMAIN_SUFFIX,
            "control_available": control_available,
        }
    fqdn = tunnel.get("fqdn") or ""
    return {
        "enabled": bool(settings.FRP_TUNNEL_ENABLED),
        "configured": bool(tunnel.get("tunnel_id")),
        "tunnel_id": tunnel.get("tunnel_id"),
        "domain_prefix": tunnel.get("domain_prefix"),
        "domain_suffix": settings.FRP_DOMAIN_SUFFIX,
        "fqdn": fqdn,
        "url": f"https://{fqdn}" if fqdn else None,
        "local_port": tunnel.get("local_port"),
        "status": tunnel.get("status") or "unknown",
        "quarantine_until": tunnel.get("quarantine_until"),
        "updated_at": tunnel.get("updated_at"),
        "control_available": control_available,
    }


def _record_tunnel(record: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not record.get("frp_tunnel_id"):
        return None
    return {
        "tunnel_id": record.get("frp_tunnel_id"),
        "domain_prefix": record.get("frp_domain_prefix"),
        "fqdn": record.get("frp_fqdn"),
        "local_port": record.get("frp_local_port"),
        "status": record.get("frp_tunnel_status") or "unknown",
        "updated_at": record.get("frp_tunnel_updated_at"),
    }


def _verify_remote_tunnel(
    tunnel: dict[str, Any],
    *,
    expected_owner: str,
    pod_uid: str,
    prefix: str,
    local_port: int,
) -> None:
    expected = {
        "owner_id": expected_owner,
        "pod_uid": pod_uid,
        "domain_prefix": prefix,
        "local_port": local_port,
    }
    for key, value in expected.items():
        actual = tunnel.get(key)
        if key == "local_port":
            try:
                actual = int(actual)
            except (TypeError, ValueError):
                pass
        if actual != value:
            raise FrpTunnelError(
                "Tunnel service response did not match the requested Pod",
                status_code=502,
                code="control_identity_mismatch",
            )


def _find_pod_tunnel(
    client: FrpControlClient, expected_owner: str, pod_uid: str
) -> Optional[dict[str, Any]]:
    for tunnel in client.list_tunnels(expected_owner):
        if tunnel.get("pod_uid") == pod_uid and tunnel.get("status") in ACTIVE_STATUSES:
            return tunnel
    return None


def get_tunnel_status(
    user: dict[str, Any],
    record: dict[str, Any],
    *,
    client: FrpControlClient = frp_control_client,
) -> dict[str, Any]:
    local = _record_tunnel(record)
    if not local:
        return _public(None)
    try:
        remote = _unwrap_tunnel(client.get_tunnel(str(local["tunnel_id"])))
        if remote.get("owner_id") != owner_id(user["id"]):
            raise FrpTunnelError(
                "Tunnel ownership mismatch", status_code=502, code="control_identity_mismatch"
            )
        store.set_instance_tunnel(
            record["instance_id"],
            remote["tunnel_id"],
            remote.get("domain_prefix") or local.get("domain_prefix") or "",
            remote.get("fqdn") or local.get("fqdn") or "",
            int(remote.get("local_port") or local.get("local_port") or 0),
            remote.get("status") or "unknown",
        )
        return _public(remote)
    except FrpControlError:
        logger.warning("FRP Control API status refresh failed for %s", record["instance_id"])
        return _public(local, control_available=False)


def create_tunnel(
    user: dict[str, Any],
    record: dict[str, Any],
    prefix: str,
    local_port: int,
    k8s_client,
    *,
    client: FrpControlClient = frp_control_client,
) -> dict[str, Any]:
    instance_id = record["instance_id"]
    with store.instance_tunnel_lock(instance_id):
        latest = store.get_instance_record(instance_id) or record
        return _create_tunnel_locked(
            user, latest, prefix, local_port, k8s_client, client=client
        )


def _create_tunnel_locked(
    user: dict[str, Any],
    record: dict[str, Any],
    prefix: str,
    local_port: int,
    k8s_client,
    *,
    client: FrpControlClient = frp_control_client,
) -> dict[str, Any]:
    if not settings.FRP_TUNNEL_ENABLED:
        raise FrpTunnelError("Public tunnels are not enabled", status_code=404, code="disabled")
    prefix, local_port = normalize_request(prefix, local_port)
    instance_id = record["instance_id"]
    if int(record["user_id"]) != int(user["id"]):
        raise FrpTunnelError("Instance not found", status_code=404, code="not_found")
    if not k8s_client.pod_has_container(instance_id, "frpc"):
        raise FrpTunnelError(
            "This instance predates tunnel support and must be restarted",
            status_code=409,
            code="agent_missing",
        )
    identity = k8s_client.get_pod_identity(instance_id)
    expected_owner = owner_id(user["id"])

    local = _record_tunnel(record)
    if (
        local
        and local.get("domain_prefix") == prefix
        and int(local.get("local_port") or 0) == local_port
        and local.get("status") in ACTIVE_STATUSES
        and k8s_client.tunnel_secret_exists(instance_id)
    ):
        return get_tunnel_status(user, record, client=client)
    if (
        local
        and local.get("status") in ACTIVE_STATUSES
        and (
            local.get("domain_prefix") != prefix
            or int(local.get("local_port") or 0) != local_port
        )
    ):
        raise FrpTunnelError(
            "Delete the current tunnel before changing its domain or port",
            status_code=409,
            code="tunnel_already_configured",
        )

    request = {
        "owner_id": expected_owner,
        "cluster_id": settings.FRP_CLUSTER_ID,
        "namespace": identity["namespace"],
        "pod_name": identity["pod_name"],
        "pod_uid": identity["pod_uid"],
        "domain_prefix": prefix,
        "local_port": local_port,
    }
    # A fresh activation needs a fresh key so the original owner can reclaim a
    # quarantined prefix. Lost-response recovery is handled by listing the
    # authoritative Pod tunnel and rotating its one-time credentials below.
    idempotency_key = f"oneclick-{secrets.token_hex(24)}"

    try:
        response = client.create_tunnel(request, idempotency_key)
    except FrpControlError as exc:
        if exc.code not in {"pod_tunnel_limit", "domain_conflict"}:
            raise _control_error(exc) from exc
        try:
            existing = _find_pod_tunnel(client, expected_owner, identity["pod_uid"])
        except FrpControlError as list_exc:
            raise _control_error(list_exc) from list_exc
        if not existing:
            raise _control_error(exc) from exc
        if existing.get("domain_prefix") != prefix or int(existing.get("local_port") or 0) != local_port:
            raise FrpTunnelError(
                "Delete the current tunnel before changing its domain or port",
                status_code=409,
                code="tunnel_already_configured",
            )
        try:
            response = client.rotate_credentials(existing["tunnel_id"])
        except FrpControlError as rotate_exc:
            raise _control_error(rotate_exc) from rotate_exc

    tunnel = _unwrap_tunnel(response)
    credentials = response.get("agent_credentials")
    if not isinstance(credentials, dict):
        try:
            response = client.rotate_credentials(tunnel["tunnel_id"])
        except FrpControlError as exc:
            raise _control_error(exc) from exc
        tunnel = _unwrap_tunnel(response)
        credentials = response.get("agent_credentials")
    if not isinstance(credentials, dict):
        raise FrpTunnelError(
            "Tunnel service did not return one-time credentials",
            status_code=502,
            code="credentials_missing",
        )

    _verify_remote_tunnel(
        tunnel,
        expected_owner=expected_owner,
        pod_uid=identity["pod_uid"],
        prefix=prefix,
        local_port=local_port,
    )
    try:
        k8s_client.upsert_tunnel_secret(instance_id, tunnel, credentials)
    except Exception as exc:
        try:
            client.delete_tunnel(tunnel["tunnel_id"])
        except FrpControlError:
            logger.exception("Failed to revoke tunnel after Secret installation failure")
        store.set_instance_tunnel(
            instance_id,
            tunnel["tunnel_id"],
            prefix,
            tunnel.get("fqdn") or f"{prefix}.{settings.FRP_DOMAIN_SUFFIX}",
            local_port,
            "secret_failed",
        )
        raise FrpTunnelError(
            "Unable to install tunnel credentials in Kubernetes",
            status_code=502,
            code="secret_install_failed",
        ) from exc
    finally:
        credentials["client_secret"] = ""

    store.set_instance_tunnel(
        instance_id,
        tunnel["tunnel_id"],
        prefix,
        tunnel.get("fqdn") or f"{prefix}.{settings.FRP_DOMAIN_SUFFIX}",
        local_port,
        tunnel.get("status") or "pending",
    )
    return _public(tunnel)


def delete_tunnel(
    user: dict[str, Any],
    record: dict[str, Any],
    k8s_client,
    *,
    client: FrpControlClient = frp_control_client,
) -> dict[str, Any]:
    instance_id = record["instance_id"]
    with store.instance_tunnel_lock(instance_id):
        latest = store.get_instance_record(instance_id) or record
        return _delete_tunnel_locked(user, latest, k8s_client, client=client)


def _delete_tunnel_locked(
    user: dict[str, Any],
    record: dict[str, Any],
    k8s_client,
    *,
    client: FrpControlClient = frp_control_client,
) -> dict[str, Any]:
    if int(record["user_id"]) != int(user["id"]):
        raise FrpTunnelError("Instance not found", status_code=404, code="not_found")
    instance_id = record["instance_id"]
    try:
        k8s_client.disable_tunnel_secret(instance_id)
    except Exception as exc:
        raise FrpTunnelError(
            "Unable to stop the tunnel agent", status_code=502, code="secret_delete_failed"
        ) from exc

    local = _record_tunnel(record)
    if not local:
        return _public(None)
    try:
        remote = _unwrap_tunnel(client.delete_tunnel(str(local["tunnel_id"])))
        store.update_instance_tunnel_status(instance_id, remote.get("status") or "quarantine")
        return _public(remote)
    except FrpControlError as exc:
        if exc.status_code == 404:
            store.update_instance_tunnel_status(instance_id, "released")
            local["status"] = "released"
            return _public(local)
        # The Secret is gone, so the local supervisor stops FRPC even when the
        # remote control plane is unavailable. Its stale-session reaper remains
        # the secondary cleanup path.
        store.update_instance_tunnel_status(instance_id, "delete_pending")
        local["status"] = "delete_pending"
        result = _public(local, control_available=False)
        result["warning"] = "Tunnel stopped locally; remote cleanup is pending"
        return result


def domain_availability(
    user: dict[str, Any],
    prefix: str,
    *,
    client: FrpControlClient = frp_control_client,
) -> dict[str, Any]:
    if not settings.FRP_TUNNEL_ENABLED:
        raise FrpTunnelError("Public tunnels are not enabled", status_code=404, code="disabled")
    prefix = normalize_prefix(prefix)
    try:
        result = client.domain_availability(prefix, owner_id(user["id"]))
    except FrpControlError as exc:
        raise _control_error(exc) from exc
    return {**result, "domain_prefix": prefix, "fqdn": f"{prefix}.{settings.FRP_DOMAIN_SUFFIX}"}


def cleanup_instance_tunnel(instance_id: str, k8s_client) -> None:
    """Best-effort cleanup used by every instance deletion path."""
    with store.instance_tunnel_lock(instance_id):
        record = store.get_instance_record(instance_id)
        if not record or not record.get("frp_tunnel_id"):
            return
        try:
            k8s_client.disable_tunnel_secret(instance_id)
        except Exception:
            logger.exception("Failed to remove FRP Secret while deleting instance %s", instance_id)
        if not FrpControlClient.configured():
            store.update_instance_tunnel_status(instance_id, "delete_pending")
            return
        try:
            remote = _unwrap_tunnel(frp_control_client.delete_tunnel(record["frp_tunnel_id"]))
            store.update_instance_tunnel_status(instance_id, remote.get("status") or "quarantine")
        except (FrpControlError, FrpTunnelError):
            logger.exception("Failed to revoke FRP tunnel while deleting instance %s", instance_id)
            store.update_instance_tunnel_status(instance_id, "delete_pending")
