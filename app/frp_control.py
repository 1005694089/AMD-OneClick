"""Server-to-server client for the FRP Control API.

This module never handles FRPS's shared token. The only credential it reads is
the RC backend Control API token, preferably from a read-only mounted file.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import httpx

from .config import settings


class FrpControlError(RuntimeError):
    """A sanitized Control API or transport failure."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 0,
        code: str = "control_api_unavailable",
        request_id: str = "",
    ):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.request_id = request_id


class FrpControlClient:
    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        transport: Optional[httpx.BaseTransport] = None,
    ):
        self.base_url = (base_url if base_url is not None else settings.FRP_CONTROL_API_URL).rstrip("/")
        self._explicit_token = token
        self._transport = transport

    @staticmethod
    def configured() -> bool:
        return bool(
            settings.FRP_CONTROL_API_URL
            and (settings.FRP_CONTROL_API_TOKEN or settings.FRP_CONTROL_API_TOKEN_FILE)
        )

    def _token(self) -> str:
        if self._explicit_token is not None:
            token = self._explicit_token.strip()
        elif settings.FRP_CONTROL_API_TOKEN:
            token = settings.FRP_CONTROL_API_TOKEN.strip()
        else:
            path = Path(settings.FRP_CONTROL_API_TOKEN_FILE)
            try:
                token = path.read_text(encoding="utf-8").strip()
            except OSError as exc:
                raise FrpControlError("FRP Control API credential is unavailable") from exc
        if len(token) < 32:
            raise FrpControlError("FRP Control API credential is invalid")
        return token

    @staticmethod
    def _verify() -> bool | str:
        return settings.FRP_CONTROL_API_CA_FILE or True

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        if not self.base_url:
            raise FrpControlError("FRP Control API URL is not configured")
        headers = {
            "Authorization": f"Bearer {self._token()}",
            "Accept": "application/json",
            "User-Agent": "amd-oneclick-frp-control/1.0",
        }
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key

        timeout = httpx.Timeout(
            connect=max(1, settings.FRP_CONTROL_CONNECT_TIMEOUT_SECONDS),
            read=max(1, settings.FRP_CONTROL_READ_TIMEOUT_SECONDS),
            write=max(1, settings.FRP_CONTROL_READ_TIMEOUT_SECONDS),
            pool=max(1, settings.FRP_CONTROL_CONNECT_TIMEOUT_SECONDS),
        )
        try:
            with httpx.Client(
                verify=self._verify(),
                timeout=timeout,
                follow_redirects=False,
                trust_env=False,
                transport=self._transport,
            ) as client:
                response = client.request(
                    method,
                    f"{self.base_url}{path}",
                    headers=headers,
                    json=json_body,
                    params=params,
                )
        except (httpx.TimeoutException, httpx.NetworkError, httpx.ProtocolError) as exc:
            raise FrpControlError("FRP Control API is unreachable") from exc

        request_id = response.headers.get("X-Request-ID", "")
        try:
            payload = response.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise FrpControlError(
                "FRP Control API returned an invalid response",
                status_code=response.status_code,
                request_id=request_id,
            ) from exc
        if not isinstance(payload, dict):
            raise FrpControlError(
                "FRP Control API returned an invalid response",
                status_code=response.status_code,
                request_id=request_id,
            )
        if response.status_code >= 400:
            raise FrpControlError(
                str(payload.get("message") or "FRP Control API rejected the request"),
                status_code=response.status_code,
                code=str(payload.get("code") or "control_api_error"),
                request_id=str(payload.get("request_id") or request_id),
            )
        if response.status_code < 200 or response.status_code >= 300:
            raise FrpControlError(
                "FRP Control API returned an unexpected status",
                status_code=response.status_code,
                request_id=request_id,
            )
        return payload

    def create_tunnel(self, request: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        return self._request(
            "POST", "/api/v1/tunnels", json_body=request, idempotency_key=idempotency_key
        )

    def get_tunnel(self, tunnel_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/v1/tunnels/{quote(tunnel_id, safe='')}")

    def list_tunnels(self, owner_id: str) -> list[dict[str, Any]]:
        payload = self._request("GET", "/api/v1/tunnels", params={"owner_id": owner_id})
        tunnels = payload.get("tunnels", [])
        if not isinstance(tunnels, list):
            raise FrpControlError("FRP Control API returned an invalid tunnel list")
        return [item for item in tunnels if isinstance(item, dict)]

    def delete_tunnel(self, tunnel_id: str) -> dict[str, Any]:
        return self._request("DELETE", f"/api/v1/tunnels/{quote(tunnel_id, safe='')}")

    def rotate_credentials(self, tunnel_id: str) -> dict[str, Any]:
        return self._request(
            "POST", f"/api/v1/tunnels/{quote(tunnel_id, safe='')}/credentials/rotate"
        )

    def domain_availability(self, prefix: str, owner_id: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/api/v1/domain-prefixes/{quote(prefix, safe='')}/availability",
            params={"owner_id": owner_id},
        )


frp_control_client = FrpControlClient()
