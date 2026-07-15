#!/usr/bin/env python3
"""Supervise one dynamically configured FRPC process and forward its logs."""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


CREDENTIAL_DIR = pathlib.Path(os.getenv("FRP_CREDENTIAL_DIR", "/run/frp/tunnel"))
GLOBAL_TOKEN_FILE = pathlib.Path(os.getenv("FRP_GLOBAL_TOKEN_FILE", "/run/frp/platform/global-token"))
FRPC_BIN = os.getenv("FRPC_BIN", "/usr/local/bin/frpc")
RUNTIME_DIR = pathlib.Path(os.getenv("FRP_RUNTIME_DIR", "/var/lib/frp-agent"))
LOG_PATH = pathlib.Path(os.getenv("FRPC_LOG_PATH", "/var/log/frpc/frpc.log"))
INGEST_URL = os.getenv("FRP_LOG_INGEST_URL", "").strip()
BANDWIDTH_LIMIT = os.getenv("FRP_BANDWIDTH_LIMIT", "2500KB").strip()
DOMAIN_SUFFIX = os.getenv("FRP_DOMAIN_SUFFIX", "radeon.firstdg.ai").strip().lower().strip(".")
POD_NAMESPACE = os.environ.get("POD_NAMESPACE", "")
POD_NAME = os.environ.get("POD_NAME", "")
POD_UID = os.environ.get("POD_UID", "")
NODE_NAME = os.environ.get("NODE_NAME", "")
POLL_SECONDS = max(float(os.getenv("FRP_AGENT_POLL_SECONDS", "2")), 0.5)
BATCH_SIZE = min(max(int(os.getenv("FRP_LOG_BATCH_SIZE", "50")), 1), 100)
PREFIX_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{1,30}[a-z0-9])$")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def toml_string(value: str) -> str:
    # JSON quoted strings are valid TOML basic strings and correctly escape secrets.
    return json.dumps(value, ensure_ascii=True)


@dataclass(frozen=True)
class TunnelConfig:
    client_id: str
    client_secret: str
    server_address: str
    server_port: int
    domain_prefix: str
    local_port: int

    @property
    def fingerprint(self) -> str:
        raw = "\0".join(
            [
                self.client_id,
                self.client_secret,
                self.server_address,
                str(self.server_port),
                self.domain_prefix,
                str(self.local_port),
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _read_required(name: str) -> str:
    value = (CREDENTIAL_DIR / name).read_text(encoding="utf-8").strip()
    if not value or "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError(f"invalid {name}")
    return value


def load_tunnel_config() -> Optional[TunnelConfig]:
    required = (
        "client-id",
        "client-secret",
        "server-address",
        "server-port",
        "domain-prefix",
        "local-port",
    )
    if not all((CREDENTIAL_DIR / name).is_file() for name in required):
        return None
    try:
        config = TunnelConfig(
            client_id=_read_required("client-id"),
            client_secret=_read_required("client-secret"),
            server_address=_read_required("server-address"),
            server_port=int(_read_required("server-port")),
            domain_prefix=_read_required("domain-prefix").lower(),
            local_port=int(_read_required("local-port")),
        )
    except (OSError, ValueError):
        return None
    if len(config.client_secret) < 32:
        return None
    if not PREFIX_PATTERN.fullmatch(config.domain_prefix):
        return None
    if not (1 <= config.server_port <= 65535 and 1024 <= config.local_port <= 65535):
        return None
    return config


def render_frpc_config(config: TunnelConfig) -> str:
    fqdn = f"{config.domain_prefix}.{DOMAIN_SUFFIX}"
    return f"""serverAddr = {toml_string(config.server_address)}
serverPort = {config.server_port}
user = {toml_string(config.client_id)}
loginFailExit = false

auth.method = "token"
auth.tokenSource.type = "file"
auth.tokenSource.file.path = {toml_string(str(GLOBAL_TOKEN_FILE))}

transport.protocol = "tcp"
transport.tls.enable = true
transport.heartbeatInterval = 30
transport.heartbeatTimeout = 90

log.to = {toml_string(str(LOG_PATH))}
log.level = "info"
log.maxDays = 1
log.disablePrintColor = true

metadatas.client_secret = {toml_string(config.client_secret)}
metadatas.pod_uid = {toml_string(POD_UID)}
metadatas.node_name = {toml_string(NODE_NAME)}

[[proxies]]
name = {toml_string(config.client_id)}
type = "http"
localIP = "127.0.0.1"
localPort = {config.local_port}
subdomain = {toml_string(config.domain_prefix)}
metadatas.local_port = {toml_string(str(config.local_port))}
transport.bandwidthLimit = {toml_string(BANDWIDTH_LIMIT)}
transport.bandwidthLimitMode = "server"
healthCheck.type = "tcp"
healthCheck.intervalSeconds = 10
healthCheck.timeoutSeconds = 3
healthCheck.maxFailed = 3
"""


class Agent:
    def __init__(self):
        self.stop_event = threading.Event()
        self.process: Optional[subprocess.Popen] = None
        self.current: Optional[TunnelConfig] = None
        self.config_path = RUNTIME_DIR / "frpc.toml"
        self.offset_path = RUNTIME_DIR / "frpc.offset"
        self.restart_after = 0.0
        self.restart_delay = 1.0

    def stop_frpc(self) -> None:
        process = self.process
        self.process = None
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)

    def start_frpc(self, config: TunnelConfig) -> None:
        RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.config_path.with_suffix(".tmp")
        temporary.write_text(render_frpc_config(config), encoding="utf-8")
        temporary.chmod(0o600)
        temporary.replace(self.config_path)
        self.process = subprocess.Popen([FRPC_BIN, "-c", str(self.config_path)])
        self.current = config
        self.restart_delay = 1.0
        print(
            f"FRP tunnel agent started for {config.domain_prefix}.{DOMAIN_SUFFIX}:{config.local_port}",
            flush=True,
        )

    def supervise(self) -> None:
        if not pathlib.Path(FRPC_BIN).is_file() or not os.access(FRPC_BIN, os.X_OK):
            raise RuntimeError("frpc binary is unavailable")
        if not GLOBAL_TOKEN_FILE.is_file():
            raise RuntimeError("FRPS platform token is unavailable")

        threading.Thread(target=self.forward_logs, name="frp-log-forwarder", daemon=True).start()
        while not self.stop_event.is_set():
            desired = load_tunnel_config()
            desired_fingerprint = desired.fingerprint if desired else ""
            current_fingerprint = self.current.fingerprint if self.current else ""

            if desired_fingerprint != current_fingerprint:
                self.stop_frpc()
                self.current = desired
                self.restart_after = 0.0
                if desired is None:
                    print("FRP tunnel agent is waiting for configuration", flush=True)

            if self.process is not None and self.process.poll() is not None:
                exit_code = self.process.returncode
                self.process = None
                self.restart_after = time.monotonic() + self.restart_delay
                self.restart_delay = min(self.restart_delay * 2, 30.0)
                print(f"frpc exited with code {exit_code}; retrying", file=sys.stderr, flush=True)

            if desired is not None and self.process is None and time.monotonic() >= self.restart_after:
                self.start_frpc(desired)

            self.stop_event.wait(POLL_SECONDS)

        self.stop_frpc()

    def _load_offset(self) -> int:
        try:
            return max(int(self.offset_path.read_text(encoding="ascii").strip()), 0)
        except (OSError, ValueError):
            return 0

    def _save_offset(self, value: int) -> None:
        temporary = self.offset_path.with_suffix(".offset.tmp")
        temporary.write_text(str(value), encoding="ascii")
        temporary.replace(self.offset_path)

    def _post_records(self, records: list[dict], config: TunnelConfig) -> None:
        payload = json.dumps({"records": records}, separators=(",", ":")).encode("utf-8")
        request = urllib.request.Request(
            INGEST_URL,
            data=payload,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "User-Agent": "amd-oneclick-frp-agent/1.0",
                "X-Frp-Client-ID": config.client_id,
                "X-Frp-Client-Secret": config.client_secret,
            },
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            if response.status != 202:
                raise RuntimeError(f"log ingest returned HTTP {response.status}")

    def forward_logs(self) -> None:
        if not INGEST_URL:
            return
        offset = self._load_offset()
        backoff = 1
        while not self.stop_event.is_set():
            config = self.current
            try:
                if config is None or not LOG_PATH.exists():
                    self.stop_event.wait(1)
                    continue
                size = LOG_PATH.stat().st_size
                if size < offset:
                    offset = 0
                    self._save_offset(offset)
                with LOG_PATH.open("r", encoding="utf-8", errors="replace") as handle:
                    handle.seek(offset)
                    records = []
                    while len(records) < BATCH_SIZE:
                        line = handle.readline()
                        if not line:
                            break
                        message = line.rstrip("\r\n")[:8192]
                        if message:
                            records.append(
                                {
                                    "source": "frpc",
                                    "observed_at": utc_now(),
                                    "namespace": POD_NAMESPACE,
                                    "pod_name": POD_NAME,
                                    "pod_uid": POD_UID,
                                    "node_name": NODE_NAME,
                                    "container": "frpc",
                                    "level": "info",
                                    "message": message,
                                }
                            )
                    next_offset = handle.tell()
                if not records:
                    self.stop_event.wait(1)
                    continue
                self._post_records(records, config)
                offset = next_offset
                self._save_offset(offset)
                backoff = 1
            except (OSError, RuntimeError, urllib.error.URLError) as exc:
                print(f"FRP log forwarding unavailable: {type(exc).__name__}", file=sys.stderr, flush=True)
                self.stop_event.wait(backoff)
                backoff = min(backoff * 2, 30)


def main() -> int:
    agent = Agent()

    def shutdown(_signum, _frame):
        agent.stop_event.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        agent.supervise()
        return 0
    except Exception as exc:
        print(f"FRP tunnel agent failed: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
