#!/usr/bin/env python3
"""
AMD OneClick image-service agent.

Polls the manager for pending image jobs and executes them on a dedicated,
cordoned CPU cluster member (e.g. wx-ms-w7900d-0042) that has a large image
volume mounted at /disk/ssd2 and direct routed SSH access to every GPU node's
InternalIP. One multi-verb daemon replaces the single-verb build-agent:

  build        build an image locally from a Dockerfile (clean context)
  pull         pull an image ref from Docker Hub / ACR
  acr_backup   tag + push an image to the ACR Enterprise backup registry
  distribute   save | ssh <node> 'ctr -n k8s.io images import -' to GPU nodes
  evict        ssh <node> 'ctr -n k8s.io images rm <ref> && content prune'

It also runs the outdated reaper as a singleton (by construction — one daemon).

This process holds the node-distribution SSH key (≈root on every GPU node) and
the ACR Enterprise push credential (via a dedicated DOCKER_CONFIG). User-supplied
Dockerfile RUN/COPY steps execute inside ephemeral build containers and MUST NOT
be able to reach either: builds use a clean tempdir context (only the Dockerfile
is written), the SSH key and DOCKER_CONFIG are never passed as --build-arg/--secret/
--ssh and never exported into the build environment, and build-time egress fails
closed unless BUILD_NETWORK is explicitly chosen.

Hardening expectations (enforced by the systemd unit / host setup, not this script):
  - dedicated low-privilege user (no docker group); rootless docker/podman recommended
  - ProtectHome=tmpfs so real /home is not readable (BindPaths re-exposes /run/user)
  - DISTRIB_SSH_KEY lives outside $HOME (under /disk/ssd2/.ssh) so ProtectHome keeps it
  - DOCKER_CONFIG points outside $HOME so registry creds survive ProtectHome
  - optional restricted docker network for build-time egress control

Only the Python standard library is used so the agent needs no pip installs.
"""
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor


def _env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and not value:
        print(f"[agent] FATAL: missing required env {name}", file=sys.stderr)
        sys.exit(2)
    return value


MANAGER_URL = _env("MANAGER_URL", required=True).rstrip("/")
BUILD_AGENT_TOKEN = _env("BUILD_AGENT_TOKEN", required=True)
AGENT_ID = _env("AGENT_ID", "image-service-1")
POLL_INTERVAL = float(_env("POLL_INTERVAL_SECONDS", "10"))
# How often the background heartbeat thread refreshes a running job's lease. Must be well under the
# manager's JOB_LEASE_TIMEOUT_SECONDS (3600s) so a long warm/pull (10-19 GB, one blocking call) is
# never reaped as stale while it is genuinely making progress.
HEARTBEAT_INTERVAL = float(_env("HEARTBEAT_INTERVAL_SECONDS", "30"))
# Generalized DOCKER_BIN -> CONTAINER_CLI: docker | nerdctl. build/pull/push/save map 1:1.
# Default to rootless nerdctl: 0042 runs nerdctl as a CLIENT of the imagesvc-owned rootless
# containerd+buildkit user services (no sudo, no docker group). Defaulting to "docker" here would
# be a landmine — a missing CONTAINER_CLI in the env file would silently fall back to the root-
# equivalent system docker daemon and defeat the rootless design.
CONTAINER_CLI = _env("CONTAINER_CLI", "nerdctl")
BUILD_TIMEOUT = int(_env("BUILD_TIMEOUT_SECONDS", "1800"))
BUILD_MEMORY = _env("BUILD_MEMORY", "8g")
BUILD_CPUSET = _env("BUILD_CPUSET", "")            # e.g. "0-3"; empty = no pin
BUILD_NETWORK = _env("BUILD_NETWORK", "")          # restricted docker network name, or "none"
# User Dockerfile RUN steps execute on the build host. With the default Docker bridge they
# get egress into private/internal networks, so we fail closed: the admin must make an
# explicit choice. Set BUILD_NETWORK to "none" (no egress; breaks apt/pip), a restricted
# network name, or "default"/"bridge" to deliberately opt into unrestricted egress.
# Scoped to BUILD ONLY — pull/distribute/evict need egress / node SSH.
BUILD_NETWORK_REQUIRED = _env("BUILD_NETWORK_REQUIRED", "1") not in {"0", "false", "False", ""}
MIN_FREE_DISK_GB = float(_env("MIN_FREE_DISK_GB", "50"))
# Free space required on a target node's containerd root before we distribute to it.
IMAGE_NODE_MIN_FREE_DISK_GB = float(_env("IMAGE_NODE_MIN_FREE_DISK_GB", "50"))
IMAGE_WORK_DIR = _env("IMAGE_WORK_DIR", "/disk/ssd2")
DISK_CHECK_PATH = _env("DISK_CHECK_PATH", IMAGE_WORK_DIR)
LOG_FLUSH_SECONDS = float(_env("LOG_FLUSH_SECONDS", "3"))
# docker's classic builder honors --memory/--cpuset/--network for RUN steps; default to it so
# our resource + egress limits actually apply. Override to "1" only if you front this with
# buildx + a resource-limited builder.
# NOTE: this is a DOCKER-ONLY knob. Rootless nerdctl has no classic builder — `nerdctl build`
# always drives rootless buildkitd and ignores DOCKER_BUILDKIT, and it does NOT accept --memory
# or --cpuset-cpus (those are run-time flags). When CONTAINER_CLI=nerdctl we therefore omit
# DOCKER_BUILDKIT and the two resource flags from the build command (see run_build); impose
# RUN-step resource limits on the rootless buildkitd --user service (cgroup limits) instead.
DOCKER_BUILDKIT = _env("DOCKER_BUILDKIT", "0")
# True when driving rootless buildkit via nerdctl (no classic builder; no --memory/--cpuset-cpus).
_IS_NERDCTL = os.path.basename(CONTAINER_CLI) == "nerdctl"

# Which job kinds this daemon will claim. CSV; default = all.
ALL_KINDS = [
    "build", "pull", "acr_backup", "push", "distribute", "warm", "evict",
    "purge_node", "purge_p2p", "purge_seed", "registry_delete", "purge_builder",
]
IMAGE_SERVICE_KINDS = [
    k.strip() for k in _env("IMAGE_SERVICE_KINDS", ",".join(ALL_KINDS)).split(",") if k.strip()
]

ACR_ENTERPRISE_REGISTRY = _env("ACR_ENTERPRISE_REGISTRY", "")
# Self-hosted LAN registry (zot) on 0042 — host:port, e.g. "10.5.10.43:5000". The push credential
# is already present under DOCKER_CONFIG (imagesvc logged in). Empty => run_push fails fast (the
# manager only enqueues push when its own LAN_REGISTRY is set, so this stays unreached until wired).
LAN_REGISTRY = _env("LAN_REGISTRY", "")
# P4 complete-delete config.
# zot registry credentials for the manifest DELETE (registry_delete). Basic user:pass; the agent is
# already logged in via DOCKER_CONFIG, but the DELETE is a raw curl so it needs the pair explicitly.
ZOT_USERNAME = _env("ZOT_USERNAME", "imagesvc")
ZOT_PASSWORD = _env("ZOT_PASSWORD", "")
# CA cert path so curl trusts the self-signed zot TLS (also in the system trust store).
ZOT_CA_CERT = _env("ZOT_CA_CERT", "/etc/zot/tls.crt")
# containerd hosts.toml drop-in dir. `ctr` (unlike the CRI/kubelet path) does NOT auto-read
# containerd's config_path, so run_warm must pass `ctr images pull --hosts-dir <this>` for the
# per-node dfdaemon mirror (127.0.0.1:4001) to be used. Must match config_path on the nodes.
CERTS_DIR = _env("CERTS_DIR", "/etc/containerd/certs.d")
DISTRIBUTE_CONCURRENCY = int(_env("DISTRIBUTE_CONCURRENCY", "2"))
CTR_NAMESPACE = _env("CTR_NAMESPACE", "k8s.io")
NODE_SSH_USER = _env("NODE_SSH_USER", "root")
DISTRIB_SSH_KEY = _env("DISTRIB_SSH_KEY", "")
# Hard ceiling on the REMOTE `ctr import` (wrapped in `timeout -s KILL`). stream_command's watchdog
# only kills the LOCAL ssh/cat process group on the image-service host; without this the remote ctr
# client would keep blocking inside the daemon's chain-ID mutex forever (the RCA failure mode),
# streaming 10.8GB into a wedged daemon. Keep < BUILD_TIMEOUT so the remote dies before the local.
REMOTE_IMPORT_TIMEOUT = int(_env("REMOTE_IMPORT_TIMEOUT_SECONDS", "1200"))
# Timeout for the cheap containerd liveness probe (a `ctr images ls -q | head` over SSH). A wedged
# daemon hangs this; a healthy one answers in <1s.
CONTAINERD_PROBE_TIMEOUT = int(_env("CONTAINERD_PROBE_TIMEOUT_SECONDS", "20"))
# Pod-safe automated recovery. Default OFF: the default path on a detected wedge is to quarantine +
# alert for an operator, never to restart containerd automatically. When enabled, a restart is still
# only ever issued to a node that hosts ZERO running user instance pods (checked live via the
# manager), so automated recovery can never destroy a user pod.
AUTO_CONTAINERD_RESTART_ENABLED = _env("AUTO_CONTAINERD_RESTART_ENABLED", "0") in {"1", "true", "True", "yes", "on"}
CONTAINERD_RESTART_COOLDOWN = int(_env("CONTAINERD_RESTART_COOLDOWN_SECONDS", "1800"))


def _cli(*args):
    """Local container-CLI argv, pinned to one containerd namespace for nerdctl.

    nerdctl's default namespace is "default", but the rootless buildkit containerd worker
    lands built images in its own namespace — so a bare `nerdctl build` then bare `nerdctl save`
    disagree and save reports "not found". Pin every LOCAL nerdctl verb (build/pull/save/tag/
    push/rm) to CTR_NAMESPACE so build and save share one store, matching the remote
    `ctr -n CTR_NAMESPACE import`. docker has no namespaces, so the flag is nerdctl-only.
    """
    if _IS_NERDCTL:
        return [CONTAINER_CLI, "--namespace", CTR_NAMESPACE, *args]
    return [CONTAINER_CLI, *args]


# Same as _cli but as a shell-string prefix, for the save|gzip|ssh pipeline in run_distribute.
def _cli_str(*args):
    return " ".join(_cli(*args))


# Built images are exported by buildkit straight to a docker-format tarball under IMAGE_WORK_DIR,
# rather than relying on `nerdctl save <ref>` reading the image back out of a containerd namespace.
# The rootless buildkit containerd worker does not reliably load the built ref into the namespace
# nerdctl save queries, so distribute reads this file directly instead. Keyed by a filesystem-safe
# digest of the ref so build and the follow-up distribute job agree on the path without sharing state.
IMAGE_TAR_DIR = os.path.join(IMAGE_WORK_DIR, "image-tars")


def _image_tar_path(ref):
    safe = hashlib.sha256(ref.encode("utf-8")).hexdigest()[:32]
    return os.path.join(IMAGE_TAR_DIR, f"{safe}.tar")

# Outdated reaper cadence: how often the daemon asks the manager for outdated (ref,node)
# pairs and enqueues evict jobs. Singleton by construction (one daemon polls).
OUTDATED_POLL_SECONDS = float(_env("OUTDATED_POLL_SECONDS", "300"))


def _request(path, payload, timeout=30, method="POST"):
    # GET endpoints (e.g. the user-pod count) carry no body; POST endpoints send JSON.
    data = None if method == "GET" else json.dumps(payload or {}).encode("utf-8")
    headers = {"Authorization": f"Bearer {BUILD_AGENT_TOKEN}"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        MANAGER_URL + path,
        data=data,
        method=method,
        headers=headers,
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body else {}


def claim_job(kinds=None):
    payload = {"agent_id": AGENT_ID}
    if kinds:
        payload["kinds"] = kinds
    return _request("/api/internal/jobs/claim", payload).get("job")


def mark_running(job_id):
    # The manager flips claimed->running when it accepts the first log line; we send an
    # explicit start marker so a job that produces no output still transitions.
    push_log(job_id, f"[agent] {AGENT_ID} starting job {job_id}\n")


def push_log(job_id, chunk):
    if not chunk:
        return
    try:
        _request(f"/api/internal/jobs/{job_id}/log", {"agent_id": AGENT_ID, "log": chunk})
    except Exception as exc:  # logging must never crash the job loop
        print(f"[agent] log push failed for {job_id}: {exc}", file=sys.stderr)


def report_result(job_id, status, result=None):
    payload = {"agent_id": AGENT_ID, "status": status}
    if result is not None:
        payload["result"] = result
    _request(f"/api/internal/jobs/{job_id}/result", payload)


def report_node_status(node, ref, status, quarantine_seconds=None):
    """Report a node's status (importing | quarantined | loaded) to the manager. Best-effort."""
    payload = {"agent_id": AGENT_ID, "node": node, "ref": ref, "status": status}
    if quarantine_seconds is not None:
        payload["quarantine_seconds"] = quarantine_seconds
    _request("/api/internal/nodes/status", payload)


def send_heartbeat(job_id):
    """Refresh a running job's lease. Best-effort; never crashes the job loop."""
    try:
        _request(f"/api/internal/jobs/{job_id}/heartbeat", {"agent_id": AGENT_ID})
    except Exception as exc:
        print(f"[agent] heartbeat failed for {job_id}: {exc}", file=sys.stderr)


class _Heartbeat:
    """Context manager running a daemon thread that heartbeats a job on a fixed interval.

    A handler may block for many minutes inside a single ctr pull / import; the heartbeat must come
    from a thread INDEPENDENT of that blocking call, or the long job would stall its own liveness
    signal and be falsely reaped. Exiting the context stops the thread promptly.
    """

    def __init__(self, job_id, interval=HEARTBEAT_INTERVAL):
        self._job_id = job_id
        self._interval = max(1.0, float(interval))
        self._stop = threading.Event()
        self._thread = None

    def _run(self):
        while not self._stop.wait(self._interval):
            send_heartbeat(self._job_id)

    def __enter__(self):
        self._thread = threading.Thread(target=self._run, name=f"hb-{self._job_id}", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        return False


def _node_user_pod_count(node):
    """Ask the manager how many running user notebook pods are on `node`.

    Returns an int, or None when the count is unknown (kube error / non-ok). The caller MUST treat
    None as 'pods present' and refuse to restart (fail safe — never destroy a user pod)."""
    try:
        resp = _request(f"/api/internal/nodes/{node}/user-pods", {}, method="GET")
    except Exception as exc:
        print(f"[agent] user-pod count error for {node}: {exc}", file=sys.stderr)
        return None
    if not resp.get("ok"):
        return None
    count = resp.get("user_pods")
    return count if isinstance(count, int) else None


# When containerd on a node last had a restart issued (per-node), for cooldown enforcement.
_last_restart_guard = threading.Lock()
_last_restart_at: dict = {}


def _handle_wedged_node(job_id, node, ip, ref=""):
    """A node's containerd is wedged. Quarantine it and (only if SAFE) attempt pod-preserving recovery.

    Hard safety contract: an automated `systemctl restart containerd` is issued ONLY when ALL hold:
      - AUTO_CONTAINERD_RESTART_ENABLED is on (default OFF — default path is quarantine + alert),
      - the node hosts ZERO running user notebook pods (live count; unknown == treated as present),
      - the per-node restart cooldown has elapsed.
    A restart kills every pod on the node, so the zero-user-pod gate is what guarantees automated
    recovery can never destroy a user instance (e.g. u-11). When not safe, we only quarantine + log
    so an operator can recover during a maintenance window."""
    # Always quarantine first so launches/retries route away from the wedge immediately. Pass the
    # real ref so the manager can anchor a quarantine row even when the node has no prior image_nodes
    # rows (a first-import wedge) — quarantine is node-wide regardless of which ref carried it.
    try:
        report_node_status(node, ref or "", "quarantined", quarantine_seconds=None)
    except Exception as exc:
        print(f"[agent] failed to quarantine {node}: {exc}", file=sys.stderr)

    if not AUTO_CONTAINERD_RESTART_ENABLED:
        push_log(job_id, f"[{node}] quarantined (containerd wedged). Auto-restart disabled; operator must recover.\n")
        return

    user_pods = _node_user_pod_count(node)
    if user_pods is None or user_pods > 0:
        push_log(
            job_id,
            f"[{node}] containerd wedged but user pods present/unknown ({user_pods}); "
            f"REFUSING auto-restart. Quarantined for operator recovery.\n",
        )
        return

    # Cooldown: at most one restart per node per CONTAINERD_RESTART_COOLDOWN window.
    nowt = time.time()
    with _last_restart_guard:
        last = _last_restart_at.get(node, 0.0)
        if nowt - last < CONTAINERD_RESTART_COOLDOWN:
            push_log(job_id, f"[{node}] auto-restart skipped (within {CONTAINERD_RESTART_COOLDOWN}s cooldown).\n")
            return
        _last_restart_at[node] = nowt

    push_log(job_id, f"[{node}] zero user pods; issuing pod-safe containerd restart.\n")
    restart = (
        ["timeout", "60", "ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
         "-i", DISTRIB_SSH_KEY, f"{NODE_SSH_USER}@{ip}", "sudo systemctl restart containerd"]
    )
    try:
        rc = subprocess.run(restart, capture_output=True, timeout=90).returncode
    except (OSError, subprocess.SubprocessError) as exc:
        push_log(job_id, f"[{node}] containerd restart command errored: {exc}\n")
        return
    if rc == 0:
        push_log(job_id, f"[{node}] containerd restarted (was wedged, zero user pods).\n")
    else:
        push_log(job_id, f"[{node}] containerd restart returned rc={rc}; left quarantined.\n")


def free_disk_gb(path):
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except OSError:
        return float("inf")


def _build_env():
    env = dict(os.environ)
    # DOCKER_BUILDKIT is a docker-only selector; rootless `nerdctl build` ignores it. Only set it
    # for docker so we don't leave a misleading no-op var in nerdctl's environment.
    if not _IS_NERDCTL:
        env["DOCKER_BUILDKIT"] = DOCKER_BUILDKIT
    return env


def stream_command(job_id, cmd, env=None):
    """Run a command, streaming its combined output to the manager. Returns True on rc==0.

    The pipeline runs in its own process group (start_new_session=True) so the watchdog
    can kill the WHOLE chain (e.g. save | ssh | import), not just the leading process.
    """
    if isinstance(cmd, str):
        push_log(job_id, "$ " + cmd + "\n")
    else:
        push_log(job_id, "$ " + " ".join(cmd) + "\n")
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
            shell=isinstance(cmd, str),
            start_new_session=True,
        )
    except FileNotFoundError as exc:
        push_log(job_id, f"Command not found: {exc}\n")
        return False

    buffer = []
    last_flush = time.time()

    def flush():
        if buffer:
            push_log(job_id, "".join(buffer))
            buffer.clear()

    # Watchdog: kills the process at BUILD_TIMEOUT even when it produces no output
    # (e.g. `RUN sleep 999999` or a stalled push/import), which the read loop alone can't
    # catch. We signal the whole process group so a piped chain dies together.
    timed_out = threading.Event()

    def _watchdog():
        timed_out.set()
        try:
            os.killpg(proc.pid, 9)
        except OSError:
            # Process group already reaped; cancel() lost the race. Harmless.
            try:
                proc.kill()
            except OSError:
                pass

    watchdog = threading.Timer(BUILD_TIMEOUT, _watchdog)
    watchdog.daemon = True
    watchdog.start()
    try:
        for line in proc.stdout:
            buffer.append(line)
            now = time.time()
            if now - last_flush > LOG_FLUSH_SECONDS:
                flush()
                last_flush = now
        proc.wait()
    finally:
        watchdog.cancel()
        if proc.stdout:
            proc.stdout.close()
        flush()

    # returncode is the source of truth: a job that exited 0 succeeded even if the timer
    # fired in the cancel() race window. Only treat it as a timeout when the process did not
    # exit cleanly AND the watchdog tripped.
    if proc.returncode == 0:
        return True
    if timed_out.is_set():
        push_log(job_id, f"\nCommand exceeded {BUILD_TIMEOUT}s timeout; killed.\n")
    return False


def _disk_guard(job_id):
    """Refuse to write to the local image volume when free space is below the floor."""
    free = free_disk_gb(DISK_CHECK_PATH)
    if free < MIN_FREE_DISK_GB:
        push_log(
            job_id,
            f"Insufficient free disk on image-service host: {free:.1f}GB < {MIN_FREE_DISK_GB}GB. Aborting.\n",
        )
        return False
    return True


def run_build(job):
    job_id = job["id"]
    ref = job["ref"]
    payload = job.get("payload") or {}
    dockerfile = payload.get("dockerfile") or ""

    if not _disk_guard(job_id):
        report_result(job_id, "failed", {"ref": ref, "error": "insufficient_disk"})
        return

    # BUILD-ONLY egress guard: pull/distribute/evict deliberately need network/SSH.
    if BUILD_NETWORK_REQUIRED and not BUILD_NETWORK:
        push_log(
            job_id,
            "Build host is misconfigured: BUILD_NETWORK is not set. User Dockerfile builds are\n"
            "refused by default to avoid giving build steps unrestricted host network egress.\n"
            "Set BUILD_NETWORK to 'none', a restricted docker network, or 'default' to opt in.\n",
        )
        report_result(job_id, "failed", {"ref": ref, "error": "build_network_unset"})
        return

    tar_path = _image_tar_path(ref)
    os.makedirs(IMAGE_TAR_DIR, exist_ok=True)
    workdir = tempfile.mkdtemp(prefix=f"oneclick-build-{job_id}-", dir=IMAGE_WORK_DIR)
    try:
        # Clean, minimal build context: only the Dockerfile. COPY/ADD can therefore only
        # reference files we placed here, never arbitrary host paths — and never the SSH
        # key or DOCKER_CONFIG, which live outside this dir.
        with open(os.path.join(workdir, "Dockerfile"), "w", encoding="utf-8") as fh:
            fh.write(dockerfile)

        build_cmd = _cli(
            "build",
            "--tag", ref,
            "--label", "amd-oneclick-custom=1",
            # Export buildkit's result straight to a docker-format tarball on disk. The follow-up
            # distribute job pipes this file into `ctr import` instead of `nerdctl save <ref>` —
            # the rootless buildkit containerd worker does not reliably load the ref into the
            # namespace nerdctl save reads, so reading the tarball directly avoids the namespace
            # ambiguity entirely. The tarball persists across the build->distribute job boundary.
            "--output", f"type=docker,dest={tar_path}",
        )
        # --force-rm / --memory / --cpuset-cpus are docker classic-builder flags. `nerdctl build`
        # drives buildkitd and rejects all three ("unknown flag"). Under rootless nerdctl, build
        # containers are always cleaned up and RUN-step limits are enforced via cgroup limits on
        # the buildkitd --user service instead (see runbook). Only pass them to docker.
        if not _IS_NERDCTL:
            build_cmd += ["--force-rm", "--memory", BUILD_MEMORY]
            if BUILD_CPUSET:
                build_cmd += ["--cpuset-cpus", BUILD_CPUSET]
        if BUILD_NETWORK:
            build_cmd += ["--network", BUILD_NETWORK]
        else:
            # Only reachable when BUILD_NETWORK_REQUIRED is explicitly disabled.
            push_log(job_id, "WARNING: building with unrestricted default network egress.\n")
        build_cmd += ["-f", os.path.join(workdir, "Dockerfile"), workdir]

        if not stream_command(job_id, build_cmd, env=_build_env()):
            report_result(job_id, "failed", {"ref": ref, "error": "build_failed"})
            return

        if not os.path.exists(tar_path) or os.path.getsize(tar_path) == 0:
            push_log(job_id, f"Build reported success but no tarball at {tar_path}.\n")
            report_result(job_id, "failed", {"ref": ref, "error": "build_no_tarball"})
            return

        push_log(job_id, f"\nBuild complete; image tarball at {tar_path}.\n")
        report_result(job_id, "succeeded", {"ref": ref, "built": True})
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def run_pull(job):
    job_id = job["id"]
    ref = job["ref"]

    if not _disk_guard(job_id):
        report_result(job_id, "failed", {"ref": ref, "error": "insufficient_disk"})
        return

    if not stream_command(job_id, _cli("pull", ref)):
        report_result(job_id, "failed", {"ref": ref, "error": "pull_failed"})
        return

    push_log(job_id, "\nPull complete.\n")
    report_result(job_id, "succeeded", {"ref": ref, "pulled": True})


def run_acr_backup(job):
    job_id = job["id"]
    src = job["ref"]
    payload = job.get("payload") or {}
    dst = payload.get("acr_target_ref")

    if not ACR_ENTERPRISE_REGISTRY:
        push_log(job_id, "ACR_ENTERPRISE_REGISTRY is not configured; cannot back up.\n")
        report_result(job_id, "failed", {"ref": src, "error": "acr_registry_unset"})
        return
    if not dst:
        push_log(job_id, "payload.acr_target_ref is required for acr_backup.\n")
        report_result(job_id, "failed", {"ref": src, "error": "acr_target_ref_missing"})
        return

    if not _disk_guard(job_id):
        report_result(job_id, "failed", {"ref": src, "error": "insufficient_disk"})
        return

    # Creds come from the dedicated DOCKER_CONFIG in the environment (systemd EnvironmentFile),
    # never via --build-arg/--secret and never reachable from a build sandbox.
    if not stream_command(job_id, _cli("tag", src, dst)):
        report_result(job_id, "failed", {"ref": src, "error": "tag_failed"})
        return
    if not stream_command(job_id, _cli("push", dst)):
        report_result(job_id, "failed", {"ref": src, "error": "push_failed"})
        return

    push_log(job_id, "\nACR Enterprise backup complete.\n")
    report_result(job_id, "succeeded", {"ref": src, "acr_backup_ref": dst})


def _run_capture(job_id, cmd, env=None, timeout=None):
    """Run a command, tee combined output to the manager log, and RETURN (rc, output).

    Unlike stream_command (which only returns a bool), run_push needs the command's stdout to
    parse a manifest digest, so this variant captures it. Uses its own process group + timeout so
    a stalled push/inspect can't hang the daemon."""
    if isinstance(cmd, str):
        push_log(job_id, "$ " + cmd + "\n")
    else:
        push_log(job_id, "$ " + " ".join(cmd) + "\n")
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            shell=isinstance(cmd, str),
            start_new_session=True,
            timeout=timeout if timeout is not None else BUILD_TIMEOUT,
        )
    except FileNotFoundError as exc:
        push_log(job_id, f"Command not found: {exc}\n")
        return 127, ""
    except subprocess.TimeoutExpired:
        push_log(job_id, f"\nCommand exceeded {timeout or BUILD_TIMEOUT}s timeout; killed.\n")
        return 124, ""
    if proc.stdout:
        push_log(job_id, proc.stdout)
    return proc.returncode, proc.stdout or ""


def _parse_repo_digest(inspect_json, lan_ref):
    """Extract the sha256 manifest digest for lan_ref from `nerdctl image inspect` JSON output.

    RepoDigests entries look like "<repo>@sha256:<hex>"; return the sha256:... for the entry whose
    repo matches lan_ref's repo (host/path, tag stripped). Returns None if not found."""
    try:
        data = json.loads(inspect_json)
    except (ValueError, TypeError):
        return None
    if isinstance(data, list):
        data = data[0] if data else {}
    repo_digests = (data or {}).get("RepoDigests") or []
    # lan_ref = host:port/repo:tag -> repo key = host:port/repo. Strip only a trailing :tag that
    # lives in the LAST path segment (never the host:port colon).
    slash = lan_ref.rfind("/")
    if slash != -1:
        ref_repo = lan_ref[:slash + 1] + lan_ref[slash + 1:].split(":", 1)[0]
    else:
        ref_repo = lan_ref.split(":", 1)[0]
    for entry in repo_digests:
        if not isinstance(entry, str) or "@" not in entry:
            continue
        repo, _, digest = entry.partition("@")
        if repo == ref_repo and digest.startswith("sha256:"):
            return digest
    # Fall back to the first sha256 digest present (single-repo push).
    for entry in repo_digests:
        if isinstance(entry, str) and "@sha256:" in entry:
            return entry.split("@", 1)[1]
    return None


def _fetch_blob_ids(job_id, lan_ref, manifest_digest):
    """Return this image's dfdaemon task-ids: the config + each layer blob digest (bare hex).

    Dragonfly v1.4.0 runs task-id == blob-digest-hex, so a node/seed caches ONE task per blob and the
    task id equals the blob's sha256 hex. We read the OCI manifest FROM ZOT (authoritative — it is
    exactly what dfdaemon pulls) by digest, and return [config, *layers] as bare hex. Best-effort:
    returns [] on any error (delete then falls back to ref-only; caller logs)."""
    if not manifest_digest or "/" not in (lan_ref or ""):
        return []
    host, _, rest = lan_ref.partition("/")
    slash = rest.rfind("/")
    repo = (rest[:slash + 1] + rest[slash + 1:].split(":", 1)[0]) if slash != -1 else rest.split(":", 1)[0]
    url = f"https://{host}/v2/{repo}/manifests/{manifest_digest}"
    cmd = (
        f"curl -sS --cacert {shlex.quote(ZOT_CA_CERT)} "
        f"-u {shlex.quote(ZOT_USERNAME + ':' + ZOT_PASSWORD)} "
        f"-H {shlex.quote('Accept: application/vnd.oci.image.manifest.v1+json')} "
        f"{shlex.quote(url)}"
    )
    rc, out = _run_capture(job_id, cmd, timeout=60)
    if rc != 0 or not out:
        return []
    try:
        m = json.loads(out)
    except (ValueError, TypeError):
        return []
    ids = []
    cfg = ((m or {}).get("config") or {}).get("digest")
    if cfg:
        ids.append(cfg)
    for layer in (m or {}).get("layers", []) or []:
        d = layer.get("digest") if isinstance(layer, dict) else None
        if d:
            ids.append(d)
    # bare hex, de-duped (a layer may repeat, e.g. empty-dir layers)
    seen = set()
    bare = []
    for d in ids:
        h = d.split(":", 1)[1] if ":" in d else d
        if h and h not in seen:
            seen.add(h)
            bare.append(h)
    return bare


def run_push(job):
    """Push a built/pulled image to the self-hosted LAN registry (zot) with OCI media types.

    P1 durable-source-of-truth step. zot rejects Docker-schema2 manifests (415), so we convert to
    OCI before pushing. The tarball is KEPT (recoverable-single-point backup + the distribute alias
    still reads it during P3). Reports the registry manifest digest so the manager can gate "ready"
    on registry durability and later delete by digest (P5)."""
    job_id = job["id"]
    src = job["ref"]
    payload = job.get("payload") or {}
    dst = payload.get("lan_target_ref")

    if not LAN_REGISTRY:
        push_log(job_id, "LAN_REGISTRY is not configured; cannot push.\n")
        report_result(job_id, "failed", {"ref": src, "error": "lan_registry_unset"})
        return
    if not dst:
        push_log(job_id, "payload.lan_target_ref is required for push.\n")
        report_result(job_id, "failed", {"ref": src, "error": "lan_target_ref_missing"})
        return
    if not _disk_guard(job_id):
        report_result(job_id, "failed", {"ref": src, "error": "insufficient_disk"})
        return

    # The source may only exist as an on-disk tarball (build exports type=docker,dest=tar; it is not
    # reliably loaded into the store). Load it first so convert/push can read it. Idempotent: a
    # second load of the same content is a no-op.
    #
    # push ALWAYS runs before distribute in the chain, so the tarball is present here (distribute
    # deletes it only on its own full success, which is strictly later). If it is nonetheless
    # missing AND the ref is not already in the local store, fail with a clear diagnostic rather
    # than letting `convert` fail cryptically (guards a manual/out-of-order re-enqueue).
    tar_path = _image_tar_path(src)
    have_tar = os.path.exists(tar_path) and os.path.getsize(tar_path) > 0
    if have_tar:
        if not stream_command(job_id, _cli("load", "-i", tar_path)):
            push_log(job_id, "Loading build tarball into the local store failed.\n")
            report_result(job_id, "failed", {"ref": src, "error": "load_failed"})
            return
    else:
        # No tarball: only proceed if the ref is already resolvable in the local store.
        rc, _ = _run_capture(job_id, _cli("image", "inspect", src, "--format", "{{.ID}}"), timeout=60)
        if rc != 0:
            push_log(
                job_id,
                f"No build tarball at {tar_path} and {src} is not in the local store; "
                "cannot push. Re-run the build/pull head step.\n",
            )
            report_result(job_id, "failed", {"ref": src, "error": "source_unavailable"})
            return

    # zot requires OCI media types. `nerdctl image convert --oci <src> <dst>` rewrites the manifest
    # to OCI and tags it as the LAN ref in one step (the retained docker-format tarball is untouched,
    # so the distribute alias still loads it during P3).
    if _IS_NERDCTL:
        if not stream_command(job_id, _cli("image", "convert", "--oci", src, dst)):
            report_result(job_id, "failed", {"ref": src, "error": "oci_convert_failed"})
            return
    else:
        # docker has no `image convert`; buildx/crane would be needed. Not supported in P1 (0042 is
        # nerdctl/containerd). Fail loudly rather than push a schema2 zot rejects.
        push_log(job_id, "OCI conversion requires nerdctl (CONTAINER_CLI=nerdctl).\n")
        report_result(job_id, "failed", {"ref": src, "error": "oci_convert_unsupported_cli"})
        return

    # Creds come from DOCKER_CONFIG (imagesvc already logged in to zot); TLS trusted via the system
    # CA store. Push over the LAN.
    if not stream_command(job_id, _cli("push", dst)):
        report_result(job_id, "failed", {"ref": src, "error": "push_failed"})
        return

    # Authoritative registry digest for the ready-gate + P5 delete-by-digest. Parse RepoDigests from
    # the pushed ref's inspect output. Non-fatal if unparseable: report succeeded with a null digest
    # so the push still counts, but log it (the manager then can't gate on digest for this ref).
    rc, out = _run_capture(
        job_id, _cli("image", "inspect", dst, "--format", "{{json .}}"), timeout=120
    )
    digest = _parse_repo_digest(out, dst) if rc == 0 else None
    if not digest:
        push_log(job_id, "WARNING: pushed OK but could not parse the registry digest.\n")

    # Enumerate this image's blob task-ids from the pushed manifest so a later delete can purge
    # exactly its P2P/seed cache tasks (task-id == blob-hex). Best-effort; empty on parse failure.
    blob_ids = _fetch_blob_ids(job_id, dst, digest) if digest else []
    if not blob_ids:
        push_log(job_id, "NOTE: could not enumerate blob task-ids; delete will fall back to ref-only P2P purge.\n")

    push_log(job_id, f"\nPush to LAN registry complete: {dst}{(' @ ' + digest) if digest else ''}"
                     f" ({len(blob_ids)} blob task-id(s)).\n")
    report_result(job_id, "succeeded", {"ref": src, "lan_target_ref": dst, "digest": digest, "blob_ids": blob_ids})


def _ssh_base(ip):
    return [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-i", DISTRIB_SSH_KEY,
        f"{NODE_SSH_USER}@{ip}",
    ]


def _node_has_binary(ip, binary):
    """Return True if `binary` is on PATH on the node (over SSH)."""
    try:
        rc = subprocess.run(
            _ssh_base(ip) + [f"command -v {binary} >/dev/null 2>&1"],
            capture_output=True,
            timeout=30,
        ).returncode
        return rc == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _node_root_free_gb(ip):
    """Free space (GB) on the node's containerd root, queried over SSH. inf if unknown."""
    # containerd root governs where `ctr import` writes; query its filesystem's avail.
    cmd = (
        "ROOT=$(sudo containerd config dump 2>/dev/null "
        "| awk -F'\"' '/^[[:space:]]*root[[:space:]]*=/{print $2; exit}'); "
        "ROOT=${ROOT:-/var/lib/containerd}; "
        "df --output=avail -k \"$ROOT\" 2>/dev/null | tail -1"
    )
    try:
        out = subprocess.run(
            _ssh_base(ip) + [cmd], capture_output=True, text=True, timeout=30
        )
        avail_kb = int(out.stdout.strip().split()[0])
        return avail_kb / (1024 ** 2)
    except (OSError, subprocess.SubprocessError, ValueError, IndexError):
        return float("inf")


def _containerd_responsive(ip):
    """True if containerd on the node answers a trivial query within CONTAINERD_PROBE_TIMEOUT.

    A daemon wedged on its chain-ID unpack mutex (the RCA failure mode) hangs even this cheap
    `ctr images ls` because the API goroutine is blocked; a healthy daemon answers immediately.
    We pin the SSH-side timeout AND wrap the remote `ctr` in `timeout` so neither side can hang
    past the deadline. Any error / non-zero / timeout reads as NOT responsive (fail safe)."""
    if not ip:
        return False
    remote = f"sudo timeout -s KILL {CONTAINERD_PROBE_TIMEOUT}s ctr -n {CTR_NAMESPACE} images ls -q >/dev/null 2>&1"
    try:
        rc = subprocess.run(
            _ssh_base(ip) + [remote],
            capture_output=True,
            timeout=CONTAINERD_PROBE_TIMEOUT + 10,
        ).returncode
        return rc == 0
    except (OSError, subprocess.SubprocessError):
        return False


# Per-node import serialization: even within one distribute job's ThreadPoolExecutor (and across
# back-to-back jobs handled by this single daemon), two `ctr import` of the same chain onto one node
# must never run concurrently — that is exactly what contends containerd's chain-ID unpack mutex.
# A lock per node name (cross-node imports still run in parallel) enforces it in-process; the
# queue-level guard in claim_next_image_job enforces it across agent restarts.
_node_locks_guard = threading.Lock()
_node_locks: dict = {}


def _node_lock(node):
    key = node or "_unknown_"
    with _node_locks_guard:
        lock = _node_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _node_locks[key] = lock
        return lock


def _pick_compressor(ips):
    """Pick zstd if present locally AND on every target node, else gzip."""
    if shutil.which("zstd") is None:
        return "gzip", "gunzip"
    for ip in ips:
        if not _node_has_binary(ip, "zstd"):
            return "gzip", "gunzip"
    return "zstd", "unzstd"


def run_distribute(job):
    # RETIRED as the default transport (P5): `warm` (P2P self-pull) is now the transport for every
    # image durable in the LAN registry. `distribute` (SSH byte-push of the tarball from 0042) is kept
    # ONLY as the legacy fallback for pre-P1 catalog images that were never pushed to zot and thus have
    # no digest to P2P-pull (see app.main._ensure_image_on_node / _enqueue_admin_image_chain, which
    # emit `distribute` only when there is no LAN-registry copy). Delete this handler once no such
    # image remains (every catalog/custom image has a recorded zot digest).
    job_id = job["id"]
    ref = job["ref"]
    payload = job.get("payload") or {}
    targets = payload.get("targets") or []
    concurrency = int(payload.get("concurrency") or DISTRIBUTE_CONCURRENCY)

    if not DISTRIB_SSH_KEY:
        push_log(job_id, "DISTRIB_SSH_KEY is not configured; cannot distribute.\n")
        report_result(job_id, "failed", {"ref": ref, "error": "distrib_ssh_key_unset"})
        return
    if not targets:
        push_log(job_id, "No targets in payload; nothing to distribute.\n")
        report_result(job_id, "failed", {"ref": ref, "error": "no_targets"})
        return

    if not _disk_guard(job_id):
        report_result(job_id, "failed", {"ref": ref, "error": "insufficient_disk"})
        return

    ips = [t["ip"] for t in targets if t.get("ip")]
    compress, decompress = _pick_compressor(ips)

    # Two image sources: a build job exported a docker tarball to disk (read it directly with cat,
    # avoiding the nerdctl-save namespace problem); a pull job loaded the ref into the local store
    # (no tarball — fall back to `nerdctl save <ref>`). Prefer the tarball when present.
    tar_path = _image_tar_path(ref)
    have_tar = os.path.exists(tar_path) and os.path.getsize(tar_path) > 0
    image_source = f"cat {shlex.quote(tar_path)}" if have_tar else _cli_str("save", ref)
    push_log(
        job_id,
        f"Distributing {ref} to {len(targets)} node(s) using {compress} "
        f"(source: {'tarball' if have_tar else 'nerdctl save'}).\n",
    )

    results = []
    lock = threading.Lock()

    def _one(target):
        node = target.get("node")
        ip = target.get("ip")
        entry = {"node": node, "ip": ip, "loaded": False, "size_bytes": None, "error": None}
        if not ip:
            entry["error"] = "missing_ip"
            return entry

        # Serialize all imports to one node (see _node_lock): two unpacks of the same chain onto one
        # node are what wedge containerd's chain-ID mutex. Cross-node imports still run in parallel.
        with _node_lock(node):
            free_gb = _node_root_free_gb(ip)
            if free_gb < IMAGE_NODE_MIN_FREE_DISK_GB:
                entry["error"] = f"node_low_disk:{free_gb:.1f}GB<{IMAGE_NODE_MIN_FREE_DISK_GB}GB"
                push_log(job_id, f"[{node}] refusing: containerd root free {free_gb:.1f}GB below floor.\n")
                return entry

            # Liveness gate: never stream 10.8GB into a daemon that is already wedged.
            if not _containerd_responsive(ip):
                entry["error"] = "containerd_wedged"
                push_log(job_id, f"[{node}] containerd not responsive before import; skipping stream.\n")
                _handle_wedged_node(job_id, node, ip, ref)
                return entry

            # Mark importing so a hung import is visible (vs. "never started"). Best-effort.
            try:
                report_node_status(node, ref, "importing")
            except Exception:
                pass

            # Wrap the REMOTE ctr in `timeout -s KILL` so the remote client dies on its own deadline
            # instead of blocking forever inside the daemon — stream_command's watchdog only reaches
            # the LOCAL process group.
            remote = (
                f"sudo {decompress} | sudo timeout -s KILL {REMOTE_IMPORT_TIMEOUT}s "
                f"ctr -n {CTR_NAMESPACE} images import -"
            )
            local = f"{image_source} | {compress}"
            ssh_remote = " ".join([
                "ssh",
                "-o", "BatchMode=yes",
                "-o", "StrictHostKeyChecking=accept-new",
                "-i", DISTRIB_SSH_KEY,
                f"{NODE_SSH_USER}@{ip}",
                f"'{remote}'",
            ])
            pipeline = f"{local} | {ssh_remote}"
            ok = stream_command(job_id, pipeline)
            entry["loaded"] = ok
            if ok:
                return entry

            # Import failed. Distinguish a wedged daemon (post-probe unresponsive) from an ordinary
            # failure so the manager can quarantine the node and trigger pod-safe recovery.
            if not _containerd_responsive(ip):
                entry["error"] = "containerd_wedged"
                push_log(job_id, f"[{node}] containerd unresponsive after failed import; node wedged.\n")
                _handle_wedged_node(job_id, node, ip, ref)
            else:
                entry["error"] = "import_failed"
            return entry

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        for entry in pool.map(_one, targets):
            with lock:
                results.append(entry)

    all_loaded = all(e["loaded"] for e in results)
    status = "succeeded" if all_loaded else "failed"
    push_log(job_id, f"\nDistribute {status}: {sum(e['loaded'] for e in results)}/{len(results)} loaded.\n")
    # On full success free the build tarball; on partial failure keep it so a retry can reuse it.
    if all_loaded and have_tar:
        try:
            os.remove(tar_path)
        except OSError:
            pass
    report_result(job_id, status, {"ref": ref, "nodes": results})


def run_warm(job):
    """P3 transport: warm an image onto nodes by triggering a per-node `ctr pull` of the LAN-registry
    ref THROUGH each node's local Dragonfly dfdaemon mirror (127.0.0.1:4001). Bytes arrive node<-P2P;
    0042 sends none (unlike run_distribute's save|ssh 'ctr import' byte-push).

    Forks run_distribute's scaffolding EXACTLY — same ThreadPoolExecutor over targets, _node_lock
    (the chain-ID unpack mutex is graceful, not eliminated), _containerd_responsive pre/post,
    _handle_wedged_node, 'importing' status, and the SAME per-node results list
    (report_result nodes=[{node,ip,loaded,size_bytes,error},...]) so the manager's warm/distribute
    lifecycle branch writes the image_nodes 'loaded' row identically.

    The ref pulled is the LAN-registry ref (payload.lan_target_ref, else the job ref). Each node's
    containerd must have /etc/containerd/certs.d/<lan-host>/hosts.toml pointing at 127.0.0.1:4001
    (P2 canary step) — otherwise the pull goes straight to zot (still correct, just not P2P)."""
    job_id = job["id"]
    ref = job["ref"]
    payload = job.get("payload") or {}
    targets = payload.get("targets") or []
    # The ref that actually lives in the LAN registry (what nodes pull). Falls back to the job ref.
    lan_ref = payload.get("lan_target_ref") or ref
    concurrency = int(payload.get("concurrency") or DISTRIBUTE_CONCURRENCY)

    if not DISTRIB_SSH_KEY:
        push_log(job_id, "DISTRIB_SSH_KEY is not configured; cannot warm.\n")
        report_result(job_id, "failed", {"ref": ref, "error": "distrib_ssh_key_unset"})
        return
    if not targets:
        push_log(job_id, "No targets in payload; nothing to warm.\n")
        report_result(job_id, "failed", {"ref": ref, "error": "no_targets"})
        return

    push_log(job_id, f"Warming {lan_ref} onto {len(targets)} node(s) via per-node P2P pull.\n")
    results = []
    lock = threading.Lock()

    def _one(target):
        node = target.get("node")
        ip = target.get("ip")
        entry = {"node": node, "ip": ip, "loaded": False, "size_bytes": None, "error": None}
        if not ip:
            entry["error"] = "missing_ip"
            return entry

        # Same per-node serialization as distribute: two unpacks of the same chain onto one node
        # wedge containerd's chain-ID mutex. Cross-node pulls still run in parallel.
        with _node_lock(node):
            free_gb = _node_root_free_gb(ip)
            if free_gb < IMAGE_NODE_MIN_FREE_DISK_GB:
                entry["error"] = f"node_low_disk:{free_gb:.1f}GB<{IMAGE_NODE_MIN_FREE_DISK_GB}GB"
                push_log(job_id, f"[{node}] refusing: containerd root free {free_gb:.1f}GB below floor.\n")
                return entry

            if not _containerd_responsive(ip):
                entry["error"] = "containerd_wedged"
                push_log(job_id, f"[{node}] containerd not responsive before pull; skipping.\n")
                _handle_wedged_node(job_id, node, ip, ref)
                return entry

            try:
                report_node_status(node, ref, "importing")
            except Exception:
                pass

            # Trigger the pull ON the node. Bytes arrive via the node-local dfdaemon mirror (P2P);
            # 0042 streams nothing. Wrap the remote ctr in `timeout -s KILL` so a stalled pull dies
            # on its own deadline instead of blocking inside the daemon.
            # CRITICAL: `ctr` does NOT auto-read containerd's config_path/certs.d (only the CRI/
            # kubelet path does). Without --hosts-dir, ctr pulls DIRECTLY from zot, bypassing the
            # dfdaemon mirror entirely — the warm "succeeds" but is NOT P2P-served, silently defeating
            # the transport. Pass --hosts-dir so the hosts.toml (127.0.0.1:4001 + auth) is honored.
            # (Verified live on the canary: bare ctr pull ignores certs.d; --hosts-dir routes P2P.)
            remote = (
                f"sudo timeout -s KILL {REMOTE_IMPORT_TIMEOUT}s "
                f"ctr -n {CTR_NAMESPACE} images pull --hosts-dir {shlex.quote(CERTS_DIR)} {shlex.quote(lan_ref)}"
            )
            ok = stream_command(job_id, _ssh_base(ip) + [remote])
            entry["loaded"] = ok
            if ok:
                return entry

            if not _containerd_responsive(ip):
                entry["error"] = "containerd_wedged"
                push_log(job_id, f"[{node}] containerd unresponsive after failed pull; node wedged.\n")
                _handle_wedged_node(job_id, node, ip, ref)
            else:
                entry["error"] = "pull_failed"
            return entry

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        for entry in pool.map(_one, targets):
            with lock:
                results.append(entry)

    all_loaded = all(e["loaded"] for e in results)
    status = "succeeded" if all_loaded else "failed"
    push_log(job_id, f"\nWarm {status}: {sum(e['loaded'] for e in results)}/{len(results)} loaded.\n")
    report_result(job_id, status, {"ref": ref, "nodes": results})


def run_evict(job):
    job_id = job["id"]
    ref = job["ref"]
    payload = job.get("payload") or {}
    targets = payload.get("targets") or []

    if not DISTRIB_SSH_KEY:
        push_log(job_id, "DISTRIB_SSH_KEY is not configured; cannot evict.\n")
        report_result(job_id, "failed", {"ref": ref, "error": "distrib_ssh_key_unset"})
        return
    if not targets:
        push_log(job_id, "No targets in payload; nothing to evict.\n")
        report_result(job_id, "failed", {"ref": ref, "error": "no_targets"})
        return

    results = []

    def _one(target):
        node = target.get("node")
        ip = target.get("ip")
        entry = {"node": node, "ip": ip, "removed": False, "error": None}
        if not ip:
            entry["error"] = "missing_ip"
            return entry
        # GC reclaims layers async — acceptable for a 5-day evict.
        remote = (
            f"sudo ctr -n {CTR_NAMESPACE} images rm {ref} "
            f"&& sudo ctr -n {CTR_NAMESPACE} content prune"
        )
        ok = stream_command(job_id, _ssh_base(ip) + [remote])
        entry["removed"] = ok
        if not ok:
            entry["error"] = "evict_failed"
        return entry

    for target in targets:
        results.append(_one(target))

    all_removed = all(e["removed"] for e in results)
    status = "succeeded" if all_removed else "failed"
    push_log(job_id, f"\nEvict {status}: {sum(e['removed'] for e in results)}/{len(results)} removed.\n")
    report_result(job_id, status, {"ref": ref, "nodes": results})


# =============================================================================
# P4 complete-delete purge handlers. Each targets ONE byte-surface. Golden rule everywhere:
# "already gone" == SUCCESS (idempotent) — a purge must converge on re-run, never fail because the
# thing was already removed. Reported failures are re-enqueued by the manager (requeue_failed_purges)
# until they succeed, which is what makes delete complete.
# =============================================================================

def run_purge_node(job):
    """Surface 1: node containerd layers. Per-node `ctr images rm` + content prune (serialized).
    Mirrors run_evict; `ctr images rm` of a missing image is already a no-op success in containerd."""
    job_id = job["id"]
    ref = job["ref"]
    payload = job.get("payload") or {}
    targets = payload.get("targets") or []
    if not DISTRIB_SSH_KEY:
        report_result(job_id, "failed", {"ref": ref, "error": "distrib_ssh_key_unset"})
        return
    if not targets:
        report_result(job_id, "failed", {"ref": ref, "error": "no_targets"})
        return

    def _one(target):
        node = target.get("node")
        ip = target.get("ip")
        entry = {"node": node, "ip": ip, "removed": False, "error": None}
        if not ip:
            entry["error"] = "missing_ip"
            return entry
        # `images rm` tolerates a missing ref (no-op); content prune reclaims layers.
        remote = (
            f"sudo ctr -n {CTR_NAMESPACE} images rm {shlex.quote(ref)} >/dev/null 2>&1; "
            f"sudo ctr -n {CTR_NAMESPACE} content prune >/dev/null 2>&1; "
            f"! sudo ctr -n {CTR_NAMESPACE} images ls -q 2>/dev/null | grep -qxF {shlex.quote(ref)}"
        )
        # The final `! grep` makes the command succeed only when the ref is truly absent afterward.
        ok = stream_command(job_id, _ssh_base(ip) + [remote])
        entry["removed"] = ok
        if not ok:
            entry["error"] = "purge_node_failed"
        return entry

    results = [_one(t) for t in targets]
    all_removed = all(e["removed"] for e in results)
    status = "succeeded" if all_removed else "failed"
    push_log(job_id, f"\nPurge_node {status}: {sum(e['removed'] for e in results)}/{len(results)} cleared.\n")
    report_result(job_id, status, {"ref": ref, "nodes": results})


# NOTE: purge_p2p and purge_seed are intentionally NOT implemented on the agent. The real Dragonfly
# v1.4.0 delete primitive is `dfctl task rm <task_id>` against each daemon's LOCAL socket (dfget has
# no delete; deletion is by task id, not URL), and the seed pods are on the cluster overlay network,
# unreachable from this host. The MANAGER runs both surfaces in-cluster via kubectl-exec — see
# app/purge_exec.py and scheduler.drain_manager_purges_job. The agent must not claim these kinds.


def run_registry_delete(job):
    """Surface 4: the LAN registry (zot) tag/manifest. DELETE /v2/<repo>/manifests/<digest>.
    Digest comes FROM THE PAYLOAD (captured at enqueue; the DB row may already be deleted). zot's
    online GC reclaims blobs async, so no push-lock is needed. Idempotent: 404 => already gone =>
    success; 200/202 => deleted."""
    job_id = job["id"]
    ref = job["ref"]
    payload = job.get("payload") or {}
    digest = payload.get("digest")
    lan_ref = payload.get("lan_target_ref")
    # No registry copy could exist iff neither a LAN ref was ever computed NOR a digest recorded.
    # Then there is nothing to delete on this surface -> success (do NOT block purge_meta forever).
    # This covers pre-P1 images and idle catalog images that were never pushed to zot.
    if not lan_ref and not digest:
        push_log(job_id, "No LAN ref/digest for this image; no registry copy to delete (no-op).\n")
        report_result(job_id, "succeeded", {"ref": ref, "registry_deleted": False})
        return
    # A registry copy is expected (lan_ref set) but we cannot address it without a digest. Fail so it
    # retries — but if it never resolves, this is a genuine leak to surface, not silently pass.
    if not digest:
        push_log(job_id, "payload.digest missing but a LAN ref exists; cannot DELETE by digest.\n")
        report_result(job_id, "failed", {"ref": ref, "error": "digest_missing"})
        return
    # Registry host: prefer the agent's configured LAN_REGISTRY, else derive from lan_ref (C4: don't
    # no-op just because the agent env is empty while a copy demonstrably exists).
    registry_host = LAN_REGISTRY or ((lan_ref or "").split("/", 1)[0] if "/" in (lan_ref or "") else "")
    if not registry_host:
        report_result(job_id, "failed", {"ref": ref, "error": "no_registry_host"})
        return
    # Derive the repo path (host:port/REPO:tag -> REPO) from the LAN ref, else from ref.
    src = lan_ref or ref
    if "/" in src:
        _host, _, rest = src.partition("/")
        # strip a trailing :tag only in the LAST path segment (never the host:port colon)
        slash = rest.rfind("/")
        if slash != -1:
            repo = rest[:slash + 1] + rest[slash + 1:].split(":", 1)[0]
        else:
            repo = rest.split(":", 1)[0]
    else:
        report_result(job_id, "failed", {"ref": ref, "error": "unparseable_repo"})
        return
    url = f"https://{registry_host}/v2/{repo}/manifests/{digest}"
    cmd = (
        f"curl -sS -o /dev/null -w '%{{http_code}}' -X DELETE "
        f"--cacert {shlex.quote(ZOT_CA_CERT)} "
        f"-u {shlex.quote(ZOT_USERNAME + ':' + ZOT_PASSWORD)} "
        f"{shlex.quote(url)}"
    )
    rc, out = _run_capture(job_id, cmd, timeout=60)
    code = (out or "").strip()[-3:]
    if rc == 0 and code in ("200", "202", "404"):
        push_log(job_id, f"\nRegistry delete OK (HTTP {code}) for {repo}@{digest}.\n")
        report_result(job_id, "succeeded", {"ref": ref, "registry_deleted": code != "404", "http": code})
    else:
        push_log(job_id, f"\nRegistry delete failed (rc={rc} HTTP {code}).\n")
        report_result(job_id, "failed", {"ref": ref, "error": f"registry_delete_http_{code or 'err'}"})


def run_purge_builder(job):
    """Surface 5: the 0042 build tarball + rootless builder cache. rm the retained tarball (kept by
    run_push as backup) + buildkit prune. 0042-local; not serialized. Missing tarball => success."""
    job_id = job["id"]
    ref = job["ref"]
    tar_path = _image_tar_path(ref)
    tar_removed = False
    try:
        os.remove(tar_path)
        tar_removed = True
    except FileNotFoundError:
        tar_removed = True  # already gone => success
    except OSError as exc:
        push_log(job_id, f"Could not remove tarball {tar_path}: {exc}\n")
        report_result(job_id, "failed", {"ref": ref, "error": "tarball_rm_failed"})
        return
    # buildkit prune is best-effort (reclaims dangling build cache, not keyed to this ref); a failure
    # here should not fail the whole purge — the durable artifact (the tarball) is already gone.
    if _IS_NERDCTL:
        stream_command(job_id, _cli("builder", "prune", "-f"))
    push_log(job_id, f"\nPurge_builder complete (tarball removed={tar_removed}).\n")
    report_result(job_id, "succeeded", {"ref": ref, "tar_removed": tar_removed})


DISPATCH = {
    "build": run_build,
    "pull": run_pull,
    "acr_backup": run_acr_backup,
    "push": run_push,
    "distribute": run_distribute,
    "warm": run_warm,
    "evict": run_evict,
    "purge_node": run_purge_node,
    # purge_p2p / purge_seed are NOT handled here: the real Dragonfly v1.4.0 delete is
    # `dfctl task rm <task_id>` against the LOCAL daemon socket, and the seeds are overlay-only
    # (unreachable from this host). The MANAGER drains those kinds in-cluster via kubectl-exec
    # (see app/purge_exec.py + scheduler.drain_manager_purges_job). This agent must not claim them.
    "registry_delete": run_registry_delete,
    "purge_builder": run_purge_builder,
}


def run_outdated_reaper():
    """Trigger the manager's outdated pass; it resolves targets and enqueues evict jobs.

    The daemon only triggers this (it has no kubectl to resolve node IPs): the manager computes
    outdated (ref,node) pairs, resolves targets, and enqueues one evict per ref server-side. The
    daemon then claims those evict jobs through its normal claim loop. Singleton by construction:
    exactly one image-service daemon runs this loop.
    """
    try:
        resp = _request("/api/internal/images/outdated", {"agent_id": AGENT_ID})
    except Exception as exc:
        print(f"[agent] outdated poll error: {exc}", file=sys.stderr)
        return
    enqueued = resp.get("enqueued")
    if enqueued:
        print(f"[agent] reaper triggered server-side evicts: {enqueued}")


def main():
    print(
        f"[agent] starting: manager={MANAGER_URL} id={AGENT_ID} poll={POLL_INTERVAL}s "
        f"kinds={IMAGE_SERVICE_KINDS} cli={CONTAINER_CLI}"
    )
    last_reap = 0.0
    while True:
        # Run the outdated reaper on its own cadence, in-band with the claim loop.
        now = time.time()
        if now - last_reap >= OUTDATED_POLL_SECONDS:
            run_outdated_reaper()
            last_reap = now

        try:
            job = claim_job(IMAGE_SERVICE_KINDS)
        except urllib.error.HTTPError as exc:
            print(f"[agent] claim HTTP {exc.code}: {exc.reason}", file=sys.stderr)
            time.sleep(POLL_INTERVAL)
            continue
        except Exception as exc:
            print(f"[agent] claim error: {exc}", file=sys.stderr)
            time.sleep(POLL_INTERVAL)
            continue

        if not job:
            time.sleep(POLL_INTERVAL)
            continue

        kind = job.get("kind")
        handler = DISPATCH.get(kind)
        if handler is None:
            print(f"[agent] unknown job kind {kind!r} for job {job.get('id')}", file=sys.stderr)
            try:
                report_result(job["id"], "failed", {"error": f"unknown_kind:{kind}"})
            except Exception:
                pass
            continue

        print(f"[agent] running job id={job['id']} kind={kind} ref={job.get('ref')}")
        try:
            mark_running(job["id"])
            with _Heartbeat(job["id"]):
                handler(job)
        except Exception as exc:
            print(f"[agent] job error for {job.get('id')} ({kind}): {exc}", file=sys.stderr)
            try:
                report_result(job["id"], "failed", {"error": str(exc)})
            except Exception:
                pass


if __name__ == "__main__":
    main()
