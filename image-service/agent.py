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
import json
import os
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
ALL_KINDS = ["build", "pull", "acr_backup", "distribute", "evict"]
IMAGE_SERVICE_KINDS = [
    k.strip() for k in _env("IMAGE_SERVICE_KINDS", ",".join(ALL_KINDS)).split(",") if k.strip()
]

ACR_ENTERPRISE_REGISTRY = _env("ACR_ENTERPRISE_REGISTRY", "")
DISTRIBUTE_CONCURRENCY = int(_env("DISTRIBUTE_CONCURRENCY", "2"))
CTR_NAMESPACE = _env("CTR_NAMESPACE", "k8s.io")
NODE_SSH_USER = _env("NODE_SSH_USER", "root")
DISTRIB_SSH_KEY = _env("DISTRIB_SSH_KEY", "")

# Outdated reaper cadence: how often the daemon asks the manager for outdated (ref,node)
# pairs and enqueues evict jobs. Singleton by construction (one daemon polls).
OUTDATED_POLL_SECONDS = float(_env("OUTDATED_POLL_SECONDS", "300"))


def _request(path, payload, timeout=30):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        MANAGER_URL + path,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {BUILD_AGENT_TOKEN}",
        },
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

    workdir = tempfile.mkdtemp(prefix=f"oneclick-build-{job_id}-", dir=IMAGE_WORK_DIR)
    try:
        # Clean, minimal build context: only the Dockerfile. COPY/ADD can therefore only
        # reference files we placed here, never arbitrary host paths — and never the SSH
        # key or DOCKER_CONFIG, which live outside this dir.
        with open(os.path.join(workdir, "Dockerfile"), "w", encoding="utf-8") as fh:
            fh.write(dockerfile)

        build_cmd = [
            CONTAINER_CLI, "build",
            "--tag", ref,
            "--label", "amd-oneclick-custom=1",
        ]
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

        push_log(job_id, "\nBuild complete.\n")
        report_result(job_id, "succeeded", {"ref": ref, "built": True})
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        # Free local disk: the built image now lives in the local containerd/docker store
        # and will be distributed by a follow-up job; drop the build tag.
        subprocess.run([CONTAINER_CLI, "image", "rm", "-f", ref], capture_output=True)


def run_pull(job):
    job_id = job["id"]
    ref = job["ref"]

    if not _disk_guard(job_id):
        report_result(job_id, "failed", {"ref": ref, "error": "insufficient_disk"})
        return

    if not stream_command(job_id, [CONTAINER_CLI, "pull", ref]):
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
    if not stream_command(job_id, [CONTAINER_CLI, "tag", src, dst]):
        report_result(job_id, "failed", {"ref": src, "error": "tag_failed"})
        return
    if not stream_command(job_id, [CONTAINER_CLI, "push", dst]):
        report_result(job_id, "failed", {"ref": src, "error": "push_failed"})
        return

    push_log(job_id, "\nACR Enterprise backup complete.\n")
    report_result(job_id, "succeeded", {"ref": src, "acr_backup_ref": dst})


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


def _pick_compressor(ips):
    """Pick zstd if present locally AND on every target node, else gzip."""
    if shutil.which("zstd") is None:
        return "gzip", "gunzip"
    for ip in ips:
        if not _node_has_binary(ip, "zstd"):
            return "gzip", "gunzip"
    return "zstd", "unzstd"


def run_distribute(job):
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
    push_log(job_id, f"Distributing {ref} to {len(targets)} node(s) using {compress}.\n")

    results = []
    lock = threading.Lock()

    def _one(target):
        node = target.get("node")
        ip = target.get("ip")
        entry = {"node": node, "ip": ip, "loaded": False, "size_bytes": None, "error": None}
        if not ip:
            entry["error"] = "missing_ip"
            return entry

        free_gb = _node_root_free_gb(ip)
        if free_gb < IMAGE_NODE_MIN_FREE_DISK_GB:
            entry["error"] = f"node_low_disk:{free_gb:.1f}GB<{IMAGE_NODE_MIN_FREE_DISK_GB}GB"
            push_log(job_id, f"[{node}] refusing: containerd root free {free_gb:.1f}GB below floor.\n")
            return entry

        remote = (
            f"sudo {decompress} | sudo ctr -n {CTR_NAMESPACE} images import -"
        )
        local = f"{CONTAINER_CLI} save {ref} | {compress}"
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
        if not ok:
            entry["error"] = "import_failed"
        return entry

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        for entry in pool.map(_one, targets):
            with lock:
                results.append(entry)

    all_loaded = all(e["loaded"] for e in results)
    status = "succeeded" if all_loaded else "failed"
    push_log(job_id, f"\nDistribute {status}: {sum(e['loaded'] for e in results)}/{len(results)} loaded.\n")
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


DISPATCH = {
    "build": run_build,
    "pull": run_pull,
    "acr_backup": run_acr_backup,
    "distribute": run_distribute,
    "evict": run_evict,
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
            handler(job)
        except Exception as exc:
            print(f"[agent] job error for {job.get('id')} ({kind}): {exc}", file=sys.stderr)
            try:
                report_result(job["id"], "failed", {"error": str(exc)})
            except Exception:
                pass


if __name__ == "__main__":
    main()
