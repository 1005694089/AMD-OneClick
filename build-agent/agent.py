#!/usr/bin/env python3
"""
AMD OneClick build-agent.

Polls the manager for pending custom image builds, builds them with Docker in a
clean build context, pushes to the ACR custom registry, and reports status + logs.

This process runs OUTSIDE the Kubernetes cluster (on the R9700 workstation). It is
the ONLY component that holds the ACR push credential (via `docker login`, ideally
written to a DOCKER_CONFIG outside of $HOME). User-supplied Dockerfile `RUN` steps
execute inside ephemeral build containers and never see that credential, the host
filesystem, or the cluster kubeconfig.

Hardening expectations (enforced by the systemd unit / host setup, not this script):
  - dedicated low-privilege user; rootless Docker or Podman recommended
  - ProtectHome=true so /home (and 7900_cluster_config) is not readable
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
import time
import urllib.error
import urllib.request


def _env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and not value:
        print(f"[agent] FATAL: missing required env {name}", file=sys.stderr)
        sys.exit(2)
    return value


MANAGER_URL = _env("MANAGER_URL", required=True).rstrip("/")
BUILD_AGENT_TOKEN = _env("BUILD_AGENT_TOKEN", required=True)
AGENT_ID = _env("AGENT_ID", "r9700-agent-1")
POLL_INTERVAL = float(_env("POLL_INTERVAL_SECONDS", "10"))
DOCKER = _env("DOCKER_BIN", "docker")
BUILD_TIMEOUT = int(_env("BUILD_TIMEOUT_SECONDS", "1800"))
BUILD_MEMORY = _env("BUILD_MEMORY", "8g")
BUILD_CPUSET = _env("BUILD_CPUSET", "")            # e.g. "0-3"; empty = no pin
BUILD_NETWORK = _env("BUILD_NETWORK", "")          # e.g. a restricted docker network name
MIN_FREE_DISK_GB = float(_env("MIN_FREE_DISK_GB", "20"))
DISK_CHECK_PATH = _env("DISK_CHECK_PATH", "/")
LOG_FLUSH_SECONDS = float(_env("LOG_FLUSH_SECONDS", "3"))
# Classic builder honors --memory/--cpuset/--network for RUN steps; default to it so
# our resource + egress limits actually apply. Override to "1" only if you front this
# with buildx + a resource-limited builder.
DOCKER_BUILDKIT = _env("DOCKER_BUILDKIT", "0")


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


def claim_job():
    return _request("/api/internal/builds/claim", {"agent_id": AGENT_ID}).get("job")


def push_log(image_id, chunk):
    if not chunk:
        return
    try:
        _request(f"/api/internal/builds/{image_id}/log", {"log": chunk})
    except Exception as exc:  # logging must never crash the build loop
        print(f"[agent] log push failed for {image_id}: {exc}", file=sys.stderr)


def report_result(image_id, status):
    _request(f"/api/internal/builds/{image_id}/result", {"status": status})


def free_disk_gb(path):
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except OSError:
        return float("inf")


def _build_env():
    env = dict(os.environ)
    env["DOCKER_BUILDKIT"] = DOCKER_BUILDKIT
    return env


def stream_command(image_id, cmd, env=None):
    """Run a command, streaming its combined output to the manager. Returns True on rc==0."""
    push_log(image_id, "$ " + " ".join(cmd) + "\n")
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=env,
        )
    except FileNotFoundError as exc:
        push_log(image_id, f"Command not found: {exc}\n")
        return False

    buffer = []
    last_flush = time.time()
    start = time.time()

    def flush():
        if buffer:
            push_log(image_id, "".join(buffer))
            buffer.clear()

    for line in proc.stdout:
        buffer.append(line)
        now = time.time()
        if now - last_flush > LOG_FLUSH_SECONDS:
            flush()
            last_flush = now
        if now - start > BUILD_TIMEOUT:
            proc.kill()
            flush()
            push_log(image_id, f"\nBuild exceeded {BUILD_TIMEOUT}s timeout; killed.\n")
            return False
    proc.wait()
    flush()
    return proc.returncode == 0


def run_build(job):
    image_id = job["id"]
    tag = job["tag"]
    dockerfile = job["dockerfile"]

    free = free_disk_gb(DISK_CHECK_PATH)
    if free < MIN_FREE_DISK_GB:
        push_log(image_id, f"Insufficient free disk on build host: {free:.1f}GB < {MIN_FREE_DISK_GB}GB. Aborting.\n")
        report_result(image_id, "failed")
        return

    workdir = tempfile.mkdtemp(prefix=f"oneclick-build-{image_id}-")
    try:
        # Clean, minimal build context: only the Dockerfile. COPY/ADD can therefore only
        # reference files we placed here, never arbitrary host paths.
        with open(os.path.join(workdir, "Dockerfile"), "w", encoding="utf-8") as fh:
            fh.write(dockerfile)

        build_cmd = [
            DOCKER, "build",
            "--tag", tag,
            "--memory", BUILD_MEMORY,
            "--label", "amd-oneclick-custom=1",
            "--force-rm",
        ]
        if BUILD_CPUSET:
            build_cmd += ["--cpuset-cpus", BUILD_CPUSET]
        if BUILD_NETWORK:
            build_cmd += ["--network", BUILD_NETWORK]
        build_cmd += ["-f", os.path.join(workdir, "Dockerfile"), workdir]

        if not stream_command(image_id, build_cmd, env=_build_env()):
            report_result(image_id, "failed")
            return

        if not stream_command(image_id, [DOCKER, "push", tag]):
            report_result(image_id, "failed")
            return

        push_log(image_id, "\nBuild and push complete.\n")
        report_result(image_id, "ready")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        # Free local disk: the image now lives in the registry and is prepulled to nodes.
        subprocess.run([DOCKER, "image", "rm", "-f", tag], capture_output=True)


def main():
    print(f"[agent] starting: manager={MANAGER_URL} id={AGENT_ID} poll={POLL_INTERVAL}s")
    while True:
        try:
            job = claim_job()
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

        print(f"[agent] building image id={job['id']} tag={job['tag']}")
        try:
            run_build(job)
        except Exception as exc:
            print(f"[agent] build error for {job.get('id')}: {exc}", file=sys.stderr)
            try:
                report_result(job["id"], "failed")
            except Exception:
                pass


if __name__ == "__main__":
    main()
