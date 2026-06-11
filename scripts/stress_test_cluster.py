#!/usr/bin/env python3
"""Cluster stress test: staggered GPU pod creation + HF download + detailed report."""
import argparse
import datetime as dt
import json
import math
import re
import subprocess
import sys
import textwrap
import time
from pathlib import Path

DEFAULT_IMAGE = "radeon-cloud-registry.cn-shanghai.cr.aliyuncs.com/admin/amd-oneclick-base:rocm7.2.1-py3.12-v20260416"
DEFAULT_IMAGE_PULL_SECRET = "acr-enterprise-pull"
DEFAULT_MODEL = "qwen/Qwen3-8B"
DEFAULT_HF_ENDPOINT = "http://134.199.133.77"


def now_utc():
    return dt.datetime.now(dt.timezone.utc)


def iso_now():
    return now_utc().isoformat()


def parse_dt(value):
    if not value:
        return None
    value = value.replace("Z", "+00:00")
    try:
        return dt.datetime.fromisoformat(value)
    except Exception:
        return None


def seconds_between(a, b):
    if not a or not b:
        return None
    return max(0.0, (b - a).total_seconds())


def stat_values(values):
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    vals.sort()
    return {
        "count": len(vals),
        "min": vals[0],
        "max": vals[-1],
        "avg": sum(vals) / len(vals),
        "p50": vals[len(vals) // 2],
        "p95": vals[min(len(vals)-1, math.ceil(len(vals) * 0.95)-1)],
    }


def fmt_stats(s):
    if not s:
        return "n/a"
    return f"count={s['count']} min={s['min']:.1f}s avg={s['avg']:.1f}s p50={s['p50']:.1f}s p95={s['p95']:.1f}s max={s['max']:.1f}s"


def run(cmd, input_text=None, check=True, timeout=None):
    proc = subprocess.run(cmd, input=input_text, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(f"Command failed {proc.returncode}: {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
    return proc


class StressTest:
    def __init__(self, args):
        self.args = args
        self.run_id = args.run_id or now_utc().strftime("stress-%Y%m%d%H%M%S")
        self.artifacts = Path(args.output_dir or f"stress-report-{self.run_id}").resolve()
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self.launch_records = []
        self.kubectl_base = ["kubectl"]
        if args.kubeconfig:
            self.kubectl_base.append(f"--kubeconfig={args.kubeconfig}")

    def kubectl(self, *args, input_text=None, check=True, timeout=None):
        return run(self.kubectl_base + list(args), input_text=input_text, check=check, timeout=timeout)

    def require_image_pull_secret(self):
        if not self.args.image_pull_secret:
            return
        proc = self.kubectl("get", "secret", self.args.image_pull_secret, "-n", self.args.namespace, check=False)
        if proc.returncode != 0:
            raise RuntimeError(
                f"image pull secret {self.args.image_pull_secret!r} was not found in namespace {self.args.namespace!r}. "
                "Create it there or pass --image-pull-secret ''."
            )

    def confirm(self):
        if self.args.yes:
            return
        print(f"Will create {self.args.count} GPU pods with {self.args.launch_interval}s interval.")
        if input("Type yes to continue: ").strip().lower() != "yes":
            raise SystemExit("aborted")

    def name(self, i):
        return f"stress-hf-{i:04d}-{self.run_id}"[:63]

    def email(self, i):
        return f"stress-{self.run_id}-{i:04d}@oneclick.local"

    def deploy_netmon(self):
        image_pull_secret = ""
        if self.args.image_pull_secret:
            image_pull_secret = f"""
      imagePullSecrets:
        - name: {self.args.image_pull_secret}"""
        yaml = f"""
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: stress-netmon-{self.run_id}
  namespace: {self.args.namespace}
  labels:
    app: amd-stress-netmon
    run-id: {self.run_id}
spec:
  selector:
    matchLabels:
      app: amd-stress-netmon
      run-id: {self.run_id}
  template:
    metadata:
      labels:
        app: amd-stress-netmon
        run-id: {self.run_id}
    spec:
      hostNetwork: true
      tolerations:
        - operator: Exists
{image_pull_secret}
      containers:
        - name: netmon
          image: {self.args.image}
          imagePullPolicy: IfNotPresent
          command: ["/bin/bash", "-lc"]
          args:
            - |
              while true; do
                ts=$(date +%s)
                awk -v ts="$ts" 'NR>2 {{ gsub(":","",$1); print "NETMON", ts, $1, $2, $10 }}' /proc/net/dev
                sleep {self.args.net_interval}
              done
          resources:
            requests:
              cpu: "10m"
              memory: "32Mi"
            limits:
              cpu: "100m"
              memory: "128Mi"
"""
        self.kubectl("apply", "-f", "-", input_text=yaml)

    def pod_yaml(self, i):
        image_pull_secret = ""
        if self.args.image_pull_secret:
            image_pull_secret = f"""
  imagePullSecrets:
    - name: {self.args.image_pull_secret}"""
        local_dir = f"/tmp/hf-download/{self.args.model.replace('/', '__')}"
        shell = f"""
set -o pipefail
echo "PHASE container_entry $(date -Is)"
echo "ACCOUNT {self.email(i)}"
echo "MODEL {self.args.model}"
echo "HF_ENDPOINT {self.args.hf_endpoint}"
export HF_ENDPOINT={self.args.hf_endpoint}
export HF_HUB_DISABLE_XET=1
export HF_HOME=/tmp/hf-home
export HUGGINGFACE_HUB_CACHE=/tmp/hf-cache
mkdir -p {local_dir} /tmp/hf-home /tmp/hf-cache
echo "PHASE env_ready $(date -Is)"
python3 --version || true
which hf || true
hf --version || true
echo "PHASE download_start $(date -Is)"
start=$(date +%s)
hf download {self.args.model} --local-dir {local_dir} --max-workers {self.args.hf_workers}
rc=$?
end=$(date +%s)
echo "PHASE download_end $(date -Is) rc=$rc seconds=$((end-start))"
du -sh {local_dir} || true
exit $rc
""".strip()
        return f"""
apiVersion: v1
kind: Pod
metadata:
  name: {self.name(i)}
  namespace: {self.args.namespace}
  labels:
    app: amd-stress-hf-download
    run-id: {self.run_id}
    virtual-account-index: "{i}"
  annotations:
    stress-test/email: {self.email(i)}
    stress-test/run-id: {self.run_id}
    stress-test/requested-at: "{iso_now()}"
spec:
  restartPolicy: Never
  dnsPolicy: None
  dnsConfig:
    nameservers: ["8.8.8.8", "8.8.4.4"]
    searches: ["default.svc.cluster.local", "svc.cluster.local", "cluster.local"]
    options:
      - name: ndots
        value: "5"
  tolerations:
    - key: amd.com/gpu
      operator: Exists
      effect: NoSchedule
{image_pull_secret}
  containers:
    - name: worker
      image: {self.args.image}
      imagePullPolicy: IfNotPresent
      command: ["/bin/bash", "-lc"]
      args:
        - |
{textwrap.indent(shell, '          ')}
      resources:
        requests:
          cpu: "{self.args.cpu_request}"
          memory: "{self.args.memory_request}"
          amd.com/gpu: "{self.args.gpu}"
        limits:
          cpu: "{self.args.cpu_limit}"
          memory: "{self.args.memory_limit}"
          amd.com/gpu: "{self.args.gpu}"
      volumeMounts:
        - name: shm
          mountPath: /dev/shm
  volumes:
    - name: shm
      emptyDir:
        medium: Memory
        sizeLimit: {self.args.shm_size}
"""

    def create_pods(self):
        yamls = self.artifacts / "pod-yamls"
        yamls.mkdir(exist_ok=True)
        for i in range(self.args.count):
            name = self.name(i)
            path = yamls / f"{name}.yaml"
            path.write_text(self.pod_yaml(i))
            t = iso_now()
            print(f"{t} create {i+1}/{self.args.count} {name}", flush=True)
            self.kubectl("apply", "-f", str(path))
            self.launch_records.append({"index": i, "pod": name, "requested_at": t, "email": self.email(i)})
            (self.artifacts / "launch_records.json").write_text(json.dumps(self.launch_records, indent=2))
            if self.args.launch_interval > 0 and i != self.args.count - 1:
                time.sleep(self.args.launch_interval)

    def get_pods_json(self):
        return json.loads(self.kubectl("get", "pods", "-n", self.args.namespace, "-l", f"run-id={self.run_id},app=amd-stress-hf-download", "-o", "json").stdout)

    def wait(self):
        deadline = time.time() + self.args.timeout_minutes * 60
        while time.time() < deadline:
            data = self.get_pods_json()
            counts = {}
            for pod in data.get("items", []):
                phase = pod.get("status", {}).get("phase", "Unknown")
                counts[phase] = counts.get(phase, 0) + 1
            print(f"{iso_now()} status={counts}", flush=True)
            done = counts.get("Succeeded", 0) + counts.get("Failed", 0)
            if done >= self.args.count:
                return
            time.sleep(self.args.poll_interval)
        print(f"{iso_now()} timeout reached; collecting partial results", flush=True)

    def collect(self):
        data = self.get_pods_json()
        (self.artifacts / "pods.json").write_text(json.dumps(data, indent=2))
        logs_dir = self.artifacts / "pod-logs"; logs_dir.mkdir(exist_ok=True)
        events_dir = self.artifacts / "pod-events"; events_dir.mkdir(exist_ok=True)
        for pod in data.get("items", []):
            name = pod["metadata"]["name"]
            (logs_dir / f"{name}.log").write_text(self.kubectl("logs", name, "-n", self.args.namespace, "--tail=-1", check=False).stdout or "")
            (events_dir / f"{name}.txt").write_text(self.kubectl("describe", "pod", name, "-n", self.args.namespace, check=False).stdout or "")
        netdir = self.artifacts / "netmon-logs"; netdir.mkdir(exist_ok=True)
        nm = self.kubectl("get", "pods", "-n", self.args.namespace, "-l", f"run-id={self.run_id},app=amd-stress-netmon", "-o", "json", check=False)
        if nm.returncode == 0:
            for pod in json.loads(nm.stdout).get("items", []):
                name = pod["metadata"]["name"]; node = pod["spec"].get("nodeName", "unknown")
                (netdir / f"{node}-{name}.log").write_text(self.kubectl("logs", name, "-n", self.args.namespace, "--tail=-1", check=False).stdout or "")
        self.write_report()

    def phase_times(self, text):
        phases = {}
        for line in text.splitlines():
            if line.startswith("PHASE "):
                parts = line.split(maxsplit=3)
                if len(parts) >= 3:
                    phases[parts[1]] = parts[2] + (" " + parts[3] if len(parts) == 4 else "")
        return phases

    def parse_download_seconds(self, phases):
        end = phases.get("download_end", "")
        m = re.search(r"seconds=(\d+)", end)
        return int(m.group(1)) if m else None

    def classify_failure(self, log, phase, reason):
        if "429 Client Error" in log or "Too Many Requests" in log:
            return "hf_429_rate_limited"
        if "ConnectTimeout" in log or "timed out" in log:
            return "network_timeout"
        if "No space left" in log:
            return "disk_full"
        if reason:
            return reason
        if phase == "Failed":
            return "failed_unknown"
        return ""

    def pod_metrics(self, pod):
        name = pod["metadata"]["name"]
        meta = pod.get("metadata", {})
        status = pod.get("status", {})
        cs = (status.get("containerStatuses") or [{}])[0]
        conditions = {c.get("type"): c for c in status.get("conditions", [])}
        created = parse_dt(meta.get("creationTimestamp"))
        scheduled = parse_dt((conditions.get("PodScheduled") or {}).get("lastTransitionTime"))
        started = None; finished = None; exit_code = ""; reason = ""
        state = cs.get("state", {})
        last_state = cs.get("lastState", {})
        term = state.get("terminated") or last_state.get("terminated")
        if state.get("running"):
            started = parse_dt(state["running"].get("startedAt"))
        if term:
            started = parse_dt(term.get("startedAt")) or started
            finished = parse_dt(term.get("finishedAt"))
            reason = term.get("reason", "")
            exit_code = term.get("exitCode", "")
        elif state.get("waiting"):
            reason = state["waiting"].get("reason", "")
        log = (self.artifacts / "pod-logs" / f"{name}.log").read_text(errors="ignore") if (self.artifacts / "pod-logs" / f"{name}.log").exists() else ""
        phases = self.phase_times(log)
        download_seconds = self.parse_download_seconds(phases)
        phase = status.get("phase", "Unknown")
        return {
            "name": name,
            "phase": phase,
            "node": pod.get("spec", {}).get("nodeName", ""),
            "created": created,
            "scheduled": scheduled,
            "started": started,
            "finished": finished,
            "schedule_seconds": seconds_between(created, scheduled),
            "startup_seconds": seconds_between(created, started),
            "run_seconds": seconds_between(started, finished),
            "download_seconds": download_seconds,
            "download_started": "download_start" in phases,
            "download_finished": "download_end" in phases,
            "download_succeeded": phase == "Succeeded" and ("rc=0" in phases.get("download_end", "")),
            "reason": reason,
            "exit_code": exit_code,
            "failure_class": self.classify_failure(log, phase, reason),
        }

    def bandwidth_summary(self):
        rows = []
        for file in (self.artifacts / "netmon-logs").glob("*.log"):
            by_iface = {}
            for line in file.read_text(errors="ignore").splitlines():
                parts = line.split()
                if len(parts) != 5 or parts[0] != "NETMON": continue
                _, ts, iface, rx, tx = parts
                if iface == "lo" or iface.startswith("veth") or iface.startswith("cni") or iface.startswith("flannel"): continue
                by_iface.setdefault(iface, []).append((int(ts), int(rx), int(tx)))
            for iface, vals in by_iface.items():
                if len(vals) < 2: continue
                first, last = vals[0], vals[-1]
                dur = max(1, last[0]-first[0])
                rx = (last[1]-first[1])/1024/1024; tx = (last[2]-first[2])/1024/1024
                rows.append((file.name, iface, dur, rx, tx, rx/dur, tx/dur))
        return sorted(rows, key=lambda r: r[5]+r[6], reverse=True)[:30]

    def write_report(self):
        data = json.loads((self.artifacts / "pods.json").read_text())
        metrics = [self.pod_metrics(p) for p in data.get("items", [])]
        counts = {}; failures = {}; node_counts = {}
        for m in metrics:
            counts[m["phase"]] = counts.get(m["phase"], 0)+1
            if m["failure_class"]: failures[m["failure_class"]] = failures.get(m["failure_class"],0)+1
            if m["node"]: node_counts[m["node"]] = node_counts.get(m["node"],0)+1
        startup_success = [m["startup_seconds"] for m in metrics if m["started"]]
        schedule_times = [m["schedule_seconds"] for m in metrics if m["scheduled"]]
        download_success = [m["download_seconds"] for m in metrics if m["download_succeeded"]]
        download_attempted = sum(1 for m in metrics if m["download_started"])
        lines = [f"# Cluster Stress Test Report `{self.run_id}`", ""]
        lines += [f"- Generated: {iso_now()}", f"- Namespace: `{self.args.namespace}`", f"- Pods requested: `{self.args.count}`", f"- Launch interval: `{self.args.launch_interval}s`", f"- Image: `{self.args.image}`", f"- Model: `{self.args.model}`", f"- HF_ENDPOINT: `{self.args.hf_endpoint}`", f"- HF workers per pod: `{self.args.hf_workers}`", ""]
        lines += ["## Executive Summary", ""]
        lines += [f"- Pod phases: `{counts}`", f"- Container startup success: `{len(startup_success)}/{self.args.count}`", f"- Download attempted: `{download_attempted}/{self.args.count}`", f"- Download succeeded: `{len(download_success)}/{self.args.count}`", f"- Failure classes: `{failures}`", ""]
        lines += ["## Timing Metrics", "", f"- Scheduling time: {fmt_stats(stat_values(schedule_times))}", f"- Container startup time: {fmt_stats(stat_values(startup_success))}", f"- Download success time: {fmt_stats(stat_values(download_success))}", ""]
        lines += ["## Node Placement", ""] + [f"- {n}: {c}" for n,c in sorted(node_counts.items())] + [""]
        lines += ["## Bandwidth Samples (top interfaces)", "", "| Node/Pod log | iface | seconds | RX MB | TX MB | RX MB/s | TX MB/s |", "|---|---:|---:|---:|---:|---:|---:|"]
        for f, iface, dur, rx, tx, rxs, txs in self.bandwidth_summary(): lines.append(f"| {f} | {iface} | {dur} | {rx:.1f} | {tx:.1f} | {rxs:.2f} | {txs:.2f} |")
        lines += ["", "## Pod Details", "", "| Pod | Phase | Node | Startup(s) | Download(s) | Failure | Exit |", "|---|---|---|---:|---:|---|---:|"]
        for m in metrics:
            startup = '' if m['startup_seconds'] is None else f"{m['startup_seconds']:.1f}"
            download = '' if m['download_seconds'] is None else str(m['download_seconds'])
            lines.append(f"| {m['name']} | {m['phase']} | {m['node']} | {startup} | {download} | {m['failure_class']} | {m['exit_code']} |")
        (self.artifacts / "REPORT.md").write_text("\n".join(lines)+"\n")
        (self.artifacts / "metrics.json").write_text(json.dumps(metrics, default=str, indent=2))
        print(f"Report written to {self.artifacts / 'REPORT.md'}")

    def cleanup(self):
        if self.args.no_cleanup:
            print("Skipping cleanup because --no-cleanup was set")
            return
        print("Cleaning up stress test resources...")
        self.kubectl("delete", "pods", "-n", self.args.namespace, "-l", f"run-id={self.run_id},app=amd-stress-hf-download", "--ignore-not-found", "--wait=false", check=False)
        self.kubectl("delete", "daemonset", f"stress-netmon-{self.run_id}", "-n", self.args.namespace, "--ignore-not-found", "--wait=false", check=False)

    def run(self):
        self.confirm(); print(f"Run ID: {self.run_id}", flush=True)
        self.require_image_pull_secret()
        try:
            self.deploy_netmon(); self.create_pods(); self.wait()
        finally:
            self.collect(); self.cleanup()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--kubeconfig", default="/home/hfang/AMD-OneClick/cluster.yaml")
    p.add_argument("--namespace", default="default")
    p.add_argument("--count", type=int, default=100)
    p.add_argument("--launch-interval", type=float, default=2.0)
    p.add_argument("--gpu", type=int, default=1)
    p.add_argument("--image", default=DEFAULT_IMAGE)
    p.add_argument("--image-pull-secret", default=DEFAULT_IMAGE_PULL_SECRET)
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--hf-endpoint", default=DEFAULT_HF_ENDPOINT)
    p.add_argument("--hf-workers", type=int, default=2)
    p.add_argument("--cpu-request", default="2")
    p.add_argument("--cpu-limit", default="8")
    p.add_argument("--memory-request", default="8Gi")
    p.add_argument("--memory-limit", default="32Gi")
    p.add_argument("--shm-size", default="16Gi")
    p.add_argument("--timeout-minutes", type=int, default=120)
    p.add_argument("--poll-interval", type=int, default=15)
    p.add_argument("--net-interval", type=int, default=5)
    p.add_argument("--output-dir")
    p.add_argument("--run-id")
    p.add_argument("--no-cleanup", action="store_true")
    p.add_argument("--yes", action="store_true")
    StressTest(p.parse_args()).run()

if __name__ == "__main__": main()
