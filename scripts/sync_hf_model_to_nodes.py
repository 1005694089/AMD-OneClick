#!/usr/bin/env python3
"""
Sync Hugging Face models to every Kubernetes node.

The DaemonSet downloads into a hostPath-backed Hugging Face cache:

  host:      /var/lib/amd-oneclick/hf-cache
  container: /root/.cache/huggingface

Notebook instances mount the same hostPath at the same container path, so a
future instance sees the model as already cached, equivalent to having run:

  HF_HOME=/root/.cache/huggingface hf download <model>

Examples:

  python3 scripts/sync_hf_model_to_nodes.py \\
    --model qwen/Qwen3-8B \\
    --hf-endpoint http://134.199.133.77 \\
    --kubeconfig /home/hfang/AMD-OneClick/cluster.yaml \\
    --yes

  python3 scripts/sync_hf_model_to_nodes.py --model Qwen/Qwen2.5-7B-Instruct --wait
"""
import argparse
import hashlib
import re
import subprocess
import sys
import time


DEFAULT_IMAGE = "radeon-cloud-registry.cn-shanghai.cr.aliyuncs.com/admin/amd-oneclick-base:rocm7.2.1-py3.12-v20260416"
DEFAULT_IMAGE_PULL_SECRET = "acr-enterprise-pull"
DEFAULT_CACHE_HOST_PATH = "/var/lib/amd-oneclick/hf-cache"
DEFAULT_CACHE_MOUNT_PATH = "/root/.cache/huggingface"


def run(cmd, input_text=None, check=True):
    proc = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"Command failed ({proc.returncode}): {' '.join(cmd)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return proc


def safe_name(model: str) -> str:
    slug = re.sub(r"[^a-z0-9-]+", "-", model.lower()).strip("-")
    digest = hashlib.sha1(model.encode()).hexdigest()[:8]
    return f"hf-sync-{slug[:36]}-{digest}"[:63]


def build_manifest(args) -> str:
    name = safe_name(args.model)
    hf_endpoint_line = f"export HF_ENDPOINT={args.hf_endpoint}" if args.hf_endpoint else ""
    image_pull_secret = ""
    if args.image_pull_secret:
        image_pull_secret = f"""
      imagePullSecrets:
        - name: {args.image_pull_secret}"""
    return f"""
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: {name}
  namespace: {args.namespace}
  labels:
    app: amd-oneclick-hf-sync
    model-hash: "{hashlib.sha1(args.model.encode()).hexdigest()[:12]}"
spec:
  selector:
    matchLabels:
      app: amd-oneclick-hf-sync
      model-hash: "{hashlib.sha1(args.model.encode()).hexdigest()[:12]}"
  template:
    metadata:
      labels:
        app: amd-oneclick-hf-sync
        model-hash: "{hashlib.sha1(args.model.encode()).hexdigest()[:12]}"
      annotations:
        amd-oneclick/model: "{args.model}"
        amd-oneclick/hf-endpoint: "{args.hf_endpoint or ''}"
    spec:
      tolerations:
        - operator: Exists
{image_pull_secret}
      containers:
        - name: sync
          image: {args.image}
          imagePullPolicy: IfNotPresent
          command: ["/bin/bash", "-lc"]
          args:
            - |
              set -eo pipefail
              export HF_HOME={args.cache_mount_path}
              export HUGGINGFACE_HUB_CACHE={args.cache_mount_path}
              export HF_HUB_DISABLE_XET={args.disable_xet}
              {hf_endpoint_line}
              mkdir -p {args.cache_mount_path}
              echo "SYNC_START $(date -Is) model={args.model} node=$(hostname)"
              hf download {args.model} --max-workers {args.max_workers}
              echo "SYNC_DONE $(date -Is) model={args.model} node=$(hostname)"
              touch {args.cache_mount_path}/.amd-oneclick-{hashlib.sha1(args.model.encode()).hexdigest()[:12]}.ready
              sleep {args.hold_seconds}
          resources:
            requests:
              cpu: "{args.cpu_request}"
              memory: "{args.memory_request}"
            limits:
              cpu: "{args.cpu_limit}"
              memory: "{args.memory_limit}"
          volumeMounts:
            - name: hf-cache
              mountPath: {args.cache_mount_path}
      volumes:
        - name: hf-cache
          hostPath:
            path: {args.cache_host_path}
            type: DirectoryOrCreate
"""


def kubectl_base(kubeconfig):
    cmd = ["kubectl"]
    if kubeconfig:
        cmd.append(f"--kubeconfig={kubeconfig}")
    return cmd


def require_image_pull_secret(cmd, namespace: str, secret_name: str):
    if not secret_name:
        return
    proc = run(cmd + ["get", "secret", secret_name, "-n", namespace], check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"image pull secret {secret_name!r} was not found in namespace {namespace!r}. "
            "Create it there or pass --image-pull-secret ''."
        )


def wait_ready(args, name):
    cmd = kubectl_base(args.kubeconfig)
    deadline = time.time() + args.timeout_minutes * 60
    while time.time() < deadline:
        proc = run(cmd + ["get", "ds", name, "-n", args.namespace, "-o", "jsonpath={.status.numberReady} {.status.desiredNumberScheduled}"], check=False)
        output = (proc.stdout or "").strip()
        print(f"{time.strftime('%H:%M:%S')} {name}: {output or 'pending'}", flush=True)
        if output:
            ready, desired = [int(x or 0) for x in output.split()]
            threshold = max(1, int(desired * args.ready_threshold + 0.999)) if desired else 0
            if ready >= threshold:
                print(f"Ready threshold reached: {ready}/{desired} (threshold={threshold})")
                return 0
        time.sleep(args.poll_interval)
    print(f"Timed out waiting for {name}", file=sys.stderr)
    return 1


def main():
    parser = argparse.ArgumentParser(description="Sync a Hugging Face model to every cluster node")
    parser.add_argument("--model", required=True, help="HF repo id, e.g. qwen/Qwen3-8B")
    parser.add_argument("--hf-endpoint", default="", help="Optional HF_ENDPOINT, e.g. http://134.199.133.77")
    parser.add_argument("--kubeconfig", default="/home/hfang/AMD-OneClick/cluster.yaml")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--image-pull-secret", default=DEFAULT_IMAGE_PULL_SECRET)
    parser.add_argument("--cache-host-path", default=DEFAULT_CACHE_HOST_PATH)
    parser.add_argument("--cache-mount-path", default=DEFAULT_CACHE_MOUNT_PATH)
    parser.add_argument("--disable-xet", default="1")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--cpu-request", default="500m")
    parser.add_argument("--cpu-limit", default="4")
    parser.add_argument("--memory-request", default="4Gi")
    parser.add_argument("--memory-limit", default="16Gi")
    parser.add_argument("--hold-seconds", type=int, default=86400)
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--timeout-minutes", type=int, default=240)
    parser.add_argument("--poll-interval", type=int, default=30)
    parser.add_argument("--ready-threshold", type=float, default=0.8)
    parser.add_argument("--delete", action="store_true", help="Delete the sync DaemonSet for this model")
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    name = safe_name(args.model)
    cmd = kubectl_base(args.kubeconfig)

    if args.delete:
        run(cmd + ["delete", "ds", name, "-n", args.namespace, "--ignore-not-found"], check=False)
        return

    require_image_pull_secret(cmd, args.namespace, args.image_pull_secret)

    if not args.yes:
        print(f"Will create/update DaemonSet {name} to sync {args.model} into {args.cache_host_path}")
        if input("Type yes to continue: ").strip().lower() != "yes":
            raise SystemExit("aborted")

    manifest = build_manifest(args)
    run(cmd + ["apply", "-f", "-"], input_text=manifest)
    print(f"Applied DaemonSet {name}")
    if args.wait:
        raise SystemExit(wait_ready(args, name))


if __name__ == "__main__":
    main()
