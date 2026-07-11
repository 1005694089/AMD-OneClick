# Disable OpenCode — deploy prep + rollback notes

Prepared 2026-07-11. **Not yet rolled out** — review, then trigger via
`scripts/deploy-opencode-off.sh`.

## What changed

**Code (`prod/radeon-global`, uncommitted):**
- `app/config.py`: new master switch `OPENCODE_ENABLED` (env `OPENCODE_ENABLED`, default **off**),
  gating only the OpenCode **web server + NodePort**. The `opencode` instance type stays
  `enabled: True` on purpose — it is the default notebook launcher (the UI hardcodes
  `instance_type=opencode` for the main + custom-image launches), so an opencode-type launch simply
  degrades to **Jupyter-only** when the switch is off. (Disabling the type would 400 the primary
  launch path.)
- `app/k8s_client.py`: gated on `settings.OPENCODE_ENABLED` — no per-instance opencode NodePort is
  allocated (`_allocate_instance_node_ports` returns `opencode_node_port=None`), the pod no longer
  starts opencode-web (`_service_launch_snippet`), and the opencode env vars + container port are
  omitted. Instances drop from a jupyter+opencode NodePort pair to a single jupyter port.
- Tests: `tests/test_k8s_nodeport.py`, `tests/test_opencode_merge_fixes.py` updated (enabled-path
  tests pin `OPENCODE_ENABLED=True`) + new disabled-by-default coverage. Full suite: 378 passed, 1 skipped.

**Cluster ops already done (immediate release):**
- Removed the opencode (4096) port from all 436 per-instance services (jupyter/ssh untouched).
- Deleted the global opencode TLS proxy stack → **NodePort 30450 freed** (manager never recreates it).
- Backups: `/tmp/opencode-nodeport-backup-20260711-103955/` (full service YAML +
  `opencode-nodeports.txt` + `opencode-tls-proxy-stack.yaml`).

> Until the manager runs the new image it keeps re-allocating opencode NodePorts on each
> launch/relaunch (that's why `sweep` runs last, after the rollout).

## Roll out (phased; one replica stays up throughout)

```bash
./scripts/deploy-opencode-off.sh status    # deployed image + live opencode port count
./scripts/deploy-opencode-off.sh build     # kaniko build FROM live image + COPY source -> Harbor:opencode-off-20260711-1101
./scripts/deploy-opencode-off.sh verify    # throwaway pod: asserts OPENCODE_ENABLED is False + gating present
./scripts/deploy-opencode-off.sh deploy    # confirm-gated: kubectl set image (RollingUpdate maxSurge=0/maxUnavailable=1)
./scripts/deploy-opencode-off.sh sweep     # confirm-gated: delete any opencode ports created before rollout finished
./scripts/deploy-opencode-off.sh cleanup   # delete the build Pod
```

Notes:
- Base image / rollback tag: `10.5.10.89:1808/xinwei/amd-oneclick-manager:hf-gpus-off-20260711-0316`.
- New tag built: `...:opencode-off-20260711-1101` (bump `Dockerfile.thin` FROM + `NEW_TAG` if a newer base is rolled first).
- Harbor is a LAN registry on `:1808`; the build uses `--insecure --skip-tls-verify` — drop those if it serves valid public TLS.
- PRE snapshot (taken during prep): `local-deploy-history/radeon-global/20260711-1101-opencode-off-PRE-deploy.yaml`. The `deploy` phase also writes its own fresh timestamped `*-PRE-deploy.yaml` immediately before `kubectl set image`.

## Rollback

Image only (behaviour reverts to opencode-enabled for *new* launches; already-freed ports/30450 stay freed):

```bash
./scripts/deploy-opencode-off.sh rollback
# == kubectl -n amd-oneclick-lablab set image deployment/amd-oneclick-lablab-manager \
#      manager=10.5.10.89:1808/xinwei/amd-oneclick-manager:hf-gpus-off-20260711-0316
```

Alternatively, on the new image you can re-enable OpenCode without a rollback by setting
`OPENCODE_ENABLED=true` in the manager ConfigMap/env and `rollout restart` (new launches get the
opencode NodePort + web again).

## Optional config cleanup (recommended)

The live ConfigMap `amd-oneclick-lablab-config` still carries
`OPENCODE_PUBLIC_BASE_URL=https://radeon-global.anruicloud.com:30450`, which points at the now-deleted
TLS proxy / freed NodePort 30450. It is harmless while `OPENCODE_ENABLED` is off (opencode URLs
resolve to `None` before that branch), but it's a footgun if OpenCode is ever re-enabled. Clear it:

```bash
kubectl -n amd-oneclick-lablab patch configmap amd-oneclick-lablab-config \
  --type=json -p '[{"op":"remove","path":"/data/OPENCODE_PUBLIC_BASE_URL"}]'
# then it takes effect on the next manager rollout (the deploy above already restarts pods)
```

## After deploy

Add a `deploy-ledger.md` entry (per `.cursor/rules/deploy-ledger-commit.mdc`) and commit the code +
ledger. The opencode-web binary is still baked into the notebook base image (harmless, unused); the
`DOCKERFILE_SUFFIX` was intentionally left unchanged.
