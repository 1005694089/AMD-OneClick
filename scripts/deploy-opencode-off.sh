#!/usr/bin/env bash
# =============================================================================
# Runbook: build + roll out the "disable OpenCode" manager change, then free any
# straggler per-instance opencode NodePorts. PHASED and confirmation-gated so you
# can review and trigger each step yourself.
#
#   ./scripts/deploy-opencode-off.sh status     # show current image + opencode port count
#   ./scripts/deploy-opencode-off.sh build      # kaniko build FROM live image + COPY source -> Harbor
#   ./scripts/deploy-opencode-off.sh verify      # smoke-test the freshly built image (flag + source)
#   ./scripts/deploy-opencode-off.sh deploy      # PRE snapshot + kubectl set image (rolling)
#   ./scripts/deploy-opencode-off.sh sweep       # delete remaining opencode (4096) NodePorts cluster-wide
#   ./scripts/deploy-opencode-off.sh rollback    # set image back to the PRE tag
#   ./scripts/deploy-opencode-off.sh cleanup     # delete the throwaway build Pod
#
# Recommended order: status -> build -> verify -> deploy -> sweep -> cleanup.
# The manager Deployment is RollingUpdate maxSurge=0/maxUnavailable=1 over 3 replicas,
# so one replica keeps serving /health and / throughout the rollout.
# =============================================================================
set -euo pipefail

export KUBECONFIG="${KUBECONFIG:-/home/zijun/128-nodes-config.yml}"
NS=amd-oneclick-lablab
DEPLOY=amd-oneclick-lablab-manager
CONTAINER=manager
REGISTRY=10.5.10.89:1808/xinwei/amd-oneclick-manager
PRE_TAG=hf-gpus-off-20260711-0316          # currently-deployed image == rollback target
NEW_TAG=opencode-off-20260711-1101         # image this runbook builds and rolls
BUILD_POD=manager-build-opencode-off
# kaniko executor (debug variant ships /busybox/sh); already cached on the fleet nodes.
KANIKO_IMG=gcr.m.daocloud.io/kaniko-project/executor:debug
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

confirm() { read -r -p "$1 [type 'yes' to proceed]: " a; [ "$a" = "yes" ] || { echo "aborted."; exit 1; }; }

opencode_port_count() {
  kubectl get svc -A -o json | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(sum(1 for i in d['items'] for p in i['spec'].get('ports',[]) if p.get('name')=='opencode'))
"
}

case "${1:-}" in
  status)
    echo "Deployed image: $(kubectl get deploy $DEPLOY -n $NS -o jsonpath='{.spec.template.spec.containers[0].image}')"
    echo "Rollout: $(kubectl get deploy $DEPLOY -n $NS -o jsonpath='ready={.status.readyReplicas}/{.spec.replicas} updated={.status.updatedReplicas}')"
    echo "opencode NodePorts currently present: $(opencode_port_count)"
    ;;

  build)
    # 1) Assemble the build context locally: Dockerfile.thin + current working-tree source.
    CTX="$(mktemp -d)"
    cp "$REPO_ROOT/Dockerfile.thin" "$CTX/Dockerfile.thin"
    cp -r "$REPO_ROOT/app" "$REPO_ROOT/templates" "$REPO_ROOT/static" "$CTX/"
    find "$CTX" -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true
    echo "Context assembled at $CTX ($(du -sh "$CTX" | cut -f1))"

    # 2) Launch the kaniko build Pod. Its initContainer waits until we cp the context in and
    #    touch /workspace/context/.ready (kubectl cp into a ConfigMap-free emptyDir avoids the
    #    documented ..data symlink gotcha), then kaniko builds and pushes to Harbor.
    cat > "/tmp/${BUILD_POD}.yaml" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${BUILD_POD}
  namespace: ${NS}
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  initContainers:
    - name: wait-context
      image: ${KANIKO_IMG}
      command: ["/busybox/sh","-c","mkdir -p /workspace/context && echo 'waiting for context...' && until [ -f /workspace/context/.ready ]; do sleep 1; done && echo 'context ready'"]
      volumeMounts:
        - {name: context, mountPath: /workspace}
  containers:
    - name: kaniko
      image: ${KANIKO_IMG}
      args:
        - --dockerfile=/workspace/context/Dockerfile.thin
        - --context=dir:///workspace/context
        - --destination=${REGISTRY}:${NEW_TAG}
        - --single-snapshot        # source-only overlay collapses to one layer (fast)
        # LAN Harbor on :1808. --insecure/--skip-tls-verify are PUSH-only in kaniko; the *-pull
        # variants are REQUIRED because Dockerfile.thin's FROM base image is pulled from this same
        # registry (kaniko fetches it itself, not from the node cache). Drop all four if Harbor
        # serves valid public TLS.
        - --insecure
        - --skip-tls-verify
        - --insecure-pull
        - --skip-tls-verify-pull
      volumeMounts:
        - {name: context, mountPath: /workspace}
        - {name: harbor-auth, mountPath: /kaniko/.docker}
  volumes:
    - {name: context, emptyDir: {}}
    - name: harbor-auth
      secret:
        secretName: kaniko-harbor-auth
        items: [{key: config.json, path: config.json}]
YAML
    kubectl delete pod "${BUILD_POD}" -n "$NS" --ignore-not-found >/dev/null 2>&1 || true
    kubectl apply -f "/tmp/${BUILD_POD}.yaml"

    echo "Waiting for the initContainer to start (so kubectl cp can write the context)..."
    for _ in $(seq 1 60); do
      st=$(kubectl get pod "${BUILD_POD}" -n "$NS" -o jsonpath='{.status.initContainerStatuses[0].state.running.startedAt}' 2>/dev/null || true)
      [ -n "$st" ] && break
      sleep 1
    done

    # 3) Stream the context in and signal ready.
    tar -C "$CTX" -cf - . | kubectl exec -i "${BUILD_POD}" -n "$NS" -c wait-context -- \
      /busybox/sh -c 'tar -C /workspace/context -xf - && touch /workspace/context/.ready'
    rm -rf "$CTX"

    # 4) Follow the build/push, then report the final phase.
    echo "Building + pushing ${REGISTRY}:${NEW_TAG} ..."
    kubectl logs -f "${BUILD_POD}" -n "$NS" -c kaniko || true
    for _ in $(seq 1 120); do
      ph=$(kubectl get pod "${BUILD_POD}" -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null || true)
      [ "$ph" = "Succeeded" ] || [ "$ph" = "Failed" ] && break
      sleep 2
    done
    echo "Build Pod phase: $(kubectl get pod "${BUILD_POD}" -n "$NS" -o jsonpath='{.status.phase}')"
    echo "If Succeeded -> run: $0 verify"
    ;;

  verify)
    # Throwaway pod from the freshly built tag. Checks the master switch defaults OFF and the
    # gating actually landed in k8s_client.py. Imports app.config only (no DB / no app.main).
    kubectl run "oc-verify-$(date +%s)" -n "$NS" --rm -it --restart=Never \
      --image="${REGISTRY}:${NEW_TAG}" --image-pull-policy=Always --command -- \
      sh -c 'python -c "from app.config import settings; print(\"OPENCODE_ENABLED=\", settings.OPENCODE_ENABLED); assert settings.OPENCODE_ENABLED is False" && c=$(grep -c "settings.OPENCODE_ENABLED" /app/app/k8s_client.py) && echo "gating refs in k8s_client.py: $c" && [ "$c" -gt 0 ]'
    ;;

  deploy)
    TS=$(date +%Y%m%d-%H%M)
    HIST="$REPO_ROOT/local-deploy-history/radeon-global"
    PRE="$HIST/${TS}-opencode-off-PRE-deploy.yaml"
    APPLIED="$HIST/${TS}-opencode-off-APPLIED-deploy.yaml"
    echo "Current image: $(kubectl get deploy $DEPLOY -n $NS -o jsonpath='{.spec.template.spec.containers[0].image}')"
    echo "Target  image: ${REGISTRY}:${NEW_TAG}"
    # Snapshot BEFORE mutating so a rollback reference always exists, even if the rollout stalls.
    kubectl get deploy "$DEPLOY" -n "$NS" -o yaml > "$PRE"; echo "PRE snapshot: $PRE"
    confirm "Roll the manager (3 replicas, one stays up) to ${NEW_TAG}?"
    kubectl set image "deployment/$DEPLOY" -n "$NS" "$CONTAINER=${REGISTRY}:${NEW_TAG}"
    kubectl rollout status "deployment/$DEPLOY" -n "$NS" --timeout=180s || \
      echo "WARNING: rollout not complete within timeout -- inspect pod logs; roll back with: $0 rollback"
    kubectl get deploy "$DEPLOY" -n "$NS" -o yaml > "$APPLIED"; echo "APPLIED snapshot: $APPLIED"
    echo "Now run: $0 sweep   (to clear opencode ports created before the rollout finished)"
    ;;

  sweep)
    # Surgical strategic-merge delete of the opencode port (4096) from every Service that still
    # has one; jupyter/ssh ports are untouched. Safe to run repeatedly.
    n=$(opencode_port_count); echo "opencode NodePorts present: $n"
    [ "$n" -eq 0 ] && { echo "nothing to sweep."; exit 0; }
    confirm "Delete the opencode (4096) port from all $n services?"
    mapfile -t PAIRS < <(kubectl get svc -A -o json | python3 -c "
import json,sys
d=json.load(sys.stdin)
for i in d['items']:
    if any(p.get('name')=='opencode' for p in i['spec'].get('ports',[])):
        print(i['metadata']['namespace'], i['metadata']['name'])
")
    ok=0; fail=0
    for pair in "${PAIRS[@]}"; do
      ns="${pair%% *}"; name="${pair##* }"
      if kubectl patch svc "$name" -n "$ns" --type=strategic \
           -p '{"spec":{"ports":[{"port":4096,"$patch":"delete"}]}}' >/dev/null 2>&1; then ok=$((ok+1)); else echo "FAILED: $ns/$name"; fail=$((fail+1)); fi
    done
    echo "swept ok=$ok fail=$fail; remaining opencode ports: $(opencode_port_count)"
    ;;

  rollback)
    echo "Rolling back to ${REGISTRY}:${PRE_TAG}"
    confirm "Set manager image back to ${PRE_TAG}?"
    kubectl set image "deployment/$DEPLOY" -n "$NS" "$CONTAINER=${REGISTRY}:${PRE_TAG}"
    kubectl rollout status "deployment/$DEPLOY" -n "$NS" --timeout=180s
    ;;

  cleanup)
    kubectl delete pod "${BUILD_POD}" -n "$NS" --ignore-not-found
    ;;

  *)
    echo "usage: $0 {status|build|verify|deploy|sweep|rollback|cleanup}"; exit 2 ;;
esac
