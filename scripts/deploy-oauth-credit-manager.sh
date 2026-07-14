#!/usr/bin/env bash
# =============================================================================
# Runbook: build + roll out the OAuth/credit-manager merge onto the radeon-global
# (lablab) manager. Ports email OTP login, GeeTest CAPTCHA gating, OAuth callback
# idempotency, persistent manager logging + audit logs. All new feature flags
# (EMAIL_LOGIN_ENABLED, CAPTCHA_ENABLED) default OFF, so this is a code-only roll
# that does not activate email login or CAPTCHA until the relevant env/secrets are
# set. PHASED so each step is triggered explicitly.
#
#   ./scripts/deploy-oauth-credit-manager.sh status    # current image + rollout
#   ./scripts/deploy-oauth-credit-manager.sh build     # kaniko build FROM live image + COPY source -> Harbor
#   ./scripts/deploy-oauth-credit-manager.sh verify     # smoke-test the freshly built image (flags off + routes present)
#   ./scripts/deploy-oauth-credit-manager.sh deploy     # PRE snapshot + kubectl set image (rolling) + APPLIED snapshot
#   ./scripts/deploy-oauth-credit-manager.sh rollback   # set image back to PRE_TAG
#   ./scripts/deploy-oauth-credit-manager.sh cleanup    # delete the throwaway build Pod
#
# The manager Deployment is RollingUpdate maxSurge=0/maxUnavailable=1 over 3
# replicas, so one replica keeps serving /health and / throughout the rollout.
# =============================================================================
set -euo pipefail

export KUBECONFIG="${KUBECONFIG:-/home/zijun/128-nodes-config.yml}"
NS=amd-oneclick-lablab
DEPLOY=amd-oneclick-lablab-manager
CONTAINER=manager
REGISTRY=10.5.10.89:1808/xinwei/amd-oneclick-manager
PRE_TAG=opencode-off-20260711-1101         # currently-deployed image == rollback target
NEW_TAG="${NEW_TAG:-oauth-credit-mgr-20260714-2358}"  # image this runbook builds and rolls
BUILD_POD=manager-build-oauth-credit
# kaniko executor (debug variant ships /busybox/sh); already cached on the fleet nodes.
KANIKO_IMG=gcr.m.daocloud.io/kaniko-project/executor:debug
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

case "${1:-}" in
  status)
    echo "Deployed image: $(kubectl get deploy $DEPLOY -n $NS -o jsonpath='{.spec.template.spec.containers[0].image}')"
    echo "Rollout: $(kubectl get deploy $DEPLOY -n $NS -o jsonpath='ready={.status.readyReplicas}/{.spec.replicas} updated={.status.updatedReplicas}')"
    echo "Target tag to build/roll: ${NEW_TAG}"
    ;;

  build)
    # 1) Assemble the build context locally: Dockerfile.thin + current working-tree source.
    CTX="$(mktemp -d)"
    cp "$REPO_ROOT/Dockerfile.thin" "$CTX/Dockerfile.thin"
    cp -r "$REPO_ROOT/app" "$REPO_ROOT/templates" "$REPO_ROOT/static" "$CTX/"
    find "$CTX" -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true
    echo "Context assembled at $CTX ($(du -sh "$CTX" | cut -f1))"

    # 2) Launch the kaniko build Pod. initContainer waits until we stream the context in and
    #    touch /workspace/context/.ready, then kaniko builds and pushes to Harbor.
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

    echo "Waiting for the initContainer to start (so we can stream the context)..."
    # Allow generous time: the kaniko image can take >60s to pull on a node that has
    # not cached it yet. Break as soon as wait-context is running (or already done).
    for _ in $(seq 1 300); do
      st=$(kubectl get pod "${BUILD_POD}" -n "$NS" -o jsonpath='{.status.initContainerStatuses[0].state.running.startedAt}' 2>/dev/null || true)
      [ -n "$st" ] && break
      term=$(kubectl get pod "${BUILD_POD}" -n "$NS" -o jsonpath='{.status.initContainerStatuses[0].state.terminated.reason}' 2>/dev/null || true)
      [ -n "$term" ] && break
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
    # Throwaway pod from the freshly built tag. Confirms the new auth features are in the image,
    # both feature flags default OFF (so a bare roll does not enable email login / CAPTCHA), and
    # the review-hardening helpers landed. Imports app.config only (no DB / no app.main).
    kubectl run "oauth-verify-$(date +%s)" -n "$NS" --rm -i --restart=Never \
      --image="${REGISTRY}:${NEW_TAG}" --image-pull-policy=Always --command -- \
      sh -c '
set -e
python -c "from app.config import settings; print(\"EMAIL_LOGIN_ENABLED=\", settings.EMAIL_LOGIN_ENABLED, \"CAPTCHA_ENABLED=\", settings.CAPTCHA_ENABLED); assert settings.EMAIL_LOGIN_ENABLED is False; assert settings.CAPTCHA_ENABLED is False"
for r in "/auth/email/request-code" "/auth/email/verify" "/auth/captcha/gate"; do
  grep -q "$r" /app/app/main.py && echo "route present: $r" || { echo "MISSING route: $r"; exit 1; }
done
grep -q "def _validate_oauth_state" /app/app/main.py || { echo "MISSING _validate_oauth_state"; exit 1; }
grep -q "def rate_limit_at_capacity" /app/app/redis_client.py || { echo "MISSING rate_limit_at_capacity"; exit 1; }
grep -q "def send_verification_code_email" /app/app/email_service.py || { echo "MISSING send_verification_code_email"; exit 1; }
echo "IMAGE VERIFY OK"
'
    ;;

  deploy)
    TS=$(date +%Y%m%d-%H%M)
    HIST="$REPO_ROOT/local-deploy-history/radeon-global"
    mkdir -p "$HIST"
    PRE="$HIST/${TS}-oauth-credit-mgr-PRE-deploy.yaml"
    APPLIED="$HIST/${TS}-oauth-credit-mgr-APPLIED-deploy.yaml"
    echo "Current image: $(kubectl get deploy $DEPLOY -n $NS -o jsonpath='{.spec.template.spec.containers[0].image}')"
    echo "Target  image: ${REGISTRY}:${NEW_TAG}"
    # Snapshot BEFORE mutating so a rollback reference always exists, even if the rollout stalls.
    kubectl get deploy "$DEPLOY" -n "$NS" -o yaml > "$PRE"; echo "PRE snapshot: $PRE"
    kubectl set image "deployment/$DEPLOY" -n "$NS" "$CONTAINER=${REGISTRY}:${NEW_TAG}"
    kubectl rollout status "deployment/$DEPLOY" -n "$NS" --timeout=300s || \
      echo "WARNING: rollout not complete within timeout -- inspect pod logs; roll back with: $0 rollback"
    kubectl get deploy "$DEPLOY" -n "$NS" -o yaml > "$APPLIED"; echo "APPLIED snapshot: $APPLIED"
    ;;

  rollback)
    echo "Rolling back to ${REGISTRY}:${PRE_TAG}"
    kubectl set image "deployment/$DEPLOY" -n "$NS" "$CONTAINER=${REGISTRY}:${PRE_TAG}"
    kubectl rollout status "deployment/$DEPLOY" -n "$NS" --timeout=180s
    ;;

  cleanup)
    kubectl delete pod "${BUILD_POD}" -n "$NS" --ignore-not-found
    ;;

  *)
    echo "usage: $0 {status|build|verify|deploy|rollback|cleanup}"; exit 2 ;;
esac
