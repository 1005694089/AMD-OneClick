#!/usr/bin/env bash
# Provision the AMD OneClick image-service daemon on the dedicated CPU node (0042).
#
# Run as root (sudo). This is intentionally idempotent and does NOT embed any
# secrets: you must supply the manager token, generate/distribute the node SSH key
# (DISTRIB_SSH_KEY), and perform the ACR Enterprise `docker login` into the
# dedicated DOCKER_CONFIG yourself (the daemon only ever uses those).
#
# Usage:
#   sudo MANAGER_URL=http://10.5.10.<mgr>:30092 \
#        BUILD_AGENT_TOKEN=xxxxxxxx \
#        IMAGE_SERVICE_NODE_NAME=wx-ms-w7900d-0042 \
#        CONTAINER_CLI=nerdctl \
#        DISTRIB_SSH_KEY=/disk/ssd2/.ssh/id_distrib \
#        IMAGE_WORK_DIR=/disk/ssd2 \
#        ACR_ENTERPRISE_REGISTRY=<acr-enterprise-host> \
#        bash image-service/setup.sh
set -euo pipefail

MANAGER_URL="${MANAGER_URL:?set MANAGER_URL}"
BUILD_AGENT_TOKEN="${BUILD_AGENT_TOKEN:?set BUILD_AGENT_TOKEN}"
IMAGE_SERVICE_NODE_NAME="${IMAGE_SERVICE_NODE_NAME:?set IMAGE_SERVICE_NODE_NAME (must match kubectl get nodes)}"
AGENT_USER="${AGENT_USER:-imagesvc}"
# Validate up front: AGENT_USER is interpolated into useradd, chown, AND a sed replacement
# string below. Restrict to the portable-safe username charset so a value containing sed
# metacharacters (& / \) can't silently corrupt the rewritten unit, and an odd value can't
# slip into chown/useradd. Fail loudly rather than write a broken service file.
if ! printf '%s' "${AGENT_USER}" | grep -Eq '^[a-z_][a-z0-9_-]*\$?$'; then
  echo "ERROR: AGENT_USER='${AGENT_USER}' is invalid; use [a-z_][a-z0-9_-]* (POSIX username chars)." >&2
  exit 1
fi
# The daemon refuses BUILDS (only) unless BUILD_NETWORK is set (fail-closed against
# unrestricted build-time egress). Default to "none" so a fresh install can build immediately
# and safely; "none" blocks ALL egress, so apt/pip in RUN steps will fail until the operator
# creates a restricted egress network and re-points BUILD_NETWORK at it (see notes below).
BUILD_NETWORK="${BUILD_NETWORK:-none}"
CONTAINER_CLI="${CONTAINER_CLI:-nerdctl}"
ACR_ENTERPRISE_REGISTRY="${ACR_ENTERPRISE_REGISTRY:-}"
CTR_NAMESPACE="${CTR_NAMESPACE:-k8s.io}"
NODE_SSH_USER="${NODE_SSH_USER:-root}"
DISTRIB_SSH_KEY="${DISTRIB_SSH_KEY:-/disk/ssd2/.ssh/id_distrib}"
IMAGE_WORK_DIR="${IMAGE_WORK_DIR:-/disk/ssd2}"
MIN_FREE_DISK_GB="${MIN_FREE_DISK_GB:-50}"
IMAGE_NODE_MIN_FREE_DISK_GB="${IMAGE_NODE_MIN_FREE_DISK_GB:-50}"
DISTRIBUTE_CONCURRENCY="${DISTRIBUTE_CONCURRENCY:-2}"
INSTALL_DIR="${INSTALL_DIR:-/opt/amd-oneclick/image-service}"
DOCKER_CONFIG_DIR="${DOCKER_CONFIG_DIR:-/etc/amd-oneclick/docker}"
ENV_FILE="/etc/amd-oneclick-image-service.env"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The directory holding the distribution SSH key is exposed ReadOnly to the unit.
SSH_KEY_DIR="$(dirname "${DISTRIB_SSH_KEY}")"

echo "==> Creating dedicated user '${AGENT_USER}' (no docker group; rootless nerdctl)"
# HOME is placed on /disk/ssd2 (NOT /home): ProtectHome=tmpfs hides real /home from the daemon, and
# rootless containerd/buildkit data must live on the big data disk anyway. XDG dirs below are set
# explicitly so nothing depends on $HOME at runtime.
AGENT_HOME="${IMAGE_WORK_DIR}/${AGENT_USER}"
id -u "${AGENT_USER}" >/dev/null 2>&1 || \
  useradd --system --home-dir "${AGENT_HOME}" --create-home --shell /usr/sbin/nologin "${AGENT_USER}"
AGENT_UID="$(id -u "${AGENT_USER}")"

echo "==> Preparing rootless XDG dirs under ${AGENT_HOME} on the data disk"
# Rootless containerd data-root and buildkit cache live here (XDG_DATA_HOME), so all image/build
# bytes stay on /disk/ssd2 (already in the unit's ReadWritePaths). XDG_CONFIG_HOME holds nerdctl/
# containerd/buildkit config. The rootless containerd/buildkit USER services own these dirs.
install -d -o "${AGENT_USER}" -g "${AGENT_USER}" -m 0750 "${AGENT_HOME}"
install -d -o "${AGENT_USER}" -g "${AGENT_USER}" -m 0700 \
  "${AGENT_HOME}/.local/share/containerd" \
  "${AGENT_HOME}/.local/share/buildkit" \
  "${AGENT_HOME}/.config"

echo "==> Installing agent to ${INSTALL_DIR}"
install -d -m 0755 "${INSTALL_DIR}"
install -m 0755 "${SRC_DIR}/agent.py" "${INSTALL_DIR}/agent.py"

echo "==> Preparing DOCKER_CONFIG dir outside \$HOME at ${DOCKER_CONFIG_DIR}"
install -d -o "${AGENT_USER}" -g "${AGENT_USER}" -m 0700 "${DOCKER_CONFIG_DIR}"

echo "==> Preparing image work dir ${IMAGE_WORK_DIR} (8-16 TB volume mount expected here)"
install -d -o "${AGENT_USER}" -g "${AGENT_USER}" -m 0750 "${IMAGE_WORK_DIR}"

echo "==> Preparing SSH key dir ${SSH_KEY_DIR} (holds DISTRIB_SSH_KEY, outside \$HOME)"
install -d -o "${AGENT_USER}" -g "${AGENT_USER}" -m 0700 "${SSH_KEY_DIR}"
if [ -f "${DISTRIB_SSH_KEY}" ]; then
  chown "${AGENT_USER}:${AGENT_USER}" "${DISTRIB_SSH_KEY}"
  chmod 0600 "${DISTRIB_SSH_KEY}"
else
  echo "    (warning: ${DISTRIB_SSH_KEY} not found; generate it per the runbook before starting)"
fi

echo "==> Writing ${ENV_FILE}"
umask 077
cat > "${ENV_FILE}" <<EOF
MANAGER_URL=${MANAGER_URL}
BUILD_AGENT_TOKEN=${BUILD_AGENT_TOKEN}
AGENT_ID=image-service-1
IMAGE_SERVICE_NODE_NAME=${IMAGE_SERVICE_NODE_NAME}
DOCKER_CONFIG=${DOCKER_CONFIG_DIR}
CONTAINER_CLI=${CONTAINER_CLI}
# --- Rootless nerdctl client coordinates (resolved uid=${AGENT_UID}) ---
# The daemon is only a CLIENT; rootless containerd+buildkit run as imagesvc `systemctl --user`
# services (see manual steps below). These point nerdctl at the rootless sockets and relocate its
# data/config onto /disk/ssd2. Without them nerdctl falls back to the ROOT system socket and fails
# under NoNewPrivileges. Do NOT point CONTAINER_CLI at any sudo wrapper (e.g. nerdctl-sys) — local
# ops must be pure-rootless, no sudo.
XDG_RUNTIME_DIR=/run/user/${AGENT_UID}
XDG_DATA_HOME=${AGENT_HOME}/.local/share
XDG_CONFIG_HOME=${AGENT_HOME}/.config
CONTAINERD_ADDRESS=/run/user/${AGENT_UID}/containerd/containerd.sock
BUILDKIT_HOST=unix:///run/user/${AGENT_UID}/buildkit/buildkitd.sock
CTR_NAMESPACE=${CTR_NAMESPACE}
NODE_SSH_USER=${NODE_SSH_USER}
DISTRIB_SSH_KEY=${DISTRIB_SSH_KEY}
ACR_ENTERPRISE_REGISTRY=${ACR_ENTERPRISE_REGISTRY}
IMAGE_WORK_DIR=${IMAGE_WORK_DIR}
# IMAGE_SERVICE_KINDS=build,pull,acr_backup,distribute,evict
# DOCKER_BUILDKIT only affects docker's classic builder; rootless `nerdctl build` ignores it and
# always uses buildkit. Harmless to leave for a docker fallback.
DOCKER_BUILDKIT=0
BUILD_TIMEOUT_SECONDS=1800
BUILD_MEMORY=8g
# BUILD_CPUSET=0-3
# BUILD_NETWORK selects the docker network for build-time RUN steps. The daemon fails closed
# (BUILDS ONLY) if this is empty. "none" (the safe default) gives NO egress; switch to a
# restricted egress network (see step 3 below) once created so apt/pip can reach approved
# mirrors only. pull/distribute/evict are unaffected by this setting.
BUILD_NETWORK=${BUILD_NETWORK}
MIN_FREE_DISK_GB=${MIN_FREE_DISK_GB}
IMAGE_NODE_MIN_FREE_DISK_GB=${IMAGE_NODE_MIN_FREE_DISK_GB}
DISTRIBUTE_CONCURRENCY=${DISTRIBUTE_CONCURRENCY}
POLL_INTERVAL_SECONDS=10
EOF
chmod 600 "${ENV_FILE}"

echo "==> Installing systemd unit (User/Group='${AGENT_USER}', ReadWritePaths=${IMAGE_WORK_DIR}, ReadOnlyPaths=${SSH_KEY_DIR}, BindPaths=/run/user/${AGENT_UID})"
# The shipped unit hard-codes User=imagesvc/Group=imagesvc, a default work dir, and a placeholder
# ssh-key dir. If the operator overrode AGENT_USER/IMAGE_WORK_DIR/DISTRIB_SSH_KEY, the installed
# copy must reflect that — otherwise the service runs as the wrong account, or (with
# ProtectSystem=strict) cannot write builds/tarballs to the work dir, or cannot read the key.
# Substitute into the installed copy rather than shipping a templated unit.
# ProtectHome=tmpfs masks /run/user, so the rootless containerd/buildkit sockets are reached via
# BindPaths=/run/user/${AGENT_UID} (NOT ReadWritePaths — BindPaths is what re-exposes the dir past
# ProtectHome). %U in the shipped unit resolves to the same uid; we rewrite the literal for
# consistency with the other operator-substituted paths.
sed -e "s/^User=.*/User=${AGENT_USER}/" \
    -e "s/^Group=.*/Group=${AGENT_USER}/" \
    -e "s|^ReadWritePaths=.*|ReadWritePaths=${DOCKER_CONFIG_DIR} ${IMAGE_WORK_DIR}|" \
    -e "s|^ReadOnlyPaths=.*|ReadOnlyPaths=${SSH_KEY_DIR}|" \
    -e "s|^BindPaths=.*|BindPaths=/run/user/${AGENT_UID}|" \
    "${SRC_DIR}/systemd/image-service.service" > /etc/systemd/system/image-service.service
chmod 0644 /etc/systemd/system/image-service.service
systemctl daemon-reload

cat <<NEXT

Provisioning done. Remaining MANUAL steps (require credentials/keys you control):

  1) Mount the 8-16 TB image volume at ${IMAGE_WORK_DIR} (see runbook B2), then:
       sudo chown -R ${AGENT_USER}:${AGENT_USER} ${IMAGE_WORK_DIR} && sudo chmod 0750 ${IMAGE_WORK_DIR}

  2) Provision the ROOTLESS nerdctl backend for '${AGENT_USER}' (uid=${AGENT_UID}). The daemon
     is only a CLIENT; rootless containerd+buildkit must run as ${AGENT_USER} --user services so
     uid-mapping happens in the user session (NOT under the hardened unit's NoNewPrivileges).
     Run on 0042, in order (see runbook Step 3 for detail):
       sudo apt-get install -y uidmap slirp4netns
       grep -q '^${AGENT_USER}:' /etc/subuid || sudo usermod --add-subuids 100000-165535 --add-subgids 100000-165535 ${AGENT_USER}
       sudo loginctl enable-linger ${AGENT_USER}                 # makes /run/user/${AGENT_UID} persist
       sudo tar -C /usr/local -xzf nerdctl-full-<ver>-linux-amd64.tar.gz   # ships containerd+buildkit+rootlesskit+CNI
       sudo -u ${AGENT_USER} env XDG_RUNTIME_DIR=/run/user/${AGENT_UID} XDG_DATA_HOME=${AGENT_HOME}/.local/share \\
         XDG_CONFIG_HOME=${AGENT_HOME}/.config containerd-rootless-setuptool.sh install
       sudo -u ${AGENT_USER} env XDG_RUNTIME_DIR=/run/user/${AGENT_UID} XDG_DATA_HOME=${AGENT_HOME}/.local/share \\
         XDG_CONFIG_HOME=${AGENT_HOME}/.config containerd-rootless-setuptool.sh install-buildkit
     Confirm the rootless data-root lands on ${IMAGE_WORK_DIR} (XDG_DATA_HOME=${AGENT_HOME}/.local/share)
     and verify, exactly as the daemon resolves it:
       sudo -u ${AGENT_USER} env XDG_RUNTIME_DIR=/run/user/${AGENT_UID} \\
         CONTAINERD_ADDRESS=/run/user/${AGENT_UID}/containerd/containerd.sock \\
         BUILDKIT_HOST=unix:///run/user/${AGENT_UID}/buildkit/buildkitd.sock nerdctl pull alpine
       # MUST succeed with ZERO sudo and write bytes under ${IMAGE_WORK_DIR}. NEVER use a sudo
       # wrapper (e.g. nerdctl-sys) or the system containerd socket for any LOCAL op.

  3) Generate the node-distribution SSH key (outside \$HOME) and add its pubkey to each
     GPU node's authorized_keys, then verify passwordless 'sudo ctr' on each node (B4-B5):
       sudo -u ${AGENT_USER} ssh-keygen -t ed25519 -N '' -C 'imagesvc-distrib' -f ${DISTRIB_SSH_KEY}

  4) Log in to ACR Enterprise into the daemon's DOCKER_CONFIG (NOT \$HOME), for acr_backup. Use the
     SAME rootless env the daemon uses (do this AFTER step 2 so the rootless backend is up):
       sudo -u ${AGENT_USER} env XDG_RUNTIME_DIR=/run/user/${AGENT_UID} \\
         CONTAINERD_ADDRESS=/run/user/${AGENT_UID}/containerd/containerd.sock \\
         DOCKER_CONFIG=${DOCKER_CONFIG_DIR} \\
         ${CONTAINER_CLI} login ${ACR_ENTERPRISE_REGISTRY:-<acr-enterprise-host>}

  5) BUILD_NETWORK currently = "${BUILD_NETWORK}". The default "none" lets builds start safely
     but blocks ALL egress, so RUN steps that apt/pip-install will fail. To allow approved
     egress, create a restricted docker network and point BUILD_NETWORK at it in ${ENV_FILE}.
     pull/distribute/evict ignore this setting. Do NOT set "default"/"bridge" unless you
     intend unrestricted build-time egress.
     For nerdctl, build RUN-step CPU/memory limits are NOT set per-build (nerdctl build takes no
     --memory/--cpuset-cpus); impose them via cgroup limits on the rootless buildkitd --user
     service instead (e.g. a systemctl --user drop-in with MemoryMax=/CPUQuota=).

  6) Start it (rootless backend from step 2 must already be running):
       sudo systemctl enable --now image-service.service
       journalctl -u image-service -f

NEXT
