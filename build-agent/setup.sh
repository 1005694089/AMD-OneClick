#!/usr/bin/env bash
# Provision the AMD OneClick build-agent on the R9700 workstation.
#
# Run as root (sudo). This is intentionally idempotent and does NOT embed any
# secrets: you must supply the build-agent token and perform `docker login`
# against the ACR push account yourself (the agent only ever uses that login).
#
# Usage:
#   sudo MANAGER_URL=http://36.150.116.200:30092 \
#        BUILD_AGENT_TOKEN=xxxxxxxx \
#        KUBECONFIG_PATH=/home/zijun/7900_cluster_config \
#        bash build-agent/setup.sh
set -euo pipefail

MANAGER_URL="${MANAGER_URL:?set MANAGER_URL}"
BUILD_AGENT_TOKEN="${BUILD_AGENT_TOKEN:?set BUILD_AGENT_TOKEN}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-/home/zijun/7900_cluster_config}"
AGENT_USER="${AGENT_USER:-buildagent}"
# Validate up front: AGENT_USER is interpolated into useradd, chown, AND a sed replacement
# string below. Restrict to the portable-safe username charset so a value containing sed
# metacharacters (& / \) can't silently corrupt the rewritten unit, and an odd value can't
# slip into chown/useradd. Fail loudly rather than write a broken service file.
if ! printf '%s' "${AGENT_USER}" | grep -Eq '^[a-z_][a-z0-9_-]*\$?$'; then
  echo "ERROR: AGENT_USER='${AGENT_USER}' is invalid; use [a-z_][a-z0-9_-]* (POSIX username chars)." >&2
  exit 1
fi
# The agent refuses builds unless BUILD_NETWORK is set (fail-closed against unrestricted
# build-time egress). Default to "none" so a fresh install can build immediately and safely;
# note "none" blocks ALL egress, so apt/pip in RUN steps will fail until the operator creates
# a restricted egress network and re-points BUILD_NETWORK at it (see notes below).
BUILD_NETWORK="${BUILD_NETWORK:-none}"
INSTALL_DIR="${INSTALL_DIR:-/opt/amd-oneclick/build-agent}"
DOCKER_CONFIG_DIR="${DOCKER_CONFIG_DIR:-/etc/amd-oneclick/docker}"
ENV_FILE="/etc/amd-oneclick-build-agent.env"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Creating dedicated user '${AGENT_USER}' (no docker group; use rootless docker/podman)"
id -u "${AGENT_USER}" >/dev/null 2>&1 || useradd --system --create-home --shell /usr/sbin/nologin "${AGENT_USER}"

echo "==> Installing agent to ${INSTALL_DIR}"
install -d -m 0755 "${INSTALL_DIR}"
install -m 0755 "${SRC_DIR}/agent.py" "${INSTALL_DIR}/agent.py"

echo "==> Preparing DOCKER_CONFIG dir outside \$HOME at ${DOCKER_CONFIG_DIR}"
install -d -o "${AGENT_USER}" -g "${AGENT_USER}" -m 0700 "${DOCKER_CONFIG_DIR}"

echo "==> Protecting kubeconfig ${KUBECONFIG_PATH} (chmod 600, not readable by ${AGENT_USER})"
if [ -f "${KUBECONFIG_PATH}" ]; then
  chmod 600 "${KUBECONFIG_PATH}"
else
  echo "    (warning: ${KUBECONFIG_PATH} not found; skipping)"
fi

echo "==> Writing ${ENV_FILE}"
umask 077
cat > "${ENV_FILE}" <<EOF
MANAGER_URL=${MANAGER_URL}
BUILD_AGENT_TOKEN=${BUILD_AGENT_TOKEN}
AGENT_ID=r9700-agent-1
DOCKER_CONFIG=${DOCKER_CONFIG_DIR}
# Rootless docker socket (uncomment + adjust UID if using rootless docker):
# DOCKER_HOST=unix:///run/user/$(id -u "${AGENT_USER}")/docker.sock
DOCKER_BIN=docker
DOCKER_BUILDKIT=0
BUILD_TIMEOUT_SECONDS=1800
BUILD_MEMORY=8g
# BUILD_CPUSET=0-3
# BUILD_NETWORK selects the docker network for build-time RUN steps. The agent fails closed
# if this is empty. "none" (the safe default) gives NO egress; switch to a restricted egress
# network (see step 3 below) once created so apt/pip can reach approved mirrors only.
BUILD_NETWORK=${BUILD_NETWORK}
MIN_FREE_DISK_GB=20
POLL_INTERVAL_SECONDS=10
EOF
chmod 600 "${ENV_FILE}"

echo "==> Installing systemd unit (User/Group set to '${AGENT_USER}')"
# The shipped unit hard-codes User=buildagent/Group=buildagent. If the operator overrode
# AGENT_USER, the service must run as THAT account — otherwise it runs as buildagent and
# cannot read the DOCKER_CONFIG dir (chowned to ${AGENT_USER} above) or the rootless socket.
# Substitute the selected user into the installed copy rather than shipping a templated unit.
sed -e "s/^User=.*/User=${AGENT_USER}/" \
    -e "s/^Group=.*/Group=${AGENT_USER}/" \
    "${SRC_DIR}/systemd/build-agent.service" > /etc/systemd/system/build-agent.service
chmod 0644 /etc/systemd/system/build-agent.service
systemctl daemon-reload

cat <<NEXT

Provisioning done. Remaining MANUAL steps (require ACR credentials you control):

  1) Log in to the ACR push account into the agent's DOCKER_CONFIG (NOT \$HOME):
       sudo -u ${AGENT_USER} env DOCKER_CONFIG=${DOCKER_CONFIG_DIR} \\
         docker login crpi-07r6ldyx2gp3ntwb.cn-shanghai.personal.cr.aliyuncs.com

  2) (Recommended) Set up rootless Docker or Podman for ${AGENT_USER} so a build
     escape is not host-root, then uncomment DOCKER_HOST in ${ENV_FILE}.

  3) BUILD_NETWORK currently = "${BUILD_NETWORK}". The default "none" lets builds start safely
     but blocks ALL egress, so RUN steps that apt/pip-install will fail. To allow approved
     egress, create a restricted docker network and point BUILD_NETWORK at it in ${ENV_FILE}:
       docker network create --internal oneclick-build-egress   # then add controlled routes
     Do NOT set BUILD_NETWORK to "default"/"bridge" unless you intend unrestricted egress.

  4) Start it:
       sudo systemctl enable --now build-agent.service
       journalctl -u build-agent -f

NEXT
