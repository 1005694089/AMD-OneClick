# FRP Tunnel Integration

This integration is disabled by default and affects only Pods created after it is enabled.

## Runtime flow

1. RC creates a notebook Pod with a dormant `frpc` sidecar.
2. The sidecar has the FRPS platform token mounted from `amd-oneclick-frp-platform`, but no client identity or domain configuration.
3. A user configures a domain prefix and local port from Profile.
4. RC reads the real Pod UID from Kubernetes and calls the FRP Control API.
5. The Control API reserves the prefix and returns one-time client credentials.
6. RC immediately writes those credentials to the Pod-owned `frp-tunnel-*` Secret. Credentials are never stored in PostgreSQL or returned to the browser.
7. Kubelet projects the Secret into the sidecar. The supervisor starts FRPC and restarts it when credentials rotate.
8. Disabling access atomically clears the Secret first, which stops FRPC, and then revokes the tunnel through the Control API. The empty Pod-owned Secret is garbage-collected with the Pod.
9. Deleting a notebook uses the same cleanup hook for user, admin, billing, idle-reaper, and reconcile paths.

The notebook container does not mount either FRP Secret, has no Kubernetes service-account token, and cannot call the Control API. A short-lived init container changes ownership only on the two FRP `emptyDir` volumes; no Pod-wide `fsGroup` is set, so notebook PVC/NFS ownership is not traversed or changed.

## Build the agent image

Build `frp-agent/Dockerfile`, scan it, and push it to a registry reachable by every GPU node. The manager requires an immutable image digest when the feature is enabled.

```bash
docker build -f frp-agent/Dockerfile -t <private-registry>/amd-oneclick-frp-agent:0.69.0-1 .
docker push <private-registry>/amd-oneclick-frp-agent:0.69.0-1
```

The Dockerfile copies FRPC v0.69.0 into a non-root Python supervisor image. Mirror both base images into the private registry if the build environment cannot reach Docker Hub.

## Kubernetes secrets

Create both Secrets in `amd-oneclick-lablab`. Obtain the values directly from the VM without placing them in Git, ConfigMaps, command history, or ticket text.

| Secret | Key | Consumer |
| --- | --- | --- |
| `amd-oneclick-frp-control-api` | `token` | RC manager only |
| `amd-oneclick-frp-platform` | `global-token` | FRPC sidecar only |

The manager Deployment must mount only the first Secret:

```yaml
volumeMounts:
  - name: frp-control-api
    mountPath: /run/secrets/frp-control-api
    readOnly: true
volumes:
  - name: frp-control-api
    secret:
      secretName: amd-oneclick-frp-control-api
      defaultMode: 0440
```

The platform Secret is referenced by future notebook Pods. It is not mounted into the manager or notebook container.

## Manager configuration

Add these values to `amd-oneclick-lablab-config` only after the agent image, Secrets, RBAC, VM listener, and NSG rule are ready:

```yaml
FRP_TUNNEL_ENABLED: "true"
FRP_CONTROL_API_URL: "https://radeon.firstdg.ai:9443"
FRP_CONTROL_API_TOKEN_FILE: "/run/secrets/frp-control-api/token"
FRP_CLUSTER_ID: "host"
FRP_DOMAIN_SUFFIX: "radeon.firstdg.ai"
FRP_AGENT_IMAGE: "<private-registry>/amd-oneclick-frp-agent@sha256:<digest>"
FRP_AGENT_IMAGE_PULL_POLICY: "IfNotPresent"
FRP_AGENT_IMAGE_PULL_SECRET_NAME: "<secret-name-if-required>"
FRP_PLATFORM_SECRET_NAME: "amd-oneclick-frp-platform"
FRP_PLATFORM_TOKEN_KEY: "global-token"
FRP_LOG_INGEST_URL: "https://radeon.firstdg.ai:7443/__frp_logs/v1/logs"
FRP_BANDWIDTH_LIMIT: "2500KB"
```

The application refuses to start with the feature enabled and an incomplete URL, image, token file, or platform Secret configuration.

## RBAC

Apply the full merged `k8s-lablab-manager-role.yaml`. It adds namespace-scoped `get/create/patch/delete` permissions for Secrets without `list/watch`.

Kubernetes RBAC cannot constrain Secret creation to an `frp-tunnel-*` prefix. The API enforces deterministic names and Pod owner references, but the manager service account remains trusted for Secret mutation inside `amd-oneclick-lablab`.

## VM network prerequisite

The current Compose mapping binds Control API port `9443` to loopback. Before RC can call it:

1. Add an NSG rule allowing TCP `9443` only from the verified IDC egress CIDR.
2. Bind VM port `9443` on the external interface.
3. Keep Nginx TLS verification enabled and use `https://radeon.firstdg.ai:9443`, not the raw IP.
4. Do not route this endpoint through Front Door and do not expose it to browsers.

Bearer authentication is acceptable for this internal POC only. Production should move to mTLS or short-lived workload identity.

## Verification

1. Confirm the manager rollout is healthy before setting `FRP_TUNNEL_ENABLED=true`.
2. Launch a new disposable notebook. Existing Pods do not gain sidecars and must be restarted.
3. Verify the Pod has `notebook` and `frpc` containers and no `frp-tunnel-*` Secret yet.
4. Configure one prefix and port from Profile.
5. Confirm the Secret owner UID matches the Pod UID and the Control API reaches `active`.
6. Test HTTP and WebSocket access through Front Door.
7. Disable public access and verify the Secret data becomes empty before the Control API enters `quarantine`.
8. Delete the notebook and verify no Pod-owned tunnel Secret remains.
9. Confirm FRPC, authorization, Nginx, and Kubernetes runtime records are present in `FrpTunnel_CL`.

## Rollback

Set `FRP_TUNNEL_ENABLED=false` and roll out the manager. This prevents new sidecars and hides the UI. Revoke active tunnels before recreating or deleting existing sidecar-enabled Pods; disabling the flag alone does not revoke an already connected FRPC process.
