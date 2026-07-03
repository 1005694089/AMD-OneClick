# Small helper image with filesystem quota tooling, used by the NFS server
# pods (init mount + quota-enforcer sidecar). Kept separate from the alpine
# nfs-server image, whose EOL apk repos can no longer install quota packages.
FROM docker.m.daocloud.io/library/ubuntu:24.04
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        xfsprogs \
        quota \
        e2fsprogs \
        util-linux \
        mount \
        coreutils \
        bash \
    && rm -rf /var/lib/apt/lists/*
