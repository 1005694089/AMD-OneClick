ARG FRP_IMAGE=fatedier/frpc:v0.69.0@sha256:2e7653fc08b28013f598ff20b024a7d8cce0f5ea810cab9e2e90ae7ce4687a6e
ARG PYTHON_IMAGE=docker.m.daocloud.io/library/python:3.13-alpine@sha256:399babc8b49529dabfd9c922f2b5eea81d611e4512e3ed250d75bd2e7683f4b0

FROM ${FRP_IMAGE} AS frp
FROM ${PYTHON_IMAGE}

RUN addgroup -S -g 10001 frp && adduser -S -D -H -u 10001 -G frp frp
COPY --from=frp /usr/bin/frpc /usr/local/bin/frpc
COPY frp-agent/agent.py /app/agent.py
RUN chmod 0755 /usr/local/bin/frpc /app/agent.py

USER 10001:10001
ENTRYPOINT ["python3", "/app/agent.py"]
