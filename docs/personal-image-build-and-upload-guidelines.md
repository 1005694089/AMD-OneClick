# Personal Image Build and Upload Guidelines

This document describes the basic requirements for building a personal container image for the Radeon Cloud platform.

The goal is simple: your image should start reliably on the platform, expose a long-running service on the expected port, and be easy for the administrator to verify and publish.

## 1. Choose a Base Image

Choose a base image that already matches your workload as closely as possible.

In general, you may start from a public image on Docker Hub or another registry, especially images related to AMD Radeon, ROCm, NAVI, or other AMD GPU software stacks.

When selecting a base image, check the following:

- It supports the target AMD GPU / ROCm environment.
- It includes a compatible Linux distribution.
- It has the Python, ROCm, Jupyter, or framework versions you need, or can install them cleanly during build.
- It can run without manual setup after the container starts.

Avoid using a random image that has not been tested with AMD GPU workloads. If you are unsure, share the candidate base image with the administrator first.

## 2. Dockerfile Structure

A custom image should be reproducible from a Dockerfile.

Example:

```dockerfile
FROM <your-selected-rocm-or-radeon-base-image>

WORKDIR /opt/project

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /opt/project

EXPOSE 8888

CMD ["bash", "-lc", "jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=${NOTEBOOK_TOKEN:-amd-oneclick}"]
```

Install dependencies during image build whenever possible. Avoid installing large packages every time the container starts.

## 3. Working Directory and `/workspace`

The platform uses `/workspace` as the user workspace path.

Important: files baked into `/workspace` inside your image may be hidden or overwritten when the platform mounts the runtime workspace volume.

For this reason, do not rely on preloaded image files under `/workspace`.

Recommended pattern:

- Put project files, examples, and startup assets in a stable image path, such as `/opt/project`.
- Use `/workspace` for runtime user files, notebooks, outputs, and temporary experiment data.
- If you need to initialize files into `/workspace`, copy them from `/opt/project` during container startup only when needed.

Example startup pattern:

```bash
if [ ! -f /workspace/.initialized ]; then
  cp -r /opt/project/examples /workspace/examples
  touch /workspace/.initialized
fi

jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root
```

This avoids losing important files when `/workspace` is mounted by Kubernetes.

## 4. Port Requirement

Port `8888` is the platform convention for exposing the notebook or web service.

Your custom image must run a long-lived process that listens on:

```text
0.0.0.0:8888
```

This is important because the platform proxy forwards user traffic to port `8888` inside the container.

Do not bind only to `127.0.0.1`, because the platform will not be able to reach the service.

Good:

```bash
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root
```

Bad:

```bash
jupyter lab --ip=127.0.0.1 --port=8888
```

If your image runs a service other than Jupyter, that service must still listen on port `8888`.

## 5. Start Command

The image should start automatically without requiring a user to enter the container and run commands manually.

The `CMD` or `ENTRYPOINT` should:

- Start the main long-running service.
- Listen on `0.0.0.0:8888`.
- Use `/workspace` as the runtime working area if needed.
- Avoid interactive prompts.
- Avoid long installation steps at startup.

Recommended Jupyter command:

```dockerfile
CMD ["bash", "-lc", "jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=${NOTEBOOK_TOKEN:-amd-oneclick}"]
```

If you need an initialization script:

```dockerfile
COPY start.sh /usr/local/bin/start.sh
RUN chmod +x /usr/local/bin/start.sh

CMD ["/usr/local/bin/start.sh"]
```

Make sure `start.sh` eventually starts a persistent process on port `8888`.

## 6. Build and Test Locally

Build the image:

```bash
docker build -t my-personal-image:20260622 .
```

Run a local test:

```bash
docker run --rm -p 8888:8888 my-personal-image:20260622
```

Check:

- The container starts successfully.
- The service is reachable at `http://localhost:8888`.
- The process keeps running.
- The service listens on `0.0.0.0:8888`.
- Required Python packages and notebooks are available.
- Startup does not require manual commands.

## 7. Tag and Upload to Alibaba Cloud Registry

After local testing, tag the image for Alibaba Cloud Container Registry.

Example:

```bash
docker tag my-personal-image:20260622 <aliyun-registry>/<namespace>/<image-name>:20260622
```

Login:

```bash
docker login <aliyun-registry>
```

Push:

```bash
docker push <aliyun-registry>/<namespace>/<image-name>:20260622
```

Use clear versioned tags. Avoid using only `latest`, because it is hard to reproduce and audit.

Good tag examples:

```text
rocm-demo-py312-20260622
llm-workshop-v1-20260622
navi-demo-jupyter-20260622
```

## 8. Send Information to the Administrator

After the image is uploaded, send the basic information to the platform administrator.

Usually, the following fields are enough:

```text
Image URL:
<full image URL>

Base image:
<base image name and tag, if known>

Purpose:
<short description of the image>

Runtime service:
<JupyterLab / Notebook / custom web service>

Port:
8888

Notes:
<optional: special environment variables, required notebooks, or known limitations>
```

Example:

```text
Image URL:
registry.cn-shanghai.aliyuncs.com/my-namespace/rocm-workshop:20260622

Base image:
rocm/dev-ubuntu-22.04:<tag>

Purpose:
Personal ROCm demo image for a notebook workshop.

Runtime service:
JupyterLab

Port:
8888

Notes:
Includes example notebooks under /opt/project/examples.
```

## 9. Common Mistakes

Avoid these common issues:

- The service listens on `127.0.0.1` instead of `0.0.0.0`.
- The service does not listen on port `8888`.
- The container exits immediately after startup.
- Important files are copied only into `/workspace` during image build.
- Large dependencies are installed every time the container starts.
- The image requires manual commands after launch.
- The image tag is only `latest`.
- The base image has not been tested with AMD GPU / ROCm workloads.

A good custom image should start cleanly, keep a service running on `0.0.0.0:8888`, and allow the user to begin working immediately after the platform launches the instance.
