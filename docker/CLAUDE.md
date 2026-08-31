# Docker Architecture

This directory contains the Kitaru development and release Dockerfiles. The
main project `CLAUDE.md` links here for Docker-specific guidance.

## Images

| Dockerfile | Image | Kitaru source |
|---|---|---|
| `dev-client.Dockerfile` | Development client | Local repository source |
| `dev-server.Dockerfile` | Development server | Local repository source |
| `dev-worker.Dockerfile` | Development worker | Local repository source |
| `release-client.Dockerfile` | Release client | Published PyPI wheel |
| `release-server.Dockerfile` | Release server | Published PyPI wheel |
| `release-worker.Dockerfile` | Release worker | Published PyPI wheel |
| `onboarding-sandbox.Dockerfile` | Onboarding sandbox | Published worker image |

`onboarding-sandbox.Dockerfile` extends the published worker image with
Node.js, npm, npx, Git, curl, jq, the `cli` and `mcp` extras, the Pydantic AI
adapter with OpenAI support in `/app/.venv`, the Kitaru repository cloned at the
release tag to `/opt/kitaru`, and the published Kitaru skills.
Releases publish it to the private Amazon ECR registry as
`kitaru-onboarding-sandbox` alongside the managed server image.

The adapter is pinned to the version validated for the hosted tour. Install it
alongside the exact worker Kitaru version so dependency resolution cannot silently
upgrade Kitaru away from the workspace version. Provider credentials are not baked
into this image; installing the adapter does not enable funded model inference.

The image bakes in the published `kitaru-skills` release. To refresh that baked copy during a manual build, use `--no-cache` so Docker reruns the remote skill installation. Once the frontend runtime-refresh change is deployed, newly created sandboxes also install published skills at startup, so skill-only releases no longer require an image rebuild. Dependency and bundled-example changes still require rebuilding the image. The frontend selects the ECR tag from the workspace version, and existing Modal sandboxes do not update when an image or skill release is published.

The development and release client, server, and worker builds resolve dependencies
from the committed `uv.lock`. The release
builds then install the matching published Kitaru wheel without resolving its
dependencies again. This keeps container releases reproducible while allowing
the Python package and container publishing processes to be recovered or run
independently.

The release builders fail when `KITARU_VERSION` is missing, differs from the
version in `pyproject.toml`, is unavailable on PyPI, or declares dependencies
that are incompatible with the lockfile.

## Stages

`dev-client.Dockerfile` installs the locked local project non-editably and
produces the `client` target. `dev-worker.Dockerfile` does the same with the
`worker` extra and produces the `worker` target.

`dev-server.Dockerfile` separates dependency installation from the two server
runtime modes:

| Stage | Purpose |
|---|---|
| `pre-builder` | Installs locked `server` and `otel` dependencies without the project |
| `common-runtime` | Installs local source editably for bind-mounted development |
| `local-runtime` | Runs uvicorn with source reload enabled |
| `builder` | Installs local source non-editably for the self-contained image |
| `runtime` | Runs the self-contained development server without copied sources |

Set `INSTALL_DEBUG_TOOLS=true` when building the server to add curl, Git,
network diagnostics, and the PostgreSQL client. Debug tools are omitted by
default. Every development server runtime sets
`KITARU_SERVER_ANALYTICS_DEBUG=true` so opted-in development events use the
analytics debug service instead of polluting the production analytics namespace.

The release Dockerfiles use these stages:

| Stage | Purpose |
|---|---|
| `uv` | Supplies the pinned uv binary to build stages |
| `base` | Creates the slim Python base and non-root `kitaru` user |
| `builder` | Creates the locked virtual environment and installs the published wheel |
| `client`, `worker`, or `server` | Copies only the virtual environment and dependency snapshot into the runtime image |

The release runtime images do not contain pip, setuptools, or wheel. The worker
images include uv. The resolved environment is recorded at
`/app/requirements.txt` for inspection.

## Build and run

Build from the repository root. A release build must use a repository checkout
whose version and lockfile match the published package version.

```bash
# Development client from local source
docker build -f docker/dev-client.Dockerfile --target client \
  -t kitaru-client-dev .

# Development worker from local source
docker build -f docker/dev-worker.Dockerfile --target worker \
  -t kitaru-worker-dev .

# Self-contained development server from local source
docker build -f docker/dev-server.Dockerfile --target runtime \
  -t kitaru-server-dev .

# Reloading server for a source bind mount
docker build -f docker/dev-server.Dockerfile --target local-runtime \
  --build-arg INSTALL_DEBUG_TOOLS=true -t kitaru-server-local .
docker run --rm -p 8000:8000 -v "$PWD/src:/app/src" kitaru-server-local

# Release client from the matching published package
docker build -f docker/release-client.Dockerfile --target client \
  --build-arg KITARU_VERSION=<version> -t kitaru-client .

# Release worker from the matching published package
docker build -f docker/release-worker.Dockerfile --target worker \
  --build-arg KITARU_VERSION=<version> -t kitaru-worker .

# Release server from the matching published package
docker build -f docker/release-server.Dockerfile --target server \
  --build-arg KITARU_VERSION=<version> -t kitaru-server .

# Onboarding sandbox from the published worker image
docker build -f docker/onboarding-sandbox.Dockerfile --target worker \
  --build-arg BASE_IMAGE=zenmldocker/kitaru-worker:<version> \
  -t kitaru-onboarding-sandbox .
```

The release server listens on port 8000 and starts the FastAPI application
factory with uvicorn. Run it against a PostgreSQL instance with the appropriate
`KITARU_SERVER_` environment variables:

```bash
docker run -p 8000:8000 \
  -e KITARU_SERVER_DB_HOST=<host> \
  -e KITARU_SERVER_DB_PWD=<password> \
  kitaru-server
```

## Build arguments

| Argument | Default | Description |
|---|---|---|
| `PYTHON_VERSION` | `3.13` | Base image Python version |
| `UV_VERSION` | `0.12.1` | uv image version used by release builds |
| `VIRTUAL_ENV` | `/app/.venv` | Release virtual environment path |
| `USERNAME` | `kitaru` | Runtime user |
| `USER_UID` / `USER_GID` | `1000` | Runtime user and group IDs |
| `KITARU_VERSION` | Empty | Required published Kitaru release version |
