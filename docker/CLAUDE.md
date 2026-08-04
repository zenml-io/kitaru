# Docker Architecture

This directory contains the Kitaru development and release Dockerfiles. The
main project `CLAUDE.md` links here for Docker-specific guidance.

## Images

| Dockerfile | Image | Kitaru source |
|---|---|---|
| `dev.Dockerfile` | Development server | Local repository source |
| `release-client.Dockerfile` | Release client | Published PyPI wheel |
| `release-server.Dockerfile` | Release server | Published PyPI wheel |

All three builds resolve dependencies from the committed `uv.lock`. The release
builds then install the matching published Kitaru wheel without resolving its
dependencies again. This keeps container releases reproducible while allowing
the Python package and container publishing processes to be recovered or run
independently.

The release builders fail when `KITARU_VERSION` is missing, differs from the
version in `pyproject.toml`, is unavailable on PyPI, or declares dependencies
that are incompatible with the lockfile.

## Stages

`dev.Dockerfile` uses `base`, `builder`, and `runtime` stages. It installs the
local project with the `server` and `otel` extras.

Both release Dockerfiles use these stages:

| Stage | Purpose |
|---|---|
| `uv` | Supplies the pinned uv binary to build stages |
| `base` | Creates the slim Python base and non-root `kitaru` user |
| `builder` | Creates the locked virtual environment and installs the published wheel |
| `client` or `server` | Copies only the virtual environment and dependency snapshot into the runtime image |

The release runtime images do not contain uv, pip, setuptools, or wheel. The
resolved environment is recorded at `/app/requirements.txt` for inspection.

## Build and run

Build from the repository root. A release build must use a repository checkout
whose version and lockfile match the published package version.

```bash
# Development server from local source
docker build -f docker/dev.Dockerfile --target runtime -t kitaru-server-dev .

# Release client from the matching published package
docker build -f docker/release-client.Dockerfile --target client \
  --build-arg KITARU_VERSION=<version> -t kitaru-client .

# Release server from the matching published package
docker build -f docker/release-server.Dockerfile --target server \
  --build-arg KITARU_VERSION=<version> -t kitaru-server .
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
