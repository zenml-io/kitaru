# Docker Architecture

This directory contains three Dockerfiles serving different purposes.
The main project `CLAUDE.md` links here for Docker-specific guidance.

## Image types

| Dockerfile | Purpose | Base image | Installs Kitaru from | UI source |
|---|---|---|---|---|
| `Dockerfile` | Production server (API + UI) | `zenmldocker/zenml-server:<tag>` | Local source | Released `kitaru-ui.tar.gz` from GitHub |
| `Dockerfile.server-dev` | Local server + UI development | `zenmldocker/zenml-server:<tag>` | Local source | Local `docker/kitaru-ui-dist/` directory |
| `Dockerfile.dev` | Remote flow execution (K8s, etc.) | `python:3.12-slim-bookworm` | Local source | N/A (no UI) |

## How the UI gets into the server image

The ZenML server serves a dashboard from `<zenml_package>/zen_server/dashboard/`.
Both server Dockerfiles replace that directory with Kitaru UI files:

- **Production** (`Dockerfile`): Downloads `kitaru-ui.tar.gz` + `.sha256` from
  the `kitaru-ui` GitHub releases, verifies the checksum, extracts into the
  dashboard directory.
- **Dev** (`Dockerfile.server-dev`): Copies from `docker/kitaru-ui-dist/` which
  the developer populates from a local `kitaru-ui` build (`pnpm build`).

Both verify that `index.html` exists after extraction/copy (build sentinel).

## ZenML server base image

The `ZENML_SERVER_TAG` build arg controls which ZenML server version is used.
This is pinned to a specific version (see the `ARG ZENML_SERVER_TAG` default
in `Dockerfile`), not `latest`. Both Dockerfiles must use the same pinned
tag — a contract test enforces alignment.

The base image provides:
- ZenML with all server + cloud extras
- Non-root user `zenml` (UID 1000) as the runtime user
- Entrypoint and CMD (uvicorn)
- Dashboard directory structure

Kitaru layers on top without overriding the entrypoint or CMD.
Both server Dockerfiles use `USER root` for build steps (package installation,
file operations) and switch back to `USER zenml` at the end. This is required
because the base image's non-root user cannot delete root-owned files created
by `COPY` instructions.

## Build args

### `Dockerfile` (production)

| Arg | Default | Description |
|-----|---------|-------------|
| `ZENML_SERVER_TAG` | *(pinned, see Dockerfile)* | ZenML server Docker image tag |
| `KITARU_UI_TAG` | `latest` | Kitaru UI GitHub release tag |
| `KITARU_UI_REPO_URL` | `https://github.com/zenml-io/kitaru-ui` | Kitaru UI repo for release downloads |

### `Dockerfile.server-dev` (dev)

| Arg | Default | Description |
|-----|---------|-------------|
| `ZENML_SERVER_TAG` | *(pinned, see Dockerfile)* | ZenML server Docker image tag |

## Developer workflow

### Testing with local UI changes (no release needed)

```bash
# 1. Build kitaru-ui
cd /path/to/kitaru-ui
pnpm install --frozen-lockfile
pnpm build

# 2. Copy dist/ into the kitaru Docker build context
cp -r dist/ /path/to/kitaru/docker/kitaru-ui-dist/

# 3. Build the dev server image
just server-dev-image

# 4. Run it
docker run -p 8080:8080 kitaru-server-dev
```

The `docker/kitaru-ui-dist/` directory is gitignored.

### Building a release-like image

```bash
just server-image UI_TAG=v0.1.0
```

### CI and release

- **CI** (`docker-smoke` in `ci.yml`): Builds `Dockerfile --target server`
  with explicit build args. Checks `/health`, dashboard sentinel, root route
  HTML, and `/devices/verify`.
- **Release** (`release.yml`): Builds and pushes `zenmldocker/kitaru:<version>`
  with `KITARU_UI_TAG=v<version>`.

## Release dependency chain

```
ZenML server release (zenmldocker/zenml-server:X.Y.Z on DockerHub)
    → Kitaru UI release (kitaru-ui.tar.gz on GitHub Releases)
        → Kitaru release (builds zenmldocker/kitaru:X.Y.Z)
```

The `ZENML_SERVER_TAG` and `KITARU_UI_TAG` must both be updated before
cutting a Kitaru release.

## Contract tests

`tests/test_dockerfile_contract.py` validates:
- `pyproject.toml` has no ZenML git direct references or direct-reference allowance
- Production Dockerfile uses `zenmldocker/zenml-server` as base with a pinned tag
- Production Dockerfile installs Kitaru from local source
- Kitaru UI is downloaded with checksum verification and `curl --fail`
- Dashboard sentinel is checked
- No legacy git-clone / install-dashboard.sh remains
- Server-dev Dockerfile exists, uses the same base, and copies from `docker/kitaru-ui-dist/`
- Both server Dockerfiles pin the same `ZENML_SERVER_TAG`
- Both server Dockerfiles switch to `USER root` for build steps
- `Dockerfile.dev` has no git refs
