# Docker Architecture

This directory contains three Dockerfiles serving different purposes.
The main project `CLAUDE.md` links here for Docker-specific guidance.

## Image types

| Dockerfile | Purpose | Base image | Installs Kitaru from | UI source |
|---|---|---|---|---|
| `Dockerfile` | Production server (API + UI) | `zenmldocker/zenml-server:<tag>` | PyPI when `KITARU_VERSION` is set; local source otherwise | Installed `kitaru` package (`kitaru/_ui/dist`) |
| `Dockerfile.server-dev` | Local server + UI development | `zenmldocker/zenml-server:<tag>` | Local source | Local `docker/kitaru-ui-dist/` directory |
| `Dockerfile.dev` | Remote flow execution (K8s, etc.) | `python:3.12-slim-bookworm` | Local source | N/A (no UI) |

## How the UI gets into the server image

The ZenML server serves a dashboard from `<zenml_package>/zen_server/dashboard/`.
Both server Dockerfiles replace that directory with Kitaru UI files, but they do
it from different sources:

- **Production** (`Dockerfile`): installs Kitaru first, resolves
  `kitaru/_ui/dist` from the installed package with Python, verifies
  `index.html`, and copies those files into ZenML's dashboard directory.
  Docker must not download a UI release itself.
- **Dev** (`Dockerfile.server-dev`): copies from `docker/kitaru-ui-dist/`, which
  the developer populates from a local Kitaru UI build (`pnpm build`).

The safety story is: the Kitaru wheel decides the official UI bundle, and Docker
only consumes that package. This prevents the wheel and Docker image from
silently using different UI releases.

Both server Dockerfiles verify that `index.html` exists after extraction/copy
(build sentinel).

## ZenML server base image

The `ZENML_SERVER_TAG` build arg controls which ZenML server version is used.
This is pinned to a specific version (see the `ARG ZENML_SERVER_TAG` default in
`Dockerfile`), not `latest`. Both Dockerfiles must use the same pinned tag — a
contract test enforces alignment.

The base image provides:

- ZenML with all server + cloud extras
- Non-root user `zenml` (UID 1000) as the runtime user
- Entrypoint and CMD (uvicorn)
- Dashboard directory structure

Kitaru layers on top without overriding the entrypoint or CMD. Both server
Dockerfiles use `USER root` for build steps (package installation, file
operations) and switch back to `USER zenml` at the end. This is required because
the base image's non-root user cannot delete root-owned files created by `COPY`
instructions.

All three Dockerfiles use [uv](https://docs.astral.sh/uv/) for Python package
installation instead of pip. uv is copied as a static binary from the distroless
image (`ghcr.io/astral-sh/uv`) — no pip install or apt-get needed. The base image
sets `VIRTUAL_ENV=/opt/venv`, so `uv pip install` targets the venv automatically
in the server Dockerfiles. `Dockerfile.dev` uses `UV_SYSTEM_PYTHON=1` instead
(no venv).

## Build args

### `Dockerfile` (production)

| Arg | Default | Description |
|-----|---------|-------------|
| `ZENML_SERVER_TAG` | *(pinned, see Dockerfile)* | ZenML server Docker image tag |
| `KITARU_VERSION` | *(empty)* | If set, install Kitaru from PyPI (`kitaru==<version>`); if empty, install from local source |

There is intentionally no `KITARU_UI_TAG` Docker build arg. Select the UI before
building the package or source tree, not inside Docker.

### `Dockerfile.server-dev` (dev)

| Arg | Default | Description |
|-----|---------|-------------|
| `ZENML_SERVER_TAG` | *(pinned, see Dockerfile)* | ZenML server Docker image tag |

## Developer workflow

### Testing with local UI changes (no release needed)

Use this path when you have an unarchived local frontend build and want Docker to
serve exactly those files:

```bash
# 1. Build Kitaru UI in the frontend monorepo
cd /path/to/zenml-frontend-monorepo/apps/kitaru-ui
pnpm install --frozen-lockfile
pnpm build

# 2. Copy dist/ into the Kitaru Docker build context
cd /path/to/kitaru
rm -rf docker/kitaru-ui-dist
cp -r /path/to/zenml-frontend-monorepo/apps/kitaru-ui/dist/ docker/kitaru-ui-dist/

# 3. Build the dev server image
just server-dev-image

# 4. Run it
docker run -p 8080:8080 kitaru-server-dev
```

The `docker/kitaru-ui-dist/` directory is gitignored.

### Building a release-like image

Use this path when you want Docker to behave like the official release path:

```bash
just server-image
```

That command first downloads the highest stable/full `kitaru-ui-v*` release into
`src/kitaru/_ui/dist/`, then builds the server image. Docker copies the UI from
the installed Kitaru package.

To pin a specific stable UI release:

```bash
just UI_TAG=kitaru-ui-v0.2.0 server-image
```

Prerelease UI belongs in the local bundle-selector and prerelease smoke workflow,
not in official Docker release builds.

### CI and release

- **CI** (`docker-smoke` in `ci.yml`): runs `scripts/download-ui.sh` first, then
  builds `Dockerfile --target server` without UI build args. It checks `/health`,
  package UI files, copied dashboard files, root route HTML, and
  `/devices/verify`.
- **Release** (`release.yml`): downloads a stable/full Kitaru UI release from
  `zenml-io/zenml-frontend-monorepo`, builds the Python package, then builds and
  pushes `zenmldocker/kitaru:<version>` using that package UI.
- **Prerelease smoke** (`ui-prerelease-smoke.yml`): explicitly enables
  `KITARU_UI_ALLOW_PRERELEASE=true` for automation-only validation and publishes
  nothing.

## Release dependency chain

```text
ZenML server release (zenmldocker/zenml-server:X.Y.Z on DockerHub)
    → stable/full Kitaru UI release (kitaru-ui-v* in zenml-io/zenml-frontend-monorepo)
        → Kitaru release (wheel bundles UI, Docker copies from installed package)
```

Before cutting a Kitaru release, make sure:

- `ZENML_SERVER_TAG` is correct and aligned across Dockerfiles/workflows.
- At least one full, non-prerelease `kitaru-ui-v*` release exists in
  `zenml-io/zenml-frontend-monorepo`.
- The `KITARU_UI_RELEASE_TOKEN` secret can read the frontend monorepo release
  assets if the repository requires authentication.

## Contract tests

`tests/test_dockerfile_contract.py` validates:

- `pyproject.toml` has no ZenML git direct references or direct-reference allowance
- Production Dockerfile uses `zenmldocker/zenml-server` as base with a pinned tag
- Production Dockerfile installs Kitaru from local source or PyPI depending on `KITARU_VERSION`
- Production Dockerfile does not download Kitaru UI releases directly
- Production Dockerfile resolves package UI with Python, verifies `index.html`, and copies into ZenML's dashboard directory
- Dashboard sentinel is checked
- No legacy git-clone / install-dashboard.sh remains
- Server-dev Dockerfile exists, uses the same base, and copies from `docker/kitaru-ui-dist/`
- Both server Dockerfiles pin the same `ZENML_SERVER_TAG`
- Both server Dockerfiles switch to `USER root` for build steps
- `Dockerfile.dev` has no git refs
