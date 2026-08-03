# Docker Architecture

This directory contains the Kitaru server Dockerfiles. The main project
`CLAUDE.md` links here for Docker-specific guidance.

## Images

Both images build the Kitaru API server on `python:<version>-slim-bookworm`
and differ only in where the package comes from:

| Dockerfile | Installs Kitaru from |
|---|---|
| `dev.Dockerfile` | Local source, `uv sync --locked --no-dev --extra server --extra otel` |
| `release.Dockerfile` | PyPI, `kitaru[server,otel]` (latest, or `KITARU_VERSION` when set) |

The source build pins every dependency through the committed `uv.lock`. The
release build resolves dependencies from PyPI at build time.

`dev.Dockerfile` stages:

| Stage | Purpose |
|---|---|
| `base` | Slim Python, uv binary, non-root user `kitaru` (UID 1000) |
| `builder` | `uv sync --locked --no-dev --extra server --extra otel --no-editable` into `/app/.venv` |
| `runtime` | Final image, copies only the venv from `builder` |

`release.Dockerfile` has the same `base`, `builder`, and `runtime` stages, with
the builder running `uv pip install "kitaru[server,otel]"` instead of a source sync.

The server listens on port 8000 and starts via
`python -m kitaru.server.api.main`. Database migrations run on startup unless
`KITARU_SERVER_SKIP_DB_MIGRATION` is set. To run migrations as a separate process,
override the command with `python -m kitaru.server.database.main`.

## Build and run

Build from the repository root (the build context is the repo root, not
`docker/`):

```bash
# From local source
docker build -f docker/dev.Dockerfile --target runtime -t kitaru-server .

# From PyPI (latest, or pin with --build-arg KITARU_VERSION=<version>)
docker build -f docker/release.Dockerfile --target runtime -t kitaru-server .
```

Run against a PostgreSQL instance:

```bash
docker run -p 8000:8000 \
  -e KITARU_SERVER_DB_HOST=<host> \
  -e KITARU_SERVER_DB_PWD=<password> \
  kitaru-server
```

Connection settings are the `KITARU_SERVER_`-prefixed variables from
`kitaru.server.config.Settings` (`KITARU_SERVER_DB_HOST`, `KITARU_SERVER_DB_PORT`,
`KITARU_SERVER_DB_USER`, `KITARU_SERVER_DB_PWD`, `KITARU_SERVER_DB_NAME`, or a full
`KITARU_SERVER_DATABASE_URL`).

## Build args

| Arg | Default | Description |
|-----|---------|-------------|
| `PYTHON_VERSION` | `3.13` | Base image Python version |
| `USERNAME` | `kitaru` | Runtime user |
| `USER_UID` / `USER_GID` | `1000` | Runtime user and group ids |
| `KITARU_VERSION` | *(empty)* | `release.Dockerfile` only. PyPI version to install, latest when empty |
