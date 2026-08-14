---
description: Run the Kitaru server with Docker — Compose for one host, or the server container against your own Postgres.
icon: docker
---

# Docker

The server is one container plus Postgres. Compose runs both on a single host; for anything bigger, run the server container against a managed Postgres and keep the same environment variables.

## CLI-managed local deployment

For one local deployment per user, let the CLI own the lifecycle:

```bash
kitaru login --local
```

Requires Docker with the Compose v2 plugin. The CLI runs the version-matched `zenmldocker/kitaru-server` image with PostgreSQL kept private to the Compose network, stores generated runtime secrets in the Kitaru configuration directory, and opens `http://localhost:8000` once healthy. Existing images are reused without an automatic pull — `kitaru login --local --upgrade` is the explicit upgrade path, and `KITARU_LOCAL_IMAGE` points source builds at a locally built image. `kitaru local logs` inspects it; `kitaru logout` stops it (add `--volumes` to delete the database).

The rest of this page covers manually managed deployments, which are separate from the CLI-owned one.

## Docker Compose

The repository ships a Compose file that builds the server and starts Postgres beside it:

```bash
git clone https://github.com/zenml-io/kitaru.git
cd kitaru
docker compose up -d
curl http://localhost:8000/health
```

The shipped Compose file runs with `KITARU_SERVER_AUTH_SCHEME: none` — fine on your laptop, not for a shared server. For a team deployment, set the auth scheme to `local` and provide real keys (see below and [Authentication](authentication.md)).

## Configuration

The server is configured entirely through `KITARU_SERVER_*` environment variables. The ones every deployment should set:

| Variable | Meaning |
| --- | --- |
| `KITARU_SERVER_DB_HOST` / `DB_PORT` / `DB_USER` / `DB_PWD` / `DB_NAME` | Postgres connection — or one `KITARU_SERVER_DATABASE_URL` instead |
| `KITARU_SERVER_AUTH_SCHEME` | `none` (open, dev only) or `local` (accounts + API keys) |
| `KITARU_SERVER_JWT_SIGNING_KEY` | Secret for login tokens — set a long random value |
| `KITARU_SERVER_SECRET_ENCRYPTION_KEY` | Key encrypting stored [secrets](secrets.md) at rest |
| `KITARU_SERVER_DEFAULT_ACCOUNT_PASSWORD` | Bootstrap password for the `default` account |
| `KITARU_SERVER_SERVER_URL` | The externally reachable URL clients use |

Operational knobs with sensible defaults — raise or lower deliberately:

| Variable | Default | Meaning |
| --- | --- | --- |
| `KITARU_SERVER_MAX_BLOB_SIZE_BYTES` | 100 MiB | Upload cap for trace exports and plugin code |
| `KITARU_SERVER_TASK_HEARTBEAT_TIMEOUT_SECONDS` | 60 | How long a silent worker holds a task before it's requeued |
| `KITARU_SERVER_TASK_RETRY_LIMIT` | 3 | Attempts before a stale task is abandoned |
| `KITARU_SERVER_EVALUATOR_TASK_TIMEOUT_SECONDS` | 300 | Per-evaluator process timeout |
| `KITARU_SERVER_IMPORTER_TASK_TIMEOUT_SECONDS` | 600 | Per-import process timeout |
| `KITARU_SERVER_EVALUATION_PAIR_LIMIT` | 100 | Max (session × evaluator) pairs per batch request |
| `KITARU_SERVER_LOG_LEVEL` | `INFO` | Server logging |

Database migrations run automatically at startup (`KITARU_SERVER_SKIP_DB_MIGRATION=true` disables that when you manage migrations yourself).

## The published image

For anything beyond a laptop, use the published server image instead of building from source:

```bash
docker run -d -p 8000:8000 \
  -e KITARU_SERVER_DB_HOST=your-postgres-host \
  -e KITARU_SERVER_DB_USER=... -e KITARU_SERVER_DB_PWD=... \
  -e KITARU_SERVER_AUTH_SCHEME=local \
  -e KITARU_SERVER_JWT_SIGNING_KEY=... \
  -e KITARU_SERVER_SECRET_ENCRYPTION_KEY=... \
  zenmldocker/kitaru-server:latest
```

Any container runtime works — the server listens on port 8000, runs as a non-root user, and all state lives in Postgres. Put TLS in front with your usual ingress or reverse proxy, and scale horizontally if needed — the server is stateless between requests. On Kubernetes, use the [Helm chart](helm.md), which wraps this same image with migrations, ingress, and secrets handled.

Workers are deployed separately, in the environments your agents live in — see [Workers in production](workers.md).

## First login

```bash
kitaru login https://kitaru.internal.example.com
kitaru status
```

Then create accounts and API keys for the team: [Authentication & API keys](authentication.md).
