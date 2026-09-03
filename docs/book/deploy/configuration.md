---
description: "Client-side configuration: servers, environment variables, and the precedence between them."
icon: gear
---

# Configuration

Three surfaces read configuration: the **CLI**, the **SDK** (`KitaruAPIClient`), and **workers**. They agree on the two variables that matter:

```bash
export KITARU_API_URL="https://kitaru.internal.example.com"
export KITARU_API_KEY="KITKEY_..."
```

`KitaruAPIClient()` resolves both on its own: the server URL from `KITARU_API_URL`, falling back to the URL stored by `kitaru login` (no URL anywhere is an error); the credential from the task token a worker injects (`KITARU_API_TOKEN`), then `KITARU_API_KEY`, then the stored `kitaru login` credential. No credential means unauthenticated, which is fine when the server runs `AUTH_SCHEME=none`. Workers and task subprocesses are handed the pair explicitly.

## Selecting a server with the CLI

`kitaru login <server>` stores that server as the default. Use `--server` for one command or `KITARU_API_URL` for the current environment:

```bash
kitaru login https://kitaru.staging.example.com
kitaru agent list
kitaru --server https://kitaru.production.example.com agent list
```

The explicit `--server` flag beats `KITARU_API_URL`, which beats the URL stored by `kitaru login`.

## CLI behavior settings

```bash
kitaru config list
kitaru config set <key> <value>
kitaru config path        # where the config file lives
```

Useful global flags and their environment twins:

| Flag | Env | Meaning |
| --- | --- | --- |
| `--output/-o json` or `jsonl` | none | Machine-readable output for scripts and assistants; `jsonl` streams progress line by line |
| `--non-interactive` | `KITARU_NON_INTERACTIVE` | Never prompt; fail instead |
| `--machine` | `KITARU_MACHINE_MODE` | Stable, parseable output defaults |
| `--request-timeout` | none | Per-request timeout (default 30s) |
| `--no-browser` | none | Print login URLs instead of opening them |

## Server and worker configuration

The server is configured through `KITARU_SERVER_*` variables ([Docker](docker.md) lists them) and workers through `KITARU_WORKER_*` ([Workers in production](workers.md)). Neither reads the CLI's config file; deployment configuration stays in the deployment's environment, which is what lets a worker container run with nothing but env vars.

### Ephemeral worker launcher

When no live worker can claim an import job's tasks, the server starts one scoped to that job in a Modal sandbox. This stays off unless a worker launcher backend is configured, and `KITARU_SERVER_SERVER_URL` must be set, since the sandbox dials back to the server and startup fails otherwise. Install the `modal` extra (`pip install 'kitaru[server,modal]'`) to pull in the Modal SDK the server needs.

```bash
KITARU_SERVER_WORKER_LAUNCHER__BACKEND=modal           # default none
KITARU_SERVER_WORKER_LAUNCHER__MODAL__TOKEN_ID=ak-...
KITARU_SERVER_WORKER_LAUNCHER__MODAL__TOKEN_SECRET=as-...
KITARU_SERVER_WORKER_LAUNCHER__MODAL__IMAGE=zenmldocker/kitaru-worker:<version>
KITARU_SERVER_WORKER_LAUNCHER__MODAL__APP_NAME=kitaru-workers   # default kitaru-workers
KITARU_SERVER_WORKER_LAUNCHER__MODAL__TIMEOUT_SECONDS=3600      # default 3600
KITARU_SERVER_WORKER_LAUNCHER__MODAL__CPU=1.0                   # optional
KITARU_SERVER_WORKER_LAUNCHER__MODAL__MEMORY_MB=2048            # optional
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `KITARU_SERVER_WORKER_LAUNCHER__BACKEND` | none | Worker launcher backend, currently `modal` |
| `KITARU_SERVER_WORKER_LAUNCHER__MODAL__TOKEN_ID` / `MODAL__TOKEN_SECRET` | none | Modal API token |
| `KITARU_SERVER_WORKER_LAUNCHER__MODAL__IMAGE` | none | Worker image the sandbox runs, the published `zenmldocker/kitaru-worker` tag matching the server version |
| `KITARU_SERVER_WORKER_LAUNCHER__MODAL__APP_NAME` | `kitaru-workers` | Modal app the sandbox runs under |
| `KITARU_SERVER_WORKER_LAUNCHER__MODAL__TIMEOUT_SECONDS` | 3600 | Sandbox lifetime and the worker's `KITARU_WORKER_TIMEOUT` |
| `KITARU_SERVER_WORKER_LAUNCHER__MODAL__CPU` / `MODAL__MEMORY_MB` | none | Sandbox resources |
