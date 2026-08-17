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
