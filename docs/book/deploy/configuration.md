---
description: Client-side configuration — contexts, environment variables, and the precedence between them.
icon: gear
---

# Configuration

Three surfaces read configuration: the **CLI**, the **SDK**
(`KitaruAPIClient`), and **workers**. They agree on the two variables
that matter:

```bash
export KITARU_API_URL="https://kitaru.internal.example.com"
export KITARU_API_KEY="KITKEY_..."
```

`KitaruAPIClient()` resolves both on its own: the server URL from
`KITARU_API_URL`, falling back to the URL stored by `kitaru login` (no
URL anywhere is an error); the credential from the task token a worker
injects (`KITARU_API_TOKEN`), then `KITARU_API_KEY`, then the stored
`kitaru login` credential — and no credential means unauthenticated,
fine when the server runs `AUTH_SCHEME=none`. Workers and task
subprocesses are handed the pair explicitly.

## Contexts (CLI)

The CLI stores a context per server — URL plus the credentials from
`kitaru login` — so switching deployments doesn't mean juggling env vars:

```bash
kitaru context add staging https://kitaru.staging.example.com
kitaru context list
kitaru context use staging
kitaru --context staging agent list     # one-off, no switch
```

Explicit flags beat environment variables, which beat the stored
context: `--server` on any command pins the call, `KITARU_API_URL` wins
over the context store, and the context is the default.

## CLI behavior settings

```bash
kitaru config list
kitaru config set <key> <value>
kitaru config path        # where the config file lives
```

Useful global flags and their environment twins:

| Flag | Env | Meaning |
|---|---|---|
| `--output/-o json` or `jsonl` | — | Machine-readable output for scripts and assistants; `jsonl` streams progress line by line |
| `--non-interactive` | `KITARU_NON_INTERACTIVE` | Never prompt; fail instead |
| `--machine` | `KITARU_MACHINE_MODE` | Stable, parseable output defaults |
| `--request-timeout` | — | Per-request timeout (default 30s) |
| `--no-browser` | — | Print login URLs instead of opening them |

## Server and worker configuration

The server is configured through `KITARU_SERVER_*` variables
([Docker](docker.md) lists them) and workers through `KITARU_WORKER_*`
([Workers in production](workers.md)). Neither reads the CLI's config
file — deployment configuration stays in the deployment's environment,
which is what lets a worker container run with nothing but env vars.
