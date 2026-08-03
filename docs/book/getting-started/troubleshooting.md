---
description: Diagnose connection, worker, and replay problems.
icon: wrench
---

# Troubleshooting

Most problems are one of three things: the client can't reach the server,
no worker is claiming the work, or the replayed subprocess is missing
something from its environment. Work down the chain.

## Start with the diagnostics

```bash
kitaru status      # who am I, which server, which context
kitaru doctor      # connection and environment checks
kitaru version
```

The CLI and SDK read `KITARU_API_URL` and `KITARU_API_KEY` from the
environment; `kitaru login` stores credentials per server context
(`kitaru context list` shows them). When a script fails with
`KITARU_API_URL is not set`, it's the environment, not the server.

## Nothing is happening

A replay, import, or evaluation that sits in `pending` almost always
means **no worker is claiming it**:

* Is a worker running? `kitaru worker list` shows workers and liveness.
* Can this worker claim this task? A worker started with `--kinds` or
  `--selector` skips tasks outside its scope; a bare
  `kitaru worker start` claims anything.
* Watch the job directly: `kitaru job watch <job-id>` shows tasks moving
  through `pending → claimed → running`.

## A replay fails

`kitaru job get <job-id>` carries the failing task's error and a tail of
the subprocess's stderr. The usual suspects:

* **The agent version has no run command** — register it with
  `--command`; that command is what the worker executes.
* **Missing dependencies or keys** — the subprocess runs in the worker's
  environment. Start the worker in the same virtualenv as your agent,
  with the provider keys exported.
* **A tool call missed under `on_miss="fail"`** — the fork took a path
  the recording doesn't answer. See
  [Tool policies](../guides/tool-policies.md) for the options.

## The server

The server's health endpoint is `GET /health`; Docker Compose users can
check `docker compose ps` and `docker compose logs server`. The
interactive API reference lives at `/docs` on your server.

Still stuck? Ask in the [community](https://www.zenml.io/slack) or
[open an issue](https://github.com/zenml-io/kitaru/issues).
