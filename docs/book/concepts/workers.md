---
description: The process that runs replays, imports, and evaluators as subprocesses in your environment — your code and data never leave your systems.
icon: gears
---

# Workers

Nothing in Kitaru executes on the server. Replays, imports, and evaluator
runs are **tasks**; a **worker** is the process that claims tasks from the
server and runs each one as a subprocess — in *your* environment, with
*your* virtualenv, credentials, and network. That is the data-privacy
story in one sentence: the server coordinates, your infrastructure
executes, and session payloads are read from the server your team already
hosts.

Start one wherever your agent's code can run:

```bash
kitaru worker start --concurrency 4
```

The worker registers itself, polls for pending tasks, heartbeats while
work is in flight, and reports results. Stop it with Ctrl-C — the first
signal drains in-flight tasks, a second one exits immediately.

## What a worker executes

| Task kind | What the subprocess is |
|---|---|
| `agent` | Your agent, started from the [agent version's](agents-and-sessions.md) run spec command — this is how replays, experiment runs, and on-demand session runs re-execute your real code |
| `evaluator` | A registered [evaluator](evaluators.md) plugin, run against one session |
| `importer` | A registered importer parsing an uploaded trace payload into sessions |

Evaluator and importer plugins declare their own dependencies (PEP 723
inline metadata for script plugins, an exact pin for package plugins), and
the worker builds each an isolated environment via `uv`. Agent tasks run
your command as-is, in the working directory and environment the agent
version declares — plus the [secrets](../deploy/secrets.md) it references.

The worker hands each subprocess its context through environment
variables: `KITARU_API_URL` and a `KITARU_API_TOKEN` — a bearer token
scoped to that one task and attempt, with your broader `KITARU_API_KEY`
deliberately stripped from the child environment — plus
`KITARU_TASK_ID` to link the recorded session to the task, and
`KITARU_REPLAY_ID` when the run is a replay, which is how the adapter
knows to apply overrides and answer tool calls from the recording. The
worker itself authenticates once with your API key and holds a
worker-scoped token it renews on its own; see
[Authentication & API keys](../deploy/authentication.md).

## Scoping workers

By default a worker claims any pending task. Narrow it when environments
differ:

```bash
# only imports and evaluations — no agent code runs here
kitaru worker start --kinds importer --kinds evaluator

# only tasks for a specific agent version's environment
kitaru worker start --selector agent_version=3

# drain one job, then exit — useful in CI
kitaru worker start --job-id <job-id>
```

Every option is also an environment variable with the `KITARU_WORKER_`
prefix (`KITARU_WORKER_CONCURRENCY`, `KITARU_WORKER_SCOPE__KINDS`, …), so a
containerized worker is configured without flags. Deployment patterns —
long-running workers on Kubernetes, one-shot workers in CI — are in
[Workers in production](../deploy/workers.md).

Check what's alive:

```bash
kitaru worker list
```

A worker that stops heartbeating loses its tasks: the server requeues them
for the next worker (or fails them at the retry cap), so a crashed pod
never strands a replay.
