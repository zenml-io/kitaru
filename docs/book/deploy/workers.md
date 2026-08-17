---
description: Deploying workers as long-running pools, scoped fleets, and one-shot CI workers, all configured through environment variables.
icon: gears
---

# Workers in production

[Workers](../concepts/workers.md) are where everything executes. In production you run them as ordinary long-lived processes (a systemd unit, a container where the published `zenmldocker/kitaru-worker` image works out of the box, or a Kubernetes Deployment), one per environment your agents' code needs.

The rule of thumb: **a worker must be able to run what it claims.** An agent replay needs your agent's virtualenv and provider keys; an evaluator or importer brings its own dependencies and needs only Python, `uv`, and network access to the server.

## Configuration

Everything `kitaru worker start` takes as a flag is also an environment variable with the `KITARU_WORKER_` prefix, which is how containerized workers are configured:

```bash
export KITARU_API_URL="https://kitaru.internal.example.com"
export KITARU_API_KEY="KITKEY_..."          # a service API key

export KITARU_WORKER_CONCURRENCY=4
export KITARU_WORKER_SCOPE__CLAIMS='[{"kind":"evaluator"},{"kind":"importer"}]'   # JSON
kitaru worker start
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `KITARU_WORKER_NAME` | hostname-pid | Stable name; restarts reuse the worker registration. In Kubernetes the pod name works out of the box. |
| `KITARU_WORKER_CONCURRENCY` | 1 | Tasks run in parallel |
| `KITARU_WORKER_SCOPE__CLAIMS` | all | JSON list of claims, such as `{"kind":"agent"}` or `{"kind":"agent","agent_version_id":"<UUID>"}` |
| `KITARU_WORKER_SCOPE__SELECTORS` | none | JSON label selectors (e.g. limit to one agent version's environment) |
| `KITARU_WORKER_SCOPE__JOB_ID` | none | Claim one job's tasks, drain, exit |
| `KITARU_WORKER_TIMEOUT` | none | Wall-clock lifetime; unset runs until stopped |
| `KITARU_WORKER_POLL_INTERVAL` | 2s | Sleep after an empty claim |
| `KITARU_WORKER_HEARTBEAT_INTERVAL` | 10s | Liveness reporting cadence |
| `KITARU_WORKER_BLOB_CACHE_ROOT` / `PAYLOAD_CACHE_ROOT` | `~/.cache/kitaru/...` | Plugin-code and payload caches, keyed by content hash |

The worker retains the API key for registration and worker-token renewal. Each task subprocess gets a narrower per-task token, with the API key stripped from its environment. Details in [Authentication & API keys](authentication.md).

## Fleet patterns

**One general worker per agent environment.** The simplest useful fleet: each environment that can run an agent gets a worker with no scope, and utility work (imports, evaluations) rides along.

**Split agent execution from plugin execution.** Agent replays need your application environment; evaluations and imports don't. A scoped pair keeps them independent:

```bash
# in the agent's environment
kitaru worker start --claim agent=<AGENT_VERSION_ID>

# anywhere cheap
kitaru worker start --claim evaluator --claim importer --concurrency 8
```

The versioned `agent` claim matches the agent version attached to each agent task, so a worker only claims replays its environment can actually run.

**One-shot workers in CI.** Pin a worker to the job you just created and it drains the job (appended evaluator tasks included), then exits:

```bash
kitaru worker start --job-id "$JOB_ID" --timeout 1800
```

This is the pattern for [CI regression gates](../guides/regression-suite.md): the runner that starts the experiment also executes it, using the PR's own checkout as the agent environment.

## Operational behavior

- **Draining**: SIGINT/SIGTERM stops claiming and finishes in-flight tasks; a second signal exits immediately. Per-task timeouts (set server-side and on agent versions) bound the wait.
- **Crash safety**: a worker that dies stops heartbeating; the server requeues its tasks to the next worker (up to the retry limit). No replay is lost to a pod eviction.
- **Liveness**: `kitaru worker list` shows the fleet and when each worker was last seen.
- **Subprocess environments**: evaluator and importer plugins run via `uv` in isolated per-plugin environments, cached by content hash; agent tasks run the agent version's command in the worker's own environment plus the version's [secrets](secrets.md). The default plugins (the five `kitaru/` importers and the built-in evaluator suite) run under the same isolation as plugins you write yourself.
