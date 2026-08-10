# Worker pool supervisor

`supervisor.py` polls a Kitaru worker pool's stats endpoint and spawns or terminates local `kitaru worker start` subprocesses to match demand. The autoscaling contract is one HTTP endpoint returning a few numbers, so any process that can poll a URL and start or stop workers can autoscale a pool, with or without Kubernetes. This script is the bare-metal counterpart of the KEDA autoscaler described in [Worker Pools and Autoscaling](https://docs.zenml.io/kitaru/deploy/worker-autoscaling).

## Prerequisites

- A running Kitaru server.
- A service-account API key.
- A worker pool. Create one with `kitaru worker pool create --name build-pool --kinds agent`.
- `kitaru[cli,worker]` installed, so both this script and the `kitaru` command it spawns are available.

## Getting started

```bash
cd examples/features/worker_pool_supervisor
uv pip install 'kitaru[cli,worker]'
export KITARU_API_URL=https://kitaru.example.com
export KITARU_API_KEY=kat_...
export KITARU_SUPERVISOR_POOL=build-pool
uv run python supervisor.py
```

The supervisor logs one line per poll with the pool's pending and in-flight task counts, its live worker count, how many subprocesses it is currently running, and how many it wants. Submit tasks that match the pool's scope and watch it spawn workers, then let the queue drain and watch it scale back down. Stop it with Ctrl+C. It sends SIGTERM to every worker it started and waits for them to exit before exiting itself.

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `KITARU_SUPERVISOR_POOL` | required | Name of the pool to supervise |
| `KITARU_SUPERVISOR_MIN_WORKERS` | `0` | Minimum worker subprocesses, even with an empty queue |
| `KITARU_SUPERVISOR_MAX_WORKERS` | `4` | Maximum worker subprocesses, regardless of queue depth |
| `KITARU_SUPERVISOR_TASKS_PER_WORKER` | `1` | Pending tasks one worker is expected to cover |
| `KITARU_SUPERVISOR_POLL_SECONDS` | `15` | Seconds between stats polls |
| `KITARU_SUPERVISOR_SCALE_DOWN_AFTER` | `3` | Consecutive polls wanting fewer workers before the supervisor terminates the surplus |
| `KITARU_WORKER_CONCURRENCY` | worker default | Passed through unchanged to every spawned worker |
| `KITARU_WORKER_DRAIN_TIMEOUT` | worker default | Passed through unchanged to every spawned worker, and bounds the supervisor's own shutdown wait |

`KITARU_API_URL` and `KITARU_API_KEY` are read from the supervisor's own environment and inherited by every spawned worker, so set them once before starting the supervisor. The desired worker count is `ceil(pending_tasks / KITARU_SUPERVISOR_TASKS_PER_WORKER)`, clamped between the min and max worker settings.

Scaling up is immediate. As soon as a poll wants more workers than are running, the supervisor spawns the difference. Scaling down is delayed. It only terminates surplus workers after `KITARU_SUPERVISOR_SCALE_DOWN_AFTER` consecutive polls want fewer of them, which absorbs a temporarily quiet queue without repeatedly killing and respawning workers.

## Scale-in is loss-free

Terminating a surplus worker sends it SIGTERM, the same signal Kubernetes sends a pod during a routine scale-down. The worker stops claiming new tasks, drains what it is holding up to its drain timeout, then releases any task it has not finished back to pending so another worker can claim it. Nothing is canceled and no work is lost, so the supervisor can scale down without waiting for tasks to finish naturally.

## Kubernetes

This script is for bare-metal, VM, or Docker Compose deployments where nothing else is watching the pool. On Kubernetes, use the `kitaru-worker` Helm chart with `keda.enabled: true` instead, described in [Worker Pools and Autoscaling](https://docs.zenml.io/kitaru/deploy/worker-autoscaling). It wires KEDA to the same stats endpoint this supervisor polls, but scales real pods instead of local subprocesses.

## Platform notes

The supervisor targets POSIX (Linux and macOS). It relies on SIGTERM to signal a graceful drain, and Windows processes do not support that signal the same way.
