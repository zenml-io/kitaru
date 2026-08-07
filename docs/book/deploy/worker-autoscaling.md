---
description: Create worker pools and autoscale them with the kitaru-worker Helm chart, KEDA, or any platform that reads one endpoint
icon: arrow-trend-up
---

# Worker Pools and Autoscaling

A worker pool is a named claim scope, made of task kinds and label selectors, that lives on the server instead of inside each worker's own configuration. Workers join a pool by name instead of declaring their own scope. Claims resolve the pool's scope at claim time, so editing a pool retargets every worker already running against it without a restart. This gives an autoscaler one stable name to read demand from and resize, instead of duplicating the worker deployment's scope inside the scaler's own configuration. A single ad-hoc worker, or a worker pinned to one job, does not need a pool and keeps working exactly as before.

## Prerequisites

- A running Kitaru server ([Docker](docker.md) or [Helm](helm.md)).
- A service-account API key ([Authentication](../guides/authentication.md)).
- For Kubernetes autoscaling: a cluster with [KEDA](https://keda.sh) installed.

## Create a pool

A worker pool has a name and a scope, the task kinds and label selectors its workers claim. Pool names are globally unique.

```bash
kitaru worker pool create --name build-pool --kinds agent --kinds evaluator
```

Narrow the scope further with `--selector`, using either `KEY=VALUE[,VALUE]` or a JSON selector object:

```bash
kitaru worker pool create --name build-pool \
  --kinds agent \
  --selector environment=production
```

A pool's scope cannot pin a job. The server rejects a job-pinned scope with a 422. Job-pinned workers are for ephemeral per-job work and stay outside the pool model.

`kitaru worker pool list`, `kitaru worker pool get <name-or-id>`, `kitaru worker pool update <name-or-id>`, and `kitaru worker pool delete <name-or-id> --force` manage existing pools. The same operations are available over the REST API as `POST /v1/worker-pools` and its sibling routes.

## Join workers to a pool

Workers join a pool with `KITARU_WORKER_POOL` or `--pool`, instead of declaring their own `--kinds`/`--selector`/`--job-id` scope. A worker sets one or the other, never both.

```bash
export KITARU_API_URL=https://kitaru.example.com
export KITARU_API_KEY=kat_...
export KITARU_WORKER_POOL=build-pool
```

```bash
kitaru worker start
```

Or pass the pool on the command line instead:

```bash
kitaru worker start --pool build-pool
```

Set `KITARU_WORKER_CONCURRENCY` (or `--concurrency`) to the number of tasks one worker process holds at once. Inspect registered workers with `kitaru worker list` and `kitaru worker get <name-or-id>`.

## Pool stats endpoint

`GET /v1/worker-pools/{pool}/stats` returns one pool's pending workload, where `{pool}` is the pool's id or its exact name. Authenticate with a service-account API key as the bearer token:

```bash
curl -H "Authorization: Bearer ${KITARU_API_KEY}" \
  https://kitaru.example.com/v1/worker-pools/build-pool/stats
```

```json
{
  "pending_tasks": 12,
  "in_flight_tasks": 3,
  "oldest_pending_seconds": 42.5,
  "live_workers": 2,
  "capacity": 8
}
```

- `pending_tasks`: tasks the pool's scope matches that are waiting to be claimed. The primary scaling signal.
- `in_flight_tasks`: matching tasks already claimed or running.
- `oldest_pending_seconds`: age of the oldest pending match in seconds, `null` when nothing is pending.
- `live_workers`: pool workers the server currently considers alive.
- `capacity`: summed concurrency of those live workers, the task slots the pool can run at once.

Any autoscaler can poll this endpoint and turn `pending_tasks` into a replica count, roughly `ceil(pending_tasks / concurrency)` where concurrency is the value each worker was started with.

## Autoscale on Kubernetes with the kitaru-worker chart

The `kitaru-worker` Helm chart deploys a Deployment of pool-joined workers at a fixed `replicaCount`, or, when `keda.enabled` is `true`, scaled by a KEDA `ScaledObject` and `TriggerAuthentication` reading the pool's stats endpoint. The chart is not published to a chart registry. Install it from a checkout of the repository:

```bash
git clone https://github.com/zenml-io/kitaru
cd kitaru
```

Create a values file:

```yaml
kitaru:
  serverURL: https://kitaru.example.com
  apiKey: kat_...

pool:
  name: build-pool

worker:
  concurrency: 4

keda:
  enabled: true
  minReplicaCount: 0
  maxReplicaCount: 20
  tasksPerReplica: 4
  cooldownPeriod: 600
```

Install:

```bash
helm install my-kitaru-worker ./helm-worker -f worker-values.yaml
```

Keep the API key out of values files in source control by referencing an existing secret instead of `kitaru.apiKey`:

```bash
kubectl create secret generic kitaru-worker-api-key \
  --from-literal=KITARU_API_KEY=kat_...
```

```yaml
kitaru:
  apiKeySecret:
    name: kitaru-worker-api-key
    key: KITARU_API_KEY
```

Set `kitaru.apiKey` or `kitaru.apiKeySecret`, not both.

`keda.tasksPerReplica` should match `worker.concurrency` so KEDA targets roughly one replica per concurrency slot of pending work. `keda.minReplicaCount` and `keda.maxReplicaCount` bound the replica count, and `keda.cooldownPeriod` is how long KEDA waits after the last active scaling condition before it scales back down. With `minReplicaCount: 0`, the pool scales all the way to zero once `pending_tasks` drops to or below `keda.activationThreshold` (0 by default, so a single pending task scales it back up from zero).

{% hint style="info" %}
KEDA is a separate cluster-wide install and is not managed by this chart: https://keda.sh/docs/latest/deploy/
{% endhint %}

## KEDA without the Helm chart

Run workers however you like and wire KEDA to the pool stats endpoint directly. This is what the chart's `keda.enabled: true` templates render, spelled out by hand:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kitaru-worker-api-key
type: Opaque
stringData:
  apiKey: kat_...
---
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: kitaru-worker
spec:
  secretTargetRef:
    - parameter: bearerToken
      name: kitaru-worker-api-key
      key: apiKey
---
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: kitaru-worker
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: kitaru-worker
  minReplicaCount: 0
  maxReplicaCount: 20
  pollingInterval: 30
  cooldownPeriod: 600
  triggers:
    - type: metrics-api
      metadata:
        url: "https://kitaru.example.com/v1/worker-pools/build-pool/stats"
        valueLocation: "pending_tasks"
        targetValue: "4"
        activationTargetValue: "0"
        authMode: "bearer"
      authenticationRef:
        name: kitaru-worker
```

```bash
kubectl apply -f keda-worker-pool.yaml
```

`scaleTargetRef` names the Deployment running your workers, already configured with `KITARU_WORKER_POOL=build-pool` and a `terminationGracePeriodSeconds` that covers the drain timeout below. `targetValue` is the worker concurrency, and `valueLocation` reads `pending_tasks` straight out of the stats JSON above.

## Scale-in

On `SIGTERM`, a worker stops claiming new tasks and starts draining. It waits up to `KITARU_WORKER_DRAIN_TIMEOUT` (`--drain-timeout`) seconds for its held tasks to finish, then kills whatever is still running and releases those tasks back to pending so another worker can claim them. A server-requested cancel still cancels the task. Only the worker's own shutdown releases instead of canceling.

Follow this guidance for pool deployments:

- Set `terminationGracePeriodSeconds` above the drain timeout so Kubernetes does not `SIGKILL` the pod before the drain finishes. The kitaru-worker chart defaults `worker.terminationGracePeriodSeconds` to `worker.drainTimeoutSeconds` plus 30 seconds of slack.
- Do not set a worker lifetime timeout (`KITARU_WORKER_TIMEOUT` / `--timeout`) on pool workers. Let the scaler decide when a replica exits, not the worker itself.
- Set the scaler's cooldown (`keda.cooldownPeriod` for KEDA) above a typical task's duration, so a replica is not scaled down mid-task and forced to drain repeatedly.

Kubernetes picks scale-in victims arbitrarily. The release-on-drain behavior above is what makes losing a busy pod to a routine scale-down safe instead of lossy.

Dead worker registrations do not accumulate either. Workers that stop sending heartbeats are pruned from the server after a retention window.

## Other platforms

The contract is one HTTP GET returning JSON, so any autoscaler that can poll a URL and read a number works: Kubernetes HPA with an external metrics adapter, a scheduled function that republishes `pending_tasks` as a cloud provider metric, the Nomad autoscaler through its APM plugin, or a plain cron loop.

For a bare VM or Docker Compose deployment, a shell loop reading the endpoint and adjusting the service's replica count is enough. It assumes `KITARU_API_URL` and `KITARU_API_KEY` are already exported:

```bash
#!/usr/bin/env bash
set -euo pipefail

POOL=build-pool
CONCURRENCY=4
MIN_REPLICAS=0
MAX_REPLICAS=10

while true; do
  pending=$(curl -fsS -H "Authorization: Bearer ${KITARU_API_KEY}" \
    "${KITARU_API_URL}/v1/worker-pools/${POOL}/stats" | jq '.pending_tasks')
  replicas=$(( (pending + CONCURRENCY - 1) / CONCURRENCY ))
  (( replicas < MIN_REPLICAS )) && replicas=$MIN_REPLICAS
  (( replicas > MAX_REPLICAS )) && replicas=$MAX_REPLICAS
  docker compose up -d --scale worker=$replicas
  sleep 60
done
```

## Related pages

- [Docker](docker.md)
- [Helm](helm.md)
- [Authentication](../guides/authentication.md)
- [Configuration](../guides/configuration.md)
