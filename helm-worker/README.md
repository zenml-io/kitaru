# Kitaru Worker Helm Chart

## Overview

This chart deploys an autoscaling [Kitaru](https://kitaru.ai) worker pool on Kubernetes. Workers dial out to an existing Kitaru server, join a named worker pool, and claim tasks from it. Scaling is driven by KEDA reading the pool's stats endpoint, not by Kitaru managing any infrastructure itself.

## Quickstart

### Install the Chart

```bash
helm install my-kitaru-worker ./helm-worker \
  --set kitaru.serverURL=https://kitaru.example.com \
  --set kitaru.apiKey=<api-key> \
  --set pool.name=<pool-name>
```

### Required values

- `kitaru.serverURL`: the URL of the Kitaru server the workers dial out to.
- `kitaru.apiKey` or `kitaru.apiKeySecret.name`/`kitaru.apiKeySecret.key`: the Kitaru API key the workers authenticate with, either as a plain value or as a reference to an existing Kubernetes secret. Set exactly one of the two.
- `pool.name`: the name of the worker pool this deployment serves. Workers register with this name instead of declaring their own scope.

## Configuration

This chart offers a multitude of configuration options. For detailed information, check the default [`values.yaml`](values.yaml) file.

### Autoscaling with KEDA

Set `keda.enabled` to `true` to have [KEDA](https://keda.sh) scale the worker deployment from the pool's stats endpoint instead of running a fixed `replicaCount`. This installs a `ScaledObject` targeting the worker `Deployment` and a `TriggerAuthentication` that forwards the Kitaru API key as a bearer token, using a `metrics-api` trigger against `{serverURL}/v1/worker-pools/{pool.name}/stats`. `keda.tasksPerReplica` should match `worker.concurrency` so KEDA targets roughly one replica per concurrency slot of pending work, and `keda.minReplicaCount` can be set to `0` to scale the pool to nothing when its queue is empty. KEDA itself is a separate cluster-wide install and is not managed by this chart.

### Scale-in and drain

A worker drains on `SIGTERM`: it stops claiming new tasks and releases any tasks it has not finished back to the queue before exiting. `worker.terminationGracePeriodSeconds` must stay comfortably above `worker.drainTimeoutSeconds` so Kubernetes does not SIGKILL the pod before the drain has a chance to finish, which is why it defaults to the drain timeout plus 30 seconds of slack. This makes routine scale-down, including KEDA scaling to zero, safe for in-flight tasks instead of losing them.

## Contributing

Feel free to [submit issues or pull requests](https://github.com/zenml-io/kitaru) if you would like to improve the chart.

## License

[This project is licensed](https://github.com/zenml-io/kitaru/blob/main/LICENSE) under the terms of the Apache-2.0 license.
