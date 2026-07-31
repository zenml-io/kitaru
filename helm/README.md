# Kitaru Helm chart

This chart deploys the standalone Kitaru server. It has no dependency on the
ZenML Helm chart or ZenML server runtime.

## Install

Kitaru requires PostgreSQL and a secret encryption key. Supply the connection
settings through environment values:

```yaml
server:
  environment:
    KITARU_SERVER_DB_HOST: postgres.example.com
    KITARU_SERVER_DB_PORT: "5432"
    KITARU_SERVER_DB_USER: kitaru
    KITARU_SERVER_DB_NAME: kitaru
    KITARU_SERVER_AUTH_SCHEME: none
  secretEnvironment:
    KITARU_SERVER_DB_PWD: replace-me
    KITARU_SERVER_SECRET_ENCRYPTION_KEY: replace-with-a-random-value
```

Install the chart from its OCI repository:

```bash
helm install kitaru oci://public.ecr.aws/zenml/kitaru \
  --namespace kitaru \
  --create-namespace \
  --version 0.2.0 \
  --values values.yaml
```

The chart owns the fixed deployment lifecycle: it runs
`python -m kitaru.server.database.main` as a pre-install and pre-upgrade hook,
starts the server from the image's default command, and checks the server's
standard health endpoints. These implementation details are not chart values.

## Managed Cloud authentication

Managed workspaces set `KITARU_SERVER_AUTH_SCHEME=cloud`, identify the server
with `KITARU_SERVER_SERVER_ID`, and configure the ZenML Cloud API through
`KITARU_SERVER_CONTROL_PLANE_API_URL`. The latter is an internal Kitaru runtime
setting. There is no separately deployed Kitaru control plane.

## Gateway API

Set `httpRoute.enabled` and provide `parentRefs` and `hostnames` to expose the
service through a Gateway API `HTTPRoute`.

```yaml
httpRoute:
  enabled: true
  parentRefs:
    - name: gateway
      namespace: gateway-system
      sectionName: https
  hostnames:
    - kitaru.example.com
```

See [`values.yaml`](values.yaml) for the available settings and defaults.
