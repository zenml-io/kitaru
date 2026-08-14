---
description: Deploy the Kitaru server on Kubernetes with the first-party Helm chart — external Postgres, automatic migrations, ingress, and TLS.
icon: dharmachakra
---

# Helm

The repository ships a first-party chart under `helm/` that deploys the Kitaru server on Kubernetes: a server Deployment (with optional autoscaling), a Service, ingress or Gateway API routing, and a database migration Job that runs before each install and upgrade so the server never starts against an unmigrated schema.

The chart deploys **the server only**. Postgres is yours to provide (managed Postgres is the expected shape), and [workers](workers.md) deploy separately in the environments your agents run in.

```bash
helm install kitaru oci://public.ecr.aws/zenml/kitaru \
  --namespace kitaru --create-namespace \
  --values my-values.yaml
```

<!-- TODO(v2-launch): confirm the final chart OCI path — the chart
     publishes to the `zenml` ECR Public alias, but the in-repo
     helm/README.md references public.ecr.aws/kitaru/kitaru. Verify one
     canonical `helm install` command against the released chart. -->

## The values that matter

A minimal production `my-values.yaml`:

```yaml
server:
  serverURL: https://kitaru.internal.example.com

  database:
    host: your-postgres-host
    username: kitaru
    passwordSecretRef:
      name: kitaru-db
      key: password
    sslMode: require

  auth:
    authScheme: local
    defaultAccount:
      passwordSecretRef:
        name: kitaru-bootstrap
        key: password

ingress:
  enabled: true
  host: kitaru.internal.example.com
```

The chart mirrors the same `KITARU_SERVER_*` configuration surface as the [Docker deployment](docker.md) — every server setting has a values path, secrets can be inline for a quick start or `secretRef`s for real deployments, and database TLS supports `disable` through `verify-full` with custom CA bundles.

The image is the published `zenmldocker/kitaru-server`, tagged to match the chart version by default — pin `server.image.tag` explicitly if you want upgrades to be deliberate.

## Operational notes

- **Migrations** run as a Helm hook Job before the server pods roll, so an upgrade that needs a schema change can't race its own pods. If the migration fails, the release fails — the previous version keeps running.
- **Scaling**: the server is stateless between requests; enable the HPA block or set replicas directly. All state is in Postgres.
- **Routing**: classic Ingress (nginx by default) and Gateway API HTTPRoute are both supported — enable exactly one.

After install, point your team at it:

```bash
kitaru login https://kitaru.internal.example.com
kitaru status
```

Then create [accounts and API keys](authentication.md) and start [workers](workers.md) where your agents live.
