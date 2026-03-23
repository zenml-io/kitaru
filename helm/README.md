# Kitaru Helm Chart

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/kitaru)](https://artifacthub.io/packages/helm/kitaru/kitaru)

![Kitaru Logo](https://raw.githubusercontent.com/zenml-io/kitaru/main/assets/kitaru_logo.png)

## Overview

[Kitaru](https://kitaru.ai) is a durable execution layer for AI agents. It provides primitives that make agent workflows persistent, replayable, and observable — without requiring users to learn a graph DSL or change their Python control flow.

## Features

- Easy deployment of the Kitaru server on Kubernetes.
- Wraps the [ZenML Helm chart](https://artifacthub.io/packages/helm/zenml/zenml) as a dependency with Kitaru-specific defaults.
- All ZenML server features available: database migrations, secrets encryption, ingress, autoscaling, and more.
- Highly configurable via Helm values.
- Supports multiple secrets store backends (AWS Secrets Manager, GCP Secrets Manager, Azure Key Vault).

## Quickstart

### Install the Chart

To install the Kitaru chart directly from Amazon ECR, use the following command:

```bash
# example command for version 0.2.0
helm install kitaru-server oci://public.ecr.aws/zenml/kitaru \
  --namespace kitaru \
  --create-namespace \
  --version 0.2.0
```

This starts a Kitaru server with a local SQLite database persisted via a
PersistentVolumeClaim. Once the pod is ready, port-forward and connect:

```bash
kubectl -n kitaru port-forward svc/kitaru-server-kitaru 8080:80
kitaru login http://localhost:8080
```

## Configuration

All configuration is done through a values file. Server settings go under
`kitaru.server` (the ZenML runtime that powers the Kitaru server). Create a
`my-values.yaml` with the settings you need, then install:

```bash
helm install kitaru-server oci://public.ecr.aws/zenml/kitaru \
  --namespace kitaru \
  --create-namespace \
  -f my-values.yaml
```

### Minimal production example

A typical production setup with MySQL, Ingress, and secrets encryption:

```yaml
kitaru:
  server:
    serverURL: https://kitaru.example.com

    database:
      url: "mysql://kitaru@mysql-host:3306/kitaru"
      passwordSecretRef:
        name: kitaru-db-password
        key: password

    auth:
      jwtSecretKey: "<openssl rand -hex 32>"

    secretsStore:
      enabled: true
      type: sql
      sql:
        encryptionKey: "<openssl rand -hex 32>"

    ingress:
      enabled: true
      host: kitaru.example.com
      annotations:
        cert-manager.io/cluster-issuer: "letsencrypt"
      tls:
        enabled: true
        secretName: kitaru-tls

  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: "1"
      memory: 2Gi
```

Before installing, create the database password Secret:

```bash
kubectl -n kitaru create secret generic kitaru-db-password \
  --from-literal=password=my-secret-password
```

### Key values

| Key | Default | Description |
|-----|---------|-------------|
| `kitaru.server.image.repository` | `zenmldocker/kitaru` | Server image |
| `kitaru.server.image.tag` | Chart version | Image tag |
| `kitaru.server.serverURL` | — | External server URL (for login redirects) |
| `kitaru.server.debug` | `false` | Enable debug logging |
| `kitaru.server.database.url` | — | External DB URL. SQLite if unset |
| `kitaru.server.database.persistence.enabled` | `true` | Persist SQLite via PVC |
| `kitaru.server.auth.jwtSecretKey` | auto-generated | JWT signing key |
| `kitaru.server.secretsStore.sql.encryptionKey` | — | Encryption key for stored secrets |
| `kitaru.server.ingress.enabled` | `false` | Enable Ingress |
| `kitaru.server.ingress.host` | — | Ingress hostname |
| `kitaru.server.ingress.tls.enabled` | `false` | Enable TLS |
| `kitaru.server.service.type` | `ClusterIP` | Service type |
| `kitaru.server.environment` | Kitaru env vars | Extra env vars |
| `kitaru.resources` | `{}` | CPU/memory requests and limits |
| `kitaru.autoscaling.enabled` | `false` | Enable HPA |

For the full list of available settings, see the
[ZenML Helm chart values](https://artifacthub.io/packages/helm/zenml/zenml?modal=values)
— all options are available under the `kitaru.server` key.

## Upgrading

```bash
helm upgrade kitaru-server oci://public.ecr.aws/zenml/kitaru \
  -n kitaru -f my-values.yaml
```

## Uninstalling

```bash
helm uninstall kitaru-server --namespace kitaru
```

The PVC created for SQLite persistence is **not** deleted automatically. To
remove it:

```bash
kubectl -n kitaru delete pvc -l app.kubernetes.io/instance=kitaru-server
```
