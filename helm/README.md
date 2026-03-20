# Kitaru Helm Chart

Deploy the [Kitaru](https://kitaru.ai) server on Kubernetes.

The Kitaru chart wraps the [ZenML Helm chart](https://artifacthub.io/packages/helm/zenml/zenml)
as a dependency, overriding defaults to use the Kitaru server image and
Kitaru-specific environment variables. All ZenML server features (database
migrations, secrets encryption, ingress, autoscaling, etc.) are available
through the subchart.

## Quick start

```bash
helm dependency update ./helm
helm install kitaru-server ./helm \
  --namespace kitaru \
  --create-namespace
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
helm dependency update ./helm
helm install kitaru-server ./helm \
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
| `kitaru.server.debug` | `true` | Enable debug logging |
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
helm dependency update ./helm
helm upgrade kitaru-server ./helm -n kitaru -f my-values.yaml
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
