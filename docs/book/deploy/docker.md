---
description: Deploy the Kitaru server using Docker or Docker Compose
icon: docker
---

# Docker

Self-host the Kitaru server so your agents run, replay, and diff on infrastructure
you control. The server stores every run's durable checkpoints and serves the
dashboard your team and coding agents use to inspect and replay them. The container
image is published at
[`zenmldocker/kitaru-server`](https://hub.docker.com/r/zenmldocker/kitaru-server) and works
with Docker, Docker Compose, or any container orchestration platform.

## Quick start

Start a server with sensible defaults:

```bash
docker run -d --platform linux/amd64 --name kitaru-server -p 8080:8080 zenmldocker/kitaru-server:latest
```

{% hint style="warning" %}
**Apple Silicon (M-series) Macs:** The published image is currently amd64-only.
Keep `--platform linux/amd64` on every `docker run`, `docker pull`, or Docker
Compose service that uses `zenmldocker/kitaru-server:*`. Without it, Docker will fail
with `no matching manifest for linux/arm64/v8`.

On Intel/AMD hosts, you can omit the flag if you prefer.
{% endhint %}

{% hint style="info" %}
Use a version-pinned tag (e.g. `zenmldocker/kitaru-server:0.2.0`) that matches your
client SDK version to avoid API incompatibilities. The official image already
includes the Kitaru UI bundled with that Kitaru release.
{% endhint %}

The server initializes an internal SQLite database on first startup. Wait for
the health endpoint before connecting:

```bash
until curl -fsS http://localhost:8080/health >/dev/null; do sleep 2; done
```

Then connect your local CLI:

```bash
kitaru login http://localhost:8080
```

This opens browser-based device authorization. After completing the flow,
verify with:

```bash
kitaru status
```

### Headless activation

The first time the server starts, you need to activate it by visiting the
dashboard at `http://localhost:8080` and creating an initial admin user account.

To skip this manual onboarding step (useful for automated or headless
deployments), pass these environment variables:

```bash
docker run -d --platform linux/amd64 --name kitaru-server -p 8080:8080 \
    -e ZENML_SERVER_AUTO_ACTIVATE=1 \
    -e ZENML_DEFAULT_USER_NAME=admin \
    -e ZENML_DEFAULT_USER_PASSWORD=password \
    zenmldocker/kitaru-server:latest
```

{% hint style="warning" %}
If `ZENML_DEFAULT_USER_PASSWORD` is omitted, the admin account is created
with an **empty password** — only appropriate for local development.
{% endhint %}

{% hint style="info" %}
The server activation variables use `ZENML_*` names because the Kitaru server
uses ZenML's server runtime internally. These are server-side configuration
knobs, not user-facing SDK settings.
{% endhint %}

### Building from source

If you are testing changes from a local checkout, use `just` when possible:

{% tabs %}
{% tab title="just (recommended)" %}
```bash
just DOCKER_REPO=kitaru-local DOCKER_TAG=dev server-image
```
{% endtab %}

{% tab title="Docker" %}
```bash
bash scripts/download-ui.sh
docker build -f docker/Dockerfile --target server -t kitaru-local:dev .
```
{% endtab %}
{% endtabs %}

The important detail is that the UI is selected before Docker builds. The
`server-image` recipe runs `scripts/download-ui.sh`, which bundles a stable/full
Kitaru UI release into the Kitaru package tree. Docker then installs Kitaru and
copies the packaged UI into the server dashboard. There is no Docker build arg
for choosing a UI release tag.

Then run it:

```bash
docker run -d --name kitaru-server -p 8080:8080 kitaru-local:dev
```

### Deployment invocation support (workload manager)

Snapshot-backed deployment execution (`kitaru invoke`,
`KitaruClient().deployments.invoke(...)`, and
`kitaru flow deployments curl`) requires server workload-manager support.

The official `zenmldocker/kitaru-server` image already enables this with:

```text
ZENML_SERVER_WORKLOAD_MANAGER_IMPLEMENTATION_SOURCE=zenml.zen_server.pipeline_execution.in_memory_workload_manager.InMemoryWorkloadManager
```

If you build a custom image (especially from plain
`zenmldocker/zenml-server`) or override server environment variables, preserve
or set a compatible workload-manager implementation explicitly.

### Container management

```bash
docker logs kitaru-server          # View server logs
docker logs kitaru-server -f       # Follow logs
docker stop kitaru-server          # Stop
docker start kitaru-server         # Restart
docker rm kitaru-server            # Remove
docker rm -f kitaru-server         # Force remove (stop + remove)
```

## Persist your data

### Default (ephemeral)

Without any volume mounts, the server stores everything inside the container:

- **Metadata:** SQLite database
- **Artifacts:** Local filesystem

This data is **lost** when the container is removed (`docker rm`). It survives
`docker stop` / `docker start`.

### Persisting the SQLite database

Mount a host directory or Docker volume at the default data path:

{% tabs %}
{% tab title="Named volume" %}
```bash
docker run -d --platform linux/amd64 --name kitaru-server -p 8080:8080 \
    -v kitaru-data:/zenml/.zenconfig/local_stores/default_zen_store \
    zenmldocker/kitaru-server:latest
```
{% endtab %}

{% tab title="Host directory" %}
```bash
mkdir kitaru-data
docker run -d --platform linux/amd64 --name kitaru-server -p 8080:8080 \
    --mount type=bind,source=$PWD/kitaru-data,target=/zenml/.zenconfig/local_stores/default_zen_store \
    zenmldocker/kitaru-server:latest
```
{% endtab %}
{% endtabs %}

{% hint style="info" %}
The path `/zenml/.zenconfig/local_stores/default_zen_store` is an internal
default inherited from the base image. It may change in a future release. For
production deployments, use an [external MySQL database](#using-mysql-recommended-for-production)
instead of relying on this path.
{% endhint %}

{% hint style="warning" %}
The container runs as UID 1000 (`zenml`). If using a bind mount, ensure the
host directory is writable by UID 1000: `chown -R 1000:1000 kitaru-data`
{% endhint %}

### Using MySQL (recommended for production)

SQLite works well for development and single-user setups, but for production
you should use MySQL:

- Supports concurrent access from multiple server replicas
- Better performance under load
- Standard backup and HA tooling

Start a MySQL container:

```bash
docker run -d --name kitaru-mysql \
    -p 3306:3306 \
    -e MYSQL_ROOT_PASSWORD=password \
    -e MYSQL_DATABASE=kitaru \
    -v kitaru-mysql:/var/lib/mysql \
    mysql:8.0
```

Then start the Kitaru server pointing at it:

```bash
docker run -d --platform linux/amd64 --name kitaru-server -p 8080:8080 \
    --add-host host.docker.internal:host-gateway \
    --env ZENML_STORE_URL=mysql://root:password@host.docker.internal:3306/kitaru \
    zenmldocker/kitaru-server:latest
```

The server automatically runs database migrations on first startup.

{% hint style="info" %}
The database URL uses `ZENML_STORE_URL` because the Kitaru server uses
ZenML's server runtime internally. Future versions may provide a
`KITARU_DATABASE_URL` equivalent.
{% endhint %}

{% hint style="warning" %}
**Linux users:** The `--add-host host.docker.internal:host-gateway` flag is
required on Linux to make `host.docker.internal` resolve inside the
container. On macOS and Windows, Docker provides this automatically.
{% endhint %}

### Docker Compose (server + MySQL)

A `docker-compose.yml` for a production-like setup:

```yaml
services:
  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: password
      MYSQL_DATABASE: kitaru
    volumes:
      - kitaru-mysql:/var/lib/mysql
    ports:
      - "3306:3306"
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 10s
      timeout: 5s
      retries: 5

  kitaru:
    image: zenmldocker/kitaru-server:latest
    platform: linux/amd64
    ports:
      - "8080:8080"
    environment:
      ZENML_STORE_URL: mysql://root:password@mysql:3306/kitaru
      ZENML_SERVER_AUTO_ACTIVATE: "1"
      ZENML_DEFAULT_USER_NAME: admin
      ZENML_DEFAULT_USER_PASSWORD: "${KITARU_ADMIN_PASSWORD:-}"
    depends_on:
      mysql:
        condition: service_healthy
    restart: on-failure

volumes:
  kitaru-mysql:
```

Create a `.env` file alongside the compose file:

```bash
KITARU_ADMIN_PASSWORD=your-secure-password
```

{% hint style="warning" %}
The `KITARU_ADMIN_PASSWORD` environment variable is optional here for
convenience. If not set, the admin account is created with an empty
password — not recommended for production.
{% endhint %}

Start:

```bash
docker compose up -d
```

Wait for health and connect:

```bash
until curl -fsS http://localhost:8080/health >/dev/null; do sleep 2; done
kitaru login http://localhost:8080
```

Tear down:

```bash
docker compose down        # Stop containers (keep data)
docker compose down -v     # Stop and delete volumes
```

## Connect to the server

### Interactive login (browser-based)

```bash
kitaru login http://localhost:8080
```

The CLI opens a browser for device authorization. If the browser does not open
automatically, copy/paste the printed URL.

### API key login (headless / CI)

For automation, create an OSS service-account API key first from an
already-authenticated admin/operator machine:

```bash
kitaru auth service-accounts create ci-runner
kitaru auth api-keys create ci-runner default -o json
```

These bootstrap commands require your current Kitaru CLI session to have
permission to manage service accounts. The headless job only needs the one-time
`key` value from the create response.

Store the one-time `key` value from the create response, then use it to connect:

```bash
kitaru login https://kitaru.example.com --api-key kat_abc123...
```

### Environment variable bootstrap (Docker / CI)

For containers or CI jobs that need to talk to the server without running
`kitaru login`:

```bash
export KITARU_SERVER_URL=https://kitaru.example.com
export KITARU_AUTH_TOKEN=kat_abc123...
export KITARU_PROJECT=my-project
```

See [Configuration](../guides/configuration.md) for the full env-var reference and
precedence rules, and [Authentication](../guides/authentication.md) for service
accounts, API keys, and short-lived bearer tokens.

### Verify connection

```bash
kitaru status     # Compact view
kitaru info       # Detailed view (shows server version, database type, deployment type)
```

### Disconnect

```bash
kitaru logout
```

## Troubleshooting

### Server won't start

Check container logs:

```bash
docker logs kitaru-server -f
```

Common causes:

- Database connection refused (wrong host/port/credentials in `ZENML_STORE_URL`)
- Port 8080 already in use (`docker: bind: address already in use`)
- Insufficient permissions on mounted volumes (UID 1000 cannot write)

### Login stalls or shows errors

- Ensure the `/health` endpoint returns 200 before attempting login.
  The server takes ~20 seconds on first startup to initialize the database.
- If the browser shows `{"detail":"An unexpected error occurred."}`, the
  dashboard assets may be missing from the image. Rebuild from source after
  running `bash scripts/download-ui.sh`, or use a newer published image tag.
- If the CLI keeps printing `authorization_pending`, the server may not be
  fully initialized. Wait and retry.
- Port **8383** belongs to bare `kitaru login` local-server mode. To connect
  to the Docker container, run `kitaru login http://localhost:8080`
  explicitly. If you already used the URL form and still see login failures,
  treat that as a separate problem: confirm `/health` returns 200 and inspect
  `docker logs kitaru-server` for the server-side error.
- Check `docker logs kitaru-server` for error details.

### `no matching manifest for linux/arm64/v8` (Apple Silicon)

If you see this error when pulling or running the image on an M-series Mac:

```
no matching manifest for linux/arm64/v8 in the manifest list entries
```

Add `--platform linux/amd64` to your `docker run` (or `docker pull`) command.
Docker Desktop will run the image under Rosetta emulation. See the
[Quick start](#quick-start) callout for the full command.

### `host.docker.internal` not resolving (Linux)

Add `--add-host host.docker.internal:host-gateway` to your `docker run`
command, or use `extra_hosts` in Docker Compose.

## Related pages

- [Configuration](../guides/configuration.md)
- [Stacks](../stacks/README.md)
- [Kubernetes Stacks](../stacks/kubernetes-stacks.md)
