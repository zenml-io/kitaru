---
description: Install the Kitaru SDK and CLI, start a local server, and log in.
icon: download
---

# Installation

Kitaru is three installable pieces: the **SDK + CLI** in your project, a
**server** your team shares (self-hosted, one per team), and **workers**
that execute replays and evaluations in your environment. For a first
session on one machine, all three run locally.

Kitaru requires **Python 3.11 or newer**.

## Install the SDK and CLI

{% tabs %}
{% tab title="uv (recommended)" %}
```bash
uv add "kitaru[cli,pydantic-ai]"
```
{% endtab %}

{% tab title="pip" %}
```bash
pip install "kitaru[cli,pydantic-ai]"
```
{% endtab %}
{% endtabs %}

| Extra | What it adds |
|---|---|
| `cli` | The `kitaru` command — login, registration, workers, jobs |
| `pydantic-ai` | The PydanticAI adapter that records your agent's runs |
| `worker` | Run a worker in this environment (`kitaru worker start`) |
| `server` | Run the Kitaru server itself from this package |

The plain `kitaru` package is the SDK alone — the async client and the
API models — which is all a production service needs to record sessions.

## Start a local server

The server is FastAPI + Postgres. For local use, the repository ships a
Docker Compose file:

```bash
git clone https://github.com/zenml-io/kitaru.git
cd kitaru
docker compose up -d
```

The server listens on `http://localhost:8000`. For a shared deployment —
your own Postgres, real auth, TLS — see
[Run the Server](../deploy/README.md).

## Log in

```bash
kitaru login --local          # http://localhost:8000
kitaru status
```

`kitaru login <url>` connects to a shared server instead. For
non-interactive use (CI, production services), create an API key and set
two environment variables — the SDK, the CLI, and workers all read them:

```bash
export KITARU_API_URL="http://localhost:8000"
export KITARU_API_KEY="KITKEY_..."
```

See [Authentication & API keys](../deploy/authentication.md) for how keys
are issued and managed.

## Verify

```bash
kitaru version
kitaru doctor
```

`kitaru doctor` checks the connection and reports what it finds.

## Next steps

Head to the [Quickstart](quickstart.md) to record and replay your first
run — or, if you already collect traces elsewhere, start with
[Import your traces](import-your-traces.md).
