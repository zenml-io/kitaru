---
description: Install the Kitaru SDK and CLI, start a local server, and log in.
icon: download
---

# Installation

Kitaru is three installable pieces: the **SDK + CLI** in your project, a **server** your team shares (self-hosted, one per team), and **workers** that execute replays and evaluations in your environment. For a first session on one machine, all three run locally.

Kitaru requires **Python 3.11 or newer**.

## Install the SDK and CLI

{% tabs %} {% tab title="uv (recommended)" %}

```bash
uv add "kitaru[cli,worker]" kitaru-pydantic-ai
```

{% endtab %}

{% tab title="pip" %}

```bash
pip install "kitaru[cli,worker]" kitaru-pydantic-ai
```

{% endtab %} {% endtabs %}

| Extra | What it adds |
| --- | --- |
| `cli` | The `kitaru` command — the full loop: import, evaluate, cohorts, experiments, workers, jobs |
| `worker` | Run a worker in this environment (`kitaru worker start`) |
| `server` | Run the Kitaru server itself from this package |
| `mcp` | The `kitaru-mcp` server for [coding assistants](../agent-native/mcp-server.md) |
| `otel` | OpenTelemetry export from the server |

The plain `kitaru` package is the SDK alone — the async client and the API models — which is all a production service needs to record sessions.

Adapters are **not** extras — each ships as its own distribution, so you install the one your framework needs alongside Kitaru:

| Framework | Install |
| --- | --- |
| [PydanticAI](../adapters/pydantic-ai.md) | `kitaru-pydantic-ai` |
| [LangGraph](../adapters/langgraph.md) (also LangChain agents, Deep Agents) | `kitaru-langgraph` |
| [OpenAI Agents SDK](../adapters/openai-agents.md) | `kitaru-openai-agents` |

## Start a local server

The server is FastAPI + Postgres, and the CLI can run both for you — all it needs is Docker with the Compose v2 plugin:

```bash
kitaru login --local
```

This provisions a server and PostgreSQL pinned to your installed Kitaru version, waits for `http://localhost:8000` to become healthy, selects it as your active server, and opens it in your browser. The lifecycle is three commands:

```bash
kitaru local logs            # inspect (add --service server --follow)
kitaru logout                # stop the containers; the database persists
kitaru logout --volumes      # stop and delete the database — a clean reset
```

After upgrading the `kitaru` package, upgrade the local server to match with `kitaru login --local --upgrade` — a plain login deliberately never replaces the server image. Prefer to manage Docker yourself, or need a shared deployment with your own Postgres, real auth, and TLS? See [Docker](../deploy/docker.md) and [Run the Server](../deploy/README.md).

## Connect

`kitaru login --local` already connected you — `kitaru status` confirms it.

Against a shared server, log in — `kitaru login <url>` — or, for non-interactive use (CI, production services), create an API key and set two environment variables that the SDK, the CLI, and workers all read:

```bash
export KITARU_API_URL="https://kitaru.your-team.example"
export KITARU_API_KEY="KITKEY_..."
```

See [Authentication & API keys](../deploy/authentication.md) for how keys are issued and managed.

## Verify

```bash
kitaru version
kitaru doctor
```

`kitaru doctor` checks the connection and reports what it finds.

## Next steps

Head to the [Quickstart](quickstart.md) to record and replay your first run — or, if you already collect traces elsewhere, start with [Import your traces](import-your-traces.md).
