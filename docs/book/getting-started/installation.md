---
description: Install the Kitaru SDKs and CLI, start a local server, and log in, for Python and TypeScript.
icon: download
---

# Installation

Kitaru is three installable pieces:

- the **SDK + CLI** in your project
- a **server** your team shares (self-hosted, one per team)
- **workers** that execute replays and evaluations in your environment

For a first session on one machine, all three run locally.

The Kitaru CLI, server, and workers require **Python 3.11 or newer**. TypeScript agents use Node **22.22 or newer in the Node 22 release line** and connect to the same server.

## Install the Python SDK and CLI

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
| `cli` | The `kitaru` command, the full loop: import, evaluate, cohorts, experiments, workers, jobs |
| `worker` | Run a worker in this environment (`kitaru worker start`) |
| `server` | Run the Kitaru server itself from this package |
| `mcp` | The `kitaru-mcp` server for [coding assistants](../agent-native/README.md) |
| `otel` | OpenTelemetry export from the server |

The plain `kitaru` package is the SDK alone (the async client and the API models), which is all a production service needs to record sessions.

Adapters are **not** extras. Each ships as its own distribution, so you install the one your framework needs alongside Kitaru:

| Framework | Install |
| --- | --- |
| [PydanticAI](../adapters/pydantic-ai.md) | `kitaru-pydantic-ai` |
| [LangGraph](../adapters/langgraph.md) (also LangChain agents, Deep Agents) | `kitaru-langgraph` |
| [OpenAI Agents SDK](../adapters/openai-agents.md) | `kitaru-openai-agents` |

## Install the TypeScript SDK and adapters

`@zenml-io/kitaru` is the framework-neutral TypeScript SDK: it creates and inspects Kitaru resources, records sessions, submits evaluations and experiments, and waits for exact jobs. The Python `kitaru` command remains the CLI for login and worker operations; there is no separate TypeScript CLI.

{% hint style="info" %}
The TypeScript packages require Node `>=22.22.0 <23` and are versioned and released together.
{% endhint %}

Install the adapter in the Node project that runs your agent:

{% tabs %}
{% tab title="Mastra" %}
```bash
pnpm add @zenml-io/kitaru-mastra @mastra/core@1.51.0
```

See the [Mastra adapter](../adapters/mastra.md) for the wrapper, replay behavior, and supported boundary.
{% endtab %}

{% tab title="Vercel AI SDK" %}
```bash
pnpm add @zenml-io/kitaru-vercel-ai ai@7.0.65
```

See the [Vercel AI SDK adapter](../adapters/vercel-ai.md) for Agent and `generateText` recording, replay behavior, and the supported boundary.
{% endtab %}

{% tab title="Build an adapter" %}
```bash
pnpm add @zenml-io/kitaru
```

The core package provides the TypeScript client and adapter primitives. It does not provide a framework-neutral agent or streaming abstraction.
{% endtab %}
{% endtabs %}

The Node agent still needs a reachable Kitaru server. Install the Python CLI and worker separately when you want to run the full loop locally, or connect the agent to your team's deployed server and workers.

No adapter for your framework? You are not blocked: [import your traces instead, build a project-local adapter, or have Kitaru call your agent](../adapters/custom.md).

## Install the agent skills

Do this now rather than later. Kitaru is a loop with real judgment calls in it: which sessions to review, when a behavior is worth freezing into a cohort, whether a replay result actually supports shipping. The [agent skills](../agent-native/README.md) teach your coding assistant how to make them with you:

{% tabs %}
{% tab title="Any skill-aware host" %}
```bash
npx skills add zenml-io/kitaru-skills
```
{% endtab %}

{% tab title="Claude Code plugin" %}
```
/plugin marketplace add zenml-io/kitaru-skills
/plugin install kitaru@kitaru
```
{% endtab %}
{% endtabs %}

`kitaru-investigation` is the front door: point your assistant at it and it will walk you from the traces you have to a reviewed cohort, choosing the review batch and stopping at checkpoints you can resume from. The others cover [replay experiments](../adapters/README.md), [building an adapter](../adapters/custom.md), and building an importer.

Pair them with the [MCP server](../agent-native/README.md) (`kitaru[mcp]`) so the assistant has bounded operations to go with the method. `kitaru` with no arguments tells you whether the skills are installed.

## Start a local server

The server is FastAPI + Postgres, and the CLI can run both for you. All it needs is [Docker](https://docs.docker.com/get-started/get-docker/) with the [Compose v2 plugin](https://docs.docker.com/compose/install/):

```bash
kitaru login --local
```

This provisions a server and PostgreSQL pinned to your installed Kitaru version, waits for `http://localhost:8000` to become healthy, selects it as your active server, and opens it in your browser. The lifecycle is three commands:

```bash
kitaru local logs            # inspect (add --service server --follow)
kitaru logout                # stop the containers; the database persists
kitaru logout --volumes      # stop and delete the database (a clean reset)
```

After upgrading the `kitaru` package, upgrade the local server to match with `kitaru login --local --upgrade`; a plain login deliberately never replaces the server image. Prefer to manage Docker yourself, or need a shared deployment with your own Postgres, real auth, and TLS? See [Docker](../deploy/docker.md) and [Deploy Kitaru](../deploy/README.md).

## Connect

`kitaru login --local` already connected you; `kitaru status` confirms it.

Against a shared server, log in with `kitaru login <url>`. For non-interactive use (CI, production services), create an API key and set two environment variables that the SDK, the CLI, and workers all read:

```bash
export KITARU_API_URL="https://kitaru.your-team.example"
export KITARU_API_KEY="KITKEY_..."
```

See [Authentication & API keys](../deploy/authentication.md) for how keys are issued and managed.

Node applications can also reuse a developer's selected CLI login without exporting its token; see the [TypeScript SDK](../adapters/typescript-sdk.md#reuse-a-developer-login). Use dedicated API keys or worker task tokens for CI and production rather than copying a developer credential store.

## Verify

```bash
kitaru version
kitaru doctor
```

`kitaru doctor` checks the connection and reports what it finds.

## Next steps

Read the [Quickstart](quickstart.md) to understand Kitaru's five-step method. For a controlled hands-on path, prepare the public [`kitaru-template`](https://github.com/zenml-io/kitaru-template) and continue with the [complete returns-agent tutorial](../tutorials/returns-agent/README.md). If you already collect traces elsewhere, start with [Import your traces](import-your-traces.md).
