---
description: Install Kitaru with one command, start the local server, and verify. Other install paths for project environments, TypeScript, and team servers.
icon: download
---

# Installation

One command installs everything a first session needs. It needs [Docker](https://docs.docker.com/get-started/get-docker/) for the local server (the CLI installs without it) and works on macOS, Linux, WSL, and Git Bash on Windows:

```bash
curl -fsSL https://kitaru.ai/install | bash
```

What it does, in order:

1. Installs the `kitaru` CLI and the `kitaru-mcp` server into an isolated [uv](https://docs.astral.sh/uv/) environment, installing uv first if you do not have it. No system Python is required.
2. Installs the [agent skills](../agent-native/setup.md) into `~/.agents/skills`, and into `~/.claude/skills` and `~/.codex/skills` when Claude Code or Codex is installed.
3. Registers the MCP server with Claude Code and Codex.
4. Runs `kitaru login --local`, which starts the server and PostgreSQL in Docker and logs you in. If Docker is not running, it prints that command for later instead.

Nothing needs `sudo`, everything lands under your home directory, and running it again upgrades. Options: `--server https://your-team.kitaru.ai` logs in to a team server instead of starting a local one, `--with kitaru-pydantic-ai` adds an adapter to the same environment, `--no-login`, `--no-skills`, `--no-mcp` skip steps, and `--no-modify-path` leaves your shell rc files alone. `bash -s -- --help` after the pipe lists everything.

Prefer to do it by hand? The installer is three commands, which you can run yourself:

```bash
uv tool install "kitaru[cli,mcp,worker]"     # CLI + MCP server, isolated from your projects
npx skills add zenml-io/kitaru-skills          # the coding-agent skills
kitaru login --local                           # local server in Docker, or: kitaru login <team-url>
```

then point your assistant at `kitaru-mcp` as described in [Set up your coding agent](../agent-native/setup.md).

Already inside Claude Code, Codex, or Cursor? Paste this instead and it runs the same installer for you:

```
Set up Kitaru on this machine by following https://kitaru.ai/install.md. Use the one-line installer, tell me what it did, and stop before logging in if Docker is not running.
```

## Verify

```bash
kitaru doctor
```

It checks the CLI, the server connection, authentication, and whether the skills are installed. If the installer skipped login because Docker was not running, start Docker and run `kitaru login --local`; the section below covers the local server's lifecycle.

Then read the [Quickstart](quickstart.md). It is written as prompts for your coding agent, and everything it needs is now in place.

## The local server

The server is FastAPI + Postgres, and the CLI can run both for you. All it needs is [Docker](https://docs.docker.com/get-started/get-docker/) with the [Compose v2 plugin](https://docs.docker.com/compose/install/):

```bash
kitaru login --local
```

This provisions a server and PostgreSQL pinned to your installed Kitaru version, waits for `http://localhost:8000` to become healthy, selects it as your active server, and opens it in your browser. If port 8000 is unavailable, select another host port with either `kitaru login --local --port 9000` or `KITARU_LOCAL_PORT=9000 kitaru login --local`. The command-line flag takes precedence over the environment variable, and the CLI remembers the selected port for later logins and logout. The lifecycle is three commands:

```bash
kitaru local logs            # inspect (add --service server --follow)
kitaru logout                # stop the containers; the database persists
kitaru logout --volumes      # stop and delete the database (a clean reset)
```

After upgrading the `kitaru` package, upgrade the local server to match with `kitaru login --local --upgrade`; a plain login deliberately never replaces the server image. Prefer to manage Docker yourself, or need a shared deployment with your own Postgres, real auth, and TLS? See [Docker](../deploy/docker.md) and [Deploy Kitaru](../deploy/README.md).

## Connect to a team server

`kitaru login --local` already connected you; `kitaru status` confirms it.

Against a shared server, log in with `kitaru login <url>`. For non-interactive use (CI, production services), create an API key and set two environment variables that the SDK, the CLI, and workers all read:

```bash
export KITARU_API_URL="https://kitaru.your-team.example"
export KITARU_API_KEY="KITKEY_..."
```

See [Authentication & API keys](../deploy/authentication.md) for how keys are issued and managed.

Node applications can also reuse a developer's selected CLI login without exporting its token; see the [TypeScript SDK](../deploy/sdks.md). Use dedicated API keys or worker task tokens for CI and production rather than copying a developer credential store.

## Other ways to install

The installer is the right choice for the CLI on a laptop. The paths below are for when Kitaru has to live inside a specific environment: your agent's own code, a Node project, CI, or a machine where you only want the skills.

Kitaru is three pieces: the **SDK + CLI**, a **server** your team shares (self-hosted, one per team), and **workers** that execute replays and evaluations in your environment. The CLI, server, and workers require **Python 3.11 or newer**; TypeScript agents use Node **22.22 or newer in the Node 22 release line** and connect to the same server. The server stores everything in **PostgreSQL**, provisioned for you locally by `kitaru login --local`; a [self-hosted deployment](../deploy/README.md) brings its own. Workers are plain processes (`kitaru worker start`) that run wherever your agent's environment lives; for containerized fleets, the published `zenmldocker/kitaru-worker` image works out of the box (see [Workers in production](../deploy/workers.md)).

### Add the Python SDK to a project

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
| `mcp` | The `kitaru-mcp` server for [coding assistants](../agent-native/setup.md) |
| `otel` | OpenTelemetry export from the server |

The plain `kitaru` package is the SDK alone (the async client and the API models), which is all a production service needs to record sessions.

Adapters are **not** extras. Each ships as its own distribution, so you install the one your framework needs alongside Kitaru:

| Framework | Install |
| --- | --- |
| [PydanticAI](../adapters/pydantic-ai.md) | `kitaru-pydantic-ai` |
| [LangGraph](../adapters/langgraph.md) (also LangChain agents, Deep Agents) | `kitaru-langgraph` |
| [OpenAI Agents SDK](../adapters/openai-agents.md) | `kitaru-openai-agents` |

### TypeScript SDK and adapters

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

No adapter for your framework? You are not blocked: [import your traces instead, or build a project-local adapter with the adapter-builder skill](../adapters/custom.md).

### Only the agent skills

Do this now rather than later. Kitaru is a loop with real judgment calls in it: which sessions to review, when a behavior is worth freezing into a cohort, whether a replay result supports shipping. The [agent skills](../agent-native/setup.md) teach your coding assistant how to make them with you:

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

Pair them with the [MCP server](../agent-native/setup.md) (`kitaru[mcp]`) so the assistant has bounded operations to go with the method. `kitaru` with no arguments tells you whether the skills are installed.

## Next steps

Read the [Quickstart](quickstart.md) to understand Kitaru's five-step method. For a controlled hands-on path, prepare the [PydanticAI returns agent example](https://github.com/zenml-io/kitaru/tree/main/examples/python/pydantic_ai_ticket_resolver) and continue with the [complete returns agent tutorial](../tutorials/returns-agent/README.md). If you already collect traces elsewhere, start with [Import your traces](import-your-traces.md).
