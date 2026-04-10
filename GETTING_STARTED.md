# Kitaru Early Tester Guide

Thanks for testing Kitaru! This guide walks you through getting set up
locally, running example flows, and exploring the CLI.

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Docker (only if you want to run the Kitaru server image)

## Option A: Local-only (no server)

Kitaru runs fully locally out of the box — no server needed. This is the
fastest way to try it.

### 1. Install Kitaru

```bash
# Clone the repo
git clone https://github.com/zenml-io/kitaru.git
cd kitaru

# Install with all extras (recommended)
uv sync --extra mcp --extra pydantic-ai

# Or minimal install (core only, no MCP/PydanticAI support)
uv sync

# Or with pip (editable install)
pip install -e ".[mcp,pydantic-ai]"

```

The extras give you:

| Extra | What it enables |
|---|---|
| `mcp` | MCP server (`kitaru-mcp`) — query executions, artifacts, and logs from Claude Code, Cursor, or any MCP client |
| `pydantic-ai` | PydanticAI adapter — wrap PydanticAI agents with Kitaru tracking |

### 2. Initialize the project

```bash
uv run kitaru init
```

This creates a `.kitaru/` directory in your project root. It tells
Kitaru where your source code lives, which matters when packaging
flows for remote execution.

### 3. Verify the installation

```bash
uv run kitaru status
uv run kitaru --version
```

You should see Kitaru reporting a local default stack. No login or
server setup is required for local use.

If you want to test stack lifecycle management explicitly, create a disposable
local stack:

```bash
uv run kitaru stack create scratch
uv run kitaru stack list
```

That gives you a second local stack to switch to or delete while testing.

### 4. Run your first flow

```bash
uv run examples/basic_flow/first_working_flow.py
```

This runs a simple two-checkpoint flow (`gather_sources` → `summarize`)
and prints the result. Everything is persisted locally — you can
inspect it afterwards.

### 5. Explore what happened

```bash
# List recent executions
uv run kitaru executions list

# Get details of the latest execution (copy the ID from the list output)
uv run kitaru executions get <EXECUTION_ID>

# View runtime logs
uv run kitaru executions logs <EXECUTION_ID>
```

### 6. Try more examples

Each example demonstrates a different Kitaru primitive. Run them in
order of complexity. Examples are grouped into subdirectories under
`examples/`. See `examples/README.md` for the full catalog.

> **Note:** Some example implementations contain `image={"base_image": ...}`
> settings in their `@flow` decorator. These only apply when running on
> remote/containerized stacks — you can ignore them for local testing.

#### Structured logging (`kitaru.log()`)

```bash
uv run examples/basic_flow/flow_with_logging.py
```

Logs structured metadata at both flow and checkpoint scope. After
running, you can separately inspect runtime log lines with
`uv run kitaru executions logs <ID>`.

#### Artifact save/load (`kitaru.save()` / `kitaru.load()`)

```bash
uv run examples/basic_flow/flow_with_artifacts.py
```

Demonstrates persisting and loading named artifacts across executions.
The script runs two flows — the second one loads artifacts from the
first.

#### Wait for human input (`kitaru.wait()`)

```bash
uv run examples/execution_management/wait_and_resume.py
```

This is the human-in-the-loop example. It starts a flow that pauses
and waits for external approval. The script prints exact CLI commands
to run in **another terminal** to approve/reject and resume:

```bash
# In a second terminal (from the kitaru repo directory):
uv run kitaru executions input <EXEC_ID> --value true
uv run kitaru executions resume <EXEC_ID>
```

#### Replay with overrides

```bash
uv run examples/replay/replay_with_overrides.py
```

Runs a flow, then replays it from a specific checkpoint with an
overridden input — demonstrating Kitaru's replay/time-travel
capability.

#### Tracked LLM calls (`kitaru.llm()`) — requires API key

```bash
# Register a model alias and set your key
uv run kitaru model register fast --model openai/gpt-5-nano
export OPENAI_API_KEY=sk-...

uv run examples/llm/flow_with_llm.py
```

Makes tracked LLM calls with token/cost metadata logged automatically.

#### PydanticAI adapter — no API key needed

```bash
uv run examples/pydantic_ai_agent/pydantic_ai_adapter.py
```

Wraps a PydanticAI agent with Kitaru tracking. Uses `TestModel` so no
API keys are required.

## Use with Claude Code / Cursor (MCP)

If you installed the `mcp` extra, you get a `kitaru-mcp` server that
lets AI assistants query your executions, artifacts, and logs directly.

### Add to Claude Code

```bash
claude mcp add kitaru-mcp kitaru-mcp
```

### Add to Cursor

Add this to your Cursor MCP config (`.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "kitaru": {
      "command": "kitaru-mcp",
      "transport": "stdio"
    }
  }
}
```

### What can the MCP server do?

Once connected, your AI assistant can:

- **List and inspect executions** — "show me my recent executions"
- **Read execution logs** — "what did my last flow print?"
- **Browse artifacts** — "load the output of the research checkpoint"
- **Run flows** — "run examples/basic_flow/first_working_flow.py:research_agent"
- **Provide wait input** — "approve the pending wait"
- **Replay executions** — "replay my last run from write_draft"
- **Check status** — "what stack am I connected to?"

This is a great way to explore Kitaru interactively — run a few
examples first, then ask your AI assistant questions about what
happened.

### Claude Code skills

If you use Claude Code, install the Kitaru skills plugin:

```bash
/plugin marketplace add zenml-io/kitaru-skills
/plugin install kitaru@kitaru
```

This adds scoping and authoring skills that teach Claude how to design and
build durable workflows. See the [Claude Code Skills docs](https://kitaru.ai/docs/agent-integrations/claude-code-skill) for details.

## Option B: Run the Kitaru server (Docker)

If you want a client-server setup (e.g. to share state across machines
or test the login flow), you can run the server as a Docker container.

The production server image is based on the official `zenmldocker/zenml-server`
image with Kitaru and the Kitaru UI layered on top.

### 1. Build the server image locally

```bash
just server-image REPO=kitaru-local TAG=dev UI_TAG=v0.1.0
# or, if you do not have `just` installed:
docker build -f docker/Dockerfile --target server \
    --build-arg KITARU_UI_TAG=v0.1.0 \
    -t kitaru-local:dev .
```

> Replace `v0.1.0` with the desired Kitaru UI release tag. Without an
> explicit tag, the build defaults to `latest` (the most recent GitHub release).

This creates a local image tag called `kitaru-local:dev`.

For local UI development (without a published Kitaru UI release), use
the dev server image instead:

```bash
# Build kitaru-ui first, then copy dist/ into the build context:
cp -r /path/to/kitaru-ui/dist/ docker/kitaru-ui-dist/
just server-dev-image
```

### 2. Start the server

```bash
docker run -d --name kitaru-server -p 8080:8080 kitaru-local:dev
```

The server can take a little while to initialize on first startup.
Wait for the health endpoint to report success before you try to log in:

```bash
until curl -fsS http://localhost:8080/health >/dev/null; do sleep 2; done
```

> Use `/health` for readiness — not `/`. A missing or half-initialized
> dashboard can make `/` misleading.

### 3. Connect your local client

```bash
uv run kitaru login http://localhost:8080
```

This uses browser-based device authorization. A few practical notes:

- The CLI prints a `/devices/verify?...` URL. Open that URL in a browser
  on the same machine that published port `8080`.
- If your browser does not open automatically, copy/paste the printed URL
  manually.
- If the browser page shows `{"detail":"An unexpected error occurred."}`
  or the CLI keeps polling with `authorization_pending`, the image likely
  does not contain the bundled dashboard assets. Rebuild the image from
  the current branch or switch to a newer published tag.
- If login stalls, `docker logs kitaru-server` is the first place to look.

After login, `uv run kitaru status` should show the server connection.

### 4. Run examples as before

All the examples from Option A work the same way — the only
difference is that executions are now stored on the server instead of
locally.

### Stop the server

```bash
docker stop kitaru-server && docker rm kitaru-server
```

## Useful CLI commands

| Command | What it does |
|---|---|
| `uv run kitaru init` | Initialize a Kitaru project (creates `.kitaru/`) |
| `uv run kitaru status` | Show connection state and active stack |
| `uv run kitaru info` | Detailed environment info |
| `uv run kitaru executions list` | List recent flow executions |
| `uv run kitaru executions get <ID>` | Detailed view of one execution |
| `uv run kitaru executions logs <ID>` | View runtime logs |
| `uv run kitaru executions replay <ID> --from <checkpoint>` | Replay from a checkpoint |
| `uv run kitaru stack list` | List available stacks |
| `uv run kitaru stack create <name>` | Create and auto-activate a local stack |
| `uv run kitaru stack delete <name> --recursive --force` | Remove a disposable stack and switch back to default if needed |
| `uv run kitaru model register <alias> --model <model>` | Register an LLM model alias |
| `uv run kitaru secrets set <name> --KEY=value` | Store a secret |

## Giving feedback

We'd love to hear what works, what's confusing, and what's missing.
In particular:

- Did installation go smoothly?
- Were the examples easy to follow?
- Did the CLI feel intuitive?
- What would you want to build with Kitaru?
- Any errors or rough edges?

Please share feedback directly with us — your experience shapes what
we build next.
