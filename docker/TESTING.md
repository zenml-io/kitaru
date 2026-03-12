# Kitaru Early Tester Guide

Thanks for testing Kitaru! This guide walks you through getting set up
locally, running example flows, and exploring the CLI.

## Prerequisites

- Python 3.11+
- Git (needed to clone the repo and install the ZenML dependency)
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

# Or with pip
pip install ".[mcp,pydantic-ai]"
```

The extras give you:

| Extra | What it enables |
|---|---|
| `mcp` | MCP server (`kitaru-mcp`) — query executions, artifacts, and logs from Claude Code, Cursor, or any MCP client |
| `pydantic-ai` | PydanticAI adapter — wrap PydanticAI agents with Kitaru tracking |

### 2. Verify the installation

```bash
uv run kitaru status
uv run kitaru --version
```

You should see Kitaru reporting a local default runner. No login or
server setup is required for local use.

If you want to test runner lifecycle management explicitly, create a disposable
local runner:

```bash
uv run kitaru runner create scratch
uv run kitaru runner list
```

That gives you a second local runner to switch to or delete while testing.

### 3. Run your first flow

```bash
uv run -m examples.first_working_flow
```

This runs a simple two-checkpoint flow (`fetch_data` → `process_data`)
and prints the result. Everything is persisted locally — you can
inspect it afterwards.

### 4. Explore what happened

```bash
# List recent executions
uv run kitaru executions list

# Get details of the latest execution (copy the ID from the list output)
uv run kitaru executions get <EXECUTION_ID>

# View runtime logs
uv run kitaru executions logs <EXECUTION_ID>
```

### 5. Try more examples

Each example demonstrates a different Kitaru primitive. Run them in
order of complexity. The implementations are now grouped into subdirectories
under `examples/`, but the stable commands below still work from the repo root.
See `examples/README.md` for the full catalog.

> **Note:** Some example implementations contain `image={"base_image": ...}`
> settings in their `@flow` decorator. These only apply when running on
> remote/containerized runners — you can ignore them for local testing.

#### Structured logging (`kitaru.log()`)

```bash
uv run -m examples.flow_with_logging
```

Logs structured metadata at both flow and checkpoint scope. After
running, you can separately inspect runtime log lines with
`uv run kitaru executions logs <ID>`.

#### Artifact save/load (`kitaru.save()` / `kitaru.load()`)

```bash
uv run -m examples.flow_with_artifacts
```

Demonstrates persisting and loading named artifacts across executions.
The script runs two flows — the second one loads artifacts from the
first.

#### Wait for human input (`kitaru.wait()`)

```bash
uv run -m examples.wait_and_resume
```

This is the human-in-the-loop example. It starts a flow that pauses
and waits for external approval. The script prints exact CLI commands
to run in **another terminal** to approve/reject and resume:

```bash
# In a second terminal (from the kitaru repo directory):
uv run kitaru executions input <EXEC_ID> --wait <WAIT_ID> --value true
uv run kitaru executions resume <EXEC_ID>
```

#### Replay with overrides

```bash
uv run -m examples.replay_with_overrides
```

Runs a flow, then replays it from a specific checkpoint with an
overridden input — demonstrating Kitaru's replay/time-travel
capability.

#### Tracked LLM calls (`kitaru.llm()`) — requires API key

```bash
# Register a model alias and set your key
uv run kitaru model register fast --model openai/gpt-4o-mini
export OPENAI_API_KEY=sk-...

uv run -m examples.flow_with_llm
```

Makes tracked LLM calls with token/cost metadata logged automatically.

#### PydanticAI adapter — no API key needed

```bash
uv run -m examples.pydantic_ai_adapter
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

- **List and inspect executions** — "show me my recent kitaru runs"
- **Read execution logs** — "what did my last flow print?"
- **Browse artifacts** — "load the output of the research checkpoint"
- **Run flows** — "run examples.basic_flow.first_working_flow:my_agent"
- **Provide wait input** — "approve the pending wait"
- **Replay executions** — "replay my last run from write_draft"
- **Check status** — "what runner am I connected to?"

This is a great way to explore Kitaru interactively — run a few
examples first, then ask your AI assistant questions about what
happened.

### Claude Code skill

If you use Claude Code, Kitaru ships a `kitaru-authoring` skill in
`.claude-plugin/skills/` that teaches Claude how to write flows,
checkpoints, waits, and use the full SDK. Just mention Kitaru
concepts and Claude Code will pick it up automatically.

## Option B: Run the Kitaru server (Docker)

If you want a client-server setup (e.g. to share state across machines
or test the login flow), you can run the server as a Docker container.

### 1. Start the server

```bash
docker run -d --name kitaru-server -p 8080:8080 zenmldocker/kitaru:latest
```

The server starts on port 8080. Give it a few seconds to initialize.

### 2. Connect your local client

```bash
uv run kitaru login http://localhost:8080
```

After login, `uv run kitaru status` should show the server connection.

### 3. Run examples as before

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
| `uv run kitaru status` | Show connection state and active runner |
| `uv run kitaru info` | Detailed environment info |
| `uv run kitaru executions list` | List recent flow executions |
| `uv run kitaru executions get <ID>` | Detailed view of one execution |
| `uv run kitaru executions logs <ID>` | View runtime logs |
| `uv run kitaru executions replay <ID> --from <checkpoint>` | Replay from a checkpoint |
| `uv run kitaru runner list` | List available runners |
| `uv run kitaru runner create <name>` | Create and auto-activate a local runner |
| `uv run kitaru runner delete <name> --recursive --force` | Remove a disposable runner and switch back to default if needed |
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
