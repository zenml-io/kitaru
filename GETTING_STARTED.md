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

# Install the core package
uv sync

# Add optional PydanticAI and native MCP support when needed
uv sync --extra pydantic-ai --extra mcp

# Or with pip (editable install)
pip install -e ".[pydantic-ai,mcp]"

```

The extras give you:

| Extra | What it enables |
|---|---|
| `mcp` | Read-only-by-default v2 MCP server (`kitaru-mcp`) for registry and activity reads, with explicit standard and destructive modes |
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
uv run examples/features/basic_flow/first_working_flow.py
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
uv run examples/features/basic_flow/flow_with_logging.py
```

Logs structured metadata at both flow and checkpoint scope. After
running, you can separately inspect runtime log lines with
`uv run kitaru executions logs <ID>`.

#### Artifact save/load (`kitaru.save()` / `kitaru.load()`)

```bash
uv run examples/features/basic_flow/flow_with_artifacts.py
```

Demonstrates persisting and loading named artifacts across executions.
The script runs two flows — the second one loads artifacts from the
first.

#### Wait for human input (`kitaru.wait()`)

```bash
uv run examples/features/execution_management/wait_and_resume.py
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
uv run examples/features/replay/replay_with_overrides.py
```

Runs a flow, then replays it from a specific checkpoint with an
overridden input — demonstrating Kitaru's replay/time-travel
capability.

#### Tracked LLM calls (`kitaru.llm()`) — requires API key

```bash
# Register a model alias and set your key
uv run kitaru model register fast --model openai/gpt-5-nano
export OPENAI_API_KEY=sk-...

uv run examples/features/llm/flow_with_llm.py
```

Makes tracked LLM calls with token/cost metadata logged automatically.

#### PydanticAI adapter — no API key needed

```bash
uv run examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py
```

Wraps a PydanticAI agent with Kitaru tracking. Uses `TestModel` so no
API keys are required.

## Use with Claude Code, Codex, or Cursor (MCP)

Installing the `mcp` extra adds the local stdio command `kitaru-mcp`. It reuses the active Kitaru CLI context and starts in read-only mode, where the client sees only two tools for bounded registry and activity reads.

Add it to Claude Code:

```bash
claude mcp add --scope project kitaru -- /absolute/path/to/.venv/bin/kitaru-mcp
```

Codex uses `~/.codex/config.toml`:

```toml
[mcp_servers.kitaru]
command = "/absolute/path/to/.venv/bin/kitaru-mcp"
args = []
```

Cursor uses `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "kitaru": {
      "command": "/absolute/path/to/.venv/bin/kitaru-mcp",
      "args": []
    }
  }
}
```

Read-only mode can inspect agents, cohorts, experiments, importers, evaluators, versions, sessions, replays, evaluations, experiment runs, jobs, and bounded child pages. Standard mode adds cohort and experiment management plus immediate asynchronous workflow starts; destructive mode adds exact-ID cancellation and deletion. Read the [MCP server guide](https://docs.zenml.io/kitaru/agent-native/mcp-server) before enabling either broader mode.

### Claude Code skills

If you use Claude Code, install the Kitaru skills plugin:

```bash
/plugin marketplace add zenml-io/kitaru-skills
/plugin install kitaru@kitaru
```

This adds scoping and authoring skills that teach Claude how to design and
build durable workflows. See the [Claude Code Skills docs](https://docs.zenml.io/kitaru/agent-native/claude-code-skill) for details.

## Option B: Run the Kitaru server (Docker)

If you want a client-server setup (e.g. to share state across machines
or test the login flow), you can run the server as a Docker container.

The production server image is based on the official `zenmldocker/zenml-server`
image with Kitaru and the Kitaru UI layered on top.

### 1. Build the server image locally

```bash
just DOCKER_REPO=kitaru-local DOCKER_TAG=dev server-image
```

This creates a local image tag called `kitaru-local:dev`.

The `server-image` recipe first bundles a stable Kitaru UI release into the
Kitaru package tree, then Docker copies that packaged UI into the server image.
Docker no longer downloads UI release assets itself.

If you do not have `just` installed, run the two steps explicitly:

```bash
bash scripts/download-ui.sh
docker build -f docker/Dockerfile --target server -t kitaru-local:dev .
```

To test a specific stable UI release in the release-like image path:

```bash
just UI_TAG=kitaru-ui-v0.2.0 DOCKER_REPO=kitaru-local DOCKER_TAG=dev server-image
```

For local UI development with an unarchived frontend build, use the dev server
image instead:

```bash
# Build kitaru-ui first, then copy dist/ into the build context:
cp -r /path/to/zenml-frontend-monorepo/apps/kitaru-ui/dist/ docker/kitaru-ui-dist/
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
  does not contain the bundled dashboard assets. Rebuild from source after
  running `bash scripts/download-ui.sh`, or switch to a newer published tag.
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

## Maintainer UI testing

Most users should not need this section. It is for Kitaru/backend maintainers
and frontend maintainers validating a Kitaru UI bundle before a release.

The key safety rule: official Kitaru builds only bundle stable/full UI releases
from `zenml-io/zenml-frontend-monorepo`. Prerelease UI testing is explicit and
local; it does not publish a Kitaru package or image.

```bash
# Download the latest stable/full UI bundle for local testing
just ui-bundle

# Download a specific stable UI bundle
just UI_TAG=kitaru-ui-v0.2.0 ui-bundle

# Download a prerelease UI bundle, explicitly opting into prereleases
just UI_TAG=kitaru-ui-v0.3.0-rc.1 ui-bundle-prerelease

# Start local Kitaru with the prepared bundle
just ui-login

# Run the smoke test with the prepared bundle and keep the server running
just ui-smoke
```

You can also bypass `just` and point Kitaru at any built UI `dist/` directory:

```bash
KITARU_UI_DIST_PATH=/absolute/path/to/dist uv run kitaru login
KITARU_UI_DIST_PATH=/absolute/path/to/dist ./scripts/smoke-test.sh --keep-server
```

If a local server is already running, restart it before changing
`KITARU_UI_DIST_PATH`:

```bash
uv run kitaru logout
KITARU_UI_DIST_PATH=/absolute/path/to/dist uv run kitaru login
```

For the full internal runbook, see `FRONTEND-TESTING.md`.

## Useful CLI commands

| Command | What it does |
|---|---|
| `uv run kitaru init` | Initialize a Kitaru project (creates `.kitaru/`) |
| `uv run kitaru status` | Show connection state and active stack |
| `uv run kitaru info` | Detailed environment info |
| `uv run kitaru executions list` | List recent flow executions |
| `uv run kitaru executions get <ID>` | Detailed view of one execution |
| `uv run kitaru executions logs <ID>` | View runtime logs |
| `uv run kitaru executions replay <ID> --at <checkpoint>` | Replay from a checkpoint |
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
