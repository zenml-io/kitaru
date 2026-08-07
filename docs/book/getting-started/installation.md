---
description: Install Kitaru with uv or pip
icon: download
---

# Installation

Install Kitaru to run, replay, and improve agents from your own environment. One package gives you the SDK, the CLI, and an optional local server and UI, all running on the same stacks and dashboard as ZenML pipelines.

Kitaru requires **Python 3.11 or newer**.

You can verify your interpreter with:

```bash
python --version
```

{% tabs %}
{% tab title="uv (recommended)" %}
```bash
uv add kitaru
```
{% endtab %}

{% tab title="pip" %}
```bash
pip install kitaru
```
{% endtab %}
{% endtabs %}

This gives you the full SDK, CLI, and everything you need to run flows locally.

## Optional extras

| Extra | What it adds |
|---|---|
| `local` | Local server and UI for browsing executions in a local web UI |
| `mcp` | MCP server for querying executions from AI assistants |
| `modal` | Python dependencies needed to create and validate Modal-backed stacks |
| `pydantic-ai` | PydanticAI adapter for wrapping agents in checkpoints |
| `openai` | OpenAI SDK for `kitaru.llm()` calls to OpenAI models |
| `anthropic` | Anthropic SDK for `kitaru.llm()` calls to Claude models |
| `llm` | Both `openai` and `anthropic` provider packages in one install |

```bash
uv add "kitaru[mcp,pydantic-ai,local]"
# or: pip install "kitaru[mcp,pydantic-ai,local]"

# Modal stacks: uv add "kitaru[modal]"
# or: pip install "kitaru[modal]"

# Provider extras: uv add "kitaru[openai]"
# or: pip install "kitaru[openai]"
```

The `modal` extra does not create Modal tokens, Docker registry logins, cloud
credentials, buckets, or registries. It only installs the Python packages Kitaru
needs to validate and create Modal stack components.

If you use Claude Code or another MCP-capable assistant, install
`kitaru[mcp]` so your assistant can query executions, inspect logs and
artifacts, provide input to waiting runs, and drive replays and diffs through
structured tool calls. This is what lets a coding agent hill-climb on your
runs. See [MCP Server](../agent-native/mcp-server.md) for setup.

## Verify Installation

{% tabs %}
{% tab title="uv project" %}
```bash
uv run kitaru --version
uv run kitaru --help
```
{% endtab %}

{% tab title="pip environment" %}
```bash
kitaru --version
kitaru --help
```
{% endtab %}
{% endtabs %}

## Local UI

Install Docker with the Compose v2 plugin, then provision a local Kitaru server and PostgreSQL database:

```bash
kitaru login --local
```

The CLI runs the version-matched `zenmldocker/kitaru-server` image, waits for `http://localhost:8000` to become healthy, selects it, and opens the dashboard. On Apple Silicon, Docker runs the amd64 server image through emulation. If Docker is unavailable, install it from [Docker's installation guide](https://docs.docker.com/get-docker/) or sign up for [Kitaru Cloud](https://cloud.zenml.io/).

The database persists when you stop the deployment:

```bash
kitaru local logs
kitaru logout
```

Delete the local PostgreSQL data only when you want a clean reset:

```bash
kitaru logout --volumes
```

After installing a newer Kitaru CLI, upgrade the local server explicitly:

```bash
kitaru login --local --upgrade
```

Developers using an unpublished server image can set `KITARU_LOCAL_IMAGE` to the name of an image that already exists in the local Docker daemon.

## Next Steps

Head to the [Quickstart](quickstart.md) to explore what's
available, see [Execution management](../guides/execution-management.md)
for lifecycle operations, or open [MCP Server](../agent-native/mcp-server.md)
for assistant-native querying.
