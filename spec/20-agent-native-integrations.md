# 20. Agent-Native Integrations

Kitaru is a runtime for AI agents. The agents that use Kitaru are themselves often built with — or orchestrated by — AI coding assistants (Claude Code, Cursor, Windsurf, etc.). This creates two distinct integration surfaces:

1. **MCP server** — lets AI assistants manage Kitaru executions as tool calls (list executions, provide wait input, trigger replay, browse artifacts)
2. **Claude Code skill** — teaches Claude Code how to author Kitaru flows, checkpoints, and adapters correctly

Both are shipped with the `kitaru` package and require no separate installation beyond the relevant extras.

---

## MCP Server

### What it is

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) server that exposes Kitaru's client API as tools that AI assistants can call. This lets an agent — or a human using an AI assistant — interact with running Kitaru executions conversationally.

Example interaction:

> **User:** "What Kitaru executions are waiting for input?"
>
> **Assistant:** *(calls `kitaru_executions_list` with `status="waiting"`)* "There's one: `kr-a8f3c2` (content_pipeline) is waiting at `approve_draft`. The question says 'Publish this draft about AI safety?'"
>
> **User:** "Approve it."
>
> **Assistant:** *(calls `kitaru_executions_input` with `exec_id="kr-a8f3c2"`, `wait="approve_draft"`, `value=true`)* "Done. The execution has resumed."

### Why it matters

Without the MCP server, an AI assistant would need to shell out to the `kitaru` CLI and parse text output. The MCP server gives structured input/output, richer context, and a better experience for both the assistant and the user.

It also unlocks **agent-to-agent orchestration**: a higher-level agent can use MCP tools to manage Kitaru executions as part of its own workflow — starting flows, waiting for results, providing input to waits, and triggering replays.

### Packaging

The MCP server lives at `src/kitaru/mcp/` as a subpackage of the main `kitaru` package. It is gated behind an optional dependency group:

```bash
# Install with MCP server support
pip install kitaru[mcp]
uv add kitaru --extra mcp
```

The `pyproject.toml` changes:

```toml
[project.optional-dependencies]
mcp = [
    "mcp>=1.26.0",
]
```

The MCP server entry point is registered as a console script:

```toml
[project.scripts]
kitaru = "kitaru.cli:cli"
kitaru-mcp = "kitaru.mcp:main"
```

This lets users run `kitaru-mcp` directly or configure it as an MCP server in their assistant's config (e.g. Claude Code's `claude_desktop_config.json` or `.mcp.json`).

### Lazy imports

The `kitaru.mcp` subpackage must not be imported by the rest of the `kitaru` package. Users who install without the `[mcp]` extra should never hit an `ImportError` from MCP dependencies.

The guard pattern:

```python
# src/kitaru/mcp/__init__.py
"""Kitaru MCP server — requires kitaru[mcp] extras."""

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    raise ImportError(
        "MCP server dependencies not installed. "
        "Install with: pip install kitaru[mcp]"
    ) from None
```

No other module in `src/kitaru/` should import from `kitaru.mcp`.

### File layout

```
src/kitaru/mcp/
    __init__.py          # lazy import guard + FastMCP app creation
    server.py            # tool definitions
    _resources.py        # MCP resource definitions (optional, future)
```

### Tools exposed

The MCP server should expose tools that mirror the `KitaruClient` API (section 13) and key CLI operations (section 14). Each tool maps to a client method.

#### Execution management

| MCP tool | Maps to | Description |
| --- | --- | --- |
| `kitaru_executions_list` | `client.executions.list(...)` | List executions with optional filters (status, flow, stack) |
| `kitaru_executions_get` | `client.executions.get(exec_id)` | Get detailed execution info (status, checkpoints, pending wait) |
| `kitaru_executions_latest` | `client.executions.latest(...)` | Get the most recent execution matching filters |
| `kitaru_executions_run` | flow invocation | Start a new flow execution by module path |
| `kitaru_executions_cancel` | `client.executions.cancel(exec_id)` | Cancel a running or waiting execution |

#### Resume, retry, replay

| MCP tool | Maps to | Description |
| --- | --- | --- |
| `kitaru_executions_input` | `client.executions.input(...)` | Provide input to a waiting execution (resume) |
| `kitaru_executions_retry` | `client.executions.retry(exec_id)` | Retry a failed execution (same execution) |
| `kitaru_executions_replay` | `client.executions.replay(...)` | Create a new execution from a previous one, optionally with overrides |

#### Artifacts

| MCP tool | Maps to | Description |
| --- | --- | --- |
| `kitaru_artifacts_list` | `client.artifacts.list(exec_id)` | List artifacts for an execution |
| `kitaru_artifacts_get` | `client.artifacts.get(artifact_id)` | Get artifact metadata and value |

#### Connection and status

| MCP tool | Maps to | Description |
| --- | --- | --- |
| `kitaru_status` | `kitaru status` CLI | Show current connection, stack, and project context |
| `kitaru_stacks_list` | `kitaru stack list` CLI | List available stacks |

### Tool design principles

- **Structured input and output.** Tools accept typed parameters and return structured JSON, not CLI text. This is the whole point of MCP over shell commands.
- **Mirror the client API.** Tool names and parameters should map obviously to `KitaruClient` methods. Do not invent a parallel vocabulary.
- **Return actionable context.** When listing executions, include enough detail (status, flow name, pending wait info) that the assistant can act without a follow-up `get` call. When a wait is pending, include the question and schema so the assistant can ask the user for input.
- **Validate before acting.** Resume input should be validated against the wait schema before being sent. Return clear validation errors.
- **No destructive defaults.** Tools that modify state (input, retry, replay, cancel) should require explicit parameters — no "cancel the most recent execution" shortcuts.

### Example tool implementation

```python
# src/kitaru/mcp/server.py
from mcp.server.fastmcp import FastMCP

from kitaru import KitaruClient

mcp = FastMCP(
    "kitaru",
    description="Manage Kitaru durable agent executions",
)


@mcp.tool()
def kitaru_executions_list(
    status: str | None = None,
    flow: str | None = None,
) -> list[dict]:
    """List Kitaru executions with optional filters.

    Args:
        status: Filter by status (running, waiting, completed, failed, cancelled).
        flow: Filter by flow name.
    """
    client = KitaruClient()
    execs = client.executions.list(status=status, flow=flow)
    return [_serialize_execution(ex) for ex in execs]


@mcp.tool()
def kitaru_executions_input(
    exec_id: str,
    wait: str,
    value: str | bool | int | float | dict | list,
) -> dict:
    """Provide input to a waiting Kitaru execution (resume).

    This continues the same execution — it does not create a new one.

    Args:
        exec_id: The execution ID (e.g. 'kr-a8f3c2').
        wait: The wait name to provide input for.
        value: The input value. Must match the wait's schema.
    """
    client = KitaruClient()
    client.executions.input(exec_id, wait=wait, value=value)
    ex = client.executions.get(exec_id)
    return _serialize_execution(ex)
```

### MCP resources (future)

MCP resources provide read-only context that assistants can pull into their context window. Future versions may expose:

- `kitaru://executions/{exec_id}` — full execution details as a resource
- `kitaru://artifacts/{artifact_id}` — artifact content as a resource
- `kitaru://flows` — list of registered flows

Resources are not in the MVP. Tools are sufficient for the initial launch.

### Running the MCP server

The server can be started directly:

```bash
kitaru-mcp
```

Or configured in an assistant's MCP config. For Claude Code (`.mcp.json`):

```json
{
  "mcpServers": {
    "kitaru": {
      "command": "kitaru-mcp",
      "args": []
    }
  }
}
```

The server uses stdio transport by default (the standard for local MCP servers).

### Authentication

The MCP server uses the same connection settings as the CLI and SDK. If the user has run `kitaru login`, the MCP server picks up the stored auth state. No separate auth flow is needed.

For remote/connected mode, the MCP server reads the same config file that `kitaru login` writes to.

---

## Claude Code Skill

### What it is

A Claude Code [skill](https://docs.anthropic.com/en/docs/claude-code/skills) — a markdown file that teaches Claude Code how to use Kitaru when writing agent code. Skills are the mechanism Anthropic provides for giving Claude Code domain-specific knowledge about libraries and frameworks.

The skill file lives in the Kitaru repository and is published as part of the package so that it can be referenced by users or installed into their `.claude/skills/` directory.

### Why it matters

Without a skill, Claude Code would need to guess at Kitaru's API from docstrings and type hints alone. The skill provides:

- correct decorator usage (`@flow`, `@checkpoint`)
- the right patterns for replay, wait, and artifact overrides
- awareness of MVP restrictions (no nested checkpoints, no wait inside checkpoints)
- PydanticAI adapter usage
- common pitfalls and how to avoid them

### File location

```
src/kitaru/skills/
    kitaru-authoring.md     # how to write Kitaru flows and checkpoints
```

The skill is also referenced from the docs site so users know it exists.

### Skill content

The skill should cover:

#### Core patterns

- `@flow` as the outer durable boundary
- `@checkpoint` for replayable work units
- `kitaru.wait()` for suspension (flow-level only, never inside a checkpoint)
- `kitaru.log()` for metadata
- `kitaru.save()` / `kitaru.load()` for explicit artifacts
- `kitaru.llm()` for tracked LLM calls
- `.submit()` + `.result()` for concurrency

#### Rules the skill must enforce

- flows cannot nest (no `@flow` inside another flow)
- `wait()` is only valid directly inside a flow body, not inside a checkpoint
- checkpoint return values must be serializable (Pydantic models or JSON-compatible types)
- no nested checkpoint-within-checkpoint semantics
- adapters must not bypass these restrictions

#### PydanticAI adapter

- `kp.wrap(agent)` pattern
- placing wrapped agent calls inside an explicit `@checkpoint`
- child events vs replay boundaries

#### Configuration and connection

- `kitaru.configure()` for project defaults
- stack selection
- `kitaru login` for connected mode

#### Common mistakes

- putting `wait()` inside a checkpoint (fails at runtime)
- returning non-serializable values from checkpoints
- nesting flows
- forgetting to use `@checkpoint` around meaningful work (losing replayability)

### How users install the skill

Users can reference the skill from their project's `.claude/skills/` directory or from their global Claude Code config. The docs should explain both paths:

```bash
# Option 1: Copy into project
mkdir -p .claude/skills
cp $(python -c "import kitaru.skills; print(kitaru.skills.__path__[0])")/kitaru-authoring.md .claude/skills/

# Option 2: Reference in CLAUDE.md
# Add to your project's CLAUDE.md:
# See kitaru-authoring skill at src/kitaru/skills/kitaru-authoring.md
```

---

## Repository changes required

### `pyproject.toml`

```toml
[project.optional-dependencies]
mcp = [
    "mcp>=1.26.0",
]

[project.scripts]
kitaru = "kitaru.cli:cli"
kitaru-mcp = "kitaru.mcp:main"
```

The skill files need to be included in the package distribution:

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/kitaru"]

[tool.hatch.build.targets.wheel.force-include]
"src/kitaru/skills" = "kitaru/skills"
```

### New files

```
src/kitaru/mcp/
    __init__.py              # lazy import guard, FastMCP app, main() entry point
    server.py                # tool definitions

src/kitaru/skills/
    __init__.py              # empty, makes it a package for path resolution
    kitaru-authoring.md      # Claude Code skill file

tests/mcp/
    __init__.py
    test_server.py           # MCP server tool tests
    conftest.py              # shared fixtures (mock KitaruClient, etc.)
```

### CI updates

The CI workflow (`.github/workflows/ci.yml`) should:

- install the `[mcp]` extra in an additional test matrix entry (or a separate job)
- run `tests/mcp/` only when the extra is installed
- the main test suite should pass without the `[mcp]` extra (verifying the lazy import guard works)

### Documentation

A new docs page should be added:

```
docs/content/docs/agent-integrations/
    meta.json                  # section metadata
    mcp-server.mdx             # MCP server setup, tool reference, example workflows
    claude-code-skill.mdx      # skill installation and usage
```

The docs should cover:

- **MCP server page:** installation (`pip install kitaru[mcp]`), running the server, configuring it in Claude Code / Cursor / other assistants, full tool reference with examples, authentication
- **Claude Code skill page:** what skills are, how to install the Kitaru skill, what it teaches Claude Code, example prompts that work well with the skill

The docs landing page (`docs/content/docs/index.mdx`) and the getting-started section should mention the agent-native integrations as a feature.

---

## Testing

### MCP server tests

MCP server tests should verify:

- **Tool registration:** all expected tools are registered with correct names and parameter schemas
- **Tool execution:** each tool calls the right `KitaruClient` method with the right arguments (mock the client)
- **Serialization:** tool outputs are JSON-serializable
- **Error handling:** tools return meaningful errors for invalid exec IDs, invalid wait names, schema validation failures
- **Lazy import guard:** importing `kitaru` without the `[mcp]` extra does not fail; importing `kitaru.mcp` without the extra raises `ImportError` with a clear message

Example test structure:

```python
# tests/mcp/test_server.py
import pytest


def test_mcp_import_guard_without_extra(monkeypatch):
    """Importing kitaru.mcp without mcp extra gives a clear error."""
    import importlib
    import sys

    # Remove mcp from available modules to simulate missing extra
    monkeypatch.setitem(sys.modules, "mcp", None)
    monkeypatch.setitem(sys.modules, "mcp.server", None)
    monkeypatch.setitem(sys.modules, "mcp.server.fastmcp", None)

    # Force reimport
    if "kitaru.mcp" in sys.modules:
        del sys.modules["kitaru.mcp"]

    with pytest.raises(ImportError, match="pip install kitaru\\[mcp\\]"):
        importlib.import_module("kitaru.mcp")


def test_executions_list_tool(mock_client):
    """kitaru_executions_list calls client.executions.list with filters."""
    from kitaru.mcp.server import kitaru_executions_list

    result = kitaru_executions_list(status="waiting")
    mock_client.executions.list.assert_called_once_with(
        status="waiting", flow=None
    )
    assert isinstance(result, list)


def test_executions_input_tool(mock_client):
    """kitaru_executions_input provides wait input and returns updated state."""
    from kitaru.mcp.server import kitaru_executions_input

    result = kitaru_executions_input(
        exec_id="kr-a8f3c2",
        wait="approve_draft",
        value=True,
    )
    mock_client.executions.input.assert_called_once_with(
        "kr-a8f3c2", wait="approve_draft", value=True
    )
```

### Skill tests

The skill file is markdown, not code, so traditional unit tests do not apply. However:

- a test should verify the skill file exists at the expected path in the installed package
- a test should verify the skill file contains key patterns (`@flow`, `@checkpoint`, `kitaru.wait()`) so it does not accidentally become stale

```python
# tests/test_skill_files.py
from pathlib import Path

import kitaru.skills


def test_skill_file_exists():
    skills_dir = Path(kitaru.skills.__path__[0])
    skill_file = skills_dir / "kitaru-authoring.md"
    assert skill_file.exists()


def test_skill_file_covers_core_primitives():
    skills_dir = Path(kitaru.skills.__path__[0])
    content = (skills_dir / "kitaru-authoring.md").read_text()
    for pattern in ["@flow", "@checkpoint", "kitaru.wait()", "kitaru.log()"]:
        assert pattern in content, f"Skill file missing coverage of {pattern}"
```

---

## MVP scope

For the March launch:

- **MCP server:** ship with the core execution management tools (list, get, input, retry, replay, cancel, artifacts list/get, status). No MCP resources yet.
- **Claude Code skill:** ship the authoring skill covering core primitives, PydanticAI adapter, and common pitfalls.
- **Docs:** one page for MCP server setup + tool reference, one page for skill installation.
- **Tests:** import guard test, mock-based tool tests, skill file existence test.

### What is NOT in the MVP

- MCP resources (read-only context URIs)
- MCP server authentication beyond reusing `kitaru login` state
- Skills for other assistants (Cursor rules, Windsurf rules) — these can follow the same pattern later
- MCP prompts (predefined prompt templates)
- SSE or streamable HTTP transport for the MCP server (stdio is sufficient for local use)

---

## Relationship to other spec sections

- **Section 13 (Client API)** defines the `KitaruClient` interface that the MCP server wraps
- **Section 14 (CLI Reference)** defines the CLI commands that the MCP tools parallel
- **Section 15 (Observability)** — OTel-native tracing is not MVP; for now the MCP server relies on the global log store and `kitaru.log()` metadata
- **Section 16 (Framework Adapters)** — the skill teaches correct adapter usage patterns
- **Section 19 (Implementation Guide)** — the MCP server and skill are additional deliverables to add to the deliverables table
