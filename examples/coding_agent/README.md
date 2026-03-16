# Coding Agent with Durable Execution (PydanticAI)

A PydanticAI coding agent wrapped in a Kitaru flow. If the agent fails
mid-task, resume from the last checkpoint instead of starting over.

## Flow structure

```
load_memory ──> research ──> plan ──> [human approval] ──> implement ──> reflect ──> save_memory
                   │            │                              │
                   └── cached   └── cached                    └── replay from here
```

Six checkpoints at natural phase boundaries with distinct agent roles:

| Checkpoint | Agent | What it does | Tools |
|---|---|---|---|
| `load_memory` | — | Loads persistent memory from the most recent completed execution | *(none)* |
| `research` | `researcher` | Reads codebase, identifies relevant files and constraints | `read_file`, `list_files`, `search_files` |
| `plan` | `planner` | Creates numbered implementation plan from research output | *(none — works only from supplied analysis)* |
| `implement` | `coder` | Executes the plan, makes code changes, verifies | `read_file`, `write_file`, `edit_file`, `list_files`, `search_files`, `run_command`, `git_diff` + skill tools |
| `reflect` | `reflector` | Extracts conventions, decisions, and notes from the completed task | *(none — structured output via `AgentMemory`)* |
| `save_memory` | — | Persists updated memory for future runs | *(none)* |

The planner has **no tools** by design. It consumes the research analysis and
produces a plan without re-reading the codebase, eliminating duplicated I/O.

A `wait()` gate between planning and implementation lets a human review the plan
before any files are modified.

## Setup

```bash
uv sync --extra pydantic-ai

# Register a model alias and store the API key as a kitaru secret
kitaru secrets set anthropic-creds --ANTHROPIC_API_KEY=sk-ant-...
kitaru model register coding-agent --model anthropic/claude-sonnet-4-20250514 --secret anthropic-creds
```

The agent resolves the `coding-agent` alias via `kitaru.adapters.pydantic_ai.resolve_model()`,
which pulls credentials from the linked secret and converts the model identifier
to PydanticAI format automatically. You can also override
with `CODING_AGENT_MODEL` (any alias or raw LiteLLM model identifier) or skip
registration entirely by setting `ANTHROPIC_API_KEY` in the environment.

## Usage

```bash
# Run the agent (PYTHONPATH=. needed because uv run uses src layout)
PYTHONPATH=. uv run python -m examples.coding_agent.flow --task "Add type hints to utils.py" --cwd /path/to/repo

# With explicit skills
PYTHONPATH=. uv run python -m examples.coding_agent.flow --task "Refactor auth module" --skills testing,refactoring

# With an MCP server (URL = SSE, command = stdio)
PYTHONPATH=. uv run python -m examples.coding_agent.flow --task "Fix bug #42" --mcp http://localhost:8080/sse
PYTHONPATH=. uv run python -m examples.coding_agent.flow --task "Fix bug #42" --mcp "npx -y @modelcontextprotocol/server-filesystem /tmp"

# Check execution status
kitaru executions get <exec-id>

# Approve the plan (flow pauses here)
kitaru executions input <exec-id> --wait approve_plan --value true
kitaru executions resume <exec-id>

# If implementation fails, replay from that checkpoint
kitaru executions replay <exec-id> --from implement

# Or replay from plan with an overridden research output
kitaru executions replay <exec-id> --from plan --override 'checkpoint.research=...'
```

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `CODING_AGENT_MODEL` | No | Model alias or LiteLLM identifier (default: `coding-agent` alias) |
| `CODING_AGENT_READ_LIMIT` | No | Max lines per `read_file` call (default: `400`) |
| `CODING_AGENT_MAX_CHARS` | No | Max chars returned per tool output (default: `12000`) |

Credentials are resolved via kitaru's model registry (`kitaru model register ... --secret`).
If no secret is linked, the agent falls back to provider env vars (e.g. `ANTHROPIC_API_KEY`).

## Memory

The agent carries persistent state across runs via `AgentMemory` — a Pydantic model with
`conventions`, `decisions`, and `notes` fields. On each run:

1. `load_memory` loads memory from the most recent completed execution
2. Memory context is injected into the researcher's prompt
3. After implementation, the `reflect` checkpoint asks the LLM to analyze what
   was done and produce an updated `AgentMemory` (conventions observed, decisions
   made, freeform notes)
4. `save_memory` persists the updated memory for next time

Memory starts empty on the first run and accumulates over time. No configuration needed.

## Skills

Skills are folder-based capability packages — a markdown file with prompt guidance,
loaded on demand. Each skill lives under `skills/`:

```
skills/
  testing/
    skill.md        # frontmatter (name, keywords) + prompt body
  docs/
    skill.md
  refactoring/
    skill.md
```

A `skill.md` looks like:

```yaml
---
name: testing
description: Test-writing guidance
keywords: [test, spec, coverage, tdd]
---

Write tests for every change you make.
...
```

| Skill | What it adds | Auto-selected when task mentions |
|---|---|---|
| `testing` | Test-writing guidance | test, spec, coverage, tdd |
| `docs` | Docstring/README guidance | doc, readme, docstring |
| `refactoring` | Safe refactoring patterns | refactor, clean, rename |

Skills are auto-selected via word-boundary keyword matching against the task + research
analysis. Override with `--skills testing,docs` to force specific skills.

**Adding a new skill:** Create a folder under `skills/`, add a `skill.md` with YAML
frontmatter. The loader discovers skills automatically — no registration needed.

Skills can also reference shared tools by name in frontmatter (`tools: [run_command]`).
These are resolved from the `tools.py` registry and added to the coder agent alongside
the prompt. Built-in skills are prompt-only since the coder already has all needed tools.

## MCP servers

Pass one or more MCP server specs with `--mcp` to give the coder access to external tools
(filesystem, GitHub, databases, etc.) via the [Model Context Protocol](https://modelcontextprotocol.io).

```bash
# SSE transport (URLs)
--mcp http://localhost:8080/sse

# Stdio transport (shell commands)
--mcp "npx -y @modelcontextprotocol/server-filesystem /tmp"

# Multiple servers
--mcp http://localhost:8080/sse --mcp "uvx mcp-server-git"
```

URLs (`http://` or `https://`) use SSE transport. Everything else is treated as a shell
command for stdio transport. MCP servers are added as PydanticAI toolsets on the coder
agent only — the researcher and planner are unaffected.

## Latency notes

This example is tuned to reduce latency and persistence overhead:

- **Tool capture is `metadata_only`** for all agents. Tool name, timing, and
  sequence are recorded, but actual file contents and grep results are not
  persisted as artifacts. Change to `{"mode": "full"}` in `utils.py` if you
  need full artifact replay.

- **The planner has no tools.** Earlier versions used the same tool-enabled
  reader agent for both research and planning, which allowed the planner to
  re-read the entire codebase. The planner now receives research output as
  text and reasons over it directly.

- **`read_file` defaults to 400 lines** (down from 2000) and tool output is
  capped at 12,000 chars (down from 30,000). Both are env-configurable.

- **`git_diff` is available to the coder** as a cheaper alternative to
  re-reading entire files after making edits.

- **Research context is passed to the coder** alongside the plan, reducing
  the need for the coder to rediscover file locations via search.

## Future: parallel sub-agents

The current implementation runs all three phases sequentially on a single machine.
When Kitaru gains support for running checkpoints as separate pods, this example
could scale to distributed execution with minimal changes:

- **Shared filesystem:** Today tools read/write the local `cwd`. With separate pods,
  each sub-agent needs access to the same working directory via a PVC
  (PersistentVolumeClaim) or network filesystem (NFS/EFS). The `cwd` deps value
  would point to this shared mount.

- **Checkpoint outputs carry context forward:** Analysis and plan are already passed
  as checkpoint return values (strings), serialized to the artifact store by Kitaru.
  This works across pods. However, file changes made by `implement` live on the
  filesystem, not in a return value — the shared mount is what makes them visible.

- **Tool path resolution:** `_resolve(cwd, path)` already uses absolute paths.
  No changes needed as long as the mount path matches `cwd`.

- **`.submit()` for parallel research:** A future version could `.submit()` multiple
  research checkpoints in parallel (e.g., one per subdirectory). Each sub-agent pod
  would need the shared mount to read files.

- **No code changes needed now:** The path to distributed is: (1) add a shared PVC
  to the Kitaru stack, (2) pass the mount path as `cwd`, (3) optionally split
  research into parallel `.submit()` calls.
