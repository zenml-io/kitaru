# Coding Agent with Durable Execution (PydanticAI)

A PydanticAI coding agent wrapped in a Kitaru flow. If the agent fails
mid-task, resume from the last checkpoint instead of starting over.

## Flow structure

```
research ──> plan ──> [human approval] ──> implement
   │            │                              │
   └── cached   └── cached                    └── replay from here
```

Three checkpoints at natural phase boundaries with distinct agent roles:

| Checkpoint | Agent | What it does | Tools |
|---|---|---|---|
| `research` | `researcher` | Reads codebase, identifies relevant files and constraints | `read_file`, `list_files`, `search_files` |
| `plan` | `planner` | Creates numbered implementation plan from research output | *(none — works only from supplied analysis)* |
| `implement` | `coder` | Executes the plan, makes code changes, verifies | `read_file`, `write_file`, `edit_file`, `list_files`, `search_files`, `run_command`, `git_diff` |

The planner has **no tools** by design. It consumes the research analysis and
produces a plan without re-reading the codebase, eliminating duplicated I/O.

A `wait()` gate between planning and implementation lets a human review the plan
before any files are modified.

## Setup

```bash
uv sync --extra pydantic-ai
export ANTHROPIC_API_KEY=sk-ant-...
```

## Usage

```bash
# Run the agent (PYTHONPATH=. needed because uv run uses src layout)
PYTHONPATH=. uv run python -m examples.coding_agent.flow --task "Add type hints to utils.py" --cwd /path/to/repo

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
| `ANTHROPIC_API_KEY` | Yes | Claude API key |
| `CODING_AGENT_MODEL` | No | Model override (default: `anthropic:claude-sonnet-4-20250514`) |
| `CODING_AGENT_READ_LIMIT` | No | Max lines per `read_file` call (default: `400`) |
| `CODING_AGENT_MAX_CHARS` | No | Max chars returned per tool output (default: `12000`) |

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
