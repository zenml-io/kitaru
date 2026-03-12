# Coding Agent with Durable Execution (PydanticAI)

A PydanticAI coding agent wrapped in a Kitaru flow. If the agent fails
mid-task, resume from the last checkpoint instead of starting over.

## Flow structure

```
research ──> plan ──> [human approval] ──> implement
   │            │                              │
   └── cached   └── cached                    └── replay from here
```

Three checkpoints at natural phase boundaries:

| Checkpoint | What it does | Tools |
|---|---|---|
| `research` | Reads codebase, identifies relevant files and constraints | `read_file`, `list_files`, `search_files`, `run_command` |
| `plan` | Creates numbered implementation plan | `read_file`, `list_files`, `search_files`, `run_command` |
| `implement` | Executes the plan, makes code changes, verifies | `read_file`, `write_file`, `edit_file`, `list_files`, `search_files`, `run_command` |

A `wait()` gate between planning and implementation lets a human review the plan
before any files are modified.

## Setup

```bash
uv sync --extra pydantic-ai
export ANTHROPIC_API_KEY=sk-ant-...
```

The `pydantic-ai` extra pins `anthropic<0.80` because pydantic-ai-slim 1.60 is
incompatible with anthropic 0.80+ (UserLocation was renamed to BetaUserLocationParam).

## Usage

```bash
# Run the agent
uv run python -m examples.coding-agent.flow --task "Add type hints to utils.py" --cwd /path/to/repo

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
