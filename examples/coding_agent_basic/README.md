# Basic Coding Agent (No Framework)

A minimal interactive coding agent built with **only kitaru primitives and LiteLLM** — no PydanticAI, no LangChain, no agent framework.

Demonstrates:

- **LiteLLM tool calling** — manual tool-call loop with dispatch
- **`kitaru.wait()`** — durable human-in-the-loop command loop
- **Planner + implementer** — two-role separation with different tool sets
- **Dynamic checkpoint IDs** — `id=f"plan_{i}"` for per-iteration replay targeting

## Setup

Register a model alias (one-time):

```bash
kitaru secrets set anthropic-creds --ANTHROPIC_API_KEY=sk-ant-...
kitaru model register coding-agent --model anthropic/claude-sonnet-4-20250514 --secret anthropic-creds
```

## Usage

### Terminal 1: start the agent

```bash
uv run python -m examples.coding_agent_basic.flow --cwd /path/to/repo
```

### Terminal 2: send commands

Each `step_N` wait accepts a free-form command:

```bash
# Send a task → agent plans
kitaru executions input <eid> --wait step_0 --value "Add type hints to utils.py"
kitaru executions resume <eid>

# Review the plan, then tell it to implement
kitaru executions input <eid> --wait step_1 --value "implement"
kitaru executions resume <eid>

# Or send another task to re-plan instead
kitaru executions input <eid> --wait step_1 --value "Actually, refactor auth.py first"
kitaru executions resume <eid>

# Done
kitaru executions input <eid> --wait step_N --value "quit"
kitaru executions resume <eid>
```

### Replay

```bash
kitaru executions replay <eid> --from implement_0
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `CODING_AGENT_MODEL` | `coding-agent` | Model alias or LiteLLM identifier |
| `CODING_AGENT_MAX_TOOL_ROUNDS` | `30` | Max tool-calling rounds per checkpoint |
