# PydanticAI adapter example

This directory shows how to wrap an existing PydanticAI agent and keep Kitaru's
durability and observability around it — no rewrite needed.

## Getting started

```bash
cd examples/integrations/pydantic_ai_agent
uv sync --extra local --extra pydantic-ai --extra openai
uv run kitaru init
```

Then run the no-key adapter example:

```bash
uv run python pydantic_ai_adapter.py       # wrap an agent, keep replay boundary
```

To let a real provider call the active-stack sandbox command tool, set provider
credentials and run:

```bash
export OPENAI_API_KEY=sk-...
uv run python pydantic_ai_sandbox_toolset.py
```

To watch real PydanticAI stream events, set provider credentials and run the
streaming example:

```bash
export OPENAI_API_KEY=sk-...
uv run python pydantic_ai_streaming.py
```

You can override the streaming or sandbox-toolset model with `PYDANTIC_AI_MODEL`; both default to
`openai:gpt-5-nano`.

These examples use your current Kitaru connection context. If you want the run
to use a deployed Kitaru server, connect first with `kitaru login <server>`
and verify with `kitaru status`.

## `pydantic_ai_adapter.py` — Wrap an agent, keep your replay boundary

Wraps a PydanticAI `Agent` with `KitaruAgent(agent, ...)`. The outer
`@checkpoint` becomes the replay boundary — if the flow is replayed, the
entire agent call is treated as a single unit. Internal model requests
and tool calls are tracked as child events under that checkpoint, giving
you full observability without changing the agent's control flow.

Uses `TestModel` so no API keys are needed to run it.

## `pydantic_ai_sandbox_toolset.py` — Run a command in the active stack sandbox

Attaches `sandbox_command_toolset(...)` to a PydanticAI agent and wraps that
agent with `KitaruAgent(checkpoint_strategy="calls")`. The model is asked to call
`run_sandbox_command` for `python --version`. The agent runs directly in the flow
body, so the dashboard can show the model request checkpoint and the
`run_sandbox_command_tool` checkpoint separately. The example then passes the
answer into a tiny `publish_sandbox_answer` checkpoint for UI/CLI inspection.
It does not call `.wait()` for the final answer, because the visible
model/tool checkpoints are also terminal graph steps in this demo shape.
Instead, the script polls execution status and prints the execution ID to open
in the UI or inspect with `kitaru executions get`.

The example uses the adapter's 20,000-character output limit and disables cache
for the sandbox command checkpoint. Your active stack must have exactly one
sandbox component. Check the active stack with `uv run kitaru stack current`,
then inspect it with `uv run kitaru stack show <name>`. If it has no sandbox
component, create a sandbox-enabled local stack with:

```bash
uv run kitaru stack create sandbox-demo --sandbox local
```

If provider credentials or sandbox support are missing, the example prints a
short setup message instead of a long provider/backend traceback.

Safety note: the model controls the shell command and optional working directory.
Anything visible to the sandbox process, including files, environment variables,
network access, and credentials, can be printed to stdout/stderr and returned to
the model. The local sandbox is a development convenience, not a security
boundary; use an isolated sandbox provider and minimal credentials for untrusted
models or prompts.

## `pydantic_ai_streaming.py` — Watch live PydanticAI events

Runs a small customer-support agent against a real provider and watches
`pydantic_ai.stream.*` events with `KitaruClient().executions.events(...)` while
the flow is running. The live events are best-effort progress updates; the final
answer printed after `.wait()` is the durable Kitaru result.

This example requires `OPENAI_API_KEY` because it uses a real OpenAI-backed
PydanticAI model. Set `PYDANTIC_AI_MODEL` to choose a different PydanticAI model
label.

For the concept walkthrough, see
[PydanticAI Adapter](https://docs.zenml.io/kitaru/adapters/pydantic-ai/).

For the full catalog, see [../../README.md](../../README.md).
