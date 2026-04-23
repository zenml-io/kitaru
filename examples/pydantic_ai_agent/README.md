# PydanticAI adapter example

This directory shows how to wrap an existing PydanticAI agent and keep Kitaru's
durability and observability around it — no rewrite needed.

## Getting started

```bash
cd examples/pydantic_ai_agent
uv pip install 'kitaru[local,pydantic-ai]'   # Install Kitaru with local runtime + PydanticAI
kitaru init                                  # Initialize a Kitaru project
```

Then run either example:

```bash
python pydantic_ai_adapter.py       # wrap an agent, keep replay boundary
python pydantic_ai_hitl_input.py    # ask human for structured (free-form) input
```

These examples use your current Kitaru connection context. If you want the run
to use a deployed Kitaru server, connect first with `kitaru login <server>`
and verify with `kitaru status`.

## `pydantic_ai_adapter.py` — Wrap an agent, keep your replay boundary

Wraps a PydanticAI `Agent` with `kp.wrap(agent)`. The outer `@checkpoint`
becomes the replay boundary — if the flow is replayed, the entire agent
call is treated as a single unit. Internal model requests and tool calls
are tracked as child events under that checkpoint, giving you full
observability without changing the agent's control flow.

Uses `TestModel` so no API keys are needed to run it.

## `pydantic_ai_hitl_input.py` — Ask a human for structured input

`kp.hitl_tool(schema=...)` accepts any type, not just `bool`. When the agent
calls a HITL-marked tool, the adapter translates the call into a flow-level
`kitaru.wait(schema=...)`, pauses the execution, and hands the validated value
back to the agent as the tool's return value.

This example defines a `BugReport` Pydantic model and a `collect_bug_report`
HITL tool. Running locally prompts for the report in the terminal; running
against a deployed server pauses the execution and lets you resolve the wait
from another terminal with `kitaru executions input <exec_id> --value '...'`.

For the concept walkthrough, see
[PydanticAI Adapter](https://kitaru.ai/docs/guides/pydantic-ai-adapter).

For the full catalog, see [../README.md](../README.md).
