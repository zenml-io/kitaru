---
description: Wrap a fast-agent app so reachable model and tool calls become Kitaru checkpoints
icon: bolt
---

# fast-agent Adapter

The fast-agent adapter wraps a [fast-agent](https://github.com/evalstate/fast-agent)
app after `FastAgent.run()` yields its `AgentApp`. fast-agent still runs the
agent. It still decides how messages, tool calls, history, and MCP servers work.
Kitaru walks the yielded app, finds the active agents it can reach, and replaces
supported model/tool methods with checkpointed versions.

```python
from kitaru.adapters.fast_agent import KitaruFastAgent

runner = KitaruFastAgent(fast)

async with runner.run() as app:
    reply = await app.send("Summarize this ticket", agent_name="support")
```

This first release is an **experimental preview**. It depends on fast-agent
runtime object shapes such as `AgentApp`, agent mappings, attached LLM objects,
and `agent.call_tool(...)`. If fast-agent changes those shapes, Kitaru may need
an adapter update.

## Install

Use the `fast-agent` extra:

```bash
uv sync --extra fast-agent --no-dev
```

The extra installs the upstream package named `fast-agent-mcp`. The current
known-good range is narrow and currently requires `Python >=3.13.5,<3.14`:

```text
fast-agent-mcp>=0.4.48,<0.4.49
```

Then initialize the project before running examples or replaying saved flows:

```bash
uv run kitaru init
```

## Minimal flow

```python
import asyncio

from kitaru import flow
from kitaru.adapters.fast_agent import KitaruFastAgent

runner = KitaruFastAgent(fast)

@flow
def support_flow(prompt: str) -> str:
    async def run_agent() -> str:
        async with runner.run() as app:
            return await app.send(prompt, agent_name="support")

    return asyncio.run(run_agent())
```

There are three important details in that small example:

1. `fast` is still your fast-agent object. Kitaru does not rebuild it.
2. `runner.run()` enters fast-agent's normal run context and waits for the app.
3. After the app exists, Kitaru wraps the discovered app agents so supported
   calls can become checkpoints while the flow is running.

## What Kitaru records

Kitaru records calls that pass through wrapped methods on discovered app agents:

- LLM `generate(...)`
- LLM `structured(...)` when the runtime LLM object exposes it
- LLM `structured_schema(...)` when the runtime LLM object exposes it
- `agent.call_tool(...)`
- the same supported calls on detached clones returned by
  `spawn_detached_instance(...)`

The concrete story for a normal tool turn is:

```text
app.send("please uppercase kitaru")
→ fast-agent asks the attached LLM for the next assistant message
→ Kitaru records that generate(...) call as a model checkpoint
→ the assistant message asks for the uppercase tool
→ fast-agent reaches agent.call_tool("uppercase", {"text": "kitaru"})
→ Kitaru records that call_tool(...) call as a tool checkpoint
→ fast-agent asks the LLM for the final answer
→ Kitaru records that second generate(...) call too
```

That app-driven tool path is covered by the real fast-agent smoke test for the
pinned runtime. Direct calls such as
`await agent.call_tool("uppercase", {"text": "kitaru"})` are recorded through
the same wrapper.

## Checkpoint strategy

The fast-agent adapter supports one strategy today:

```python
KitaruFastAgent(fast, checkpoint_strategy="calls")
```

`"calls"` means Kitaru creates one checkpoint per supported model or tool call
when the call happens inside a Kitaru flow and outside any existing Kitaru
checkpoint. If the same logical call runs again with caching enabled, Kitaru can
reuse the saved checkpoint result instead of calling the LLM or tool again.

You can pass checkpoint config for adapter-created model and tool checkpoints:

```python
runner = KitaruFastAgent(
    fast,
    model_checkpoint_config={"cache": True, "retries": 1},
    tool_checkpoint_config={"cache": True, "retries": 0},
)
```

These configs accept `cache`, `retries`, `type`, and `runtime="inline"`.
`runtime="isolated"` is rejected for now because the adapter-created checkpoint
body closes over live fast-agent model/tool objects in the current Python
process.

## Usage and cost statistics

When `save_usage=True` (the default), each supported fast-agent model call tries
to log one canonical `llm_usage_v1` record while the model-call checkpoint is
still running. The sequence is:

```text
fast-agent calls LLM.generate(...)
→ Kitaru opens the model-call checkpoint
→ the original LLM method runs
→ Kitaru reads token counts from fast-agent's usage accumulator
→ Kitaru logs llm_usage_v1 on that checkpoint
→ the original model result is returned unchanged
```

The primary source is fast-agent's own usage accumulator. Kitaru records the
turn added during the wrapped model call when the agent or LLM exposes
`usage_accumulator` and fast-agent's `last_turn_usage(...)` shape can read it.
If that is not available, Kitaru falls back to known result fields such as
`usage`, `token_usage`, `usage_metadata`, or `usage_details` when those fields
contain recognizable token counts.

Cost uses the same path as the mature Kitaru adapters: a custom calculator
when you provide one, otherwise `genai-prices` when provider, model, and token
counts are known.

```python
from kitaru.adapters.fast_agent import FastAgentUsageSummary, KitaruFastAgent


def my_fast_agent_cost_calculator(usage: FastAgentUsageSummary) -> float | None:
    if usage.model_name == "memory-fast-agent-demo":
        return 0.0
    return None


runner = KitaruFastAgent(
    fast,
    save_usage=True,
    cost_calculator=my_fast_agent_cost_calculator,
)
```

Kitaru does not guess cost when fast-agent exposes no token counts, or when the
provider/model cannot be priced. In that case the checkpoint still proves the
model call happened, but no empty usage record is created. Tool checkpoints are
not counted as LLM token spend; if a tool triggers a model call through a
wrapped LLM method, that model call gets its own record.

Set `save_usage=False` when you want model/tool checkpointing but do not want
fast-agent token usage metadata persisted. `save_usage` and `cost_calculator`
apply to Kitaru's default call recorder; if you pass a custom `call_recorder`,
that recorder must implement any usage logging it needs.

## Replay identity

For caching and replay, Kitaru builds each checkpoint identity from logical call
inputs:

- agent name
- call kind (`model` or `tool`)
- operation (`generate`, `structured`, `structured_schema`, or `call_tool`)
- call arguments and keyword arguments
- tool name, when available
- model/provider names, when available

This is deliberately strict. Imagine a tool call charges a customer. If Kitaru
used Python object memory addresses as the identity, two equivalent tool-call
objects could look different on every run and never reuse the checkpoint. If it
ignored the tool arguments, two different charges could accidentally reuse the
same result. The adapter therefore accepts primitive, mapping, sequence,
dataclass, Pydantic, and public-attribute inputs, and raises `KitaruUsageError`
when it cannot build a stable identity.

## What Kitaru does and does not do

### Kitaru does

- Enter the wrapped fast-agent run context.
- Wrap agents discovered on the yielded app's `_agents`, `agents`, or
  `active_agents` mapping.
- Record supported model and tool calls as Kitaru checkpoints while a Kitaru
  flow is active.
- Record `llm_usage_v1` metadata for model calls when fast-agent exposes token
  counts through the usage accumulator or a known result shape.
- Estimate model-call cost through `cost_calculator` when provided, otherwise
  `genai-prices` when provider, model, and token counts are known.
- Preserve normal fast-agent behavior by calling the original LLM/tool method
  inside each checkpoint.
- Wrap detached agent clones returned by `spawn_detached_instance(...)`.

### Kitaru does not do

- Replace fast-agent's runtime, history handling, MCP behavior, or app loop.
- Record hidden fast-agent internals that do not pass through wrapped methods.
- Provide a coarse outer app-run checkpoint strategy yet.
- Provide fast-agent streaming checkpoints yet.
- Provide human approval/resume helpers yet.
- Provide sandbox helper tools yet.
- Provide a full capture policy for prompts, responses, transcripts, and event
  artifacts yet. The preview API only has `save_usage` for usage metadata.
- Provide per-tool checkpoint overrides yet.
- Save a rich fast-agent transcript or event timeline yet.
- Promise broad `structured_schema(...)` support across fast-agent versions; the
  wrapper supports it when the runtime object exposes that method.
- Support isolated-runtime execution for adapter-created fast-agent checkpoints
  yet.

## Runnable example

The included example uses real fast-agent objects but no provider key. It builds
a real `ToolAgent`, attaches an in-memory LLM, registers a local `uppercase`
tool, and runs both app-driven and direct tool calls. The in-memory LLM appends a
small local usage turn after each fake model response, so you can inspect
`llm_usage_v1` records without paying a provider.

```bash
uv sync --extra fast-agent --no-dev
uv run kitaru init
uv run python examples/integrations/fast_agent_agent/fast_agent_adapter.py
```

You should see output like:

```text
fast-agent adapter demo summary:
- model_reply: memory reply to hello from fast-agent
- app_tool_reply: memory tool loop complete
- direct_tool_reply: REPLAY

Submitted Kitaru execution: <execution-id>
```

Inspect the execution in Kitaru and look for checkpoints named like:

- `fast_agent_demo_generate_model_call`
- `fast_agent_demo_uppercase_tool_call`

The `fast_agent_demo_generate_model_call` checkpoints should also include
`llm_usage_v1` metadata with provider `memory` and model
`memory-fast-agent-demo`. The example usage numbers are deterministic local word
counts, not provider-billed tokens.

For the broader catalog, see [Examples](../getting-started/examples.md).

## Troubleshooting

- **"requires optional dependency `fast-agent-mcp`"** — install with
  `uv sync --extra fast-agent --no-dev` or `pip install 'kitaru[fast-agent]'`.
- **Python version resolution fails** — the preview extra currently requires
  `Python >=3.13.5,<3.14` because this adapter is pinned to a narrow
  fast-agent range.
- **"Could not discover fast-agent agents"** — Kitaru expected the object yielded
  by `run()` to expose a non-empty `_agents`, `agents`, or `active_agents`
  mapping. Check that you are wrapping the object that yields the app, not an
  already-finished result.
- **No checkpoints appear** — make sure the agent call happens inside a Kitaru
  `@flow` and not inside another `@checkpoint` body. The adapter needs flow
  scope so it can create one checkpoint per supported call.
- **A model checkpoint appears but no usage record appears** — expected when
  fast-agent exposes no token counts for that call. The adapter skips empty
  `llm_usage_v1` records instead of inventing usage.
- **A tool did work but no tool checkpoint appeared** — check whether that tool
  path reached `agent.call_tool(...)`. Kitaru records reachable `call_tool(...)`
  calls; it cannot record hidden fast-agent work that bypasses the wrapped
  method.
- **`runtime="isolated"` is rejected** — expected for this preview. Use inline
  runtime or omit `runtime` in the adapter checkpoint config.

## Related docs

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Replay and overrides</strong></td><td>Re-run a flow with cached outputs for completed checkpoints</td><td><a href="../guides/replay-and-overrides.md">../guides/replay-and-overrides.md</a></td></tr><tr><td><strong>Checkpoints</strong></td><td>How Kitaru records replayable units of work</td><td><a href="../concepts/checkpoints.md">../concepts/checkpoints.md</a></td></tr><tr><td><strong>Examples</strong></td><td>Browse runnable examples, including the provider-free fast-agent demo</td><td><a href="../getting-started/examples.md">../getting-started/examples.md</a></td></tr></tbody></table>
