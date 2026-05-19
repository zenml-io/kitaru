# All adapters showcase

One customer-support scenario, four agent frameworks, one Kitaru durable-execution
layer. The point of this example is to show that the `@flow` / `@checkpoint`
boundary is identical no matter which agent harness a team picks — only the
inner agent changes.

## What it runs

A single business task: *"Where is order ORD-1007? Look it up, apply the
shipping policy, tell me what happens next."*

The same task is executed through every adapter Kitaru ships today:

| # | Adapter | Module |
|---|---|---|
| 1 | PydanticAI | `kitaru.adapters.pydantic_ai.KitaruAgent` |
| 2 | OpenAI Agents SDK | `kitaru.adapters.openai_agents.KitaruRunner` |
| 3 | LangGraph (calls mode) | `kitaru.adapters.langgraph.KitaruGraphRunner` |
| 4 | Claude Agent SDK | `kitaru.adapters.claude_agent_sdk.KitaruClaudeRunner` |

Each adapter is wrapped in the same `@flow` shape:

```python
@flow
def some_framework_flow(question: str) -> str:
    return runner.run_sync(question).output
```

That's the value: a single primitive (`@flow`) gives every adapter the same
durable execution, checkpoint tracking, artifact persistence, and replay
surface — without locking the team into any framework.

## Running it

```bash
uv sync --extra local \
        --extra pydantic-ai \
        --extra openai-agents \
        --extra langgraph-openai \
        --extra claude-agent-sdk
uv run kitaru init

export OPENAI_API_KEY=sk-...
export ANTHROPIC_API_KEY=sk-ant-...

uv run python examples/integrations/all_adapters_showcase/all_adapters.py
```

Both keys are required: PydanticAI, OpenAI Agents, and LangGraph all call the
OpenAI API by default; the Claude Agent SDK calls Anthropic.

## What you see

After all four runs the script prints a consolidated comparison table:

```
Adapter           Framework         Exec ID   Status     Ckpts  Took   Notes
----------------  ----------------  --------  ---------  -----  -----  --------
PydanticAI        pydantic-ai       cfc94bbf  completed  5      43.8s  -
OpenAI Agents     openai-agents     2e6550a0  completed  5      46.2s  -
LangGraph         langgraph         a9603542  completed  2      14.3s  checkpoint_strategy='calls'
Claude Agent SDK  claude-agent-sdk  bdce99c5  completed  1      20.1s  tool-free invocation
```

…followed by each adapter's final answer text and the matching Kitaru
`exec_id` so you can inspect any run in the dashboard or via
`kitaru executions get <id>`.

The checkpoint counts reflect each adapter's natural granularity:

| Adapter | Checkpoints | Notes |
|---|---|---|
| PydanticAI (granular) | one per model request + one per tool call | hooks PydanticAI's native event surface |
| OpenAI Agents (`calls`) | one per OpenAI model call + one per function tool call | hooks the Runner's per-turn boundary |
| LangGraph (`calls`) | one per LangChain model call + one consolidating `langgraph_summary` checkpoint | via `KitaruLangGraphMiddleware` |
| Claude Agent SDK | one per `query()` invocation | the SDK doesn't expose per-step extension points yet |

## What to look at in the code

- The four `run_*` functions are almost identical in shape: build the
  framework's native agent, wrap it in the matching Kitaru adapter, and put
  the call inside a one-line `@flow`.
- `_lookup_order` and `_shipping_policy` are plain Python functions reused by
  every framework's tool decorator. The business logic does not change with
  the harness.
- The flow body returns a single derived value in each case. Granular adapters
  emit many sibling checkpoints; Kitaru's flow-return coercer records the
  return value as a separate artifact so `.wait()` resolves cleanly without
  needing a manual `finalize_*` sink.
