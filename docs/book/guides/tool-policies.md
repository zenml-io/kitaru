---
description: Choose whether replayed tool calls use recorded results, static results, live tools, or model-generated responses.
icon: shield-halved
---

# Tool policies

When a [replay](../concepts/replay.md) reaches a tool call, the **tool policy** determines how the adapter responds. You can configure individual tools and set a default for all others. Without a suitable policy, a replay can call a live tool and repeat its side effects.

```python
from kitaru.api_models.v1.replay_config import (
    HistoryConfig,
    PassthroughConfig,
    StaticCase,
    StaticConfig,
    ToolPolicy,
)

policy = ToolPolicy(
    default=HistoryConfig(scope="baseline", on_miss="fail"),
    tools={
        "get_current_time": PassthroughConfig(),
        "refund_payment": StaticConfig(
            cases=[
                StaticCase(match={"order_id": "4821"}, result="refund issued: $129.00")
            ],
            on_miss="error_result",
        ),
    },
)
```

A policy belongs to a replay or an [experiment](../concepts/experiments.md). The adapter applies it when the re-running agent calls a tool. On the CLI, pass the same structure to `kitaru experiment create --tool-policy` as JSON:

```bash
--tool-policy '{"default": {"type": "history", "scope": "baseline", "on_miss": "fail"},
                "tools": {"get_current_time": {"type": "passthrough"}}}'
```

{% hint style="info" %} The OpenAI Agents adapter does not support a `history` default. Keep its default as `passthrough` and add a named `history` override for each direct function tool you want to replay. See the [OpenAI Agents adapter page](../adapters/openai-agents.md). {% endhint %}

Anything beyond `passthrough` needs a runtime that can intercept tool calls. A replay or experiment run with a non-passthrough policy is rejected when the agent version does not declare the `tool_policies` [runtime capability](../concepts/agents-and-sessions.md).

## The four policies

### `history`: use a recorded result

The adapter looks for a recorded call with the same tool name and arguments. If it finds one, it returns the recorded result without executing the live tool.

`scope` says which recordings answer:

| Scope | Answers come from |
| --- | --- |
| `baseline` | Only the session being replayed. This is the narrowest scope. |
| `cohort_version` | Any session in the experiment's cohort. This scope is valid only inside experiments. |
| `agent` | Any session belonging to the agent. |

`on_miss` controls what happens when no recorded call matches:

- `fail`: stop the replay without executing the tool. Use this for tools with side effects.
- `error_result`: return a tool error to the agent and continue the replay.
- `passthrough`: execute the live tool. Use this only when repeating the call is safe.

A model or prompt change may cause the agent to call a tool that does not appear in the baseline. With `fail`, that call stops the replay. With `error_result`, the agent receives an error and the evaluator can assess its response.

### `static`: return a configured result

Each `StaticCase` matches arguments with `match_mode="exact"` or `"subset"` and returns the configured `result`. Use it to test a specific condition, such as a refund that has already succeeded, or to stub a tool that was absent from the baseline. `on_miss` controls unmatched arguments as described above.

### `passthrough`: execute the live tool

**This is the default when you set no policy.** The adapter executes the live tool and returns its result. This may be appropriate for safe read-only calls such as clocks or search. It is unsafe for calls that write data or trigger external actions. Prefer a `history` default and configure `passthrough` only for specific tools that are safe to repeat.

### `llm`: generate a result with a model

`LLMConfig(model=..., instructions=...)` asks a model to generate a response to the tool call. This can simulate a tool when no recorded result is available.

{% hint style="warning" %} The API accepts and stores the `llm` policy, but the PydanticAI, Mastra, and Vercel AI SDK adapters do not support it. Those adapters reject the policy before executing the configured tool. Use `static` when you need to provide a simulated result. Check the relevant adapter page before relying on `llm` elsewhere. {% endhint %}

History matching is guaranteed only within the same adapter implementation. Different frameworks can apply schema defaults, coercion, or serialization differently, which changes the cache key even when a tool call looks equivalent. A completed history match replays its result, including `null`, except in LangGraph: it requires a recorded `ToolMessage` or `Command` envelope and fails closed for `null` or malformed completed results. An occurrence-based `baseline` lookup can also match a failed call; the adapter raises its stored error without executing the live tool. Lookups without an occurrence, including `agent` and `cohort_version` scope, consider completed calls only, so a failed-only history is a miss and follows `on_miss`.

Adapters raise a Kitaru replay error for a failed match rather than recreating the original exception class or structured retry signal. This aborts the current adapter run unless application code catches that replay error.

## How matching works

A recorded `tool_call` node has a cache key derived from the tool name and its canonical JSON arguments. During replay, the adapter computes the same key for the attempted call and asks the server for a match within the policy's scope. Calls with different arguments have different keys and do not match.

A baseline can call the same tool with identical arguments more than once and receive different results. With `baseline` scope, the PydanticAI, LangGraph, OpenAI Agents, and TypeScript (Mastra and Vercel AI SDK) adapters consume those recorded results in invocation order: the first replayed call gets the first recorded result, the second gets the second, and so on. A replayed call past the last recorded occurrence is a miss and follows the configured `on_miss` behavior. With `cohort_version` and `agent` scope, the newest completed matching recording answers every call.

If a tool call's arguments cannot be serialized to canonical JSON, the call has no cache key. A history lookup cannot match it, so replay follows the configured `on_miss` behavior. Keep tool arguments JSON-serializable if you plan to replay them from history.

## Choosing a policy

| Situation | Policy |
| --- | --- |
| Reproducing a failure faithfully | `history(baseline, on_miss="fail")` everywhere |
| Fork that may explore new paths | `history` default with `on_miss="error_result"`; `fail` on side-effecting tools |
| Injecting a counterfactual | `static` on the tool in question, `history` for the rest |
| Read-only tools that are cheap and safe | `passthrough`, scoped per tool |
| Regression suite over a cohort | `history(cohort_version, on_miss="fail")` |
