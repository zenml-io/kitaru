---
description: What a replayed tool call gets — the recorded answer, a canned result, the live tool, or a hard stop.
icon: shield-halved
---

# Tool policies

When a [replay](../concepts/replay.md) re-runs your agent and the agent
calls a tool, something has to answer. The **tool policy** decides what:
per tool name, with a default for everything else. It's the difference
between a replay that's a safe, sealed experiment and one that refunds a
card twice.

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
            cases=[StaticCase(match={"order_id": "4821"},
                              result="refund issued: $129.00")],
            on_miss="error_result",
        ),
    },
)
```

A policy travels with a replay or an
[experiment](../concepts/experiments.md); the adapter enforces it inside
your re-running agent at the tool boundary. On the CLI, the same
structure goes to `kitaru experiment create --tool-policy` as JSON:

```bash
--tool-policy '{"default": {"type": "history", "scope": "baseline", "on_miss": "fail"},
                "tools": {"get_current_time": {"type": "passthrough"}}}'
```

## The four policies

### `history` — answered from the recording

The workhorse. The call is matched against recorded tool calls — same
tool name, same arguments — and gets the recorded result. Your agent
believes it called the tool; nothing outside the process happened.

`scope` says which recordings answer:

| Scope | Answers come from |
|---|---|
| `baseline` | Only the session being replayed — the strictest, most faithful choice |
| `cohort_version` | Any session in the experiment's cohort — useful when runs share tool traffic (only valid inside experiments) |
| `agent` | Any of the agent's sessions — the widest net |

`on_miss` says what an *unrecorded* call does — and this is where the
safety posture lives:

* `fail` — stop the replay. Nothing unrecorded ever executes. Use this
  for anything with side effects.
* `error_result` — hand the agent a tool error and let it cope. The
  replay continues, and how the agent handles the miss is itself signal.
* `passthrough` — fall through to the live tool. Only for tools that are
  safe to re-execute.

A fork that changes the model or prompt *will* sometimes take new paths
and call tools the baseline never called. A miss isn't noise — it's the
fork diverging. `fail` tells you loudly; `error_result` lets the run
finish so the evaluator can judge the recovery.

### `static` — a canned answer

You script the world. Each `StaticCase` matches arguments (`match_mode="exact"`
or `"subset"`) and returns your `result` — ideal for injecting the
counterfactual ("what if the refund had already succeeded?") or for
stubbing a tool that didn't exist when the baseline was recorded.
`on_miss` works as above.

### `passthrough` — the live tool

The real thing, live. **This is the default when you set no policy at
all** — a deliberate choice for read-only tools (clocks, retrieval,
search), and a footgun for anything that writes. If a replay must touch
a live system, prefer scoping `passthrough` to the specific safe tools
and keeping a `history` default, rather than the reverse.

### `llm` — a model plays the tool

`LLMConfig(model=..., instructions=...)` asks a model to answer the tool
call in-distribution — for simulating tools whose recordings you don't
have.

{% hint style="warning" %}
The `llm` policy is accepted and stored by the API but **not yet
supported by the PydanticAI adapter** — a replay that reaches an
`llm`-configured tool fails with a policy error. Treat it as roadmap; use
`static` for scripted worlds today.
{% endhint %}

## How matching works

A recorded `tool_call` node carries a cache key: a hash of the tool name
and its canonical JSON arguments. The replaying adapter computes the same
key for the live call and asks the server for a match within the policy's
scope. Exact-argument matching is the point — a call with different
arguments is a different call, and pretending otherwise would corrupt the
world your fork runs in.

One edge case: a tool call whose arguments can't be serialized to
canonical JSON gets no cache key at all. No lookup can match it, so on
replay it always takes the `on_miss` path — one more reason to keep tool
arguments JSON-clean.

## Choosing a posture

| Situation | Policy |
|---|---|
| Reproducing a failure faithfully | `history(baseline, on_miss="fail")` everywhere |
| Fork that may explore new paths | `history` default with `on_miss="error_result"`; `fail` on side-effecting tools |
| Injecting a counterfactual | `static` on the tool in question, `history` for the rest |
| Read-only tools that are cheap and safe | `passthrough`, scoped per tool |
| Regression suite over a cohort | `history(cohort_version, on_miss="fail")` |
