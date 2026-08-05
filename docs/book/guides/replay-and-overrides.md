---
description: Reproduce a recorded session, fork it with an override, and compare the two — to debug a failure or test a change before shipping it.
icon: rotate-left
---

# Replay a failure and fork it

Replay re-executes a recorded [session](../concepts/agents-and-sessions.md)
to produce a **new session** — the same run, with exactly the changes you
specify. One mechanism, two jobs:

* **Debug a failure.** A production run went wrong. Replay it unchanged
  and you have the failure on your desk, reproducible, without touching
  production.
* **Test a change.** You want to swap the model, tighten the prompt, or
  ship the code in your working tree. Fork the run with that one change
  and read what it did.

This guide assumes the [Quickstart](../getting-started/quickstart.md)
setup: a registered agent with a run command, a registered
[evaluator](../concepts/evaluators.md), and a
[worker](../concepts/workers.md) running in the agent's environment.

## The three-session discipline

Every trustworthy comparison involves three sessions:

1. **Observed** — the original recording (recorded or imported).
2. **Reproduced** — an unchanged replay of it. If this doesn't hold up —
   evaluations disagree, the path is wildly different — stop. Your run
   depends on something the recording doesn't answer (live tool traffic,
   nondeterminism you haven't pinned), and no fork from it can be trusted.
3. **Forked** — the replay with one thing changed. Because the baseline
   reproduced, the difference between it and the fork is your change.

## Anatomy of a replay

```python
import asyncio
from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.replay import ReplayCreateRequest
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    HistoryConfig,
    ReplayOverride,
    ToolPolicy,
)

RECORDED_TOOLS = ToolPolicy(default=HistoryConfig(scope="baseline", on_miss="fail"))

async def main() -> None:
    client = KitaruAPIClient()
    replay = await client.replays.create(
        ReplayCreateRequest(
            baseline_session_id=SESSION_ID,
            # agent_version_id: defaults to the version the baseline recorded
            override=ReplayOverride(model={"openai:gpt-5.4": "openai:gpt-5-nano"}),
            tool_policy=RECORDED_TOOLS,
            evaluators=[EvaluatorConfig(evaluator="refund-check")],
            evaluate_baselines=True,
        )
    )
    print(replay.id, replay.job_id)

asyncio.run(main())
```

Field by field:

* `baseline_session_id` — the recording to re-run.
* `agent_version_id` — which code runs. Omitted, it's the version the
  baseline was recorded with (the faithful choice). Point it at a newly
  registered version to replay old traffic **against your working tree**.
* `override` — the fork. Omit it for a pure reproduction.
* `tool_policy` — what tool calls hit. **The default is `passthrough`:
  live tools.** For a replay that touches nothing real, set a `history`
  policy as above. Details in [Tool policies](tool-policies.md).
* `evaluators` — at least one, always. A replay is never just "it ran";
  it's scored on arrival.
* `evaluate_baselines` — score the original session with the same
  evaluators, so the comparison exists as soon as the replay settles.

Watch it with `kitaru job watch <job-id>`; when the replay reads
`completed`, `client.replays.get(replay.id)` carries the
`result_session_id`.

## Overrides

One `ReplayOverride`, four knobs — change one at a time:

| Field | Effect |
|---|---|
| `model` | Swap models at the model-call boundary. A string replaces every model; a `{old: new}` map replaces selectively. |
| `system_prompt` | Replace the system prompt for the re-run. |
| `prompt` | Replace the user prompt — ask a different question of the same recorded world. |
| `model_params` | Adjust sampling parameters (temperature, etc.) at the adapter level. |

Code changes need no override at all: register the new code as an agent
version and pass its `agent_version_id`. Replays run **from the top** —
the whole agent re-executes against the recorded world, so the entire
decision path downstream of your change is real.

{% hint style="warning" %}
With the default `passthrough` policy, a replayed `refund_payment` call
**refunds the card again**. Set a `history` policy for anything with side
effects, or use `static` to inject a canned result. If your agent must
behave differently under replay, check for the `KITARU_REPLAY_ID`
environment variable — it is set only in replayed runs.
{% endhint %}

## Reading the comparison

Both sides are sessions with evaluations. Read them together:

```python
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.filter import FilterCondition, FilterOp

async def evaluations_for(client, session_id):
    return {
        e.name: e
        async for e in client.evaluations.iter(EvaluationListParams(
            filter=FilterCondition(field="session_id", op=FilterOp.EQ, value=session_id)
        ))
    }

baseline_evals = await evaluations_for(client, baseline_session_id)
fork_evals = await evaluations_for(client, result_session_id)
for name, b in baseline_evals.items():
    f = fork_evals.get(name)
    print(name, "baseline:", b.score, "fork:", f.score if f else "—")
```

Session rollups carry the operational deltas — `cost`, `tokens`,
`llm_call_count`, `tool_call_count` — so "same pass rate, 40% cheaper,
one extra model call" is three field reads. For node-level inspection,
`list_nodes(include_payloads=True)` on both sessions shows you exactly
where the paths diverged.

## When a replay fails

A replay settles `failed` when its pipeline can't produce the comparison:
the agent process exited nonzero, a tool call missed under
`on_miss="fail"`, or an evaluator crashed. The job's tasks carry the
error and a log tail — `kitaru job get <job-id>` and
`kitaru job watch` surface them. The common causes:

* **No run spec** — the agent version must carry a run command; registering
  with `--command` is what makes a session replayable.
* **Worker environment** — the subprocess needs your agent's dependencies
  and provider keys; it inherits them from the worker's environment.
* **Unrecorded tool call** — the fork took a path the baseline never took.
  That's information, not noise: widen the history scope, add a `static`
  case for it, or accept `error_result` and let the agent handle it.

## From one replay to many

The same request against many sessions is a
[cohort](../concepts/cohorts.md) plus an
[experiment](../concepts/experiments.md) — one replay per session, fanned
out and scored identically. That's the subject of
[Build a regression suite from production](regression-suite.md).
