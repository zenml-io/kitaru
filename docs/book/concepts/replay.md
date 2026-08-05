---
description: Re-run a recorded session against your real code — unchanged for a faithful baseline, or forked with one thing different.
icon: rotate-left
---

# Replay

Replay is the verb the whole product hangs off. A
[session](agents-and-sessions.md) is a recording; a **replay** re-executes
it: your agent's real code runs again, and the recording answers for the
world the original run saw. With a `history` [tool policy](#tool-policies),
tool calls are served from the recorded session — nothing touches your real
systems.

The discipline comes first: **replay unchanged before you change anything.**
An unchanged replay that reproduces the original is your faithful baseline.
Fork from that baseline with exactly one thing different — a model, a
prompt, a code change — and the diff you read is your change, not replay
noise.

## What a replay is

A replay names a **baseline session**, the **agent version** to run (by
default, the version the baseline was recorded with), an optional
**override**, a **tool policy**, and at least one
[evaluator](evaluators.md). The server turns it into a job; a
[worker](workers.md) in your environment starts your agent from its run
spec, feeding it the baseline's inputs. The re-run records a fresh session
(`origin: replay`), and the evaluators score it as soon as it completes.

```python
import asyncio
from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.replay import ReplayCreateRequest
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    HistoryConfig,
    ToolPolicy,
)

async def main() -> None:
    client = KitaruAPIClient()
    replay = await client.replays.create(
        ReplayCreateRequest(
            baseline_session_id=BASELINE_ID,
            evaluators=[EvaluatorConfig(evaluator="refund-check")],
            tool_policy=ToolPolicy(
                default=HistoryConfig(scope="baseline", on_miss="fail")
            ),
            evaluate_baselines=True,
        )
    )
    print(replay.id, replay.job_id, replay.status)

asyncio.run(main())
```

`evaluate_baselines=True` scores the baseline session with the same
evaluators, so the comparison you want — baseline evaluations next to
replay evaluations — exists as soon as the replay settles. Watch the job
with `kitaru job watch <job-id>`, then read `result_session_id` off the
replay.

A replay moves `pending → evaluating → completed` (or `failed` /
`canceled`). Its output is deliberately plain: the result session plus its
evaluation rows. You compare baseline and result by reading both sessions'
evaluations, cost, and tokens — see
[Replay a failure and fork it](../guides/replay-and-overrides.md) for the
full loop.

## Forking: the override

An override changes one thing about the re-run and leaves everything else
alone:

```python
from kitaru.api_models.v1.replay_config import ReplayOverride

override = ReplayOverride(
    model={"openai:gpt-5.4": "openai:gpt-5-nano"},  # or just "openai:gpt-5-nano"
    # system_prompt="...",   # replace the system prompt
    # prompt="...",          # replace the user prompt
    # model_params={"temperature": 0.0},
)
```

* `model` swaps the model on every matching model call — a plain string
  replaces all of them, a map replaces old with new per model.
* `system_prompt` and `prompt` rewrite the run's inputs before the agent
  starts.
* `model_params` adjusts sampling parameters at the adapter level.

Replays re-run the agent **from the top**. There is no partial,
mid-run cut point: the recording answers the world's side of the
conversation, and your agent recomputes its own side in full. That is what
makes a fork trustworthy — the whole decision path is real.

## Tool policies

The tool policy decides what happens when the re-running agent calls a
tool. The default answers per tool name, with one fallback for everything
else:

| Policy | What a tool call gets |
|---|---|
| `history` | The recorded result for the same call, matched by tool name and arguments, from the baseline (or a wider scope). `on_miss` decides what an unrecorded call does: `fail`, `passthrough`, or `error_result`. |
| `static` | A canned result you define per case — exact or subset argument matching. |
| `passthrough` | The real tool, live. This is the default when you set no policy. |
| `llm` | A model answers the tool call in-distribution. Accepted by the API but not yet supported by the adapter — treat it as roadmap. |

For the "nothing touches real systems" guarantee, set
`default=HistoryConfig(scope="baseline", on_miss="fail")` — recorded calls
are answered from the recording and anything novel stops the replay instead
of hitting production. The full matrix, including per-tool overrides and
history scopes, is in [Tool policies](../guides/tool-policies.md).

## Scale: cohorts and experiments

One replay answers a question about one run. The same machinery applied to
a [cohort](cohorts.md) of runs, with the change expressed as an
[experiment](experiments.md), answers the question that matters before you
ship: *what does this change do to last week's production traffic?* That is
the [regression suite](../guides/regression-suite.md).
