---
description: Re-run a recorded session against your real code, unchanged for a faithful baseline or forked with one thing different.
icon: rotate-left
---

# Replay

Replay is the verb the whole product hangs on. A [session](agents-and-sessions.md) is a recording; a **replay** re-executes it. Your agent's real code runs again, and the recording answers for the world the original run saw. With a `history` [tool policy](#tool-policies), tool calls are served from the recorded session, so nothing touches your real systems.

The discipline comes first: **replay unchanged before you change anything.** An unchanged replay that reproduces the original is your faithful baseline. Fork from that baseline with exactly one thing different (a model, a prompt, a code change) and the diff you read is your change, not replay noise.

## What a replay is

A replay names a **baseline session**, the **agent version** to run (by default, the version the baseline was recorded with), an optional **override**, a **tool policy**, and at least one [evaluator](evaluators.md). The server turns it into a job; a [worker](workers.md) in your environment starts your agent from its run spec, feeding it the baseline's inputs. The re-run records a fresh session (`origin: replay`), and the evaluators evaluate it as soon as it completes.

For a one-off replay, the CLI exposes the same create, list, and get flow:

```bash
kitaru replay create <baseline-session-id> \
  --evaluator refund-check@1 \
  --tool-policy '{"default":{"type":"history","scope":"baseline","on_miss":"fail"}}' \
  --evaluate-baselines --output json
kitaru replay list --output json
kitaru replay get <replay-id> --output json
```

Creation returns immediately with the replay and its job. Use `kitaru job watch <job-id>` to follow it, `kitaru job get <job-id> --tasks` to inspect task failures, or `kitaru job cancel <job-id>` to request cancellation.

{% hint style="warning" %} `kitaru replay create` is not idempotent. If the command fails after the server accepted it, retrying can create another replay and job. Run `kitaru replay list` and check for the first replay before retrying. Omitting `--tool-policy` uses the server default, which may execute live tools. {% endhint %}

{% hint style="info" %} The OpenAI Agents adapter does not support a `history` default. Keep its default as `passthrough` and add a named `history` override for each direct function tool you want to replay. See the [OpenAI Agents adapter page](../adapters/openai-agents.md). {% endhint %}

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

`evaluate_baselines=True` evaluates the baseline session with the same evaluators, so the comparison you want, baseline evaluations next to replay evaluations, exists as soon as the replay settles. Watch the job with `kitaru job watch <job-id>`, then read `result_session_id` off the replay.

A replay moves `pending → evaluating → completed` (or `failed` / `canceled`). Its output is intentionally plain: the result session plus its evaluation rows. You compare baseline and result by reading both sessions' evaluations, cost, and tokens. See [Replay a failure and fork it](../guides/replay-and-overrides.md) for the full loop.

## Forking: the override

There is no separate fork operation in the API; a "fork" is a replay that carries an `override`. The word is shorthand for that, the way "baseline" is shorthand for a replay without one. Both are the same call.

An override changes one thing about the re-run and leaves everything else alone:

```python
from kitaru.api_models.v1.replay_config import ReplayOverride

override = ReplayOverride(
    model={"openai:gpt-5.4": "openai:gpt-5-nano"},  # or just "openai:gpt-5-nano"
    # system_prompt="...",   # replace the system prompt
    # prompt="...",          # replace the user prompt
    # model_params={"temperature": 0.0},
)
```

- `model` swaps the model on every matching model call: a plain string replaces all of them, a map replaces old with new per model.
- `system_prompt` and `prompt` rewrite the run's inputs before the agent starts.
- `model_params` adjusts sampling parameters at the adapter level.

Replays re-run the agent **from the top**. There is no partial, mid-run cut point: the recording answers the world's side of the conversation, and your agent recomputes its own side in full. That is what makes a fork trustworthy: the whole decision path is real.

## Tool policies

The tool policy decides what happens when the re-running agent calls a tool. The default answers per tool name, with one fallback for everything else:

| Policy | What a tool call gets |
| --- | --- |
| `history` | The recorded result for the same call, matched by tool name and arguments, from the baseline (or a wider scope). `on_miss` decides what an unrecorded call does: `fail`, `passthrough`, or `error_result`. |
| `static` | A canned result you define per case, with exact or subset argument matching. |
| `passthrough` | The real tool, live. This is the current server default when you set no policy. |
| `llm` | A model answers the tool call in-distribution. The API accepts it, but adapter support varies; PydanticAI, Mastra, and Vercel AI SDK currently reject it. |

For the "nothing touches real systems" guarantee, set `default=HistoryConfig(scope="baseline", on_miss="fail")`. Recorded calls are answered from the recording and anything novel stops the replay instead of hitting production. The full matrix, including per-tool overrides and history scopes, is in [Tool policies](../guides/tool-policies.md).

Overrides and non-passthrough tool policies both depend on the agent version's declared [runtime capabilities](agents-and-sessions.md). Creating a replay whose config carries one the version cannot apply is rejected with 422.

## Scale: cohorts and experiments

One replay answers a question about one session. The same machinery applied to a [cohort](cohorts.md) of sessions, with the change expressed as an [experiment](experiments.md), answers the question that matters before you ship: _what does this change do to last week's production traffic?_ That is the [regression suite](../guides/regression-suite.md).
