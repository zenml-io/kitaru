---
description: Reproduce a recorded session, fork it with an override, and compare the two, so you can debug a failure or test a change before shipping it.
icon: rotate-left
---

# Replay a failure and fork it

Replay re-executes a recorded [session](../concepts/agents-and-sessions.md) to produce a **new session**: the same run, with exactly the changes you specify. One mechanism covers two jobs:

- **Debug a failure.** A production run went wrong. Replay it unchanged and you have the failure on your desk, reproducible without touching production.
- **Test a change.** You want to swap the model, tighten the prompt, or ship the code in your working tree. Fork the run with that one change and read what it did.

This guide assumes you prepared the [PydanticAI returns agent example](https://github.com/zenml-io/kitaru/tree/main/examples/python/pydantic_ai_ticket_resolver) and completed the setup and Define phases of the [returns agent tutorial](../tutorials/returns-agent/README.md): a registered agent with a run command, a registered [evaluator](../concepts/evaluators.md), and a [worker](../concepts/workers.md) running in the agent's environment.

## Create the one-off replay from the CLI

Create an unchanged reproduction with an explicit recorded-history policy:

```bash
kitaru replay create <baseline-session-id> \
  --evaluator refund-check@1 \
  --tool-policy '{"default":{"type":"history","scope":"baseline","on_miss":"fail"}}' \
  --baseline-evaluation-mode if_missing --output json
```

The JSON result contains the replay ID and job ID. The command queues work and returns; it does not wait for the worker:

```bash
kitaru job watch <job-id>
kitaru replay get <replay-id> --output json
```

Use `kitaru job get <job-id> --tasks` for task errors and `kitaru job cancel <job-id>` to request cancellation. `kitaru replay list --output json` lists both standalone and experiment-created replays.

{% hint style="warning" %} The SDK's automatic retries (timeouts, dropped connections, 5xx) are safe: they reuse the same idempotency key, so a retried create settles as at most one replay. Manually re-running `kitaru replay create` after an ambiguous failure is not, since each invocation mints its own key. Check `kitaru replay list` before retrying by hand, or the manual retry may create a duplicate replay and job. Omitting `--tool-policy` uses the server default and may execute live tools. {% endhint %}

{% hint style="info" %} The OpenAI Agents adapter does not support a `history` default. Keep its default as `passthrough` and add a named `history` override for each direct function tool you want to replay. See the [OpenAI Agents adapter page](../adapters/openai-agents.md). {% endhint %}

## The three-session discipline

Every trustworthy comparison involves three sessions:

1. **Observed:** the original recording (recorded or imported).
2. **Reproduced:** an unchanged replay of it. If this does not hold up, because evaluations disagree or the path is wildly different, stop. Your run depends on something the recording does not answer, such as live tool traffic or nondeterminism you have not pinned, and no fork from it can be trusted.
3. **Forked:** the replay with one thing changed. Because the baseline reproduced, the difference between it and the fork is your change.

## Anatomy of a replay

```python
import asyncio
from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.plugin import EvaluatorConfig
from kitaru.api_models.v1.replay import BaselineEvaluationMode, ReplayCreateRequest
from kitaru.api_models.v1.replay_config import (
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
            baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
        )
    )
    print(replay.id, replay.job_id)


asyncio.run(main())
```

Field by field:

- `baseline_session_id`: the recording to re-run.
- `agent_version_id`: which registered run spec executes. Omitted, it uses the version recorded on the baseline session. Point it at another registered version to replay old traffic against a candidate build. Because an agent version stores a `RunSpec` pointer (`command`, `working_dir`, environment, secrets) rather than a code snapshot or commit hash, keep each version in a separate directory, git worktree, or container image so editing code in place does not change what older versions run.
- `override`: the fork. Omit it for a pure reproduction.
- `tool_policy`: what tool calls hit. If omitted, the server applies its default, which currently passes calls through to live tools. For a replay that touches nothing real, set a `history` policy as above. Details in [Tool policies](tool-policies.md).
- `evaluators`: at least one, always. A replay is evaluated on arrival.
- `baseline_evaluation_mode`: `IF_MISSING` (the default) reuses the latest baseline evaluation with the same evaluator version and parameters, or schedules one if none matches. `FORCE` always reruns baseline evaluations; `NONE` skips them. Result sessions receive fresh evaluations.

Watch it with `kitaru job watch <job-id>`; when the replay reads `completed`, `client.replays.get(replay.id)` carries the `result_session_id`.

## Expected answers during replay

Keep gold labels in the baseline session's metadata, separate from agent inputs. Evaluators of the result session receive `SessionView.baseline`, an optional `EvaluationBaselineContext` containing the baseline's `session_id` and `metadata`. This context also accompanies a later direct evaluation of the result session. It does not include the baseline's full trace or outputs. The result session's own metadata remains separate in `session.session.metadata`.

An evaluator can read gold labels from `session.baseline.metadata` for a replay result and fall back to `session.session.metadata` for an ordinary session. A baseline evaluation has no baseline context unless that session was itself the result of an earlier replay. See [Write an evaluator](write-an-evaluator.md) for an example. The replayed agent receives the baseline inputs and replay ID, without baseline metadata or permission to read the baseline session.

Context refers to the immediate baseline, without following earlier replays. Prefer the original labeled session as your baseline; replaying a previous result does not recover labels from its ancestors.

Metadata is read when the evaluation task specification is fetched, not preserved as a permanent snapshot. Keep labels unchanged while comparing a pair. After editing gold labels, use `--baseline-evaluation-mode force` or `BaselineEvaluationMode.FORCE` for the next replay so the baseline is evaluated again; `if_missing` can reuse evaluations made before the edit.

## Overrides

One `ReplayOverride`, four knobs; change one at a time:

| Field | Effect |
| --- | --- |
| `model` | Swap models at the model-call boundary. A string replaces every model; a `{old: new}` map replaces selectively. |
| `system_prompt` | Replace the system prompt for the re-run. |
| `prompt` | Replace the user prompt to ask a different question of the same recorded world. |
| `model_params` | Adjust sampling parameters (temperature, etc.) at the adapter level. |

Code changes need no override at all: put the candidate code in its own working directory, git worktree, or container image, register it as a new agent version with `--display-version`, and pass its `agent_version_id` (or `--agent NAME@VERSION`). Do not mutate the directory used by an earlier agent version in place, because an agent version stores only a `RunSpec` pointer (`command`, `working_dir`, environment, secrets), not a code snapshot or commit hash. Replays run **from the top**: the whole agent re-executes against the recorded world, so the entire decision path downstream of your change is real.

{% hint style="warning" %} With the default `passthrough` policy, a replayed `refund_payment` call **refunds the card again**. Set a `history` policy for anything with side effects, or use `static` to inject a canned result. If your agent must behave differently under replay, check for the `KITARU_REPLAY_ID` environment variable; it is set only in replayed runs. {% endhint %}

## Reading the comparison

Both sides are sessions with evaluations. Read them together:

```python
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.filter import FilterCondition, FilterOp


async def evaluations_for(client, session_id):
    return {
        e.name: e
        async for e in client.evaluations.iter(
            EvaluationListParams(
                filter=FilterCondition(
                    field="session_id", op=FilterOp.EQ, value=session_id
                )
            )
        )
    }


baseline_evals = await evaluations_for(client, baseline_session_id)
fork_evals = await evaluations_for(client, result_session_id)
for name, b in baseline_evals.items():
    f = fork_evals.get(name)
    print(name, "baseline:", b.score, "fork:", f.score if f else "-")
```

Session rollups carry the operational deltas (`cost`, `tokens`, `llm_call_count`, `tool_call_count`), so "same pass rate, 40% cheaper, one extra model call" is three field reads. For node-level inspection, `list_nodes(include_payloads=True)` on both sessions shows where the paths diverged.

## When a replay fails

A replay settles `failed` when its pipeline cannot produce the comparison: the agent process exited nonzero, a tool call missed under `on_miss="fail"`, or an evaluator crashed. The job's tasks carry the error and a log tail; `kitaru job get <job-id>` and `kitaru job watch` surface them, and [Troubleshooting](../getting-started/troubleshooting.md) walks the diagnosis. The common causes:

- **No run spec:** the agent version must carry a run command; registering with `--command` is what makes a session replayable.
- **Worker environment:** the subprocess needs your agent's dependencies and provider keys; it inherits them from the worker's environment.
- **Unrecorded tool call:** the fork took a path or passed tool arguments the baseline never recorded. That is information: add a `static` case for side-effecting tools whose arguments changed, widen the history scope only if the tool's answer is independent of the conversation, or accept `error_result` and inspect the replayed session for failed or mocked tool nodes (`attributes.mocked: true`). See [Tool policies](tool-policies.md).

## From one replay to many

The same request against many sessions is a [cohort](../concepts/cohorts.md) plus an [experiment](../concepts/experiments.md): one replay per session, fanned out and evaluated identically. That is the subject of [Build a regression suite from production](regression-suite.md).
