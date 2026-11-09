---
description: A named change, replayed across a cohort and evaluated. What improved, what regressed, before you ship.
icon: vials
---

# Experiments

A [replay](replay.md) is one counterfactual. An **experiment** is that counterfactual at population scale: take a [cohort](cohorts.md) of real runs, apply one change to all of them, evaluate every re-run with the same [evaluators](evaluators.md), and read what improved and what regressed.

The split of responsibilities is deliberate:

- The **experiment** holds the _change_: an override (model, prompt, params), a tool policy, and the evaluator list. It is reusable.
- An **experiment run** supplies the _population and the code_: one cohort version and one agent version. Run the same experiment against next week's cohort version, or the same cohort against your PR's agent version.

```python
import asyncio
import os
import uuid

from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.experiment import ExperimentCreateRequest
from kitaru.api_models.v1.experiment_run import ExperimentRunCreateRequest
from kitaru.api_models.v1.plugin import EvaluatorConfig
from kitaru.api_models.v1.replay_config import (
    HistoryConfig,
    ReplayOverride,
    ToolPolicy,
)


async def main() -> None:
    client = KitaruAPIClient()
    agent_id = uuid.UUID(os.environ["KITARU_AGENT_ID"])

    experiment = await client.experiments.create(
        ExperimentCreateRequest(
            agent_id=agent_id,
            name="cheaper-model",
            description="Would gpt-5-nano have held on refund tickets?",
            override=ReplayOverride(model={"openai:gpt-5.4": "openai:gpt-5-nano"}),
            tool_policy=ToolPolicy(
                default=HistoryConfig(scope="cohort_version", on_miss="fail")
            ),
            evaluators=[EvaluatorConfig(evaluator="refund-check")],
        )
    )

    run = await client.experiments.start_run(
        experiment.id,
        ExperimentRunCreateRequest(
            cohort_version_id=COHORT_VERSION_ID,
            agent_version_id=AGENT_VERSION_ID,
            evaluate_baselines=True,
        ),
    )
    print(run.id, run.status, run.progress)


asyncio.run(main())
```

{% hint style="info" %} The OpenAI Agents adapter does not support a `history` default. Keep its default as `passthrough` and add a named `history` override for each direct function tool you want to replay. See the [OpenAI Agents adapter page](../adapters/openai-agents.md). {% endhint %}

The same two steps from the CLI (the change as JSON on the experiment, the population and code on the run):

```bash
kitaru experiment create cheaper-model \
  --agent support-agent \
  --evaluator refund-check@latest \
  --override '{"model": {"openai:gpt-5.4": "openai:gpt-5-nano"}}' \
  --tool-policy '{"default": {"type": "history", "scope": "cohort_version", "on_miss": "fail"}}'

kitaru experiment run start cheaper-model \
  --cohort-version <cohort-version-id> \
  --agent support-agent@1 \
  --evaluate-baselines --wait
```

Starting a run fans out **one replay per session** in the cohort version. [Workers](workers.md) in your environment execute them; the run's `progress` counts replays through `pending → evaluating → completed` (plus `failed` / `canceled`), and the run settles when the last replay does. `evaluate_baselines=True` evaluates the original sessions too, so every replay has its baseline numbers to sit next to.

With a `history` tool policy scoped to `cohort_version`, replayed tool calls can be answered from any recording in the cohort (useful when runs share tool traffic), and `on_miss="fail"` keeps anything unrecorded from reaching a live system.

## Reading a run

A run's evaluation aggregates compare its baseline and result sessions, grouped by evaluation name, evaluator version, and data type. Read them through the SDK:

```python
aggregates = await client.experiment_runs.get_evaluation_aggregates(run.id)
for aggregate in aggregates:
    print(aggregate.name, aggregate.baseline.mean, aggregate.result.mean)
    print(aggregate.baseline.pass_rate, aggregate.result.pass_rate)
```

From the CLI, use `kitaru experiment run get <run-id> --aggregate`. The output includes the run and its `evaluation_aggregates`; without the flag, it returns the run alone. The REST endpoint is `GET /api/v1/experiment-runs/{id}/evaluation-aggregates`.

Statistics cover every evaluation linked to the run's replays. Numeric `mean` averages the recorded scores; boolean `mean` gives the share of true scores. `pass_rate` uses only evaluations with an explicit `passed` flag, independently of the score. Categorical results include value counts. Each group also includes baseline and result values for the 50 most recent replays, oldest first. Later evaluations of the same sessions do not change the run's pinned results.

Cost and token totals ([tracked per model call](../guides/llm-calls.md)) remain on each result session. The end-to-end workflow, including gating CI on a frozen cohort version, is in [Build a regression suite from production](../guides/regression-suite.md).

A failed replay fails the run. Aggregates describe the available evaluation rows, so inspect each side's counts and the run's progress before interpreting the comparison. Watch a run with `kitaru experiment run watch <run>`, inspect its jobs with `kitaru experiment run jobs <run>`, and cancel with `kitaru experiment run cancel <run>`; already finished replays keep their results.
