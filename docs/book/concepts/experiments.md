---
description: A named change, replayed across a cohort and scored — what improved, what regressed, before you ship.
icon: vials
---

# Experiments

A [replay](replay.md) is one counterfactual. An **experiment** is that
counterfactual at population scale: take a [cohort](cohorts.md) of real
runs, apply one change to all of them, score every re-run with the same
[evaluators](evaluators.md), and read what improved and what regressed.

The split of responsibilities is deliberate:

* The **experiment** holds the *change*: an override (model, prompt,
  params), a tool policy, and the evaluator list. It is reusable.
* An **experiment run** supplies the *population and the code*: one cohort
  version and one agent version. Run the same experiment against next
  week's cohort version, or the same cohort against your PR's agent
  version.

```python
import asyncio
from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.experiment import ExperimentCreateRequest
from kitaru.api_models.v1.experiment_run import ExperimentRunCreateRequest
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig,
    HistoryConfig,
    ReplayOverride,
    ToolPolicy,
)

async def main() -> None:
    client = KitaruAPIClient.from_env()

    experiment = await client.experiments.create(
        ExperimentCreateRequest(
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

The same two steps from the CLI — the change as JSON on the experiment,
the population and code on the run:

```bash
kitaru experiment create cheaper-model \
  --evaluator refund-check@latest \
  --override '{"model": {"openai:gpt-5.4": "openai:gpt-5-nano"}}' \
  --tool-policy '{"default": {"type": "history", "scope": "cohort_version", "on_miss": "fail"}}'

kitaru experiment run start cheaper-model \
  --cohort-version <cohort-version-id> \
  --agent support-agent@1 \
  --evaluate-baselines --wait
```

Starting a run fans out **one replay per session** in the cohort version.
[Workers](workers.md) in your environment execute them; the run's
`progress` counts replays through `pending → evaluating → completed`
(plus `failed` / `canceled`), and the run settles when the last replay
does. `evaluate_baselines=True` scores the original sessions too, so every
replay has its baseline numbers to sit next to.

With a `history` tool policy scoped to `cohort_version`, replayed tool
calls can be answered from any recording in the cohort — useful when runs
share tool traffic — and `on_miss="fail"` keeps anything unrecorded from
reaching a live system.

## Reading a run

A run's output is deliberately raw: its replays, each with a result
session, and the evaluation rows on both sides. Compare them by reading
the evaluations:

```python
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.filter import FilterCondition, FilterOp

async for evaluation in client.evaluations.iter(
    EvaluationListParams(
        filter=FilterCondition(field="session_id", op=FilterOp.EQ, value=session_id)
    )
):
    print(evaluation.name, evaluation.score, evaluation.passed)
```

Numbers average, booleans count into pass rates, categorical labels diff as
transitions, and free text gets read. Cost and token totals ride on each
result session, so "the cheaper model held on 18 of 20 tickets and cut cost
41%" is two loops over stored rows. The end-to-end workflow — including
gating CI on a frozen cohort version — is in
[Build a regression suite from production](../guides/regression-suite.md).

A failed replay fails the run: the comparison the experiment exists for
cannot be produced for that session, and the numbers never silently shrink
their denominator. Watch a run with `kitaru experiment run watch <run>`,
inspect its jobs with `kitaru experiment run jobs <run>`, and cancel with
`kitaru experiment run cancel <run>`; already finished replays keep their
results.
