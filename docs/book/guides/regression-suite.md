---
description: Freeze a cohort of recorded runs, replay it against a change, and use the results in CI.
icon: vials
---

# Build a regression suite from production

Replaying a change against one session shows how it affects that case. A regression suite repeats the comparison across a fixed set of recorded or imported sessions. These sessions complement synthetic fixtures: they preserve inputs and behavior seen in real runs, while synthetic cases can cover conditions that have not occurred in production.

This guide selects a population, freezes it as a cohort version, defines a change as an experiment, and runs that experiment in CI.

## 1. Select the population

Pick sessions that cover important behavior and known failures. You can filter by agent, status, or time, or start with sessions linked to a specific incident:

```python
import asyncio
from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.session import SessionListParams

async def main() -> None:
    client = KitaruAPIClient()
    refund_runs = [
        s.id
        async for s in client.sessions.iter(SessionListParams(
            filter=FilterCondition(field="agent_id", op=FilterOp.EQ, value=AGENT_ID),
            size=100,
        ))
    ][:50]
```

A useful starting point is a recent sample of traffic plus sessions linked to past incidents. [Imported sessions](import-langfuse-traces.md) work like recorded sessions. If you tagged an import, use that tag to select it (`kitaru session list --tag imported-baseline`). You can select directly recorded sessions with `--agent` or `--filter`.

## 2. Freeze it into a cohort version

`cohort create` accepts a session selection through `--tag`, `--session`, `--sessions-file`, or `--filter`. It stores the matching sessions as version 1. Here, `--agent` names the agent that owns the cohort; it does not select sessions:

```bash
kitaru cohort create refund-regression --agent support-agent \
  --tag imported-baseline --display-version week-32
```

The client can create the cohort and its first version from the selection above:

```python
from kitaru.api_models.v1.cohort import CohortCreateRequest
from kitaru.api_models.v1.cohort_version import CohortVersionCreateRequest

cohort = await client.cohorts.create(
    CohortCreateRequest(name="refund-regression", agent_id=AGENT_ID)
)
version = await client.cohorts.create_version(
    cohort.id,
    CohortVersionCreateRequest(add_session_ids=refund_runs, display_version="week-32"),
)
```

[Cohort versions are immutable](../concepts/cohorts.md). Version 1 therefore keeps the same 50 sessions. To add or remove sessions, create a new version so later comparisons show that the population changed.

## 3. Make the change an experiment

The experiment holds everything about the change _except_ the population:

```python
import os
import uuid

from kitaru.api_models.v1.experiment import ExperimentCreateRequest
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig, HistoryConfig, ReplayOverride, ToolPolicy,
)

experiment = await client.experiments.create(
    ExperimentCreateRequest(
        agent_id=uuid.UUID(os.environ["KITARU_AGENT_ID"]),
        name="cheaper-model",
        override=ReplayOverride(model={"openai:gpt-5.4": "openai:gpt-5-nano"}),
        tool_policy=ToolPolicy(
            default=HistoryConfig(scope="cohort_version", on_miss="fail")
        ),
        evaluators=[
            EvaluatorConfig(evaluator="refund-check"),
            EvaluatorConfig(evaluator="tone-judge"),
        ],
    )
)
```

Both evaluators must already be registered. In this example, `tone-judge` represents a second evaluator written for your application. See [Write an evaluator](write-an-evaluator.md).

To test a code change, omit `override` and register the branch as a new agent version. The experiment run selects that version. The `history` policy with `scope="cohort_version"` can answer tool calls from any recording in the cohort. With `on_miss="fail"`, an unmatched call stops its replay instead of reaching the live tool.

The CLI form takes the override and tool policy as JSON:

```bash
kitaru experiment create cheaper-model \
  --agent support-agent \
  --evaluator refund-check@latest --evaluator tone-judge@latest \
  --override '{"model": {"openai:gpt-5.4": "openai:gpt-5-nano"}}' \
  --tool-policy '{"default": {"type": "history", "scope": "cohort_version", "on_miss": "fail"}}'
```

## 4. Run it and read it

If the candidate version does not exist yet, register it with `kitaru agent version register`. The following example uses `support-agent@2`:

```bash
kitaru experiment run start cheaper-model \
  --cohort-version <cohort-version-id> \
  --agent support-agent@2 \
  --evaluate-baselines \
  --wait --timeout 1800
```

Or from the client:

```python
from kitaru.api_models.v1.experiment_run import ExperimentRunCreateRequest

run = await client.experiments.start_run(
    experiment.id,
    ExperimentRunCreateRequest(
        cohort_version_id=version.id,
        agent_version_id=AGENT_VERSION_ID,   # e.g. your PR's registered version
        evaluate_baselines=True,
    ),
)
```

Workers create one replay task per session, and `run.progress` reports how many have finished. When the run settles, each replay has a result session. If `evaluate_baselines=True`, the baseline and result sessions both have evaluations. The example below compares boolean pass results:

```python
from kitaru.api_models.v1.evaluation import EvaluationListParams
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.replay import ReplayListParams

replays = [
    r
    async for r in client.replays.iter(ReplayListParams(
        filter=FilterCondition(field="experiment_run_id", op=FilterOp.EQ, value=run.id)
    ))
]

async def passed(session_id, name="refund_issued"):
    async for e in client.evaluations.iter(EvaluationListParams(
        filter=FilterCondition(field="session_id", op=FilterOp.EQ, value=session_id)
    )):
        if e.name == name:
            return e.passed
    return None

baseline_pass = [await passed(r.baseline_session_id) for r in replays]
fork_pass = [await passed(r.result_session_id) for r in replays]
print(f"baseline: {sum(filter(None, baseline_pass))}/{len(replays)}")
print(f"fork:     {sum(filter(None, fork_pass))}/{len(replays)}")
```

You can also aggregate cost from the result sessions' rollups. Report both the summary and the underlying failures, for example: _"gpt-5-nano passed 47 of 50 refund tickets and reduced recorded cost by 41%. The failed cases were sessions 12, 19, and 44."_ You can then [replay and inspect](replay-and-overrides.md) each failed session.

## 5. Gate on it

To use the experiment in CI, register the pull request's code as an agent version and start a run against the frozen cohort version:

```bash
kitaru agent version register support-agent --command "python support.py"
kitaru experiment run start cheaper-model \
  --cohort-version <cohort-version-id> \
  --agent support-agent@<new-version> \
  --evaluate-baselines --wait --timeout 1800
```

`--wait` blocks until the run settles and exits nonzero if it fails, so the CI job can use the command as a gate. `--output jsonl` streams progress in a machine-readable format. A long-running worker pool can execute the suite, or the CI job can start a worker with `kitaru worker start`.

A practical setup uses a small cohort for pull requests and a larger traffic sample for scheduled runs. When you find a new failure, add its session to a new cohort version. Future runs will then include that case, although the evaluator still needs to detect the behavior for the CI gate to catch it.
