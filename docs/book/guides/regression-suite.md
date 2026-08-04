---
description: Freeze a cohort of real runs, replay it against every change, and gate your CI on what production already taught you.
icon: vials
---

# Build a regression suite from production

One replay tells you about one run. A hundred replays tell you whether a
change is safe to ship. The insight this guide operationalizes: **your
production traces are your test suite** — every recorded or imported
session is a test case with real inputs, a real decision path, and a
known outcome. You never have to write synthetic fixtures again; you have
to *select* well.

The loop: select a cohort → freeze it → make the change an experiment →
replay and score → keep the winner → gate on it.

## 1. Select the population

Pick the sessions that represent what you can't afford to break. List and
filter by agent, status, or time; or start from the sessions a specific
failure taught you about:

```python
import asyncio
from kitaru.client import KitaruAPIClient
from kitaru.api_models.v1.filter import FilterCondition, FilterOp
from kitaru.api_models.v1.session import SessionListParams

async def main() -> None:
    client = KitaruAPIClient.from_env()
    refund_runs = [
        s.id
        async for s in client.sessions.iter(SessionListParams(
            filter=FilterCondition(field="agent_id", op=FilterOp.EQ, value=AGENT_ID),
            size=100,
        ))
    ][:50]
```

A good starting population is one week of traffic plus every session that
ever triggered an incident. [Imported sessions](import-langfuse-traces.md)
qualify exactly like recorded ones — your Langfuse history from before
Kitaru existed is admissible evidence, and if you imported it with
`--tag`, the tag *is* your selection
(`kitaru session list --filter '{"field": "tag", "op": "eq", "value": "imported-baseline"}'`).

## 2. Freeze it into a cohort version

From the CLI:

```bash
kitaru cohort create refund-regression --agent support-agent
kitaru cohort version create refund-regression \
  --add-session <id> --add-session <id> \
  --display-version week-32
```

Or from the client, straight from the selection above:

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

[Cohort versions are immutable](../concepts/cohorts.md). That's what makes
week-over-week numbers comparable: version 1 is always the same 50
sessions, and "the suite got harder" is an explicit new version, not a
silent drift.

## 3. Make the change an experiment

The experiment holds everything about the change *except* the population:

```python
from kitaru.api_models.v1.experiment import ExperimentCreateRequest
from kitaru.api_models.v1.replay_config import (
    EvaluatorConfig, HistoryConfig, ReplayOverride, ToolPolicy,
)

experiment = await client.experiments.create(
    ExperimentCreateRequest(
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

Testing a **code** change instead? Leave `override` out entirely and
register your branch as a new agent version — the run supplies it next.
The `history` policy scoped to `cohort_version` answers tool calls from
any recording in the cohort, and `on_miss="fail"` keeps 50 replays from
touching a single live system.

The CLI form takes the override and tool policy as JSON:

```bash
kitaru experiment create cheaper-model \
  --evaluator refund-check@latest --evaluator tone-judge@latest \
  --override '{"model": {"openai:gpt-5.4": "openai:gpt-5-nano"}}' \
  --tool-policy '{"default": {"type": "history", "scope": "cohort_version", "on_miss": "fail"}}'
```

## 4. Run it and read it

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

Workers fan out one replay per session; `run.progress` counts them
through. When the run settles, every replay has a result session and both
sides carry evaluations. Aggregate them the way the data types suggest —
numbers average, booleans count into pass rates, labels diff as
transitions:

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

Add cost from the result sessions' rollups and the headline writes
itself: *"gpt-5-nano held on 47 of 50 refund tickets and cut cost 41%; the
3 misses are sessions #…, #…, #… — read those."* The misses are the
point: each one is a concrete recorded run you can
[replay and step through](replay-and-overrides.md), not a percentage.

## 5. Gate on it

The cohort that caught a failure becomes the gate that keeps it caught.
In CI: register the PR's code as an agent version, start a run against
the frozen cohort version, and fail the build on the result:

```bash
kitaru agent version register support-agent --command "python support.py"
kitaru experiment run start cheaper-model \
  --cohort-version <cohort-version-id> \
  --agent support-agent@<new-version> \
  --evaluate-baselines --wait --timeout 1800
```

`--wait` blocks until the run settles and exits nonzero on failure, so
the command doubles as the CI gate; `--output jsonl` streams progress in
a form your pipeline can parse. A worker executes the suite — either a
long-running pool, or one started in the CI job itself
(`kitaru worker start`).

Keep two shapes: a small cohort per PR (the incidents plus a dozen
representative runs), and the wide week-of-traffic sweep nightly. When a
new failure ships anyway, the postmortem's last step is one line: add its
session to the cohort — that's a new version — and it can never ship
again unnoticed.
