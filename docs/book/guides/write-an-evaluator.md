---
description: Turn your domain expert's criteria into a versioned evaluator: code checks, LLM judges, human calibration, and backfilling your history.
icon: chart-line
---

# Write an evaluator

Your domain expert already knows what a good run looks like. An [evaluator](../concepts/evaluators.md) is that knowledge as code: a small Python callable that reads one recorded session and writes named, typed verdicts. This guide takes you from criteria to a registered, calibrated evaluator you can trust to gate a release.

## From criteria to code

Start from what the expert actually says. "A good refund resolution issues exactly one refund, quotes the amount, and doesn't promise anything we don't do" is three checks:

```bash
kitaru evaluator scaffold refund-quality
```

```python
# refund_quality_evaluator.py
from kitaru.task.evaluator import EvaluationResult, SessionView


def evaluate(session: SessionView, **params) -> list[EvaluationResult]:
    refunds = [
        n for n in session.nodes
        if n.node_type == "tool_call" and n.tool_name == "refund_payment"
    ]
    reply = str(session.session.outputs or "")

    return [
        EvaluationResult(
            name="single_refund",
            score=len(refunds) == 1,
            passed=len(refunds) == 1,
            explanation=f"{len(refunds)} refund call(s)",
        ),
        EvaluationResult(
            name="amount_quoted",
            score="$" in reply,
            passed="$" in reply,
        ),
    ]
```

`SessionView` is the whole recording: `session.session` is the [session](../concepts/agents-and-sessions.md) with its inputs, outputs, and rollups; `session.nodes` is every model call and tool call with payloads. Return one result or a list; each becomes one stored evaluation. Pick the type by how you'll read a thousand of them: **numbers average, booleans count, labels diff as transitions, free text gets read.** Use `passed` for the verdict and `explanation` for the sentence you'll want when a gate goes red.

## An LLM judge is just an evaluator

For criteria that need judgment (tone, helpfulness, "did it actually answer the question"), call a model inside `evaluate`. Declare the dependency inline (PEP 723) and the worker builds the environment:

```python
# /// script
# dependencies = ["openai>=2"]
# ///
from kitaru.task.evaluator import EvaluationResult, SessionView
from openai import OpenAI


def evaluate(session: SessionView, **params) -> EvaluationResult:
    reply = str(session.session.outputs or "")
    verdict = OpenAI().responses.create(
        model=params.get("judge_model", "gpt-5-nano"),
        input=f"Customer-support reply:\n{reply}\n\n"
              "Is this reply professional and non-committal about policy? yes/no, one reason.",
    ).output_text
    ok = verdict.strip().lower().startswith("yes")
    return EvaluationResult(name="tone", score=ok, passed=ok, explanation=verdict)
```

The judge runs on your [worker](../concepts/workers.md), so its API key is worker environment configuration, the same place your agent's keys live. `params` (here `judge_model`) are set per replay or experiment via `EvaluatorConfig(evaluator="tone-judge", params={...})`, so one evaluator serves cheap-per-PR and thorough-nightly configurations.

## Test offline, then register

```bash
kitaru evaluator test refund_quality_evaluator.py --entrypoint evaluate
kitaru evaluator register refund-quality \
  --script refund_quality_evaluator.py --entrypoint evaluate
```

Evaluators are versioned: re-registering with `kitaru evaluator version register refund-quality --script ...` creates version 2, and every evaluation row records exactly which version wrote it. Tightening a criterion never rewrites history: old rows keep their provenance, and you can re-score any population with the new version.

## Calibrate against human judgment

Before an evaluator gates anything, check that it agrees with the human it's standing in for. The structured way to collect the human side is an [investigation](../concepts/investigations.md): pose the criteria as questions over a slice of sessions and the answers land as annotations, one per session per question. Labels can also be written directly as evaluations:

```python
from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import SessionEvaluationsRequest

await client.sessions.merge_evaluations(
    session_id,
    SessionEvaluationsRequest(evaluations=[
        EvaluationResult(name="human_tone", score=True, explanation="Good recovery"),
    ]),
)
```

Run the evaluator over the same slice, then compare the `tone` column against `human_tone` per session. Where they disagree, the explanation field tells you which side is confused. Fix the evaluator (new version) or the criteria, and repeat until the agreement rate earns your trust. The labeled slice is worth keeping as a [cohort](../concepts/cohorts.md): it's your calibration set for every future evaluator version.

## Backfill your history

Evaluators run against stored sessions, so day one of a new evaluator can cover months of history, recorded and [imported](import-langfuse-traces.md) alike. From the CLI, select by tag or take everything:

```bash
kitaru session evaluate --tag imported-baseline \
  --evaluator refund-quality@latest --wait
```

Or from the client, with explicit IDs:

```python
from kitaru.api_models.v1.evaluation import EvaluationBatchCreateRequest
from kitaru.api_models.v1.replay_config import EvaluatorConfig

job = await client.evaluations.create(
    EvaluationBatchCreateRequest(
        input_session_ids=all_session_ids,     # capped per request; batch as needed
        evaluators=[EvaluatorConfig(evaluator="refund-quality")],
    )
)
```

Each (session, evaluator) pair is its own task; one failure never stops the rest. When the backfill lands, the sessions where `passed=False` are your first triage queue, and the ones worth freezing into the cohort your next [experiment](regression-suite.md) runs against.
