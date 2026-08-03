---
description: Evaluators score sessions; evaluations are the rows they write. Numbers average, words count, free text gets read.
icon: chart-line
---

# Evaluators & Evaluations

Replay tells you what a change *did*; evaluators tell you whether it
*helped*. An **evaluator** is a small piece of your code that reads one
[session](agents-and-sessions.md) — the recording, node by node — and
writes one or more **evaluations**: named, typed verdicts that Kitaru
stores against the session.

Because evaluators run against recorded sessions, they score baselines,
replays, and imported traces identically. The same evaluator that grades
today's production traffic grades the fork you're thinking about shipping.

## The evaluator contract

An evaluator is a callable — a single Python file or an installable
package — that receives the full session and returns results:

```python
"""refund_check.py — did the agent actually issue the refund?"""

from kitaru.task.evaluator import EvaluationResult, SessionView


def evaluate(session: SessionView, **params) -> EvaluationResult:
    refund_calls = [
        node for node in session.nodes
        if node.node_type == "tool_call" and node.tool_name == "refund_payment"
    ]
    return EvaluationResult(
        name="refund_issued",
        score=bool(refund_calls),
        passed=bool(refund_calls),
        explanation=f"{len(refund_calls)} refund tool call(s) in the session",
    )
```

`SessionView` gives you the session and all its nodes with payloads. Return
one `EvaluationResult` or a list — each becomes one stored evaluation row.
`params` are per-run knobs you pass when you attach the evaluator to a
replay or experiment.

Scaffold, exercise, and register it with the CLI:

```bash
kitaru evaluator scaffold refund-check          # writes refund_check_evaluator.py
kitaru evaluator test refund_check_evaluator.py --entrypoint evaluate
kitaru evaluator register refund-check \
  --script refund_check_evaluator.py --entrypoint evaluate
```

Evaluators are versioned like agents: registering again with
`kitaru evaluator version register` creates version 2, and every stored
evaluation remembers exactly which evaluator version wrote it. An LLM judge
is just an evaluator that calls a model inside `evaluate` — same contract,
same rows. The walkthrough is in
[Write an evaluator](../guides/write-an-evaluator.md).

## The evaluation row

One evaluation is one named result for one session. The data type is
derived from what you set, never declared:

| You set | Stored type | How to read a batch of them |
|---|---|---|
| `score=0.87` | `float` | numbers average |
| `score=True` | `bool` | pass rates count |
| `value="escalated"` | `str` | free text gets read |
| `score=0.9, value="polite"` | `categorical` | labels count, transitions diff |

`passed` is an independent optional verdict — a threshold you decided in
the evaluator, not something derived from the score — and `explanation`
says why, which is what you'll actually read when a regression gate goes
red.

## Human labels are evaluations too

There is no separate labeling system. A human verdict is an evaluation
written directly onto the session:

```python
from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import SessionEvaluationsRequest

await client.sessions.merge_evaluations(
    session_id,
    SessionEvaluationsRequest(
        evaluations=[
            EvaluationResult(name="human_quality", score=True,
                             explanation="Correct refund, good tone"),
        ]
    ),
)
```

Manual evaluations upsert by name — re-sending `human_quality` overwrites
the earlier verdict. Rows written by evaluator runs carry their evaluator
version and task; manual rows carry neither, which is how you tell them
apart. Comparing your evaluator's column against the human column on the
same sessions is how you calibrate the evaluator before you let it gate
anything.

## Running evaluators in batch

Score existing sessions without replaying anything:

```python
from kitaru.api_models.v1.evaluation import EvaluationBatchCreateRequest
from kitaru.api_models.v1.replay_config import EvaluatorConfig

job = await client.evaluations.create(
    EvaluationBatchCreateRequest(
        input_session_ids=session_ids,
        evaluators=[EvaluatorConfig(evaluator="refund-check")],
    )
)
```

Each (session, evaluator) pair runs as its own task on a
[worker](workers.md) — in your environment, next to your credentials — and
one failed pair never cancels the rest. Read results back with
`client.evaluations.list(...)`, filtered by session.

Evaluators are also how [replays](replay.md) and
[experiments](experiments.md) get their numbers: both require at least one
evaluator, so a re-run is never just "it finished" — it's scored the moment
it lands.
