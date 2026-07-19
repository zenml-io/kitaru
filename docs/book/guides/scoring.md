---
description: Score stored executions and replay attempts with immutable evidence, score history, filters, and descriptive aggregates.
icon: chart-line
---

# Scoring Executions

Scoring lets you evaluate stored executions without running the agent again. A scorer reads the evidence Kitaru recorded for an execution, returns a bounded `Score`, and Kitaru saves the result as append-only history.

Use scoring when you want to answer questions like:

- Did this completed execution satisfy a rubric?
- Did a replay candidate improve on its original execution?
- Which stored executions have a valid score from a specific scorer revision?
- What was the exact score evidence for this attempt at the time it ran?

Scoring is different from replay:

1. Replay creates new executions by running code from a checkpoint forward.
2. Scoring reads stored execution evidence and writes score observations.
3. Replay can attach scorers after its child executions finish, but scoring itself does not submit the agent again.

## Define a score

`Score` is the public value returned by a scorer. Its `value` must be a finite number in the inclusive `[0.0, 1.0]` range.

```python
import kitaru

score = kitaru.Score(
    value=0.92,
    explanation="The answer cites the policy and gives a concrete next step.",
    metadata={"rubric": "support-answer-v1"},
)
```

Kitaru also accepts boolean shorthand: `True` becomes `1.0` and `False` becomes `0.0`. It rejects `None`, strings, NaN, infinity, and out-of-range numbers instead of clamping them.

Keep `metadata` non-sensitive. It is for small labels, booleans, enum values, and counts, not prompts, customer content, secrets, or file paths.

## Declare a scorer

Use `@kitaru.scorer(...)` to declare a callable as a scorer. The capability is required so the scorer's evidence boundary is explicit.

```python
import kitaru
from kitaru.scoring import ExecutionEvidence

@kitaru.scorer(capability="pure", name="has-policy-citation")
def has_policy_citation(evidence: ExecutionEvidence) -> kitaru.Score:
    output = evidence.outputs.get("result")
    text = output if isinstance(output, str) else ""
    has_citation = "policy" in text.lower()
    return kitaru.Score(
        value=has_citation,
        explanation="The final answer mentions the policy." if has_citation else "No policy citation found.",
    )
```

A scorer can return:

- `kitaru.Score`
- a finite number in `[0.0, 1.0]`
- `True` or `False`
- `None`, which records an `ABSTAINED` observation
- `ScoreObservationOutcome` for advanced cases where you want to return an explicit `ABSTAINED`, `BLOCKED`, or `ERROR` outcome

Kitaru snapshots the scorer declaration before evaluation. The snapshot includes the name, qualified name, captured source text when available, source hash, configuration hash, capability declaration, and output contract. It does not serialize the callable, pickle bytecode, or persist credentials.

## Pure versus grounded scorers

Most scorers should be pure. A pure scorer receives only frozen `ExecutionEvidence` from the stored execution. It cannot call live systems through Kitaru's scoring handle.

Use a grounded scorer only when it needs read-only context from an external system at scoring time, such as looking up a public ticket status or a stable policy document.

```python
import kitaru
from kitaru.scoring import (
    ExecutionEvidence,
    GroundedCapabilityDeclaration,
    GroundedWorld,
)

@kitaru.scorer(
    capability="grounded",
    name="ticket-still-open",
    grounded_capabilities=[
        GroundedCapabilityDeclaration(
            name="ticket_lookup",
            revision="v1",
            read_only=True,
        )
    ],
)
def ticket_still_open(
    evidence: ExecutionEvidence,
    world: GroundedWorld,
) -> kitaru.Score:
    ticket_id = str(evidence.outputs.get("ticket_id", ""))
    ticket = world.call(
        "ticket_lookup",
        ticket_id,
        request_summary={"ticket_id": ticket_id},
    )
    return kitaru.Score(value=ticket["status"] == "open")
```

Grounded access is default-deny. At evaluation time you must provide both:

1. A `GroundedPolicySnapshot` that lists allowed read-only capability declarations and exact or prefix-matched resource identifiers.
2. Runtime `GroundedCapability` callables that implement those read-only lookups.

If the scorer asks for an unavailable capability, a write-capable capability, a denied resource, or a call that times out, Kitaru records a `BLOCKED` observation instead of inventing a numeric score. Credentials are resolved by your runtime code and are not persisted in the score record. Kitaru stores bounded provenance such as the policy, call timing, resource identifier, request summary, and result summary.

## Evaluate stored executions

Use the collection API when you want to score one or many executions in one durable attempt:

```python
client = kitaru.KitaruClient()

result = client.executions.evaluate(
    ["exec-123", "exec-456"],
    [has_policy_citation],
    name="support-answer-rubric",
    idempotency_key="support-answer-rubric-2026-07-18",
)

print(result.experiment_id)
print(result.aggregate.scored)
```

Use the execution convenience when you already have one `Execution` object:

```python
execution = client.executions.get("exec-123")
result = execution.evaluate(has_policy_citation, name="single-execution-check")
```

Both entry points call the same evaluation service. They freeze evidence for the requested execution and scorer matrix, reserve one score attempt, append one observation per execution and scorer, and write an immutable descriptive aggregate.

The result is a `ScoreAttemptResult` with:

- `record`: the durable experiment record for the attempt
- `observations`: the appended `ScoreObservation` objects
- `aggregate`: descriptive counts and score summaries for the attempt
- `aggregate_reference`: the immutable aggregate artifact reference

`result.to_json()` returns Kitaru score models as JSON-serializable dictionaries.

## Attach scoring to replay

Registered Pydantic AI replay attempts can score verified replay children after the replay finishes:

```python
replay_result = agent.replay(
    "original-exec-id",
    at="draft_answer",
    idempotency_key="replay-with-policy-rubric",
    on_error="collect",
    uncovered_policy="fail",
    scorers=[has_policy_citation],
)
```

When `scorers` is non-empty, replay must wait for terminal child evidence. If you pass `wait=False`, Kitaru raises a usage error. If you omit `wait`, Kitaru waits so scoring can run against verified child executions.

Scoring starts only for replay children whose experiment membership was verified. The scoring service receives those child execution IDs and does not replay them again.

## Comparative scoring

Comparative scorers compare a candidate execution with its immediate original execution. Mark the scorer with `comparative=True` and accept candidate evidence first, original evidence second:

```python
from kitaru.scoring import ExecutionEvidence

@kitaru.scorer(
    capability="pure",
    name="shorter-than-original",
    comparative=True,
)
def shorter_than_original(
    candidate: ExecutionEvidence,
    original: ExecutionEvidence,
) -> kitaru.Score:
    candidate_text = str(candidate.outputs.get("result", ""))
    original_text = str(original.outputs.get("result", ""))
    return kitaru.Score(
        value=len(candidate_text) <= len(original_text),
        explanation="Candidate answer is no longer than the original.",
    )
```

For score-only evaluation, pass `comparative=True` if you want Kitaru to resolve original evidence for the target executions. For replay-attached scoring, Kitaru uses recorded replay lineage to pair each verified child with its immediate original.

If a comparative scorer runs on an execution that has no immediate original, Kitaru records an `ABSTAINED` observation. It does not create a fake baseline score.

## Read score history

Each scorer invocation produces an append-only `ScoreObservation` with one of four statuses:

- `SCORED`: contains a `Score`
- `ABSTAINED`: no numeric score, for intentional abstains or missing comparison evidence
- `BLOCKED`: no numeric score, for denied grounded access or timeout
- `ERROR`: no numeric score, for scorer exceptions or invalid returns

Read all matching observations through `execution.scores`:

```python
history = execution.scores.list(scorer_name="has-policy-citation")

latest = execution.scores.latest_valid(
    scorer_name="has-policy-citation",
    scorer_revision=history[-1].scorer.revision,
    scorer_configuration_hash=history[-1].scorer.configuration_hash,
)
```

`latest_valid()` only selects within one scorer revision and configuration. If multiple revisions or configurations match and you do not pass `scorer_revision` plus `scorer_configuration_hash`, Kitaru raises an ambiguity error instead of silently comparing incompatible scores.

You can also call the collection methods directly:

```python
client.executions.score_history(
    "exec-123",
    experiment_id=result.experiment_id,
    valid=True,
)

client.executions.latest_valid_score(
    "exec-123",
    scorer_name="has-policy-citation",
    scorer_revision=latest.scorer.revision,
    scorer_configuration_hash=latest.scorer.configuration_hash,
)
```

By default, history includes superseded observations. Pass `include_superseded=False` when you want the valid projection used by latest-score selectors.

## Filter execution lists by scores

`client.executions.list(...)` can narrow executions by score observations before applying ordinary execution predicates and pagination:

```python
from kitaru import ScoreFilter

matching = client.executions.list(
    flow="support_answer_flow",
    status="completed",
    score=ScoreFilter(
        scorer_name="has-policy-citation",
        minimum=0.8,
        valid=True,
        candidate_cap=500,
    ),
)
```

`ScoreFilter` fields are:

- `experiment_id`
- `scorer_name`
- `scorer_revision`
- `scorer_configuration_hash`
- `valid`
- `minimum`
- `maximum`
- `candidate_cap`

The score filter first queries score-observation metadata, deduplicates execution IDs up to `candidate_cap`, then applies execution filters such as `flow`, `status`, `limit`, `page`, and `size`. If the score filter matches more candidates than the cap, Kitaru asks you to narrow the filter or raise the cap. That prevents misleading sparse pages.

## Aggregates and immutability

Every completed scoring attempt writes an immutable aggregate. It records the exact selected observation IDs plus descriptive counts:

- planned
- scored
- abstained
- blocked
- error

For each scorer revision and configuration, the aggregate includes the denominator, mean, minimum, maximum, spread, and comparative candidate-minus-original deltas when paired scores are available.

Aggregates are descriptive evidence, not policy decisions. Kitaru does not emit pass/fail verdicts, thresholds, protection gates, historical-suite reruns, or CI enforcement from these scoring APIs. The exception is scorers attached to a registered-Agent replay attempt (`scorers=[...]`): those feed the attempt's [verdict](replay-and-overrides.md#verdicts-and-protections), where an objective and protections gate the experiment.

Score observations and aggregates are append-only. If you evaluate again, correct a score, or supersede an observation, Kitaru writes new records. Existing score evidence remains addressable by observation ID and aggregate reference.

## Evaluation spend

Scoring does not rerun the agent or replay machinery. Pure scorers read stored evidence only.

A grounded scorer can still spend money or consume quota if the runtime capability you provide calls an external service. Treat that as evaluation spend. Keep grounded policies narrow, use read-only capabilities, set timeouts, and keep retained request/result summaries bounded and non-sensitive.

## Reference

The generated SDK reference is available at [sdkdocs.kitaru.ai](https://sdkdocs.kitaru.ai). The main public types are:

- `kitaru.Score`
- `kitaru.scorer`
- `kitaru.KitaruClient.executions.evaluate(...)`
- `Execution.evaluate(...)`
- `Execution.scores`
- `ScoreObservation`
- `ScoreAttemptAggregate`
- `GroundedPolicySnapshot`
- `GroundedCapability`
