---
description: "Turn your domain expert's criteria into a versioned evaluator: code checks, LLM judges, human calibration, and backfilling your history."
icon: chart-line
---

# Write an evaluator

Your domain expert already knows what a good run looks like. An [evaluator](../concepts/evaluators.md) is that knowledge as code: a small Python callable that reads one recorded session and writes named, typed verdicts. This guide takes you from criteria to a registered, calibrated evaluator you can trust in a release gate.

For existing TypeScript evaluation code or native Mastra scorers, use the [TypeScript and Mastra evaluator guide](typescript-evaluators.md) to register a Python wrapper around a pinned Node artifact.

For typed questions about session content using TypeSafe's jev model, use [Judge evaluations](judge-evaluations.md). That guide covers credentials, question design, and validation against human labels without writing a custom evaluator.

## From criteria to code

Start from what the expert says. "A good refund resolution issues exactly one refund, quotes the amount, and does not promise anything we do not do" is three checks:

```bash
kitaru evaluator scaffold refund-quality
```

```python
# refund_quality_evaluator.py
from kitaru.task.evaluator import EvaluationResult, SessionView


def evaluate(session: SessionView, **params) -> list[EvaluationResult]:
    refunds = [
        n
        for n in session.nodes
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

`SessionView` is the whole recording: `session.session` is the [session](../concepts/agents-and-sessions.md) with its inputs, outputs, and rollups; `session.nodes` is every model call and tool call with payloads. Return one result or a list; each becomes one stored evaluation. `evaluate` can also be `async def`, for example to call a model client asynchronously, and the task process awaits it. Pick the type by how you will read a thousand of them: **numbers average, booleans count, labels diff as transitions, free text gets read.** Use `passed` for the verdict and `explanation` for the sentence you will want when a gate goes red.

## Inspecting session nodes

Each entry in `session.nodes` is a `SessionNodeResponse` ordered by start time. Its main attributes are:

| Attribute | Meaning |
| --- | --- |
| `node_type` | `"llm_call"`, `"tool_call"`, `"subagent_call"`, or `"span"` (`NodeType`). |
| `name` | Display name for the node. |
| `tool_name` | Tool name on `tool_call` nodes (`None` on other node types). |
| `status`, `error` | `"in_progress"`, `"completed"`, or `"failed"` (`NodeStatus`), plus the error string on failed nodes. |
| `inputs`, `outputs` | Recorded node payloads (populated on `SessionView`; structure depends on the adapter or importer). |
| `model`, `requested_model`, `model_provider`, `model_params` | Served model, requested model, provider, and request parameters on `llm_call` nodes. |
| `tokens`, `cost` | Token usage (`input_tokens`, `output_tokens`, `cached_input_tokens`, `cache_write_input_tokens`, `reasoning_tokens`) and call cost. |
| `attributes`, `metadata` | Span attributes (for example `{"mocked": True, "policy": "history"}` on replayed tool nodes) and arbitrary metadata. |
| `started_at`, `ended_at` | Node start and end timestamps (`datetime` or `None`). |
| `external_id`, `parent_external_id` | Source node identifier and parent node identifier for walking the call tree. |

Exact `inputs` and `outputs` shapes depend on the recording adapter or importer that produced the session. Inspect a real session first so your evaluator checks the actual keys your traces carry:

```bash
kitaru session read <session-id>
kitaru session nodes <session-id> --include-payloads -o json
```

Guard payload lookups against missing keys or unexpected types when reading tool arguments and results:

```python
from kitaru.task.evaluator import EvaluationResult, SessionView


def evaluate(session: SessionView, **params) -> EvaluationResult:
    del params
    refund_nodes = [
        node
        for node in session.nodes
        if node.node_type == "tool_call" and node.tool_name == "refund_payment"
    ]
    if not refund_nodes:
        return EvaluationResult(
            name="refund_within_limit",
            score=False,
            passed=False,
            explanation="No refund_payment tool call recorded.",
        )

    call = refund_nodes[0]
    args = call.inputs if isinstance(call.inputs, dict) else {}
    result = call.outputs if isinstance(call.outputs, dict) else {}
    amount = args.get("amount")
    accepted = result.get("accepted") is True
    passed = (
        call.status == "completed"
        and accepted
        and isinstance(amount, (int, float))
        and amount <= 100
    )

    return EvaluationResult(
        name="refund_within_limit",
        score=passed,
        passed=passed,
        explanation=f"status={call.status}, amount={amount!r}, accepted={accepted!r}",
    )
```

## Keep expected answers in session metadata

Store a reviewed expected answer, or gold label, in the original session's metadata, for example `{"expected_action": "refund"}`. Keep it out of the agent's inputs so the agent cannot use the answer to choose its action.

When evaluating a replay result, `SessionView.baseline` provides an `EvaluationBaselineContext` with the original session's `session_id` and `metadata`. This also applies when you evaluate that result session again later with `kitaru session evaluate`. The evaluated recording remains in `session.session` and `session.nodes`; its own metadata belongs to the new run and may differ from the original metadata.

Choose the expected answer from `session.baseline.metadata` when that context exists, otherwise from `session.session.metadata`. For an ordinary session, including a baseline evaluation, `session.baseline` is `None` unless that session was itself produced by an earlier replay.

The context follows one replay relationship. If A produced B and B produced C, evaluating B exposes A's metadata, while evaluating C exposes B's metadata. Labels are not inherited, and `FORCE` does not change that relationship. Keep the original labeled session as the baseline for comparisons, or explicitly maintain the same reviewed labels on each intermediate baseline.

For an agent whose structured output contains an `action` field:

```python
from kitaru.task.evaluator import EvaluationResult, SessionView


def evaluate(session: SessionView, **params) -> EvaluationResult:
    metadata = (
        session.baseline.metadata
        if session.baseline is not None
        else session.session.metadata
    )
    expected = metadata.get("expected_action")
    outputs = session.session.outputs
    actual = outputs.get("action") if isinstance(outputs, dict) else None
    if not isinstance(expected, str) or not isinstance(actual, str):
        return EvaluationResult(
            name="expected_action",
            value="unknown",
            explanation="Missing expected action or structured action output",
        )
    ok = actual == expected
    return EvaluationResult(name="expected_action", score=ok, passed=ok)
```

The baseline context contains metadata, not the baseline's full trace or outputs. It is available to the evaluator; the replayed agent receives the baseline inputs and replay ID, without baseline metadata or permission to read the baseline session.

{% hint style="warning" %}
Baseline metadata is read when the server supplies the evaluation task specification, not saved as a permanent label snapshot. Avoid editing gold labels while comparing a baseline and replay result. After changing a label, use `baseline_evaluation_mode=BaselineEvaluationMode.FORCE` (CLI: `--baseline-evaluation-mode force`) for the next replay: `IF_MISSING` can reuse an older baseline evaluation with the same evaluator version and parameters even though the metadata changed. See [Replay and overrides](replay-and-overrides.md).
{% endhint %}
## An LLM judge is an evaluator

For criteria that need judgment, such as tone, helpfulness, or whether the reply answered the question, call a model inside `evaluate`. Declare the dependency inline (PEP 723) and the worker builds the environment:

```python
# /// script
# dependencies = ["openai>=2"]
# ///
from kitaru.task.evaluator import EvaluationResult, SessionView
from openai import OpenAI


def evaluate(session: SessionView, **params) -> EvaluationResult:
    reply = str(session.session.outputs or "")
    verdict = (
        OpenAI()
        .responses.create(
            model=params.get("judge_model", "gpt-5-nano"),
            input=f"Customer-support reply:\n{reply}\n\n"
            "Is this reply professional and non-committal about policy? yes/no, one reason.",
        )
        .output_text
    )
    ok = verdict.strip().lower().startswith("yes")
    return EvaluationResult(name="tone", score=ok, passed=ok, explanation=verdict)
```

The judge runs on your [worker](../concepts/workers.md), so its API key is worker environment configuration, the same place your agent's keys live. `params` (here `judge_model`) are set per replay or experiment via `EvaluatorConfig(evaluator="tone-judge", params={...})`, so one evaluator serves cheap-per-PR and thorough-nightly configurations.

## Check it loads, then register

```bash
kitaru evaluator test refund_quality_evaluator.py --entrypoint evaluate
kitaru evaluator register refund-quality \
  --script refund_quality_evaluator.py --entrypoint evaluate
```

`kitaru evaluator test` imports the script in a child process and checks that the entrypoint matches the callable signature (`loaded: true`, `invoked: false`). It does **not** call `evaluate` or build a `SessionView`, so bugs in your node parsing or payload assumptions only appear when a worker runs the evaluator against a real session.

After registering, run the evaluator on one session you expect to fail and one session you expect to pass, then inspect the written verdicts:

```bash
kitaru session evaluate <expected-fail-session-id> \
  --evaluator refund-quality@1 --wait
kitaru session evaluate <expected-pass-session-id> \
  --evaluator refund-quality@1 --wait
kitaru evaluation list --size 20
```

An evaluator that calls a provider, like the OpenAI judge above, can declare its provider and a [connection schema](provider-connections.md) on register with `--provider` and `--connection-schema`.

Evaluators are versioned: re-registering with `kitaru evaluator version register refund-quality --script ...` creates version 2, and every evaluation row records exactly which version wrote it. Tightening a criterion never rewrites history: old rows keep their provenance, and you can evaluate any population again with the new version.

When `--description` is omitted on `kitaru evaluator register --script ...`, Kitaru reads a top-level `DESCRIPTION` string literal or the script's module docstring as the initial description. To edit or clear the description, metadata, logo URL, or connection schema later, run `kitaru evaluator update refund-quality --description "..."` (or pass `--description` explicitly on `kitaru evaluator version register`).

## Calibrate against human judgment

Before an evaluator gates anything, check that it agrees with the human it stands in for. The structured way to collect the human side is an [investigation](../concepts/investigations.md), and by design your coding assistant authors it for you: it picks the slice of sessions, poses the criteria as questions, and interviews you against the evidence. The answers land as annotations, one per session per question. Labels can also be written directly as evaluations:

```python
from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import SessionEvaluationsRequest

await client.sessions.create_evaluations(
    session_id,
    SessionEvaluationsRequest(
        evaluations=[
            EvaluationResult(
                name="human_tone", score=True, explanation="Good recovery"
            ),
        ]
    ),
)
```

Run the evaluator over the same slice, then compare the `tone` column against `human_tone` per session. Where they disagree, the explanation field tells you which side is confused. Fix the evaluator (new version) or the criteria, and repeat until the agreement rate earns your trust. The labeled slice is worth keeping as a [cohort](../concepts/cohorts.md): it is your calibration set for every future evaluator version.

## Backfill your history

Evaluators run against stored sessions, so day one of a new evaluator can cover months of history, recorded and [imported](import-langfuse-traces.md) alike. From the CLI, select by tag or take everything:

```bash
kitaru session evaluate --tag imported-baseline \
  --evaluator refund-quality@latest --wait
```

`kitaru session evaluate` automatically splits large selections into batches within the server's per-request limit (default 100 `(session, evaluator)` pairs, configurable with `KITARU_SERVER_EVALUATION_PAIR_LIMIT`) and combines the results when `--wait` is set. Direct Python SDK and REST callers send one request per call and should batch `input_session_ids` within that cap:

```python
from kitaru.api_models.v1.evaluation import EvaluationBatchCreateRequest
from kitaru.api_models.v1.plugin import EvaluatorConfig

job = await client.evaluations.create(
    EvaluationBatchCreateRequest(
        input_session_ids=all_session_ids,  # capped per request; batch as needed
        evaluators=[EvaluatorConfig(evaluator="refund-quality")],
    )
)
```

Each (session, evaluator) pair is its own task; one failure never stops the rest. When the backfill lands, the sessions where `passed=False` are your first triage queue, and the ones worth freezing into the cohort your next [experiment](regression-suite.md) runs against.
