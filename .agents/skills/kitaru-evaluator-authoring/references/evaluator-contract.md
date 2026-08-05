# Kitaru evaluator contract

An evaluator receives a `SessionView` containing a `SessionResponse` and ordered `SessionNodeResponse` values with payloads. It returns one `EvaluationResult` or a non-empty list of results with unique names.

## Minimal deterministic evaluator

```python
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Evaluate one reviewed behavior."""

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.task.evaluator import SessionView


def evaluate(session: SessionView) -> EvaluationResult:
    """Return whether the session satisfies the reviewed behavior."""
    observed = session.session.outputs
    passed = observed == "expected"
    return EvaluationResult(
        name="reviewed_behavior",
        score=passed,
        passed=passed,
        explanation=f"Expected 'expected', observed {observed!r}.",
    )
```

## Result types

- Boolean: set `score` to `True` or `False`; set `passed` when it is a verdict.
- Numeric: set `score` to a finite float; set `passed` only when a reviewed threshold exists.
- String: set `value` to readable text.
- Categorical: set both a numeric or boolean `score` and a string `value`.

At least one of `score` or `value` is required. Keep `name` stable because it becomes the evaluation column used by filters and comparisons.

## Session evidence

Use `session.session` for canonical session fields. Use `session.nodes` for ordered LLM, tool, subagent, and span evidence. Inspect node types and explicit fields rather than relying on incidental list positions.

Imported sessions may wrap inputs and outputs in a `turns` list. Replayed sessions may contain the native value directly. Normalize both shapes when the importer and adapter produce them.

## Errors and missing data

Raise a clear error when evaluation is impossible and the job should fail. Return an explicit result when missing data is itself a reviewed failure condition. Do not silently convert missing costs, outputs, or evidence into a passing score.

## Local validation

```bash
uv run kitaru evaluator test evaluator.py --entrypoint evaluate
```

This validates loading and the callable signature in a child process. Behavioral tests must call the evaluator with representative `SessionView` objects.
