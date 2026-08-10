"""Deterministic evaluator used by export contract tests."""

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.task.evaluator import SessionView


def evaluate(
    session: SessionView, *, expected: str, weight: float
) -> list[EvaluationResult]:
    """Score the fixture answer and expose a second numeric metric."""
    correct = str(session.session.outputs).endswith(expected)
    return [
        EvaluationResult(
            name="correctness", score=weight if correct else 0.0, passed=correct
        ),
        EvaluationResult(name="length", score=float(len(expected))),
    ]
