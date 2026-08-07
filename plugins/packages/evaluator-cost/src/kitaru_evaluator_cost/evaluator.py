"""Recorded-cost evaluator."""

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.task.evaluator import SessionView


def evaluate(session: SessionView) -> EvaluationResult:
    """Report the total recorded cost of a session."""
    recorded_cost = session.session.cost
    if recorded_cost is None:
        return EvaluationResult(
            name="cost",
            score=0.0,
            explanation="The session has no recorded cost.",
        )
    return EvaluationResult(
        name="cost",
        score=float(recorded_cost),
        explanation=f"The session recorded a total cost of {recorded_cost}.",
    )
