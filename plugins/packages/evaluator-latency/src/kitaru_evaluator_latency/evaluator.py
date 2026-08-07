"""Session-latency evaluator."""

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.task.evaluator import SessionView


def evaluate(session: SessionView) -> EvaluationResult:
    """Measure the wall-clock duration of a session in seconds."""
    started_at = session.session.started_at
    ended_at = session.session.ended_at
    if started_at is None or ended_at is None:
        return EvaluationResult(
            name="latency_seconds",
            score=0.0,
            explanation="The session has no complete timing information.",
        )
    duration = max((ended_at - started_at).total_seconds(), 0.0)
    return EvaluationResult(
        name="latency_seconds",
        score=duration,
        explanation=f"The session ran for {duration:.3f} seconds.",
    )
