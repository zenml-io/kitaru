# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Evaluate whether a support response matches the expected outcome."""

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.task.evaluator import SessionView


def evaluate(session: SessionView) -> EvaluationResult:
    """Compare the session output with its expected output.

    Args:
        session: Session and trace nodes to evaluate.

    Returns:
        Exact outcome match result.
    """
    actual = session.session.outputs
    expected = session.session.expected
    if expected is None and isinstance(session.session.inputs, dict):
        expected = session.session.inputs.get("expected_output")
    passed = actual == expected
    return EvaluationResult(
        name="expected_outcome",
        score=passed,
        passed=passed,
        explanation=(
            "Response matches the expected support outcome."
            if passed
            else f"Expected {expected!r}, received {actual!r}."
        ),
    )
