"""A local scorer compatible with DABstep v1 development-answer matching."""

from typing import Any

from examples.python.dabstep_coding_agent.evaluator import matches_answer


def score_answer(answer: str, expected: str) -> dict[str, Any]:
    """Score one DABstep answer without exposing the expected answer to an agent.

    This implements the public DABstep v1 answer-comparison behavior locally so
    the wrapper can retain a small, inspectable receipt. It is for development
    tasks and workflow validation, not a claim of uncontaminated benchmark
    capability.

    Args:
        answer: The answer written by the coding agent.
        expected: The answer held outside the agent-visible workdir.

    Returns:
        A JSON-serializable scoring receipt.
    """
    normalized_answer = answer.strip().lower()
    normalized_expected = expected.strip().lower()
    passed = matches_answer(normalized_answer, normalized_expected)
    return {
        "evaluator": "dabstep-v1-compatible",
        "passed": passed,
        "answer_length": len(answer),
        "expected_answer_length": len(expected),
    }
