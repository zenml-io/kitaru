"""Deterministic correctness evaluator for DABstep V0 sessions."""

import math
import re
from difflib import SequenceMatcher

from kitaru.task.evaluator import EvaluationResult, SessionView


def evaluate(
    session: SessionView, expected_by_task: dict[str, str] | None = None
) -> EvaluationResult:
    """Compare the recorded answer with the private answer for its task.

    Args:
        session: Recorded DABstep coding-agent session.
        expected_by_task: Expected answers keyed by DABstep task ID. These
            evaluator parameters are not passed to the agent process.

    Returns:
        A deterministic correctness verdict using DABstep V1-style numeric and
        normalized-text matching.
    """
    if expected_by_task is None:
        raise ValueError("expected_by_task is required")

    inputs = session.session.inputs
    outputs = session.session.outputs
    if not isinstance(inputs, dict) or not isinstance(outputs, dict):
        return EvaluationResult(
            name="dabstep_answer_correct",
            score=False,
            passed=False,
            explanation="The session has no structured DABstep inputs or outputs.",
        )

    task_id = str(inputs.get("task_id", ""))
    expected = expected_by_task.get(task_id)
    observed = outputs.get("answer")
    if expected is None:
        raise ValueError(f"No expected answer configured for task {task_id!r}")
    if not isinstance(observed, str):
        return EvaluationResult(
            name="dabstep_answer_correct",
            score=False,
            passed=False,
            explanation=f"Task {task_id} produced no string answer.",
        )

    passed = matches_answer(observed.strip().lower(), expected.strip().lower())
    return EvaluationResult(
        name="dabstep_answer_correct",
        score=passed,
        passed=passed,
        explanation=(
            f"Task {task_id} answer matched the configured DABstep answer."
            if passed
            else f"Task {task_id} answer did not match the configured DABstep answer."
        ),
    )


def matches_answer(answer: str, expected: str) -> bool:
    """Match answers without importing project-local code on remote workers."""
    if "not applicable" in {answer, expected}:
        return answer == expected
    if _is_numeric_with_commas(answer) or _is_numeric_with_commas(expected):
        answer_number = _first_number(answer)
        expected_number = _first_number(expected)
        return (
            answer_number is not None
            and expected_number is not None
            and _numbers_match(answer_number, expected_number)
        )
    if _is_list(answer) or _is_list(expected):
        answer_items = _list_items(answer)
        expected_items = _list_items(expected)
        return len(answer_items) == len(expected_items) and all(
            matches_answer(item, expected_item)
            for item, expected_item in zip(answer_items, expected_items, strict=True)
        )

    answer_number = _first_number(answer)
    expected_number = _first_number(expected)
    if answer_number is not None and expected_number is not None:
        return _numbers_match(answer_number, expected_number)

    answer_compact = re.sub(r"[^\w]", "", answer)
    expected_compact = re.sub(r"[^\w]", "", expected)
    if answer_compact == expected_compact:
        return True

    return SequenceMatcher(None, answer, expected).ratio() > 0.95


def _is_list(value: str) -> bool:
    return ";" in value or "," in value


def _is_numeric_with_commas(value: str) -> bool:
    return bool(
        re.fullmatch(
            r"\$?(?:\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+[.,]\d+)",
            value.strip(),
        )
    )


def _list_items(value: str) -> list[str]:
    return sorted(
        item.strip()
        for item in re.split(r"[,;]", re.sub(r"^\[|\]$", "", value.strip()))
        if item.strip()
    )


def _first_number(value: str) -> float | None:
    match = re.search(
        r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)",
        value.replace(",", "").replace("$", ""),
    )
    return float(match.group()) if match else None


def _numbers_match(answer: float, expected: float) -> bool:
    if answer == expected:
        return True
    if answer < 1 and expected < 1:
        return math.isclose(answer, expected, rel_tol=1e-4, abs_tol=1e-4)
    answer_places = _decimal_places(answer)
    expected_places = _decimal_places(expected)
    if round(answer, min(answer_places, expected_places)) == round(
        expected, min(answer_places, expected_places)
    ):
        return True
    return math.isclose(answer, expected, rel_tol=1e-4, abs_tol=1e-4)


def _decimal_places(value: float) -> int:
    text = str(value)
    return len(text.split(".", maxsplit=1)[1]) if "." in text else 0
