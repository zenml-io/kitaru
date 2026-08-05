# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Evaluate whether a returns resolution matches the reviewed policy outcome."""

import json
from typing import Any

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.task.evaluator import SessionView

EXPECTED_ACTIONS = {
    "ticket-001": "refund",
    "ticket-002": "escalate",
    "ticket-003": "escalate",
    "ticket-004": "escalate",
    "ticket-005": "escalate",
    "ticket-006": "replacement",
    "ticket-007": "escalate",
    "ticket-008": "escalate",
    "ticket-009": "refund",
    "ticket-010": "refund",
}


def _get_latest_turn(value: Any, field: str) -> Any:
    """Unwrap one field from the latest imported turn when present."""
    if isinstance(value, dict) and isinstance(value.get("turns"), list):
        turns = value["turns"]
        if not turns:
            raise ValueError("The imported session has no turns.")
        return turns[-1].get(field)
    return value


def _get_ticket_id(inputs: Any) -> str:
    """Read the synthetic ticket identifier from recorded inputs."""
    value = _get_latest_turn(inputs, "inputs")
    if not isinstance(value, dict) or not isinstance(value.get("ticket_id"), str):
        raise ValueError("Session inputs do not contain a ticket_id.")
    return value["ticket_id"]


def _get_action(outputs: Any) -> str:
    """Read the terminal action from native or imported outputs."""
    value = _get_latest_turn(outputs, "outputs")
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict) or not isinstance(value.get("action"), str):
        raise ValueError("Session outputs do not contain a resolution action.")
    return value["action"]


def evaluate(session: SessionView) -> EvaluationResult:
    """Pass when the terminal action matches the reviewed ticket outcome."""
    ticket_id = _get_ticket_id(session.session.inputs)
    expected = EXPECTED_ACTIONS[ticket_id]
    actual = _get_action(session.session.outputs)
    passed = actual == expected
    return EvaluationResult(
        name="policy_correct",
        score=passed,
        passed=passed,
        explanation=(f"{ticket_id}: expected {expected}, observed {actual}."),
    )
