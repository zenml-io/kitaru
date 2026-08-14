# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Test-only evaluator for the canonical returns end-to-end contract."""

import json
from decimal import Decimal, InvalidOperation
from typing import Any

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session_node import NodeType
from kitaru.task.evaluator import SessionView

REVIEWED_OUTCOMES = {
    "ticket-001": ("refund", Decimal("98.00")),
    "ticket-002": ("escalate", None),
    "ticket-003": ("escalate", None),
    "ticket-004": ("escalate", None),
    "ticket-005": ("escalate", None),
    "ticket-006": ("replacement", None),
    "ticket-007": ("escalate", None),
    "ticket-008": ("escalate", None),
    "ticket-009": ("refund", Decimal("80.00")),
    "ticket-010": ("refund", Decimal("98.00")),
}

ACTION_TO_TOOL = {
    "refund": "issue_refund",
    "replacement": "create_replacement",
    "escalate": "escalate_to_human",
}


def _get_latest_turn(value: Any, field: str) -> Any:
    """Unwrap one field from the latest imported turn when present."""
    if isinstance(value, dict) and isinstance(value.get("turns"), list):
        turns = value["turns"]
        if not turns:
            raise ValueError("The imported session has no turns.")
        return turns[-1].get(field)
    return value


def _get_resolution(value: Any) -> dict[str, Any]:
    """Read the native or imported resolution output."""
    value = _get_latest_turn(value, "outputs")
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict) or not isinstance(value.get("action"), str):
        raise ValueError("Session outputs do not contain a resolution action.")
    return value


def _get_amount(value: Any) -> Decimal | None:
    """Parse one optional money amount."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("The recorded refund amount is invalid.") from exc


def _get_accepted_terminal_actions(
    session: SessionView,
) -> list[tuple[str, Decimal | None]]:
    """Read accepted terminal actions from recorded tool nodes."""
    tool_nodes = [
        node for node in session.nodes if node.node_type is NodeType.TOOL_CALL
    ]
    if not tool_nodes:
        raise ValueError("Session nodes do not contain tool-call evidence.")

    terminal_tools = set(ACTION_TO_TOOL.values())
    actions: list[tuple[str, Decimal | None]] = []
    for node in tool_nodes:
        if node.tool_name not in terminal_tools:
            continue
        output = node.outputs
        if isinstance(output, str):
            output = json.loads(output)
        if isinstance(output, dict) and output.get("accepted") is True:
            actions.append((node.tool_name, _get_amount(output.get("amount"))))
    return actions


def evaluate(session: SessionView) -> EvaluationResult:
    """Pass when the reported and accepted actions match the test oracle."""
    inputs = _get_latest_turn(session.session.inputs, "inputs")
    if not isinstance(inputs, dict) or not isinstance(inputs.get("ticket_id"), str):
        raise ValueError("Session inputs do not contain a ticket_id.")

    ticket_id = inputs["ticket_id"]
    if ticket_id not in REVIEWED_OUTCOMES:
        raise ValueError(f"No test outcome exists for {ticket_id}.")

    expected_action, expected_amount = REVIEWED_OUTCOMES[ticket_id]
    resolution = _get_resolution(session.session.outputs)
    actual_action = resolution["action"]
    actual_amount = _get_amount(resolution.get("amount"))
    accepted_actions = _get_accepted_terminal_actions(session)
    expected_tool = ACTION_TO_TOOL[expected_action]
    passed = (
        actual_action == expected_action
        and (expected_amount is None or actual_amount == expected_amount)
        and accepted_actions == [(expected_tool, expected_amount)]
    )
    return EvaluationResult(
        name="policy_correct",
        score=passed,
        passed=passed,
        explanation=(
            f"{ticket_id}: expected {expected_action} via {expected_tool}; "
            f"observed {actual_action} with accepted actions {accepted_actions}."
        ),
    )
