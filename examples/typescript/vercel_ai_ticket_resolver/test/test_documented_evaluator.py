"""Contract tests for the evaluator documented in the manual walkthrough."""

import json
import re
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from kitaru.api_models.v1.session import SessionDetailResponse
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
)
from kitaru.task.evaluator import SessionView
from kitaru.task.plugins import load_plugin_entrypoint

EXAMPLE_DIR = Path(__file__).parents[1]
README_PATH = EXAMPLE_DIR / "README.md"
GUIDED_README_PATH = EXAMPLE_DIR / "README_AGENT_GUIDED.md"
SOURCE_PATTERN = re.compile(
    r"<!-- documented-evaluator:start -->\n```python\n(.*?)\n```\n"
    r"<!-- documented-evaluator:end -->",
    re.DOTALL,
)

BASELINE_ACTIONS = {
    "ticket-001": ("refund", 98),
    "ticket-002": ("escalate", None),
    "ticket-003": ("escalate", None),
    "ticket-004": ("refund", 280),
    "ticket-005": ("escalate", None),
    "ticket-006": ("replacement", None),
    "ticket-007": ("refund", 120),
    "ticket-008": ("escalate", None),
    "ticket-009": ("refund", 80),
    "ticket-010": ("refund", 98),
}

ACTION_TO_TOOL = {
    "refund": "issue_refund",
    "replacement": "create_replacement",
    "escalate": "escalate_to_human",
}


@pytest.fixture(scope="module")
def evaluator(tmp_path_factory: pytest.TempPathFactory) -> Callable[..., Any]:
    """Extract and load the exact evaluator source shown to the reader."""
    readme = README_PATH.read_text()
    matches = SOURCE_PATTERN.findall(readme)
    assert len(matches) == 1, "README must contain one stable evaluator source block"
    path = tmp_path_factory.mktemp("documented-evaluator") / "evaluator.py"
    path.write_text(matches[0])
    return load_plugin_entrypoint(path, "evaluate", "Evaluator")


def _session(
    ticket_id: str,
    action: str,
    amount: int | None,
    *,
    imported: bool = False,
    node_status: NodeStatus = NodeStatus.COMPLETED,
    node_outputs: Any | None = None,
    extra_nodes: list[SessionNodeResponse] | None = None,
    session_outputs: Any | None = None,
) -> SessionView:
    resolution = {
        "action": action,
        "reason": "Synthetic fixture result.",
        "customer_reply": "The synthetic request was resolved.",
    }
    if amount is not None:
        resolution["amount"] = amount
    if imported:
        inputs: Any = {"turns": [{"inputs": {"ticket_id": ticket_id}}]}
        outputs: Any = {"turns": [{"outputs": resolution}]}
    else:
        inputs = (
            f"Ticket ID: {ticket_id}\n"
            "Customer: Test <test@example.test>\n"
            "Subject: Synthetic ticket\n\nSynthetic fixture body."
        )
        outputs = {"text": json.dumps(resolution)}
    if session_outputs is not None:
        outputs = session_outputs
    terminal_output = {"accepted": True}
    if amount is not None:
        terminal_output["amount"] = amount
    node = SessionNodeResponse.model_construct(
        id=uuid.uuid4(),
        session_id=uuid.uuid4(),
        index=1,
        node_type=NodeType.TOOL_CALL,
        status=node_status,
        tool_name=ACTION_TO_TOOL[action],
        outputs=terminal_output if node_outputs is None else node_outputs,
    )
    session = SessionDetailResponse.model_construct(inputs=inputs, outputs=outputs)
    return SessionView(session=session, nodes=[node, *(extra_nodes or [])])


def _terminal_node(tool_name: str, outputs: Any) -> SessionNodeResponse:
    return SessionNodeResponse.model_construct(
        id=uuid.uuid4(),
        session_id=uuid.uuid4(),
        index=2,
        node_type=NodeType.TOOL_CALL,
        status=NodeStatus.COMPLETED,
        tool_name=tool_name,
        outputs=outputs,
    )


def test_documented_evaluator_reports_the_deterministic_baseline(
    evaluator: Callable[..., Any],
) -> None:
    """The scripted baseline has exactly the two intended policy failures."""
    results = {
        ticket_id: evaluator(_session(ticket_id, action, amount)).passed
        for ticket_id, (action, amount) in BASELINE_ACTIONS.items()
    }

    assert {ticket_id for ticket_id, passed in results.items() if not passed} == {
        "ticket-004",
        "ticket-007",
    }
    assert sum(results.values()) == 8


def test_documented_evaluator_accepts_imported_and_vercel_replay_shapes(
    evaluator: Callable[..., Any],
) -> None:
    """One rubric supports the old imported shape and Vercel replay summaries."""
    imported = evaluator(_session("ticket-001", "refund", 98, imported=True))
    replay = evaluator(_session("ticket-004", "escalate", None))

    assert imported.passed is True
    assert replay.passed is True


@pytest.mark.parametrize(
    ("view", "message"),
    [
        (
            _session(
                "ticket-001",
                "refund",
                98,
                session_outputs={"finish_reason": "stop"},
            ),
            "outputs.text",
        ),
        (
            _session(
                "ticket-001",
                "refund",
                98,
                session_outputs={"text": "not-json"},
            ),
            "valid resolution JSON",
        ),
        (
            _session(
                "ticket-001",
                "refund",
                98,
                node_status=NodeStatus.FAILED,
            ),
            "exactly one accepted completed terminal action",
        ),
        (
            _session(
                "ticket-001",
                "refund",
                98,
                extra_nodes=[
                    _terminal_node(
                        "escalate_to_human",
                        {"accepted": True},
                    )
                ],
            ),
            "exactly one accepted completed terminal action",
        ),
        (
            _session(
                "ticket-001",
                "refund",
                98,
                node_outputs={"accepted": True, "amount": 50},
            ),
            "conflicts with the resolution amount",
        ),
    ],
)
def test_documented_evaluator_rejects_unverifiable_evidence(
    evaluator: Callable[..., Any],
    view: SessionView,
    message: str,
) -> None:
    """Missing, malformed, conflicting, and incomplete evidence is explicit."""
    with pytest.raises(ValueError, match=message):
        evaluator(view)


def test_walkthroughs_state_the_safety_and_recovery_contracts() -> None:
    """Keep high-risk boundaries visible in both reader paths."""
    manual = README_PATH.read_text()
    guided = GUIDED_README_PATH.read_text()

    for required in (
        ".state/baseline-sessions.json",
        "--fresh",
        "--adopt ticket-id=session-id",
        "scripted",
        "evaluator.py",
        "Python",
        "ticket-004",
        "ticket-007",
        "ticket-001",
        "ticket-009",
        "ticket-010",
    ):
        assert required in manual
        assert required in guided
    assert "does not ship `evaluator.py`" in manual
    assert "one question at a time" in guided
    assert "approval" in guided.lower()
    assert "terminal" in guided.lower()
