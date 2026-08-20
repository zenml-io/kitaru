"""Dummy evaluators producing every evaluation result shape."""

import hashlib
import time
from typing import Any

from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.evaluator import EvaluationResult, SessionView

GRADES = ("poor", "good", "excellent")


def _float_param(params: dict[str, Any], name: str, default: float) -> float:
    """Read a float parameter with a default."""
    value = params.get(name, default)
    return float(value)


def _apply_failure_modes(session: SessionView, params: dict[str, Any]) -> None:
    """Sleep and fail deterministically as configured by the parameters."""
    sleep_ms = _float_param(params, "sleep_ms", 0.0)
    if sleep_ms > 0:
        time.sleep(sleep_ms / 1000)
    failure_rate = _float_param(params, "failure_rate", 0.0)
    if failure_rate <= 0:
        return
    digest = hashlib.sha256(f"{session.session.id}:evaluator-fail".encode()).hexdigest()
    if int(digest[:8], 16) % 10_000 < failure_rate * 10_000:
        raise RuntimeError("Simulated evaluator failure")


def _get_token_total(session: SessionView) -> float:
    """Sum the session's input and output tokens."""
    tokens = session.session.tokens
    if tokens is None:
        return 0.0
    return float((tokens.input_tokens or 0) + (tokens.output_tokens or 0))


def evaluate_outcome(session: SessionView, **params: Any) -> list[EvaluationResult]:
    """Score session completion and node health as pass or fail verdicts."""
    _apply_failure_modes(session, params)
    completed = (
        session.session.status == SessionStatus.COMPLETED
        and session.session.outputs is not None
    )
    failed_nodes = sum(1 for node in session.nodes if node.status == NodeStatus.FAILED)
    return [
        EvaluationResult(
            name="completed",
            score=completed,
            passed=completed,
            explanation=f"session ended {session.session.status}",
        ),
        EvaluationResult(
            name="healthy_nodes",
            score=failed_nodes == 0,
            passed=failed_nodes == 0,
            explanation=f"{failed_nodes} failed node(s)",
        ),
    ]


def evaluate_expected_match(session: SessionView, **params: Any) -> EvaluationResult:
    """Score the session outputs against the expected outputs in its metadata."""
    _apply_failure_modes(session, params)
    expected = session.session.metadata.get("expected")
    if expected is None:
        return EvaluationResult(
            name="expected_match",
            value="skipped",
            explanation="no expected outputs recorded",
        )
    matched = session.session.outputs == expected
    return EvaluationResult(
        name="expected_match",
        score=matched,
        passed=matched,
        explanation="outputs matched expected"
        if matched
        else "outputs did not match expected",
    )


def evaluate_efficiency(session: SessionView, **params: Any) -> list[EvaluationResult]:
    """Score token, cost, and latency usage as numeric values."""
    _apply_failure_modes(session, params)
    token_budget = _float_param(params, "token_budget", 4000.0)
    token_total = _get_token_total(session)
    cost = float(session.session.cost or 0)
    latency = 0.0
    if session.session.started_at and session.session.ended_at:
        latency = (
            session.session.ended_at - session.session.started_at
        ).total_seconds()
    tool_calls = sum(
        1 for node in session.nodes if node.node_type == NodeType.TOOL_CALL
    )
    return [
        EvaluationResult(
            name="token_total",
            score=token_total,
            explanation=f"budget {token_budget:.0f}",
        ),
        EvaluationResult(name="cost", score=cost),
        EvaluationResult(name="latency_seconds", score=latency),
        EvaluationResult(name="tool_call_count", score=float(tool_calls)),
        EvaluationResult(
            name="token_efficiency",
            score=max(0.0, 1.0 - token_total / token_budget),
            passed=token_total <= token_budget,
        ),
    ]


def evaluate_grade(session: SessionView, **params: Any) -> EvaluationResult:
    """Grade the session as a category with a numeric score."""
    _apply_failure_modes(session, params)
    token_budget = _float_param(params, "token_budget", 4000.0)
    completed = session.session.status == SessionStatus.COMPLETED
    token_total = _get_token_total(session)
    score = 0.0
    if completed:
        score = 0.5 + 0.5 * max(0.0, 1.0 - token_total / token_budget)
    grade = GRADES[min(len(GRADES) - 1, int(score * len(GRADES)))]
    return EvaluationResult(
        name="grade",
        score=score,
        value=grade,
        passed=grade != "poor",
        explanation=f"completed={completed} tokens={token_total:.0f}",
    )


def evaluate_notes(session: SessionView, **params: Any) -> EvaluationResult:
    """Describe the session as a plain text note."""
    _apply_failure_modes(session, params)
    models = sorted({node.model for node in session.nodes if node.model})
    tools = sorted({node.tool_name for node in session.nodes if node.tool_name})
    return EvaluationResult(
        name="notes",
        value=(
            f"{session.session.status} session with {len(session.nodes)} nodes, "
            f"models {', '.join(models) or 'none'}, "
            f"tools {', '.join(tools) or 'none'}"
        ),
    )


def evaluate_suite(session: SessionView, **params: Any) -> list[EvaluationResult]:
    """Run every dummy evaluator at once."""
    _apply_failure_modes(session, params)
    # Strip the failure parameters so the delegated evaluators do not sleep
    # or fail a second time.
    inner = {k: v for k, v in params.items() if k not in ("sleep_ms", "failure_rate")}
    return [
        *evaluate_outcome(session, **inner),
        evaluate_expected_match(session, **inner),
        *evaluate_efficiency(session, **inner),
        evaluate_grade(session, **inner),
        evaluate_notes(session, **inner),
    ]
