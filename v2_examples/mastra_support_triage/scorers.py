"""Deterministic evaluator for the Mastra support-triage demo."""

import json

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.evaluator import SessionView

# A live model phrases the same triage decision differently on every run, so each
# keyword group below accepts the spellings a model actually returns. The bar
# stays high on purpose: a demo that shows evaluation catching regressions cannot
# tell a regression from a rephrasing once the bar is tuned down to whatever the
# current model scores. The sibling vercel_ai_support_triage example scores the
# same five groups, so keep both in step when a spelling variant is added.
DECISION_THRESHOLD = 0.8


def _decision_structure(session: SessionView) -> float:
    text = json.dumps(session.session.outputs, sort_keys=True).lower()
    required = (
        ("decision",),
        ("evidence",),
        ("risk",),
        ("nextaction", "next_action", "next action", "next step"),
        ("refundreview", "refund_review", "refund review", "refund-review"),
    )
    return sum(
        any(value in text for value in alternatives) for alternatives in required
    ) / len(required)


def _trace_completeness(session: SessionView) -> float:
    roots = [
        node
        for node in session.nodes
        if node.node_type is NodeType.SPAN and node.parent_index is None
    ]
    llm_nodes = [node for node in session.nodes if node.node_type is NodeType.LLM_CALL]
    tool_names = {
        node.tool_name
        for node in session.nodes
        if node.node_type is NodeType.TOOL_CALL and node.status is NodeStatus.COMPLETED
    }
    checks = (
        len(roots) == 1,
        len(llm_nodes) >= 2,
        {"lookupAccount", "lookupOrder", "queueRefundReview"} <= tool_names,
    )
    return sum(checks) / len(checks)


def _side_effect_safety(session: SessionView) -> float:
    actions = [
        node
        for node in session.nodes
        if node.node_type is NodeType.TOOL_CALL
        and node.tool_name == "queueRefundReview"
    ]
    if len(actions) != 1 or actions[0].status is not NodeStatus.COMPLETED:
        return 0.0
    if session.session.origin is SessionOrigin.REPLAY:
        attributes = actions[0].attributes or {}
        return float(
            attributes.get("mocked") is True and attributes.get("policy") == "history"
        )
    return 1.0


def evaluate(session: SessionView) -> list[EvaluationResult]:
    """Evaluate decision shape, trace completeness, and side-effect safety."""
    decision = _decision_structure(session)
    trace = _trace_completeness(session)
    side_effect = _side_effect_safety(session)
    return [
        EvaluationResult(
            name="decision_structure",
            score=decision,
            passed=decision >= DECISION_THRESHOLD,
        ),
        EvaluationResult(name="trace_completeness", score=trace, passed=trace == 1.0),
        EvaluationResult(
            name="side_effect_safety", score=side_effect, passed=side_effect == 1.0
        ),
    ]
