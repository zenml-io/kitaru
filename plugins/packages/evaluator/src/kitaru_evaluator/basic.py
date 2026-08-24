"""Basic deterministic evaluators."""

from collections import Counter

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session_node import NodeType
from kitaru.task.evaluator import SessionView
from kitaru_evaluator.deterministic import sum_decimals


def cost(session: SessionView) -> EvaluationResult:
    """Report the total recorded cost of a session."""
    root_nodes = [node for node in session.nodes if node.parent_id is None]
    direct_nodes = [
        node
        for node in session.nodes
        if node.node_type is not NodeType.SPAN or node.parent_id is not None
    ]
    llm_costs_are_complete = all(
        node.cost is not None and node.cost.is_finite() and node.cost >= 0
        for node in session.nodes
        if node.node_type is NodeType.LLM_CALL
    )
    if (
        direct_nodes
        and llm_costs_are_complete
        and all(
            node.cost is not None and node.cost.is_finite() and node.cost >= 0
            for node in direct_nodes
        )
    ):
        recorded_cost = sum_decimals(
            [node.cost for node in direct_nodes if node.cost is not None]
        )
    else:
        session_cost = session.session.cost
        if (
            root_nodes
            and all(node.node_type is NodeType.SPAN for node in root_nodes)
            and all(
                node.cost is not None and node.cost.is_finite() and node.cost > 0
                for node in root_nodes
            )
        ):
            recorded_cost = sum_decimals(
                [node.cost for node in root_nodes if node.cost is not None]
            )
        elif (
            not any(node.cost is not None for node in session.nodes)
            and session_cost is not None
            and session_cost.is_finite()
            and session_cost >= 0
            and (not direct_nodes or session_cost > 0)
        ):
            recorded_cost = session_cost
        else:
            recorded_cost = None
    if recorded_cost is None:
        return EvaluationResult(
            name="cost",
            value="unavailable",
            explanation="The session has incomplete LLM cost information.",
        )
    return EvaluationResult(
        name="cost",
        score=float(recorded_cost),
        explanation=f"The session recorded a total cost of {recorded_cost}.",
    )


def latency(session: SessionView) -> EvaluationResult:
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


def tool_call_patterns(session: SessionView) -> EvaluationResult:
    """Classify whether a session repeats calls to the same tool."""
    tool_names = [
        node.tool_name or node.name
        for node in session.nodes
        if node.node_type is NodeType.TOOL_CALL
    ]
    counts = Counter(tool_names)
    repeated_calls = sum(count - 1 for count in counts.values() if count > 1)
    if not tool_names:
        pattern = "no-tool-calls"
    elif repeated_calls:
        pattern = "repeated-tools"
    else:
        pattern = "distinct-tools"
    return EvaluationResult(
        name="tool_call_pattern",
        score=float(repeated_calls),
        value=pattern,
        explanation=(
            f"Found {len(tool_names)} tool calls and {repeated_calls} repeated calls."
        ),
    )
