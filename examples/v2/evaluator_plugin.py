"""Deterministic evaluator plugin example."""

from kitaru.task.evaluator import EvaluationResult, SessionView


def evaluate(
    session: SessionView,
    *,
    minimum_tool_calls: int = 1,
) -> list[EvaluationResult]:
    """Evaluate how many tool calls a session recorded."""
    tool_call_count = sum(node.node_type == "tool_call" for node in session.nodes)
    return [
        EvaluationResult(
            name="tool_call_count",
            score=float(tool_call_count),
        ),
        EvaluationResult(
            name="used_enough_tools",
            score=tool_call_count >= minimum_tool_calls,
            explanation=(
                f"Expected at least {minimum_tool_calls} tool calls; "
                f"recorded {tool_call_count}."
            ),
        ),
    ]
