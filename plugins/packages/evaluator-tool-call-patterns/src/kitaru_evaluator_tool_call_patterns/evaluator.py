"""Tool-call pattern evaluator."""

from collections import Counter

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session_node import NodeType
from kitaru.task.evaluator import SessionView


def evaluate(session: SessionView) -> EvaluationResult:
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
