#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Low-cost signals for selecting sessions to review."""

from collections import Counter

from kitaru.api_models.v1.evaluation import EvaluationResult
from kitaru.api_models.v1.session_node import NodeType
from kitaru.task.evaluator import SessionView


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


def cost(session: SessionView) -> EvaluationResult:
    """Report the total recorded cost of a session."""
    recorded_cost = session.session.cost
    if recorded_cost is None:
        return EvaluationResult(
            name="cost",
            score=0.0,
            explanation="The session has no recorded cost.",
        )
    return EvaluationResult(
        name="cost",
        score=float(recorded_cost),
        explanation=f"The session recorded a total cost of {recorded_cost}.",
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
