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
"""Deterministic session scorers."""

from kitaru.api_models.v1.session_nodes import (
    NodeStatus,
    NodeType,
)
from kitaru.job.scorer import SessionView


def answer_quality(session: SessionView, keywords: list[str] | None = None) -> float:
    """Score the final answer on keyword coverage and brevity."""
    text = str(session.session.outputs or "")
    if not text:
        return 0.0
    lowered = text.lower()
    keywords = keywords or ["answer"]
    coverage = sum(keyword.lower() in lowered for keyword in keywords) / len(keywords)
    brevity = min(1.0, 40 / len(text))
    return round((coverage + brevity) / 2, 4)


def tool_efficiency(session: SessionView, budget: int = 4) -> float:
    """Score tool usage on success rate within a call budget."""
    tool_nodes = [
        node for node in session.nodes if node.node_type is NodeType.TOOL_CALL
    ]
    if not tool_nodes:
        return 1.0
    completed = sum(node.status is NodeStatus.COMPLETED for node in tool_nodes)
    return round((completed / len(tool_nodes)) * min(1.0, budget / len(tool_nodes)), 4)


def token_budget(session: SessionView, max_tokens: int = 2000) -> float:
    """Score total token usage against a budget."""
    tokens = session.session.tokens
    if tokens is None:
        return 1.0
    total = (
        (tokens.input_tokens or 0)
        + (tokens.output_tokens or 0)
        + (tokens.cached_input_tokens or 0)
        + (tokens.reasoning_tokens or 0)
    )
    return round(min(1.0, max(0.0, 1 - total / max_tokens)), 4)
