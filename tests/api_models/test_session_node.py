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
"""Tests for session node API models."""

import uuid
from typing import Any

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
    SessionNodeResponse,
)


def _node(index: int, parent_index: int | None) -> SessionNodeCreateRequest:
    """Build a session node create request for the batch validator tests.

    Args:
        index: Node index.
        parent_index: Parent node index.

    Returns:
        A minimal session node create request.
    """
    return SessionNodeCreateRequest(
        index=index,
        parent_index=parent_index,
        node_type=NodeType.SPAN,
        name="node",
        status=NodeStatus.COMPLETED,
        inputs=None,
        outputs=None,
        attributes=None,
    )


def _response(reasoning_selectors: list[str], outputs: Any) -> SessionNodeResponse:
    """Build a session node response for the reasoning property tests.

    Args:
        reasoning_selectors: Pointers selecting visible reasoning.
        outputs: Node outputs.

    Returns:
        A minimal session node response.
    """
    return SessionNodeResponse(
        id=uuid.uuid4(),
        session_id=uuid.uuid4(),
        index=0,
        parent_index=None,
        secondary_parent_indexes=[],
        secondary_parent_ids=[],
        node_type=NodeType.LLM_CALL,
        name="call",
        status=NodeStatus.COMPLETED,
        reasoning_selectors=reasoning_selectors,
        outputs=outputs,
        metadata={},
    )


def test_parent_index_before_index_accepted() -> None:
    """Accept a batch where every parent_index precedes its own index."""
    batch = SessionNodeBatchRequest(nodes=[_node(0, None), _node(1, 0)])
    assert len(batch.nodes) == 2


def test_parent_index_equal_to_index_rejected() -> None:
    """Reject a node whose parent_index equals its own index."""
    with pytest.raises(ValidationError):
        SessionNodeBatchRequest(nodes=[_node(0, 0)])


def test_parent_index_after_index_rejected() -> None:
    """Reject a node whose parent_index is greater than its own index."""
    with pytest.raises(ValidationError):
        SessionNodeBatchRequest(nodes=[_node(0, None), _node(1, 2)])


def test_negative_index_rejected() -> None:
    """Reject a node with a negative index."""
    with pytest.raises(ValidationError):
        _node(-1, None)


def test_negative_parent_index_rejected() -> None:
    """Reject a node with a negative parent index."""
    with pytest.raises(ValidationError):
        _node(1, -1)


def test_negative_secondary_parent_index_rejected() -> None:
    """Reject a node with a negative secondary parent index."""
    with pytest.raises(ValidationError):
        SessionNodeCreateRequest(
            index=1,
            secondary_parent_indexes=[-1],
            node_type=NodeType.SPAN,
            name="node",
            status=NodeStatus.COMPLETED,
            inputs=None,
            outputs=None,
            attributes=None,
        )


def test_batch_at_cap_accepted() -> None:
    """Accept a batch of exactly the maximum node count."""
    batch = SessionNodeBatchRequest(nodes=[_node(i, None) for i in range(500)])
    assert len(batch.nodes) == 500


def test_batch_over_cap_rejected() -> None:
    """Reject a batch larger than the maximum node count."""
    with pytest.raises(ValidationError):
        SessionNodeBatchRequest(nodes=[_node(i, None) for i in range(501)])


def test_reasoning_joins_selected_strings() -> None:
    """Join the strings the selectors pick out of the outputs by newline."""
    response = _response(
        ["/parts/0/content", "/parts/1/content"],
        {"parts": [{"content": "first"}, {"content": "second"}]},
    )
    assert response.reasoning == "first\nsecond"


def test_reasoning_skips_non_string_hits() -> None:
    """Keep only the selectors that resolve to a string."""
    response = _response(
        ["/parts/0", "/parts/1/content"],
        {"parts": [{"content": "first"}, {"content": "second"}]},
    )
    assert response.reasoning == "second"


def test_reasoning_without_outputs() -> None:
    """Resolve no reasoning when the outputs are absent."""
    assert _response(["/parts/0/content"], None).reasoning is None


def test_reasoning_without_selectors() -> None:
    """Resolve no reasoning when the node names no selector."""
    assert _response([], {"parts": [{"content": "first"}]}).reasoning is None


def test_reasoning_ignores_unresolved_selector() -> None:
    """Resolve no reasoning when no selector matches the outputs."""
    assert _response(["/missing"], {"parts": []}).reasoning is None
