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
    NodeLink,
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
    SessionNodeResponse,
)


def _node(
    external_id: str,
    parent_external_id: str | None = None,
    linked_external_ids: list[str] | None = None,
) -> SessionNodeCreateRequest:
    """Build a session node create request for the batch validator tests.

    Args:
        external_id: Node external id.
        parent_external_id: Parent node external id.
        linked_external_ids: External ids the node links to.

    Returns:
        A minimal session node create request.
    """
    return SessionNodeCreateRequest(
        external_id=external_id,
        parent_external_id=parent_external_id,
        links=[
            NodeLink(external_id=external_id, kind="parent")
            for external_id in linked_external_ids or []
        ],
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
        external_id="n0",
        links=[],
        node_type=NodeType.LLM_CALL,
        name="call",
        status=NodeStatus.COMPLETED,
        reasoning_selectors=reasoning_selectors,
        outputs=outputs,
        metadata={},
    )


def test_parent_before_child_accepted() -> None:
    """Accept a batch carrying a parent ahead of its own node."""
    batch = SessionNodeBatchRequest(nodes=[_node("a"), _node("b", "a")])
    assert len(batch.nodes) == 2


def test_parent_outside_batch_accepted() -> None:
    """Accept a parent external id the batch does not carry."""
    batch = SessionNodeBatchRequest(nodes=[_node("b", "stored")])
    assert batch.nodes[0].parent_external_id == "stored"


def test_self_parent_rejected() -> None:
    """Reject a node naming itself as its parent."""
    with pytest.raises(ValidationError):
        SessionNodeBatchRequest(nodes=[_node("a", "a")])


def test_self_link_rejected() -> None:
    """Reject a node linking to itself."""
    with pytest.raises(ValidationError):
        SessionNodeBatchRequest(nodes=[_node("a", None, ["a"])])


def test_batched_parent_after_child_accepted() -> None:
    """Accept a node whose batched parent follows it."""
    batch = SessionNodeBatchRequest(nodes=[_node("a", "b"), _node("b")])
    assert batch.nodes[0].parent_external_id == "b"


def test_batched_link_target_after_node_accepted() -> None:
    """Accept a node whose batched link target follows it."""
    batch = SessionNodeBatchRequest(nodes=[_node("a", None, ["b"]), _node("b")])
    assert batch.nodes[0].links == [NodeLink(external_id="b", kind="parent")]


def test_repeated_external_id_rejected() -> None:
    """Reject a batch repeating a node external id."""
    with pytest.raises(ValidationError):
        SessionNodeBatchRequest(nodes=[_node("a"), _node("a")])


def test_empty_external_id_rejected() -> None:
    """Reject a node with an empty external id."""
    with pytest.raises(ValidationError):
        _node("")


def test_batch_at_cap_accepted() -> None:
    """Accept a batch of exactly the maximum node count."""
    batch = SessionNodeBatchRequest(nodes=[_node(f"n{i}") for i in range(500)])
    assert len(batch.nodes) == 500


def test_batch_over_cap_rejected() -> None:
    """Reject a batch larger than the maximum node count."""
    with pytest.raises(ValidationError):
        SessionNodeBatchRequest(nodes=[_node(f"n{i}") for i in range(501)])


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
