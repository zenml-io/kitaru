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

import pytest
from pydantic import ValidationError

from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
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
