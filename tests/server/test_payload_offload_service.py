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
"""Tests for payload offload and hydration."""

import uuid
from typing import Any

from conftest import build_payload_offload_service
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.server.domain.session_node import SessionNode


def _node(session_id: uuid.UUID, index: int, **overrides: Any) -> SessionNode:
    values: dict[str, Any] = {
        "session_id": session_id,
        "index": index,
        "node_type": NodeType.LLM_CALL,
        "name": "call",
        "status": NodeStatus.COMPLETED,
    }
    values.update(overrides)
    return SessionNode(**values)


async def test_offload_keeps_byte_identical_values_of_different_media_types_apart() -> (
    None
):
    """Store a text value and a byte-identical JSON value as separate blobs."""
    fakes = build_payload_offload_service(threshold_bytes=10)
    owner_id = uuid.uuid4()
    session_id = uuid.uuid4()

    padded = "x" * 50
    # The JSON encoding of the plain string equals the raw text of the quoted
    # string, so the two candidates hash to the same sha256 despite carrying
    # different media types.
    reasoning = f'"{padded}"'
    inputs = padded

    nodes = [
        _node(session_id, 0, reasoning=reasoning),
        _node(session_id, 1, inputs=inputs),
    ]
    offloaded = await fakes.service.offload_nodes(nodes, owner_id)
    reasoning_node, inputs_node = offloaded

    assert reasoning_node.reasoning_blob_id is not None
    assert inputs_node.inputs_blob_id is not None
    assert reasoning_node.reasoning_blob_id != inputs_node.inputs_blob_id

    reasoning_blob = await fakes.blob_repository.get(reasoning_node.reasoning_blob_id)
    inputs_blob = await fakes.blob_repository.get(inputs_node.inputs_blob_id)
    assert reasoning_blob.sha256 == inputs_blob.sha256
    assert reasoning_blob.media_type == "text/plain"
    assert inputs_blob.media_type == "application/json"

    hydrated = await fakes.service.hydrate_nodes(offloaded)
    hydrated_reasoning, hydrated_inputs = hydrated
    assert hydrated_reasoning.reasoning == reasoning
    assert hydrated_inputs.inputs == inputs
