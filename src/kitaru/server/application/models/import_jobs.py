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
"""Trace importer application models."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import Field

from kitaru.server.base import FrozenModel
from kitaru.server.domain.session import SessionStatus, TokenUsage
from kitaru.server.domain.session_node import NodeStatus, NodeType


class ImporterDescriptor(FrozenModel):
    """Deploy-time trace importer description."""

    id: str
    display_name: str
    version: str
    file_extensions: list[str]
    max_upload_bytes: int


class ImportContext(FrozenModel):
    """User selections applied to one import job."""

    agent_version_id: uuid.UUID
    source_instance: str | None = None


class ReplayReadiness(FrozenModel):
    """Conservative replay-readiness assessment."""

    level: str
    root_inputs_available: bool
    graph_complete: bool
    tool_call_count: int
    replayable_tool_call_count: int
    reasons: list[str] = Field(default_factory=list)


class NormalizedNode(FrozenModel):
    """Provider-independent session node."""

    source_id: str
    parent_source_id: str | None = None
    trace_id: str
    node_type: NodeType
    name: str
    status: NodeStatus
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    inputs: Any = None
    outputs: Any = None
    requested_model: str | None = None
    model: str | None = None
    provider: str | None = None
    tokens: TokenUsage | None = None
    cost: Decimal | None = None
    model_params: dict[str, Any] | None = None
    tool_name: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    source_metadata: dict[str, Any] = Field(default_factory=dict)


class NormalizedTurn(FrozenModel):
    """One source trace within a multi-turn session."""

    trace_id: str
    inputs: Any = None
    outputs: Any = None
    started_at: datetime | None = None
    ended_at: datetime | None = None


class NormalizedSession(FrozenModel):
    """Provider-independent multi-turn session."""

    source_id: str
    source_instance: str
    name: str | None = None
    status: SessionStatus
    turns: list[NormalizedTurn]
    nodes: list[NormalizedNode]
    inputs: Any = None
    outputs: Any = None
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    source_metadata: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    readiness: ReplayReadiness
    content_digest: str


class NormalizationError(FrozenModel):
    """One source session rejected during normalization."""

    source_id: str | None = None
    message: str


class NormalizedImport(FrozenModel):
    """Sessions and isolated errors produced by one importer."""

    sessions: list[NormalizedSession] = Field(default_factory=list)
    errors: list[NormalizationError] = Field(default_factory=list)
