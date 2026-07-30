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
"""Private normalization models for the Langfuse importer."""

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, Self

from pydantic import Field

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.base import FrozenModel
from kitaru.task.importer import ParsedNode, ParsedSession, SessionImportError

InvalidImport = SessionImportError


class ImporterDescriptor(FrozenModel):
    """Deploy-time trace importer description."""

    id: str
    display_name: str
    version: str
    file_extensions: list[str]
    max_upload_bytes: int
    status: Literal["available", "unavailable"] = "available"
    error_code: str | None = None


class ImportContext(FrozenModel):
    """User selections applied to one import job."""

    source_instance: str | None = None
    filename: str | None = None

    @classmethod
    def from_params(cls, params: dict[str, Any] | None) -> Self:
        """Build a context from the job spec importer params.

        Unrecognized keys are ignored so that adding a job parameter does
        not break plugins released against an earlier contract.

        Args:
            params: Importer params of the job spec.

        Returns:
            Context holding the keys this contract understands.
        """
        params = params or {}
        return cls(
            source_instance=params.get("source_instance"),
            filename=params.get("filename"),
        )


class ReplayReadiness(FrozenModel):
    """Conservative replay-readiness assessment."""

    level: Literal["unavailable", "ready", "partial"]
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


def _parsed_nodes(nodes: list[NormalizedNode]) -> list[ParsedNode]:
    """Convert flat normalized nodes into the parsed-node tree."""
    converted = {
        node.source_id: ParsedNode(
            trace_id=node.trace_id,
            node_type=node.node_type,
            name=node.name,
            status=node.status,
            error=node.error,
            started_at=node.started_at,
            ended_at=node.ended_at,
            inputs=node.inputs,
            outputs=node.outputs,
            requested_model=node.requested_model,
            model=node.model,
            provider=node.provider,
            tokens=node.tokens,
            cost=node.cost,
            model_params=node.model_params,
            tool_name=node.tool_name,
            external_id=node.source_id,
            attributes=node.attributes,
            metadata=node.source_metadata,
        )
        for node in nodes
    }
    roots: list[ParsedNode] = []
    for node in nodes:
        parsed = converted[node.source_id]
        if node.parent_source_id in converted:
            converted[node.parent_source_id].children.append(parsed)
        else:
            roots.append(parsed)
    return roots


def parsed_items(
    normalized: NormalizedImport,
) -> list[ParsedSession | ImportFailure]:
    """Convert legacy provider normalization into the unified parser contract."""
    items: list[ParsedSession | ImportFailure] = []
    for session in normalized.sessions:
        metadata = dict(session.source_metadata)
        metadata["normalization_warnings"] = session.warnings
        metadata["replay_readiness"] = session.readiness.model_dump(mode="json")
        metadata["source_content_digest"] = session.content_digest
        items.append(
            ParsedSession(
                external_id=f"{session.source_instance}:{session.source_id}",
                name=session.name,
                status=session.status,
                inputs=session.inputs,
                outputs=session.outputs,
                expected=None,
                error=session.error,
                started_at=session.started_at,
                ended_at=session.ended_at,
                metadata=metadata,
                nodes=_parsed_nodes(session.nodes),
            )
        )
    items.extend(
        ImportFailure(line=position, external_id=error.source_id, error=error.message)
        for position, error in enumerate(normalized.errors, start=1)
    )
    return items
