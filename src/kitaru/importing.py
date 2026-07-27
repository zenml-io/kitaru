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
"""Import of sessions with user-defined importer functions."""

import uuid
from collections.abc import Callable, Iterable, Iterator
from decimal import Decimal
from typing import Any, Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.jobs import JobSpecImporter
from kitaru.api_models.v1.session_nodes import (
    NodeStatus,
    NodeType,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    TokenUsage,
)
from kitaru.ids import uuid7


class SessionImportError(Exception):
    """Raised when an importer cannot be loaded or does not produce parsed items."""


class ParsedNode(BaseModel):
    """Parsed session node."""

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    node_type: NodeType
    name: str
    status: NodeStatus = NodeStatus.COMPLETED
    error: str | None = None
    started_at: AwareDatetime | None = None
    ended_at: AwareDatetime | None = None
    inputs: JsonValue = None
    outputs: JsonValue = None
    requested_model: str | None = None
    model: str | None = None
    provider: str | None = None
    tokens: TokenUsage | None = None
    cost: Decimal | None = None
    model_params: dict[str, JsonValue] | None = None
    tool_name: str | None = None
    external_id: str | None = None
    attributes: dict[str, JsonValue] = Field(default_factory=dict)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    children: list["ParsedNode"] = Field(default_factory=list)


class ParsedSession(BaseModel):
    """Parsed session."""

    model_config = ConfigDict(extra="forbid")

    external_id: str
    name: str | None = None
    status: SessionStatus = SessionStatus.COMPLETED
    inputs: JsonValue = None
    outputs: JsonValue = None
    expected: JsonValue = None
    error: str | None = None
    started_at: AwareDatetime | None = None
    ended_at: AwareDatetime | None = None
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    nodes: list[ParsedNode] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_status(self) -> Self:
        """Validate that the session is terminal.

        Raises:
            ValueError: The session is in progress.

        Returns:
            Validated session.
        """
        if self.status is SessionStatus.IN_PROGRESS:
            raise ValueError("Imported sessions cannot be in progress")
        return self


class ParseFailure(BaseModel):
    """Parse failure."""

    model_config = ConfigDict(extra="forbid")

    line: int = Field(ge=0)
    external_id: str | None = None
    error: str


ParsedItem = ParsedSession | ParseFailure
Parser = Callable[..., Iterable[ParsedItem]]


def call_parser(
    parser: Parser, payload: bytes, params: dict[str, Any]
) -> Iterator[ParsedItem]:
    """Call a parser on a payload and iterate what it yields.

    An importer is called as ``parse(payload: bytes, **params)`` and
    yields parsed sessions and parse failures.

    Args:
        parser: Parser function.
        payload: Payload content.
        params: Keyword arguments for the parser.

    Raises:
        SessionImportError: The parser rejected the call.

    Returns:
        Iterator over the parsed items.
    """
    try:
        return iter(parser(payload, **params))
    except Exception as exc:
        raise SessionImportError(
            f"Importer raised {type(exc).__name__}: {exc}"
        ) from exc


def session_request(
    importer: JobSpecImporter, parsed: ParsedSession
) -> SessionCreateRequest:
    """Build the create request of a parsed session.

    Args:
        importer: Importer of the job spec.
        parsed: Parsed session.

    Returns:
        Session create request.
    """
    return SessionCreateRequest(
        agent_id=importer.agent_id,
        origin=SessionOrigin.IMPORTED,
        provider=importer.provider,
        status=parsed.status,
        name=parsed.name,
        inputs=parsed.inputs,
        outputs=parsed.outputs,
        expected=parsed.expected,
        error=parsed.error,
        started_at=parsed.started_at,
        ended_at=parsed.ended_at,
        external_id=parsed.external_id,
        metadata=parsed.metadata,
    )


def node_request(
    parsed: ParsedNode,
    node_id: uuid.UUID,
    parent_id: uuid.UUID | None,
    sequence: int,
) -> SessionNodeCreateRequest:
    """Build the ingest request of a parsed node.

    Args:
        parsed: Parsed node.
        node_id: Id assigned to the node.
        parent_id: Id of the primary parent.
        sequence: Order within the session.

    Returns:
        Session node create request.
    """
    return SessionNodeCreateRequest(
        id=node_id,
        parent_id=parent_id,
        sequence=sequence,
        external_id=parsed.external_id,
        node_type=parsed.node_type,
        name=parsed.name,
        status=parsed.status,
        error=parsed.error,
        started_at=parsed.started_at,
        ended_at=parsed.ended_at,
        inputs=parsed.inputs,
        outputs=parsed.outputs,
        requested_model=parsed.requested_model,
        model=parsed.model,
        provider=parsed.provider,
        tokens=parsed.tokens,
        cost=parsed.cost,
        model_params=parsed.model_params,
        tool_name=parsed.tool_name,
        attributes=parsed.attributes,
        metadata=parsed.metadata,
    )


def flatten_nodes(nodes: list[ParsedNode]) -> list[SessionNodeCreateRequest]:
    """Flatten a parsed node tree into ingest requests.

    The harness owns ids, parent links, and sequence numbers. The walk is
    depth first, so every node arrives after its parent.

    Args:
        nodes: Root nodes of the tree.

    Returns:
        Ingest requests in walk order.
    """
    requests: list[SessionNodeCreateRequest] = []

    def walk(node: ParsedNode, parent_id: uuid.UUID | None) -> None:
        node_id = uuid7()
        requests.append(node_request(node, node_id, parent_id, len(requests)))
        for child in node.children:
            walk(child, node_id)

    for root in nodes:
        walk(root, None)
    return requests
