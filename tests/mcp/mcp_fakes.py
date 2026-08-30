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
"""Shared in-process MCP server builders and SDK client fakes."""

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, cast

from mcp.server import MCPServer, ServerRequestContext
from mcp.server.mcpserver import Context

from kitaru.api_models.v1.annotation import (
    AnnotationResponse,
    AnnotationSelector,
    AnnotationUpdateRequest,
    InvestigationAnswerCreateRequest,
    ManualAnnotationCreateRequest,
)
from kitaru.api_models.v1.base import JsonValue
from kitaru.api_models.v1.tag import TagCreateRequest, TagResponse, TagUpdateRequest
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.server import create_server
from kitaru.mcp.settings import CapabilityMode, MCPSettings


def build_server_context(
    client: object, *, mode: CapabilityMode = CapabilityMode.READ_ONLY
) -> tuple[MCPServer[MCPServerState], Context[MCPServerState, Any]]:
    """Build a server and request context bound to a fake SDK client."""
    state = MCPServerState(MCPSettings(), cast(Any, client))
    server = create_server(MCPSettings(mode=mode))
    request_context = ServerRequestContext(
        session=cast(Any, None),
        lifespan_context=state,
        protocol_version="2026-07-28",
        method="tools/call",
    )
    return server, Context(request_context=request_context, mcp_server=server)


class _NullResource:
    """Resource whose every method reports the target as missing."""

    def __getattr__(self, name: str) -> Callable[..., Awaitable[object]]:
        async def _missing(*_args: object, **_kwargs: object) -> object:
            raise MCPToolError("not_found", f"null client has no data for {name}")

        return _missing


class NullClient:
    """SDK client fake with every resource present and empty."""

    def __getattr__(self, name: str) -> _NullResource:
        return _NullResource()

    async def close(self) -> None:
        return None


def _get_tag(name: str, *, tag_id: uuid.UUID | None = None) -> TagResponse:
    now = datetime.now(UTC)
    return TagResponse(
        id=tag_id or uuid.uuid4(),
        owner_id=uuid.uuid4(),
        name=name,
        created=now,
        updated=now,
    )


def _get_annotation(
    *,
    value: JsonValue,
    selector: AnnotationSelector | None = None,
    session_id: uuid.UUID | None = None,
    investigation_session_id: uuid.UUID | None = None,
    question_key: str | None = None,
    annotation_id: uuid.UUID | None = None,
) -> AnnotationResponse:
    now = datetime.now(UTC)
    return AnnotationResponse(
        id=annotation_id or uuid.uuid4(),
        owner_id=uuid.uuid4(),
        session_id=session_id or uuid.uuid4(),
        investigation_session_id=investigation_session_id,
        question_key=question_key,
        selector=selector,
        value=value,
        created=now,
        updated=now,
    )


class EchoTags(_NullResource):
    """Tag resource that returns the name it was told to store.

    Operations it does not define, such as listing, stay missing.
    """

    async def create(
        self, request: TagCreateRequest, idempotency_key: str | None = None
    ) -> TagResponse:
        """Return a tag carrying the submitted name."""
        return _get_tag(request.name)

    async def update(self, tag_id: uuid.UUID, request: TagUpdateRequest) -> TagResponse:
        """Return the tag under its new name."""
        return _get_tag(request.name, tag_id=tag_id)


class EchoAnnotations(_NullResource):
    """Annotation resource that returns the value it was told to store.

    Operations it does not define, such as listing, stay missing.
    """

    async def create(
        self,
        request: ManualAnnotationCreateRequest | InvestigationAnswerCreateRequest,
        idempotency_key: str | None = None,
    ) -> AnnotationResponse:
        """Return an annotation carrying the submitted value."""
        if isinstance(request, ManualAnnotationCreateRequest):
            return _get_annotation(
                value=request.value,
                selector=request.selector,
                session_id=request.session_id,
            )
        return _get_annotation(
            value=request.value,
            selector=request.selector,
            investigation_session_id=request.investigation_session_id,
            question_key=request.question_key,
        )

    async def update(
        self, annotation_id: uuid.UUID, request: AnnotationUpdateRequest
    ) -> AnnotationResponse:
        """Return the annotation under its new value."""
        return _get_annotation(value=request.value, annotation_id=annotation_id)


class EchoClient(NullClient):
    """SDK client fake that reads back the values it was asked to store.

    `NullClient` reports every target as missing, so a tool call never renders
    a payload and a value from the request cannot reach the response. These two
    resources behave the way the real server does for the mutations they cover:
    what a caller writes comes back in the result, which is what makes the
    redaction property able to fail. Every other resource stays missing.
    """

    def __init__(self) -> None:
        """Attach the resources that echo their request back."""
        self.tags = EchoTags()
        self.annotations = EchoAnnotations()
