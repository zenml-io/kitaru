#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded investigation and annotation handlers."""

import asyncio

import httpx

from kitaru.api_models.v1.annotation import (
    AnnotationListParams,
    AnnotationUpdateRequest,
    InvestigationAnswerCreateRequest,
    ManualAnnotationCreateRequest,
)
from kitaru.api_models.v1.investigation import (
    InvestigationCreateRequest,
    InvestigationListParams,
    InvestigationSessionsListParams,
    InvestigationSessionUpdateRequest,
    InvestigationUpdateRequest,
)
from kitaru.api_models.v1.tag import (
    TagCreateRequest,
    TagLinkCreateRequest,
    TagUpdateRequest,
)
from kitaru.client.dashboard_urls import get_investigation_review_url
from kitaru.client.exceptions import APIError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import PageData, ReviewItem, ToolSuccessPayload
from kitaru.mcp.models.review import (
    AnnotationUpdate,
    InvestigationAnswerCreate,
    InvestigationCreate,
    InvestigationUpdate,
    ManualAnnotationCreate,
    ReviewGet,
    ReviewListSessions,
    ReviewManageRequest,
    ReviewReadRequest,
    SetInvestigationSessionVerdict,
    TagCreate,
    TagLink,
    TagUpdate,
)
from kitaru.mcp.tools.registry import build_page_data

_INFO_LOOKUP_MAX_SECONDS = 5.0
_INFO_LOOKUP_HANDLER_FRACTION = 0.25


async def handle_review_read(
    state: MCPServerState, request: ReviewReadRequest
) -> object:
    """Execute one bounded investigation or annotation read."""
    if isinstance(request, ReviewGet):
        resource = (
            state.client.investigations
            if request.kind == "investigation"
            else state.client.annotations
        )
        return await resource.get(request.id)
    if isinstance(request, ReviewListSessions):
        params = InvestigationSessionsListParams(
            cursor=request.cursor, size=request.size
        )
        page = await state.client.investigations.list_sessions(
            request.investigation_id, params
        )
        return build_page_data(page, request.size, PageData[ReviewItem])
    common = request.model_dump(include={"cursor", "size", "sort", "filter"})
    if request.kind == "investigation":
        page = await state.client.investigations.list(
            InvestigationListParams.model_validate(common)
        )
    else:
        page = await state.client.annotations.list(
            AnnotationListParams.model_validate(common)
        )
    return build_page_data(page, request.size, PageData[ReviewItem])


async def handle_review_manage(
    state: MCPServerState, request: ReviewManageRequest
) -> object:
    """Perform one investigation or annotation mutation."""
    if isinstance(request, InvestigationCreate):
        info = None
        warnings: list[str] = []
        try:
            timeout = min(
                _INFO_LOOKUP_MAX_SECONDS,
                state.settings.handler_timeout * _INFO_LOOKUP_HANDLER_FRACTION,
            )
            async with asyncio.timeout(timeout):
                info = await state.client.info.get()
        # ValueError covers malformed info payloads: JSON decoding and Pydantic
        # validation errors both derive from it. The short deadline reserves most
        # of the handler budget for the mutation and its response.
        except (APIError, httpx.HTTPError, TimeoutError, ValueError):
            warnings.append(
                "Could not resolve a dashboard review link because the server "
                "info request failed. The investigation was created."
            )
        dto = InvestigationCreateRequest(
            agent_id=request.agent_id,
            name=request.name,
            description=request.description,
            sessions=request.sessions,
        )
        investigation = await state.client.investigations.create(dto)
        review_url: str | None = None
        if info is not None:
            review_url = get_investigation_review_url(
                info,
                state.client.base_url,
                agent_id=investigation.agent_id,
                investigation_id=investigation.id,
            )
        return ToolSuccessPayload(
            data=investigation,
            links={"review": review_url} if review_url else {},
            warnings=warnings,
        )
    if isinstance(request, InvestigationUpdate):
        values = request.model_dump(
            include={"name", "description", "status"}, exclude_unset=True
        )
        if request.clear_description:
            values["description"] = None
        return await state.client.investigations.update(
            request.investigation_id,
            InvestigationUpdateRequest.model_validate(values),
        )
    if isinstance(request, SetInvestigationSessionVerdict):
        return await state.client.investigations.update_session(
            request.investigation_id,
            request.session_id,
            InvestigationSessionUpdateRequest(verdict=request.verdict),
        )
    if isinstance(request, ManualAnnotationCreate):
        return await state.client.annotations.create(
            ManualAnnotationCreateRequest(
                session_id=request.session_id,
                selector=request.selector,
                value=request.value,
            )
        )
    if isinstance(request, InvestigationAnswerCreate):
        return await state.client.annotations.create(
            InvestigationAnswerCreateRequest(
                investigation_session_id=request.investigation_session_id,
                question_key=request.question_key,
                selector=request.selector,
                value=request.value,
            )
        )
    if isinstance(request, AnnotationUpdate):
        return await state.client.annotations.update(
            request.annotation_id, AnnotationUpdateRequest(value=request.value)
        )
    if isinstance(request, TagCreate):
        return await state.client.tags.create(TagCreateRequest(name=request.name))
    if isinstance(request, TagUpdate):
        return await state.client.tags.update(
            request.tag_id, TagUpdateRequest(name=request.name)
        )
    assert isinstance(request, TagLink)
    return await state.client.tags.create_link(
        request.tag_id,
        TagLinkCreateRequest(
            resource_type=request.resource_type, resource_id=request.resource_id
        ),
    )
