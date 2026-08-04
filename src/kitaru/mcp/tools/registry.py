#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded registry-read handler."""

from typing import TypeVar

from kitaru.api_models.v1.agent import AgentListParams
from kitaru.api_models.v1.base import ListParams, Page, ResponseModel
from kitaru.api_models.v1.cohort import CohortListParams
from kitaru.api_models.v1.evaluator import EvaluatorListParams
from kitaru.api_models.v1.experiment import ExperimentListParams
from kitaru.api_models.v1.importer import ImporterListParams
from kitaru.client.references import (
    ParentKind,
    PluginKind,
    resolve_parent,
    resolve_plugin_version,
)
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import PageData, PageMetadata
from kitaru.mcp.models.registry import (
    RegistryGetRequest,
    RegistryGetVersionRequest,
    RegistryListRequest,
    RegistryListVersionsRequest,
    RegistryReadRequest,
)


async def handle_registry_read(
    state: MCPServerState, request: RegistryReadRequest
) -> object:
    """Execute one bounded registry operation."""
    client = state.client
    if isinstance(request, RegistryGetRequest):
        return await resolve_parent(client, request.kind, request.reference)
    if isinstance(request, RegistryListRequest):
        common = request.model_dump(include={"cursor", "size", "sort", "filter"})
        if request.kind is ParentKind.AGENT:
            page = await client.agents.list(AgentListParams.model_validate(common))
        elif request.kind is ParentKind.COHORT:
            page = await client.cohorts.list(CohortListParams.model_validate(common))
        elif request.kind is ParentKind.EXPERIMENT:
            page = await client.experiments.list(
                ExperimentListParams.model_validate(common)
            )
        elif request.kind is ParentKind.IMPORTER:
            page = await client.importers.list(
                ImporterListParams.model_validate(common)
            )
        else:
            page = await client.evaluators.list(
                EvaluatorListParams.model_validate(common)
            )
        return _get_page(page, request.size)
    if isinstance(request, RegistryListVersionsRequest):
        kind = ParentKind(request.kind)
        parent = await resolve_parent(client, kind, request.parent_reference)
        params = ListParams(cursor=request.cursor, size=request.size, sort=request.sort)
        if request.kind == "agent":
            page = await client.agents.list_versions(parent.id, params)
        elif request.kind == "cohort":
            page = await client.cohorts.list_versions(parent.id, params)
        elif request.kind == "importer":
            page = await client.importers.list_versions(parent.id, params)
        else:
            page = await client.evaluators.list_versions(parent.id, params)
        return _get_page(page, request.size)
    return await _get_version(state, request)


async def _get_version(
    state: MCPServerState, request: RegistryGetVersionRequest
) -> object:
    if request.kind in {"agent", "cohort"}:
        if (
            request.version_id is None
            or request.parent_reference is not None
            or request.version is not None
        ):
            raise MCPToolError(
                "invalid_arguments",
                "Agent and cohort versions require only version_id.",
            )
        resource = (
            state.client.agent_versions
            if request.kind == "agent"
            else state.client.cohort_versions
        )
        return await resource.get(request.version_id)
    if (
        request.parent_reference is None
        or request.version is None
        or request.version_id is not None
    ):
        raise MCPToolError(
            "invalid_arguments",
            "Plugin versions require parent_reference and version only.",
        )
    parent_kind = ParentKind(request.kind)
    parent = await resolve_parent(state.client, parent_kind, request.parent_reference)
    return await resolve_plugin_version(
        state.client, PluginKind(request.kind), parent.id, request.version
    )


PageItemT = TypeVar("PageItemT", bound=ResponseModel)


def _get_page(page: Page[PageItemT], requested_size: int) -> PageData:
    return PageData(
        items=[item.model_dump(mode="json") for item in page.items],
        page=PageMetadata(
            size=requested_size,
            next_cursor=page.next_cursor,
            has_more=page.next_cursor is not None,
        ),
    )
