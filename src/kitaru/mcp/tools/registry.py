#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded registry-read handler."""

from typing import TypeVar

from kitaru.api_models.v1.agent import AgentListParams
from kitaru.api_models.v1.agent_version import AgentVersionListParams
from kitaru.api_models.v1.base import ListParams, Page, ResponseModel
from kitaru.api_models.v1.cohort import CohortListParams
from kitaru.api_models.v1.cohort_version import CohortVersionListParams
from kitaru.api_models.v1.evaluator import EvaluatorListParams
from kitaru.api_models.v1.experiment import ExperimentListParams
from kitaru.api_models.v1.importer import ImporterListParams
from kitaru.api_models.v1.tag import TagListParams
from kitaru.api_models.v1.worker import WorkerListParams
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.common import PageData, PageMetadata, RegistryItem
from kitaru.mcp.models.registry import (
    RegistryGetRequest,
    RegistryGetVersionRequest,
    RegistryGetWorkerRequest,
    RegistryListRequest,
    RegistryListVersionsRequest,
    RegistryReadRequest,
)
from kitaru.mcp.references import (
    ParentKind,
    PluginKind,
    resolve_parent,
    resolve_plugin_version,
)


async def handle_registry_read(
    state: MCPServerState, request: RegistryReadRequest
) -> object:
    """Execute one bounded registry operation."""
    client = state.client
    if isinstance(request, RegistryGetWorkerRequest):
        return await client.workers.get(request.worker_id)
    if isinstance(request, RegistryGetRequest):
        return await resolve_parent(client, request.kind, request.reference)
    if isinstance(request, RegistryListRequest):
        common = request.model_dump(include={"cursor", "size", "sort", "filter"})
        if request.kind == "tag":
            page = await client.tags.list(TagListParams.model_validate(common))
        elif request.kind == "worker":
            page = await client.workers.list(WorkerListParams.model_validate(common))
        elif request.kind is ParentKind.AGENT:
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
        return build_page_data(page, request.size, PageData[RegistryItem])
    if isinstance(request, RegistryListVersionsRequest):
        kind = ParentKind(request.kind)
        parent = await resolve_parent(client, kind, request.parent_reference)
        common = request.model_dump(include={"cursor", "size", "sort", "filter"})
        if request.kind == "agent":
            page = await client.agents.list_versions(
                parent.id, AgentVersionListParams.model_validate(common)
            )
        elif request.kind == "cohort":
            page = await client.cohorts.list_versions(
                parent.id, CohortVersionListParams.model_validate(common)
            )
        elif request.kind == "importer":
            common.pop("filter", None)
            page = await client.importers.list_versions(
                parent.id, ListParams.model_validate(common)
            )
        else:
            common.pop("filter", None)
            page = await client.evaluators.list_versions(
                parent.id, ListParams.model_validate(common)
            )
        return build_page_data(page, request.size, PageData[RegistryItem])
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
PageSourceT = TypeVar("PageSourceT", bound=ResponseModel)


def build_page_data(
    page: Page[PageSourceT],
    requested_size: int,
    page_data_type: type[PageData[PageItemT]],
) -> PageData[PageItemT]:
    """Build a page using the result envelope's concrete item types."""
    return page_data_type(
        items=page.items,
        page=PageMetadata(
            size=requested_size,
            next_cursor=page.next_cursor,
            has_more=page.next_cursor is not None,
        ),
    )
