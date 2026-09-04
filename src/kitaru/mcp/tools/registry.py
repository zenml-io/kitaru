#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded registry-read handler."""

from typing import TypeVar

from kitaru.api_models.v1.agent import AgentListParams
from kitaru.api_models.v1.agent_version import AgentVersionListParams
from kitaru.api_models.v1.analyzer import AnalyzerListParams
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
from kitaru.mcp.tools.params import build_list_params


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
        if request.kind == "tag":
            page = await client.tags.list(build_list_params(TagListParams, request))
        elif request.kind == "worker":
            page = await client.workers.list(
                build_list_params(WorkerListParams, request)
            )
        elif request.kind is ParentKind.AGENT:
            page = await client.agents.list(build_list_params(AgentListParams, request))
        elif request.kind is ParentKind.COHORT:
            page = await client.cohorts.list(
                build_list_params(CohortListParams, request)
            )
        elif request.kind is ParentKind.EXPERIMENT:
            page = await client.experiments.list(
                build_list_params(ExperimentListParams, request)
            )
        elif request.kind is ParentKind.IMPORTER:
            page = await client.importers.list(
                build_list_params(ImporterListParams, request)
            )
        elif request.kind is ParentKind.EVALUATOR:
            page = await client.evaluators.list(
                build_list_params(EvaluatorListParams, request)
            )
        else:
            page = await client.analyzers.list(
                build_list_params(AnalyzerListParams, request)
            )
        return build_page_data(page, request.size, PageData[RegistryItem])
    if isinstance(request, RegistryListVersionsRequest):
        kind = ParentKind(request.kind)
        parent = await resolve_parent(client, kind, request.parent_reference)
        if request.kind == "agent":
            page = await client.agents.list_versions(
                parent.id, build_list_params(AgentVersionListParams, request)
            )
        elif request.kind == "cohort":
            page = await client.cohorts.list_versions(
                parent.id, build_list_params(CohortVersionListParams, request)
            )
        elif request.kind == "importer":
            page = await client.importers.list_versions(
                parent.id, build_list_params(ListParams, request, with_filter=False)
            )
        elif request.kind == "evaluator":
            page = await client.evaluators.list_versions(
                parent.id, build_list_params(ListParams, request, with_filter=False)
            )
        else:
            page = await client.analyzers.list_versions(
                parent.id, build_list_params(ListParams, request, with_filter=False)
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
