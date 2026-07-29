"""Shared plugin route operations."""

import uuid
from typing import TypeVar

from kitaru.api_models.v1.base import ListParams, Page
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorListParams,
    EvaluatorResponse,
    EvaluatorUpdateRequest,
    EvaluatorVersionCreateRequest,
    EvaluatorVersionResponse,
    EvaluatorVersionUpdateRequest,
)
from kitaru.api_models.v1.importer import (
    ImporterCreateRequest,
    ImporterListParams,
    ImporterResponse,
    ImporterUpdateRequest,
    ImporterVersionCreateRequest,
    ImporterVersionResponse,
    ImporterVersionUpdateRequest,
)
from kitaru.server.adapters.rest.mapping.plugins import (
    plugin_list_params_to_filter,
    plugin_source_to_domain,
    plugin_to_response,
    plugin_update_to_command,
    plugin_version_list_filter,
    plugin_version_to_response,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.domain.plugin import PluginKind

PluginResponseT = TypeVar("PluginResponseT", EvaluatorResponse, ImporterResponse)
PluginVersionResponseT = TypeVar(
    "PluginVersionResponseT", EvaluatorVersionResponse, ImporterVersionResponse
)


async def create_plugin(
    body: EvaluatorCreateRequest | ImporterCreateRequest,
    service: PluginService,
    actor: AuthContext,
    response_type: type[PluginResponseT],
) -> PluginResponseT:
    """Create a kind-bound plugin."""
    plugin = await service.create_plugin(
        name=body.name,
        description=body.description,
        provider=body.provider if isinstance(body, ImporterCreateRequest) else None,
        metadata=body.metadata,
        actor=actor,
    )
    return plugin_to_response(plugin, response_type)


async def list_plugins(
    params: EvaluatorListParams | ImporterListParams,
    service: PluginService,
    actor: AuthContext,
    kind: PluginKind,
    response_type: type[PluginResponseT],
) -> Page[PluginResponseT]:
    """List kind-bound plugins."""
    items, cursor = await service.list_plugins(
        plugin_list_params_to_filter(params, kind), actor=actor
    )
    return Page(
        items=[plugin_to_response(item, response_type) for item in items],
        next_cursor=cursor,
    )


async def get_plugin(
    plugin_id: uuid.UUID,
    service: PluginService,
    actor: AuthContext,
    response_type: type[PluginResponseT],
) -> PluginResponseT:
    """Get a kind-bound plugin."""
    return plugin_to_response(
        await service.get_plugin(plugin_id, actor=actor), response_type
    )


async def update_plugin(
    plugin_id: uuid.UUID,
    body: EvaluatorUpdateRequest | ImporterUpdateRequest,
    service: PluginService,
    actor: AuthContext,
    response_type: type[PluginResponseT],
) -> PluginResponseT:
    """Update a kind-bound plugin."""
    plugin = await service.update_plugin(
        plugin_id, plugin_update_to_command(body), actor=actor
    )
    return plugin_to_response(plugin, response_type)


async def delete_plugin(
    plugin_id: uuid.UUID, service: PluginService, actor: AuthContext
) -> None:
    """Delete a kind-bound plugin."""
    await service.delete_plugin(plugin_id, actor=actor)


async def create_plugin_version(
    plugin_id: uuid.UUID,
    body: EvaluatorVersionCreateRequest | ImporterVersionCreateRequest,
    service: PluginService,
    actor: AuthContext,
    response_type: type[PluginVersionResponseT],
) -> PluginVersionResponseT:
    """Create a kind-bound plugin version."""
    version = await service.create_version(
        plugin_id,
        source=plugin_source_to_domain(body.source),
        display_version=body.display_version,
        actor=actor,
    )
    return plugin_version_to_response(version, response_type)


async def list_plugin_versions(
    plugin_id: uuid.UUID,
    params: ListParams,
    service: PluginService,
    actor: AuthContext,
    response_type: type[PluginVersionResponseT],
) -> Page[PluginVersionResponseT]:
    """List kind-bound plugin versions."""
    items, cursor = await service.list_versions(
        plugin_version_list_filter(plugin_id, params), actor=actor
    )
    return Page(
        items=[plugin_version_to_response(item, response_type) for item in items],
        next_cursor=cursor,
    )


async def get_plugin_version(
    plugin_id: uuid.UUID,
    version: int,
    service: PluginService,
    actor: AuthContext,
    response_type: type[PluginVersionResponseT],
) -> PluginVersionResponseT:
    """Get a kind-bound plugin version."""
    entity = await service.get_version(plugin_id, version, actor=actor)
    return plugin_version_to_response(entity, response_type)


async def update_plugin_version(
    plugin_id: uuid.UUID,
    version: int,
    body: EvaluatorVersionUpdateRequest | ImporterVersionUpdateRequest,
    service: PluginService,
    actor: AuthContext,
    response_type: type[PluginVersionResponseT],
) -> PluginVersionResponseT:
    """Update a kind-bound plugin version display label."""
    entity = await service.update_version(
        plugin_id,
        version,
        display_version=body.display_version,
        actor=actor,
    )
    return plugin_version_to_response(entity, response_type)
