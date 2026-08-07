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
"""Shared evaluator and importer route orchestration.

Evaluators and importers are both plugin resources, one ``PluginService``
bound to a different ``PluginKind``. The field-for-field mapping is
identical modulo the response class, so both routers share these functions
instead of duplicating them.
"""

import uuid
from typing import TypeVar

from kitaru.api_models.v1.base import ListParams, Page, TimestampedResponseModel
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorUpdateRequest,
)
from kitaru.api_models.v1.filter import Filter
from kitaru.api_models.v1.importer import ImporterCreateRequest, ImporterUpdateRequest
from kitaru.api_models.v1.plugin import PluginSource as WirePluginSource
from kitaru.server.adapters.rest.mapping.plugins import (
    plugin_create_to_command,
    plugin_list_params_to_filter,
    plugin_source_to_domain,
    plugin_to_response,
    plugin_update_to_command,
    plugin_version_to_response,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.plugin import PluginVersionFilter
from kitaru.server.application.services.plugin_service import PluginService

PluginResponseT = TypeVar("PluginResponseT", bound=TimestampedResponseModel)
PluginVersionResponseT = TypeVar(
    "PluginVersionResponseT", bound=TimestampedResponseModel
)


async def create_plugin(
    service: PluginService,
    body: EvaluatorCreateRequest | ImporterCreateRequest,
    response_class: type[PluginResponseT],
    actor: AuthContext,
) -> PluginResponseT:
    """Create a plugin from an evaluator or importer create request.

    Args:
        service: Plugin service bound to the resource's kind.
        body: Evaluator or importer create request.
        response_class: ``EvaluatorResponse`` or ``ImporterResponse``.
        actor: Caller context.

    Returns:
        Created plugin response.
    """
    plugin = await service.create_plugin(plugin_create_to_command(body), actor=actor)
    return plugin_to_response(plugin, response_class)


async def list_plugins(
    service: PluginService,
    params: ListParams,
    response_class: type[PluginResponseT],
    actor: AuthContext,
    filter_: Filter | None = None,
) -> Page[PluginResponseT]:
    """List plugins of the resource's kind.

    Args:
        service: Plugin service bound to the resource's kind.
        params: List params carrying pagination.
        response_class: ``EvaluatorResponse`` or ``ImporterResponse``.
        actor: Caller context.
        filter_: Filter expression, when present on the wire params.

    Returns:
        Page of plugin responses.
    """
    plugin_filter = plugin_list_params_to_filter(params, service.kind, filter_)
    plugins, next_cursor = await service.list_plugins(plugin_filter, actor=actor)
    return Page[response_class](  # ty: ignore[invalid-type-form]
        items=[plugin_to_response(plugin, response_class) for plugin in plugins],
        next_cursor=next_cursor,
    )


async def get_plugin(
    service: PluginService,
    plugin_id: uuid.UUID,
    response_class: type[PluginResponseT],
    actor: AuthContext,
) -> PluginResponseT:
    """Get a plugin of the resource's kind by id.

    Args:
        service: Plugin service bound to the resource's kind.
        plugin_id: Id of the plugin.
        response_class: ``EvaluatorResponse`` or ``ImporterResponse``.
        actor: Caller context.

    Returns:
        Plugin response.
    """
    plugin = await service.get_plugin(plugin_id, actor=actor)
    return plugin_to_response(plugin, response_class)


async def update_plugin(
    service: PluginService,
    plugin_id: uuid.UUID,
    body: EvaluatorUpdateRequest | ImporterUpdateRequest,
    response_class: type[PluginResponseT],
    actor: AuthContext,
) -> PluginResponseT:
    """Update a plugin from an evaluator or importer update request.

    Args:
        service: Plugin service bound to the resource's kind.
        plugin_id: Id of the plugin.
        body: Evaluator or importer update request.
        response_class: ``EvaluatorResponse`` or ``ImporterResponse``.
        actor: Caller context.

    Returns:
        Updated plugin response.
    """
    command = plugin_update_to_command(body)
    plugin = await service.update_plugin(plugin_id, command, actor=actor)
    return plugin_to_response(plugin, response_class)


async def delete_plugin(
    service: PluginService, plugin_id: uuid.UUID, actor: AuthContext
) -> None:
    """Delete a plugin of the resource's kind.

    Args:
        service: Plugin service bound to the resource's kind.
        plugin_id: Id of the plugin.
        actor: Caller context.
    """
    await service.delete_plugin(plugin_id, actor=actor)


async def create_version(
    service: PluginService,
    plugin_id: uuid.UUID,
    source: WirePluginSource,
    display_version: str | None,
    response_class: type[PluginVersionResponseT],
    actor: AuthContext,
) -> PluginVersionResponseT:
    """Create a plugin version.

    Args:
        service: Plugin service bound to the resource's kind.
        plugin_id: Id of the plugin.
        source: Wire plugin code source.
        display_version: Human-readable designator.
        response_class: ``EvaluatorVersionResponse`` or
            ``ImporterVersionResponse``.
        actor: Caller context.

    Returns:
        Created plugin version response.
    """
    version = await service.create_version(
        plugin_id,
        source=plugin_source_to_domain(source),
        display_version=display_version,
        actor=actor,
    )
    return plugin_version_to_response(version, response_class)


async def list_versions(
    service: PluginService,
    plugin_id: uuid.UUID,
    params: ListParams,
    response_class: type[PluginVersionResponseT],
    actor: AuthContext,
) -> Page[PluginVersionResponseT]:
    """List a plugin's versions.

    Args:
        service: Plugin service bound to the resource's kind.
        plugin_id: Id of the plugin.
        params: List params carrying pagination.
        response_class: ``EvaluatorVersionResponse`` or
            ``ImporterVersionResponse``.
        actor: Caller context.

    Returns:
        Page of plugin version responses.
    """
    version_filter = PluginVersionFilter(
        plugin_id=plugin_id, cursor=params.cursor, size=params.size, sort=params.sort
    )
    versions, next_cursor = await service.list_versions(version_filter, actor=actor)
    return Page[response_class](  # ty: ignore[invalid-type-form]
        items=[
            plugin_version_to_response(version, response_class) for version in versions
        ],
        next_cursor=next_cursor,
    )


async def get_version(
    service: PluginService,
    plugin_id: uuid.UUID,
    version: int,
    response_class: type[PluginVersionResponseT],
    actor: AuthContext,
) -> PluginVersionResponseT:
    """Get a plugin version by plugin id and version number.

    Args:
        service: Plugin service bound to the resource's kind.
        plugin_id: Id of the plugin.
        version: Version number.
        response_class: ``EvaluatorVersionResponse`` or
            ``ImporterVersionResponse``.
        actor: Caller context.

    Returns:
        Plugin version response.
    """
    plugin_version = await service.get_version(plugin_id, version, actor=actor)
    return plugin_version_to_response(plugin_version, response_class)


async def update_version(
    service: PluginService,
    plugin_id: uuid.UUID,
    version: int,
    display_version: str | None,
    response_class: type[PluginVersionResponseT],
    actor: AuthContext,
) -> PluginVersionResponseT:
    """Update a plugin version's display version.

    Args:
        service: Plugin service bound to the resource's kind.
        plugin_id: Id of the plugin.
        version: Version number.
        display_version: New display version, unchanged when ``None``.
        response_class: ``EvaluatorVersionResponse`` or
            ``ImporterVersionResponse``.
        actor: Caller context.

    Returns:
        Updated plugin version response.
    """
    plugin_version = await service.update_version(
        plugin_id, version, display_version=display_version, actor=actor
    )
    return plugin_version_to_response(plugin_version, response_class)
