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
"""Shared evaluator and importer DTO conversions.

Evaluators and importers are both plugin resources, one ``PluginService``
bound to a different ``PluginKind``. The field-for-field mapping is
identical modulo the response class, so both routers share these functions
instead of duplicating them.
"""

from typing import Any, TypeVar

from kitaru.api_models.v1.base import (
    ListParams,
    OwnedResponseModel,
    TimestampedResponseModel,
)
from kitaru.api_models.v1.evaluator import (
    EvaluatorUpdateRequest,
    EvaluatorVersionResponse,
)
from kitaru.api_models.v1.filter import Filter
from kitaru.api_models.v1.importer import ImporterUpdateRequest, ImporterVersionResponse
from kitaru.api_models.v1.plugin import PackagePluginSource as WirePackagePluginSource
from kitaru.api_models.v1.plugin import PluginSource as WirePluginSource
from kitaru.api_models.v1.plugin import ScriptPluginSource as WireScriptPluginSource
from kitaru.server.adapters.rest.mapping.filtering import filter_to_expression
from kitaru.server.application.models.plugin import (
    EvaluatorFilter,
    ImporterFilter,
    PluginFilter,
    PluginUpdate,
)
from kitaru.server.domain.plugin import (
    PackagePluginSource as DomainPackagePluginSource,
)
from kitaru.server.domain.plugin import Plugin, PluginKind, PluginVersion
from kitaru.server.domain.plugin import PluginSource as DomainPluginSource
from kitaru.server.domain.plugin import (
    ScriptPluginSource as DomainScriptPluginSource,
)

PluginResponseT = TypeVar("PluginResponseT", bound=OwnedResponseModel)
PluginVersionResponseT = TypeVar(
    "PluginVersionResponseT", bound=TimestampedResponseModel
)

# A plugin version exposes its parent under a resource-specific field name,
# keyed by response class rather than branched on kind.
_VERSION_PARENT_FIELD: dict[type, str] = {
    EvaluatorVersionResponse: "evaluator_id",
    ImporterVersionResponse: "importer_id",
}


def plugin_source_to_domain(source: WirePluginSource) -> DomainPluginSource:
    """Convert a wire plugin source to its domain value object.

    Args:
        source: Wire plugin source.

    Returns:
        Domain plugin source.
    """
    if isinstance(source, WireScriptPluginSource):
        return DomainScriptPluginSource(
            blob_id=source.blob_id, entrypoint=source.entrypoint
        )
    return DomainPackagePluginSource(
        requirement=source.requirement, entrypoint=source.entrypoint
    )


def plugin_source_to_wire(source: DomainPluginSource) -> WirePluginSource:
    """Convert a domain plugin source to its wire value object.

    Args:
        source: Domain plugin source.

    Returns:
        Wire plugin source.
    """
    if isinstance(source, DomainScriptPluginSource):
        return WireScriptPluginSource(
            blob_id=source.blob_id, entrypoint=source.entrypoint
        )
    return WirePackagePluginSource(
        requirement=source.requirement, entrypoint=source.entrypoint
    )


def plugin_to_response(
    plugin: Plugin, response_class: type[PluginResponseT]
) -> PluginResponseT:
    """Convert a plugin entity to its response DTO.

    Args:
        plugin: Stored plugin.
        response_class: ``EvaluatorResponse`` or ``ImporterResponse``.

    Returns:
        Plugin response.
    """
    assert plugin.created is not None
    assert plugin.updated is not None
    fields: dict[str, Any] = {
        "id": plugin.id,
        "owner_id": plugin.owner_id,
        "name": plugin.name,
        "description": plugin.description,
        "metadata": plugin.metadata,
        "latest_version": plugin.latest_version,
        "created": plugin.created,
        "updated": plugin.updated,
    }
    if plugin.kind is PluginKind.IMPORTER:
        fields["provider"] = plugin.provider
    return response_class(**fields)


def plugin_version_to_response(
    version: PluginVersion, response_class: type[PluginVersionResponseT]
) -> PluginVersionResponseT:
    """Convert a plugin version entity to its response DTO.

    Args:
        version: Stored plugin version.
        response_class: ``EvaluatorVersionResponse`` or
            ``ImporterVersionResponse``.

    Returns:
        Plugin version response.
    """
    assert version.created is not None
    assert version.updated is not None
    fields: dict[str, Any] = {
        "id": version.id,
        _VERSION_PARENT_FIELD[response_class]: version.plugin_id,
        "version": version.version,
        "display_version": version.display_version,
        "source": plugin_source_to_wire(version.source),
        "created": version.created,
        "updated": version.updated,
    }
    return response_class(**fields)


def plugin_list_params_to_filter(
    params: ListParams,
    kind: PluginKind,
    filter_: Filter | None,
) -> PluginFilter:
    """Convert list params to the application plugin filter.

    Args:
        params: List params carrying pagination.
        kind: Plugin kind this listing is scoped to.
        filter_: Filter expression, when present on the wire params.

    Returns:
        Evaluator or importer filter, scoped to the given kind.
    """
    expression = filter_to_expression(filter_) if filter_ is not None else None
    filter_class = EvaluatorFilter if kind is PluginKind.EVALUATOR else ImporterFilter
    return filter_class(
        kind=kind,
        expression=expression,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def plugin_update_to_command(
    body: EvaluatorUpdateRequest | ImporterUpdateRequest,
) -> PluginUpdate:
    """Convert an evaluator or importer update request to a plugin command.

    Args:
        body: Evaluator or importer update request.

    Returns:
        Plugin update command.
    """
    return PluginUpdate(**body.model_dump(exclude_unset=True))
