"""Shared evaluator and importer DTO conversions."""

import uuid
from typing import TypeVar

from kitaru.api_models.v1.base import ListParams
from kitaru.api_models.v1.evaluator import (
    EvaluatorListParams,
    EvaluatorResponse,
    EvaluatorUpdateRequest,
    EvaluatorVersionResponse,
)
from kitaru.api_models.v1.importer import (
    ImporterListParams,
    ImporterResponse,
    ImporterUpdateRequest,
    ImporterVersionResponse,
)
from kitaru.api_models.v1.plugin import (
    PackagePluginSource,
    PluginSource,
    ScriptPluginSource,
)
from kitaru.server.adapters.rest.mapping.partial import to_partial
from kitaru.server.application.models.plugin import (
    PluginFilter,
    PluginUpdate,
    PluginVersionFilter,
)
from kitaru.server.domain.plugin import (
    PackagePluginSource as DomainPackageSource,
)
from kitaru.server.domain.plugin import (
    Plugin,
    PluginKind,
    PluginVersion,
)
from kitaru.server.domain.plugin import (
    PluginSource as DomainPluginSource,
)
from kitaru.server.domain.plugin import (
    ScriptPluginSource as DomainScriptSource,
)

PluginResponseT = TypeVar("PluginResponseT", EvaluatorResponse, ImporterResponse)
PluginVersionResponseT = TypeVar(
    "PluginVersionResponseT", EvaluatorVersionResponse, ImporterVersionResponse
)


def plugin_to_response(
    plugin: Plugin, response_type: type[PluginResponseT]
) -> PluginResponseT:
    """Convert a plugin entity to a kind-specific response."""
    assert plugin.created is not None
    assert plugin.updated is not None
    values = {
        "id": plugin.id,
        "owner_id": plugin.owner_id,
        "name": plugin.name,
        "description": plugin.description,
        "metadata": plugin.metadata,
        "latest_version": plugin.latest_version,
        "created": plugin.created,
        "updated": plugin.updated,
    }
    if response_type is ImporterResponse:
        values["provider"] = plugin.provider
    return response_type.model_validate(values)


def plugin_version_to_response(
    version: PluginVersion,
    response_type: type[PluginVersionResponseT],
) -> PluginVersionResponseT:
    """Convert a plugin version to a kind-specific response."""
    assert version.created is not None
    assert version.updated is not None
    if isinstance(version.source, DomainPackageSource):
        source: PluginSource = PackagePluginSource(
            requirement=version.source.requirement,
            entrypoint=version.source.entrypoint,
        )
    else:
        source = ScriptPluginSource(
            blob_id=version.source.blob_id,
            entrypoint=version.source.entrypoint,
        )
    parent_name = (
        "evaluator_id" if response_type is EvaluatorVersionResponse else "importer_id"
    )
    return response_type.model_validate(
        {
            "id": version.id,
            parent_name: version.plugin_id,
            "version": version.version,
            "display_version": version.display_version,
            "source": source,
            "created": version.created,
            "updated": version.updated,
        }
    )


def plugin_list_params_to_filter(
    params: EvaluatorListParams | ImporterListParams, kind: PluginKind
) -> PluginFilter:
    """Convert kind-specific plugin list query parameters."""
    values = params.model_dump(mode="python")
    return PluginFilter(kind=kind, **values)


def plugin_version_list_filter(
    plugin_id: uuid.UUID, params: ListParams
) -> PluginVersionFilter:
    """Convert plugin version pagination parameters."""
    return PluginVersionFilter(
        plugin_id=plugin_id,
        cursor=params.cursor,
        size=params.size,
        sort=params.sort,
    )


def plugin_update_to_command(
    body: EvaluatorUpdateRequest | ImporterUpdateRequest,
) -> PluginUpdate:
    """Convert a plugin PATCH body while preserving unset fields."""
    return to_partial(PluginUpdate, body)


def plugin_source_to_domain(source: PluginSource) -> DomainPluginSource:
    """Convert a plugin source request to its domain value."""
    if isinstance(source, PackagePluginSource):
        return DomainPackageSource(
            requirement=source.requirement,
            entrypoint=source.entrypoint,
        )
    return DomainScriptSource(
        blob_id=source.blob_id,
        entrypoint=source.entrypoint,
    )
