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
"""Importer DTO conversions."""

import kitaru.api_models.v1.plugins as plugin_models
from kitaru.api_models.v1.importers import (
    ImporterResponse,
    ImporterVersionResponse,
)
from kitaru.server.domain.plugin import Plugin, PluginVersion


def importer_to_response(plugin: Plugin) -> ImporterResponse:
    """Convert a plugin entity to its importer response DTO.

    Args:
        plugin: Stored plugin.

    Returns:
        Importer response.
    """
    assert plugin.created is not None
    assert plugin.updated is not None
    return ImporterResponse(
        id=plugin.id,
        owner_id=plugin.owner_id,
        name=plugin.name,
        provider=plugin.provider,
        metadata=plugin.metadata,
        latest_version=plugin.latest_version,
        created=plugin.created,
        updated=plugin.updated,
    )


def importer_version_to_response(version: PluginVersion) -> ImporterVersionResponse:
    """Convert a plugin version entity to its importer version response DTO.

    Args:
        version: Stored plugin version.

    Returns:
        Importer version response.
    """
    assert version.created is not None
    return ImporterVersionResponse(
        id=version.id,
        importer_id=version.plugin_id,
        version=version.version,
        format=plugin_models.PluginFormat(version.format.value),
        blob_id=version.blob_id,
        entrypoint=version.entrypoint,
        created=version.created,
    )
