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
"""Generic plugin and plugin version resolution."""

import uuid
from collections.abc import Sequence

from kitaru.api_models.v1.task import REQUIRES_CREDENTIALS_LABEL
from kitaru.base import FrozenModel
from kitaru.server.application.interfaces.connection_repository import (
    ConnectionRepository,
)
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.services.connection_resolution import (
    resolve_connection_id,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.names import get_namespace
from kitaru.server.domain.plugin import Plugin, PluginKind, PluginVersion
from kitaru.server.domain.replay_config import PluginConfig
from kitaru.server.domain.task import RESERVED_LABEL_PREFIX


async def resolve_plugin(
    name: str, kind: PluginKind, repository: PluginRepository
) -> Plugin:
    """Resolve a plugin by kind and unique name.

    Args:
        name: Plugin name.
        kind: Plugin kind.
        repository: Plugin repository, queried for the given kind.

    Raises:
        PluginNotFound: No plugin has this kind and name.

    Returns:
        Resolved plugin.
    """
    return await repository.get_by_name(kind, name)


async def resolve_plugin_version(
    plugin: Plugin,
    version: int | None,
    repository: PluginRepository,
) -> PluginVersion:
    """Resolve an explicit plugin version or the plugin's latest version.

    Args:
        plugin: Plugin the version belongs to.
        version: Explicit version number, None resolves to the latest.
        repository: Plugin repository.

    Raises:
        PluginVersionNotFound: The resolved version has no matching plugin
            version.

    Returns:
        Resolved plugin version.
    """
    number = version if version is not None else plugin.latest_version
    return await repository.get_version(plugin.id, number)


class ResolvedPlugin(FrozenModel):
    """Resolved plugin."""

    plugin: Plugin
    plugin_version: PluginVersion
    connection_id: uuid.UUID | None
    requires_credentials: bool


async def resolve_plugin_credentials(
    plugin: Plugin,
    connection_id: uuid.UUID | None,
    connection_repository: ConnectionRepository,
) -> tuple[uuid.UUID | None, bool]:
    """Resolve a plugin's connection and whether it needs the worker's credentials.

    Args:
        plugin: Plugin providing the connection provider and schema.
        connection_id: Named connection, None resolves the provider's default.
        connection_repository: Connection repository.

    Raises:
        ConnectionNotFound: No connection has the named id.

    Returns:
        Resolved connection id and whether the claiming worker must hold the
        provider's credentials.
    """
    resolved = await resolve_connection_id(
        connection_id, plugin.provider, connection_repository
    )
    requires_credentials = plugin.connection_schema is not None and resolved is None
    return resolved, requires_credentials


async def resolve_plugin_config(
    name: str,
    version: int | None,
    kind: PluginKind,
    connection_id: uuid.UUID | None,
    plugin_repository: PluginRepository,
    connection_repository: ConnectionRepository,
) -> ResolvedPlugin:
    """Resolve a plugin, its version, and its connection credentials together.

    Args:
        name: Plugin name.
        version: Explicit version number, None resolves to the latest.
        kind: Plugin kind.
        connection_id: Named connection, None resolves the provider's default.
        plugin_repository: Plugin repository, queried for the given kind.
        connection_repository: Connection repository.

    Raises:
        PluginNotFound: No plugin has this kind and name.
        PluginVersionNotFound: The resolved version has no matching plugin
            version.
        ConnectionNotFound: No connection has the named id.

    Returns:
        Resolved plugin, version, connection id, and credential requirement.
    """
    plugin = await resolve_plugin(name, kind, plugin_repository)
    plugin_version = await resolve_plugin_version(plugin, version, plugin_repository)
    connection_id, requires_credentials = await resolve_plugin_credentials(
        plugin, connection_id, connection_repository
    )
    return ResolvedPlugin(
        plugin=plugin,
        plugin_version=plugin_version,
        connection_id=connection_id,
        requires_credentials=requires_credentials,
    )


def check_unique_plugin_versions(configs: Sequence[PluginConfig], label: str) -> None:
    """Reject a resolved plugin version that appears more than once.

    Args:
        configs: Resolved plugin configs.
        label: Plugin kind, used to name the list in the error message.

    Raises:
        ValidationError: Two configs resolve to the same plugin version.
    """
    seen_ids: set[uuid.UUID] = set()
    for config in configs:
        if config.plugin_version_id in seen_ids:
            raise ValidationError(
                f"An {label} version appears more than once in the {label} list"
            )
        seen_ids.add(config.plugin_version_id)


PLUGIN_NAMESPACE_LABEL = f"{RESERVED_LABEL_PREFIX}plugin_namespace"
PLUGIN_PROVIDER_LABEL = f"{RESERVED_LABEL_PREFIX}provider"


def get_plugin_task_labels(
    name: str, provider: str | None = None, requires_credentials: bool = False
) -> dict[str, str]:
    """Build the labels stamped on a task running a plugin.

    Args:
        name: Plugin name.
        provider: Plugin provider, None stamps no provider label.
        requires_credentials: Whether the claiming worker must hold the
            provider's credentials.

    Returns:
        Plugin task labels.
    """
    namespace = get_namespace(name)
    labels = {}
    if namespace is not None:
        labels[PLUGIN_NAMESPACE_LABEL] = namespace
    if provider is not None:
        labels[PLUGIN_PROVIDER_LABEL] = provider
        if requires_credentials:
            labels[REQUIRES_CREDENTIALS_LABEL] = provider
    return labels
