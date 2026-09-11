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

from kitaru.api_models.v1.task import REQUIRES_CREDENTIALS_LABEL
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.domain.names import get_namespace
from kitaru.server.domain.plugin import (
    Plugin,
    PluginKind,
    PluginNotFound,
    PluginVersion,
)
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


async def has_connection_schema(
    kind: PluginKind, name: str, plugin_repository: PluginRepository
) -> bool:
    """Report whether the named plugin declares a connection schema.

    Args:
        kind: Plugin kind.
        name: Plugin name.
        plugin_repository: Plugin repository.

    Returns:
        Whether the plugin declares a connection schema, False once the
        plugin is deleted.
    """
    try:
        plugin = await plugin_repository.get_by_name(kind, name)
    except PluginNotFound:
        return False
    return plugin.connection_schema is not None


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
