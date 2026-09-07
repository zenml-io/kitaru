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

from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.domain.names import get_namespace
from kitaru.server.domain.plugin import Plugin, PluginKind, PluginVersion
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


PLUGIN_NAMESPACE_LABEL = f"{RESERVED_LABEL_PREFIX}plugin_namespace"
PLUGIN_PROVIDER_LABEL = f"{RESERVED_LABEL_PREFIX}provider"


def get_plugin_task_labels(name: str) -> dict[str, str]:
    """Build the labels stamped on a task running a plugin.

    Args:
        name: Plugin name.

    Returns:
        Labels, empty for a plugin without a namespace.
    """
    namespace = get_namespace(name)
    if namespace is None:
        return {}
    return {PLUGIN_NAMESPACE_LABEL: namespace}


def get_importer_task_labels(name: str, provider: str | None) -> dict[str, str]:
    """Build the labels stamped on a task running an importer.

    Args:
        name: Plugin name.
        provider: Provider the importer reads, None stamps no provider label.

    Returns:
        Labels, empty for an importer with neither a namespace nor a
        provider.
    """
    labels = get_plugin_task_labels(name)
    if provider is not None:
        labels[PLUGIN_PROVIDER_LABEL] = provider
    return labels
