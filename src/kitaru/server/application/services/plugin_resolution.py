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
"""Registry plugin resolution helper."""

from kitaru.server.application.interfaces.plugin_repository import (
    PluginRepository,
)
from kitaru.server.application.models.plugins import PluginFilter
from kitaru.server.domain.plugin import (
    Plugin,
    PluginKind,
    PluginNameNotFound,
    PluginVersion,
)


async def resolve_plugin(
    plugin_repository: PluginRepository,
    kind: PluginKind,
    name: str,
    version: int | None,
) -> tuple[Plugin, PluginVersion]:
    """Resolve the plugin and registered version a reference runs.

    Args:
        plugin_repository: Plugin repository.
        kind: Kind of the referenced plugin.
        name: Name of the referenced plugin.
        version: Referenced version number, the latest one for ``None``.

    Raises:
        PluginNameNotFound: No plugin of the kind has the name.
        PluginVersionNotFound: The plugin has no such version.

    Returns:
        Resolved plugin and plugin version.
    """
    plugins, _ = await plugin_repository.query(
        PluginFilter(kind=kind, name=name, page_size=1)
    )
    if not plugins:
        raise PluginNameNotFound(kind, name)
    plugin = plugins[0]
    resolved = version if version is not None else plugin.latest_version
    return plugin, await plugin_repository.get_version(plugin.id, resolved)


async def resolve_plugin_version(
    plugin_repository: PluginRepository,
    kind: PluginKind,
    name: str,
    version: int | None,
) -> PluginVersion:
    """Resolve the registered version a plugin reference runs.

    Args:
        plugin_repository: Plugin repository.
        kind: Kind of the referenced plugin.
        name: Name of the referenced plugin.
        version: Referenced version number, the latest one for ``None``.

    Raises:
        PluginNameNotFound: No plugin of the kind has the name.
        PluginVersionNotFound: The plugin has no such version.

    Returns:
        Resolved plugin version.
    """
    _, resolved = await resolve_plugin(plugin_repository, kind, name, version)
    return resolved
