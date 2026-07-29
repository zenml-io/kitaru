"""Shared plugin and plugin-version resolution."""

from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.domain.plugin import Plugin, PluginKind, PluginVersion


async def resolve_plugin(
    name: str, kind: PluginKind, repository: PluginRepository
) -> Plugin:
    """Resolve a plugin by kind and unique name."""
    return await repository.get_by_name(kind, name)


async def resolve_plugin_version(
    plugin: Plugin,
    version: int | None,
    repository: PluginRepository,
) -> PluginVersion:
    """Resolve an explicit version or the plugin's current latest version."""
    number = version if version is not None else plugin.latest_version
    return await repository.get_version_number(plugin.id, number)
