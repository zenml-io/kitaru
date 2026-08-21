#!/usr/bin/env python3
"""Validate installed bundled-plugin artifacts and default registration."""

import argparse
import asyncio
import importlib
import importlib.metadata
import uuid
from typing import cast

from kitaru.server.api import bootstrap
from kitaru.server.api.bootstrap import (
    DEFAULT_PLUGIN_DEFINITIONS,
    register_default_plugins,
)
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    PackagePluginSource,
    Plugin,
    PluginKind,
    PluginNotFound,
    PluginSource,
    PluginVersion,
    PluginVersionNotFound,
)
from kitaru.task.plugins import load_source_ref


class _MemoryPluginRepository:
    """Store the plugin operations used by default registration."""

    def __init__(self) -> None:
        """Initialize empty plugin and version stores."""
        self.plugins: dict[uuid.UUID, Plugin] = {}
        self.versions: dict[tuple[uuid.UUID, int], PluginVersion] = {}

    async def create(self, plugin: Plugin) -> Plugin:
        """Store one plugin."""
        for stored in self.plugins.values():
            if stored.kind == plugin.kind and stored.name == plugin.name:
                raise DuplicatePluginName(plugin.kind, plugin.name)
        self.plugins[plugin.id] = plugin
        return plugin

    async def get_by_name(self, kind: PluginKind, name: str) -> Plugin:
        """Load one plugin by kind and name."""
        for plugin in self.plugins.values():
            if plugin.kind == kind and plugin.name == name:
                return plugin
        raise PluginNotFound(name)

    async def create_version(
        self,
        plugin_id: uuid.UUID,
        source: PluginSource,
        display_version: str | None,
    ) -> PluginVersion:
        """Store the next version for one plugin."""
        plugin = self.plugins.get(plugin_id)
        if plugin is None:
            raise PluginNotFound(plugin_id)
        version_number = plugin.latest_version + 1
        stored_plugin = plugin.model_copy(update={"latest_version": version_number})
        self.plugins[plugin_id] = stored_plugin
        version = PluginVersion(
            plugin_id=plugin_id,
            version=version_number,
            display_version=display_version,
            source=source,
        )
        self.versions[(plugin_id, version_number)] = version
        return version

    async def get_version(self, plugin_id: uuid.UUID, version: int) -> PluginVersion:
        """Load one stored plugin version."""
        try:
            return self.versions[(plugin_id, version)]
        except KeyError as error:
            raise PluginVersionNotFound(plugin_id, version) from error


async def _probe(expected_requirements: set[str], import_modules: set[str]) -> None:
    for requirement in expected_requirements:
        distribution, separator, expected_version = requirement.partition("==")
        if not separator:
            raise RuntimeError(f"Bundled requirement is not exact: {requirement!r}")
        installed_version = importlib.metadata.version(distribution)
        if installed_version != expected_version:
            raise RuntimeError(
                f"Bundled requirement {requirement!r} installed as "
                f"{installed_version!r}"
            )

    for module_name in import_modules:
        module = importlib.import_module(module_name)
        if not getattr(module, "__all__", None):
            raise RuntimeError(f"Standalone package {module_name!r} has no public API")

    definitions = tuple(
        definition
        for definition in DEFAULT_PLUGIN_DEFINITIONS
        if definition.requirement in expected_requirements
    )
    actual_requirements = {definition.requirement for definition in definitions}
    if actual_requirements != expected_requirements:
        raise RuntimeError(
            "Default requirements differ from installed artifacts: "
            f"expected={sorted(expected_requirements)!r}, "
            f"actual={sorted(actual_requirements)!r}"
        )

    identities = {(definition.kind, definition.name) for definition in definitions}
    if len(identities) != len(definitions):
        raise RuntimeError("Default plugin identities are not unique")
    for definition in definitions:
        load_source_ref(definition.entrypoint, definition.kind.value.capitalize())

    repository = _MemoryPluginRepository()
    plugin_repository = cast(PluginRepository, repository)
    original_definitions = bootstrap.DEFAULT_PLUGIN_DEFINITIONS
    bootstrap.DEFAULT_PLUGIN_DEFINITIONS = definitions
    try:
        await register_default_plugins(plugin_repository)
        await register_default_plugins(plugin_repository)
    finally:
        bootstrap.DEFAULT_PLUGIN_DEFINITIONS = original_definitions
    if len(repository.plugins) != len(definitions):
        raise RuntimeError("Default registration did not create one row per definition")
    if len(repository.versions) != len(definitions):
        raise RuntimeError("Repeated default registration was not idempotent")

    by_identity = {
        (definition.kind, definition.name): definition for definition in definitions
    }
    for plugin in repository.plugins.values():
        definition = by_identity[(plugin.kind, plugin.name)]
        if plugin.owner_id is not None or plugin.latest_version != 1:
            raise RuntimeError(f"Plugin {plugin.name!r} has invalid registration state")
        version = await repository.get_version(plugin.id, 1)
        if not isinstance(version.source, PackagePluginSource):
            raise RuntimeError(f"Plugin {plugin.name!r} is not package-backed")
        if version.source.requirement != definition.requirement:
            raise RuntimeError(f"Plugin {plugin.name!r} has the wrong requirement")
        if version.source.entrypoint != definition.entrypoint:
            raise RuntimeError(f"Plugin {plugin.name!r} has the wrong entrypoint")
        if version.display_version != definition.display_version:
            raise RuntimeError(f"Plugin {plugin.name!r} has the wrong display version")


def main() -> int:
    """Validate installed entrypoints and their stored plugin sources."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--requirement",
        action="append",
        default=[],
        help="Exact installed plugin requirement expected in the bundle.",
    )
    parser.add_argument(
        "--module",
        action="append",
        default=[],
        help="Standalone package module that must import from the installed wheel.",
    )
    arguments = parser.parse_args()
    asyncio.run(_probe(set(arguments.requirement), set(arguments.module)))
    print("Installed plugin artifact probe passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
