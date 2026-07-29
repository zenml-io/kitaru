#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
"""Bound evaluator or importer registry use cases."""

import uuid

from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.plugin import (
    PluginFilter,
    PluginUpdate,
    PluginVersionFilter,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.plugin import (
    Plugin,
    PluginKind,
    PluginNotFound,
    PluginSource,
    PluginVersion,
    ScriptPluginSource,
)


class PluginService:
    """Registry use cases bound to one plugin kind."""

    def __init__(
        self,
        repository: PluginRepository,
        blob_repository: BlobRepository,
        kind: PluginKind,
    ) -> None:
        self._repository = repository
        self._blob_repository = blob_repository
        self._kind = kind

    @property
    def kind(self) -> PluginKind:
        """Return the plugin kind this service exposes."""
        return self._kind

    async def create_plugin(
        self,
        name: str,
        description: str | None,
        provider: str | None,
        metadata: dict,
        actor: AuthContext,
    ) -> Plugin:
        """Create a plugin of the bound kind."""
        return await self._repository.create(
            Plugin(
                owner_id=actor.account.id,
                kind=self._kind,
                name=name,
                description=description,
                provider=provider,
                metadata=metadata,
            )
        )

    async def get_plugin(self, plugin_id: uuid.UUID, actor: AuthContext) -> Plugin:
        """Get a plugin of the bound kind."""
        _ = actor
        plugin = await self._repository.get(plugin_id)
        if plugin.kind is not self._kind:
            raise PluginNotFound(plugin_id)
        return plugin

    async def list_plugins(
        self, plugin_filter: PluginFilter, actor: AuthContext
    ) -> tuple[list[Plugin], str | None]:
        """List plugins of the bound kind."""
        _ = actor
        scoped = plugin_filter.model_copy(update={"kind": self._kind})
        return await self._repository.query(scoped)

    async def update_plugin(
        self,
        plugin_id: uuid.UUID,
        command: PluginUpdate,
        actor: AuthContext,
    ) -> Plugin:
        """Partially update plugin metadata."""
        plugin = await self.get_plugin(plugin_id, actor)
        if "description" in command.model_fields_set:
            plugin.update_description(command.description)
        if "metadata" in command.model_fields_set:
            if command.metadata is None:
                raise ValidationError("Plugin metadata cannot be null")
            plugin.update_metadata(command.metadata)
        return await self._repository.update(plugin)

    async def delete_plugin(self, plugin_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a plugin and its versions."""
        await self.get_plugin(plugin_id, actor)
        await self._repository.delete(plugin_id)

    async def create_version(
        self,
        plugin_id: uuid.UUID,
        source: PluginSource,
        display_version: str | None,
        actor: AuthContext,
    ) -> PluginVersion:
        """Create the next immutable code version."""
        _ = actor
        await self.get_plugin(plugin_id, actor)
        if isinstance(source, ScriptPluginSource):
            await self._blob_repository.get(source.blob_id)
        version = await self._repository.next_version(plugin_id)
        return await self._repository.create_version(
            PluginVersion(
                plugin_id=plugin_id,
                version=version,
                display_version=display_version,
                source=source,
            )
        )

    async def get_version(
        self,
        plugin_id: uuid.UUID,
        version: int,
        actor: AuthContext,
    ) -> PluginVersion:
        """Get a numbered version of a plugin of the bound kind."""
        await self.get_plugin(plugin_id, actor)
        return await self._repository.get_version_number(plugin_id, version)

    async def list_versions(
        self,
        version_filter: PluginVersionFilter,
        actor: AuthContext,
    ) -> tuple[list[PluginVersion], str | None]:
        """List versions of a plugin of the bound kind."""
        await self.get_plugin(version_filter.plugin_id, actor)
        return await self._repository.query_versions(version_filter)

    async def update_version(
        self,
        plugin_id: uuid.UUID,
        version: int,
        display_version: str | None,
        actor: AuthContext,
    ) -> PluginVersion:
        """Update a plugin version's display label."""
        stored = await self.get_version(plugin_id, version, actor)
        stored.update_display_version(display_version)
        return await self._repository.update_version(stored)
