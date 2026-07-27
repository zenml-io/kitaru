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
"""Plugin use cases."""

import uuid
from typing import Any

from kitaru.server.application.interfaces.blob_repository import (
    BlobRepository,
)
from kitaru.server.application.interfaces.plugin_repository import (
    PluginRepository,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.plugins import (
    PluginFilter,
    PluginVersionFilter,
)
from kitaru.server.domain.plugin import (
    Plugin,
    PluginFormat,
    PluginKind,
    PluginNotFound,
    PluginVersion,
)


class PluginService:
    """Plugin use cases."""

    def __init__(
        self,
        repository: PluginRepository,
        blob_repository: BlobRepository,
        kind: PluginKind,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Plugin repository.
            blob_repository: Blob repository.
            kind: Plugin kind the service is bound to.
        """
        self._repository = repository
        self._blob_repository = blob_repository
        self._kind = kind

    @property
    def kind(self) -> PluginKind:
        """Plugin kind the service is bound to.

        Returns:
            Plugin kind the service is bound to.
        """
        return self._kind

    async def create_plugin(
        self,
        name: str,
        provider: str | None,
        metadata: dict[str, Any],
        actor: AuthContext,
    ) -> Plugin:
        """Create a plugin owned by the caller.

        Args:
            name: Plugin name.
            provider: Provider the plugin imports from.
            metadata: Kind-specific configuration.
            actor: Caller context.

        Raises:
            InvalidPlugin: The plugin carries fields its kind does not
                support.
            DuplicatePluginName: The plugin name is already registered for
                the kind.

        Returns:
            Created plugin.
        """
        plugin = Plugin(
            owner_id=actor.account.id,
            kind=self._kind,
            name=name,
            provider=provider,
            metadata=metadata,
        )
        return await self._repository.create(plugin)

    async def get_plugin(self, plugin_id: uuid.UUID, actor: AuthContext) -> Plugin:
        """Get a plugin of the bound kind by id.

        Args:
            plugin_id: Id of the plugin.
            actor: Caller context.

        Raises:
            PluginNotFound: No plugin has this id, or the plugin has
                another kind.

        Returns:
            Stored plugin.
        """
        _ = actor
        plugin = await self._repository.get(plugin_id)
        if plugin.kind is not self._kind:
            raise PluginNotFound(plugin_id)
        return plugin

    async def list_plugins(
        self, plugin_filter: PluginFilter, actor: AuthContext
    ) -> tuple[list[Plugin], int]:
        """List plugins matching a filter.

        Args:
            plugin_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching plugins and the total match count.
        """
        _ = actor
        return await self._repository.query(plugin_filter)

    async def delete_plugin(self, plugin_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a plugin and its versions.

        Args:
            plugin_id: Id of the plugin.
            actor: Caller context.

        Raises:
            PluginNotFound: No plugin has this id, or the plugin has
                another kind.
        """
        await self.get_plugin(plugin_id, actor=actor)
        await self._repository.delete(plugin_id)

    async def create_version(
        self,
        plugin_id: uuid.UUID,
        format: PluginFormat,
        blob_id: uuid.UUID,
        entrypoint: str,
        actor: AuthContext,
    ) -> PluginVersion:
        """Create a plugin version under the next version number.

        Args:
            plugin_id: Id of the plugin.
            format: Code format.
            blob_id: Id of the code blob.
            entrypoint: Attribute implementing the plugin.
            actor: Caller context.

        Raises:
            PluginNotFound: No plugin has this id, or the plugin has
                another kind.
            BlobNotFound: No blob has this id.
            InvalidPluginVersion: The entrypoint violates its shape rules.

        Returns:
            Created plugin version.
        """
        await self.get_plugin(plugin_id, actor=actor)
        await self._blob_repository.get(blob_id)
        version = PluginVersion(
            plugin_id=plugin_id,
            format=format,
            blob_id=blob_id,
            entrypoint=entrypoint,
        )
        return await self._repository.create_version(version)

    async def get_version(
        self,
        plugin_id: uuid.UUID,
        version: int,
        actor: AuthContext,
    ) -> PluginVersion:
        """Get a plugin version by version number.

        Args:
            plugin_id: Id of the plugin.
            version: Version number.
            actor: Caller context.

        Raises:
            PluginNotFound: No plugin has this id, or the plugin has
                another kind.
            PluginVersionNotFound: The plugin has no such version.

        Returns:
            Stored plugin version.
        """
        await self.get_plugin(plugin_id, actor=actor)
        return await self._repository.get_version(plugin_id, version)

    async def list_versions(
        self,
        version_filter: PluginVersionFilter,
        actor: AuthContext,
    ) -> tuple[list[PluginVersion], int]:
        """List the versions of a plugin.

        Args:
            version_filter: Filter and pagination parameters.
            actor: Caller context.

        Raises:
            PluginNotFound: No plugin has the filtered id, or the plugin
                has another kind.

        Returns:
            Page of matching plugin versions and the total match count.
        """
        await self.get_plugin(version_filter.plugin_id, actor=actor)
        return await self._repository.query_versions(version_filter)
