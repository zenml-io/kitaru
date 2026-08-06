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
"""Plugin registry use cases, bound to one plugin kind."""

import uuid
from typing import Any

from kitaru.analytics.events import AnalyticsEvent
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.plugin import (
    PluginFilter,
    PluginUpdate,
    PluginVersionFilter,
)
from kitaru.server.application.services import analytics_events
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.names import RESERVED_PLUGIN_NAME_PREFIX
from kitaru.server.domain.plugin import (
    DefaultPluginReadOnly,
    Plugin,
    PluginKind,
    PluginSource,
    PluginVersion,
    ReservedPluginName,
    ScriptPluginSource,
)


class PluginService:
    """Plugin and version registry, bound to a single plugin kind."""

    def __init__(
        self,
        kind: PluginKind,
        repository: PluginRepository,
        blob_repository: BlobRepository,
        analytics: ServerAnalytics | None = None,
    ) -> None:
        """Initialize the service.

        Args:
            kind: Plugin kind this service manages.
            repository: Plugin repository.
            blob_repository: Blob repository, for script source validation.
            analytics: Analytics tracker, None skips tracking.
        """
        self.kind = kind
        self._repository = repository
        self._blob_repository = blob_repository
        self._analytics = analytics

    @staticmethod
    def _validate_plugin_mutation(plugin: Plugin) -> None:
        """Reject public mutation of a released default plugin.

        Args:
            plugin: Plugin targeted by the mutation.

        Raises:
            DefaultPluginReadOnly: The plugin is ownerless and reserved.
        """
        if plugin.owner_id is None and plugin.name.startswith(
            RESERVED_PLUGIN_NAME_PREFIX
        ):
            raise DefaultPluginReadOnly(plugin.name)

    async def create_plugin(
        self,
        name: str,
        description: str | None,
        provider: str | None,
        metadata: dict[str, Any],
        actor: AuthContext,
    ) -> Plugin:
        """Create a plugin owned by the caller.

        Args:
            name: Plugin name.
            description: Plugin description.
            provider: Source system, evaluators must leave this unset.
            metadata: Arbitrary metadata.
            actor: Caller context.

        Raises:
            DuplicatePluginName: The (kind, name) pair is already registered.
            InvalidPluginProvider: The kind is evaluator and provider is set.
            ReservedPluginName: ``name`` starts with the reserved default-plugin
                prefix.

        Returns:
            Created plugin.
        """
        if name.startswith(RESERVED_PLUGIN_NAME_PREFIX):
            raise ReservedPluginName(name)
        plugin = Plugin(
            owner_id=actor.account.id,
            kind=self.kind,
            name=name,
            description=description,
            provider=provider,
            metadata=metadata,
        )
        return await self._repository.create(plugin)

    async def get_plugin(self, plugin_id: uuid.UUID, actor: AuthContext) -> Plugin:
        """Get a plugin by id.

        Args:
            plugin_id: Id of the plugin.
            actor: Caller context.

        Raises:
            PluginNotFound: No plugin has this id.

        Returns:
            Stored plugin.
        """
        _ = actor
        return await self._repository.get(plugin_id)

    async def list_plugins(
        self, plugin_filter: PluginFilter, actor: AuthContext
    ) -> tuple[list[Plugin], str | None]:
        """List plugins matching a filter.

        Args:
            plugin_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching plugins and the next cursor.
        """
        _ = actor
        return await self._repository.query(plugin_filter)

    async def update_plugin(
        self, plugin_id: uuid.UUID, update: PluginUpdate, actor: AuthContext
    ) -> Plugin:
        """Partially update a plugin.

        Args:
            plugin_id: Id of the plugin.
            update: Fields to change, omitted fields stay unchanged.
            actor: Caller context.

        Raises:
            PluginNotFound: No plugin has this id.

        Returns:
            Updated plugin.
        """
        _ = actor
        plugin = await self._repository.get(plugin_id)
        self._validate_plugin_mutation(plugin)
        if "description" in update.model_fields_set:
            plugin.update_description(update.description)
        if "metadata" in update.model_fields_set:
            assert update.metadata is not None
            plugin.update_metadata(update.metadata)
        return await self._repository.update(plugin)

    async def delete_plugin(self, plugin_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a plugin, cascading its versions.

        Args:
            plugin_id: Id of the plugin.
            actor: Caller context.

        Raises:
            PluginNotFound: No plugin has this id.
            PluginInUse: A version is referenced by a stored evaluation.
        """
        _ = actor
        plugin = await self._repository.get(plugin_id)
        self._validate_plugin_mutation(plugin)
        await self._repository.delete(plugin_id)

    async def create_version(
        self,
        plugin_id: uuid.UUID,
        source: PluginSource,
        display_version: str | None,
        actor: AuthContext,
    ) -> PluginVersion:
        """Create a plugin version with a server-assigned version number.

        Args:
            plugin_id: Id of the plugin.
            source: Plugin code source.
            display_version: Human-readable designator.
            actor: Caller context.

        Raises:
            PluginNotFound: No plugin has this id.
            BlobNotFound: The script source names an unknown blob.

        Returns:
            Created plugin version.
        """
        plugin = await self._repository.get(plugin_id)
        self._validate_plugin_mutation(plugin)
        if isinstance(source, ScriptPluginSource):
            await self._blob_repository.get(source.blob_id)
        version = await self._repository.create_version(
            plugin_id, source, display_version
        )
        if self._analytics is not None:
            self._analytics.track(
                actor.account.id,
                AnalyticsEvent.PLUGIN_REGISTERED,
                analytics_events.build_plugin_registered_properties(self.kind, source),
            )
        return version

    async def get_version(
        self, plugin_id: uuid.UUID, version: int, actor: AuthContext
    ) -> PluginVersion:
        """Get a plugin version by plugin id and version number.

        Args:
            plugin_id: Id of the plugin.
            version: Version number.
            actor: Caller context.

        Raises:
            PluginVersionNotFound: No version with this number exists for
                this plugin.

        Returns:
            Stored plugin version.
        """
        _ = actor
        return await self._repository.get_version(plugin_id, version)

    async def list_versions(
        self, version_filter: PluginVersionFilter, actor: AuthContext
    ) -> tuple[list[PluginVersion], str | None]:
        """List plugin versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching plugin versions and the next cursor.
        """
        _ = actor
        return await self._repository.query_versions(version_filter)

    async def update_version(
        self,
        plugin_id: uuid.UUID,
        version: int,
        display_version: str | None,
        actor: AuthContext,
    ) -> PluginVersion:
        """Partially update a plugin version.

        Args:
            plugin_id: Id of the plugin.
            version: Version number.
            display_version: New display version, unchanged when ``None``.
            actor: Caller context.

        Raises:
            PluginVersionNotFound: No version with this number exists for
                this plugin.

        Returns:
            Updated plugin version.
        """
        _ = actor
        plugin = await self._repository.get(plugin_id)
        self._validate_plugin_mutation(plugin)
        plugin_version = await self._repository.get_version(plugin_id, version)
        if display_version is not None:
            plugin_version.update_display_version(display_version)
        return await self._repository.update_version(plugin_version)
