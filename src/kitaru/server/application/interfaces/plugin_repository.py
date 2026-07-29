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
"""Plugin and plugin version repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.plugin import PluginFilter, PluginVersionFilter
from kitaru.server.domain.plugin import Plugin, PluginKind, PluginSource, PluginVersion


class PluginRepository(Protocol):
    """Plugin and plugin version persistence operations."""

    async def create(self, plugin: Plugin) -> Plugin:
        """Persist a new plugin.

        Args:
            plugin: Plugin to store.

        Raises:
            DuplicatePluginName: The (kind, name) pair is already registered.

        Returns:
            Stored plugin with timestamps set.
        """
        ...

    async def get(self, plugin_id: uuid.UUID) -> Plugin:
        """Load a plugin by id.

        Args:
            plugin_id: Id of the plugin.

        Raises:
            PluginNotFound: No plugin has this id.

        Returns:
            Stored plugin.
        """
        ...

    async def get_by_name(self, kind: PluginKind, name: str) -> Plugin:
        """Load a plugin by kind and name.

        Args:
            kind: Plugin kind.
            name: Plugin name.

        Raises:
            PluginNotFound: No plugin has this kind and name.

        Returns:
            Stored plugin.
        """
        ...

    async def query(
        self, plugin_filter: PluginFilter
    ) -> tuple[list[Plugin], str | None]:
        """Query plugins matching a filter.

        Args:
            plugin_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugins and the next cursor.
        """
        ...

    async def update(self, plugin: Plugin) -> Plugin:
        """Persist changes to an existing plugin.

        Args:
            plugin: Plugin with modified fields.

        Raises:
            PluginNotFound: No plugin has this id.
            DuplicatePluginName: The (kind, name) pair is already registered.

        Returns:
            Stored plugin with the updated timestamp renewed.
        """
        ...

    async def delete(self, plugin_id: uuid.UUID) -> None:
        """Delete a plugin by id, cascading its versions.

        Args:
            plugin_id: Id of the plugin.

        Raises:
            PluginNotFound: No plugin has this id.
        """
        ...

    async def create_version(
        self,
        plugin_id: uuid.UUID,
        source: PluginSource,
        display_version: str | None,
    ) -> PluginVersion:
        """Persist a new plugin version with a server-assigned version number.

        Args:
            plugin_id: Id of the plugin.
            source: Plugin code source.
            display_version: Human-readable designator.

        Raises:
            PluginNotFound: No plugin has this id.
            BlobNotFound: The script source names an unknown blob.
            DuplicatePluginVersion: The version number is already registered.

        Returns:
            Stored plugin version with timestamps set.
        """
        ...

    async def get_version(self, plugin_id: uuid.UUID, version: int) -> PluginVersion:
        """Load a plugin version by plugin id and version number.

        Args:
            plugin_id: Id of the plugin.
            version: Version number.

        Raises:
            PluginVersionNotFound: No version with this number exists for
                this plugin.

        Returns:
            Stored plugin version.
        """
        ...

    async def query_versions(
        self, version_filter: PluginVersionFilter
    ) -> tuple[list[PluginVersion], str | None]:
        """Query plugin versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugin versions and the next cursor.
        """
        ...

    async def update_version(self, plugin_version: PluginVersion) -> PluginVersion:
        """Persist changes to an existing plugin version.

        Args:
            plugin_version: Plugin version with modified fields.

        Raises:
            PluginVersionNotFound: No version has this id.

        Returns:
            Stored plugin version with the updated timestamp renewed.
        """
        ...
