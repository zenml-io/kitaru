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
"""Plugin repository interface."""

import uuid
from typing import Protocol

from kitaru.server.application.models.plugins import (
    PluginFilter,
    PluginVersionFilter,
)
from kitaru.server.domain.plugin import Plugin, PluginVersion


class PluginRepository(Protocol):
    """Plugin persistence operations."""

    async def create(self, plugin: Plugin) -> Plugin:
        """Persist a new plugin.

        Args:
            plugin: Plugin to store.

        Raises:
            DuplicatePluginName: The plugin name is already registered for
                the kind.

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

    async def get_many(self, plugin_ids: list[uuid.UUID]) -> dict[uuid.UUID, Plugin]:
        """Load plugins by id.

        Args:
            plugin_ids: Ids of the plugins.

        Returns:
            Stored plugins keyed by id, missing ids omitted.
        """
        ...

    async def query(self, plugin_filter: PluginFilter) -> tuple[list[Plugin], int]:
        """Query plugins matching a filter.

        Args:
            plugin_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugins and the total match count.
        """
        ...

    async def delete(self, plugin_id: uuid.UUID) -> None:
        """Delete a plugin by id, including its versions.

        Args:
            plugin_id: Id of the plugin.

        Raises:
            PluginNotFound: No plugin has this id.
        """
        ...

    async def create_version(self, version: PluginVersion) -> PluginVersion:
        """Persist a new plugin version under the next version number.

        Args:
            version: Plugin version to store.

        Raises:
            PluginNotFound: No plugin has the version's plugin id.
            BlobNotFound: No blob has the version's blob id.
            DuplicatePluginVersion: The allocated version number is
                already registered for the plugin.

        Returns:
            Stored plugin version with the version number and timestamp
            set.
        """
        ...

    async def get_version(self, plugin_id: uuid.UUID, version: int) -> PluginVersion:
        """Load a plugin version by version number.

        Args:
            plugin_id: Id of the plugin.
            version: Version number.

        Raises:
            PluginVersionNotFound: The plugin has no such version.

        Returns:
            Stored plugin version.
        """
        ...

    async def get_version_by_id(self, version_id: uuid.UUID) -> PluginVersion:
        """Load a plugin version by id.

        Args:
            version_id: Id of the plugin version.

        Raises:
            PluginVersionIdNotFound: No plugin version has this id.

        Returns:
            Stored plugin version.
        """
        ...

    async def get_versions_by_ids(
        self, version_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, PluginVersion]:
        """Load plugin versions by id.

        Args:
            version_ids: Ids of the plugin versions.

        Returns:
            Stored plugin versions keyed by id, missing ids omitted.
        """
        ...

    async def query_versions(
        self, version_filter: PluginVersionFilter
    ) -> tuple[list[PluginVersion], int]:
        """Query plugin versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugin versions and the total match count.
        """
        ...
