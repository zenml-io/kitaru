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
"""SQL plugin repository."""

import uuid

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.schemas.plugin import (
    PLUGIN_NAME_UNIQUE_CONSTRAINT,
    PLUGIN_VERSION_BLOB_ID_FOREIGN_KEY,
    PLUGIN_VERSION_UNIQUE_CONSTRAINT,
    PluginSchema,
    PluginVersionSchema,
)
from kitaru.server.application.models.plugins import (
    PluginFilter,
    PluginVersionFilter,
)
from kitaru.server.domain.blob import BlobNotFound
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    DuplicatePluginVersion,
    Plugin,
    PluginNotFound,
    PluginVersion,
    PluginVersionIdNotFound,
    PluginVersionNotFound,
)


class SQLPluginRepository:
    """Plugin repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

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
        row = PluginSchema.from_domain(plugin)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == PLUGIN_NAME_UNIQUE_CONSTRAINT:
                raise DuplicatePluginName(plugin.name) from exc
            raise
        return row.to_domain()

    async def get(self, plugin_id: uuid.UUID) -> Plugin:
        """Load a plugin by id.

        Args:
            plugin_id: Id of the plugin.

        Raises:
            PluginNotFound: No plugin has this id.

        Returns:
            Stored plugin.
        """
        row = await self._session.get(PluginSchema, plugin_id)
        if row is None:
            raise PluginNotFound(plugin_id)
        return row.to_domain()

    async def get_many(self, plugin_ids: list[uuid.UUID]) -> dict[uuid.UUID, Plugin]:
        """Load plugins by id.

        Args:
            plugin_ids: Ids of the plugins.

        Returns:
            Stored plugins keyed by id, missing ids omitted.
        """
        if not plugin_ids:
            return {}
        statement = select(PluginSchema).where(col(PluginSchema.id).in_(plugin_ids))
        rows = (await self._session.scalars(statement)).all()
        return {row.id: row.to_domain() for row in rows}

    async def query(self, plugin_filter: PluginFilter) -> tuple[list[Plugin], int]:
        """Query plugins matching a filter.

        Args:
            plugin_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugins and the total match count.
        """
        statement = select(PluginSchema).where(
            col(PluginSchema.kind) == plugin_filter.kind.value
        )
        if plugin_filter.name is not None:
            statement = statement.where(col(PluginSchema.name) == plugin_filter.name)
        if plugin_filter.provider is not None:
            statement = statement.where(
                col(PluginSchema.provider) == plugin_filter.provider
            )
        if plugin_filter.owner_id is not None:
            statement = statement.where(
                col(PluginSchema.owner_id) == plugin_filter.owner_id
            )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(PluginSchema.id),
            page=plugin_filter.page,
            page_size=plugin_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

    async def delete(self, plugin_id: uuid.UUID) -> None:
        """Delete a plugin by id, including its versions.

        Args:
            plugin_id: Id of the plugin.

        Raises:
            PluginNotFound: No plugin has this id.
        """
        row = await self._session.get(PluginSchema, plugin_id)
        if row is None:
            raise PluginNotFound(plugin_id)
        await self._session.delete(row)
        await self._session.flush()

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
        statement = (
            update(PluginSchema)
            .where(col(PluginSchema.id) == version.plugin_id)
            .values(latest_version=col(PluginSchema.latest_version) + 1)
            .returning(col(PluginSchema.latest_version))
            .execution_options(synchronize_session=False)
        )
        allocated = (await self._session.execute(statement)).scalar_one_or_none()
        if allocated is None:
            raise PluginNotFound(version.plugin_id)
        row = PluginVersionSchema.from_domain(
            version.model_copy(update={"version": allocated})
        )
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            constraint = violated_constraint(exc)
            if constraint == PLUGIN_VERSION_UNIQUE_CONSTRAINT:
                raise DuplicatePluginVersion(allocated) from exc
            if constraint == PLUGIN_VERSION_BLOB_ID_FOREIGN_KEY:
                raise BlobNotFound(version.blob_id) from exc
            raise
        return row.to_domain()

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
        statement = select(PluginVersionSchema).where(
            col(PluginVersionSchema.plugin_id) == plugin_id,
            col(PluginVersionSchema.version) == version,
        )
        row = (await self._session.scalars(statement)).first()
        if row is None:
            raise PluginVersionNotFound(plugin_id, version)
        return row.to_domain()

    async def get_version_by_id(self, version_id: uuid.UUID) -> PluginVersion:
        """Load a plugin version by id.

        Args:
            version_id: Id of the plugin version.

        Raises:
            PluginVersionIdNotFound: No plugin version has this id.

        Returns:
            Stored plugin version.
        """
        row = await self._session.get(PluginVersionSchema, version_id)
        if row is None:
            raise PluginVersionIdNotFound(version_id)
        return row.to_domain()

    async def get_versions_by_ids(
        self, version_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, PluginVersion]:
        """Load plugin versions by id.

        Args:
            version_ids: Ids of the plugin versions.

        Returns:
            Stored plugin versions keyed by id, missing ids omitted.
        """
        if not version_ids:
            return {}
        statement = select(PluginVersionSchema).where(
            col(PluginVersionSchema.id).in_(version_ids)
        )
        rows = (await self._session.scalars(statement)).all()
        return {row.id: row.to_domain() for row in rows}

    async def query_versions(
        self, version_filter: PluginVersionFilter
    ) -> tuple[list[PluginVersion], int]:
        """Query plugin versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugin versions and the total match count.
        """
        statement = select(PluginVersionSchema).where(
            col(PluginVersionSchema.plugin_id) == version_filter.plugin_id
        )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(PluginVersionSchema.version),
            page=version_filter.page,
            page_size=version_filter.page_size,
        )
        return [row.to_domain() for row in rows], total
