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
"""SQL plugin and plugin version repository."""

import uuid
from collections.abc import Callable, Mapping

from sqlalchemy import Select, select, update

from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.evaluation import (
    EVALUATION_EVALUATOR_VERSION_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.orm.plugin import (
    PLUGIN_KIND_NAME_UNIQUE_CONSTRAINT,
    PLUGIN_VERSION_BLOB_ID_FOREIGN_KEY,
    PLUGIN_VERSION_PLUGIN_ID_VERSION_UNIQUE_CONSTRAINT,
    PluginORM,
    PluginVersionORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.plugin import PluginFilter, PluginVersionFilter
from kitaru.server.domain.base import DomainError, NotFoundError
from kitaru.server.domain.blob import BlobNotFound
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    DuplicatePluginVersion,
    Plugin,
    PluginInUse,
    PluginKind,
    PluginNotFound,
    PluginSource,
    PluginVersion,
    PluginVersionIdNotFound,
    PluginVersionNotFound,
    ScriptPluginSource,
)

PLUGIN_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": PluginORM.id,
    "name": PluginORM.name,
    "provider": PluginORM.provider,
}


class SQLPluginRepository(BaseSQLRepository[PluginORM]):
    """Plugin and plugin version repository backed by the application database."""

    orm_class = PluginORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return PluginNotFound(entity_id)

    async def create(self, plugin: Plugin) -> Plugin:
        """Persist a new plugin.

        Args:
            plugin: Plugin to store.

        Raises:
            DuplicatePluginName: The (kind, name) pair is already registered.

        Returns:
            Stored plugin with timestamps set.
        """
        row = PluginORM.from_domain(plugin)
        await self._add(
            row,
            {
                PLUGIN_KIND_NAME_UNIQUE_CONSTRAINT: lambda: DuplicatePluginName(
                    plugin.kind, plugin.name
                )
            },
        )
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
        row = await self._get_row(plugin_id)
        return row.to_domain()

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
        statement = select(PluginORM).where(
            PluginORM.kind == kind.value, PluginORM.name == name
        )
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise PluginNotFound(name)
        return row.to_domain()

    async def query(
        self, plugin_filter: PluginFilter
    ) -> tuple[list[Plugin], str | None]:
        """Query plugins matching a filter.

        Args:
            plugin_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugins and the next cursor.
        """
        statement = select(PluginORM).where(PluginORM.kind == plugin_filter.kind.value)
        if plugin_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    plugin_filter.expression, PLUGIN_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, plugin_filter, id_column=PluginORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

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
        row = await self._get_row(plugin.id)
        row.name = plugin.name
        row.description = plugin.description
        row.provider = plugin.provider
        row.metadata_ = plugin.metadata
        await self._flush(
            {
                PLUGIN_KIND_NAME_UNIQUE_CONSTRAINT: lambda: DuplicatePluginName(
                    plugin.kind, plugin.name
                )
            }
        )
        return row.to_domain()

    async def delete(self, plugin_id: uuid.UUID) -> None:
        """Delete a plugin by id, cascading its versions.

        Args:
            plugin_id: Id of the plugin.

        Raises:
            PluginNotFound: No plugin has this id.
            PluginInUse: A version is referenced by a stored evaluation.
        """
        await self._delete_row(
            plugin_id,
            {
                EVALUATION_EVALUATOR_VERSION_ID_FOREIGN_KEY: lambda: PluginInUse(
                    plugin_id
                )
            },
        )

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
        result = await self._session.execute(
            update(PluginORM)
            .where(PluginORM.id == plugin_id)
            .values(latest_version=PluginORM.latest_version + 1)
            .returning(PluginORM.latest_version)
        )
        version_number = result.scalar_one_or_none()
        if version_number is None:
            raise PluginNotFound(plugin_id)
        row = PluginVersionORM.from_domain(
            plugin_id, version_number, display_version, source
        )
        constraints: dict[str, Callable[[], DomainError]] = {
            PLUGIN_VERSION_PLUGIN_ID_VERSION_UNIQUE_CONSTRAINT: lambda: (
                DuplicatePluginVersion(plugin_id, version_number)
            ),
        }
        if isinstance(source, ScriptPluginSource):
            constraints[PLUGIN_VERSION_BLOB_ID_FOREIGN_KEY] = lambda: BlobNotFound(
                source.blob_id
            )
        await self._add(row, constraints)
        return row.to_domain()

    async def _get_version_row(
        self,
        statement: Select[tuple[PluginVersionORM]],
        plugin_id: uuid.UUID,
        version: int,
    ) -> PluginVersionORM:
        """Load a plugin version row, raising when the statement matches none.

        Args:
            statement: Select statement identifying at most one row.
            plugin_id: Id of the plugin, for the not-found error.
            version: Version number, for the not-found error.

        Raises:
            PluginVersionNotFound: No row matches the statement.

        Returns:
            Matching row.
        """
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise PluginVersionNotFound(plugin_id, version)
        return row

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
        statement = select(PluginVersionORM).where(
            PluginVersionORM.plugin_id == plugin_id, PluginVersionORM.version == version
        )
        row = await self._get_version_row(statement, plugin_id, version)
        return row.to_domain()

    async def get_version_by_id(self, plugin_version_id: uuid.UUID) -> PluginVersion:
        """Load a plugin version by id.

        Args:
            plugin_version_id: Id of the plugin version.

        Raises:
            PluginVersionIdNotFound: No plugin version has this id.

        Returns:
            Stored plugin version.
        """
        row = await self._session.get(PluginVersionORM, plugin_version_id)
        if row is None:
            raise PluginVersionIdNotFound(plugin_version_id)
        return row.to_domain()

    async def query_versions(
        self, version_filter: PluginVersionFilter
    ) -> tuple[list[PluginVersion], str | None]:
        """Query plugin versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching plugin versions and the next cursor.
        """
        statement = select(PluginVersionORM).where(
            PluginVersionORM.plugin_id == version_filter.plugin_id
        )
        rows, next_cursor = await paginate(
            self._session, statement, version_filter, id_column=PluginVersionORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update_version(self, plugin_version: PluginVersion) -> PluginVersion:
        """Persist changes to an existing plugin version.

        Args:
            plugin_version: Plugin version with modified fields.

        Raises:
            PluginVersionNotFound: No version has this id.

        Returns:
            Stored plugin version with the updated timestamp renewed.
        """
        statement = select(PluginVersionORM).where(
            PluginVersionORM.id == plugin_version.id
        )
        row = await self._get_version_row(
            statement, plugin_version.plugin_id, plugin_version.version
        )
        row.display_version = plugin_version.display_version
        await self._flush()
        return row.to_domain()
