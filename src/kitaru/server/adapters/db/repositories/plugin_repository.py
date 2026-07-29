"""SQL plugin registry repository."""

import uuid

from sqlalchemy import select, update

from kitaru.server.adapters.db.orm.plugin import (
    PLUGIN_NAME_UNIQUE_CONSTRAINT,
    PluginORM,
    PluginVersionORM,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.plugin import (
    PluginFilter,
    PluginVersionFilter,
)
from kitaru.server.domain.base import NotFoundError
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    Plugin,
    PluginKind,
    PluginNotFound,
    PluginVersion,
    PluginVersionNotFound,
)


class SQLPluginRepository(BaseSQLRepository[PluginORM]):
    """Plugin and version repository backed by PostgreSQL."""

    orm_class = PluginORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return PluginNotFound(entity_id)

    async def create(self, plugin: Plugin) -> Plugin:
        row = PluginORM.from_domain(plugin)
        await self._add(
            row,
            {
                PLUGIN_NAME_UNIQUE_CONSTRAINT: lambda: DuplicatePluginName(
                    plugin.kind, plugin.name
                )
            },
        )
        return row.to_domain()

    async def get(self, plugin_id: uuid.UUID) -> Plugin:
        return (await self._get_row(plugin_id)).to_domain()

    async def get_by_name(self, kind: PluginKind, name: str) -> Plugin:
        row = (
            await self._session.scalars(
                select(PluginORM).where(
                    PluginORM.kind == kind.value, PluginORM.name == name
                )
            )
        ).one_or_none()
        if row is None:
            raise PluginNotFound(name)
        return row.to_domain()

    async def query(
        self, plugin_filter: PluginFilter
    ) -> tuple[list[Plugin], str | None]:
        statement = select(PluginORM).where(PluginORM.kind == plugin_filter.kind.value)
        if plugin_filter.name is not None:
            statement = statement.where(PluginORM.name == plugin_filter.name)
        if plugin_filter.provider is not None:
            statement = statement.where(PluginORM.provider == plugin_filter.provider)
        rows, cursor = await paginate(
            self._session, statement, plugin_filter, PluginORM.id
        )
        return [row.to_domain() for row in rows], cursor

    async def update(self, plugin: Plugin) -> Plugin:
        row = await self._get_row(plugin.id)
        row.description = plugin.description
        row.metadata_ = plugin.metadata
        row.latest_version = plugin.latest_version
        await self._session.flush()
        return row.to_domain()

    async def delete(self, plugin_id: uuid.UUID) -> None:
        await self._delete_row(plugin_id)

    async def next_version(self, plugin_id: uuid.UUID) -> int:
        version = (
            await self._session.execute(
                update(PluginORM)
                .where(PluginORM.id == plugin_id)
                .values(latest_version=PluginORM.latest_version + 1)
                .returning(PluginORM.latest_version)
            )
        ).scalar_one_or_none()
        if version is None:
            raise PluginNotFound(plugin_id)
        return version

    async def create_version(self, version: PluginVersion) -> PluginVersion:
        row = PluginVersionORM.from_domain(version)
        self._session.add(row)
        await self._session.flush()
        return row.to_domain()

    async def get_version(self, version_id: uuid.UUID) -> PluginVersion:
        row = await self._session.get(PluginVersionORM, version_id)
        if row is None:
            raise PluginVersionNotFound(version_id)
        return row.to_domain()

    async def get_version_number(
        self, plugin_id: uuid.UUID, version: int
    ) -> PluginVersion:
        row = (
            await self._session.scalars(
                select(PluginVersionORM).where(
                    PluginVersionORM.plugin_id == plugin_id,
                    PluginVersionORM.version == version,
                )
            )
        ).one_or_none()
        if row is None:
            raise PluginVersionNotFound(version)
        return row.to_domain()

    async def get_many_versions(
        self, ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, PluginVersion]:
        if not ids:
            return {}
        rows = (
            await self._session.scalars(
                select(PluginVersionORM).where(PluginVersionORM.id.in_(ids))
            )
        ).all()
        return {row.id: row.to_domain() for row in rows}

    async def query_versions(
        self, version_filter: PluginVersionFilter
    ) -> tuple[list[PluginVersion], str | None]:
        rows, cursor = await paginate(
            self._session,
            select(PluginVersionORM).where(
                PluginVersionORM.plugin_id == version_filter.plugin_id
            ),
            version_filter,
            PluginVersionORM.id,
        )
        return [row.to_domain() for row in rows], cursor

    async def update_version(self, version: PluginVersion) -> PluginVersion:
        row = await self._session.get(PluginVersionORM, version.id)
        if row is None:
            raise PluginVersionNotFound(version.id)
        row.display_version = version.display_version
        await self._session.flush()
        return row.to_domain()

    async def delete_version(self, version_id: uuid.UUID) -> None:
        row = await self._session.get(PluginVersionORM, version_id)
        if row is None:
            raise PluginVersionNotFound(version_id)
        await self._session.delete(row)
        await self._session.flush()
