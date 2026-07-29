"""SQL agent repositories."""

import uuid

from sqlalchemy import delete, exists, select, update
from sqlalchemy.exc import IntegrityError

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.orm.agent import (
    AGENT_NAME_UNIQUE_CONSTRAINT,
    AgentORM,
)
from kitaru.server.adapters.db.orm.agent_version import (
    AGENT_VERSION_UNIQUE_CONSTRAINT,
    AgentVersionORM,
    AgentVersionSecretORM,
)
from kitaru.server.adapters.db.orm.session import SessionORM
from kitaru.server.adapters.db.orm.task import TaskORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.agent import AgentFilter
from kitaru.server.application.models.agent_version import AgentVersionFilter
from kitaru.server.domain.agent import (
    Agent,
    AgentNotFound,
    DuplicateAgentName,
)
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotFound,
)
from kitaru.server.domain.base import NotFoundError


class SQLAgentRepository(BaseSQLRepository[AgentORM]):
    """Agent repository backed by PostgreSQL."""

    orm_class = AgentORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return AgentNotFound(entity_id)

    async def create(self, agent: Agent) -> Agent:
        row = AgentORM.from_domain(agent)
        await self._add(
            row,
            {AGENT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateAgentName(agent.name)},
        )
        return row.to_domain()

    async def get(self, agent_id: uuid.UUID, exclusive: bool = False) -> Agent:
        statement = select(AgentORM).where(AgentORM.id == agent_id)
        if exclusive:
            statement = statement.with_for_update()
        row = (await self._session.scalars(statement)).one_or_none()
        if row is None:
            raise AgentNotFound(agent_id)
        return row.to_domain()

    async def get_by_name(self, name: str) -> Agent:
        row = (
            await self._session.scalars(select(AgentORM).where(AgentORM.name == name))
        ).one_or_none()
        if row is None:
            raise AgentNotFound(name)
        return row.to_domain()

    async def query(self, agent_filter: AgentFilter) -> tuple[list[Agent], str | None]:
        statement = select(AgentORM)
        if agent_filter.name is not None:
            statement = statement.where(AgentORM.name == agent_filter.name)
        rows, cursor = await paginate(
            self._session, statement, agent_filter, AgentORM.id
        )
        return [row.to_domain() for row in rows], cursor

    async def update(self, agent: Agent) -> Agent:
        row = await self._get_row(agent.id)
        row.name = agent.name
        row.description = agent.description
        row.latest_version = agent.latest_version
        await self._flush(
            {AGENT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateAgentName(agent.name)}
        )
        return row.to_domain()

    async def delete(self, agent_id: uuid.UUID) -> None:
        await self._delete_row(agent_id)

    async def next_version(self, agent_id: uuid.UUID) -> int:
        statement = (
            update(AgentORM)
            .where(AgentORM.id == agent_id)
            .values(latest_version=AgentORM.latest_version + 1)
            .returning(AgentORM.latest_version)
        )
        version = (await self._session.execute(statement)).scalar_one_or_none()
        if version is None:
            raise AgentNotFound(agent_id)
        return version

    async def version_is_frozen(self, version_id: uuid.UUID) -> bool:
        statement = select(
            exists().where(
                (SessionORM.agent_version_id == version_id)
                | (TaskORM.agent_version_id == version_id)
            )
        )
        return bool(await self._session.scalar(statement))


class SQLAgentVersionRepository(BaseSQLRepository[AgentVersionORM]):
    """Agent-version repository backed by PostgreSQL."""

    orm_class = AgentVersionORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        return AgentVersionNotFound(entity_id)

    async def _secret_ids(self, version_id: uuid.UUID) -> list[uuid.UUID]:
        statement = (
            select(AgentVersionSecretORM.secret_id)
            .where(AgentVersionSecretORM.agent_version_id == version_id)
            .order_by(AgentVersionSecretORM.index)
        )
        return list((await self._session.scalars(statement)).all())

    async def _to_domain(self, row: AgentVersionORM) -> AgentVersion:
        return row.to_domain(await self._secret_ids(row.id))

    async def create(self, version: AgentVersion) -> AgentVersion:
        row = AgentVersionORM.from_domain(version)
        await self._add(row)
        if version.run_spec is not None:
            for index, secret_id in enumerate(version.run_spec.secret_ids):
                self._session.add(
                    AgentVersionSecretORM(
                        agent_version_id=version.id,
                        secret_id=secret_id,
                        index=index,
                    )
                )
            try:
                await self._session.flush()
            except IntegrityError as exc:
                if violated_constraint(exc) == AGENT_VERSION_UNIQUE_CONSTRAINT:
                    raise
                raise
        return await self._to_domain(row)

    async def get(self, version_id: uuid.UUID) -> AgentVersion:
        return await self._to_domain(await self._get_row(version_id))

    async def get_by_version(self, agent_id: uuid.UUID, version: int) -> AgentVersion:
        row = (
            await self._session.scalars(
                select(AgentVersionORM).where(
                    AgentVersionORM.agent_id == agent_id,
                    AgentVersionORM.version == version,
                )
            )
        ).one_or_none()
        if row is None:
            raise AgentVersionNotFound(agent_id)
        return await self._to_domain(row)

    async def get_many(self, ids: list[uuid.UUID]) -> dict[uuid.UUID, AgentVersion]:
        rows = await self._load_by_ids(ids)
        return {row_id: await self._to_domain(row) for row_id, row in rows.items()}

    async def query(
        self, version_filter: AgentVersionFilter
    ) -> tuple[list[AgentVersion], str | None]:
        rows, cursor = await paginate(
            self._session,
            select(AgentVersionORM).where(
                AgentVersionORM.agent_id == version_filter.agent_id
            ),
            version_filter,
            AgentVersionORM.id,
        )
        return [await self._to_domain(row) for row in rows], cursor

    async def update(self, version: AgentVersion) -> AgentVersion:
        row = await self._get_row(version.id)
        source = AgentVersionORM.from_domain(version)
        for name in (
            "display_version",
            "description",
            "run_command",
            "run_working_dir",
            "run_env",
            "run_timeout_seconds",
            "capabilities",
        ):
            setattr(row, name, getattr(source, name))
        await self._session.execute(
            delete(AgentVersionSecretORM).where(
                AgentVersionSecretORM.agent_version_id == version.id
            )
        )
        if version.run_spec is not None:
            for index, secret_id in enumerate(version.run_spec.secret_ids):
                self._session.add(
                    AgentVersionSecretORM(
                        agent_version_id=version.id,
                        secret_id=secret_id,
                        index=index,
                    )
                )
        await self._session.flush()
        return await self._to_domain(row)

    async def delete(self, version_id: uuid.UUID) -> None:
        await self._delete_row(version_id)
