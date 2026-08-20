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
"""SQL agent version repository."""

import uuid
from collections.abc import Mapping, Sequence

from sqlalchemy import delete, select, update

from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.filtering import (
    FilterBinding,
    build_tag_condition_binding,
    compile_filter_expression,
)
from kitaru.server.adapters.db.orm.agent import AgentORM
from kitaru.server.adapters.db.orm.agent_version import AgentVersionORM
from kitaru.server.adapters.db.orm.agent_version_secret import (
    AGENT_VERSION_SECRET_SECRET_ID_FOREIGN_KEY,
    AgentVersionSecretORM,
)
from kitaru.server.adapters.db.orm.experiment_run import (
    EXPERIMENT_RUN_AGENT_VERSION_ID_FOREIGN_KEY,
)
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.agent_version import AgentVersionFilter
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionInUse,
    AgentVersionNotFound,
    RunSpec,
)
from kitaru.server.domain.base import NotFoundError, ValidationError

AGENT_VERSION_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": AgentVersionORM.id,
    "tag": build_tag_condition_binding(
        TagResourceType.AGENT_VERSION, AgentVersionORM.id
    ),
}


class SQLAgentVersionRepository(BaseSQLRepository[AgentVersionORM]):
    """Agent version repository backed by the application database."""

    orm_class = AgentVersionORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return AgentVersionNotFound(entity_id)

    async def _bump_latest_version(self, agent_id: uuid.UUID) -> int:
        """Bump the owning agent's version counter and return the new value.

        Args:
            agent_id: Id of the agent to bump.

        Raises:
            AgentNotFound: No agent has this id.

        Returns:
            New version number.
        """
        statement = (
            update(AgentORM)
            .where(AgentORM.id == agent_id, AgentORM.deleted_at.is_(None))
            .values(latest_version=AgentORM.latest_version + 1)
            .returning(AgentORM.latest_version)
        )
        result = await self._session.execute(statement)
        row = result.first()
        if row is None:
            raise AgentNotFound(agent_id)
        return row[0]

    async def _insert_secret_links(
        self, agent_version_id: uuid.UUID, run_spec: RunSpec | None
    ) -> None:
        """Insert the secret link rows for a run spec's secret ids.

        Args:
            agent_version_id: Id of the owning agent version.
            run_spec: Run spec carrying the secret ids, or ``None``.

        Raises:
            ValidationError: A secret id does not resolve to a stored secret.
        """
        if run_spec is not None:
            for index, secret_id in enumerate(run_spec.secret_ids):
                self._session.add(
                    AgentVersionSecretORM(
                        agent_version_id=agent_version_id,
                        secret_id=secret_id,
                        index=index,
                    )
                )
        await self._flush(
            {
                AGENT_VERSION_SECRET_SECRET_ID_FOREIGN_KEY: lambda: ValidationError(
                    "Run spec references a secret that does not exist"
                )
            }
        )

    async def _sync_secret_links(
        self, agent_version_id: uuid.UUID, run_spec: RunSpec | None
    ) -> None:
        """Replace the secret link rows to match a run spec's secret ids.

        Args:
            agent_version_id: Id of the owning agent version.
            run_spec: Run spec carrying the desired secret ids, or ``None``.

        Raises:
            ValidationError: A secret id does not resolve to a stored secret.
        """
        await self._session.execute(
            delete(AgentVersionSecretORM).where(
                AgentVersionSecretORM.agent_version_id == agent_version_id
            )
        )
        await self._insert_secret_links(agent_version_id, run_spec)

    async def _load_secret_ids(self, agent_version_id: uuid.UUID) -> list[uuid.UUID]:
        """Load the ordered secret ids of one agent version.

        Args:
            agent_version_id: Id of the agent version.

        Returns:
            Secret ids in link order.
        """
        statement = (
            select(AgentVersionSecretORM.secret_id)
            .where(AgentVersionSecretORM.agent_version_id == agent_version_id)
            .order_by(AgentVersionSecretORM.index)
        )
        return list((await self._session.scalars(statement)).all())

    async def _load_secret_ids_bulk(
        self, agent_version_ids: Sequence[uuid.UUID]
    ) -> dict[uuid.UUID, list[uuid.UUID]]:
        """Load the ordered secret ids of several agent versions in one query.

        Args:
            agent_version_ids: Ids of the agent versions.

        Returns:
            Secret ids in link order, keyed by agent version id.
        """
        if not agent_version_ids:
            return {}
        statement = (
            select(
                AgentVersionSecretORM.agent_version_id, AgentVersionSecretORM.secret_id
            )
            .where(AgentVersionSecretORM.agent_version_id.in_(agent_version_ids))
            .order_by(
                AgentVersionSecretORM.agent_version_id, AgentVersionSecretORM.index
            )
        )
        secret_ids: dict[uuid.UUID, list[uuid.UUID]] = {}
        for agent_version_id, secret_id in (
            await self._session.execute(statement)
        ).all():
            secret_ids.setdefault(agent_version_id, []).append(secret_id)
        return secret_ids

    async def create(self, agent_version: AgentVersion) -> AgentVersion:
        """Persist a new agent version.

        The version number comes from an ``UPDATE ... RETURNING`` bump of
        the owning agent's version counter, in the same transaction as the
        insert.

        Args:
            agent_version: Agent version to store.

        Raises:
            AgentNotFound: No agent has the given agent id.
            ValidationError: The run spec references an unknown secret id.

        Returns:
            Stored agent version with its assigned version number and
            timestamps set.
        """
        version_number = await self._bump_latest_version(agent_version.agent_id)
        stored = agent_version.model_copy(update={"version": version_number})
        row = AgentVersionORM.from_domain(stored)
        await self._add(row)
        await self._insert_secret_links(row.id, stored.run_spec)
        secret_ids = stored.run_spec.secret_ids if stored.run_spec is not None else []
        return row.to_domain(secret_ids)

    async def get(self, agent_version_id: uuid.UUID) -> AgentVersion:
        """Load an agent version by id.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Stored agent version.
        """
        row = await self._get_row(agent_version_id)
        secret_ids = await self._load_secret_ids(agent_version_id)
        return row.to_domain(secret_ids)

    async def get_agent_id(self, agent_version_id: uuid.UUID) -> uuid.UUID:
        """Load the id of the agent a version belongs to.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Id of the owning agent.
        """
        statement = select(AgentVersionORM.agent_id).where(
            AgentVersionORM.id == agent_version_id
        )
        result = await self._session.execute(statement)
        row = result.first()
        if row is None:
            raise AgentVersionNotFound(agent_version_id)
        return row[0]

    async def query(
        self, agent_version_filter: AgentVersionFilter
    ) -> tuple[list[AgentVersion], str | None]:
        """Query agent versions matching a filter.

        Args:
            agent_version_filter: Filter and pagination parameters.

        Returns:
            Page of matching agent versions and the next cursor.
        """
        statement = select(AgentVersionORM)
        if agent_version_filter.agent_id is not None:
            statement = statement.where(
                AgentVersionORM.agent_id == agent_version_filter.agent_id
            )
        if agent_version_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    agent_version_filter.expression, AGENT_VERSION_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, agent_version_filter, id_column=AgentVersionORM.id
        )
        secret_ids_by_version = await self._load_secret_ids_bulk(
            [row.id for row in rows]
        )
        return [
            row.to_domain(secret_ids_by_version.get(row.id, [])) for row in rows
        ], next_cursor

    async def update(self, agent_version: AgentVersion) -> AgentVersion:
        """Persist changes to an existing agent version.

        Replacing the run spec replaces the secret link rows to match its
        secret ids.

        Args:
            agent_version: Agent version with modified fields.

        Raises:
            AgentVersionNotFound: No agent version has this id.
            ValidationError: The run spec references an unknown secret id.

        Returns:
            Stored agent version with the updated timestamp renewed.
        """
        row = await self._get_row(agent_version.id)
        run_spec = agent_version.run_spec
        row.owner_id = agent_version.owner_id
        row.agent_id = agent_version.agent_id
        row.version = agent_version.version
        row.display_version = agent_version.display_version
        row.description = agent_version.description
        row.run_command = run_spec.command if run_spec is not None else None
        row.run_working_dir = run_spec.working_dir if run_spec is not None else None
        row.run_env = run_spec.env if run_spec is not None else None
        row.run_timeout_seconds = (
            run_spec.timeout_seconds if run_spec is not None else None
        )
        row.capabilities = agent_version.capabilities.model_dump(mode="json")
        await self._flush()
        await self._sync_secret_links(agent_version.id, run_spec)
        secret_ids = run_spec.secret_ids if run_spec is not None else []
        return row.to_domain(secret_ids)

    async def delete(self, agent_version_id: uuid.UUID) -> None:
        """Delete an agent version by id.

        Args:
            agent_version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.
            AgentVersionInUse: The version is referenced by an experiment run.
        """
        await self._delete_row(
            agent_version_id,
            {
                EXPERIMENT_RUN_AGENT_VERSION_ID_FOREIGN_KEY: lambda: AgentVersionInUse(
                    agent_version_id
                )
            },
        )
