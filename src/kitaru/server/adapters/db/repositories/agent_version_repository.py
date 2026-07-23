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

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.schemas.agent_version import (
    AGENT_VERSION_AGENT_ID_FOREIGN_KEY,
    AGENT_VERSION_UNIQUE_CONSTRAINT,
    AgentVersionSchema,
    AgentVersionSecretSchema,
)
from kitaru.server.application.models.agent_versions import AgentVersionFilter
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionNotFound,
    DuplicateAgentVersion,
)


class SQLAgentVersionRepository:
    """Agent version repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def _load_secret_ids(
        self, version_ids: list[uuid.UUID]
    ) -> dict[uuid.UUID, list[uuid.UUID]]:
        """Load run spec secret ids for a set of versions.

        Args:
            version_ids: Ids of the agent versions.

        Returns:
            Secret ids in insertion order, keyed by version id.
        """
        if not version_ids:
            return {}
        statement = (
            select(AgentVersionSecretSchema)
            .where(col(AgentVersionSecretSchema.agent_version_id).in_(version_ids))
            .order_by(col(AgentVersionSecretSchema.id))
        )
        rows = (await self._session.scalars(statement)).all()
        secret_ids: dict[uuid.UUID, list[uuid.UUID]] = {}
        for row in rows:
            secret_ids.setdefault(row.agent_version_id, []).append(row.secret_id)
        return secret_ids

    def _secret_ids(self, version: AgentVersion) -> list[uuid.UUID]:
        """Return the secret ids a version's run spec references.

        Args:
            version: Agent version.

        Returns:
            Secret ids, empty without a run spec.
        """
        if version.run_spec is None:
            return []
        return version.run_spec.secret_ids

    async def create(self, version: AgentVersion) -> AgentVersion:
        """Persist a new agent version.

        Args:
            version: Agent version to store.

        Raises:
            AgentNotFound: No agent has the version's agent id.
            DuplicateAgentVersion: The version is already registered for
                the agent.

        Returns:
            Stored agent version with timestamps set.
        """
        row = AgentVersionSchema.from_domain(version)
        secret_ids = self._secret_ids(version)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                for secret_id in secret_ids:
                    self._session.add(
                        AgentVersionSecretSchema(
                            agent_version_id=version.id, secret_id=secret_id
                        )
                    )
                await self._session.flush()
        except IntegrityError as exc:
            constraint = violated_constraint(exc)
            if constraint == AGENT_VERSION_UNIQUE_CONSTRAINT:
                raise DuplicateAgentVersion(version.version) from exc
            if constraint == AGENT_VERSION_AGENT_ID_FOREIGN_KEY:
                raise AgentNotFound(version.agent_id) from exc
            raise
        return row.to_domain(secret_ids)

    async def get(self, version_id: uuid.UUID) -> AgentVersion:
        """Load an agent version by id.

        Args:
            version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.

        Returns:
            Stored agent version.
        """
        row = await self._session.get(AgentVersionSchema, version_id)
        if row is None:
            raise AgentVersionNotFound(version_id)
        secret_ids = await self._load_secret_ids([row.id])
        return row.to_domain(secret_ids.get(row.id, []))

    async def query(
        self, version_filter: AgentVersionFilter
    ) -> tuple[list[AgentVersion], int]:
        """Query agent versions matching a filter.

        Args:
            version_filter: Filter and pagination parameters.

        Returns:
            Page of matching agent versions and the total match count.
        """
        statement = select(AgentVersionSchema)
        if version_filter.agent_id is not None:
            statement = statement.where(
                col(AgentVersionSchema.agent_id) == version_filter.agent_id
            )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(AgentVersionSchema.id),
            page=version_filter.page,
            page_size=version_filter.page_size,
        )
        secret_ids = await self._load_secret_ids([row.id for row in rows])
        return [row.to_domain(secret_ids.get(row.id, [])) for row in rows], total

    async def update(self, version: AgentVersion) -> AgentVersion:
        """Persist changes to an existing agent version.

        Args:
            version: Agent version with modified fields.

        Raises:
            AgentVersionNotFound: No agent version has this id.
            DuplicateAgentVersion: The version is already registered for
                the agent.

        Returns:
            Stored agent version with the updated timestamp renewed.
        """
        row = await self._session.get(AgentVersionSchema, version.id)
        if row is None:
            raise AgentVersionNotFound(version.id)
        run_spec = version.run_spec
        row.owner_id = version.owner_id
        row.agent_id = version.agent_id
        row.version = version.version
        row.description = version.description
        row.run_command = run_spec.command if run_spec else None
        row.run_working_dir = run_spec.working_dir if run_spec else None
        row.run_env = run_spec.env if run_spec else None
        row.run_timeout_seconds = run_spec.timeout_seconds if run_spec else None
        row.capabilities = version.capabilities.model_dump()
        secret_ids = self._secret_ids(version)
        try:
            async with self._session.begin_nested():
                await self._session.execute(
                    delete(AgentVersionSecretSchema).where(
                        col(AgentVersionSecretSchema.agent_version_id) == version.id
                    )
                )
                for secret_id in secret_ids:
                    self._session.add(
                        AgentVersionSecretSchema(
                            agent_version_id=version.id, secret_id=secret_id
                        )
                    )
                await self._session.flush()
        except IntegrityError as exc:
            if violated_constraint(exc) == AGENT_VERSION_UNIQUE_CONSTRAINT:
                raise DuplicateAgentVersion(version.version) from exc
            raise
        return row.to_domain(secret_ids)

    async def delete(self, version_id: uuid.UUID) -> None:
        """Delete an agent version by id.

        Args:
            version_id: Id of the agent version.

        Raises:
            AgentVersionNotFound: No agent version has this id.
        """
        row = await self._session.get(AgentVersionSchema, version_id)
        if row is None:
            raise AgentVersionNotFound(version_id)
        await self._session.delete(row)
        await self._session.flush()
