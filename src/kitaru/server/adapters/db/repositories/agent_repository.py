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
"""SQL agent repository."""

import uuid
from collections.abc import Mapping

from sqlalchemy import ColumnElement, Select, delete, or_, select
from sqlalchemy.orm import InstrumentedAttribute

from kitaru.server.adapters.db.filtering import FilterBinding, compile_filter_expression
from kitaru.server.adapters.db.orm.agent import AGENT_NAME_UNIQUE_CONSTRAINT, AgentORM
from kitaru.server.adapters.db.orm.agent_version import AgentVersionORM
from kitaru.server.adapters.db.orm.cohort import CohortORM
from kitaru.server.adapters.db.orm.experiment import ExperimentORM
from kitaru.server.adapters.db.orm.experiment_run import ExperimentRunORM
from kitaru.server.adapters.db.orm.job import JobORM
from kitaru.server.adapters.db.orm.replay import ReplayORM
from kitaru.server.adapters.db.orm.session import SessionORM
from kitaru.server.adapters.db.orm.task import TaskORM
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.repositories.base import BaseSQLRepository
from kitaru.server.application.models.agent import AgentFilter
from kitaru.server.domain.agent import Agent, AgentNotFound, DuplicateAgentName
from kitaru.server.domain.base import NotFoundError

AGENT_FILTER_BINDINGS: Mapping[str, FilterBinding] = {
    "id": AgentORM.id,
    "name": AgentORM.name,
}


class SQLAgentRepository(BaseSQLRepository[AgentORM]):
    """Agent repository backed by the application database."""

    orm_class = AgentORM

    def _not_found(self, entity_id: uuid.UUID) -> NotFoundError:
        """Build the not-found error for an id.

        Args:
            entity_id: Id of the missing row.

        Returns:
            Not-found error.
        """
        return AgentNotFound(entity_id)

    async def create(self, agent: Agent) -> Agent:
        """Persist a new agent.

        Args:
            agent: Agent to store.

        Raises:
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Stored agent with timestamps set.
        """
        row = AgentORM.from_domain(agent)
        await self._add(
            row, {AGENT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateAgentName(agent.name)}
        )
        return row.to_domain()

    async def get(self, agent_id: uuid.UUID) -> Agent:
        """Load an agent by id.

        Args:
            agent_id: Id of the agent.

        Raises:
            AgentNotFound: No agent has this id.

        Returns:
            Stored agent.
        """
        row = await self._get_row(agent_id)
        return row.to_domain()

    async def query(self, agent_filter: AgentFilter) -> tuple[list[Agent], str | None]:
        """Query agents matching a filter.

        Args:
            agent_filter: Filter and pagination parameters.

        Returns:
            Page of matching agents and the next cursor.
        """
        statement = select(AgentORM)
        if agent_filter.expression is not None:
            statement = statement.where(
                compile_filter_expression(
                    agent_filter.expression, AGENT_FILTER_BINDINGS
                )
            )
        rows, next_cursor = await paginate(
            self._session, statement, agent_filter, id_column=AgentORM.id
        )
        return [row.to_domain() for row in rows], next_cursor

    async def update(self, agent: Agent) -> Agent:
        """Persist changes to an existing agent.

        Args:
            agent: Agent with modified fields.

        Raises:
            AgentNotFound: No agent has this id.
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Stored agent with the updated timestamp renewed.
        """
        row = await self._get_row(agent.id)
        row.owner_id = agent.owner_id
        row.name = agent.name
        row.description = agent.description
        row.latest_version = agent.latest_version
        await self._flush(
            {AGENT_NAME_UNIQUE_CONSTRAINT: lambda: DuplicateAgentName(agent.name)}
        )
        return row.to_domain()

    async def delete(self, agent_id: uuid.UUID) -> None:
        """Delete an agent by id.

        Deleting an agent cascades its versions, sessions, cohorts, experiments,
        investigations, and jobs.

        Args:
            agent_id: Id of the agent.

        Raises:
            AgentNotFound: No agent has this id.
        """
        session_ids = select(SessionORM.id).where(SessionORM.agent_id == agent_id)
        version_ids = select(AgentVersionORM.id).where(
            AgentVersionORM.agent_id == agent_id
        )
        experiment_ids = select(ExperimentORM.id).where(
            ExperimentORM.agent_id == agent_id
        )
        # Every row a concurrent writer could reference is locked before any
        # of the subtree is deleted, so such an insert waits on its parent
        # and fails on the foreign key once the delete commits instead of
        # landing between two delete statements. Rows are locked in the order
        # the write paths take them: task rows first and in id order, then
        # the agent, experiments, cohorts, sessions, and versions. The job set
        # is read again once those are locked, since no job for the agent can
        # commit after that, and tasks of jobs that appeared in between are
        # locked then.
        job_ids = await self._get_job_ids(agent_id, session_ids, version_ids)
        await self._lock_rows(TaskORM.id, TaskORM.job_id.in_(job_ids))
        row = await self._get_row(agent_id, exclusive=True)
        await self._lock_rows(ExperimentORM.id, ExperimentORM.agent_id == agent_id)
        await self._lock_rows(CohortORM.id, CohortORM.agent_id == agent_id)
        await self._lock_rows(SessionORM.id, SessionORM.agent_id == agent_id)
        await self._lock_rows(AgentVersionORM.id, AgentVersionORM.agent_id == agent_id)
        job_ids = await self._get_job_ids(agent_id, session_ids, version_ids)
        await self._lock_rows(TaskORM.id, TaskORM.job_id.in_(job_ids))
        await self._lock_rows(JobORM.id, JobORM.id.in_(job_ids))
        # The database checks a restricting foreign key after each cascade
        # step rather than at the end of the statement, so rows behind such
        # keys are deleted before the agent row in dependency order.
        await self._session.execute(delete(JobORM).where(JobORM.id.in_(job_ids)))
        await self._session.execute(
            delete(ExperimentRunORM).where(
                ExperimentRunORM.experiment_id.in_(experiment_ids)
            )
        )
        await self._session.execute(
            delete(CohortORM).where(CohortORM.agent_id == agent_id)
        )
        await self._session.execute(
            delete(SessionORM).where(SessionORM.agent_id == agent_id)
        )
        await self._session.delete(row)
        await self._session.flush()

    async def _get_job_ids(
        self,
        agent_id: uuid.UUID,
        session_ids: Select[tuple[uuid.UUID]],
        version_ids: Select[tuple[uuid.UUID]],
    ) -> list[uuid.UUID]:
        """Get the ids of the jobs whose tasks or replays belong to an agent.

        Args:
            agent_id: Id of the agent.
            session_ids: Select of the agent's session ids.
            version_ids: Select of the agent's version ids.

        Returns:
            Job ids in ascending order.
        """
        task_job_ids = select(TaskORM.job_id).where(
            or_(
                TaskORM.agent_id == agent_id,
                TaskORM.agent_version_id.in_(version_ids),
                TaskORM.input_session_id.in_(session_ids),
                TaskORM.result_session_id.in_(session_ids),
            )
        )
        replay_job_ids = select(ReplayORM.job_id).where(
            ReplayORM.baseline_session_id.in_(session_ids)
        )
        result = await self._session.scalars(
            select(JobORM.id)
            .where(or_(JobORM.id.in_(task_job_ids), JobORM.id.in_(replay_job_ids)))
            .order_by(JobORM.id.asc())
        )
        return list(result.all())

    async def _lock_rows(
        self,
        id_column: InstrumentedAttribute[uuid.UUID],
        condition: ColumnElement[bool],
    ) -> None:
        """Lock the matching rows in id order for the rest of the transaction.

        Args:
            id_column: Id column of the table.
            condition: Filter selecting the rows to lock.
        """
        await self._session.execute(
            select(id_column)
            .where(condition)
            .order_by(id_column.asc())
            .with_for_update()
        )
