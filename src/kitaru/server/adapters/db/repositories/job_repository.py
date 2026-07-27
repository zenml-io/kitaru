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
"""SQL job repository."""

import uuid
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import (
    ColumnElement,
    and_,
    case,
    delete,
    exists,
    func,
    or_,
    select,
    update,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlmodel import col

from kitaru.server.adapters.db.errors import violated_constraint
from kitaru.server.adapters.db.pagination import paginate
from kitaru.server.adapters.db.schemas.agent_version import AgentVersionSchema
from kitaru.server.adapters.db.schemas.job import (
    JOB_AGENT_VERSION_ID_FOREIGN_KEY,
    JOB_EXPERIMENT_RUN_ID_FOREIGN_KEY,
    JOB_INPUT_SESSION_ID_FOREIGN_KEY,
    JOB_PAYLOAD_BLOB_ID_FOREIGN_KEY,
    JOB_PLUGIN_VERSION_ID_FOREIGN_KEY,
    JOB_REPLAY_CONFIG_ID_FOREIGN_KEY,
    JOB_RESULT_SESSION_ID_FOREIGN_KEY,
    JOB_SCORER_UNIQUE_CONSTRAINT,
    JOB_SESSION_UNIQUE_CONSTRAINT,
    JobSchema,
)
from kitaru.server.application.models.jobs import JobFilter
from kitaru.server.domain.agent_version import AgentVersionNotFound
from kitaru.server.domain.blob import BlobNotFound
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.experiment_run import ExperimentRunNotFound
from kitaru.server.domain.job import (
    DuplicateReplaySession,
    DuplicateScoreJob,
    Import,
    Job,
    JobNotFound,
    JobStatus,
    Replay,
    Score,
    SessionRun,
)
from kitaru.server.domain.plugin import PluginVersionIdNotFound
from kitaru.server.domain.replay_config import ReplayConfigNotFound
from kitaru.server.domain.session import SessionNotFound


def translate_job_integrity_error(exc: IntegrityError, job: Job) -> None:
    """Translate a job write integrity error into the domain error.

    Args:
        exc: Integrity error raised by a flush.
        job: Job that was written.

    Raises:
        DuplicateReplaySession: The run already replays the input
            session.
        DuplicateScoreJob: The parent job already scores the input
            session with the scorer.
        ExperimentRunNotFound: No experiment run has the job's
            experiment run id.
        ReplayConfigNotFound: No replay config has the job's job
            config id.
        AgentVersionNotFound: No agent version has the job's agent
            version id.
        PluginVersionIdNotFound: No plugin version has the job's plugin
            version id.
        BlobNotFound: No blob has the job's payload blob id.
        SessionNotFound: No session has the job's input session id.
    """
    constraint = violated_constraint(exc)
    if isinstance(job, Replay):
        if constraint == JOB_SESSION_UNIQUE_CONSTRAINT:
            assert job.experiment_run_id is not None
            raise DuplicateReplaySession(
                job.experiment_run_id, job.input_session_id
            ) from exc
        if constraint == JOB_EXPERIMENT_RUN_ID_FOREIGN_KEY:
            assert job.experiment_run_id is not None
            raise ExperimentRunNotFound(job.experiment_run_id) from exc
        if constraint == JOB_REPLAY_CONFIG_ID_FOREIGN_KEY:
            raise ReplayConfigNotFound(job.replay_config_id) from exc
        if constraint == JOB_INPUT_SESSION_ID_FOREIGN_KEY:
            raise SessionNotFound(job.input_session_id) from exc
    if isinstance(job, Score):
        if constraint == JOB_SCORER_UNIQUE_CONSTRAINT:
            assert job.parent_job_id is not None
            raise DuplicateScoreJob(
                job.parent_job_id, job.input_session_id, job.scorer_config.name
            ) from exc
        if constraint == JOB_INPUT_SESSION_ID_FOREIGN_KEY:
            raise SessionNotFound(job.input_session_id) from exc
        if constraint == JOB_PLUGIN_VERSION_ID_FOREIGN_KEY:
            assert job.plugin_version_id is not None
            raise PluginVersionIdNotFound(job.plugin_version_id) from exc
    if isinstance(job, Import):
        if constraint == JOB_PLUGIN_VERSION_ID_FOREIGN_KEY:
            raise PluginVersionIdNotFound(job.plugin_version_id) from exc
        if constraint == JOB_PAYLOAD_BLOB_ID_FOREIGN_KEY:
            raise BlobNotFound(job.payload_blob_id) from exc
    if constraint == JOB_AGENT_VERSION_ID_FOREIGN_KEY:
        assert job.agent_version_id is not None
        raise AgentVersionNotFound(job.agent_version_id) from exc


class SQLJobRepository:
    """Job repository backed by the application database."""

    def __init__(self, session: AsyncSession) -> None:
        """Initialize the repository.

        Args:
            session: Database session for all operations.
        """
        self._session = session

    async def create(self, job: Job) -> Job:
        """Persist a new job.

        Args:
            job: Job to store.

        Raises:
            ExperimentRunNotFound: No experiment run has the job's
                experiment run id.
            ReplayConfigNotFound: No replay config has the job's job
                config id.
            AgentVersionNotFound: No agent version has the job's agent
                version id.
            SessionNotFound: No session has the job's original session
                id.
            DuplicateReplaySession: The run already replays the original
                session.

        Returns:
            Stored job with timestamps set.
        """
        row = JobSchema.from_domain(job)
        try:
            async with self._session.begin_nested():
                self._session.add(row)
                await self._session.flush()
        except IntegrityError as exc:
            translate_job_integrity_error(exc, job)
            raise
        return row.to_domain()

    async def create_many(self, jobs: list[Job]) -> list[Job]:
        """Persist new jobs as one batch.

        Args:
            jobs: Jobs to store.

        Raises:
            DuplicateScoreJob: A parent job already scores an input
                session with a scorer.
            AgentVersionNotFound: No agent version has a job's agent
                version id.
            PluginVersionIdNotFound: No plugin version has a job's plugin
                version id.
            SessionNotFound: No session has a job's input session id.

        Returns:
            Stored jobs with timestamps set.
        """
        rows = [JobSchema.from_domain(job) for job in jobs]
        try:
            async with self._session.begin_nested():
                self._session.add_all(rows)
                await self._session.flush()
        except IntegrityError as exc:
            for job in jobs:
                translate_job_integrity_error(exc, job)
            raise
        return [row.to_domain() for row in rows]

    async def get(self, job_id: uuid.UUID, for_update: bool = False) -> Job:
        """Load a job by id.

        Args:
            job_id: Id of the job.
            for_update: Lock the row for the transaction.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job.
        """
        row = await self._session.get(JobSchema, job_id, with_for_update=for_update)
        if row is None:
            raise JobNotFound(job_id)
        return row.to_domain()

    async def query(self, job_filter: JobFilter) -> tuple[list[Job], int]:
        """Query jobs matching a filter.

        With the staleness context set, the status filter matches claimed
        or running jobs with lost heartbeats as pending, or as timed
        out once the attempt count reached the maximum.

        Args:
            job_filter: Filter and pagination parameters.

        Returns:
            Page of matching jobs and the total match count.
        """
        statement = select(JobSchema)
        if job_filter.experiment_run_id is not None:
            statement = statement.where(
                col(JobSchema.experiment_run_id) == job_filter.experiment_run_id
            )
        if job_filter.input_session_id is not None:
            statement = statement.where(
                col(JobSchema.input_session_id) == job_filter.input_session_id
            )
        if job_filter.kind is not None:
            statement = statement.where(col(JobSchema.kind) == job_filter.kind.value)
        if job_filter.execution_target is not None:
            statement = statement.where(
                col(JobSchema.execution_target) == job_filter.execution_target.value
            )
        if job_filter.status is not None:
            if (
                job_filter.stale_before is not None
                and job_filter.max_attempts is not None
            ):
                status = self._effective_status(
                    job_filter.stale_before, job_filter.max_attempts
                )
            else:
                status = col(JobSchema.status)
            statement = statement.where(status == job_filter.status.value)
        if job_filter.worker_id is not None:
            statement = statement.where(
                col(JobSchema.worker_id) == job_filter.worker_id
            )
        if job_filter.standalone is not None:
            if job_filter.standalone:
                statement = statement.where(col(JobSchema.experiment_run_id).is_(None))
            else:
                statement = statement.where(
                    col(JobSchema.experiment_run_id).is_not(None)
                )
        rows, total = await paginate(
            self._session,
            statement,
            order_by=col(JobSchema.id),
            page=job_filter.page,
            page_size=job_filter.page_size,
        )
        return [row.to_domain() for row in rows], total

    def _apply(self, row: JobSchema, job: Job) -> None:
        """Copy domain job fields onto an existing row.

        Args:
            row: Row to update.
            job: Job with modified fields.
        """
        row.kind = job.kind.value
        row.agent_version_id = job.agent_version_id
        row.result_session_id = job.result_session_id
        row.status = job.status.value
        row.attempt = job.attempt
        row.worker_id = job.worker_id
        row.execution_target = job.execution_target.value
        row.executor_handle = job.executor_handle
        row.claimed_at = job.claimed_at
        row.heartbeat_at = job.heartbeat_at
        row.started_at = job.started_at
        row.ended_at = job.ended_at
        row.error = job.error
        if isinstance(job, Replay):
            row.experiment_run_id = job.experiment_run_id
            row.replay_config_id = job.replay_config_id
            row.input_session_id = job.input_session_id
            row.passed = job.passed
            row.score = job.score
            row.scores = job.scores
            row.diff = job.diff
        elif isinstance(job, SessionRun):
            row.inputs = job.inputs
            row.name = job.name
        elif isinstance(job, Score):
            row.parent_job_id = job.parent_job_id
            row.input_session_id = job.input_session_id
            row.plugin_version_id = job.plugin_version_id
            row.scorer_name = job.scorer_config.name
            row.scorer_config = job.scorer_config.model_dump(mode="json")
            row.score = job.score
        elif isinstance(job, Import):
            row.plugin_version_id = job.plugin_version_id
            row.payload_blob_id = job.payload_blob_id
            row.inputs = job.inputs
            row.stats = None if job.stats is None else job.stats.model_dump(mode="json")

    async def update(self, job: Job) -> Job:
        """Persist changes to an existing job.

        Args:
            job: Job with modified fields.

        Raises:
            JobNotFound: No job has this id.
            SessionNotFound: No session has the job's result session id.

        Returns:
            Stored job with the updated timestamp renewed.
        """
        row = await self._session.get(JobSchema, job.id)
        if row is None:
            raise JobNotFound(job.id)
        try:
            async with self._session.begin_nested():
                self._apply(row, job)
                await self._session.flush()
        except IntegrityError as exc:
            constraint = violated_constraint(exc)
            if constraint == JOB_RESULT_SESSION_ID_FOREIGN_KEY:
                assert job.result_session_id is not None
                raise SessionNotFound(job.result_session_id) from exc
            translate_job_integrity_error(exc, job)
            raise
        return row.to_domain()

    async def delete(self, job_id: uuid.UUID) -> None:
        """Delete a job by id.

        Args:
            job_id: Id of the job.

        Raises:
            JobNotFound: No job has this id.
        """
        row = await self._session.get(JobSchema, job_id)
        if row is None:
            raise JobNotFound(job_id)
        await self._session.delete(row)
        await self._session.flush()

    async def list_children(self, parent_job_id: uuid.UUID) -> list[Job]:
        """Load every job fanned out from a parent job.

        Args:
            parent_job_id: Id of the parent job.

        Returns:
            Child jobs in id order.
        """
        statement = (
            select(JobSchema)
            .where(col(JobSchema.parent_job_id) == parent_job_id)
            .order_by(col(JobSchema.id))
        )
        rows = (await self._session.scalars(statement)).all()
        return [row.to_domain() for row in rows]

    async def delete_children(self, parent_job_id: uuid.UUID) -> None:
        """Delete every job fanned out from a parent job.

        Args:
            parent_job_id: Id of the parent job.
        """
        await self._session.execute(
            delete(JobSchema).where(col(JobSchema.parent_job_id) == parent_job_id)
        )
        await self._session.flush()

    def _stale_condition(self, stale_before: datetime) -> ColumnElement[bool]:
        """Build the lost-heartbeat condition on claimed or running rows.

        Args:
            stale_before: Heartbeats older than this time count as lost.

        Returns:
            SQL condition.
        """
        return and_(
            col(JobSchema.status).in_(
                [JobStatus.CLAIMED.value, JobStatus.RUNNING.value]
            ),
            func.coalesce(col(JobSchema.heartbeat_at), col(JobSchema.claimed_at))
            < stale_before,
        )

    def _effective_status(
        self, stale_before: datetime, max_attempts: int
    ) -> ColumnElement[str]:
        """Build the status expression with the staleness rule applied.

        Args:
            stale_before: Heartbeats older than this time count as lost.
            max_attempts: Attempt count at which a stale job times out.

        Returns:
            SQL expression.
        """
        stale = self._stale_condition(stale_before)
        return case(
            (
                and_(stale, col(JobSchema.attempt) >= max_attempts),
                JobStatus.TIMED_OUT.value,
            ),
            (stale, JobStatus.PENDING.value),
            else_=col(JobSchema.status),
        )

    def _scope_conditions(
        self,
        agent_ids: Sequence[uuid.UUID] | None,
        experiment_run_id: uuid.UUID | None,
        parent_job_id: uuid.UUID | None,
    ) -> list[ColumnElement[bool]]:
        """Build the claim scope conditions.

        Args:
            agent_ids: Ids of the agents to scope to.
            experiment_run_id: Id of the experiment run to scope to.
            parent_job_id: Id of the parent job to scope to.

        Returns:
            SQL conditions.
        """
        parent = aliased(JobSchema)
        if parent_job_id is not None:
            conditions = [col(JobSchema.parent_job_id) == parent_job_id]
        elif experiment_run_id is not None:
            conditions = [
                or_(
                    col(JobSchema.experiment_run_id) == experiment_run_id,
                    select(col(parent.id))
                    .where(
                        col(parent.id) == col(JobSchema.parent_job_id),
                        col(parent.experiment_run_id) == experiment_run_id,
                    )
                    .exists(),
                )
            ]
        else:
            conditions = [col(JobSchema.execution_target) == ExecutionTarget.POOL.value]
        if agent_ids is not None:
            conditions.append(
                or_(
                    col(JobSchema.agent_version_id).is_(None),
                    exists().where(
                        col(AgentVersionSchema.id) == col(JobSchema.agent_version_id),
                        col(AgentVersionSchema.agent_id).in_(agent_ids),
                    ),
                )
            )
        return conditions

    async def requeue_stale(
        self,
        stale_before: datetime,
        max_attempts: int,
        agent_ids: Sequence[uuid.UUID] | None = None,
        experiment_run_id: uuid.UUID | None = None,
        parent_job_id: uuid.UUID | None = None,
    ) -> list[Job]:
        """Requeue or time out jobs with lost heartbeats within a scope.

        Args:
            stale_before: Heartbeats older than this time count as lost.
            max_attempts: Attempt count at which a stale job times out.
            agent_ids: Ids of the agents to scope to.
            experiment_run_id: Id of the experiment run to scope to.
            parent_job_id: Id of the parent job to scope to.

        Returns:
            Jobs the staleness rule moved.
        """
        statement = (
            select(JobSchema)
            .where(
                *self._scope_conditions(agent_ids, experiment_run_id, parent_job_id),
                self._stale_condition(stale_before),
            )
            .with_for_update(skip_locked=True)
        )
        rows = (await self._session.scalars(statement)).all()
        for row in rows:
            job = row.to_domain()
            self._apply(row, job.with_staleness(stale_before, max_attempts))
        await self._session.flush()
        return [row.to_domain() for row in rows]

    async def claim_pending(
        self,
        worker_id: uuid.UUID,
        limit: int,
        agent_ids: Sequence[uuid.UUID] | None = None,
        experiment_run_id: uuid.UUID | None = None,
        parent_job_id: uuid.UUID | None = None,
    ) -> list[Job]:
        """Atomically claim pending jobs within a scope for a worker.

        Rows locked by a concurrent claim are skipped via
        ``FOR UPDATE SKIP LOCKED``, so parallel workers never double-claim.

        Args:
            worker_id: Id of the claiming worker.
            limit: Maximum number of jobs to claim.
            agent_ids: Ids of the agents to scope to.
            experiment_run_id: Id of the experiment run to scope to.
            parent_job_id: Id of the parent job to scope to.

        Returns:
            Claimed jobs.
        """
        statement = (
            select(JobSchema)
            .where(
                *self._scope_conditions(agent_ids, experiment_run_id, parent_job_id),
                col(JobSchema.status) == JobStatus.PENDING.value,
            )
            .order_by(col(JobSchema.id))
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        rows = (await self._session.scalars(statement)).all()
        for row in rows:
            job = row.to_domain()
            job.claim(worker_id)
            self._apply(row, job)
        await self._session.flush()
        return [row.to_domain() for row in rows]

    async def heartbeat_many(
        self,
        worker_id: uuid.UUID,
        job_ids: Sequence[uuid.UUID],
        heartbeat_at: datetime,
    ) -> list[Job]:
        """Record one worker heartbeat on every claimed or running job it owns.

        Args:
            worker_id: Id of the heartbeating worker.
            job_ids: Ids of the jobs the worker reports.
            heartbeat_at: Time of the heartbeat.

        Returns:
            Jobs the heartbeat reached.
        """
        if not job_ids:
            return []
        statement = (
            update(JobSchema)
            .where(
                col(JobSchema.id).in_(job_ids),
                col(JobSchema.worker_id) == worker_id,
                col(JobSchema.status).in_(
                    [JobStatus.CLAIMED.value, JobStatus.RUNNING.value]
                ),
            )
            .values(heartbeat_at=heartbeat_at)
            .returning(JobSchema)
        )
        rows = (
            await self._session.scalars(
                statement, execution_options={"synchronize_session": False}
            )
        ).all()
        await self._session.flush()
        return [row.to_domain() for row in rows]

    async def count_by_status(
        self, run_ids: list[uuid.UUID], stale_before: datetime, max_attempts: int
    ) -> dict[uuid.UUID, dict[JobStatus, int]]:
        """Count jobs by status for a set of experiment runs.

        Claimed or running jobs with lost heartbeats count as pending,
        or as timed out once the attempt count reached the maximum, without
        writing.

        Args:
            run_ids: Ids of the experiment runs.
            stale_before: Heartbeats older than this time count as lost.
            max_attempts: Attempt count at which a stale job times out.

        Returns:
            Job counts by status, keyed by experiment run id.
        """
        if not run_ids:
            return {}
        effective_status = self._effective_status(stale_before, max_attempts)
        statement = (
            select(
                col(JobSchema.experiment_run_id),
                effective_status,
                func.count(),
            )
            .where(col(JobSchema.experiment_run_id).in_(run_ids))
            .group_by(col(JobSchema.experiment_run_id), effective_status)
        )
        counts: dict[uuid.UUID, dict[JobStatus, int]] = {}
        for run_id, status, count in (await self._session.execute(statement)).all():
            counts.setdefault(run_id, {})[JobStatus(status)] = count
        return counts

    async def references_agent_version(self, version_id: uuid.UUID) -> bool:
        """Report whether a stored job references an agent version.

        Args:
            version_id: Id of the agent version.

        Returns:
            ``True`` when a stored job references the version.
        """
        statement = select(
            exists().where(col(JobSchema.agent_version_id) == version_id)
        )
        return bool((await self._session.execute(statement)).scalar())
