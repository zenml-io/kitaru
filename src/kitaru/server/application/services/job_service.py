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
"""Job use cases and the job-and-task composition the command endpoints run."""

import uuid

from kitaru.api_models.v1.job import JobKind
from kitaru.api_models.v1.task import TaskOnFailure
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.plugin_repository import PluginRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.job import (
    EvaluationBatchCreate,
    ImportCreate,
    JobFilter,
    SessionRunCreate,
)
from kitaru.server.application.models.task import TaskFilter, TaskPolicy
from kitaru.server.application.services.agent_version_resolution import (
    resolve_agent_id,
    resolve_runnable_agent_version,
)
from kitaru.server.application.services.evaluator_resolution import validate_evaluators
from kitaru.server.application.services.plugin_resolution import (
    get_plugin_task_labels,
    resolve_plugin,
    resolve_plugin_version,
)
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.job import Job, JobAlreadySettled, JobNotSettled
from kitaru.server.domain.names import RESERVED_LABEL_PREFIX
from kitaru.server.domain.plugin import PluginKind
from kitaru.server.domain.task import AgentTask, EvaluationTask, ImportTask, Task

AGENT_VERSION_LABEL = f"{RESERVED_LABEL_PREFIX}agent_version"
# TODO: Drop the unprefixed label, see DEPRECATIONS.md.
LEGACY_AGENT_VERSION_LABEL = "agent_version"
SESSION_NAME_ENV = "KITARU_SESSION_NAME"


def get_agent_task_labels(agent_version_id: uuid.UUID) -> dict[str, str]:
    """Build the labels stamped on a task running an agent version.

    Args:
        agent_version_id: Id of the agent version.

    Returns:
        Labels.
    """
    value = str(agent_version_id)
    return {AGENT_VERSION_LABEL: value, LEGACY_AGENT_VERSION_LABEL: value}


async def add_tasks(
    tasks: list[Task], repository: JobRepository, task_repository: TaskRepository
) -> list[Task]:
    """Append many tasks to an unsettled job under one job row lock.

    Shared with the replay pipeline, which appends tasks to a replay's job
    from outside the request that created it. Every task must belong to the
    same job, so the settled check taken under the one lock covers all of
    them.

    Args:
        tasks: Tasks to append, every one belonging to the same job.
        repository: Job repository.
        task_repository: Task repository.

    Raises:
        JobNotFound: No job has the tasks' job id.
        JobAlreadySettled: The job already reached a terminal status.

    Returns:
        Created tasks, in the given order.
    """
    if not tasks:
        return []
    job = await repository.get(tasks[0].job_id, exclusive=True)
    if job.settled:
        raise JobAlreadySettled(job.id)
    return [await task_repository.create(task) for task in tasks]


async def add_task(
    task: Task, repository: JobRepository, task_repository: TaskRepository
) -> Task:
    """Append a task to an unsettled job.

    Args:
        task: Task to append.
        repository: Job repository.
        task_repository: Task repository.

    Raises:
        JobNotFound: No job has the task's job id.
        JobAlreadySettled: The job already reached a terminal status.

    Returns:
        Created task.
    """
    created = await add_tasks([task], repository, task_repository)
    return created[0]


class JobService:
    """Job use cases."""

    def __init__(
        self,
        repository: JobRepository,
        task_repository: TaskRepository,
        session_repository: SessionRepository,
        agent_repository: AgentRepository,
        agent_version_repository: AgentVersionRepository,
        plugin_repository: PluginRepository,
        blob_repository: BlobRepository,
        transitions: TaskTransitions,
        policy: TaskPolicy,
    ) -> None:
        """Initialize the service.

        Args:
            repository: Job repository.
            task_repository: Task repository.
            session_repository: Session repository.
            agent_repository: Agent repository.
            agent_version_repository: Agent version repository.
            plugin_repository: Plugin repository.
            blob_repository: Blob repository.
            transitions: Task transition dispatch.
            policy: Task execution policy.
        """
        self._repository = repository
        self._tasks = task_repository
        self._sessions = session_repository
        self._agents = agent_repository
        self._agent_versions = agent_version_repository
        self._plugins = plugin_repository
        self._blobs = blob_repository
        self._transitions = transitions
        self._policy = policy

    async def get_job(self, job_id: uuid.UUID, actor: AuthContext) -> Job:
        """Get a job by id.

        Args:
            job_id: Id of the job.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Stored job.
        """
        _ = actor
        return await self._repository.get(job_id)

    async def list_jobs(
        self, job_filter: JobFilter, actor: AuthContext
    ) -> tuple[list[Job], str | None]:
        """List jobs matching a filter.

        Args:
            job_filter: Filter and pagination parameters.
            actor: Caller context.

        Returns:
            Page of matching jobs and the next cursor.
        """
        _ = actor
        return await self._repository.query(job_filter)

    async def list_job_tasks(
        self, job_id: uuid.UUID, task_filter: TaskFilter, actor: AuthContext
    ) -> tuple[list[Task], str | None]:
        """List the tasks of a job.

        Args:
            job_id: Id of the job.
            task_filter: Filter and pagination parameters.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.

        Returns:
            Page of matching tasks and the next cursor.
        """
        _ = actor
        await self._repository.get(job_id)
        return await self._tasks.query(
            task_filter.model_copy(update={"job_id": job_id})
        )

    async def cancel_job(self, job_id: uuid.UUID, actor: AuthContext) -> Job:
        """Request cancellation of a job and cancel its pending tasks.

        Claimed and running tasks keep their status and carry the request
        until their worker or the staleness sweep writes a terminal value.

        Args:
            job_id: Id of the job.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.
            JobAlreadySettled: The job already reached a terminal status.

        Returns:
            Job carrying the cancel request.
        """
        _ = actor
        job = await self._repository.get(job_id)
        if job.settled:
            raise JobAlreadySettled(job_id)
        return await self._transitions.cancel_job(job_id)

    async def delete_job(self, job_id: uuid.UUID, actor: AuthContext) -> None:
        """Delete a settled job, cascading its tasks.

        Args:
            job_id: Id of the job.
            actor: Caller context.

        Raises:
            JobNotFound: No job has this id.
            JobNotSettled: The job has not reached a terminal status.
        """
        _ = actor
        job = await self._repository.get(job_id)
        if not job.settled:
            raise JobNotSettled(job_id)
        await self._repository.delete(job_id)

    async def create_job(self, kind: JobKind, actor: AuthContext) -> Job:
        """Create an empty pending job owned by the caller.

        Args:
            kind: Workflow creating the job.
            actor: Caller context.

        Returns:
            Created job.
        """
        return await self._repository.create(Job(owner_id=actor.account.id, kind=kind))

    async def add_task(self, task: Task) -> Task:
        """Append a task to an unsettled job.

        Args:
            task: Task to append.

        Raises:
            JobNotFound: No job has the task's job id.
            JobAlreadySettled: The job already reached a terminal status.

        Returns:
            Created task.
        """
        return await add_task(task, self._repository, self._tasks)

    async def create_session_run(
        self, command: SessionRunCreate, actor: AuthContext
    ) -> Job:
        """Create a job running one agent task on an agent version.

        Args:
            command: Fields for the run.
            actor: Caller context.

        Raises:
            AgentVersionNotFound: No agent version has this id.
            AgentVersionWithoutRunSpec: The agent version carries no run spec.

        Returns:
            Created job.
        """
        agent_version = await resolve_runnable_agent_version(
            command.agent_version_id, self._agent_versions
        )
        job = await self.create_job(JobKind.SESSION_RUN, actor)
        env = {SESSION_NAME_ENV: command.name} if command.name is not None else {}
        await self.add_task(
            AgentTask(
                job_id=job.id,
                agent_version_id=agent_version.id,
                inputs=command.inputs,
                labels=get_agent_task_labels(agent_version.id),
                env=env,
            )
        )
        return job

    async def create_import(self, command: ImportCreate, actor: AuthContext) -> Job:
        """Create a job running one importer task on a payload blob.

        An omitted importer version resolves to the importer's latest. An
        agent version is stamped on every session the import creates, and
        the sessions carry none when the command names none.

        Args:
            command: Fields for the import.
            actor: Caller context.

        Raises:
            PluginNotFound: No importer has this name.
            PluginVersionNotFound: The importer has no version with this
                number.
            BlobNotFound: No blob has the payload id.
            AgentNotFound: No agent has this id.
            AgentVersionNotFound: No agent version has this id.
            AgentVersionAgentMismatch: The agent version belongs to another
                agent.

        Returns:
            Created job.
        """
        plugin = await resolve_plugin(
            command.importer, PluginKind.IMPORTER, self._plugins
        )
        plugin_version = await resolve_plugin_version(
            plugin, command.version, self._plugins
        )
        payload = await self._blobs.get(command.payload_blob_id)
        agent = await self._agents.get(command.agent_id)
        if command.agent_version_id is not None:
            await resolve_agent_id(
                command.agent_version_id, agent.id, self._agent_versions
            )
        job = await self.create_job(JobKind.IMPORT, actor)
        await self.add_task(
            ImportTask(
                job_id=job.id,
                plugin_version_id=plugin_version.id,
                payload_blob_id=payload.id,
                agent_id=agent.id,
                agent_version_id=command.agent_version_id,
                labels=get_plugin_task_labels(plugin.name),
                params=command.params,
            )
        )
        return job

    async def create_evaluations(
        self, command: EvaluationBatchCreate, actor: AuthContext
    ) -> Job:
        """Create a job scoring every input session with every evaluator.

        Every pair is a `continue` task, so one failed scoring never cancels
        the rest and the job outcome reports whether all of them scored.

        Args:
            command: Sessions to score and evaluators to score them with.
            actor: Caller context.

        Raises:
            ValidationError: The pair count exceeds the cap, an input
                session does not exist, the input sessions do not all belong
                to one agent, or a config is scoped to another agent.
            SessionNotEvaluatable: An input session is in progress.
            PluginNotFound: A config names an unknown evaluator.
            PluginVersionNotFound: A config names an unknown version.

        Returns:
            Created job.
        """
        pairs = len(command.input_session_ids) * len(command.evaluators)
        if pairs > self._policy.evaluation_pair_limit:
            raise ValidationError(
                f"Evaluation request holds {pairs} pairs, the cap is "
                f"{self._policy.evaluation_pair_limit}"
            )
        stored = await self._sessions.get_many(
            command.input_session_ids, include_payloads=False
        )
        for session_id in command.input_session_ids:
            session = stored.get(session_id)
            if session is None:
                raise ValidationError(f"Session {session_id} was not found")
            session.check_evaluate()
        agent_ids = {session.agent_id for session in stored.values()}
        if len(agent_ids) > 1:
            raise ValidationError("Input sessions must belong to a single agent")
        evaluators = await validate_evaluators(
            command.evaluators, self._plugins, next(iter(agent_ids)), actor
        )
        job = await self.create_job(JobKind.EVALUATION, actor)
        # The job was just created in this call and cannot have settled yet, so
        # each pair skips add_task's redundant per-iteration settled check.
        for session_id in command.input_session_ids:
            for evaluator in evaluators:
                await self._tasks.create(
                    EvaluationTask(
                        job_id=job.id,
                        plugin_version_id=evaluator.evaluator_version_id,
                        input_session_id=session_id,
                        labels=get_plugin_task_labels(evaluator.evaluator),
                        params=evaluator.params,
                        on_failure=TaskOnFailure.CONTINUE,
                    )
                )
        return job
