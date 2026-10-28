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
"""In-memory insight fake and builders that wire services to the fakes."""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import NamedTuple

from kitaru.server.api.composition import register_subscribers
from kitaru.server.application.events import EventDispatcher
from kitaru.server.application.models.auth import (
    GrantKind,
    TaskAuthContext,
    TaskPrincipal,
    WorkerAuthContext,
    WorkerPrincipal,
)
from kitaru.server.application.models.insight import InsightFilter
from kitaru.server.application.models.task import TaskPolicy
from kitaru.server.application.payload_store import PayloadStore
from kitaru.server.application.services.experiment_run_service import (
    ExperimentRunService,
)
from kitaru.server.application.services.experiment_service import ExperimentService
from kitaru.server.application.services.import_service import ImportService
from kitaru.server.application.services.job_service import JobService
from kitaru.server.application.services.replay_service import ReplayService
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.application.services.task_spec import TaskSpecBuilder
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.account import Account
from kitaru.server.domain.insight import Insight, InsightNotFound
from kitaru.server.domain.plugin import PluginVersionIdNotFound
from kitaru.server.filtering import FilterCondition
from kitaru_test_support.blobs_plugins import (
    FakeBlobRepository,
    FakePluginRepository,
    build_payload_store,
)
from kitaru_test_support.cohorts_workers import (
    FakeCohortRepository,
    FakeCohortVersionRepository,
    FakeWorkerRepository,
)
from kitaru_test_support.common import (
    _evaluate_filter_expression,
    _matches_condition,
    _paginate_fake,
    _renewed_timestamp,
)
from kitaru_test_support.evaluations import (
    FakeEvaluationRepository,
    FakeExperimentRunRepository,
)
from kitaru_test_support.experiments_replays import (
    FakeExperimentRepository,
    FakeImportRepository,
    FakeReplayRepository,
)
from kitaru_test_support.jobs_tasks import FakeJobRepository, FakeTaskRepository
from kitaru_test_support.secrets_agents import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeConnectionRepository,
    FakeSecretRepository,
)
from kitaru_test_support.session_nodes import FakeSessionNodeRepository
from kitaru_test_support.sessions import FakeSessionRepository, FakeTagRepository


class FakeInsightRepository:
    """In-memory insight repository."""

    def __init__(
        self,
        plugin_repository: FakePluginRepository | None = None,
    ) -> None:
        """Initialize the repository.

        Args:
            plugin_repository: Fake plugin repository, consulted to mirror
                the analyzer version foreign key.
        """
        self._insights: dict[uuid.UUID, Insight] = {}
        self._plugin_repository = plugin_repository

    async def create_many(self, insights: list[Insight]) -> list[Insight]:
        """Persist a batch of new insights in one transaction.

        Args:
            insights: Insights to store, in input order.

        Raises:
            PluginVersionIdNotFound: No plugin version has the analyzer
                version id.

        Returns:
            Stored insights in input order, with timestamps set.
        """
        if not insights:
            return []
        analyzer_version_id = insights[0].analyzer_version_id
        if (
            analyzer_version_id is not None
            and self._plugin_repository is not None
            and analyzer_version_id not in self._plugin_repository._versions
        ):
            raise PluginVersionIdNotFound(analyzer_version_id)
        now = datetime.now(UTC)
        stored = [
            insight.model_copy(update={"created": now, "updated": now})
            for insight in insights
        ]
        for insight in stored:
            self._insights[insight.id] = insight
        return [insight.model_copy() for insight in stored]

    async def get(self, insight_id: uuid.UUID, exclusive: bool = False) -> Insight:
        """Load an insight by id.

        Args:
            insight_id: Id of the insight.
            exclusive: Accepted for protocol parity; the fake does not lock.

        Raises:
            InsightNotFound: No insight has this id.

        Returns:
            Stored insight.
        """
        stored = self._insights.get(insight_id)
        if stored is None:
            raise InsightNotFound(insight_id)
        return stored.model_copy()

    def _evaluate_type_condition(
        self, insight: Insight, condition: FilterCondition
    ) -> bool:
        """Evaluate a type filter condition against an insight's data type.

        Args:
            insight: Insight to evaluate.
            condition: Validated type condition.

        Returns:
            Whether the insight's data type matches.
        """
        return _matches_condition(insight.data.type, condition)

    async def query(
        self, insight_filter: InsightFilter
    ) -> tuple[list[Insight], str | None]:
        """Query insights matching a filter.

        Args:
            insight_filter: Filter and pagination parameters.

        Returns:
            Page of matching insights and the next cursor.
        """
        insights = list(self._insights.values())
        if insight_filter.expression is not None:
            resolvers = {
                "type": self._evaluate_type_condition,
            }
            insights = [
                insight
                for insight in insights
                if _evaluate_filter_expression(
                    insight, insight_filter.expression, resolvers
                )
            ]
        page, next_cursor = _paginate_fake(insights, insight_filter)
        return [insight.model_copy() for insight in page], next_cursor

    async def update(self, insight: Insight) -> Insight:
        """Persist changes to an existing insight.

        Args:
            insight: Insight with modified fields.

        Raises:
            InsightNotFound: No insight has this id.

        Returns:
            Stored insight with the updated timestamp renewed.
        """
        stored = self._insights.get(insight.id)
        if stored is None:
            raise InsightNotFound(insight.id)
        now = _renewed_timestamp(stored.updated)
        updated = insight.model_copy(update={"created": stored.created, "updated": now})
        self._insights[insight.id] = updated
        return updated.model_copy()

    async def delete(self, insight_id: uuid.UUID) -> None:
        """Delete an insight by id.

        Args:
            insight_id: Id of the insight.

        Raises:
            InsightNotFound: No insight has this id.
        """
        if insight_id not in self._insights:
            raise InsightNotFound(insight_id)
        del self._insights[insight_id]


class TaskSubstrate(NamedTuple):
    """Fake repositories shared by every job- and task-driving service builder."""

    sessions: FakeSessionRepository
    agents: FakeAgentRepository
    agent_versions: FakeAgentVersionRepository
    blobs: FakeBlobRepository
    plugins: FakePluginRepository
    secrets: FakeSecretRepository
    workers: FakeWorkerRepository
    tasks: FakeTaskRepository
    jobs: FakeJobRepository
    imports: FakeImportRepository
    insights: FakeInsightRepository
    connections: FakeConnectionRepository


def _build_task_substrate() -> TaskSubstrate:
    """Wire the fake repositories every job- and task-driving service needs.

    Returns:
        Fake repositories shared by job, task, and replay service builders.
    """
    sessions = FakeSessionRepository()
    agents = FakeAgentRepository()
    agent_versions = FakeAgentVersionRepository(agents)
    blobs = FakeBlobRepository()
    plugins = FakePluginRepository(blob_repository=blobs)
    secrets = FakeSecretRepository()
    workers = FakeWorkerRepository()
    tasks = FakeTaskRepository(sessions=sessions)
    jobs = FakeJobRepository(tasks=tasks)
    tasks.jobs = jobs
    imports = FakeImportRepository()
    insights = FakeInsightRepository(plugin_repository=plugins)
    connections = FakeConnectionRepository()
    return TaskSubstrate(
        sessions=sessions,
        agents=agents,
        agent_versions=agent_versions,
        blobs=blobs,
        plugins=plugins,
        secrets=secrets,
        workers=workers,
        tasks=tasks,
        jobs=jobs,
        imports=imports,
        insights=insights,
        connections=connections,
    )


class JobAndTaskServices(NamedTuple):
    """Job and task services sharing one set of fake repositories."""

    job_service: JobService
    task_service: TaskService
    import_service: ImportService
    jobs: FakeJobRepository
    tasks: FakeTaskRepository
    sessions: FakeSessionRepository
    agents: FakeAgentRepository
    agent_versions: FakeAgentVersionRepository
    plugins: FakePluginRepository
    blobs: FakeBlobRepository
    secrets: FakeSecretRepository
    workers: FakeWorkerRepository
    imports: FakeImportRepository
    connections: FakeConnectionRepository


def build_job_and_task_services(
    policy: TaskPolicy | None = None,
) -> JobAndTaskServices:
    """Wire fake-backed job and task services sharing one event dispatcher.

    Mirrors the production wiring in ``dependencies.py``: both services share
    one ``TaskTransitions`` dispatch, so a transition applied through one
    service is visible to the other's repositories.

    Args:
        policy: Task execution policy, defaults applied when omitted.

    Returns:
        Job and task services plus their backing fake repositories.
    """
    substrate = _build_task_substrate()
    transitions = TaskTransitions(
        task_repository=substrate.tasks,
        job_repository=substrate.jobs,
        dispatcher=EventDispatcher(),
        import_repository=substrate.imports,
    )
    task_policy = policy if policy is not None else TaskPolicy()
    replays = FakeReplayRepository()
    spec_builder = TaskSpecBuilder(
        agent_version_repository=substrate.agent_versions,
        plugin_repository=substrate.plugins,
        blob_repository=substrate.blobs,
        secret_repository=substrate.secrets,
        replay_repository=replays,
        import_repository=substrate.imports,
        connection_repository=substrate.connections,
        policy=task_policy,
    )
    task_service = TaskService(
        repository=substrate.tasks,
        worker_repository=substrate.workers,
        session_repository=substrate.sessions,
        job_repository=substrate.jobs,
        replay_repository=replays,
        spec_builder=spec_builder,
        transitions=transitions,
        policy=task_policy,
    )
    job_service = JobService(
        repository=substrate.jobs,
        task_repository=substrate.tasks,
        session_repository=substrate.sessions,
        agent_version_repository=substrate.agent_versions,
        plugin_repository=substrate.plugins,
        connection_repository=substrate.connections,
        transitions=transitions,
        policy=task_policy,
    )
    import_service = ImportService(
        repository=substrate.imports,
        job_repository=substrate.jobs,
        task_repository=substrate.tasks,
        session_repository=substrate.sessions,
        agent_repository=substrate.agents,
        agent_version_repository=substrate.agent_versions,
        plugin_repository=substrate.plugins,
        blob_repository=substrate.blobs,
        connection_repository=substrate.connections,
        transitions=transitions,
    )
    return JobAndTaskServices(
        job_service=job_service,
        task_service=task_service,
        import_service=import_service,
        jobs=substrate.jobs,
        tasks=substrate.tasks,
        sessions=substrate.sessions,
        agents=substrate.agents,
        agent_versions=substrate.agent_versions,
        plugins=substrate.plugins,
        blobs=substrate.blobs,
        secrets=substrate.secrets,
        workers=substrate.workers,
        imports=substrate.imports,
        connections=substrate.connections,
    )


class ReplayServices(NamedTuple):
    """Replay, experiment, and experiment run services sharing one set of fakes."""

    experiment_service: ExperimentService
    experiment_run_service: ExperimentRunService
    replay_service: ReplayService
    job_service: JobService
    task_service: TaskService
    experiments: FakeExperimentRepository
    experiment_runs: FakeExperimentRunRepository
    replays: FakeReplayRepository
    jobs: FakeJobRepository
    tasks: FakeTaskRepository
    sessions: FakeSessionRepository
    session_nodes: FakeSessionNodeRepository
    agents: FakeAgentRepository
    agent_versions: FakeAgentVersionRepository
    cohorts: FakeCohortRepository
    cohort_versions: FakeCohortVersionRepository
    plugins: FakePluginRepository
    blobs: FakeBlobRepository
    secrets: FakeSecretRepository
    workers: FakeWorkerRepository
    evaluations: FakeEvaluationRepository
    tags: FakeTagRepository
    imports: FakeImportRepository
    insights: FakeInsightRepository
    connections: FakeConnectionRepository
    transitions: TaskTransitions
    payload_store: PayloadStore


def build_replay_services(policy: TaskPolicy | None = None) -> ReplayServices:
    """Wire fake-backed replay, experiment, and run services sharing one dispatcher.

    Mirrors the production wiring in ``composition.py`` and
    ``dependencies.py``: the event dispatcher carries the same subscribers
    registered on the request-scoped dispatcher, so a task transition applied
    through one service drives the replay pipeline and run finalization the
    same way it does in production.

    Args:
        policy: Task execution policy, defaults applied when omitted.

    Returns:
        Replay, experiment, and run services plus their backing fake
        repositories.
    """
    substrate = _build_task_substrate()
    sessions = substrate.sessions
    agents = substrate.agents
    agent_versions = substrate.agent_versions
    blobs = substrate.blobs
    plugins = substrate.plugins
    secrets = substrate.secrets
    workers = substrate.workers
    tasks = substrate.tasks
    jobs = substrate.jobs
    imports = substrate.imports
    insights = substrate.insights
    connections = substrate.connections
    tags = FakeTagRepository()
    cohorts = FakeCohortRepository(tags=tags)
    experiment_runs = FakeExperimentRunRepository(tag_repository=tags)
    replays = FakeReplayRepository(experiment_runs=experiment_runs)
    cohort_versions = FakeCohortVersionRepository(
        cohorts=cohorts, sessions=sessions, experiment_runs=experiment_runs, tags=tags
    )
    session_nodes = FakeSessionNodeRepository(
        sessions=sessions, cohort_versions=cohort_versions
    )
    experiments = FakeExperimentRepository(
        tag_repository=tags,
        experiment_run_repository=experiment_runs,
        replay_repository=replays,
    )
    evaluations = FakeEvaluationRepository(plugin_repository=plugins)

    dispatcher = EventDispatcher()
    register_subscribers(
        dispatcher,
        job_repository=jobs,
        task_repository=tasks,
        replay_repository=replays,
        experiment_repository=experiments,
        experiment_run_repository=experiment_runs,
        evaluation_repository=evaluations,
        session_repository=sessions,
        import_repository=imports,
        insight_repository=insights,
    )
    transitions = TaskTransitions(
        task_repository=tasks,
        job_repository=jobs,
        dispatcher=dispatcher,
        import_repository=imports,
    )
    task_policy = policy if policy is not None else TaskPolicy()
    spec_builder = TaskSpecBuilder(
        agent_version_repository=agent_versions,
        plugin_repository=plugins,
        blob_repository=blobs,
        secret_repository=secrets,
        replay_repository=replays,
        import_repository=imports,
        connection_repository=connections,
        policy=task_policy,
    )
    task_service = TaskService(
        repository=tasks,
        worker_repository=workers,
        session_repository=sessions,
        job_repository=jobs,
        replay_repository=replays,
        spec_builder=spec_builder,
        transitions=transitions,
        policy=task_policy,
    )
    job_service = JobService(
        repository=jobs,
        task_repository=tasks,
        session_repository=sessions,
        agent_version_repository=agent_versions,
        plugin_repository=plugins,
        connection_repository=connections,
        transitions=transitions,
        policy=task_policy,
    )
    payload_store = build_payload_store(blob_repository=blobs).store
    experiment_service = ExperimentService(
        repository=experiments,
        plugin_repository=plugins,
        connection_repository=connections,
        experiment_run_repository=experiment_runs,
        agent_repository=agents,
        cohort_version_repository=cohort_versions,
        session_repository=sessions,
        agent_version_repository=agent_versions,
        replay_repository=replays,
        job_repository=jobs,
        task_repository=tasks,
        evaluation_repository=evaluations,
        transitions=transitions,
        payload_store=payload_store,
    )
    replay_service = ReplayService(
        repository=replays,
        experiment_repository=experiments,
        experiment_run_repository=experiment_runs,
        job_repository=jobs,
        task_repository=tasks,
        evaluation_repository=evaluations,
        session_repository=sessions,
        session_node_repository=session_nodes,
        agent_version_repository=agent_versions,
        plugin_repository=plugins,
        connection_repository=connections,
        payload_store=payload_store,
    )
    experiment_run_service = ExperimentRunService(
        repository=experiment_runs,
        replay_repository=replays,
        job_repository=jobs,
        transitions=transitions,
    )
    return ReplayServices(
        experiment_service=experiment_service,
        experiment_run_service=experiment_run_service,
        replay_service=replay_service,
        job_service=job_service,
        task_service=task_service,
        experiments=experiments,
        experiment_runs=experiment_runs,
        replays=replays,
        jobs=jobs,
        tasks=tasks,
        sessions=sessions,
        session_nodes=session_nodes,
        agents=agents,
        agent_versions=agent_versions,
        cohorts=cohorts,
        cohort_versions=cohort_versions,
        plugins=plugins,
        blobs=blobs,
        secrets=secrets,
        workers=workers,
        evaluations=evaluations,
        tags=tags,
        imports=imports,
        insights=insights,
        connections=connections,
        transitions=transitions,
        payload_store=payload_store,
    )


def build_worker_actor(account: Account, worker_id: uuid.UUID) -> WorkerAuthContext:
    """Build a worker-principal context for a worker owned by the account.

    Args:
        account: Account the context acts as.
        worker_id: Id of the worker the principal claims to be.

    Returns:
        Context carrying a worker principal.
    """
    return WorkerAuthContext(
        account=account, principal=WorkerPrincipal(worker_id=worker_id)
    )


def build_task_actor(
    account: Account,
    task_id: uuid.UUID,
    attempt: int,
    worker_id: uuid.UUID,
    job_id: uuid.UUID | None = None,
    granted_session_ids: Sequence[uuid.UUID] = (),
) -> TaskAuthContext:
    """Build a task-principal context fenced by attempt.

    Args:
        account: Account the context acts as.
        task_id: Id of the task the principal claims to run.
        attempt: Attempt the principal is fenced by.
        worker_id: Id of the worker holding the attempt.
        job_id: Id of the job the task belongs to, defaulting to an unrelated
            job so job-scoped checks fail closed.
        granted_session_ids: Ids of the sessions the task reads without owning.

    Returns:
        Context carrying a task principal.
    """
    grants: dict[GrantKind, frozenset[uuid.UUID]] = {}
    if granted_session_ids:
        grants[GrantKind.SESSION] = frozenset(granted_session_ids)
    return TaskAuthContext(
        account=account,
        principal=TaskPrincipal(
            task_id=task_id,
            attempt=attempt,
            worker_id=worker_id,
            job_id=job_id if job_id is not None else uuid.uuid4(),
            grants=grants,
        ),
    )
