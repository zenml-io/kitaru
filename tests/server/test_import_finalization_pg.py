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
"""Concurrency tests for replay import finalization against PostgreSQL."""

import asyncio
import random
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from itertools import count
from typing import NamedTuple

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from conftest import pg_session_with_engine, postgres_available
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.replay import ReplayStatus
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.task import TaskStatus
from kitaru.api_models.v1.worker import WorkerRuntime, WorkerScope
from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.adapters.db.repositories.replay_repository import (
    SQLReplayRepository,
)
from kitaru.server.adapters.db.repositories.secret_repository import (
    SQLSecretRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.adapters.db.repositories.worker_repository import (
    SQLWorkerRepository,
)
from kitaru.server.api.composition import build_event_dispatcher
from kitaru.server.application.events import EventDispatcher, JobsSettled
from kitaru.server.application.models.auth import (
    AuthContext,
    TaskAuthContext,
    TaskPrincipal,
)
from kitaru.server.application.models.session import SessionCreate, SessionUpdate
from kitaru.server.application.models.task import TaskPolicy, TaskUpdate
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.application.services.task_spec import TaskSpecBuilder
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion, FunctionRunSpec
from kitaru.server.domain.blob import Blob
from kitaru.server.domain.job import Job
from kitaru.server.domain.plugin import PackagePluginSource, Plugin, PluginKind
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import (
    EvaluatorConfig,
    ReplayConfig,
    default_tool_policy,
)
from kitaru.server.domain.session import DuplicatePendingImportSession, Session
from kitaru.server.domain.task import (
    AgentTask,
    EvaluationTask,
    ImportTask,
)
from kitaru.server.domain.worker import Worker

RACE_ITERATIONS = 20
EVALUATOR_SOURCE = PackagePluginSource(
    requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score"
)


class PipelineSeed(NamedTuple):
    """Rows every replay case of one test shares."""

    owner: Account
    agent: Agent
    agent_version: AgentVersion
    worker: Worker
    evaluator_version_ids: list[uuid.UUID]


class ReplayCase(NamedTuple):
    """One provisional replay pipeline around a placeholder session."""

    job_id: uuid.UUID
    replay_id: uuid.UUID
    task_id: uuid.UUID
    placeholder_id: uuid.UUID


def _build_task_service(
    session: AsyncSession, engine: AsyncEngine, dispatcher: EventDispatcher
) -> TaskService:
    """Build a task service on the SQL repositories of one session."""
    policy = TaskPolicy()
    spec_builder = TaskSpecBuilder(
        agent_version_repository=SQLAgentVersionRepository(session),
        plugin_repository=SQLPluginRepository(session),
        blob_repository=SQLBlobRepository(session),
        secret_repository=SQLSecretRepository(
            session, AesGcmCipher("test-encryption-key")
        ),
        replay_repository=SQLReplayRepository(session),
        policy=policy,
    )
    transitions = TaskTransitions(
        task_repository=SQLTaskRepository(session),
        job_repository=SQLJobRepository(session),
        dispatcher=dispatcher,
    )
    return TaskService(
        repository=SQLTaskRepository(session),
        worker_repository=SQLWorkerRepository(session),
        session_repository=SQLSessionRepository(session, engine),
        job_repository=SQLJobRepository(session),
        spec_builder=spec_builder,
        transitions=transitions,
        policy=policy,
    )


def _build_session_service(
    session: AsyncSession, engine: AsyncEngine, dispatcher: EventDispatcher
) -> SessionService:
    """Build a session service on the SQL repositories of one session."""
    return SessionService(
        repository=SQLSessionRepository(session, engine),
        task_repository=SQLTaskRepository(session),
        agent_version_repository=SQLAgentVersionRepository(session),
        replay_repository=SQLReplayRepository(session),
        dispatcher=dispatcher,
    )


def _build_transitions(
    session: AsyncSession, dispatcher: EventDispatcher
) -> TaskTransitions:
    """Build task transitions on the SQL repositories of one session."""
    return TaskTransitions(
        task_repository=SQLTaskRepository(session),
        job_repository=SQLJobRepository(session),
        dispatcher=dispatcher,
    )


async def _seed_static(session: AsyncSession) -> PipelineSeed:
    """Seed the account, agent, function agent version, worker, and evaluators."""
    now = datetime.now(UTC)
    owner = await SQLAccountRepository(session).create(Account(name="owner"))
    agent = await SQLAgentRepository(session).create(
        Agent(owner_id=owner.id, name="agent")
    )
    agent_version = await SQLAgentVersionRepository(session).create(
        AgentVersion(
            owner_id=owner.id,
            agent_id=agent.id,
            run_spec=FunctionRunSpec(entrypoint="agent:run", timeout_seconds=60),
        )
    )
    worker = await SQLWorkerRepository(session).register(
        Worker(
            owner_id=owner.id,
            name="worker",
            scope=WorkerScope(),
            runtime=WorkerRuntime(platform="bare"),
            last_seen_at=now,
        )
    )
    plugins = SQLPluginRepository(session)
    evaluator_version_ids: list[uuid.UUID] = []
    for name in ("accuracy", "helpfulness"):
        plugin = await plugins.create(
            Plugin(owner_id=owner.id, kind=PluginKind.EVALUATOR, name=name)
        )
        version = await plugins.create_version(
            plugin.id, EVALUATOR_SOURCE, display_version=None
        )
        evaluator_version_ids.append(version.id)
    return PipelineSeed(owner, agent, agent_version, worker, evaluator_version_ids)


async def _create_config(
    session: AsyncSession, seed: PipelineSeed, evaluator_count: int
) -> uuid.UUID:
    """Create a replay config carrying the first N seeded evaluators."""
    names = ("accuracy", "helpfulness")
    config = await SQLExperimentRepository(session).create_replay_config(
        ReplayConfig(
            owner_id=seed.owner.id,
            tool_policy=default_tool_policy(),
            evaluators=[
                EvaluatorConfig(
                    evaluator=names[index],
                    version=1,
                    evaluator_version_id=seed.evaluator_version_ids[index],
                )
                for index in range(evaluator_count)
            ],
        )
    )
    return config.id


async def _seed_replay_case(
    session: AsyncSession,
    engine: AsyncEngine,
    seed: PipelineSeed,
    config_id: uuid.UUID,
    numbers: Iterator[int],
    task_status: TaskStatus,
) -> ReplayCase:
    """Seed one provisional replay job, its agent task, and its placeholder."""
    now = datetime.now(UTC)
    sessions = SQLSessionRepository(session, engine)
    baseline = await sessions.create(
        Session(
            owner_id=seed.owner.id,
            agent_id=seed.agent.id,
            number=next(numbers),
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
            inputs={"q": "hi"},
        )
    )
    job = await SQLJobRepository(session).create(
        Job(
            owner_id=seed.owner.id,
            kind=JobKind.REPLAY,
            status=JobStatus.RUNNING,
            provisional=True,
        )
    )
    replay = await SQLReplayRepository(session).create(
        Replay(
            owner_id=seed.owner.id,
            job_id=job.id,
            replay_config_id=config_id,
            baseline_session_id=baseline.id,
        )
    )
    tasks = SQLTaskRepository(session)
    task = await tasks.create(
        AgentTask(
            job_id=job.id,
            agent_version_id=seed.agent_version.id,
            status=task_status,
            attempt=1,
            worker_id=seed.worker.id,
            claimed_at=now,
            heartbeat_at=now,
            started_at=now,
            ended_at=now if task_status is TaskStatus.COMPLETED else None,
            inputs={"q": "hi"},
        )
    )
    placeholder = await sessions.create(
        Session(
            owner_id=seed.owner.id,
            agent_id=seed.agent.id,
            number=next(numbers),
            agent_version_id=seed.agent_version.id,
            task_id=task.id,
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id=f"run-{uuid.uuid4().hex[:8]}",
        )
    )
    task.link_result_session(placeholder.id)
    await tasks.update(task)
    return ReplayCase(job.id, replay.id, task.id, placeholder.id)


async def _report_task_status(
    session_factory: async_sessionmaker[AsyncSession],
    engine: AsyncEngine,
    seed: PipelineSeed,
    task_id: uuid.UUID,
    job_id: uuid.UUID,
    command: TaskUpdate,
    barrier: asyncio.Barrier,
    settled_events: list[JobsSettled] | None = None,
) -> str:
    """Report a task transition on its own service stack and commit it."""
    async with session_factory() as session:
        dispatcher = build_event_dispatcher(session, engine)
        if settled_events is not None:

            async def record(event: JobsSettled) -> None:
                settled_events.append(event)

            dispatcher.register(JobsSettled, record)
        service = _build_task_service(session, engine, dispatcher)
        actor = TaskAuthContext(
            account=seed.owner,
            principal=TaskPrincipal(
                task_id=task_id,
                attempt=1,
                worker_id=seed.worker.id,
                job_id=job_id,
            ),
        )
        await barrier.wait()
        await asyncio.sleep(random.uniform(0.0, 0.005))
        try:
            await service.update_task(task_id, command, actor)
            await session.commit()
            return "ok"
        except Exception as exc:
            await session.rollback()
            return type(exc).__name__


async def _finalize_import(
    session_factory: async_sessionmaker[AsyncSession],
    engine: AsyncEngine,
    seed: PipelineSeed,
    session_id: uuid.UUID,
    barrier: asyncio.Barrier,
) -> str:
    """Finalize a placeholder session on its own service stack and commit it."""
    async with session_factory() as session:
        service = _build_session_service(
            session, engine, build_event_dispatcher(session, engine)
        )
        command = SessionUpdate(
            status=SessionStatus.COMPLETED,
            outputs={"answer": "hi"},
            ended_at=datetime.now(UTC),
        )
        await barrier.wait()
        await asyncio.sleep(random.uniform(0.0, 0.005))
        try:
            await service.update_session(
                session_id, command, AuthContext(account=seed.owner)
            )
            await session.commit()
            return "ok"
        except Exception as exc:
            await session.rollback()
            return type(exc).__name__


async def _cancel_job(
    session_factory: async_sessionmaker[AsyncSession],
    engine: AsyncEngine,
    job_id: uuid.UUID,
    barrier: asyncio.Barrier,
) -> str:
    """Cancel a job on its own service stack and commit it."""
    async with session_factory() as session:
        transitions = _build_transitions(
            session, build_event_dispatcher(session, engine)
        )
        await barrier.wait()
        await asyncio.sleep(random.uniform(0.0, 0.005))
        try:
            await transitions.cancel_job(job_id)
            await session.commit()
            return "ok"
        except Exception as exc:
            await session.rollback()
            return type(exc).__name__


async def test_task_completion_races_import_finalization() -> None:
    """A completion and an import finalization append the evaluators exactly once."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session_with_engine() as (seed_session, engine):
        seed = await _seed_static(seed_session)
        config_id = await _create_config(seed_session, seed, evaluator_count=2)
        await seed_session.commit()
        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        numbers = count(1)

        for iteration in range(RACE_ITERATIONS):
            case = await _seed_replay_case(
                seed_session,
                engine,
                seed,
                config_id,
                numbers,
                task_status=TaskStatus.RUNNING,
            )
            await seed_session.commit()

            barrier = asyncio.Barrier(2)
            results = await asyncio.wait_for(
                asyncio.gather(
                    _report_task_status(
                        session_factory,
                        engine,
                        seed,
                        case.task_id,
                        case.job_id,
                        TaskUpdate(status=TaskStatus.COMPLETED),
                        barrier,
                    ),
                    _finalize_import(
                        session_factory, engine, seed, case.placeholder_id, barrier
                    ),
                ),
                timeout=30,
            )
            assert results == ["ok", "ok"], f"iteration {iteration}: {results}"

            async with session_factory() as verify:
                job = await SQLJobRepository(verify).get(case.job_id)
                assert job.status is JobStatus.RUNNING
                assert not job.provisional
                replay = await SQLReplayRepository(verify).get(case.replay_id)
                assert replay.status is ReplayStatus.EVALUATING
                tasks = await SQLTaskRepository(verify).list_by_job(case.job_id)
                evaluators = [
                    task for task in tasks if isinstance(task, EvaluationTask)
                ]
                assert len(evaluators) == 2, f"iteration {iteration}: {evaluators}"
                assert all(
                    task.status is TaskStatus.PENDING
                    and task.input_session_id == case.placeholder_id
                    for task in evaluators
                )
                agent_task = await SQLTaskRepository(verify).get(case.task_id)
                assert agent_task.status is TaskStatus.COMPLETED
                placeholder = await SQLSessionRepository(verify, engine).get(
                    case.placeholder_id
                )
                assert placeholder.status is SessionStatus.COMPLETED


async def test_job_cancel_races_import_finalization() -> None:
    """A job cancel and an import finalization settle the job exactly once."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session_with_engine() as (seed_session, engine):
        seed = await _seed_static(seed_session)
        config_id = await _create_config(seed_session, seed, evaluator_count=1)
        await seed_session.commit()
        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        numbers = count(1)

        for iteration in range(RACE_ITERATIONS):
            case = await _seed_replay_case(
                seed_session,
                engine,
                seed,
                config_id,
                numbers,
                task_status=TaskStatus.COMPLETED,
            )
            await seed_session.commit()

            barrier = asyncio.Barrier(2)
            results = await asyncio.wait_for(
                asyncio.gather(
                    _cancel_job(session_factory, engine, case.job_id, barrier),
                    _finalize_import(
                        session_factory, engine, seed, case.placeholder_id, barrier
                    ),
                ),
                timeout=30,
            )
            assert results == ["ok", "ok"], f"iteration {iteration}: {results}"

            async with session_factory() as verify:
                job = await SQLJobRepository(verify).get(case.job_id)
                replay = await SQLReplayRepository(verify).get(case.replay_id)
                tasks = await SQLTaskRepository(verify).list_by_job(case.job_id)
                if job.settled:
                    assert job.status is JobStatus.CANCELED, (
                        f"iteration {iteration}: {job.status}"
                    )
                    assert replay.status is ReplayStatus.CANCELED, (
                        f"iteration {iteration}: {replay.status}"
                    )
                    assert all(task.terminal for task in tasks)
                    continue
                # The finalize path won the job row after the cancel pass
                # missed its just-appended evaluator tasks. The job carries
                # the cancel request and only live evaluator tasks, which
                # the sweep's propagation backstop settles.
                assert job.cancel_requested_at is not None
                assert not job.provisional
                assert replay.status is ReplayStatus.EVALUATING
                live = [task for task in tasks if not task.terminal]
                assert live, f"iteration {iteration}: drained but unsettled"
                assert all(isinstance(task, EvaluationTask) for task in live)

            async with session_factory() as backstop:
                transitions = _build_transitions(
                    backstop, build_event_dispatcher(backstop, engine)
                )
                await transitions.request_jobs_cancel([case.job_id])
                await transitions.settle_job_if_drained(case.job_id)
                await backstop.commit()

            async with session_factory() as verify:
                job = await SQLJobRepository(verify).get(case.job_id)
                assert job.status is JobStatus.CANCELED
                replay = await SQLReplayRepository(verify).get(case.replay_id)
                assert replay.status is ReplayStatus.CANCELED


async def test_sibling_task_completions_race_settles_job_once() -> None:
    """Two sibling completions settle the job and map the replay exactly once."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session_with_engine() as (seed_session, engine):
        seed = await _seed_static(seed_session)
        config_id = await _create_config(seed_session, seed, evaluator_count=0)
        await seed_session.commit()
        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        numbers = count(1)

        for iteration in range(RACE_ITERATIONS):
            now = datetime.now(UTC)
            sessions = SQLSessionRepository(seed_session, engine)
            baseline = await sessions.create(
                Session(
                    owner_id=seed.owner.id,
                    agent_id=seed.agent.id,
                    number=next(numbers),
                    origin=SessionOrigin.RECORDED,
                    status=SessionStatus.COMPLETED,
                    inputs={"q": "hi"},
                )
            )
            job = await SQLJobRepository(seed_session).create(
                Job(
                    owner_id=seed.owner.id,
                    kind=JobKind.REPLAY,
                    status=JobStatus.RUNNING,
                )
            )
            replay = await SQLReplayRepository(seed_session).create(
                Replay(
                    owner_id=seed.owner.id,
                    job_id=job.id,
                    replay_config_id=config_id,
                    baseline_session_id=baseline.id,
                    evaluate_baselines=True,
                )
            )
            tasks = SQLTaskRepository(seed_session)
            agent_task = await tasks.create(
                AgentTask(
                    job_id=job.id,
                    agent_version_id=seed.agent_version.id,
                    status=TaskStatus.RUNNING,
                    attempt=1,
                    worker_id=seed.worker.id,
                    claimed_at=now,
                    heartbeat_at=now,
                    started_at=now,
                    inputs={"q": "hi"},
                )
            )
            result_session = await sessions.create(
                Session(
                    owner_id=seed.owner.id,
                    agent_id=seed.agent.id,
                    number=next(numbers),
                    agent_version_id=seed.agent_version.id,
                    task_id=agent_task.id,
                    origin=SessionOrigin.REPLAY,
                    status=SessionStatus.COMPLETED,
                    outputs={"answer": "hi"},
                )
            )
            agent_task.link_result_session(result_session.id)
            await tasks.update(agent_task)
            baseline_evaluator = await tasks.create(
                EvaluationTask(
                    job_id=job.id,
                    plugin_version_id=seed.evaluator_version_ids[0],
                    input_session_id=baseline.id,
                    status=TaskStatus.RUNNING,
                    attempt=1,
                    worker_id=seed.worker.id,
                    claimed_at=now,
                    heartbeat_at=now,
                    started_at=now,
                )
            )
            await seed_session.commit()

            barrier = asyncio.Barrier(2)
            settled_events: list[JobsSettled] = []
            results = await asyncio.wait_for(
                asyncio.gather(
                    _report_task_status(
                        session_factory,
                        engine,
                        seed,
                        agent_task.id,
                        job.id,
                        TaskUpdate(status=TaskStatus.COMPLETED),
                        barrier,
                        settled_events,
                    ),
                    _report_task_status(
                        session_factory,
                        engine,
                        seed,
                        baseline_evaluator.id,
                        job.id,
                        TaskUpdate(
                            status=TaskStatus.COMPLETED,
                            result=[{"name": "accuracy", "score": 1.0}],
                        ),
                        barrier,
                        settled_events,
                    ),
                ),
                timeout=30,
            )
            assert results == ["ok", "ok"], f"iteration {iteration}: {results}"
            assert len(settled_events) == 1, f"iteration {iteration}: {settled_events}"
            assert [settled.id for settled in settled_events[0].jobs] == [job.id]

            async with session_factory() as verify:
                stored_job = await SQLJobRepository(verify).get(job.id)
                assert stored_job.status is JobStatus.COMPLETED
                stored_replay = await SQLReplayRepository(verify).get(replay.id)
                assert stored_replay.status is ReplayStatus.COMPLETED
                stored_tasks = await SQLTaskRepository(verify).list_by_job(job.id)
                assert len(stored_tasks) == 2
                assert all(task.status is TaskStatus.COMPLETED for task in stored_tasks)


async def test_concurrent_imports_adopt_the_placeholder_once() -> None:
    """Two concurrent imports of one external id both land on the placeholder."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session_with_engine() as (seed_session, engine):
        seed = await _seed_static(seed_session)
        plugins = SQLPluginRepository(seed_session)
        importer = await plugins.create(
            Plugin(owner_id=seed.owner.id, kind=PluginKind.IMPORTER, name="langfuse")
        )
        importer_version = await plugins.create_version(
            importer.id, EVALUATOR_SOURCE, display_version=None
        )
        payload, _ = await SQLBlobRepository(seed_session).create(
            Blob(
                owner_id=seed.owner.id,
                sha256="0" * 64,
                size=2,
                media_type="application/json",
                data=b"{}",
            )
        )
        await seed_session.commit()
        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        numbers = count(1)

        async def adopt(
            task_id: uuid.UUID,
            job_id: uuid.UUID,
            external_id: str,
            name: str,
            barrier: asyncio.Barrier,
        ) -> str:
            """Import a session by external id on its own service stack."""
            async with session_factory() as session:
                service = _build_session_service(
                    session, engine, build_event_dispatcher(session, engine)
                )
                actor = TaskAuthContext(
                    account=seed.owner,
                    principal=TaskPrincipal(
                        task_id=task_id,
                        attempt=1,
                        worker_id=seed.worker.id,
                        job_id=job_id,
                    ),
                )
                command = SessionCreate(
                    origin=SessionOrigin.IMPORTED,
                    external_id=external_id,
                    imported_from="langfuse",
                    name=name,
                    inputs={"q": "hi"},
                    outputs={"answer": "hi"},
                )
                await barrier.wait()
                await asyncio.sleep(random.uniform(0.0, 0.005))
                try:
                    stored = await service.create_session(command, actor)
                    await session.commit()
                    return f"ok:{stored.id}"
                except Exception as exc:
                    await session.rollback()
                    return type(exc).__name__

        for iteration in range(RACE_ITERATIONS):
            now = datetime.now(UTC)
            external_id = f"trace-{uuid.uuid4().hex[:8]}"
            placeholder = await SQLSessionRepository(seed_session, engine).create(
                Session(
                    owner_id=seed.owner.id,
                    agent_id=seed.agent.id,
                    number=next(numbers),
                    origin=SessionOrigin.REPLAY,
                    status=SessionStatus.PENDING_IMPORT,
                    external_id=external_id,
                )
            )
            import_task_ids: list[tuple[uuid.UUID, uuid.UUID]] = []
            for _ in range(2):
                import_job = await SQLJobRepository(seed_session).create(
                    Job(
                        owner_id=seed.owner.id,
                        kind=JobKind.IMPORT,
                        status=JobStatus.RUNNING,
                    )
                )
                import_task = await SQLTaskRepository(seed_session).create(
                    ImportTask(
                        job_id=import_job.id,
                        plugin_version_id=importer_version.id,
                        payload_blob_id=payload.id,
                        agent_id=seed.agent.id,
                        status=TaskStatus.RUNNING,
                        attempt=1,
                        worker_id=seed.worker.id,
                        claimed_at=now,
                        heartbeat_at=now,
                        started_at=now,
                    )
                )
                import_task_ids.append((import_task.id, import_job.id))
            await seed_session.commit()

            barrier = asyncio.Barrier(2)
            results = await asyncio.wait_for(
                asyncio.gather(
                    adopt(*import_task_ids[0], external_id, "import-a", barrier),
                    adopt(*import_task_ids[1], external_id, "import-b", barrier),
                ),
                timeout=30,
            )
            assert results == [
                f"ok:{placeholder.id}",
                f"ok:{placeholder.id}",
            ], f"iteration {iteration}: {results}"

            async with session_factory() as verify:
                repository = SQLSessionRepository(verify, engine)
                adopted = await repository.get_pending_import_by_external_id(
                    seed.owner.id, external_id
                )
                assert adopted is not None
                assert adopted.id == placeholder.id
                assert adopted.name in {"import-a", "import-b"}
                assert adopted.imported_from == "langfuse"
                assert adopted.outputs == {"answer": "hi"}

        # The partial unique index rejects a second pending placeholder for
        # an external id one placeholder already holds.
        blocked_id = f"trace-{uuid.uuid4().hex[:8]}"
        await SQLSessionRepository(seed_session, engine).create(
            Session(
                owner_id=seed.owner.id,
                agent_id=seed.agent.id,
                number=next(numbers),
                origin=SessionOrigin.REPLAY,
                status=SessionStatus.PENDING_IMPORT,
                external_id=blocked_id,
            )
        )
        with pytest.raises(DuplicatePendingImportSession):
            await SQLSessionRepository(seed_session, engine).create(
                Session(
                    owner_id=seed.owner.id,
                    agent_id=seed.agent.id,
                    number=next(numbers),
                    origin=SessionOrigin.REPLAY,
                    status=SessionStatus.PENDING_IMPORT,
                    external_id=blocked_id,
                )
            )
        await seed_session.rollback()
