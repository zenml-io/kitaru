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
"""Concurrency tests for task cancellation against PostgreSQL."""

import asyncio
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import pg_session_with_engine, postgres_available
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.task import TaskStatus
from kitaru.api_models.v1.worker import WorkerRuntime, WorkerScope
from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.errors import is_lock_not_available
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
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
from kitaru.server.application.events import EventDispatcher, TaskTerminal
from kitaru.server.application.models.auth import (
    TaskAuthContext,
    TaskPrincipal,
    WorkerAuthContext,
    WorkerPrincipal,
)
from kitaru.server.application.models.task import TaskPolicy, TaskUpdate
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.application.services.task_spec import TaskSpecBuilder
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion, RunSpec
from kitaru.server.domain.job import Job
from kitaru.server.domain.task import AgentTask, Task
from kitaru.server.domain.worker import Worker

TASK_LOW_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
TASK_HIGH_ID = uuid.UUID("ffffffff-ffff-ffff-ffff-fffffffffff2")
JOB_LOW_ID = uuid.UUID("00000000-0000-0000-0000-00000000000a")
JOB_HIGH_ID = uuid.UUID("ffffffff-ffff-ffff-ffff-fffffffffffb")


def _build_task_service(
    session: AsyncSession, dispatcher: EventDispatcher
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
        session_repository=SQLSessionRepository(session),
        job_repository=SQLJobRepository(session),
        spec_builder=spec_builder,
        transitions=transitions,
        policy=policy,
    )


def _build_transitions(session: AsyncSession) -> TaskTransitions:
    """Build task transitions on the SQL repositories of one session."""
    return TaskTransitions(
        task_repository=SQLTaskRepository(session),
        job_repository=SQLJobRepository(session),
        dispatcher=EventDispatcher(),
    )


async def _wait_for_lock_wait(
    session_factory: async_sessionmaker[AsyncSession], blocked: asyncio.Event
) -> None:
    """Set the event once a backend on this database waits on a row lock."""
    async with session_factory() as session:
        for _ in range(400):
            rows = (
                await session.execute(
                    text(
                        "SELECT pid FROM pg_stat_activity "
                        "WHERE wait_event_type = 'Lock' "
                        "AND datname = current_database()"
                    )
                )
            ).all()
            await session.rollback()
            if rows:
                break
            await asyncio.sleep(0.01)
        blocked.set()


async def test_hard_failure_report_survives_concurrent_job_cancel() -> None:
    """A worker reporting an aborting hard failure survives a concurrent cancel.

    Regression test for a lock-order deadlock: the report path used to lock
    only the reported task row before abort propagation locked the job's
    remaining task rows in ascending id order, while the cancellation path
    locks every task row in one ascending id-ordered statement. Reporting a
    high-id task while a cancel was in flight made the two transactions
    acquire the same rows in opposite orders, and PostgreSQL killed one of
    them. The report path now stamps the job alone and leaves the siblings to
    the sweep, so it holds one task row and cannot cycle against the cancel.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session_with_engine() as (seed_session, engine):
        now = datetime.now(UTC)
        owner = await SQLAccountRepository(seed_session).create(Account(name="owner"))
        agent = await SQLAgentRepository(seed_session).create(
            Agent(owner_id=owner.id, name="agent")
        )
        agent_version = await SQLAgentVersionRepository(seed_session).create(
            AgentVersion(owner_id=owner.id, agent_id=agent.id)
        )
        worker = await SQLWorkerRepository(seed_session).register(
            Worker(
                owner_id=owner.id,
                name="worker",
                scope=WorkerScope(),
                runtime=WorkerRuntime(platform="bare"),
                last_seen_at=now,
            )
        )
        job = await SQLJobRepository(seed_session).create(
            Job(owner_id=owner.id, kind=JobKind.REPLAY, status=JobStatus.RUNNING)
        )
        for task_id in (TASK_LOW_ID, TASK_HIGH_ID):
            await SQLTaskRepository(seed_session).create(
                AgentTask(
                    id=task_id,
                    job_id=job.id,
                    agent_version_id=agent_version.id,
                    status=TaskStatus.RUNNING,
                    attempt=1,
                    worker_id=worker.id,
                    claimed_at=now,
                    heartbeat_at=now,
                    started_at=now,
                )
            )
        await seed_session.commit()

        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        reporter_at_barrier = asyncio.Event()
        canceler_blocked = asyncio.Event()

        async def report_failure() -> str:
            async with session_factory() as session:
                dispatcher = EventDispatcher()

                async def hold_at_terminal(event: TaskTerminal) -> None:
                    _ = event
                    reporter_at_barrier.set()
                    await asyncio.wait_for(canceler_blocked.wait(), timeout=20)

                dispatcher.register(TaskTerminal, hold_at_terminal)
                service = _build_task_service(session, dispatcher)
                actor = TaskAuthContext(
                    account=owner,
                    principal=TaskPrincipal(
                        task_id=TASK_HIGH_ID,
                        attempt=1,
                        worker_id=worker.id,
                        job_id=job.id,
                    ),
                )
                try:
                    await service.update_task(
                        TASK_HIGH_ID,
                        TaskUpdate(status=TaskStatus.FAILED, error="boom"),
                        actor,
                    )
                    await session.commit()
                    return "ok"
                except Exception as exc:
                    await session.rollback()
                    return type(exc).__name__

        async def cancel_job() -> str:
            await asyncio.wait_for(reporter_at_barrier.wait(), timeout=20)
            async with session_factory() as session:
                transitions = _build_transitions(session)
                try:
                    await transitions.cancel_job(job.id)
                    await session.commit()
                    return "ok"
                except Exception as exc:
                    await session.rollback()
                    return type(exc).__name__

        async def release_reporter() -> None:
            await asyncio.wait_for(reporter_at_barrier.wait(), timeout=20)
            # Give the canceler a moment to reach its first task row lock.
            await asyncio.sleep(0.05)
            await _wait_for_lock_wait(session_factory, canceler_blocked)

        results = await asyncio.wait_for(
            asyncio.gather(report_failure(), cancel_job(), release_reporter()),
            timeout=30,
        )
        assert results[:2] == ["ok", "ok"]

        async with session_factory() as verify_session:
            tasks = SQLTaskRepository(verify_session)
            failed = await tasks.get(TASK_HIGH_ID)
            assert failed.status is TaskStatus.FAILED
            sibling = await tasks.get(TASK_LOW_ID)
            assert sibling.cancel_requested_at is not None


async def test_job_cancel_survives_concurrent_task_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancel pass and a concurrent worker claim lock job rows in one order.

    Regression test for a lock-order deadlock: the cancellation path used to
    lock job rows one at a time in caller-supplied order while the claim
    path started jobs in claimed-task order, so a cancel pass racing a claim
    of freshly appended tasks could acquire two job rows in opposite orders,
    and PostgreSQL killed one of the transactions. The cancellation path now
    locks every task row, then the job rows, each in one id-ordered
    statement, and the claim path starts jobs in ascending id order.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session_with_engine() as (seed_session, engine):
        now = datetime.now(UTC)
        owner = await SQLAccountRepository(seed_session).create(Account(name="owner"))
        agent = await SQLAgentRepository(seed_session).create(
            Agent(owner_id=owner.id, name="agent")
        )
        agent_version = await SQLAgentVersionRepository(seed_session).create(
            AgentVersion(
                owner_id=owner.id,
                agent_id=agent.id,
                run_spec=RunSpec(command="run.sh", timeout_seconds=60),
            )
        )
        worker = await SQLWorkerRepository(seed_session).register(
            Worker(
                owner_id=owner.id,
                name="worker",
                scope=WorkerScope(),
                runtime=WorkerRuntime(platform="bare"),
                last_seen_at=now,
            )
        )
        for job_id in (JOB_LOW_ID, JOB_HIGH_ID):
            await SQLJobRepository(seed_session).create(
                Job(
                    id=job_id,
                    owner_id=owner.id,
                    kind=JobKind.REPLAY,
                    status=JobStatus.RUNNING,
                )
            )
        await seed_session.commit()

        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        canceler_past_task_statements = asyncio.Event()
        tasks_appended = asyncio.Event()
        claimer_locked_first_job = asyncio.Event()
        canceler_blocked = asyncio.Event()

        real_cancel_pending = SQLTaskRepository.cancel_pending

        async def paused_cancel_pending(
            self: SQLTaskRepository, job_ids: Sequence[uuid.UUID], now: datetime
        ) -> list[Task]:
            canceled = await real_cancel_pending(self, job_ids, now)
            canceler_past_task_statements.set()
            await asyncio.wait_for(tasks_appended.wait(), timeout=20)
            return canceled

        monkeypatch.setattr(SQLTaskRepository, "cancel_pending", paused_cancel_pending)

        real_start_job = TaskTransitions.start_job
        started_job_ids: list[uuid.UUID] = []

        async def paused_start_job(self: TaskTransitions, job_id: uuid.UUID) -> None:
            await real_start_job(self, job_id)
            started_job_ids.append(job_id)
            if len(started_job_ids) == 1:
                claimer_locked_first_job.set()
                await asyncio.wait_for(canceler_blocked.wait(), timeout=20)

        monkeypatch.setattr(TaskTransitions, "start_job", paused_start_job)

        async def cancel_jobs() -> str:
            async with session_factory() as session:
                transitions = _build_transitions(session)
                try:
                    await transitions.request_jobs_cancel([JOB_LOW_ID, JOB_HIGH_ID])
                    await session.commit()
                    return "ok"
                except Exception as exc:
                    await session.rollback()
                    return type(exc).__name__

        async def claim_tasks() -> str:
            async with session_factory() as session:
                service = _build_task_service(session, EventDispatcher())
                actor = WorkerAuthContext(
                    account=owner, principal=WorkerPrincipal(worker_id=worker.id)
                )
                try:
                    claimed = await service.claim_tasks(10, actor=actor)
                    await session.commit()
                    return f"ok:{len(claimed)}"
                except Exception as exc:
                    await session.rollback()
                    return type(exc).__name__

        canceler = asyncio.create_task(cancel_jobs())
        await asyncio.wait_for(canceler_past_task_statements.wait(), timeout=20)

        # Append the pending tasks after the canceler's task statements ran,
        # so the claim races only the canceler's job row locks. The lower
        # task id belongs to the higher job id, which put the claim's job
        # locks in claimed-task order ahead of the fix.
        async with session_factory() as append_session:
            repository = SQLTaskRepository(append_session)
            for task_id, job_id in (
                (TASK_LOW_ID, JOB_HIGH_ID),
                (TASK_HIGH_ID, JOB_LOW_ID),
            ):
                await repository.create(
                    AgentTask(
                        id=task_id,
                        job_id=job_id,
                        agent_version_id=agent_version.id,
                    )
                )
            await append_session.commit()

        claimer = asyncio.create_task(claim_tasks())
        await asyncio.wait_for(claimer_locked_first_job.wait(), timeout=20)
        tasks_appended.set()
        monitor = asyncio.create_task(
            _wait_for_lock_wait(session_factory, canceler_blocked)
        )

        results = await asyncio.wait_for(
            asyncio.gather(canceler, claimer, monitor), timeout=30
        )
        assert results[:2] == ["ok", "ok:2"]

        async with session_factory() as verify_session:
            jobs = SQLJobRepository(verify_session)
            for job_id in (JOB_LOW_ID, JOB_HIGH_ID):
                job = await jobs.get(job_id)
                assert job.cancel_requested_at is not None


async def test_cancel_propagation_skips_a_job_another_sweep_holds() -> None:
    """A second sweeper's NOWAIT propagation fails fast instead of waiting.

    Two replicas run the propagation backstop over the same candidate job.
    The first holds the job's task rows, and the second has to give up that
    job and move on rather than block the rest of its tick behind it.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session_with_engine() as (seed_session, engine):
        now = datetime.now(UTC)
        owner = await SQLAccountRepository(seed_session).create(Account(name="owner"))
        agent = await SQLAgentRepository(seed_session).create(
            Agent(owner_id=owner.id, name="agent")
        )
        agent_version = await SQLAgentVersionRepository(seed_session).create(
            AgentVersion(owner_id=owner.id, agent_id=agent.id)
        )
        job = await SQLJobRepository(seed_session).create(
            Job(
                owner_id=owner.id,
                kind=JobKind.REPLAY,
                status=JobStatus.RUNNING,
                cancel_requested_at=now,
            )
        )
        await SQLTaskRepository(seed_session).create(
            AgentTask(
                id=TASK_LOW_ID,
                job_id=job.id,
                agent_version_id=agent_version.id,
                status=TaskStatus.RUNNING,
                attempt=1,
                claimed_at=now,
                heartbeat_at=now,
                started_at=now,
            )
        )
        await seed_session.commit()

        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        async with session_factory() as holder:
            await SQLTaskRepository(holder).lock_by_jobs([job.id])
            async with session_factory() as sweeper:
                with pytest.raises(DBAPIError) as raised:
                    await asyncio.wait_for(
                        _build_transitions(sweeper).request_jobs_cancel(
                            [job.id], nowait=True
                        ),
                        timeout=10,
                    )
                await sweeper.rollback()
            assert is_lock_not_available(raised.value)
            await holder.rollback()

        async with session_factory() as verify_session:
            task = await SQLTaskRepository(verify_session).get(TASK_LOW_ID)
            assert task.cancel_requested_at is None


async def test_job_cancel_survives_concurrent_job_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A job cancel and a concurrent job delete lock task rows in one order.

    Regression test for a lock-order deadlock: the job delete used to lock
    the job row first and let the delete's cascade lock the task rows
    unordered after it, while the cancellation path locks every task row
    before the job row. A cancel holding the task rows while a delete held
    the job row made the two transactions wait on each other, and PostgreSQL
    killed one of them. The delete now locks the task rows in id order
    before the job row.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session_with_engine() as (seed_session, engine):
        now = datetime.now(UTC)
        owner = await SQLAccountRepository(seed_session).create(Account(name="owner"))
        agent = await SQLAgentRepository(seed_session).create(
            Agent(owner_id=owner.id, name="agent")
        )
        agent_version = await SQLAgentVersionRepository(seed_session).create(
            AgentVersion(owner_id=owner.id, agent_id=agent.id)
        )
        worker = await SQLWorkerRepository(seed_session).register(
            Worker(
                owner_id=owner.id,
                name="worker",
                scope=WorkerScope(),
                runtime=WorkerRuntime(platform="bare"),
                last_seen_at=now,
            )
        )
        job = await SQLJobRepository(seed_session).create(
            Job(owner_id=owner.id, kind=JobKind.REPLAY, status=JobStatus.RUNNING)
        )
        for task_id in (TASK_LOW_ID, TASK_HIGH_ID):
            await SQLTaskRepository(seed_session).create(
                AgentTask(
                    id=task_id,
                    job_id=job.id,
                    agent_version_id=agent_version.id,
                    status=TaskStatus.RUNNING,
                    attempt=1,
                    worker_id=worker.id,
                    claimed_at=now,
                    heartbeat_at=now,
                    started_at=now,
                )
            )
        await seed_session.commit()

        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        canceler_past_task_statements = asyncio.Event()
        deleter_blocked = asyncio.Event()

        real_cancel_pending = SQLTaskRepository.cancel_pending

        async def paused_cancel_pending(
            self: SQLTaskRepository, job_ids: Sequence[uuid.UUID], now: datetime
        ) -> list[Task]:
            canceled = await real_cancel_pending(self, job_ids, now)
            canceler_past_task_statements.set()
            await asyncio.wait_for(deleter_blocked.wait(), timeout=20)
            return canceled

        monkeypatch.setattr(SQLTaskRepository, "cancel_pending", paused_cancel_pending)

        async def cancel_job() -> str:
            async with session_factory() as session:
                transitions = _build_transitions(session)
                try:
                    await transitions.cancel_job(job.id)
                    await session.commit()
                    return "ok"
                except Exception as exc:
                    await session.rollback()
                    return type(exc).__name__

        async def delete_job() -> str:
            await asyncio.wait_for(canceler_past_task_statements.wait(), timeout=20)
            async with session_factory() as session:
                try:
                    await SQLJobRepository(session).delete(job.id)
                    await session.commit()
                    return "ok"
                except Exception as exc:
                    await session.rollback()
                    return type(exc).__name__

        async def release_canceler() -> None:
            await asyncio.wait_for(canceler_past_task_statements.wait(), timeout=20)
            # Give the deleter a moment to reach its first task row lock.
            await asyncio.sleep(0.05)
            await _wait_for_lock_wait(session_factory, deleter_blocked)

        results = await asyncio.wait_for(
            asyncio.gather(cancel_job(), delete_job(), release_canceler()),
            timeout=30,
        )
        assert results[:2] == ["ok", "ok"]

        async with session_factory() as verify_session:
            assert await SQLJobRepository(verify_session).get_many([job.id]) == {}
            remaining = await SQLTaskRepository(verify_session).get_many(
                [TASK_LOW_ID, TASK_HIGH_ID]
            )
            assert remaining == {}
