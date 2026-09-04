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
"""Tests for worker use cases."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from worker_coverage_cases import COVERAGE_CASES, CoverageCase, CoverageIds

from conftest import UNSCOPED_WORKER_SCOPE, FakeWorkerRepository, create_worker
from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.task import TaskKind
from kitaru.api_models.v1.worker import (
    LabelSelector,
    WorkerClaim,
    WorkerRuntime,
    WorkerScope,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.worker import WorkerFilter
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.application.services.worker_service import WorkerService
from kitaru.server.domain.account import Account
from kitaru.server.domain.task import AgentTask, ImportTask
from kitaru.server.domain.worker import WorkerAccessDenied, WorkerNotFound
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording track calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.tracked: list[tuple[uuid.UUID, str, dict[str, Any]]] = []

    def track(
        self,
        user_id: uuid.UUID,
        event: AnalyticsEvent | str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Record a track call instead of buffering it.

        Args:
            user_id: User id.
            event: Event name.
            properties: Event properties.
        """
        self.tracked.append((user_id, event, properties or {}))


@pytest.fixture
def repository() -> FakeWorkerRepository:
    """Provide a fake worker repository."""
    return FakeWorkerRepository()


@pytest.fixture
def service(repository: FakeWorkerRepository) -> WorkerService:
    """Provide a worker service backed by the fake repository."""
    return WorkerService(repository=repository, liveness_timeout_seconds=60)


async def test_register_worker(service: WorkerService) -> None:
    """Register a new worker."""
    worker = await service.register_worker(
        name="worker-1",
        scope=UNSCOPED_WORKER_SCOPE,
        runtime=WorkerRuntime(platform="bare"),
        metadata={"region": "eu"},
        actor=ACTOR,
    )
    assert worker.name == "worker-1"
    assert worker.owner_id == ACTOR.account.id
    assert worker.metadata == {"region": "eu"}
    assert worker.created is not None
    assert worker.updated is not None


async def test_register_worker_tracks_every_registration(
    repository: FakeWorkerRepository,
) -> None:
    """Track the worker registered event on every registration."""
    analytics = _RecordingAnalytics()
    service = WorkerService(
        repository=repository, liveness_timeout_seconds=60, analytics=analytics
    )

    for _ in range(2):
        await service.register_worker(
            name="worker-1",
            scope=UNSCOPED_WORKER_SCOPE,
            runtime=WorkerRuntime(platform="docker"),
            metadata={},
            actor=ACTOR,
        )

    assert (
        analytics.tracked
        == [
            (
                ACTOR.account.id,
                AnalyticsEvent.WORKER_REGISTERED,
                {"worker_platform": "docker"},
            )
        ]
        * 2
    )


async def test_register_worker_same_name_creates_a_second_worker(
    service: WorkerService,
) -> None:
    """Registering under an existing name creates a separate worker."""
    first = await service.register_worker(
        name="worker-1",
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)]),
        runtime=WorkerRuntime(platform="bare"),
        metadata={"region": "eu"},
        actor=ACTOR,
    )
    second = await service.register_worker(
        name="worker-1",
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.IMPORTER)]),
        runtime=WorkerRuntime(platform="docker"),
        metadata={"region": "us"},
        actor=ACTOR,
    )
    assert second.id != first.id
    assert (await service.get_worker(first.id, actor=ACTOR)).scope == first.scope
    assert (await service.get_worker(second.id, actor=ACTOR)).scope == second.scope


@pytest.mark.parametrize(
    "case", COVERAGE_CASES, ids=[case.name for case in COVERAGE_CASES]
)
async def test_is_covered_matches_claim_cases(
    service: WorkerService, repository: FakeWorkerRepository, case: CoverageCase
) -> None:
    """is_covered agrees with the shared worker coverage cases."""
    ids = CoverageIds(
        job_id=uuid.uuid4(),
        other_job_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        agent_version_id=uuid.uuid4(),
        agent_version_id_2=uuid.uuid4(),
        plugin_version_id=uuid.uuid4(),
        import_id=uuid.uuid4(),
        session_id=uuid.uuid4(),
    )
    await create_worker(repository, ACTOR.account.id, scope=case.scope(ids))
    assert await service.is_covered([case.task(ids)]) is case.covered


async def test_is_covered_ignores_stale_workers(
    service: WorkerService, repository: FakeWorkerRepository
) -> None:
    """A covering worker outside the liveness window does not count."""
    await create_worker(
        repository,
        ACTOR.account.id,
        scope=UNSCOPED_WORKER_SCOPE,
        last_seen_at=datetime.now(UTC) - timedelta(minutes=5),
    )
    task = AgentTask(job_id=uuid.uuid4(), agent_version_id=uuid.uuid4())
    assert await service.is_covered([task]) is False


async def test_is_covered_without_workers(service: WorkerService) -> None:
    """No live workers means no task is covered."""
    task = AgentTask(job_id=uuid.uuid4(), agent_version_id=uuid.uuid4())
    assert await service.is_covered([task]) is False


async def test_is_covered_requires_every_task(
    service: WorkerService, repository: FakeWorkerRepository
) -> None:
    """One uncovered task among covered ones leaves the job uncovered."""
    await create_worker(
        repository,
        ACTOR.account.id,
        scope=WorkerScope(claims=[WorkerClaim(kind=TaskKind.AGENT)]),
    )
    job_id = uuid.uuid4()
    agent_task = AgentTask(job_id=job_id, agent_version_id=uuid.uuid4())
    import_task = ImportTask(job_id=job_id, import_id=uuid.uuid4())
    assert await service.is_covered([agent_task]) is True
    assert await service.is_covered([agent_task, import_task]) is False


async def test_is_covered_without_tasks(service: WorkerService) -> None:
    """A job with nothing to run needs no worker."""
    assert await service.is_covered([]) is True


async def test_is_covered_counts_workers_of_other_accounts(
    service: WorkerService, repository: FakeWorkerRepository
) -> None:
    """A live worker owned by another account still covers the task."""
    other = AuthContext(account=Account(id=uuid.uuid4(), name="bob"))
    await create_worker(repository, other.account.id, scope=UNSCOPED_WORKER_SCOPE)
    task = AgentTask(job_id=uuid.uuid4(), agent_version_id=uuid.uuid4())
    assert await service.is_covered([task]) is True


async def test_register_ephemeral_worker(service: WorkerService) -> None:
    """Register a worker with the ephemeral scope of one job."""
    job_id = uuid.uuid4()
    runtime = WorkerRuntime(platform="bare")
    worker = await service.register_ephemeral_worker(
        job_id=job_id, runtime=runtime, actor=ACTOR
    )
    assert worker.name == f"job-{job_id}"
    assert worker.scope == WorkerScope(
        claims=[
            WorkerClaim(kind=TaskKind.IMPORTER),
            WorkerClaim(kind=TaskKind.EVALUATOR),
        ],
        selectors=[
            LabelSelector(
                key="kitaru/plugin_namespace", values=["kitaru"], required=True
            )
        ],
        job_id=job_id,
    )
    assert worker.runtime == runtime
    assert worker.metadata == {"ephemeral": "true"}
    assert worker.owner_id == ACTOR.account.id


async def test_renew_worker_stamps_last_seen_at(service: WorkerService) -> None:
    """Renewing a worker stamps it as seen."""
    created = await service.register_worker(
        name="worker-1",
        scope=UNSCOPED_WORKER_SCOPE,
        runtime=WorkerRuntime(platform="bare"),
        metadata={},
        actor=ACTOR,
    )
    await service.renew_worker(created.id, actor=ACTOR)
    reloaded = await service.get_worker(created.id, actor=ACTOR)
    assert reloaded.last_seen_at >= created.last_seen_at


async def test_renew_worker_not_found(service: WorkerService) -> None:
    """Renewing an unknown worker raises WorkerNotFound."""
    with pytest.raises(WorkerNotFound):
        await service.renew_worker(uuid.uuid4(), actor=ACTOR)


async def test_renew_worker_of_another_account(service: WorkerService) -> None:
    """Renewing a worker of another account raises WorkerAccessDenied."""
    created = await service.register_worker(
        name="worker-1",
        scope=UNSCOPED_WORKER_SCOPE,
        runtime=WorkerRuntime(platform="bare"),
        metadata={},
        actor=ACTOR,
    )
    other = AuthContext(account=Account(id=uuid.uuid4(), name="bob"))
    with pytest.raises(WorkerAccessDenied):
        await service.renew_worker(created.id, actor=other)


async def test_get_worker(service: WorkerService) -> None:
    """Load a stored worker by id."""
    created = await service.register_worker(
        name="worker-1",
        scope=UNSCOPED_WORKER_SCOPE,
        runtime=WorkerRuntime(platform="bare"),
        metadata={},
        actor=ACTOR,
    )
    loaded = await service.get_worker(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_worker_not_found(service: WorkerService) -> None:
    """Raise for an unknown worker id."""
    missing_id = uuid.uuid4()
    with pytest.raises(WorkerNotFound, match=f"Worker {missing_id} was not found"):
        await service.get_worker(missing_id, actor=ACTOR)


async def test_list_workers(service: WorkerService) -> None:
    """List workers newest-first with filters."""
    for name in ["worker-1", "worker-2", "worker-3"]:
        await service.register_worker(
            name=name,
            scope=UNSCOPED_WORKER_SCOPE,
            runtime=WorkerRuntime(platform="bare"),
            metadata={},
            actor=ACTOR,
        )

    workers, next_cursor = await service.list_workers(WorkerFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [worker.name for worker in workers] == ["worker-3", "worker-2", "worker-1"]

    workers, next_cursor = await service.list_workers(
        WorkerFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="worker-2")
        ),
        actor=ACTOR,
    )
    assert next_cursor is None
    assert workers[0].name == "worker-2"


async def test_list_workers_hides_stale_unless_included(
    service: WorkerService, repository: FakeWorkerRepository
) -> None:
    """Leave workers past the liveness window out unless the filter includes them."""
    now = datetime.now(UTC)
    stale = await create_worker(
        repository,
        ACTOR.account.id,
        name="stale",
        last_seen_at=now - timedelta(hours=1),
    )
    fresh = await create_worker(
        repository, ACTOR.account.id, name="fresh", last_seen_at=now
    )

    workers, next_cursor = await service.list_workers(WorkerFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [worker.id for worker in workers] == [fresh.id]

    workers, _ = await service.list_workers(
        WorkerFilter(include_stale=True), actor=ACTOR
    )
    assert {worker.id for worker in workers} == {stale.id, fresh.id}


async def test_delete_worker(service: WorkerService) -> None:
    """Delete a stored worker."""
    created = await service.register_worker(
        name="worker-1",
        scope=UNSCOPED_WORKER_SCOPE,
        runtime=WorkerRuntime(platform="bare"),
        metadata={},
        actor=ACTOR,
    )
    await service.delete_worker(created.id, actor=ACTOR)
    with pytest.raises(WorkerNotFound):
        await service.get_worker(created.id, actor=ACTOR)


async def test_delete_worker_not_found(service: WorkerService) -> None:
    """Raise for an unknown worker id."""
    with pytest.raises(WorkerNotFound):
        await service.delete_worker(uuid.uuid4(), actor=ACTOR)


async def test_worker_scope_round_trip(service: WorkerService) -> None:
    """Round-trip a scope carrying selectors and a job pin."""
    scope = WorkerScope(
        claims=[WorkerClaim(kind=TaskKind.AGENT), WorkerClaim(kind=TaskKind.EVALUATOR)],
        selectors=[LabelSelector(key="agent_version", values=["v1", "v2"])],
        job_id=uuid.uuid4(),
    )
    created = await service.register_worker(
        name="worker-1",
        scope=scope,
        runtime=WorkerRuntime(platform="kubernetes", namespace="default"),
        metadata={},
        actor=ACTOR,
    )
    loaded = await service.get_worker(created.id, actor=ACTOR)
    assert loaded.scope == scope


async def test_is_live_true(service: WorkerService) -> None:
    """Report a recently seen worker as live."""
    created = await service.register_worker(
        name="worker-1",
        scope=UNSCOPED_WORKER_SCOPE,
        runtime=WorkerRuntime(platform="bare"),
        metadata={},
        actor=ACTOR,
    )
    assert created.is_live(datetime.now(UTC), timeout_seconds=60) is True


async def test_is_live_false(
    service: WorkerService, repository: FakeWorkerRepository
) -> None:
    """Report a worker outside the liveness window as not live."""
    stale = await create_worker(
        repository,
        ACTOR.account.id,
        last_seen_at=datetime.now(UTC) - timedelta(minutes=5),
    )
    assert stale.is_live(datetime.now(UTC), timeout_seconds=60) is False
