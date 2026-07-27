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
"""Contract tests for job repositories."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeBlobRepository,
    FakeJobRepository,
    FakePluginRepository,
    FakeReplayConfigRepository,
    FakeSessionRepository,
    FakeWorkerRepository,
    pg_session,
    postgres_available,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import (
    SQLAgentRepository,
)
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.blob_repository import (
    SQLBlobRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import (
    SQLJobRepository,
)
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.adapters.db.repositories.replay_config_repository import (
    SQLReplayConfigRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.worker_repository import (
    SQLWorkerRepository,
)
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.blob_repository import (
    BlobRepository,
)
from kitaru.server.application.interfaces.job_repository import (
    JobRepository,
)
from kitaru.server.application.interfaces.plugin_repository import (
    PluginRepository,
)
from kitaru.server.application.interfaces.replay_config_repository import (
    ReplayConfigRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.interfaces.worker_repository import (
    WorkerRepository,
)
from kitaru.server.application.models.jobs import JobFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionInUse,
    AgentVersionNotFound,
    RunSpec,
)
from kitaru.server.domain.blob import Blob, BlobNotFound
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.job import (
    DuplicateScoreJob,
    Import,
    JobKind,
    JobNotFound,
    JobStatus,
    ReplayJob,
    Score,
    SessionRun,
    WorkerScope,
)
from kitaru.server.domain.plugin import (
    Plugin,
    PluginFormat,
    PluginKind,
    PluginVersion,
    PluginVersionIdNotFound,
)
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    RegistryScorerConfig,
    ReplayConfig,
    ScoringPolicy,
    SourceRef,
    SourceScorerConfig,
    ToolPolicyConfig,
)
from kitaru.server.domain.session import (
    Session,
    SessionInUse,
    SessionNotFound,
    SessionOrigin,
    SessionStatus,
)
from kitaru.server.domain.worker import Worker

SCORING_POLICY = ScoringPolicy(
    scorers=[
        SourceScorerConfig(
            name="conciseness",
            source=SourceRef(module="my_pkg.scorers", attribute="conciseness"),
        )
    ],
    pass_threshold=0.5,
)


class Setup(NamedTuple):
    """Repository bundle for job contract tests."""

    jobs: JobRepository
    configs: ReplayConfigRepository
    sessions: SessionRepository
    versions: AgentVersionRepository
    agents: AgentRepository
    blobs: BlobRepository
    plugins: PluginRepository
    workers: WorkerRepository
    owner_id: uuid.UUID


class Seed(NamedTuple):
    """Seeded rows for job contract tests."""

    session: Session
    version: AgentVersion
    config: ReplayConfig


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each job repository implementation plus an owner id."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        versions = FakeAgentVersionRepository(agents)
        sessions = FakeSessionRepository(agents, versions)
        configs = FakeReplayConfigRepository()
        blobs = FakeBlobRepository()
        plugins = FakePluginRepository(blobs)
        jobs = FakeJobRepository(sessions, versions, plugins, blobs)
        yield Setup(
            jobs,
            configs,
            sessions,
            versions,
            agents,
            blobs,
            plugins,
            FakeWorkerRepository(),
            uuid.uuid4(),
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning account first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield Setup(
            SQLJobRepository(session),
            SQLReplayConfigRepository(session),
            SQLSessionRepository(session),
            SQLAgentVersionRepository(session),
            SQLAgentRepository(session),
            SQLBlobRepository(session),
            SQLPluginRepository(session),
            SQLWorkerRepository(session),
            owner.id,
        )


async def seed_rows(setup: Setup, name: str = "support-bot") -> Seed:
    """Store an agent, a runnable version, a session, and a config.

    Args:
        setup: Repository bundle.
        name: Agent name.

    Returns:
        Seeded rows.
    """
    agent = await setup.agents.create(Agent(owner_id=setup.owner_id, name=name))
    version = await setup.versions.create(
        AgentVersion(
            owner_id=setup.owner_id,
            agent_id=agent.id,
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        )
    )
    session = await setup.sessions.create(
        Session(
            owner_id=setup.owner_id,
            agent_id=agent.id,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )
    config = await setup.configs.create(
        ReplayConfig(
            owner_id=setup.owner_id,
            tool_policy=ToolPolicyConfig(default=HistoryPolicy()),
            scoring_policy=SCORING_POLICY,
        )
    )
    return Seed(session, version, config)


def job_entity(seed: Seed, **overrides: object) -> ReplayJob:
    """Build a standalone replay job entity.

    Args:
        seed: Seeded rows.
        **overrides: Field overrides.

    Returns:
        Replay job entity.
    """
    values: dict[str, object] = {
        "agent_version_id": seed.version.id,
        "input_session_id": seed.session.id,
        "execution_target": ExecutionTarget.POOL,
        **overrides,
    }
    return ReplayJob.model_validate(values)


def session_run_entity(seed: Seed, **overrides: object) -> SessionRun:
    """Build a session run entity.

    Args:
        seed: Seeded rows.
        **overrides: Field overrides.

    Returns:
        SessionRun entity.
    """
    values: dict[str, object] = {
        "agent_version_id": seed.version.id,
        "execution_target": ExecutionTarget.POOL,
        **overrides,
    }
    return SessionRun.model_validate(values)


async def test_create_round_trips_all_fields(setup: Setup) -> None:
    """Store a job and round-trip every field."""
    seed = await seed_rows(setup)
    created = await setup.jobs.create(job_entity(seed))
    assert created.created is not None
    assert created.updated is not None
    loaded = await setup.jobs.get(created.id)
    assert loaded == created
    assert isinstance(loaded, ReplayJob)
    assert loaded.kind is JobKind.REPLAY
    assert loaded.experiment_run_id is None
    assert loaded.agent_version_id == seed.version.id
    assert loaded.input_session_id == seed.session.id
    assert loaded.result_session_id is None
    assert loaded.status is JobStatus.PENDING
    assert loaded.attempt == 1
    assert loaded.result is None


async def test_create_unknown_references(setup: Setup) -> None:
    """Raise for unknown version and session ids."""
    seed = await seed_rows(setup)
    missing_id = uuid.uuid4()
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await setup.jobs.create(job_entity(seed, agent_version_id=missing_id))
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await setup.jobs.create(job_entity(seed, input_session_id=missing_id))


async def test_standalone_jobs_repeat_freely(setup: Setup) -> None:
    """Replay the same session standalone any number of times."""
    seed = await seed_rows(setup)
    first = await setup.jobs.create(job_entity(seed))
    second = await setup.jobs.create(job_entity(seed))
    assert isinstance(first, ReplayJob)
    assert isinstance(second, ReplayJob)
    assert first.input_session_id == second.input_session_id
    _, total = await setup.jobs.query(JobFilter(input_session_id=seed.session.id))
    assert total == 2


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown job id."""
    missing_id = uuid.uuid4()
    with pytest.raises(JobNotFound, match=f"Job {missing_id} was not found"):
        await setup.jobs.get(missing_id)


async def test_query_filters(setup: Setup) -> None:
    """Query jobs by session, status, and standalone."""
    seed = await seed_rows(setup)
    other_seed = await seed_rows(setup, name="triage-bot")
    first = await setup.jobs.create(job_entity(seed))
    await setup.jobs.create(job_entity(other_seed))

    jobs, total = await setup.jobs.query(JobFilter())
    assert total == 2

    jobs, total = await setup.jobs.query(JobFilter(input_session_id=seed.session.id))
    assert total == 1
    assert jobs[0].id == first.id

    jobs, total = await setup.jobs.query(JobFilter(status=JobStatus.PENDING))
    assert total == 2
    jobs, total = await setup.jobs.query(JobFilter(status=JobStatus.RUNNING))
    assert total == 0

    jobs, total = await setup.jobs.query(JobFilter(standalone=True))
    assert total == 2
    jobs, total = await setup.jobs.query(JobFilter(standalone=False))
    assert total == 0

    jobs, total = await setup.jobs.query(JobFilter(page=2, page_size=1))
    assert total == 2
    assert len(jobs) == 1


async def test_references_agent_version(setup: Setup) -> None:
    """Report whether a job references an agent version."""
    seed = await seed_rows(setup)
    assert await setup.jobs.references_agent_version(seed.version.id) is False
    await setup.jobs.create(job_entity(seed))
    assert await setup.jobs.references_agent_version(seed.version.id) is True
    assert await setup.jobs.references_agent_version(uuid.uuid4()) is False


async def test_session_delete_blocked_by_job(setup: Setup) -> None:
    """Block deleting a session that a job references."""
    seed = await seed_rows(setup)
    await setup.jobs.create(job_entity(seed))
    with pytest.raises(
        SessionInUse, match=f"Session {seed.session.id} is referenced by jobs"
    ):
        await setup.sessions.delete(seed.session.id)
    # The failed delete leaves the repository usable.
    loaded = await setup.sessions.get(seed.session.id)
    assert loaded.id == seed.session.id


async def test_result_session_delete_blocked_by_job(setup: Setup) -> None:
    """Block deleting a session that a job links as its result."""
    seed = await seed_rows(setup)
    result = await setup.sessions.create(
        Session(
            owner_id=setup.owner_id,
            agent_id=seed.session.agent_id,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )
    await setup.jobs.create(job_entity(seed, result_session_id=result.id))
    with pytest.raises(
        SessionInUse, match=f"Session {result.id} is referenced by jobs"
    ):
        await setup.sessions.delete(result.id)


async def test_agent_version_delete_blocked_by_job(setup: Setup) -> None:
    """Block deleting an agent version that a job references."""
    seed = await seed_rows(setup)
    await setup.jobs.create(job_entity(seed))
    with pytest.raises(
        AgentVersionInUse,
        match=f"Agent version {seed.version.id} is referenced by jobs",
    ):
        await setup.versions.delete(seed.version.id)


async def test_delete_removes_job(setup: Setup) -> None:
    """Delete a job."""
    seed = await seed_rows(setup)
    created = await setup.jobs.create(job_entity(seed))
    await setup.jobs.delete(created.id)
    with pytest.raises(JobNotFound, match=f"Job {created.id} was not found"):
        await setup.jobs.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown job id."""
    missing_id = uuid.uuid4()
    with pytest.raises(JobNotFound, match=f"Job {missing_id} was not found"):
        await setup.jobs.delete(missing_id)


async def test_update_round_trips_runner_fields(setup: Setup) -> None:
    """Persist runner-side field changes and renew the updated timestamp."""
    seed = await seed_rows(setup)
    created = await setup.jobs.create(job_entity(seed))
    assert isinstance(created, ReplayJob)
    result = await setup.sessions.create(
        Session(
            owner_id=setup.owner_id,
            agent_id=seed.session.agent_id,
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.COMPLETED,
        )
    )
    created.start()
    created.link_result_session(result.id)
    created.complete(None)
    updated = await setup.jobs.update(created)
    assert isinstance(updated, ReplayJob)
    assert updated.status is JobStatus.COMPLETED
    assert updated.result_session_id == result.id
    assert updated.started_at is not None
    assert updated.ended_at is not None
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await setup.jobs.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown job id."""
    seed = await seed_rows(setup)
    with pytest.raises(JobNotFound):
        await setup.jobs.update(job_entity(seed))


async def test_update_unknown_result_session(setup: Setup) -> None:
    """Raise for a result session id that does not resolve."""
    seed = await seed_rows(setup)
    created = await setup.jobs.create(job_entity(seed))
    missing_id = uuid.uuid4()
    created.start()
    created.link_result_session(missing_id)
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await setup.jobs.update(created)
    # The failed update leaves the repository usable.
    loaded = await setup.jobs.get(created.id)
    assert loaded.result_session_id is None


async def test_session_run_round_trips_all_fields(setup: Setup) -> None:
    """Store a session run and round-trip every field."""
    seed = await seed_rows(setup)
    created = await setup.jobs.create(
        session_run_entity(seed, inputs={"prompt": "hi"}, name="smoke")
    )
    assert created.created is not None
    assert created.updated is not None
    loaded = await setup.jobs.get(created.id)
    assert loaded == created
    assert isinstance(loaded, SessionRun)
    assert loaded.kind is JobKind.SESSION_RUN
    assert loaded.agent_version_id == seed.version.id
    assert loaded.inputs == {"prompt": "hi"}
    assert loaded.name == "smoke"
    assert loaded.execution_target is ExecutionTarget.POOL
    assert loaded.executor_handle is None
    assert loaded.result_session_id is None
    assert loaded.status is JobStatus.PENDING
    assert loaded.standalone is True


async def test_session_run_create_unknown_version(setup: Setup) -> None:
    """Raise for an unknown agent version id."""
    seed = await seed_rows(setup)
    missing_id = uuid.uuid4()
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await setup.jobs.create(session_run_entity(seed, agent_version_id=missing_id))


async def test_query_kind_and_execution_target_filters(setup: Setup) -> None:
    """Query jobs by kind and execution target."""
    seed = await seed_rows(setup)
    replay_job = await setup.jobs.create(
        job_entity(seed, execution_target=ExecutionTarget.ON_DEMAND)
    )
    session_run_job = await setup.jobs.create(session_run_entity(seed))

    jobs, total = await setup.jobs.query(JobFilter(kind=JobKind.REPLAY))
    assert total == 1
    assert jobs[0].id == replay_job.id

    jobs, total = await setup.jobs.query(JobFilter(kind=JobKind.SESSION_RUN))
    assert total == 1
    assert jobs[0].id == session_run_job.id

    jobs, total = await setup.jobs.query(
        JobFilter(execution_target=ExecutionTarget.POOL)
    )
    assert total == 1
    assert jobs[0].id == session_run_job.id
    jobs, total = await setup.jobs.query(
        JobFilter(execution_target=ExecutionTarget.ON_DEMAND)
    )
    assert total == 1
    assert jobs[0].id == replay_job.id


async def seed_worker(setup: Setup, name: str = "worker-1") -> uuid.UUID:
    """Store a worker for the claim tests.

    Args:
        setup: Repository bundle.
        name: Worker name.

    Returns:
        Id of the stored worker.
    """
    worker = await setup.workers.create(
        Worker(owner_id=setup.owner_id, name=name, last_seen_at=datetime.now(UTC))
    )
    return worker.id


async def seed_scorer(setup: Setup, name: str = "relevance") -> PluginVersion:
    """Store a scorer plugin with one code version.

    Args:
        setup: Repository bundle.
        name: Scorer name.

    Returns:
        Stored plugin version.
    """
    blob = await setup.blobs.create(
        Blob(
            owner_id=setup.owner_id,
            sha256=name.ljust(64, "0"),
            size=4,
            media_type="text/x-python",
            data=b"code",
        )
    )
    plugin = await setup.plugins.create(
        Plugin(owner_id=setup.owner_id, kind=PluginKind.SCORER, name=name)
    )
    return await setup.plugins.create_version(
        PluginVersion(
            plugin_id=plugin.id,
            format=PluginFormat.INLINE,
            blob_id=blob.id,
            entrypoint="score",
        )
    )


def score_entity(seed: Seed, parent_id: uuid.UUID, **overrides: object) -> Score:
    """Build a source-arm score job entity.

    Args:
        seed: Seeded rows.
        parent_id: Id of the parent replay.
        **overrides: Field overrides.

    Returns:
        Score entity.
    """
    values: dict[str, object] = {
        "parent_job_id": parent_id,
        "input_session_id": seed.session.id,
        "agent_version_id": seed.version.id,
        "scorer_config": SourceScorerConfig(
            name="conciseness",
            source=SourceRef(module="my_pkg.scorers", attribute="conciseness"),
        ),
        "execution_target": ExecutionTarget.POOL,
        **overrides,
    }
    return Score.model_validate(values)


async def test_create_many_round_trips_score_jobs(setup: Setup) -> None:
    """Store score jobs as one batch and read them back by kind."""
    seed = await seed_rows(setup)
    parent = await setup.jobs.create(job_entity(seed))
    version = await seed_scorer(setup)
    registry = Score(
        parent_job_id=parent.id,
        input_session_id=seed.session.id,
        plugin_version_id=version.id,
        scorer_config=RegistryScorerConfig(name="relevance", version=1),
        execution_target=ExecutionTarget.POOL,
    )
    stored = await setup.jobs.create_many([score_entity(seed, parent.id), registry])
    assert len(stored) == 2
    loaded = await setup.jobs.get(stored[1].id)
    assert isinstance(loaded, Score)
    assert loaded.kind is JobKind.SCORE
    assert loaded.parent_job_id == parent.id
    assert loaded.plugin_version_id == version.id
    assert loaded.agent_version_id is None
    assert loaded.scorer_config == RegistryScorerConfig(name="relevance", version=1)
    assert loaded.created is not None


async def test_create_many_rejects_duplicate_scorers(setup: Setup) -> None:
    """Reject a second score job for the same parent, session, and scorer."""
    seed = await seed_rows(setup)
    parent = await setup.jobs.create(job_entity(seed))
    await setup.jobs.create_many([score_entity(seed, parent.id)])
    with pytest.raises(DuplicateScoreJob):
        await setup.jobs.create_many([score_entity(seed, parent.id)])
    assert len(await setup.jobs.list_children(parent.id)) == 1


async def test_list_and_delete_children(setup: Setup) -> None:
    """List the score jobs of a parent and drop them again."""
    seed = await seed_rows(setup)
    parent = await setup.jobs.create(job_entity(seed))
    other_parent = await setup.jobs.create(job_entity(seed))
    child_id = (await setup.jobs.create_many([score_entity(seed, parent.id)]))[0].id
    await setup.jobs.create_many([score_entity(seed, other_parent.id)])
    children = await setup.jobs.list_children(parent.id)
    assert [child.id for child in children] == [child_id]

    await setup.jobs.delete_children(parent.id)
    assert await setup.jobs.list_children(parent.id) == []
    assert len(await setup.jobs.list_children(other_parent.id)) == 1


async def test_delete_parent_cascades_to_children(setup: Setup) -> None:
    """Drop the score jobs of a deleted parent."""
    seed = await seed_rows(setup)
    parent = await setup.jobs.create(job_entity(seed))
    child = (await setup.jobs.create_many([score_entity(seed, parent.id)]))[0]
    await setup.jobs.delete(parent.id)
    with pytest.raises(JobNotFound):
        await setup.jobs.get(child.id)


async def test_claim_pending_parent_scope(setup: Setup) -> None:
    """Claim only the scoped parent and its own score jobs."""
    seed = await seed_rows(setup)
    parent = await setup.jobs.create(job_entity(seed))
    other_parent = await setup.jobs.create(job_entity(seed))
    child = (await setup.jobs.create_many([score_entity(seed, parent.id)]))[0]
    await setup.jobs.create_many([score_entity(seed, other_parent.id)])
    claimed = await setup.jobs.claim_pending(
        await seed_worker(setup), 10, WorkerScope(job_id=parent.id)
    )
    assert {job.id for job in claimed} == {parent.id, child.id}


async def test_claim_pending_job_scope_covers_the_job_itself(setup: Setup) -> None:
    """Claim the pinned job and its children under one job scope."""
    seed = await seed_rows(setup)
    parent = await setup.jobs.create(
        job_entity(seed, execution_target=ExecutionTarget.ON_DEMAND)
    )
    await setup.jobs.create(job_entity(seed))
    claimed = await setup.jobs.claim_pending(
        await seed_worker(setup), 10, WorkerScope(job_id=parent.id)
    )
    assert [job.id for job in claimed] == [parent.id]


async def test_claim_pending_kind_scope(setup: Setup) -> None:
    """Claim only jobs of the scoped kinds."""
    seed = await seed_rows(setup)
    await setup.jobs.create(job_entity(seed))
    run = await setup.jobs.create(session_run_entity(seed))
    claimed = await setup.jobs.claim_pending(
        await seed_worker(setup), 10, WorkerScope(kinds=[JobKind.SESSION_RUN])
    )
    assert [job.id for job in claimed] == [run.id]


async def test_claim_pending_version_scope_passes_version_less_jobs(
    setup: Setup,
) -> None:
    """Match the scoped versions plus every job without a version."""
    seed = await seed_rows(setup)
    other = await seed_rows(setup, name="other-bot")
    mine = await setup.jobs.create(job_entity(seed))
    await setup.jobs.create(job_entity(other))
    version = await seed_importer(setup)
    payload = await seed_payload(setup)
    unbound = await setup.jobs.create(
        Import(
            plugin_version_id=version.id,
            payload_blob_id=payload.id,
            agent_id=seed.version.agent_id,
            execution_target=ExecutionTarget.POOL,
        )
    )
    claimed = await setup.jobs.claim_pending(
        await seed_worker(setup),
        10,
        WorkerScope(agent_version_ids=[seed.version.id]),
    )
    assert {job.id for job in claimed} == {mine.id, unbound.id}


async def test_list_children_many_groups_by_parent(setup: Setup) -> None:
    """Group the children of a batch of parents by parent id."""
    seed = await seed_rows(setup)
    parent = await setup.jobs.create(job_entity(seed))
    other_parent = await setup.jobs.create(job_entity(seed))
    childless = await setup.jobs.create(job_entity(seed))
    child = (await setup.jobs.create_many([score_entity(seed, parent.id)]))[0]
    other_child = (await setup.jobs.create_many([score_entity(seed, other_parent.id)]))[
        0
    ]
    children = await setup.jobs.list_children_many(
        [parent.id, other_parent.id, childless.id]
    )
    assert set(children) == {parent.id, other_parent.id}
    assert [job.id for job in children[parent.id]] == [child.id]
    assert [job.id for job in children[other_parent.id]] == [other_child.id]
    assert await setup.jobs.list_children_many([]) == {}


async def test_claim_pending_version_scope_matches_registry_score_jobs(
    setup: Setup,
) -> None:
    """Match score jobs without an agent version and skip other versions."""
    seed = await seed_rows(setup)
    other = await seed_rows(setup, name="other-bot")
    parent = await setup.jobs.create(job_entity(seed))
    other_parent = await setup.jobs.create(job_entity(other))
    version = await seed_scorer(setup)
    registry = (
        await setup.jobs.create_many(
            [
                Score(
                    parent_job_id=parent.id,
                    input_session_id=seed.session.id,
                    plugin_version_id=version.id,
                    scorer_config=RegistryScorerConfig(name="relevance", version=1),
                    execution_target=ExecutionTarget.POOL,
                )
            ]
        )
    )[0]
    await setup.jobs.create_many([score_entity(other, other_parent.id)])
    claimed = await setup.jobs.claim_pending(
        await seed_worker(setup), 10, WorkerScope(agent_version_ids=[seed.version.id])
    )
    assert registry.id in {job.id for job in claimed}
    assert other_parent.id not in {job.id for job in claimed}


async def test_requeue_stale_reports_resolved_jobs(setup: Setup) -> None:
    """Report the jobs the staleness rule moved."""
    seed = await seed_rows(setup)
    job = await setup.jobs.create(
        job_entity(seed, execution_target=ExecutionTarget.POOL)
    )
    await setup.jobs.claim_pending(await seed_worker(setup), 1, WorkerScope())
    resolved = await setup.jobs.requeue_stale(
        datetime.now(UTC) + timedelta(seconds=60), 3, WorkerScope()
    )
    assert [entry.id for entry in resolved] == [job.id]
    assert resolved[0].status is JobStatus.PENDING
    assert await setup.jobs.requeue_stale(datetime.now(UTC), 3, WorkerScope()) == []


async def seed_importer(setup: Setup, name: str = "langfuse") -> PluginVersion:
    """Store an importer plugin with one code version.

    Args:
        setup: Repository bundle.
        name: Importer name.

    Returns:
        Stored plugin version.
    """
    blob = await setup.blobs.create(
        Blob(
            owner_id=setup.owner_id,
            sha256=f"importer{name}".ljust(64, "0"),
            size=4,
            media_type="text/x-python",
            data=b"code",
        )
    )
    plugin = await setup.plugins.create(
        Plugin(owner_id=setup.owner_id, kind=PluginKind.IMPORTER, name=name)
    )
    return await setup.plugins.create_version(
        PluginVersion(
            plugin_id=plugin.id,
            format=PluginFormat.INLINE,
            blob_id=blob.id,
            entrypoint="parse",
        )
    )


async def seed_payload(setup: Setup) -> Blob:
    """Store a payload blob for import tests.

    Args:
        setup: Repository bundle.

    Returns:
        Stored blob.
    """
    return await setup.blobs.create(
        Blob(
            owner_id=setup.owner_id,
            sha256="payload".ljust(64, "0"),
            size=7,
            media_type="application/jsonl",
            data=b"payload",
        )
    )


async def test_import_round_trips_all_fields(setup: Setup) -> None:
    """Store an import job with a result and read every field back."""
    version = await seed_importer(setup)
    payload = await seed_payload(setup)
    seed = await seed_rows(setup)
    stored = await setup.jobs.create(
        Import(
            plugin_version_id=version.id,
            payload_blob_id=payload.id,
            agent_id=seed.version.agent_id,
            inputs={"project": "demo"},
            execution_target=ExecutionTarget.POOL,
        )
    )
    assert isinstance(stored, Import)
    assert stored.kind is JobKind.IMPORT
    stored.claim(await seed_worker(setup))
    stored.start()
    stored.complete(
        {
            "created": 2,
            "skipped": 1,
            "failed": 1,
            "failures": [{"line": 7, "external_id": "ext-7", "error": "bad line"}],
        }
    )
    await setup.jobs.update(stored)
    loaded = await setup.jobs.get(stored.id)
    assert isinstance(loaded, Import)
    assert loaded.plugin_version_id == version.id
    assert loaded.payload_blob_id == payload.id
    assert loaded.agent_id == seed.version.agent_id
    assert loaded.inputs == {"project": "demo"}
    assert loaded.agent_version_id is None
    assert loaded.status is JobStatus.COMPLETED
    assert loaded.result == {
        "created": 2,
        "skipped": 1,
        "failed": 1,
        "failures": [{"line": 7, "external_id": "ext-7", "error": "bad line"}],
    }
    assert loaded.created is not None


async def test_import_create_unknown_references(setup: Setup) -> None:
    """Reject an import referencing a missing plugin version or blob."""
    version = await seed_importer(setup)
    payload = await seed_payload(setup)
    seed = await seed_rows(setup)
    with pytest.raises(PluginVersionIdNotFound):
        await setup.jobs.create(
            Import(
                plugin_version_id=uuid.uuid4(),
                payload_blob_id=payload.id,
                agent_id=seed.version.agent_id,
                execution_target=ExecutionTarget.POOL,
            )
        )
    with pytest.raises(BlobNotFound):
        await setup.jobs.create(
            Import(
                plugin_version_id=version.id,
                payload_blob_id=uuid.uuid4(),
                agent_id=seed.version.agent_id,
                execution_target=ExecutionTarget.POOL,
            )
        )


async def test_claim_pending_matches_unbound_import_jobs(setup: Setup) -> None:
    """Claim a pool import job under a version-scoped claim."""
    seed = await seed_rows(setup)
    version = await seed_importer(setup)
    payload = await seed_payload(setup)
    stored = await setup.jobs.create(
        Import(
            plugin_version_id=version.id,
            payload_blob_id=payload.id,
            agent_id=seed.version.agent_id,
            execution_target=ExecutionTarget.POOL,
        )
    )
    worker_id = await seed_worker(setup, name="import-runner")
    claimed = await setup.jobs.claim_pending(
        worker_id, 10, WorkerScope(agent_version_ids=[seed.version.id])
    )
    assert [job.id for job in claimed] == [stored.id]


async def test_heartbeat_many_touches_owned_active_jobs(setup: Setup) -> None:
    """Record one heartbeat per claimed or running job the worker owns."""
    seed = await seed_rows(setup)
    worker_id = await seed_worker(setup)
    other_worker_id = await seed_worker(setup, name="worker-2")
    mine = await setup.jobs.create(session_run_entity(seed))
    foreign = await setup.jobs.create(session_run_entity(seed))
    pending = await setup.jobs.create(session_run_entity(seed))
    await setup.jobs.claim_pending(worker_id, 1, WorkerScope())
    await setup.jobs.claim_pending(other_worker_id, 1, WorkerScope())
    now = datetime.now(UTC)

    reached = await setup.jobs.heartbeat_many(
        worker_id, [mine.id, foreign.id, pending.id, uuid.uuid4()], now
    )

    assert [job.id for job in reached] == [mine.id]
    assert (await setup.jobs.get(mine.id)).heartbeat_at == now
    assert (await setup.jobs.get(pending.id)).heartbeat_at is None


async def test_heartbeat_many_skips_terminal_jobs(setup: Setup) -> None:
    """Leave a terminal job untouched and out of the result."""
    seed = await seed_rows(setup)
    worker_id = await seed_worker(setup)
    stored = await setup.jobs.create(session_run_entity(seed))
    await setup.jobs.claim_pending(worker_id, 1, WorkerScope())
    claimed = await setup.jobs.get(stored.id)
    claimed.cancel()
    await setup.jobs.update(claimed)

    reached = await setup.jobs.heartbeat_many(worker_id, [stored.id], datetime.now(UTC))

    assert reached == []


async def test_heartbeat_many_without_job_ids(setup: Setup) -> None:
    """Report nothing for an empty heartbeat."""
    worker_id = await seed_worker(setup)
    assert await setup.jobs.heartbeat_many(worker_id, [], datetime.now(UTC)) == []
