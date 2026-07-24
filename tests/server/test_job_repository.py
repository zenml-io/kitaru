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
from typing import NamedTuple

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeJobRepository,
    FakeReplayConfigRepository,
    FakeSessionRepository,
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
from kitaru.server.adapters.db.repositories.job_repository import (
    SQLJobRepository,
)
from kitaru.server.adapters.db.repositories.replay_config_repository import (
    SQLReplayConfigRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.job_repository import (
    JobRepository,
)
from kitaru.server.application.interfaces.replay_config_repository import (
    ReplayConfigRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
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
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.job import (
    JobKind,
    JobNotFound,
    JobStatus,
    Replay,
    SessionRun,
)
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    ReplayConfig,
    ReplayConfigNotFound,
    ScorerConfig,
    ScoringPolicy,
    ScoringResult,
    SourceRef,
    ToolPolicyConfig,
)
from kitaru.server.domain.session import (
    Session,
    SessionInUse,
    SessionNotFound,
    SessionOrigin,
    SessionStatus,
)

SCORING_POLICY = ScoringPolicy(
    scorers=[
        ScorerConfig(
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
        jobs = FakeJobRepository(sessions, versions, configs)
        yield Setup(jobs, configs, sessions, versions, agents, uuid.uuid4())
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


def job_entity(seed: Seed, **overrides: object) -> Replay:
    """Build a standalone job entity.

    Args:
        seed: Seeded rows.
        **overrides: Field overrides.

    Returns:
        Replay entity.
    """
    values: dict[str, object] = {
        "replay_config_id": seed.config.id,
        "agent_version_id": seed.version.id,
        "original_session_id": seed.session.id,
        **overrides,
    }
    return Replay.model_validate(values)


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
    assert isinstance(loaded, Replay)
    assert loaded.kind is JobKind.REPLAY
    assert loaded.experiment_run_id is None
    assert loaded.replay_config_id == seed.config.id
    assert loaded.agent_version_id == seed.version.id
    assert loaded.original_session_id == seed.session.id
    assert loaded.result_session_id is None
    assert loaded.status is JobStatus.PENDING
    assert loaded.attempt == 1
    assert loaded.passed is None
    assert loaded.score is None
    assert loaded.scores is None
    assert loaded.diff is None


async def test_create_unknown_references(setup: Setup) -> None:
    """Raise for unknown config, version, and session ids."""
    seed = await seed_rows(setup)
    missing_id = uuid.uuid4()
    with pytest.raises(
        ReplayConfigNotFound, match=f"Replay config {missing_id} was not found"
    ):
        await setup.jobs.create(job_entity(seed, replay_config_id=missing_id))
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await setup.jobs.create(job_entity(seed, agent_version_id=missing_id))
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await setup.jobs.create(job_entity(seed, original_session_id=missing_id))


async def test_standalone_jobs_repeat_freely(setup: Setup) -> None:
    """Replay the same session standalone any number of times."""
    seed = await seed_rows(setup)
    first = await setup.jobs.create(job_entity(seed))
    second = await setup.jobs.create(job_entity(seed))
    assert isinstance(first, Replay)
    assert isinstance(second, Replay)
    assert first.original_session_id == second.original_session_id
    _, total = await setup.jobs.query(JobFilter(original_session_id=seed.session.id))
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

    jobs, total = await setup.jobs.query(JobFilter(original_session_id=seed.session.id))
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


async def test_config_delete_if_unreferenced_by_job(setup: Setup) -> None:
    """Keep a config row while a job references it."""
    seed = await seed_rows(setup)
    await setup.jobs.create(job_entity(seed))
    assert await setup.configs.delete_if_unreferenced(seed.config.id) is False
    loaded = await setup.configs.get(seed.config.id)
    assert loaded.id == seed.config.id


async def test_delete_removes_job(setup: Setup) -> None:
    """Delete a job and free its config for deletion."""
    seed = await seed_rows(setup)
    created = await setup.jobs.create(job_entity(seed))
    await setup.jobs.delete(created.id)
    with pytest.raises(JobNotFound, match=f"Job {created.id} was not found"):
        await setup.jobs.get(created.id)
    assert await setup.configs.delete_if_unreferenced(seed.config.id) is True


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown job id."""
    missing_id = uuid.uuid4()
    with pytest.raises(JobNotFound, match=f"Job {missing_id} was not found"):
        await setup.jobs.delete(missing_id)


async def test_update_round_trips_runner_fields(setup: Setup) -> None:
    """Persist runner-side field changes and renew the updated timestamp."""
    seed = await seed_rows(setup)
    created = await setup.jobs.create(job_entity(seed))
    assert isinstance(created, Replay)
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
    created.complete(
        ScoringResult(passed=True, score=0.8, scores={"conciseness": 0.8}),
        diff={"cost_delta": -0.1},
    )
    updated = await setup.jobs.update(created)
    assert isinstance(updated, Replay)
    assert updated.status is JobStatus.COMPLETED
    assert updated.result_session_id == result.id
    assert updated.started_at is not None
    assert updated.ended_at is not None
    assert updated.passed is True
    assert updated.score == 0.8
    assert updated.scores == {"conciseness": 0.8}
    assert updated.diff == {"cost_delta": -0.1}
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
    replay_job = await setup.jobs.create(job_entity(seed))
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
    _, total = await setup.jobs.query(
        JobFilter(execution_target=ExecutionTarget.ON_DEMAND)
    )
    assert total == 0
