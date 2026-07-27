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
"""Contract tests for replay repositories."""

import uuid
from collections.abc import AsyncGenerator
from typing import NamedTuple

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeJobRepository,
    FakeReplayConfigRepository,
    FakeReplayRepository,
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
from kitaru.server.adapters.db.repositories.replay_repository import (
    SQLReplayRepository,
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
from kitaru.server.application.interfaces.replay_repository import (
    ReplayRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.models.replays import ReplayFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion, RunSpec
from kitaru.server.domain.execution import ExecutionTarget
from kitaru.server.domain.job import JobNotFound, ReplayJob
from kitaru.server.domain.replay import (
    DuplicateReplayJob,
    Replay,
    ReplayJobNotFound,
    ReplayNotFound,
)
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    ReplayConfig,
    ReplayConfigNotFound,
    ScoringPolicy,
    ScoringResult,
    SourceRef,
    SourceScorerConfig,
    ToolPolicyConfig,
)
from kitaru.server.domain.session import (
    Session,
    SessionNotFound,
    SessionOrigin,
    SessionStatus,
)

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
    """Repository bundle for replay contract tests."""

    replays: ReplayRepository
    jobs: JobRepository
    configs: ReplayConfigRepository
    sessions: SessionRepository
    versions: AgentVersionRepository
    agents: AgentRepository
    owner_id: uuid.UUID


class Seed(NamedTuple):
    """Seeded rows for replay contract tests."""

    session: Session
    version: AgentVersion
    config: ReplayConfig
    job: ReplayJob


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each replay repository implementation plus an owner id."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        versions = FakeAgentVersionRepository(agents)
        sessions = FakeSessionRepository(agents, versions)
        configs = FakeReplayConfigRepository()
        jobs = FakeJobRepository(sessions, versions)
        replays = FakeReplayRepository(jobs, configs, sessions)
        yield Setup(replays, jobs, configs, sessions, versions, agents, uuid.uuid4())
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning account first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield Setup(
            SQLReplayRepository(session),
            SQLJobRepository(session),
            SQLReplayConfigRepository(session),
            SQLSessionRepository(session),
            SQLAgentVersionRepository(session),
            SQLAgentRepository(session),
            owner.id,
        )


async def seed_rows(setup: Setup, name: str = "support-bot") -> Seed:
    """Store an agent, a runnable version, a session, a config, and a job.

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
    job = await setup.jobs.create(
        ReplayJob(
            agent_version_id=version.id,
            input_session_id=session.id,
            execution_target=ExecutionTarget.POOL,
        )
    )
    assert isinstance(job, ReplayJob)
    return Seed(session, version, config, job)


def replay_entity(setup: Setup, seed: Seed, **overrides: object) -> Replay:
    """Build a replay entity.

    Args:
        setup: Repository bundle.
        seed: Seeded rows.
        **overrides: Field overrides.

    Returns:
        Replay entity.
    """
    values: dict[str, object] = {
        "owner_id": setup.owner_id,
        "job_id": seed.job.id,
        "replay_config_id": seed.config.id,
        "input_session_id": seed.session.id,
        **overrides,
    }
    return Replay.model_validate(values)


async def test_create_round_trips_all_fields(setup: Setup) -> None:
    """Store a replay and round-trip every field."""
    seed = await seed_rows(setup)
    created = await setup.replays.create(replay_entity(setup, seed))
    assert created.created is not None
    assert created.updated is not None
    loaded = await setup.replays.get(created.id)
    assert loaded == created
    assert loaded.job_id == seed.job.id
    assert loaded.experiment_run_id is None
    assert loaded.replay_config_id == seed.config.id
    assert loaded.input_session_id == seed.session.id
    assert loaded.passed is None
    assert loaded.score is None
    assert loaded.scores is None
    assert loaded.diff is None
    assert loaded.error is None


async def test_create_unknown_references(setup: Setup) -> None:
    """Raise for unknown job, config, and session ids."""
    seed = await seed_rows(setup)
    missing_id = uuid.uuid4()
    with pytest.raises(JobNotFound, match=f"Job {missing_id} was not found"):
        await setup.replays.create(replay_entity(setup, seed, job_id=missing_id))
    with pytest.raises(
        ReplayConfigNotFound, match=f"Replay config {missing_id} was not found"
    ):
        await setup.replays.create(
            replay_entity(setup, seed, replay_config_id=missing_id)
        )
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await setup.replays.create(
            replay_entity(setup, seed, input_session_id=missing_id)
        )


async def test_create_rejects_a_second_replay_per_job(setup: Setup) -> None:
    """Reject a second replay bound to the same job."""
    seed = await seed_rows(setup)
    await setup.replays.create(replay_entity(setup, seed))
    with pytest.raises(
        DuplicateReplayJob, match=f"Job {seed.job.id} already has a replay"
    ):
        await setup.replays.create(replay_entity(setup, seed))


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown replay id."""
    missing_id = uuid.uuid4()
    with pytest.raises(ReplayNotFound, match=f"Replay {missing_id} was not found"):
        await setup.replays.get(missing_id)


async def test_get_by_job(setup: Setup) -> None:
    """Load a replay through its job id."""
    seed = await seed_rows(setup)
    created = await setup.replays.create(replay_entity(setup, seed))
    assert (await setup.replays.get_by_job(seed.job.id)).id == created.id
    missing_id = uuid.uuid4()
    with pytest.raises(ReplayJobNotFound, match=f"Job {missing_id} has no replay"):
        await setup.replays.get_by_job(missing_id)


async def test_get_many_by_jobs(setup: Setup) -> None:
    """Load replays by job id, omitting jobs without one."""
    seed = await seed_rows(setup)
    other = await seed_rows(setup, name="triage-bot")
    created = await setup.replays.create(replay_entity(setup, seed))
    replays = await setup.replays.get_many_by_jobs([seed.job.id, other.job.id])
    assert replays == {seed.job.id: created}
    assert await setup.replays.get_many_by_jobs([]) == {}


async def test_query_filters(setup: Setup) -> None:
    """Query replays by input session and scoring outcome."""
    seed = await seed_rows(setup)
    other = await seed_rows(setup, name="triage-bot")
    first = await setup.replays.create(replay_entity(setup, seed))
    await setup.replays.create(replay_entity(setup, other, job_id=other.job.id))

    replays, total = await setup.replays.query(ReplayFilter())
    assert total == 2

    replays, total = await setup.replays.query(
        ReplayFilter(input_session_id=seed.session.id)
    )
    assert total == 1
    assert replays[0].id == first.id

    replays, total = await setup.replays.query(ReplayFilter(passed=True))
    assert total == 0

    first.complete(
        ScoringResult(passed=True, score=0.8, scores={"conciseness": 0.8}), None
    )
    await setup.replays.update(first)
    replays, total = await setup.replays.query(ReplayFilter(passed=True))
    assert total == 1
    assert replays[0].id == first.id

    replays, total = await setup.replays.query(ReplayFilter(page=2, page_size=1))
    assert total == 2
    assert len(replays) == 1


async def test_update_round_trips_the_verdict(setup: Setup) -> None:
    """Persist the settled verdict and renew the updated timestamp."""
    seed = await seed_rows(setup)
    created = await setup.replays.create(replay_entity(setup, seed))
    created.complete(
        ScoringResult(passed=True, score=0.8, scores={"conciseness": 0.8}),
        {"cost_delta": -0.1},
    )
    updated = await setup.replays.update(created)
    assert updated.passed is True
    assert updated.score == 0.8
    assert updated.scores == {"conciseness": 0.8}
    assert updated.diff == {"cost_delta": -0.1}
    assert created.updated is not None
    assert updated.updated is not None
    assert updated.updated > created.updated
    assert await setup.replays.get(created.id) == updated


async def test_update_round_trips_the_error(setup: Setup) -> None:
    """Persist a failed replay without a verdict."""
    seed = await seed_rows(setup)
    created = await setup.replays.create(replay_entity(setup, seed))
    created.fail("Scorer 'conciseness' did not complete")
    updated = await setup.replays.update(created)
    assert updated.error == "Scorer 'conciseness' did not complete"
    assert updated.passed is None


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown replay id."""
    seed = await seed_rows(setup)
    with pytest.raises(ReplayNotFound):
        await setup.replays.update(replay_entity(setup, seed))


async def test_delete_job_cascades_to_replay(setup: Setup) -> None:
    """Drop the replay of a deleted job."""
    seed = await seed_rows(setup)
    created = await setup.replays.create(replay_entity(setup, seed))
    await setup.jobs.delete(seed.job.id)
    if isinstance(setup.replays, FakeReplayRepository):
        setup.replays.remove_for_jobs({seed.job.id})
    with pytest.raises(ReplayNotFound):
        await setup.replays.get(created.id)


async def test_config_delete_if_unreferenced_by_replay(setup: Setup) -> None:
    """Keep a config row while a replay references it."""
    seed = await seed_rows(setup)
    await setup.replays.create(replay_entity(setup, seed))
    assert await setup.configs.delete_if_unreferenced(seed.config.id) is False
    assert (await setup.configs.get(seed.config.id)).id == seed.config.id
