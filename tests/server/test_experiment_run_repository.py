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
"""Contract tests for experiment run repositories."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import NamedTuple

import pytest
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlmodel import SQLModel

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeCohortRepository,
    FakeExperimentRepository,
    FakeExperimentRunRepository,
    FakeReplayConfigRepository,
    FakeReplayRepository,
    FakeSessionRepository,
    FakeTagRepository,
    db_settings,
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
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
)
from kitaru.server.adapters.db.repositories.experiment_run_repository import (
    SQLExperimentRunRepository,
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
from kitaru.server.adapters.db.repositories.tag_repository import (
    SQLTagRepository,
)
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.cohort_repository import (
    CohortRepository,
)
from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRepository,
)
from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
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
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.experiment_runs import ExperimentRunFilter
from kitaru.server.application.models.replays import ReplayFilter
from kitaru.server.database.service import DatabaseService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionInUse,
    RunSpec,
)
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.experiment import Experiment, ExperimentNotFound
from kitaru.server.domain.experiment_run import (
    ExperimentRun,
    ExperimentRunNotFound,
    ExperimentRunStatus,
)
from kitaru.server.domain.replay import (
    HEARTBEAT_TIMEOUT_ERROR,
    DuplicateReplaySession,
    Replay,
    ReplayStatus,
)
from kitaru.server.domain.replay_config import (
    PassthroughPolicy,
    ReplayConfig,
    ScorerConfig,
    ScoringPolicy,
    SourceRef,
    ToolPolicyConfig,
)
from kitaru.server.domain.session import Session, SessionOrigin, SessionStatus
from kitaru.server.domain.tag import Tag, TagLink, TagResourceType

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
    """Repository bundle for experiment run contract tests."""

    runs: ExperimentRunRepository
    replays: ReplayRepository
    experiments: ExperimentRepository
    configs: ReplayConfigRepository
    cohorts: CohortRepository
    sessions: SessionRepository
    versions: AgentVersionRepository
    agents: AgentRepository
    tags: TagRepository
    owner_id: uuid.UUID


class Seed(NamedTuple):
    """Seeded rows for experiment run contract tests."""

    experiment: Experiment
    version: AgentVersion
    sessions: list[Session]
    config: ReplayConfig


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each experiment run repository implementation plus an owner id."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        tags = FakeTagRepository()
        versions = FakeAgentVersionRepository(agents)
        sessions = FakeSessionRepository(agents, versions, tags)
        cohorts = FakeCohortRepository(sessions, agents, tags)
        configs = FakeReplayConfigRepository()
        experiments = FakeExperimentRepository(cohorts, configs, tags)
        replays = FakeReplayRepository(sessions, versions, configs)
        runs = FakeExperimentRunRepository(experiments, replays, tags)
        yield Setup(
            runs,
            replays,
            experiments,
            configs,
            cohorts,
            sessions,
            versions,
            agents,
            tags,
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
            SQLExperimentRunRepository(session),
            SQLReplayRepository(session),
            SQLExperimentRepository(session),
            SQLReplayConfigRepository(session),
            SQLCohortRepository(session),
            SQLSessionRepository(session),
            SQLAgentVersionRepository(session),
            SQLAgentRepository(session),
            SQLTagRepository(session),
            owner.id,
        )


async def seed_experiment(
    setup: Setup, name: str = "swap-model", session_count: int = 2
) -> Seed:
    """Store an experiment with a runnable version and cohort sessions.

    Args:
        setup: Repository bundle.
        name: Experiment name.
        session_count: Number of cohort sessions.

    Returns:
        Seeded rows.
    """
    agent = await setup.agents.create(
        Agent(owner_id=setup.owner_id, name=f"{name}-bot")
    )
    version = await setup.versions.create(
        AgentVersion(
            owner_id=setup.owner_id,
            agent_id=agent.id,
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        )
    )
    sessions = [
        await setup.sessions.create(
            Session(
                owner_id=setup.owner_id,
                agent_id=agent.id,
                origin=SessionOrigin.RECORDED,
                status=SessionStatus.COMPLETED,
            )
        )
        for _ in range(session_count)
    ]
    cohort = await setup.cohorts.create(
        Cohort(
            owner_id=setup.owner_id,
            name=f"{name}-cohort",
            agent_id=agent.id,
            session_count=session_count,
        ),
        [session.id for session in sessions],
    )
    config = await setup.configs.create(
        ReplayConfig(
            owner_id=setup.owner_id,
            tool_policy=ToolPolicyConfig(default=PassthroughPolicy()),
            scoring_policy=SCORING_POLICY,
        )
    )
    experiment = await setup.experiments.create(
        Experiment(
            owner_id=setup.owner_id,
            name=name,
            cohort_id=cohort.id,
            replay_config_id=config.id,
        )
    )
    return Seed(experiment, version, sessions, config)


def run_entity(setup: Setup, seed: Seed, **overrides: object) -> ExperimentRun:
    """Build an experiment run entity.

    Args:
        setup: Repository bundle.
        seed: Seeded rows.
        **overrides: Field overrides.

    Returns:
        Experiment run entity.
    """
    values: dict[str, object] = {
        "owner_id": setup.owner_id,
        "experiment_id": seed.experiment.id,
        "agent_version_id": seed.version.id,
        **overrides,
    }
    return ExperimentRun.model_validate(values)


def replay_entities(run: ExperimentRun, seed: Seed) -> list[Replay]:
    """Build one replay per seeded session for a run.

    Args:
        run: Experiment run of the replays.
        seed: Seeded rows.

    Returns:
        Replay entities.
    """
    return [
        Replay(
            experiment_run_id=run.id,
            replay_config_id=seed.config.id,
            agent_version_id=seed.version.id,
            original_session_id=session.id,
        )
        for session in seed.sessions
    ]


async def test_create_assigns_number_and_stores_replays(setup: Setup) -> None:
    """Store a run with its replays and assign the first number."""
    seed = await seed_experiment(setup)
    run = run_entity(setup, seed, score_baselines=True)
    created = await setup.runs.create(run, replay_entities(run, seed))
    assert created.number == 1
    assert created.status is ExperimentRunStatus.PENDING
    assert created.agent_version_id == seed.version.id
    assert created.score_baselines is True
    assert created.created is not None
    assert created.updated is not None
    loaded = await setup.runs.get(created.id)
    assert loaded == created

    replays, total = await setup.replays.query(ReplayFilter(experiment_run_id=run.id))
    assert total == 2
    assert {replay.original_session_id for replay in replays} == {
        session.id for session in seed.sessions
    }
    for replay in replays:
        assert replay.status is ReplayStatus.PENDING
        assert replay.replay_config_id == seed.config.id


async def test_create_increments_number_per_experiment(setup: Setup) -> None:
    """Count run numbers per experiment independently."""
    seed = await seed_experiment(setup)
    other_seed = await seed_experiment(setup, name="other")
    first = await setup.runs.create(run_entity(setup, seed), [])
    second = await setup.runs.create(run_entity(setup, seed), [])
    other = await setup.runs.create(run_entity(setup, other_seed), [])
    assert first.number == 1
    assert second.number == 2
    assert other.number == 1


async def test_create_unknown_experiment(setup: Setup) -> None:
    """Raise for an unknown experiment id."""
    seed = await seed_experiment(setup)
    run = run_entity(setup, seed, experiment_id=uuid.uuid4())
    with pytest.raises(
        ExperimentNotFound, match=f"Experiment {run.experiment_id} was not found"
    ):
        await setup.runs.create(run, [])


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown experiment run id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentRunNotFound, match=f"Experiment run {missing_id} was not found"
    ):
        await setup.runs.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query runs by experiment with pagination."""
    seed = await seed_experiment(setup)
    other_seed = await seed_experiment(setup, name="other")
    first = await setup.runs.create(run_entity(setup, seed), [])
    second = await setup.runs.create(run_entity(setup, seed), [])
    await setup.runs.create(run_entity(setup, other_seed), [])

    runs, total = await setup.runs.query(ExperimentRunFilter())
    assert total == 3

    runs, total = await setup.runs.query(
        ExperimentRunFilter(experiment_id=seed.experiment.id)
    )
    assert total == 2
    assert [run.id for run in runs] == [first.id, second.id]

    runs, total = await setup.runs.query(
        ExperimentRunFilter(experiment_id=seed.experiment.id, page=2, page_size=1)
    )
    assert total == 2
    assert [run.id for run in runs] == [second.id]


async def test_query_by_tag(setup: Setup) -> None:
    """Query runs attached to a tag name."""
    seed = await seed_experiment(setup)
    tagged = await setup.runs.create(run_entity(setup, seed), [])
    await setup.runs.create(run_entity(setup, seed), [])
    tag = await setup.tags.create(Tag(owner_id=setup.owner_id, name="prod"))
    await setup.tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.EXPERIMENT_RUN,
            resource_id=tagged.id,
        )
    )

    runs, total = await setup.runs.query(ExperimentRunFilter(tag="prod"))
    assert total == 1
    assert runs[0].id == tagged.id

    runs, total = await setup.runs.query(ExperimentRunFilter(tag="missing"))
    assert total == 0


async def test_query_by_status(setup: Setup) -> None:
    """Query runs by status."""
    seed = await seed_experiment(setup)
    pending = await setup.runs.create(run_entity(setup, seed), [])
    started = await setup.runs.create(run_entity(setup, seed), [])
    started.start()
    await setup.runs.update(started)

    runs, total = await setup.runs.query(
        ExperimentRunFilter(status=ExperimentRunStatus.PENDING)
    )
    assert total == 1
    assert runs[0].id == pending.id

    runs, total = await setup.runs.query(
        ExperimentRunFilter(status=ExperimentRunStatus.RUNNING)
    )
    assert total == 1
    assert runs[0].id == started.id

    runs, total = await setup.runs.query(
        ExperimentRunFilter(
            experiment_id=seed.experiment.id, status=ExperimentRunStatus.RUNNING
        )
    )
    assert total == 1
    runs, total = await setup.runs.query(
        ExperimentRunFilter(status=ExperimentRunStatus.COMPLETED)
    )
    assert total == 0


async def test_has_runs(setup: Setup) -> None:
    """Report run existence per experiment."""
    seed = await seed_experiment(setup)
    assert await setup.runs.has_runs(seed.experiment.id) is False
    await setup.runs.create(run_entity(setup, seed), [])
    assert await setup.runs.has_runs(seed.experiment.id) is True
    assert await setup.runs.has_runs(uuid.uuid4()) is False


async def test_delete_removes_run_replays_and_tag_links(setup: Setup) -> None:
    """Delete a run with its replays and tag links."""
    seed = await seed_experiment(setup)
    run = await setup.runs.create(run_entity(setup, seed), [])
    replays = replay_entities(run, seed)
    for replay in replays:
        await setup.replays.create(replay)
    tag = await setup.tags.create(Tag(owner_id=setup.owner_id, name="prod"))
    await setup.tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.EXPERIMENT_RUN,
            resource_id=run.id,
        )
    )

    await setup.runs.delete(run.id)
    with pytest.raises(
        ExperimentRunNotFound, match=f"Experiment run {run.id} was not found"
    ):
        await setup.runs.get(run.id)
    _, total = await setup.replays.query(ReplayFilter(experiment_run_id=run.id))
    assert total == 0
    _, total = await setup.runs.query(ExperimentRunFilter(tag="prod"))
    assert total == 0
    # The experiment still references the config.
    assert await setup.configs.delete_if_unreferenced(seed.config.id) is False


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown experiment run id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentRunNotFound, match=f"Experiment run {missing_id} was not found"
    ):
        await setup.runs.delete(missing_id)


async def test_count_by_status(setup: Setup) -> None:
    """Count replays by status per run."""
    seed = await seed_experiment(setup)
    run = run_entity(setup, seed)
    created = await setup.runs.create(run, replay_entities(run, seed))
    stale_before = datetime.now(UTC) - timedelta(seconds=60)
    counts = await setup.replays.count_by_status([created.id], stale_before, 3)
    assert counts == {created.id: {ReplayStatus.PENDING: 2}}
    assert await setup.replays.count_by_status([], stale_before, 3) == {}
    assert await setup.replays.count_by_status([uuid.uuid4()], stale_before, 3) == {}


async def test_count_by_status_reports_staleness(setup: Setup) -> None:
    """Report stale claims as pending or timed out without writing."""
    seed = await seed_experiment(setup)
    run = run_entity(setup, seed)
    created = await setup.runs.create(run, replay_entities(run, seed))
    claimed = await setup.replays.claim_pending(created.id, "worker-1", 2)
    fresh = datetime.now(UTC) - timedelta(seconds=60)
    counts = await setup.replays.count_by_status([created.id], fresh, 3)
    assert counts == {created.id: {ReplayStatus.CLAIMED: 2}}
    # A threshold in the future marks both claims stale: one requeues, one
    # at the attempt limit times out.
    exhausted = claimed[0]
    exhausted = exhausted.model_copy(update={"attempt": 3})
    await setup.replays.update(exhausted)
    stale = datetime.now(UTC) + timedelta(seconds=60)
    counts = await setup.replays.count_by_status([created.id], stale, 3)
    assert counts == {created.id: {ReplayStatus.PENDING: 1, ReplayStatus.TIMED_OUT: 1}}
    # Reporting never writes.
    loaded = await setup.replays.get(exhausted.id)
    assert loaded.status is ReplayStatus.CLAIMED


async def test_update_run(setup: Setup) -> None:
    """Persist run status changes and renew the updated timestamp."""
    seed = await seed_experiment(setup)
    created = await setup.runs.create(run_entity(setup, seed), [])
    created.start()
    updated = await setup.runs.update(created)
    assert updated.status is ExperimentRunStatus.RUNNING
    assert updated.started_at is not None
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await setup.runs.get(created.id)
    assert loaded == updated


async def test_update_run_not_found(setup: Setup) -> None:
    """Raise for an unknown experiment run id."""
    seed = await seed_experiment(setup)
    run = run_entity(setup, seed)
    with pytest.raises(
        ExperimentRunNotFound, match=f"Experiment run {run.id} was not found"
    ):
        await setup.runs.update(run)


async def test_duplicate_replay_session_within_run(setup: Setup) -> None:
    """Reject a second replay of the same session within one run."""
    seed = await seed_experiment(setup, session_count=1)
    run = run_entity(setup, seed)
    created = await setup.runs.create(run, replay_entities(run, seed))
    duplicate = Replay(
        experiment_run_id=created.id,
        replay_config_id=seed.config.id,
        agent_version_id=seed.version.id,
        original_session_id=seed.sessions[0].id,
    )
    with pytest.raises(
        DuplicateReplaySession,
        match=f"Session {seed.sessions[0].id} is already replayed in "
        f"experiment run {created.id}",
    ):
        await setup.replays.create(duplicate)
    # The same session replays freely in another run of the experiment.
    second_run = run_entity(setup, seed)
    await setup.runs.create(second_run, replay_entities(second_run, seed))


async def test_agent_version_delete_blocked_by_run(setup: Setup) -> None:
    """Block deleting an agent version referenced by a run."""
    seed = await seed_experiment(setup)
    await setup.runs.create(run_entity(setup, seed), [])
    with pytest.raises(
        AgentVersionInUse,
        match=f"Agent version {seed.version.id} is referenced by",
    ):
        await setup.versions.delete(seed.version.id)


async def test_claim_pending_claims_up_to_limit(setup: Setup) -> None:
    """Claim pending replays in id order up to the limit."""
    seed = await seed_experiment(setup, session_count=3)
    run = run_entity(setup, seed)
    created = await setup.runs.create(run, replay_entities(run, seed))
    other_seed = await seed_experiment(setup, name="other")
    other_run = run_entity(setup, other_seed)
    await setup.runs.create(other_run, replay_entities(other_run, other_seed))

    first = await setup.replays.claim_pending(created.id, "worker-1", 2)
    assert len(first) == 2
    for replay in first:
        assert replay.status is ReplayStatus.CLAIMED
        assert replay.worker_id == "worker-1"
        assert replay.claimed_at is not None
        assert replay.heartbeat_at is not None

    second = await setup.replays.claim_pending(created.id, "worker-2", 5)
    assert len(second) == 1
    assert second[0].worker_id == "worker-2"
    assert {replay.id for replay in first}.isdisjoint({replay.id for replay in second})
    assert await setup.replays.claim_pending(created.id, "worker-2", 5) == []

    # The other run's replays stay untouched.
    others, _ = await setup.replays.query(ReplayFilter(experiment_run_id=other_run.id))
    assert all(replay.status is ReplayStatus.PENDING for replay in others)


async def test_requeue_stale_requeues_and_times_out(setup: Setup) -> None:
    """Requeue stale claims and time them out at the attempt limit."""
    seed = await seed_experiment(setup)
    run = run_entity(setup, seed)
    created = await setup.runs.create(run, replay_entities(run, seed))
    claimed = await setup.replays.claim_pending(created.id, "worker-1", 2)
    exhausted = claimed[0].model_copy(update={"attempt": 3})
    await setup.replays.update(exhausted)

    fresh = datetime.now(UTC) - timedelta(seconds=60)
    await setup.replays.requeue_stale(created.id, fresh, 3)
    loaded = await setup.replays.get(claimed[1].id)
    assert loaded.status is ReplayStatus.CLAIMED

    stale = datetime.now(UTC) + timedelta(seconds=60)
    await setup.replays.requeue_stale(created.id, stale, 3)
    requeued = await setup.replays.get(claimed[1].id)
    assert requeued.status is ReplayStatus.PENDING
    assert requeued.attempt == 2
    assert requeued.worker_id is None
    assert requeued.claimed_at is None
    assert requeued.heartbeat_at is None
    assert requeued.started_at is None
    timed_out = await setup.replays.get(exhausted.id)
    assert timed_out.status is ReplayStatus.TIMED_OUT
    assert timed_out.error == HEARTBEAT_TIMEOUT_ERROR
    assert timed_out.ended_at is not None


async def test_replay_query_by_worker_id(setup: Setup) -> None:
    """Query replays by claiming worker id."""
    seed = await seed_experiment(setup)
    run = run_entity(setup, seed)
    created = await setup.runs.create(run, replay_entities(run, seed))
    first = await setup.replays.claim_pending(created.id, "worker-1", 1)
    await setup.replays.claim_pending(created.id, "worker-2", 1)

    replays, total = await setup.replays.query(ReplayFilter(worker_id="worker-1"))
    assert total == 1
    assert replays[0].id == first[0].id

    replays, total = await setup.replays.query(
        ReplayFilter(experiment_run_id=created.id, worker_id="worker-3")
    )
    assert total == 0


async def test_replay_query_status_projects_staleness(setup: Setup) -> None:
    """Match a status filter with the staleness context on the projected status."""
    seed = await seed_experiment(setup)
    run = run_entity(setup, seed)
    created = await setup.runs.create(run, replay_entities(run, seed))
    claimed = await setup.replays.claim_pending(created.id, "worker-1", 2)
    exhausted = claimed[0].model_copy(update={"attempt": 3})
    await setup.replays.update(exhausted)

    fresh = datetime.now(UTC) - timedelta(seconds=60)
    _, total = await setup.replays.query(
        ReplayFilter(status=ReplayStatus.CLAIMED, stale_before=fresh, max_attempts=3)
    )
    assert total == 2

    # A threshold in the future marks both claims stale: one matches
    # pending, one at the attempt limit matches timed out.
    stale = datetime.now(UTC) + timedelta(seconds=60)
    replays, total = await setup.replays.query(
        ReplayFilter(status=ReplayStatus.PENDING, stale_before=stale, max_attempts=3)
    )
    assert total == 1
    assert replays[0].id == claimed[1].id
    replays, total = await setup.replays.query(
        ReplayFilter(status=ReplayStatus.TIMED_OUT, stale_before=stale, max_attempts=3)
    )
    assert total == 1
    assert replays[0].id == exhausted.id
    _, total = await setup.replays.query(
        ReplayFilter(status=ReplayStatus.CLAIMED, stale_before=stale, max_attempts=3)
    )
    assert total == 0

    # Without the staleness context the stored status matches.
    _, total = await setup.replays.query(ReplayFilter(status=ReplayStatus.CLAIMED))
    assert total == 2
    # Filtering never writes.
    stored = await setup.replays.get(claimed[1].id)
    assert stored.status is ReplayStatus.CLAIMED


async def test_concurrent_claims_do_not_double_claim() -> None:
    """Skip rows locked by a concurrent claim instead of double-claiming."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    settings = db_settings()
    await DatabaseService.create_db(settings, force_drop=True)
    engine = create_async_engine(DatabaseService.generate_database_uri(settings))
    try:
        async with engine.begin() as connection:
            await connection.run_sync(SQLModel.metadata.create_all)
        factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        async with factory() as session:
            accounts = SQLAccountRepository(session)
            owner = await accounts.create(Account(name="owner"))
            setup = Setup(
                SQLExperimentRunRepository(session),
                SQLReplayRepository(session),
                SQLExperimentRepository(session),
                SQLReplayConfigRepository(session),
                SQLCohortRepository(session),
                SQLSessionRepository(session),
                SQLAgentVersionRepository(session),
                SQLAgentRepository(session),
                SQLTagRepository(session),
                owner.id,
            )
            seed = await seed_experiment(setup, session_count=4)
            run = run_entity(setup, seed)
            created = await setup.runs.create(run, replay_entities(run, seed))
            await session.commit()
        # Two open transactions claim concurrently. FOR UPDATE SKIP LOCKED
        # makes the second claim skip the rows the first one still locks.
        async with factory() as first_session, factory() as second_session:
            first_claimed = await SQLReplayRepository(first_session).claim_pending(
                created.id, "worker-1", 2
            )
            second_claimed = await SQLReplayRepository(second_session).claim_pending(
                created.id, "worker-2", 4
            )
            await first_session.commit()
            await second_session.commit()
        assert len(first_claimed) == 2
        assert len(second_claimed) == 2
        first_ids = {replay.id for replay in first_claimed}
        second_ids = {replay.id for replay in second_claimed}
        assert first_ids.isdisjoint(second_ids)
        async with factory() as session:
            replays, total = await SQLReplayRepository(session).query(
                ReplayFilter(experiment_run_id=created.id)
            )
        assert total == 4
        assert all(replay.status is ReplayStatus.CLAIMED for replay in replays)
    finally:
        await engine.dispose()
