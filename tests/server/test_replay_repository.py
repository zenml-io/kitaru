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
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionInUse,
    AgentVersionNotFound,
    RunSpec,
)
from kitaru.server.domain.replay import Replay, ReplayNotFound, ReplayStatus
from kitaru.server.domain.replay_config import (
    HistoryPolicy,
    ReplayConfig,
    ReplayConfigNotFound,
    ScorerConfig,
    ScoringPolicy,
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
    """Repository bundle for replay contract tests."""

    replays: ReplayRepository
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


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each replay repository implementation plus an owner id."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        versions = FakeAgentVersionRepository(agents)
        sessions = FakeSessionRepository(agents, versions)
        configs = FakeReplayConfigRepository()
        replays = FakeReplayRepository(sessions, versions, configs)
        yield Setup(replays, configs, sessions, versions, agents, uuid.uuid4())
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


def replay_entity(seed: Seed, **overrides: object) -> Replay:
    """Build a standalone replay entity.

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


async def test_create_round_trips_all_fields(setup: Setup) -> None:
    """Store a replay and round-trip every field."""
    seed = await seed_rows(setup)
    created = await setup.replays.create(replay_entity(seed))
    assert created.created is not None
    assert created.updated is not None
    loaded = await setup.replays.get(created.id)
    assert loaded == created
    assert loaded.experiment_run_id is None
    assert loaded.replay_config_id == seed.config.id
    assert loaded.agent_version_id == seed.version.id
    assert loaded.original_session_id == seed.session.id
    assert loaded.result_session_id is None
    assert loaded.status is ReplayStatus.PENDING
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
        await setup.replays.create(replay_entity(seed, replay_config_id=missing_id))
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await setup.replays.create(replay_entity(seed, agent_version_id=missing_id))
    with pytest.raises(SessionNotFound, match=f"Session {missing_id} was not found"):
        await setup.replays.create(replay_entity(seed, original_session_id=missing_id))


async def test_standalone_replays_repeat_freely(setup: Setup) -> None:
    """Replay the same session standalone any number of times."""
    seed = await seed_rows(setup)
    first = await setup.replays.create(replay_entity(seed))
    second = await setup.replays.create(replay_entity(seed))
    assert first.original_session_id == second.original_session_id
    _, total = await setup.replays.query(
        ReplayFilter(original_session_id=seed.session.id)
    )
    assert total == 2


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown replay id."""
    missing_id = uuid.uuid4()
    with pytest.raises(ReplayNotFound, match=f"Replay {missing_id} was not found"):
        await setup.replays.get(missing_id)


async def test_query_filters(setup: Setup) -> None:
    """Query replays by session, status, and standalone."""
    seed = await seed_rows(setup)
    other_seed = await seed_rows(setup, name="triage-bot")
    first = await setup.replays.create(replay_entity(seed))
    await setup.replays.create(replay_entity(other_seed))

    replays, total = await setup.replays.query(ReplayFilter())
    assert total == 2

    replays, total = await setup.replays.query(
        ReplayFilter(original_session_id=seed.session.id)
    )
    assert total == 1
    assert replays[0].id == first.id

    replays, total = await setup.replays.query(
        ReplayFilter(status=ReplayStatus.PENDING)
    )
    assert total == 2
    replays, total = await setup.replays.query(
        ReplayFilter(status=ReplayStatus.RUNNING)
    )
    assert total == 0

    replays, total = await setup.replays.query(ReplayFilter(standalone=True))
    assert total == 2
    replays, total = await setup.replays.query(ReplayFilter(standalone=False))
    assert total == 0

    replays, total = await setup.replays.query(ReplayFilter(page=2, page_size=1))
    assert total == 2
    assert len(replays) == 1


async def test_references_agent_version(setup: Setup) -> None:
    """Report whether a replay references an agent version."""
    seed = await seed_rows(setup)
    assert await setup.replays.references_agent_version(seed.version.id) is False
    await setup.replays.create(replay_entity(seed))
    assert await setup.replays.references_agent_version(seed.version.id) is True
    assert await setup.replays.references_agent_version(uuid.uuid4()) is False


async def test_session_delete_blocked_by_replay(setup: Setup) -> None:
    """Block deleting a session that a replay references."""
    seed = await seed_rows(setup)
    await setup.replays.create(replay_entity(seed))
    with pytest.raises(
        SessionInUse, match=f"Session {seed.session.id} is referenced by replays"
    ):
        await setup.sessions.delete(seed.session.id)
    # The failed delete leaves the repository usable.
    loaded = await setup.sessions.get(seed.session.id)
    assert loaded.id == seed.session.id


async def test_result_session_delete_blocked_by_replay(setup: Setup) -> None:
    """Block deleting a session that a replay links as its result."""
    seed = await seed_rows(setup)
    result = await setup.sessions.create(
        Session(
            owner_id=setup.owner_id,
            agent_id=seed.session.agent_id,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )
    await setup.replays.create(replay_entity(seed, result_session_id=result.id))
    with pytest.raises(
        SessionInUse, match=f"Session {result.id} is referenced by replays"
    ):
        await setup.sessions.delete(result.id)


async def test_agent_version_delete_blocked_by_replay(setup: Setup) -> None:
    """Block deleting an agent version that a replay references."""
    seed = await seed_rows(setup)
    await setup.replays.create(replay_entity(seed))
    with pytest.raises(
        AgentVersionInUse,
        match=f"Agent version {seed.version.id} is referenced by replays",
    ):
        await setup.versions.delete(seed.version.id)


async def test_config_delete_if_unreferenced_by_replay(setup: Setup) -> None:
    """Keep a config row while a replay references it."""
    seed = await seed_rows(setup)
    await setup.replays.create(replay_entity(seed))
    assert await setup.configs.delete_if_unreferenced(seed.config.id) is False
    loaded = await setup.configs.get(seed.config.id)
    assert loaded.id == seed.config.id
