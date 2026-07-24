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
"""Contract tests for agent repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeCohortRepository,
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
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.cohort_repository import (
    CohortRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.models.agents import AgentFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import (
    Agent,
    AgentInUse,
    AgentNotFound,
    DuplicateAgentName,
)
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.session import (
    Session,
    SessionOrigin,
    SessionStatus,
)

Setup = tuple[AgentRepository, AgentVersionRepository, uuid.UUID, uuid.UUID]
DeleteSetup = tuple[AgentRepository, SessionRepository, CohortRepository, uuid.UUID]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each agent repository implementation plus two owner ids."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        versions = FakeAgentVersionRepository(agents)
        yield agents, versions, uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning accounts first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        other_owner = await accounts.create(Account(name="other-owner"))
        yield (
            SQLAgentRepository(session),
            SQLAgentVersionRepository(session),
            owner.id,
            other_owner.id,
        )


@pytest.fixture(params=["fake", "postgres"])
async def delete_setup(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[DeleteSetup, None]:
    """Provide each agent repository implementation plus referencing repositories."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        sessions = FakeSessionRepository(agents)
        cohorts = FakeCohortRepository(sessions, agents)
        yield agents, sessions, cohorts, uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning account first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield (
            SQLAgentRepository(session),
            SQLSessionRepository(session),
            SQLCohortRepository(session),
            owner.id,
        )


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new agent with both timestamps set."""
    repository, _, owner_id, _ = setup
    agent = await repository.create(
        Agent(owner_id=owner_id, name="support-bot", description="Answers tickets")
    )
    assert agent.name == "support-bot"
    assert agent.owner_id == owner_id
    assert agent.description == "Answers tickets"
    assert agent.created is not None
    assert agent.updated is not None


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second agent with the same name."""
    repository, _, owner_id, other_owner_id = setup
    await repository.create(Agent(owner_id=owner_id, name="support-bot"))
    with pytest.raises(
        DuplicateAgentName, match="Agent name 'support-bot' is already registered"
    ):
        await repository.create(Agent(owner_id=other_owner_id, name="support-bot"))


async def test_create_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate name failure."""
    repository, _, owner_id, _ = setup
    await repository.create(Agent(owner_id=owner_id, name="support-bot"))
    with pytest.raises(DuplicateAgentName):
        await repository.create(Agent(owner_id=owner_id, name="support-bot"))
    agent = await repository.create(Agent(owner_id=owner_id, name="triage-bot"))
    assert agent.name == "triage-bot"


async def test_get(setup: Setup) -> None:
    """Load a stored agent by id."""
    repository, _, owner_id, _ = setup
    created = await repository.create(Agent(owner_id=owner_id, name="support-bot"))
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown agent id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query agents with filters and pagination."""
    repository, _, owner_id, other_owner_id = setup
    support = await repository.create(Agent(owner_id=owner_id, name="support-bot"))
    await repository.create(Agent(owner_id=owner_id, name="triage-bot"))
    coder = await repository.create(Agent(owner_id=other_owner_id, name="coder"))

    agents, total = await repository.query(AgentFilter())
    assert total == 3
    assert [agent.name for agent in agents] == ["support-bot", "triage-bot", "coder"]

    agents, total = await repository.query(AgentFilter(name="support-bot"))
    assert total == 1
    assert agents[0] == support

    agents, total = await repository.query(AgentFilter(owner_id=other_owner_id))
    assert total == 1
    assert agents[0] == coder

    agents, total = await repository.query(AgentFilter(page=2, page_size=2))
    assert total == 3
    assert [agent.name for agent in agents] == ["coder"]

    agents, total = await repository.query(AgentFilter(name="missing"))
    assert total == 0
    assert agents == []


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, _, owner_id, _ = setup
    created = await repository.create(Agent(owner_id=owner_id, name="support-bot"))
    created.update_name("triage-bot")
    created.update_description("Sorts tickets")
    updated = await repository.update(created)
    assert updated.name == "triage-bot"
    assert updated.description == "Sorts tickets"
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown agent id."""
    repository, _, owner_id, _ = setup
    agent = Agent(owner_id=owner_id, name="support-bot")
    with pytest.raises(AgentNotFound, match=f"Agent {agent.id} was not found"):
        await repository.update(agent)


async def test_update_duplicate_name(setup: Setup) -> None:
    """Reject renaming an agent to a registered name."""
    repository, _, owner_id, _ = setup
    await repository.create(Agent(owner_id=owner_id, name="support-bot"))
    triage = await repository.create(Agent(owner_id=owner_id, name="triage-bot"))
    triage.name = "support-bot"
    with pytest.raises(
        DuplicateAgentName, match="Agent name 'support-bot' is already registered"
    ):
        await repository.update(triage)


async def test_delete(setup: Setup) -> None:
    """Delete a stored agent."""
    repository, _, owner_id, _ = setup
    created = await repository.create(Agent(owner_id=owner_id, name="support-bot"))
    await repository.delete(created.id)
    with pytest.raises(AgentNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown agent id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_delete_with_versions(setup: Setup) -> None:
    """Reject deleting an agent that still has versions."""
    repository, version_repository, owner_id, _ = setup
    created = await repository.create(Agent(owner_id=owner_id, name="support-bot"))
    version = await version_repository.create(
        AgentVersion(owner_id=owner_id, agent_id=created.id, version="v1")
    )
    with pytest.raises(
        AgentInUse, match=f"Agent {created.id} is referenced by agent versions"
    ):
        await repository.delete(created.id)

    await version_repository.delete(version.id)
    await repository.delete(created.id)
    with pytest.raises(AgentNotFound):
        await repository.get(created.id)


async def test_delete_with_sessions(delete_setup: DeleteSetup) -> None:
    """Reject deleting an agent that still has sessions."""
    repository, session_repository, _, owner_id = delete_setup
    created = await repository.create(Agent(owner_id=owner_id, name="support-bot"))
    session = await session_repository.create(
        Session(
            owner_id=owner_id,
            agent_id=created.id,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )
    with pytest.raises(
        AgentInUse, match=f"Agent {created.id} is referenced by sessions"
    ):
        await repository.delete(created.id)

    await session_repository.delete(session.id)
    await repository.delete(created.id)
    with pytest.raises(AgentNotFound):
        await repository.get(created.id)


async def test_delete_with_cohorts(delete_setup: DeleteSetup) -> None:
    """Reject deleting an agent that is still referenced by cohorts."""
    repository, _, cohort_repository, owner_id = delete_setup
    created = await repository.create(Agent(owner_id=owner_id, name="support-bot"))
    cohort = await cohort_repository.create(
        Cohort(
            owner_id=owner_id,
            agent_id=created.id,
            name="baseline",
            session_count=0,
        ),
        [],
    )
    with pytest.raises(
        AgentInUse, match=f"Agent {created.id} is referenced by cohorts"
    ):
        await repository.delete(created.id)

    await cohort_repository.delete(cohort.id)
    await repository.delete(created.id)
    with pytest.raises(AgentNotFound):
        await repository.get(created.id)
