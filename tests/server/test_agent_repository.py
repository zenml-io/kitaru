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
from collections.abc import AsyncGenerator, Callable

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.models.agent import AgentFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound, DuplicateAgentName
from kitaru.server.domain.agent_version import AgentVersion, AgentVersionNotFound
from kitaru.server.filtering import FilterCondition

Setup = tuple[AgentRepository, uuid.UUID, Callable[[], AgentVersionRepository]]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each agent repository implementation, an owner id, and a
    version repository factory sharing the same backing store."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        yield agents, uuid.uuid4(), lambda: FakeAgentVersionRepository(agents)
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield (
            SQLAgentRepository(session),
            owner.id,
            lambda: SQLAgentVersionRepository(session),
        )


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new agent with both timestamps set."""
    repository, owner_id, _ = setup
    agent = await repository.create(
        Agent(owner_id=owner_id, name="assistant", description="Helps")
    )
    assert agent.name == "assistant"
    assert agent.owner_id == owner_id
    assert agent.description == "Helps"
    assert agent.latest_version == 0
    assert agent.created is not None
    assert agent.updated is not None


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second agent with the same name."""
    repository, owner_id, _ = setup
    await repository.create(Agent(owner_id=owner_id, name="assistant"))
    with pytest.raises(
        DuplicateAgentName, match="Agent name 'assistant' is already registered"
    ):
        await repository.create(Agent(owner_id=owner_id, name="assistant"))


async def test_create_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate name failure."""
    repository, owner_id, _ = setup
    await repository.create(Agent(owner_id=owner_id, name="assistant"))
    with pytest.raises(DuplicateAgentName):
        await repository.create(Agent(owner_id=owner_id, name="assistant"))
    agent = await repository.create(Agent(owner_id=owner_id, name="reviewer"))
    assert agent.name == "reviewer"


async def test_get(setup: Setup) -> None:
    """Load a stored agent by id."""
    repository, owner_id, _ = setup
    created = await repository.create(Agent(owner_id=owner_id, name="assistant"))
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown agent id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query agents newest-first with filters."""
    repository, owner_id, _ = setup
    assistant = await repository.create(Agent(owner_id=owner_id, name="assistant"))
    await repository.create(Agent(owner_id=owner_id, name="reviewer"))
    triager = await repository.create(Agent(owner_id=owner_id, name="triager"))

    agents, next_cursor = await repository.query(AgentFilter())
    assert next_cursor is None
    assert [agent.name for agent in agents] == ["triager", "reviewer", "assistant"]

    agents, next_cursor = await repository.query(
        AgentFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="assistant")
        )
    )
    assert next_cursor is None
    assert agents[0] == assistant

    agents, next_cursor = await repository.query(
        AgentFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="missing")
        )
    )
    assert next_cursor is None
    assert agents == []

    assert triager.name == "triager"


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, _ = setup
    created = [
        await repository.create(Agent(owner_id=owner_id, name=f"agent-{i}"))
        for i in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[Agent] = []
    cursor = None
    while True:
        agents, next_cursor = await repository.query(AgentFilter(cursor=cursor, size=2))
        collected.extend(agents)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({agent.id for agent in collected}) == 5


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id, _ = setup
    created = await repository.create(
        Agent(owner_id=owner_id, name="assistant", description="Helps")
    )
    created.update_name("renamed")
    created.update_description("Reviews")
    updated = await repository.update(created)
    assert updated.name == "renamed"
    assert updated.description == "Reviews"
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown agent id."""
    repository, owner_id, _ = setup
    agent = Agent(owner_id=owner_id, name="assistant")
    with pytest.raises(AgentNotFound, match=f"Agent {agent.id} was not found"):
        await repository.update(agent)


async def test_update_duplicate_name(setup: Setup) -> None:
    """Reject renaming an agent to a registered name."""
    repository, owner_id, _ = setup
    await repository.create(Agent(owner_id=owner_id, name="assistant"))
    reviewer = await repository.create(Agent(owner_id=owner_id, name="reviewer"))
    reviewer.name = "assistant"
    with pytest.raises(
        DuplicateAgentName, match="Agent name 'assistant' is already registered"
    ):
        await repository.update(reviewer)


async def test_delete(setup: Setup) -> None:
    """Delete a stored agent."""
    repository, owner_id, _ = setup
    created = await repository.create(Agent(owner_id=owner_id, name="assistant"))
    await repository.delete(created.id)
    with pytest.raises(AgentNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown agent id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_delete_cascades_versions(setup: Setup) -> None:
    """Delete an agent together with its versions."""
    repository, owner_id, make_version_repository = setup
    agent = await repository.create(Agent(owner_id=owner_id, name="assistant"))
    version_repository = make_version_repository()
    version = await version_repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent.id)
    )

    await repository.delete(agent.id)
    with pytest.raises(AgentVersionNotFound):
        await version_repository.get(version.id)
