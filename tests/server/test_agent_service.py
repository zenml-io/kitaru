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
"""Tests for agent use cases."""

import uuid

import pytest

from conftest import FakeAgentRepository, FakeAgentVersionRepository
from kitaru.server.application.models.agents import AgentFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import (
    AgentInUse,
    AgentNotFound,
    DuplicateAgentName,
)
from kitaru.server.domain.agent_version import AgentVersion

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))
FOREIGN_ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="bob"))


@pytest.fixture
def repository() -> FakeAgentRepository:
    """Provide a fake agent repository."""
    return FakeAgentRepository()


@pytest.fixture
def service(repository: FakeAgentRepository) -> AgentService:
    """Provide an agent service backed by the fake repository."""
    return AgentService(repository=repository)


async def test_create_agent(service: AgentService) -> None:
    """Create an agent owned by the caller."""
    agent = await service.create_agent(
        name="support-bot", description="Answers tickets", actor=ACTOR
    )
    assert agent.name == "support-bot"
    assert agent.owner_id == ACTOR.account.id
    assert agent.description == "Answers tickets"
    assert agent.created is not None
    assert agent.updated is not None


async def test_create_agent_duplicate_name(service: AgentService) -> None:
    """Reject a second agent with the same name."""
    await service.create_agent(name="support-bot", description=None, actor=ACTOR)
    with pytest.raises(
        DuplicateAgentName, match="Agent name 'support-bot' is already registered"
    ):
        await service.create_agent(
            name="support-bot", description=None, actor=FOREIGN_ACTOR
        )


async def test_get_agent(service: AgentService) -> None:
    """Load a stored agent by id."""
    created = await service.create_agent(
        name="support-bot", description=None, actor=ACTOR
    )
    loaded = await service.get_agent(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_agent_not_found(service: AgentService) -> None:
    """Raise for an unknown agent id."""
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await service.get_agent(missing_id, actor=ACTOR)


async def test_list_agents(service: AgentService) -> None:
    """List agents with filters and pagination."""
    for name in ["support-bot", "triage-bot", "coder"]:
        await service.create_agent(name=name, description=None, actor=ACTOR)

    agents, total = await service.list_agents(AgentFilter(), actor=ACTOR)
    assert total == 3
    assert [agent.name for agent in agents] == ["support-bot", "triage-bot", "coder"]

    agents, total = await service.list_agents(
        AgentFilter(name="triage-bot"), actor=ACTOR
    )
    assert total == 1
    assert agents[0].name == "triage-bot"

    agents, total = await service.list_agents(
        AgentFilter(page=2, page_size=2), actor=ACTOR
    )
    assert total == 3
    assert [agent.name for agent in agents] == ["coder"]


async def test_update_agent(service: AgentService) -> None:
    """Update the name and description of an agent."""
    created = await service.create_agent(
        name="support-bot", description=None, actor=ACTOR
    )
    updated = await service.update_agent(
        created.id, name="triage-bot", description=None, actor=ACTOR
    )
    assert updated.name == "triage-bot"
    assert updated.description is None
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    updated = await service.update_agent(
        created.id, name=None, description="Sorts tickets", actor=ACTOR
    )
    assert updated.name == "triage-bot"
    assert updated.description == "Sorts tickets"


async def test_update_agent_duplicate_name(service: AgentService) -> None:
    """Reject renaming an agent to a registered name."""
    await service.create_agent(name="support-bot", description=None, actor=ACTOR)
    other = await service.create_agent(name="triage-bot", description=None, actor=ACTOR)
    with pytest.raises(
        DuplicateAgentName, match="Agent name 'support-bot' is already registered"
    ):
        await service.update_agent(
            other.id, name="support-bot", description=None, actor=ACTOR
        )


async def test_update_agent_not_found(service: AgentService) -> None:
    """Raise for an unknown agent id."""
    with pytest.raises(AgentNotFound):
        await service.update_agent(
            uuid.uuid4(), name="triage-bot", description=None, actor=ACTOR
        )


async def test_delete_agent(service: AgentService) -> None:
    """Delete a stored agent."""
    created = await service.create_agent(
        name="support-bot", description=None, actor=ACTOR
    )
    await service.delete_agent(created.id, actor=ACTOR)
    with pytest.raises(AgentNotFound):
        await service.get_agent(created.id, actor=ACTOR)


async def test_delete_agent_not_found(service: AgentService) -> None:
    """Raise for an unknown agent id."""
    with pytest.raises(AgentNotFound):
        await service.delete_agent(uuid.uuid4(), actor=ACTOR)


async def test_delete_agent_with_versions(
    service: AgentService, repository: FakeAgentRepository
) -> None:
    """Reject deleting an agent that still has versions."""
    created = await service.create_agent(
        name="support-bot", description=None, actor=ACTOR
    )
    version_repository = FakeAgentVersionRepository(repository)
    version = await version_repository.create(
        AgentVersion(owner_id=ACTOR.account.id, agent_id=created.id, version="v1")
    )
    with pytest.raises(
        AgentInUse, match=f"Agent {created.id} is referenced by agent versions"
    ):
        await service.delete_agent(created.id, actor=ACTOR)

    await version_repository.delete(version.id)
    await service.delete_agent(created.id, actor=ACTOR)
    with pytest.raises(AgentNotFound):
        await service.get_agent(created.id, actor=ACTOR)
