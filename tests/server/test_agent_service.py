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
from typing import Any

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    create_agent,
    create_agent_version,
)
from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.application.models.agent import (
    AgentCreate,
    AgentFilter,
    AgentUpdate,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import AgentNotFound, DuplicateAgentName
from kitaru.server.domain.agent_version import AgentVersionNotFound
from kitaru.server.domain.base import ValidationError
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))
FOREIGN_ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="bob"))


class _RecordingAnalytics(ServerAnalytics):
    """Analytics tracker recording track calls instead of buffering them."""

    def __init__(self) -> None:
        """Initialize the tracker."""
        self.tracked: list[tuple[uuid.UUID, AnalyticsEvent | str, dict[str, Any]]] = []

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
        AgentCreate(name="assistant", description="Helps"), ACTOR
    )
    assert agent.name == "assistant"
    assert agent.owner_id == ACTOR.account.id
    assert agent.description == "Helps"
    assert agent.latest_version == 0
    assert agent.created is not None
    assert agent.updated is not None


async def test_create_agent_duplicate_name(service: AgentService) -> None:
    """Reject a second agent with the same name."""
    await service.create_agent(AgentCreate(name="assistant"), ACTOR)
    with pytest.raises(
        DuplicateAgentName, match="Agent name 'assistant' is already registered"
    ):
        await service.create_agent(AgentCreate(name="assistant"), ACTOR)


async def test_get_agent(service: AgentService) -> None:
    """Load a stored agent by id."""
    created = await service.create_agent(AgentCreate(name="assistant"), ACTOR)
    loaded = await service.get_agent(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_agent_not_found(service: AgentService) -> None:
    """Raise for an unknown agent id."""
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await service.get_agent(missing_id, actor=ACTOR)


async def test_get_agent_foreign_owner(service: AgentService) -> None:
    """Read an agent owned by another account."""
    created = await service.create_agent(AgentCreate(name="assistant"), ACTOR)
    loaded = await service.get_agent(created.id, actor=FOREIGN_ACTOR)
    assert loaded == created


async def test_list_agents(service: AgentService) -> None:
    """List agents newest-first with filters."""
    for name in ["assistant", "reviewer", "triager"]:
        await service.create_agent(AgentCreate(name=name), ACTOR)

    agents, next_cursor = await service.list_agents(AgentFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [agent.name for agent in agents] == ["triager", "reviewer", "assistant"]

    agents, next_cursor = await service.list_agents(
        AgentFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="reviewer")
        ),
        actor=ACTOR,
    )
    assert next_cursor is None
    assert agents[0].name == "reviewer"


async def test_list_agents_walks_pages(service: AgentService) -> None:
    """Walk every page of agents via next_cursor."""
    for name in ["assistant", "reviewer", "triager"]:
        await service.create_agent(AgentCreate(name=name), ACTOR)

    collected: list[str] = []
    cursor = None
    while True:
        agents, next_cursor = await service.list_agents(
            AgentFilter(cursor=cursor, size=2), actor=ACTOR
        )
        collected.extend(agent.name for agent in agents)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == ["triager", "reviewer", "assistant"]


async def test_update_agent_name(service: AgentService) -> None:
    """Update an agent's name."""
    created = await service.create_agent(AgentCreate(name="assistant"), ACTOR)
    updated = await service.update_agent(
        created.id, AgentUpdate(name="renamed"), actor=ACTOR
    )
    assert updated.name == "renamed"
    assert updated.description is None
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated


async def test_update_agent_description(service: AgentService) -> None:
    """Update an agent's description without touching its name."""
    created = await service.create_agent(
        AgentCreate(name="assistant", description="Helps"), ACTOR
    )
    updated = await service.update_agent(
        created.id, AgentUpdate(description="Reviews"), actor=ACTOR
    )
    assert updated.name == "assistant"
    assert updated.description == "Reviews"


async def test_update_agent_clears_description(service: AgentService) -> None:
    """Clear an agent's description with an explicit null."""
    created = await service.create_agent(
        AgentCreate(name="assistant", description="Helps"), ACTOR
    )
    updated = await service.update_agent(
        created.id, AgentUpdate(description=None), actor=ACTOR
    )
    assert updated.description is None


async def test_update_agent_omitted_fields_unchanged(service: AgentService) -> None:
    """Leave every field unchanged when the command sets none of it."""
    created = await service.create_agent(
        AgentCreate(name="assistant", description="Helps"), ACTOR
    )
    updated = await service.update_agent(created.id, AgentUpdate(), actor=ACTOR)
    assert updated.name == "assistant"
    assert updated.description == "Helps"


async def test_update_agent_cannot_clear_name(service: AgentService) -> None:
    """Reject clearing the agent name with an explicit null."""
    created = await service.create_agent(AgentCreate(name="assistant"), ACTOR)
    with pytest.raises(ValidationError, match="Agent name cannot be cleared"):
        await service.update_agent(created.id, AgentUpdate(name=None), actor=ACTOR)


async def test_update_agent_not_found(service: AgentService) -> None:
    """Raise for an unknown agent id."""
    with pytest.raises(AgentNotFound):
        await service.update_agent(uuid.uuid4(), AgentUpdate(name="x"), actor=ACTOR)


async def test_update_agent_duplicate_name(service: AgentService) -> None:
    """Reject renaming an agent to a registered name."""
    await service.create_agent(AgentCreate(name="assistant"), ACTOR)
    other = await service.create_agent(AgentCreate(name="reviewer"), ACTOR)
    with pytest.raises(DuplicateAgentName):
        await service.update_agent(other.id, AgentUpdate(name="assistant"), actor=ACTOR)


async def test_delete_agent(service: AgentService) -> None:
    """Delete a stored agent."""
    created = await service.create_agent(AgentCreate(name="assistant"), ACTOR)
    await service.delete_agent(created.id, actor=ACTOR)
    with pytest.raises(AgentNotFound):
        await service.get_agent(created.id, actor=ACTOR)


async def test_delete_agent_not_found(service: AgentService) -> None:
    """Raise for an unknown agent id."""
    with pytest.raises(AgentNotFound):
        await service.delete_agent(uuid.uuid4(), actor=ACTOR)


async def test_delete_agent_cascades_versions(
    service: AgentService, repository: FakeAgentRepository
) -> None:
    """Deleting an agent cascades its versions."""
    created = await create_agent(repository, ACTOR.account.id)
    version_repository = FakeAgentVersionRepository(repository)
    version = await create_agent_version(
        version_repository, created.id, ACTOR.account.id
    )

    await service.delete_agent(created.id, actor=ACTOR)

    with pytest.raises(AgentNotFound):
        await service.get_agent(created.id, actor=ACTOR)
    with pytest.raises(AgentVersionNotFound):
        await version_repository.get(version.id)


async def test_create_agent_tracks_agent_created(
    repository: FakeAgentRepository,
) -> None:
    """Fire AGENT_CREATED with no properties beyond the commons."""
    analytics = _RecordingAnalytics()
    service = AgentService(repository=repository, analytics=analytics)

    await service.create_agent(AgentCreate(name="assistant"), ACTOR)

    assert len(analytics.tracked) == 1
    user_id, event, properties = analytics.tracked[0]
    assert user_id == ACTOR.account.id
    assert event == AnalyticsEvent.AGENT_CREATED
    assert properties == {}


async def test_create_agent_without_analytics_tracker(service: AgentService) -> None:
    """Create an agent normally when no analytics tracker is configured."""
    agent = await service.create_agent(AgentCreate(name="assistant"), ACTOR)
    assert agent.owner_id == ACTOR.account.id
