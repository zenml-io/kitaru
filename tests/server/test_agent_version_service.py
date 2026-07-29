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
"""Tests for agent version use cases."""

import uuid

import pytest

from conftest import FakeAgentRepository, FakeAgentVersionRepository, create_agent
from kitaru.server.application.models.agent_version import (
    AgentVersionFilter,
    AgentVersionUpdate,
)
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import AgentNotFound
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersionNotFound,
    RunSpec,
)

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide a fake agent repository."""
    return FakeAgentRepository()


@pytest.fixture
def repository(agent_repository: FakeAgentRepository) -> FakeAgentVersionRepository:
    """Provide a fake agent version repository sharing the agent fake."""
    return FakeAgentVersionRepository(agent_repository)


@pytest.fixture
def service(repository: FakeAgentVersionRepository) -> AgentVersionService:
    """Provide an agent version service backed by the fake repository."""
    return AgentVersionService(repository=repository)


@pytest.fixture
async def agent_id(agent_repository: FakeAgentRepository) -> uuid.UUID:
    """Provide the id of an agent to version."""
    agent = await create_agent(agent_repository, ACTOR.account.id)
    return agent.id


async def test_create_version(
    service: AgentVersionService, agent_id: uuid.UUID
) -> None:
    """Create a version owned by the caller with an empty run spec and capabilities."""
    version = await service.create_version(
        agent_id=agent_id,
        display_version="v1",
        description="First cut",
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    assert version.agent_id == agent_id
    assert version.owner_id == ACTOR.account.id
    assert version.version == 1
    assert version.display_version == "v1"
    assert version.description == "First cut"
    assert version.run_spec is None
    assert version.capabilities == AgentCapabilities()
    assert version.created is not None
    assert version.updated is not None


async def test_create_version_numbering_sequence(
    service: AgentVersionService, agent_id: uuid.UUID
) -> None:
    """Assign consecutive version numbers per agent."""
    first = await service.create_version(
        agent_id=agent_id,
        display_version=None,
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    second = await service.create_version(
        agent_id=agent_id,
        display_version=None,
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    assert first.version == 1
    assert second.version == 2


async def test_create_version_with_run_spec(
    service: AgentVersionService, agent_id: uuid.UUID
) -> None:
    """Store a run spec and explicit capabilities."""
    secret_id = uuid.uuid4()
    run_spec = RunSpec(command="run.sh", secret_ids=[secret_id], timeout_seconds=120)
    capabilities = AgentCapabilities(tools=["search"])
    version = await service.create_version(
        agent_id=agent_id,
        display_version=None,
        description=None,
        run_spec=run_spec,
        capabilities=capabilities,
        actor=ACTOR,
    )
    assert version.run_spec == run_spec
    assert version.capabilities == capabilities


async def test_create_version_missing_agent(service: AgentVersionService) -> None:
    """Raise when the agent does not exist."""
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await service.create_version(
            agent_id=missing_id,
            display_version=None,
            description=None,
            run_spec=None,
            capabilities=None,
            actor=ACTOR,
        )


async def test_get_version(service: AgentVersionService, agent_id: uuid.UUID) -> None:
    """Load a stored agent version by id."""
    created = await service.create_version(
        agent_id=agent_id,
        display_version=None,
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    loaded = await service.get_version(created.id, actor=ACTOR)
    assert loaded == created


async def test_get_version_not_found(service: AgentVersionService) -> None:
    """Raise for an unknown agent version id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await service.get_version(missing_id, actor=ACTOR)


async def test_list_versions(
    service: AgentVersionService,
    agent_id: uuid.UUID,
    agent_repository: FakeAgentRepository,
) -> None:
    """List only the versions of the requested agent."""
    other_agent = await create_agent(agent_repository, ACTOR.account.id, name="other")
    v1 = await service.create_version(
        agent_id=agent_id,
        display_version=None,
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    v2 = await service.create_version(
        agent_id=agent_id,
        display_version=None,
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    await service.create_version(
        agent_id=other_agent.id,
        display_version=None,
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )

    versions, next_cursor = await service.list_versions(
        AgentVersionFilter(agent_id=agent_id), actor=ACTOR
    )
    assert next_cursor is None
    assert [version.id for version in versions] == [v2.id, v1.id]


async def test_list_versions_walks_pages(
    service: AgentVersionService, agent_id: uuid.UUID
) -> None:
    """Walk every page of an agent's versions via next_cursor."""
    created = [
        await service.create_version(
            agent_id=agent_id,
            display_version=None,
            description=None,
            run_spec=None,
            capabilities=None,
            actor=ACTOR,
        )
        for _ in range(3)
    ]
    expected_order = list(reversed([version.id for version in created]))

    collected: list[uuid.UUID] = []
    cursor = None
    while True:
        versions, next_cursor = await service.list_versions(
            AgentVersionFilter(agent_id=agent_id, cursor=cursor, size=1), actor=ACTOR
        )
        collected.extend(version.id for version in versions)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order


async def test_update_version_display_version_and_description(
    service: AgentVersionService, agent_id: uuid.UUID
) -> None:
    """Update the display version and description."""
    created = await service.create_version(
        agent_id=agent_id,
        display_version="v1",
        description="First cut",
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    updated = await service.update_version(
        created.id,
        AgentVersionUpdate(display_version="v1.1", description="Second cut"),
        actor=ACTOR,
    )
    assert updated.display_version == "v1.1"
    assert updated.description == "Second cut"
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated


async def test_update_version_clears_display_version_and_description(
    service: AgentVersionService, agent_id: uuid.UUID
) -> None:
    """Clear the display version and description with an explicit null."""
    created = await service.create_version(
        agent_id=agent_id,
        display_version="v1",
        description="First cut",
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    updated = await service.update_version(
        created.id,
        AgentVersionUpdate(display_version=None, description=None),
        actor=ACTOR,
    )
    assert updated.display_version is None
    assert updated.description is None


async def test_update_version_omitted_fields_unchanged(
    service: AgentVersionService, agent_id: uuid.UUID
) -> None:
    """Leave every field unchanged when the command sets none of it."""
    run_spec = RunSpec(command="run.sh")
    created = await service.create_version(
        agent_id=agent_id,
        display_version="v1",
        description="First cut",
        run_spec=run_spec,
        capabilities=AgentCapabilities(tools=["search"]),
        actor=ACTOR,
    )
    updated = await service.update_version(
        created.id, AgentVersionUpdate(), actor=ACTOR
    )
    assert updated.display_version == "v1"
    assert updated.description == "First cut"
    assert updated.run_spec == run_spec
    assert updated.capabilities == AgentCapabilities(tools=["search"])


async def test_update_version_replaces_run_spec(
    service: AgentVersionService, agent_id: uuid.UUID
) -> None:
    """Replace the run spec wholesale, including its secret ids."""
    created = await service.create_version(
        agent_id=agent_id,
        display_version=None,
        description=None,
        run_spec=RunSpec(command="old.sh", secret_ids=[uuid.uuid4()]),
        capabilities=None,
        actor=ACTOR,
    )
    new_secret_id = uuid.uuid4()
    new_run_spec = RunSpec(command="new.sh", secret_ids=[new_secret_id])
    updated = await service.update_version(
        created.id, AgentVersionUpdate(run_spec=new_run_spec), actor=ACTOR
    )
    assert updated.run_spec == new_run_spec
    assert updated.run_spec is not None
    assert updated.run_spec.secret_ids == [new_secret_id]


async def test_update_version_clears_run_spec(
    service: AgentVersionService, agent_id: uuid.UUID
) -> None:
    """Clear the run spec with an explicit null."""
    created = await service.create_version(
        agent_id=agent_id,
        display_version=None,
        description=None,
        run_spec=RunSpec(command="run.sh"),
        capabilities=None,
        actor=ACTOR,
    )
    updated = await service.update_version(
        created.id, AgentVersionUpdate(run_spec=None), actor=ACTOR
    )
    assert updated.run_spec is None


async def test_update_version_capabilities_null_clears_to_empty(
    service: AgentVersionService, agent_id: uuid.UUID
) -> None:
    """Clear capabilities to an empty value with an explicit null."""
    created = await service.create_version(
        agent_id=agent_id,
        display_version=None,
        description=None,
        run_spec=None,
        capabilities=AgentCapabilities(tools=["search"]),
        actor=ACTOR,
    )
    updated = await service.update_version(
        created.id, AgentVersionUpdate(capabilities=None), actor=ACTOR
    )
    assert updated.capabilities == AgentCapabilities()


async def test_update_version_not_found(service: AgentVersionService) -> None:
    """Raise for an unknown agent version id."""
    with pytest.raises(AgentVersionNotFound):
        await service.update_version(
            uuid.uuid4(), AgentVersionUpdate(description="x"), actor=ACTOR
        )


async def test_delete_version(
    service: AgentVersionService, agent_id: uuid.UUID
) -> None:
    """Delete a stored agent version."""
    created = await service.create_version(
        agent_id=agent_id,
        display_version=None,
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    await service.delete_version(created.id, actor=ACTOR)
    with pytest.raises(AgentVersionNotFound):
        await service.get_version(created.id, actor=ACTOR)


async def test_delete_version_not_found(service: AgentVersionService) -> None:
    """Raise for an unknown agent version id."""
    with pytest.raises(AgentVersionNotFound):
        await service.delete_version(uuid.uuid4(), actor=ACTOR)
