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

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeSecretRepository,
    create_secret,
)
from kitaru.server.application.models.agent_versions import AgentVersionFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.agent_version_service import (
    AgentVersionService,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersionNotFound,
    DuplicateAgentVersion,
    RunSpec,
)
from kitaru.server.domain.secret import SecretNotFound

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))

CAPABILITIES = AgentCapabilities(
    tools=["search"], mcp_servers=["files"], skills=["review"]
)


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide a fake agent repository."""
    return FakeAgentRepository()


@pytest.fixture
def secret_repository() -> FakeSecretRepository:
    """Provide a fake secret repository."""
    return FakeSecretRepository()


@pytest.fixture
def repository(
    agent_repository: FakeAgentRepository,
    secret_repository: FakeSecretRepository,
) -> FakeAgentVersionRepository:
    """Provide a fake agent version repository."""
    return FakeAgentVersionRepository(agent_repository, secret_repository)


@pytest.fixture
def service(
    repository: FakeAgentVersionRepository,
    agent_repository: FakeAgentRepository,
    secret_repository: FakeSecretRepository,
) -> AgentVersionService:
    """Provide an agent version service backed by the fake repositories."""
    return AgentVersionService(
        repository=repository,
        agent_repository=agent_repository,
        secret_repository=secret_repository,
    )


@pytest.fixture
async def agent(agent_repository: FakeAgentRepository) -> Agent:
    """Provide a stored agent."""
    return await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="support-bot")
    )


async def test_create_version(service: AgentVersionService, agent: Agent) -> None:
    """Create an agent version without a run spec."""
    version = await service.create_version(
        agent.id,
        version="v1",
        description="Initial version",
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    assert version.agent_id == agent.id
    assert version.owner_id == ACTOR.account.id
    assert version.version == "v1"
    assert version.description == "Initial version"
    assert version.run_spec is None
    assert version.capabilities == AgentCapabilities()
    assert version.created is not None
    assert version.updated is not None


async def test_create_version_with_run_spec(
    service: AgentVersionService,
    secret_repository: FakeSecretRepository,
    agent: Agent,
) -> None:
    """Create a runnable agent version referencing a secret."""
    secret = await create_secret(secret_repository, ACTOR.account.id)
    run_spec = RunSpec(
        command="python agent.py",
        working_dir="/app",
        env={"MODE": "replay"},
        secret_ids=[secret.id],
        timeout_seconds=600,
    )
    version = await service.create_version(
        agent.id,
        version="v1",
        description=None,
        run_spec=run_spec,
        capabilities=CAPABILITIES,
        actor=ACTOR,
    )
    assert version.run_spec == run_spec
    assert version.capabilities == CAPABILITIES


async def test_create_version_unknown_agent(service: AgentVersionService) -> None:
    """Raise for an unknown agent id."""
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await service.create_version(
            missing_id,
            version="v1",
            description=None,
            run_spec=None,
            capabilities=None,
            actor=ACTOR,
        )


async def test_create_version_duplicate(
    service: AgentVersionService, agent: Agent
) -> None:
    """Reject a second version with the same label for the agent."""
    await service.create_version(
        agent.id,
        version="v1",
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    with pytest.raises(
        DuplicateAgentVersion, match="Agent version 'v1' is already registered"
    ):
        await service.create_version(
            agent.id,
            version="v1",
            description=None,
            run_spec=None,
            capabilities=None,
            actor=ACTOR,
        )


async def test_create_version_same_label_other_agent(
    service: AgentVersionService,
    agent_repository: FakeAgentRepository,
    agent: Agent,
) -> None:
    """Register the same version label for two agents."""
    other = await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="triage-bot")
    )
    await service.create_version(
        agent.id,
        version="v1",
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    version = await service.create_version(
        other.id,
        version="v1",
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    assert version.agent_id == other.id


async def test_create_version_missing_secret(
    service: AgentVersionService, agent: Agent
) -> None:
    """Raise for a run spec referencing an unknown secret."""
    missing_id = uuid.uuid4()
    run_spec = RunSpec(
        command="python agent.py", secret_ids=[missing_id], timeout_seconds=600
    )
    with pytest.raises(SecretNotFound, match=f"Secret {missing_id} was not found"):
        await service.create_version(
            agent.id,
            version="v1",
            description=None,
            run_spec=run_spec,
            capabilities=None,
            actor=ACTOR,
        )


async def test_create_version_internal_secret(
    service: AgentVersionService,
    secret_repository: FakeSecretRepository,
    agent: Agent,
) -> None:
    """Raise for a run spec referencing an internal secret."""
    secret = await create_secret(secret_repository, ACTOR.account.id, internal=True)
    run_spec = RunSpec(
        command="python agent.py", secret_ids=[secret.id], timeout_seconds=600
    )
    with pytest.raises(SecretNotFound, match=f"Secret {secret.id} was not found"):
        await service.create_version(
            agent.id,
            version="v1",
            description=None,
            run_spec=run_spec,
            capabilities=None,
            actor=ACTOR,
        )


async def test_get_version(service: AgentVersionService, agent: Agent) -> None:
    """Load a stored agent version by id."""
    created = await service.create_version(
        agent.id,
        version="v1",
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
    agent_repository: FakeAgentRepository,
    agent: Agent,
) -> None:
    """List agent versions with filters and pagination."""
    other = await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="triage-bot")
    )
    for label in ["v1", "v2", "v3"]:
        await service.create_version(
            agent.id,
            version=label,
            description=None,
            run_spec=None,
            capabilities=None,
            actor=ACTOR,
        )
    await service.create_version(
        other.id,
        version="v1",
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )

    versions, total = await service.list_versions(
        AgentVersionFilter(agent_id=agent.id), actor=ACTOR
    )
    assert total == 3
    assert [version.version for version in versions] == ["v1", "v2", "v3"]

    versions, total = await service.list_versions(
        AgentVersionFilter(agent_id=agent.id, page=2, page_size=2), actor=ACTOR
    )
    assert total == 3
    assert [version.version for version in versions] == ["v3"]


async def test_list_versions_unknown_agent(service: AgentVersionService) -> None:
    """Raise for an unknown agent id."""
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await service.list_versions(
            AgentVersionFilter(agent_id=missing_id), actor=ACTOR
        )


async def test_update_version(
    service: AgentVersionService,
    secret_repository: FakeSecretRepository,
    agent: Agent,
) -> None:
    """Update the description, run spec, and capabilities of a version."""
    secret = await create_secret(secret_repository, ACTOR.account.id)
    created = await service.create_version(
        agent.id,
        version="v1",
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    updated = await service.update_version(
        created.id,
        description="Tuned prompt",
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    assert updated.description == "Tuned prompt"
    assert updated.run_spec is None
    assert updated.capabilities == AgentCapabilities()
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated

    run_spec = RunSpec(
        command="python agent.py", secret_ids=[secret.id], timeout_seconds=600
    )
    updated = await service.update_version(
        created.id,
        description=None,
        run_spec=run_spec,
        capabilities=CAPABILITIES,
        actor=ACTOR,
    )
    assert updated.description == "Tuned prompt"
    assert updated.run_spec == run_spec
    assert updated.capabilities == CAPABILITIES


async def test_update_version_missing_secret(
    service: AgentVersionService, agent: Agent
) -> None:
    """Raise for a new run spec referencing an unknown secret."""
    created = await service.create_version(
        agent.id,
        version="v1",
        description=None,
        run_spec=None,
        capabilities=None,
        actor=ACTOR,
    )
    missing_id = uuid.uuid4()
    run_spec = RunSpec(
        command="python agent.py", secret_ids=[missing_id], timeout_seconds=600
    )
    with pytest.raises(SecretNotFound, match=f"Secret {missing_id} was not found"):
        await service.update_version(
            created.id,
            description=None,
            run_spec=run_spec,
            capabilities=None,
            actor=ACTOR,
        )
    loaded = await service.get_version(created.id, actor=ACTOR)
    assert loaded.run_spec is None


async def test_update_version_not_found(service: AgentVersionService) -> None:
    """Raise for an unknown agent version id."""
    with pytest.raises(AgentVersionNotFound):
        await service.update_version(
            uuid.uuid4(),
            description="Tuned prompt",
            run_spec=None,
            capabilities=None,
            actor=ACTOR,
        )


async def test_delete_version(service: AgentVersionService, agent: Agent) -> None:
    """Delete a stored agent version."""
    created = await service.create_version(
        agent.id,
        version="v1",
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
