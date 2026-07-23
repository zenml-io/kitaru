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
"""Contract tests for agent version repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest
from pydantic import SecretStr

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeSecretRepository,
    pg_session,
    postgres_available,
)
from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import (
    SQLAgentRepository,
)
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.secret_repository import (
    SQLSecretRepository,
)
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.secret_repository import (
    SecretRepository,
)
from kitaru.server.application.models.agent_versions import AgentVersionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    AgentVersionNotFound,
    DuplicateAgentVersion,
    RunSpec,
)
from kitaru.server.domain.secret import Secret, SecretInUse, SecretNotFound

Setup = tuple[AgentVersionRepository, AgentRepository, SecretRepository, uuid.UUID]

CAPABILITIES = AgentCapabilities(
    tools=["search"], mcp_servers=["files"], skills=["review"]
)

VALUES = {"password": SecretStr("hunter2")}


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each agent version repository implementation plus an owner id."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        secrets = FakeSecretRepository()
        versions = FakeAgentVersionRepository(agents, secrets)
        yield versions, agents, secrets, uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning account first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield (
            SQLAgentVersionRepository(session),
            SQLAgentRepository(session),
            SQLSecretRepository(session, AesGcmCipher("test-encryption-key")),
            owner.id,
        )


async def create_agent(
    repository: AgentRepository, owner_id: uuid.UUID, name: str = "support-bot"
) -> Agent:
    """Store an agent for version tests.

    Args:
        repository: Agent repository.
        owner_id: Id of the owning account.
        name: Agent name.

    Returns:
        Stored agent.
    """
    return await repository.create(Agent(owner_id=owner_id, name=name))


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new agent version with both timestamps set."""
    repository, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    version = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent.id, version="v1")
    )
    assert version.agent_id == agent.id
    assert version.owner_id == owner_id
    assert version.version == "v1"
    assert version.description is None
    assert version.run_spec is None
    assert version.capabilities == AgentCapabilities()
    assert version.created is not None
    assert version.updated is not None


async def test_create_with_run_spec(setup: Setup) -> None:
    """Store a runnable version and round-trip its run spec."""
    repository, agents, secrets, owner_id = setup
    agent = await create_agent(agents, owner_id)
    db = await secrets.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    smtp = await secrets.create(Secret(owner_id=owner_id, name="smtp", values=VALUES))
    run_spec = RunSpec(
        command="python agent.py",
        working_dir="/app",
        env={"MODE": "replay"},
        secret_ids=[db.id, smtp.id],
        timeout_seconds=600,
    )
    version = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent.id,
            version="v1",
            run_spec=run_spec,
            capabilities=CAPABILITIES,
        )
    )
    assert version.run_spec == run_spec
    assert version.capabilities == CAPABILITIES
    loaded = await repository.get(version.id)
    assert loaded == version
    assert loaded.run_spec is not None
    assert loaded.run_spec.secret_ids == [db.id, smtp.id]


async def test_create_duplicate_version(setup: Setup) -> None:
    """Reject a second version with the same label for the agent."""
    repository, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent.id, version="v1")
    )
    with pytest.raises(
        DuplicateAgentVersion, match="Agent version 'v1' is already registered"
    ):
        await repository.create(
            AgentVersion(owner_id=owner_id, agent_id=agent.id, version="v1")
        )


async def test_create_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate version failure."""
    repository, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent.id, version="v1")
    )
    with pytest.raises(DuplicateAgentVersion):
        await repository.create(
            AgentVersion(owner_id=owner_id, agent_id=agent.id, version="v1")
        )
    version = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent.id, version="v2")
    )
    assert version.version == "v2"


async def test_create_same_label_other_agent(setup: Setup) -> None:
    """Register the same version label for two agents."""
    repository, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    other = await create_agent(agents, owner_id, name="triage-bot")
    await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent.id, version="v1")
    )
    version = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=other.id, version="v1")
    )
    assert version.agent_id == other.id


async def test_create_unknown_agent(setup: Setup) -> None:
    """Raise for an unknown agent id."""
    repository, _, _, owner_id = setup
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await repository.create(
            AgentVersion(owner_id=owner_id, agent_id=missing_id, version="v1")
        )


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown agent version id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await repository.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query agent versions with filters and pagination."""
    repository, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    other = await create_agent(agents, owner_id, name="triage-bot")
    for label in ["v1", "v2", "v3"]:
        await repository.create(
            AgentVersion(owner_id=owner_id, agent_id=agent.id, version=label)
        )
    await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=other.id, version="v1")
    )

    versions, total = await repository.query(AgentVersionFilter())
    assert total == 4

    versions, total = await repository.query(AgentVersionFilter(agent_id=agent.id))
    assert total == 3
    assert [version.version for version in versions] == ["v1", "v2", "v3"]

    versions, total = await repository.query(
        AgentVersionFilter(agent_id=agent.id, page=2, page_size=2)
    )
    assert total == 3
    assert [version.version for version in versions] == ["v3"]


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, agents, secrets, owner_id = setup
    agent = await create_agent(agents, owner_id)
    db = await secrets.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    smtp = await secrets.create(Secret(owner_id=owner_id, name="smtp", values=VALUES))
    created = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent.id,
            version="v1",
            run_spec=RunSpec(
                command="python agent.py", secret_ids=[db.id], timeout_seconds=600
            ),
        )
    )
    created.update_description("Tuned prompt")
    run_spec = RunSpec(
        command="python agent.py --replay",
        working_dir="/app",
        env={"MODE": "replay"},
        secret_ids=[smtp.id],
        timeout_seconds=900,
    )
    created.update_run_spec(run_spec, frozen=False)
    created.update_capabilities(CAPABILITIES, frozen=False)
    updated = await repository.update(created)
    assert updated.description == "Tuned prompt"
    assert updated.run_spec == run_spec
    assert updated.capabilities == CAPABILITIES
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown agent version id."""
    repository, _, _, owner_id = setup
    version = AgentVersion(owner_id=owner_id, agent_id=uuid.uuid4(), version="v1")
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {version.id} was not found"
    ):
        await repository.update(version)


async def test_update_duplicate_version(setup: Setup) -> None:
    """Reject relabeling a version to a registered label."""
    repository, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent.id, version="v1")
    )
    second = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent.id, version="v2")
    )
    second.version = "v1"
    with pytest.raises(
        DuplicateAgentVersion, match="Agent version 'v1' is already registered"
    ):
        await repository.update(second)


async def test_delete(setup: Setup) -> None:
    """Delete a stored agent version."""
    repository, agents, _, owner_id = setup
    agent = await create_agent(agents, owner_id)
    created = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent.id, version="v1")
    )
    await repository.delete(created.id)
    with pytest.raises(AgentVersionNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown agent version id."""
    repository, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await repository.delete(missing_id)


async def test_secret_delete_while_referenced(setup: Setup) -> None:
    """Reject deleting a secret a run spec references."""
    repository, agents, secrets, owner_id = setup
    agent = await create_agent(agents, owner_id)
    secret = await secrets.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    version = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent.id,
            version="v1",
            run_spec=RunSpec(
                command="python agent.py",
                secret_ids=[secret.id],
                timeout_seconds=600,
            ),
        )
    )
    with pytest.raises(
        SecretInUse, match=f"Secret {secret.id} is referenced by agent versions"
    ):
        await secrets.delete(secret.id)
    loaded = await secrets.get(secret.id)
    assert loaded.name == "db"

    # Deleting the version removes the reference, so the secret delete
    # succeeds afterwards.
    await repository.delete(version.id)
    await secrets.delete(secret.id)
    with pytest.raises(SecretNotFound):
        await secrets.get(secret.id)
