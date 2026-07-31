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
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest

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
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.secret_repository import (
    SQLSecretRepository,
)
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.models.agent_version import AgentVersionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent, AgentNotFound
from kitaru.server.domain.agent_version import (
    AgentCapabilities,
    AgentVersion,
    AgentVersionNotFound,
    RunSpec,
)
from kitaru.server.domain.secret import Secret

Setup = tuple[
    AgentVersionRepository,
    uuid.UUID,
    uuid.UUID,
    Callable[[], Awaitable[uuid.UUID]],
    Callable[[], Awaitable[uuid.UUID]],
]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each agent version repository implementation, an owner id, an
    agent id to attach versions to, and factories for further agent ids and
    secret ids a run spec can reference."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        secrets = FakeSecretRepository()
        owner_id = uuid.uuid4()

        async def make_agent_id() -> uuid.UUID:
            created = await agents.create(
                Agent(owner_id=owner_id, name=f"agent-{uuid.uuid4().hex[:8]}")
            )
            return created.id

        async def make_secret_id() -> uuid.UUID:
            created = await secrets.create(
                Secret(
                    owner_id=owner_id, name=f"secret-{uuid.uuid4().hex[:8]}", values={}
                )
            )
            return created.id

        agent_id = await make_agent_id()
        yield (
            FakeAgentVersionRepository(agents),
            owner_id,
            agent_id,
            make_agent_id,
            make_secret_id,
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agent_repository = SQLAgentRepository(session)
        secret_repository = SQLSecretRepository(
            session, AesGcmCipher("test-encryption-key")
        )

        async def make_agent_id() -> uuid.UUID:
            created = await agent_repository.create(
                Agent(owner_id=owner.id, name=f"agent-{uuid.uuid4().hex[:8]}")
            )
            return created.id

        async def make_secret_id() -> uuid.UUID:
            created = await secret_repository.create(
                Secret(
                    owner_id=owner.id, name=f"secret-{uuid.uuid4().hex[:8]}", values={}
                )
            )
            return created.id

        agent_id = await make_agent_id()
        yield (
            SQLAgentVersionRepository(session),
            owner.id,
            agent_id,
            make_agent_id,
            make_secret_id,
        )


async def test_create_sets_version_and_timestamps(setup: Setup) -> None:
    """Assign version 1 and both timestamps to the first version."""
    repository, owner_id, agent_id, _, _ = setup
    version = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            display_version="v1",
            description="First cut",
        )
    )
    assert version.agent_id == agent_id
    assert version.owner_id == owner_id
    assert version.version == 1
    assert version.display_version == "v1"
    assert version.description == "First cut"
    assert version.run_spec is None
    assert version.capabilities == AgentCapabilities()
    assert version.created is not None
    assert version.updated is not None


async def test_create_numbers_versions_sequentially(setup: Setup) -> None:
    """Assign consecutive version numbers per agent."""
    repository, owner_id, agent_id, _, _ = setup
    first = await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
    second = await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
    third = await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
    assert [first.version, second.version, third.version] == [1, 2, 3]


async def test_create_missing_agent(setup: Setup) -> None:
    """Raise when the agent does not exist."""
    repository, owner_id, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(AgentNotFound, match=f"Agent {missing_id} was not found"):
        await repository.create(AgentVersion(owner_id=owner_id, agent_id=missing_id))


async def test_create_with_run_spec_and_secret_order(setup: Setup) -> None:
    """Round-trip a run spec, preserving the secret id order."""
    repository, owner_id, agent_id, _, make_secret_id = setup
    secret_ids = [
        await make_secret_id(),
        await make_secret_id(),
        await make_secret_id(),
    ]
    run_spec = RunSpec(
        command="run.sh",
        working_dir="/app",
        env={"FOO": "bar"},
        secret_ids=secret_ids,
        timeout_seconds=120,
    )
    version = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent_id, run_spec=run_spec)
    )
    assert version.run_spec == run_spec
    loaded = await repository.get(version.id)
    assert loaded.run_spec is not None
    assert loaded.run_spec.secret_ids == secret_ids


async def test_get(setup: Setup) -> None:
    """Load a stored agent version by id."""
    repository, owner_id, agent_id, _, _ = setup
    created = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent_id)
    )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown agent version id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await repository.get(missing_id)


async def test_query_scoped_to_agent(setup: Setup) -> None:
    """Query only the versions of the requested agent, newest-first."""
    repository, owner_id, agent_id, make_agent_id, _ = setup
    other_agent_id = await make_agent_id()

    v1 = await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
    v2 = await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
    await repository.create(AgentVersion(owner_id=owner_id, agent_id=other_agent_id))

    versions, next_cursor = await repository.query(
        AgentVersionFilter(agent_id=agent_id)
    )
    assert next_cursor is None
    assert [version.id for version in versions] == [v2.id, v1.id]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, agent_id, _, _ = setup
    created = [
        await repository.create(AgentVersion(owner_id=owner_id, agent_id=agent_id))
        for _ in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[AgentVersion] = []
    cursor = None
    while True:
        versions, next_cursor = await repository.query(
            AgentVersionFilter(agent_id=agent_id, cursor=cursor, size=2)
        )
        collected.extend(versions)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({version.id for version in collected}) == 5


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id, agent_id, _, _ = setup
    created = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            display_version="v1",
            description="First cut",
        )
    )
    created.update_display_version("v1.1")
    created.update_description("Second cut")
    created.update_capabilities(AgentCapabilities(tools=["search"]))
    updated = await repository.update(created)
    assert updated.display_version == "v1.1"
    assert updated.description == "Second cut"
    assert updated.capabilities == AgentCapabilities(tools=["search"])
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown agent version id."""
    repository, owner_id, agent_id, _, _ = setup
    version = AgentVersion(owner_id=owner_id, agent_id=agent_id)
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {version.id} was not found"
    ):
        await repository.update(version)


async def test_update_replaces_run_spec_and_secret_links(setup: Setup) -> None:
    """Replace the run spec, including its secret links, on update."""
    repository, owner_id, agent_id, _, make_secret_id = setup
    old_secret_ids = [await make_secret_id(), await make_secret_id()]
    created = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            run_spec=RunSpec(command="old.sh", secret_ids=old_secret_ids),
        )
    )
    new_secret_ids = [await make_secret_id()]
    new_run_spec = RunSpec(command="new.sh", secret_ids=new_secret_ids)
    created.update_run_spec(new_run_spec)
    updated = await repository.update(created)
    assert updated.run_spec == new_run_spec

    loaded = await repository.get(created.id)
    assert loaded.run_spec is not None
    assert loaded.run_spec.secret_ids == new_secret_ids


async def test_update_clears_run_spec_and_secret_links(setup: Setup) -> None:
    """Drop the secret links when the run spec is cleared."""
    repository, owner_id, agent_id, _, make_secret_id = setup
    created = await repository.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            run_spec=RunSpec(command="run.sh", secret_ids=[await make_secret_id()]),
        )
    )
    created.update_run_spec(None)
    updated = await repository.update(created)
    assert updated.run_spec is None
    loaded = await repository.get(created.id)
    assert loaded.run_spec is None


async def test_delete(setup: Setup) -> None:
    """Delete a stored agent version."""
    repository, owner_id, agent_id, _, _ = setup
    created = await repository.create(
        AgentVersion(owner_id=owner_id, agent_id=agent_id)
    )
    await repository.delete(created.id)
    with pytest.raises(AgentVersionNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown agent version id."""
    repository, _, _, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        AgentVersionNotFound, match=f"Agent version {missing_id} was not found"
    ):
        await repository.delete(missing_id)
