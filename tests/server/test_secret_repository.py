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
"""Contract tests for secret repositories."""

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
from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.orm.secret import SecretORM
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
from kitaru.server.application.interfaces.secret_repository import (
    SecretRepository,
)
from kitaru.server.application.models.secret import SecretFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion, RunSpec
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.secret import (
    DuplicateSecretName,
    Secret,
    SecretInUse,
    SecretNotFound,
)
from kitaru.server.filtering import FilterCondition

Setup = tuple[SecretRepository, uuid.UUID, uuid.UUID]
RestrictionSetup = tuple[SecretRepository, AgentVersionRepository, uuid.UUID, uuid.UUID]

VALUES = {"username": SecretStr("svc"), "password": SecretStr("hunter2")}


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each secret repository implementation plus two owner ids."""
    if request.param == "fake":
        yield FakeSecretRepository(), uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning accounts first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        other_owner = await accounts.create(Account(name="other-owner"))
        repository = SQLSecretRepository(session, AesGcmCipher("test-encryption-key"))
        yield repository, owner.id, other_owner.id


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new secret with both timestamps set."""
    repository, owner_id, _ = setup
    secret = await repository.create(
        Secret(owner_id=owner_id, name="db", values=VALUES)
    )
    assert secret.name == "db"
    assert secret.owner_id == owner_id
    assert secret.internal is False
    assert secret.type is None
    assert secret.values == VALUES
    assert secret.created is not None
    assert secret.updated is not None


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second secret with the same name."""
    repository, owner_id, other_owner_id = setup
    await repository.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    with pytest.raises(
        DuplicateSecretName, match="Secret name 'db' is already registered"
    ):
        await repository.create(
            Secret(owner_id=other_owner_id, name="db", values=VALUES)
        )


async def test_create_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate name failure."""
    repository, owner_id, _ = setup
    await repository.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    with pytest.raises(DuplicateSecretName):
        await repository.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    secret = await repository.create(
        Secret(owner_id=owner_id, name="smtp", values=VALUES)
    )
    assert secret.name == "smtp"


async def test_get(setup: Setup) -> None:
    """Load a stored secret by id with its values round-tripped."""
    repository, owner_id, _ = setup
    created = await repository.create(
        Secret(owner_id=owner_id, name="db", values=VALUES)
    )
    loaded = await repository.get(created.id)
    assert loaded == created
    assert loaded.values == VALUES


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown secret id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(SecretNotFound, match=f"Secret {missing_id} was not found"):
        await repository.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query secrets newest-first with filters."""
    repository, owner_id, other_owner_id = setup
    db = await repository.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    await repository.create(Secret(owner_id=owner_id, name="smtp", values=VALUES))
    s3 = await repository.create(
        Secret(owner_id=other_owner_id, name="s3", values=VALUES)
    )

    secrets, next_cursor = await repository.query(SecretFilter())
    assert next_cursor is None
    assert [secret.name for secret in secrets] == ["s3", "smtp", "db"]

    secrets, next_cursor = await repository.query(
        SecretFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="db")
        )
    )
    assert next_cursor is None
    assert secrets[0] == db

    secrets, next_cursor = await repository.query(SecretFilter(owner_id=other_owner_id))
    assert next_cursor is None
    assert secrets[0] == s3

    secrets, next_cursor = await repository.query(
        SecretFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="missing")
        )
    )
    assert next_cursor is None
    assert secrets == []


async def test_query_internal_filter(setup: Setup) -> None:
    """Query secrets filtered on the internal flag."""
    repository, owner_id, _ = setup
    db = await repository.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    hidden = await repository.create(
        Secret(owner_id=owner_id, name="hidden", internal=True, values=VALUES)
    )

    secrets, next_cursor = await repository.query(SecretFilter())
    assert next_cursor is None
    assert len(secrets) == 2

    secrets, next_cursor = await repository.query(SecretFilter(internal=False))
    assert next_cursor is None
    assert secrets[0] == db

    secrets, next_cursor = await repository.query(SecretFilter(internal=True))
    assert next_cursor is None
    assert secrets[0] == hidden


async def test_query_owner_id_filter(setup: Setup) -> None:
    """Query secrets filtered on owner_id, excluding secrets of other owners."""
    repository, owner_id, other_owner_id = setup
    db = await repository.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    await repository.create(Secret(owner_id=other_owner_id, name="s3", values=VALUES))

    secrets, next_cursor = await repository.query(SecretFilter(owner_id=owner_id))
    assert next_cursor is None
    assert [secret.id for secret in secrets] == [db.id]


async def test_query_sort_created_asc(setup: Setup) -> None:
    """Sort secrets oldest-first with sort=created:asc."""
    repository, owner_id, _ = setup
    db = await repository.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    smtp = await repository.create(
        Secret(owner_id=owner_id, name="smtp", values=VALUES)
    )
    s3 = await repository.create(Secret(owner_id=owner_id, name="s3", values=VALUES))

    secrets, next_cursor = await repository.query(SecretFilter(sort="created:asc"))
    assert next_cursor is None
    assert secrets == [db, smtp, s3]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, _ = setup
    created = [
        await repository.create(
            Secret(owner_id=owner_id, name=f"secret-{i}", values=VALUES)
        )
        for i in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[Secret] = []
    cursor = None
    while True:
        secrets, next_cursor = await repository.query(
            SecretFilter(cursor=cursor, size=2)
        )
        collected.extend(secrets)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({secret.id for secret in collected}) == 5


async def test_query_filter_persists_across_cursor(setup: Setup) -> None:
    """Keep a filter applied across every page of a cursor walk."""
    repository, owner_id, _ = setup
    for i in range(3):
        await repository.create(
            Secret(owner_id=owner_id, name=f"visible-{i}", values=VALUES)
        )
    for i in range(2):
        await repository.create(
            Secret(owner_id=owner_id, name=f"hidden-{i}", internal=True, values=VALUES)
        )

    collected: list[Secret] = []
    cursor = None
    while True:
        secrets, next_cursor = await repository.query(
            SecretFilter(internal=False, cursor=cursor, size=1)
        )
        collected.extend(secrets)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert len(collected) == 3
    assert all(not secret.internal for secret in collected)


async def test_query_invalid_cursor(setup: Setup) -> None:
    """Raise for a cursor string that fails to decode."""
    repository, _, _ = setup
    with pytest.raises(ValidationError):
        await repository.query(SecretFilter(cursor="not-a-valid-cursor"))


async def test_query_cursor_sort_mismatch(setup: Setup) -> None:
    """Raise when a cursor is replayed with a different sort."""
    repository, owner_id, _ = setup
    await repository.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    await repository.create(Secret(owner_id=owner_id, name="smtp", values=VALUES))
    _, next_cursor = await repository.query(SecretFilter(size=1))
    assert next_cursor is not None
    with pytest.raises(ValidationError):
        await repository.query(
            SecretFilter(cursor=next_cursor, size=1, sort="created:asc")
        )


async def test_query_cursor_filter_mismatch(setup: Setup) -> None:
    """Raise when a cursor is replayed after the filter changes."""
    repository, owner_id, _ = setup
    await repository.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    await repository.create(Secret(owner_id=owner_id, name="smtp", values=VALUES))
    await repository.create(Secret(owner_id=owner_id, name="s3", values=VALUES))
    _, next_cursor = await repository.query(SecretFilter(internal=False, size=1))
    assert next_cursor is not None
    with pytest.raises(ValidationError):
        await repository.query(SecretFilter(cursor=next_cursor, size=1))


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id, _ = setup
    created = await repository.create(
        Secret(owner_id=owner_id, name="db", values=VALUES)
    )
    created.update_type("database")
    created.update_values({"password": SecretStr("hunter3")})
    updated = await repository.update(created)
    assert updated.type == "database"
    assert updated.values == {"password": SecretStr("hunter3")}
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown secret id."""
    repository, owner_id, _ = setup
    secret = Secret(owner_id=owner_id, name="db", values=VALUES)
    with pytest.raises(SecretNotFound, match=f"Secret {secret.id} was not found"):
        await repository.update(secret)


async def test_update_duplicate_name(setup: Setup) -> None:
    """Reject renaming a secret to a registered name."""
    repository, owner_id, _ = setup
    await repository.create(Secret(owner_id=owner_id, name="db", values=VALUES))
    smtp = await repository.create(
        Secret(owner_id=owner_id, name="smtp", values=VALUES)
    )
    smtp.name = "db"
    with pytest.raises(
        DuplicateSecretName, match="Secret name 'db' is already registered"
    ):
        await repository.update(smtp)


async def test_delete(setup: Setup) -> None:
    """Delete a stored secret."""
    repository, owner_id, _ = setup
    created = await repository.create(
        Secret(owner_id=owner_id, name="db", values=VALUES)
    )
    await repository.delete(created.id)
    with pytest.raises(SecretNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown secret id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(SecretNotFound, match=f"Secret {missing_id} was not found"):
        await repository.delete(missing_id)


@pytest.fixture(params=["fake", "postgres"])
async def restriction_setup(
    request: pytest.FixtureRequest,
) -> AsyncGenerator[RestrictionSetup, None]:
    """Provide a secret repository wired to an agent version repository
    sharing its backend, an owner id, and an agent id."""
    if request.param == "fake":
        owner_id = uuid.uuid4()
        agents = FakeAgentRepository()
        agent = await agents.create(Agent(owner_id=owner_id, name="assistant"))
        agent_versions = FakeAgentVersionRepository(agents)
        secrets = FakeSecretRepository(agent_versions=agent_versions)
        yield secrets, agent_versions, owner_id, agent.id
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        agents_repository = SQLAgentRepository(session)
        agent = await agents_repository.create(
            Agent(owner_id=owner.id, name="assistant")
        )
        yield (
            SQLSecretRepository(session, AesGcmCipher("test-encryption-key")),
            SQLAgentVersionRepository(session),
            owner.id,
            agent.id,
        )


async def test_delete_restricted_by_agent_version(
    restriction_setup: RestrictionSetup,
) -> None:
    """Reject deleting a secret referenced by an agent version's run spec."""
    repository, agent_versions, owner_id, agent_id = restriction_setup
    secret = await repository.create(
        Secret(owner_id=owner_id, name="db", values=VALUES)
    )
    await agent_versions.create(
        AgentVersion(
            owner_id=owner_id,
            agent_id=agent_id,
            run_spec=RunSpec(command="run.sh", secret_ids=[secret.id]),
        )
    )

    with pytest.raises(SecretInUse):
        await repository.delete(secret.id)


async def test_values_encrypted_at_rest() -> None:
    """Store no plaintext value in the values_encrypted column."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        repository = SQLSecretRepository(session, AesGcmCipher("test-encryption-key"))
        created = await repository.create(
            Secret(owner_id=owner.id, name="db", values=VALUES)
        )
        row = await session.get(SecretORM, created.id)
        assert row is not None
        assert "hunter2" not in row.values_encrypted
        assert "password" not in row.values_encrypted
