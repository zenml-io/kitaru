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
"""Contract tests for connection repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest
from pydantic import SecretStr
from sqlalchemy.exc import IntegrityError

from conftest import (
    FakeConnectionRepository,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.orm.connection import ConnectionORM
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.connection_repository import (
    SQLConnectionRepository,
)
from kitaru.server.adapters.db.repositories.secret_repository import (
    SQLSecretRepository,
)
from kitaru.server.application.interfaces.connection_repository import (
    ConnectionRepository,
)
from kitaru.server.application.models.connection import ConnectionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.connection import (
    Connection,
    ConnectionNotFound,
    DuplicateConnectionName,
)
from kitaru.server.domain.secret import Secret
from kitaru.server.filtering import FilterCondition

Setup = tuple[ConnectionRepository, uuid.UUID, uuid.UUID]

ENV = {"LANGFUSE_BASE_URL": "https://cloud.langfuse.com"}


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each connection repository implementation plus an owner and secret id."""
    if request.param == "fake":
        yield FakeConnectionRepository(), uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id and secret_id columns carry foreign keys, so store the
        # rows they point at first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        secrets = SQLSecretRepository(session, AesGcmCipher("test-encryption-key"))
        secret = await secrets.create(
            Secret(
                owner_id=owner.id,
                name="connection-values",
                internal=True,
                values={"LANGFUSE_SECRET_KEY": SecretStr("sk")},
            )
        )
        yield SQLConnectionRepository(session), owner.id, secret.id


def build_connection(
    owner_id: uuid.UUID,
    secret_id: uuid.UUID,
    name: str = "langfuse-prod",
    provider: str = "langfuse",
    default: bool = False,
) -> Connection:
    """Build a connection to store.

    Args:
        owner_id: Id of the owning account.
        secret_id: Secret holding the sensitive values.
        name: Connection name.
        provider: Provider the connection addresses.
        default: Whether the connection is the provider's default.

    Returns:
        Connection.
    """
    return Connection(
        owner_id=owner_id,
        name=name,
        provider=provider,
        env=dict(ENV),
        secret_id=secret_id,
        default=default,
    )


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new connection with both timestamps set."""
    repository, owner_id, secret_id = setup
    connection = await repository.create(build_connection(owner_id, secret_id))
    assert connection.name == "langfuse-prod"
    assert connection.provider == "langfuse"
    assert connection.owner_id == owner_id
    assert connection.env == ENV
    assert connection.secret_id == secret_id
    assert connection.default is False
    assert connection.created is not None
    assert connection.updated is not None


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second connection with the same name."""
    repository, owner_id, secret_id = setup
    await repository.create(build_connection(owner_id, secret_id))
    with pytest.raises(
        DuplicateConnectionName,
        match="Connection name 'langfuse-prod' is already registered",
    ):
        await repository.create(build_connection(owner_id, secret_id))


async def test_create_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate name failure."""
    repository, owner_id, secret_id = setup
    await repository.create(build_connection(owner_id, secret_id))
    with pytest.raises(DuplicateConnectionName):
        await repository.create(build_connection(owner_id, secret_id))
    connection = await repository.create(
        build_connection(owner_id, secret_id, name="langfuse-staging")
    )
    assert connection.name == "langfuse-staging"


async def test_get(setup: Setup) -> None:
    """Load a stored connection by id."""
    repository, owner_id, secret_id = setup
    created = await repository.create(build_connection(owner_id, secret_id))

    loaded = await repository.get(created.id)

    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown connection id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        ConnectionNotFound, match=f"Connection {missing_id} was not found"
    ):
        await repository.get(missing_id)


async def test_get_default(setup: Setup) -> None:
    """Load the provider's default connection, None when it has none."""
    repository, owner_id, secret_id = setup
    await repository.create(build_connection(owner_id, secret_id, name="plain"))
    created = await repository.create(
        build_connection(owner_id, secret_id, name="chosen", default=True)
    )

    assert await repository.get_default("langfuse") == created
    assert await repository.get_default("langsmith") is None


async def test_create_default_clears_the_previous_default(setup: Setup) -> None:
    """Leave one default per provider when a second default is stored."""
    repository, owner_id, secret_id = setup
    first = await repository.create(
        build_connection(owner_id, secret_id, name="first", default=True)
    )
    second = await repository.create(
        build_connection(owner_id, secret_id, name="second", default=True)
    )

    assert (await repository.get(first.id)).default is False
    assert await repository.get_default("langfuse") == await repository.get(second.id)


async def test_create_default_leaves_other_providers_alone(setup: Setup) -> None:
    """Keep the default of another provider when a default is stored."""
    repository, owner_id, secret_id = setup
    other = await repository.create(
        build_connection(
            owner_id, secret_id, name="smith", provider="langsmith", default=True
        )
    )
    await repository.create(
        build_connection(owner_id, secret_id, name="fuse", default=True)
    )

    assert (await repository.get(other.id)).default is True


async def test_update_default_clears_the_previous_default(setup: Setup) -> None:
    """Clear the provider's previous default when another takes over."""
    repository, owner_id, secret_id = setup
    first = await repository.create(
        build_connection(owner_id, secret_id, name="first", default=True)
    )
    second = await repository.create(
        build_connection(owner_id, secret_id, name="second")
    )

    second.update_default(True)
    await repository.update(second)

    assert (await repository.get(first.id)).default is False
    assert (await repository.get(second.id)).default is True


async def test_query(setup: Setup) -> None:
    """Return connections matching a filter."""
    repository, owner_id, secret_id = setup
    await repository.create(build_connection(owner_id, secret_id, name="fuse"))
    await repository.create(
        build_connection(owner_id, secret_id, name="smith", provider="langsmith")
    )

    connections, next_cursor = await repository.query(
        ConnectionFilter(
            expression=FilterCondition(
                field="provider", op=FilterOp.EQ, value="langsmith"
            )
        )
    )

    assert next_cursor is None
    assert [connection.name for connection in connections] == ["smith"]


async def test_query_default_filter(setup: Setup) -> None:
    """Return only the connections matching the default flag."""
    repository, owner_id, secret_id = setup
    await repository.create(build_connection(owner_id, secret_id, name="plain"))
    await repository.create(
        build_connection(owner_id, secret_id, name="chosen", default=True)
    )

    connections, _ = await repository.query(
        ConnectionFilter(
            expression=FilterCondition(field="default", op=FilterOp.EQ, value=True)
        )
    )

    assert [connection.name for connection in connections] == ["chosen"]


async def test_update(setup: Setup) -> None:
    """Store changed fields and renew the updated timestamp."""
    repository, owner_id, secret_id = setup
    created = await repository.create(build_connection(owner_id, secret_id))

    created.update_env({"LANGFUSE_BASE_URL": "https://eu.langfuse.com"})
    updated = await repository.update(created)

    assert updated.env == {"LANGFUSE_BASE_URL": "https://eu.langfuse.com"}
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown connection id."""
    repository, owner_id, secret_id = setup
    with pytest.raises(ConnectionNotFound):
        await repository.update(build_connection(owner_id, secret_id))


async def test_update_duplicate_name(setup: Setup) -> None:
    """Reject renaming a connection onto another's name."""
    repository, owner_id, secret_id = setup
    await repository.create(build_connection(owner_id, secret_id, name="first"))
    second = await repository.create(
        build_connection(owner_id, secret_id, name="second")
    )

    renamed = second.model_copy(update={"name": "first"})
    with pytest.raises(DuplicateConnectionName):
        await repository.update(renamed)


async def test_delete(setup: Setup) -> None:
    """Delete a stored connection."""
    repository, owner_id, secret_id = setup
    created = await repository.create(build_connection(owner_id, secret_id))

    await repository.delete(created.id)

    with pytest.raises(ConnectionNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown connection id."""
    repository, _, _ = setup
    with pytest.raises(ConnectionNotFound):
        await repository.delete(uuid.uuid4())


async def test_partial_unique_index_rejects_a_second_default() -> None:
    """Reject two defaults for one provider written without clearing the first."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        secrets = SQLSecretRepository(session, AesGcmCipher("test-encryption-key"))
        secret = await secrets.create(
            Secret(owner_id=owner.id, name="values", values={"K": SecretStr("v")})
        )
        repository = SQLConnectionRepository(session)
        await repository.create(
            build_connection(owner.id, secret.id, name="first", default=True)
        )
        session.add(
            ConnectionORM.from_domain(
                build_connection(owner.id, secret.id, name="second", default=True)
            )
        )

        with pytest.raises(IntegrityError):
            await session.flush()
