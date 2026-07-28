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
"""Tests for secret use cases."""

import uuid

import pytest
from pydantic import SecretStr

from conftest import FakeSecretRepository, create_secret
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.secret import SecretFilter
from kitaru.server.application.services.secret_service import SecretService
from kitaru.server.domain.account import Account
from kitaru.server.domain.secret import DuplicateSecretName, SecretNotFound

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))
FOREIGN_ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="bob"))

VALUES = {"username": SecretStr("svc"), "password": SecretStr("hunter2")}


@pytest.fixture
def repository() -> FakeSecretRepository:
    """Provide a fake secret repository."""
    return FakeSecretRepository()


@pytest.fixture
def service(repository: FakeSecretRepository) -> SecretService:
    """Provide a secret service backed by the fake repository."""
    return SecretService(repository=repository)


async def test_create_secret(service: SecretService) -> None:
    """Create a secret owned by the caller."""
    secret = await service.create_secret(
        name="db", type="database", values=VALUES, actor=ACTOR
    )
    assert secret.name == "db"
    assert secret.owner_id == ACTOR.account.id
    assert secret.internal is False
    assert secret.type == "database"
    assert secret.values == VALUES
    assert secret.created is not None
    assert secret.updated is not None


async def test_secret_values_hidden_in_repr(service: SecretService) -> None:
    """Keep plaintext values out of the entity repr."""
    created = await service.create_secret(
        name="db", type=None, values=VALUES, actor=ACTOR
    )
    assert "hunter2" not in repr(created)
    assert "hunter2" not in str(created.values)


async def test_create_secret_duplicate_name(service: SecretService) -> None:
    """Reject a second secret with the same name."""
    await service.create_secret(name="db", type=None, values=VALUES, actor=ACTOR)
    with pytest.raises(
        DuplicateSecretName, match="Secret name 'db' is already registered"
    ):
        await service.create_secret(name="db", type=None, values=VALUES, actor=ACTOR)


async def test_get_secret(service: SecretService) -> None:
    """Load a stored secret by id."""
    created = await service.create_secret(
        name="db", type=None, values=VALUES, actor=ACTOR
    )
    loaded = await service.get_secret(created.id, actor=ACTOR)
    assert loaded == created
    assert loaded.values == VALUES


async def test_get_secret_not_found(service: SecretService) -> None:
    """Raise for an unknown secret id."""
    missing_id = uuid.uuid4()
    with pytest.raises(SecretNotFound, match=f"Secret {missing_id} was not found"):
        await service.get_secret(missing_id, actor=ACTOR)


async def test_get_secret_foreign_owner(service: SecretService) -> None:
    """Read a secret owned by another account."""
    created = await service.create_secret(
        name="db", type=None, values=VALUES, actor=ACTOR
    )
    loaded = await service.get_secret(created.id, actor=FOREIGN_ACTOR)
    assert loaded == created


async def test_get_secret_internal(
    service: SecretService, repository: FakeSecretRepository
) -> None:
    """Report an internal secret as not found."""
    created = await create_secret(repository, ACTOR.account.id, internal=True)
    with pytest.raises(SecretNotFound, match=f"Secret {created.id} was not found"):
        await service.get_secret(created.id, actor=ACTOR)


async def test_list_secrets(service: SecretService) -> None:
    """List secrets newest-first with filters."""
    for name in ["db", "smtp", "s3"]:
        await service.create_secret(name=name, type=None, values=VALUES, actor=ACTOR)

    secrets, next_cursor = await service.list_secrets(SecretFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [secret.name for secret in secrets] == ["s3", "smtp", "db"]

    secrets, next_cursor = await service.list_secrets(
        SecretFilter(name="smtp"), actor=ACTOR
    )
    assert next_cursor is None
    assert secrets[0].name == "smtp"


async def test_list_secrets_walks_pages(service: SecretService) -> None:
    """Walk every page of secrets via next_cursor."""
    for name in ["db", "smtp", "s3"]:
        await service.create_secret(name=name, type=None, values=VALUES, actor=ACTOR)

    collected: list[str] = []
    cursor = None
    while True:
        secrets, next_cursor = await service.list_secrets(
            SecretFilter(cursor=cursor, size=2), actor=ACTOR
        )
        collected.extend(secret.name for secret in secrets)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == ["s3", "smtp", "db"]


async def test_list_secrets_all_owners(service: SecretService) -> None:
    """List secrets of every account."""
    await service.create_secret(name="mine", type=None, values=VALUES, actor=ACTOR)
    await service.create_secret(
        name="theirs", type=None, values=VALUES, actor=FOREIGN_ACTOR
    )

    secrets, next_cursor = await service.list_secrets(SecretFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [secret.name for secret in secrets] == ["theirs", "mine"]


async def test_list_secrets_excludes_internal(
    service: SecretService, repository: FakeSecretRepository
) -> None:
    """Never list internal secrets."""
    await service.create_secret(name="db", type=None, values=VALUES, actor=ACTOR)
    await create_secret(repository, ACTOR.account.id, name="hidden", internal=True)

    secrets, next_cursor = await service.list_secrets(SecretFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [secret.name for secret in secrets] == ["db"]


async def test_list_secrets_excludes_internal_across_cursor(
    service: SecretService, repository: FakeSecretRepository
) -> None:
    """Keep the forced internal=False filter stable across a cursor walk."""
    for name in ["db", "smtp"]:
        await service.create_secret(name=name, type=None, values=VALUES, actor=ACTOR)
    await create_secret(repository, ACTOR.account.id, name="hidden", internal=True)

    collected: list[str] = []
    cursor = None
    while True:
        secrets, next_cursor = await service.list_secrets(
            SecretFilter(cursor=cursor, size=1), actor=ACTOR
        )
        collected.extend(secret.name for secret in secrets)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == ["smtp", "db"]


async def test_update_secret(service: SecretService) -> None:
    """Update the type and values of a secret."""
    created = await service.create_secret(
        name="db", type=None, values=VALUES, actor=ACTOR
    )
    updated = await service.update_secret(
        created.id, type="database", values=None, actor=ACTOR
    )
    assert updated.type == "database"
    assert updated.values == VALUES
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    updated = await service.update_secret(
        created.id, type=None, values={"password": SecretStr("hunter3")}, actor=ACTOR
    )
    assert updated.type == "database"
    assert updated.values == {"password": SecretStr("hunter3")}


async def test_update_secret_not_found(service: SecretService) -> None:
    """Raise for an unknown secret id."""
    with pytest.raises(SecretNotFound):
        await service.update_secret(
            uuid.uuid4(), type="database", values=None, actor=ACTOR
        )


async def test_update_secret_foreign_owner(service: SecretService) -> None:
    """Update a secret owned by another account."""
    created = await service.create_secret(
        name="db", type=None, values=VALUES, actor=ACTOR
    )
    await service.update_secret(
        created.id, type="database", values=None, actor=FOREIGN_ACTOR
    )
    loaded = await service.get_secret(created.id, actor=ACTOR)
    assert loaded.type == "database"


async def test_update_secret_internal(
    service: SecretService, repository: FakeSecretRepository
) -> None:
    """Report an internal secret as not found when updating it."""
    created = await create_secret(repository, ACTOR.account.id, internal=True)
    with pytest.raises(SecretNotFound, match=f"Secret {created.id} was not found"):
        await service.update_secret(
            created.id, type="database", values=None, actor=ACTOR
        )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_delete_secret(service: SecretService) -> None:
    """Delete a stored secret."""
    created = await service.create_secret(
        name="db", type=None, values=VALUES, actor=ACTOR
    )
    await service.delete_secret(created.id, actor=ACTOR)
    with pytest.raises(SecretNotFound):
        await service.get_secret(created.id, actor=ACTOR)


async def test_delete_secret_not_found(service: SecretService) -> None:
    """Raise for an unknown secret id."""
    with pytest.raises(SecretNotFound):
        await service.delete_secret(uuid.uuid4(), actor=ACTOR)


async def test_delete_secret_foreign_owner(service: SecretService) -> None:
    """Delete a secret owned by another account."""
    created = await service.create_secret(
        name="db", type=None, values=VALUES, actor=ACTOR
    )
    await service.delete_secret(created.id, actor=FOREIGN_ACTOR)
    with pytest.raises(SecretNotFound):
        await service.get_secret(created.id, actor=ACTOR)


async def test_delete_secret_internal(
    service: SecretService, repository: FakeSecretRepository
) -> None:
    """Report an internal secret as not found when deleting it."""
    created = await create_secret(repository, ACTOR.account.id, internal=True)
    with pytest.raises(SecretNotFound, match=f"Secret {created.id} was not found"):
        await service.delete_secret(created.id, actor=ACTOR)
    loaded = await repository.get(created.id)
    assert loaded == created
