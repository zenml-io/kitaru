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
"""Tests for connection use cases."""

import uuid

import pytest
from pydantic import SecretStr

from conftest import FakeConnectionRepository, FakeSecretRepository
from kitaru.api_models.v1.filter import FilterOp
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.connection import (
    ConnectionCreate,
    ConnectionFilter,
)
from kitaru.server.application.services.connection_service import (
    ConnectionService,
    build_internal_secret_name,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.connection import (
    ConnectionNotFound,
    DuplicateConnectionName,
    InvalidConnectionValues,
)
from kitaru.server.domain.names import validate_name
from kitaru.server.domain.secret import SecretNotFound
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))

SECRETS = {
    "LANGFUSE_PUBLIC_KEY": SecretStr("pk"),
    "LANGFUSE_SECRET_KEY": SecretStr("sk"),
}
ENV = {"LANGFUSE_BASE_URL": "https://cloud.langfuse.com"}


@pytest.fixture
def repository() -> FakeConnectionRepository:
    """Provide a fake connection repository."""
    return FakeConnectionRepository()


@pytest.fixture
def secret_repository() -> FakeSecretRepository:
    """Provide a fake secret repository."""
    return FakeSecretRepository()


@pytest.fixture
def service(
    repository: FakeConnectionRepository, secret_repository: FakeSecretRepository
) -> ConnectionService:
    """Provide a connection service backed by the fake repositories."""
    return ConnectionService(repository=repository, secret_repository=secret_repository)


def build_command(
    name: str = "langfuse-prod",
    provider: str = "langfuse",
    env: dict[str, str] | None = None,
    secrets: dict[str, SecretStr] | None = None,
    default: bool = False,
) -> ConnectionCreate:
    """Build a connection create command.

    Args:
        name: Connection name.
        provider: Provider the connection addresses.
        env: Non-secret values.
        secrets: Sensitive values.
        default: Whether the connection is the provider's default.

    Returns:
        Create command.
    """
    return ConnectionCreate(
        name=name,
        provider=provider,
        env=env if env is not None else dict(ENV),
        secrets=secrets if secrets is not None else dict(SECRETS),
        default=default,
    )


async def test_create_connection(
    service: ConnectionService, secret_repository: FakeSecretRepository
) -> None:
    """Create a connection owned by the caller with its internal secret."""
    connection = await service.create_connection(build_command(), actor=ACTOR)

    assert connection.name == "langfuse-prod"
    assert connection.provider == "langfuse"
    assert connection.owner_id == ACTOR.account.id
    assert connection.env == ENV
    assert connection.default is False
    secret = await secret_repository.get(connection.secret_id)
    assert secret.internal is True
    assert secret.owner_id == ACTOR.account.id
    assert secret.values == SECRETS


async def test_internal_secret_name_is_a_valid_name(
    service: ConnectionService, secret_repository: FakeSecretRepository
) -> None:
    """Name the internal secret after the connection id."""
    connection = await service.create_connection(build_command(), actor=ACTOR)

    secret = await secret_repository.get(connection.secret_id)
    assert secret.name == build_internal_secret_name(connection.id)
    assert validate_name(secret.name) == secret.name


async def test_create_connection_duplicate_name(service: ConnectionService) -> None:
    """Reject a second connection with the same name."""
    await service.create_connection(build_command(), actor=ACTOR)

    with pytest.raises(
        DuplicateConnectionName,
        match="Connection name 'langfuse-prod' is already registered",
    ):
        await service.create_connection(build_command(), actor=ACTOR)


async def test_create_connection_rejects_reserved_env_key(
    service: ConnectionService,
) -> None:
    """Reject an env key under the reserved prefix."""
    with pytest.raises(InvalidConnectionValues):
        await service.create_connection(
            build_command(env={"KITARU_API_URL": "https://example.com"}), actor=ACTOR
        )


async def test_create_connection_rejects_reserved_secret_key(
    service: ConnectionService,
) -> None:
    """Reject a secret key under the reserved prefix."""
    with pytest.raises(InvalidConnectionValues):
        await service.create_connection(
            build_command(secrets={"KITARU_API_KEY": SecretStr("k")}), actor=ACTOR
        )


async def test_create_connection_rejects_overlapping_key(
    service: ConnectionService,
) -> None:
    """Reject a key set as both an env value and a secret."""
    with pytest.raises(InvalidConnectionValues):
        await service.create_connection(
            build_command(
                env={"LANGFUSE_BASE_URL": "https://example.com"},
                secrets={"LANGFUSE_BASE_URL": SecretStr("x")},
            ),
            actor=ACTOR,
        )


async def test_create_default_clears_the_previous_default(
    service: ConnectionService,
) -> None:
    """Leave one default per provider when a second default is created."""
    first = await service.create_connection(
        build_command(name="first", default=True), actor=ACTOR
    )
    second = await service.create_connection(
        build_command(name="second", default=True), actor=ACTOR
    )

    assert (await service.get_connection(first.id, actor=ACTOR)).default is False
    assert (await service.get_connection(second.id, actor=ACTOR)).default is True


async def test_get_connection_not_found(service: ConnectionService) -> None:
    """Raise for an unknown connection id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ConnectionNotFound, match=f"Connection {missing_id} was not found"
    ):
        await service.get_connection(missing_id, actor=ACTOR)


async def test_list_connections(service: ConnectionService) -> None:
    """List connections matching a filter."""
    await service.create_connection(build_command(name="first"), actor=ACTOR)
    await service.create_connection(
        build_command(name="second", provider="langsmith", secrets={}, env={}),
        actor=ACTOR,
    )

    connections, next_cursor = await service.list_connections(
        ConnectionFilter(
            expression=FilterCondition(
                field="provider", op=FilterOp.EQ, value="langsmith"
            )
        ),
        actor=ACTOR,
    )

    assert next_cursor is None
    assert [connection.name for connection in connections] == ["second"]


async def test_update_connection_merges_env_and_secrets(
    service: ConnectionService, secret_repository: FakeSecretRepository
) -> None:
    """Upsert env and secret entries by key instead of replacing them."""
    created = await service.create_connection(build_command(), actor=ACTOR)

    updated = await service.update_connection(
        created.id,
        env={"LANGFUSE_BASE_URL": "https://eu.langfuse.com", "EXTRA": "1"},
        secrets={"LANGFUSE_SECRET_KEY": SecretStr("rotated")},
        default=None,
        actor=ACTOR,
    )

    assert updated.env == {
        "LANGFUSE_BASE_URL": "https://eu.langfuse.com",
        "EXTRA": "1",
    }
    secret = await secret_repository.get(updated.secret_id)
    assert secret.values == {
        "LANGFUSE_PUBLIC_KEY": SecretStr("pk"),
        "LANGFUSE_SECRET_KEY": SecretStr("rotated"),
    }


async def test_update_connection_rejects_overlapping_key(
    service: ConnectionService,
) -> None:
    """Reject an env key that the internal secret already holds."""
    created = await service.create_connection(build_command(), actor=ACTOR)

    with pytest.raises(InvalidConnectionValues):
        await service.update_connection(
            created.id,
            env={"LANGFUSE_PUBLIC_KEY": "leaked"},
            secrets=None,
            default=None,
            actor=ACTOR,
        )


async def test_update_connection_sets_the_default(service: ConnectionService) -> None:
    """Clear the provider's previous default when another takes over."""
    first = await service.create_connection(
        build_command(name="first", default=True), actor=ACTOR
    )
    second = await service.create_connection(build_command(name="second"), actor=ACTOR)

    await service.update_connection(
        second.id, env=None, secrets=None, default=True, actor=ACTOR
    )

    assert (await service.get_connection(first.id, actor=ACTOR)).default is False
    assert (await service.get_connection(second.id, actor=ACTOR)).default is True


async def test_update_connection_not_found(service: ConnectionService) -> None:
    """Raise for an unknown connection id."""
    missing_id = uuid.uuid4()
    with pytest.raises(ConnectionNotFound):
        await service.update_connection(
            missing_id, env=None, secrets=None, default=None, actor=ACTOR
        )


async def test_delete_connection_deletes_its_secret(
    service: ConnectionService, secret_repository: FakeSecretRepository
) -> None:
    """Delete the connection and the internal secret holding its values."""
    created = await service.create_connection(build_command(), actor=ACTOR)

    await service.delete_connection(created.id, actor=ACTOR)

    with pytest.raises(ConnectionNotFound):
        await service.get_connection(created.id, actor=ACTOR)
    with pytest.raises(SecretNotFound):
        await secret_repository.get(created.secret_id)


async def test_delete_connection_not_found(service: ConnectionService) -> None:
    """Raise for an unknown connection id."""
    missing_id = uuid.uuid4()
    with pytest.raises(ConnectionNotFound):
        await service.delete_connection(missing_id, actor=ACTOR)
