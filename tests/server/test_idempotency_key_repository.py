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
"""Contract tests for idempotency key repositories."""

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta

import pytest

from conftest import (
    FakeIdempotencyKeyRepository,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.server.adapters.db.encryption import AesGcmCipher
from kitaru.server.adapters.db.orm.idempotency_key import IdempotencyKeyORM
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.idempotency_key_repository import (
    SQLIdempotencyKeyRepository,
)
from kitaru.server.application.interfaces.idempotency_key_repository import (
    IdempotencyKeyRepository,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.idempotency_key import (
    IdempotencyKey,
    IdempotencyKeyAlreadyExists,
    IdempotencyKeyResponseUndecryptable,
)

Setup = tuple[IdempotencyKeyRepository, uuid.UUID, uuid.UUID]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each idempotency key repository implementation and two account ids."""
    if request.param == "fake":
        yield FakeIdempotencyKeyRepository(), uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, _engine):
        # The account_id column has a foreign key to the account table, so
        # store the owning accounts first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        other_owner = await accounts.create(Account(name="other-owner"))
        yield (
            SQLIdempotencyKeyRepository(session, AesGcmCipher("test-key")),
            owner.id,
            other_owner.id,
        )


async def test_create_and_get_roundtrip(setup: Setup) -> None:
    """Store a new idempotency key and load it back by account and key."""
    repository, account_id, _ = setup
    created = await repository.create(
        IdempotencyKey(
            account_id=account_id,
            key="request-1",
            fingerprint="f" * 64,
            method="POST",
            path="/api/v1/agents",
        )
    )
    assert created.created is not None

    loaded = await repository.get(account_id, "request-1")
    assert loaded == created


async def test_get_missing_returns_none(setup: Setup) -> None:
    """Return None for a key that was never stored."""
    repository, account_id, _ = setup
    assert await repository.get(account_id, "missing") is None


async def test_create_duplicate_key_raises(setup: Setup) -> None:
    """Raise when the same account reuses a key."""
    repository, account_id, _ = setup
    await repository.create(
        IdempotencyKey(
            account_id=account_id,
            key="request-1",
            fingerprint="f" * 64,
            method="POST",
            path="/api/v1/agents",
        )
    )
    with pytest.raises(IdempotencyKeyAlreadyExists):
        await repository.create(
            IdempotencyKey(
                account_id=account_id,
                key="request-1",
                fingerprint="0" * 64,
                method="POST",
                path="/api/v1/agents",
            )
        )


async def test_create_same_key_different_account_allowed(setup: Setup) -> None:
    """Allow two accounts to use the same key independently."""
    repository, account_id, other_account_id = setup
    first = await repository.create(
        IdempotencyKey(
            account_id=account_id,
            key="request-1",
            fingerprint="f" * 64,
            method="POST",
            path="/api/v1/agents",
        )
    )
    second = await repository.create(
        IdempotencyKey(
            account_id=other_account_id,
            key="request-1",
            fingerprint="f" * 64,
            method="POST",
            path="/api/v1/agents",
        )
    )
    assert first.id != second.id
    assert await repository.get(account_id, "request-1") == first
    assert await repository.get(other_account_id, "request-1") == second


async def test_store_response_roundtrip(setup: Setup) -> None:
    """Persist the response bytes, status, and content type under a key."""
    repository, account_id, _ = setup
    created = await repository.create(
        IdempotencyKey(
            account_id=account_id,
            key="request-1",
            fingerprint="f" * 64,
            method="POST",
            path="/api/v1/agents",
        )
    )
    await repository.store_response(
        created.id,
        response_status=201,
        response_body=b'{"id": "abc"}',
        response_content_type="application/json",
    )

    loaded = await repository.get(account_id, "request-1")
    assert loaded is not None
    assert loaded.response_status == 201
    assert loaded.response_body == b'{"id": "abc"}'
    assert loaded.response_content_type == "application/json"


async def test_store_response_encrypted_roundtrip() -> None:
    """Store the body encrypted at rest, load it as stored, and decrypt it."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, _engine):
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        repository = SQLIdempotencyKeyRepository(session, AesGcmCipher("test-key"))
        created = await repository.create(
            IdempotencyKey(
                account_id=owner.id,
                key="request-1",
                fingerprint="f" * 64,
                method="POST",
                path="/api/v1/api-keys",
            )
        )
        await repository.store_response(
            created.id,
            response_status=201,
            response_body=b'{"key": "secret"}',
            response_content_type="application/json",
            encrypt=True,
        )

        row = await session.get(IdempotencyKeyORM, created.id)
        assert row is not None
        assert row.response_body is not None
        assert b"secret" not in row.response_body

        loaded = await repository.get(owner.id, "request-1")
        assert loaded is not None
        assert loaded.response_body == row.response_body
        assert (
            repository.decrypt_response_body(loaded.response_body)
            == b'{"key": "secret"}'
        )


async def test_decrypt_response_body_rejects_plaintext(setup: Setup) -> None:
    """Raise when the body was not stored encrypted."""
    repository, _, _ = setup
    with pytest.raises(IdempotencyKeyResponseUndecryptable):
        repository.decrypt_response_body(b"not-encrypted")


async def test_delete_expired_with_zero_limit_deletes_nothing(setup: Setup) -> None:
    """Treat a non-positive limit as an empty batch."""
    repository, account_id, _ = setup
    created = await repository.create(
        IdempotencyKey(
            account_id=account_id,
            key="old",
            fingerprint="f" * 64,
            method="POST",
            path="/api/v1/agents",
        )
    )
    assert created.created is not None
    cutoff = created.created + timedelta(seconds=1)

    assert await repository.delete_expired(cutoff, limit=0) == 0
    assert await repository.delete_expired(cutoff, limit=-1) == 0
    assert await repository.get(account_id, "old") == created


async def test_delete_expired_respects_cutoff(setup: Setup) -> None:
    """Delete only keys created before the cutoff."""
    repository, account_id, _ = setup
    await repository.create(
        IdempotencyKey(
            account_id=account_id,
            key="old",
            fingerprint="f" * 64,
            method="POST",
            path="/api/v1/agents",
        )
    )
    fresh = await repository.create(
        IdempotencyKey(
            account_id=account_id,
            key="fresh",
            fingerprint="f" * 64,
            method="POST",
            path="/api/v1/agents",
        )
    )

    assert fresh.created is not None
    deleted = await repository.delete_expired(fresh.created, limit=100)

    assert deleted == 1
    assert await repository.get(account_id, "old") is None
    assert await repository.get(account_id, "fresh") == fresh


async def test_delete_expired_respects_limit(setup: Setup) -> None:
    """Delete no more than the requested batch limit."""
    repository, account_id, _ = setup
    for i in range(3):
        await repository.create(
            IdempotencyKey(
                account_id=account_id,
                key=f"key-{i}",
                fingerprint="f" * 64,
                method="POST",
                path="/api/v1/agents",
            )
        )

    cutoff = datetime.now(UTC) + timedelta(minutes=1)
    deleted = await repository.delete_expired(cutoff, limit=2)

    assert deleted == 2
