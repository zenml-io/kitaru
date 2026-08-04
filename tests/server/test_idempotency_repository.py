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
"""PostgreSQL contract tests for idempotency reservations."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import pg_session_with_engine, postgres_available
from kitaru.server.adapters.db.orm.idempotency_record import IdempotencyRecordORM
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.idempotency_repository import (
    SQLIdempotencyRepository,
)
from kitaru.server.application.models.idempotency import (
    IdempotencyActorScope,
    IdempotencyClaimKind,
    IdempotencyRequest,
    IdempotencyReservation,
    IdempotencyStoredResponse,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.idempotency import IdempotencyRecord, IdempotencyState

NOW = datetime(2026, 8, 4, 10, 0, tzinfo=UTC)


def _record(owner_id: uuid.UUID, key: str = "request-1") -> IdempotencyRecord:
    return IdempotencyRecord(
        actor_account_id=owner_id,
        actor_principal_kind="account",
        actor_principal_identity=str(owner_id),
        method="POST",
        route="/v1/replays",
        caller_key=key,
        fingerprint="a" * 64,
    )


def _reservation(record: IdempotencyRecord) -> IdempotencyReservation:
    return IdempotencyReservation(
        record_id=record.id,
        actor=IdempotencyActorScope(
            account_id=record.actor_account_id,
            principal_kind="account",
            principal_identity=record.actor_principal_identity,
        ),
        request=IdempotencyRequest(
            method=record.method,
            route=record.route,
            caller_key=record.caller_key,
            fingerprint=record.fingerprint,
        ),
    )


@pytest.fixture(autouse=True)
async def require_postgres() -> None:
    """Skip this module when the local PostgreSQL service is absent."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")


async def test_reserve_complete_and_read_authoritative_response() -> None:
    """A committed result wins subsequent reservations for the same scope."""
    async with pg_session_with_engine() as (session, engine):
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        await session.commit()
        first_repository = SQLIdempotencyRepository(session)
        record = _record(owner.id)
        first = await first_repository.reserve(record, 1)
        assert first.kind is IdempotencyClaimKind.OWNED
        completed = await first_repository.complete(
            _reservation(record),
            IdempotencyStoredResponse(
                status_code=201,
                body=b'{"id":"stable"}',
                headers={"content-type": "application/json"},
            ),
            NOW,
            NOW + timedelta(hours=24),
        )
        assert completed.state is IdempotencyState.COMPLETED
        await session.commit()

        session_factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        async with session_factory() as duplicate_session:
            duplicate = await SQLIdempotencyRepository(duplicate_session).reserve(
                _record(owner.id), 1
            )
            assert duplicate.kind is IdempotencyClaimKind.EXISTING
            assert duplicate.record is not None
            assert duplicate.record.response_body == b'{"id":"stable"}'


async def test_lock_timeout_keeps_the_waiter_transaction_usable() -> None:
    """A bounded conflict wait rolls back only its savepoint."""
    async with pg_session_with_engine() as (setup_session, engine):
        owner = await SQLAccountRepository(setup_session).create(Account(name="owner"))
        await setup_session.commit()
        session_factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        async with (
            session_factory() as owner_session,
            session_factory() as waiter_session,
        ):
            record = _record(owner.id)
            owned = await SQLIdempotencyRepository(owner_session).reserve(record, 1)
            assert owned.kind is IdempotencyClaimKind.OWNED

            waiter_repository = SQLIdempotencyRepository(waiter_session)
            timed_out = await waiter_repository.reserve(_record(owner.id), 0.05)
            assert timed_out.kind is IdempotencyClaimKind.TIMED_OUT
            assert await waiter_session.scalar(select(1)) == 1

            await owner_session.rollback()
            takeover = await waiter_repository.reserve(_record(owner.id), 1)
            assert takeover.kind is IdempotencyClaimKind.OWNED


async def test_waiter_takes_over_after_predecessor_rollback() -> None:
    """ON CONFLICT waits and inserts when the predecessor rolls back."""
    async with pg_session_with_engine() as (setup_session, engine):
        owner = await SQLAccountRepository(setup_session).create(Account(name="owner"))
        await setup_session.commit()
        session_factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        async with (
            session_factory() as owner_session,
            session_factory() as waiter_session,
        ):
            record = _record(owner.id)
            await SQLIdempotencyRepository(owner_session).reserve(record, 1)
            waiter = asyncio.create_task(
                SQLIdempotencyRepository(waiter_session).reserve(_record(owner.id), 2)
            )
            await asyncio.sleep(0.05)
            await owner_session.rollback()
            assert (await waiter).kind is IdempotencyClaimKind.OWNED


async def test_cleanup_is_bounded_and_never_deletes_pending_records() -> None:
    """Cleanup uses expiry ordering and retains in-flight reservations."""
    async with pg_session_with_engine() as (session, _):
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        await session.commit()
        repository = SQLIdempotencyRepository(session)
        for index in range(3):
            record = _record(owner.id, key=f"completed-{index}")
            await repository.reserve(record, 1)
            await repository.complete(
                _reservation(record),
                IdempotencyStoredResponse(status_code=201, body=b"{}", headers={}),
                NOW - timedelta(days=2),
                NOW - timedelta(days=1, seconds=-index),
            )
        pending = _record(owner.id, key="pending")
        await repository.reserve(pending, 1)
        await session.commit()

        assert await repository.cleanup_expired(NOW, limit=2) == 2
        await session.commit()
        remaining = await session.scalar(
            select(func.count()).select_from(IdempotencyRecordORM)
        )
        assert remaining == 2
        pending_row = await session.get(IdempotencyRecordORM, pending.id)
        assert pending_row is not None
        assert pending_row.state == IdempotencyState.PENDING.value
