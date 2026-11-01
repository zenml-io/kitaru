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
"""Payload store tests against PostgreSQL."""

import asyncio
import hashlib
import json
from collections import Counter
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from conftest import capture_statements, pg_session_with_engine, postgres_available
from kitaru.server.adapters.db.blob_data_store import DatabaseBlobDataStore
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.application.interfaces.blob_data_store import BlobDataStores
from kitaru.server.application.payload_store import PayloadStore
from kitaru.server.domain.account import Account
from kitaru.server.domain.blob import BlobStorageBackend
from kitaru.server.domain.payload import Payload, PayloadMediaType

THRESHOLD_BYTES = 64


@pytest.fixture
async def database() -> AsyncGenerator[tuple[AsyncSession, AsyncEngine], None]:
    """Provide a session and its engine on a fresh test database."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        yield session, engine


def _build_store(session: AsyncSession) -> PayloadStore:
    return PayloadStore(
        SQLBlobRepository(session),
        BlobDataStores(
            {BlobStorageBackend.DATABASE: DatabaseBlobDataStore(session)},
            BlobStorageBackend.DATABASE,
        ),
        THRESHOLD_BYTES,
    )


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


async def _wait_until_blocked(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Return once a backend on this database waits on a row lock."""
    async with session_factory() as session:
        for _ in range(500):
            rows = (
                await session.execute(
                    text(
                        "SELECT pid FROM pg_stat_activity "
                        "WHERE wait_event_type = 'Lock' "
                        "AND datname = current_database()"
                    )
                )
            ).all()
            await session.rollback()
            if rows:
                return
            await asyncio.sleep(0.01)
    pytest.fail("No backend waited on a row lock")


async def test_offload_stores_one_row_per_new_content_and_media_type(
    database: tuple[AsyncSession, AsyncEngine],
) -> None:
    """Store one registry row per new (sha256, media_type), reusing stored rows."""
    session, _ = database
    owner = await SQLAccountRepository(session).create(Account(name="owner"))
    store = _build_store(session)
    known_value = {"known": "k" * 100}
    known = Payload.from_json(known_value)
    await store.offload([known], owner.id)

    padded = "x" * 100
    first_value = {"first": "a" * 100}
    first = Payload.from_json(first_value)
    repeat = Payload.from_json(first_value)
    # The JSON encoding of the plain string equals the raw text of the quoted
    # string, so these two share content bytes but not a media type.
    text_payload = Payload.from_text(f'"{padded}"')
    json_payload = Payload.from_json(padded)
    small = Payload.from_json({"small": 1})
    known_again = Payload.from_json(known_value)
    await store.offload(
        [first, repeat, text_payload, json_payload, small, known_again], owner.id
    )

    contents = {
        (_json_bytes(known_value), PayloadMediaType.JSON),
        (_json_bytes(first_value), PayloadMediaType.JSON),
        (_json_bytes(padded), PayloadMediaType.TEXT),
        (_json_bytes(padded), PayloadMediaType.JSON),
    }
    rows = (
        await session.execute(
            text("SELECT id, owner_id, sha256, size, media_type, stored_in FROM blob")
        )
    ).all()
    assert {row[1:] for row in rows} == {
        (owner.id, hashlib.sha256(data).hexdigest(), len(data), media_type, "database")
        for data, media_type in contents
    }
    content_hashes = (
        await session.execute(text("SELECT sha256 FROM blob_content"))
    ).scalars()
    assert set(content_hashes) == {
        hashlib.sha256(data).hexdigest() for data, _ in contents
    }

    id_by_key = {(row.sha256, row.media_type): row.id for row in rows}
    for payload, data in [
        (first, _json_bytes(first_value)),
        (repeat, _json_bytes(first_value)),
        (text_payload, _json_bytes(padded)),
        (json_payload, _json_bytes(padded)),
        (known_again, _json_bytes(known_value)),
    ]:
        assert payload.media_type is not None
        key = (hashlib.sha256(data).hexdigest(), payload.media_type)
        assert payload.blob_id == id_by_key[key]
    assert known_again.blob_id == known.blob_id
    assert small.blob_id is None


async def test_offload_registers_new_blobs_in_one_statement(
    database: tuple[AsyncSession, AsyncEngine],
) -> None:
    """Register every new blob of a batch with one INSERT and no savepoints."""
    session, engine = database
    owner = await SQLAccountRepository(session).create(Account(name="owner"))
    store = _build_store(session)
    payloads = [Payload.from_json({"index": i, "pad": "p" * 100}) for i in range(50)]

    with capture_statements(engine) as statements:
        await store.offload(payloads, owner.id)

    # One registry read, one content write, one registry write.
    assert Counter(statement.split()[0] for statement in statements) == {
        "SELECT": 1,
        "INSERT": 2,
    }
    blob_count = (await session.execute(text("SELECT count(*) FROM blob"))).scalar()
    assert blob_count == 50


async def test_offload_reuses_the_row_a_concurrent_offload_commits(
    database: tuple[AsyncSession, AsyncEngine],
) -> None:
    """Point both payloads at one row when two transactions offload one value."""
    session, engine = database
    owner = await SQLAccountRepository(session).create(Account(name="owner"))
    await session.commit()
    session_factory = async_sessionmaker(
        bind=engine, class_=AsyncSession, expire_on_commit=False
    )
    shared_value = {"shared": "s" * 100}
    writer_payload = Payload.from_json(shared_value)
    reader_payload = Payload.from_json(shared_value)
    fresh = Payload.from_json({"fresh": "f" * 100})

    async with session_factory() as writer:
        await _build_store(writer).offload([writer_payload], owner.id)
        reader = asyncio.create_task(
            _build_store(session).offload([reader_payload, fresh], owner.id)
        )
        await _wait_until_blocked(session_factory)
        await writer.commit()
    await asyncio.wait_for(reader, timeout=20)

    assert writer_payload.blob_id is not None
    assert reader_payload.blob_id == writer_payload.blob_id
    assert fresh.blob_id is not None
    shared_hash = hashlib.sha256(_json_bytes(shared_value)).hexdigest()
    shared_rows = (
        await session.execute(
            text("SELECT id FROM blob WHERE sha256 = :sha256"), {"sha256": shared_hash}
        )
    ).scalars()
    assert list(shared_rows) == [writer_payload.blob_id]
