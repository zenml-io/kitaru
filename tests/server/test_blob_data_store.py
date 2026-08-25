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
"""Contract tests for blob data stores."""

from collections.abc import AsyncGenerator

import pytest

from conftest import FakeBlobDataStore, pg_session, postgres_available
from kitaru.server.adapters.db.blob_data_store import DatabaseBlobDataStore
from kitaru.server.application.interfaces.blob_data_store import BlobDataStore
from kitaru.server.domain.blob import BlobNotFound


@pytest.fixture(params=["fake", "postgres"])
async def store(request: pytest.FixtureRequest) -> AsyncGenerator[BlobDataStore, None]:
    """Provide each blob data store implementation."""
    if request.param == "fake":
        yield FakeBlobDataStore()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        yield DatabaseBlobDataStore(session)


async def test_put_and_get(store: BlobDataStore) -> None:
    """Store content and load it back by hash."""
    await store.put("a" * 64, b"content")
    assert await store.get("a" * 64) == b"content"


async def test_put_is_idempotent(store: BlobDataStore) -> None:
    """Keep the first write on a repeat put of the same hash."""
    await store.put("a" * 64, b"first")
    await store.put("a" * 64, b"first")
    assert await store.get("a" * 64) == b"first"


async def test_get_not_found(store: BlobDataStore) -> None:
    """Raise for a hash with no stored content."""
    with pytest.raises(BlobNotFound, match=f"Blob {'a' * 64} was not found"):
        await store.get("a" * 64)


async def test_delete(store: BlobDataStore) -> None:
    """Delete stored content."""
    await store.put("a" * 64, b"content")
    await store.delete("a" * 64)
    with pytest.raises(BlobNotFound):
        await store.get("a" * 64)


async def test_delete_missing_is_idempotent(store: BlobDataStore) -> None:
    """Delete a hash with no stored content without raising."""
    await store.delete("a" * 64)
