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

import uuid
from collections.abc import AsyncGenerator, Iterator
from typing import Any

import boto3
import pytest
from moto.server import ThreadedMotoServer
from pydantic import SecretStr

from conftest import FakeBlobDataStore, pg_session, postgres_available
from kitaru.server.adapters.db.blob_data_store import DatabaseBlobDataStore
from kitaru.server.application.interfaces.blob_data_store import BlobDataStore
from kitaru.server.blob_storage_settings import S3BlobStorageSettings
from kitaru.server.domain.blob import BlobContentNotFound


@pytest.fixture(scope="module")
def s3_endpoint() -> Iterator[str]:
    """Run an in-process moto S3 server and yield its endpoint URL."""
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


def _create_s3_client(endpoint: str) -> Any:
    """Create a boto3 S3 client for the moto server.

    Args:
        endpoint: Moto server endpoint URL.

    Returns:
        S3 client.
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _create_s3_store(
    endpoint: str, monkeypatch: pytest.MonkeyPatch
) -> tuple[BlobDataStore, str]:
    """Create a fresh bucket on the moto server and a store over it.

    Args:
        endpoint: Moto server endpoint URL.
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The store and the name of its bucket.
    """
    pytest.importorskip("obstore")
    from kitaru.server.adapters.blobstore.s3 import S3BlobDataStore

    # obstore rejects plain-HTTP endpoints unless this is set.
    monkeypatch.setenv("AWS_ALLOW_HTTP", "true")
    bucket = f"kitaru-{uuid.uuid4().hex}"
    _create_s3_client(endpoint).create_bucket(Bucket=bucket)
    settings = S3BlobStorageSettings(
        bucket=bucket,
        region="us-east-1",
        endpoint_url=endpoint,
        access_key_id="test",
        secret_access_key=SecretStr("test"),
    )
    return S3BlobDataStore(settings), bucket


@pytest.fixture(params=["fake", "postgres", "s3"])
async def store(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> AsyncGenerator[BlobDataStore, None]:
    """Provide each blob data store implementation."""
    if request.param == "fake":
        yield FakeBlobDataStore()
        return
    if request.param == "s3":
        yield _create_s3_store(request.getfixturevalue("s3_endpoint"), monkeypatch)[0]
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
    with pytest.raises(
        BlobContentNotFound, match=f"Blob content for sha256 {'a' * 64} was not found"
    ):
        await store.get("a" * 64)


async def test_delete(store: BlobDataStore) -> None:
    """Delete stored content."""
    await store.put("a" * 64, b"content")
    await store.delete("a" * 64)
    with pytest.raises(BlobContentNotFound):
        await store.get("a" * 64)


async def test_delete_missing_is_idempotent(store: BlobDataStore) -> None:
    """Delete a hash with no stored content without raising."""
    await store.delete("a" * 64)


async def test_put_many_and_get_many(store: BlobDataStore) -> None:
    """Store multiple contents and load them back by hash in one call."""
    await store.put_many({"a" * 64: b"first", "b" * 64: b"second"})
    assert await store.get_many(["a" * 64, "b" * 64]) == {
        "a" * 64: b"first",
        "b" * 64: b"second",
    }


async def test_put_many_is_idempotent_on_repeat_hashes(store: BlobDataStore) -> None:
    """Keep the first write on a repeat put_many of the same hashes."""
    await store.put_many({"a" * 64: b"first"})
    await store.put_many({"a" * 64: b"first"})
    assert await store.get_many(["a" * 64]) == {"a" * 64: b"first"}


async def test_put_many_empty_is_a_no_op(store: BlobDataStore) -> None:
    """Accept an empty put_many without raising."""
    await store.put_many({})


async def test_get_many_missing_hash_raises(store: BlobDataStore) -> None:
    """Raise for a hash with no stored content among the requested hashes."""
    await store.put_many({"a" * 64: b"content"})
    with pytest.raises(
        BlobContentNotFound, match=f"Blob content for sha256 {'b' * 64} was not found"
    ):
        await store.get_many(["a" * 64, "b" * 64])


async def test_get_many_empty_is_a_no_op(store: BlobDataStore) -> None:
    """Return an empty mapping for an empty get_many."""
    assert await store.get_many([]) == {}


async def test_s3_stores_objects_under_prefix(
    s3_endpoint: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Store S3 content under the default key prefix."""
    store, bucket = _create_s3_store(s3_endpoint, monkeypatch)
    await store.put("a" * 64, b"content")
    listed = _create_s3_client(s3_endpoint).list_objects_v2(Bucket=bucket)
    assert [obj["Key"] for obj in listed["Contents"]] == [f"blobs/{'a' * 64}"]
