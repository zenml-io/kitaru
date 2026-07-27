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
"""Contract tests for blob repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakeBlobRepository, pg_session, postgres_available
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.blob_repository import (
    SQLBlobRepository,
)
from kitaru.server.application.interfaces.blob_repository import (
    BlobRepository,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.blob import (
    Blob,
    BlobNotFound,
    DuplicateBlobContent,
)

Setup = tuple[BlobRepository, uuid.UUID]

CONTENT = b"def score(session):\n    return 1.0\n"


def blob(owner_id: uuid.UUID, sha256: str = "a" * 64, data: bytes = CONTENT) -> Blob:
    """Build a blob holding inline content.

    Args:
        owner_id: Id of the owning account.
        sha256: Hash of the content.
        data: Inline content.

    Returns:
        Blob without timestamps set.
    """
    return Blob(
        owner_id=owner_id,
        sha256=sha256,
        size=len(data),
        media_type="text/x-python",
        data=data,
    )


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each blob repository implementation plus an owner id."""
    if request.param == "fake":
        yield FakeBlobRepository(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning account first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield SQLBlobRepository(session), owner.id


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new blob with the creation timestamp set."""
    repository, owner_id = setup
    stored = await repository.create(blob(owner_id))
    assert stored.owner_id == owner_id
    assert stored.sha256 == "a" * 64
    assert stored.size == len(CONTENT)
    assert stored.media_type == "text/x-python"
    assert stored.data == CONTENT
    assert stored.uri is None
    assert stored.created is not None


async def test_create_duplicate_content(setup: Setup) -> None:
    """Reject a second blob with the same content hash."""
    repository, owner_id = setup
    await repository.create(blob(owner_id))
    with pytest.raises(
        DuplicateBlobContent, match=f"Blob content '{'a' * 64}' is already stored"
    ):
        await repository.create(blob(owner_id))


async def test_create_after_duplicate_failure(setup: Setup) -> None:
    """Keep the repository usable after a duplicate content failure."""
    repository, owner_id = setup
    await repository.create(blob(owner_id))
    with pytest.raises(DuplicateBlobContent):
        await repository.create(blob(owner_id))
    stored = await repository.create(blob(owner_id, sha256="b" * 64))
    assert stored.sha256 == "b" * 64


async def test_get(setup: Setup) -> None:
    """Load a stored blob by id with its content round-tripped."""
    repository, owner_id = setup
    created = await repository.create(blob(owner_id))
    loaded = await repository.get(created.id)
    assert loaded == created
    assert loaded.data == CONTENT


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown blob id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(BlobNotFound, match=f"Blob {missing_id} was not found"):
        await repository.get(missing_id)


async def test_get_by_sha256(setup: Setup) -> None:
    """Load a stored blob by content hash."""
    repository, owner_id = setup
    created = await repository.create(blob(owner_id))
    loaded = await repository.get_by_sha256("a" * 64)
    assert loaded == created


async def test_get_by_sha256_missing(setup: Setup) -> None:
    """Report unstored content as missing."""
    repository, _ = setup
    assert await repository.get_by_sha256("c" * 64) is None


async def test_get_hashes(setup: Setup) -> None:
    """Load content hashes by id and omit ids that do not resolve."""
    repository, owner_id = setup
    first = await repository.create(blob(owner_id))
    second = await repository.create(blob(owner_id, sha256="b" * 64))
    hashes = await repository.get_hashes([first.id, second.id, uuid.uuid4()])
    assert hashes == {first.id: "a" * 64, second.id: "b" * 64}


async def test_get_hashes_without_ids(setup: Setup) -> None:
    """Load nothing for an empty id list."""
    repository, _ = setup
    assert await repository.get_hashes([]) == {}
