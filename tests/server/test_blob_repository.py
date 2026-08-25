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

import hashlib
import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakeBlobRepository, pg_session, postgres_available
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.adapters.db.repositories.task_repository import SQLTaskRepository
from kitaru.server.application.interfaces.blob_repository import BlobRepository
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.blob import (
    Blob,
    BlobInUse,
    BlobNotFound,
    BlobStorageBackend,
)
from kitaru.server.domain.job import Job
from kitaru.server.domain.plugin import (
    PackagePluginSource,
    Plugin,
    PluginKind,
    ScriptPluginSource,
)
from kitaru.server.domain.task import ImportTask

Setup = tuple[BlobRepository, uuid.UUID]


def _blob(owner_id: uuid.UUID | None, content: bytes = b"content") -> Blob:
    return Blob(
        owner_id=owner_id,
        sha256=hashlib.sha256(content).hexdigest(),
        size=len(content),
        media_type="text/plain",
        stored_in=BlobStorageBackend.DATABASE,
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
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        yield SQLBlobRepository(session), owner.id


async def test_create_sets_timestamp(setup: Setup) -> None:
    """Store a new blob with its created timestamp set."""
    repository, owner_id = setup
    blob, created = await repository.create(_blob(owner_id))
    assert created is True
    assert blob.owner_id == owner_id
    assert blob.size == len(b"content")
    assert blob.media_type == "text/plain"
    assert blob.stored_in == BlobStorageBackend.DATABASE
    assert blob.created is not None


async def test_create_and_round_trip_without_owner(setup: Setup) -> None:
    """Store and reload a default plugin blob with no owner."""
    repository, _ = setup
    created, _ = await repository.create(_blob(None))
    assert created.owner_id is None
    loaded = await repository.get(created.id)
    assert loaded.owner_id is None


async def test_create_dedup(setup: Setup) -> None:
    """Return the existing row unmarked as created on a duplicate sha256."""
    repository, owner_id = setup
    first, created_first = await repository.create(_blob(owner_id, b"same"))
    assert created_first is True
    second, created_second = await repository.create(_blob(owner_id, b"same"))
    assert created_second is False
    assert second.id == first.id
    assert second.sha256 == first.sha256


async def test_get(setup: Setup) -> None:
    """Load a stored blob by id."""
    repository, owner_id = setup
    created, _ = await repository.create(_blob(owner_id))
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown blob id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(BlobNotFound, match=f"Blob {missing_id} was not found"):
        await repository.get(missing_id)


async def test_delete(setup: Setup) -> None:
    """Delete a stored blob."""
    repository, owner_id = setup
    created, _ = await repository.create(_blob(owner_id))
    await repository.delete(created.id)
    with pytest.raises(BlobNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown blob id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(BlobNotFound, match=f"Blob {missing_id} was not found"):
        await repository.delete(missing_id)


async def test_delete_in_use(setup: Setup) -> None:
    """Reject deleting a blob referenced by a plugin version."""
    repository, owner_id = setup
    blob, _ = await repository.create(_blob(owner_id))

    if isinstance(repository, FakeBlobRepository):
        repository.mark_referenced(blob.id)
    else:
        assert isinstance(repository, SQLBlobRepository)
        plugin_repository = SQLPluginRepository(repository._session)
        plugin = await plugin_repository.create(
            Plugin(owner_id=owner_id, kind=PluginKind.EVALUATOR, name="scorer")
        )
        await plugin_repository.create_version(
            plugin.id,
            ScriptPluginSource(blob_id=blob.id, entrypoint="score"),
            display_version=None,
        )

    with pytest.raises(BlobInUse, match=f"Blob {blob.id} is in use"):
        await repository.delete(blob.id)


async def test_delete_leaves_task_with_payload() -> None:
    """Deleting a blob leaves the import task that names it as payload."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name="assistant")
        )
        blob_repository = SQLBlobRepository(session)
        payload_blob, _ = await blob_repository.create(_blob(owner.id, b"payload"))
        plugin_repository = SQLPluginRepository(session)
        plugin = await plugin_repository.create(
            Plugin(owner_id=owner.id, kind=PluginKind.IMPORTER, name="importer")
        )
        version = await plugin_repository.create_version(
            plugin.id,
            PackagePluginSource(
                requirement="kitaru-importer==1.0.0", entrypoint="pkg:run"
            ),
            display_version=None,
        )
        job_repository = SQLJobRepository(session)
        job = await job_repository.create(
            Job(owner_id=owner.id, kind=JobKind.IMPORT, status=JobStatus.PENDING)
        )
        task_repository = SQLTaskRepository(session)
        task = await task_repository.create(
            ImportTask(
                job_id=job.id,
                plugin_version_id=version.id,
                payload_blob_id=payload_blob.id,
                agent_id=agent.id,
            )
        )

        await blob_repository.delete(payload_blob.id)

        stored = await task_repository.get(task.id)
        assert isinstance(stored, ImportTask)
        assert stored.payload_blob_id == payload_blob.id
