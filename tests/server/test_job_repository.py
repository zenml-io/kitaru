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
"""Contract tests for job repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import FakeJobRepository, pg_session, postgres_available
from kitaru.api_models.v1.job import JobStatus
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.models.job import JobFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.job import Job, JobNotFound

Setup = tuple[JobRepository, uuid.UUID]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each job repository implementation plus an owner id."""
    if request.param == "fake":
        yield FakeJobRepository(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        yield SQLJobRepository(session), owner.id


def _job(owner_id: uuid.UUID, status: JobStatus = JobStatus.PENDING) -> Job:
    """Build a job for repository tests.

    Args:
        owner_id: Id of the owning account.
        status: Job status.

    Returns:
        Unstored job.
    """
    return Job(owner_id=owner_id, status=status)


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new job with both timestamps set."""
    repository, owner_id = setup
    job = await repository.create(_job(owner_id))
    assert job.owner_id == owner_id
    assert job.status is JobStatus.PENDING
    assert job.created is not None
    assert job.updated is not None


async def test_get(setup: Setup) -> None:
    """Load a stored job by id."""
    repository, owner_id = setup
    created = await repository.create(_job(owner_id))
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown job id."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(JobNotFound, match=f"Job {missing_id} was not found"):
        await repository.get(missing_id)


async def test_get_exclusive(setup: Setup) -> None:
    """Load a job with a row lock, a no-op difference for the fake backend."""
    repository, owner_id = setup
    created = await repository.create(_job(owner_id))
    loaded = await repository.get(created.id, exclusive=True)
    assert loaded == created


async def test_update_renews_timestamp_and_persists_fields(setup: Setup) -> None:
    """Persist changes and renew the updated timestamp."""
    repository, owner_id = setup
    created = await repository.create(_job(owner_id))
    created.status = JobStatus.RUNNING
    updated = await repository.update(created)
    assert updated.status is JobStatus.RUNNING
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise when updating a job that does not exist."""
    repository, owner_id = setup
    missing = _job(owner_id)
    with pytest.raises(JobNotFound):
        await repository.update(missing)


async def test_query_filters_by_status(setup: Setup) -> None:
    """Filter jobs by status."""
    repository, owner_id = setup
    await repository.create(_job(owner_id, status=JobStatus.PENDING))
    completed = await repository.create(_job(owner_id, status=JobStatus.COMPLETED))

    jobs, next_cursor = await repository.query(JobFilter(status=JobStatus.COMPLETED))
    assert next_cursor is None
    assert [job.id for job in jobs] == [completed.id]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id = setup
    created = [await repository.create(_job(owner_id)) for _ in range(5)]
    expected_order = list(reversed(created))

    collected: list[Job] = []
    cursor = None
    while True:
        jobs, next_cursor = await repository.query(JobFilter(cursor=cursor, size=2))
        collected.extend(jobs)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order
    assert len({job.id for job in collected}) == 5


async def test_delete(setup: Setup) -> None:
    """Delete a stored job."""
    repository, owner_id = setup
    created = await repository.create(_job(owner_id))
    await repository.delete(created.id)
    with pytest.raises(JobNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise when deleting a job that does not exist."""
    repository, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(JobNotFound, match=f"Job {missing_id} was not found"):
        await repository.delete(missing_id)
