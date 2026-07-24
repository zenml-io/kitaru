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
"""Contract tests for import job repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeImportJobRepository,
    pg_session,
    postgres_available,
)
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import (
    SQLAgentRepository,
)
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.import_job_repository import (
    SQLImportJobRepository,
)
from kitaru.server.application.interfaces.import_job_repository import (
    ImportJobRepository,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.import_job import (
    ImportJob,
    ImportJobError,
    ImportJobNotFound,
    ImportJobStatus,
)

Setup = tuple[ImportJobRepository, uuid.UUID, uuid.UUID]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each repository implementation and valid foreign keys."""
    if request.param == "fake":
        owner_id = uuid.uuid4()
        agents = FakeAgentRepository()
        versions = FakeAgentVersionRepository(agents)
        agent = await agents.create(Agent(owner_id=owner_id, name="support-bot"))
        version = await versions.create(
            AgentVersion(
                owner_id=owner_id,
                agent_id=agent.id,
                version="v1",
            )
        )
        yield FakeImportJobRepository(), owner_id, version.id
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        account = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=account.id, name="support-bot")
        )
        version = await SQLAgentVersionRepository(session).create(
            AgentVersion(
                owner_id=account.id,
                agent_id=agent.id,
                version="v1",
            )
        )
        yield SQLImportJobRepository(session), account.id, version.id


def new_job(owner_id: uuid.UUID, version_id: uuid.UUID) -> ImportJob:
    """Build one pending import job."""
    return ImportJob(
        owner_id=owner_id,
        agent_version_id=version_id,
        importer_id="langfuse",
        importer_version="1",
        source_instance="project-1",
        filename="traces.jsonl",
        content=b"{}\n",
    )


async def test_create_and_get(setup: Setup) -> None:
    """Persist all pending job fields."""
    repository, owner_id, version_id = setup

    created = await repository.create(new_job(owner_id, version_id))
    loaded = await repository.get(created.id)

    assert loaded == created
    assert loaded.created is not None
    assert loaded.updated is not None
    assert loaded.content == b"{}\n"
    assert loaded.status is ImportJobStatus.PENDING


async def test_claim_next_once(setup: Setup) -> None:
    """Atomically move one pending job to running."""
    repository, owner_id, version_id = setup
    created = await repository.create(new_job(owner_id, version_id))

    claimed = await repository.claim_next("worker-1")
    none_left = await repository.claim_next("worker-2")

    assert claimed is not None
    assert claimed.id == created.id
    assert claimed.status is ImportJobStatus.RUNNING
    assert claimed.worker_id == "worker-1"
    assert claimed.started_at is not None
    assert none_left is None


async def test_complete_discards_upload(setup: Setup) -> None:
    """Persist partial completion details without retaining raw evidence."""
    repository, owner_id, version_id = setup
    job = await repository.create(new_job(owner_id, version_id))
    job.start("worker-1")
    session_id = uuid.uuid4()
    job.complete(
        source_session_count=2,
        imported_count=1,
        deduplicated_count=0,
        session_ids=[session_id],
        errors=[ImportJobError(source_id="bad", message="invalid source")],
    )

    updated = await repository.update(job)

    assert updated.status is ImportJobStatus.COMPLETED_WITH_ERRORS
    assert updated.content is None
    assert updated.failed_count == 1
    assert updated.session_ids == [session_id]
    assert updated.errors[0].source_id == "bad"


async def test_get_unknown(setup: Setup) -> None:
    """Raise the domain not-found error."""
    repository, _, _ = setup

    with pytest.raises(ImportJobNotFound):
        await repository.get(uuid.uuid4())
