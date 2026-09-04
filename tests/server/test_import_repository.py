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
"""Contract tests for import repositories."""

import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import NamedTuple

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from conftest import (
    FakeImportRepository,
    pg_session_with_engine,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.imports import ImportFailure, ImportStats
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.adapters.db.orm.imports import ImportORM
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.blob_repository import SQLBlobRepository
from kitaru.server.adapters.db.repositories.import_repository import (
    SQLImportRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.plugin_repository import (
    SQLPluginRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.interfaces.import_repository import ImportRepository
from kitaru.server.application.models.imports import ImportFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.blob import Blob, BlobStorageBackend
from kitaru.server.domain.imports import Import, ImportNotFound
from kitaru.server.domain.job import Job
from kitaru.server.domain.plugin import PackagePluginSource, Plugin, PluginKind
from kitaru.server.domain.replay_config import EvaluatorConfig
from kitaru.server.domain.session import Session
from kitaru.server.filtering import FilterCondition


class Setup(NamedTuple):
    """Import repository under test, plus rows an import can reference."""

    imports: ImportRepository
    owner_id: uuid.UUID
    agent_id: uuid.UUID
    agent_id_2: uuid.UUID
    agent_version_id: uuid.UUID
    importer_version_id: uuid.UUID
    payload_blob_id: uuid.UUID
    make_job_id: Callable[[], Awaitable[uuid.UUID]]


async def _seed_postgres(session: AsyncSession) -> Setup:
    """Create the account, agent, importer, and payload rows an import references.

    Returns:
        Import repository and the ids of the rows it can point imports at.
    """
    owner = await SQLAccountRepository(session).create(Account(name="owner"))
    agent = await SQLAgentRepository(session).create(
        Agent(owner_id=owner.id, name="assistant")
    )
    agent_2 = await SQLAgentRepository(session).create(
        Agent(owner_id=owner.id, name="assistant-2")
    )
    agent_version = await SQLAgentVersionRepository(session).create(
        AgentVersion(owner_id=owner.id, agent_id=agent.id)
    )
    plugin_repository = SQLPluginRepository(session)
    plugin = await plugin_repository.create(
        Plugin(owner_id=owner.id, kind=PluginKind.IMPORTER, name="importer")
    )
    importer_version = await plugin_repository.create_version(
        plugin.id,
        PackagePluginSource(requirement="kitaru-importer==1.0.0", entrypoint="pkg:run"),
        display_version=None,
    )
    payload, _ = await SQLBlobRepository(session).create(
        Blob(
            owner_id=owner.id,
            sha256="0" * 64,
            size=4,
            media_type="text/csv",
            stored_in=BlobStorageBackend.DATABASE,
        )
    )
    jobs_repository = SQLJobRepository(session)

    async def make_job_id() -> uuid.UUID:
        job = await jobs_repository.create(
            Job(owner_id=owner.id, kind=JobKind.IMPORT, status=JobStatus.PENDING)
        )
        return job.id

    return Setup(
        imports=SQLImportRepository(session),
        owner_id=owner.id,
        agent_id=agent.id,
        agent_id_2=agent_2.id,
        agent_version_id=agent_version.id,
        importer_version_id=importer_version.id,
        payload_blob_id=payload.id,
        make_job_id=make_job_id,
    )


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each import repository implementation and its collaborators."""
    if request.param == "fake":

        async def make_job_id() -> uuid.UUID:
            return uuid.uuid4()

        yield Setup(
            imports=FakeImportRepository(),
            owner_id=uuid.uuid4(),
            agent_id=uuid.uuid4(),
            agent_id_2=uuid.uuid4(),
            agent_version_id=uuid.uuid4(),
            importer_version_id=uuid.uuid4(),
            payload_blob_id=uuid.uuid4(),
            make_job_id=make_job_id,
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, _):
        yield await _seed_postgres(session)


def _evaluator() -> EvaluatorConfig:
    return EvaluatorConfig(
        evaluator="accuracy",
        version=1,
        params={"threshold": 0.5},
        evaluator_version_id=uuid.uuid4(),
    )


def _stats() -> ImportStats:
    return ImportStats(
        created=2,
        skipped=1,
        failed=1,
        failures=[ImportFailure(line=4, external_id="ext-4", error="bad row")],
    )


async def _create_import(setup: Setup, job_id: uuid.UUID | None = None) -> Import:
    """Store an import pointed at the setup's rows."""
    return await setup.imports.create(
        Import(
            owner_id=setup.owner_id,
            job_id=job_id if job_id is not None else await setup.make_job_id(),
            agent_id=setup.agent_id,
            agent_version_id=setup.agent_version_id,
            importer_version_id=setup.importer_version_id,
            payload_blob_id=setup.payload_blob_id,
            params={"delimiter": ","},
            evaluators=[_evaluator()],
        )
    )


async def test_create_and_get(setup: Setup) -> None:
    """Store an import and load it back with timestamps and evaluators set."""
    created = await _create_import(setup)
    assert created.created is not None
    assert created.updated is not None
    assert created.stats is None
    assert created.error is None
    loaded = await setup.imports.get(created.id)
    assert loaded == created
    assert loaded.params == {"delimiter": ","}
    assert loaded.evaluators[0].params == {"threshold": 0.5}


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown import id."""
    missing_id = uuid.uuid4()
    with pytest.raises(ImportNotFound, match=f"Import {missing_id} was not found"):
        await setup.imports.get(missing_id)


async def test_get_by_job_id(setup: Setup) -> None:
    """Load the import owning a job."""
    job_id = await setup.make_job_id()
    created = await _create_import(setup, job_id=job_id)
    loaded = await setup.imports.get_by_job_id(job_id)
    assert loaded == created


async def test_get_by_job_id_miss(setup: Setup) -> None:
    """Return None when the job holds no import."""
    assert await setup.imports.get_by_job_id(uuid.uuid4()) is None


async def test_update_records_stats(setup: Setup) -> None:
    """Persist recorded stats and renew the updated timestamp."""
    created = await _create_import(setup)
    created.record_stats(_stats())
    updated = await setup.imports.update(created)
    assert updated.stats == _stats()
    assert updated.error is None
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated
    assert await setup.imports.get(created.id) == updated


async def test_update_records_error(setup: Setup) -> None:
    """Persist a recorded error."""
    created = await _create_import(setup)
    created.record_error("parse failed")
    updated = await setup.imports.update(created)
    assert updated.error == "parse failed"
    assert updated.stats is None
    assert await setup.imports.get(created.id) == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise when updating an import that does not exist."""
    missing = Import(
        owner_id=setup.owner_id,
        agent_id=setup.agent_id,
        importer_version_id=setup.importer_version_id,
        payload_blob_id=setup.payload_blob_id,
    )
    with pytest.raises(ImportNotFound):
        await setup.imports.update(missing)


async def test_query_filters_by_agent_id(setup: Setup) -> None:
    """Filter imports by agent id."""
    matching = await _create_import(setup)
    await setup.imports.create(
        Import(
            owner_id=setup.owner_id,
            agent_id=setup.agent_id_2,
            importer_version_id=setup.importer_version_id,
            payload_blob_id=setup.payload_blob_id,
        )
    )

    imports, next_cursor = await setup.imports.query(
        ImportFilter(
            expression=FilterCondition(
                field="agent_id", op=FilterOp.EQ, value=setup.agent_id
            )
        )
    )
    assert next_cursor is None
    assert [import_.id for import_ in imports] == [matching.id]


async def test_query_filters_by_job_id(setup: Setup) -> None:
    """Filter imports by job id."""
    await _create_import(setup)
    job_id = await setup.make_job_id()
    matching = await _create_import(setup, job_id=job_id)

    imports, next_cursor = await setup.imports.query(
        ImportFilter(
            expression=FilterCondition(field="job_id", op=FilterOp.EQ, value=job_id)
        )
    )
    assert next_cursor is None
    assert [import_.id for import_ in imports] == [matching.id]


async def test_delete_job_nulls_job_id() -> None:
    """Null an import's job pointer when the job is deleted."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, _):
        setup = await _seed_postgres(session)
        job_id = await setup.make_job_id()
        created = await _create_import(setup, job_id=job_id)

        await SQLJobRepository(session).delete(job_id)

        reloaded = await setup.imports.get(created.id)
        assert reloaded.job_id is None


async def test_delete_import_nulls_session_import_id() -> None:
    """Null a session's import pointer when the import row is deleted."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        setup = await _seed_postgres(session)
        created = await _create_import(setup)
        sessions = SQLSessionRepository(session, engine)
        stored = await sessions.create(
            Session(
                owner_id=setup.owner_id,
                agent_id=setup.agent_id,
                number=1,
                origin=SessionOrigin.IMPORTED,
                import_id=created.id,
            )
        )
        assert stored.import_id == created.id

        await session.execute(delete(ImportORM).where(ImportORM.id == created.id))

        reloaded = await sessions.get(stored.id, include_payloads=False)
        assert reloaded.import_id is None
