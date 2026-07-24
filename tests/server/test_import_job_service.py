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
"""Import job use case tests."""

import uuid
from datetime import UTC, datetime

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeImportJobRepository,
    FakeSessionNodeRepository,
    FakeSessionRepository,
)
from kitaru.server.adapters.importers.registry import ImporterRegistry
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.import_jobs import (
    NormalizedNode,
    NormalizedSession,
    NormalizedTurn,
    ReplayReadiness,
)
from kitaru.server.application.services.import_job_service import (
    ImportedSessionService,
    ImportJobService,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion, AgentVersionNotFound
from kitaru.server.domain.import_job import (
    ImportJob,
    ImportJobNotFound,
    ImportJobStatus,
)
from kitaru.server.domain.session import SessionOrigin, SessionStatus
from kitaru.server.domain.session_node import NodeStatus, NodeType

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))
OTHER_ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="bob"))
STARTED_AT = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
ENDED_AT = datetime(2026, 7, 24, 10, 1, tzinfo=UTC)


@pytest.fixture
def agent_repository() -> FakeAgentRepository:
    """Provide an agent repository."""
    return FakeAgentRepository()


@pytest.fixture
def version_repository(
    agent_repository: FakeAgentRepository,
) -> FakeAgentVersionRepository:
    """Provide an agent version repository."""
    return FakeAgentVersionRepository(agent_repository)


@pytest.fixture
def job_repository() -> FakeImportJobRepository:
    """Provide an import job repository."""
    return FakeImportJobRepository()


@pytest.fixture
def service(
    job_repository: FakeImportJobRepository,
    version_repository: FakeAgentVersionRepository,
) -> ImportJobService:
    """Provide an import job service."""
    return ImportJobService(
        repository=job_repository,
        agent_version_repository=version_repository,
        registry=ImporterRegistry(),
    )


@pytest.fixture
async def version(
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
) -> AgentVersion:
    """Provide a stored agent version."""
    agent = await agent_repository.create(
        Agent(owner_id=ACTOR.account.id, name="support-bot")
    )
    return await version_repository.create(
        AgentVersion(
            owner_id=ACTOR.account.id,
            agent_id=agent.id,
            version="v1",
        )
    )


def normalized_session(digest: str = "a" * 64) -> NormalizedSession:
    """Build one normalized session."""
    return NormalizedSession(
        source_id="conversation-1",
        source_instance="project-1",
        name="Support conversation",
        status=SessionStatus.COMPLETED,
        turns=[
            NormalizedTurn(
                trace_id="trace-1",
                inputs={"message": "hello"},
                outputs={"answer": "hi"},
                started_at=STARTED_AT,
                ended_at=ENDED_AT,
            )
        ],
        nodes=[
            NormalizedNode(
                source_id="trace-1:root",
                trace_id="trace-1",
                node_type=NodeType.SPAN,
                name="agent",
                status=NodeStatus.COMPLETED,
                started_at=STARTED_AT,
                ended_at=ENDED_AT,
                inputs={"message": "hello"},
                outputs={"answer": "hi"},
                source_metadata={"langfuse.id": "root"},
            )
        ],
        inputs={
            "schema_version": 1,
            "turns": [
                {
                    "source_trace_id": "trace-1",
                    "inputs": {"message": "hello"},
                }
            ],
        },
        outputs={"answer": "hi"},
        started_at=STARTED_AT,
        ended_at=ENDED_AT,
        source_metadata={"langfuse.session_id": "conversation-1"},
        readiness=ReplayReadiness(
            level="ready",
            root_inputs_available=True,
            graph_complete=True,
            tool_call_count=0,
            replayable_tool_call_count=0,
        ),
        content_digest=digest,
    )


async def test_create_job_persists_temporary_upload(
    service: ImportJobService,
    version: AgentVersion,
) -> None:
    """Create a pending job targeting one explicit agent version."""
    job = await service.create_job(
        importer_id="langfuse",
        agent_version_id=version.id,
        source_instance="project-1",
        filename="traces.jsonl",
        content=b'{"traceId":"trace-1"}\n',
        actor=ACTOR,
    )

    assert job.status is ImportJobStatus.PENDING
    assert job.owner_id == ACTOR.account.id
    assert job.agent_version_id == version.id
    assert job.content == b'{"traceId":"trace-1"}\n'
    assert job.importer_version == "1"


async def test_create_job_hides_another_accounts_agent_version(
    service: ImportJobService,
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
) -> None:
    """Reject an agent version owned by another account."""
    agent = await agent_repository.create(
        Agent(owner_id=OTHER_ACTOR.account.id, name="other-bot")
    )
    version = await version_repository.create(
        AgentVersion(
            owner_id=OTHER_ACTOR.account.id,
            agent_id=agent.id,
            version="v1",
        )
    )

    with pytest.raises(AgentVersionNotFound):
        await service.create_job(
            importer_id="langfuse",
            agent_version_id=version.id,
            source_instance=None,
            filename="traces.jsonl",
            content=b"{}\n",
            actor=ACTOR,
        )


async def test_get_job_hides_another_accounts_job(
    service: ImportJobService,
    job_repository: FakeImportJobRepository,
    version: AgentVersion,
) -> None:
    """Do not expose job state across accounts."""
    job = await job_repository.create(
        ImportJob(
            owner_id=OTHER_ACTOR.account.id,
            agent_version_id=version.id,
            importer_id="langfuse",
            importer_version="1",
            filename="traces.jsonl",
            content=b"{}\n",
        )
    )

    with pytest.raises(ImportJobNotFound):
        await service.get_job(job.id, actor=ACTOR)


async def test_import_session_deduplicates_and_revises(
    agent_repository: FakeAgentRepository,
    version_repository: FakeAgentVersionRepository,
    version: AgentVersion,
) -> None:
    """Reuse exact evidence and create a new immutable changed revision."""
    session_repository = FakeSessionRepository(agent_repository, version_repository)
    node_repository = FakeSessionNodeRepository(session_repository)
    service = ImportedSessionService(
        session_repository=session_repository,
        node_repository=node_repository,
        agent_version_repository=version_repository,
    )
    job = ImportJob(
        owner_id=ACTOR.account.id,
        agent_version_id=version.id,
        importer_id="langfuse",
        importer_version="1",
        source_instance="project-1",
        filename="traces.jsonl",
    )

    first, first_created = await service.import_session(job, normalized_session())
    duplicate, duplicate_created = await service.import_session(
        job, normalized_session()
    )
    second, second_created = await service.import_session(
        job, normalized_session("b" * 64)
    )

    assert first_created is True
    assert duplicate_created is False
    assert duplicate.id == first.id
    assert second_created is True
    assert first.origin is SessionOrigin.IMPORTED
    assert first.source_revision == 1
    assert second.source_revision == 2
    assert second.supersedes_session_id == first.id
    assert second.agent_version_id == version.id
    assert second.replay_readiness == {
        "level": "ready",
        "root_inputs_available": True,
        "graph_complete": True,
        "tool_call_count": 0,
        "replayable_tool_call_count": 0,
        "reasons": [],
    }
    stored_nodes = await node_repository.list_for_session(
        second.id, include_payloads=True
    )
    assert stored_nodes[0].external_id == "trace-1:root"
    assert stored_nodes[0].attributes["source_metadata"] == {"langfuse.id": "root"}
