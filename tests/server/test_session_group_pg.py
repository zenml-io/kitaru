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
"""Session group tests against PostgreSQL."""

import uuid

import pytest

from conftest import pg_session_with_engine, postgres_available
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.job import Job
from kitaru.server.domain.session import Session, SessionNotFound
from kitaru.server.filtering import FilterCondition


async def test_group_follows_session_and_job_deletes() -> None:
    """Drop a deleted session from its group, and the group with its job."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name="assistant")
        )
        sessions = SQLSessionRepository(session, engine)
        jobs = SQLJobRepository(session)
        kept, deleted, outside = [
            await sessions.create(
                Session(
                    owner_id=owner.id,
                    agent_id=agent.id,
                    number=number,
                    origin=SessionOrigin.RECORDED,
                )
            )
            for number in (1, 2, 3)
        ]
        job = await jobs.create(
            Job(owner_id=owner.id, kind=JobKind.ANALYSIS, status=JobStatus.PENDING)
        )
        group_id = await sessions.create_group(job.id, [kept.id, deleted.id])

        async def list_group() -> set[uuid.UUID]:
            members, _ = await sessions.query(
                SessionFilter(
                    expression=FilterCondition(
                        field="session_group_id", op=FilterOp.EQ, value=group_id
                    )
                ),
                include_payloads=False,
            )
            return {member.id for member in members}

        assert await list_group() == {kept.id, deleted.id}
        assert await sessions.is_in_group(kept.id, [group_id])
        assert not await sessions.is_in_group(outside.id, [group_id])

        await sessions.delete(deleted.id)
        assert await list_group() == {kept.id}

        await jobs.delete(job.id)
        assert await list_group() == set()
        assert (await sessions.get(outside.id, include_payloads=False)).id == outside.id
        assert (await sessions.get(kept.id, include_payloads=False)).id == kept.id


async def test_create_group_rejects_a_missing_session() -> None:
    """Refuse a group naming a session that does not exist."""
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session_with_engine() as (session, engine):
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        job = await SQLJobRepository(session).create(
            Job(owner_id=owner.id, kind=JobKind.ANALYSIS, status=JobStatus.PENDING)
        )
        with pytest.raises(SessionNotFound):
            await SQLSessionRepository(session, engine).create_group(
                job.id, [uuid.uuid4()]
            )
