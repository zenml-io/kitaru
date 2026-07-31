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
"""Contract tests for replay repositories."""

import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest

from conftest import (
    FakeJobRepository,
    FakeReplayRepository,
    FakeSessionRepository,
    create_job,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.replay import ReplayStatus
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.replay_repository import (
    SQLReplayRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.models.replay import ReplayFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.job import Job
from kitaru.server.domain.replay import (
    Replay,
    ReplayAlreadyExistsForJob,
    ReplayNotFound,
)
from kitaru.server.domain.replay_config import (
    PassthroughConfig,
    ReplayConfig,
    ToolPolicy,
)
from kitaru.server.domain.session import Session
from kitaru.server.filtering import FilterCondition

Setup = tuple[
    ReplayRepository,
    uuid.UUID,
    Callable[[], Awaitable[uuid.UUID]],
    Callable[[], Awaitable[uuid.UUID]],
    Callable[[], Awaitable[uuid.UUID]],
]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each replay repository implementation, an owner id, and factories
    for a fresh job id, replay config id, and baseline session id."""
    if request.param == "fake":
        owner_id = uuid.uuid4()
        jobs = FakeJobRepository()
        sessions = FakeSessionRepository()

        async def make_job_id() -> uuid.UUID:
            job = await create_job(jobs, owner_id)
            return job.id

        async def make_config_id() -> uuid.UUID:
            config = ReplayConfig(
                owner_id=owner_id,
                tool_policy=ToolPolicy(default=PassthroughConfig()),
                evaluators=[],
            )
            return config.id

        async def make_session_id() -> uuid.UUID:
            created = await sessions.create(
                Session(
                    owner_id=owner_id,
                    agent_id=uuid.uuid4(),
                    origin=SessionOrigin.RECORDED,
                )
            )
            return created.id

        yield (
            FakeReplayRepository(),
            owner_id,
            make_job_id,
            make_config_id,
            make_session_id,
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        jobs_repository = SQLJobRepository(session)
        experiments_repository = SQLExperimentRepository(session)
        sessions_repository = SQLSessionRepository(session)

        async def make_job_id() -> uuid.UUID:
            job = await jobs_repository.create(
                Job(owner_id=owner.id, status=JobStatus.PENDING)
            )
            return job.id

        async def make_config_id() -> uuid.UUID:
            config = await experiments_repository.create_replay_config(
                ReplayConfig(
                    owner_id=owner.id,
                    tool_policy=ToolPolicy(default=PassthroughConfig()),
                    evaluators=[],
                )
            )
            return config.id

        async def make_session_id() -> uuid.UUID:
            agent = await SQLAgentRepository(session).create(
                Agent(owner_id=owner.id, name=f"agent-{uuid.uuid4().hex[:8]}")
            )
            created = await sessions_repository.create(
                Session(
                    owner_id=owner.id, agent_id=agent.id, origin=SessionOrigin.RECORDED
                )
            )
            return created.id

        yield (
            SQLReplayRepository(session),
            owner.id,
            make_job_id,
            make_config_id,
            make_session_id,
        )


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new replay with both timestamps set."""
    repository, owner_id, make_job_id, make_config_id, make_session_id = setup
    replay = await repository.create(
        Replay(
            owner_id=owner_id,
            job_id=await make_job_id(),
            replay_config_id=await make_config_id(),
            baseline_session_id=await make_session_id(),
        )
    )
    assert replay.status is ReplayStatus.PENDING
    assert replay.created is not None
    assert replay.updated is not None


async def test_get(setup: Setup) -> None:
    """Load a stored replay by id."""
    repository, owner_id, make_job_id, make_config_id, make_session_id = setup
    created = await repository.create(
        Replay(
            owner_id=owner_id,
            job_id=await make_job_id(),
            replay_config_id=await make_config_id(),
            baseline_session_id=await make_session_id(),
        )
    )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown replay id."""
    repository, *_ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(ReplayNotFound, match=f"Replay {missing_id} was not found"):
        await repository.get(missing_id)


async def test_get_by_job_id(setup: Setup) -> None:
    """Load the replay owning a job."""
    repository, owner_id, make_job_id, make_config_id, make_session_id = setup
    job_id = await make_job_id()
    created = await repository.create(
        Replay(
            owner_id=owner_id,
            job_id=job_id,
            replay_config_id=await make_config_id(),
            baseline_session_id=await make_session_id(),
        )
    )
    loaded = await repository.get_by_job_id(job_id)
    assert loaded == created


async def test_get_by_job_id_miss(setup: Setup) -> None:
    """Return None when the job holds no replay."""
    repository, *_ = setup
    assert await repository.get_by_job_id(uuid.uuid4()) is None


async def test_unique_job_id(setup: Setup) -> None:
    """A second replay cannot claim the same job."""
    repository, owner_id, make_job_id, make_config_id, make_session_id = setup
    job_id = await make_job_id()
    config_id = await make_config_id()
    await repository.create(
        Replay(
            owner_id=owner_id,
            job_id=job_id,
            replay_config_id=config_id,
            baseline_session_id=await make_session_id(),
        )
    )
    with pytest.raises(ReplayAlreadyExistsForJob):
        await repository.create(
            Replay(
                owner_id=owner_id,
                job_id=job_id,
                replay_config_id=config_id,
                baseline_session_id=await make_session_id(),
            )
        )


async def test_update_renews_timestamp(setup: Setup) -> None:
    """Persist status changes and renew the updated timestamp."""
    repository, owner_id, make_job_id, make_config_id, make_session_id = setup
    created = await repository.create(
        Replay(
            owner_id=owner_id,
            job_id=await make_job_id(),
            replay_config_id=await make_config_id(),
            baseline_session_id=await make_session_id(),
        )
    )
    created.start_evaluating()
    updated = await repository.update(created)
    assert updated.status is ReplayStatus.EVALUATING
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise when updating a replay that does not exist."""
    repository, owner_id, make_job_id, make_config_id, make_session_id = setup
    missing = Replay(
        owner_id=owner_id,
        job_id=await make_job_id(),
        replay_config_id=await make_config_id(),
        baseline_session_id=await make_session_id(),
    )
    with pytest.raises(ReplayNotFound):
        await repository.update(missing)


async def test_query_filters_by_status(setup: Setup) -> None:
    """Filter replays by status."""
    repository, owner_id, make_job_id, make_config_id, make_session_id = setup
    config_id = await make_config_id()
    pending = await repository.create(
        Replay(
            owner_id=owner_id,
            job_id=await make_job_id(),
            replay_config_id=config_id,
            baseline_session_id=await make_session_id(),
        )
    )
    evaluating = await repository.create(
        Replay(
            owner_id=owner_id,
            job_id=await make_job_id(),
            replay_config_id=config_id,
            baseline_session_id=await make_session_id(),
        )
    )
    evaluating.start_evaluating()
    await repository.update(evaluating)

    replays, next_cursor = await repository.query(
        ReplayFilter(
            expression=FilterCondition(
                field="status", op=FilterOp.EQ, value=ReplayStatus.EVALUATING
            )
        )
    )
    assert next_cursor is None
    assert [replay.id for replay in replays] == [evaluating.id]
    _ = pending


async def test_exists_for_replay_config(setup: Setup) -> None:
    """Report whether any replay references a replay config."""
    repository, owner_id, make_job_id, make_config_id, make_session_id = setup
    config_id = await make_config_id()
    assert await repository.exists_for_replay_config(config_id) is False
    await repository.create(
        Replay(
            owner_id=owner_id,
            job_id=await make_job_id(),
            replay_config_id=config_id,
            baseline_session_id=await make_session_id(),
        )
    )
    assert await repository.exists_for_replay_config(config_id) is True
