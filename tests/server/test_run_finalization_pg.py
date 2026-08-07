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
"""Concurrency tests for experiment run finalization against PostgreSQL."""

import asyncio
import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from conftest import pg_session_with_engine, postgres_available
from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.replay import ReplayStatus
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.agent_version_repository import (
    SQLAgentVersionRepository,
)
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.cohort_version_repository import (
    SQLCohortVersionRepository,
)
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
)
from kitaru.server.adapters.db.repositories.experiment_run_repository import (
    SQLExperimentRunRepository,
)
from kitaru.server.adapters.db.repositories.job_repository import SQLJobRepository
from kitaru.server.adapters.db.repositories.replay_repository import (
    SQLReplayRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.application.events import ReplaysSettled
from kitaru.server.application.services.run_finalization import (
    finalize_runs_if_drained,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.cohort_version import CohortVersion
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.job import Job
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import (
    PassthroughConfig,
    ReplayConfig,
    ToolPolicy,
)
from kitaru.server.domain.session import Session


async def _settle_and_finalize(session: AsyncSession, replay_id: uuid.UUID) -> None:
    """Mark one replay completed and run finalization within the same session."""
    replay_repository = SQLReplayRepository(session)
    run_repository = SQLExperimentRunRepository(session)
    replay = await replay_repository.get(replay_id)
    replay.complete()
    replay = await replay_repository.update(replay)
    await finalize_runs_if_drained(
        ReplaysSettled(replays=[replay]),
        replay_repository=replay_repository,
        experiment_run_repository=run_repository,
    )


async def test_finalize_runs_if_drained_serializes_concurrent_settlements() -> None:
    """Two replays of one run settling in overlapping transactions still finalize it.

    Regression test for a race in run finalization: it used to count
    non-settled replays before locking the run row, so two replays settling
    concurrently could each read the other's still-uncommitted completion as
    live and both skip finalization, leaving the run running forever. Locking
    the run row first forces the second settlement to wait for the first to
    commit, so its recount sees accurate, post-commit data.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")

    async with pg_session_with_engine() as (seed_session, engine):
        owner = await SQLAccountRepository(seed_session).create(Account(name="owner"))
        agent = await SQLAgentRepository(seed_session).create(
            Agent(owner_id=owner.id, name="agent")
        )
        agent_version = await SQLAgentVersionRepository(seed_session).create(
            AgentVersion(owner_id=owner.id, agent_id=agent.id)
        )
        cohort = await SQLCohortRepository(seed_session).create(
            Cohort(owner_id=owner.id, name="cohort", agent_id=agent.id)
        )
        cohort_version = await SQLCohortVersionRepository(seed_session).create(
            CohortVersion(owner_id=owner.id, cohort_id=cohort.id, session_count=0),
            [],
        )
        config = await SQLExperimentRepository(seed_session).create_replay_config(
            ReplayConfig(
                owner_id=owner.id,
                tool_policy=ToolPolicy(default=PassthroughConfig()),
                evaluators=[],
            )
        )
        experiment = await SQLExperimentRepository(seed_session).create(
            Experiment(
                owner_id=owner.id,
                name="exp",
                agent_id=agent.id,
                replay_config_id=config.id,
            )
        )
        run = await SQLExperimentRunRepository(seed_session).create(
            ExperimentRun(
                owner_id=owner.id,
                experiment_id=experiment.id,
                number=1,
                cohort_version_id=cohort_version.id,
                agent_version_id=agent_version.id,
            )
        )

        replay_ids = []
        for number in range(1, 3):
            job = await SQLJobRepository(seed_session).create(
                Job(
                    owner_id=owner.id,
                    kind=JobKind.REPLAY,
                    status=JobStatus.RUNNING,
                )
            )
            baseline = await SQLSessionRepository(seed_session, engine).create(
                Session(
                    owner_id=owner.id,
                    agent_id=agent.id,
                    number=number,
                    origin=SessionOrigin.RECORDED,
                )
            )
            replay = await SQLReplayRepository(seed_session).create(
                Replay(
                    owner_id=owner.id,
                    job_id=job.id,
                    experiment_run_id=run.id,
                    replay_config_id=config.id,
                    baseline_session_id=baseline.id,
                    status=ReplayStatus.EVALUATING,
                )
            )
            replay_ids.append(replay.id)
        await seed_session.commit()

        session_factory = async_sessionmaker(
            bind=engine, class_=AsyncSession, expire_on_commit=False
        )
        session_a = session_factory()
        session_b = session_factory()
        try:
            # Settle and finalize-check the first replay, but do not commit
            # yet: with the fix this holds the run row's FOR UPDATE lock
            # open, exactly mirroring the first of two replay completions
            # still being mid-transaction when the second one settles.
            await _settle_and_finalize(session_a, replay_ids[0])

            task_b = asyncio.create_task(_settle_and_finalize(session_b, replay_ids[1]))
            await asyncio.sleep(0.2)
            assert not task_b.done(), (
                "second settlement did not block on the run row lock, "
                "finalization is not serialized"
            )

            await session_a.commit()
            await asyncio.wait_for(task_b, timeout=5.0)
            await session_b.commit()
        finally:
            await session_a.close()
            await session_b.close()

        async with session_factory() as verify_session:
            final_run = await SQLExperimentRunRepository(verify_session).get(run.id)
            assert final_run.status == ExperimentRunStatus.COMPLETED
