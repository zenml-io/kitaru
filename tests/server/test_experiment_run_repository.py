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
"""Contract tests for experiment run repositories."""

import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable

import pytest

from conftest import (
    FakeCohortRepository,
    FakeCohortVersionRepository,
    FakeExperimentRunRepository,
    FakeSessionRepository,
    FakeTagRepository,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
from kitaru.api_models.v1.filter import FilterOp
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
from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.models.experiment_run import ExperimentRunFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.cohort import Cohort
from kitaru.server.domain.cohort_version import CohortVersion
from kitaru.server.domain.experiment import Experiment
from kitaru.server.domain.experiment_run import (
    DuplicateExperimentRunNumber,
    ExperimentRun,
    ExperimentRunNotFound,
)
from kitaru.server.domain.replay_config import (
    PassthroughConfig,
    ReplayConfig,
    ToolPolicy,
)
from kitaru.server.filtering import AndExpression, FilterCondition

Setup = tuple[
    ExperimentRunRepository,
    uuid.UUID,
    Callable[[], Awaitable[uuid.UUID]],
    Callable[[], Awaitable[uuid.UUID]],
    Callable[[], Awaitable[uuid.UUID]],
]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each run repository implementation, an owner id, and factories
    for a fresh experiment id, cohort version id, and agent version id."""
    if request.param == "fake":
        owner_id = uuid.uuid4()

        async def make_experiment_id() -> uuid.UUID:
            return uuid.uuid4()

        async def make_cohort_version_id() -> uuid.UUID:
            return uuid.uuid4()

        async def make_agent_version_id() -> uuid.UUID:
            return uuid.uuid4()

        yield (
            FakeExperimentRunRepository(),
            owner_id,
            make_experiment_id,
            make_cohort_version_id,
            make_agent_version_id,
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agents_repository = SQLAgentRepository(session)
        agent = await agents_repository.create(Agent(owner_id=owner.id, name="agent"))
        agent_versions_repository = SQLAgentVersionRepository(session)
        experiments_repository = SQLExperimentRepository(session)
        cohorts_repository = SQLCohortRepository(session)
        cohort_versions_repository = SQLCohortVersionRepository(session)

        async def make_experiment_id() -> uuid.UUID:
            config = await experiments_repository.create_replay_config(
                ReplayConfig(
                    owner_id=owner.id,
                    tool_policy=ToolPolicy(default=PassthroughConfig()),
                    evaluators=[],
                )
            )
            experiment = await experiments_repository.create(
                Experiment(
                    owner_id=owner.id,
                    name=f"exp-{uuid.uuid4().hex[:8]}",
                    replay_config_id=config.id,
                )
            )
            return experiment.id

        async def make_cohort_version_id() -> uuid.UUID:
            cohort = await cohorts_repository.create(
                Cohort(
                    owner_id=owner.id,
                    name=f"cohort-{uuid.uuid4().hex[:8]}",
                    agent_id=agent.id,
                )
            )
            version = await cohort_versions_repository.create(
                CohortVersion(owner_id=owner.id, cohort_id=cohort.id, session_count=0),
                [],
            )
            return version.id

        async def make_agent_version_id() -> uuid.UUID:
            version = await agent_versions_repository.create(
                AgentVersion(owner_id=owner.id, agent_id=agent.id)
            )
            return version.id

        yield (
            SQLExperimentRunRepository(session),
            owner.id,
            make_experiment_id,
            make_cohort_version_id,
            make_agent_version_id,
        )


def _run(
    owner_id: uuid.UUID,
    experiment_id: uuid.UUID,
    cohort_version_id: uuid.UUID,
    agent_version_id: uuid.UUID,
    number: int = 1,
) -> ExperimentRun:
    return ExperimentRun(
        owner_id=owner_id,
        experiment_id=experiment_id,
        number=number,
        cohort_version_id=cohort_version_id,
        agent_version_id=agent_version_id,
    )


async def test_create_sets_timestamps(setup: Setup) -> None:
    """Store a new run with both timestamps set."""
    (
        repository,
        owner_id,
        make_experiment_id,
        make_cohort_version_id,
        make_agent_version_id,
    ) = setup
    run = await repository.create(
        _run(
            owner_id,
            await make_experiment_id(),
            await make_cohort_version_id(),
            await make_agent_version_id(),
        )
    )
    assert run.status is ExperimentRunStatus.RUNNING
    assert run.created is not None
    assert run.updated is not None


async def test_get(setup: Setup) -> None:
    """Load a stored run by id."""
    (
        repository,
        owner_id,
        make_experiment_id,
        make_cohort_version_id,
        make_agent_version_id,
    ) = setup
    created = await repository.create(
        _run(
            owner_id,
            await make_experiment_id(),
            await make_cohort_version_id(),
            await make_agent_version_id(),
        )
    )
    loaded = await repository.get(created.id)
    assert loaded == created


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown run id."""
    repository, *_ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentRunNotFound, match=f"Experiment run {missing_id} was not found"
    ):
        await repository.get(missing_id)


async def test_duplicate_run_number_conflicts(setup: Setup) -> None:
    """Reject a second run with the same (experiment, number) pair."""
    (
        repository,
        owner_id,
        make_experiment_id,
        make_cohort_version_id,
        make_agent_version_id,
    ) = setup
    experiment_id = await make_experiment_id()
    agent_version_id = await make_agent_version_id()
    await repository.create(
        _run(
            owner_id,
            experiment_id,
            await make_cohort_version_id(),
            agent_version_id,
            number=1,
        )
    )
    with pytest.raises(DuplicateExperimentRunNumber):
        await repository.create(
            _run(
                owner_id,
                experiment_id,
                await make_cohort_version_id(),
                agent_version_id,
                number=1,
            )
        )


async def test_update_renews_timestamp(setup: Setup) -> None:
    """Persist status changes and renew the updated timestamp."""
    (
        repository,
        owner_id,
        make_experiment_id,
        make_cohort_version_id,
        make_agent_version_id,
    ) = setup
    created = await repository.create(
        _run(
            owner_id,
            await make_experiment_id(),
            await make_cohort_version_id(),
            await make_agent_version_id(),
        )
    )
    created.cancel()
    updated = await repository.update(created)
    assert updated.status is ExperimentRunStatus.CANCELING
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated


async def test_delete(setup: Setup) -> None:
    """Delete a stored run."""
    (
        repository,
        owner_id,
        make_experiment_id,
        make_cohort_version_id,
        make_agent_version_id,
    ) = setup
    created = await repository.create(
        _run(
            owner_id,
            await make_experiment_id(),
            await make_cohort_version_id(),
            await make_agent_version_id(),
        )
    )
    await repository.delete(created.id)
    with pytest.raises(ExperimentRunNotFound):
        await repository.get(created.id)


async def test_query_filters_by_experiment_and_status(setup: Setup) -> None:
    """Filter runs by experiment id and status."""
    (
        repository,
        owner_id,
        make_experiment_id,
        make_cohort_version_id,
        make_agent_version_id,
    ) = setup
    experiment_id = await make_experiment_id()
    agent_version_id = await make_agent_version_id()
    matching = await repository.create(
        _run(owner_id, experiment_id, await make_cohort_version_id(), agent_version_id)
    )
    other_experiment = await repository.create(
        _run(
            owner_id,
            await make_experiment_id(),
            await make_cohort_version_id(),
            agent_version_id,
        )
    )

    runs, next_cursor = await repository.query(
        ExperimentRunFilter(
            expression=FilterCondition(
                field="experiment_id", op=FilterOp.EQ, value=experiment_id
            )
        )
    )
    assert next_cursor is None
    assert [run.id for run in runs] == [matching.id]

    runs, _ = await repository.query(
        ExperimentRunFilter(
            expression=FilterCondition(
                field="status", op=FilterOp.EQ, value=ExperimentRunStatus.RUNNING
            )
        )
    )
    assert {run.id for run in runs} >= {matching.id, other_experiment.id}


async def test_query_filters_by_cohort_version(setup: Setup) -> None:
    """Filter runs by cohort version id."""
    (
        repository,
        owner_id,
        make_experiment_id,
        make_cohort_version_id,
        make_agent_version_id,
    ) = setup
    experiment_id = await make_experiment_id()
    agent_version_id = await make_agent_version_id()
    cohort_version_id = await make_cohort_version_id()
    matching = await repository.create(
        _run(owner_id, experiment_id, cohort_version_id, agent_version_id, number=1)
    )
    await repository.create(
        _run(
            owner_id,
            experiment_id,
            await make_cohort_version_id(),
            agent_version_id,
            number=2,
        )
    )

    runs, next_cursor = await repository.query(
        ExperimentRunFilter(
            expression=FilterCondition(
                field="cohort_version_id", op=FilterOp.EQ, value=cohort_version_id
            )
        )
    )
    assert next_cursor is None
    assert [run.id for run in runs] == [matching.id]


async def test_get_max_number(setup: Setup) -> None:
    """Read the highest run number, 0 when the experiment has no runs."""
    (
        repository,
        owner_id,
        make_experiment_id,
        make_cohort_version_id,
        make_agent_version_id,
    ) = setup
    experiment_id = await make_experiment_id()
    agent_version_id = await make_agent_version_id()
    assert await repository.get_max_number(experiment_id) == 0
    await repository.create(
        _run(
            owner_id,
            experiment_id,
            await make_cohort_version_id(),
            agent_version_id,
            number=1,
        )
    )
    await repository.create(
        _run(
            owner_id,
            experiment_id,
            await make_cohort_version_id(),
            agent_version_id,
            number=2,
        )
    )
    assert await repository.get_max_number(experiment_id) == 2


async def test_exists_for_experiment(setup: Setup) -> None:
    """Report whether an experiment has any run."""
    (
        repository,
        owner_id,
        make_experiment_id,
        make_cohort_version_id,
        make_agent_version_id,
    ) = setup
    experiment_id = await make_experiment_id()
    assert await repository.exists_for_experiment(experiment_id) is False
    await repository.create(
        _run(
            owner_id,
            experiment_id,
            await make_cohort_version_id(),
            await make_agent_version_id(),
        )
    )
    assert await repository.exists_for_experiment(experiment_id) is True


async def test_query_filters_by_agent_version(setup: Setup) -> None:
    """Filter runs by the agent version they replayed."""
    (
        repository,
        owner_id,
        make_experiment_id,
        make_cohort_version_id,
        make_agent_version_id,
    ) = setup
    agent_version_id = await make_agent_version_id()
    matching = await repository.create(
        _run(
            owner_id,
            await make_experiment_id(),
            await make_cohort_version_id(),
            agent_version_id,
        )
    )
    await repository.create(
        _run(
            owner_id,
            await make_experiment_id(),
            await make_cohort_version_id(),
            await make_agent_version_id(),
        )
    )

    runs, next_cursor = await repository.query(
        ExperimentRunFilter(
            expression=FilterCondition(
                field="agent_version_id", op=FilterOp.EQ, value=agent_version_id
            )
        )
    )
    assert next_cursor is None
    assert [run.id for run in runs] == [matching.id]


async def test_query_filters_by_cohort_spanning_versions() -> None:
    """Filter runs by cohort, matching runs against any of its versions.

    Postgres-only: a run stores a cohort version, so resolving the cohort
    behind it is a subquery the fake resolves from a different direction.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name="agent")
        )
        agent_version = await SQLAgentVersionRepository(session).create(
            AgentVersion(owner_id=owner.id, agent_id=agent.id)
        )
        experiments = SQLExperimentRepository(session)
        config = await experiments.create_replay_config(
            ReplayConfig(
                owner_id=owner.id,
                tool_policy=ToolPolicy(default=PassthroughConfig()),
                evaluators=[],
            )
        )
        experiment = await experiments.create(
            Experiment(owner_id=owner.id, name="experiment", replay_config_id=config.id)
        )
        cohorts = SQLCohortRepository(session)
        cohort_versions = SQLCohortVersionRepository(session)
        cohort = await cohorts.create(
            Cohort(owner_id=owner.id, name="cohort", agent_id=agent.id)
        )
        first = await cohort_versions.create(
            CohortVersion(owner_id=owner.id, cohort_id=cohort.id, session_count=0),
            [],
        )
        second = await cohort_versions.create(
            CohortVersion(owner_id=owner.id, cohort_id=cohort.id, session_count=0),
            [],
        )
        other_cohort = await cohorts.create(
            Cohort(owner_id=owner.id, name="other-cohort", agent_id=agent.id)
        )
        other_version = await cohort_versions.create(
            CohortVersion(
                owner_id=owner.id, cohort_id=other_cohort.id, session_count=0
            ),
            [],
        )

        repository = SQLExperimentRunRepository(session)
        on_first = await repository.create(
            _run(owner.id, experiment.id, first.id, agent_version.id, number=1)
        )
        on_second = await repository.create(
            _run(owner.id, experiment.id, second.id, agent_version.id, number=2)
        )
        await repository.create(
            _run(owner.id, experiment.id, other_version.id, agent_version.id, number=3)
        )

        runs, _ = await repository.query(
            ExperimentRunFilter(
                expression=FilterCondition(
                    field="cohort_id", op=FilterOp.EQ, value=cohort.id
                )
            )
        )
        assert {run.id for run in runs} == {on_first.id, on_second.id}

        runs, _ = await repository.query(
            ExperimentRunFilter(
                expression=FilterCondition(
                    field="cohort_id", op=FilterOp.EQ, value=uuid.uuid4()
                )
            )
        )
        assert runs == []


async def test_query_filters_by_agent_spanning_versions() -> None:
    """Filter runs by agent, matching runs against any of its versions.

    Postgres-only: a run stores an agent version, so resolving the agent
    behind it is a subquery the fake has no handle on.
    """
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agents = SQLAgentRepository(session)
        agent_versions = SQLAgentVersionRepository(session)
        agent = await agents.create(Agent(owner_id=owner.id, name="agent"))
        first_version = await agent_versions.create(
            AgentVersion(owner_id=owner.id, agent_id=agent.id)
        )
        second_version = await agent_versions.create(
            AgentVersion(owner_id=owner.id, agent_id=agent.id)
        )
        other_agent = await agents.create(Agent(owner_id=owner.id, name="other-agent"))
        other_version = await agent_versions.create(
            AgentVersion(owner_id=owner.id, agent_id=other_agent.id)
        )
        experiments = SQLExperimentRepository(session)
        config = await experiments.create_replay_config(
            ReplayConfig(
                owner_id=owner.id,
                tool_policy=ToolPolicy(default=PassthroughConfig()),
                evaluators=[],
            )
        )
        experiment = await experiments.create(
            Experiment(owner_id=owner.id, name="experiment", replay_config_id=config.id)
        )
        cohorts = SQLCohortRepository(session)
        cohort_versions = SQLCohortVersionRepository(session)
        cohort = await cohorts.create(
            Cohort(owner_id=owner.id, name="cohort", agent_id=agent.id)
        )
        cohort_version = await cohort_versions.create(
            CohortVersion(owner_id=owner.id, cohort_id=cohort.id, session_count=0),
            [],
        )

        repository = SQLExperimentRunRepository(session)
        on_first = await repository.create(
            _run(owner.id, experiment.id, cohort_version.id, first_version.id, number=1)
        )
        on_second = await repository.create(
            _run(
                owner.id, experiment.id, cohort_version.id, second_version.id, number=2
            )
        )
        await repository.create(
            _run(owner.id, experiment.id, cohort_version.id, other_version.id, number=3)
        )

        runs, _ = await repository.query(
            ExperimentRunFilter(
                expression=FilterCondition(
                    field="agent_id", op=FilterOp.EQ, value=agent.id
                )
            )
        )
        assert {run.id for run in runs} == {on_first.id, on_second.id}


async def test_fake_query_filters_by_cohort_spanning_versions() -> None:
    """The fake resolves the cohort filter the way the SQL repository does.

    The Postgres twin of this test skips wherever no database is reachable,
    which is every pull request, so the semantics are pinned here too.
    """
    owner_id = uuid.uuid4()
    tags = FakeTagRepository()
    cohorts = FakeCohortRepository(tags=tags)
    sessions = FakeSessionRepository()
    runs = FakeExperimentRunRepository(tag_repository=tags)
    cohort_versions = FakeCohortVersionRepository(
        cohorts=cohorts, sessions=sessions, experiment_runs=runs
    )
    cohort = await cohorts.create(
        Cohort(owner_id=owner_id, name="cohort", agent_id=uuid.uuid4())
    )
    other_cohort = await cohorts.create(
        Cohort(owner_id=owner_id, name="other-cohort", agent_id=uuid.uuid4())
    )
    first = await cohort_versions.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort.id, session_count=0), []
    )
    second = await cohort_versions.create(
        CohortVersion(owner_id=owner_id, cohort_id=cohort.id, session_count=0), []
    )
    other_version = await cohort_versions.create(
        CohortVersion(owner_id=owner_id, cohort_id=other_cohort.id, session_count=0),
        [],
    )

    experiment_id = uuid.uuid4()
    on_first = await runs.create(
        _run(owner_id, experiment_id, first.id, uuid.uuid4(), number=1)
    )
    on_second = await runs.create(
        _run(owner_id, experiment_id, second.id, uuid.uuid4(), number=2)
    )
    await runs.create(
        _run(owner_id, experiment_id, other_version.id, uuid.uuid4(), number=3)
    )

    matching, _ = await runs.query(
        ExperimentRunFilter(
            expression=FilterCondition(
                field="cohort_id", op=FilterOp.EQ, value=cohort.id
            )
        )
    )
    assert {run.id for run in matching} == {on_first.id, on_second.id}

    empty, _ = await runs.query(
        ExperimentRunFilter(
            expression=FilterCondition(
                field="cohort_id", op=FilterOp.EQ, value=uuid.uuid4()
            )
        )
    )
    assert empty == []


async def test_fake_refuses_the_agent_filter() -> None:
    """The fake refuses the agent filter rather than under-filtering.

    Resolving it needs an agent version lookup the fake has no handle on, so
    a service test that reaches for it should fail loudly, not silently pass.
    """
    owner_id = uuid.uuid4()
    runs = FakeExperimentRunRepository()
    await runs.create(
        _run(owner_id, uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), number=1)
    )
    with pytest.raises(NotImplementedError, match="agent_id"):
        await runs.query(
            ExperimentRunFilter(
                expression=FilterCondition(
                    field="agent_id", op=FilterOp.EQ, value=uuid.uuid4()
                )
            )
        )


async def test_fake_refuses_the_agent_filter_on_an_empty_store() -> None:
    """Refuse the agent filter before any run is evaluated.

    Refusing per stored run would stay silent here, and a test asserting an
    empty page would pass without the filter ever running.
    """
    runs = FakeExperimentRunRepository()
    with pytest.raises(NotImplementedError, match="agent_id"):
        await runs.query(
            ExperimentRunFilter(
                expression=FilterCondition(
                    field="agent_id", op=FilterOp.EQ, value=uuid.uuid4()
                )
            )
        )


async def test_fake_refuses_the_agent_filter_behind_a_false_operand() -> None:
    """Refuse the agent filter even when an earlier operand answers false.

    `all()` short-circuits, so a per-run refusal never fires when the first
    condition already excluded the run.
    """
    owner_id = uuid.uuid4()
    runs = FakeExperimentRunRepository()
    await runs.create(
        _run(owner_id, uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), number=1)
    )
    with pytest.raises(NotImplementedError, match="agent_id"):
        await runs.query(
            ExperimentRunFilter(
                expression=AndExpression(
                    operands=(
                        FilterCondition(
                            field="experiment_id", op=FilterOp.EQ, value=uuid.uuid4()
                        ),
                        FilterCondition(
                            field="agent_id", op=FilterOp.EQ, value=uuid.uuid4()
                        ),
                    )
                )
            )
        )
