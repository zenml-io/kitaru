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
"""Contract tests for experiment repositories."""

import uuid
from collections.abc import AsyncGenerator
from typing import NamedTuple

import pytest

from conftest import (
    FakeAgentRepository,
    FakeAgentVersionRepository,
    FakeCohortRepository,
    FakeExperimentRepository,
    FakeExperimentRunRepository,
    FakeJobRepository,
    FakeReplayConfigRepository,
    FakeSessionRepository,
    FakeTagRepository,
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
from kitaru.server.adapters.db.repositories.cohort_repository import (
    SQLCohortRepository,
)
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
)
from kitaru.server.adapters.db.repositories.experiment_run_repository import (
    SQLExperimentRunRepository,
)
from kitaru.server.adapters.db.repositories.replay_config_repository import (
    SQLReplayConfigRepository,
)
from kitaru.server.adapters.db.repositories.session_repository import (
    SQLSessionRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import (
    SQLTagRepository,
)
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.agent_version_repository import (
    AgentVersionRepository,
)
from kitaru.server.application.interfaces.cohort_repository import (
    CohortRepository,
)
from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRepository,
)
from kitaru.server.application.interfaces.experiment_run_repository import (
    ExperimentRunRepository,
)
from kitaru.server.application.interfaces.replay_config_repository import (
    ReplayConfigRepository,
)
from kitaru.server.application.interfaces.session_repository import (
    SessionRepository,
)
from kitaru.server.application.interfaces.tag_repository import TagRepository
from kitaru.server.application.models.experiments import ExperimentFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersion, RunSpec
from kitaru.server.domain.cohort import Cohort, CohortInUse, CohortNotFound
from kitaru.server.domain.experiment import (
    DuplicateExperimentName,
    Experiment,
    ExperimentInUse,
    ExperimentNotFound,
)
from kitaru.server.domain.experiment_run import ExperimentRun
from kitaru.server.domain.replay_config import (
    PassthroughPolicy,
    ReplayConfig,
    ReplayConfigNotFound,
    ReplayOverride,
    ScorerConfig,
    ScoringPolicy,
    SourceRef,
    ToolPolicyConfig,
)
from kitaru.server.domain.session import Session, SessionOrigin, SessionStatus
from kitaru.server.domain.tag import (
    Tag,
    TagLink,
    TagLinkNotFound,
    TagResourceType,
)

SCORING_POLICY = ScoringPolicy(
    scorers=[
        ScorerConfig(
            name="conciseness",
            source=SourceRef(module="my_pkg.scorers", attribute="conciseness"),
        )
    ],
    pass_threshold=0.5,
)


class Setup(NamedTuple):
    """Repository bundle for experiment contract tests."""

    experiments: ExperimentRepository
    runs: ExperimentRunRepository
    configs: ReplayConfigRepository
    cohorts: CohortRepository
    sessions: SessionRepository
    versions: AgentVersionRepository
    agents: AgentRepository
    tags: TagRepository
    owner_id: uuid.UUID


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each experiment repository implementation plus an owner id."""
    if request.param == "fake":
        agents = FakeAgentRepository()
        tags = FakeTagRepository()
        versions = FakeAgentVersionRepository(agents)
        sessions = FakeSessionRepository(agents, versions, tags)
        cohorts = FakeCohortRepository(sessions, agents, tags)
        configs = FakeReplayConfigRepository()
        experiments = FakeExperimentRepository(cohorts, configs, tags)
        jobs = FakeJobRepository(sessions, versions, configs)
        runs = FakeExperimentRunRepository(experiments, jobs, tags)
        yield Setup(
            experiments,
            runs,
            configs,
            cohorts,
            sessions,
            versions,
            agents,
            tags,
            uuid.uuid4(),
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        # The owner_id column has a foreign key to the account table, so
        # store the owning account first.
        accounts = SQLAccountRepository(session)
        owner = await accounts.create(Account(name="owner"))
        yield Setup(
            SQLExperimentRepository(session),
            SQLExperimentRunRepository(session),
            SQLReplayConfigRepository(session),
            SQLCohortRepository(session),
            SQLSessionRepository(session),
            SQLAgentVersionRepository(session),
            SQLAgentRepository(session),
            SQLTagRepository(session),
            owner.id,
        )


async def seed_cohort(setup: Setup, name: str = "baseline") -> Cohort:
    """Store an agent with one completed session inside a cohort.

    Args:
        setup: Repository bundle.
        name: Cohort name.

    Returns:
        Stored cohort.
    """
    agent = await setup.agents.create(
        Agent(owner_id=setup.owner_id, name=f"{name}-bot")
    )
    session = await setup.sessions.create(
        Session(
            owner_id=setup.owner_id,
            agent_id=agent.id,
            origin=SessionOrigin.RECORDED,
            status=SessionStatus.COMPLETED,
        )
    )
    return await setup.cohorts.create(
        Cohort(owner_id=setup.owner_id, name=name, agent_id=agent.id, session_count=1),
        [session.id],
    )


async def seed_config(setup: Setup) -> ReplayConfig:
    """Store a replay config.

    Args:
        setup: Repository bundle.

    Returns:
        Stored replay config.
    """
    return await setup.configs.create(
        ReplayConfig(
            owner_id=setup.owner_id,
            tool_policy=ToolPolicyConfig(default=PassthroughPolicy()),
            scoring_policy=SCORING_POLICY,
        )
    )


def experiment_entity(
    owner_id: uuid.UUID,
    cohort_id: uuid.UUID,
    replay_config_id: uuid.UUID,
    **overrides: object,
) -> Experiment:
    """Build an experiment entity.

    Args:
        owner_id: Id of the owning account.
        cohort_id: Id of the cohort.
        replay_config_id: Id of the replay config.
        **overrides: Field overrides.

    Returns:
        Experiment entity.
    """
    values: dict[str, object] = {
        "owner_id": owner_id,
        "cohort_id": cohort_id,
        "replay_config_id": replay_config_id,
        "name": "swap-model",
        **overrides,
    }
    return Experiment.model_validate(values)


async def seed_experiment(setup: Setup, **overrides: object) -> Experiment:
    """Store an experiment over a fresh cohort and config.

    Args:
        setup: Repository bundle.
        **overrides: Experiment field overrides.

    Returns:
        Stored experiment.
    """
    name = str(overrides.get("name", "swap-model"))
    cohort = await seed_cohort(setup, name=f"{name}-cohort")
    config = await seed_config(setup)
    return await setup.experiments.create(
        experiment_entity(setup.owner_id, cohort.id, config.id, **overrides)
    )


async def seed_run(setup: Setup, experiment: Experiment) -> ExperimentRun:
    """Store a run of an experiment.

    Args:
        setup: Repository bundle.
        experiment: Experiment to run.

    Returns:
        Stored experiment run.
    """
    cohort = await setup.cohorts.get(experiment.cohort_id)
    version = await setup.versions.create(
        AgentVersion(
            owner_id=setup.owner_id,
            agent_id=cohort.agent_id,
            version="v1",
            run_spec=RunSpec(command="python agent.py", timeout_seconds=600),
        )
    )
    return await setup.runs.create(
        ExperimentRun(
            owner_id=setup.owner_id,
            experiment_id=experiment.id,
            agent_version_id=version.id,
        ),
        [],
    )


async def test_create_round_trips_all_fields(setup: Setup) -> None:
    """Store an experiment and round-trip every field."""
    cohort = await seed_cohort(setup)
    config = await seed_config(setup)
    experiment = experiment_entity(
        setup.owner_id, cohort.id, config.id, description="Swap the model"
    )
    created = await setup.experiments.create(experiment)
    assert created.created is not None
    assert created.updated is not None
    loaded = await setup.experiments.get(created.id)
    assert loaded == created
    assert loaded.name == "swap-model"
    assert loaded.description == "Swap the model"
    assert loaded.cohort_id == cohort.id
    assert loaded.replay_config_id == config.id


async def test_create_duplicate_name(setup: Setup) -> None:
    """Reject a second experiment with the same name."""
    cohort = await seed_cohort(setup)
    config = await seed_config(setup)
    await setup.experiments.create(
        experiment_entity(setup.owner_id, cohort.id, config.id)
    )
    with pytest.raises(
        DuplicateExperimentName,
        match="Experiment name 'swap-model' is already registered",
    ):
        await setup.experiments.create(
            experiment_entity(setup.owner_id, cohort.id, config.id)
        )
    # The failed create leaves the repository usable.
    created = await setup.experiments.create(
        experiment_entity(setup.owner_id, cohort.id, config.id, name="other")
    )
    assert created.name == "other"


async def test_create_unknown_cohort(setup: Setup) -> None:
    """Raise for an unknown cohort id."""
    config = await seed_config(setup)
    missing_id = uuid.uuid4()
    with pytest.raises(CohortNotFound, match=f"Cohort {missing_id} was not found"):
        await setup.experiments.create(
            experiment_entity(setup.owner_id, missing_id, config.id)
        )


async def test_create_unknown_config(setup: Setup) -> None:
    """Raise for an unknown replay config id."""
    cohort = await seed_cohort(setup)
    missing_id = uuid.uuid4()
    with pytest.raises(
        ReplayConfigNotFound, match=f"Replay config {missing_id} was not found"
    ):
        await setup.experiments.create(
            experiment_entity(setup.owner_id, cohort.id, missing_id)
        )


async def test_get_not_found(setup: Setup) -> None:
    """Raise for an unknown experiment id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentNotFound, match=f"Experiment {missing_id} was not found"
    ):
        await setup.experiments.get(missing_id)


async def test_query(setup: Setup) -> None:
    """Query experiments by name with pagination."""
    cohort = await seed_cohort(setup)
    config = await seed_config(setup)
    for name in ["one", "two", "three"]:
        await setup.experiments.create(
            experiment_entity(setup.owner_id, cohort.id, config.id, name=name)
        )

    experiments, total = await setup.experiments.query(ExperimentFilter())
    assert total == 3
    assert [experiment.name for experiment in experiments] == ["one", "two", "three"]

    experiments, total = await setup.experiments.query(ExperimentFilter(name="two"))
    assert total == 1

    experiments, total = await setup.experiments.query(
        ExperimentFilter(page=2, page_size=2)
    )
    assert total == 3
    assert [experiment.name for experiment in experiments] == ["three"]


async def test_query_by_tag(setup: Setup) -> None:
    """Query experiments attached to a tag name."""
    cohort = await seed_cohort(setup)
    config = await seed_config(setup)
    tagged = await setup.experiments.create(
        experiment_entity(setup.owner_id, cohort.id, config.id, name="tagged")
    )
    await setup.experiments.create(
        experiment_entity(setup.owner_id, cohort.id, config.id, name="other")
    )
    tag = await setup.tags.create(Tag(owner_id=setup.owner_id, name="prod"))
    await setup.tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.EXPERIMENT,
            resource_id=tagged.id,
        )
    )

    experiments, total = await setup.experiments.query(ExperimentFilter(tag="prod"))
    assert total == 1
    assert experiments[0].id == tagged.id

    experiments, total = await setup.experiments.query(ExperimentFilter(tag="missing"))
    assert total == 0


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    created = await seed_experiment(setup)
    other_cohort = await seed_cohort(setup, name="other")
    new_config = await seed_config(setup)
    created.update_name("swap-model-v2")
    created.update_description("Second try")
    created.update_cohort_id(other_cohort.id, frozen=False)
    created.update_replay_config_id(new_config.id, frozen=False)
    updated = await setup.experiments.update(created)
    assert updated.name == "swap-model-v2"
    assert updated.description == "Second try"
    assert updated.cohort_id == other_cohort.id
    assert updated.replay_config_id == new_config.id
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated > created.updated
    loaded = await setup.experiments.get(created.id)
    assert loaded == updated


async def test_update_duplicate_name(setup: Setup) -> None:
    """Reject a new name that is already registered."""
    await seed_experiment(setup)
    other = await seed_experiment(setup, name="other")
    other.update_name("swap-model")
    with pytest.raises(
        DuplicateExperimentName,
        match="Experiment name 'swap-model' is already registered",
    ):
        await setup.experiments.update(other)


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown experiment id."""
    cohort = await seed_cohort(setup)
    config = await seed_config(setup)
    experiment = experiment_entity(setup.owner_id, cohort.id, config.id)
    with pytest.raises(
        ExperimentNotFound, match=f"Experiment {experiment.id} was not found"
    ):
        await setup.experiments.update(experiment)


async def test_delete_removes_tag_links(setup: Setup) -> None:
    """Delete an experiment with its tag links."""
    created = await seed_experiment(setup)
    tag = await setup.tags.create(Tag(owner_id=setup.owner_id, name="prod"))
    await setup.tags.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.EXPERIMENT,
            resource_id=created.id,
        )
    )
    await setup.experiments.delete(created.id)
    with pytest.raises(ExperimentNotFound):
        await setup.experiments.get(created.id)
    with pytest.raises(TagLinkNotFound):
        await setup.tags.delete_link(tag.id, TagResourceType.EXPERIMENT, created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown experiment id."""
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentNotFound, match=f"Experiment {missing_id} was not found"
    ):
        await setup.experiments.delete(missing_id)


async def test_delete_blocked_while_runs_exist(setup: Setup) -> None:
    """Block deleting an experiment that has runs."""
    created = await seed_experiment(setup)
    await seed_run(setup, created)
    with pytest.raises(
        ExperimentInUse,
        match=f"Experiment {created.id} is referenced by experiment runs",
    ):
        await setup.experiments.delete(created.id)
    # The failed delete leaves the repository usable.
    loaded = await setup.experiments.get(created.id)
    assert loaded.id == created.id


async def test_cohort_delete_blocked_while_referenced(setup: Setup) -> None:
    """Block deleting a cohort that an experiment references."""
    created = await seed_experiment(setup)
    with pytest.raises(
        CohortInUse,
        match=f"Cohort {created.cohort_id} is referenced by experiments",
    ):
        await setup.cohorts.delete(created.cohort_id)
    await setup.experiments.delete(created.id)
    await setup.cohorts.delete(created.cohort_id)


async def test_config_delete_if_unreferenced(setup: Setup) -> None:
    """Delete a config row only while nothing references it."""
    created = await seed_experiment(setup)
    assert await setup.configs.delete_if_unreferenced(created.replay_config_id) is False
    loaded = await setup.configs.get(created.replay_config_id)
    assert loaded.id == created.replay_config_id
    await setup.experiments.delete(created.id)
    assert await setup.configs.delete_if_unreferenced(created.replay_config_id) is True
    with pytest.raises(ReplayConfigNotFound):
        await setup.configs.get(created.replay_config_id)


async def test_config_round_trip(setup: Setup) -> None:
    """Store a replay config and round-trip the JSONB payloads."""
    override = ReplayOverride(
        model={"gpt-4o": "claude-sonnet-5"},
        system_prompt="Be terse.",
        model_params={"temperature": 0.2},
    )
    created = await setup.configs.create(
        ReplayConfig(
            owner_id=setup.owner_id,
            override=override,
            tool_policy=ToolPolicyConfig(default=PassthroughPolicy()),
            scoring_policy=SCORING_POLICY,
        )
    )
    loaded = await setup.configs.get(created.id)
    assert loaded == created
    assert loaded.override == override
    assert loaded.scoring_policy == SCORING_POLICY
    many = await setup.configs.get_many([created.id, uuid.uuid4()])
    assert set(many) == {created.id}
