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
"""Contract tests for experiment and replay config repositories."""

import uuid
from collections.abc import AsyncGenerator

import pytest

from conftest import (
    FakeExperimentRepository,
    FakeTagRepository,
    pg_session,
    postgres_available,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.adapters.db.repositories.account_repository import (
    SQLAccountRepository,
)
from kitaru.server.adapters.db.repositories.agent_repository import SQLAgentRepository
from kitaru.server.adapters.db.repositories.experiment_repository import (
    SQLExperimentRepository,
)
from kitaru.server.adapters.db.repositories.tag_repository import SQLTagRepository
from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRepository,
)
from kitaru.server.application.models.experiment import ExperimentFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.experiment import (
    DuplicateExperimentName,
    Experiment,
    ExperimentNotFound,
)
from kitaru.server.domain.replay_config import (
    EvaluatorConfig,
    ReplayConfig,
    ReplayConfigNotFound,
    default_tool_policy,
)
from kitaru.server.domain.tag import Tag, TagLink
from kitaru.server.filtering import FilterCondition

Setup = tuple[ExperimentRepository, uuid.UUID, uuid.UUID]


@pytest.fixture(params=["fake", "postgres"])
async def setup(request: pytest.FixtureRequest) -> AsyncGenerator[Setup, None]:
    """Provide each experiment repository implementation plus an owner id."""
    if request.param == "fake":
        yield FakeExperimentRepository(), uuid.uuid4(), uuid.uuid4()
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name="agent")
        )
        yield SQLExperimentRepository(session), owner.id, agent.id


def _config(owner_id: uuid.UUID) -> ReplayConfig:
    return ReplayConfig(
        owner_id=owner_id,
        tool_policy=default_tool_policy(),
        evaluators=[
            EvaluatorConfig(
                evaluator="accuracy", version=1, evaluator_version_id=uuid.uuid4()
            )
        ],
    )


def _experiment(
    owner_id: uuid.UUID,
    agent_id: uuid.UUID,
    replay_config_id: uuid.UUID,
    name: str = "exp",
) -> Experiment:
    return Experiment(
        owner_id=owner_id,
        name=name,
        agent_id=agent_id,
        replay_config_id=replay_config_id,
    )


async def test_create_replay_config_sets_timestamps(setup: Setup) -> None:
    """Store a new replay config with both timestamps set."""
    repository, owner_id, _ = setup
    config = await repository.create_replay_config(_config(owner_id))
    assert config.owner_id == owner_id
    assert config.created is not None
    assert config.updated is not None


async def test_get_replay_config(setup: Setup) -> None:
    """Load a stored replay config by id."""
    repository, owner_id, _ = setup
    created = await repository.create_replay_config(_config(owner_id))
    loaded = await repository.get_replay_config(created.id)
    assert loaded == created


async def test_get_replay_config_not_found(setup: Setup) -> None:
    """Raise for an unknown replay config id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        ReplayConfigNotFound, match=f"Replay config {missing_id} was not found"
    ):
        await repository.get_replay_config(missing_id)


async def test_delete_replay_config(setup: Setup) -> None:
    """Delete a stored replay config."""
    repository, owner_id, _ = setup
    created = await repository.create_replay_config(_config(owner_id))
    await repository.delete_replay_config(created.id)
    with pytest.raises(ReplayConfigNotFound):
        await repository.get_replay_config(created.id)


async def test_delete_replay_config_not_found(setup: Setup) -> None:
    """Raise for an unknown replay config id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(ReplayConfigNotFound):
        await repository.delete_replay_config(missing_id)


async def test_create_experiment_sets_timestamps(setup: Setup) -> None:
    """Store a new experiment with both timestamps set."""
    repository, owner_id, agent_id = setup
    config = await repository.create_replay_config(_config(owner_id))
    experiment = await repository.create(
        _experiment(owner_id, agent_id, config.id, name="assistant-eval")
    )
    assert experiment.name == "assistant-eval"
    assert experiment.replay_config_id == config.id
    assert experiment.created is not None
    assert experiment.updated is not None


async def test_create_experiment_duplicate_name(setup: Setup) -> None:
    """Reject a second experiment with the same name."""
    repository, owner_id, agent_id = setup
    config = await repository.create_replay_config(_config(owner_id))
    await repository.create(_experiment(owner_id, agent_id, config.id, name="exp"))
    with pytest.raises(
        DuplicateExperimentName, match="Experiment name 'exp' is already registered"
    ):
        await repository.create(_experiment(owner_id, agent_id, config.id, name="exp"))


async def test_get_experiment_not_found(setup: Setup) -> None:
    """Raise for an unknown experiment id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentNotFound, match=f"Experiment {missing_id} was not found"
    ):
        await repository.get(missing_id)


async def test_query_by_name(setup: Setup) -> None:
    """Query experiments newest-first with a name filter."""
    repository, owner_id, agent_id = setup
    config = await repository.create_replay_config(_config(owner_id))
    first = await repository.create(
        _experiment(owner_id, agent_id, config.id, name="assistant")
    )
    await repository.create(_experiment(owner_id, agent_id, config.id, name="reviewer"))

    experiments, next_cursor = await repository.query(ExperimentFilter())
    assert next_cursor is None
    assert [experiment.name for experiment in experiments] == ["reviewer", "assistant"]

    experiments, next_cursor = await repository.query(
        ExperimentFilter(
            expression=FilterCondition(field="name", op=FilterOp.EQ, value="assistant")
        )
    )
    assert next_cursor is None
    assert experiments == [first]


async def test_query_walks_pages(setup: Setup) -> None:
    """Walk every page via next_cursor without duplicates or gaps."""
    repository, owner_id, agent_id = setup
    config = await repository.create_replay_config(_config(owner_id))
    created = [
        await repository.create(
            _experiment(owner_id, agent_id, config.id, name=f"exp-{i}")
        )
        for i in range(5)
    ]
    expected_order = list(reversed(created))

    collected: list[Experiment] = []
    cursor = None
    while True:
        experiments, next_cursor = await repository.query(
            ExperimentFilter(cursor=cursor, size=2)
        )
        collected.extend(experiments)
        if next_cursor is None:
            break
        cursor = next_cursor

    assert collected == expected_order


async def test_update(setup: Setup) -> None:
    """Persist field changes and renew the updated timestamp."""
    repository, owner_id, agent_id = setup
    config = await repository.create_replay_config(_config(owner_id))
    created = await repository.create(_experiment(owner_id, agent_id, config.id))
    created.update_name("renamed")
    created.update_description("A new description")
    updated = await repository.update(created)
    assert updated.name == "renamed"
    assert updated.description == "A new description"
    assert updated.created == created.created
    assert updated.updated is not None
    assert created.updated is not None
    assert updated.updated >= created.updated
    loaded = await repository.get(created.id)
    assert loaded == updated


async def test_update_not_found(setup: Setup) -> None:
    """Raise for an unknown experiment id."""
    repository, owner_id, agent_id = setup
    experiment = _experiment(owner_id, agent_id, uuid.uuid4())
    with pytest.raises(
        ExperimentNotFound, match=f"Experiment {experiment.id} was not found"
    ):
        await repository.update(experiment)


async def test_update_replay_config_id(setup: Setup) -> None:
    """Repoint an experiment at a new replay config."""
    repository, owner_id, agent_id = setup
    old_config = await repository.create_replay_config(_config(owner_id))
    new_config = await repository.create_replay_config(_config(owner_id))
    created = await repository.create(_experiment(owner_id, agent_id, old_config.id))
    created.update_replay_config_id(new_config.id, has_runs=False)
    updated = await repository.update(created)
    assert updated.replay_config_id == new_config.id


async def test_delete(setup: Setup) -> None:
    """Delete a stored experiment."""
    repository, owner_id, agent_id = setup
    config = await repository.create_replay_config(_config(owner_id))
    created = await repository.create(_experiment(owner_id, agent_id, config.id))
    await repository.delete(created.id)
    with pytest.raises(ExperimentNotFound):
        await repository.get(created.id)


async def test_delete_not_found(setup: Setup) -> None:
    """Raise for an unknown experiment id."""
    repository, _, _ = setup
    missing_id = uuid.uuid4()
    with pytest.raises(
        ExperimentNotFound, match=f"Experiment {missing_id} was not found"
    ):
        await repository.delete(missing_id)


async def test_delete_experiment_then_config_succeeds(setup: Setup) -> None:
    """Delete an experiment before its config, avoiding the FK restrict."""
    repository, owner_id, agent_id = setup
    config = await repository.create_replay_config(_config(owner_id))
    created = await repository.create(_experiment(owner_id, agent_id, config.id))
    await repository.delete(created.id)
    await repository.delete_replay_config(config.id)
    with pytest.raises(ReplayConfigNotFound):
        await repository.get_replay_config(config.id)


TagSetup = tuple[
    ExperimentRepository, SQLTagRepository | FakeTagRepository, uuid.UUID, uuid.UUID
]


@pytest.fixture(params=["fake", "postgres"])
async def tag_setup(request: pytest.FixtureRequest) -> AsyncGenerator[TagSetup, None]:
    """Provide an experiment repository and a tag repository sharing state."""
    if request.param == "fake":
        tag_repository = FakeTagRepository()
        yield (
            FakeExperimentRepository(tag_repository=tag_repository),
            tag_repository,
            uuid.uuid4(),
            uuid.uuid4(),
        )
        return
    if not await postgres_available():
        pytest.skip("PostgreSQL is not reachable")
    async with pg_session() as session:
        owner = await SQLAccountRepository(session).create(Account(name="owner"))
        agent = await SQLAgentRepository(session).create(
            Agent(owner_id=owner.id, name="agent")
        )
        yield (
            SQLExperimentRepository(session),
            SQLTagRepository(session),
            owner.id,
            agent.id,
        )


async def test_query_by_tag(tag_setup: TagSetup) -> None:
    """Filter experiments by a tag linked to the resource."""
    repository, tag_repository, owner_id, agent_id = tag_setup
    config = await repository.create_replay_config(_config(owner_id))
    tagged = await repository.create(
        _experiment(owner_id, agent_id, config.id, name="tagged-exp")
    )
    await repository.create(
        _experiment(owner_id, agent_id, config.id, name="untagged-exp")
    )

    tag = await tag_repository.create(Tag(owner_id=owner_id, name="smoke"))
    await tag_repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.EXPERIMENT,
            resource_id=tagged.id,
        )
    )

    experiments, next_cursor = await repository.query(
        ExperimentFilter(
            expression=FilterCondition(field="tag", op=FilterOp.EQ, value="smoke")
        )
    )
    assert next_cursor is None
    assert experiments == [tagged]
