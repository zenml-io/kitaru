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
"""Tests for experiment use cases."""

import uuid

import pytest

from conftest import (
    FakeExperimentRepository,
    FakePluginRepository,
    FakeTagRepository,
    create_plugin,
)
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.experiment import (
    ExperimentCreate,
    ExperimentFilter,
    ExperimentUpdate,
)
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.application.services.experiment_service import ExperimentService
from kitaru.server.domain.account import Account
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.experiment import DuplicateExperimentName, ExperimentNotFound
from kitaru.server.domain.plugin import PackagePluginSource, PluginKind, PluginNotFound
from kitaru.server.domain.replay_config import (
    PassthroughConfig,
    ReplayConfigNotFound,
    ReplayOverride,
    ToolPolicy,
)
from kitaru.server.domain.tag import Tag, TagLink

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))

SOURCE = PackagePluginSource(requirement="kitaru-scorer==1.0.0", entrypoint="pkg:score")


@pytest.fixture
def plugin_repository() -> FakePluginRepository:
    """Provide a fake plugin repository."""
    return FakePluginRepository()


@pytest.fixture
def repository() -> FakeExperimentRepository:
    """Provide a fake experiment repository."""
    return FakeExperimentRepository()


@pytest.fixture
def service(
    repository: FakeExperimentRepository, plugin_repository: FakePluginRepository
) -> ExperimentService:
    """Provide an experiment service backed by the fake repositories."""
    return ExperimentService(repository=repository, plugin_repository=plugin_repository)


async def _register_evaluator(
    plugin_repository: FakePluginRepository, name: str = "accuracy"
) -> uuid.UUID:
    plugin = await create_plugin(
        plugin_repository, ACTOR.account.id, kind=PluginKind.EVALUATOR, name=name
    )
    version = await plugin_repository.create_version(
        plugin.id, SOURCE, display_version="v1"
    )
    return version.id


async def test_create_experiment_resolves_evaluators(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """Resolve evaluators and inline the replay config in the response."""
    version_id = await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    experiment, config = await service.create_experiment(command, actor=ACTOR)
    assert experiment.name == "exp1"
    assert experiment.owner_id == ACTOR.account.id
    assert experiment.replay_config_id == config.id
    assert config.owner_id == ACTOR.account.id
    assert config.override is None
    assert config.tool_policy == ToolPolicy(default=PassthroughConfig())
    assert len(config.evaluators) == 1
    assert config.evaluators[0].evaluator == "accuracy"
    assert config.evaluators[0].version == 1
    assert config.evaluators[0].evaluator_version_id == version_id


async def test_create_experiment_with_override_and_tool_policy(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """Store an explicit override and tool policy."""
    await _register_evaluator(plugin_repository)
    override = ReplayOverride(prompt="new prompt")
    tool_policy = ToolPolicy(default=PassthroughConfig())
    command = ExperimentCreate(
        name="exp1",
        override=override,
        tool_policy=tool_policy,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    _, config = await service.create_experiment(command, actor=ACTOR)
    assert config.override == override
    assert config.tool_policy == tool_policy


async def test_create_experiment_duplicate_name(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """Reject a second experiment with the same name."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1", evaluators=[EvaluatorConfigInput(evaluator="accuracy")]
    )
    await service.create_experiment(command, actor=ACTOR)
    with pytest.raises(DuplicateExperimentName):
        await service.create_experiment(command, actor=ACTOR)


async def test_create_experiment_unknown_evaluator(service: ExperimentService) -> None:
    """Raise when an evaluator config names an unknown evaluator."""
    command = ExperimentCreate(
        name="exp1", evaluators=[EvaluatorConfigInput(evaluator="missing")]
    )
    with pytest.raises(PluginNotFound):
        await service.create_experiment(command, actor=ACTOR)


async def test_create_experiment_duplicate_evaluator_version(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """Reject two evaluator configs resolving to the same version."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        evaluators=[
            EvaluatorConfigInput(evaluator="accuracy", version=1),
            EvaluatorConfigInput(evaluator="accuracy"),
        ],
    )
    with pytest.raises(ValidationError, match="appears more than once"):
        await service.create_experiment(command, actor=ACTOR)


async def test_get_experiment(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """Load a stored experiment and its replay config by id."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1", evaluators=[EvaluatorConfigInput(evaluator="accuracy")]
    )
    created, created_config = await service.create_experiment(command, actor=ACTOR)
    loaded, loaded_config = await service.get_experiment(created.id, actor=ACTOR)
    assert loaded == created
    assert loaded_config == created_config


async def test_get_experiment_not_found(service: ExperimentService) -> None:
    """Raise for an unknown experiment id."""
    with pytest.raises(ExperimentNotFound):
        await service.get_experiment(uuid.uuid4(), actor=ACTOR)


async def test_list_experiments(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """List experiments newest-first with a name filter."""
    await _register_evaluator(plugin_repository)
    for name in ["assistant", "reviewer"]:
        command = ExperimentCreate(
            name=name, evaluators=[EvaluatorConfigInput(evaluator="accuracy")]
        )
        await service.create_experiment(command, actor=ACTOR)

    pairs, next_cursor = await service.list_experiments(ExperimentFilter(), actor=ACTOR)
    assert next_cursor is None
    assert [experiment.name for experiment, _ in pairs] == ["reviewer", "assistant"]
    for _, config in pairs:
        assert config.evaluators[0].evaluator == "accuracy"


async def test_list_experiments_by_tag(
    plugin_repository: FakePluginRepository,
) -> None:
    """Filter experiments by a tag linked to the resource."""
    tag_repository = FakeTagRepository()
    repository = FakeExperimentRepository(tag_repository=tag_repository)
    service = ExperimentService(
        repository=repository, plugin_repository=plugin_repository
    )
    await _register_evaluator(plugin_repository)
    tagged_command = ExperimentCreate(
        name="tagged", evaluators=[EvaluatorConfigInput(evaluator="accuracy")]
    )
    untagged_command = ExperimentCreate(
        name="untagged", evaluators=[EvaluatorConfigInput(evaluator="accuracy")]
    )
    tagged, _ = await service.create_experiment(tagged_command, actor=ACTOR)
    await service.create_experiment(untagged_command, actor=ACTOR)

    tag = await tag_repository.create(Tag(owner_id=ACTOR.account.id, name="smoke"))
    await tag_repository.create_link(
        TagLink(
            tag_id=tag.id,
            resource_type=TagResourceType.EXPERIMENT,
            resource_id=tagged.id,
        )
    )

    pairs, next_cursor = await service.list_experiments(
        ExperimentFilter(tag="smoke"), actor=ACTOR
    )
    assert next_cursor is None
    assert [experiment.name for experiment, _ in pairs] == ["tagged"]


async def test_update_experiment_name(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """Update an experiment's name without touching its replay config."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1", evaluators=[EvaluatorConfigInput(evaluator="accuracy")]
    )
    created, created_config = await service.create_experiment(command, actor=ACTOR)
    updated, updated_config = await service.update_experiment(
        created.id, ExperimentUpdate(name="renamed"), actor=ACTOR
    )
    assert updated.name == "renamed"
    assert updated_config.id == created_config.id


async def test_update_experiment_cannot_clear_name(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """Reject clearing the experiment name with an explicit null."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1", evaluators=[EvaluatorConfigInput(evaluator="accuracy")]
    )
    created, _ = await service.create_experiment(command, actor=ACTOR)
    with pytest.raises(ValidationError, match="Experiment name cannot be cleared"):
        await service.update_experiment(
            created.id, ExperimentUpdate(name=None), actor=ACTOR
        )


async def test_update_experiment_new_evaluators_replaces_config(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    repository: FakeExperimentRepository,
) -> None:
    """Build a new replay config and delete the old one when evaluators change."""
    await _register_evaluator(plugin_repository)
    await _register_evaluator(plugin_repository, name="relevance")
    command = ExperimentCreate(
        name="exp1", evaluators=[EvaluatorConfigInput(evaluator="accuracy")]
    )
    created, old_config = await service.create_experiment(command, actor=ACTOR)

    updated, new_config = await service.update_experiment(
        created.id,
        ExperimentUpdate(evaluators=[EvaluatorConfigInput(evaluator="relevance")]),
        actor=ACTOR,
    )
    assert new_config.id != old_config.id
    assert updated.replay_config_id == new_config.id
    assert new_config.evaluators[0].evaluator == "relevance"
    with pytest.raises(ReplayConfigNotFound):
        await repository.get_replay_config(old_config.id)


async def test_update_experiment_cannot_clear_evaluators(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """Reject clearing every evaluator with an explicit null or empty list."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1", evaluators=[EvaluatorConfigInput(evaluator="accuracy")]
    )
    created, _ = await service.create_experiment(command, actor=ACTOR)
    with pytest.raises(ValidationError, match="evaluators cannot be cleared"):
        await service.update_experiment(
            created.id, ExperimentUpdate(evaluators=[]), actor=ACTOR
        )


async def test_update_experiment_cannot_clear_tool_policy(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """Reject clearing the tool policy with an explicit null."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1", evaluators=[EvaluatorConfigInput(evaluator="accuracy")]
    )
    created, _ = await service.create_experiment(command, actor=ACTOR)
    with pytest.raises(ValidationError, match="tool policy cannot be cleared"):
        await service.update_experiment(
            created.id, ExperimentUpdate(tool_policy=None), actor=ACTOR
        )


async def test_update_experiment_explicit_null_override_clears(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """Clear the override with an explicit null."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        override=ReplayOverride(prompt="hi"),
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, _ = await service.create_experiment(command, actor=ACTOR)
    _, config = await service.update_experiment(
        created.id, ExperimentUpdate(override=None), actor=ACTOR
    )
    assert config.override is None


async def test_update_experiment_omitted_fields_unchanged(
    service: ExperimentService, plugin_repository: FakePluginRepository
) -> None:
    """Leave the replay config untouched when the command sets none of it."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1",
        override=ReplayOverride(prompt="hi"),
        evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
    )
    created, created_config = await service.create_experiment(command, actor=ACTOR)
    updated, config = await service.update_experiment(
        created.id, ExperimentUpdate(description="new"), actor=ACTOR
    )
    assert updated.description == "new"
    assert config.id == created_config.id
    assert config.override == created_config.override


async def test_delete_experiment_removes_config(
    service: ExperimentService,
    plugin_repository: FakePluginRepository,
    repository: FakeExperimentRepository,
) -> None:
    """Delete an experiment and its replay config."""
    await _register_evaluator(plugin_repository)
    command = ExperimentCreate(
        name="exp1", evaluators=[EvaluatorConfigInput(evaluator="accuracy")]
    )
    created, config = await service.create_experiment(command, actor=ACTOR)
    await service.delete_experiment(created.id, actor=ACTOR)
    with pytest.raises(ExperimentNotFound):
        await service.get_experiment(created.id, actor=ACTOR)
    with pytest.raises(ReplayConfigNotFound):
        await repository.get_replay_config(config.id)


async def test_delete_experiment_not_found(service: ExperimentService) -> None:
    """Raise for an unknown experiment id."""
    with pytest.raises(ExperimentNotFound):
        await service.delete_experiment(uuid.uuid4(), actor=ACTOR)
