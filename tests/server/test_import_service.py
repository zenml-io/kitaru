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
"""Tests for the import service."""

import uuid

import pytest

from conftest import (
    JobAndTaskServices,
    build_job_and_task_services,
    create_agent,
    create_agent_version,
    create_blob,
    create_plugin,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.imports import ImportCreate, ImportFilter
from kitaru.server.application.models.replay_config import (
    AnalyzerConfigInput,
    EvaluatorConfigInput,
)
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersionAgentMismatch
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.imports import ImportNotFound
from kitaru.server.domain.plugin import (
    PluginKind,
    PluginNotFound,
    PluginVersion,
    ScriptPluginSource,
)
from kitaru.server.domain.task import ImportTask
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job, task, and import services."""
    return build_job_and_task_services()


async def _importer_version(services: JobAndTaskServices) -> PluginVersion:
    """Register the csv importer with one version."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.IMPORTER, name="csv"
    )
    return await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )


async def _evaluator_version(
    services: JobAndTaskServices, name: str, agent_id: uuid.UUID | None = None
) -> PluginVersion:
    """Register an evaluator with one version, scoped to an agent when given."""
    plugin = await create_plugin(
        services.plugins,
        ACTOR.account.id,
        PluginKind.EVALUATOR,
        name=name,
        agent_id=agent_id,
    )
    return await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="score"),
        display_version=None,
    )


async def _analyzer_version(services: JobAndTaskServices, name: str) -> PluginVersion:
    """Register an analyzer with one version."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.ANALYZER, name=name
    )
    return await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="analyze"),
        display_version=None,
    )


async def _import_command(
    services: JobAndTaskServices,
    agent: Agent | None = None,
    agent_version_id: uuid.UUID | None = None,
    evaluators: list[EvaluatorConfigInput] | None = None,
    analyzers: list[AnalyzerConfigInput] | None = None,
) -> ImportCreate:
    """Build a create command naming a stored payload and agent."""
    payload = await create_blob(services.blobs, ACTOR.account.id, content=b"csv-data")
    if agent is None:
        agent = await create_agent(services.agents, ACTOR.account.id)
    return ImportCreate(
        importer="csv",
        agent_id=agent.id,
        agent_version_id=agent_version_id,
        payload_blob_id=payload.id,
        params={"delimiter": ","},
        evaluators=evaluators if evaluators is not None else [],
        analyzers=analyzers if analyzers is not None else [],
    )


async def test_create_import_creates_the_row_job_and_task_together(
    services: JobAndTaskServices,
) -> None:
    """An import lands with its pending job and the one task linking it."""
    version = await _importer_version(services)
    command = await _import_command(services)

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert import_.owner_id == ACTOR.account.id
    assert import_.agent_id == command.agent_id
    assert import_.importer_version_id == version.id
    assert import_.payload_blob_id == command.payload_blob_id
    assert import_.params == {"delimiter": ","}
    assert import_.stats is None
    assert import_.error is None
    assert import_.job_id is not None
    job = await services.jobs.get(import_.job_id)
    assert job.kind is JobKind.IMPORT
    assert job.status is JobStatus.PENDING
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job.id), actor=ACTOR
    )
    assert len(tasks) == 1
    task = tasks[0]
    assert isinstance(task, ImportTask)
    assert task.import_id == import_.id
    assert task.job_id == job.id


async def test_create_import_stores_the_resolved_evaluators(
    services: JobAndTaskServices,
) -> None:
    """The import row carries the evaluators resolved to concrete versions."""
    await _importer_version(services)
    evaluator_version = await _evaluator_version(services, "accuracy")
    command = await _import_command(
        services,
        evaluators=[EvaluatorConfigInput(evaluator="accuracy", params={"k": 1})],
    )

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert len(import_.evaluators) == 1
    evaluator = import_.evaluators[0]
    assert evaluator.evaluator == "accuracy"
    assert evaluator.version == 1
    assert evaluator.params == {"k": 1}
    assert evaluator.evaluator_version_id == evaluator_version.id
    stored = await services.imports.get(import_.id)
    assert stored.evaluators == import_.evaluators


async def test_create_import_rejects_an_unknown_evaluator(
    services: JobAndTaskServices,
) -> None:
    """An evaluator config naming no evaluator is rejected."""
    await _importer_version(services)
    command = await _import_command(
        services, evaluators=[EvaluatorConfigInput(evaluator="does-not-exist")]
    )
    with pytest.raises(PluginNotFound):
        await services.import_service.create_import(command, actor=ACTOR)


async def test_create_import_rejects_an_evaluator_scoped_to_another_agent(
    services: JobAndTaskServices,
) -> None:
    """An evaluator scoped to a different agent than the import's is rejected."""
    await _importer_version(services)
    other = await create_agent(services.agents, ACTOR.account.id, name="other")
    await _evaluator_version(services, "accuracy", agent_id=other.id)
    command = await _import_command(
        services, evaluators=[EvaluatorConfigInput(evaluator="accuracy")]
    )
    with pytest.raises(ValidationError):
        await services.import_service.create_import(command, actor=ACTOR)


async def test_create_import_rejects_duplicate_evaluator_versions(
    services: JobAndTaskServices,
) -> None:
    """Two evaluator configs resolving to one version are rejected."""
    await _importer_version(services)
    await _evaluator_version(services, "accuracy")
    command = await _import_command(
        services,
        evaluators=[
            EvaluatorConfigInput(evaluator="accuracy"),
            EvaluatorConfigInput(evaluator="accuracy", version=1),
        ],
    )
    with pytest.raises(ValidationError):
        await services.import_service.create_import(command, actor=ACTOR)


async def test_create_import_stores_the_resolved_analyzers(
    services: JobAndTaskServices,
) -> None:
    """The import row carries the analyzers resolved to concrete versions."""
    await _importer_version(services)
    analyzer_version = await _analyzer_version(services, "trends")
    command = await _import_command(
        services,
        analyzers=[AnalyzerConfigInput(analyzer="trends", params={"k": 1})],
    )

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert len(import_.analyzers) == 1
    analyzer = import_.analyzers[0]
    assert analyzer.analyzer == "trends"
    assert analyzer.version == 1
    assert analyzer.params == {"k": 1}
    assert analyzer.analyzer_version_id == analyzer_version.id
    stored = await services.imports.get(import_.id)
    assert stored.analyzers == import_.analyzers


async def test_create_import_rejects_an_unknown_analyzer(
    services: JobAndTaskServices,
) -> None:
    """An analyzer config naming no analyzer is rejected."""
    await _importer_version(services)
    command = await _import_command(
        services, analyzers=[AnalyzerConfigInput(analyzer="does-not-exist")]
    )
    with pytest.raises(PluginNotFound):
        await services.import_service.create_import(command, actor=ACTOR)


async def test_create_import_rejects_duplicate_analyzer_versions(
    services: JobAndTaskServices,
) -> None:
    """Two analyzer configs resolving to one version are rejected."""
    await _importer_version(services)
    await _analyzer_version(services, "trends")
    command = await _import_command(
        services,
        analyzers=[
            AnalyzerConfigInput(analyzer="trends"),
            AnalyzerConfigInput(analyzer="trends", version=1),
        ],
    )
    with pytest.raises(ValidationError):
        await services.import_service.create_import(command, actor=ACTOR)


async def test_create_import_resolves_latest_version_by_default(
    services: JobAndTaskServices,
) -> None:
    """An omitted import version resolves to the importer's latest."""
    v1 = await _importer_version(services)
    plugin = await services.plugins.get(v1.plugin_id)
    v2 = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    command = await _import_command(services)

    import_ = await services.import_service.create_import(command, actor=ACTOR)
    assert import_.importer_version_id == v2.id
    assert import_.importer_version_id != v1.id


async def test_create_import_stamps_the_job_kind_import(
    services: JobAndTaskServices,
) -> None:
    """An import's job carries the import kind."""
    await _importer_version(services)
    command = await _import_command(services)

    import_ = await services.import_service.create_import(command, actor=ACTOR)
    assert import_.job_id is not None
    job = await services.jobs.get(import_.job_id)
    assert job.kind is JobKind.IMPORT


async def test_create_import_stamps_the_agent_version_on_the_import(
    services: JobAndTaskServices,
) -> None:
    """An import naming an agent version carries it on the import row."""
    await _importer_version(services)
    agent = await create_agent(services.agents, ACTOR.account.id)
    version = await create_agent_version(
        services.agent_versions, agent_id=agent.id, owner_id=ACTOR.account.id
    )
    command = await _import_command(services, agent=agent, agent_version_id=version.id)

    import_ = await services.import_service.create_import(command, actor=ACTOR)
    assert import_.agent_version_id == version.id


async def test_create_import_rejects_a_version_of_another_agent(
    services: JobAndTaskServices,
) -> None:
    """An import pairing an agent with another agent's version is rejected."""
    await _importer_version(services)
    agent = await create_agent(services.agents, ACTOR.account.id)
    other = await create_agent(services.agents, ACTOR.account.id, name="other")
    version = await create_agent_version(
        services.agent_versions, agent_id=other.id, owner_id=ACTOR.account.id
    )
    command = await _import_command(services, agent=agent, agent_version_id=version.id)

    with pytest.raises(AgentVersionAgentMismatch):
        await services.import_service.create_import(command, actor=ACTOR)


async def test_get_import(services: JobAndTaskServices) -> None:
    """A created import reads back by id."""
    await _importer_version(services)
    command = await _import_command(services)
    import_ = await services.import_service.create_import(command, actor=ACTOR)

    stored = await services.import_service.get_import(import_.id, actor=ACTOR)
    assert stored == import_


async def test_get_import_not_found(services: JobAndTaskServices) -> None:
    """An unknown import id raises."""
    with pytest.raises(ImportNotFound):
        await services.import_service.get_import(uuid.uuid4(), actor=ACTOR)


async def test_list_imports_filters_by_agent_id(
    services: JobAndTaskServices,
) -> None:
    """The list narrows to one agent's imports."""
    await _importer_version(services)
    agent = await create_agent(services.agents, ACTOR.account.id)
    other = await create_agent(services.agents, ACTOR.account.id, name="other")
    first = await services.import_service.create_import(
        await _import_command(services, agent=agent), actor=ACTOR
    )
    await services.import_service.create_import(
        await _import_command(services, agent=other), actor=ACTOR
    )

    imports, next_cursor = await services.import_service.list_imports(
        ImportFilter(
            expression=FilterCondition(field="agent_id", op=FilterOp.EQ, value=agent.id)
        ),
        actor=ACTOR,
    )
    assert next_cursor is None
    assert [import_.id for import_ in imports] == [first.id]
