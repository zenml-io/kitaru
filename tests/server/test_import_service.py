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
from typing import Any

import pytest

from conftest import (
    JobAndTaskServices,
    build_job_and_task_services,
    create_agent,
    create_agent_version,
    create_blob,
    create_connection,
    create_plugin,
    create_secret,
    create_session,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.task import (
    REQUIRES_CREDENTIALS_LABEL,
    TaskOnFailure,
    TaskStatus,
)
from kitaru.server.api.bootstrap import register_default_plugins
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.imports import (
    ImportAnalyze,
    ImportCreate,
    ImportFilter,
)
from kitaru.server.application.models.replay_config import (
    AnalyzerConfigInput,
    EvaluatorConfigInput,
)
from kitaru.server.application.models.task import TaskFilter
from kitaru.server.application.services.plugin_resolution import PLUGIN_PROVIDER_LABEL
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.agent_version import AgentVersionAgentMismatch
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.connection import ConnectionNotFound
from kitaru.server.domain.imports import Import, ImportNotFound
from kitaru.server.domain.plugin import (
    PackagePluginSource,
    PluginKind,
    PluginNotFound,
    PluginVersion,
    ScriptPluginSource,
)
from kitaru.server.domain.task import AnalysisTask, ImportTask
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
async def services() -> JobAndTaskServices:
    """Provide fake-backed job, task, and import services."""
    services = build_job_and_task_services()
    plugin = await create_plugin(
        services.plugins, None, PluginKind.ANALYZER, name="kitaru/post-import-insights"
    )
    await services.plugins.create_version(
        plugin.id,
        PackagePluginSource(
            requirement="kitaru-post-import-insights==0.1.0",
            entrypoint="kitaru_post_import_insights.analyzer:analyze_post_import_sessions",
        ),
        display_version=None,
    )
    return services


async def _importer_version(
    services: JobAndTaskServices,
    provider: str | None = None,
    connection_schema: dict[str, Any] | None = None,
) -> PluginVersion:
    """Register the csv importer with one version."""
    plugin = await create_plugin(
        services.plugins,
        ACTOR.account.id,
        PluginKind.IMPORTER,
        name="csv",
        provider=provider,
        connection_schema=connection_schema,
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


async def _analyzer_version(
    services: JobAndTaskServices, name: str, provider: str | None = None
) -> PluginVersion:
    """Register an analyzer with one version."""
    plugin = await create_plugin(
        services.plugins,
        ACTOR.account.id,
        PluginKind.ANALYZER,
        name=name,
        provider=provider,
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
    connection_id: uuid.UUID | None = None,
    max_sessions: int | None = None,
    fetch: bool = False,
) -> ImportCreate:
    """Build a create command naming a stored payload or a query, and an agent."""
    if fetch:
        payload_blob_id, fetch_query = None, {"since": "2026-08-01T00:00:00Z"}
    else:
        payload = await create_blob(
            services.blobs, ACTOR.account.id, content=b"csv-data"
        )
        payload_blob_id, fetch_query = payload.id, None
    if agent is None:
        agent = await create_agent(services.agents, ACTOR.account.id)
    return ImportCreate(
        importer="csv",
        agent_id=agent.id,
        agent_version_id=agent_version_id,
        connection_id=connection_id,
        payload_blob_id=payload_blob_id,
        fetch_query=fetch_query,
        params={"delimiter": ","},
        evaluators=evaluators if evaluators is not None else [],
        analyzers=analyzers if analyzers is not None else [],
        max_sessions=max_sessions,
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
    assert import_.analyzers == []
    assert command.analyzers == []
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


async def test_create_import_stores_max_sessions(
    services: JobAndTaskServices,
) -> None:
    """The import row carries the session cap from the command."""
    await _importer_version(services)
    command = await _import_command(services, max_sessions=5)

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert import_.max_sessions == 5
    stored = await services.imports.get(import_.id)
    assert stored.max_sessions == 5


async def test_create_import_without_max_sessions_stores_none(
    services: JobAndTaskServices,
) -> None:
    """An import command without a session cap stores none."""
    await _importer_version(services)
    command = await _import_command(services)

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert import_.max_sessions is None


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


async def test_create_import_from_an_api_stores_the_fetch_query(
    services: JobAndTaskServices,
) -> None:
    """An API import stores its query and names no payload blob."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.IMPORTER, name="csv"
    )
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    agent = await create_agent(services.agents, ACTOR.account.id)
    command = ImportCreate(
        importer="csv",
        agent_id=agent.id,
        fetch_query={"since": "2026-08-01T00:00:00Z"},
    )

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert import_.payload_blob_id is None
    assert import_.fetch_query == {"since": "2026-08-01T00:00:00Z"}


async def test_create_import_stores_the_resolved_analyzers(
    services: JobAndTaskServices,
) -> None:
    """The import row carries the analyzers resolved to concrete versions."""
    await _importer_version(services)
    analyzer_version = await _analyzer_version(services, "trends")
    command = await _import_command(
        services,
        analyzers=[
            AnalyzerConfigInput(analyzer="trends", params={"k": 1}, min_sessions=8)
        ],
    )

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert len(import_.analyzers) == 1
    analyzer = import_.analyzers[0]
    assert analyzer.analyzer == "trends"
    assert analyzer.version == 1
    assert analyzer.params == {"k": 1}
    assert analyzer.min_sessions == 8
    assert analyzer.analyzer_version_id == analyzer_version.id
    stored = await services.imports.get(import_.id)
    assert stored.analyzers == import_.analyzers


async def test_explicit_builtin_analyzer_is_not_duplicated(
    services: JobAndTaskServices,
) -> None:
    """Keep an explicitly configured built-in analyzer and its parameters once."""
    await _importer_version(services)
    command = await _import_command(
        services,
        analyzers=[
            AnalyzerConfigInput(
                analyzer="kitaru/post-import-insights", params={"agent_name": "returns"}
            )
        ],
    )
    import_ = await services.import_service.create_import(command, actor=ACTOR)
    assert len(import_.analyzers) == 1
    assert import_.analyzers[0].params == {"agent_name": "returns"}


async def test_import_preserves_both_selected_insight_analyzers(
    services: JobAndTaskServices,
) -> None:
    """The caller can select independent deterministic and OpenAI analysis."""
    await register_default_plugins(services.plugins)
    await _importer_version(services)
    command = await _import_command(
        services,
        analyzers=[
            AnalyzerConfigInput(analyzer="kitaru/post-import-insights"),
            AnalyzerConfigInput(
                analyzer="kitaru/openai-post-import-insights",
                params={"model": "test-model"},
            ),
        ],
    )

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert [item.analyzer for item in import_.analyzers] == [
        "kitaru/post-import-insights",
        "kitaru/openai-post-import-insights",
    ]
    assert import_.analyzers[0].provider is None
    assert import_.analyzers[1].provider == "openai"
    assert import_.analyzers[1].params == {"model": "test-model"}


async def test_create_import_stores_the_analyzer_named_connection(
    services: JobAndTaskServices,
) -> None:
    """The resolved analyzer records its explicitly named connection."""
    await _importer_version(services)
    await _analyzer_version(services, "trends", provider="langfuse")
    secret = await create_secret(
        services.secrets, ACTOR.account.id, name="analyzer-values", internal=True
    )
    connection = await create_connection(
        services.connections, ACTOR.account.id, secret.id, name="analyzer"
    )
    command = await _import_command(
        services,
        analyzers=[AnalyzerConfigInput(analyzer="trends", connection_id=connection.id)],
    )

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert import_.analyzers[0].connection_id == connection.id


async def test_create_import_stores_the_analyzer_default_connection(
    services: JobAndTaskServices,
) -> None:
    """The resolved analyzer records its provider's default connection."""
    await _importer_version(services)
    await _analyzer_version(services, "trends", provider="langfuse")
    secret = await create_secret(
        services.secrets, ACTOR.account.id, name="analyzer-values", internal=True
    )
    connection = await create_connection(
        services.connections,
        ACTOR.account.id,
        secret.id,
        name="analyzer",
        default=True,
    )
    command = await _import_command(
        services, analyzers=[AnalyzerConfigInput(analyzer="trends")]
    )

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert import_.analyzers[0].connection_id == connection.id


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


async def test_create_import_stamps_the_provider_label(
    services: JobAndTaskServices,
) -> None:
    """The importer task carries the provider label for worker routing."""
    await _importer_version(services, provider="langfuse")
    command = await _import_command(services)

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert import_.job_id is not None
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=import_.job_id), actor=ACTOR
    )
    assert tasks[0].labels[PLUGIN_PROVIDER_LABEL] == "langfuse"


async def test_create_import_omits_the_provider_label_without_a_provider(
    services: JobAndTaskServices,
) -> None:
    """An importer with no provider stamps no provider label."""
    await _importer_version(services)
    command = await _import_command(services)

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert import_.job_id is not None
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=import_.job_id), actor=ACTOR
    )
    assert PLUGIN_PROVIDER_LABEL not in tasks[0].labels


async def test_create_import_stores_the_named_connection(
    services: JobAndTaskServices,
) -> None:
    """The import row carries the connection the command names."""
    await _importer_version(services, provider="langfuse")
    secret = await create_secret(
        services.secrets, ACTOR.account.id, name="values", internal=True
    )
    connection = await create_connection(
        services.connections, ACTOR.account.id, secret.id
    )
    command = await _import_command(services, connection_id=connection.id, fetch=True)

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert import_.connection_id == connection.id


async def test_create_import_uses_the_provider_default(
    services: JobAndTaskServices,
) -> None:
    """An import naming no connection records the provider's default."""
    await _importer_version(services, provider="langfuse")
    secret = await create_secret(
        services.secrets, ACTOR.account.id, name="values", internal=True
    )
    await create_connection(
        services.connections, ACTOR.account.id, secret.id, name="other"
    )
    default = await create_connection(
        services.connections, ACTOR.account.id, secret.id, name="main", default=True
    )
    command = await _import_command(services, fetch=True)

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert import_.connection_id == default.id


async def test_create_import_without_a_default_connection(
    services: JobAndTaskServices,
) -> None:
    """An import naming no connection records none when the provider has no default."""
    await _importer_version(services, provider="langfuse")
    command = await _import_command(services, fetch=True)

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert import_.connection_id is None


async def test_create_import_from_a_file_resolves_no_connection(
    services: JobAndTaskServices,
) -> None:
    """A file import records no connection even when the provider has a default."""
    await _importer_version(services, provider="langfuse")
    secret = await create_secret(
        services.secrets, ACTOR.account.id, name="values", internal=True
    )
    await create_connection(
        services.connections, ACTOR.account.id, secret.id, name="main", default=True
    )
    command = await _import_command(services)

    import_ = await services.import_service.create_import(command, actor=ACTOR)

    assert import_.connection_id is None


async def test_create_import_rejects_an_unknown_connection(
    services: JobAndTaskServices,
) -> None:
    """Reject a command naming a connection that does not exist."""
    await _importer_version(services)
    command = await _import_command(services, connection_id=uuid.uuid4(), fetch=True)

    with pytest.raises(ConnectionNotFound):
        await services.import_service.create_import(command, actor=ACTOR)


async def _import_task_labels(
    services: JobAndTaskServices, command: ImportCreate
) -> dict[str, str]:
    """Create the import and return its importer task's labels."""
    import_ = await services.import_service.create_import(command, actor=ACTOR)
    assert import_.job_id is not None
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=import_.job_id), actor=ACTOR
    )
    return tasks[0].labels


async def test_create_import_stamps_the_requires_credentials_label(
    services: JobAndTaskServices,
) -> None:
    """An API import with a connection schema and no connection needs the worker's."""
    await _importer_version(
        services, provider="langfuse", connection_schema={"type": "object"}
    )

    labels = await _import_task_labels(
        services, await _import_command(services, fetch=True)
    )

    assert labels[REQUIRES_CREDENTIALS_LABEL] == "langfuse"
    assert labels[PLUGIN_PROVIDER_LABEL] == "langfuse"


async def test_create_import_omits_the_requires_credentials_label_with_a_connection(
    services: JobAndTaskServices,
) -> None:
    """An API import resolving a connection carries its credentials itself."""
    await _importer_version(
        services, provider="langfuse", connection_schema={"type": "object"}
    )
    secret = await create_secret(
        services.secrets, ACTOR.account.id, name="values", internal=True
    )
    await create_connection(
        services.connections, ACTOR.account.id, secret.id, name="main", default=True
    )

    labels = await _import_task_labels(
        services, await _import_command(services, fetch=True)
    )

    assert REQUIRES_CREDENTIALS_LABEL not in labels
    assert labels[PLUGIN_PROVIDER_LABEL] == "langfuse"


async def test_create_import_omits_the_requires_credentials_label_without_a_schema(
    services: JobAndTaskServices,
) -> None:
    """An importer declaring no connection schema stamps no requires label."""
    await _importer_version(services, provider="langfuse")

    labels = await _import_task_labels(
        services, await _import_command(services, fetch=True)
    )

    assert REQUIRES_CREDENTIALS_LABEL not in labels


async def test_create_import_from_a_file_omits_the_requires_credentials_label(
    services: JobAndTaskServices,
) -> None:
    """A file import never talks to the provider."""
    await _importer_version(
        services, provider="langfuse", connection_schema={"type": "object"}
    )

    labels = await _import_task_labels(services, await _import_command(services))

    assert REQUIRES_CREDENTIALS_LABEL not in labels


async def _imported_session(
    services: JobAndTaskServices,
    import_: Import,
    status: SessionStatus = SessionStatus.COMPLETED,
) -> None:
    """Store one session the import created."""
    await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=import_.agent_id,
        origin=SessionOrigin.IMPORTED,
        status=status,
        import_id=import_.id,
    )


async def _analyzable_import(services: JobAndTaskServices) -> Import:
    """Create an import that has one completed session."""
    await _importer_version(services)
    command = await _import_command(services)
    import_ = await services.import_service.create_import(command, actor=ACTOR)
    await _imported_session(services, import_)
    return import_


async def _analysis_tasks(
    services: JobAndTaskServices, job_id: uuid.UUID
) -> list[AnalysisTask]:
    """Return the analysis tasks of a job."""
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job_id), actor=ACTOR
    )
    assert all(isinstance(task, AnalysisTask) for task in tasks)
    return [task for task in tasks if isinstance(task, AnalysisTask)]


async def test_analyze_import_creates_a_job_with_one_task_per_analyzer(
    services: JobAndTaskServices,
) -> None:
    """An analysis job holds one task per analyzer, each scoped to the import."""
    import_ = await _analyzable_import(services)
    trends = await _analyzer_version(services, "trends")
    outcomes = await _analyzer_version(services, "outcomes")

    job = await services.import_service.analyze_import(
        import_.id,
        ImportAnalyze(
            analyzers=[
                AnalyzerConfigInput(analyzer="trends", params={"k": 1}),
                AnalyzerConfigInput(analyzer="outcomes"),
            ]
        ),
        actor=ACTOR,
    )

    assert job.owner_id == ACTOR.account.id
    assert job.kind is JobKind.ANALYSIS
    assert job.status is JobStatus.PENDING
    assert job.id != import_.job_id
    tasks = await _analysis_tasks(services, job.id)
    assert {task.plugin_version_id for task in tasks} == {trends.id, outcomes.id}
    for task in tasks:
        assert task.import_id == import_.id
        assert task.agent_id == import_.agent_id
        assert task.on_failure is TaskOnFailure.CONTINUE
    by_version = {task.plugin_version_id: task for task in tasks}
    assert by_version[trends.id].params == {"k": 1}
    assert by_version[outcomes.id].params == {}


async def test_analyze_import_leaves_the_import_row_unchanged(
    services: JobAndTaskServices,
) -> None:
    """A rerun does not rewrite the analyzers stored on the import."""
    import_ = await _analyzable_import(services)
    await _analyzer_version(services, "trends")

    await services.import_service.analyze_import(
        import_.id,
        ImportAnalyze(analyzers=[AnalyzerConfigInput(analyzer="trends")]),
        actor=ACTOR,
    )

    stored = await services.imports.get(import_.id)
    assert stored.analyzers == []
    assert stored.job_id == import_.job_id


async def test_analyze_import_stamps_the_requires_credentials_label(
    services: JobAndTaskServices,
) -> None:
    """An analyzer with a connection schema and no connection needs the worker's."""
    import_ = await _analyzable_import(services)
    plugin = await create_plugin(
        services.plugins,
        ACTOR.account.id,
        PluginKind.ANALYZER,
        name="trends",
        provider="openai",
        connection_schema={"type": "object"},
    )
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="analyze"),
        display_version=None,
    )

    job = await services.import_service.analyze_import(
        import_.id,
        ImportAnalyze(analyzers=[AnalyzerConfigInput(analyzer="trends")]),
        actor=ACTOR,
    )

    [task] = await _analysis_tasks(services, job.id)
    assert task.connection_id is None
    assert task.labels[REQUIRES_CREDENTIALS_LABEL] == "openai"
    assert task.labels[PLUGIN_PROVIDER_LABEL] == "openai"


async def test_analyze_import_stores_the_named_connection(
    services: JobAndTaskServices,
) -> None:
    """The task carries the connection named on its analyzer config."""
    import_ = await _analyzable_import(services)
    await _analyzer_version(services, "trends", provider="openai")
    secret = await create_secret(
        services.secrets, ACTOR.account.id, name="analyzer-values", internal=True
    )
    connection = await create_connection(
        services.connections, ACTOR.account.id, secret.id, name="analyzer"
    )

    job = await services.import_service.analyze_import(
        import_.id,
        ImportAnalyze(
            analyzers=[
                AnalyzerConfigInput(analyzer="trends", connection_id=connection.id)
            ]
        ),
        actor=ACTOR,
    )

    [task] = await _analysis_tasks(services, job.id)
    assert task.connection_id == connection.id
    assert REQUIRES_CREDENTIALS_LABEL not in task.labels


async def test_analyze_import_rejects_an_unknown_import(
    services: JobAndTaskServices,
) -> None:
    """An unknown import id resolves to not found."""
    await _analyzer_version(services, "trends")

    with pytest.raises(ImportNotFound):
        await services.import_service.analyze_import(
            uuid.uuid4(),
            ImportAnalyze(analyzers=[AnalyzerConfigInput(analyzer="trends")]),
            actor=ACTOR,
        )


@pytest.mark.parametrize("in_progress", [False, True])
async def test_analyze_import_skips_without_eligible_sessions(
    services: JobAndTaskServices, in_progress: bool
) -> None:
    """A rerun with no eligible sessions completes with an explicit skip."""
    await _importer_version(services)
    command = await _import_command(services)
    import_ = await services.import_service.create_import(command, actor=ACTOR)
    if in_progress:
        await _imported_session(services, import_, status=SessionStatus.IN_PROGRESS)
    await _analyzer_version(services, "trends")
    job = await services.import_service.analyze_import(
        import_.id,
        ImportAnalyze(analyzers=[AnalyzerConfigInput(analyzer="trends")]),
        actor=ACTOR,
    )
    assert job.status is JobStatus.COMPLETED
    assert (await services.jobs.get(job.id)).status is JobStatus.COMPLETED
    (task,) = await _analysis_tasks(services, job.id)
    assert task.status is TaskStatus.SKIPPED
    assert task.result is None


async def test_analyze_import_mixes_skipped_and_runnable_analyzers(
    services: JobAndTaskServices,
) -> None:
    """Skipped built-ins do not complete a job containing a runnable custom analyzer."""
    import_ = await _analyzable_import(services)
    trends = await _analyzer_version(services, "trends")
    job = await services.import_service.analyze_import(
        import_.id,
        ImportAnalyze(
            analyzers=[
                AnalyzerConfigInput(analyzer="kitaru/post-import-insights"),
                AnalyzerConfigInput(analyzer="trends"),
            ]
        ),
        actor=ACTOR,
    )
    assert job.status is JobStatus.PENDING
    tasks = await _analysis_tasks(services, job.id)
    assert len(tasks) == 2
    assert {task.status for task in tasks} == {TaskStatus.SKIPPED, TaskStatus.PENDING}
    runnable = next(task for task in tasks if task.plugin_version_id == trends.id)
    assert runnable.status is TaskStatus.PENDING
    assert runnable.result is None


async def test_analyze_import_rejects_an_unknown_analyzer(
    services: JobAndTaskServices,
) -> None:
    """An unknown analyzer name resolves to not found."""
    import_ = await _analyzable_import(services)

    with pytest.raises(PluginNotFound):
        await services.import_service.analyze_import(
            import_.id,
            ImportAnalyze(analyzers=[AnalyzerConfigInput(analyzer="missing")]),
            actor=ACTOR,
        )


async def test_analyze_import_rejects_duplicate_analyzer_versions(
    services: JobAndTaskServices,
) -> None:
    """Two configs resolving to one analyzer version are rejected."""
    import_ = await _analyzable_import(services)
    await _analyzer_version(services, "trends")

    with pytest.raises(ValidationError):
        await services.import_service.analyze_import(
            import_.id,
            ImportAnalyze(
                analyzers=[
                    AnalyzerConfigInput(analyzer="trends"),
                    AnalyzerConfigInput(analyzer="trends", version=1),
                ]
            ),
            actor=ACTOR,
        )
