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
"""Tests for the task execution spec builder."""

import uuid

import pytest
from pydantic import SecretStr

from conftest import (
    JobAndTaskServices,
    build_job_and_task_services,
    build_worker_actor,
    create_agent,
    create_analysis_task,
    create_blob,
    create_connection,
    create_import,
    create_import_task,
    create_job,
    create_plugin,
    create_secret,
    create_worker,
)
from kitaru.api_models.v1.imports import ImportQuery
from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account
from kitaru.server.domain.imports import Import
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.domain.task import (
    AnalysisTask,
    AnalysisTaskDetails,
    ApiImportSourceSpec,
    BlobImportSourceSpec,
    ImportTask,
    ImportTaskDetails,
    ScriptPluginSpec,
)

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job and task services."""
    return build_job_and_task_services()


async def test_import_spec_is_built_from_the_import_row(
    services: JobAndTaskServices,
) -> None:
    """The importer spec takes its plugin, payload, agent, and params off the import."""
    plugin = await create_plugin(
        services.plugins,
        ACTOR.account.id,
        PluginKind.IMPORTER,
        name="csv-importer",
        provider="acme",
    )
    code_blob = await create_blob(services.blobs, ACTOR.account.id, content=b"code")
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=code_blob.id, entrypoint="run"),
        display_version=None,
    )
    payload = await create_blob(
        services.blobs, ACTOR.account.id, content=b"payload-data"
    )
    agent = await create_agent(services.agents, ACTOR.account.id)
    job = await create_job(services.jobs, ACTOR.account.id)
    import_ = await create_import(
        services.imports,
        ACTOR.account.id,
        agent.id,
        job_id=job.id,
        importer_version_id=version.id,
        payload_blob_id=payload.id,
        params={"delimiter": ","},
    )
    task = await create_import_task(services.tasks, job.id, import_id=import_.id)

    spec = await services.task_service.get_spec(task.id, actor=ACTOR)

    assert spec.timeout_seconds == (
        services.task_service._policy.importer_timeout_seconds
    )
    assert spec.hooks == []
    assert isinstance(spec.details, ImportTaskDetails)
    assert isinstance(spec.details.plugin, ScriptPluginSpec)
    assert spec.details.plugin.blob_id == code_blob.id
    assert spec.details.plugin.sha256 == code_blob.sha256
    assert spec.details.provider == "acme"
    assert isinstance(spec.details.source, BlobImportSourceSpec)
    assert spec.details.source.blob_id == payload.id
    assert spec.details.source.sha256 == payload.sha256
    assert spec.details.agent_id == agent.id
    assert spec.details.params == {"delimiter": ","}


async def test_import_spec_carries_the_api_source(
    services: JobAndTaskServices,
) -> None:
    """An API import's spec names the fetch entrypoint and query, no payload."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.IMPORTER, name="api-importer"
    )
    code_blob = await create_blob(services.blobs, ACTOR.account.id, content=b"code")
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=code_blob.id, entrypoint="run"),
        display_version=None,
    )
    agent = await create_agent(services.agents, ACTOR.account.id)
    job = await create_job(services.jobs, ACTOR.account.id)
    import_ = await services.imports.create(
        Import(
            owner_id=ACTOR.account.id,
            job_id=job.id,
            agent_id=agent.id,
            importer_version_id=version.id,
            fetch_query={"since": "2026-08-01T00:00:00Z"},
        )
    )
    task = await create_import_task(services.tasks, job.id, import_id=import_.id)

    spec = await services.task_service.get_spec(task.id, actor=ACTOR)

    assert isinstance(spec.details, ImportTaskDetails)
    assert isinstance(spec.details.source, ApiImportSourceSpec)
    assert spec.details.source.query == ImportQuery.model_validate(
        {"since": "2026-08-01T00:00:00Z"}
    )


async def test_analysis_spec_is_built_from_the_task(
    services: JobAndTaskServices,
) -> None:
    """The analyzer spec takes its plugin, agent, sessions, and params off the task."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.ANALYZER, name="trends"
    )
    code_blob = await create_blob(services.blobs, ACTOR.account.id, content=b"code")
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=code_blob.id, entrypoint="analyze"),
        display_version=None,
    )
    agent = await create_agent(services.agents, ACTOR.account.id)
    job = await create_job(services.jobs, ACTOR.account.id)
    import_id = uuid.uuid4()
    task = await create_analysis_task(
        services.tasks,
        job.id,
        plugin_version_id=version.id,
        agent_id=agent.id,
        import_id=import_id,
        params={"focus": "errors"},
    )

    spec = await services.task_service.get_spec(task.id, actor=ACTOR)

    assert spec.timeout_seconds == (
        services.task_service._policy.analyzer_timeout_seconds
    )
    assert spec.hooks == []
    assert isinstance(spec.details, AnalysisTaskDetails)
    assert isinstance(spec.details.plugin, ScriptPluginSpec)
    assert spec.details.plugin.blob_id == code_blob.id
    assert spec.details.plugin.sha256 == code_blob.sha256
    assert spec.details.analyzer_name == "trends"
    assert spec.details.agent_id == agent.id
    assert spec.details.import_id == import_id
    assert spec.details.params == {"focus": "errors"}


async def test_analysis_spec_uses_the_recorded_connection(
    services: JobAndTaskServices,
) -> None:
    """An analysis task injects its recorded connection's env and secrets."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.ANALYZER, name="trends"
    )
    code_blob = await create_blob(services.blobs, ACTOR.account.id, content=b"code")
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=code_blob.id, entrypoint="analyze"),
        display_version=None,
    )
    connection_id = await store_connection(
        services,
        env={"LANGFUSE_BASE_URL": "https://cloud", "REGION": "eu"},
    )
    agent = await create_agent(services.agents, ACTOR.account.id)
    job = await create_job(services.jobs, ACTOR.account.id)
    task = await create_analysis_task(
        services.tasks,
        job.id,
        plugin_version_id=version.id,
        agent_id=agent.id,
        connection_id=connection_id,
    )

    spec = await services.task_service.get_spec(task.id, actor=ACTOR)

    assert spec.env == {
        "LANGFUSE_BASE_URL": "https://cloud",
        "REGION": "eu",
    }
    assert spec.secret_env == {"LANGFUSE_SECRET_KEY": "sk"}


async def test_analysis_spec_task_env_wins_over_the_connection_env(
    services: JobAndTaskServices,
) -> None:
    """The analysis task's env overrides connection values of the same key."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.ANALYZER, name="trends"
    )
    code_blob = await create_blob(services.blobs, ACTOR.account.id, content=b"code")
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=code_blob.id, entrypoint="analyze"),
        display_version=None,
    )
    connection_id = await store_connection(
        services,
        env={"LANGFUSE_BASE_URL": "https://cloud", "REGION": "eu"},
    )
    agent = await create_agent(services.agents, ACTOR.account.id)
    job = await create_job(services.jobs, ACTOR.account.id)
    task = await services.tasks.create(
        AnalysisTask(
            job_id=job.id,
            plugin_version_id=version.id,
            agent_id=agent.id,
            import_id=uuid.uuid4(),
            connection_id=connection_id,
            env={"LANGFUSE_BASE_URL": "https://self-hosted"},
        )
    )

    spec = await services.task_service.get_spec(task.id, actor=ACTOR)

    assert spec.env == {
        "LANGFUSE_BASE_URL": "https://self-hosted",
        "REGION": "eu",
    }


async def test_analysis_spec_ignores_a_deleted_connection(
    services: JobAndTaskServices,
) -> None:
    """A deleted analysis connection contributes no environment values."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.ANALYZER, name="trends"
    )
    code_blob = await create_blob(services.blobs, ACTOR.account.id, content=b"code")
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=code_blob.id, entrypoint="analyze"),
        display_version=None,
    )
    connection_id = await store_connection(services)
    await services.connections.delete(connection_id)
    agent = await create_agent(services.agents, ACTOR.account.id)
    job = await create_job(services.jobs, ACTOR.account.id)
    task = await services.tasks.create(
        AnalysisTask(
            job_id=job.id,
            plugin_version_id=version.id,
            agent_id=agent.id,
            import_id=uuid.uuid4(),
            connection_id=connection_id,
            env={"REGION": "eu"},
        )
    )

    spec = await services.task_service.get_spec(task.id, actor=ACTOR)

    assert spec.env == {"REGION": "eu"}
    assert spec.secret_env == {}


async def test_missing_import_row_cancels_the_task_at_claim(
    services: JobAndTaskServices,
) -> None:
    """A task whose import row is gone is canceled instead of handed out."""
    job = await create_job(services.jobs, ACTOR.account.id)
    task = await create_import_task(services.tasks, job.id, import_id=uuid.uuid4())
    worker = await create_worker(services.workers, ACTOR.account.id)

    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )

    assert claimed == []
    stored = await services.tasks.get(task.id)
    assert stored.status is TaskStatus.CANCELED


async def test_import_without_importer_version_cancels_the_task_at_claim(
    services: JobAndTaskServices,
) -> None:
    """A task whose import lost its importer version is canceled at claim."""
    agent = await create_agent(services.agents, ACTOR.account.id)
    job = await create_job(services.jobs, ACTOR.account.id)
    import_ = await services.imports.create(
        Import(
            owner_id=ACTOR.account.id,
            job_id=job.id,
            agent_id=agent.id,
            importer_version_id=None,
            payload_blob_id=uuid.uuid4(),
        )
    )
    task = await create_import_task(services.tasks, job.id, import_id=import_.id)
    worker = await create_worker(services.workers, ACTOR.account.id)

    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )

    assert claimed == []
    stored = await services.tasks.get(task.id)
    assert stored.status is TaskStatus.CANCELED


async def build_import_plugin_version(
    services: JobAndTaskServices, provider: str | None = "langfuse"
) -> uuid.UUID:
    """Store an importer plugin with one script version.

    Args:
        services: Fake-backed job and task services.
        provider: Provider the importer reads.

    Returns:
        Id of the stored importer version.
    """
    plugin = await create_plugin(
        services.plugins,
        ACTOR.account.id,
        PluginKind.IMPORTER,
        name="trace-importer",
        provider=provider,
    )
    code_blob = await create_blob(services.blobs, ACTOR.account.id, content=b"code")
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=code_blob.id, entrypoint="run"),
        display_version=None,
    )
    return version.id


async def store_connection(
    services: JobAndTaskServices,
    name: str = "langfuse-prod",
    env: dict[str, str] | None = None,
    default: bool = False,
) -> uuid.UUID:
    """Store a connection with an internal secret holding one value.

    Args:
        services: Fake-backed job and task services.
        name: Connection name.
        env: Non-secret values.
        default: Whether the connection is the provider's default.

    Returns:
        Id of the stored connection.
    """
    secret = await create_secret(
        services.secrets,
        ACTOR.account.id,
        name=f"{name}-values",
        internal=True,
        values={"LANGFUSE_SECRET_KEY": SecretStr("sk")},
    )
    connection = await create_connection(
        services.connections,
        ACTOR.account.id,
        secret.id,
        name=name,
        env=env if env is not None else {"LANGFUSE_BASE_URL": "https://cloud"},
        default=default,
    )
    return connection.id


async def test_import_spec_uses_the_named_connection(
    services: JobAndTaskServices,
) -> None:
    """An import naming a connection carries that connection's env and secrets."""
    version_id = await build_import_plugin_version(services)
    connection_id = await store_connection(services, name="named")
    await store_connection(services, name="fallback", default=True)
    agent = await create_agent(services.agents, ACTOR.account.id)
    job = await create_job(services.jobs, ACTOR.account.id)
    payload = await create_blob(services.blobs, ACTOR.account.id, content=b"payload")
    import_ = await create_import(
        services.imports,
        ACTOR.account.id,
        agent.id,
        job_id=job.id,
        importer_version_id=version_id,
        payload_blob_id=payload.id,
        connection_id=connection_id,
    )
    task = await create_import_task(services.tasks, job.id, import_id=import_.id)

    spec = await services.task_service.get_spec(task.id, actor=ACTOR)

    assert spec.env == {"LANGFUSE_BASE_URL": "https://cloud"}
    assert spec.secret_env == {"LANGFUSE_SECRET_KEY": "sk"}


async def test_import_spec_ignores_an_unrecorded_default(
    services: JobAndTaskServices,
) -> None:
    """An import that recorded no connection injects nothing, whatever the default."""
    version_id = await build_import_plugin_version(services)
    await store_connection(services, name="fallback", default=True)
    agent = await create_agent(services.agents, ACTOR.account.id)
    job = await create_job(services.jobs, ACTOR.account.id)
    payload = await create_blob(services.blobs, ACTOR.account.id, content=b"payload")
    import_ = await create_import(
        services.imports,
        ACTOR.account.id,
        agent.id,
        job_id=job.id,
        importer_version_id=version_id,
        payload_blob_id=payload.id,
    )
    task = await create_import_task(services.tasks, job.id, import_id=import_.id)

    spec = await services.task_service.get_spec(task.id, actor=ACTOR)

    assert spec.env == {}
    assert spec.secret_env == {}
    stored = await services.imports.get(import_.id)
    assert stored.connection_id is None


async def test_import_spec_without_a_connection(
    services: JobAndTaskServices,
) -> None:
    """An importer with no matching connection injects nothing extra."""
    version_id = await build_import_plugin_version(services, provider=None)
    agent = await create_agent(services.agents, ACTOR.account.id)
    job = await create_job(services.jobs, ACTOR.account.id)
    payload = await create_blob(services.blobs, ACTOR.account.id, content=b"payload")
    import_ = await create_import(
        services.imports,
        ACTOR.account.id,
        agent.id,
        job_id=job.id,
        importer_version_id=version_id,
        payload_blob_id=payload.id,
    )
    task = await create_import_task(services.tasks, job.id, import_id=import_.id)

    spec = await services.task_service.get_spec(task.id, actor=ACTOR)

    assert spec.env == {}
    assert spec.secret_env == {}
    stored = await services.imports.get(import_.id)
    assert stored.connection_id is None


async def test_import_spec_task_env_wins_over_the_connection_env(
    services: JobAndTaskServices,
) -> None:
    """The task's own env overrides the connection env of the same key."""
    version_id = await build_import_plugin_version(services)
    connection_id = await store_connection(
        services,
        name="fallback",
        env={"LANGFUSE_BASE_URL": "https://cloud", "REGION": "eu"},
        default=True,
    )
    agent = await create_agent(services.agents, ACTOR.account.id)
    job = await create_job(services.jobs, ACTOR.account.id)
    payload = await create_blob(services.blobs, ACTOR.account.id, content=b"payload")
    import_ = await create_import(
        services.imports,
        ACTOR.account.id,
        agent.id,
        job_id=job.id,
        importer_version_id=version_id,
        payload_blob_id=payload.id,
        connection_id=connection_id,
    )
    stored_task = await services.tasks.create(
        ImportTask(
            job_id=job.id,
            import_id=import_.id,
            env={"LANGFUSE_BASE_URL": "https://self-hosted"},
        )
    )

    spec = await services.task_service.get_spec(stored_task.id, actor=ACTOR)

    assert spec.env == {
        "LANGFUSE_BASE_URL": "https://self-hosted",
        "REGION": "eu",
    }


async def test_missing_connection_cancels_the_task_at_claim(
    services: JobAndTaskServices,
) -> None:
    """A task whose named connection is gone is canceled instead of handed out."""
    version_id = await build_import_plugin_version(services)
    agent = await create_agent(services.agents, ACTOR.account.id)
    job = await create_job(services.jobs, ACTOR.account.id)
    payload = await create_blob(services.blobs, ACTOR.account.id, content=b"payload")
    import_ = await create_import(
        services.imports,
        ACTOR.account.id,
        agent.id,
        job_id=job.id,
        importer_version_id=version_id,
        payload_blob_id=payload.id,
        connection_id=uuid.uuid4(),
    )
    task = await create_import_task(services.tasks, job.id, import_id=import_.id)
    worker = await create_worker(services.workers, ACTOR.account.id)

    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )

    assert claimed == []
    stored = await services.tasks.get(task.id)
    assert stored.status is TaskStatus.CANCELED
