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

from conftest import (
    JobAndTaskServices,
    build_job_and_task_services,
    build_worker_actor,
    create_agent,
    create_blob,
    create_import,
    create_import_task,
    create_job,
    create_plugin,
    create_worker,
)
from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.domain.account import Account
from kitaru.server.domain.imports import Import
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.domain.task import (
    ApiSourceSpec,
    BlobSourceSpec,
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
    assert isinstance(spec.details.source, BlobSourceSpec)
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
        ScriptPluginSource(
            blob_id=code_blob.id, entrypoint="run", fetch_entrypoint="fetch"
        ),
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
    assert isinstance(spec.details.source, ApiSourceSpec)
    assert spec.details.source.entrypoint == "fetch"
    assert spec.details.source.query == {"since": "2026-08-01T00:00:00Z"}


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
