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
"""End-to-end tests for the import pipeline and its evaluator fan-out."""

import uuid
from typing import Any

import pytest

from conftest import (
    ReplayServices,
    build_replay_services,
    build_task_actor,
    build_worker_actor,
    create_agent,
    create_blob,
    create_import,
    create_import_task,
    create_job,
    create_plugin,
    create_session,
    create_worker,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.task import TaskOnFailure, TaskStatus
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.application.models.task import TaskFilter, TaskUpdate
from kitaru.server.domain.account import Account
from kitaru.server.domain.imports import Import
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.domain.replay_config import EvaluatorConfig
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import EvaluationTask, ImportTask, Task
from kitaru.server.domain.worker import Worker
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))
STATS = {"created": 3, "skipped": 0, "failed": 0}


@pytest.fixture
def services() -> ReplayServices:
    """Provide fake-backed services sharing the production subscribers."""
    return build_replay_services()


async def _evaluator(services: ReplayServices, name: str) -> EvaluatorConfig:
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, kind=PluginKind.EVALUATOR, name=name
    )
    blob = await create_blob(services.blobs, ACTOR.account.id, content=name.encode())
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=blob.id, entrypoint="score"),
        display_version=None,
    )
    return EvaluatorConfig(
        evaluator=plugin.name,
        version=version.version,
        params={"threshold": 0.5},
        evaluator_version_id=version.id,
    )


async def _import_with_task(
    services: ReplayServices, evaluators: list[EvaluatorConfig]
) -> tuple[Import, ImportTask]:
    plugin = await create_plugin(
        services.plugins,
        ACTOR.account.id,
        PluginKind.IMPORTER,
        name=f"imp{uuid.uuid4().hex[:8]}",
    )
    code_blob = await create_blob(
        services.blobs, ACTOR.account.id, content=uuid.uuid4().bytes
    )
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=code_blob.id, entrypoint="parse"),
        display_version=None,
    )
    payload = await create_blob(
        services.blobs, ACTOR.account.id, content=uuid.uuid4().bytes
    )
    agent = await create_agent(
        services.agents, ACTOR.account.id, name=f"a{uuid.uuid4().hex[:8]}"
    )
    job = await create_job(services.jobs, ACTOR.account.id, kind=JobKind.IMPORT)
    import_ = await create_import(
        services.imports,
        ACTOR.account.id,
        agent.id,
        job_id=job.id,
        importer_version_id=version.id,
        payload_blob_id=payload.id,
        evaluators=evaluators,
    )
    task = await create_import_task(services.tasks, job.id, import_id=import_.id)
    return import_, task


async def _imported_session(
    services: ReplayServices,
    import_: Import,
    status: SessionStatus = SessionStatus.COMPLETED,
) -> Session:
    return await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=import_.agent_id,
        origin=SessionOrigin.IMPORTED,
        status=status,
        import_id=import_.id,
    )


async def _claim_and_start(
    services: ReplayServices, worker: Worker, expected: int
) -> list[Task]:
    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert len(claimed) == expected
    tasks = [item.task for item in claimed]
    for task in tasks:
        await services.task_service.update_task(
            task.id,
            TaskUpdate(status=TaskStatus.RUNNING),
            actor=build_task_actor(ACTOR.account, task.id, task.attempt, worker.id),
        )
    return tasks


async def _finish(
    services: ReplayServices, worker: Worker, task: Task, command: TaskUpdate
) -> None:
    await services.task_service.update_task(
        task.id,
        command,
        actor=build_task_actor(ACTOR.account, task.id, task.attempt, worker.id),
    )


async def _evaluator_tasks(
    services: ReplayServices, job_id: uuid.UUID
) -> list[EvaluationTask]:
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job_id), actor=ACTOR
    )
    return [task for task in tasks if isinstance(task, EvaluationTask)]


def _result(name: str) -> list[dict[str, Any]]:
    return [{"name": name, "score": 1.0}]


async def test_completed_import_appends_one_task_per_session_and_evaluator(
    services: ReplayServices,
) -> None:
    """Three sessions and two evaluators fan out into six continue tasks."""
    evaluators = [
        await _evaluator(services, "accuracy"),
        await _evaluator(services, "tone"),
    ]
    import_, import_task = await _import_with_task(services, evaluators)
    sessions = [await _imported_session(services, import_) for _ in range(3)]
    worker = await create_worker(services.workers, ACTOR.account.id)

    (running,) = await _claim_and_start(services, worker, 1)
    await _finish(
        services,
        worker,
        running,
        TaskUpdate(status=TaskStatus.COMPLETED, result=STATS),
    )

    stored = await services.imports.get(import_.id)
    assert stored.stats is not None
    assert stored.stats.created == 3
    assert stored.error is None
    evaluator_tasks = await _evaluator_tasks(services, import_task.job_id)
    assert len(evaluator_tasks) == 6
    assert all(task.on_failure is TaskOnFailure.CONTINUE for task in evaluator_tasks)
    assert all(task.params == {"threshold": 0.5} for task in evaluator_tasks)
    assert {
        (task.input_session_id, task.plugin_version_id) for task in evaluator_tasks
    } == {
        (session.id, evaluator.evaluator_version_id)
        for session in sessions
        for evaluator in evaluators
    }


async def test_in_progress_session_is_skipped(services: ReplayServices) -> None:
    """A session still in progress receives no evaluator task."""
    evaluator = await _evaluator(services, "accuracy")
    import_, import_task = await _import_with_task(services, [evaluator])
    completed = await _imported_session(services, import_)
    await _imported_session(services, import_, status=SessionStatus.IN_PROGRESS)
    worker = await create_worker(services.workers, ACTOR.account.id)

    (running,) = await _claim_and_start(services, worker, 1)
    await _finish(
        services,
        worker,
        running,
        TaskUpdate(status=TaskStatus.COMPLETED, result=STATS),
    )

    evaluator_tasks = await _evaluator_tasks(services, import_task.job_id)
    assert [task.input_session_id for task in evaluator_tasks] == [completed.id]


async def test_import_without_evaluators_stamps_stats_and_appends_nothing(
    services: ReplayServices,
) -> None:
    """An import naming no evaluators records its stats and settles the job."""
    import_, import_task = await _import_with_task(services, [])
    await _imported_session(services, import_)
    worker = await create_worker(services.workers, ACTOR.account.id)

    (running,) = await _claim_and_start(services, worker, 1)
    await _finish(
        services,
        worker,
        running,
        TaskUpdate(status=TaskStatus.COMPLETED, result=STATS),
    )

    stored = await services.imports.get(import_.id)
    assert stored.stats is not None
    assert stored.stats.created == 3
    assert await _evaluator_tasks(services, import_task.job_id) == []
    job = await services.jobs.get(import_task.job_id)
    assert job.status is JobStatus.COMPLETED


async def test_failed_import_stamps_error_and_appends_nothing(
    services: ReplayServices,
) -> None:
    """A failed import records the task error and fans out no evaluators."""
    evaluator = await _evaluator(services, "accuracy")
    import_, import_task = await _import_with_task(services, [evaluator])
    await _imported_session(services, import_)
    worker = await create_worker(services.workers, ACTOR.account.id)

    (running,) = await _claim_and_start(services, worker, 1)
    await _finish(
        services,
        worker,
        running,
        TaskUpdate(status=TaskStatus.FAILED, error="parse failed"),
    )

    stored = await services.imports.get(import_.id)
    assert stored.stats is None
    assert stored.error == "parse failed"
    assert await _evaluator_tasks(services, import_task.job_id) == []
    job = await services.jobs.get(import_task.job_id)
    assert job.status is JobStatus.FAILED


async def test_task_without_import_row_is_ignored(services: ReplayServices) -> None:
    """A terminal import task whose import row is gone changes nothing."""
    job = await create_job(services.jobs, ACTOR.account.id, kind=JobKind.IMPORT)
    import_task = await create_import_task(services.tasks, job.id)
    worker = await create_worker(services.workers, ACTOR.account.id)

    # The claim cancels a task whose import row does not resolve, which is
    # the terminal transition the handler observes.
    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert claimed == []

    stored = await services.tasks.get(import_task.id)
    assert stored.status is TaskStatus.CANCELED
    assert await _evaluator_tasks(services, job.id) == []


async def test_job_settles_only_after_evaluator_tasks_drain(
    services: ReplayServices,
) -> None:
    """The import's job stays running until every appended evaluator task ends."""
    evaluator = await _evaluator(services, "accuracy")
    import_, import_task = await _import_with_task(services, [evaluator])
    await _imported_session(services, import_)
    await _imported_session(services, import_)
    worker = await create_worker(services.workers, ACTOR.account.id)

    (running,) = await _claim_and_start(services, worker, 1)
    await _finish(
        services,
        worker,
        running,
        TaskUpdate(status=TaskStatus.COMPLETED, result=STATS),
    )
    job = await services.jobs.get(import_task.job_id)
    assert job.status is JobStatus.RUNNING

    first, second = await _claim_and_start(services, worker, 2)
    await _finish(
        services,
        worker,
        first,
        TaskUpdate(status=TaskStatus.COMPLETED, result=_result("accuracy")),
    )
    job = await services.jobs.get(import_task.job_id)
    assert job.status is JobStatus.RUNNING

    await _finish(
        services,
        worker,
        second,
        TaskUpdate(status=TaskStatus.COMPLETED, result=_result("accuracy")),
    )
    job = await services.jobs.get(import_task.job_id)
    assert job.status is JobStatus.COMPLETED


async def test_evaluation_rows_land_on_the_imported_sessions(
    services: ReplayServices,
) -> None:
    """Completing the evaluator tasks writes evaluation rows on each session."""
    evaluator = await _evaluator(services, "accuracy")
    import_, _ = await _import_with_task(services, [evaluator])
    sessions = [await _imported_session(services, import_) for _ in range(2)]
    worker = await create_worker(services.workers, ACTOR.account.id)

    (running,) = await _claim_and_start(services, worker, 1)
    await _finish(
        services,
        worker,
        running,
        TaskUpdate(status=TaskStatus.COMPLETED, result=STATS),
    )
    for task in await _claim_and_start(services, worker, 2):
        await _finish(
            services,
            worker,
            task,
            TaskUpdate(status=TaskStatus.COMPLETED, result=_result("accuracy")),
        )

    for session in sessions:
        evaluations, _ = await services.evaluations.query(
            EvaluationFilter(
                expression=FilterCondition(
                    field="session_id", op=FilterOp.EQ, value=session.id
                )
            )
        )
        assert len(evaluations) == 1
        assert evaluations[0].evaluation.name == "accuracy"
        assert evaluations[0].evaluation.score == 1.0
        assert evaluations[0].evaluation.evaluator_version_id == (
            evaluator.evaluator_version_id
        )
