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
"""Tests for job use cases."""

import uuid

import pytest

from conftest import (
    JobAndTaskServices,
    build_job_and_task_services,
    build_task_actor,
    build_worker_actor,
    create_agent,
    create_agent_task,
    create_agent_version,
    create_blob,
    create_evaluation_task,
    create_job,
    create_plugin,
    create_session,
    create_worker,
)
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.task import TaskKind, TaskOnFailure, TaskStatus
from kitaru.server.application.events import Event, TaskTerminal
from kitaru.server.application.models.auth import (
    AuthContext,
)
from kitaru.server.application.models.imports import ImportCreate
from kitaru.server.application.models.job import (
    EvaluationBatchCreate,
    JobFilter,
    SessionRunCreate,
)
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.application.models.task import TaskFilter, TaskPolicy, TaskUpdate
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import (
    AgentVersionAgentMismatch,
    AgentVersionWithoutRunSpec,
    RunSpec,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.job import JobAlreadySettled, JobNotFound, JobNotSettled
from kitaru.server.domain.plugin import PluginKind, ScriptPluginSource
from kitaru.server.domain.session import SessionNotEvaluatable
from kitaru.server.domain.task import (
    AgentTask,
    DuplicateEvaluationTask,
    EvaluationTask,
    ImportTask,
)
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def services() -> JobAndTaskServices:
    """Provide fake-backed job and task services."""
    return build_job_and_task_services()


async def _runnable_agent_version(services: JobAndTaskServices) -> uuid.UUID:
    agent = await create_agent(services.agents, ACTOR.account.id)
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACTOR.account.id,
        run_spec=RunSpec(command="run.sh", timeout_seconds=60),
    )
    return version.id


async def _evaluator_version(
    services: JobAndTaskServices, name: str, agent_id: uuid.UUID | None = None
) -> uuid.UUID:
    plugin = await create_plugin(
        services.plugins,
        ACTOR.account.id,
        PluginKind.EVALUATOR,
        name=name,
        agent_id=agent_id,
    )
    version = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="score"),
        display_version=None,
    )
    return version.id


async def test_create_session_run_creates_a_pending_job_with_one_agent_task(
    services: JobAndTaskServices,
) -> None:
    """A session run creates one job holding one labeled agent task."""
    version_id = await _runnable_agent_version(services)
    job = await services.job_service.create_session_run(
        SessionRunCreate(agent_version_id=version_id, inputs={"q": "hi"}, name="run-1"),
        actor=ACTOR,
    )
    assert job.status is JobStatus.PENDING
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job.id), actor=ACTOR
    )
    assert len(tasks) == 1
    task = tasks[0]
    assert isinstance(task, AgentTask)
    assert task.agent_version_id == version_id
    assert task.labels == {
        "kitaru/agent_version": str(version_id),
        "agent_version": str(version_id),
    }
    assert task.env == {"KITARU_SESSION_NAME": "run-1"}


async def test_create_session_run_stamps_the_job_kind_session_run(
    services: JobAndTaskServices,
) -> None:
    """A session run's job carries the session_run kind."""
    version_id = await _runnable_agent_version(services)
    job = await services.job_service.create_session_run(
        SessionRunCreate(agent_version_id=version_id, inputs=None), actor=ACTOR
    )
    assert job.kind is JobKind.SESSION_RUN


async def test_create_session_run_without_a_name_sets_no_session_name_env(
    services: JobAndTaskServices,
) -> None:
    """A session run without a name carries no session name env extra."""
    version_id = await _runnable_agent_version(services)
    job = await services.job_service.create_session_run(
        SessionRunCreate(agent_version_id=version_id, inputs=None), actor=ACTOR
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job.id), actor=ACTOR
    )
    assert tasks[0].env == {}


async def test_create_session_run_rejects_a_version_without_a_run_spec(
    services: JobAndTaskServices,
) -> None:
    """An agent version without a run spec fails at creation, not at claim time."""
    agent = await create_agent(services.agents, ACTOR.account.id)
    version = await create_agent_version(
        services.agent_versions, agent_id=agent.id, owner_id=ACTOR.account.id
    )
    with pytest.raises(AgentVersionWithoutRunSpec):
        await services.job_service.create_session_run(
            SessionRunCreate(agent_version_id=version.id, inputs=None), actor=ACTOR
        )


async def test_create_import_resolves_latest_version_by_default(
    services: JobAndTaskServices,
) -> None:
    """An omitted import version resolves to the importer's latest."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.IMPORTER, name="csv"
    )
    v1 = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    v2 = await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    payload = await create_blob(services.blobs, ACTOR.account.id, content=b"csv-data")
    agent = await create_agent(services.agents, ACTOR.account.id)

    job = await services.job_service.create_import(
        ImportCreate(
            importer="csv", agent_id=agent.id, payload_blob_id=payload.id, params={}
        ),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job.id), actor=ACTOR
    )
    task = tasks[0]
    assert isinstance(task, ImportTask)
    assert task.plugin_version_id == v2.id
    assert task.plugin_version_id != v1.id
    assert task.labels == {}


async def test_create_import_stamps_the_job_kind_import(
    services: JobAndTaskServices,
) -> None:
    """An import's job carries the import kind."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.IMPORTER, name="csv"
    )
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    payload = await create_blob(services.blobs, ACTOR.account.id, content=b"csv-data")
    agent = await create_agent(services.agents, ACTOR.account.id)

    job = await services.job_service.create_import(
        ImportCreate(
            importer="csv", agent_id=agent.id, payload_blob_id=payload.id, params={}
        ),
        actor=ACTOR,
    )
    assert job.kind is JobKind.IMPORT


async def test_create_import_stamps_the_agent_version_on_its_task(
    services: JobAndTaskServices,
) -> None:
    """An import naming an agent version carries it onto the importer task."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.IMPORTER, name="csv"
    )
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    payload = await create_blob(services.blobs, ACTOR.account.id, content=b"csv-data")
    agent = await create_agent(services.agents, ACTOR.account.id)
    version = await create_agent_version(
        services.agent_versions, agent_id=agent.id, owner_id=ACTOR.account.id
    )

    job = await services.job_service.create_import(
        ImportCreate(
            importer="csv",
            agent_id=agent.id,
            agent_version_id=version.id,
            payload_blob_id=payload.id,
            params={},
        ),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job.id), actor=ACTOR
    )
    task = tasks[0]
    assert isinstance(task, ImportTask)
    assert task.agent_version_id == version.id


async def test_create_import_rejects_a_version_of_another_agent(
    services: JobAndTaskServices,
) -> None:
    """An import pairing an agent with another agent's version is rejected."""
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, PluginKind.IMPORTER, name="csv"
    )
    await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=uuid.uuid4(), entrypoint="run"),
        display_version=None,
    )
    payload = await create_blob(services.blobs, ACTOR.account.id, content=b"csv-data")
    agent = await create_agent(services.agents, ACTOR.account.id)
    other = await create_agent(services.agents, ACTOR.account.id, name="other")
    version = await create_agent_version(
        services.agent_versions, agent_id=other.id, owner_id=ACTOR.account.id
    )

    with pytest.raises(AgentVersionAgentMismatch):
        await services.job_service.create_import(
            ImportCreate(
                importer="csv",
                agent_id=agent.id,
                agent_version_id=version.id,
                payload_blob_id=payload.id,
                params={},
            ),
            actor=ACTOR,
        )


async def test_create_evaluations_rejects_over_the_pair_cap() -> None:
    """A batch whose pair count exceeds the cap is rejected."""
    services = build_job_and_task_services(policy=TaskPolicy(evaluation_pair_limit=1))
    await _evaluator_version(services, "scorer")
    session_a = await create_session(
        services.sessions, ACTOR.account.id, agent_id=uuid.uuid4()
    )
    session_b = await create_session(
        services.sessions, ACTOR.account.id, agent_id=uuid.uuid4()
    )
    with pytest.raises(ValidationError):
        await services.job_service.create_evaluations(
            EvaluationBatchCreate(
                input_session_ids=[session_a.id, session_b.id],
                evaluators=[EvaluatorConfigInput(evaluator="scorer")],
            ),
            actor=ACTOR,
        )


async def test_create_evaluations_rejects_an_unknown_session(
    services: JobAndTaskServices,
) -> None:
    """An unknown input session id fails the whole batch."""
    await _evaluator_version(services, "scorer")
    with pytest.raises(ValidationError):
        await services.job_service.create_evaluations(
            EvaluationBatchCreate(
                input_session_ids=[uuid.uuid4()],
                evaluators=[EvaluatorConfigInput(evaluator="scorer")],
            ),
            actor=ACTOR,
        )


async def test_create_evaluations_rejects_an_in_progress_session(
    services: JobAndTaskServices,
) -> None:
    """An in-progress input session fails the whole batch."""
    await _evaluator_version(services, "scorer")
    session = await create_session(
        services.sessions, ACTOR.account.id, agent_id=uuid.uuid4()
    )
    with pytest.raises(SessionNotEvaluatable):
        await services.job_service.create_evaluations(
            EvaluationBatchCreate(
                input_session_ids=[session.id],
                evaluators=[EvaluatorConfigInput(evaluator="scorer")],
            ),
            actor=ACTOR,
        )


async def test_create_evaluations_scoped_evaluator_all_sessions_match(
    services: JobAndTaskServices,
) -> None:
    """Accept a scoped evaluator when every input session belongs to its agent."""
    agent_id = uuid.uuid4()
    await _evaluator_version(services, "scorer", agent_id=agent_id)
    session_a = await create_session(
        services.sessions, ACTOR.account.id, agent_id, status=SessionStatus.COMPLETED
    )
    session_b = await create_session(
        services.sessions, ACTOR.account.id, agent_id, status=SessionStatus.COMPLETED
    )
    job = await services.job_service.create_evaluations(
        EvaluationBatchCreate(
            input_session_ids=[session_a.id, session_b.id],
            evaluators=[EvaluatorConfigInput(evaluator="scorer")],
        ),
        actor=ACTOR,
    )
    assert job.kind is JobKind.EVALUATION


async def test_create_evaluations_scoped_evaluator_session_from_other_agent(
    services: JobAndTaskServices,
) -> None:
    """Reject a scoped evaluator whose only session belongs to another agent."""
    await _evaluator_version(services, "scorer", agent_id=uuid.uuid4())
    session = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.COMPLETED,
    )
    with pytest.raises(ValidationError, match="scoped to a different agent"):
        await services.job_service.create_evaluations(
            EvaluationBatchCreate(
                input_session_ids=[session.id],
                evaluators=[EvaluatorConfigInput(evaluator="scorer")],
            ),
            actor=ACTOR,
        )


async def test_create_evaluations_rejects_sessions_spanning_agents(
    services: JobAndTaskServices,
) -> None:
    """Reject input sessions that do not all belong to one agent."""
    await _evaluator_version(services, "scorer")
    session_a = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.COMPLETED,
    )
    session_b = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.COMPLETED,
    )
    with pytest.raises(ValidationError, match="single agent"):
        await services.job_service.create_evaluations(
            EvaluationBatchCreate(
                input_session_ids=[session_a.id, session_b.id],
                evaluators=[EvaluatorConfigInput(evaluator="scorer")],
            ),
            actor=ACTOR,
        )


async def test_create_evaluations_makes_one_continue_task_per_pair(
    services: JobAndTaskServices,
) -> None:
    """One continue evaluator task is created per (session, evaluator) pair."""
    await _evaluator_version(services, "accuracy")
    await _evaluator_version(services, "tone")
    agent_id = uuid.uuid4()
    session_a = await create_session(
        services.sessions, ACTOR.account.id, agent_id, status=SessionStatus.COMPLETED
    )
    session_b = await create_session(
        services.sessions, ACTOR.account.id, agent_id, status=SessionStatus.COMPLETED
    )

    job = await services.job_service.create_evaluations(
        EvaluationBatchCreate(
            input_session_ids=[session_a.id, session_b.id],
            evaluators=[
                EvaluatorConfigInput(evaluator="accuracy"),
                EvaluatorConfigInput(evaluator="tone"),
            ],
        ),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job.id), actor=ACTOR
    )
    assert len(tasks) == 4
    assert all(isinstance(task, EvaluationTask) for task in tasks)
    assert all(task.on_failure is TaskOnFailure.CONTINUE for task in tasks)
    assert all(task.kind is TaskKind.EVALUATOR for task in tasks)


async def test_create_evaluations_stamps_the_job_kind_evaluation(
    services: JobAndTaskServices,
) -> None:
    """An evaluation batch's job carries the evaluation kind."""
    await _evaluator_version(services, "accuracy")
    session = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.COMPLETED,
    )
    job = await services.job_service.create_evaluations(
        EvaluationBatchCreate(
            input_session_ids=[session.id],
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    assert job.kind is JobKind.EVALUATION


async def test_add_task_conflicts_on_a_settled_job(
    services: JobAndTaskServices,
) -> None:
    """Appending a task to a settled job conflicts."""
    job = await create_job(services.jobs, ACTOR.account.id, status=JobStatus.COMPLETED)
    with pytest.raises(JobAlreadySettled):
        await services.job_service.add_task(
            AgentTask(job_id=job.id, agent_version_id=uuid.uuid4())
        )


async def test_cancel_job_cancels_pending_tasks_and_stamps_in_flight_ones(
    services: JobAndTaskServices,
) -> None:
    """Cancel stamps cancel_requested_at on the job and its in-flight tasks."""
    version_id = await _runnable_agent_version(services)
    job = await services.job_service.create_session_run(
        SessionRunCreate(agent_version_id=version_id, inputs=None), actor=ACTOR
    )
    pending_task = await create_agent_task(
        services.tasks, job.id, agent_version_id=version_id
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job.id), actor=ACTOR
    )
    claimable_task = next(t for t in tasks if t.id != pending_task.id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        1, actor=build_worker_actor(ACTOR.account, worker.id)
    )

    canceled_job = await services.job_service.cancel_job(job.id, actor=ACTOR)
    assert canceled_job.cancel_requested_at is not None

    stored_pending = await services.tasks.get(pending_task.id)
    assert stored_pending.status is TaskStatus.CANCELED

    stored_claimed = await services.tasks.get(claimable_task.id)
    assert stored_claimed.status is TaskStatus.CLAIMED
    assert stored_claimed.cancel_requested_at is not None


async def test_cancel_job_rejects_an_already_settled_job(
    services: JobAndTaskServices,
) -> None:
    """Canceling a settled job conflicts."""
    job = await create_job(services.jobs, ACTOR.account.id, status=JobStatus.COMPLETED)
    with pytest.raises(JobAlreadySettled):
        await services.job_service.cancel_job(job.id, actor=ACTOR)


async def test_delete_job_cascades_its_tasks(services: JobAndTaskServices) -> None:
    """Deleting a settled job removes its tasks in the fake, mirroring the cascade."""
    job = await create_job(services.jobs, ACTOR.account.id, status=JobStatus.COMPLETED)
    task = await create_agent_task(services.tasks, job.id)
    await services.job_service.delete_job(job.id, actor=ACTOR)
    with pytest.raises(JobNotFound):
        await services.jobs.get(job.id)
    assert await services.tasks.get_many([task.id]) == {}


async def test_delete_job_rejects_an_unsettled_job(
    services: JobAndTaskServices,
) -> None:
    """Deleting a job that has not settled conflicts and leaves it stored."""
    job = await create_job(services.jobs, ACTOR.account.id)
    with pytest.raises(JobNotSettled):
        await services.job_service.delete_job(job.id, actor=ACTOR)
    assert (await services.jobs.get(job.id)).id == job.id


async def test_settlement_precedence_failed_over_canceled_over_completed(
    services: JobAndTaskServices,
) -> None:
    """A counted hard failure settles the job failed even beside a canceled task."""
    job = await create_job(services.jobs, ACTOR.account.id)
    version_id = await _runnable_agent_version(services)
    failing = await create_agent_task(
        services.tasks,
        job.id,
        agent_version_id=version_id,
        on_failure=TaskOnFailure.ABORT,
    )
    sibling = await create_agent_task(
        services.tasks, job.id, agent_version_id=version_id
    )
    worker = await create_worker(services.workers, ACTOR.account.id)
    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert {item.task.id for item in claimed} == {failing.id, sibling.id}

    await services.task_service.update_task(
        failing.id,
        TaskUpdate(status=TaskStatus.FAILED, error="boom"),
        actor=build_task_actor(ACTOR.account, failing.id, 1, worker.id),
    )
    # The report stamps the job alone. It does not settle the job until every
    # task reaches a terminal status, and it leaves the sibling row untouched
    # for the sweep's propagation backstop.
    job_after = await services.jobs.get(job.id)
    assert job_after.status is JobStatus.RUNNING
    assert job_after.cancel_requested_at is not None

    sibling_after = await services.tasks.get(sibling.id)
    assert sibling_after.status is TaskStatus.CLAIMED
    assert sibling_after.cancel_requested_at is None

    assert await services.task_service.list_unpropagated_cancel_job_ids() == [job.id]
    await services.task_service.propagate_job_cancel(job.id)

    sibling_after = await services.tasks.get(sibling.id)
    assert sibling_after.status is TaskStatus.CLAIMED
    assert sibling_after.cancel_requested_at is not None

    # The claimed sibling reaches its own terminal status the usual way,
    # through its worker, and that drains the job.
    await services.task_service.update_task(
        sibling.id,
        TaskUpdate(status=TaskStatus.CANCELED),
        actor=build_task_actor(ACTOR.account, sibling.id, 1, worker.id),
    )
    job_after = await services.jobs.get(job.id)
    assert job_after.status is JobStatus.FAILED
    assert job_after.error == "boom"


async def test_ignore_failure_neither_cancels_nor_counts(
    services: JobAndTaskServices,
) -> None:
    """An ignore task's failure settles the job completed without touching siblings."""
    job = await create_job(services.jobs, ACTOR.account.id)
    version_id = await _runnable_agent_version(services)
    ignored = await create_agent_task(
        services.tasks,
        job.id,
        agent_version_id=version_id,
        on_failure=TaskOnFailure.IGNORE,
    )
    sibling = await create_agent_task(
        services.tasks, job.id, agent_version_id=version_id
    )
    worker = await create_worker(services.workers, ACTOR.account.id)
    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert len(claimed) == 2

    await services.task_service.update_task(
        ignored.id,
        TaskUpdate(status=TaskStatus.FAILED, error="boom"),
        actor=build_task_actor(ACTOR.account, ignored.id, 1, worker.id),
    )
    sibling_after = await services.tasks.get(sibling.id)
    assert sibling_after.status is TaskStatus.CLAIMED
    assert sibling_after.cancel_requested_at is None

    await services.task_service.update_task(
        sibling.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, sibling.id, 1, worker.id),
    )
    session = await create_session(
        services.sessions, ACTOR.account.id, agent_id=uuid.uuid4(), task_id=sibling.id
    )
    session.status = SessionStatus.COMPLETED
    await services.sessions.update(session)
    await services.task_service.update_task(
        sibling.id,
        TaskUpdate(status=TaskStatus.COMPLETED),
        actor=build_task_actor(ACTOR.account, sibling.id, 1, worker.id),
    )
    job_after = await services.jobs.get(job.id)
    assert job_after.status is JobStatus.COMPLETED


async def test_appended_task_blocks_settlement(services: JobAndTaskServices) -> None:
    """A task a TaskTerminal subscriber appends lands before the job settles."""
    job = await create_job(services.jobs, ACTOR.account.id)
    version_id = await _runnable_agent_version(services)
    task = await create_agent_task(services.tasks, job.id, agent_version_id=version_id)

    appended_id = uuid.uuid4()

    async def append_a_task(event: Event) -> None:
        assert isinstance(event, TaskTerminal)
        await services.job_service.add_task(
            AgentTask(id=appended_id, job_id=job.id, agent_version_id=version_id)
        )

    services.task_service._transitions._dispatcher.register(TaskTerminal, append_a_task)

    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    session = await create_session(
        services.sessions, ACTOR.account.id, agent_id=uuid.uuid4(), task_id=task.id
    )
    session.status = SessionStatus.COMPLETED
    await services.task_service.update_task(
        task.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, task.id, 1, worker.id),
    )
    await services.sessions.update(session)
    await services.task_service.update_task(
        task.id,
        TaskUpdate(status=TaskStatus.COMPLETED),
        actor=build_task_actor(ACTOR.account, task.id, 1, worker.id),
    )

    job_after = await services.jobs.get(job.id)
    assert not job_after.settled
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job.id), actor=ACTOR
    )
    assert appended_id in {t.id for t in tasks}


async def test_list_jobs_filters_by_status(services: JobAndTaskServices) -> None:
    """List jobs filters by status."""
    await create_job(services.jobs, ACTOR.account.id, status=JobStatus.PENDING)
    await create_job(services.jobs, ACTOR.account.id, status=JobStatus.COMPLETED)
    items, _ = await services.job_service.list_jobs(
        JobFilter(
            expression=FilterCondition(
                field="status", op=FilterOp.EQ, value=JobStatus.COMPLETED
            )
        ),
        actor=ACTOR,
    )
    assert len(items) == 1
    assert items[0].status is JobStatus.COMPLETED


async def test_list_jobs_filters_by_kind(services: JobAndTaskServices) -> None:
    """List jobs filters by kind."""
    await create_job(services.jobs, ACTOR.account.id, kind=JobKind.SESSION_RUN)
    await create_job(services.jobs, ACTOR.account.id, kind=JobKind.REPLAY)
    items, _ = await services.job_service.list_jobs(
        JobFilter(
            expression=FilterCondition(
                field="kind", op=FilterOp.EQ, value=JobKind.REPLAY
            )
        ),
        actor=ACTOR,
    )
    assert len(items) == 1
    assert items[0].kind is JobKind.REPLAY


async def test_list_job_tasks_requires_an_existing_job(
    services: JobAndTaskServices,
) -> None:
    """Listing the tasks of an unknown job conflicts."""
    with pytest.raises(JobNotFound):
        await services.job_service.list_job_tasks(
            uuid.uuid4(), TaskFilter(), actor=ACTOR
        )


async def test_duplicate_evaluator_pair_within_a_job_conflicts(
    services: JobAndTaskServices,
) -> None:
    """The unique (job_id, input_session_id, plugin_version_id) key backstops."""
    job = await create_job(services.jobs, ACTOR.account.id)
    evaluator_id = await _evaluator_version(services, "scorer")
    session = await create_session(
        services.sessions, ACTOR.account.id, agent_id=uuid.uuid4()
    )
    await create_evaluation_task(
        services.tasks,
        job.id,
        plugin_version_id=evaluator_id,
        input_session_id=session.id,
    )
    with pytest.raises(DuplicateEvaluationTask):
        await create_evaluation_task(
            services.tasks,
            job.id,
            plugin_version_id=evaluator_id,
            input_session_id=session.id,
        )
