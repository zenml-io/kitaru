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
"""End-to-end tests for the replay pipeline and experiment run fan-out."""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest

from conftest import (
    ReplayServices,
    build_replay_services,
    build_task_actor,
    build_worker_actor,
    create_agent,
    create_agent_task,
    create_agent_version,
    create_blob,
    create_cohort,
    create_cohort_version,
    create_evaluation_task,
    create_import_task,
    create_job,
    create_plugin,
    create_replay,
    create_session,
    create_worker,
)
from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.replay import ReplayStatus
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.task import TaskOnFailure, TaskStatus
from kitaru.server.application.models.auth import (
    AuthContext,
)
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.application.models.experiment import ExperimentCreate
from kitaru.server.application.models.experiment_run import ExperimentRunCreate
from kitaru.server.application.models.job import JobFilter
from kitaru.server.application.models.replay import ReplayCreate, ReplayFilter
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.application.models.session import SessionCreate, SessionUpdate
from kitaru.server.application.models.task import TaskFilter, TaskUpdate
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionAgentMismatch,
    CommandRunSpec,
    FunctionRunSpec,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.cohort_version import CohortVersion, CohortVersionIdNotFound
from kitaru.server.domain.plugin import PluginKind, PluginVersion, ScriptPluginSource
from kitaru.server.domain.replay import (
    DuplicateReplayForBaseline,
    ReplayBaselineNotTerminal,
)
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import (
    AgentTask,
    CommandAgentTaskDetails,
    EvaluationTask,
    TaskResultSessionAlreadyLinked,
)
from kitaru.server.filtering import FilterCondition

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


@pytest.fixture
def services() -> ReplayServices:
    """Provide fake-backed replay, experiment, and run services."""
    return build_replay_services()


async def _agent_version_with_run_spec(services: ReplayServices) -> AgentVersion:
    agent = await create_agent(services.agents, ACTOR.account.id)
    return await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACTOR.account.id,
        run_spec=CommandRunSpec(command="run.sh", timeout_seconds=60),
    )


async def _function_agent_version(services: ReplayServices) -> AgentVersion:
    agent = await create_agent(services.agents, ACTOR.account.id)
    return await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACTOR.account.id,
        run_spec=FunctionRunSpec(entrypoint="pkg.mod:run", timeout_seconds=60),
    )


async def _evaluator_version(services: ReplayServices, name: str) -> PluginVersion:
    plugin = await create_plugin(
        services.plugins, ACTOR.account.id, kind=PluginKind.EVALUATOR, name=name
    )
    blob = await create_blob(services.blobs, ACTOR.account.id, content=name.encode())
    return await services.plugins.create_version(
        plugin.id,
        ScriptPluginSource(blob_id=blob.id, entrypoint="score"),
        display_version=None,
    )


async def _baseline_session(
    services: ReplayServices, agent_version: AgentVersion
) -> Session:
    return await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=agent_version.agent_id,
        agent_version_id=agent_version.id,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.COMPLETED,
        inputs={"q": "hi"},
    )


async def _cohort_version(
    services: ReplayServices,
    agent_id: uuid.UUID,
    session_ids: Sequence[uuid.UUID] = (),
    name: str = "cohort",
) -> CohortVersion:
    cohort = await create_cohort(
        services.cohorts, ACTOR.account.id, agent_id, name=name
    )
    return await create_cohort_version(
        services.cohort_versions, ACTOR.account.id, cohort.id, session_ids
    )


async def test_standalone_replay_pipeline_end_to_end(services: ReplayServices) -> None:
    """A standalone replay runs through the whole pipeline to completion."""
    agent_version = await _agent_version_with_run_spec(services)
    evaluator = await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    assert bundle.replay.status is ReplayStatus.PENDING
    assert bundle.result_session_id is None

    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert len(tasks) == 1
    agent_task = tasks[0]
    assert isinstance(agent_task, AgentTask)
    assert agent_task.inputs == baseline.inputs
    assert agent_task.env == {}
    assert agent_task.labels == {"agent_version": str(agent_version.id)}
    assert agent_task.on_failure is TaskOnFailure.ABORT

    spec = await services.task_service.get_spec(agent_task.id, actor=ACTOR)
    assert isinstance(spec.details, CommandAgentTaskDetails)
    assert spec.details.replay_id == bundle.replay.id

    worker = await create_worker(services.workers, ACTOR.account.id)
    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert len(claimed) == 1
    await services.task_service.update_task(
        agent_task.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, agent_task.id, 1, worker.id),
    )

    result_session = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=agent_version.agent_id,
        agent_version_id=agent_version.id,
        origin=SessionOrigin.REPLAY,
        task_id=agent_task.id,
    )
    result_session.status = SessionStatus.COMPLETED
    await services.sessions.update(result_session)
    stored_agent_task = await services.tasks.get(agent_task.id)
    assert isinstance(stored_agent_task, AgentTask)
    stored_agent_task.result_session_id = result_session.id
    await services.tasks.update(stored_agent_task)

    await services.task_service.update_task(
        agent_task.id,
        TaskUpdate(status=TaskStatus.COMPLETED),
        actor=build_task_actor(ACTOR.account, agent_task.id, 1, worker.id),
    )

    replay_after = await services.replays.get(bundle.replay.id)
    assert replay_after.status is ReplayStatus.EVALUATING

    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    eval_tasks = [task for task in tasks if isinstance(task, EvaluationTask)]
    assert len(eval_tasks) == 1
    eval_task = eval_tasks[0]
    assert eval_task.input_session_id == result_session.id
    assert eval_task.plugin_version_id == evaluator.id
    assert eval_task.on_failure is TaskOnFailure.ABORT

    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert len(claimed) == 1
    await services.task_service.update_task(
        eval_task.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, eval_task.id, 1, worker.id),
    )
    await services.task_service.update_task(
        eval_task.id,
        TaskUpdate(
            status=TaskStatus.COMPLETED,
            result=[{"name": "accuracy", "score": 0.9}],
        ),
        actor=build_task_actor(ACTOR.account, eval_task.id, 1, worker.id),
    )

    evaluations, _ = await services.evaluations.query(
        EvaluationFilter(
            expression=FilterCondition(
                field="session_id", op=FilterOp.EQ, value=result_session.id
            )
        )
    )
    assert len(evaluations) == 1
    assert evaluations[0].evaluation.name == "accuracy"
    assert evaluations[0].evaluation.score == 0.9
    assert evaluations[0].evaluation.task_id == eval_task.id
    assert evaluations[0].evaluation.evaluator_version_id == evaluator.id

    job_after = await services.jobs.get(bundle.replay.job_id)
    assert job_after.status is JobStatus.COMPLETED

    replay_final = await services.replays.get(bundle.replay.id)
    assert replay_final.status is ReplayStatus.COMPLETED

    final_bundle = await services.replay_service.get_replay(
        bundle.replay.id, actor=ACTOR
    )
    assert final_bundle.result_session_id == result_session.id


async def test_standalone_replay_stamps_the_job_kind_replay(
    services: ReplayServices,
) -> None:
    """A standalone replay's job carries the replay kind."""
    agent_version = await _agent_version_with_run_spec(services)
    await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    job = await services.jobs.get(bundle.replay.job_id)
    assert job.kind is JobKind.REPLAY


async def test_function_mode_replay_creates_a_provisional_job_with_no_wait_task(
    services: ReplayServices,
) -> None:
    """A function-mode replay's job is provisional and holds only its agent task."""
    agent_version = await _function_agent_version(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.provisional is True

    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert len(tasks) == 1
    agent_task = tasks[0]
    assert isinstance(agent_task, AgentTask)
    assert agent_task.job_id == bundle.replay.job_id
    assert agent_task.status is TaskStatus.PENDING


async def test_command_mode_replay_creates_a_non_provisional_job(
    services: ReplayServices,
) -> None:
    """A command-mode agent version's replay job is not provisional."""
    agent_version = await _agent_version_with_run_spec(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.provisional is False

    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert len(tasks) == 1
    assert isinstance(tasks[0], AgentTask)


async def test_append_result_evaluations_defers_a_pending_import_result_session(
    services: ReplayServices,
) -> None:
    """A completed agent task with a pending-import session appends nothing yet."""
    agent_version = await _function_agent_version(services)
    await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    function_task = next(task for task in tasks if isinstance(task, AgentTask))

    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    function_actor = build_task_actor(ACTOR.account, function_task.id, 1, worker.id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=function_actor
    )
    await services.session_service.create_session(
        SessionCreate(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id="ext-1",
        ),
        actor=function_actor,
    )

    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.COMPLETED), actor=function_actor
    )

    tasks_after, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert not any(isinstance(task, EvaluationTask) for task in tasks_after)
    replay_after = await services.replays.get(bundle.replay.id)
    assert replay_after.status is ReplayStatus.PENDING


async def _run_function_task_to_a_pending_import_placeholder(
    services: ReplayServices,
    job_id: uuid.UUID,
    external_id: str,
    worker_id: uuid.UUID,
) -> tuple[AgentTask, Session]:
    """Claim, run, and complete a job's function task against a fresh placeholder."""
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=job_id), actor=ACTOR
    )
    function_task = next(task for task in tasks if isinstance(task, AgentTask))
    function_actor = build_task_actor(ACTOR.account, function_task.id, 1, worker_id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=function_actor
    )
    placeholder = await services.session_service.create_session(
        SessionCreate(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id=external_id,
        ),
        actor=function_actor,
    )
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.COMPLETED), actor=function_actor
    )
    return function_task, placeholder


async def _adopt_via_import_task(
    services: ReplayServices,
    agent_id: uuid.UUID,
    external_id: str,
    worker_id: uuid.UUID,
) -> Session:
    """Adopt a pending-import placeholder through a fresh, running import task."""
    import_task = await create_import_task(
        services.tasks, uuid.uuid4(), agent_id=agent_id
    )
    now = datetime.now(UTC)
    import_task.claim(worker_id, now)
    import_task.start(now)
    await services.tasks.update(import_task)
    return await services.session_service.create_session(
        SessionCreate(origin=SessionOrigin.IMPORTED, external_id=external_id),
        actor=build_task_actor(ACTOR.account, import_task.id, 1, worker_id),
    )


async def test_continue_replay_on_import_completed_appends_evaluators(
    services: ReplayServices,
) -> None:
    """A completed import appends one evaluator task per config evaluator."""
    agent_version = await _function_agent_version(services)
    evaluator = await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )

    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    _, placeholder = await _run_function_task_to_a_pending_import_placeholder(
        services, bundle.replay.job_id, "ext-1", worker.id
    )
    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    assert adopted.id == placeholder.id

    finalized = await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )

    job_after = await services.jobs.get(bundle.replay.job_id)
    assert job_after.provisional is False
    assert not job_after.settled

    tasks_after, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    eval_tasks = [task for task in tasks_after if isinstance(task, EvaluationTask)]
    assert len(eval_tasks) == 1
    assert eval_tasks[0].input_session_id == finalized.id
    assert eval_tasks[0].plugin_version_id == evaluator.id

    replay_after = await services.replays.get(bundle.replay.id)
    assert replay_after.status is ReplayStatus.EVALUATING


async def test_continue_replay_on_import_completed_with_no_evaluators_settles_the_job(
    services: ReplayServices,
) -> None:
    """A completed import with no configured evaluators drains the job to completed."""
    agent_version = await _function_agent_version(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )

    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    _, placeholder = await _run_function_task_to_a_pending_import_placeholder(
        services, bundle.replay.job_id, "ext-1", worker.id
    )
    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    assert adopted.id == placeholder.id

    await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )

    job_after = await services.jobs.get(bundle.replay.job_id)
    assert job_after.status is JobStatus.COMPLETED
    assert job_after.provisional is False

    replay_after = await services.replays.get(bundle.replay.id)
    assert replay_after.status is ReplayStatus.COMPLETED


async def test_continue_replay_on_import_failed_fails_the_job_with_the_sessions_error(
    services: ReplayServices,
) -> None:
    """A failed import fails the job with the session's error."""
    agent_version = await _function_agent_version(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )

    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    _, placeholder = await _run_function_task_to_a_pending_import_placeholder(
        services, bundle.replay.job_id, "ext-1", worker.id
    )

    await services.session_service.update_session(
        placeholder.id,
        SessionUpdate(status=SessionStatus.FAILED, error="no import arrived"),
        actor=ACTOR,
    )

    job_after = await services.jobs.get(bundle.replay.job_id)
    assert job_after.status is JobStatus.FAILED
    assert job_after.error == "no import arrived"

    replay_after = await services.replays.get(bundle.replay.id)
    assert replay_after.status is ReplayStatus.FAILED
    assert replay_after.error == "no import arrived"


async def test_continue_replay_on_import_without_a_producer_task_is_a_noop(
    services: ReplayServices,
) -> None:
    """A finalized import for a session no task produced raises nothing."""
    session = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.PENDING_IMPORT,
    )

    finalized = await services.session_service.update_session(
        session.id,
        SessionUpdate(status=SessionStatus.FAILED, error="boom"),
        actor=ACTOR,
    )
    assert finalized.status is SessionStatus.FAILED


async def test_continue_replay_on_import_outside_a_replay_job_is_a_noop(
    services: ReplayServices,
) -> None:
    """A finalized import whose task belongs to a job with no replay is a no-op."""
    job = await create_job(services.jobs, ACTOR.account.id)
    task = await create_agent_task(services.tasks, job.id)
    placeholder = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=uuid.uuid4(),
        status=SessionStatus.PENDING_IMPORT,
        task_id=task.id,
    )

    finalized = await services.session_service.update_session(
        placeholder.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )

    job_after = await services.jobs.get(job.id)
    assert job_after.status is JobStatus.PENDING
    assert finalized.status is SessionStatus.COMPLETED


async def test_continue_replay_on_import_after_the_replay_settled_is_a_noop(
    services: ReplayServices,
) -> None:
    """A finalized import for an already-settled replay leaves it alone."""
    agent_version = await _function_agent_version(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    _, placeholder = await _run_function_task_to_a_pending_import_placeholder(
        services, bundle.replay.job_id, "ext-2", worker.id
    )

    # Fail the replay before the import finalizes, so the settlement below
    # runs against an already-settled replay.
    stored_replay = await services.replays.get(bundle.replay.id)
    stored_replay.fail("agent task failed")
    await services.replays.update(stored_replay)

    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-2", worker.id
    )
    assert adopted.id == placeholder.id

    await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )

    unchanged = await services.replays.get(bundle.replay.id)
    assert unchanged.status is ReplayStatus.FAILED
    assert unchanged.error == "agent task failed"
    tasks_after, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert not any(isinstance(task, EvaluationTask) for task in tasks_after)


async def test_continue_replay_on_import_before_agent_task_completes(
    services: ReplayServices,
) -> None:
    """An import finalizing before its agent task completes settles on completion."""
    agent_version = await _function_agent_version(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    function_task = next(task for task in tasks if isinstance(task, AgentTask))

    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    function_actor = build_task_actor(ACTOR.account, function_task.id, 1, worker.id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=function_actor
    )
    placeholder = await services.session_service.create_session(
        SessionCreate(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id="ext-1",
        ),
        actor=function_actor,
    )

    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    assert adopted.id == placeholder.id
    await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )

    job_after_import = await services.jobs.get(bundle.replay.job_id)
    assert job_after_import.provisional is False
    assert not job_after_import.settled

    stored_function_task = await services.tasks.get(function_task.id)
    assert stored_function_task.status is TaskStatus.RUNNING

    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.COMPLETED), actor=function_actor
    )

    tasks_after_completion, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert not any(isinstance(task, EvaluationTask) for task in tasks_after_completion)
    job_settled = await services.jobs.get(bundle.replay.job_id)
    assert job_settled.status is JobStatus.COMPLETED
    replay_settled = await services.replays.get(bundle.replay.id)
    assert replay_settled.status is ReplayStatus.COMPLETED


async def test_function_mode_replay_end_to_end(services: ReplayServices) -> None:
    """A function-mode replay adopts its placeholder and finishes like a command one."""
    agent_version = await _function_agent_version(services)
    evaluator = await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    assert bundle.replay.status is ReplayStatus.PENDING

    job_before = await services.jobs.get(bundle.replay.job_id)
    assert job_before.provisional is True

    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    function_task = next(task for task in tasks if isinstance(task, AgentTask))

    worker = await create_worker(services.workers, ACTOR.account.id)
    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert [item.task.id for item in claimed] == [function_task.id]

    _, placeholder = await _run_function_task_to_a_pending_import_placeholder(
        services, bundle.replay.job_id, "ext-1", worker.id
    )
    assert placeholder.status == SessionStatus.PENDING_IMPORT

    replay_after_function_task = await services.replays.get(bundle.replay.id)
    assert replay_after_function_task.status is ReplayStatus.PENDING
    tasks_after_function_task, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert not any(
        isinstance(task, EvaluationTask) for task in tasks_after_function_task
    )

    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    assert adopted.id == placeholder.id
    assert adopted.status == SessionStatus.PENDING_IMPORT

    finalized = await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )

    job_after_finalize = await services.jobs.get(bundle.replay.job_id)
    assert job_after_finalize.provisional is False

    replay_evaluating = await services.replays.get(bundle.replay.id)
    assert replay_evaluating.status is ReplayStatus.EVALUATING

    tasks_after_finalize, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    eval_task = next(
        task for task in tasks_after_finalize if isinstance(task, EvaluationTask)
    )
    assert eval_task.input_session_id == finalized.id
    assert eval_task.plugin_version_id == evaluator.id

    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert [item.task.id for item in claimed] == [eval_task.id]
    eval_actor = build_task_actor(ACTOR.account, eval_task.id, 1, worker.id)
    await services.task_service.update_task(
        eval_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=eval_actor
    )
    await services.task_service.update_task(
        eval_task.id,
        TaskUpdate(
            status=TaskStatus.COMPLETED,
            result=[{"name": "accuracy", "score": 1.0}],
        ),
        actor=eval_actor,
    )

    job_after = await services.jobs.get(bundle.replay.job_id)
    assert job_after.status is JobStatus.COMPLETED

    replay_final = await services.replays.get(bundle.replay.id)
    assert replay_final.status is ReplayStatus.COMPLETED

    final_bundle = await services.replay_service.get_replay(
        bundle.replay.id, actor=ACTOR
    )
    assert final_bundle.result_session_id == finalized.id


async def test_evaluate_baselines_skips_already_scored_pairs(
    services: ReplayServices,
) -> None:
    """Baseline evaluator tasks skip evaluator versions that already scored it."""
    agent_version = await _agent_version_with_run_spec(services)
    evaluator_a = await _evaluator_version(services, "accuracy")
    evaluator_b = await _evaluator_version(services, "tone")
    baseline = await _baseline_session(services, agent_version)

    prior_job = await create_job(services.jobs, ACTOR.account.id)
    prior_task = await create_evaluation_task(
        services.tasks,
        prior_job.id,
        plugin_version_id=evaluator_a.id,
        input_session_id=baseline.id,
        on_failure=TaskOnFailure.CONTINUE,
    )
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    await services.task_service.update_task(
        prior_task.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, prior_task.id, 1, worker.id),
    )
    await services.task_service.update_task(
        prior_task.id,
        TaskUpdate(
            status=TaskStatus.COMPLETED,
            result=[{"name": "accuracy", "score": 1.0}],
        ),
        actor=build_task_actor(ACTOR.account, prior_task.id, 1, worker.id),
    )

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[
                EvaluatorConfigInput(evaluator="accuracy"),
                EvaluatorConfigInput(evaluator="tone"),
            ],
            evaluate_baselines=True,
        ),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    baseline_tasks = [
        task
        for task in tasks
        if isinstance(task, EvaluationTask) and task.input_session_id == baseline.id
    ]
    assert len(baseline_tasks) == 1
    assert baseline_tasks[0].plugin_version_id == evaluator_b.id
    assert baseline_tasks[0].on_failure is TaskOnFailure.ABORT


async def test_agent_task_failure_cancels_baseline_tasks_and_fails_replay(
    services: ReplayServices,
) -> None:
    """An agent task's hard failure aborts baseline tasks and fails the replay."""
    agent_version = await _agent_version_with_run_spec(services)
    await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
            evaluate_baselines=True,
        ),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    agent_task = next(task for task in tasks if isinstance(task, AgentTask))
    baseline_task = next(task for task in tasks if isinstance(task, EvaluationTask))

    worker = await create_worker(services.workers, ACTOR.account.id)
    claimed = await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    assert len(claimed) == 2

    await services.task_service.update_task(
        agent_task.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, agent_task.id, 1, worker.id),
    )
    await services.task_service.update_task(
        agent_task.id,
        TaskUpdate(status=TaskStatus.FAILED, error="boom"),
        actor=build_task_actor(ACTOR.account, agent_task.id, 1, worker.id),
    )

    job_after = await services.jobs.get(bundle.replay.job_id)
    assert job_after.cancel_requested_at is not None
    await services.task_service.propagate_job_cancel(bundle.replay.job_id)

    baseline_after = await services.tasks.get(baseline_task.id)
    assert baseline_after.status is TaskStatus.CLAIMED
    assert baseline_after.cancel_requested_at is not None

    await services.task_service.update_task(
        baseline_task.id,
        TaskUpdate(status=TaskStatus.CANCELED),
        actor=build_task_actor(ACTOR.account, baseline_task.id, 1, worker.id),
    )

    job_after = await services.jobs.get(bundle.replay.job_id)
    assert job_after.status is JobStatus.FAILED
    assert job_after.error == "boom"

    replay_after = await services.replays.get(bundle.replay.id)
    assert replay_after.status is ReplayStatus.FAILED
    assert replay_after.error == "boom"


async def test_baseline_evaluator_failure_fails_the_replay(
    services: ReplayServices,
) -> None:
    """A baseline evaluator's hard failure aborts the agent task and fails it."""
    agent_version = await _agent_version_with_run_spec(services)
    await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
            evaluate_baselines=True,
        ),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    agent_task = next(task for task in tasks if isinstance(task, AgentTask))
    baseline_task = next(task for task in tasks if isinstance(task, EvaluationTask))

    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )

    await services.task_service.update_task(
        baseline_task.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, baseline_task.id, 1, worker.id),
    )
    await services.task_service.update_task(
        baseline_task.id,
        TaskUpdate(status=TaskStatus.FAILED, error="scoring failed"),
        actor=build_task_actor(ACTOR.account, baseline_task.id, 1, worker.id),
    )

    await services.task_service.propagate_job_cancel(bundle.replay.job_id)

    agent_after = await services.tasks.get(agent_task.id)
    assert agent_after.cancel_requested_at is not None
    await services.task_service.update_task(
        agent_task.id,
        TaskUpdate(status=TaskStatus.CANCELED),
        actor=build_task_actor(ACTOR.account, agent_task.id, 1, worker.id),
    )

    job_after = await services.jobs.get(bundle.replay.job_id)
    assert job_after.status is JobStatus.FAILED

    replay_after = await services.replays.get(bundle.replay.id)
    assert replay_after.status is ReplayStatus.FAILED
    assert replay_after.error == "scoring failed"


async def _create_experiment_with_evaluator(
    services: ReplayServices, agent_id: uuid.UUID, evaluator_name: str = "accuracy"
) -> tuple[uuid.UUID, uuid.UUID]:
    await _evaluator_version(services, evaluator_name)
    experiment, config = await services.experiment_service.create_experiment(
        ExperimentCreate(
            name="exp1",
            agent_id=agent_id,
            evaluators=[EvaluatorConfigInput(evaluator=evaluator_name)],
        ),
        actor=ACTOR,
    )
    return experiment.id, config.id


async def test_start_run_creates_one_replay_per_cohort_session(
    services: ReplayServices,
) -> None:
    """A run fans out one replay per cohort session, each pointing at the config."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, config_id = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    sessions = [await _baseline_session(services, agent_version) for _ in range(3)]
    cohort_version = await _cohort_version(
        services, agent_version.agent_id, [session.id for session in sessions]
    )

    run, counts = await services.experiment_service.start_run(
        experiment_id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version.id, agent_version_id=agent_version.id
        ),
        actor=ACTOR,
    )
    assert run.number == 1
    assert counts.total == 3
    assert counts.pending == 3

    bundles, _ = await services.replay_service.list_replays(
        ReplayFilter(
            expression=FilterCondition(
                field="experiment_run_id", op=FilterOp.EQ, value=run.id
            )
        ),
        actor=ACTOR,
    )
    assert {bundle.replay.baseline_session_id for bundle in bundles} == {
        session.id for session in sessions
    }
    for bundle in bundles:
        assert bundle.replay.replay_config_id == config_id
        assert bundle.replay.experiment_run_id == run.id


async def test_start_run_creates_one_agent_task_per_replay_with_matching_fields(
    services: ReplayServices,
) -> None:
    """The batched fan-out gives each replay its own job holding one agent task."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    sessions = [await _baseline_session(services, agent_version) for _ in range(3)]
    cohort_version = await _cohort_version(
        services, agent_version.agent_id, [session.id for session in sessions]
    )

    run, _ = await services.experiment_service.start_run(
        experiment_id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version.id, agent_version_id=agent_version.id
        ),
        actor=ACTOR,
    )
    bundles, _ = await services.replay_service.list_replays(
        ReplayFilter(
            expression=FilterCondition(
                field="experiment_run_id", op=FilterOp.EQ, value=run.id
            )
        ),
        actor=ACTOR,
    )
    assert len({bundle.replay.job_id for bundle in bundles}) == len(bundles)

    baselines_by_id = {session.id: session for session in sessions}
    for bundle in bundles:
        baseline = baselines_by_id[bundle.replay.baseline_session_id]
        tasks, _ = await services.task_service.list_tasks(
            TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
        )
        assert len(tasks) == 1
        agent_task = tasks[0]
        assert isinstance(agent_task, AgentTask)
        assert agent_task.inputs == baseline.inputs
        assert agent_task.env == {}
        assert agent_task.labels == {"agent_version": str(agent_version.id)}
        assert agent_task.on_failure is TaskOnFailure.ABORT

        spec = await services.task_service.get_spec(agent_task.id, actor=ACTOR)
        assert isinstance(spec.details, CommandAgentTaskDetails)
        assert spec.details.replay_id == bundle.replay.id


async def test_start_run_stamps_the_job_kind_replay(
    services: ReplayServices,
) -> None:
    """A run's fanned-out replay jobs carry the replay kind."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    sessions = [await _baseline_session(services, agent_version) for _ in range(3)]
    cohort_version = await _cohort_version(
        services, agent_version.agent_id, [session.id for session in sessions]
    )

    run, _ = await services.experiment_service.start_run(
        experiment_id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version.id, agent_version_id=agent_version.id
        ),
        actor=ACTOR,
    )
    bundles, _ = await services.replay_service.list_replays(
        ReplayFilter(
            expression=FilterCondition(
                field="experiment_run_id", op=FilterOp.EQ, value=run.id
            )
        ),
        actor=ACTOR,
    )
    for bundle in bundles:
        job = await services.jobs.get(bundle.replay.job_id)
        assert job.kind is JobKind.REPLAY


async def test_start_run_evaluate_baselines_skips_already_scored_sessions(
    services: ReplayServices,
) -> None:
    """The batched fan-out skips baseline evaluator tasks per already-scored session."""
    agent_version = await _agent_version_with_run_spec(services)
    evaluator_a = await _evaluator_version(services, "accuracy")
    evaluator_b = await _evaluator_version(services, "tone")
    experiment, _ = await services.experiment_service.create_experiment(
        ExperimentCreate(
            name="exp1",
            agent_id=agent_version.agent_id,
            evaluators=[
                EvaluatorConfigInput(evaluator="accuracy"),
                EvaluatorConfigInput(evaluator="tone"),
            ],
        ),
        actor=ACTOR,
    )
    scored_session = await _baseline_session(services, agent_version)
    unscored_session = await _baseline_session(services, agent_version)

    prior_job = await create_job(services.jobs, ACTOR.account.id)
    prior_task = await create_evaluation_task(
        services.tasks,
        prior_job.id,
        plugin_version_id=evaluator_a.id,
        input_session_id=scored_session.id,
        on_failure=TaskOnFailure.CONTINUE,
    )
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    await services.task_service.update_task(
        prior_task.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, prior_task.id, 1, worker.id),
    )
    await services.task_service.update_task(
        prior_task.id,
        TaskUpdate(
            status=TaskStatus.COMPLETED,
            result=[{"name": "accuracy", "score": 1.0}],
        ),
        actor=build_task_actor(ACTOR.account, prior_task.id, 1, worker.id),
    )

    cohort_version = await _cohort_version(
        services,
        agent_version.agent_id,
        [scored_session.id, unscored_session.id],
    )
    run, _ = await services.experiment_service.start_run(
        experiment.id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version.id,
            agent_version_id=agent_version.id,
            evaluate_baselines=True,
        ),
        actor=ACTOR,
    )
    bundles, _ = await services.replay_service.list_replays(
        ReplayFilter(
            expression=FilterCondition(
                field="experiment_run_id", op=FilterOp.EQ, value=run.id
            )
        ),
        actor=ACTOR,
    )
    bundles_by_baseline = {
        bundle.replay.baseline_session_id: bundle for bundle in bundles
    }

    scored_tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundles_by_baseline[scored_session.id].replay.job_id),
        actor=ACTOR,
    )
    scored_baseline_tasks = [
        task
        for task in scored_tasks
        if isinstance(task, EvaluationTask)
        and task.input_session_id == scored_session.id
    ]
    assert len(scored_baseline_tasks) == 1
    assert scored_baseline_tasks[0].plugin_version_id == evaluator_b.id

    unscored_tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundles_by_baseline[unscored_session.id].replay.job_id),
        actor=ACTOR,
    )
    unscored_baseline_tasks = [
        task
        for task in unscored_tasks
        if isinstance(task, EvaluationTask)
        and task.input_session_id == unscored_session.id
    ]
    assert {task.plugin_version_id for task in unscored_baseline_tasks} == {
        evaluator_a.id,
        evaluator_b.id,
    }


async def test_start_run_rejects_an_empty_cohort_version(
    services: ReplayServices,
) -> None:
    """An empty cohort version rejects the run before any job is created."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    cohort_version = await _cohort_version(services, agent_version.agent_id)
    with pytest.raises(ValidationError, match="has no sessions"):
        await services.experiment_service.start_run(
            experiment_id,
            ExperimentRunCreate(
                cohort_version_id=cohort_version.id, agent_version_id=agent_version.id
            ),
            actor=ACTOR,
        )


async def test_start_run_unknown_cohort_version(services: ReplayServices) -> None:
    """Starting a run with an unknown cohort version id raises not-found."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    with pytest.raises(CohortVersionIdNotFound):
        await services.experiment_service.start_run(
            experiment_id,
            ExperimentRunCreate(
                cohort_version_id=uuid.uuid4(), agent_version_id=agent_version.id
            ),
            actor=ACTOR,
        )


async def test_start_run_rejects_a_cohort_version_of_another_agent(
    services: ReplayServices,
) -> None:
    """A cohort version of another agent's cohort rejects the run."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    other_agent = await create_agent(services.agents, ACTOR.account.id, name="other")
    session = await _baseline_session(services, agent_version)
    cohort_version = await _cohort_version(services, other_agent.id, [session.id])
    with pytest.raises(ValidationError, match="does not belong to agent"):
        await services.experiment_service.start_run(
            experiment_id,
            ExperimentRunCreate(
                cohort_version_id=cohort_version.id, agent_version_id=agent_version.id
            ),
            actor=ACTOR,
        )


async def test_start_run_rejects_an_agent_version_of_another_agent(
    services: ReplayServices,
) -> None:
    """An agent version of another agent rejects the run."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    session = await _baseline_session(services, agent_version)
    cohort_version = await _cohort_version(
        services, agent_version.agent_id, [session.id]
    )
    other_agent = await create_agent(services.agents, ACTOR.account.id, name="other")
    other_version = await create_agent_version(
        services.agent_versions,
        agent_id=other_agent.id,
        owner_id=ACTOR.account.id,
        run_spec=CommandRunSpec(command="run.sh", timeout_seconds=60),
    )
    with pytest.raises(AgentVersionAgentMismatch):
        await services.experiment_service.start_run(
            experiment_id,
            ExperimentRunCreate(
                cohort_version_id=cohort_version.id, agent_version_id=other_version.id
            ),
            actor=ACTOR,
        )


async def test_run_numbers_increment_per_experiment(services: ReplayServices) -> None:
    """Run numbers are assigned sequentially per experiment."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    session_a = await _baseline_session(services, agent_version)
    session_b = await _baseline_session(services, agent_version)
    cohort_version_a = await _cohort_version(
        services, agent_version.agent_id, [session_a.id]
    )
    cohort_version_b = await _cohort_version(
        services, agent_version.agent_id, [session_b.id], name="cohort-b"
    )

    run_1, _ = await services.experiment_service.start_run(
        experiment_id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version_a.id, agent_version_id=agent_version.id
        ),
        actor=ACTOR,
    )
    run_2, _ = await services.experiment_service.start_run(
        experiment_id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version_b.id, agent_version_id=agent_version.id
        ),
        actor=ACTOR,
    )
    assert run_1.number == 1
    assert run_2.number == 2


async def test_duplicate_replay_for_the_same_baseline_in_a_run_conflicts(
    services: ReplayServices,
) -> None:
    """The unique (experiment_run_id, baseline_session_id) key backstops."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    session = await _baseline_session(services, agent_version)
    cohort_version = await _cohort_version(
        services, agent_version.agent_id, [session.id]
    )
    run, _ = await services.experiment_service.start_run(
        experiment_id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version.id, agent_version_id=agent_version.id
        ),
        actor=ACTOR,
    )
    bundles, _ = await services.replay_service.list_replays(
        ReplayFilter(
            expression=FilterCondition(
                field="experiment_run_id", op=FilterOp.EQ, value=run.id
            )
        ),
        actor=ACTOR,
    )
    existing = bundles[0]
    other_job = await create_job(services.jobs, ACTOR.account.id)
    with pytest.raises(DuplicateReplayForBaseline):
        await create_replay(
            services.replays,
            ACTOR.account.id,
            other_job.id,
            existing.replay.replay_config_id,
            existing.replay.baseline_session_id,
            experiment_run_id=run.id,
        )


async def _cancel_run(services: ReplayServices, run_id: uuid.UUID) -> None:
    """Drive the two cancellation phases the way the endpoint does."""
    await services.experiment_run_service.mark_run_canceling(run_id, actor=ACTOR)
    await services.experiment_run_service.cancel_run_jobs(run_id, actor=ACTOR)


async def test_run_cancel_pending_only_run_cancels_immediately(
    services: ReplayServices,
) -> None:
    """A run with only pending tasks drains and cancels within the same call."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    sessions = [await _baseline_session(services, agent_version) for _ in range(2)]
    cohort_version = await _cohort_version(
        services, agent_version.agent_id, [session.id for session in sessions]
    )
    run, _ = await services.experiment_service.start_run(
        experiment_id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version.id, agent_version_id=agent_version.id
        ),
        actor=ACTOR,
    )

    await _cancel_run(services, run.id)
    canceled_run, counts = await services.experiment_run_service.get_run(
        run.id, actor=ACTOR
    )
    assert canceled_run.status is ExperimentRunStatus.CANCELED
    assert counts.canceled == 2
    assert counts.non_settled == 0


async def test_marking_an_already_canceling_run_is_a_no_op(
    services: ReplayServices,
) -> None:
    """Re-marking a canceling run leaves it alone so a retry can finish phase two."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    session = await _baseline_session(services, agent_version)
    cohort_version = await _cohort_version(
        services, agent_version.agent_id, [session.id]
    )
    run, _ = await services.experiment_service.start_run(
        experiment_id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version.id, agent_version_id=agent_version.id
        ),
        actor=ACTOR,
    )

    await services.experiment_run_service.mark_run_canceling(run.id, actor=ACTOR)
    await services.experiment_run_service.mark_run_canceling(run.id, actor=ACTOR)

    marked, _ = await services.experiment_run_service.get_run(run.id, actor=ACTOR)
    assert marked.status is ExperimentRunStatus.CANCELING
    await _cancel_run(services, run.id)
    canceled_run, _ = await services.experiment_run_service.get_run(run.id, actor=ACTOR)
    assert canceled_run.status is ExperimentRunStatus.CANCELED


async def test_run_cancel_in_flight_task_keeps_status_until_it_terminates(
    services: ReplayServices,
) -> None:
    """A claimed task keeps its status through cancel and the run finalizes later."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    session = await _baseline_session(services, agent_version)
    cohort_version = await _cohort_version(
        services, agent_version.agent_id, [session.id]
    )
    run, _ = await services.experiment_service.start_run(
        experiment_id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version.id, agent_version_id=agent_version.id
        ),
        actor=ACTOR,
    )
    bundles, _ = await services.replay_service.list_replays(
        ReplayFilter(
            expression=FilterCondition(
                field="experiment_run_id", op=FilterOp.EQ, value=run.id
            )
        ),
        actor=ACTOR,
    )
    replay = bundles[0].replay
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )

    await _cancel_run(services, run.id)
    canceling_run, _ = await services.experiment_run_service.get_run(
        run.id, actor=ACTOR
    )
    assert canceling_run.status is ExperimentRunStatus.CANCELING

    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=replay.job_id), actor=ACTOR
    )
    agent_task = tasks[0]
    assert agent_task.status is TaskStatus.CLAIMED
    assert agent_task.cancel_requested_at is not None

    await services.task_service.update_task(
        agent_task.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, agent_task.id, 1, worker.id),
    )
    await services.task_service.update_task(
        agent_task.id,
        TaskUpdate(status=TaskStatus.CANCELED),
        actor=build_task_actor(ACTOR.account, agent_task.id, 1, worker.id),
    )

    final_run, counts = await services.experiment_run_service.get_run(
        run.id, actor=ACTOR
    )
    assert final_run.status is ExperimentRunStatus.CANCELED
    assert counts.canceled == 1


async def test_finalize_precedence_canceling_beats_failure(
    services: ReplayServices,
) -> None:
    """Cancellation wins over a failed replay when the run was canceling."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    sessions = [await _baseline_session(services, agent_version) for _ in range(2)]
    cohort_version = await _cohort_version(
        services, agent_version.agent_id, [session.id for session in sessions]
    )
    run, _ = await services.experiment_service.start_run(
        experiment_id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version.id, agent_version_id=agent_version.id
        ),
        actor=ACTOR,
    )
    bundles, _ = await services.replay_service.list_replays(
        ReplayFilter(
            expression=FilterCondition(
                field="experiment_run_id", op=FilterOp.EQ, value=run.id
            )
        ),
        actor=ACTOR,
    )
    replay_a, replay_b = bundles[0].replay, bundles[1].replay

    worker = await create_worker(services.workers, ACTOR.account.id)
    # Claim both agent tasks so cancel_run only stamps cancel_requested_at,
    # leaving their terminal status to this test's simulated transitions.
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )

    await _cancel_run(services, run.id)

    tasks_a, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=replay_a.job_id), actor=ACTOR
    )
    task_a = tasks_a[0]
    tasks_b, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=replay_b.job_id), actor=ACTOR
    )
    task_b = tasks_b[0]

    await services.task_service.update_task(
        task_a.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, task_a.id, 1, worker.id),
    )
    await services.task_service.update_task(
        task_a.id,
        TaskUpdate(status=TaskStatus.FAILED, error="crashed"),
        actor=build_task_actor(ACTOR.account, task_a.id, 1, worker.id),
    )
    await services.task_service.update_task(
        task_b.id,
        TaskUpdate(status=TaskStatus.RUNNING),
        actor=build_task_actor(ACTOR.account, task_b.id, 1, worker.id),
    )
    await services.task_service.update_task(
        task_b.id,
        TaskUpdate(status=TaskStatus.CANCELED),
        actor=build_task_actor(ACTOR.account, task_b.id, 1, worker.id),
    )

    final_run, counts = await services.experiment_run_service.get_run(
        run.id, actor=ACTOR
    )
    assert final_run.status is ExperimentRunStatus.CANCELED
    assert counts.failed == 1
    assert counts.canceled == 1


async def test_function_mode_run_fan_out_mixed_settlement_and_cancel(
    services: ReplayServices,
) -> None:
    """A function-mode run settles per replay and cancellation drains the rest."""
    agent_version = await _function_agent_version(services)
    experiment, _ = await services.experiment_service.create_experiment(
        ExperimentCreate(name="exp1", agent_id=agent_version.agent_id, evaluators=[]),
        actor=ACTOR,
    )
    sessions = [await _baseline_session(services, agent_version) for _ in range(3)]
    cohort_version = await _cohort_version(
        services, agent_version.agent_id, [session.id for session in sessions]
    )
    run, _ = await services.experiment_service.start_run(
        experiment.id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version.id, agent_version_id=agent_version.id
        ),
        actor=ACTOR,
    )
    bundles, _ = await services.replay_service.list_replays(
        ReplayFilter(
            expression=FilterCondition(
                field="experiment_run_id", op=FilterOp.EQ, value=run.id
            )
        ),
        actor=ACTOR,
    )
    completed_replay, failed_replay, open_replay = (bundle.replay for bundle in bundles)
    for bundle in bundles:
        job = await services.jobs.get(bundle.replay.job_id)
        assert job.provisional is True

    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    _, completed_placeholder = await _run_function_task_to_a_pending_import_placeholder(
        services, completed_replay.job_id, "ext-completed", worker.id
    )
    _, failed_placeholder = await _run_function_task_to_a_pending_import_placeholder(
        services, failed_replay.job_id, "ext-failed", worker.id
    )
    await _run_function_task_to_a_pending_import_placeholder(
        services, open_replay.job_id, "ext-open", worker.id
    )

    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-completed", worker.id
    )
    assert adopted.id == completed_placeholder.id
    await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )
    await services.session_service.update_session(
        failed_placeholder.id,
        SessionUpdate(status=SessionStatus.FAILED, error="import failed"),
        actor=ACTOR,
    )

    mid_run, mid_counts = await services.experiment_run_service.get_run(
        run.id, actor=ACTOR
    )
    assert mid_run.status is ExperimentRunStatus.RUNNING
    assert mid_counts.completed == 1
    assert mid_counts.failed == 1
    assert mid_counts.non_settled == 1

    await _cancel_run(services, run.id)

    final_run, counts = await services.experiment_run_service.get_run(
        run.id, actor=ACTOR
    )
    assert final_run.status is ExperimentRunStatus.CANCELED
    assert counts.completed == 1
    assert counts.failed == 1
    assert counts.canceled == 1
    open_job = await services.jobs.get(open_replay.job_id)
    assert open_job.status is JobStatus.CANCELED


async def test_function_mode_evaluate_baselines_shares_the_provisional_job(
    services: ReplayServices,
) -> None:
    """Baseline evaluator tasks land in the function replay's provisional job."""
    agent_version = await _function_agent_version(services)
    evaluator = await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
            evaluate_baselines=True,
        ),
        actor=ACTOR,
    )

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.provisional is True
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert len(tasks) == 2
    baseline_tasks = [task for task in tasks if isinstance(task, EvaluationTask)]
    assert len(baseline_tasks) == 1
    assert baseline_tasks[0].input_session_id == baseline.id
    assert baseline_tasks[0].plugin_version_id == evaluator.id
    assert baseline_tasks[0].on_failure is TaskOnFailure.ABORT


async def test_baseline_evaluator_failure_fails_the_unimported_function_job(
    services: ReplayServices,
) -> None:
    """A baseline evaluator failure fails the job and a later import is a no-op."""
    agent_version = await _function_agent_version(services)
    await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
            evaluate_baselines=True,
        ),
        actor=ACTOR,
    )
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    _, placeholder = await _run_function_task_to_a_pending_import_placeholder(
        services, bundle.replay.job_id, "ext-1", worker.id
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    baseline_task = next(task for task in tasks if isinstance(task, EvaluationTask))
    baseline_actor = build_task_actor(ACTOR.account, baseline_task.id, 1, worker.id)
    await services.task_service.update_task(
        baseline_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=baseline_actor
    )
    await services.task_service.update_task(
        baseline_task.id,
        TaskUpdate(status=TaskStatus.FAILED, error="scoring failed"),
        actor=baseline_actor,
    )

    job_after = await services.jobs.get(bundle.replay.job_id)
    assert job_after.status is JobStatus.FAILED
    assert job_after.error == "scoring failed"
    replay_after = await services.replays.get(bundle.replay.id)
    assert replay_after.status is ReplayStatus.FAILED

    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    assert adopted.id == placeholder.id
    finalized = await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )
    assert finalized.status is SessionStatus.COMPLETED

    job_unchanged = await services.jobs.get(bundle.replay.job_id)
    assert job_unchanged.status is JobStatus.FAILED
    assert job_unchanged.error == "scoring failed"
    replay_unchanged = await services.replays.get(bundle.replay.id)
    assert replay_unchanged.status is ReplayStatus.FAILED
    tasks_after, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    eval_tasks = [task for task in tasks_after if isinstance(task, EvaluationTask)]
    assert [task.id for task in eval_tasks] == [baseline_task.id]


async def test_import_finalizes_while_a_baseline_evaluator_runs(
    services: ReplayServices,
) -> None:
    """Import finalization appends result evaluators alongside a live baseline one."""
    agent_version = await _function_agent_version(services)
    await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
            evaluate_baselines=True,
        ),
        actor=ACTOR,
    )
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    _, placeholder = await _run_function_task_to_a_pending_import_placeholder(
        services, bundle.replay.job_id, "ext-1", worker.id
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    baseline_task = next(task for task in tasks if isinstance(task, EvaluationTask))
    baseline_actor = build_task_actor(ACTOR.account, baseline_task.id, 1, worker.id)
    await services.task_service.update_task(
        baseline_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=baseline_actor
    )

    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    assert adopted.id == placeholder.id
    await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )

    job_after_finalize = await services.jobs.get(bundle.replay.job_id)
    assert job_after_finalize.provisional is False
    assert not job_after_finalize.settled
    replay_after_finalize = await services.replays.get(bundle.replay.id)
    assert replay_after_finalize.status is ReplayStatus.EVALUATING
    tasks_after_finalize, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    result_task = next(
        task
        for task in tasks_after_finalize
        if isinstance(task, EvaluationTask) and task.id != baseline_task.id
    )
    assert result_task.input_session_id == placeholder.id
    assert result_task.status is TaskStatus.PENDING

    await services.task_service.update_task(
        baseline_task.id,
        TaskUpdate(
            status=TaskStatus.COMPLETED,
            result=[{"name": "accuracy", "score": 0.5}],
        ),
        actor=baseline_actor,
    )
    job_mid = await services.jobs.get(bundle.replay.job_id)
    assert not job_mid.settled

    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    result_actor = build_task_actor(ACTOR.account, result_task.id, 1, worker.id)
    await services.task_service.update_task(
        result_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=result_actor
    )
    await services.task_service.update_task(
        result_task.id,
        TaskUpdate(
            status=TaskStatus.COMPLETED,
            result=[{"name": "accuracy", "score": 1.0}],
        ),
        actor=result_actor,
    )

    job_final = await services.jobs.get(bundle.replay.job_id)
    assert job_final.status is JobStatus.COMPLETED
    replay_final = await services.replays.get(bundle.replay.id)
    assert replay_final.status is ReplayStatus.COMPLETED


async def test_function_mode_cancel_while_the_agent_task_is_pending(
    services: ReplayServices,
) -> None:
    """Canceling before the claim settles the provisional job canceled."""
    agent_version = await _function_agent_version(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )

    await services.job_service.cancel_job(bundle.replay.job_id, actor=ACTOR)

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.status is JobStatus.CANCELED
    replay = await services.replays.get(bundle.replay.id)
    assert replay.status is ReplayStatus.CANCELED
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert tasks[0].status is TaskStatus.CANCELED


async def test_function_mode_cancel_while_the_agent_task_is_running(
    services: ReplayServices,
) -> None:
    """A cancel during the run settles the job once the worker reports canceled."""
    agent_version = await _function_agent_version(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    function_task = tasks[0]
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    function_actor = build_task_actor(ACTOR.account, function_task.id, 1, worker.id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=function_actor
    )

    await services.job_service.cancel_job(bundle.replay.job_id, actor=ACTOR)

    job_mid = await services.jobs.get(bundle.replay.job_id)
    assert not job_mid.settled
    stored_task = await services.tasks.get(function_task.id)
    assert stored_task.status is TaskStatus.RUNNING
    assert stored_task.cancel_requested_at is not None

    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.CANCELED), actor=function_actor
    )

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.status is JobStatus.CANCELED
    replay = await services.replays.get(bundle.replay.id)
    assert replay.status is ReplayStatus.CANCELED


async def test_function_mode_cancel_between_placeholder_creation_and_completion(
    services: ReplayServices,
) -> None:
    """A cancel racing the completion settles canceled and keeps the placeholder."""
    agent_version = await _function_agent_version(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    function_task = tasks[0]
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    function_actor = build_task_actor(ACTOR.account, function_task.id, 1, worker.id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=function_actor
    )
    placeholder = await services.session_service.create_session(
        SessionCreate(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id="ext-1",
        ),
        actor=function_actor,
    )

    await services.job_service.cancel_job(bundle.replay.job_id, actor=ACTOR)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.COMPLETED), actor=function_actor
    )

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.status is JobStatus.CANCELED
    replay = await services.replays.get(bundle.replay.id)
    assert replay.status is ReplayStatus.CANCELED
    stored_placeholder = await services.sessions.get(placeholder.id)
    assert stored_placeholder.status is SessionStatus.PENDING_IMPORT


async def test_function_mode_cancel_while_awaiting_import_then_import_finalizes(
    services: ReplayServices,
) -> None:
    """Canceling a drained job settles it and a later import finalizes plainly."""
    agent_version = await _function_agent_version(services)
    await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    _, placeholder = await _run_function_task_to_a_pending_import_placeholder(
        services, bundle.replay.job_id, "ext-1", worker.id
    )
    job_open = await services.jobs.get(bundle.replay.job_id)
    assert not job_open.settled

    await services.job_service.cancel_job(bundle.replay.job_id, actor=ACTOR)

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.status is JobStatus.CANCELED
    replay = await services.replays.get(bundle.replay.id)
    assert replay.status is ReplayStatus.CANCELED

    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    assert adopted.id == placeholder.id
    finalized = await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )
    assert finalized.status is SessionStatus.COMPLETED

    job_unchanged = await services.jobs.get(bundle.replay.job_id)
    assert job_unchanged.status is JobStatus.CANCELED
    replay_unchanged = await services.replays.get(bundle.replay.id)
    assert replay_unchanged.status is ReplayStatus.CANCELED
    tasks_after, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert not any(isinstance(task, EvaluationTask) for task in tasks_after)


async def test_function_mode_cancel_while_evaluating(
    services: ReplayServices,
) -> None:
    """A cancel during evaluation settles the finalized job canceled."""
    agent_version = await _function_agent_version(services)
    await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    _, placeholder = await _run_function_task_to_a_pending_import_placeholder(
        services, bundle.replay.job_id, "ext-1", worker.id
    )
    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    assert adopted.id == placeholder.id
    await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )
    replay_evaluating = await services.replays.get(bundle.replay.id)
    assert replay_evaluating.status is ReplayStatus.EVALUATING

    await services.job_service.cancel_job(bundle.replay.job_id, actor=ACTOR)

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.status is JobStatus.CANCELED
    replay = await services.replays.get(bundle.replay.id)
    assert replay.status is ReplayStatus.CANCELED
    tasks_after, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    eval_task = next(task for task in tasks_after if isinstance(task, EvaluationTask))
    assert eval_task.status is TaskStatus.CANCELED


async def test_function_task_timeout_after_placeholder_creation_fails_the_job(
    services: ReplayServices,
) -> None:
    """A timed-out function task fails the job and a later import is a no-op."""
    agent_version = await _function_agent_version(services)
    await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    function_task = tasks[0]
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    function_actor = build_task_actor(ACTOR.account, function_task.id, 1, worker.id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=function_actor
    )
    placeholder = await services.session_service.create_session(
        SessionCreate(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id="ext-1",
        ),
        actor=function_actor,
    )

    await services.task_service.update_task(
        function_task.id,
        TaskUpdate(status=TaskStatus.TIMED_OUT, error="deadline exceeded"),
        actor=function_actor,
    )

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.status is JobStatus.FAILED
    assert job.error == "deadline exceeded"
    replay = await services.replays.get(bundle.replay.id)
    assert replay.status is ReplayStatus.FAILED

    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    assert adopted.id == placeholder.id
    finalized = await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )
    assert finalized.status is SessionStatus.COMPLETED

    job_unchanged = await services.jobs.get(bundle.replay.job_id)
    assert job_unchanged.status is JobStatus.FAILED
    tasks_after, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert not any(isinstance(task, EvaluationTask) for task in tasks_after)


async def test_requeued_function_task_reclaims_its_placeholder(
    services: ReplayServices,
) -> None:
    """A requeued attempt keeps its placeholder and reclaims it by external id."""
    agent_version = await _function_agent_version(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    function_task = tasks[0]
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    first_actor = build_task_actor(ACTOR.account, function_task.id, 1, worker.id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=first_actor
    )
    placeholder = await services.session_service.create_session(
        SessionCreate(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id="ext-1",
        ),
        actor=first_actor,
    )

    stored = await services.tasks.get(function_task.id)
    stored.claimed_at = datetime.now(UTC) - timedelta(hours=1)
    await services.tasks.update(stored)
    now = datetime.now(UTC)
    assert await services.task_service.list_stale_task_ids(now) == [function_task.id]
    await services.task_service.sweep_stale_task(function_task.id, now)

    requeued = await services.tasks.get(function_task.id)
    assert requeued.status is TaskStatus.PENDING
    assert requeued.result_session_id == placeholder.id
    kept = await services.sessions.get(placeholder.id)
    assert kept.task_id == function_task.id
    assert kept.status is SessionStatus.PENDING_IMPORT

    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    second_actor = build_task_actor(ACTOR.account, function_task.id, 2, worker.id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=second_actor
    )

    reclaimed = await services.session_service.create_session(
        SessionCreate(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id="ext-1",
        ),
        actor=second_actor,
    )
    assert reclaimed.id == placeholder.id

    with pytest.raises(TaskResultSessionAlreadyLinked):
        await services.session_service.create_session(
            SessionCreate(
                origin=SessionOrigin.REPLAY,
                status=SessionStatus.PENDING_IMPORT,
                external_id="ext-2",
            ),
            actor=second_actor,
        )

    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.COMPLETED), actor=second_actor
    )
    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    assert adopted.id == placeholder.id
    await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.status is JobStatus.COMPLETED
    replay = await services.replays.get(bundle.replay.id)
    assert replay.status is ReplayStatus.COMPLETED


async def test_retry_reclaims_a_placeholder_finalized_meanwhile(
    services: ReplayServices,
) -> None:
    """A retry whose placeholder finalized mid-flight completes against it."""
    agent_version = await _function_agent_version(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    function_task = tasks[0]
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    first_actor = build_task_actor(ACTOR.account, function_task.id, 1, worker.id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=first_actor
    )
    placeholder = await services.session_service.create_session(
        SessionCreate(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id="ext-1",
        ),
        actor=first_actor,
    )

    stored = await services.tasks.get(function_task.id)
    stored.claimed_at = datetime.now(UTC) - timedelta(hours=1)
    await services.tasks.update(stored)
    now = datetime.now(UTC)
    await services.task_service.sweep_stale_task(function_task.id, now)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    second_actor = build_task_actor(ACTOR.account, function_task.id, 2, worker.id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=second_actor
    )

    adopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    assert adopted.id == placeholder.id
    await services.session_service.update_session(
        adopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )

    reclaimed = await services.session_service.create_session(
        SessionCreate(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id="ext-1",
        ),
        actor=second_actor,
    )
    assert reclaimed.id == placeholder.id
    assert reclaimed.status is SessionStatus.COMPLETED
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.COMPLETED), actor=second_actor
    )

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.status is JobStatus.COMPLETED
    replay = await services.replays.get(bundle.replay.id)
    assert replay.status is ReplayStatus.COMPLETED


async def test_requeued_command_task_discards_its_recording_and_links_fresh(
    services: ReplayServices,
) -> None:
    """A command-mode requeue severs both link directions and attempt 2 links fresh."""
    agent_version = await _agent_version_with_run_spec(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    command_task = tasks[0]
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    first_actor = build_task_actor(ACTOR.account, command_task.id, 1, worker.id)
    await services.task_service.update_task(
        command_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=first_actor
    )
    recording = await services.session_service.create_session(
        SessionCreate(origin=SessionOrigin.REPLAY),
        actor=first_actor,
    )
    assert recording.status is SessionStatus.IN_PROGRESS

    stored = await services.tasks.get(command_task.id)
    stored.claimed_at = datetime.now(UTC) - timedelta(hours=1)
    await services.tasks.update(stored)
    now = datetime.now(UTC)
    assert await services.task_service.list_stale_task_ids(now) == [command_task.id]
    await services.task_service.sweep_stale_task(command_task.id, now)

    requeued = await services.tasks.get(command_task.id)
    assert requeued.status is TaskStatus.PENDING
    assert requeued.result_session_id is None
    discarded = await services.sessions.get(recording.id)
    assert discarded.task_id is None

    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    second_actor = build_task_actor(ACTOR.account, command_task.id, 2, worker.id)
    await services.task_service.update_task(
        command_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=second_actor
    )
    fresh = await services.session_service.create_session(
        SessionCreate(origin=SessionOrigin.REPLAY),
        actor=second_actor,
    )
    assert fresh.id != recording.id
    relinked = await services.tasks.get(command_task.id)
    assert relinked.result_session_id == fresh.id


async def test_canceled_replay_retry_still_reclaims_its_placeholder(
    services: ReplayServices,
) -> None:
    """A cancel during a running retry leaves the placeholder reclaim intact."""
    agent_version = await _function_agent_version(services)
    await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
        ),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    function_task = tasks[0]
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    first_actor = build_task_actor(ACTOR.account, function_task.id, 1, worker.id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=first_actor
    )
    placeholder = await services.session_service.create_session(
        SessionCreate(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id="ext-1",
        ),
        actor=first_actor,
    )

    stored = await services.tasks.get(function_task.id)
    stored.claimed_at = datetime.now(UTC) - timedelta(hours=1)
    await services.tasks.update(stored)
    await services.task_service.sweep_stale_task(function_task.id, datetime.now(UTC))
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    second_actor = build_task_actor(ACTOR.account, function_task.id, 2, worker.id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=second_actor
    )

    await services.job_service.cancel_job(bundle.replay.job_id, actor=ACTOR)

    reclaimed = await services.session_service.create_session(
        SessionCreate(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id="ext-1",
        ),
        actor=second_actor,
    )
    assert reclaimed.id == placeholder.id

    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.CANCELED), actor=second_actor
    )

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.status is JobStatus.CANCELED
    replay = await services.replays.get(bundle.replay.id)
    assert replay.status is ReplayStatus.CANCELED
    kept = await services.sessions.get(placeholder.id)
    assert kept.status is SessionStatus.PENDING_IMPORT
    assert kept.task_id == function_task.id
    tasks_after, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert not any(isinstance(task, EvaluationTask) for task in tasks_after)


async def test_import_arriving_before_the_placeholder_reimports_to_complete(
    services: ReplayServices,
) -> None:
    """An import landing before the placeholder exists resolves on a re-import."""
    agent_version = await _function_agent_version(services)
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    function_task = tasks[0]
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        10, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    function_actor = build_task_actor(ACTOR.account, function_task.id, 1, worker.id)
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=function_actor
    )

    early = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    await services.session_service.update_session(
        early.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )

    placeholder = await services.session_service.create_session(
        SessionCreate(
            origin=SessionOrigin.REPLAY,
            status=SessionStatus.PENDING_IMPORT,
            external_id="ext-1",
        ),
        actor=function_actor,
    )
    assert placeholder.id != early.id
    await services.task_service.update_task(
        function_task.id, TaskUpdate(status=TaskStatus.COMPLETED), actor=function_actor
    )

    job_waiting = await services.jobs.get(bundle.replay.job_id)
    assert job_waiting.provisional is True
    assert not job_waiting.settled

    readopted = await _adopt_via_import_task(
        services, agent_version.agent_id, "ext-1", worker.id
    )
    assert readopted.id == placeholder.id
    await services.session_service.update_session(
        readopted.id, SessionUpdate(status=SessionStatus.COMPLETED), actor=ACTOR
    )

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.status is JobStatus.COMPLETED
    replay = await services.replays.get(bundle.replay.id)
    assert replay.status is ReplayStatus.COMPLETED


async def test_two_standalone_function_replays_of_one_baseline_coexist(
    services: ReplayServices,
) -> None:
    """A second standalone replay of one baseline is not a duplicate."""
    agent_version = await _function_agent_version(services)
    baseline = await _baseline_session(services, agent_version)

    first = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )
    second = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )

    assert first.replay.id != second.replay.id
    assert first.replay.job_id != second.replay.job_id
    for bundle in (first, second):
        job = await services.jobs.get(bundle.replay.job_id)
        assert job.provisional is True


async def test_function_mode_replay_of_an_imported_baseline(
    services: ReplayServices,
) -> None:
    """An imported session replays through a function-mode agent version."""
    agent_version = await _function_agent_version(services)
    baseline = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=agent_version.agent_id,
        origin=SessionOrigin.IMPORTED,
        status=SessionStatus.COMPLETED,
        inputs={"q": "hi"},
        imported_from="langfuse",
        external_id="trace-1",
    )

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            agent_version_id=agent_version.id,
            evaluators=[],
        ),
        actor=ACTOR,
    )

    job = await services.jobs.get(bundle.replay.job_id)
    assert job.provisional is True
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert len(tasks) == 1
    agent_task = tasks[0]
    assert isinstance(agent_task, AgentTask)
    assert agent_task.inputs == baseline.inputs


async def test_create_replay_rejects_a_pending_import_baseline(
    services: ReplayServices,
) -> None:
    """A baseline still pending import rejects the replay."""
    agent_version = await _function_agent_version(services)
    baseline = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=agent_version.agent_id,
        agent_version_id=agent_version.id,
        origin=SessionOrigin.REPLAY,
        status=SessionStatus.PENDING_IMPORT,
        external_id="ext-base",
    )
    with pytest.raises(ReplayBaselineNotTerminal):
        await services.replay_service.create_replay(
            ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
            actor=ACTOR,
        )


async def test_create_replay_rejects_an_in_progress_baseline(
    services: ReplayServices,
) -> None:
    """A baseline still in progress rejects the replay."""
    agent_version = await _function_agent_version(services)
    baseline = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=agent_version.agent_id,
        agent_version_id=agent_version.id,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.IN_PROGRESS,
    )
    with pytest.raises(ReplayBaselineNotTerminal):
        await services.replay_service.create_replay(
            ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
            actor=ACTOR,
        )


async def test_create_replay_accepts_a_failed_baseline(
    services: ReplayServices,
) -> None:
    """A failed baseline is terminal and replays normally."""
    agent_version = await _function_agent_version(services)
    baseline = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=agent_version.agent_id,
        agent_version_id=agent_version.id,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.FAILED,
        error="boom",
    )
    bundle = await services.replay_service.create_replay(
        ReplayCreate(baseline_session_id=baseline.id, evaluators=[]),
        actor=ACTOR,
    )
    assert bundle.replay.baseline_session_id == baseline.id


async def test_start_run_rejects_a_cohort_containing_a_non_terminal_session(
    services: ReplayServices,
) -> None:
    """A non-terminal cohort session aborts the run before anything is created."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    terminal_session = await _baseline_session(services, agent_version)
    non_terminal_session = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=agent_version.agent_id,
        agent_version_id=agent_version.id,
        origin=SessionOrigin.RECORDED,
        status=SessionStatus.IN_PROGRESS,
        inputs={"q": "hi"},
    )
    cohort_version = await _cohort_version(
        services,
        agent_version.agent_id,
        [terminal_session.id, non_terminal_session.id],
    )

    with pytest.raises(ReplayBaselineNotTerminal):
        await services.experiment_service.start_run(
            experiment_id,
            ExperimentRunCreate(
                cohort_version_id=cohort_version.id, agent_version_id=agent_version.id
            ),
            actor=ACTOR,
        )

    replays, _ = await services.replay_service.list_replays(ReplayFilter(), actor=ACTOR)
    assert replays == []
    jobs, _ = await services.job_service.list_jobs(JobFilter(), actor=ACTOR)
    assert jobs == []
    tasks, _ = await services.task_service.list_tasks(TaskFilter(), actor=ACTOR)
    assert tasks == []
