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

import pytest

from conftest import (
    ReplayServices,
    build_replay_services,
    build_task_actor,
    build_worker_actor,
    create_agent,
    create_agent_version,
    create_blob,
    create_cohort,
    create_cohort_version,
    create_evaluation_task,
    create_job,
    create_plugin,
    create_replay,
    create_session,
    create_worker,
    get_replay_job_id,
)
from kitaru.api_models.v1.experiment_run import ExperimentRunStatus
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.replay import BaselineEvaluationMode, ReplayStatus
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.task import TaskOnFailure, TaskStatus
from kitaru.server.application.models.auth import (
    AuthContext,
)
from kitaru.server.application.models.evaluation import EvaluationFilter
from kitaru.server.application.models.experiment import ExperimentCreate
from kitaru.server.application.models.experiment_run import ExperimentRunCreate
from kitaru.server.application.models.replay import ReplayCreate, ReplayFilter
from kitaru.server.application.models.replay_config import EvaluatorConfigInput
from kitaru.server.application.models.task import TaskFilter, TaskUpdate
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import (
    AgentVersion,
    AgentVersionAgentMismatch,
    RunSpec,
)
from kitaru.server.domain.base import ValidationError
from kitaru.server.domain.cohort_version import CohortVersion, CohortVersionIdNotFound
from kitaru.server.domain.plugin import PluginKind, PluginVersion, ScriptPluginSource
from kitaru.server.domain.replay import DuplicateReplayForBaseline
from kitaru.server.domain.session import Session, SessionNotEvaluatable
from kitaru.server.domain.task import AgentTask, AgentTaskDetails, EvaluationTask
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
        run_spec=RunSpec(command="run.sh", timeout_seconds=60),
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
    assert bundle.replay.result_session_id is None

    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundle.replay.job_id), actor=ACTOR
    )
    assert len(tasks) == 1
    agent_task = tasks[0]
    assert isinstance(agent_task, AgentTask)
    assert baseline.inputs is not None
    assert agent_task.inputs == baseline.inputs.value
    assert agent_task.env == {}
    assert agent_task.labels == {"agent_version": str(agent_version.id)}
    assert agent_task.on_failure is TaskOnFailure.ABORT

    spec = await services.task_service.get_spec(agent_task.id, actor=ACTOR)
    assert isinstance(spec.details, AgentTaskDetails)
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
    replay_with_task = await services.replays.get_by_job_id(
        get_replay_job_id(bundle.replay)
    )
    assert replay_with_task is not None
    replay_with_task.link_result_session(result_session.id)
    await services.replays.update(replay_with_task)

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

    job_after = await services.jobs.get(get_replay_job_id(bundle.replay))
    assert job_after.status is JobStatus.COMPLETED

    replay_final = await services.replays.get(bundle.replay.id)
    assert replay_final.status is ReplayStatus.COMPLETED

    final_bundle = await services.replay_service.get_replay(
        bundle.replay.id, actor=ACTOR
    )
    assert final_bundle.replay.result_session_id == result_session.id


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
    job = await services.jobs.get(get_replay_job_id(bundle.replay))
    assert job.kind is JobKind.REPLAY


async def test_if_missing_skips_already_scored_pairs(
    services: ReplayServices,
) -> None:
    """IF_MISSING baseline evaluator tasks skip already-scored evaluator versions."""
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
            baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
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


async def test_if_missing_with_different_params_does_not_adopt(
    services: ReplayServices,
) -> None:
    """IF_MISSING does not adopt a prior evaluation run with different params."""
    agent_version = await _agent_version_with_run_spec(services)
    evaluator = await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    prior_job = await create_job(services.jobs, ACTOR.account.id)
    prior_task = await create_evaluation_task(
        services.tasks,
        prior_job.id,
        plugin_version_id=evaluator.id,
        input_session_id=baseline.id,
        params={"threshold": 0.5},
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
                EvaluatorConfigInput(evaluator="accuracy", params={"threshold": 0.9}),
            ],
            baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
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
    assert baseline_tasks[0].plugin_version_id == evaluator.id
    assert baseline_tasks[0].params == {"threshold": 0.9}


async def test_force_scores_already_scored_pairs(services: ReplayServices) -> None:
    """FORCE creates a baseline evaluator task even for an already-scored pair."""
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
            baseline_evaluation_mode=BaselineEvaluationMode.FORCE,
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
    assert {task.plugin_version_id for task in baseline_tasks} == {
        evaluator_a.id,
        evaluator_b.id,
    }


async def test_none_creates_no_baseline_evaluation_tasks(
    services: ReplayServices,
) -> None:
    """NONE appends no baseline evaluator task."""
    agent_version = await _agent_version_with_run_spec(services)
    await _evaluator_version(services, "accuracy")
    baseline = await _baseline_session(services, agent_version)

    bundle = await services.replay_service.create_replay(
        ReplayCreate(
            baseline_session_id=baseline.id,
            evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
            baseline_evaluation_mode=BaselineEvaluationMode.NONE,
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
    assert baseline_tasks == []


async def test_create_replay_rejects_in_progress_baseline_evaluation(
    services: ReplayServices,
) -> None:
    """Reject scoring a baseline session that is in progress."""
    agent_version = await _agent_version_with_run_spec(services)
    await _evaluator_version(services, "accuracy")
    baseline = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=agent_version.agent_id,
        agent_version_id=agent_version.id,
        origin=SessionOrigin.RECORDED,
        inputs={"q": "hi"},
    )
    with pytest.raises(SessionNotEvaluatable):
        await services.replay_service.create_replay(
            ReplayCreate(
                baseline_session_id=baseline.id,
                evaluators=[EvaluatorConfigInput(evaluator="accuracy")],
                baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
            ),
            actor=ACTOR,
        )


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
            baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
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

    job_after = await services.jobs.get(get_replay_job_id(bundle.replay))
    assert job_after.cancel_requested_at is not None
    await services.task_service.propagate_job_cancel(get_replay_job_id(bundle.replay))

    baseline_after = await services.tasks.get(baseline_task.id)
    assert baseline_after.status is TaskStatus.CLAIMED
    assert baseline_after.cancel_requested_at is not None

    await services.task_service.update_task(
        baseline_task.id,
        TaskUpdate(status=TaskStatus.CANCELED),
        actor=build_task_actor(ACTOR.account, baseline_task.id, 1, worker.id),
    )

    job_after = await services.jobs.get(get_replay_job_id(bundle.replay))
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
            baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
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

    await services.task_service.propagate_job_cancel(get_replay_job_id(bundle.replay))

    agent_after = await services.tasks.get(agent_task.id)
    assert agent_after.cancel_requested_at is not None
    await services.task_service.update_task(
        agent_task.id,
        TaskUpdate(status=TaskStatus.CANCELED),
        actor=build_task_actor(ACTOR.account, agent_task.id, 1, worker.id),
    )

    job_after = await services.jobs.get(get_replay_job_id(bundle.replay))
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
        assert baseline.inputs is not None
        assert agent_task.inputs == baseline.inputs.value
        assert agent_task.env == {}
        assert agent_task.labels == {"agent_version": str(agent_version.id)}
        assert agent_task.on_failure is TaskOnFailure.ABORT

        spec = await services.task_service.get_spec(agent_task.id, actor=ACTOR)
        assert isinstance(spec.details, AgentTaskDetails)
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
        job = await services.jobs.get(get_replay_job_id(bundle.replay))
        assert job.kind is JobKind.REPLAY


async def test_start_run_rejects_in_progress_cohort_session_evaluation(
    services: ReplayServices,
) -> None:
    """Reject scoring a cohort baseline session that is in progress."""
    agent_version = await _agent_version_with_run_spec(services)
    experiment_id, _ = await _create_experiment_with_evaluator(
        services, agent_version.agent_id
    )
    session = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=agent_version.agent_id,
        agent_version_id=agent_version.id,
        origin=SessionOrigin.RECORDED,
        inputs={"q": "hi"},
    )
    cohort_version = await _cohort_version(
        services, agent_version.agent_id, [session.id]
    )
    with pytest.raises(SessionNotEvaluatable):
        await services.experiment_service.start_run(
            experiment_id,
            ExperimentRunCreate(
                cohort_version_id=cohort_version.id,
                agent_version_id=agent_version.id,
                baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
            ),
            actor=ACTOR,
        )


async def test_start_run_if_missing_skips_already_scored_sessions(
    services: ReplayServices,
) -> None:
    """IF_MISSING skips baseline evaluator tasks per already-scored session."""
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
            baseline_evaluation_mode=BaselineEvaluationMode.IF_MISSING,
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


async def test_start_run_force_scores_already_scored_sessions(
    services: ReplayServices,
) -> None:
    """FORCE creates a baseline evaluator task even for an already-scored session."""
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
        services, agent_version.agent_id, [scored_session.id]
    )
    run, _ = await services.experiment_service.start_run(
        experiment.id,
        ExperimentRunCreate(
            cohort_version_id=cohort_version.id,
            agent_version_id=agent_version.id,
            baseline_evaluation_mode=BaselineEvaluationMode.FORCE,
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
    scored_tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundles[0].replay.job_id), actor=ACTOR
    )
    scored_baseline_tasks = [
        task
        for task in scored_tasks
        if isinstance(task, EvaluationTask)
        and task.input_session_id == scored_session.id
    ]
    assert {task.plugin_version_id for task in scored_baseline_tasks} == {
        evaluator_a.id,
        evaluator_b.id,
    }


async def test_start_run_none_creates_no_baseline_evaluation_tasks(
    services: ReplayServices,
) -> None:
    """NONE appends no baseline evaluator task to the fanned-out replays."""
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
            cohort_version_id=cohort_version.id,
            agent_version_id=agent_version.id,
            baseline_evaluation_mode=BaselineEvaluationMode.NONE,
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
    tasks, _ = await services.task_service.list_tasks(
        TaskFilter(job_id=bundles[0].replay.job_id), actor=ACTOR
    )
    baseline_tasks = [
        task
        for task in tasks
        if isinstance(task, EvaluationTask) and task.input_session_id == session.id
    ]
    assert baseline_tasks == []


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
        run_spec=RunSpec(command="run.sh", timeout_seconds=60),
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
