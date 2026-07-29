"""Replay job creation and event-driven advancement."""

import uuid

from kitaru.server.application.events import (
    EventRegistry,
    JobSettled,
    ReplaySettled,
    TaskTerminal,
)
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.services.job_service import JobService
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.job import JobStatus
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import (
    EvaluatorConfig,
    ReplayConfig,
    ReplayOverride,
    ToolPolicy,
    effective_inputs,
)
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import AgentTask, EvaluationTask, TaskStatus


async def create_replay_pipeline(
    *,
    owner_id: uuid.UUID,
    baseline_session: Session,
    agent_version: AgentVersion,
    evaluators: list[EvaluatorConfig],
    evaluate_baselines: bool,
    job_service: JobService,
    replay_repository: ReplayRepository,
    task_repository: TaskRepository,
    override: ReplayOverride | None = None,
    tool_policy: ToolPolicy | None = None,
    experiment_run_id: uuid.UUID | None = None,
) -> tuple[Replay, ReplayConfig]:
    """Create a replay, its generic job, and all immediately runnable tasks."""
    config = ReplayConfig(
        owner_id=owner_id,
        override=override,
        tool_policy=tool_policy or ToolPolicy(),
        evaluators=evaluators,
    )
    if experiment_run_id is None:
        config.check_standalone()
    job = await job_service.create_job(owner_id)
    replay = Replay(
        owner_id=owner_id,
        job_id=job.id,
        experiment_run_id=experiment_run_id,
        replay_config_id=config.id,
        baseline_session_id=baseline_session.id,
        evaluate_baselines=evaluate_baselines,
    )
    replay, config = await replay_repository.create(replay, config)
    await job_service.add_task(
        job.id,
        AgentTask(
            job_id=job.id,
            agent_version_id=agent_version.id,
            inputs=effective_inputs(baseline_session.inputs, override),
            labels={"agent_version": str(agent_version.id)},
            env={"KITARU_REPLAY_ID": str(replay.id)},
        ),
    )
    if evaluate_baselines:
        for evaluator in evaluators:
            assert evaluator.evaluator_version_id is not None
            if await task_repository.completed_evaluator_exists(
                baseline_session.id, evaluator.evaluator_version_id
            ):
                continue
            await job_service.add_task(
                job.id,
                EvaluationTask(
                    job_id=job.id,
                    plugin_version_id=evaluator.evaluator_version_id,
                    input_session_id=baseline_session.id,
                    params=evaluator.params,
                ),
            )
    return replay, config


async def append_result_evaluations(
    event: TaskTerminal,
    replay_repository: ReplayRepository,
    job_service: JobService,
) -> None:
    """Append result evaluator tasks after a replay agent completes."""
    task = event.task
    if not isinstance(task, AgentTask) or task.status is not TaskStatus.COMPLETED:
        return
    replay = await replay_repository.get_by_job(task.job_id)
    if replay is None:
        return
    assert task.result_session_id is not None
    config = await replay_repository.get_config(replay.replay_config_id)
    for evaluator in config.evaluators:
        assert evaluator.evaluator_version_id is not None
        await job_service.add_task(
            task.job_id,
            EvaluationTask(
                job_id=task.job_id,
                plugin_version_id=evaluator.evaluator_version_id,
                input_session_id=task.result_session_id,
                params=evaluator.params,
            ),
        )
    replay.start_evaluating()
    await replay_repository.update(replay)


async def settle_replay(
    event: JobSettled,
    replay_repository: ReplayRepository,
    events: EventRegistry,
) -> None:
    """Map a settled replay job onto its replay aggregate."""
    replay = await replay_repository.get_by_job(event.job.id)
    if replay is None:
        return
    if event.job.status is JobStatus.COMPLETED:
        replay.complete()
    elif event.job.status is JobStatus.FAILED:
        replay.fail(event.job.error)
    elif event.job.status is JobStatus.CANCELED:
        replay.cancel()
    else:
        return
    stored = await replay_repository.update(replay)
    await events.dispatch(ReplaySettled(stored))
