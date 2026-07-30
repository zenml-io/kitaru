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
"""Replay job and task composition, shared by standalone replays and run fan-out."""

import uuid

from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.task import TaskOnFailure, TaskStatus
from kitaru.server.application.events import (
    EventDispatcher,
    JobSettled,
    ReplaySettled,
    TaskTerminal,
)
from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRepository,
)
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.job_service import add_task
from kitaru.server.domain.job import Job
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import ReplayConfig, effective_inputs
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import AgentTask, EvaluationTask

REPLAY_ID_ENV = "KITARU_REPLAY_ID"
AGENT_VERSION_LABEL = "agent_version"


async def create_replay_pipeline(
    baseline: Session,
    agent_version_id: uuid.UUID,
    config: ReplayConfig,
    evaluate_baselines: bool,
    experiment_run_id: uuid.UUID | None,
    actor: AuthContext,
    replay_repository: ReplayRepository,
    job_repository: JobRepository,
    task_repository: TaskRepository,
) -> Replay:
    """Create a replay's job, initial tasks, and replay row in one pipeline.

    The agent task carries the baseline session's inputs with the config's
    override applied, the replay id in its env extras, and the agent version
    as a label. With ``evaluate_baselines``, one baseline evaluator task is
    appended per evaluator that has not already scored the baseline session.

    Args:
        baseline: Session being replayed.
        agent_version_id: Agent version to replay with.
        config: Replay config the replay and, for a run, its sibling
            replays all point at.
        evaluate_baselines: Whether to also score the baseline session.
        experiment_run_id: Run this replay belongs to, ``None`` for a
            standalone replay.
        actor: Caller context.
        replay_repository: Replay repository.
        job_repository: Job repository.
        task_repository: Task repository.

    Raises:
        DuplicateReplayForBaseline: The run already holds a replay for this
            baseline session.

    Returns:
        Created replay.
    """
    job = await job_repository.create(Job(owner_id=actor.account.id))
    replay = await replay_repository.create(
        Replay(
            owner_id=actor.account.id,
            job_id=job.id,
            experiment_run_id=experiment_run_id,
            replay_config_id=config.id,
            baseline_session_id=baseline.id,
            evaluate_baselines=evaluate_baselines,
        )
    )
    await add_task(
        AgentTask(
            job_id=job.id,
            agent_version_id=agent_version_id,
            inputs=effective_inputs(baseline.inputs, config.override),
            env={REPLAY_ID_ENV: str(replay.id)},
            labels={AGENT_VERSION_LABEL: str(agent_version_id)},
            on_failure=TaskOnFailure.ABORT,
        ),
        job_repository,
        task_repository,
    )
    if evaluate_baselines:
        scored = await task_repository.get_scored_evaluator_version_ids(baseline.id)
        for evaluator in config.evaluators:
            if evaluator.evaluator_version_id in scored:
                continue
            await add_task(
                EvaluationTask(
                    job_id=job.id,
                    plugin_version_id=evaluator.evaluator_version_id,
                    input_session_id=baseline.id,
                    params=evaluator.params,
                    on_failure=TaskOnFailure.ABORT,
                ),
                job_repository,
                task_repository,
            )
    return replay


async def append_result_evaluations(
    event: TaskTerminal,
    replay_repository: ReplayRepository,
    experiment_repository: ExperimentRepository,
    job_repository: JobRepository,
    task_repository: TaskRepository,
) -> None:
    """Append the replay's result evaluator tasks when its agent task completes.

    A no-op when the terminal task is not an agent task, did not complete, or
    does not belong to a replay's job.

    Args:
        event: TaskTerminal event.
        replay_repository: Replay repository.
        experiment_repository: Experiment repository, for the replay config.
        job_repository: Job repository.
        task_repository: Task repository.
    """
    task = event.task
    if not isinstance(task, AgentTask) or task.status is not TaskStatus.COMPLETED:
        return
    replay = await replay_repository.get_by_job_id(task.job_id)
    if replay is None:
        return
    config = await experiment_repository.get_replay_config(replay.replay_config_id)
    assert task.result_session_id is not None
    for evaluator in config.evaluators:
        await add_task(
            EvaluationTask(
                job_id=task.job_id,
                plugin_version_id=evaluator.evaluator_version_id,
                input_session_id=task.result_session_id,
                params=evaluator.params,
                on_failure=TaskOnFailure.ABORT,
            ),
            job_repository,
            task_repository,
        )
    replay.start_evaluating()
    await replay_repository.update(replay)


async def settle_replay(
    event: JobSettled,
    replay_repository: ReplayRepository,
    dispatcher: EventDispatcher,
) -> None:
    """Map a job's settlement outcome onto its replay and emit ReplaySettled.

    A no-op when the job holds no replay.

    Args:
        event: JobSettled event.
        replay_repository: Replay repository.
        dispatcher: Event dispatcher to emit ``ReplaySettled`` on.
    """
    job = event.job
    replay = await replay_repository.get_by_job_id(job.id)
    if replay is None:
        return
    if job.status is JobStatus.COMPLETED:
        replay.complete()
    elif job.status is JobStatus.FAILED:
        replay.fail(job.error)
    elif job.status is JobStatus.CANCELED:
        replay.cancel()
    else:
        return
    stored = await replay_repository.update(replay)
    await dispatcher.dispatch(ReplaySettled(replay=stored))
