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
from collections.abc import Sequence

from kitaru.api_models.v1.job import JobKind, JobStatus
from kitaru.api_models.v1.replay import ReplayStatus
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.task import TaskOnFailure, TaskStatus
from kitaru.server.application.events import (
    EventDispatcher,
    JobsSettled,
    ReplaysSettled,
    SessionImportFinalized,
    TaskTerminal,
)
from kitaru.server.application.interfaces.experiment_repository import (
    ExperimentRepository,
)
from kitaru.server.application.interfaces.job_repository import JobRepository
from kitaru.server.application.interfaces.replay_repository import ReplayRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.services.task_transitions import TaskTransitions
from kitaru.server.domain.agent_version import AgentVersion, FunctionRunSpec
from kitaru.server.domain.job import Job
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.replay_config import ReplayConfig
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import AgentTask, EvaluationTask, Task

AGENT_VERSION_LABEL = "agent_version"


async def create_replay_pipelines(
    baselines: Sequence[Session],
    agent_version: AgentVersion,
    config: ReplayConfig,
    evaluate_baselines: bool,
    experiment_run_id: uuid.UUID | None,
    actor: AuthContext,
    replay_repository: ReplayRepository,
    job_repository: JobRepository,
    task_repository: TaskRepository,
) -> list[Replay]:
    """Create many replays' jobs, initial tasks, and replay rows in three bulk writes.

    Each agent task carries its baseline session's inputs and the agent
    version as a label. A function-mode agent version creates its jobs
    provisional, finalized by the server when the placeholder session's
    import finalizes. With ``evaluate_baselines``, one baseline evaluator
    task is appended per evaluator that has not already scored the baseline
    session.

    Args:
        baselines: Sessions being replayed.
        agent_version: Agent version to replay with.
        config: Replay config every created replay points at.
        evaluate_baselines: Whether to also score the baseline sessions.
        experiment_run_id: Run the replays belong to, ``None`` for
            standalone replays.
        actor: Caller context.
        replay_repository: Replay repository.
        job_repository: Job repository.
        task_repository: Task repository.

    Returns:
        Created replays, in baseline order.
    """
    if not baselines:
        return []
    provisional = isinstance(agent_version.run_spec, FunctionRunSpec)
    jobs = [
        Job(owner_id=actor.account.id, kind=JobKind.REPLAY, provisional=provisional)
        for _ in baselines
    ]
    replays = [
        Replay(
            owner_id=actor.account.id,
            job_id=job.id,
            experiment_run_id=experiment_run_id,
            replay_config_id=config.id,
            baseline_session_id=baseline.id,
            evaluate_baselines=evaluate_baselines,
        )
        for job, baseline in zip(jobs, baselines, strict=True)
    ]
    scored_by_session: dict[uuid.UUID, set[uuid.UUID]] = {}
    if evaluate_baselines:
        scored_by_session = await task_repository.get_scored_evaluator_version_ids_many(
            [baseline.id for baseline in baselines]
        )
    tasks: list[Task] = []
    for job, baseline in zip(jobs, baselines, strict=True):
        tasks.append(
            AgentTask(
                job_id=job.id,
                agent_version_id=agent_version.id,
                inputs=baseline.inputs,
                labels={AGENT_VERSION_LABEL: str(agent_version.id)},
                on_failure=TaskOnFailure.ABORT,
            )
        )
        if not evaluate_baselines:
            continue
        scored = scored_by_session.get(baseline.id, set())
        for evaluator in config.evaluators:
            if evaluator.evaluator_version_id in scored:
                continue
            tasks.append(
                EvaluationTask(
                    job_id=job.id,
                    plugin_version_id=evaluator.evaluator_version_id,
                    input_session_id=baseline.id,
                    params=evaluator.params,
                    on_failure=TaskOnFailure.ABORT,
                )
            )
    await job_repository.create_many(jobs)
    stored_replays = await replay_repository.create_many(replays)
    await task_repository.create_many(tasks)
    return stored_replays


async def append_result_evaluations(
    event: TaskTerminal,
    replay_repository: ReplayRepository,
    experiment_repository: ExperimentRepository,
    task_repository: TaskRepository,
    session_repository: SessionRepository,
) -> None:
    """Append the replay's result evaluator tasks when its result session is ready.

    Fires on the completion of an agent task with a terminal result session.
    A completed agent task whose result session is still pending import
    defers to the import finalization. A no-op when the terminal task is
    not an agent task, did not complete, or does not belong to a replay's
    job.
    Inserts the evaluator tasks without locking the job row. The completing
    task's own transition settles the job afterward, in the same
    transaction, and its drained scan reads every task including these, so
    the job can never be judged drained before they exist.

    Args:
        event: TaskTerminal event.
        replay_repository: Replay repository.
        experiment_repository: Experiment repository, for the replay config.
        task_repository: Task repository.
        session_repository: Session repository, for the result session status.
    """
    task = event.task
    if not isinstance(task, AgentTask) or task.status is not TaskStatus.COMPLETED:
        return
    replay = await replay_repository.get_by_job_id(task.job_id)
    # A finalized import may already have appended the evaluator tasks
    # for this replay before the agent task's completion landed.
    if replay is None or replay.status is not ReplayStatus.PENDING:
        return
    assert task.result_session_id is not None
    session = await session_repository.get(task.result_session_id)
    if session.status is SessionStatus.PENDING_IMPORT:
        return
    config = await experiment_repository.get_replay_config(replay.replay_config_id)
    evaluator_tasks: list[Task] = [
        EvaluationTask(
            job_id=task.job_id,
            plugin_version_id=evaluator.evaluator_version_id,
            input_session_id=task.result_session_id,
            params=evaluator.params,
            on_failure=TaskOnFailure.ABORT,
        )
        for evaluator in config.evaluators
    ]
    if evaluator_tasks:
        await task_repository.create_many(evaluator_tasks)
    replay.start_evaluating()
    await replay_repository.update(replay)


async def continue_replay_on_import(
    event: SessionImportFinalized,
    task_repository: TaskRepository,
    replay_repository: ReplayRepository,
    experiment_repository: ExperimentRepository,
    transitions: TaskTransitions,
) -> None:
    """Finalize or fail a replay's provisional job when its placeholder finalizes.

    A no-op when the finalized session was not produced by a task, the
    task's job belongs to no replay, or the replay already settled. A
    completed import appends the replay's result evaluator tasks before
    finalizing the job, so its drained scan reads every task including
    these. A failed import fails the job with the session's error.

    Args:
        event: SessionImportFinalized event.
        task_repository: Task repository.
        replay_repository: Replay repository.
        experiment_repository: Experiment repository, for the replay config.
        transitions: Task transition dispatch.
    """
    session = event.session
    if session.task_id is None:
        return
    # Lock the producer task row so a concurrent completion of the agent
    # task, which appends under the same lock, cannot append twice.
    producer = await task_repository.get(session.task_id, exclusive=True)
    replay = await replay_repository.get_by_job_id(producer.job_id)
    if replay is None or replay.settled:
        return
    if session.status is not SessionStatus.COMPLETED:
        await transitions.fail_job(producer.job_id, session.error)
        return
    config = await experiment_repository.get_replay_config(replay.replay_config_id)
    evaluator_tasks: list[Task] = [
        EvaluationTask(
            job_id=producer.job_id,
            plugin_version_id=evaluator.evaluator_version_id,
            input_session_id=session.id,
            params=evaluator.params,
            on_failure=TaskOnFailure.ABORT,
        )
        for evaluator in config.evaluators
    ]
    if evaluator_tasks:
        await task_repository.create_many(evaluator_tasks)
    replay.start_evaluating()
    await replay_repository.update(replay)
    await transitions.finalize_job(producer.job_id)


def _apply_job_settlement(replay: Replay, job: Job) -> bool:
    """Apply a settled job's outcome onto its replay.

    Args:
        replay: Replay to update.
        job: Job that settled.

    Returns:
        Whether the job's status mapped onto a replay transition.
    """
    if job.status is JobStatus.COMPLETED:
        replay.complete()
    elif job.status is JobStatus.FAILED:
        replay.fail(job.error)
    elif job.status is JobStatus.CANCELED:
        replay.cancel()
    else:
        return False
    return True


async def settle_replays(
    event: JobsSettled,
    replay_repository: ReplayRepository,
    dispatcher: EventDispatcher,
) -> None:
    """Map settled jobs' outcomes onto their replays and emit ReplaysSettled.

    A job holding no replay is skipped. The updated replays publish a
    single ``ReplaysSettled``.

    Args:
        event: JobsSettled event.
        replay_repository: Replay repository.
        dispatcher: Event dispatcher to emit ``ReplaysSettled`` on.
    """
    replays_by_job_id = await replay_repository.get_many_by_job_ids(
        [job.id for job in event.jobs]
    )
    changed: list[Replay] = []
    for job in event.jobs:
        replay = replays_by_job_id.get(job.id)
        if replay is None:
            continue
        if not _apply_job_settlement(replay, job):
            continue
        changed.append(replay)
    if not changed:
        return
    stored = await replay_repository.update_many(changed)
    await dispatcher.dispatch(ReplaysSettled(replays=stored))
