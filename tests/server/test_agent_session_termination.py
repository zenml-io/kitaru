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
"""Agent task termination closes only its unfinished result session."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from kitaru.api_models.v1.job import JobStatus
from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.application.events import TaskTerminal
from kitaru.server.application.models.auth import AuthContext, TaskAuthContext
from kitaru.server.application.models.task import TaskPolicy, TaskUpdate
from kitaru.server.application.services.agent_session_termination import (
    fail_unfinished_agent_session,
)
from kitaru.server.domain.account import Account
from kitaru.server.domain.agent_version import RunSpec
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import AgentTask
from kitaru_test_support import (
    ReplayServices,
    build_replay_services,
    build_task_actor,
    build_worker_actor,
    create_agent,
    create_agent_task,
    create_agent_version,
    create_evaluation_task,
    create_job,
    create_session,
    create_worker,
)

ACTOR = AuthContext(account=Account(id=uuid.uuid4(), name="ann"))


async def _running_agent(
    services: ReplayServices,
    status: SessionStatus = SessionStatus.IN_PROGRESS,
) -> tuple[AgentTask, TaskAuthContext, Session]:
    agent = await create_agent(services.agents, ACTOR.account.id)
    version = await create_agent_version(
        services.agent_versions,
        agent_id=agent.id,
        owner_id=ACTOR.account.id,
        run_spec=RunSpec(command="run.sh", timeout_seconds=60),
    )
    job = await create_job(services.jobs, ACTOR.account.id)
    task = await create_agent_task(services.tasks, job.id, agent_version_id=version.id)
    worker = await create_worker(services.workers, ACTOR.account.id)
    await services.task_service.claim_tasks(
        1, actor=build_worker_actor(ACTOR.account, worker.id)
    )
    actor = build_task_actor(ACTOR.account, task.id, 1, worker.id)
    await services.task_service.update_task(
        task.id, TaskUpdate(status=TaskStatus.RUNNING), actor=actor
    )
    session = await create_session(
        services.sessions,
        ACTOR.account.id,
        agent_id=agent.id,
        agent_version_id=version.id,
        task_id=task.id,
        status=status,
        inputs={"ticket": "123"},
        outputs={"partial": "response"},
        output_text_selector="$.partial",
        metadata={"source": "test"},
        error="recorded error" if status is SessionStatus.FAILED else None,
        ended_at=datetime.now(UTC) if status is not SessionStatus.IN_PROGRESS else None,
    )
    return task, actor, session


@pytest.mark.parametrize(
    ("status", "error", "expected_error"),
    [
        (TaskStatus.FAILED, "executor failed", "executor failed"),
        (TaskStatus.FAILED, None, "Agent task failed."),
        (TaskStatus.FAILED, "", ""),
        (TaskStatus.TIMED_OUT, "timeout details", "timeout details"),
        (TaskStatus.TIMED_OUT, None, "Agent task timed out."),
        (TaskStatus.CANCELED, None, "Agent task canceled."),
    ],
)
async def test_unsuccessful_agent_task_closes_its_result_session(
    status: TaskStatus,
    error: str | None,
    expected_error: str,
) -> None:
    services = build_replay_services()
    task, actor, original = await _running_agent(services)

    terminal = await services.task_service.update_task(
        task.id, TaskUpdate(status=status, error=error), actor=actor
    )

    session = await services.sessions.get(original.id, include_payloads=True)
    assert session.status is SessionStatus.FAILED
    assert session.ended_at == terminal.ended_at
    assert session.error == expected_error
    assert session.inputs == original.inputs
    assert session.outputs == original.outputs
    assert session.output_text_selector == original.output_text_selector
    assert session.metadata == original.metadata
    assert session.task_id == task.id


@pytest.mark.parametrize("status", [SessionStatus.COMPLETED, SessionStatus.FAILED])
async def test_agent_failure_preserves_an_already_finished_session(
    status: SessionStatus,
) -> None:
    services = build_replay_services()
    task, actor, original = await _running_agent(services, status)

    await services.task_service.update_task(
        task.id,
        TaskUpdate(status=TaskStatus.FAILED, error="later failure"),
        actor=actor,
    )

    assert await services.sessions.get(original.id, include_payloads=True) == original


@pytest.mark.parametrize("cancel_requested", [False, True])
async def test_stale_agent_termination_closes_its_result_session(
    cancel_requested: bool,
) -> None:
    services = build_replay_services(policy=TaskPolicy(retry_limit=1))
    task, _, original = await _running_agent(services)
    now = datetime.now(UTC)
    task = await services.tasks.get(task.id)
    task.heartbeat_at = now - timedelta(hours=1)
    if cancel_requested:
        task.request_cancel(now)
    await services.tasks.update(task)

    await services.task_service.sweep_stale_task(task.id, now)

    terminal = await services.tasks.get(task.id)
    session = await services.sessions.get(original.id, include_payloads=True)
    assert terminal.status is (
        TaskStatus.CANCELED if cancel_requested else TaskStatus.ABANDONED
    )
    assert session.status is SessionStatus.FAILED
    assert session.ended_at == now
    assert session.error == (terminal.error or "Agent task canceled.")
    assert session.outputs == original.outputs


async def test_stale_retry_unlinks_but_does_not_finish_the_previous_session() -> None:
    services = build_replay_services(policy=TaskPolicy(retry_limit=2))
    task, _, original = await _running_agent(services)
    now = datetime.now(UTC)
    task = await services.tasks.get(task.id)
    task.heartbeat_at = now - timedelta(hours=1)
    await services.tasks.update(task)

    await services.task_service.sweep_stale_task(task.id, now)

    session = await services.sessions.get(original.id, include_payloads=True)
    assert (await services.tasks.get(task.id)).status is TaskStatus.PENDING
    assert session.task_id is None
    assert session.status is SessionStatus.IN_PROGRESS
    assert session.ended_at is None
    assert session.outputs == original.outputs
    assert (await services.jobs.get(task.job_id)).status is JobStatus.RUNNING

    await services.job_service.cancel_job(task.job_id, actor=ACTOR)

    assert (await services.tasks.get(task.id)).status is TaskStatus.CANCELED
    assert await services.sessions.get(original.id, include_payloads=True) == session


async def test_evaluator_failure_leaves_its_input_session_unchanged() -> None:
    services = build_replay_services()
    agent_task, _, original = await _running_agent(services)
    task = await create_evaluation_task(
        services.tasks, agent_task.job_id, input_session_id=original.id
    )
    now = datetime.now(UTC)
    task.claim(uuid.uuid4(), now)
    task.start(now)
    await services.tasks.update(task)
    assert task.worker_id is not None
    actor = build_task_actor(ACTOR.account, task.id, task.attempt, task.worker_id)

    await services.task_service.update_task(
        task.id, TaskUpdate(status=TaskStatus.FAILED, error="evaluator failed"), actor
    )

    assert await services.sessions.get(original.id, include_payloads=True) == original


async def test_agent_failure_without_a_result_session_still_settles_the_job() -> None:
    services = build_replay_services()
    task, actor, original = await _running_agent(services)
    await services.sessions.delete(original.id)

    await services.task_service.update_task(
        task.id, TaskUpdate(status=TaskStatus.FAILED, error="before recording"), actor
    )

    assert (await services.jobs.get(task.job_id)).status is JobStatus.FAILED


async def test_repeated_terminal_event_preserves_the_finished_session() -> None:
    services = build_replay_services()
    task, actor, original = await _running_agent(services)
    terminal = await services.task_service.update_task(
        task.id, TaskUpdate(status=TaskStatus.FAILED, error="first failure"), actor
    )
    finished = await services.sessions.get(original.id, include_payloads=True)

    await fail_unfinished_agent_session(
        TaskTerminal(task=terminal, previous_status=TaskStatus.RUNNING),
        services.sessions,
    )

    assert await services.sessions.get(original.id, include_payloads=True) == finished


async def test_agent_failure_marks_pending_mastra_recording_ineligible() -> None:
    services = build_replay_services()
    task, actor, original = await _running_agent(services)
    original.origin = SessionOrigin.RECORDED
    original.framework = "mastra"
    original.metadata = {"source": "test", "mastra_replay_state": "pending"}
    await services.sessions.update(original)

    await services.task_service.update_task(
        task.id, TaskUpdate(status=TaskStatus.FAILED, error="agent crashed"), actor
    )

    session = await services.sessions.get(original.id, include_payloads=True)
    assert session.status is SessionStatus.FAILED
    assert session.metadata == {
        "source": "test",
        "mastra_replay_state": "ineligible",
        "mastra_replay_reason": "abandoned",
    }
    assert session.inputs == original.inputs
    assert session.outputs == original.outputs
