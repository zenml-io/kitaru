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
"""Close unfinished result sessions after unsuccessful agent tasks."""

from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.application.events import TaskTerminal
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.domain.task import AgentTask


async def fail_unfinished_agent_session(
    event: TaskTerminal, session_repository: SessionRepository
) -> None:
    """Fail an unsuccessful agent task's still-running result session.

    Sessions can only be linked while their task is running, and a requeue
    unlinks the previous attempt's session. Pending and claimed tasks
    therefore have no result session to close. Skipping them also keeps
    batch cancellation and claim-time input failures from acquiring session
    locks after job locks or one session at a time. Running tasks terminate
    individually, locking their session before job settlement in the same
    transaction.

    Args:
        event: Task terminal event.
        session_repository: Session repository.
    """
    task = event.task
    if (
        not isinstance(task, AgentTask)
        or event.previous_status is not TaskStatus.RUNNING
        or task.status
        not in {
            TaskStatus.FAILED,
            TaskStatus.TIMED_OUT,
            TaskStatus.CANCELED,
            TaskStatus.ABANDONED,
        }
    ):
        return
    session = await session_repository.get_by_task_id(
        task.id, include_payloads=False, exclusive=True
    )
    if session is None or session.status is not SessionStatus.IN_PROGRESS:
        return
    session.update_metadata(
        session.resolve_replay_metadata(
            status=SessionStatus.FAILED,
            metadata=session.metadata,
            inputs=None,
            replacing_inputs=False,
        )
    )
    session.finish(
        status=SessionStatus.FAILED,
        output_text_selector=session.output_text_selector,
        error=task.error
        if task.error is not None
        else f"Agent task {task.status.value.replace('_', ' ')}.",
        ended_at=task.ended_at,
    )
    await session_repository.update(session)
