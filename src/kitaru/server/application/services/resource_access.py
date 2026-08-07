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
"""Row-scoped resource access for task principals."""

import uuid

from kitaru.api_models.v1.session import SessionStatus
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import (
    AuthContext,
    GrantKind,
    TaskPrincipal,
)
from kitaru.server.domain.blob import BlobAccessDenied
from kitaru.server.domain.session import Session, SessionAccessDenied
from kitaru.server.domain.task import (
    EvaluationTaskDetails,
    ImportTask,
    ImportTaskDetails,
    ScriptPluginSpec,
    TaskSpec,
)


async def check_task_attempt(actor: AuthContext, tasks: TaskRepository) -> None:
    """Require a task principal's token to name the task's current attempt.

    A requeue leaves the superseded attempt's token unexpired, and the task
    id it carries stays valid across attempts, so the attempt has to be
    checked against the stored task.

    An account principal always passes.

    Args:
        actor: Caller context.
        tasks: Task repository.

    Raises:
        TaskNotFound: The principal names a task that no longer exists.
        TaskAttemptMismatch: The token is fenced by an attempt the task has
            moved past.
    """
    if not isinstance(actor.principal, TaskPrincipal):
        return
    task = await tasks.get(actor.principal.task_id)
    task.check_attempt(actor.principal.attempt)


def build_task_grants(spec: TaskSpec) -> dict[GrantKind, frozenset[uuid.UUID]]:
    """Derive the resources a task may reach from the spec it runs.

    Args:
        spec: Execution spec of the claimed task.

    Returns:
        Granted resource ids by kind, kinds the spec needs nothing of omitted.
    """
    sessions: set[uuid.UUID] = set()
    blobs: set[uuid.UUID] = set()
    details = spec.details
    if isinstance(details, EvaluationTaskDetails):
        sessions.add(details.input_session_id)
    if isinstance(details, (EvaluationTaskDetails, ImportTaskDetails)) and isinstance(
        details.plugin, ScriptPluginSpec
    ):
        blobs.add(details.plugin.blob_id)
    if isinstance(details, ImportTaskDetails):
        blobs.add(details.payload.blob_id)
    grants: dict[GrantKind, frozenset[uuid.UUID]] = {}
    if sessions:
        grants[GrantKind.SESSION] = frozenset(sessions)
    if blobs:
        grants[GrantKind.BLOB] = frozenset(blobs)
    return grants


def check_task_session_read(
    session_id: uuid.UUID, session_task_id: uuid.UUID | None, actor: AuthContext
) -> None:
    """Require a task principal to own the session or hold a grant for it.

    An account principal always passes.

    Args:
        session_id: Id of the session being read.
        session_task_id: Id of the task the session is linked to, if any.
        actor: Caller context.

    Raises:
        SessionAccessDenied: A task principal neither owns the session nor
            holds a grant for it.
    """
    if not isinstance(actor.principal, TaskPrincipal):
        return
    principal = actor.principal
    if session_task_id == principal.task_id or principal.has_grant(
        GrantKind.SESSION, session_id
    ):
        return
    raise SessionAccessDenied(session_id)


async def check_task_session_write(
    session: Session, actor: AuthContext, tasks: TaskRepository
) -> None:
    """Require a task principal to own the session being written.

    An import task principal additionally writes any pending-import session
    of the account, the adopted placeholder it fills.

    An account principal always passes.

    Args:
        session: Session being written.
        actor: Caller context.
        tasks: Task repository.

    Raises:
        SessionAccessDenied: A task principal does not own the session.
    """
    if not isinstance(actor.principal, TaskPrincipal):
        return
    if session.task_id == actor.principal.task_id:
        return
    if session.status is SessionStatus.PENDING_IMPORT:
        task = await tasks.get(actor.principal.task_id)
        if isinstance(task, ImportTask):
            return
    raise SessionAccessDenied(session.id)


def check_task_blob_read(blob_id: uuid.UUID, actor: AuthContext) -> None:
    """Require a task principal to hold a grant for the blob being read.

    An account principal always passes.

    Args:
        blob_id: Id of the blob being read.
        actor: Caller context.

    Raises:
        BlobAccessDenied: A task principal holds no grant for the blob.
    """
    if not isinstance(actor.principal, TaskPrincipal):
        return
    if not actor.principal.has_grant(GrantKind.BLOB, blob_id):
        raise BlobAccessDenied(blob_id)
