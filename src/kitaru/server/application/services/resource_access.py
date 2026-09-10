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

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionOrigin
from kitaru.server.application.interfaces.task_repository import TaskRepository
from kitaru.server.application.models.auth import (
    AuthContext,
    GrantKind,
    TaskPrincipal,
)
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.domain.base import ForbiddenError
from kitaru.server.domain.blob import BlobAccessDenied
from kitaru.server.domain.session import Session, SessionAccessDenied
from kitaru.server.domain.task import (
    AnalysisTaskDetails,
    BlobImportSourceSpec,
    EvaluationTaskDetails,
    ImportTaskDetails,
    ScriptPluginSpec,
    TaskSpec,
)
from kitaru.server.filtering import AndExpression, FilterCondition


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
    imports: set[uuid.UUID] = set()
    details = spec.details
    if isinstance(details, EvaluationTaskDetails):
        sessions.add(details.input_session_id)
    if isinstance(details, AnalysisTaskDetails):
        imports.add(details.import_id)
    if isinstance(
        details, (EvaluationTaskDetails, ImportTaskDetails, AnalysisTaskDetails)
    ) and isinstance(details.plugin, ScriptPluginSpec):
        blobs.add(details.plugin.blob_id)
    if isinstance(details, ImportTaskDetails) and isinstance(
        details.source, BlobImportSourceSpec
    ):
        blobs.add(details.source.blob_id)
    grants: dict[GrantKind, frozenset[uuid.UUID]] = {}
    if sessions:
        grants[GrantKind.SESSION] = frozenset(sessions)
    if blobs:
        grants[GrantKind.BLOB] = frozenset(blobs)
    if imports:
        grants[GrantKind.IMPORT] = frozenset(imports)
    return grants


def check_task_session_read(session: Session, actor: AuthContext) -> None:
    """Require a task principal to own the session or hold a grant covering it.

    A grant covers the session directly by id or through the import that
    created it. An account principal always passes.

    Args:
        session: Session being read.
        actor: Caller context.

    Raises:
        SessionAccessDenied: A task principal neither owns the session nor
            holds a grant covering it.
    """
    if not isinstance(actor.principal, TaskPrincipal):
        return
    principal = actor.principal
    if session.task_id == principal.task_id or principal.has_grant(
        GrantKind.SESSION, session.id
    ):
        return
    if session.import_id is not None and principal.has_grant(
        GrantKind.IMPORT, session.import_id
    ):
        return
    raise SessionAccessDenied(session.id)


def check_task_session_write(session: Session, actor: AuthContext) -> None:
    """Require a task principal to own the session being written.

    An imported session is open to any task principal, so a later import
    contributes to a session an earlier one created. Owner scoping is
    applied by the repository. An account principal always passes.

    Args:
        session: Session being written.
        actor: Caller context.

    Raises:
        SessionAccessDenied: A task principal neither owns the session nor
            writes into an imported one.
    """
    if not isinstance(actor.principal, TaskPrincipal):
        return
    if session.origin == SessionOrigin.IMPORTED:
        return
    if session.task_id != actor.principal.task_id:
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


def scope_task_session_filter(
    session_filter: SessionFilter, actor: AuthContext
) -> SessionFilter:
    """Restrict a session listing to the imports a task principal is granted.

    An account principal's filter passes through unchanged.

    Args:
        session_filter: Filter the caller sent.
        actor: Caller context.

    Raises:
        ForbiddenError: A task principal holds no import grant.

    Returns:
        Filter restricted to the granted imports.
    """
    if not isinstance(actor.principal, TaskPrincipal):
        return session_filter
    import_ids = actor.principal.grants.get(GrantKind.IMPORT)
    if not import_ids:
        raise ForbiddenError(
            f"Task {actor.principal.task_id} is not granted a session listing"
        )
    scope = FilterCondition(field="import_id", op=FilterOp.IN, value=sorted(import_ids))
    expression = (
        scope
        if session_filter.expression is None
        else AndExpression(operands=(scope, session_filter.expression))
    )
    return session_filter.model_copy(update={"expression": expression})
