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
"""Row-scoped session access for task principals."""

import uuid

from kitaru.server.application.models.auth import AuthContext, TaskPrincipal
from kitaru.server.domain.session import SessionAccessDenied


def check_task_session_read(
    session_id: uuid.UUID, session_task_id: uuid.UUID | None, actor: AuthContext
) -> None:
    """Require a task principal to own the session or hold it as its input session.

    An account principal always passes.

    Args:
        session_id: Id of the session being read.
        session_task_id: Id of the task the session is linked to, if any.
        actor: Caller context.

    Raises:
        SessionAccessDenied: A task principal owns neither the session nor
            holds it as its task's input session.
    """
    if not isinstance(actor.principal, TaskPrincipal):
        return
    principal = actor.principal
    if session_task_id == principal.task_id or session_id == principal.input_session_id:
        return
    raise SessionAccessDenied(session_id)


def check_task_session_write(
    session_id: uuid.UUID, session_task_id: uuid.UUID | None, actor: AuthContext
) -> None:
    """Require a task principal to own the session being written.

    An account principal always passes.

    Args:
        session_id: Id of the session being written.
        session_task_id: Id of the task the session is linked to, if any.
        actor: Caller context.

    Raises:
        SessionAccessDenied: A task principal does not own the session.
    """
    if not isinstance(actor.principal, TaskPrincipal):
        return
    if session_task_id != actor.principal.task_id:
        raise SessionAccessDenied(session_id)
