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
"""Tests for task-principal result session recovery."""

import uuid
from types import SimpleNamespace
from typing import Any, cast

import pytest

from kitaru.api_models.v1.session import SessionCreateRequest, SessionOrigin
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError, NotFoundError
from kitaru.client.session_recovery import create_or_get_result_session


class _FakeSessions:
    def __init__(self, created: Any, get_error: BaseException | None = None) -> None:
        self.created = created
        self.get_error = get_error
        self.get_calls: list[uuid.UUID] = []

    async def create(self, request: Any) -> Any:
        if isinstance(self.created, BaseException):
            raise self.created
        return self.created

    async def get(self, session_id: uuid.UUID) -> Any:
        self.get_calls.append(session_id)
        if self.get_error is not None:
            raise self.get_error
        return session_id


class _FakeTasks:
    def __init__(self, result_session_id: uuid.UUID | None) -> None:
        self.result_session_id = result_session_id
        self.get_calls: list[uuid.UUID] = []

    async def get(self, task_id: uuid.UUID) -> Any:
        self.get_calls.append(task_id)
        return SimpleNamespace(result_session_id=self.result_session_id)


class _FakeClient:
    def __init__(self, sessions: _FakeSessions, tasks: _FakeTasks) -> None:
        self.sessions = sessions
        self.tasks = tasks


def _request() -> SessionCreateRequest:
    return SessionCreateRequest(
        origin=SessionOrigin.RECORDED, inputs=None, outputs=None
    )


async def test_success_passthrough() -> None:
    """Return the created session and skip task recovery entirely."""
    session = object()
    client = _FakeClient(_FakeSessions(session), _FakeTasks(None))

    result = await create_or_get_result_session(
        cast(KitaruAPIClient, client), _request(), task_id=uuid.uuid4()
    )

    assert result is session
    assert client.tasks.get_calls == []


async def test_conflict_recovers_existing_result_session() -> None:
    """Read the task's result session when the create hits the 409."""
    result_session_id = uuid.uuid4()
    task_id = uuid.uuid4()
    client = _FakeClient(
        _FakeSessions(APIError(409, "Task already links a result session")),
        _FakeTasks(result_session_id),
    )

    result = await create_or_get_result_session(
        cast(KitaruAPIClient, client), _request(), task_id
    )

    assert result == result_session_id
    assert client.tasks.get_calls == [task_id]
    assert client.sessions.get_calls == [result_session_id]


async def test_conflict_without_result_session_reraises() -> None:
    """Re-raise the 409 when the task carries no result session to recover."""
    client = _FakeClient(
        _FakeSessions(APIError(409, "Task already links a result session")),
        _FakeTasks(None),
    )

    with pytest.raises(APIError) as exc_info:
        await create_or_get_result_session(
            cast(KitaruAPIClient, client), _request(), uuid.uuid4()
        )
    assert exc_info.value.status_code == 409


async def test_conflict_without_task_id_reraises() -> None:
    """Re-raise the 409 outside a task, where there is nothing to recover."""
    client = _FakeClient(
        _FakeSessions(APIError(409, "Task already links a result session")),
        _FakeTasks(uuid.uuid4()),
    )

    with pytest.raises(APIError):
        await create_or_get_result_session(
            cast(KitaruAPIClient, client), _request(), task_id=None
        )
    assert client.tasks.get_calls == []


async def test_other_error_reraises() -> None:
    """Re-raise an unrelated error without attempting task recovery."""
    client = _FakeClient(
        _FakeSessions(NotFoundError(404, "not found")), _FakeTasks(None)
    )

    with pytest.raises(NotFoundError):
        await create_or_get_result_session(
            cast(KitaruAPIClient, client), _request(), task_id=uuid.uuid4()
        )
    assert client.tasks.get_calls == []
