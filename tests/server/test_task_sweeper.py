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
"""Tests for the background stale-task sweep loop."""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any, cast

import pytest
from asyncpg.exceptions import LockNotAvailableError
from sqlalchemy.exc import DBAPIError

from conftest import build_job_and_task_services, local_settings
from kitaru.analytics.client import AnalyticsClient
from kitaru.server.api import task_sweeper
from kitaru.server.api.config import APISettings
from kitaru.server.application.services.task_service import TaskService
from kitaru.server.database.service import DatabaseService


def _lock_not_available_error() -> DBAPIError:
    """Build a database error chained like a driver-reported NOWAIT failure."""
    adapter_error = Exception("adapter error")
    adapter_error.__cause__ = LockNotAvailableError("could not obtain lock")
    return DBAPIError("SELECT task", None, adapter_error)


class _RecordingSession:
    """Session double recording the commits and rollbacks a sweep unit drives."""

    def __init__(self) -> None:
        """Initialize the counters."""
        self.commits = 0
        self.rollbacks = 0

    async def commit(self) -> None:
        """Record a commit."""
        self.commits += 1

    async def rollback(self) -> None:
        """Record a rollback."""
        self.rollbacks += 1


class _StubDatabase:
    """Database service double yielding one recording session per call."""

    def __init__(self) -> None:
        """Initialize the session log."""
        self.sessions: list[_RecordingSession] = []
        self.engine: Any = None

    async def get_async_session(self) -> AsyncGenerator[Any, None]:
        """Yield a fresh recording session.

        Yields:
            Recording session double.
        """
        session = _RecordingSession()
        self.sessions.append(session)
        yield session


def _stub_sweeper_wiring(monkeypatch: pytest.MonkeyPatch, service: TaskService) -> None:
    """Bind the sweeper's per-transaction service build to one fake-backed service.

    Args:
        monkeypatch: Patcher for the sweeper module.
        service: Service every sweep unit runs against.
    """
    monkeypatch.setattr(
        task_sweeper, "get_server_analytics", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(task_sweeper, "get_task_service", lambda *args: service)


async def test_sweep_once_propagates_before_rescuing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every cancel propagation runs before the first stale rescue.

    Rescuing first would requeue a stale task of a canceling job, because the
    rescue reads the task's own cancel stamp, which the propagation has not
    written yet.
    """
    services = build_job_and_task_services()
    task_id, job_id = uuid.uuid4(), uuid.uuid4()
    calls: list[str] = []

    async def record_sweep(
        self: TaskService, task_id: uuid.UUID, now: datetime
    ) -> None:
        calls.append("rescue")

    async def record_propagate(self: TaskService, job_id: uuid.UUID) -> None:
        calls.append("propagate")

    async def candidates(*args: Any) -> tuple[list[uuid.UUID], list[uuid.UUID]]:
        return [task_id], [job_id]

    monkeypatch.setattr(TaskService, "sweep_stale_task", record_sweep)
    monkeypatch.setattr(TaskService, "propagate_job_cancel", record_propagate)
    monkeypatch.setattr(task_sweeper, "_read_candidates", candidates)
    _stub_sweeper_wiring(monkeypatch, services.task_service)

    await task_sweeper.sweep_once(
        cast(DatabaseService, _StubDatabase()),
        local_settings(),
        AnalyticsClient(enabled=False),
    )

    assert calls == ["propagate", "rescue"]


async def test_sweep_once_continues_after_a_failing_item(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An item that raises is rolled back and the remaining items still run."""
    services = build_job_and_task_services()
    first, second = uuid.uuid4(), uuid.uuid4()
    swept: list[uuid.UUID] = []

    async def failing_sweep(
        self: TaskService, task_id: uuid.UUID, now: datetime
    ) -> None:
        swept.append(task_id)
        if task_id == first:
            raise RuntimeError("boom")

    async def candidates(*args: Any) -> tuple[list[uuid.UUID], list[uuid.UUID]]:
        return [first, second], []

    monkeypatch.setattr(TaskService, "sweep_stale_task", failing_sweep)
    monkeypatch.setattr(task_sweeper, "_read_candidates", candidates)
    _stub_sweeper_wiring(monkeypatch, services.task_service)
    database = _StubDatabase()

    await task_sweeper.sweep_once(
        cast(DatabaseService, database),
        local_settings(),
        AnalyticsClient(enabled=False),
    )

    assert swept == [first, second]
    assert [session.rollbacks for session in database.sessions] == [1, 0]
    assert [session.commits for session in database.sessions] == [0, 1]


async def test_sweep_once_skips_a_job_whose_task_rows_are_held(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A contended NOWAIT acquisition rolls back that job alone."""
    services = build_job_and_task_services()
    held, free = uuid.uuid4(), uuid.uuid4()
    propagated: list[uuid.UUID] = []

    async def failing_propagate(self: TaskService, job_id: uuid.UUID) -> None:
        propagated.append(job_id)
        if job_id == held:
            raise _lock_not_available_error()

    async def candidates(*args: Any) -> tuple[list[uuid.UUID], list[uuid.UUID]]:
        return [], [held, free]

    monkeypatch.setattr(TaskService, "propagate_job_cancel", failing_propagate)
    monkeypatch.setattr(task_sweeper, "_read_candidates", candidates)
    _stub_sweeper_wiring(monkeypatch, services.task_service)
    database = _StubDatabase()

    await task_sweeper.sweep_once(
        cast(DatabaseService, database),
        local_settings(),
        AnalyticsClient(enabled=False),
    )

    assert propagated == [held, free]
    assert [session.rollbacks for session in database.sessions] == [1, 0]
    assert [session.commits for session in database.sessions] == [0, 1]


async def test_start_task_sweeper_returns_none_when_interval_is_zero() -> None:
    """A zero interval disables the sweeper."""
    database = DatabaseService(local_settings())
    settings = local_settings(TASK_SWEEP_INTERVAL_SECONDS=0)

    task = task_sweeper.start_task_sweeper(
        database, settings, AnalyticsClient(enabled=False)
    )

    assert task is None
    await database.cleanup()


async def test_start_task_sweeper_returns_a_running_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A positive interval starts a task that stop_task_sweeper cancels cleanly."""
    calls = 0

    async def fake_sweep_once(
        database: DatabaseService,
        settings: APISettings,
        analytics: AnalyticsClient,
    ) -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(task_sweeper, "sweep_once", fake_sweep_once)
    database = DatabaseService(local_settings())
    settings = local_settings(TASK_SWEEP_INTERVAL_SECONDS=1000)

    task = task_sweeper.start_task_sweeper(
        database, settings, AnalyticsClient(enabled=False)
    )
    assert task is not None
    await asyncio.sleep(0.05)
    assert calls == 1
    assert not task.done()

    await task_sweeper.stop_task_sweeper(task)
    assert task.done()
    assert task.cancelled()
    await database.cleanup()


async def test_run_sweep_loop_continues_after_a_failing_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tick that raises is logged and the loop keeps ticking on schedule."""
    calls: list[int] = []

    async def fake_sweep_once(
        database: DatabaseService,
        settings: APISettings,
        analytics: AnalyticsClient,
    ) -> None:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("boom")

    monkeypatch.setattr(task_sweeper, "sweep_once", fake_sweep_once)
    database = DatabaseService(local_settings())
    settings = local_settings(TASK_SWEEP_INTERVAL_SECONDS=1)

    task = task_sweeper.start_task_sweeper(
        database, settings, AnalyticsClient(enabled=False)
    )
    assert task is not None
    try:
        for _ in range(50):
            if len(calls) >= 3:
                break
            await asyncio.sleep(0.1)
        assert len(calls) >= 3
        assert not task.done()
    finally:
        await task_sweeper.stop_task_sweeper(task)
    await database.cleanup()
