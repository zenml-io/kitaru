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

import pytest

from conftest import local_settings
from kitaru.analytics.client import AnalyticsClient
from kitaru.server.api import task_sweeper
from kitaru.server.api.config import APISettings
from kitaru.server.database.service import DatabaseService


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
