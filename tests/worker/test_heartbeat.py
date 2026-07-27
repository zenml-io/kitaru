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
"""Tests for the batched worker heartbeat."""

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from typing import cast

from fakes import FakeClient

from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import ServerError
from kitaru.worker.heartbeat import WorkerHeartbeat


@contextlib.asynccontextmanager
async def running_heartbeat(
    heartbeat: WorkerHeartbeat,
) -> AsyncIterator[asyncio.Task[None]]:
    """Run a heartbeat task for the duration of the block."""
    task = asyncio.create_task(heartbeat.run())
    try:
        yield task
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_reports_every_registered_job() -> None:
    """Send one heartbeat carrying every registered job id."""
    fake = FakeClient()
    heartbeat = WorkerHeartbeat(cast(KitaruAPIClient, fake), fake.worker_id, 0.02)
    first, second = uuid.uuid4(), uuid.uuid4()
    heartbeat.register(first)
    heartbeat.register(second)

    async with running_heartbeat(heartbeat):
        async with asyncio.timeout(10):
            while not fake.heartbeat_job_ids:
                await asyncio.sleep(0.01)

    assert sorted(fake.heartbeat_job_ids[0]) == sorted([first, second])
    assert fake.heartbeat_worker_ids[0] == fake.worker_id


async def test_skips_the_request_when_nothing_is_registered() -> None:
    """Skip the heartbeat request while no job is registered."""
    fake = FakeClient()
    heartbeat = WorkerHeartbeat(cast(KitaruAPIClient, fake), fake.worker_id, 0.02)

    async with running_heartbeat(heartbeat):
        await asyncio.sleep(0.07)

    assert fake.heartbeat_count == 0


async def test_abandoned_job_sets_its_cancel_event() -> None:
    """Set the cancel event of a job the server reports as abandoned."""
    fake = FakeClient(cancel_on_heartbeat=True)
    heartbeat = WorkerHeartbeat(cast(KitaruAPIClient, fake), fake.worker_id, 0.02)
    job_id = uuid.uuid4()
    canceled = heartbeat.register(job_id)

    async with running_heartbeat(heartbeat):
        async with asyncio.timeout(10):
            await canceled.wait()

    assert canceled.is_set()


async def test_unregister_stops_reporting_the_job() -> None:
    """Stop reporting a job once it is unregistered."""
    fake = FakeClient()
    heartbeat = WorkerHeartbeat(cast(KitaruAPIClient, fake), fake.worker_id, 0.02)
    job_id = uuid.uuid4()
    heartbeat.register(job_id)
    heartbeat.unregister(job_id)

    async with running_heartbeat(heartbeat):
        await asyncio.sleep(0.07)

    assert fake.heartbeat_count == 0


async def test_failed_heartbeat_is_logged_and_does_not_raise() -> None:
    """Swallow a failed heartbeat request and keep the loop running."""
    fake = FakeClient()
    fake.heartbeat_error = ServerError(500, "boom")
    heartbeat = WorkerHeartbeat(cast(KitaruAPIClient, fake), fake.worker_id, 0.02)
    heartbeat.register(uuid.uuid4())

    async with running_heartbeat(heartbeat):
        async with asyncio.timeout(10):
            while fake.heartbeat_count == 0:
                await asyncio.sleep(0.01)
