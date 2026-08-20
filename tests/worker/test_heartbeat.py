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

import httpx
from fakes import FakeKitaruAPIClient, as_client

from kitaru.api_models.v1.worker import WorkerHeartbeatResponse
from kitaru.client.exceptions import ServerError
from kitaru.worker.heartbeat import WorkerHeartbeat
from kitaru.worker.inflight import InflightTasks


async def test_run_skips_the_request_when_nothing_is_registered() -> None:
    """An interval with no registered tasks sends no heartbeat request."""
    client = FakeKitaruAPIClient()
    heartbeat = WorkerHeartbeat(
        as_client(client), uuid.uuid4(), InflightTasks(), interval=0.01
    )

    task = asyncio.create_task(heartbeat.run())
    await asyncio.sleep(0.03)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert client.workers.heartbeats == []


async def test_run_sends_registered_task_ids_and_sets_canceled_events() -> None:
    """A heartbeat batches registered ids and sets events the server cancels."""
    client = FakeKitaruAPIClient()
    worker_id = uuid.uuid4()
    inflight = InflightTasks()
    heartbeat = WorkerHeartbeat(as_client(client), worker_id, inflight, interval=0.01)

    task_id_a = uuid.uuid4()
    task_id_b = uuid.uuid4()
    event_a = inflight.register(task_id_a)
    event_b = inflight.register(task_id_b)

    client.workers.heartbeat_responses.append(
        WorkerHeartbeatResponse(cancel_task_ids=[task_id_a])
    )

    task = asyncio.create_task(heartbeat.run())
    await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert len(client.workers.heartbeats) >= 1
    sent_worker_id, request = client.workers.heartbeats[0]
    assert sent_worker_id == worker_id
    assert set(request.task_ids) == {task_id_a, task_id_b}
    assert event_a.is_set()
    assert not event_b.is_set()


async def test_run_resolves_cancel_ids_against_the_live_registry() -> None:
    """A cancel id for a task unregistered before the response arrives is ignored."""
    client = FakeKitaruAPIClient()
    inflight = InflightTasks()
    heartbeat = WorkerHeartbeat(
        as_client(client), uuid.uuid4(), inflight, interval=0.01
    )
    task_id = uuid.uuid4()
    inflight.register(task_id)
    client.workers.heartbeat_responses.append(
        WorkerHeartbeatResponse(cancel_task_ids=[task_id])
    )
    inflight.unregister(task_id)

    task = asyncio.create_task(heartbeat.run())
    await asyncio.sleep(0.03)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    # No error raised despite the cancel id no longer being registered.
    assert task_id not in inflight.get_ids()


async def test_run_tolerates_a_failed_heartbeat_request() -> None:
    """A failed heartbeat request is logged and does not stop the loop."""
    client = FakeKitaruAPIClient()
    inflight = InflightTasks()
    heartbeat = WorkerHeartbeat(
        as_client(client), uuid.uuid4(), inflight, interval=0.01
    )
    task_id = uuid.uuid4()
    inflight.register(task_id)
    client.workers.heartbeat_responses.append(ServerError(500, "boom"))
    client.workers.heartbeat_responses.append(
        WorkerHeartbeatResponse(cancel_task_ids=[task_id])
    )

    task = asyncio.create_task(heartbeat.run())
    # Poll for the heartbeat after the failed one instead of sleeping a fixed
    # window, which a loaded runner can overshoot before the loop runs twice.
    deadline = asyncio.get_running_loop().time() + 5.0
    while (
        len(client.workers.heartbeats) < 2
        and asyncio.get_running_loop().time() < deadline
    ):
        await asyncio.sleep(0.01)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert len(client.workers.heartbeats) >= 2


async def test_run_tolerates_a_transport_error() -> None:
    """A raw transport error is logged and does not stop the loop."""
    client = FakeKitaruAPIClient()
    inflight = InflightTasks()
    heartbeat = WorkerHeartbeat(
        as_client(client), uuid.uuid4(), inflight, interval=0.01
    )
    inflight.register(uuid.uuid4())
    client.workers.heartbeat_responses.append(httpx.ConnectError("down"))

    task = asyncio.create_task(heartbeat.run())
    await asyncio.sleep(0.03)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert len(client.workers.heartbeats) >= 1
