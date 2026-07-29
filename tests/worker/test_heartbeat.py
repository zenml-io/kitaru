"""Worker heartbeat tests."""

import asyncio
import contextlib
import uuid
from types import SimpleNamespace
from typing import Any, cast

from kitaru.worker.heartbeat import WorkerHeartbeat


class Workers:
    def __init__(self, canceled: list[uuid.UUID]) -> None:
        self.canceled = canceled
        self.calls: list[tuple[uuid.UUID, Any]] = []
        self.called = asyncio.Event()

    async def heartbeat(self, worker_id, request):
        self.calls.append((worker_id, request))
        self.called.set()
        return SimpleNamespace(cancel_task_ids=self.canceled)


async def test_heartbeat_batches_tasks_and_sets_live_cancel_event() -> None:
    worker_id = uuid.uuid4()
    canceled_id = uuid.uuid4()
    removed_id = uuid.uuid4()
    workers = Workers([canceled_id, removed_id])
    client = cast(Any, SimpleNamespace(workers=workers))
    heartbeat = WorkerHeartbeat(client, worker_id, interval=0.01)
    canceled = heartbeat.register(canceled_id)
    heartbeat.register(removed_id)
    heartbeat.unregister(removed_id)

    task = asyncio.create_task(heartbeat.run())
    await asyncio.wait_for(workers.called.wait(), timeout=1)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert workers.calls[0][0] == worker_id
    assert workers.calls[0][1].task_ids == [canceled_id]
    assert canceled.is_set()


async def test_heartbeat_skips_request_when_no_tasks() -> None:
    workers = Workers([])
    client = cast(Any, SimpleNamespace(workers=workers))
    heartbeat = WorkerHeartbeat(client, uuid.uuid4(), interval=0.01)

    task = asyncio.create_task(heartbeat.run())
    await asyncio.sleep(0.03)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert not workers.calls
