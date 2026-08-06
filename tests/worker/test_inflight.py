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
"""Tests for the in-flight task registry."""

import asyncio
import uuid

from kitaru.worker.inflight import InflightTasks


def test_register_returns_an_event_and_unregister_drops_it() -> None:
    """register() returns a fresh event, unregister() drops the entry."""
    inflight = InflightTasks()
    task_id = uuid.uuid4()

    event = inflight.register(task_id)
    assert isinstance(event, asyncio.Event)
    assert not event.is_set()
    assert inflight.get_ids() == [task_id]

    inflight.unregister(task_id)
    assert inflight.get_ids() == []


def test_cancel_sets_only_the_registered_event() -> None:
    """cancel() sets the task's event and ignores unknown ids."""
    inflight = InflightTasks()
    task_id = uuid.uuid4()
    event = inflight.register(task_id)

    inflight.cancel(uuid.uuid4())
    assert not event.is_set()

    inflight.cancel(task_id)
    assert event.is_set()


def test_cancel_all_sets_every_event() -> None:
    """cancel_all() sets the event of every registered task."""
    inflight = InflightTasks()
    events = [inflight.register(uuid.uuid4()) for _ in range(3)]

    inflight.cancel_all()

    assert all(event.is_set() for event in events)
