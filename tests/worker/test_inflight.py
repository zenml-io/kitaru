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


def test_cancel_all_does_not_mark_any_task_released() -> None:
    """cancel_all() sets events without marking any task released."""
    inflight = InflightTasks()
    task_id = uuid.uuid4()
    event = inflight.register(task_id)

    inflight.cancel_all()

    assert event.is_set()
    assert inflight.was_released(task_id) is False


def test_release_all_sets_every_event_and_marks_it_released() -> None:
    """release_all() sets the event of every registered task and marks it released."""
    inflight = InflightTasks()
    task_ids = [uuid.uuid4() for _ in range(3)]
    events = [inflight.register(task_id) for task_id in task_ids]

    inflight.release_all()

    assert all(event.is_set() for event in events)
    assert all(inflight.was_released(task_id) for task_id in task_ids)


def test_unregister_clears_the_released_mark() -> None:
    """unregister() drops a task's released mark along with its entry."""
    inflight = InflightTasks()
    task_id = uuid.uuid4()
    inflight.register(task_id)
    inflight.release_all()

    inflight.unregister(task_id)

    assert inflight.was_released(task_id) is False
