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
"""In-flight task registry with per-task cancellation events."""

import asyncio
import uuid


class InflightTasks:
    """Tracks the tasks the worker holds and their cancellation events."""

    def __init__(self) -> None:
        """Initialize the registry."""
        self._registered: dict[uuid.UUID, asyncio.Event] = {}

    def register(self, task_id: uuid.UUID) -> asyncio.Event:
        """Report a task as held by this worker.

        Args:
            task_id: Id of the task now held.

        Returns:
            Event set when cancellation of the task is requested.
        """
        event = asyncio.Event()
        self._registered[task_id] = event
        return event

    def unregister(self, task_id: uuid.UUID) -> None:
        """Stop reporting a task as held by this worker.

        Args:
            task_id: Id of the task no longer held.
        """
        self._registered.pop(task_id, None)

    def get_ids(self) -> list[uuid.UUID]:
        """List the ids of the held tasks.

        Returns:
            Held task ids.
        """
        return list(self._registered)

    def cancel(self, task_id: uuid.UUID) -> None:
        """Request cancellation of a held task, ignoring unknown ids.

        Args:
            task_id: Id of the task to cancel.
        """
        event = self._registered.get(task_id)
        if event is not None:
            event.set()

    def cancel_all(self) -> None:
        """Request cancellation of every held task."""
        for event in self._registered.values():
            event.set()
