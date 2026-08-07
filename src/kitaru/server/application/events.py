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
"""In-process domain event registry."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from kitaru.api_models.v1.task import TaskStatus
from kitaru.server.domain.job import Job
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.session import Session
from kitaru.server.domain.task import Task


@dataclass(frozen=True)
class Event:
    """Domain event."""


@dataclass(frozen=True)
class TaskTerminal(Event):
    """Task terminal event."""

    task: Task
    previous_status: TaskStatus


@dataclass(frozen=True)
class SessionImportFinalized(Event):
    """Session import finalized event."""

    session: Session


@dataclass(frozen=True)
class JobsSettled(Event):
    """Jobs settled event."""

    jobs: list[Job]


@dataclass(frozen=True)
class ReplaysSettled(Event):
    """Replays settled event."""

    replays: list[Replay]


EventT = TypeVar("EventT", bound=Event)

EventHandler = Callable[[Any], Awaitable[None]]


class EventDispatcher:
    """Registry dispatching events to their handlers in registration order."""

    def __init__(self) -> None:
        """Initialize the dispatcher."""
        self._handlers: dict[type[Event], list[EventHandler]] = {}

    def register(
        self, event_type: type[EventT], handler: Callable[[EventT], Awaitable[None]]
    ) -> None:
        """Add a handler for an event type.

        Args:
            event_type: Event type the handler listens for.
            handler: Handler awaited on dispatch.
        """
        self._handlers.setdefault(event_type, []).append(handler)

    async def dispatch(self, event: Event) -> None:
        """Await every handler registered for the event's type, in order.

        Handlers run on the caller's transaction, so they commit or roll back
        with the transition that emitted the event.

        Args:
            event: Event to dispatch.
        """
        for handler in self._handlers.get(type(event), []):
            await handler(event)
