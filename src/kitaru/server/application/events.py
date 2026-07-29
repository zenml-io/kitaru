"""In-process application event dispatch."""

from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from kitaru.server.domain.job import Job
from kitaru.server.domain.replay import Replay
from kitaru.server.domain.task import Task, TaskStatus


@dataclass(frozen=True)
class TaskTerminal:
    """A task entered a terminal state."""

    task: Task
    previous_status: TaskStatus


@dataclass(frozen=True)
class JobSettled:
    """A job entered a terminal state."""

    job: Job


@dataclass(frozen=True)
class ReplaySettled:
    """A replay entered a terminal state."""

    replay: Replay


EventT = TypeVar("EventT")
EventHandler = Callable[[Any], Awaitable[None]]


class EventRegistry:
    """Dispatch application events sequentially in the request transaction."""

    def __init__(self) -> None:
        self._handlers: dict[type[Any], list[EventHandler]] = defaultdict(list)

    def register(
        self, event_type: type[EventT], handler: Callable[[EventT], Awaitable[None]]
    ) -> None:
        """Register one event subscriber."""
        self._handlers[event_type].append(handler)

    async def dispatch(self, event: object) -> None:
        """Await every subscriber in registration order."""
        for handler in self._handlers[type(event)]:
            await handler(event)
