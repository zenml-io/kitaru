"""Runtime-local child-event tracking for the PydanticAI adapter."""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field


def _normalize_agent_name(agent_name: str | None) -> str:
    """Normalize an agent name for artifact and metadata keys."""
    raw = (agent_name or "agent").strip()
    if not raw:
        raw = "agent"
    normalized = re.sub(r"\W+", "_", raw).strip("_")
    return normalized or "agent"


@dataclass
class ChildEventTracker:
    """Track model/tool child-event lineage for one agent run."""

    agent_name: str
    run_label: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    _counter: int = 0
    _current_model_event_id: str | None = None
    _pending_tool_event_ids: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Normalize adapter-visible naming fields."""
        self.agent_name = _normalize_agent_name(self.agent_name)

    @property
    def current_model_event_id(self) -> str | None:
        """Return the most recently completed model event ID."""
        return self._current_model_event_id

    def next_event_id(self, event_type: str) -> str:
        """Allocate a stable child-event identifier for this run."""
        normalized_type = event_type.strip().lower()
        if normalized_type not in {"llm_call", "tool_call"}:
            raise ValueError(f"Unsupported child event type: {event_type!r}.")
        self._counter += 1
        return f"{self.agent_name}_{self.run_label}_{normalized_type}_{self._counter}"

    def parent_ids_for_model(self) -> list[str]:
        """Return parent IDs for the next model call."""
        return list(self._pending_tool_event_ids)

    def mark_model_complete(self, event_id: str) -> None:
        """Record a completed model event and reset pending tool fan-in."""
        self._current_model_event_id = event_id
        self._pending_tool_event_ids.clear()

    def mark_tool_start(self, event_id: str) -> list[str]:
        """Record a tool start and return its parent model event IDs."""
        parent_ids: list[str] = []
        if self._current_model_event_id is not None:
            parent_ids.append(self._current_model_event_id)
        self._pending_tool_event_ids.append(event_id)
        return parent_ids


_CURRENT_TRACKER: ContextVar[ChildEventTracker | None] = ContextVar(
    "kitaru_pydantic_ai_child_event_tracker",
    default=None,
)


@contextmanager
def tracker_scope(agent_name: str | None) -> Iterator[ChildEventTracker]:
    """Install a child-event tracker for one wrapped agent run."""
    tracker = ChildEventTracker(agent_name=agent_name or "agent")
    tracker_token = _CURRENT_TRACKER.set(tracker)
    try:
        yield tracker
    finally:
        _CURRENT_TRACKER.reset(tracker_token)


def get_current_tracker() -> ChildEventTracker | None:
    """Return the active run-local child-event tracker, if any."""
    return _CURRENT_TRACKER.get()
