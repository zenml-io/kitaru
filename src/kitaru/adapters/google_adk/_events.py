"""Event models for Google ADK adapter observability."""

from __future__ import annotations

from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

ADKEventStatus = Literal["completed", "failed", "metadata_only"]
ADKEventKind = Literal[
    "runner_call_started",
    "runner_call_completed",
    "runner_call_failed",
    "agent_call",
    "model_call",
    "tool_call",
    "adk_event",
    "error",
]


class ADKEventError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exception_type: str
    message: str


class ADKRunEvent(BaseModel):
    """Observed ADK event or adapter-created checkpoint event."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    kind: ADKEventKind
    status: ADKEventStatus
    sequence_index: int
    run_label: str
    runner_name: str
    checkpoint_id: str | None = None
    checkpoint_name: str | None = None
    model_name: str | None = None
    tool_name: str | None = None
    tool_call_id: str | None = None
    duration_ms: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    artifacts: dict[str, str] = Field(default_factory=dict)
    error: ADKEventError | None = None


ADKAdapterEvent = ADKRunEvent
ADKEventsTypeAdapter = TypeAdapter(list[ADKAdapterEvent])


def error_from_exception(error: BaseException) -> ADKEventError:
    return ADKEventError(exception_type=type(error).__name__, message=str(error))


def dump_adk_events(events: list[ADKAdapterEvent]) -> list[dict[str, Any]]:
    return cast(
        list[dict[str, Any]],
        ADKEventsTypeAdapter.dump_python(events, mode="json"),
    )
