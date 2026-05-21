"""Event skeletons for Claude Agent SDK adapter observability."""

from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

EventStatus = Literal["completed", "failed"]
ClaudeEventKind = Literal["invocation"]


class ClaudeEventError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exception_type: str
    message: str


class ClaudeRunEvent(BaseModel):
    """Event shape for one Claude Agent SDK invocation."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    kind: ClaudeEventKind = "invocation"
    status: EventStatus
    sequence_index: int
    run_label: str
    runner_name: str
    duration_ms: float | None = None
    session_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    artifacts: dict[str, str] = Field(default_factory=dict)
    error: ClaudeEventError | None = None


ClaudeAdapterEvent = ClaudeRunEvent
ClaudeEventsTypeAdapter = TypeAdapter(list[ClaudeAdapterEvent])


def error_from_exception(error: BaseException) -> ClaudeEventError:
    return ClaudeEventError(exception_type=type(error).__name__, message=str(error))


def dump_claude_events(events: list[ClaudeAdapterEvent]) -> list[dict[str, Any]]:
    return cast(
        list[dict[str, Any]],
        ClaudeEventsTypeAdapter.dump_python(events, mode="json"),
    )
