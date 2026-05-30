"""Event skeletons for Gemini Interactions adapter observability."""

from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

EventStatus = Literal["completed", "failed"]
GeminiEventKind = Literal["interaction"]


class GeminiInteractionEventError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exception_type: str
    message: str


class GeminiInteractionRunEvent(BaseModel):
    """Event shape for one Gemini interaction boundary."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    kind: GeminiEventKind = "interaction"
    status: EventStatus
    sequence_index: int
    run_label: str
    runner_name: str
    duration_ms: float | None = None
    interaction_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    artifacts: dict[str, str] = Field(default_factory=dict)
    error: GeminiInteractionEventError | None = None


GeminiAdapterEvent = GeminiInteractionRunEvent
GeminiEventsTypeAdapter = TypeAdapter(list[GeminiAdapterEvent])


def error_from_exception(error: BaseException) -> GeminiInteractionEventError:
    return GeminiInteractionEventError(
        exception_type=type(error).__name__,
        message="Exception message redacted by default.",
    )


def dump_gemini_events(events: list[GeminiAdapterEvent]) -> list[dict[str, Any]]:
    return cast(
        list[dict[str, Any]],
        GeminiEventsTypeAdapter.dump_python(events, mode="json"),
    )
