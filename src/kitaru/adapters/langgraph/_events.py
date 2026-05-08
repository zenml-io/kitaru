"""Event skeletons for LangGraph adapter observability."""

from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

EventStatus = Literal["completed", "failed", "interrupted"]
LangGraphEventKind = Literal[
    "graph_call_started",
    "graph_call_completed",
    "graph_call_failed",
    "graph_interrupted",
    "checkpoint_state_captured",
]


class LangGraphEventError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exception_type: str
    message: str


class LangGraphRunEvent(BaseModel):
    """Minimal graph-level event shape for LangGraph tracking."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    kind: LangGraphEventKind
    status: EventStatus
    sequence_index: int
    run_label: str
    graph_name: str
    duration_ms: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    artifacts: dict[str, str] = Field(default_factory=dict)
    error: LangGraphEventError | None = None


LangGraphAdapterEvent = LangGraphRunEvent
LangGraphEventsTypeAdapter = TypeAdapter(list[LangGraphAdapterEvent])


def error_from_exception(error: BaseException) -> LangGraphEventError:
    return LangGraphEventError(exception_type=type(error).__name__, message=str(error))


def dump_langgraph_events(events: list[LangGraphAdapterEvent]) -> list[dict[str, Any]]:
    return cast(
        list[dict[str, Any]],
        LangGraphEventsTypeAdapter.dump_python(events, mode="json"),
    )
