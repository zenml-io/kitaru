"""Event skeletons for OpenAI Agents SDK adapter observability."""

from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

EventStatus = Literal["completed", "failed"]
OpenAIEventKind = Literal[
    "runner_call",
    "llm_call",
    "tool_call",
    "custom_tool_call",
    "mcp_call",
    "handoff",
    "interruption",
    "hosted_tool_observed",
]


class OpenAIEventError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exception_type: str
    message: str


class OpenAIRunEvent(BaseModel):
    """Minimal event shape shared by later adapter tracking work."""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    kind: OpenAIEventKind
    status: EventStatus
    sequence_index: int
    run_label: str
    agent_name: str
    checkpoint_id: str | None = None
    checkpoint_name: str | None = None
    duration_ms: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    artifacts: dict[str, str] = Field(default_factory=dict)
    error: OpenAIEventError | None = None


OpenAIAdapterEvent = OpenAIRunEvent
OpenAIEventsTypeAdapter = TypeAdapter(list[OpenAIAdapterEvent])


def error_from_exception(error: BaseException) -> OpenAIEventError:
    return OpenAIEventError(exception_type=type(error).__name__, message=str(error))


def dump_openai_events(events: list[OpenAIAdapterEvent]) -> list[dict[str, Any]]:
    return cast(
        list[dict[str, Any]],
        OpenAIEventsTypeAdapter.dump_python(events, mode="json"),
    )
