from typing import Annotated, Any, Literal, cast

from kitaru._llm_usage import LLMBillingEffect, LLMCacheStatus

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from pydantic_ai.usage import RequestUsage

from ._policy import CaptureMode

EventStatus = Literal["completed", "failed"]
ToolsetKind = Literal["function", "mcp", "generic"]
DeferredKind = Literal["hitl", "approval_required", "call_deferred"]


class EventError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exception_type: str
    message: str


class _BaseEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    status: EventStatus
    sequence_index: int
    turn_index: int
    parent_event_ids: list[str] = Field(default_factory=list)
    checkpoint_name: str | None = None
    checkpoint_id: str | None = None


class PydanticAIUsageSummary(BaseModel):
    """Kitaru-owned usage input for Pydantic AI cost calculators."""

    model_config = ConfigDict(extra="forbid")

    adapter_name: Literal["pydantic_ai"] = "pydantic_ai"
    provider: str | None = None
    model_name: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cached_input_tokens: int | None = None
    reasoning_tokens: int | None = None
    raw_usage: dict[str, Any] | None = None


class ModelEvent(_BaseEvent):
    kind: Literal["llm_call"] = "llm_call"
    fan_in_from: list[str] = Field(default_factory=list)
    duration_ms: float | None = None
    artifacts: dict[str, str] = Field(default_factory=dict)
    model_name: str | None = None
    provider_name: str | None = None
    usage: RequestUsage | None = None
    billing_effect: LLMBillingEffect = "incurred"
    cache_status: LLMCacheStatus = "executed"
    error: EventError | None = None
    stream_event_count: int | None = None


class ToolEvent(_BaseEvent):
    kind: Literal["tool_call"] = "tool_call"
    tool_name: str
    toolset_kind: ToolsetKind
    hitl: bool = False
    capture_mode: CaptureMode
    fan_out_from: str | None = None
    duration_ms: float | None = None
    artifacts: dict[str, str] = Field(default_factory=dict)
    error: EventError | None = None


class DeferredEvent(_BaseEvent):
    kind: Literal["deferred"] = "deferred"
    tool_name: str
    deferred_kind: DeferredKind
    wait_name: str
    metadata: dict[str, Any] | None = None
    approved: bool | None = None


class StreamEvent(_BaseEvent):
    kind: Literal["event_stream"] = "event_stream"
    index: int
    duration_ms: float
    error: EventError | None = None


AgentEvent = Annotated[
    ModelEvent | ToolEvent | DeferredEvent | StreamEvent, Field(discriminator="kind")
]
AgentEventsTypeAdapter = TypeAdapter(list[AgentEvent])


class EventStreamHandlerSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    call_count: int
    duration_ms: float


class RunSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_name: str
    run_label: str
    status: EventStatus
    model_call_count: int
    tool_call_count: int
    total_events: int
    turn_count: int
    event_ids_in_order: list[str]
    duration_ms: float
    checkpoint_name: str | None = None
    checkpoint_id: str | None = None
    exec_id: str | None = None
    event_stream_handler: EventStreamHandlerSummary | None = None
    error: EventError | None = None


def error_from_exception(error: BaseException) -> EventError:
    return EventError(exception_type=type(error).__name__, message=str(error))


def dump_agent_events(events: list[AgentEvent]) -> list[dict[str, Any]]:
    return cast(
        list[dict[str, Any]], AgentEventsTypeAdapter.dump_python(events, mode="json")
    )
