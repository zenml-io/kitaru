"""Provider-neutral data models for imported agent traces."""

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class SourceObservationType(StrEnum):
    """Observation types currently emitted by supported trace providers."""

    AGENT = "AGENT"
    CHAIN = "CHAIN"
    GENERATION = "GENERATION"
    RETRIEVER = "RETRIEVER"
    SPAN = "SPAN"
    TOOL = "TOOL"


class ObservationKind(StrEnum):
    """Provider-neutral semantic role of one observation."""

    AGENT_CALL = "agent_call"
    CHAIN = "chain"
    LLM_CALL = "llm_call"
    RETRIEVAL_CALL = "retrieval_call"
    SPAN = "span"
    TOOL_CALL = "tool_call"


class ObservationStatus(StrEnum):
    """Normalized terminal state reported by the source provider."""

    SUCCESS = "success"
    ERROR = "error"
    UNKNOWN = "unknown"


class TraceIntegrity(StrEnum):
    """How completely the exported observations describe a trace graph."""

    COMPLETE = "complete"
    ROOT_OMITTED = "root_omitted"
    FRAGMENTED = "fragmented"
    INVALID = "invalid"


class TraceSource(BaseModel):
    """Stable external identity of an imported trace."""

    provider: str
    project_id: str
    trace_id: str

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("provider", "project_id", "trace_id")
    @classmethod
    def _require_nonempty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("source identity fields cannot be empty")
        return value

    @property
    def identity(self) -> tuple[str, str, str]:
        """Return the provider-scoped identity used for idempotent imports."""
        return (self.provider, self.project_id, self.trace_id)


class TraceUsage(BaseModel):
    """Token or unit consumption reported for one observation."""

    input: int | float | None = None
    output: int | float | None = None
    total: int | float | None = None
    unit: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid", frozen=True)


class TraceCost(BaseModel):
    """Cost reported for one observation."""

    input: float | None = None
    output: float | None = None
    total: float | None = None
    currency: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid", frozen=True)


class ImportedObservation(BaseModel):
    """One provider observation preserved in a canonical trace graph."""

    id: str
    trace_id: str
    parent_id: str | None = None
    name: str
    source_type: SourceObservationType
    kind: ObservationKind
    started_at: datetime
    ended_at: datetime | None = None
    status: ObservationStatus = ObservationStatus.UNKNOWN
    status_message: str | None = None
    input_present: bool = False
    output_present: bool = False
    input: Any = None
    output: Any = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    model: str | None = None
    usage: TraceUsage | None = None
    cost: TraceCost | None = None
    latency_ms: float | None = None

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("id", "trace_id", "name")
    @classmethod
    def _require_nonempty(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("observation identity and name fields cannot be empty")
        return value

    @field_validator("started_at", "ended_at")
    @classmethod
    def _require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("observation timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def _validate_interval(self) -> "ImportedObservation":
        if self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("observation ended_at cannot precede started_at")
        return self


class ImportedTrace(BaseModel):
    """A normalized external trace ready to persist as a synthetic execution."""

    source: TraceSource
    observations: list[ImportedObservation]
    integrity: TraceIntegrity
    name: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    input_present: bool = False
    output_present: bool = False
    input: Any = None
    output: Any = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    missing_parent_ids: list[str] = Field(default_factory=list)
    component_count: int = 1
    content_digest: str

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("started_at", "ended_at")
    @classmethod
    def _require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.utcoffset() is None:
            raise ValueError("trace timestamps must include a timezone")
        return value

    @field_validator("content_digest")
    @classmethod
    def _validate_digest(cls, value: str) -> str:
        if len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError("content_digest must be a lowercase SHA-256 digest")
        return value

    @model_validator(mode="after")
    def _validate_graph_summary(self) -> "ImportedTrace":
        if self.component_count < 1:
            raise ValueError("component_count must be at least one")
        if (
            self.ended_at is not None
            and self.started_at is not None
            and self.ended_at < self.started_at
        ):
            raise ValueError("trace ended_at cannot precede started_at")

        observation_ids = [observation.id for observation in self.observations]
        if len(observation_ids) != len(set(observation_ids)):
            raise ValueError("observation ids must be unique within a trace")
        if any(
            observation.trace_id != self.source.trace_id
            for observation in self.observations
        ):
            raise ValueError("all observations must belong to the source trace")
        return self
