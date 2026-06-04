"""Public serializable models for the LangGraph adapter."""

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

LangGraphStreamMode = Literal[
    "values",
    "updates",
    "messages",
    "custom",
    "checkpoints",
    "tasks",
    "debug",
]


class LangGraphInterruptSummary(BaseModel):
    """Serializable summary of one pending LangGraph interrupt."""

    model_config = ConfigDict(extra="forbid")

    index: int
    interrupt_id: str | None = None
    value: Any | None = None
    resumable: bool = True
    namespace: str | None = None
    task_id: str | None = None
    node_name: str | None = None


class LangGraphStateSummary(BaseModel):
    """Safe, metadata-first summary of a LangGraph state snapshot."""

    model_config = ConfigDict(extra="forbid")

    latest_checkpoint_id: str | None = None
    checkpoint_ns: str | None = None
    next_nodes: list[str] = Field(default_factory=list)
    interrupts: list[LangGraphInterruptSummary] = Field(default_factory=list)
    values: Any | None = None
    tasks: Any | None = None


class LangGraphPendingState(BaseModel):
    """Checkpoint-safe pointer to an interrupted LangGraph thread."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    thread_id: str
    checkpoint_id: str | None = None
    checkpoint_ns: str | None = None
    next_nodes: list[str] = Field(default_factory=list)
    interrupts: list[LangGraphInterruptSummary] = Field(default_factory=list)
    created_at_iso: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
    warnings: list[str] = Field(default_factory=list)

    @field_validator("thread_id")
    @classmethod
    def _validate_thread_id(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("thread_id must be a non-empty stable string.")
        return value


class LangGraphRunRequest(BaseModel):
    """Serializable input to ``KitaruGraphRunner.invoke(...)``."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    schema_version: Literal[1] = 1
    kind: Literal["start", "resume"]
    input: Any | None = None
    command: Any | None = None
    thread_id: str
    checkpoint_id: str | None = None
    checkpoint_ns: str | None = None
    context: Any | None = None
    configurable: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)
    durability: Literal["sync", "async", "exit"] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def start(
        cls,
        input: Any,
        *,
        thread_id: str,
        checkpoint_id: str | None = None,
        checkpoint_ns: str | None = None,
        context: Any | None = None,
        configurable: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        durability: Literal["sync", "async", "exit"] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "LangGraphRunRequest":
        """Build a request for a fresh LangGraph invocation."""
        return cls(
            kind="start",
            input=input,
            thread_id=thread_id,
            checkpoint_id=checkpoint_id,
            checkpoint_ns=checkpoint_ns,
            context=context,
            configurable=configurable or {},
            config=config or {},
            durability=durability,
            metadata=metadata or {},
        )

    @classmethod
    def resume(
        cls,
        command: Any,
        *,
        thread_id: str,
        checkpoint_id: str | None = None,
        checkpoint_ns: str | None = None,
        context: Any | None = None,
        configurable: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        durability: Literal["sync", "async", "exit"] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "LangGraphRunRequest":
        """Build a request that resumes an interrupted LangGraph thread."""
        return cls(
            kind="resume",
            command=command,
            thread_id=thread_id,
            checkpoint_id=checkpoint_id,
            checkpoint_ns=checkpoint_ns,
            context=context,
            configurable=configurable or {},
            config=config or {},
            durability=durability,
            metadata=metadata or {},
        )

    @field_validator("thread_id")
    @classmethod
    def _validate_thread_id(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("thread_id must be a non-empty stable string.")
        return value

    @field_validator("config", "configurable", "metadata")
    @classmethod
    def _validate_dict(cls, value: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise ValueError("value must be a dictionary.")
        return value

    @model_validator(mode="after")
    def _validate_kind_contract(self) -> "LangGraphRunRequest":
        provided_fields = self.model_fields_set
        if self.kind == "start":
            if "input" not in provided_fields:
                raise ValueError("kind='start' requires input.")
            if self.command is not None:
                raise ValueError("kind='start' forbids command.")
        if self.kind == "resume":
            if self.command is None:
                raise ValueError("kind='resume' requires command.")
            if self.input is not None:
                raise ValueError("kind='resume' forbids input.")
        return self


LangGraphResumeRequest = LangGraphRunRequest


class LangGraphUsageSummary(BaseModel):
    """Best-effort usage details extracted from a graph output."""

    model_config = ConfigDict(extra="forbid")

    model_name: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    raw: dict[str, Any] | None = None


class LangGraphRunResult(BaseModel):
    """Serializable output from a LangGraph invocation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    status: Literal["completed", "interrupted", "failed"]
    output: Any | None = None
    thread_id: str
    latest_checkpoint_id: str | None = None
    next_nodes: list[str] = Field(default_factory=list)
    interrupts: list[LangGraphInterruptSummary] = Field(default_factory=list)
    pending_state: LangGraphPendingState | None = None
    state_summary: LangGraphStateSummary | None = None
    state_artifact_name: str | None = None
    output_artifact_name: str | None = None
    event_log_artifact_name: str | None = None
    run_summary_artifact_name: str | None = None
    usage: LangGraphUsageSummary | None = None
    estimated_cost_usd: float | None = None
    warnings: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_status_contract(self) -> "LangGraphRunResult":
        if self.status == "interrupted":
            if not self.interrupts:
                raise ValueError(
                    "Interrupted LangGraph run results require interrupts."
                )
            if self.pending_state is None:
                raise ValueError(
                    "Interrupted LangGraph run results require pending_state."
                )
        if self.status == "completed" and self.pending_state is not None:
            raise ValueError(
                "Completed LangGraph run results must not include pending_state."
            )
        return self
