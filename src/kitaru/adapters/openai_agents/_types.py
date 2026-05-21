"""Public serializable models for the OpenAI Agents SDK adapter."""

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from kitaru.checkpoint import _raise_if_checkpoint_output_handle_in_value


class OpenAIRunStateEnvelope(BaseModel):
    """Checkpoint-safe wrapper around serialized OpenAI ``RunState``."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    agents_sdk_version: str
    state_json: dict[str, Any]
    strict_sdk_version: bool = True
    context_codec: str | None = None
    created_at_iso: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
    warnings: list[str] = Field(default_factory=list)


class OpenAIInterruptionSummary(BaseModel):
    """Human-readable and machine-addressable pending interruption summary."""

    model_config = ConfigDict(extra="forbid")

    index: int
    kind: str
    tool_name: str | None = None
    call_id: str | None = None
    message: str | None = None
    arguments: dict[str, Any] | None = None
    arguments_preview: str | None = None


class OpenAIApprovalDecision(BaseModel):
    """Pure data object for approving or rejecting one interruption."""

    model_config = ConfigDict(extra="forbid")

    interruption_index: int = 0
    approve: bool
    rejection_message: str | None = None

    @model_validator(mode="after")
    def _validate_rejection_message(self) -> "OpenAIApprovalDecision":
        if self.approve and self.rejection_message is not None:
            raise ValueError("Approved decisions must not include a rejection message.")
        if not self.approve and self.rejection_message == "":
            raise ValueError("Rejection messages must be non-empty when provided.")
        return self


OpenAIInput = str | list[dict[str, Any]]


class OpenAIRunRequest(BaseModel):
    """Serializable input to ``KitaruRunner.run(...)`` / ``run_sync(...)``."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    kind: Literal["start", "resume"]
    input: OpenAIInput | None = None
    pending_state: OpenAIRunStateEnvelope | None = None
    decision: OpenAIApprovalDecision | None = None
    max_turns: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def start(
        cls,
        input: OpenAIInput,
        *,
        max_turns: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "OpenAIRunRequest":
        """Build a request for a fresh OpenAI agent run."""
        return cls(
            kind="start",
            input=input,
            max_turns=max_turns,
            metadata=metadata or {},
        )

    @classmethod
    def resume(
        cls,
        pending_state: OpenAIRunStateEnvelope,
        decision: OpenAIApprovalDecision,
        *,
        max_turns: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "OpenAIRunRequest":
        """Build a request that resumes a paused OpenAI run."""
        return cls(
            kind="resume",
            pending_state=pending_state,
            decision=decision,
            max_turns=max_turns,
            metadata=metadata or {},
        )

    @field_validator("input")
    @classmethod
    def _validate_input(cls, value: OpenAIInput | None) -> OpenAIInput | None:
        if value is None or isinstance(value, str):
            return value
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return value
        raise ValueError("OpenAI run input must be a string or list of dictionaries.")

    @field_validator("input", mode="before")
    @classmethod
    def _reject_input_checkpoint_handle(cls, value: Any) -> Any:
        _raise_if_checkpoint_output_handle_in_value(
            value,
            field_name="OpenAIRunRequest.input",
            expected="materialized OpenAI input content",
        )
        return value

    @model_validator(mode="after")
    def _validate_kind_contract(self) -> "OpenAIRunRequest":
        if self.kind == "start":
            if self.input is None:
                raise ValueError("kind='start' requires input.")
            if self.pending_state is not None or self.decision is not None:
                raise ValueError("kind='start' forbids pending_state and decision.")
        if self.kind == "resume":
            if self.pending_state is None or self.decision is None:
                raise ValueError("kind='resume' requires pending_state and decision.")
            if self.input is not None:
                raise ValueError("kind='resume' forbids input.")
        return self


class OpenAIUsageSummary(BaseModel):
    """Normalized usage details extracted from an OpenAI Agents SDK run."""

    model_config = ConfigDict(extra="forbid")

    model_name: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    raw: dict[str, Any] | None = None


class OpenAIRunResult(BaseModel):
    """Serializable output from an OpenAI run."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    status: Literal["completed", "interrupted"]
    final_output: Any | None = None
    pending_state: OpenAIRunStateEnvelope | None = None
    interruptions: list[OpenAIInterruptionSummary] = Field(default_factory=list)
    last_response_id: str | None = None
    usage: dict[str, Any] | None = None
    estimated_cost_usd: float | None = None
    event_log_artifact_name: str | None = None
    run_summary_artifact_name: str | None = None
    state_artifact_name: str | None = None
    output_artifact_name: str | None = None
    warnings: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_status_contract(self) -> "OpenAIRunResult":
        if self.status == "completed" and self.pending_state is not None:
            raise ValueError(
                "Completed OpenAI run results must not include pending_state."
            )
        if self.status == "interrupted":
            if self.pending_state is None:
                raise ValueError(
                    "Interrupted OpenAI run results require pending_state."
                )
            if not self.interruptions:
                raise ValueError(
                    "Interrupted OpenAI run results require at least one interruption."
                )
        return self
