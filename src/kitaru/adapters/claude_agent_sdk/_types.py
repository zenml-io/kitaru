"""Public serializable models for the Claude Agent SDK adapter."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from kitaru.checkpoint import _raise_if_checkpoint_output_handle


class ClaudeRunRequest(BaseModel):
    """Serializable input to ``KitaruClaudeRunner.run(...)`` / ``run_sync(...)``."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    kind: Literal["start", "resume"]
    prompt: str
    resume_session_id: str | None = None
    cwd: str | None = None
    max_turns: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def start(
        cls,
        prompt: str,
        *,
        cwd: str | None = None,
        max_turns: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "ClaudeRunRequest":
        """Build a request for a fresh Claude Agent SDK invocation."""
        return cls(
            kind="start",
            prompt=prompt,
            cwd=cwd,
            max_turns=max_turns,
            metadata=metadata or {},
        )

    @classmethod
    def resume(
        cls,
        prompt: str,
        session_id: str,
        *,
        cwd: str | None = None,
        max_turns: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "ClaudeRunRequest":
        """Build a request that resumes a Claude Agent SDK session."""
        return cls(
            kind="resume",
            prompt=prompt,
            resume_session_id=session_id,
            cwd=cwd,
            max_turns=max_turns,
            metadata=metadata or {},
        )

    @field_validator("prompt")
    @classmethod
    def _validate_prompt(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Claude run prompt must be non-empty.")
        return value

    @field_validator("prompt", mode="before")
    @classmethod
    def _reject_prompt_checkpoint_handle(cls, value: Any) -> Any:
        _raise_if_checkpoint_output_handle(
            value,
            field_name="ClaudeRunRequest.prompt",
            expected="a materialized string prompt",
        )
        return value

    @field_validator("resume_session_id")
    @classmethod
    def _validate_resume_session_id(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("resume_session_id must be non-empty when provided.")
        return value

    @field_validator("resume_session_id", mode="before")
    @classmethod
    def _reject_resume_session_id_checkpoint_handle(cls, value: Any) -> Any:
        _raise_if_checkpoint_output_handle(
            value,
            field_name="ClaudeRunRequest.resume_session_id",
            expected="a materialized Claude session id string",
        )
        return value

    @field_validator("cwd", mode="before")
    @classmethod
    def _reject_cwd_checkpoint_handle(cls, value: Any) -> Any:
        _raise_if_checkpoint_output_handle(
            value,
            field_name="ClaudeRunRequest.cwd",
            expected="a materialized filesystem path string",
        )
        return value

    @field_validator("max_turns")
    @classmethod
    def _validate_max_turns(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("max_turns must be positive when provided.")
        return value

    @model_validator(mode="after")
    def _validate_kind_contract(self) -> "ClaudeRunRequest":
        if self.kind == "start" and self.resume_session_id is not None:
            raise ValueError("kind='start' forbids resume_session_id.")
        if self.kind == "resume" and self.resume_session_id is None:
            raise ValueError("kind='resume' requires resume_session_id.")
        return self


class ClaudeUsageSummary(BaseModel):
    """Kitaru-owned usage input for Claude Agent SDK cost calculators."""

    model_config = ConfigDict(extra="forbid")

    adapter_name: Literal["claude_agent_sdk"] = "claude_agent_sdk"
    provider: str | None = "anthropic"
    model_name: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cached_input_tokens: int | None = None
    reasoning_tokens: int | None = None
    raw_usage: dict[str, Any] | None = None
    raw_model_usage: dict[str, Any] | None = None


class ClaudeRunResult(BaseModel):
    """Serializable output from one Claude Agent SDK invocation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    status: Literal["completed"] = "completed"
    session_id: str | None = None
    final_text: str | None = None
    transcript_path: str | None = None
    usage: dict[str, Any] | None = None
    cost_usd: float | None = None
    model_usage: dict[str, Any] | None = None
    stop_reason: str | None = None
    subtype: str | None = None
    num_turns: int | None = None
    duration_ms: float | None = None
    duration_api_ms: float | None = None

    messages_artifact_name: str | None = None
    transcript_artifact_name: str | None = None
    options_manifest_artifact_name: str | None = None
    output_artifact_name: str | None = None
    usage_artifact_name: str | None = None
    event_log_artifact_name: str | None = None
    run_summary_artifact_name: str | None = None

    warnings: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
