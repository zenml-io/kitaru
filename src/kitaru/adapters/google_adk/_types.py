"""Public serializable models for the Google ADK adapter."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from kitaru.checkpoint import _raise_if_checkpoint_output_handle_in_value

from ._outputs import final_output_preview as final_output_preview


class ADKRunRequest(BaseModel):
    """Serializable input for one Google ADK runner turn."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    user_id: str
    session_id: str
    message: Any
    run_kwargs: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("user_id", "session_id")
    @classmethod
    def _validate_stable_id(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("must be a stable non-empty string")
        return value

    @field_validator("run_kwargs", "metadata")
    @classmethod
    def _validate_mapping(cls, value: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise ValueError("must be a dictionary")
        return value

    @model_validator(mode="after")
    def _reject_checkpoint_handles(self) -> ADKRunRequest:
        _raise_if_checkpoint_output_handle_in_value(
            self.message,
            field_name="message",
            expected="a materialized ADK message payload",
        )
        _raise_if_checkpoint_output_handle_in_value(
            self.run_kwargs,
            field_name="run_kwargs",
            expected="materialized ADK runner keyword arguments",
        )
        return self


class ADKHandoffRequest(BaseModel):
    """Serializable description of a pending ADK human-action request.

    The ``kind`` field determines which optional fields are meaningful:

    - ``tool_confirmation``: ``function_call_id`` is the original tool call id,
      ``request_function_call_id`` is ADK's synthetic confirmation request id,
      and ``tool_name`` / ``tool_args`` / ``message`` describe what needs human
      approval.
    - ``credential_request``: ``function_call_id`` is the original tool call id,
      ``request_function_call_id`` is ADK's synthetic credential request id, and
      ``auth_config`` contains ADK's serialized auth request.
    - ``human_input``: ``function_call_id`` is the workflow interrupt id,
      ``message`` / ``payload`` / ``response_schema`` describe the requested
      reply.

    This remains one Pydantic model, rather than a discriminated union, so older
    serialized results continue to validate and round-trip unchanged.
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["tool_confirmation", "credential_request", "human_input"]
    event_id: str | None = None
    invocation_id: str | None = None
    author: str | None = None
    function_call_id: str | None = None
    request_function_call_id: str | None = None
    tool_name: str | None = None
    tool_args: dict[str, Any] | None = None
    message: str | None = None
    payload: Any | None = None
    auth_config: dict[str, Any] | None = None
    response_schema: Any | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("tool_args", "auth_config", "raw")
    @classmethod
    def _validate_optional_mapping(
        cls,
        value: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if value is not None and not isinstance(value, Mapping):
            raise ValueError("must be a dictionary when provided")
        return dict(value) if value is not None else None


class ADKUsageSummary(BaseModel):
    """Best-effort token/cost usage extracted from ADK events."""

    model_config = ConfigDict(extra="forbid")

    model_name: str | None = None
    provider_name: str | None = None
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    cached_input_tokens: int | None = Field(default=None, ge=0)
    reasoning_tokens: int | None = Field(default=None, ge=0)
    raw: dict[str, Any] | None = None

    @field_validator("raw")
    @classmethod
    def _validate_raw(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is not None and not isinstance(value, Mapping):
            raise ValueError("raw must be a dictionary when provided")
        return dict(value) if value is not None else None


class ADKRunResult(BaseModel):
    """Serializable result for one Google ADK runner turn."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    status: Literal["completed", "requires_action", "failed"]
    final_output: Any | None = None
    events: list[dict[str, Any]] = Field(default_factory=list)
    handoffs: list[ADKHandoffRequest] = Field(default_factory=list)
    state_delta: Any | None = None
    usage: ADKUsageSummary | None = None
    estimated_cost_usd: float | None = None
    event_log_artifact_name: str | None = None
    run_summary_artifact_name: str | None = None
    output_artifact_name: str | None = None
    warnings: list[str] = Field(default_factory=list)

    @field_validator("events")
    @classmethod
    def _validate_events(cls, value: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            raise ValueError("events must be a list")
        for event in value:
            if not isinstance(event, dict):
                raise ValueError("events must contain dictionaries")
        return value

    @field_validator("handoffs")
    @classmethod
    def _validate_handoffs(
        cls,
        value: list[ADKHandoffRequest],
    ) -> list[ADKHandoffRequest]:
        if not isinstance(value, list):
            raise ValueError("handoffs must be a list")
        return value
