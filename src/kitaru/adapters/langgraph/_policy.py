"""Capture and durability policy models for the LangGraph adapter."""

from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ._types import LangGraphStreamMode
from ._utils import (
    CheckpointConfig,
    ToolCheckpointOverride,
    validate_checkpoint_config,
    validate_tool_checkpoint_overrides,
    with_default_type,
)


class LangGraphCapturePolicy(BaseModel):
    """Controls what the LangGraph adapter captures for observability."""

    model_config = ConfigDict(extra="forbid")

    save_input: bool = True
    save_output: bool = True
    save_config: bool = True
    save_context: bool = False
    save_state_snapshot: bool = True
    save_state_values: bool = False
    save_state_tasks: bool = True
    save_usage: bool = True
    emit_call_events: bool = True
    save_model_input: bool = Field(
        default=True,
        description=(
            "Persist a redacted structural model-input envelope for sync "
            "calls-mode checkpoints; raw message/system text is omitted."
        ),
    )
    save_model_response: bool = Field(
        default=True,
        description=(
            "Record the true model checkpoint output as an event artifact "
            "reference. Disabling this removes the event reference only; the "
            "checkpoint still stores its return value for replay."
        ),
    )
    save_model_usage: bool = True
    save_tool_args: bool = True
    save_tool_result: bool = Field(
        default=True,
        description=(
            "Record the true tool checkpoint output as an event artifact "
            "reference. Disabling this removes the event reference only; the "
            "checkpoint still stores its return value for replay."
        ),
    )
    fail_on_event_persistence_error: bool = False
    capture_mode: Literal["metadata", "full"] = "metadata"


class LangGraphCallCheckpointPolicy(BaseModel):
    """Checkpoint settings for LangGraph calls-mode model/tool boundaries.

    Async LangChain hooks are currently metadata-only. The
    ``async_checkpoint_policy`` field is present to make that explicit and to
    leave room for a future opt-in mode, but the only accepted value today is
    ``"metadata_only"``.
    """

    model_config = ConfigDict(extra="forbid")

    model_checkpoint_config: CheckpointConfig | Literal[False] | None = None
    tool_checkpoint_config: CheckpointConfig | Literal[False] | None = None
    tool_checkpoint_config_by_name: dict[str, ToolCheckpointOverride] = Field(
        default_factory=dict
    )
    summary_checkpoint_config: CheckpointConfig | None = None
    persist_run_artifacts: bool = Field(
        default=True,
        description=(
            "In calls mode, persist the aggregate event log and run summary. "
            "Set to False to skip both summary checkpoint persistence and "
            "direct event/run-summary metadata persistence."
        ),
    )
    nested_checkpoint_policy: Literal["error", "metadata_only"] = "error"
    async_checkpoint_policy: Literal["metadata_only"] = Field(
        default="metadata_only",
        description=(
            "Async LangChain model/tool hooks do not open true Kitaru "
            "checkpoints yet; the only supported policy today is "
            "'metadata_only'."
        ),
    )

    def model_post_init(self, __context: object) -> None:
        self.model_checkpoint_config = _validate_optional_call_config(
            self.model_checkpoint_config,
            context="model_checkpoint_config",
        )
        self.tool_checkpoint_config = _validate_optional_call_config(
            self.tool_checkpoint_config,
            context="tool_checkpoint_config",
        )
        self.tool_checkpoint_config_by_name = (
            validate_tool_checkpoint_overrides(
                self.tool_checkpoint_config_by_name,
                context="tool_checkpoint_config_by_name",
            )
            or {}
        )
        self.summary_checkpoint_config = validate_checkpoint_config(
            self.summary_checkpoint_config,
            context="summary_checkpoint_config",
        )


def _validate_optional_call_config(
    config: CheckpointConfig | Literal[False] | None,
    *,
    context: str,
) -> CheckpointConfig | Literal[False] | None:
    if config is False:
        return False
    return validate_checkpoint_config(config, context=context)


def resolve_model_checkpoint_config(
    policy: LangGraphCallCheckpointPolicy,
) -> CheckpointConfig | None:
    """Return the effective model-call checkpoint config, or ``None`` if off."""
    if policy.model_checkpoint_config is False:
        return None
    config = cast(CheckpointConfig, policy.model_checkpoint_config or {})
    return with_default_type(config, "model_call")


def resolve_tool_call_checkpoint_config(
    policy: LangGraphCallCheckpointPolicy,
    *,
    tool_name: str,
) -> CheckpointConfig | None:
    """Return the effective tool-call checkpoint config, or ``None`` if off."""
    override = policy.tool_checkpoint_config_by_name.get(tool_name)
    if override is False:
        return None
    if override is not None:
        return with_default_type(override, "tool_call")
    if policy.tool_checkpoint_config is False:
        return None
    config = cast(CheckpointConfig, policy.tool_checkpoint_config or {})
    return with_default_type(config, "tool_call")


def resolve_summary_checkpoint_config(
    policy: LangGraphCallCheckpointPolicy,
) -> CheckpointConfig:
    """Return the effective calls-mode summary checkpoint config."""
    config = cast(CheckpointConfig, policy.summary_checkpoint_config or {})
    return with_default_type(config, "langgraph_summary")


class LangGraphStreamPolicy(BaseModel):
    """Controls best-effort live LangGraph stream event payloads."""

    model_config = ConfigDict(extra="forbid")

    default_modes: tuple[LangGraphStreamMode, ...] = (
        "messages",
        "updates",
        "custom",
    )
    include_raw_payloads: bool = False
    allow_debug: bool = False
    include_custom_payload: bool = True
    max_display_chars: int = Field(default=240, ge=1, le=4000)

    @field_validator("default_modes")
    @classmethod
    def _validate_default_modes(
        cls, value: tuple[LangGraphStreamMode, ...]
    ) -> tuple[LangGraphStreamMode, ...]:
        if not value:
            raise ValueError("default_modes must include at least one stream mode.")
        return tuple(dict.fromkeys(value))

    @model_validator(mode="after")
    def _validate_debug_default(self) -> "LangGraphStreamPolicy":
        if "debug" in self.default_modes and not self.allow_debug:
            raise ValueError(
                "default_modes cannot include 'debug' unless allow_debug=True."
            )
        return self


class LangGraphDurabilityPolicy(BaseModel):
    """LangGraph-owned durability settings observed by the adapter."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["sync", "async", "exit"] = "sync"
    require_thread_id: bool = True
    require_checkpointer: bool = False
    warn_without_checkpointer: bool = True
    warn_ephemeral_checkpointer: bool = True
    inspect_state_after_run: bool = True
