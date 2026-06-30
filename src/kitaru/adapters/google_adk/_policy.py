"""Capture and durability policies for the Google ADK adapter."""

from __future__ import annotations

from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field

from ._constants import (
    DEFAULT_MODEL_CHECKPOINT_TYPE,
    DEFAULT_SUMMARY_CHECKPOINT_TYPE,
    DEFAULT_TOOL_CHECKPOINT_TYPE,
)
from ._utils import (
    CheckpointConfig,
    ToolCheckpointOverride,
    validate_checkpoint_config,
    validate_tool_checkpoint_overrides,
    with_default_type,
)


class ADKCapturePolicy(BaseModel):
    """Controls what the Google ADK adapter captures for observability."""

    model_config = ConfigDict(extra="forbid")

    emit_call_events: bool = True
    save_input: bool = True
    save_output: bool = True
    save_events: bool = True
    save_usage: bool = True
    save_model_input: bool = True
    save_model_response: bool = True
    save_model_usage: bool = True
    save_tool_args: bool = True
    save_tool_result: bool = True
    fail_on_event_persistence_error: bool = False
    capture_mode: Literal["metadata", "full"] = "metadata"


class ADKCallCheckpointPolicy(BaseModel):
    """Checkpoint settings for ADK explicit-wrapper calls mode.

    ``KitaruADKRunner(checkpoint_strategy="calls")`` installs this policy in
    the tracker context for explicit ``KitaruADKModel`` and ``KitaruADKTool``
    wrappers. Current public Google ADK plugin callbacks are still only
    before/after/error hooks; they do not give Kitaru a callable that performs
    the model or tool operation. ``require_true_call_hooks`` is therefore a
    guard for plugin-only observation paths, not a blocker for explicit-wrapper
    calls mode.
    """

    model_config = ConfigDict(extra="forbid")

    model_checkpoint_config: CheckpointConfig | Literal[False] | None = None
    tool_checkpoint_config: CheckpointConfig | Literal[False] | None = None
    tool_checkpoint_config_by_name: dict[str, ToolCheckpointOverride] = Field(
        default_factory=dict
    )
    summary_checkpoint_config: CheckpointConfig | None = None
    persist_run_artifacts: bool = True
    nested_checkpoint_policy: Literal["error", "metadata_only"] = "error"
    require_true_call_hooks: bool = Field(
        default=True,
        description=(
            "When True, refuse plugin-only observation paths because ADK "
            "before/after callbacks cannot place provider/tool work inside a "
            "checkpoint. Explicit KitaruADKModel/KitaruADKTool calls mode does "
            "not use this gate. Set False to opt into metadata-only plugin "
            "observation."
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
    policy: ADKCallCheckpointPolicy,
) -> CheckpointConfig | None:
    """Return the effective model-call checkpoint config, or ``None`` if off."""
    if policy.model_checkpoint_config is False:
        return None
    config = cast(CheckpointConfig, policy.model_checkpoint_config or {})
    return with_default_type(config, DEFAULT_MODEL_CHECKPOINT_TYPE)


def resolve_tool_call_checkpoint_config(
    policy: ADKCallCheckpointPolicy,
    *,
    tool_name: str,
) -> CheckpointConfig | None:
    """Return the effective tool-call checkpoint config, or ``None`` if off."""
    override = policy.tool_checkpoint_config_by_name.get(tool_name)
    if override is False:
        return None
    if override is not None:
        return with_default_type(override, DEFAULT_TOOL_CHECKPOINT_TYPE)
    if policy.tool_checkpoint_config is False:
        return None
    config = cast(CheckpointConfig, policy.tool_checkpoint_config or {})
    return with_default_type(config, DEFAULT_TOOL_CHECKPOINT_TYPE)


def resolve_summary_checkpoint_config(
    policy: ADKCallCheckpointPolicy,
) -> CheckpointConfig:
    """Return the effective calls-mode summary checkpoint config."""
    config = cast(CheckpointConfig, policy.summary_checkpoint_config or {})
    return with_default_type(config, DEFAULT_SUMMARY_CHECKPOINT_TYPE)
