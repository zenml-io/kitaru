"""Capture and durability policy models for the OpenAI Agents SDK adapter."""

from pydantic import BaseModel, ConfigDict, Field

from ._utils import (
    CheckpointConfig,
    ToolCheckpointOverride,
    validate_checkpoint_config,
    validate_checkpoint_strategy,
    validate_tool_checkpoint_overrides,
)


class OpenAICapturePolicy(BaseModel):
    """Controls what the OpenAI adapter captures for observability."""

    model_config = ConfigDict(extra="forbid")

    emit_child_events: bool = True
    save_input: bool = True
    save_final_output: bool = True
    save_run_state: bool = True
    save_interruption_payloads: bool = True
    save_response_items: bool = False
    save_usage: bool = True
    correlate_otel_spans: bool = True
    include_stream_text_deltas: bool = False


class OpenAIDurabilityPolicy(BaseModel):
    """Checkpoint configuration for OpenAI Agents SDK durable boundaries."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    checkpoint_strategy: str = "calls"
    run_checkpoint_config: CheckpointConfig | None = None
    model_checkpoint_config: CheckpointConfig | None = None
    tool_checkpoint_config: CheckpointConfig | None = None
    tool_checkpoint_config_by_name: dict[str, ToolCheckpointOverride] | None = Field(
        default=None
    )
    custom_tool_checkpoint_config: CheckpointConfig | None = None
    mcp_checkpoint_config: CheckpointConfig | None = None

    def model_post_init(self, __context: object) -> None:
        self.checkpoint_strategy = validate_checkpoint_strategy(
            self.checkpoint_strategy
        )
        self.run_checkpoint_config = validate_checkpoint_config(
            self.run_checkpoint_config, context="run_checkpoint_config"
        )
        self.model_checkpoint_config = validate_checkpoint_config(
            self.model_checkpoint_config, context="model_checkpoint_config"
        )
        self.tool_checkpoint_config = validate_checkpoint_config(
            self.tool_checkpoint_config, context="tool_checkpoint_config"
        )
        self.tool_checkpoint_config_by_name = validate_tool_checkpoint_overrides(
            self.tool_checkpoint_config_by_name,
            context="tool_checkpoint_config_by_name",
        )
        self.custom_tool_checkpoint_config = validate_checkpoint_config(
            self.custom_tool_checkpoint_config,
            context="custom_tool_checkpoint_config",
        )
        self.mcp_checkpoint_config = validate_checkpoint_config(
            self.mcp_checkpoint_config, context="mcp_checkpoint_config"
        )
