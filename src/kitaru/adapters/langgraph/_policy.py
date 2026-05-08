"""Capture and durability policy models for the LangGraph adapter."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


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
    save_stream_events: bool = True
    max_stream_events: int = Field(default=500, ge=0)
    save_usage: bool = True
    capture_mode: Literal["metadata", "full"] = "metadata"


class LangGraphDurabilityPolicy(BaseModel):
    """LangGraph-owned durability settings observed by the adapter."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["sync", "async", "exit"] = "sync"
    require_thread_id: bool = True
    require_checkpointer: bool = False
    warn_without_checkpointer: bool = True
    warn_ephemeral_checkpointer: bool = True
    inspect_state_after_run: bool = True
