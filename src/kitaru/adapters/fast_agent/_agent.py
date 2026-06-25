"""Public fast-agent wrapper."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any, Literal, cast

from kitaru.analytics import AnalyticsEvent, track
from kitaru.errors import KitaruUsageError

from ._usage import FastAgentUsageSummary
from ._utils import CheckpointConfig, validate_checkpoint_config
from ._wrapping import FastAgentCallRecorder, kitaru_call_recorder, wrap_fast_agent_app

FastAgentCheckpointStrategy = Literal["calls"]


class _DefaultCallRecorder:
    pass


_DEFAULT_CALL_RECORDER = _DefaultCallRecorder()


def validate_checkpoint_strategy(value: str) -> FastAgentCheckpointStrategy:
    """Validate the initial fast-agent adapter strategy vocabulary."""
    if value == "calls":
        return cast(FastAgentCheckpointStrategy, value)
    raise KitaruUsageError(
        f"Unsupported fast-agent checkpoint strategy {value!r}. Expected 'calls'."
    )


class KitaruFastAgent:
    """Wrap a fast-agent application and install calls-mode wrappers on run.

    fast-agent still creates and runs the application. Once ``fast.run()`` has
    yielded its ``AgentApp``, Kitaru walks the active agent objects and wraps
    their model, tool, and detached-clone call paths.
    """

    def __init__(
        self,
        fast_agent: Any,
        *,
        checkpoint_strategy: FastAgentCheckpointStrategy = "calls",
        call_recorder: FastAgentCallRecorder | None | _DefaultCallRecorder = (
            _DEFAULT_CALL_RECORDER
        ),
        model_checkpoint_config: CheckpointConfig | None = None,
        tool_checkpoint_config: CheckpointConfig | None = None,
        save_usage: bool = True,
        cost_calculator: Callable[[FastAgentUsageSummary], float | None] | None = None,
    ) -> None:
        if call_recorder is not _DEFAULT_CALL_RECORDER and (
            not save_usage or cost_calculator is not None
        ):
            raise KitaruUsageError(
                "fast-agent save_usage and cost_calculator only apply when "
                "Kitaru creates the default call recorder. Pass the usage "
                "configuration to your custom recorder instead, or omit "
                "call_recorder."
            )

        self._fast_agent = fast_agent
        self._checkpoint_strategy = validate_checkpoint_strategy(checkpoint_strategy)
        self._call_recorder = (
            kitaru_call_recorder(
                model_checkpoint_config=validate_checkpoint_config(
                    model_checkpoint_config,
                    context="model_checkpoint_config",
                ),
                tool_checkpoint_config=validate_checkpoint_config(
                    tool_checkpoint_config,
                    context="tool_checkpoint_config",
                ),
                save_usage=save_usage,
                cost_calculator=cost_calculator,
            )
            if call_recorder is _DEFAULT_CALL_RECORDER
            else cast(FastAgentCallRecorder | None, call_recorder)
        )
        track(
            AnalyticsEvent.FAST_AGENT_WRAPPED,
            {
                "checkpoint_strategy": self._checkpoint_strategy,
                "call_recorder": (
                    "default"
                    if call_recorder is _DEFAULT_CALL_RECORDER
                    else "passthrough"
                    if call_recorder is None
                    else "custom"
                ),
                "has_model_checkpoint_config": model_checkpoint_config is not None,
                "has_tool_checkpoint_config": tool_checkpoint_config is not None,
                "save_usage": save_usage,
                "has_cost_calculator": cost_calculator is not None,
            },
        )

    @property
    def fast_agent(self) -> Any:
        return self._fast_agent

    @property
    def checkpoint_strategy(self) -> FastAgentCheckpointStrategy:
        return self._checkpoint_strategy

    @asynccontextmanager
    async def run(self) -> AsyncIterator[Any]:
        """Run the wrapped fast-agent app and wrap active agents after startup."""
        async with self._fast_agent.run() as app:
            wrap_fast_agent_app(app, recorder=self._call_recorder)
            yield app
