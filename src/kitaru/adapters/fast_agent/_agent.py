"""Public fast-agent wrapper."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Literal, cast

from kitaru.errors import KitaruUsageError

from ._wrapping import FastAgentCallRecorder, wrap_fast_agent_app

FastAgentCheckpointStrategy = Literal["calls"]


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
        call_recorder: FastAgentCallRecorder | None = None,
    ) -> None:
        self._fast_agent = fast_agent
        self._checkpoint_strategy = validate_checkpoint_strategy(checkpoint_strategy)
        self._call_recorder = call_recorder

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
