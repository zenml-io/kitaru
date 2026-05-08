"""Kitaru wrapper for PydanticAI ``MCPServer`` instances."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from pydantic_ai.tools import AgentDepsT, RunContext
from pydantic_ai.toolsets import ToolsetTool

from ._events import ToolsetKind
from ._logging import logger
from ._policy import CapturePolicy
from ._utils import CheckpointConfig, ToolCheckpointOverrides
from ._toolset import KitaruToolset

if TYPE_CHECKING:
    from pydantic_ai.mcp import MCPServer


_MISSING = object()


def _mcp_server_is_running(server: object) -> bool:
    """Return whether a PydanticAI MCP server is already lifecycle-open."""
    try:
        is_running = getattr(server, "is_running", _MISSING)
    except Exception:
        return False

    if is_running is not _MISSING:
        try:
            if callable(is_running):
                return bool(cast(Callable[[], object], is_running)())
            return bool(is_running)
        except Exception:
            return False

    try:
        running_count = getattr(server, "_running_count", None)
        if running_count is not None:
            return bool(running_count)
        return getattr(server, "_client", None) is not None
    except Exception:
        return False


@dataclass
class KitaruMCPServer(KitaruToolset[AgentDepsT]):
    """Tracked toolset wrapper for an ``MCPServer``; instantiate via :func:`kitaruify_toolset`."""

    toolset_kind: ToolsetKind = field(default="mcp", init=False)
    _default_checkpoint_type: str = field(default="mcp_call", init=False)
    _warned_running_mcp_checkpoint_bypass: bool = field(
        default=False,
        init=False,
        repr=False,
    )

    @property
    def _server(self) -> MCPServer:
        from pydantic_ai.mcp import MCPServer

        assert isinstance(self.wrapped, MCPServer)
        return self.wrapped

    def _should_use_tool_checkpoint(
        self,
        *,
        name: str,
        ctx: RunContext[AgentDepsT],
        tool: ToolsetTool[AgentDepsT],
        checkpoint_config: CheckpointConfig | None,
    ) -> bool:
        if not super()._should_use_tool_checkpoint(
            name=name,
            ctx=ctx,
            tool=tool,
            checkpoint_config=checkpoint_config,
        ):
            return False
        if _mcp_server_is_running(self.wrapped):
            self._warn_running_mcp_checkpoint_bypass_once(name)
            return False
        return True

    def _warn_running_mcp_checkpoint_bypass_once(self, tool_name: str) -> None:
        if self._warned_running_mcp_checkpoint_bypass:
            return
        self._warned_running_mcp_checkpoint_bypass = True
        logger.warning(
            "Kitaru detected an already-running PydanticAI MCP server while "
            "calling tool %r. The MCP tool call will stay on the current event "
            "loop instead of opening a granular MCP checkpoint, which avoids "
            "moving loop-bound MCP lifecycle resources across threads. The call "
            "is still tracked as an adapter tool event, but it is not persisted "
            "as its own per-call checkpoint.",
            tool_name,
        )


def kitaruify_mcp_server(
    server: MCPServer,
    *,
    capture: CapturePolicy,
    tool_checkpoint_config: CheckpointConfig | None = None,
    tool_checkpoint_config_by_name: ToolCheckpointOverrides | None = None,
) -> KitaruMCPServer[AgentDepsT]:
    return KitaruMCPServer(
        server,
        capture=capture,
        tool_checkpoint_config=tool_checkpoint_config,
        tool_checkpoint_config_by_name=tool_checkpoint_config_by_name,
    )
