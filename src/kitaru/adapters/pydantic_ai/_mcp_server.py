"""Kitaru wrapper for PydanticAI ``MCPServer`` instances.

Subclass of :class:`KitaruToolset` that tags MCP calls with
``toolset_kind='mcp'`` and honors ``MCPServer.cache_tools`` to skip redundant
``tools/list`` round-trips on checkpoint re-entry.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pydantic_ai.tools import AgentDepsT, RunContext, ToolDefinition
from pydantic_ai.toolsets import ToolsetTool

from ._events import ToolsetKind
from ._policy import CapturePolicy
from ._toolset import KitaruToolset

if TYPE_CHECKING:
    from pydantic_ai.mcp import MCPServer


@dataclass
class KitaruMCPServer(KitaruToolset[AgentDepsT]):
    """Tracked toolset wrapper for an ``MCPServer``; instantiate via :func:`kitaruify_toolset`."""

    toolset_kind: ToolsetKind = field(default='mcp', init=False)
    _default_checkpoint_type: str = field(default='mcp_call', init=False)

    def __post_init__(self) -> None:
        self._cached_tool_defs: dict[str, ToolDefinition] | None = None

    @property
    def _server(self) -> MCPServer:
        from pydantic_ai.mcp import MCPServer

        assert isinstance(self.wrapped, MCPServer)
        return self.wrapped

    def tool_for_tool_def(self, tool_def: ToolDefinition) -> ToolsetTool[AgentDepsT]:
        return self._server.tool_for_tool_def(tool_def)

    async def get_tools(
        self, ctx: RunContext[AgentDepsT]
    ) -> dict[str, ToolsetTool[AgentDepsT]]:
        if self._server.cache_tools and self._cached_tool_defs is not None:
            return {
                name: self.tool_for_tool_def(tool_def)
                for name, tool_def in self._cached_tool_defs.items()
            }

        result = await super().get_tools(ctx)
        if self._server.cache_tools:
            self._cached_tool_defs = {name: tool.tool_def for name, tool in result.items()}
        return result


def kitaruify_mcp_server(
    server: MCPServer, *, capture: CapturePolicy
) -> KitaruMCPServer[AgentDepsT]:
    return KitaruMCPServer(server, capture=capture)
