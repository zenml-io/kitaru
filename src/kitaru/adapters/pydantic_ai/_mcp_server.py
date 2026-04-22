"""Kitaru wrapper for PydanticAI ``MCPServer`` instances."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pydantic_ai.tools import AgentDepsT

from ._events import ToolsetKind
from ._policy import CapturePolicy
from ._utils import CheckpointConfig, ToolCheckpointOverrides
from ._toolset import KitaruToolset

if TYPE_CHECKING:
    from pydantic_ai.mcp import MCPServer


@dataclass
class KitaruMCPServer(KitaruToolset[AgentDepsT]):
    """Tracked toolset wrapper for an ``MCPServer``; instantiate via :func:`kitaruify_toolset`."""

    toolset_kind: ToolsetKind = field(default='mcp', init=False)
    _default_checkpoint_type: str = field(default='mcp_call', init=False)

    @property
    def _server(self) -> MCPServer:
        from pydantic_ai.mcp import MCPServer

        assert isinstance(self.wrapped, MCPServer)
        return self.wrapped


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
