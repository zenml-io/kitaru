"""Kitaru MCP server entrypoint.

This module is intentionally isolated behind the optional ``kitaru[mcp]`` extra.
Importing ``kitaru`` must continue to work without MCP dependencies installed.
"""

from __future__ import annotations

import importlib

_MCP_INSTALL_ERROR = (
    "MCP server dependencies are not installed. Install with: pip install kitaru[mcp]"
)


def _require_mcp_dependencies() -> None:
    """Ensure optional MCP dependencies are available."""
    try:
        importlib.import_module("mcp.server.fastmcp")
    except ImportError:
        raise ImportError(_MCP_INSTALL_ERROR) from None


_require_mcp_dependencies()


def main() -> object:
    """Run the Kitaru MCP server entrypoint lazily."""
    return importlib.import_module("kitaru.mcp.server").main()


__all__ = ["main"]
