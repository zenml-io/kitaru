"""Compatibility helpers for PydanticAI MCP optional dependencies."""

from __future__ import annotations


def ensure_pydantic_ai_mcp_import_compat() -> None:
    """Install import aliases needed by supported PydanticAI/MCP combinations.

    PydanticAI 1.86+ imports ``streamable_http_client`` from the MCP SDK. The
    MCP version currently compatible with Kitaru's ZenML server dependency still
    exposes the same helper as ``streamablehttp_client``. Adding the alias lets
    ``pydantic_ai.mcp`` import without forcing an unsatisfiable MCP upgrade.
    """
    try:
        from mcp.client import streamable_http
    except ImportError:
        return

    if hasattr(streamable_http, "streamable_http_client"):
        return
    legacy_client = getattr(streamable_http, "streamablehttp_client", None)
    if legacy_client is not None:
        streamable_http.__dict__["streamable_http_client"] = legacy_client
