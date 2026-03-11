"""Tests for optional MCP dependency import boundaries."""

from __future__ import annotations

import importlib
import sys
import types

import pytest


def _simulate_missing_mcp_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force imports to behave as if the optional `mcp` package is missing."""
    monkeypatch.setitem(sys.modules, "mcp", None)
    monkeypatch.setitem(sys.modules, "mcp.server", None)
    monkeypatch.setitem(sys.modules, "mcp.server.fastmcp", None)


def test_importing_kitaru_does_not_require_mcp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Base SDK import should work even when MCP extras are unavailable."""
    _simulate_missing_mcp_dependency(monkeypatch)

    module = importlib.import_module("kitaru")
    reloaded = importlib.reload(module)

    assert reloaded.__name__ == "kitaru"


def test_importing_kitaru_mcp_without_extra_raises_clear_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Importing MCP entrypoint without extras should raise a helpful error."""
    _simulate_missing_mcp_dependency(monkeypatch)
    sys.modules.pop("kitaru.mcp", None)

    with pytest.raises(ImportError, match=r"kitaru\[mcp\]"):
        importlib.import_module("kitaru.mcp")


def test_importing_kitaru_mcp_keeps_server_lazy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Importing `kitaru.mcp` should not import the server module eagerly."""
    original_mcp = sys.modules.get("kitaru.mcp")
    original_server = sys.modules.get("kitaru.mcp.server")

    try:
        sys.modules.pop("kitaru.mcp", None)
        sys.modules.pop("kitaru.mcp.server", None)

        monkeypatch.setitem(sys.modules, "mcp", types.ModuleType("mcp"))
        monkeypatch.setitem(
            sys.modules,
            "mcp.server",
            types.ModuleType("mcp.server"),
        )
        monkeypatch.setitem(
            sys.modules,
            "mcp.server.fastmcp",
            types.ModuleType("mcp.server.fastmcp"),
        )

        module = importlib.import_module("kitaru.mcp")
        reloaded = importlib.reload(module)

        assert callable(reloaded.main)
        assert "kitaru.mcp.server" not in sys.modules
    finally:
        if original_mcp is not None:
            sys.modules["kitaru.mcp"] = original_mcp
        else:
            sys.modules.pop("kitaru.mcp", None)

        if original_server is not None:
            sys.modules["kitaru.mcp.server"] = original_server
        else:
            sys.modules.pop("kitaru.mcp.server", None)
