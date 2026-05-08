"""PydanticAI MCP lifecycle regression tests for the Kitaru adapter."""

from __future__ import annotations

import asyncio
import importlib
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from textwrap import dedent
from typing import Any

import pytest


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _import_mcp_stdio() -> Any:
    pytest.importorskip("pydantic_ai")
    import kitaru.adapters.pydantic_ai  # noqa: F401

    try:
        from pydantic_ai.mcp import MCPServerStdio
    except ImportError as exc:  # pragma: no cover - failure message is the test value
        pytest.fail(f"pydantic_ai.mcp import failed: {exc}")
    return MCPServerStdio


def test_pydantic_ai_mcp_import_smoke() -> None:
    kitaru_adapter = importlib.import_module("kitaru.adapters.pydantic_ai")
    pydantic_ai = importlib.import_module("pydantic_ai")

    MCPServerStdio = _import_mcp_stdio()
    assert "Agent" in pydantic_ai.__dict__
    assert "KitaruAgent" in kitaru_adapter.__dict__
    assert MCPServerStdio is not None
    try:
        import mcp.server.fastmcp  # noqa: F401
    except ImportError as exc:  # pragma: no cover - failure message is the test value
        pytest.fail(f"mcp.server.fastmcp import failed: {exc}")


def _write_echo_server(tmp_path: Path) -> Path:
    server_path = tmp_path / "echo_mcp_server.py"
    server_path.write_text(
        dedent(
            """
            from mcp.server.fastmcp import FastMCP

            mcp = FastMCP("kitaru-test-mcp")

            @mcp.tool()
            def echo(text: str) -> str:
                return f"echo:{text}"

            if __name__ == "__main__":
                mcp.run()
            """
        ).strip()
        + "\n"
    )
    return server_path


def _run_context() -> Any:
    from pydantic_ai.models.test import TestModel
    from pydantic_ai.tools import RunContext
    from pydantic_ai.usage import RunUsage

    return RunContext(deps=None, model=TestModel(), usage=RunUsage())


@pytest.mark.anyio
async def test_preopened_mcp_server_call_stays_on_current_loop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import anyio

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    MCPServerStdio = _import_mcp_stdio()
    server = MCPServerStdio(
        sys.executable,
        args=[str(_write_echo_server(tmp_path))],
        read_timeout=2,
        timeout=5,
        cache_tools=True,
    )
    wrapped = kitaruify_toolset(
        server,
        capture=CapturePolicy(correlate_otel_spans=False),
        mcp_checkpoint_config={},
    )

    async def fail_checkpoint(**_kwargs: Any) -> Any:
        raise AssertionError("pre-opened MCP calls must not cross the thread bridge")

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.run_async_in_checkpoint",
        fail_checkpoint,
    )

    async with server:
        ctx = _run_context()
        tool = (await wrapped.get_tools(ctx))["echo"]
        with anyio.fail_after(5), _flow_scope(name="preopened_mcp_flow"):
            result = await wrapped.call_tool(
                "echo",
                {"text": "preopened"},
                ctx,
                tool,
            )

    assert result == "echo:preopened"


@pytest.mark.anyio
async def test_auto_connect_mcp_server_still_uses_granular_checkpoint_bridge(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import anyio

    from kitaru.adapters.pydantic_ai import CapturePolicy
    from kitaru.adapters.pydantic_ai._toolset import kitaruify_toolset
    from kitaru.runtime import _flow_scope

    MCPServerStdio = _import_mcp_stdio()
    server = MCPServerStdio(
        sys.executable,
        args=[str(_write_echo_server(tmp_path))],
        read_timeout=2,
        timeout=5,
        cache_tools=True,
    )
    wrapped = kitaruify_toolset(
        server,
        capture=CapturePolicy(correlate_otel_spans=False),
        mcp_checkpoint_config={},
    )
    checkpoint_called = False

    async def bridge_checkpoint(
        *,
        body: Callable[[], Awaitable[Any]],
        **_kwargs: Any,
    ) -> Any:
        nonlocal checkpoint_called
        checkpoint_called = True

        async def run_body() -> Any:
            return await body()

        return await asyncio.to_thread(lambda: asyncio.run(run_body()))

    monkeypatch.setattr(
        "kitaru.adapters.pydantic_ai._toolset.run_async_in_checkpoint",
        bridge_checkpoint,
    )

    ctx = _run_context()
    tool = (await wrapped.get_tools(ctx))["echo"]
    with anyio.fail_after(5), _flow_scope(name="auto_connect_mcp_flow"):
        result = await wrapped.call_tool("echo", {"text": "auto"}, ctx, tool)

    assert result == "echo:auto"
    assert checkpoint_called is True
