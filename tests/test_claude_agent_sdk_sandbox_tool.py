"""Tests for the Claude Agent SDK Kitaru sandbox MCP tool."""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
import types
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import ANY

import pytest

from kitaru._config._sandbox import SandboxCommandResult
from kitaru.errors import (
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
)


def _purge_claude_adapter_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.claude_agent_sdk"):
            monkeypatch.delitem(sys.modules, cached, raising=False)


@pytest.fixture
def fake_claude_sdk(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    sdk = types.ModuleType("claude_agent_sdk")
    sdk.__dict__["tool_calls"] = []
    sdk.__dict__["server_calls"] = []

    def tool(name: str, description: str, input_schema: dict[str, Any]) -> Any:
        sdk.__dict__["tool_calls"].append(
            {"name": name, "description": description, "input_schema": input_schema}
        )

        def decorate(handler: Any) -> SimpleNamespace:
            return SimpleNamespace(
                name=name,
                description=description,
                input_schema=input_schema,
                handler=handler,
            )

        return decorate

    def create_sdk_mcp_server(
        *,
        name: str,
        version: str = "1.0.0",
        tools: list[Any] | None = None,
    ) -> dict[str, Any]:
        server = {"type": "sdk", "name": name, "version": version, "tools": tools or []}
        sdk.__dict__["server_calls"].append(server)
        return server

    class ClaudeAgentOptions:
        def __init__(self, **kwargs: Any) -> None:
            self.__dict__.update(kwargs)

    sdk.__dict__["tool"] = tool
    sdk.__dict__["create_sdk_mcp_server"] = create_sdk_mcp_server
    sdk.__dict__["ClaudeAgentOptions"] = ClaudeAgentOptions
    sdk.__dict__["ResultMessage"] = type("ResultMessage", (), {})
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", sdk)
    _purge_claude_adapter_modules(monkeypatch)
    return sdk


@pytest.fixture
def sandbox_tool_module(fake_claude_sdk: types.ModuleType) -> types.ModuleType:
    _ = fake_claude_sdk
    return importlib.import_module("kitaru.adapters.claude_agent_sdk._sandbox_tool")


def _payload(tool_result: dict[str, Any]) -> dict[str, Any]:
    assert set(tool_result) == {"content", "is_error"}
    content = cast(list[dict[str, str]], tool_result["content"])
    return cast(dict[str, Any], json.loads(content[0]["text"]))


def _sandbox_result(
    *,
    exit_code: int = 0,
    stdout: str = "hello\n",
    stderr: str = "warning\n",
    cleanup_error: str | None = None,
) -> SandboxCommandResult:
    return SandboxCommandResult(
        command="echo hello",
        cwd="/workspace",
        stdout=stdout,
        stderr=stderr,
        exit_code=exit_code,
        stdout_truncated=False,
        stderr_truncated=True,
        stack_id="stack-1",
        stack_name="sandboxed",
        sandbox_id="sandbox-1",
        sandbox_name="docker",
        session_id="session-1",
        cleanup="destroy",
        cleanup_succeeded=True,
        cleanup_error=cleanup_error,
    )


def test_helper_registers_sdk_mcp_server_and_metadata(
    sandbox_tool_module: types.ModuleType,
    fake_claude_sdk: types.ModuleType,
) -> None:
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server(
        server_name="kitaru_custom",
        tool_name="sandbox_exec",
        description="Run it safely",
        default_max_chars=123,
        default_cleanup="close",
    )

    assert fake_claude_sdk.__dict__["tool_calls"] == [
        {
            "name": "sandbox_exec",
            "description": "Run it safely",
            "input_schema": {
                "type": "object",
                "properties": ANY,
                "required": ["command"],
                "additionalProperties": False,
            },
        }
    ]
    assert fake_claude_sdk.__dict__["server_calls"][0]["name"] == "kitaru_custom"
    assert (
        fake_claude_sdk.__dict__["server_calls"][0]["tools"][0].name == "sandbox_exec"
    )

    metadata = sandbox_tool_module.kitaru_sandbox_mcp_metadata(server)
    assert metadata == {
        "kind": "kitaru_sandbox_command_mcp_server",
        "server_name": "kitaru_custom",
        "tool_name": "sandbox_exec",
        "allowed_tool_name": "mcp__kitaru_custom__sandbox_exec",
        "default_max_chars": 123,
        "default_cleanup": "close",
    }


def test_public_default_allowed_tool_name(
    sandbox_tool_module: types.ModuleType,
) -> None:
    assert sandbox_tool_module.KITARU_SANDBOX_MCP_SERVER_NAME == "kitaru"
    assert sandbox_tool_module.KITARU_SANDBOX_COMMAND_TOOL_NAME == "run_command"
    assert (
        sandbox_tool_module.KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME
        == "mcp__kitaru__run_command"
    )


def test_real_installed_sdk_server_shape_drives_manifest_and_cache_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("claude_agent_sdk")
    _purge_claude_adapter_modules(monkeypatch)
    sandbox_tool = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._sandbox_tool"
    )
    serialization = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._serialization"
    )
    types_module = importlib.import_module("kitaru.adapters.claude_agent_sdk._types")

    server = sandbox_tool.create_kitaru_sandbox_mcp_server(
        default_max_chars=321,
        default_cleanup="close",
    )

    assert isinstance(server, dict)
    assert server["type"] == "sdk"
    assert server["name"] == "kitaru"
    assert "instance" in server
    metadata = sandbox_tool.kitaru_sandbox_mcp_metadata(server)
    assert metadata == {
        "kind": "kitaru_sandbox_command_mcp_server",
        "server_name": "kitaru",
        "tool_name": "run_command",
        "allowed_tool_name": "mcp__kitaru__run_command",
        "default_max_chars": 321,
        "default_cleanup": "close",
    }

    manifest = serialization.redacted_options_manifest(
        {"mcp_servers": {"kitaru": server}},
        types_module.ClaudeRunRequest.start("hello"),
    )
    options = cast(dict[str, Any], manifest["options"])
    mcp_servers = cast(dict[str, Any], options["mcp_servers"])
    assert mcp_servers["kitaru"] == metadata
    assert serialization.to_cache_identity(server) == metadata


def test_handler_offloads_shared_sandbox_helper(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    offloaded: list[tuple[Any, tuple[Any, ...], dict[str, Any]]] = []

    def fake_run_sandbox_command(
        command: object, **kwargs: Any
    ) -> SandboxCommandResult:
        calls.append({"command": command, **kwargs})
        return _sandbox_result(
            stdout="secret-value\n",
            stderr="warning secret-value\n",
            cleanup_error="cleanup mentioned secret-value",
        )

    async def fake_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        offloaded.append((func, args, kwargs))
        return func(*args, **kwargs)

    monkeypatch.setattr(
        sandbox_tool_module, "_run_sandbox_command", fake_run_sandbox_command
    )
    monkeypatch.setattr(sandbox_tool_module.asyncio, "to_thread", fake_to_thread)

    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server(
        default_max_chars=456,
        default_cleanup="close",
    )
    tool = server["tools"][0]
    result = asyncio.run(
        tool.handler(
            {
                "command": ["python", "--version"],
                "cwd": "/workspace",
                "env": {"SECRET_TOKEN": "secret-value"},
            }
        )
    )

    assert offloaded and offloaded[0][0] is fake_run_sandbox_command
    assert calls == [
        {
            "command": ["python", "--version"],
            "cwd": "/workspace",
            "env": {"SECRET_TOKEN": "secret-value"},
            "max_chars": 456,
            "cleanup": "close",
        }
    ]
    payload = _payload(result)
    assert payload == {
        "status": "completed",
        "command": "echo hello",
        "cwd": "/workspace",
        "stdout": "[REDACTED_ENV]\n",
        "stderr": "warning [REDACTED_ENV]\n",
        "exit_code": 0,
        "stdout_truncated": False,
        "stderr_truncated": True,
        "stack": {"id": "stack-1", "name": "sandboxed"},
        "sandbox": {"id": "sandbox-1", "name": "docker"},
        "session_id": "session-1",
        "cleanup": {
            "policy": "destroy",
            "succeeded": True,
            "error": "cleanup mentioned [REDACTED_ENV]",
        },
    }
    assert "secret-value" not in json.dumps(payload)
    assert result["is_error"] is False


def test_non_zero_process_exit_is_completed_tool_result(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sandbox_tool_module,
        "_run_sandbox_command",
        lambda command, **kwargs: _sandbox_result(exit_code=17),
    )
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server()

    result = asyncio.run(server["tools"][0].handler({"command": "false"}))

    payload = _payload(result)
    assert payload["status"] == "completed"
    assert payload["exit_code"] == 17
    assert result["is_error"] is False


def test_handler_does_not_redact_short_ordinary_env_values(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_sandbox_command(
        command: object, **kwargs: Any
    ) -> SandboxCommandResult:
        _ = command, kwargs
        return _sandbox_result(
            stdout="debug flag is 1\n",
            stderr="exit marker 1\n",
            cleanup_error="cleanup marker 1",
        )

    monkeypatch.setattr(
        sandbox_tool_module, "_run_sandbox_command", fake_run_sandbox_command
    )
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server()

    result = asyncio.run(
        server["tools"][0].handler({"command": "echo hi", "env": {"DEBUG": "1"}})
    )

    payload = _payload(result)
    assert payload["stdout"] == "debug flag is 1\n"
    assert payload["stderr"] == "exit marker 1\n"
    assert payload["cleanup"]["error"] == "cleanup marker 1"
    assert "[REDACTED_ENV]" not in json.dumps(payload)
    assert result["is_error"] is False


@pytest.mark.parametrize(
    "arguments",
    [
        {},
        {"command": ""},
        {"command": ["python", ""]},
        {"command": "echo hi", "cwd": 123},
        {"command": "echo hi", "env": {"DEBUG": 1}},
        {"command": "echo hi", "max_chars": True},
        {"command": "echo hi", "cleanup": "keep"},
    ],
)
def test_handler_rejects_malformed_inputs_before_offload(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    arguments: dict[str, Any],
) -> None:
    offloaded: list[tuple[Any, tuple[Any, ...], dict[str, Any]]] = []

    def fake_run_sandbox_command(
        command: object, **kwargs: Any
    ) -> SandboxCommandResult:
        raise AssertionError(f"sandbox helper should not run: {command!r}, {kwargs!r}")

    async def fake_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        offloaded.append((func, args, kwargs))
        return func(*args, **kwargs)

    monkeypatch.setattr(
        sandbox_tool_module, "_run_sandbox_command", fake_run_sandbox_command
    )
    monkeypatch.setattr(sandbox_tool_module.asyncio, "to_thread", fake_to_thread)
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server()

    result = asyncio.run(server["tools"][0].handler(arguments))

    payload = _payload(result)
    assert payload["status"] == "failed"
    assert payload["error"]["category"] == "usage"
    assert offloaded == []
    assert result["is_error"] is True


@pytest.mark.parametrize(
    ("exc", "category"),
    [
        (KitaruUsageError("bad input"), "usage"),
        (KitaruStateError("no sandbox"), "state"),
        (KitaruFeatureNotAvailableError("missing method"), "feature_not_available"),
        (KitaruBackendError("provider failed"), "backend"),
        (KitaruRuntimeError("runtime failed"), "runtime"),
        (RuntimeError("unexpected"), "runtime"),
    ],
)
def test_handler_maps_failures_to_structured_tool_errors(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    exc: Exception,
    category: str,
) -> None:
    def fail(command: object, **kwargs: Any) -> SandboxCommandResult:
        _ = command, kwargs
        raise exc

    monkeypatch.setattr(sandbox_tool_module, "_run_sandbox_command", fail)
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server()

    result = asyncio.run(server["tools"][0].handler({"command": "echo hi"}))

    payload = _payload(result)
    assert payload == {
        "status": "failed",
        "error": {
            "type": type(exc).__name__,
            "category": category,
            "message": str(exc),
        },
    }
    assert result["is_error"] is True


def test_handler_redacts_env_values_from_failure_messages(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(command: object, **kwargs: Any) -> SandboxCommandResult:
        _ = command, kwargs
        raise KitaruBackendError("provider printed secret-value")

    monkeypatch.setattr(sandbox_tool_module, "_run_sandbox_command", fail)
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server()

    result = asyncio.run(
        server["tools"][0].handler(
            {"command": "echo hi", "env": {"SECRET_TOKEN": "secret-value"}}
        )
    )

    payload = _payload(result)
    assert payload["error"]["message"] == "provider printed [REDACTED_ENV]"
    assert "secret-value" not in json.dumps(payload)


def test_missing_claude_custom_tool_apis_raise_feature_not_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = types.ModuleType("claude_agent_sdk")
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", sdk)
    _purge_claude_adapter_modules(monkeypatch)
    module = importlib.import_module("kitaru.adapters.claude_agent_sdk._sandbox_tool")

    with pytest.raises(KitaruFeatureNotAvailableError, match="create_sdk_mcp_server"):
        module.create_kitaru_sandbox_mcp_server()


def test_invalid_helper_defaults_raise_usage_errors(
    sandbox_tool_module: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="server_name"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(server_name=" ")
    with pytest.raises(KitaruUsageError, match="default_max_chars"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(default_max_chars=0)
    with pytest.raises(KitaruUsageError, match="default_max_chars"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(default_max_chars=True)
    with pytest.raises(KitaruUsageError, match="default_cleanup"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(default_cleanup="keep")
