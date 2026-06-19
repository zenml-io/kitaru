"""Tests for the Claude Agent SDK Kitaru sandbox MCP tool."""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
import tempfile
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

    class ToolAnnotations:
        def __init__(self, **kwargs: Any) -> None:
            self.__dict__.update(kwargs)

    def tool(
        name: str,
        description: str,
        input_schema: dict[str, Any],
        annotations: Any | None = None,
    ) -> Any:
        sdk.__dict__["tool_calls"].append(
            {
                "name": name,
                "description": description,
                "input_schema": input_schema,
                "annotations": annotations,
            }
        )

        def decorate(handler: Any) -> SimpleNamespace:
            return SimpleNamespace(
                name=name,
                description=description,
                input_schema=input_schema,
                handler=handler,
                annotations=annotations,
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
    sdk.__dict__["ToolAnnotations"] = ToolAnnotations
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
            "annotations": ANY,
        }
    ]
    schema = fake_claude_sdk.__dict__["tool_calls"][0]["input_schema"]
    command_string_schema = schema["properties"]["command"]["anyOf"][0]
    command_array_schema = schema["properties"]["command"]["anyOf"][1]
    cwd_string_schema = schema["properties"]["cwd"]["anyOf"][0]
    assert command_string_schema["maxLength"] == (
        sandbox_tool_module._MAX_COMMAND_STRING_CHARS
    )
    assert (
        command_array_schema["maxItems"] == sandbox_tool_module._MAX_COMMAND_ARGV_ITEMS
    )
    assert command_array_schema["items"]["maxLength"] == (
        sandbox_tool_module._MAX_COMMAND_ARG_CHARS
    )
    assert cwd_string_schema["maxLength"] == sandbox_tool_module._MAX_CWD_CHARS
    env_object_schema = schema["properties"]["env"]["anyOf"][0]
    assert env_object_schema["maxProperties"] == sandbox_tool_module._MAX_ENV_VARS
    assert (
        env_object_schema["propertyNames"]["maxLength"]
        == sandbox_tool_module._MAX_ENV_KEY_CHARS
    )
    assert (
        env_object_schema["additionalProperties"]["maxLength"]
        == sandbox_tool_module._MAX_ENV_VALUE_CHARS
    )
    max_chars_integer_schema = schema["properties"]["max_chars"]["anyOf"][0]
    assert max_chars_integer_schema["maximum"] == 123
    assert (
        "maximum"
        not in sandbox_tool_module._INPUT_SCHEMA["properties"]["max_chars"]["anyOf"][0]
    )

    annotation = fake_claude_sdk.__dict__["tool_calls"][0]["annotations"]
    assert annotation.maxResultSizeChars == (
        sandbox_tool_module._tool_result_max_size_chars(123)
    )
    assert fake_claude_sdk.__dict__["server_calls"][0]["name"] == "kitaru_custom"
    sdk_tool = fake_claude_sdk.__dict__["server_calls"][0]["tools"][0]
    assert sdk_tool.name == "sandbox_exec"
    assert sdk_tool.annotations.maxResultSizeChars == (
        sandbox_tool_module._tool_result_max_size_chars(123)
    )

    assert isinstance(server, dict)
    assert sandbox_tool_module.KITARU_SANDBOX_MCP_METADATA_KEY not in server
    cli_mcp_config = {key: value for key, value in server.items() if key != "instance"}
    assert sandbox_tool_module.KITARU_SANDBOX_MCP_METADATA_KEY not in cli_mcp_config
    assert sandbox_tool_module.KITARU_SANDBOX_MCP_METADATA_KEY not in json.dumps(
        cli_mcp_config, default=repr
    )

    metadata = sandbox_tool_module.kitaru_sandbox_mcp_metadata(server)
    assert metadata == {
        "kind": "kitaru_sandbox_command_mcp_server",
        "server_name": "kitaru_custom",
        "tool_name": "sandbox_exec",
        "description": "Run it safely",
        "allowed_tool_name": "mcp__kitaru_custom__sandbox_exec",
        "default_max_chars": 123,
        "default_cleanup": "close",
    }


def test_helper_defaults_stay_within_claude_mcp_result_limit(
    sandbox_tool_module: types.ModuleType,
    fake_claude_sdk: types.ModuleType,
) -> None:
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server()
    default_max_chars = sandbox_tool_module.DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS

    assert (
        default_max_chars
        == (
            sandbox_tool_module._CLAUDE_MCP_MAX_RESULT_SIZE_CHARS
            - sandbox_tool_module._TOOL_RESULT_JSON_OVERHEAD_CHARS
        )
        // 2
    )
    assert (
        sandbox_tool_module._tool_result_max_size_chars(default_max_chars)
        == sandbox_tool_module._CLAUDE_MCP_MAX_RESULT_SIZE_CHARS
    )

    schema = fake_claude_sdk.__dict__["tool_calls"][0]["input_schema"]
    max_chars_integer_schema = schema["properties"]["max_chars"]["anyOf"][0]
    assert max_chars_integer_schema["maximum"] == default_max_chars

    annotation = fake_claude_sdk.__dict__["tool_calls"][0]["annotations"]
    assert (
        annotation.maxResultSizeChars
        == sandbox_tool_module._CLAUDE_MCP_MAX_RESULT_SIZE_CHARS
    )
    metadata = sandbox_tool_module.kitaru_sandbox_mcp_metadata(server)
    assert metadata is not None
    assert metadata["default_max_chars"] == default_max_chars


def test_public_default_allowed_tool_name(
    sandbox_tool_module: types.ModuleType,
) -> None:
    assert sandbox_tool_module.KITARU_SANDBOX_MCP_SERVER_NAME == "kitaru"
    assert sandbox_tool_module.KITARU_SANDBOX_COMMAND_TOOL_NAME == "run_command"
    assert (
        sandbox_tool_module.KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME
        == "mcp__kitaru__run_command"
    )
    adapter = importlib.import_module("kitaru.adapters.claude_agent_sdk")
    assert (
        adapter.DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS
        == sandbox_tool_module.DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS
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

    sdk_tool = sandbox_tool.create_kitaru_sandbox_command_tool(default_max_chars=321)
    assert sdk_tool.annotations is not None
    assert sdk_tool.annotations.maxResultSizeChars == (
        sandbox_tool._tool_result_max_size_chars(321)
    )

    server = sandbox_tool.create_kitaru_sandbox_mcp_server(
        default_max_chars=321,
        default_cleanup="close",
    )

    assert isinstance(server, dict)
    assert server["type"] == "sdk"
    assert server["name"] == "kitaru"
    assert "instance" in server
    assert sandbox_tool.KITARU_SANDBOX_MCP_METADATA_KEY not in server
    cli_mcp_config = {key: value for key, value in server.items() if key != "instance"}
    assert sandbox_tool.KITARU_SANDBOX_MCP_METADATA_KEY not in cli_mcp_config
    assert sandbox_tool.KITARU_SANDBOX_MCP_METADATA_KEY not in json.dumps(
        cli_mcp_config, default=repr
    )

    metadata = sandbox_tool.kitaru_sandbox_mcp_metadata(server)
    assert metadata == {
        "kind": "kitaru_sandbox_command_mcp_server",
        "server_name": "kitaru",
        "tool_name": "run_command",
        "description": (
            "Run a shell command or argv-style command list through the active "
            "Kitaru stack's sandbox component. Use this instead of Claude's "
            "built-in Bash when command execution should be owned by Kitaru."
        ),
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


def test_kitaru_sandbox_mcp_server_cache_identity_includes_description(
    sandbox_tool_module: types.ModuleType,
) -> None:
    serialization = importlib.import_module(
        "kitaru.adapters.claude_agent_sdk._serialization"
    )

    concise_server = sandbox_tool_module.create_kitaru_sandbox_mcp_server(
        description="Run only safe read commands."
    )
    broader_server = sandbox_tool_module.create_kitaru_sandbox_mcp_server(
        description="Run read and write commands when needed."
    )

    concise_identity = serialization.to_cache_identity(concise_server)
    broader_identity = serialization.to_cache_identity(broader_server)
    assert concise_identity["description"] == "Run only safe read commands."
    assert broader_identity["description"] == "Run read and write commands when needed."
    assert concise_identity != broader_identity


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


def test_handler_trims_json_expanded_output_to_claude_result_limit(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdout = "😀" * sandbox_tool_module.DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS

    def fake_run_sandbox_command(
        command: object, **kwargs: Any
    ) -> SandboxCommandResult:
        _ = command, kwargs
        return _sandbox_result(stdout=stdout, stderr="")

    monkeypatch.setattr(
        sandbox_tool_module, "_run_sandbox_command", fake_run_sandbox_command
    )
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server()
    tool = server["tools"][0]

    result = asyncio.run(tool.handler({"command": "echo lots"}))

    text = result["content"][0]["text"]
    assert len(text) <= tool.annotations.maxResultSizeChars
    payload = _payload(result)
    assert payload["status"] == "completed"
    assert payload["stdout_truncated"] is True
    assert payload["stdout"].endswith(
        "truncated by Kitaru to fit Claude MCP result limit]"
    )
    assert len(payload["stdout"]) < len(stdout)
    assert result["is_error"] is False


def test_handler_trims_newline_expanded_streams_to_claude_result_limit(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream = "\n" * sandbox_tool_module.DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS

    def fake_run_sandbox_command(
        command: object, **kwargs: Any
    ) -> SandboxCommandResult:
        _ = command, kwargs
        return _sandbox_result(stdout=stream, stderr=stream).model_copy(
            update={"stdout_truncated": False, "stderr_truncated": False}
        )

    monkeypatch.setattr(
        sandbox_tool_module, "_run_sandbox_command", fake_run_sandbox_command
    )
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server()
    tool = server["tools"][0]

    result = asyncio.run(tool.handler({"command": "echo lots"}))

    text = result["content"][0]["text"]
    assert len(text) <= tool.annotations.maxResultSizeChars
    payload = _payload(result)
    assert payload["status"] == "completed"
    assert payload["stdout_truncated"] or payload["stderr_truncated"]
    assert result["is_error"] is False


def test_handler_trims_json_expanded_failure_to_claude_result_limit(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(command: object, **kwargs: Any) -> SandboxCommandResult:
        _ = command, kwargs
        raise KitaruBackendError(
            "provider emitted oversized detail "
            + ("😀" * sandbox_tool_module.DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS)
        )

    monkeypatch.setattr(sandbox_tool_module, "_run_sandbox_command", fail)
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server()
    tool = server["tools"][0]

    result = asyncio.run(tool.handler({"command": "echo hi"}))

    text = result["content"][0]["text"]
    assert len(text) <= tool.annotations.maxResultSizeChars
    payload = _payload(result)
    assert payload["status"] == "failed"
    assert payload["error"]["message"].endswith(
        "truncated by Kitaru to fit Claude MCP result limit]"
    )
    assert result["is_error"] is True


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


def test_env_redactions_sort_by_length_then_value(
    sandbox_tool_module: types.ModuleType,
) -> None:
    assert sandbox_tool_module._env_redactions(
        {
            "FIRST_CONFIG": "zzzz",
            "SECOND_CONFIG": "aaaa",
            "THIRD_CONFIG": "longer-value",
        }
    ) == ("longer-value", "aaaa", "zzzz")


def test_handler_redacts_non_trivial_env_values_from_innocuous_keys(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_value = '{"api_key":"dummy-secret-marker"}'

    def fake_run_sandbox_command(
        command: object, **kwargs: Any
    ) -> SandboxCommandResult:
        _ = command, kwargs
        return _sandbox_result(
            stdout=f"loaded config {config_value}\n",
            stderr=f"debug config {config_value}\n",
            cleanup_error=f"cleanup config {config_value}",
        )

    monkeypatch.setattr(
        sandbox_tool_module, "_run_sandbox_command", fake_run_sandbox_command
    )
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server()

    result = asyncio.run(
        server["tools"][0].handler(
            {"command": "echo hi", "env": {"CONFIG": config_value}}
        )
    )

    payload = _payload(result)
    assert payload["stdout"] == "loaded config [REDACTED_ENV]\n"
    assert payload["stderr"] == "debug config [REDACTED_ENV]\n"
    assert payload["cleanup"]["error"] == "cleanup config [REDACTED_ENV]"
    assert config_value not in json.dumps(payload)
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
        {"command": "x" * 8_193},
        {"command": ["python", ""]},
        {"command": ["python", "x" * 4_097]},
        {"command": ["python"] * 129},
        {"command": "echo hi", "cwd": 123},
        {"command": "echo hi", "cwd": "/" + ("x" * 4_096)},
        {"command": "echo hi", "env": {"DEBUG": 1}},
        {"command": "echo hi", "env": {"DEBUG": "x" * 8_193}},
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


def test_handler_rejects_max_chars_above_default_before_offload(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
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
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server(default_max_chars=10)

    result = asyncio.run(
        server["tools"][0].handler({"command": "echo hi", "max_chars": 11})
    )

    payload = _payload(result)
    assert payload["status"] == "failed"
    assert payload["error"]["category"] == "usage"
    assert "default_max_chars" in payload["error"]["message"]
    assert offloaded == []
    assert result["is_error"] is True


def test_handler_accepts_max_chars_at_configured_default(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run_sandbox_command(
        command: object, **kwargs: Any
    ) -> SandboxCommandResult:
        calls.append({"command": command, **kwargs})
        return _sandbox_result()

    async def fake_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr(
        sandbox_tool_module, "_run_sandbox_command", fake_run_sandbox_command
    )
    monkeypatch.setattr(sandbox_tool_module.asyncio, "to_thread", fake_to_thread)
    server = sandbox_tool_module.create_kitaru_sandbox_mcp_server(default_max_chars=10)

    result = asyncio.run(
        server["tools"][0].handler({"command": "echo hi", "max_chars": 10})
    )

    assert _payload(result)["status"] == "completed"
    assert calls == [
        {
            "command": "echo hi",
            "cwd": None,
            "env": None,
            "max_chars": 10,
            "cleanup": "destroy",
        }
    ]
    assert result["is_error"] is False


def test_handler_rejects_unknown_arguments_before_offload(
    sandbox_tool_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
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

    result = asyncio.run(
        server["tools"][0].handler({"command": "echo hi", "timeout_seconds": 5})
    )

    payload = _payload(result)
    assert payload["status"] == "failed"
    assert payload["error"]["category"] == "usage"
    assert "timeout_seconds" in payload["error"]["message"]
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
    with pytest.raises(KitaruUsageError, match="server_name"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(server_name="bad name")
    with pytest.raises(KitaruUsageError, match="server_name"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(server_name="bad.name")
    with pytest.raises(KitaruUsageError, match="server_name"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(server_name="bad__name")
    with pytest.raises(KitaruUsageError, match="tool_name"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(tool_name="run command")
    with pytest.raises(KitaruUsageError, match="tool_name"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(tool_name="run__command")
    with pytest.raises(KitaruUsageError, match="default_max_chars"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(default_max_chars=0)
    with pytest.raises(KitaruUsageError, match="default_max_chars"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(default_max_chars=True)
    with pytest.raises(KitaruUsageError, match="result-size ceiling"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(
            default_max_chars=(
                sandbox_tool_module.DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS + 1
            )
        )
    with pytest.raises(KitaruUsageError, match="default_cleanup"):
        sandbox_tool_module.create_kitaru_sandbox_mcp_server(default_cleanup="keep")


def test_allowed_tool_name_rejects_invalid_components(
    sandbox_tool_module: types.ModuleType,
) -> None:
    with pytest.raises(KitaruUsageError, match="server_name"):
        sandbox_tool_module.allowed_tool_name("bad name", "run_command")
    with pytest.raises(KitaruUsageError, match="tool_name"):
        sandbox_tool_module.allowed_tool_name("kitaru", "run__command")


def test_sandbox_example_uses_cost_guarded_tool_capable_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("claude_agent_sdk")
    _purge_claude_adapter_modules(monkeypatch)
    module_name = (
        "examples.integrations.claude_agent_sdk_agent.claude_agent_sdk_sandbox_tool"
    )
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    example = importlib.import_module(module_name)

    assert example.DEFAULT_MODEL == "sonnet"
    assert example.DEFAULT_MAX_BUDGET_USD == 0.10
    assert example._coerce_optional_budget(0) is None
    assert example._coerce_optional_budget(0.25) == 0.25
    with pytest.raises(SystemExit, match="finite"):
        example._coerce_optional_budget(float("nan"))
    with pytest.raises(SystemExit, match="finite"):
        example._coerce_optional_budget(float("inf"))

    runner = example._build_runner(
        model=example.DEFAULT_MODEL,
        max_budget_usd=example.DEFAULT_MAX_BUDGET_USD,
    )
    options = runner._build_options(
        example.ClaudeRunRequest.start("hello", cwd="/tmp", max_turns=3)
    )
    assert options is not None

    assert options.model == "sonnet"
    assert options.max_budget_usd == 0.10
    assert options.effort == "low"
    assert options.setting_sources == []
    assert options.extra_args == {"bare": None}
    assert options.strict_mcp_config is True
    assert options.permission_mode == "dontAsk"
    assert options.tools == []
    assert options.disallowed_tools == ["Bash"]
    assert options.allowed_tools == [example.KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME]


def test_sandbox_example_main_defaults_claude_cwd_to_temp_dir(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("claude_agent_sdk")
    _purge_claude_adapter_modules(monkeypatch)
    module_name = (
        "examples.integrations.claude_agent_sdk_agent.claude_agent_sdk_sandbox_tool"
    )
    monkeypatch.delitem(sys.modules, module_name, raising=False)
    example = importlib.import_module(module_name)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr(sys, "argv", ["claude_agent_sdk_sandbox_tool.py"])
    monkeypatch.setattr(example, "_print_result", lambda result: None)

    captured: dict[str, Any] = {}

    def fake_run(
        command: str,
        claude_cwd: str,
        max_turns: int,
        model: str | None,
        max_budget_usd: float | None,
    ) -> SimpleNamespace:
        captured.update(
            {
                "command": command,
                "claude_cwd": claude_cwd,
                "max_turns": max_turns,
                "model": model,
                "max_budget_usd": max_budget_usd,
            }
        )
        result = example.ClaudeRunResult(final_text="ok")
        return SimpleNamespace(wait=lambda: result)

    monkeypatch.setattr(example.inspect_sandbox_with_claude, "run", fake_run)

    example.main()

    assert captured == {
        "command": example.DEFAULT_SANDBOX_COMMAND,
        "claude_cwd": example._default_claude_cwd(),
        "max_turns": 3,
        "model": "sonnet",
        "max_budget_usd": 0.10,
    }
    assert captured["claude_cwd"].startswith(tempfile.gettempdir())
