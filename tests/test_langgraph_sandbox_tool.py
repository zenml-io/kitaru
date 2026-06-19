"""Tests for the LangGraph/LangChain sandbox command tool factory."""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from typing import Any, Literal, cast

import pytest

from kitaru import SandboxCommandResult
from kitaru.adapters.langgraph import (
    DEFAULT_SANDBOX_COMMAND_TOOL_MAX_CHARS,
    SandboxCommandToolArgs,
    _sandbox_tool,
    create_sandbox_command_tool,
)
from kitaru.errors import (
    KitaruFeatureNotAvailableError,
    KitaruStateError,
    KitaruUsageError,
)


def _sandbox_result(
    *,
    command: str = "echo hello",
    cwd: str | None = None,
    stdout: str = "hello\n",
    stderr: str = "",
    exit_code: int = 0,
    cleanup: Literal["destroy", "close"] = "destroy",
) -> SandboxCommandResult:
    return SandboxCommandResult(
        command=command,
        cwd=cwd,
        stdout=stdout,
        stderr=stderr,
        exit_code=exit_code,
        stdout_truncated=False,
        stderr_truncated=False,
        stack_id="stack-1",
        stack_name="local-sandbox-stack",
        sandbox_id="sandbox-1",
        sandbox_name="local",
        session_id="session-1",
        cleanup=cleanup,
        cleanup_succeeded=True,
        cleanup_error=None,
    )


def test_factory_builds_structured_tool_with_conservative_defaults() -> None:
    tool = create_sandbox_command_tool()

    assert tool.name == "run_sandbox_command"
    assert "active Kitaru stack sandbox" in tool.description
    assert "non-zero exit code" in tool.description
    assert "redacts the command text" in tool.description
    assert "Deep Agents" in tool.description
    assert "exfiltrate secrets" in tool.description
    assert tool.args_schema is SandboxCommandToolArgs
    assert set(SandboxCommandToolArgs.model_fields) == {"command", "cwd"}
    assert getattr(tool, "coroutine", None) is None


def test_default_tool_uses_model_friendly_output_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run_sandbox_command(
        command: str,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        max_chars: int,
        cleanup: str,
    ) -> SandboxCommandResult:
        calls.append({"max_chars": max_chars})
        return _sandbox_result(command=command, cwd=cwd)

    monkeypatch.setattr(
        _sandbox_tool.kitaru,
        "run_sandbox_command",
        fake_run_sandbox_command,
    )

    _ = create_sandbox_command_tool().invoke({"command": "echo hello"})

    assert DEFAULT_SANDBOX_COMMAND_TOOL_MAX_CHARS == 20_000
    assert calls == [{"max_chars": DEFAULT_SANDBOX_COMMAND_TOOL_MAX_CHARS}]


def test_tool_forwards_command_options_and_returns_full_result_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_run_sandbox_command(
        command: str,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        max_chars: int,
        cleanup: str,
    ) -> SandboxCommandResult:
        calls.append(
            {
                "command": command,
                "cwd": cwd,
                "env": dict(env) if env is not None else None,
                "max_chars": max_chars,
                "cleanup": cleanup,
            }
        )
        return _sandbox_result(
            command=command,
            cwd=cwd,
            cleanup=cast(Literal["destroy", "close"], cleanup),
        )

    monkeypatch.setattr(
        _sandbox_tool.kitaru,
        "run_sandbox_command",
        fake_run_sandbox_command,
    )

    tool = create_sandbox_command_tool(
        default_cwd="/workspace",
        env={"SAFE_FLAG": "1"},
        max_chars=123,
        cleanup="close",
    )
    payload = json.loads(tool.invoke({"command": "echo hello", "cwd": "/tmp"}))

    assert calls == [
        {
            "command": "echo hello",
            "cwd": "/tmp",
            "env": {"SAFE_FLAG": "1"},
            "max_chars": 123,
            "cleanup": "close",
        }
    ]
    assert payload == {
        "command": "[REDACTED]",
        "cwd": "/tmp",
        "stdout": "hello\n",
        "stderr": "",
        "exit_code": 0,
        "stdout_truncated": False,
        "stderr_truncated": False,
        "stack_id": "stack-1",
        "stack_name": "local-sandbox-stack",
        "sandbox_id": "sandbox-1",
        "sandbox_name": "local",
        "session_id": "session-1",
        "cleanup": "close",
        "cleanup_succeeded": True,
        "cleanup_error": None,
    }


def test_tool_uses_default_cwd_and_copies_static_env_at_factory_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    env = {"SAFE_FLAG": "before"}

    def fake_run_sandbox_command(
        command: str,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        max_chars: int,
        cleanup: str,
    ) -> SandboxCommandResult:
        calls.append({"cwd": cwd, "env": dict(env or {})})
        return _sandbox_result(
            command=command,
            cwd=cwd,
            cleanup=cast(Literal["destroy", "close"], cleanup),
        )

    monkeypatch.setattr(
        _sandbox_tool.kitaru,
        "run_sandbox_command",
        fake_run_sandbox_command,
    )

    tool = create_sandbox_command_tool(default_cwd="/workspace", env=env)
    env["SAFE_FLAG"] = "after"
    _ = tool.invoke({"command": "pwd"})

    assert calls == [{"cwd": "/workspace", "env": {"SAFE_FLAG": "before"}}]


def test_non_zero_exit_result_returns_json_instead_of_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_sandbox_command(
        command: str,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
        max_chars: int,
        cleanup: str,
    ) -> SandboxCommandResult:
        return _sandbox_result(
            command=command,
            cwd=cwd,
            stdout="",
            stderr="boom\n",
            exit_code=2,
            cleanup=cast(Literal["destroy", "close"], cleanup),
        )

    monkeypatch.setattr(
        _sandbox_tool.kitaru,
        "run_sandbox_command",
        fake_run_sandbox_command,
    )

    payload = json.loads(create_sandbox_command_tool().invoke({"command": "false"}))

    assert payload["command"] == "[REDACTED]"
    assert payload["exit_code"] == 2
    assert payload["stderr"] == "boom\n"


def test_kitaru_sandbox_errors_propagate_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = KitaruStateError("The active stack has no sandbox component.")

    def fake_run_sandbox_command(*_args: Any, **_kwargs: Any) -> SandboxCommandResult:
        raise expected

    monkeypatch.setattr(
        _sandbox_tool.kitaru,
        "run_sandbox_command",
        fake_run_sandbox_command,
    )

    with pytest.raises(KitaruStateError) as exc_info:
        create_sandbox_command_tool().invoke({"command": "echo hello"})

    assert exc_info.value is expected


def test_missing_langchain_tool_api_raises_feature_guidance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "langchain_core.tools", None)

    with pytest.raises(KitaruFeatureNotAvailableError, match="StructuredTool"):
        create_sandbox_command_tool()


def test_empty_tool_name_is_a_kitaru_usage_error() -> None:
    with pytest.raises(KitaruUsageError, match="non-empty"):
        create_sandbox_command_tool(name="  ")


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"default_cwd": 123}, "default_cwd"),
        ({"env": [("SAFE_FLAG", "1")]}, "env"),
        ({"env": {"SAFE_FLAG": 1}}, "string keys to string values"),
        ({"max_chars": -1}, "non-negative integer"),
        ({"max_chars": True}, "non-negative integer"),
        ({"max_chars": 10.5}, "non-negative integer"),
        ({"cleanup": "keep"}, "cleanup"),
    ],
)
def test_invalid_factory_options_raise_usage_error_before_tool_call(
    kwargs: dict[str, Any],
    message: str,
) -> None:
    with pytest.raises(KitaruUsageError, match=message):
        create_sandbox_command_tool(**kwargs)


def test_first_version_does_not_supply_custom_async_coroutine() -> None:
    tool = create_sandbox_command_tool()

    assert getattr(tool, "coroutine", None) is None
