"""Claude Agent SDK MCP tool for Kitaru sandbox commands."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast

from kitaru._config._sandbox import DEFAULT_SANDBOX_COMMAND_MAX_CHARS
from kitaru.errors import (
    KitaruBackendError,
    KitaruError,
    KitaruFeatureNotAvailableError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
)

SandboxCommandCleanup = Literal["destroy", "close"]
SandboxCommand = str | list[str]

KITARU_SANDBOX_MCP_SERVER_NAME = "kitaru"
KITARU_SANDBOX_COMMAND_TOOL_NAME = "run_command"
KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME = (
    f"mcp__{KITARU_SANDBOX_MCP_SERVER_NAME}__{KITARU_SANDBOX_COMMAND_TOOL_NAME}"
)
KITARU_SANDBOX_MCP_METADATA_KIND = "kitaru_sandbox_command_mcp_server"
KITARU_SANDBOX_MCP_METADATA_KEY = "__kitaru_sandbox_command_mcp_server__"

_DEFAULT_DESCRIPTION = (
    "Run a shell command or argv-style command list through the active Kitaru "
    "stack's sandbox component. Use this instead of Claude's built-in Bash "
    "when command execution should be owned by Kitaru."
)

_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "command": {
            "anyOf": [
                {"type": "string", "minLength": 1},
                {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                    "minItems": 1,
                },
            ],
            "description": "Command string or argv-style command list to run.",
        },
        "cwd": {
            "anyOf": [{"type": "string"}, {"type": "null"}],
            "description": "Optional working directory inside the sandbox.",
        },
        "env": {
            "anyOf": [
                {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                },
                {"type": "null"},
            ],
            "description": "Optional environment variables for the command.",
        },
        "max_chars": {
            "anyOf": [{"type": "integer", "minimum": 1}, {"type": "null"}],
            "description": "Optional per-stream output collection limit.",
        },
        "cleanup": {
            "anyOf": [{"enum": ["destroy", "close"]}, {"type": "null"}],
            "description": "Optional sandbox session cleanup policy.",
        },
    },
    "required": ["command"],
    "additionalProperties": False,
}


def create_kitaru_sandbox_mcp_server(
    *,
    server_name: str = KITARU_SANDBOX_MCP_SERVER_NAME,
    tool_name: str = KITARU_SANDBOX_COMMAND_TOOL_NAME,
    description: str | None = None,
    default_max_chars: int = DEFAULT_SANDBOX_COMMAND_MAX_CHARS,
    default_cleanup: SandboxCommandCleanup = "destroy",
) -> Any:
    """Create a Claude Agent SDK MCP server for Kitaru sandbox commands.

    The returned object is meant for ``ClaudeAgentOptions(mcp_servers={...})``.
    It is not a Claude built-in tool and should not be passed through the
    ``tools=[...]`` option.
    """
    normalized_server_name = _normalize_identifier(server_name, "server_name")
    normalized_tool_name = _normalize_identifier(tool_name, "tool_name")
    normalized_max_chars = _normalize_default_max_chars(default_max_chars)
    normalized_cleanup = _normalize_default_cleanup(default_cleanup)
    tool_api, create_server = _claude_sdk_mcp_apis()

    sdk_tool = create_kitaru_sandbox_command_tool(
        tool_name=normalized_tool_name,
        description=description,
        default_max_chars=normalized_max_chars,
        default_cleanup=normalized_cleanup,
        tool_api=tool_api,
    )
    server = create_server(
        name=normalized_server_name, version="1.0.0", tools=[sdk_tool]
    )
    _attach_metadata(
        server,
        server_name=normalized_server_name,
        tool_name=normalized_tool_name,
        default_max_chars=normalized_max_chars,
        default_cleanup=normalized_cleanup,
    )
    return server


def create_kitaru_sandbox_command_tool(
    *,
    tool_name: str = KITARU_SANDBOX_COMMAND_TOOL_NAME,
    description: str | None = None,
    default_max_chars: int = DEFAULT_SANDBOX_COMMAND_MAX_CHARS,
    default_cleanup: SandboxCommandCleanup = "destroy",
    tool_api: Any | None = None,
) -> Any:
    """Create the lower-level Claude SDK tool used by the MCP server helper."""
    normalized_tool_name = _normalize_identifier(tool_name, "tool_name")
    normalized_max_chars = _normalize_default_max_chars(default_max_chars)
    normalized_cleanup = _normalize_default_cleanup(default_cleanup)
    tool_factory = tool_api or _claude_sdk_mcp_apis()[0]

    async def handler(arguments: Mapping[str, Any]) -> dict[str, Any]:
        payload = await _run_kitaru_sandbox_command_tool(
            arguments,
            default_max_chars=normalized_max_chars,
            default_cleanup=normalized_cleanup,
        )
        return _claude_tool_result(payload)

    return tool_factory(
        normalized_tool_name,
        description or _DEFAULT_DESCRIPTION,
        dict(_INPUT_SCHEMA),
    )(handler)


async def _run_kitaru_sandbox_command_tool(
    arguments: Mapping[str, Any],
    *,
    default_max_chars: int,
    default_cleanup: SandboxCommandCleanup,
) -> dict[str, Any]:
    env_redactions: tuple[str, ...] = ()
    try:
        command, cwd, env, max_chars, cleanup = _tool_arguments(
            arguments,
            default_max_chars=default_max_chars,
            default_cleanup=default_cleanup,
        )
        env_redactions = _env_redactions(env)
        result = await asyncio.to_thread(
            _run_sandbox_command,
            command,
            cwd=cwd,
            env=env,
            max_chars=max_chars,
            cleanup=cleanup,
        )
    except Exception as exc:
        return cast(
            dict[str, Any], _redact_env_values(_failed_payload(exc), env_redactions)
        )

    return cast(
        dict[str, Any],
        _redact_env_values(
            {
                "status": "completed",
                "command": result.command,
                "cwd": result.cwd,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.exit_code,
                "stdout_truncated": result.stdout_truncated,
                "stderr_truncated": result.stderr_truncated,
                "stack": {"id": result.stack_id, "name": result.stack_name},
                "sandbox": {"id": result.sandbox_id, "name": result.sandbox_name},
                "session_id": result.session_id,
                "cleanup": {
                    "policy": result.cleanup,
                    "succeeded": result.cleanup_succeeded,
                    "error": result.cleanup_error,
                },
            },
            env_redactions,
        ),
    )


def _tool_arguments(
    arguments: Mapping[str, Any],
    *,
    default_max_chars: int,
    default_cleanup: SandboxCommandCleanup,
) -> tuple[
    SandboxCommand,
    str | None,
    Mapping[str, str] | None,
    int,
    SandboxCommandCleanup,
]:
    if not isinstance(arguments, Mapping):
        raise KitaruUsageError(
            "Claude sandbox command tool arguments must be a mapping."
        )
    command = _tool_command(arguments.get("command", _MISSING))
    cwd = _tool_optional_string(arguments.get("cwd"), "cwd")
    env = _tool_env(arguments.get("env"))
    max_chars = _tool_max_chars(arguments.get("max_chars"), default_max_chars)
    cleanup = _tool_cleanup(arguments.get("cleanup"), default_cleanup)
    return command, cwd, env, max_chars, cleanup


_MISSING = object()


def _tool_command(value: Any) -> SandboxCommand:
    if isinstance(value, str):
        if not value.strip():
            raise KitaruUsageError("Sandbox command must be a non-empty string.")
        return value
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        if not value:
            raise KitaruUsageError("Sandbox command list cannot be empty.")
        command: list[str] = []
        for item in value:
            if not isinstance(item, str) or not item:
                raise KitaruUsageError(
                    "Sandbox command list items must be non-empty strings."
                )
            command.append(item)
        return command
    if value is _MISSING:
        raise KitaruUsageError("Sandbox command tool input requires `command`.")
    raise KitaruUsageError("Sandbox command must be a string or list of strings.")


def _tool_optional_string(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    raise KitaruUsageError(f"Sandbox command `{field_name}` must be a string or null.")


def _tool_env(value: Any) -> Mapping[str, str] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise KitaruUsageError("Sandbox command `env` must be an object or null.")
    normalized: dict[str, str] = {}
    for key, nested in value.items():
        if not isinstance(key, str) or not isinstance(nested, str):
            raise KitaruUsageError(
                "Sandbox command `env` keys and values must be strings."
            )
        normalized[key] = nested
    return normalized


def _tool_max_chars(value: Any, default_max_chars: int) -> int:
    if value is None:
        return default_max_chars
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise KitaruUsageError(
            "Sandbox command `max_chars` must be a positive integer or null."
        )
    return value


def _tool_cleanup(
    value: Any,
    default_cleanup: SandboxCommandCleanup,
) -> SandboxCommandCleanup:
    if value is None:
        return default_cleanup
    if value not in {"destroy", "close"}:
        raise KitaruUsageError(
            "Sandbox command `cleanup` must be 'destroy', 'close', or null."
        )
    return cast(SandboxCommandCleanup, value)


def _run_sandbox_command(
    command: SandboxCommand,
    *,
    cwd: str | None,
    env: Mapping[str, str] | None,
    max_chars: int,
    cleanup: SandboxCommandCleanup,
) -> Any:
    from kitaru.config import run_sandbox_command

    return run_sandbox_command(
        command,
        cwd=cwd,
        env=env,
        max_chars=max_chars,
        cleanup=cleanup,
    )


def kitaru_sandbox_mcp_metadata(value: Any) -> dict[str, Any] | None:
    """Return Kitaru sandbox MCP metadata attached to a server config."""
    if not isinstance(value, Mapping):
        return None
    metadata = value.get(KITARU_SANDBOX_MCP_METADATA_KEY)
    if (
        isinstance(metadata, Mapping)
        and metadata.get("kind") == KITARU_SANDBOX_MCP_METADATA_KIND
    ):
        return dict(metadata)
    return None


def allowed_tool_name(server_name: str, tool_name: str) -> str:
    """Build Claude's allowed-tools entry for an MCP server tool."""
    return f"mcp__{server_name}__{tool_name}"


def _claude_sdk_mcp_apis() -> tuple[Any, Any]:
    try:
        from claude_agent_sdk import create_sdk_mcp_server, tool
    except (ImportError, AttributeError) as exc:
        raise KitaruFeatureNotAvailableError(
            "The installed claude-agent-sdk does not expose the custom-tool "
            "MCP APIs required by create_kitaru_sandbox_mcp_server(). Install "
            "a version that provides `tool(...)` and `create_sdk_mcp_server(...)`."
        ) from exc
    return tool, create_sdk_mcp_server


def _attach_metadata(
    server: Any,
    *,
    server_name: str,
    tool_name: str,
    default_max_chars: int,
    default_cleanup: SandboxCommandCleanup,
) -> None:
    if isinstance(server, dict):
        server[KITARU_SANDBOX_MCP_METADATA_KEY] = {
            "kind": KITARU_SANDBOX_MCP_METADATA_KIND,
            "server_name": server_name,
            "tool_name": tool_name,
            "allowed_tool_name": allowed_tool_name(server_name, tool_name),
            "default_max_chars": default_max_chars,
            "default_cleanup": default_cleanup,
        }


def _claude_tool_result(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(payload, sort_keys=True),
            }
        ],
        "is_error": payload.get("status") == "failed",
    }


def _failed_payload(exc: Exception) -> dict[str, Any]:
    return {
        "status": "failed",
        "error": {
            "type": type(exc).__name__,
            "category": _error_category(exc),
            "message": str(exc),
        },
    }


_SENSITIVE_ENV_KEY_FRAGMENTS = (
    "authorization",
    "cookie",
    "credential",
    "key",
    "password",
    "secret",
    "token",
)
_SECRET_VALUE_PREFIXES = ("sk-", "ak_", "pk_", "Bearer ")
_SECRET_VALUE_FRAGMENTS = ("secret", "token", "password")


def _env_redactions(env: Mapping[str, str] | None) -> tuple[str, ...]:
    if env is None or not isinstance(env, Mapping):
        return ()
    values: set[str] = set()
    for key, value in env.items():
        if _should_redact_env_value(key, value):
            values.add(value)
    return tuple(sorted(values, key=lambda item: len(item), reverse=True))


def _should_redact_env_value(key: str, value: str) -> bool:
    if len(value) < 4:
        return False
    lowered_key = key.lower()
    if any(fragment in lowered_key for fragment in _SENSITIVE_ENV_KEY_FRAGMENTS):
        return True
    return _is_secret_like_env_value(value)


def _is_secret_like_env_value(value: str) -> bool:
    lowered_value = value.lower()
    if any(value.startswith(prefix) for prefix in _SECRET_VALUE_PREFIXES):
        return True
    if any(fragment in lowered_value for fragment in _SECRET_VALUE_FRAGMENTS):
        return True
    return (
        len(value) >= 8
        and any(char.isalpha() for char in value)
        and any(char.isdigit() for char in value)
    )


def _redact_env_values(value: Any, redactions: tuple[str, ...]) -> Any:
    if not redactions:
        return value
    if isinstance(value, str):
        redacted = value
        for secret in redactions:
            redacted = redacted.replace(secret, "[REDACTED_ENV]")
        return redacted
    if isinstance(value, Mapping):
        return {
            key: _redact_env_values(nested, redactions) for key, nested in value.items()
        }
    if isinstance(value, list):
        return [_redact_env_values(item, redactions) for item in value]
    return value


def _error_category(exc: Exception) -> str:
    if isinstance(exc, KitaruUsageError):
        return "usage"
    if isinstance(exc, KitaruStateError):
        return "state"
    if isinstance(exc, KitaruFeatureNotAvailableError):
        return "feature_not_available"
    if isinstance(exc, KitaruBackendError):
        return "backend"
    if isinstance(exc, KitaruRuntimeError | KitaruError):
        return "runtime"
    return "runtime"


def _normalize_identifier(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise KitaruUsageError(
            f"Claude sandbox MCP `{field_name}` must be a non-empty string."
        )
    return value.strip()


def _normalize_default_max_chars(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise KitaruUsageError(
            "Claude sandbox MCP `default_max_chars` must be a positive integer."
        )
    return value


def _normalize_default_cleanup(value: str) -> SandboxCommandCleanup:
    if value not in {"destroy", "close"}:
        raise KitaruUsageError(
            "Claude sandbox MCP `default_cleanup` must be 'destroy' or 'close'."
        )
    return cast(SandboxCommandCleanup, value)


__all__ = [
    "KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME",
    "KITARU_SANDBOX_COMMAND_TOOL_NAME",
    "KITARU_SANDBOX_MCP_METADATA_KEY",
    "KITARU_SANDBOX_MCP_METADATA_KIND",
    "KITARU_SANDBOX_MCP_SERVER_NAME",
    "allowed_tool_name",
    "create_kitaru_sandbox_command_tool",
    "create_kitaru_sandbox_mcp_server",
    "kitaru_sandbox_mcp_metadata",
]
