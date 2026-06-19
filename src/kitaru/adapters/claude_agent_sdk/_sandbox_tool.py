"""Claude Agent SDK MCP tool for Kitaru sandbox commands."""

from __future__ import annotations

import asyncio
import copy
import json
import re
from collections.abc import Mapping
from contextlib import suppress
from typing import Any, Literal, cast

from kitaru._config import _sandbox as sandbox_config
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


def _format_allowed_tool_name(server_name: str, tool_name: str) -> str:
    return f"mcp__{server_name}__{tool_name}"


KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME = _format_allowed_tool_name(
    KITARU_SANDBOX_MCP_SERVER_NAME,
    KITARU_SANDBOX_COMMAND_TOOL_NAME,
)
KITARU_SANDBOX_MCP_METADATA_KIND = "kitaru_sandbox_command_mcp_server"
KITARU_SANDBOX_MCP_METADATA_KEY = "__kitaru_sandbox_command_mcp_server__"

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_PRIVATE_METADATA_ATTRIBUTE = "_kitaru_sandbox_mcp_metadata"

_DEFAULT_DESCRIPTION = (
    "Run a shell command or argv-style command list through the active Kitaru "
    "stack's sandbox component. Use this instead of Claude's built-in Bash "
    "when command execution should be owned by Kitaru."
)
# The tool returns one JSON object with stdout and stderr. Claude reads
# maxResultSizeChars from the MCP tool metadata before the model sees the result.
# Claude Code currently caps anthropic/maxResultSizeChars at 500,000 characters,
# so keep the default per-stream collection budget low enough for both streams
# plus ordinary JSON fields to fit under that ceiling.
_CLAUDE_MCP_MAX_RESULT_SIZE_CHARS = 500_000
_TOOL_RESULT_JSON_OVERHEAD_CHARS = 65_536
DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS = (
    _CLAUDE_MCP_MAX_RESULT_SIZE_CHARS - _TOOL_RESULT_JSON_OVERHEAD_CHARS
) // 2
_TOOL_ARGUMENT_KEYS = frozenset({"command", "cwd", "env", "max_chars", "cleanup"})
_MAX_COMMAND_STRING_CHARS = 8_192
_MAX_COMMAND_ARGV_ITEMS = 128
_MAX_COMMAND_ARG_CHARS = 4_096
_MAX_CWD_CHARS = 4_096
_MAX_ENV_VARS = 64
_MAX_ENV_KEY_CHARS = 256
_MAX_ENV_VALUE_CHARS = 8_192
_MAX_ENV_TOTAL_CHARS = 65_536
_RESULT_TRUNCATION_MARKER = "\n...[truncated by Kitaru to fit Claude MCP result limit]"
_MAX_NON_STREAM_RESULT_FIELD_CHARS = 4_096

_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "command": {
            "anyOf": [
                {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": _MAX_COMMAND_STRING_CHARS,
                },
                {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": _MAX_COMMAND_ARG_CHARS,
                    },
                    "minItems": 1,
                    "maxItems": _MAX_COMMAND_ARGV_ITEMS,
                },
            ],
            "description": "Command string or argv-style command list to run.",
        },
        "cwd": {
            "anyOf": [
                {"type": "string", "maxLength": _MAX_CWD_CHARS},
                {"type": "null"},
            ],
            "description": "Optional working directory inside the sandbox.",
        },
        "env": {
            "anyOf": [
                {
                    "type": "object",
                    "maxProperties": _MAX_ENV_VARS,
                    "propertyNames": {
                        "type": "string",
                        "maxLength": _MAX_ENV_KEY_CHARS,
                    },
                    "additionalProperties": {
                        "type": "string",
                        "maxLength": _MAX_ENV_VALUE_CHARS,
                    },
                },
                {"type": "null"},
            ],
            "description": "Optional environment variables for the command.",
        },
        "max_chars": {
            "anyOf": [{"type": "integer", "minimum": 1}, {"type": "null"}],
            "description": (
                "Optional per-stream output collection limit. Claude may lower "
                "this per-call limit, but cannot raise it above the helper's "
                "configured default_max_chars."
            ),
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
    default_max_chars: int = DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS,
    default_cleanup: SandboxCommandCleanup = "destroy",
) -> Any:
    """Create a Claude Agent SDK MCP server for Kitaru sandbox commands.

    The returned object is meant for ``ClaudeAgentOptions(mcp_servers={...})``.
    It is not a Claude built-in tool and should not be passed through the
    ``tools=[...]`` option.
    """
    normalized_server_name = _normalize_identifier(server_name, "server_name")
    normalized_tool_name = _normalize_identifier(tool_name, "tool_name")
    normalized_description = description or _DEFAULT_DESCRIPTION
    normalized_max_chars = _normalize_default_max_chars(default_max_chars)
    normalized_cleanup = _normalize_default_cleanup(default_cleanup)
    tool_api, create_server = _claude_sdk_mcp_apis()

    sdk_tool = create_kitaru_sandbox_command_tool(
        tool_name=normalized_tool_name,
        description=normalized_description,
        default_max_chars=normalized_max_chars,
        default_cleanup=normalized_cleanup,
        tool_api=tool_api,
    )
    server = create_server(
        name=normalized_server_name, version="1.0.0", tools=[sdk_tool]
    )
    return _attach_metadata(
        server,
        server_name=normalized_server_name,
        tool_name=normalized_tool_name,
        description=normalized_description,
        default_max_chars=normalized_max_chars,
        default_cleanup=normalized_cleanup,
    )


def create_kitaru_sandbox_command_tool(
    *,
    tool_name: str = KITARU_SANDBOX_COMMAND_TOOL_NAME,
    description: str | None = None,
    default_max_chars: int = DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS,
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
        return _claude_tool_result(
            payload,
            max_size_chars=_tool_result_max_size_chars(normalized_max_chars),
        )

    return tool_factory(
        normalized_tool_name,
        description or _DEFAULT_DESCRIPTION,
        _input_schema(normalized_max_chars),
        annotations=_claude_tool_annotations(normalized_max_chars),
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
    _validate_known_tool_arguments(arguments)
    command = _tool_command(arguments.get("command", _MISSING))
    cwd = _tool_cwd(arguments.get("cwd"))
    env = _tool_env(arguments.get("env"))
    max_chars = _tool_max_chars(arguments.get("max_chars"), default_max_chars)
    cleanup = _tool_cleanup(arguments.get("cleanup"), default_cleanup)
    return command, cwd, env, max_chars, cleanup


_MISSING = object()


def _input_schema(default_max_chars: int) -> dict[str, Any]:
    schema = copy.deepcopy(_INPUT_SCHEMA)
    max_chars_schema = schema["properties"]["max_chars"]
    max_chars_schema["anyOf"][0]["maximum"] = default_max_chars
    return schema


def _validate_known_tool_arguments(arguments: Mapping[str, Any]) -> None:
    unknown = [
        key
        for key in arguments
        if not isinstance(key, str) or key not in _TOOL_ARGUMENT_KEYS
    ]
    if not unknown:
        return
    unknown_names = ", ".join(str(key) for key in sorted(unknown, key=str))
    allowed_names = ", ".join(sorted(_TOOL_ARGUMENT_KEYS))
    raise KitaruUsageError(
        "Sandbox command tool received unknown argument(s): "
        f"{unknown_names}. Allowed arguments are: {allowed_names}."
    )


def _tool_command(value: Any) -> SandboxCommand:
    if value is _MISSING:
        raise KitaruUsageError("Sandbox command tool input requires `command`.")
    command = sandbox_config._normalize_command(value)
    _validate_tool_command_size(command)
    return command


def _validate_tool_command_size(command: SandboxCommand) -> None:
    if isinstance(command, str):
        if len(command) > _MAX_COMMAND_STRING_CHARS:
            raise KitaruUsageError(
                "Sandbox command string may contain at most "
                f"{_MAX_COMMAND_STRING_CHARS} characters."
            )
        return
    if len(command) > _MAX_COMMAND_ARGV_ITEMS:
        raise KitaruUsageError(
            f"Sandbox command list may contain at most {_MAX_COMMAND_ARGV_ITEMS} items."
        )
    for item in command:
        if len(item) > _MAX_COMMAND_ARG_CHARS:
            raise KitaruUsageError(
                "Sandbox command list items may contain at most "
                f"{_MAX_COMMAND_ARG_CHARS} characters."
            )


def _tool_cwd(value: Any) -> str | None:
    cwd = sandbox_config._normalize_cwd(value)
    if cwd is not None and len(cwd) > _MAX_CWD_CHARS:
        raise KitaruUsageError(
            f"Sandbox command `cwd` may contain at most {_MAX_CWD_CHARS} characters."
        )
    return cwd


def _tool_env(value: Any) -> Mapping[str, str] | None:
    normalized = sandbox_config._normalize_env(value)
    _validate_tool_env_size(normalized)
    return normalized


def _validate_tool_env_size(env: Mapping[str, str] | None) -> None:
    if not env:
        return
    if len(env) > _MAX_ENV_VARS:
        raise KitaruUsageError(
            f"Sandbox command `env` may contain at most {_MAX_ENV_VARS} variables."
        )
    total_chars = 0
    for key, value in env.items():
        if len(key) > _MAX_ENV_KEY_CHARS:
            raise KitaruUsageError(
                "Sandbox command `env` keys may contain at most "
                f"{_MAX_ENV_KEY_CHARS} characters."
            )
        if len(value) > _MAX_ENV_VALUE_CHARS:
            raise KitaruUsageError(
                "Sandbox command `env` values may contain at most "
                f"{_MAX_ENV_VALUE_CHARS} characters."
            )
        total_chars += len(key) + len(value)
    if total_chars > _MAX_ENV_TOTAL_CHARS:
        raise KitaruUsageError(
            "Sandbox command `env` may contain at most "
            f"{_MAX_ENV_TOTAL_CHARS} total characters across keys and values."
        )


def _tool_max_chars(value: Any, default_max_chars: int) -> int:
    if value is None:
        return default_max_chars
    max_chars = sandbox_config._normalize_max_chars(value)
    if max_chars <= 0:
        raise KitaruUsageError(
            "Sandbox command `max_chars` must be a positive integer or null."
        )
    if max_chars > default_max_chars:
        raise KitaruUsageError(
            "Sandbox command `max_chars` must be a positive integer <= "
            f"configured default_max_chars ({default_max_chars}), or null."
        )
    return max_chars


def _tool_cleanup(
    value: Any,
    default_cleanup: SandboxCommandCleanup,
) -> SandboxCommandCleanup:
    if value is None:
        return default_cleanup
    return sandbox_config._normalize_cleanup(value)


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
    metadata = getattr(value, _PRIVATE_METADATA_ATTRIBUTE, None)
    if _is_kitaru_sandbox_mcp_metadata(metadata):
        return dict(cast(Mapping[str, Any], metadata))
    if isinstance(value, Mapping):
        metadata = value.get(KITARU_SANDBOX_MCP_METADATA_KEY)
        if _is_kitaru_sandbox_mcp_metadata(metadata):
            return dict(cast(Mapping[str, Any], metadata))
    return None


def allowed_tool_name(server_name: str, tool_name: str) -> str:
    """Build Claude's allowed-tools entry for an MCP server tool."""
    normalized_server_name = _normalize_identifier(server_name, "server_name")
    normalized_tool_name = _normalize_identifier(tool_name, "tool_name")
    return _format_allowed_tool_name(normalized_server_name, normalized_tool_name)


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


def _claude_tool_annotations(default_max_chars: int) -> Any:
    try:
        from claude_agent_sdk import ToolAnnotations
    except (ImportError, AttributeError) as exc:
        raise KitaruFeatureNotAvailableError(
            "The installed claude-agent-sdk does not expose ToolAnnotations, "
            "which Kitaru uses to preserve large sandbox command results."
        ) from exc
    annotations_factory = cast(Any, ToolAnnotations)
    return annotations_factory(
        maxResultSizeChars=_tool_result_max_size_chars(default_max_chars)
    )


def _tool_result_max_size_chars(default_max_chars: int) -> int:
    return min(
        (default_max_chars * 2) + _TOOL_RESULT_JSON_OVERHEAD_CHARS,
        _CLAUDE_MCP_MAX_RESULT_SIZE_CHARS,
    )


class _KitaruSandboxMcpServerConfig(dict[str, Any]):
    """Dict-shaped Claude MCP config with private Kitaru metadata."""

    def __init__(
        self,
        server: Mapping[str, Any],
        *,
        metadata: Mapping[str, Any],
    ) -> None:
        super().__init__(server)
        setattr(self, _PRIVATE_METADATA_ATTRIBUTE, dict(metadata))


def _is_kitaru_sandbox_mcp_metadata(value: Any) -> bool:
    return (
        isinstance(value, Mapping)
        and value.get("kind") == KITARU_SANDBOX_MCP_METADATA_KIND
    )


def _attach_metadata(
    server: Any,
    *,
    server_name: str,
    tool_name: str,
    description: str,
    default_max_chars: int,
    default_cleanup: SandboxCommandCleanup,
) -> Any:
    metadata = {
        "kind": KITARU_SANDBOX_MCP_METADATA_KIND,
        "server_name": server_name,
        "tool_name": tool_name,
        "description": description,
        "allowed_tool_name": allowed_tool_name(server_name, tool_name),
        "default_max_chars": default_max_chars,
        "default_cleanup": default_cleanup,
    }
    if isinstance(server, dict):
        return _KitaruSandboxMcpServerConfig(server, metadata=metadata)
    with suppress(AttributeError, TypeError):
        setattr(server, _PRIVATE_METADATA_ATTRIBUTE, metadata)
    return server


def _claude_tool_result(
    payload: dict[str, Any],
    *,
    max_size_chars: int,
) -> dict[str, Any]:
    fitted_payload = _fit_payload_to_result_size(payload, max_size_chars)
    return {
        "content": [
            {
                "type": "text",
                "text": _payload_json_text(fitted_payload),
            }
        ],
        "is_error": fitted_payload.get("status") == "failed",
    }


def _fit_payload_to_result_size(
    payload: dict[str, Any],
    max_size_chars: int,
) -> dict[str, Any]:
    if _payload_json_len(payload) <= max_size_chars:
        return payload

    fitted = copy.deepcopy(payload)
    if fitted.get("status") == "completed":
        fitted = _truncate_text_field_to_chars(
            fitted,
            ("cleanup", "error"),
            _MAX_NON_STREAM_RESULT_FIELD_CHARS,
        )
        stream_fields = sorted(
            (("stdout", "stdout_truncated"), ("stderr", "stderr_truncated")),
            key=lambda item: len(_get_nested_text(fitted, (item[0],)) or ""),
            reverse=True,
        )
        for field_name, flag_name in stream_fields:
            fitted = _fit_text_field_to_result_size(
                fitted,
                (field_name,),
                max_size_chars,
                truncation_flag=flag_name,
            )
            if _payload_json_len(fitted) <= max_size_chars:
                return fitted
        return _minimal_oversized_completed_payload(fitted, max_size_chars)

    fitted = _fit_text_field_to_result_size(
        fitted,
        ("error", "message"),
        max_size_chars,
        truncation_flag=None,
    )
    if _payload_json_len(fitted) <= max_size_chars:
        return fitted
    return _minimal_oversized_error_payload()


def _fit_text_field_to_result_size(
    payload: dict[str, Any],
    path: tuple[str, ...],
    max_size_chars: int,
    *,
    truncation_flag: str | None,
) -> dict[str, Any]:
    if _payload_json_len(payload) <= max_size_chars:
        return payload
    original = _get_nested_text(payload, path)
    if original is None or original == "":
        return payload

    base = copy.deepcopy(payload)
    if truncation_flag is not None:
        base[truncation_flag] = True
    _set_nested_value(base, path, "")
    if _payload_json_len(base) > max_size_chars:
        return base

    low = 0
    high = len(original)
    best = base
    while low <= high:
        keep_chars = (low + high) // 2
        candidate = copy.deepcopy(base)
        _set_nested_value(candidate, path, _truncated_text(original, keep_chars))
        if _payload_json_len(candidate) <= max_size_chars:
            best = candidate
            low = keep_chars + 1
        else:
            high = keep_chars - 1
    return best


def _truncate_text_field_to_chars(
    payload: dict[str, Any],
    path: tuple[str, ...],
    max_chars: int,
) -> dict[str, Any]:
    text = _get_nested_text(payload, path)
    if text is None or len(text) <= max_chars:
        return payload
    fitted = copy.deepcopy(payload)
    _set_nested_value(fitted, path, _truncated_text(text, max_chars))
    return fitted


def _truncated_text(text: str, keep_chars: int) -> str:
    if keep_chars >= len(text):
        return text
    if keep_chars <= 0:
        return _RESULT_TRUNCATION_MARKER.lstrip()
    return f"{text[:keep_chars]}{_RESULT_TRUNCATION_MARKER}"


def _minimal_oversized_completed_payload(
    payload: dict[str, Any],
    max_size_chars: int,
) -> dict[str, Any]:
    fitted = copy.deepcopy(payload)
    fitted["stdout"] = ""
    fitted["stderr"] = ""
    fitted["stdout_truncated"] = True
    fitted["stderr_truncated"] = True
    fitted = _truncate_text_field_to_chars(fitted, ("cleanup", "error"), 256)
    if _payload_json_len(fitted) <= max_size_chars:
        return fitted
    return _minimal_oversized_error_payload()


def _minimal_oversized_error_payload() -> dict[str, Any]:
    return {
        "status": "failed",
        "error": {
            "type": "KitaruRuntimeError",
            "category": "runtime",
            "message": (
                "Kitaru could not fit the sandbox MCP tool result inside "
                "Claude's result-size limit."
            ),
        },
    }


def _payload_json_text(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True)


def _payload_json_len(payload: Mapping[str, Any]) -> int:
    return len(_payload_json_text(payload))


def _get_nested_text(payload: Mapping[str, Any], path: tuple[str, ...]) -> str | None:
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current if isinstance(current, str) else None


def _set_nested_value(
    payload: dict[str, Any], path: tuple[str, ...], value: Any
) -> None:
    current: dict[str, Any] = payload
    for key in path[:-1]:
        nested = current.get(key)
        if not isinstance(nested, dict):
            nested = {}
            current[key] = nested
        current = nested
    current[path[-1]] = value


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


def _env_redactions(env: Mapping[str, str] | None) -> tuple[str, ...]:
    if env is None or not isinstance(env, Mapping):
        return ()
    values: set[str] = set()
    for key, value in env.items():
        if _should_redact_env_value(key, value):
            values.add(value)
    return tuple(sorted(values, key=lambda item: (-len(item), item)))


def _should_redact_env_value(key: str, value: str) -> bool:
    if not value:
        return False
    lowered_key = key.lower()
    if any(fragment in lowered_key for fragment in _SENSITIVE_ENV_KEY_FRAGMENTS):
        return True
    return len(value) >= 4


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
    normalized = value.strip()
    if "__" in normalized or _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise KitaruUsageError(
            f"Claude sandbox MCP `{field_name}` must contain only letters, "
            "digits, underscores, or hyphens, and must not contain `__`."
        )
    return normalized


def _normalize_default_max_chars(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise KitaruUsageError(
            "Claude sandbox MCP `default_max_chars` must be a positive integer."
        )
    if value > DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS:
        raise KitaruUsageError(
            "Claude sandbox MCP `default_max_chars` must be <= "
            f"{DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS}, so the JSON tool "
            "result stays within Claude's MCP result-size ceiling."
        )
    return value


def _normalize_default_cleanup(value: str) -> SandboxCommandCleanup:
    if value not in {sandbox_config.CLEANUP_DESTROY, sandbox_config.CLEANUP_CLOSE}:
        raise KitaruUsageError(
            "Claude sandbox MCP `default_cleanup` must be 'destroy' or 'close'."
        )
    return sandbox_config._normalize_cleanup(value)


__all__ = [
    "DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS",
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
