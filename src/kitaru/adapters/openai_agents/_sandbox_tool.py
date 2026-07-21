"""OpenAI Agents SDK tool factory for Kitaru sandbox commands."""

import asyncio
import json
from typing import Any, Final, Literal, cast

from agents.tool import FunctionTool

import kitaru
import kitaru.config as kitaru_config
from kitaru._config._sandbox import (
    normalize_timeout_seconds as _normalize_sandbox_timeout_seconds,
)
from kitaru.config import DEFAULT_SANDBOX_COMMAND_MAX_CHARS, SandboxCommandResult
from kitaru.errors import (
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruStateError,
    KitaruUsageError,
)

SandboxToolCleanupPolicy = Literal["destroy", "close"]

_CLEANUP_DESTROY: Final = "destroy"
_CLEANUP_CLOSE: Final = "close"
_VALID_CLEANUP_POLICIES: Final = (_CLEANUP_DESTROY, _CLEANUP_CLOSE)
_MODEL_VISIBLE_RESULT_FIELDS: Final = (
    "stdout",
    "stderr",
    "exit_code",
    "stdout_truncated",
    "stderr_truncated",
    "timed_out",
    "cleanup_succeeded",
    "cleanup_error",
)
_SANDBOX_TOOL_TIMEOUT_ERROR = (
    "sandbox_command_tool timeout_seconds must be a finite number greater than 0."
)
DEFAULT_SANDBOX_TOOL_TIMEOUT_SECONDS: Final = 30.0
_DEFAULT_TOOL_NAME = "kitaru_sandbox_command"
_DEFAULT_DESCRIPTION = (
    "Run a shell command in the sandbox attached to Kitaru's active stack. "
    "Inspect exit_code before trusting stdout."
)


def _tool_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "command": {
                "type": "string",
                "description": (
                    "Non-empty shell command to run inside the active stack sandbox."
                ),
            },
            "cwd": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "description": "Optional working directory inside the sandbox.",
            },
        },
        "required": ["command"],
        "additionalProperties": False,
    }


def sandbox_command_tool(
    *,
    name: str = _DEFAULT_TOOL_NAME,
    description: str | None = None,
    max_chars: int = DEFAULT_SANDBOX_COMMAND_MAX_CHARS,
    timeout_seconds: float | None = DEFAULT_SANDBOX_TOOL_TIMEOUT_SECONDS,
    cleanup: SandboxToolCleanupPolicy = _CLEANUP_DESTROY,
) -> FunctionTool:
    """Return an OpenAI Agents SDK tool backed by Kitaru's active sandbox.

    The model controls only the command string and optional working directory.
    Output size, timeout, and cleanup behavior stay application-owned factory
    settings.

    This keeps the model from setting environment variables on the tool call,
    but it does not make sandbox secrets private from model-chosen commands.
    A command such as ``cat /path/to/secret`` or ``env`` can still read any file,
    environment variable, credential, or network resource reachable from the
    active sandbox. Use least-privileged sandboxes with no unnecessary secrets,
    and wrap this tool with your own command allowlist or validator when prompts
    or users are not fully trusted.
    """
    tool_name = _normalize_tool_name(name)
    tool_description = _normalize_description(description)
    normalized_max_chars = _normalize_max_chars(max_chars)
    normalized_timeout_seconds = _normalize_timeout_seconds(timeout_seconds)
    normalized_cleanup = _normalize_cleanup(cleanup)

    async def _invoke_sandbox_command(_context: Any, input_json: str) -> str:
        parsed = _parse_tool_input(input_json)
        if "error" in parsed:
            return _json_result(parsed)

        command_options: dict[str, Any] = {
            "cwd": cast(str | None, parsed["cwd"]),
            "max_chars": normalized_max_chars,
            "cleanup": normalized_cleanup,
        }
        if normalized_timeout_seconds is not None:
            command_options["timeout_seconds"] = normalized_timeout_seconds
        result = await asyncio.to_thread(
            kitaru.run_sandbox_command,
            cast(str, parsed["command"]),
            **command_options,
        )
        return _json_result(_model_visible_result_payload(result))

    tool = FunctionTool(
        name=tool_name,
        description=tool_description,
        params_json_schema=_tool_schema(),
        on_invoke_tool=_invoke_sandbox_command,
        strict_json_schema=False,
    )

    def _cache_identity() -> dict[str, Any]:
        identity: dict[str, Any] = {
            "kind": "sandbox_command_tool",
            "max_chars": normalized_max_chars,
            "cleanup": normalized_cleanup,
            "active_sandbox": _active_sandbox_cache_identity(),
        }
        if normalized_timeout_seconds is not None:
            identity["timeout_seconds"] = normalized_timeout_seconds
        return identity

    object.__setattr__(tool, "_kitaru_cache_identity", _cache_identity)
    return tool


def _model_visible_result_payload(result: SandboxCommandResult) -> dict[str, Any]:
    return result.model_dump(mode="json", include=set(_MODEL_VISIBLE_RESULT_FIELDS))


def _active_sandbox_cache_identity() -> dict[str, Any]:
    try:
        return kitaru_config._active_sandbox_cache_identity()
    except (
        KitaruBackendError,
        KitaruFeatureNotAvailableError,
        KitaruStateError,
    ) as exc:
        return {
            "kind": "active_sandbox_unavailable",
            "error_type": type(exc).__name__,
            "message": str(exc),
        }


def _normalize_tool_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise KitaruUsageError("sandbox_command_tool name must be a non-empty string.")
    return name.strip()


def _normalize_description(description: str | None) -> str:
    if description is None:
        return _DEFAULT_DESCRIPTION
    if not isinstance(description, str) or not description.strip():
        raise KitaruUsageError(
            "sandbox_command_tool description must be a non-empty string or None."
        )
    return description.strip()


def _normalize_max_chars(max_chars: int) -> int:
    if isinstance(max_chars, bool) or not isinstance(max_chars, int):
        raise KitaruUsageError("sandbox_command_tool max_chars must be an integer.")
    if max_chars < 0:
        raise KitaruUsageError("sandbox_command_tool max_chars must be >= 0.")
    return max_chars


def _normalize_timeout_seconds(timeout_seconds: float | None) -> float | None:
    return _normalize_sandbox_timeout_seconds(
        timeout_seconds,
        error_message=_SANDBOX_TOOL_TIMEOUT_ERROR,
    )


def _normalize_cleanup(cleanup: str) -> SandboxToolCleanupPolicy:
    if cleanup not in _VALID_CLEANUP_POLICIES:
        expected = " or ".join(f"'{policy}'" for policy in _VALID_CLEANUP_POLICIES)
        raise KitaruUsageError(
            f"sandbox_command_tool cleanup must be either {expected}."
        )
    return cleanup


def _parse_tool_input(input_json: str) -> dict[str, Any]:
    try:
        payload = json.loads(input_json)
    except json.JSONDecodeError:
        return _input_error("Tool input must be valid JSON.")

    if not isinstance(payload, dict):
        return _input_error("Tool input must be a JSON object.")

    extra_fields = sorted(set(payload) - {"command", "cwd"})
    if extra_fields:
        return _input_error(
            "Unsupported tool input field(s): " + ", ".join(extra_fields)
        )

    command = payload.get("command")
    if not isinstance(command, str) or not command.strip():
        return _input_error("Tool input field 'command' must be a non-empty string.")

    cwd = payload.get("cwd")
    if cwd is not None and not isinstance(cwd, str):
        return _input_error("Tool input field 'cwd' must be a string or null.")

    return {"command": command, "cwd": cwd}


def _input_error(message: str) -> dict[str, Any]:
    return {"error": {"code": "invalid_tool_input", "message": message}}


def _json_result(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


__all__ = [
    "DEFAULT_SANDBOX_TOOL_TIMEOUT_SECONDS",
    "SandboxToolCleanupPolicy",
    "sandbox_command_tool",
]
