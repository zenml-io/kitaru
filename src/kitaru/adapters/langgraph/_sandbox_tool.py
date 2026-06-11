"""LangChain tool factory for Kitaru sandbox command execution."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, Field

import kitaru
from kitaru.errors import KitaruFeatureNotAvailableError, KitaruUsageError

DEFAULT_SANDBOX_COMMAND_TOOL_NAME = "run_sandbox_command"
DEFAULT_SANDBOX_COMMAND_TOOL_MAX_CHARS = 20_000
DEFAULT_SANDBOX_COMMAND_TOOL_DESCRIPTION = """\
Run one shell command in the active Kitaru stack sandbox. The tool returns JSON
with stdout, stderr, exit code, output truncation flags, and sandbox/session
metadata. A non-zero exit code is returned in that JSON and does not mean the
tool itself failed. Use this for shell-shaped inspection, tests, scripts, or
build commands. Do not use it for Deep Agents filesystem operations; this is
not a Deep Agents backend. Avoid commands that intentionally exfiltrate secrets
or depend on host-local files outside the sandbox.
"""


class SandboxCommandToolArgs(BaseModel):
    """Model-facing arguments for the Kitaru sandbox command tool."""

    command: str = Field(
        description="Shell command to run inside the active Kitaru stack sandbox."
    )
    cwd: str | None = Field(
        default=None,
        description=(
            "Optional working directory inside the sandbox. If omitted, the "
            "factory default_cwd is used."
        ),
    )


def create_sandbox_command_tool(
    *,
    name: str = DEFAULT_SANDBOX_COMMAND_TOOL_NAME,
    description: str | None = None,
    default_cwd: str | None = None,
    env: Mapping[str, str] | None = None,
    max_chars: int = DEFAULT_SANDBOX_COMMAND_TOOL_MAX_CHARS,
    cleanup: Literal["destroy", "close"] = "destroy",
) -> Any:
    """Create a LangChain tool that executes commands through Kitaru's sandbox.

    The returned tool exposes only ``command`` and ``cwd`` to the model. Static
    environment variables may be supplied by application code at factory time,
    but dynamic environment variables are intentionally not part of the model
    schema.
    """
    _validate_tool_factory_options(
        name=name,
        default_cwd=default_cwd,
        env=env,
        max_chars=max_chars,
        cleanup=cleanup,
    )

    StructuredTool = _import_structured_tool()
    copied_env = dict(env) if env is not None else None
    tool_description = description or DEFAULT_SANDBOX_COMMAND_TOOL_DESCRIPTION

    def _run_command(command: str, cwd: str | None = None) -> str:
        result = kitaru.run_sandbox_command(
            command,
            cwd=cwd if cwd is not None else default_cwd,
            env=copied_env,
            max_chars=max_chars,
            cleanup=cleanup,
        )
        return result.model_dump_json()

    return StructuredTool.from_function(
        func=_run_command,
        name=name.strip(),
        description=tool_description,
        args_schema=SandboxCommandToolArgs,
    )


def _validate_tool_factory_options(
    *,
    name: str,
    default_cwd: str | None,
    env: Mapping[str, str] | None,
    max_chars: int,
    cleanup: Literal["destroy", "close"],
) -> None:
    """Validate public factory options before the model can call the tool."""
    if not isinstance(name, str) or not name.strip():
        raise KitaruUsageError("Sandbox command tool name must be a non-empty string.")
    if default_cwd is not None and not isinstance(default_cwd, str):
        raise KitaruUsageError(
            "Sandbox command tool default_cwd must be a string or None."
        )
    if not isinstance(max_chars, int) or isinstance(max_chars, bool) or max_chars < 0:
        raise KitaruUsageError(
            "Sandbox command tool max_chars must be a non-negative integer."
        )
    if cleanup not in {"destroy", "close"}:
        raise KitaruUsageError(
            "Sandbox command tool cleanup must be either 'destroy' or 'close'."
        )
    if env is None:
        return
    if not isinstance(env, Mapping):
        raise KitaruUsageError("Sandbox command tool env must be a mapping or None.")
    invalid_items = [
        key
        for key, value in env.items()
        if not isinstance(key, str) or not isinstance(value, str)
    ]
    if invalid_items:
        raise KitaruUsageError(
            "Sandbox command tool env must map string keys to string values."
        )


def _import_structured_tool() -> Any:
    try:
        from langchain_core.tools import StructuredTool
    except ImportError as exc:
        raise KitaruFeatureNotAvailableError(
            "create_sandbox_command_tool() requires LangChain's StructuredTool "
            "API. Install the LangGraph/LangChain extras with "
            "`uv sync --extra langgraph` or `pip install 'kitaru[langgraph]'`, "
            "then retry."
        ) from exc
    return StructuredTool


__all__ = [
    "DEFAULT_SANDBOX_COMMAND_TOOL_DESCRIPTION",
    "DEFAULT_SANDBOX_COMMAND_TOOL_MAX_CHARS",
    "DEFAULT_SANDBOX_COMMAND_TOOL_NAME",
    "SandboxCommandToolArgs",
    "create_sandbox_command_tool",
]
