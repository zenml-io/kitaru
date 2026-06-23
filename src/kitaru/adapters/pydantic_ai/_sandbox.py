from __future__ import annotations

from typing import Any, Final

import kitaru
from kitaru._config._sandbox import (
    SandboxCleanupPolicy,
    _normalize_cleanup,
    _normalize_max_chars,
)
from kitaru.config import SandboxCommandResult
from pydantic import BaseModel, ConfigDict
from pydantic_ai import FunctionToolset

SANDBOX_COMMAND_TOOL_NAME: Final = "run_sandbox_command"
DEFAULT_SANDBOX_TOOL_MAX_CHARS: Final = 20_000


class SandboxCommandToolResult(BaseModel):
    """LLM-facing result for one command executed in the active sandbox."""

    command: str
    cwd: str | None
    stdout: str
    stderr: str
    exit_code: int
    stdout_truncated: bool
    stderr_truncated: bool
    cleanup_succeeded: bool
    cleanup_error: str | None

    model_config = ConfigDict(extra="forbid")


_SANDBOX_COMMAND_TOOL_RESULT_FIELDS: Final[set[str]] = set(
    SandboxCommandToolResult.model_fields
)


def _tool_result_from_core_result(
    result: SandboxCommandResult,
) -> SandboxCommandToolResult:
    """Copy only model-useful fields from the core sandbox command result."""
    return SandboxCommandToolResult.model_validate(
        result.model_dump(include=_SANDBOX_COMMAND_TOOL_RESULT_FIELDS)
    )


def sandbox_command_toolset(
    *,
    max_chars: int = DEFAULT_SANDBOX_TOOL_MAX_CHARS,
    cleanup: SandboxCleanupPolicy = "destroy",
) -> FunctionToolset[Any]:
    """Return a PydanticAI toolset for active-stack sandbox commands.

    The returned toolset contains one tool, ``run_sandbox_command``. The model
    controls the command string and optional sandbox working directory. The
    Python caller controls output truncation and session cleanup through this
    factory, and environment variables are intentionally not exposed as tool
    arguments.
    """
    normalized_max_chars = _normalize_max_chars(max_chars)
    normalized_cleanup = _normalize_cleanup(cleanup)
    toolset: FunctionToolset[Any] = FunctionToolset()

    @toolset.tool_plain(name=SANDBOX_COMMAND_TOOL_NAME)
    def run_sandbox_command(
        command: str,
        cwd: str | None = None,
    ) -> SandboxCommandToolResult:
        """Run one shell command in the active Kitaru stack sandbox."""
        result = kitaru.run_sandbox_command(
            command,
            cwd=cwd,
            max_chars=normalized_max_chars,
            cleanup=normalized_cleanup,
        )
        return _tool_result_from_core_result(result)

    return toolset
