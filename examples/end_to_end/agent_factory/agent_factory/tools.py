"""Tool factory — stage 1 ships only `exec` (in-process subprocess).

Later stages replace `_run_exec` with a Docker-backed worker but keep the
same `ExecResult` shape and the same permission-gated factory.
"""

import subprocess

from pydantic import BaseModel
from pydantic_ai import Tool

from .permissions import PermissionHandler

_MAX_OUTPUT_LINES = 200
_MAX_LINE_BYTES = 2000


class ExecResult(BaseModel):
    exit_code: int
    stdout: str
    stderr: str


def _truncate(text: str, max_lines: int = _MAX_OUTPUT_LINES) -> str:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line
        if len(line.encode()) > _MAX_LINE_BYTES:
            line = line.encode()[:_MAX_LINE_BYTES].decode(errors="replace") + "...<line truncated>"
        lines.append(line)
        if len(lines) >= max_lines:
            lines.append(f"...<truncated; first {max_lines} lines shown>")
            break
    return "\n".join(lines)


def _run_exec(command: str) -> ExecResult:
    """Run a shell command in the host process and return its output."""
    completed = subprocess.run(
        ["bash", "-c", command],
        capture_output=True,
        text=True,
    )
    return ExecResult(
        exit_code=completed.returncode,
        stdout=_truncate(completed.stdout),
        stderr=_truncate(completed.stderr),
    )


def build_tools(permission_handler: PermissionHandler) -> list[Tool]:
    """Build the pydantic-ai toolset for an agent based on its profile's permissions."""
    tools: list[Tool] = []

    if permission_handler.can_use_tool("exec"):

        def exec_tool(command: str) -> ExecResult:
            permission_handler.require_tool("exec")
            return _run_exec(command)

        tools.append(
            Tool(
                exec_tool,
                name="exec",
                description=(
                    "Run a shell command in the host process. Returns exit_code, "
                    "stdout, stderr. stdout/stderr are truncated to ~200 lines."
                ),
            )
        )

    return tools
