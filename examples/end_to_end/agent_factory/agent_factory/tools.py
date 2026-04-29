"""Tool factory.

Stage 1 ships only `exec` (in-process subprocess). Stage 2 routes `exec`
through an optional `DockerWorker` so the same `ExecResult` shape is
preserved while gaining filesystem and network isolation.
"""

import subprocess
from typing import Protocol

from pydantic import BaseModel
from pydantic_ai import Tool

from .permissions import PermissionHandler

_MAX_OUTPUT_LINES = 200
_MAX_LINE_BYTES = 2000


class ExecResult(BaseModel):
    exit_code: int
    stdout: str
    stderr: str


class _Worker(Protocol):
    """Anything with a `run(command) -> ExecResult` is a valid sandbox.

    Stage 2's DockerWorker satisfies this; future Modal/remote workers can
    too without the tool factory caring which.
    """

    def run(self, command: str) -> ExecResult: ...


def _truncate(text: str, max_lines: int = _MAX_OUTPUT_LINES) -> str:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line
        if len(line.encode()) > _MAX_LINE_BYTES:
            line = (
                line.encode()[:_MAX_LINE_BYTES].decode(errors="replace")
                + "...<line truncated>"
            )
        lines.append(line)
        if len(lines) >= max_lines:
            lines.append(f"...<truncated; first {max_lines} lines shown>")
            break
    return "\n".join(lines)


def _run_exec_in_process(command: str) -> ExecResult:
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


def build_tools(
    permission_handler: PermissionHandler,
    *,
    worker: _Worker | None = None,
) -> list[Tool]:
    """Build the pydantic-ai toolset for an agent based on its profile's permissions.

    Pass a `worker` to route `exec` through a sandbox (stage 2+); omit it to
    run shell commands in the host process (stage 1).
    """
    tools: list[Tool] = []

    if permission_handler.can_use_tool("exec"):
        sandboxed = worker is not None
        if worker is not None:
            captured_worker = worker

            def exec_tool(command: str) -> ExecResult:
                permission_handler.require_tool("exec")
                return captured_worker.run(command)
        else:

            def exec_tool(command: str) -> ExecResult:
                permission_handler.require_tool("exec")
                return _run_exec_in_process(command)

        location = (
            "in an isolated Docker worker" if sandboxed else "in the host process"
        )
        tools.append(
            Tool(
                exec_tool,
                name="exec",
                description=(
                    f"Run a shell command {location}. Returns exit_code, stdout, "
                    "stderr. stdout/stderr are truncated to ~200 lines."
                ),
            )
        )

    return tools
