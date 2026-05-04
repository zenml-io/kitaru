"""Tool factory.

Stage 1 ships only `exec` (in-process subprocess). Stage 2 routes `exec`
through an optional `DockerSandbox` so the same `ExecResult` shape is
preserved while gaining filesystem and network isolation. Stage 3 adds
the host-side `skill` tool (list/read/search over a markdown directory).
Stage 5 adds the host-side `exec_service` tool (typed-union dispatcher
over the services package).
"""

import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ValidationError
from pydantic_ai import Tool

from .permissions import PermissionHandler
from .services import ALL_SERVICES, build_service_description

_MAX_OUTPUT_LINES = 200
_MAX_LINE_BYTES = 2000

_SKILL_LIST_GLOB = "**/SKILL.md"
_SKILL_MAX_RESULTS = 200
_SKILL_MAX_READ_BYTES = 100_000


class ExecResult(BaseModel):
    exit_code: int
    stdout: str
    stderr: str


class _Sandbox(Protocol):
    """Anything with a `run(command) -> ExecResult` is a valid sandbox.

    Stage 2's DockerSandbox satisfies this; future Modal/remote sandboxes can
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


def _resolve_skill_path(skills_root: Path, relative: str) -> Path:
    """Resolve a relative skill path under skills_root; reject escapes."""
    resolved = (skills_root / relative).resolve()
    if not resolved.is_relative_to(skills_root):
        raise ValueError(
            f"Invalid skill path {relative!r}: path must stay within "
            f"{skills_root}. Use action='list' to discover valid paths."
        )
    return resolved


def _list_skill_files(skills_root: Path) -> list[Path]:
    return sorted(skills_root.glob(_SKILL_LIST_GLOB))


def _run_skill(
    skills_root: Path,
    action: Literal["list", "read", "search"],
    *,
    path: str | None,
    query: str | None,
) -> dict[str, Any]:
    """Host-side skill tool: list, read, or search markdown skill files."""
    if not skills_root.exists():
        return {"action": action, "skills_root": None, "count": 0}

    if action == "list":
        files = _list_skill_files(skills_root)[:_SKILL_MAX_RESULTS]
        return {
            "action": "list",
            "skills_root": str(skills_root),
            "count": len(files),
            "items": [
                {
                    "path": f.relative_to(skills_root).as_posix(),
                    "bytes": f.stat().st_size,
                }
                for f in files
            ],
        }

    if action == "read":
        if not path:
            raise ValueError("`path` is required when action='read'")
        target = _resolve_skill_path(skills_root, path)
        if not target.is_file():
            raise ValueError(
                f"Skill file not found: {path}. Use action='list' first."
            )
        raw = target.read_bytes()
        truncated = len(raw) > _SKILL_MAX_READ_BYTES
        content = raw[:_SKILL_MAX_READ_BYTES].decode("utf-8", errors="replace")
        return {
            "action": "read",
            "skills_root": str(skills_root),
            "path": target.relative_to(skills_root).as_posix(),
            "truncated": truncated,
            "content": content,
        }

    if not query:
        raise ValueError("`query` is required when action='search'")
    needle = query.lower()
    matches: list[dict[str, Any]] = []
    for skill_file in _list_skill_files(skills_root):
        try:
            text = skill_file.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for index, line in enumerate(text.splitlines(), start=1):
            if needle in line.lower():
                matches.append(
                    {
                        "path": skill_file.relative_to(skills_root).as_posix(),
                        "line_number": index,
                        "line": line,
                    }
                )
                if len(matches) >= _SKILL_MAX_RESULTS:
                    break
        if len(matches) >= _SKILL_MAX_RESULTS:
            break
    return {
        "action": "search",
        "skills_root": str(skills_root),
        "count": len(matches),
        "items": matches,
    }


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
    sandbox: _Sandbox | None = None,
    skills_directory: Path | None = None,
    allowed_services: set[str] | None = None,
) -> list[Tool]:
    """Build the pydantic-ai toolset for an agent based on its profile's permissions.

    Pass a `sandbox` to route `exec` through it (stage 2+); omit it to run
    shell commands in the host process (stage 1). Pass a `skills_directory`
    to enable the `skill` tool (stage 3+). Pass `allowed_services` to
    enable `exec_service` (stage 5+) — the tool description is built
    dynamically from this set so the LLM only sees the services this
    agent can actually dispatch to.
    """
    tools: list[Tool] = []

    if permission_handler.can_use_tool("exec"):
        sandboxed = sandbox is not None
        runner: Callable[[str], ExecResult] = (
            sandbox.run if sandbox is not None else _run_exec_in_process
        )

        def exec_tool(command: str) -> ExecResult:
            permission_handler.require_tool("exec")
            return runner(command)

        location = (
            "in an isolated Docker sandbox" if sandboxed else "in the host process"
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

    if permission_handler.can_use_tool("exec_service"):
        services = allowed_services or set()
        unknown = services - ALL_SERVICES.keys()
        if unknown:
            raise ValueError(
                f"Profile.allowed_services contains unknown service names: "
                f"{sorted(unknown)}. Known services: {sorted(ALL_SERVICES)}."
            )

        def exec_service(service_name: str, args: dict[str, Any]) -> Any:
            permission_handler.require_tool("exec_service")
            if service_name not in services:
                raise ValueError(
                    f"Service {service_name!r} is not in this agent's "
                    f"allowed_services. Allowed: {sorted(services)}."
                )
            call = ALL_SERVICES[service_name]
            try:
                validated = call.args_model.model_validate(args)
            except ValidationError as exc:
                raise ValueError(
                    f"Invalid args for {service_name!r}: {exc.errors()}"
                ) from exc
            return call.handler(validated).model_dump()

        tools.append(
            Tool(
                exec_service,
                name="exec_service",
                description=build_service_description(services),
            )
        )

    if permission_handler.can_use_tool("skill"):
        if skills_directory is None:
            raise ValueError(
                "Profile.allowed_tools includes 'skill' but no skill_source "
                "was configured. Set Profile.skill_source to a SkillSource "
                "(e.g. LocalSkillSource(path=...))."
            )
        skills_root = skills_directory.resolve()

        def skill_tool(
            action: Literal["list", "read", "search"],
            path: str | None = None,
            query: str | None = None,
        ) -> dict[str, Any]:
            permission_handler.require_tool("skill")
            return _run_skill(skills_root, action, path=path, query=query)

        tools.append(
            Tool(
                skill_tool,
                name="skill",
                description=(
                    "Read this agent's procedure from local markdown files. "
                    "Use `action='list'` first to discover available skill files, "
                    "then `action='read'` with a path returned by list to fetch one, "
                    "or `action='search'` with a query to grep across them. "
                    "Always read your skill before doing anything else."
                ),
            )
        )

    return tools
