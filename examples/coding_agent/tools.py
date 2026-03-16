"""Tools and helpers for the coding agent.

All tool functions receive ``RunContext[str]`` where deps is the working
directory path.  The ``TOOL_REGISTRY`` dict maps tool names to callables
so that skills can reference tools by name in their ``skill.md`` frontmatter.
"""

import os
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

from pydantic_ai import RunContext

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MAX_CHARS: int = int(os.environ.get("CODING_AGENT_MAX_CHARS", "12000"))
_DEFAULT_READ_LIMIT: int = int(os.environ.get("CODING_AGENT_READ_LIMIT", "400"))


def _truncate(text: str, max_chars: int = _MAX_CHARS) -> str:
    """Truncate text and append a notice if it exceeds *max_chars*."""
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"\n... [truncated, {len(text)} chars total]"


def _resolve(cwd: str, path: str) -> Path:
    """Resolve *path* relative to *cwd*, blocking escapes above *cwd*."""
    base = Path(cwd).resolve()
    target = (base / path).resolve()
    if not target.is_relative_to(base):
        raise ValueError(f"Path escapes working directory: {path}")
    return target


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


def read_file(
    ctx: RunContext[str], path: str, offset: int = 0, limit: int = _DEFAULT_READ_LIMIT
) -> str:
    """Read a file with line numbers.

    *offset* is 0-based, *limit* caps lines returned (default 400).
    Use offset to page through large files rather than reading them whole.
    """
    target = _resolve(ctx.deps, path)
    try:
        lines = target.read_text().splitlines()
    except FileNotFoundError:
        return f"Error: file not found: {path}"
    except PermissionError:
        return f"Error: permission denied: {path}"

    selected = lines[offset : offset + limit]
    numbered = [f"{i + offset + 1:>6}\t{line}" for i, line in enumerate(selected)]
    return _truncate("\n".join(numbered))


def write_file(ctx: RunContext[str], path: str, content: str) -> str:
    """Write *content* to *path*, creating parent directories as needed."""
    target = _resolve(ctx.deps, path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    except PermissionError:
        return f"Error: permission denied: {path}"
    return f"Wrote {len(content)} chars to {path}"


def edit_file(ctx: RunContext[str], path: str, old: str, new: str) -> str:
    """Replace a single exact occurrence of *old* with *new* in *path*."""
    target = _resolve(ctx.deps, path)
    try:
        text = target.read_text()
    except FileNotFoundError:
        return f"Error: file not found: {path}"
    except PermissionError:
        return f"Error: permission denied: {path}"

    count = text.count(old)
    if count == 0:
        return f"Error: old string not found in {path}"
    if count > 1:
        return f"Error: old string appears {count} times in {path} (expected exactly 1)"

    target.write_text(text.replace(old, new, 1))
    return f"Edited {path}"


def list_files(ctx: RunContext[str], pattern: str) -> str:
    """List files matching a glob *pattern* relative to the working directory."""
    base = Path(ctx.deps).resolve()
    matches = sorted(base.glob(pattern))[:500]
    relative = [str(m.relative_to(base)) for m in matches if m.is_file()]
    if not relative:
        return f"No files matching: {pattern}"
    return "\n".join(relative)


def search_files(ctx: RunContext[str], pattern: str, glob: str = "**/*") -> str:
    """Search file contents for *pattern* using grep. Returns matching lines."""
    base = Path(ctx.deps).resolve()
    try:
        result = subprocess.run(
            ["grep", "-rn", "--include", glob, pattern, str(base)],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        return "Error: search timed out after 30s"

    output = result.stdout
    output = output.replace(str(base) + "/", "")
    return _truncate(output) if output else f"No matches for: {pattern}"


def run_command(ctx: RunContext[str], command: str, timeout: int = 30) -> str:
    """Run a shell command in the working directory. Returns exit code + output."""
    base = Path(ctx.deps).resolve()
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(base),
        )
    except subprocess.TimeoutExpired:
        return f"Error: command timed out after {timeout}s"

    output = result.stdout + result.stderr
    return _truncate(f"Exit code: {result.returncode}\n{output}")


def git_diff(ctx: RunContext[str], path: str | None = None) -> str:
    """Show uncommitted changes via git diff.

    If *path* is given, scopes the diff to that file. Returns a clear
    error if the working directory is not a git repository.
    """
    base = Path(ctx.deps).resolve()
    cmd = ["git", "diff"]
    if path is not None:
        target = _resolve(ctx.deps, path)
        cmd.append(str(target))
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(base),
        )
    except subprocess.TimeoutExpired:
        return "Error: git diff timed out after 30s"

    if result.returncode != 0:
        stderr = result.stderr.strip()
        if "not a git repository" in stderr.lower():
            return "Error: not a git repository. Use read_file to inspect changes."
        return f"Error: git diff failed (exit {result.returncode}): {stderr}"

    output = result.stdout
    if not output:
        return "No uncommitted changes" + (f" in {path}" if path else "")
    return _truncate(output)


# ---------------------------------------------------------------------------
# Tool collections + registry
# ---------------------------------------------------------------------------

RESEARCHER_TOOLS: list[Any] = [read_file, list_files, search_files]

CODER_TOOLS: list[Any] = [
    read_file,
    write_file,
    edit_file,
    list_files,
    search_files,
    run_command,
    git_diff,
]

TOOL_REGISTRY: dict[str, Callable[..., Any]] = {f.__name__: f for f in CODER_TOOLS}
