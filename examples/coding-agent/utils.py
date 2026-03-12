"""Tools, agents, and system prompts for the PydanticAI coding agent."""

import os
import subprocess
from pathlib import Path

from pydantic_ai import Agent, RunContext

from kitaru.adapters import pydantic_ai as kp

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MAX_CHARS: int = 30_000


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
# Tools — all receive RunContext[str] where deps is the working directory
# ---------------------------------------------------------------------------


def read_file(
    ctx: RunContext[str], path: str, offset: int = 0, limit: int = 2000
) -> str:
    """Read a file with line numbers.

    *offset* is 0-based, *limit* caps lines returned.
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
    # Make paths relative to cwd
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


# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------

READER_PROMPT: str = (
    "You are a code analysis assistant. Read files, search the codebase, "
    "and build a thorough understanding of the relevant code. "
    "Explain what you find clearly. Do NOT make any changes to files. "
    "Do NOT try to access files that are not in the working directory."
)

CODER_PROMPT: str = (
    "You are a code implementation assistant. Follow the provided plan exactly. "
    "Make the required changes, verify correctness by reading the result, "
    "and run any relevant commands to confirm the changes work."
)

# ---------------------------------------------------------------------------
# Pre-built wrapped agents
# ---------------------------------------------------------------------------

MODEL: str = os.environ.get("CODING_AGENT_MODEL", "anthropic:claude-sonnet-4-20250514")

reader = kp.wrap(
    Agent(
        MODEL,
        tools=[read_file, list_files, search_files, run_command],
        system_prompt=READER_PROMPT,
    ),
    name="reader",
    tool_capture_config={"mode": "full"},
)

coder = kp.wrap(
    Agent(
        MODEL,
        tools=[read_file, write_file, edit_file, list_files, search_files, run_command],
        system_prompt=CODER_PROMPT,
    ),
    name="coder",
    tool_capture_config={"mode": "full"},
)
