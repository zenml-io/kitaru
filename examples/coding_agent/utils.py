"""Tools, agents, and system prompts for the PydanticAI coding agent."""

import os
import subprocess
from pathlib import Path
from typing import Any, Dict

from pydantic_ai import Agent, RunContext

from kitaru.adapters import pydantic_ai as kp
from kitaru.adapters.pydantic_ai import resolve_model

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MAX_CHARS: int = int(os.environ.get("CODING_AGENT_MAX_CHARS", "12000"))


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

_DEFAULT_READ_LIMIT: int = int(os.environ.get("CODING_AGENT_READ_LIMIT", "400"))


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
# System prompts
# ---------------------------------------------------------------------------

RESEARCHER_PROMPT: str = (
    "You are a code analysis assistant. Your job is to read files and search "
    "the codebase to build a thorough understanding of the relevant code for "
    "the given task.\n\n"
    "Guidelines:\n"
    "- Start with narrow, targeted searches "
    "(grep for key terms, list specific directories)\n"
    "- Read small file windows (use offset/limit to page through large files)\n"
    "- Focus on files directly relevant to the task\n"
    "- Do NOT make any changes to files\n"
    "- Do NOT try to access files outside the working directory\n"
    "- Produce a concise summary: relevant files, current behavior, constraints, "
    "and any unknowns"
)

PLANNER_PROMPT: str = (
    "You are a planning assistant. You receive a task description and a research "
    "analysis of the codebase. Your job is to create a clear, actionable "
    "implementation plan.\n\n"
    "Guidelines:\n"
    "- Rely ONLY on the research analysis provided — you have no file access\n"
    "- Produce a numbered plan with: files to modify, specific changes, order "
    "of operations\n"
    "- Include verification steps the implementer should run\n"
    "- Keep it compact — bullet points, not essays\n"
    "- If the analysis is missing information, note it as a gap rather than guessing"
)

CODER_PROMPT: str = (
    "You are a code implementation assistant. Follow the provided plan exactly. "
    "Make the required changes, verify correctness, and confirm the changes work.\n\n"
    "Guidelines:\n"
    "- Prefer edit_file over write_file for existing files (smaller, safer edits)\n"
    "- Use git_diff to inspect your changes instead of re-reading entire files\n"
    "- Run targeted verification commands (specific tests, type checks on changed "
    "files) — not the full test suite\n"
    "- If the plan references files you haven't seen, read just the relevant "
    "sections using offset/limit"
)

# ---------------------------------------------------------------------------
# Pre-built wrapped agents
# ---------------------------------------------------------------------------

MODEL: Any
_env_overlay: Dict[str, str]
MODEL, _env_overlay = resolve_model(
    os.environ.get("CODING_AGENT_MODEL") or "coding-agent"
)
os.environ.update(_env_overlay)

researcher: Any = kp.wrap(
    Agent(
        MODEL,
        tools=[read_file, list_files, search_files],
        system_prompt=RESEARCHER_PROMPT,
    ),
    name="researcher",
    tool_capture_config={"mode": "metadata_only"},
)

planner: Any = kp.wrap(
    Agent(
        MODEL,
        system_prompt=PLANNER_PROMPT,
    ),
    name="planner",
    tool_capture_config={"mode": "metadata_only"},
)

coder: Any = kp.wrap(
    Agent(
        MODEL,
        tools=[
            read_file,
            write_file,
            edit_file,
            list_files,
            search_files,
            run_command,
            git_diff,
        ],
        system_prompt=CODER_PROMPT,
    ),
    name="coder",
    tool_capture_config={"mode": "metadata_only"},
)
