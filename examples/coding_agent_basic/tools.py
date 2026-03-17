"""Tool implementations and schemas for the basic coding agent."""

import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

_MAX_CHARS = 12_000
_READ_LIMIT = 400


def _truncate(text: str, max_chars: int = _MAX_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + f"\n... [truncated, {len(text)} chars total]"


def _resolve(cwd: str, path: str) -> Path:
    """Resolve a relative path within the working directory."""
    base = Path(cwd).resolve()
    target = (base / path).resolve()
    if not target.is_relative_to(base):
        raise ValueError(f"Path escapes working directory: {path}")
    return target


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------


def read_file(cwd: str, path: str, offset: int = 0, limit: int = _READ_LIMIT) -> str:
    """Read a file with line numbers."""
    target = _resolve(cwd, path)
    try:
        lines = target.read_text().splitlines()
    except FileNotFoundError:
        return f"Error: file not found: {path}"
    except Exception as e:
        return f"Error reading {path}: {e}"

    selected = lines[offset : offset + limit]
    numbered = [f"{i + offset + 1:>6}\t{line}" for i, line in enumerate(selected)]
    result = "\n".join(numbered)
    if len(lines) > offset + limit:
        result += f"\n... [showing lines {offset + 1}-{offset + len(selected)} of {len(lines)}]"
    return _truncate(result)


def write_file(cwd: str, path: str, content: str) -> str:
    """Write content to a file."""
    target = _resolve(cwd, path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)
    return f"Wrote {len(content)} chars to {path}"


def edit_file(cwd: str, path: str, old: str, new: str) -> str:
    """Replace a single exact occurrence of old with new."""
    target = _resolve(cwd, path)
    try:
        text = target.read_text()
    except FileNotFoundError:
        return f"Error: file not found: {path}"

    count = text.count(old)
    if count == 0:
        return f"Error: old string not found in {path}"
    if count > 1:
        return f"Error: old string appears {count} times in {path} (expected exactly 1)"

    target.write_text(text.replace(old, new, 1))
    return f"Edited {path}"


def list_files(cwd: str, pattern: str) -> str:
    """List files matching a glob pattern."""
    base = Path(cwd).resolve()
    matches = sorted(base.glob(pattern))[:500]
    relative = [str(m.relative_to(base)) for m in matches if m.is_file()]
    if not relative:
        return f"No files matching: {pattern}"
    return "\n".join(relative)


def search_files(cwd: str, pattern: str, glob: str = "**/*") -> str:
    """Search file contents for a pattern using grep."""
    base = Path(cwd).resolve()
    try:
        result = subprocess.run(
            ["grep", "-rn", "--include", glob, pattern, str(base)],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired:
        return "Error: search timed out after 30s"
    output = result.stdout.replace(str(base) + "/", "")
    return _truncate(output) if output else f"No matches for: {pattern}"


def run_command(cwd: str, command: str, timeout: int = 30) -> str:
    """Run a shell command in the working directory."""
    base = Path(cwd).resolve()
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
    except Exception as e:
        return f"Error executing command: {e}"

    output = result.stdout + result.stderr
    return _truncate(f"Exit code: {result.returncode}\n{output}")


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

_TOOL_FUNCTIONS: dict[str, Callable[..., str]] = {
    "read_file": read_file,
    "write_file": write_file,
    "edit_file": edit_file,
    "list_files": list_files,
    "search_files": search_files,
    "run_command": run_command,
}


def dispatch_tool(cwd: str, name: str, arguments: dict[str, Any]) -> str:
    """Execute a tool by name."""
    func = _TOOL_FUNCTIONS.get(name)
    if func is None:
        return f"Error: unknown tool '{name}'"
    try:
        return func(cwd, **arguments)
    except Exception as exc:
        return f"Error running {name}: {type(exc).__name__}: {exc}"


# ---------------------------------------------------------------------------
# Schemas (OpenAI function-calling format)
# ---------------------------------------------------------------------------

_READ_FILE_SCHEMA: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "read_file",
        "description": "Read a file with line numbers. Use offset/limit to page through large files.",
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "File path relative to cwd"},
                "offset": {"type": "integer", "description": "0-based line offset", "default": 0},
                "limit": {"type": "integer", "description": "Max lines to return", "default": 400},
            },
            "required": ["path"],
        },
    },
}

_LIST_FILES_SCHEMA: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "list_files",
        "description": "List files matching a glob pattern relative to the working directory.",
        "parameters": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Glob pattern (e.g. '**/*.py')"},
            },
            "required": ["pattern"],
        },
    },
}

_SEARCH_FILES_SCHEMA: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "search_files",
        "description": "Search file contents for a pattern using grep.",
        "parameters": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Search pattern (regex)"},
                "glob": {"type": "string", "description": "File glob to limit search", "default": "**/*"},
            },
            "required": ["pattern"],
        },
    },
}

_WRITE_FILE_SCHEMA: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "write_file",
        "description": "Write content to a file, creating parent directories as needed.",
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "File path relative to cwd"},
                "content": {"type": "string", "description": "File content to write"},
            },
            "required": ["path", "content"],
        },
    },
}

_EDIT_FILE_SCHEMA: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "edit_file",
        "description": "Replace a single exact occurrence of old with new in a file.",
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "File path relative to cwd"},
                "old": {"type": "string", "description": "Exact text to find"},
                "new": {"type": "string", "description": "Replacement text"},
            },
            "required": ["path", "old", "new"],
        },
    },
}

_RUN_COMMAND_SCHEMA: dict[str, Any] = {
    "type": "function",
    "function": {
        "name": "run_command",
        "description": "Run a shell command in the working directory.",
        "parameters": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "Shell command to run"},
                "timeout": {"type": "integer", "description": "Timeout in seconds", "default": 30},
            },
            "required": ["command"],
        },
    },
}

READER_SCHEMAS: list[dict[str, Any]] = [
    _READ_FILE_SCHEMA,
    _LIST_FILES_SCHEMA,
    _SEARCH_FILES_SCHEMA,
]

ALL_SCHEMAS: list[dict[str, Any]] = [
    _READ_FILE_SCHEMA,
    _WRITE_FILE_SCHEMA,
    _EDIT_FILE_SCHEMA,
    _LIST_FILES_SCHEMA,
    _SEARCH_FILES_SCHEMA,
    _RUN_COMMAND_SCHEMA,
]
