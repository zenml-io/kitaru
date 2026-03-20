"""Tool implementations and schemas for the basic coding agent."""

import subprocess
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from pydantic import BaseModel, Field
from zenml.types import HTMLString, JSONString, MarkdownString

_MAX_CHARS = 12_000
_READ_LIMIT = 400

ToolResult = str | HTMLString | MarkdownString | JSONString


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
# File tools
# ---------------------------------------------------------------------------


def read_file(
    cwd: str, path: str, offset: int = 0, limit: int = _READ_LIMIT
) -> MarkdownString:
    """Read a file with line numbers."""
    target = _resolve(cwd, path)
    try:
        lines = target.read_text().splitlines()
    except FileNotFoundError:
        return MarkdownString(f"Error: file not found: {path}")
    except Exception as e:
        return MarkdownString(f"Error reading {path}: {e}")

    selected = lines[offset : offset + limit]
    numbered = [f"{i + offset + 1:>6}\t{line}" for i, line in enumerate(selected)]
    result = "\n".join(numbered)
    if len(lines) > offset + limit:
        shown = f"{offset + 1}-{offset + len(selected)}"
        result += f"\n... [showing lines {shown} of {len(lines)}]"
    return MarkdownString(_truncate(result))


def write_file(cwd: str, path: str, content: str) -> JSONString:
    """Write content to a file."""
    target = _resolve(cwd, path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)
    return JSONString(f'{{"path": "{path}", "chars": {len(content)}}}')


def edit_file(cwd: str, path: str, old: str, new: str) -> JSONString:
    """Replace a single exact occurrence of old with new."""
    target = _resolve(cwd, path)
    try:
        text = target.read_text()
    except FileNotFoundError:
        return JSONString(f'{{"error": "file not found: {path}"}}')

    count = text.count(old)
    if count == 0:
        return JSONString(f'{{"error": "old string not found in {path}"}}')
    if count > 1:
        return JSONString(
            f'{{"error": "old string appears {count} times '
            f'in {path} (expected exactly 1)"}}'
        )

    target.write_text(text.replace(old, new, 1))
    return JSONString(f'{{"path": "{path}", "status": "edited"}}')


def list_files(cwd: str, pattern: str) -> MarkdownString:
    """List files matching a glob pattern."""
    base = Path(cwd).resolve()
    matches = sorted(base.glob(pattern))[:500]
    relative = [str(m.relative_to(base)) for m in matches if m.is_file()]
    if not relative:
        return MarkdownString(f"No files matching: {pattern}")
    return MarkdownString("\n".join(relative))


def search_files(cwd: str, pattern: str, glob: str = "**/*") -> MarkdownString:
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
        return MarkdownString("Error: search timed out after 30s")
    output = result.stdout.replace(str(base) + "/", "")
    return MarkdownString(_truncate(output) if output else f"No matches for: {pattern}")


def run_command(cwd: str, command: str, timeout: int = 30) -> MarkdownString:
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
        return MarkdownString(f"Error: command timed out after {timeout}s")
    except Exception as e:
        return MarkdownString(f"Error executing command: {e}")

    output = result.stdout + result.stderr
    return MarkdownString(_truncate(f"Exit code: {result.returncode}\n{output}"))


# ---------------------------------------------------------------------------
# Python execution
# ---------------------------------------------------------------------------


def python_exec(cwd: str, code: str, timeout: int = 60) -> MarkdownString:
    """Execute a Python script via ``uv run`` and return stdout + stderr.

    If the script needs third-party packages, include PEP 723 inline
    metadata at the top::

        # /// script
        # dependencies = ["plotly", "pandas"]
        # ///

    ``uv run`` will resolve and install them automatically into a
    cached ephemeral environment — no manual pip install needed.
    """
    base = Path(cwd).resolve()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(code)
        script_path = f.name

    try:
        result = subprocess.run(
            ["uv", "run", script_path],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(base) if base.is_dir() else None,
        )
    except subprocess.TimeoutExpired:
        return MarkdownString(f"Error: script timed out after {timeout}s")
    except Exception as e:
        return MarkdownString(f"Error executing script: {e}")
    finally:
        Path(script_path).unlink(missing_ok=True)

    output = result.stdout + result.stderr
    return MarkdownString(_truncate(f"Exit code: {result.returncode}\n{output}"))


# ---------------------------------------------------------------------------
# Web tools
# ---------------------------------------------------------------------------


def web_fetch(cwd: str, url: str, max_chars: int = _MAX_CHARS) -> HTMLString:
    """Fetch a URL and return its text content."""
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return HTMLString(
            f"Error: only http/https URLs are supported, got {parsed.scheme!r}"
        )

    try:
        result = subprocess.run(
            ["curl", "-sL", "--max-time", "15", "-A", "KitaruAgent/1.0", url],
            capture_output=True,
            text=True,
            timeout=20,
        )
    except subprocess.TimeoutExpired:
        return HTMLString("Error: fetch timed out")
    except Exception as e:
        return HTMLString(f"Error fetching URL: {e}")

    if result.returncode != 0:
        return HTMLString(
            f"Error: curl exited with code {result.returncode}\n{result.stderr}"
        )
    return HTMLString(_truncate(result.stdout, max_chars))


def web_search(cwd: str, query: str) -> MarkdownString:
    """Search the web using a text query (via DuckDuckGo HTML)."""
    import re
    from urllib.parse import quote_plus

    search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        result = subprocess.run(
            [
                "curl",
                "-sL",
                "--max-time",
                "15",
                "-A",
                "KitaruAgent/1.0",
                search_url,
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )
    except subprocess.TimeoutExpired:
        return MarkdownString("Error: search timed out")
    except Exception as e:
        return MarkdownString(f"Error searching: {e}")

    if result.returncode != 0:
        return MarkdownString(f"Error: curl exited with code {result.returncode}")

    links = re.findall(
        r'class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        result.stdout,
    )
    snippets = re.findall(
        r'class="result__snippet"[^>]*>(.*?)</(?:td|div)>',
        result.stdout,
        re.DOTALL,
    )

    if not links:
        return MarkdownString("No search results found.")

    results: list[str] = []
    for i, (href, title) in enumerate(links[:10]):
        title_clean = re.sub(r"<[^>]+>", "", title).strip()
        snippet = ""
        if i < len(snippets):
            snippet = re.sub(r"<[^>]+>", "", snippets[i]).strip()
        results.append(f"{i + 1}. {title_clean}\n   {href}\n   {snippet}")

    return MarkdownString("\n\n".join(results))


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

_TOOL_FUNCTIONS: dict[str, Callable[..., ToolResult]] = {
    "read_file": read_file,
    "write_file": write_file,
    "edit_file": edit_file,
    "list_files": list_files,
    "search_files": search_files,
    "run_command": run_command,
    "python_exec": python_exec,
    "web_fetch": web_fetch,
    "web_search": web_search,
}


# ---------------------------------------------------------------------------
# Parameter models (drive both schema generation and input validation)
# ---------------------------------------------------------------------------


class ReadFileParams(BaseModel):
    path: str = Field(description="File path relative to cwd")
    offset: int = Field(default=0, description="0-based line offset")
    limit: int = Field(default=_READ_LIMIT, description="Max lines to return")


class WriteFileParams(BaseModel):
    path: str = Field(description="File path relative to cwd")
    content: str = Field(description="File content to write")


class EditFileParams(BaseModel):
    path: str = Field(description="File path relative to cwd")
    old: str = Field(description="Exact text to find")
    new: str = Field(description="Replacement text")


class ListFilesParams(BaseModel):
    pattern: str = Field(description="Glob pattern (e.g. '**/*.py')")


class SearchFilesParams(BaseModel):
    pattern: str = Field(description="Search pattern (regex)")
    glob: str = Field(default="**/*", description="File glob to limit search")


class RunCommandParams(BaseModel):
    command: str = Field(description="Shell command to run")
    timeout: int = Field(default=30, description="Timeout in seconds")


class PythonExecParams(BaseModel):
    code: str = Field(description="Python source code to execute")
    timeout: int = Field(default=60, description="Timeout in seconds")


class WebFetchParams(BaseModel):
    url: str = Field(description="URL to fetch (http or https)")


class WebSearchParams(BaseModel):
    query: str = Field(description="Search query")


# ---------------------------------------------------------------------------
# Schema generation + dispatch
# ---------------------------------------------------------------------------

_TOOL_MODELS: dict[str, type[BaseModel]] = {
    "read_file": ReadFileParams,
    "write_file": WriteFileParams,
    "edit_file": EditFileParams,
    "list_files": ListFilesParams,
    "search_files": SearchFilesParams,
    "run_command": RunCommandParams,
    "python_exec": PythonExecParams,
    "web_fetch": WebFetchParams,
    "web_search": WebSearchParams,
}

_TOOL_DESCRIPTIONS: dict[str, str] = {
    "read_file": (
        "Read a file with line numbers. Use offset/limit to page through large files."
    ),
    "write_file": "Write content to a file, creating parent directories as needed.",
    "edit_file": "Replace a single exact occurrence of old with new in a file.",
    "list_files": (
        "List files matching a glob pattern relative to the working directory."
    ),
    "search_files": "Search file contents for a pattern using grep.",
    "run_command": "Run a shell command in the working directory.",
    "python_exec": (
        "Execute a Python script via uv run and return stdout + stderr. "
        "Use for math, data processing, generating plots "
        "(save to file with plt.savefig or plotly write_html/write_image), "
        "or any task best solved with code. "
        "The script runs in the working directory. "
        "If the script needs third-party packages, add PEP 723 inline "
        "metadata at the top of the script:\n"
        "# /// script\n"
        '# dependencies = ["plotly", "pandas"]\n'
        "# ///\n"
        "uv will install them automatically."
    ),
    "web_fetch": (
        "Fetch a URL and return its text content. "
        "Use for reading web pages, API responses, documentation, etc."
    ),
    "web_search": (
        "Search the web using a text query. "
        "Returns titles, URLs, and snippets from top results. "
        "Use web_fetch to read a specific result page."
    ),
}


def _schema_from_model(
    name: str, description: str, model: type[BaseModel]
) -> dict[str, Any]:
    """Wrap a Pydantic model's JSON schema in the OpenAI function-calling envelope."""
    schema = model.model_json_schema()
    # model_json_schema adds a top-level 'title' key that OpenAI doesn't expect
    schema.pop("title", None)
    return {
        "type": "function",
        "function": {"name": name, "description": description, "parameters": schema},
    }


ALL_SCHEMAS: list[dict[str, Any]] = [
    _schema_from_model(name, _TOOL_DESCRIPTIONS[name], model)
    for name, model in _TOOL_MODELS.items()
]


def dispatch_tool(cwd: str, name: str, arguments: dict[str, Any]) -> ToolResult:
    """Execute a tool by name, validating arguments via the Pydantic model."""
    func = _TOOL_FUNCTIONS.get(name)
    model = _TOOL_MODELS.get(name)
    if func is None or model is None:
        return f"Error: unknown tool '{name}'"
    try:
        validated = model(**arguments)
        return func(cwd, **validated.model_dump())
    except Exception as exc:
        return f"Error running {name}: {type(exc).__name__}: {exc}"
