"""Shared Claude Agent SDK boundary for the compliance review example.

This module keeps the Claude-specific integration in one place:

- local JSON retrieval helpers are exposed as read-only MCP tools
- `run_agent_turn()` executes one Claude Agent SDK turn
- `ClaudeAgentResult` is the durable boundary object returned by Kitaru
  checkpoints in later stages

The important mental model for the example is:

    one Claude Agent SDK turn == one Kitaru checkpoint
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from claude_agent_sdk import (
    ClaudeAgentOptions,
    ResultMessage,
    ToolAnnotations,
    create_sdk_mcp_server,
    query,
    tool,
)
from pydantic import BaseModel, Field

from kitaru import get_secret
from kitaru.config import classify_stack_deployment_type

# Keep `retrieval_tools` on a single module identity so
# `isinstance`/`@tool` registration is stable regardless of cwd.
_REPO_ROOT = str(Path(__file__).resolve().parents[3])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from . import tools as retrieval_tools  # noqa: E402

COMPANY_TOOLS_SERVER_NAME = "company_docs"
DEFAULT_ALLOWED_TOOLS = [
    "mcp__company_docs__search_documents",
    "mcp__company_docs__read_document",
    "mcp__company_docs__read_section",
    "mcp__company_docs__list_documents",
    "mcp__company_docs__get_company_info",
]

_READ_ONLY_CLOSED_WORLD = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    openWorldHint=False,
)
_NON_ALPHANUMERIC = re.compile(r"[^A-Za-z0-9]")
CLAUDE_AGENT_SDK_REQUIREMENT = "claude-agent-sdk>=0.1.58,<0.2"
KITARU_REQUIREMENT = "kitaru>=0.5.1"
ANTHROPIC_SECRET_NAME = "anthropic"
ANTHROPIC_API_KEY_ENV = "ANTHROPIC_API_KEY"
_REMOTE_STACK_DEPLOYMENT_TYPES = frozenset(
    {"kubernetes", "vertex", "sagemaker", "azureml"}
)
_anthropic_key_checked = False


class ClaudeAgentResult(BaseModel):
    """Durable result boundary for a completed Claude Agent SDK turn."""

    session_id: str
    cwd: str
    transcript_path: str
    result: str | None = None
    usage: dict[str, Any] | None = None
    cost_usd: float | None = None
    model_usage: dict[str, Any] | None = None
    stop_reason: str | None = None
    subtype: str | None = None
    num_turns: int | None = None
    transcript_path_note: str = Field(
        default=(
            "Resolved from the documented Claude Agent SDK local session layout. "
            "If the SDK changes its transcript storage scheme, update "
            "resolve_claude_transcript_path()."
        )
    )


@tool(
    "search_documents",
    "Search Acme Corp documents and standards for relevant passages.",
    {"query": str},
    annotations=_READ_ONLY_CLOSED_WORLD,
)
async def search_documents_tool(args: dict[str, Any]) -> dict[str, Any]:
    """Claude MCP wrapper for `tools.search_documents()`."""
    try:
        results = retrieval_tools.search_documents(query=str(args["query"]))
    except Exception as exc:
        return _tool_error(exc)
    return _tool_json_response(results)


@tool(
    "read_document",
    "Read a full Acme Corp document, standard, or company profile by ID.",
    {"doc_id": str},
    annotations=_READ_ONLY_CLOSED_WORLD,
)
async def read_document_tool(args: dict[str, Any]) -> dict[str, Any]:
    """Claude MCP wrapper for `tools.read_document()`."""
    try:
        text = retrieval_tools.read_document(doc_id=str(args["doc_id"]))
    except Exception as exc:
        return _tool_error(exc)
    return _tool_text_response(text)


@tool(
    "read_section",
    "Read one named section from an Acme Corp document or standard.",
    {"doc_id": str, "section": str},
    annotations=_READ_ONLY_CLOSED_WORLD,
)
async def read_section_tool(args: dict[str, Any]) -> dict[str, Any]:
    """Claude MCP wrapper for `tools.read_section()`."""
    try:
        text = retrieval_tools.read_section(
            doc_id=str(args["doc_id"]),
            section=str(args["section"]),
        )
    except Exception as exc:
        return _tool_error(exc)
    return _tool_text_response(text)


@tool(
    "list_documents",
    "List available Acme Corp documents and metadata.",
    {},
    annotations=_READ_ONLY_CLOSED_WORLD,
)
async def list_documents_tool(args: dict[str, Any]) -> dict[str, Any]:
    """Claude MCP wrapper for `tools.list_documents()`."""
    try:
        documents = retrieval_tools.list_documents()
    except Exception as exc:
        return _tool_error(exc)
    return _tool_json_response(documents)


@tool(
    "get_company_info",
    "Look up Acme Corp metadata and review scope.",
    {},
    annotations=_READ_ONLY_CLOSED_WORLD,
)
async def get_company_info_tool(args: dict[str, Any]) -> dict[str, Any]:
    """Claude MCP wrapper for `tools.get_company_info()`."""
    try:
        company_info = retrieval_tools.get_company_info()
    except Exception as exc:
        return _tool_error(exc)
    return _tool_json_response(company_info)


company_tools = create_sdk_mcp_server(
    name=COMPANY_TOOLS_SERVER_NAME,
    version="0.1.0",
    tools=[
        search_documents_tool,
        read_document_tool,
        read_section_tool,
        list_documents_tool,
        get_company_info_tool,
    ],
)


def _ensure_anthropic_api_key() -> None:
    """Load Anthropic credentials for remote runs when the shell env is absent."""
    global _anthropic_key_checked
    if _anthropic_key_checked or os.environ.get(ANTHROPIC_API_KEY_ENV):
        _anthropic_key_checked = True
        return

    if classify_stack_deployment_type() not in _REMOTE_STACK_DEPLOYMENT_TYPES:
        _anthropic_key_checked = True
        return

    try:
        secret = get_secret(ANTHROPIC_SECRET_NAME)
    except Exception as exc:
        raise RuntimeError(
            f"Remote compliance-review runs require a centralized Anthropic "
            f"secret, but the lookup failed: {exc}. Create the secret with: "
            f"kitaru secrets set {ANTHROPIC_SECRET_NAME} "
            f"--{ANTHROPIC_API_KEY_ENV}=sk-ant-..."
        ) from exc

    anthropic_api_key = secret.get(ANTHROPIC_API_KEY_ENV)
    if not anthropic_api_key:
        raise RuntimeError(
            f"Secret '{ANTHROPIC_SECRET_NAME}' exists, but it does not contain "
            f"{ANTHROPIC_API_KEY_ENV}. Update it with: kitaru secrets set "
            f"{ANTHROPIC_SECRET_NAME} --{ANTHROPIC_API_KEY_ENV}=sk-ant-..."
        )

    os.environ[ANTHROPIC_API_KEY_ENV] = anthropic_api_key
    _anthropic_key_checked = True


async def run_agent_turn(
    prompt: str,
    *,
    allowed_tools: list[str] | None = None,
    resume: str | None = None,
    cwd: str | Path | None = None,
    max_turns: int | None = None,
) -> dict[str, Any]:
    """Run one Claude Agent SDK turn with the compliance-review tools.

    Args:
        prompt: User prompt to send to Claude.
        allowed_tools: MCP tool permission names. Defaults to the example's
            read-only company document tools.
        resume: Optional Claude session ID to resume.
        cwd: Working directory for the Claude session. This matters because
            Claude stores session transcripts under an encoded form of the cwd.
        max_turns: Optional SDK turn limit for guarded demos/tests.

    Returns:
        A plain dictionary suitable for `to_claude_agent_result()`.
    """
    _ensure_anthropic_api_key()
    resolved_cwd = _resolve_cwd(cwd)
    final: ResultMessage | None = None
    stderr_lines: list[str] = []
    options = ClaudeAgentOptions(
        mcp_servers={COMPANY_TOOLS_SERVER_NAME: company_tools},
        allowed_tools=allowed_tools or DEFAULT_ALLOWED_TOOLS,
        resume=resume,
        cwd=resolved_cwd,
        max_turns=max_turns,
        stderr=_collect_stderr(stderr_lines),
    )

    try:
        async for message in query(prompt=prompt, options=options):
            if isinstance(message, ResultMessage):
                final = message
    except Exception as exc:
        if final is not None and final.is_error:
            raise RuntimeError(_format_result_error(final, stderr_lines)) from exc
        raise RuntimeError(_format_transport_error(exc, stderr_lines)) from exc

    if final is None:
        raise RuntimeError("Claude Agent SDK did not return a final ResultMessage.")

    if final.is_error:
        raise RuntimeError(_format_result_error(final, stderr_lines))

    if not (final.result and final.result.strip()):
        raise RuntimeError(_format_empty_result_error(final, stderr_lines))

    transcript_path = resolve_claude_transcript_path(
        final.session_id,
        cwd=resolved_cwd,
    )
    return {
        "session_id": final.session_id,
        "cwd": resolved_cwd,
        "transcript_path": transcript_path,
        "result": final.result,
        "usage": final.usage,
        "cost_usd": final.total_cost_usd,
        "model_usage": final.model_usage,
        "stop_reason": final.stop_reason,
        "subtype": final.subtype,
        "num_turns": final.num_turns,
    }


def to_claude_agent_result(response: Mapping[str, Any]) -> ClaudeAgentResult:
    """Convert a Claude Agent SDK response dictionary into the boundary model."""
    session_id = str(response["session_id"])
    cwd = str(response.get("cwd") or _resolve_cwd(None))
    transcript_path = str(
        response.get("transcript_path")
        or resolve_claude_transcript_path(session_id, cwd=cwd)
    )
    return ClaudeAgentResult(
        session_id=session_id,
        cwd=cwd,
        transcript_path=transcript_path,
        result=response.get("result"),
        usage=response.get("usage"),
        cost_usd=response.get("cost_usd"),
        model_usage=response.get("model_usage"),
        stop_reason=response.get("stop_reason"),
        subtype=response.get("subtype"),
        num_turns=response.get("num_turns"),
    )


def resolve_claude_transcript_path(session_id: str, *, cwd: str | Path) -> str:
    """Resolve the expected local Claude transcript path for a session.

    Official Claude Agent SDK session docs currently describe local session
    files as:

        ~/.claude/projects/<encoded-cwd>/<session-id>.jsonl

    where `<encoded-cwd>` is the absolute working directory with every
    non-alphanumeric character replaced by `-`.

    This function intentionally centralizes that assumption. It does not check
    that the file exists, because the SDK owns when the transcript is created
    and flushed. If the SDK changes this filesystem layout, this should be the
    only example helper that needs to change.
    """
    if not session_id:
        raise ValueError("session_id must be a non-empty string.")

    encoded_cwd = _encode_claude_project_dir(cwd)
    return str(
        Path.home() / ".claude" / "projects" / encoded_cwd / f"{session_id}.jsonl"
    )


def _resolve_cwd(cwd: str | Path | None) -> str:
    """Resolve the cwd string used by both Claude and transcript lookup."""
    return str(Path(cwd or os.getcwd()).expanduser().resolve())


def _encode_claude_project_dir(cwd: str | Path) -> str:
    """Encode a cwd using the documented Claude Agent SDK project-dir rule."""
    resolved = _resolve_cwd(cwd)
    # The SDK's documented encoding is ASCII-focused. Non-ASCII characters
    # pass the regex unchanged, producing a path the SDK will not find on
    # resume, which silently starts a fresh Claude session. Raise here so
    # the failure is diagnosable instead of invisible.
    if not resolved.isascii():
        raise ValueError(
            "Claude transcript paths for non-ASCII working directories are not "
            f"currently supported by this example: {resolved!r}"
        )
    return _NON_ALPHANUMERIC.sub("-", resolved)


def _collect_stderr(stderr_lines: list[str]) -> Callable[[str], None]:
    """Collect Claude CLI stderr lines for clearer surfaced failures."""

    def _append(line: str) -> None:
        stderr_lines.append(line)

    return _append


def _format_result_error(
    final: ResultMessage,
    stderr_lines: list[str],
) -> str:
    """Format a Claude result-level error, preserving the meaningful cause."""
    detail = (
        final.result or final.stop_reason or final.subtype or "Unknown Claude error"
    )
    return _append_stderr(
        f"Claude Agent SDK turn failed: {detail}",
        stderr_lines,
    )


def _format_transport_error(exc: Exception, stderr_lines: list[str]) -> str:
    """Format a transport/process failure with any collected stderr context."""
    return _append_stderr(
        f"Claude Agent SDK transport failed: {exc}",
        stderr_lines,
    )


def _format_empty_result_error(
    final: ResultMessage,
    stderr_lines: list[str],
) -> str:
    """Format a completed-but-empty-text turn so callers see why Claude stopped."""
    parts: list[str] = []
    if final.stop_reason is not None:
        parts.append(f"stop_reason={final.stop_reason!r}")
    if final.subtype is not None:
        parts.append(f"subtype={final.subtype!r}")
    parts.append(f"num_turns={final.num_turns}")
    context = ", ".join(parts)
    return _append_stderr(
        f"Claude Agent SDK turn finished without producing result text ({context})",
        stderr_lines,
    )


def _append_stderr(message: str, stderr_lines: list[str]) -> str:
    """Attach a short stderr tail when the Claude CLI emitted one."""
    if not stderr_lines:
        return message
    stderr_tail = "\n".join(stderr_lines[-20:])
    return f"{message}\nClaude CLI stderr:\n{stderr_tail}"


def _tool_json_response(data: Any) -> dict[str, Any]:
    """Return JSON-ish data in the MCP text content shape expected by Claude."""
    return _tool_text_response(json.dumps(data, indent=2, sort_keys=True))


def _tool_text_response(text: str) -> dict[str, Any]:
    """Return text in the MCP content shape expected by Claude."""
    return {"content": [{"type": "text", "text": text}]}


def _tool_error(exc: Exception) -> dict[str, Any]:
    """Return a recoverable MCP tool error payload."""
    return {
        "content": [{"type": "text", "text": f"{type(exc).__name__}: {exc}"}],
        "is_error": True,
    }
