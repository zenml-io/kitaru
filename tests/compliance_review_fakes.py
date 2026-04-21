"""Shared fakes for compliance review example tests."""

from __future__ import annotations

import json
import re
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

_NON_ALPHANUMERIC = re.compile(r"[^A-Za-z0-9]")


def install_fake_claude_agent_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install a tiny fake `claude_agent_sdk` module for guarded tests."""
    fake_sdk = types.ModuleType("claude_agent_sdk")

    @dataclass
    class ClaudeAgentOptions:
        mcp_servers: dict[str, Any] | None = None
        allowed_tools: list[str] | None = None
        resume: str | None = None
        cwd: str | Path | None = None
        max_turns: int | None = None
        stderr: Any = None

    @dataclass
    class ResultMessage:
        subtype: str
        duration_ms: int
        duration_api_ms: int
        is_error: bool
        num_turns: int
        session_id: str
        total_cost_usd: float | None = None
        usage: dict[str, Any] | None = None
        result: str | None = None
        stop_reason: str | None = None
        model_usage: dict[str, Any] | None = None

    @dataclass
    class ToolAnnotations:
        readOnlyHint: bool | None = None
        destructiveHint: bool | None = None
        openWorldHint: bool | None = None

    def tool(name, description, input_schema, annotations=None):
        def decorate(func):
            func.name = name
            func.description = description
            func.input_schema = input_schema
            func.annotations = annotations
            return func

        return decorate

    def create_sdk_mcp_server(name, version="1.0.0", tools=None):
        return {"type": "sdk", "name": name, "version": version, "tools": tools or []}

    async def query(*, prompt, options=None):
        raise AssertionError("query() should not run in guarded compliance tests.")
        yield  # pragma: no cover

    for name, value in {
        "ClaudeAgentOptions": ClaudeAgentOptions,
        "ResultMessage": ResultMessage,
        "ToolAnnotations": ToolAnnotations,
        "create_sdk_mcp_server": create_sdk_mcp_server,
        "query": query,
        "tool": tool,
    }.items():
        setattr(fake_sdk, name, value)

    monkeypatch.setitem(sys.modules, "claude_agent_sdk", fake_sdk)


def clear_compliance_review_modules(*module_names: str) -> None:
    """Clear compliance review modules so they re-import with the fake SDK."""
    compliance_modules = [
        name
        for name in sys.modules
        if name == "examples.compliance_review"
        or name.startswith("examples.compliance_review.")
    ]
    for module_name in (*compliance_modules, *module_names):
        sys.modules.pop(module_name, None)


def configure_fake_claude_home(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> Path:
    """Point Claude's test transcript home at a writable temporary directory."""
    home = tmp_path / "fake_claude_home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    return home


def fake_claude_transcript_path(session_id: str, *, cwd: Path) -> str:
    """Return the fake transcript path using Claude's documented layout."""
    encoded_cwd = _NON_ALPHANUMERIC.sub(
        "-",
        str(Path(cwd).expanduser().resolve()),
    )
    return str(
        Path.home() / ".claude" / "projects" / encoded_cwd / f"{session_id}.jsonl"
    )


def fake_claude_response(
    *,
    prompt: str,
    cwd: Path,
    session_id: str,
    result: str | None = None,
) -> dict[str, Any]:
    """Build a Claude-shaped response and write its fake transcript JSONL."""
    transcript_path = fake_claude_transcript_path(session_id, cwd=cwd)
    transcript_file = Path(transcript_path)
    transcript_file.parent.mkdir(parents=True, exist_ok=True)
    transcript_file.write_text(
        json.dumps(
            {
                "type": "user",
                "session_id": session_id,
                "message": {"role": "user", "content": prompt},
            },
            sort_keys=True,
        )
        + "\n"
        + json.dumps(
            {
                "type": "result",
                "session_id": session_id,
                "result": (
                    result if result is not None else f"Stubbed finding for: {prompt}"
                ),
            },
            sort_keys=True,
        )
        + "\n"
    )
    return {
        "session_id": session_id,
        "cwd": str(cwd),
        "transcript_path": transcript_path,
        "result": result if result is not None else f"Stubbed finding for: {prompt}",
        "usage": {"input_tokens": 10, "output_tokens": 20},
        "cost_usd": 0.0,
        "model_usage": {"stub-model": {"inputTokens": 10, "outputTokens": 20}},
        "stop_reason": "end_turn",
        "subtype": "success",
        "num_turns": 1,
    }
