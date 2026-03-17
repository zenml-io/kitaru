"""Planner agent — explores the codebase and produces a numbered plan."""

import tempfile
from pathlib import Path
from typing import Any

import kitaru
from kitaru import checkpoint

try:
    from ..tools import READER_SCHEMAS
    from ..llm import tool_loop
except (ImportError, SystemError):
    from tools import READER_SCHEMAS
    from llm import tool_loop

_WORKSPACE = Path(tempfile.mkdtemp(prefix="planner_"))

SYSTEM_PROMPT = (
    "You are a planning assistant. Given a coding task, explore the "
    "codebase using the available read-only tools, then create a "
    "clear, numbered implementation plan.\n\n"
    "Guidelines:\n"
    "- Start by listing files and reading relevant code to "
    "understand the codebase\n"
    "- Be specific: name files, functions, and line ranges\n"
    "- Include verification steps (tests to run, commands to check)\n"
    "- Keep it compact — bullet points, not essays\n"
    "- End with a final text response containing ONLY the "
    "numbered plan"
)


@checkpoint(type="llm_call")
def plan(task: str) -> str:
    """Explore the codebase and produce a numbered plan."""
    cwd = str(_WORKSPACE)
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"Working directory: {cwd}\n\n"
                f"Task: {task}\n\n"
                "Explore the codebase, then create a numbered "
                "implementation plan."
            ),
        },
    ]
    result, tool_calls, rounds = tool_loop(
        messages=messages, tools=READER_SCHEMAS, cwd=cwd
    )
    kitaru.log(phase="plan", tool_calls=tool_calls, rounds=rounds)
    return result
