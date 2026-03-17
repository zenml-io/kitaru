"""Implementer agent — executes a plan using all available tools."""

import tempfile
from pathlib import Path
from typing import Any

import kitaru
from kitaru import checkpoint

try:
    from ..tools import ALL_SCHEMAS
    from ..llm import tool_loop
except (ImportError, SystemError):
    from tools import ALL_SCHEMAS
    from llm import tool_loop

_WORKSPACE = Path(tempfile.mkdtemp(prefix="implementer_"))

SYSTEM_PROMPT = (
    "You are a code implementation assistant. Follow the provided "
    "plan and make the required changes using the available tools."
    "\n\n"
    "Guidelines:\n"
    "- Prefer edit_file over write_file for existing files "
    "(smaller, safer edits)\n"
    "- Read files before editing to understand the current state\n"
    "- Run verification commands after making changes\n"
    "- Report what you did and the outcome"
)


@checkpoint(type="llm_call")
def implement(task: str, implementation_plan: str) -> str:
    """Execute the plan using a tool-calling loop."""
    cwd = str(_WORKSPACE)
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"Task: {task}\n\n"
                f"Plan:\n{implementation_plan}\n\n"
                "Execute this plan now using the available tools."
            ),
        },
    ]
    result, tool_calls, rounds = tool_loop(
        messages=messages, tools=ALL_SCHEMAS, cwd=cwd
    )
    kitaru.log(phase="implement", tool_calls=tool_calls, rounds=rounds)
    return result
