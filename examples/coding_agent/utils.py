"""Agents and system prompts for the PydanticAI coding agent."""

import os
from typing import Any

from pydantic import BaseModel
from pydantic_ai import Agent

from kitaru.adapters import pydantic_ai as kp
from kitaru.adapters.pydantic_ai import resolve_model

from .tools import CODER_TOOLS, RESEARCHER_TOOLS

# ---------------------------------------------------------------------------
# Memory model
# ---------------------------------------------------------------------------


class AgentMemory(BaseModel):
    """Persistent state carried across coding agent runs."""

    conventions: list[str] = []
    decisions: list[str] = []
    notes: dict[str, str] = {}


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

REFLECTOR_PROMPT: str = (
    "You analyze completed coding tasks and extract reusable knowledge.\n\n"
    "Given a task, its implementation result, and existing memory from past runs, "
    "produce updated memory.\n\n"
    "Guidelines:\n"
    "- Add conventions you observed (e.g. 'uses pytest', 'prefers dataclasses')\n"
    "- Record decisions made during this task (e.g. 'chose SQLite for simplicity')\n"
    "- Add freeform notes as key-value pairs (e.g. auth_lib: 'uses JWT via pyjwt')\n"
    "- Keep existing entries that are still relevant\n"
    "- Remove entries contradicted by new information\n"
    "- Be concise — each entry should be a short phrase, not a paragraph"
)

# ---------------------------------------------------------------------------
# Model resolution
# ---------------------------------------------------------------------------

MODEL: Any
_env_overlay: dict[str, str]
MODEL, _env_overlay = resolve_model(
    os.environ.get("CODING_AGENT_MODEL") or "coding-agent"
)
os.environ.update(_env_overlay)

# ---------------------------------------------------------------------------
# Pre-built wrapped agents
# ---------------------------------------------------------------------------

researcher: Any = kp.wrap(
    Agent(
        MODEL,
        tools=RESEARCHER_TOOLS,
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
        tools=CODER_TOOLS,
        system_prompt=CODER_PROMPT,
    ),
    name="coder",
    tool_capture_config={"mode": "metadata_only"},
)

reflector: Any = kp.wrap(
    Agent(
        MODEL,
        output_type=AgentMemory,
        system_prompt=REFLECTOR_PROMPT,
    ),
    name="reflector",
    tool_capture_config={"mode": "metadata_only"},
)
