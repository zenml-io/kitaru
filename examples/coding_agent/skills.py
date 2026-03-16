"""Folder-based skill loader for the coding agent.

Each skill is a directory under ``skills/`` containing:

- ``skill.md`` — YAML frontmatter (name, description, keywords, tools)
  followed by the prompt body (markdown)

The ``tools`` field in frontmatter references function names from
``tools.py`` (the shared tool registry).  Skills without a ``tools``
field are prompt-only — they inject guidance into the coder's system
prompt without adding extra tools.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .tools import TOOL_REGISTRY

_SKILLS_DIR = Path(__file__).parent / "skills"

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n(.*)", re.DOTALL)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Skill:
    """A named capability package loaded from a skill folder."""

    name: str
    description: str
    prompt: str
    keywords: tuple[str, ...] = ()
    tools: tuple[Callable[..., Any], ...] = ()


# ---------------------------------------------------------------------------
# Frontmatter parsing + discovery
# ---------------------------------------------------------------------------


def _parse_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    """Split a skill.md into parsed YAML frontmatter + body."""
    m = _FRONTMATTER_RE.match(text)
    if not m:
        return {}, text
    fm = yaml.safe_load(m.group(1)) or {}
    return fm, m.group(2).strip()


def _resolve_tools(names: list[str]) -> tuple[Callable[..., Any], ...]:
    """Resolve tool function names via the shared registry."""
    resolved = []
    for name in names:
        fn = TOOL_REGISTRY.get(name)
        if fn is not None:
            resolved.append(fn)
    return tuple(resolved)


def _load_skill(skill_dir: Path) -> Skill | None:
    """Load a single skill from its directory."""
    skill_md = skill_dir / "skill.md"
    if not skill_md.is_file():
        return None

    fm, body = _parse_frontmatter(skill_md.read_text())
    name = fm.get("name")
    if not isinstance(name, str) or not name:
        return None

    description = fm.get("description", "")
    if not isinstance(description, str):
        description = ""

    keywords_raw = fm.get("keywords", [])
    keywords = tuple(keywords_raw) if isinstance(keywords_raw, list) else ()

    tool_names = fm.get("tools", [])
    tools = _resolve_tools(tool_names) if isinstance(tool_names, list) else ()

    return Skill(
        name=name,
        description=description,
        prompt=body,
        keywords=keywords,
        tools=tools,
    )


def discover_skills(skills_dir: Path = _SKILLS_DIR) -> dict[str, Skill]:
    """Scan a directory for skill folders and return them keyed by name."""
    skills: dict[str, Skill] = {}
    if not skills_dir.is_dir():
        return skills

    for child in sorted(skills_dir.iterdir()):
        if not child.is_dir() or child.name.startswith(("_", ".")):
            continue
        skill = _load_skill(child)
        if skill is not None:
            skills[skill.name] = skill
    return skills


ALL_SKILLS: dict[str, Skill] = discover_skills()


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------

_WORD_RE_CACHE: dict[str, re.Pattern[str]] = {}


def _matches_keyword(keyword: str, text: str) -> bool:
    """Word-boundary keyword match (avoids 'documented' matching 'doc')."""
    pattern = _WORD_RE_CACHE.get(keyword)
    if pattern is None:
        pattern = re.compile(r"\b" + re.escape(keyword) + r"\b")
        _WORD_RE_CACHE[keyword] = pattern
    return bool(pattern.search(text))


def select_skills(
    task: str,
    analysis: str,
    explicit: list[str] | None = None,
) -> list[Skill]:
    """Select skills relevant to the task.

    If *explicit* names are provided, use those directly.
    Otherwise, word-boundary keyword match against task + analysis text.
    """
    if explicit:
        return [ALL_SKILLS[name] for name in explicit if name in ALL_SKILLS]

    text = (task + " " + analysis).lower()
    return [
        skill
        for skill in ALL_SKILLS.values()
        if any(_matches_keyword(kw, text) for kw in skill.keywords)
    ]
