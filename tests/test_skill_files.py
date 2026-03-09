"""Tests for Claude Code skill content shipped in the repo."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_plugin_skill_file_exists() -> None:
    """The plugin marketplace skill file should be present."""
    skill_file = (
        REPO_ROOT / ".claude-plugin" / "skills" / "kitaru-authoring" / "SKILL.md"
    )
    assert skill_file.exists()


def test_plugin_skill_covers_core_primitives() -> None:
    """Skill content should cover critical Kitaru authoring primitives."""
    skill_file = (
        REPO_ROOT / ".claude-plugin" / "skills" / "kitaru-authoring" / "SKILL.md"
    )
    content = skill_file.read_text(encoding="utf-8")

    for pattern in [
        "@flow",
        "@checkpoint",
        "kitaru.wait()",
        "kitaru.log()",
    ]:
        assert pattern in content, f"Skill file missing coverage of {pattern}"
