"""Tests for packaged Claude Code skill content."""

from __future__ import annotations

from pathlib import Path

import kitaru.skills


def test_skill_file_exists() -> None:
    """The packaged Kitaru authoring skill should be present."""
    skills_dir = Path(kitaru.skills.__path__[0])
    skill_file = skills_dir / "kitaru-authoring.md"
    assert skill_file.exists()


def test_skill_file_covers_core_primitives() -> None:
    """Skill content should cover critical Kitaru authoring primitives."""
    skills_dir = Path(kitaru.skills.__path__[0])
    content = (skills_dir / "kitaru-authoring.md").read_text(encoding="utf-8")

    for pattern in [
        "@kitaru.flow",
        "@kitaru.checkpoint",
        "kitaru.wait()",
        "kitaru.log()",
    ]:
        assert pattern in content, f"Skill file missing coverage of {pattern}"
