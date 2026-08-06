#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Cross-platform discovery of installed Kitaru agent skills."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from kitaru.cli.skill_discovery import get_kitaru_skill_status


def _write_skill(directory: Path, name: str) -> Path:
    """Create a minimal skill document and return its path."""
    directory.mkdir(parents=True)
    path = directory / "SKILL.md"
    path.write_text(
        f"---\nname: {name}\ndescription: Test skill.\n---\n",
        encoding="utf-8",
    )
    return path


def test_discovery_finds_project_and_user_skills_without_fixed_name_list(
    tmp_path: Path,
) -> None:
    """Discovery follows the Kitaru prefix across supported hosts and scopes."""
    project = tmp_path / "project"
    nested = project / "src" / "package"
    nested.mkdir(parents=True)
    (project / ".git").touch()
    home = tmp_path / "home"
    home.mkdir()

    project_skill = _write_skill(
        project / ".agents" / "skills" / "renamed-folder",
        "kitaru-investigation",
    )
    user_skill = _write_skill(
        home / ".codex" / "skills" / "future-skill",
        "kitaru-future-workflow",
    )
    _write_skill(
        project / ".claude" / "skills" / "unrelated",
        "unrelated-skill",
    )

    status = get_kitaru_skill_status(cwd=nested, home=home)

    assert status["installed"] is True
    assert status["skill_count"] == 2
    assert status["skills"] == [
        "kitaru-future-workflow",
        "kitaru-investigation",
    ]
    assert status["installations"] == [
        {
            "name": "kitaru-investigation",
            "scope": "project",
            "host": "agents",
            "path": str(project_skill.parent),
        },
        {
            "name": "kitaru-future-workflow",
            "scope": "user",
            "host": "codex",
            "path": str(user_skill.parent),
        },
    ]


def test_discovery_ignores_invalid_skills_and_reports_checked_locations(
    tmp_path: Path,
) -> None:
    """Missing or malformed skill frontmatter cannot create false positives."""
    project = tmp_path / "project"
    project.mkdir()
    home = tmp_path / "home"
    home.mkdir()
    invalid = project / ".agents" / "skills" / "kitaru-invalid"
    invalid.mkdir(parents=True)
    (invalid / "SKILL.md").write_text("# Missing frontmatter\n", encoding="utf-8")

    status = get_kitaru_skill_status(cwd=project, home=home)

    assert status["installed"] is False
    assert status["skill_count"] == 0
    assert status["skills"] == []
    assert status["installations"] == []
    assert str(project / ".agents" / "skills") in status["locations_checked"]
    assert str(home / ".claude" / "skills") in status["locations_checked"]


@pytest.mark.parametrize(
    "frontmatter",
    [
        "---\nname: kitaru-incomplete\n---\n",
        "---\nname: kitaru-invalid\ndescription: Test skill.\ninvalid: [\n---\n",
    ],
)
def test_discovery_rejects_incomplete_or_invalid_frontmatter(
    tmp_path: Path, frontmatter: str
) -> None:
    """Only loadable skill metadata counts as an installed skill."""
    project = tmp_path / "project"
    skill = project / ".agents" / "skills" / "skill"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(frontmatter, encoding="utf-8")
    home = tmp_path / "home"
    home.mkdir()

    status = get_kitaru_skill_status(cwd=project, home=home)

    assert status["installed"] is False


def test_discovery_rejects_oversized_skill_documents(tmp_path: Path) -> None:
    """A bounded read prevents one skill document from delaying CLI startup."""
    project = tmp_path / "project"
    skill = project / ".agents" / "skills" / "skill"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(
        "---\nname: kitaru-oversized\ndescription: " + "x" * 70_000 + "\n---\n",
        encoding="utf-8",
    )
    home = tmp_path / "home"
    home.mkdir()

    status = get_kitaru_skill_status(cwd=project, home=home)

    assert status["installed"] is False


def test_discovery_rejects_symlinked_skill_documents(tmp_path: Path) -> None:
    """Discovery does not follow a skill document outside the scanned tree."""
    project = tmp_path / "project"
    skill = project / ".agents" / "skills" / "skill"
    skill.mkdir(parents=True)
    target = tmp_path / "external-skill.md"
    target.write_text(
        "---\nname: kitaru-linked\ndescription: Test skill.\n---\n",
        encoding="utf-8",
    )
    try:
        (skill / "SKILL.md").symlink_to(target)
    except OSError:
        pytest.skip("Symlinks are unavailable on this platform")
    home = tmp_path / "home"
    home.mkdir()

    status = get_kitaru_skill_status(cwd=project, home=home)

    assert status["installed"] is False


def test_discovery_counts_shared_skill_once_but_reports_each_host(
    tmp_path: Path,
) -> None:
    """One skill installed for multiple hosts remains one logical skill."""
    project = tmp_path / "project"
    project.mkdir()
    home = tmp_path / "home"
    home.mkdir()
    _write_skill(
        project / ".agents" / "skills" / "kitaru-investigation",
        "kitaru-investigation",
    )
    _write_skill(
        project / ".claude" / "skills" / "kitaru-investigation",
        "kitaru-investigation",
    )

    status = get_kitaru_skill_status(cwd=project, home=home)

    assert status["skill_count"] == 1
    assert status["skills"] == ["kitaru-investigation"]
    assert [entry["host"] for entry in status["installations"]] == [
        "agents",
        "claude",
    ]


@pytest.mark.parametrize(
    ("path_method", "error_type"),
    [("cwd", OSError), ("home", OSError), ("home", RuntimeError)],
)
def test_discovery_returns_empty_status_when_default_path_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    path_method: str,
    error_type: type[OSError] | type[RuntimeError],
) -> None:
    """Default path lookup failures do not break CLI startup or diagnostics."""

    def _raise_path_error() -> Path:
        raise error_type("unavailable")

    monkeypatch.setattr(Path, path_method, _raise_path_error)

    assert get_kitaru_skill_status() == {
        "installed": False,
        "skill_count": 0,
        "skills": [],
        "installations": [],
        "locations_checked": [],
    }


def test_discovery_ignores_nested_frontmatter_names(tmp_path: Path) -> None:
    """A nested name cannot overwrite the required top-level skill name."""
    project = tmp_path / "project"
    project.mkdir()
    home = tmp_path / "home"
    home.mkdir()
    skill = project / ".agents" / "skills" / "skill"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(
        "---\n"
        "name: kitaru-first\n"
        "description: Test skill.\n"
        "metadata:\n"
        "  name: kitaru-nested\n"
        "---\n",
        encoding="utf-8",
    )

    status = get_kitaru_skill_status(cwd=project, home=home)

    assert status["skills"] == ["kitaru-first"]


def test_discovery_does_not_accept_indented_block_scalar_name(tmp_path: Path) -> None:
    """An indented name inside a block scalar is not frontmatter metadata."""
    project = tmp_path / "project"
    project.mkdir()
    home = tmp_path / "home"
    home.mkdir()
    skill = project / ".agents" / "skills" / "skill"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(
        "---\ndescription: |\n  name: kitaru-not-a-skill\n---\n",
        encoding="utf-8",
    )

    status = get_kitaru_skill_status(cwd=project, home=home)

    assert status["installed"] is False


def test_discovery_classifies_home_as_user_scope(tmp_path: Path) -> None:
    """Skills in the home directory remain user-scoped when cwd is home."""
    home = tmp_path / "home"
    home.mkdir()
    skill = _write_skill(
        home / ".agents" / "skills" / "kitaru-user",
        "kitaru-user",
    )

    status = get_kitaru_skill_status(cwd=home, home=home)

    assert status["installations"] == [
        {
            "name": "kitaru-user",
            "scope": "user",
            "host": "agents",
            "path": str(skill.parent),
        }
    ]


def test_discovery_skips_unreadable_directory_and_finds_other_locations(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An unreadable location does not prevent discovery in readable roots."""
    project = tmp_path / "project"
    project.mkdir()
    home = tmp_path / "home"
    home.mkdir()
    unreadable = project / ".agents" / "skills"
    unreadable.mkdir(parents=True)
    _write_skill(
        home / ".codex" / "skills" / "kitaru-user",
        "kitaru-user",
    )
    original_iterdir = Path.iterdir

    def _raise_for_unreadable_directory(path: Path) -> Iterator[Path]:
        if path == unreadable:
            raise OSError("permission denied")
        return original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", _raise_for_unreadable_directory)

    status = get_kitaru_skill_status(cwd=project, home=home)

    assert status["skills"] == ["kitaru-user"]
