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
"""Discover Kitaru agent skills in supported project and user locations."""

from collections.abc import Sequence
from pathlib import Path
from typing import Literal, TypedDict

INSTALL_COMMAND = "npx skills add zenml-io/kitaru-skills"
SKILLS_URL = "https://github.com/zenml-io/kitaru-skills"

SkillHost = Literal["agents", "claude", "codex"]
SkillScope = Literal["project", "user"]


class SkillInstallation(TypedDict):
    """One detected skill location for a supported agent host."""

    name: str
    scope: SkillScope
    host: SkillHost
    path: str


class KitaruSkillStatus(TypedDict):
    """Machine-readable summary of detected Kitaru agent skills."""

    installed: bool
    skill_count: int
    skills: list[str]
    installations: list[SkillInstallation]
    locations_checked: list[str]


_SKILL_DIRECTORIES: tuple[tuple[SkillHost, Path], ...] = (
    ("agents", Path(".agents") / "skills"),
    ("claude", Path(".claude") / "skills"),
    ("codex", Path(".codex") / "skills"),
)
_KITARU_SKILL_PREFIX = "kitaru-"


def _get_empty_skill_status() -> KitaruSkillStatus:
    """Return the empty skill-discovery result."""
    return {
        "installed": False,
        "skill_count": 0,
        "skills": [],
        "installations": [],
        "locations_checked": [],
    }


def get_kitaru_skill_status(
    *, cwd: Path | None = None, home: Path | None = None
) -> KitaruSkillStatus:
    """Report Kitaru skills installed for the current project or user.

    The search uses ordinary dot-directories supported on macOS, Linux, and
    Windows. Filesystem errors are ignored so onboarding and diagnostics remain
    available when an unrelated location is unreadable.

    Args:
        cwd: Working directory used to locate project-scoped skills.
        home: User home used to locate user-scoped skills.

    Returns:
        Machine-readable discovery status with every matching installation.
    """
    if cwd is None:
        try:
            current = Path.cwd().absolute()
        except OSError:
            return _get_empty_skill_status()
    else:
        current = cwd.absolute()
    if home is None:
        try:
            user_home = Path.home().absolute()
        except (OSError, RuntimeError):
            return _get_empty_skill_status()
    else:
        user_home = home.absolute()
    locations = _get_search_locations(current, user_home)
    installations: list[SkillInstallation] = []

    for scope, host, directory in locations:
        try:
            children = sorted(directory.iterdir(), key=lambda path: path.name)
        except OSError:
            continue
        for child in children:
            name = _get_skill_name(child / "SKILL.md")
            if name is None or not name.startswith(_KITARU_SKILL_PREFIX):
                continue
            installations.append(
                {
                    "name": name,
                    "scope": scope,
                    "host": host,
                    "path": str(child),
                }
            )

    names = sorted({installation["name"] for installation in installations})
    return {
        "installed": bool(names),
        "skill_count": len(names),
        "skills": names,
        "installations": installations,
        "locations_checked": [str(directory) for _, _, directory in locations],
    }


def select_visible_skill_names(
    names: Sequence[str], *, limit: int = 5
) -> tuple[list[str], int]:
    """Select a bounded skill-name summary and count the remainder."""
    visible = list(names[:limit])
    return visible, len(names) - len(visible)


def _get_search_locations(
    cwd: Path, home: Path
) -> list[tuple[SkillScope, SkillHost, Path]]:
    """Return unique project and user skill directories in search order."""
    locations: list[tuple[SkillScope, SkillHost, Path]] = []
    seen: set[Path] = set()
    for root in _get_project_search_roots(cwd, home):
        for host, relative in _SKILL_DIRECTORIES:
            directory = root / relative
            if directory not in seen:
                scope: SkillScope = "user" if root == home else "project"
                locations.append((scope, host, directory))
                seen.add(directory)
    for host, relative in _SKILL_DIRECTORIES:
        directory = home / relative
        if directory not in seen:
            locations.append(("user", host, directory))
            seen.add(directory)
    return locations


def _get_project_search_roots(cwd: Path, home: Path) -> list[Path]:
    """Search from the working directory through its nearest Git root."""
    if _is_git_root(cwd):
        return [cwd]
    candidates = [cwd]
    for parent in cwd.parents:
        if parent == home:
            break
        candidates.append(parent)
        if _is_git_root(parent):
            return candidates
    return [cwd]


def _is_git_root(path: Path) -> bool:
    """Return whether a path carries a Git directory or worktree marker."""
    try:
        return (path / ".git").exists()
    except OSError:
        return False


def _get_skill_name(path: Path) -> str | None:
    """Read a simple skill name from bounded YAML frontmatter."""
    try:
        with path.open(encoding="utf-8") as skill_file:
            if skill_file.readline().strip() != "---":
                return None
            name: str | None = None
            for _ in range(100):
                line = skill_file.readline()
                if not line:
                    return None
                if line.strip() == "---":
                    break
                key, separator, value = line.partition(":")
                if name is None and separator and key == "name":
                    name = value.strip().strip("'\"")
            else:
                return None
    except (OSError, UnicodeError):
        return None
    return name or None
