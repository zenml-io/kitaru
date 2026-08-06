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

import os
import stat
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, TypedDict

import yaml

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
_MAX_SKILL_DOCUMENT_BYTES = 64 * 1024


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
    """Read a valid skill name from bounded YAML frontmatter."""
    document = _read_skill_document(path)
    if document is None:
        return None
    lines = document.splitlines()
    if not lines or lines[0].strip() != "---":
        return None
    try:
        closing_line = next(
            index for index, line in enumerate(lines[1:], start=1) if line == "---"
        )
    except StopIteration:
        return None
    try:
        metadata = yaml.safe_load("\n".join(lines[1:closing_line]))
    except yaml.YAMLError:
        return None
    if not isinstance(metadata, dict):
        return None
    name = metadata.get("name")
    description = metadata.get("description")
    if not isinstance(name, str) or not name.strip():
        return None
    if not isinstance(description, str) or not description.strip():
        return None
    return name.strip()


def _read_skill_document(path: Path) -> str | None:
    """Read one regular, non-symlinked skill document within a byte limit."""
    try:
        if path.is_symlink():
            return None
        flags = os.O_RDONLY
        flags |= getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                return None
            chunks: list[bytes] = []
            remaining = _MAX_SKILL_DOCUMENT_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, min(8192, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
        finally:
            os.close(descriptor)
    except OSError:
        return None
    contents = b"".join(chunks)
    if len(contents) > _MAX_SKILL_DOCUMENT_BYTES:
        return None
    try:
        return contents.decode("utf-8")
    except UnicodeError:
        return None
