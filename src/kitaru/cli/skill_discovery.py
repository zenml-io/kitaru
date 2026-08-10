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

import json
import os
import re
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
_MAX_CLAUDE_PLUGIN_REGISTRY_BYTES = 4 * 1024 * 1024
_CLAUDE_PLUGIN_ID = "kitaru@kitaru"
_SKILL_NAME_PATTERN = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*\Z")


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

    plugin_installations, plugin_locations = _get_claude_plugin_installations(
        current, user_home
    )
    installations.extend(plugin_installations)

    names = sorted({installation["name"] for installation in installations})
    return {
        "installed": bool(names),
        "skill_count": len(names),
        "skills": names,
        "installations": installations,
        "locations_checked": [str(directory) for _, _, directory in locations]
        + plugin_locations,
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
    try:
        if path.parent.is_symlink():
            return None
    except OSError:
        return None
    document = _read_skill_frontmatter(path)
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
    except (yaml.YAMLError, RecursionError):
        return None
    if not isinstance(metadata, dict):
        return None
    name = metadata.get("name")
    description = metadata.get("description")
    if (
        not isinstance(name, str)
        or len(name) > 64
        or _SKILL_NAME_PATTERN.fullmatch(name) is None
    ):
        return None
    if not isinstance(description, str) or not description.strip():
        return None
    return name


def _read_skill_frontmatter(path: Path) -> str | None:
    """Read complete YAML frontmatter within the skill metadata byte limit."""
    contents = _read_regular_bytes(
        path,
        max_bytes=_MAX_SKILL_DOCUMENT_BYTES,
        reject_oversized=False,
    )
    if contents is None:
        return None
    lines = contents.splitlines(keepends=True)
    if not lines or lines[0].strip() != b"---":
        return None
    frontmatter_bytes = len(lines[0])
    for line in lines[1:]:
        frontmatter_bytes += len(line)
        if line.rstrip(b"\r\n") == b"---":
            try:
                return contents[:frontmatter_bytes].decode("utf-8")
            except UnicodeError:
                return None
    return None


def _read_regular_bytes(
    path: Path, *, max_bytes: int, reject_oversized: bool
) -> bytes | None:
    """Read a bounded prefix from one regular, non-symlinked file."""
    try:
        if not stat.S_ISREG(path.stat(follow_symlinks=False).st_mode):
            return None
        flags = os.O_RDONLY
        flags |= getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_NONBLOCK", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                return None
            chunks: list[bytes] = []
            remaining = max_bytes + int(reject_oversized)
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
    if reject_oversized and len(contents) > max_bytes:
        return None
    return contents


def _read_regular_text(path: Path, *, max_bytes: int) -> str | None:
    """Read one regular, non-symlinked UTF-8 file within a byte limit."""
    contents = _read_regular_bytes(
        path,
        max_bytes=max_bytes,
        reject_oversized=True,
    )
    if contents is None:
        return None
    try:
        return contents.decode("utf-8")
    except UnicodeError:
        return None


def _get_claude_plugin_installations(
    cwd: Path, home: Path
) -> tuple[list[SkillInstallation], list[str]]:
    """Discover Kitaru skills installed through the Claude marketplace."""
    registry = home / ".claude" / "plugins" / "installed_plugins.json"
    checked = [str(registry)]
    document = _read_regular_text(registry, max_bytes=_MAX_CLAUDE_PLUGIN_REGISTRY_BYTES)
    if document is None:
        return [], checked
    try:
        registry_data = json.loads(document)
    except (ValueError, RecursionError):
        return [], checked
    if not isinstance(registry_data, dict):
        return [], checked
    plugins = registry_data.get("plugins")
    if not isinstance(plugins, dict):
        return [], checked
    records = plugins.get(_CLAUDE_PLUGIN_ID)
    if not isinstance(records, list):
        return [], checked

    project_roots = set(_get_project_search_roots(cwd, home))
    installations: list[SkillInstallation] = []
    seen_directories: set[tuple[SkillScope, Path]] = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        scope = _get_claude_plugin_scope(record, project_roots, home)
        install_path = record.get("installPath")
        if scope is None or not isinstance(install_path, str) or not install_path:
            continue
        skills_directory = _get_configured_path(install_path, home) / "skills"
        checked.append(str(skills_directory))
        directory_key = (scope, skills_directory)
        if directory_key in seen_directories:
            continue
        seen_directories.add(directory_key)
        try:
            skill_directories = sorted(
                skills_directory.iterdir(), key=lambda path: path.name
            )
        except OSError:
            continue
        for skill_directory in skill_directories:
            name = _get_skill_name(skill_directory / "SKILL.md")
            if name is None or not name.startswith(_KITARU_SKILL_PREFIX):
                continue
            installations.append(
                {
                    "name": name,
                    "scope": scope,
                    "host": "claude",
                    "path": str(skill_directory),
                }
            )
    return installations, list(dict.fromkeys(checked))


def _get_claude_plugin_scope(
    record: dict[object, object], project_roots: set[Path], home: Path
) -> SkillScope | None:
    """Map one Claude plugin record to its active Kitaru search scope."""
    scope = record.get("scope")
    if scope == "user":
        return "user"
    if not isinstance(scope, str) or scope not in {"project", "local"}:
        return None
    project_path = record.get("projectPath")
    if not isinstance(project_path, str) or not project_path:
        return None
    if _get_configured_path(project_path, home) not in project_roots:
        return None
    return "project"


def _get_configured_path(value: str, home: Path) -> Path:
    """Resolve an absolute or home-relative path from Claude plugin state."""
    if value == "~":
        return home
    if value.startswith(("~/", "~\\")):
        return (home / value[2:]).absolute()
    path = Path(value)
    return path.absolute() if path.is_absolute() else (home / path).absolute()
