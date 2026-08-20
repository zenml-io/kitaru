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
"""Parity contract for repo-local developer skills."""

import subprocess
from pathlib import Path

import pytest
import yaml

from kitaru.cli.skill_discovery import get_kitaru_skill_status

REPO_ROOT = Path(__file__).parents[1]

# Host-neutral skills should stay byte-identical. Add a name here only when a
# documented host capability requires different Claude Code instructions.
HOST_SPECIFIC_SKILLS: frozenset[str] = frozenset()

REPO_SKILLS = (
    "kitaru-adapter-development",
    "kitaru-dev",
    "kitaru-docs",
    "kitaru-importer-development",
    "kitaru-release",
    "kitaru-tests-release",
    "kitaru-ui-api-development",
)


@pytest.mark.parametrize("skill_root", (".agents/skills", ".claude/skills"))
def test_repo_skill_catalog_covers_every_skill(skill_root: str) -> None:
    result = subprocess.run(
        [
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            f"{skill_root}/*/SKILL.md",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    canonical_names = {Path(path).parent.name for path in result.stdout.splitlines()}

    assert canonical_names == set(REPO_SKILLS)


def test_repo_skill_catalog_is_discoverable_by_each_host(tmp_path: Path) -> None:
    status = get_kitaru_skill_status(cwd=REPO_ROOT, home=tmp_path)
    installations = {
        (installation["host"], installation["name"])
        for installation in status["installations"]
    }

    assert installations == {
        (host, name) for host in ("agents", "claude") for name in REPO_SKILLS
    }


def _load_frontmatter(content: bytes) -> dict[str, object]:
    text = content.decode("utf-8")
    _, frontmatter, _ = text.split("---", maxsplit=2)
    metadata = yaml.safe_load(frontmatter)
    assert isinstance(metadata, dict)
    return metadata


@pytest.mark.parametrize("name", REPO_SKILLS)
def test_repo_skill_is_available_to_codex_and_claude(name: str) -> None:
    agents_path = REPO_ROOT / ".agents" / "skills" / name / "SKILL.md"
    claude_path = REPO_ROOT / ".claude" / "skills" / name / "SKILL.md"
    agents_content = agents_path.read_bytes()
    claude_content = claude_path.read_bytes()

    for content in (agents_content, claude_content):
        metadata = _load_frontmatter(content)
        assert metadata["name"] == name
        assert isinstance(metadata["description"], str)
        assert metadata["description"]

    if name not in HOST_SPECIFIC_SKILLS:
        assert agents_content == claude_content
