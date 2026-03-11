"""Tests for Kitaru skill content shipped in the repo."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_AUTHORING = (
    REPO_ROOT / ".claude-plugin" / "skills" / "kitaru-authoring" / "SKILL.md"
)
LOCAL_AUTHORING = REPO_ROOT / ".claude" / "skills" / "kitaru-authoring" / "SKILL.md"
PLUGIN_SCOPING = REPO_ROOT / ".claude-plugin" / "skills" / "kitaru-scoping" / "SKILL.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_plugin_skill_file_exists() -> None:
    """The plugin marketplace skill file should be present."""
    assert PLUGIN_AUTHORING.exists()


def test_authoring_skills_cover_current_capabilities() -> None:
    """Both authoring skills should cover the current Kitaru surface."""
    required_patterns = [
        "@flow",
        "@checkpoint",
        "wait()",
        "log()",
        "flowhandle",
        "kitaruclient",
        "kitaru_executions_replay",
        "checkpoint.<selector>",
        "wait.<selector>",
        "metadata_only",
        "deferred tool flows are not supported",
        "handle.exec_id",
        "handle.status",
        "handle.wait()",
        "handle.get()",
    ]

    for content in (_read(PLUGIN_AUTHORING), _read(LOCAL_AUTHORING)):
        normalized = content.lower()
        for pattern in required_patterns:
            assert pattern in normalized, (
                f"Authoring skill missing coverage of {pattern}"
            )


def test_plugin_and_local_authoring_skills_share_core_sections() -> None:
    """The shipped authoring skills should stay structurally aligned."""
    required_headings = [
        "## Mental model",
        "## Authoring guardrails",
        "## Primitive reference",
        "## Replay and control surfaces",
        "## Operational surfaces: what exists where",
        "## PydanticAI adapter",
        "## Common mistakes checklist",
    ]

    plugin_content = _read(PLUGIN_AUTHORING)
    local_content = _read(LOCAL_AUTHORING)

    for heading in required_headings:
        assert heading in plugin_content
        assert heading in local_content

    assert "kitaru-scoping" in plugin_content
    assert "scope the" in local_content.lower()
    assert "architecture first" in local_content.lower()


def test_scoping_skill_mentions_operator_surface_and_replay_anchors() -> None:
    """The scoping skill should capture the operational design decisions."""
    content = _read(PLUGIN_SCOPING)

    for pattern in [
        "Operator Surface",
        "Replay anchors",
        "KitaruClient",
        "CLI",
        "MCP",
        "deferred tool flows are not supported",
    ]:
        assert pattern in content, f"Scoping skill missing coverage of {pattern}"
