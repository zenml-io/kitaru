import datetime
from pathlib import Path

import pytest
from scripts.changelog_fragments import (
    ChangelogFragmentError,
    build_changelog,
    load_fragments,
    render_section,
)

RELEASE_DATE = datetime.date(2026, 9, 9)
CHANGELOG = "# Changelog\n\nIntro.\n\n## [0.1.0]\n\n### Added\n\n- Old entry.\n"


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    (tmp_path / "CHANGELOG.md").write_text(CHANGELOG)
    fragment_dir = tmp_path / "changelog.d"
    fragment_dir.mkdir()
    (fragment_dir / "README.md").write_text("# Changelog fragments\n")
    (fragment_dir / ".DS_Store").write_bytes(b"\x00")
    return tmp_path


def _write_fragment(repo: Path, name: str, content: str) -> Path:
    path = repo / "changelog.d" / name
    path.write_text(content)
    return path


def test_fragments_are_grouped_by_section_and_ordered_by_pr_number(
    repo: Path,
) -> None:
    _write_fragment(repo, "20.added.md", "- Second addition.\n")
    _write_fragment(repo, "9.added.md", "- First addition.\n\n- Third addition.\n")
    _write_fragment(repo, "15.fixed.md", "- A fix.\n")
    _write_fragment(repo, "draft.changed.md", "- A change.\n")

    fragments = load_fragments(repo / "changelog.d")

    assert [fragment.name for fragment in fragments] == ["9", "15", "20", "draft"]
    assert render_section("0.2.0", RELEASE_DATE, fragments) == (
        "## [0.2.0] - 2026-09-09\n\n"
        "### Added\n\n"
        "- First addition.\n- Third addition.\n- Second addition.\n\n"
        "### Changed\n\n- A change.\n\n"
        "### Fixed\n\n- A fix.\n"
    )


def test_build_inserts_the_section_above_the_latest_release(repo: Path) -> None:
    _write_fragment(repo, "9.added.md", "- New entry.\n")

    section = build_changelog("0.2.0", RELEASE_DATE, repo)

    assert section == "## [0.2.0] - 2026-09-09\n\n### Added\n\n- New entry.\n"
    assert (repo / "CHANGELOG.md").read_text() == (
        "# Changelog\n\nIntro.\n\n"
        "## [0.2.0] - 2026-09-09\n\n### Added\n\n- New entry.\n\n"
        "## [0.1.0]\n\n### Added\n\n- Old entry.\n"
    )
    assert sorted(path.name for path in (repo / "changelog.d").iterdir()) == [
        ".DS_Store",
        "README.md",
    ]


def test_build_appends_the_first_release_section(repo: Path) -> None:
    (repo / "CHANGELOG.md").write_text("# Changelog\n\nIntro.\n")
    _write_fragment(repo, "9.added.md", "- New entry.\n")

    build_changelog("0.1.0", RELEASE_DATE, repo)

    assert (repo / "CHANGELOG.md").read_text() == (
        "# Changelog\n\nIntro.\n\n"
        "## [0.1.0] - 2026-09-09\n\n### Added\n\n- New entry.\n"
    )


def test_build_rejects_an_existing_release_section_without_writes(
    repo: Path,
) -> None:
    fragment = _write_fragment(repo, "9.added.md", "- New entry.\n")

    with pytest.raises(ChangelogFragmentError, match="already contains"):
        build_changelog("0.1.0", RELEASE_DATE, repo)

    assert (repo / "CHANGELOG.md").read_text() == CHANGELOG
    assert fragment.is_file()


def test_build_writes_an_empty_release_section(repo: Path) -> None:
    assert build_changelog("0.2.0", RELEASE_DATE, repo) == "## [0.2.0] - 2026-09-09\n"

    assert (repo / "CHANGELOG.md").read_text() == (
        "# Changelog\n\nIntro.\n\n## [0.2.0] - 2026-09-09\n\n"
        "## [0.1.0]\n\n### Added\n\n- Old entry.\n"
    )


def test_missing_fragment_directory_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ChangelogFragmentError, match="missing fragment directory"):
        load_fragments(tmp_path / "changelog.d")


@pytest.mark.parametrize(
    ("name", "content", "error"),
    [
        ("notes.md", "- Entry.\n", "must be named"),
        ("9.md", "- Entry.\n", "must be named"),
        ("9.added.txt", "- Entry.\n", "must be named"),
        ("9.improved.md", "- Entry.\n", "unknown section"),
        ("9.added.md", "\n", "is empty"),
        ("9.added.md", "Entry without a bullet.\n", "list item"),
        ("9.added.md", "- Entry.\n  wrapped onto a second line.\n", "list item"),
    ],
)
def test_invalid_fragments_are_rejected(
    repo: Path, name: str, content: str, error: str
) -> None:
    _write_fragment(repo, name, content)

    with pytest.raises(ChangelogFragmentError, match=error):
        load_fragments(repo / "changelog.d")


def test_repository_fragments_are_valid() -> None:
    load_fragments()
