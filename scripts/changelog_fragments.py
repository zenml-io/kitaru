"""Changelog fragment assembly."""

import argparse
import datetime
import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
FRAGMENT_DIRNAME = "changelog.d"
CHANGELOG_FILENAME = "CHANGELOG.md"
IGNORED_FRAGMENT_FILENAMES = frozenset({"README.md"})

# Section headings in the order they appear inside one release.
SECTION_HEADINGS = {
    "added": "Added",
    "changed": "Changed",
    "deprecated": "Deprecated",
    "removed": "Removed",
    "fixed": "Fixed",
    "security": "Security",
}

_FRAGMENT_NAME = re.compile(r"^(?P<name>[A-Za-z0-9_-]+)\.(?P<section>[a-z]+)\.md$")
_RELEASE_HEADING = re.compile(r"^## ", flags=re.MULTILINE)


class ChangelogFragmentError(Exception):
    """Raised when a changelog fragment cannot be assembled."""


@dataclass(frozen=True)
class Fragment:
    """Changelog fragment."""

    path: Path
    name: str
    section: str
    entries: str

    @property
    def sort_key(self) -> tuple[int, int, str]:
        """Order numeric pull request names ascending ahead of other names."""
        if self.name.isdigit():
            return (0, int(self.name), "")
        return (1, 0, self.name)


def load_fragments(directory: Path = REPO_ROOT / FRAGMENT_DIRNAME) -> list[Fragment]:
    """Load and validate every fragment in the directory."""
    if not directory.is_dir():
        raise ChangelogFragmentError(f"missing fragment directory: {directory}")
    fragments = []
    for path in sorted(directory.iterdir()):
        if path.name in IGNORED_FRAGMENT_FILENAMES:
            continue
        match = _FRAGMENT_NAME.match(path.name)
        if not path.is_file() or match is None:
            raise ChangelogFragmentError(
                f"{path.name}: fragment must be named <pr-number>.<section>.md"
            )
        section = match.group("section")
        if section not in SECTION_HEADINGS:
            raise ChangelogFragmentError(
                f"{path.name}: unknown section {section!r}, expected one of "
                + ", ".join(SECTION_HEADINGS)
            )
        entries = _read_entries(path)
        fragments.append(Fragment(path, match.group("name"), section, entries))
    return sorted(fragments, key=lambda fragment: fragment.sort_key)


def _read_entries(path: Path) -> str:
    lines = [line.rstrip() for line in path.read_text().splitlines()]
    lines = [line for line in lines if line]
    if not lines:
        raise ChangelogFragmentError(f"{path.name}: fragment is empty")
    for line in lines:
        if not line.startswith("- "):
            raise ChangelogFragmentError(
                f"{path.name}: every line must be a list item starting with '- '"
            )
    return "\n".join(lines)


def render_section(version: str, date: datetime.date, fragments: list[Fragment]) -> str:
    """Render the release section holding the fragments."""
    if not fragments:
        raise ChangelogFragmentError("no changelog fragments to release")
    blocks = [f"## [{version}] - {date.isoformat()}"]
    for section, heading in SECTION_HEADINGS.items():
        entries = [
            fragment.entries for fragment in fragments if fragment.section == section
        ]
        if entries:
            blocks.append(f"### {heading}")
            blocks.append("\n".join(entries))
    return "\n\n".join(blocks) + "\n"


def insert_section(changelog: str, section: str) -> str:
    """Insert a release section above the first existing release."""
    match = _RELEASE_HEADING.search(changelog)
    if match is None:
        return changelog.rstrip("\n") + "\n\n" + section
    return changelog[: match.start()] + section + "\n" + changelog[match.start() :]


def build_changelog(
    version: str, date: datetime.date, repo_root: Path = REPO_ROOT
) -> str:
    """Move the fragments into a new release section of the changelog."""
    changelog_path = repo_root / CHANGELOG_FILENAME
    fragments = load_fragments(repo_root / FRAGMENT_DIRNAME)
    section = render_section(version, date, fragments)
    try:
        changelog = changelog_path.read_text()
    except FileNotFoundError as error:
        raise ChangelogFragmentError(f"missing changelog: {error.filename}") from error
    if f"## [{version}]" in changelog:
        raise ChangelogFragmentError(
            f"changelog already contains a [{version}] section"
        )
    changelog_path.write_text(insert_section(changelog, section))
    for fragment in fragments:
        fragment.path.unlink()
    return section


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Assemble CHANGELOG.md from the changelog.d fragments."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("check", help="Validate every fragment.")

    build_parser = subparsers.add_parser(
        "build", help="Move the fragments into a new release section."
    )
    build_parser.add_argument("--version", required=True)
    build_parser.add_argument(
        "--date", type=datetime.date.fromisoformat, default=datetime.date.today()
    )
    build_parser.add_argument(
        "--draft",
        action="store_true",
        help="Print the release section without changing any file.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the changelog fragment CLI."""
    args = _parse_args()
    try:
        if args.command == "check":
            fragments = load_fragments()
            output = f"{len(fragments)} valid changelog fragment(s)"
        elif args.draft:
            output = render_section(args.version, args.date, load_fragments())
        else:
            output = build_changelog(args.version, args.date)
    except ChangelogFragmentError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(output, end="" if output.endswith("\n") else "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
