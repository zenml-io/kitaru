"""Validate the exact frontend artifact selected for a Kitaru release."""

import argparse
import json
import re
import sys
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
UI_REPOSITORY = "zenml-io/zenml-frontend-monorepo"
UI_ARCHIVE = "kitaru-ui.tar.gz"
UI_TAG_PATTERN = re.compile(
    r"kitaru-ui-v\d+\.\d+\.\d+(?P<prerelease>-[0-9A-Za-z][0-9A-Za-z.-]*)?"
)


class UIReleaseError(ValueError):
    """Raised when the selected frontend release is incomplete or invalid."""


@dataclass(frozen=True, slots=True)
class UIRelease:
    """One exact frontend artifact selected for a Kitaru package release."""

    kitaru_version: str
    repository: str
    tag: str
    archive: str
    allow_prerelease: bool

    def to_json(self) -> str:
        """Serialize the declaration for workflow consumption."""
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))


def _get_string(document: dict[str, object], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value:
        raise UIReleaseError(f"{key} must be a non-empty string")
    return value


def load_ui_release(version: str, repo_root: Path = REPO_ROOT) -> UIRelease:
    """Load the committed frontend declaration for one Kitaru version."""
    path = repo_root / "releases" / "python" / "kitaru" / f"{version}.toml"
    try:
        document = tomllib.loads(path.read_text())
    except FileNotFoundError as error:
        raise UIReleaseError(f"missing frontend declaration: {path}") from error
    except tomllib.TOMLDecodeError as error:
        raise UIReleaseError(f"invalid frontend declaration {path}: {error}") from error

    if document.get("schema-version") != 1:
        raise UIReleaseError("schema-version must be 1")
    declared_version = _get_string(document, "kitaru-version")
    if declared_version != version:
        raise UIReleaseError(
            f"kitaru-version {declared_version} does not match {version}"
        )
    tag = _get_string(document, "ui-tag")
    tag_match = UI_TAG_PATTERN.fullmatch(tag)
    if tag_match is None:
        raise UIReleaseError(f"invalid ui-tag: {tag}")

    return UIRelease(
        kitaru_version=version,
        repository=UI_REPOSITORY,
        tag=tag,
        archive=UI_ARCHIVE,
        allow_prerelease=tag_match.group("prerelease") is not None,
    )


def main() -> int:
    """Print one validated frontend declaration as JSON."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    try:
        print(load_ui_release(args.version).to_json())
    except UIReleaseError as error:
        print(f"release_ui_error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
