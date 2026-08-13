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
UI_TAG_PATTERN = re.compile(r"kitaru-ui-v\d+\.\d+\.\d+(?:-[0-9A-Za-z][0-9A-Za-z.-]*)?")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


class UIReleaseError(ValueError):
    """Raised when the selected frontend release is incomplete or invalid."""


@dataclass(frozen=True, slots=True)
class UIRelease:
    """One exact frontend artifact selected for a Kitaru package release."""

    kitaru_version: str
    repository: str
    tag: str
    archive: str
    sha256: str
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
    repository = _get_string(document, "ui-repository")
    if repository != UI_REPOSITORY:
        raise UIReleaseError(f"ui-repository must be {UI_REPOSITORY}")
    tag = _get_string(document, "ui-tag")
    if UI_TAG_PATTERN.fullmatch(tag) is None:
        raise UIReleaseError(f"invalid ui-tag: {tag}")
    archive = _get_string(document, "ui-archive")
    if archive != UI_ARCHIVE:
        raise UIReleaseError(f"ui-archive must be {UI_ARCHIVE}")
    sha256 = _get_string(document, "ui-sha256")
    if SHA256_PATTERN.fullmatch(sha256) is None:
        raise UIReleaseError("ui-sha256 must be 64 lowercase hexadecimal characters")
    allow_prerelease = document.get("allow-prerelease")
    if not isinstance(allow_prerelease, bool):
        raise UIReleaseError("allow-prerelease must be true or false")
    if "-" in tag and not allow_prerelease:
        raise UIReleaseError("a frontend prerelease requires allow-prerelease = true")

    return UIRelease(
        kitaru_version=version,
        repository=repository,
        tag=tag,
        archive=archive,
        sha256=sha256,
        allow_prerelease=allow_prerelease,
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
