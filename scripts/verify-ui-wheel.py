# /// script
# requires-python = ">=3.11"
# ///
"""Verify that a built Kitaru wheel contains the bundled UI assets."""

from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

_EXPECTED_FILES = (
    "kitaru/_ui/dist/index.html",
    "kitaru/_ui/bundle_manifest.json",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify that a Kitaru wheel includes packaged UI assets."
    )
    parser.add_argument("wheel", type=Path, help="Path to a built .whl file.")
    return parser.parse_args()


def _missing_ui_files(names: set[str]) -> list[str]:
    return [expected for expected in _EXPECTED_FILES if expected not in names]


def main() -> int:
    args = _parse_args()
    wheel = args.wheel
    if not wheel.is_file():
        print(f"::error::Wheel not found: {wheel}", file=sys.stderr)
        return 1

    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())

    missing = _missing_ui_files(names)
    if missing:
        for path in missing:
            print(f"::error::{path} missing from wheel")
        return 1

    ui_files = sum(name.startswith("kitaru/_ui/dist/") for name in names)
    print(f"Kitaru UI verified: {ui_files} asset files in {wheel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
