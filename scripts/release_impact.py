"""Validate pull-request release signals against changed repository paths."""

import argparse
import json
import sys
from pathlib import Path

if __package__:
    from scripts.release_units import ReleaseInventory, load_inventory
else:
    from release_units import ReleaseInventory, load_inventory

NO_RELEASE_LABEL = "requires:none"
ALL_PLUGINS_LABEL = "requires:plugins"


class ReleaseImpactError(ValueError):
    """Raised when a pull request has incomplete release signals."""


def _matches(path: str, pattern: str) -> bool:
    if pattern.endswith("/**"):
        prefix = pattern.removesuffix("/**")
        return path == prefix or path.startswith(f"{prefix}/")
    return path == pattern


def infer_release_labels(
    changed_files: list[str], inventory: ReleaseInventory
) -> set[str]:
    """Infer repository-local release labels from changed paths."""
    return {
        unit.release_label
        for unit in inventory.units
        if any(
            _matches(path, pattern)
            for path in changed_files
            for pattern in unit.impact_paths
        )
    }


def validate_release_impact(
    labels: set[str], changed_files: list[str], inventory: ReleaseInventory
) -> set[str]:
    """Validate PR labels and return the labels inferred from changed paths."""
    release_labels = {label for label in labels if label.startswith("requires:")}
    if not release_labels:
        raise ReleaseImpactError("add at least one requires:* label")
    if NO_RELEASE_LABEL in release_labels and len(release_labels) != 1:
        raise ReleaseImpactError(
            "requires:none cannot be combined with other requires:* labels"
        )

    known_plugin_labels = {unit.release_label for unit in inventory.plugin_units}
    unknown_plugin_labels = sorted(
        label
        for label in release_labels
        if label.startswith("requires:plugin:") and label not in known_plugin_labels
    )
    if unknown_plugin_labels:
        raise ReleaseImpactError(
            f"unknown plugin release label: {unknown_plugin_labels[0]}"
        )

    inferred = infer_release_labels(changed_files, inventory)
    missing = sorted(
        label
        for label in inferred
        if label not in release_labels
        and not (label in known_plugin_labels and ALL_PLUGINS_LABEL in release_labels)
    )
    if missing:
        raise ReleaseImpactError(
            "changed paths require missing label(s): " + ", ".join(missing)
        )
    if inferred and NO_RELEASE_LABEL in release_labels:
        raise ReleaseImpactError(
            "requires:none conflicts with changed release surfaces: "
            + ", ".join(sorted(inferred))
        )
    return inferred


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", action="append", default=[])
    parser.add_argument("--changed-files", type=Path, required=True)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args()


def main() -> int:
    """Run the pull-request release-impact check."""
    args = _parse_args()
    changed_files = [
        line.strip()
        for line in args.changed_files.read_text().splitlines()
        if line.strip()
    ]
    inventory = load_inventory()
    try:
        inferred = validate_release_impact(set(args.label), changed_files, inventory)
    except ReleaseImpactError as error:
        if args.format == "json":
            print(json.dumps({"status": "invalid", "error": str(error)}))
        else:
            print(f"error: {error}", file=sys.stderr)
        return 2

    if args.format == "json":
        print(json.dumps({"status": "valid", "inferred_labels": sorted(inferred)}))
    else:
        rendered = ", ".join(sorted(inferred)) or "none"
        print(f"Release impact is valid. Inferred labels: {rendered}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
