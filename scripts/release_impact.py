"""Report direct release units and validate declared release follow-ups."""

import argparse
import json
import sys
from pathlib import Path

if __package__:
    from scripts.release_units import ReleaseInventory, load_inventory
else:
    from release_units import ReleaseInventory, load_inventory

ALL_PLUGINS_LABEL = "requires:plugins"
KNOWN_FOLLOWUP_LABELS = frozenset(
    {
        ALL_PLUGINS_LABEL,
        "requires:plugins:importers",
        "requires:plugins:adapters",
        "requires:plugins:evaluators",
        "requires:frontend",
        "requires:skills",
        "requires:zenml-docs",
        "requires:website",
    }
)


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
    """Validate follow-up labels and return units inferred from changed paths."""
    release_labels = {label for label in labels if label.startswith("requires:")}

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

    known_release_labels = {
        unit.release_label for unit in inventory.units
    } | KNOWN_FOLLOWUP_LABELS
    unknown_release_labels = sorted(release_labels - known_release_labels)
    if unknown_release_labels:
        raise ReleaseImpactError(
            f"unknown release follow-up label: {unknown_release_labels[0]}"
        )

    inferred = infer_release_labels(changed_files, inventory)
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

    declared = {
        label
        for label in args.label
        if label.startswith("requires:") and label not in inferred
    }
    if args.format == "json":
        print(
            json.dumps(
                {
                    "status": "valid",
                    "direct_release_units": sorted(inferred),
                    "declared_followups": sorted(declared),
                }
            )
        )
    else:
        direct = ", ".join(sorted(inferred)) or "none"
        followups = ", ".join(sorted(declared)) or "none"
        print(f"Direct release units: {direct}")
        print(f"Declared follow-ups: {followups}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
