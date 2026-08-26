import subprocess
import sys
from pathlib import Path

import pytest
from scripts.release_impact import (
    ReleaseImpactError,
    infer_release_labels,
    validate_release_impact,
)
from scripts.release_units import load_inventory

REPO_ROOT = Path(__file__).parents[2]


@pytest.fixture(scope="module")
def inventory():
    return load_inventory(REPO_ROOT)


def test_infers_core_and_named_plugin_from_paths(inventory) -> None:
    labels = infer_release_labels(
        [
            "src/kitaru/client.py",
            "plugins/packages/langfuse-importer/src/importer.py",
        ],
        inventory,
    )

    assert labels == {"requires:core", "requires:plugin:langfuse"}


def test_infers_direct_units_without_labels(inventory) -> None:
    inferred = validate_release_impact(
        set(),
        [
            "src/kitaru/client.py",
            "plugins/packages/langfuse-importer/src/importer.py",
        ],
        inventory,
    )

    assert inferred == {"requires:core", "requires:plugin:langfuse"}


def test_accepts_manual_followups(inventory) -> None:
    inferred = validate_release_impact(
        {"requires:frontend", "requires:plugins:adapters"},
        ["src/kitaru/client.py"],
        inventory,
    )

    assert inferred == {"requires:core"}


@pytest.mark.parametrize(
    "labels",
    [
        {"requires:none"},
        {"requires:other"},
        {"requires:plugin:missing"},
    ],
)
def test_rejects_unknown_release_labels(inventory, labels) -> None:
    with pytest.raises(ReleaseImpactError, match=r"unknown .* label"):
        validate_release_impact(labels, [], inventory)


def test_allows_no_labels_for_non_release_paths(inventory) -> None:
    assert (
        validate_release_impact(
            set(),
            [".github/workflows/release-impact.yml"],
            inventory,
        )
        == set()
    )


def test_release_impact_cli_runs_as_a_script(tmp_path: Path) -> None:
    changed_files = tmp_path / "changed-files.txt"
    changed_files.write_text("src/kitaru/client.py\n")

    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "release_impact.py"),
            "--changed-files",
            str(changed_files),
            "--label",
            "requires:skills",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Direct release units: requires:core" in result.stdout
    assert "Declared follow-ups: requires:skills" in result.stdout
