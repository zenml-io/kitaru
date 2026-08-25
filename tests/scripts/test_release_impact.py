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


def test_accepts_matching_release_labels(inventory) -> None:
    inferred = validate_release_impact(
        {"requires:core", "requires:plugin:langfuse", "requires:skills"},
        [
            "src/kitaru/client.py",
            "plugins/packages/langfuse-importer/src/importer.py",
        ],
        inventory,
    )

    assert inferred == {"requires:core", "requires:plugin:langfuse"}


def test_all_plugins_label_covers_changed_plugin_path(inventory) -> None:
    validate_release_impact(
        {"requires:plugins"},
        ["plugins/packages/logfire-importer/src/importer.py"],
        inventory,
    )


@pytest.mark.parametrize(
    ("labels", "message"),
    [
        (set(), "add at least one"),
        ({"requires:none", "requires:skills"}, "cannot be combined"),
        ({"requires:plugin:missing"}, "unknown plugin release label"),
    ],
)
def test_rejects_invalid_label_sets(inventory, labels, message) -> None:
    with pytest.raises(ReleaseImpactError, match=message):
        validate_release_impact(labels, [], inventory)


def test_rejects_missing_inferred_label(inventory) -> None:
    with pytest.raises(ReleaseImpactError, match="requires:core"):
        validate_release_impact(
            {"requires:skills"},
            ["src/kitaru/client.py"],
            inventory,
        )


def test_allows_requires_none_for_non_release_paths(inventory) -> None:
    assert (
        validate_release_impact(
            {"requires:none"},
            [".github/workflows/release-impact.yml"],
            inventory,
        )
        == set()
    )
