"""Tests for package-scoped release planning."""

import importlib.util
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "release_plan.py"
SPEC = importlib.util.spec_from_file_location("release_plan", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
release_plan = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_plan)


def test_detects_only_changed_plugin_package() -> None:
    """A plugin path does not imply a core release."""
    units = release_plan.get_affected_units(
        ("plugins/packages/langfuse-importer/src/example.py",)
    )

    assert units == ("langfuse-importer",)


def test_requires_one_label_for_each_affected_unit() -> None:
    """Each independently releasable unit requires one bump decision."""
    paths = (
        "src/kitaru/client/client.py",
        "plugins/packages/evaluator/src/kitaru_evaluator/basic.py",
    )

    with pytest.raises(release_plan.ReleasePlanError, match="evaluator"):
        release_plan.create_plan(paths, ("release:core:minor",))


def test_rejects_release_label_for_unaffected_unit() -> None:
    """A stale package label cannot silently release another package."""
    paths = ("plugins/packages/langfuse-importer/pyproject.toml",)

    with pytest.raises(release_plan.ReleasePlanError, match="unaffected units"):
        release_plan.create_plan(
            paths,
            ("release:langfuse-importer:patch", "release:core:patch"),
        )


def test_calculates_independent_plugin_patch() -> None:
    """A plugin patch leaves the core package outside the release plan."""
    plan = release_plan.create_plan(
        ("plugins/packages/langfuse-importer/pyproject.toml",),
        ("release:langfuse-importer:patch",),
    )

    assert len(plan.packages) == 1
    package = plan.packages[0]
    assert package.unit == "langfuse-importer"
    assert package.current_version == "0.1.1"
    assert package.next_version == "0.1.2"
    assert package.default_plugin is True


def test_calculates_canonical_rc_version() -> None:
    """RC plans use the canonical PEP 440 package version."""
    plan = release_plan.create_plan(
        ("src/kitaru/client/client.py",),
        ("release:core:minor", "release:channel:rc"),
    )

    assert plan.channel == "rc"
    assert plan.packages[0].next_version == "0.22.0rc1"


def test_allows_explicit_no_release_decision() -> None:
    """An affected unit can explicitly declare that no artifact should ship."""
    plan = release_plan.create_plan(
        ("src/kitaru/client/client.py",),
        ("release:core:none",),
    )

    assert plan.packages[0].next_version is None


def test_increments_existing_release_candidate() -> None:
    """A later RC keeps the base version and increments its sequence."""
    version = release_plan.Version.parse("0.22.0rc1")

    assert str(version.bump("patch", "rc")) == "0.22.0rc2"


def test_promotes_release_candidate_to_stable() -> None:
    """A stable bump removes the RC suffix without changing the base version."""
    version = release_plan.Version.parse("0.22.0rc2")

    assert str(version.bump("stable", "stable")) == "0.22.0"
