"""Smoke test to verify the package imports correctly."""

import kitaru


def test_package_imports() -> None:
    assert kitaru.__name__ == "kitaru"


def test_package_has_version() -> None:
    """Verify the package exposes a version via importlib.metadata."""
    from importlib.metadata import version

    v = version("kitaru")
    assert v
    assert "." in v
