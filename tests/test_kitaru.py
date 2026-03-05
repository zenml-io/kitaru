"""Smoke test to verify the package imports correctly."""

import kitaru


def test_package_imports() -> None:
    assert kitaru.__name__ == "kitaru"
