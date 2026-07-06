"""Tests for optional Modal dependency import boundaries."""

from __future__ import annotations

import importlib
import sys

import pytest


def _simulate_missing_modal_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force imports to behave as if the optional `modal` package is missing."""
    for cached in list(sys.modules):
        if cached == "modal" or cached.startswith("modal."):
            monkeypatch.delitem(sys.modules, cached, raising=False)
    monkeypatch.setitem(sys.modules, "modal", None)
    monkeypatch.setitem(sys.modules, "modal.config", None)


def test_importing_kitaru_does_not_require_modal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Base SDK import should work even when Modal extras are unavailable."""
    _simulate_missing_modal_dependency(monkeypatch)

    module = importlib.import_module("kitaru")
    reloaded = importlib.reload(module)

    assert reloaded.__name__ == "kitaru"


def test_importing_kitaru_config_does_not_require_modal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Config helpers should import without the Modal optional extra."""
    _simulate_missing_modal_dependency(monkeypatch)
    for cached in ("kitaru.config", "kitaru._config._stacks"):
        monkeypatch.delitem(sys.modules, cached, raising=False)
    kitaru_package = sys.modules.get("kitaru")
    if kitaru_package is not None:
        monkeypatch.delattr(kitaru_package, "config", raising=False)
    config_package = sys.modules.get("kitaru._config")
    if config_package is not None:
        monkeypatch.delattr(config_package, "_stacks", raising=False)

    module = importlib.import_module("kitaru.config")

    assert module.ModalStackSpec.__name__ == "ModalStackSpec"
