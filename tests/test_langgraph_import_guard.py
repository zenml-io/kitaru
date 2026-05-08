"""Import-time guard tests for the LangGraph adapter."""

from __future__ import annotations

import importlib
import sys

import pytest

from kitaru.errors import KitaruFeatureNotAvailableError


def _purge_langgraph_adapter_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.langgraph"):
            monkeypatch.delitem(sys.modules, cached, raising=False)


def test_import_without_langgraph_raises_feature_not_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_langgraph_adapter_modules(monkeypatch)
    monkeypatch.setitem(sys.modules, "langgraph", None)

    with pytest.raises(KitaruFeatureNotAvailableError, match="langgraph"):
        importlib.import_module("kitaru.adapters.langgraph")


def test_transitive_langgraph_import_error_is_not_masked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_langgraph_adapter_modules(monkeypatch)
    for cached in list(sys.modules):
        if cached == "langgraph" or cached.startswith("langgraph."):
            monkeypatch.delitem(sys.modules, cached, raising=False)

    class BrokenLangGraphImporter:
        def find_spec(self, fullname: str, path: object = None, target: object = None):
            if fullname == "langgraph":
                raise ModuleNotFoundError(
                    "No module named 'langchain_core'", name="langchain_core"
                )
            return None

    importer = BrokenLangGraphImporter()
    monkeypatch.setattr(sys, "meta_path", [importer, *sys.meta_path])

    with pytest.raises(ModuleNotFoundError, match="langchain_core"):
        importlib.import_module("kitaru.adapters.langgraph")
