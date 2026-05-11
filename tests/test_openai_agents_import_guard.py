"""Import-time guard tests for the OpenAI Agents SDK adapter."""

from __future__ import annotations

import importlib
import sys

import pytest

from kitaru.errors import KitaruFeatureNotAvailableError


def _purge_openai_adapter_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.openai_agents"):
            monkeypatch.delitem(sys.modules, cached, raising=False)


def test_import_without_openai_agents_raises_feature_not_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_openai_adapter_modules(monkeypatch)
    monkeypatch.setitem(sys.modules, "agents", None)

    with pytest.raises(KitaruFeatureNotAvailableError, match="openai-agents"):
        importlib.import_module("kitaru.adapters.openai_agents")


def test_transitive_openai_agents_import_error_is_not_masked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_openai_adapter_modules(monkeypatch)
    for cached in list(sys.modules):
        if cached == "agents" or cached.startswith("agents."):
            monkeypatch.delitem(sys.modules, cached, raising=False)

    class BrokenAgentsImporter:
        def find_spec(self, fullname: str, path: object = None, target: object = None):
            if fullname == "agents":
                raise ModuleNotFoundError("No module named 'openai'", name="openai")
            return None

    importer = BrokenAgentsImporter()
    monkeypatch.setattr(sys, "meta_path", [importer, *sys.meta_path])

    with pytest.raises(ModuleNotFoundError, match="openai"):
        importlib.import_module("kitaru.adapters.openai_agents")
