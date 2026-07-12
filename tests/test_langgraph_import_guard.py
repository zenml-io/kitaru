"""Import-time guard tests for the LangGraph adapter."""

from __future__ import annotations

import importlib
import sys
import types

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


def test_package_import_stays_safe_when_sandbox_tool_api_is_missing_until_factory_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_langgraph_adapter_modules(monkeypatch)
    monkeypatch.setitem(sys.modules, "langgraph", types.ModuleType("langgraph"))
    monkeypatch.setitem(sys.modules, "langchain_core.tools", None)

    adapter = importlib.import_module("kitaru.adapters.langgraph")

    assert adapter.create_sandbox_command_tool
    with pytest.raises(KitaruFeatureNotAvailableError, match="StructuredTool"):
        adapter.create_sandbox_command_tool()


def test_langchain_middleware_import_guard_uses_langgraph_extra_guidance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_langgraph_adapter_modules(monkeypatch)
    for cached in list(sys.modules):
        if cached == "langchain" or cached.startswith("langchain."):
            monkeypatch.delitem(sys.modules, cached, raising=False)

    class MissingLangChainImporter:
        def find_spec(self, fullname: str, path: object = None, target: object = None):
            if fullname == "langchain" or fullname.startswith("langchain."):
                raise ModuleNotFoundError(
                    "No module named 'langchain'",
                    name="langchain",
                )
            return None

    monkeypatch.setattr(sys, "meta_path", [MissingLangChainImporter(), *sys.meta_path])

    with pytest.raises(KitaruFeatureNotAvailableError, match=r"kitaru\[langgraph\]"):
        importlib.import_module("kitaru.adapters.langgraph.langchain")


def test_broken_langchain_middleware_import_preserves_compatibility_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _purge_langgraph_adapter_modules(monkeypatch)
    for cached in list(sys.modules):
        if cached == "langchain" or cached.startswith("langchain."):
            monkeypatch.delitem(sys.modules, cached, raising=False)

    class BrokenLangChainImporter:
        def find_spec(self, fullname: str, path: object = None, target: object = None):
            if fullname == "langchain.agents.middleware":
                raise ImportError("LangChain middleware API failed to import")
            return None

    monkeypatch.setattr(sys, "meta_path", [BrokenLangChainImporter(), *sys.meta_path])

    with pytest.raises(
        ImportError,
        match="installed LangChain version may be incompatible",
    ) as exc_info:
        importlib.import_module("kitaru.adapters.langgraph.langchain")

    assert "LangChain middleware API failed to import" in str(exc_info.value)
