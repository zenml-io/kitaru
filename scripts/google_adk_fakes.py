"""Shared fake Google ADK modules for deterministic adapter tests."""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

import pytest


class FakeBaseLlm:
    """Minimal stand-in for ``google.adk.models.base_llm.BaseLlm``."""

    def __init__(self, *, model: str | None = None, **_kwargs: Any) -> None:
        self.model = model or ""

    @classmethod
    def supported_models(cls) -> list[str]:
        return []


class FakeFunctionResponse:
    """Minimal stand-in for ``google.genai.types.FunctionResponse``."""

    def __init__(
        self,
        *,
        name: str | None = None,
        id: str | None = None,
        response: dict[str, Any] | None = None,
        **_kwargs: Any,
    ) -> None:
        self.name = name
        self.id = id
        self.response = response or {}


class FakePart:
    """Minimal stand-in for ``google.genai.types.Part``."""

    def __init__(
        self,
        *,
        function_response: Any | None = None,
        functionResponse: Any | None = None,
        text: str | None = None,
        **_kwargs: Any,
    ) -> None:
        self.function_response = function_response or functionResponse
        self.text = text

    @classmethod
    def from_text(cls, *, text: str) -> FakePart:
        return cls(text=text)


class FakeContent:
    """Minimal stand-in for ``google.genai.types.Content``."""

    def __init__(self, *, role: str | None = None, parts: list[Any] | None = None):
        self.role = role
        self.parts = list(parts or [])


class FakeBaseTool:
    """Minimal stand-in for ``google.adk.tools.base_tool.BaseTool``."""

    def __init__(
        self,
        *,
        name: str,
        description: str = "",
        is_long_running: bool = False,
        custom_metadata: dict[str, Any] | None = None,
        **_kwargs: Any,
    ) -> None:
        self.name = name
        self.description = description
        self.is_long_running = is_long_running
        self.custom_metadata = custom_metadata


def purge_google_adk_adapter_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove cached Kitaru Google ADK adapter modules."""
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.google_adk"):
            monkeypatch.delitem(sys.modules, cached, raising=False)


def install_fake_google_adk(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install a minimal fake ``google.adk`` package tree."""
    google = ModuleType("google")
    google.__path__ = []  # type: ignore[attr-defined]
    adk = ModuleType("google.adk")
    adk.__path__ = []  # type: ignore[attr-defined]
    models = ModuleType("google.adk.models")
    models.__path__ = []  # type: ignore[attr-defined]
    base_llm = ModuleType("google.adk.models.base_llm")
    tools = ModuleType("google.adk.tools")
    tools.__path__ = []  # type: ignore[attr-defined]
    base_tool = ModuleType("google.adk.tools.base_tool")
    genai = ModuleType("google.genai")
    genai.__path__ = []  # type: ignore[attr-defined]
    genai_types = ModuleType("google.genai.types")

    base_llm.__dict__["BaseLlm"] = FakeBaseLlm
    base_tool.__dict__["BaseTool"] = FakeBaseTool
    genai_types.__dict__["Content"] = FakeContent
    genai_types.__dict__["FunctionResponse"] = FakeFunctionResponse
    genai_types.__dict__["Part"] = FakePart
    google.__dict__["adk"] = adk
    google.__dict__["genai"] = genai
    adk.__dict__["models"] = models
    adk.__dict__["tools"] = tools
    models.__dict__["base_llm"] = base_llm
    tools.__dict__["base_tool"] = base_tool
    genai.__dict__["types"] = genai_types

    for name, module in {
        "google": google,
        "google.adk": adk,
        "google.adk.models": models,
        "google.adk.models.base_llm": base_llm,
        "google.adk.tools": tools,
        "google.adk.tools.base_tool": base_tool,
        "google.genai": genai,
        "google.genai.types": genai_types,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
