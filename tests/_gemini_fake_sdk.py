"""Shared fake google-genai module setup for Gemini adapter tests."""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest


def purge_gemini_adapter_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove cached Gemini adapter modules so import-time guards rerun."""
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.gemini"):
            monkeypatch.delitem(sys.modules, cached, raising=False)


def install_fake_google_genai(
    monkeypatch: pytest.MonkeyPatch,
    *,
    include_client: bool = True,
    include_types: bool = True,
    function_call_annotations: dict[str, Any] | None = None,
    function_result_annotations: dict[str, Any] | None = None,
) -> types.ModuleType:
    """Install a minimal fake google-genai module tree for import tests."""
    for cached in (
        "google.genai._interactions",
        "google.genai._interactions.types",
    ):
        monkeypatch.delitem(sys.modules, cached, raising=False)

    google = types.ModuleType("google")
    google.__path__ = []  # type: ignore[attr-defined]
    genai = types.ModuleType("google.genai")
    google_module: Any = google
    genai_module: Any = genai
    google_module.genai = genai
    if include_client:
        genai_module.Client = object
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.genai", genai)
    if include_types:
        interactions = types.ModuleType("google.genai._interactions")
        interactions.__path__ = []  # type: ignore[attr-defined]
        interaction_types = types.ModuleType("google.genai._interactions.types")
        interactions_module: Any = interactions
        interaction_types_module: Any = interaction_types
        interaction_types_module.FunctionCallContent = type(
            "FunctionCallContent",
            (),
            {
                "__annotations__": {"id": str}
                if function_call_annotations is None
                else function_call_annotations
            },
        )
        interaction_types_module.FunctionResultContent = type(
            "FunctionResultContent",
            (),
            {
                "__annotations__": {"call_id": str}
                if function_result_annotations is None
                else function_result_annotations
            },
        )
        genai_module._interactions = interactions
        interactions_module.types = interaction_types
        monkeypatch.setitem(sys.modules, "google.genai._interactions", interactions)
        monkeypatch.setitem(
            sys.modules,
            "google.genai._interactions.types",
            interaction_types,
        )
    return genai
