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
    interaction_type_module: str = "legacy",
    function_call_annotations: dict[str, Any] | None = None,
    function_result_annotations: dict[str, Any] | None = None,
) -> types.ModuleType:
    """Install a minimal fake google-genai module tree for import tests."""
    for cached in (
        "google.genai._interactions",
        "google.genai._interactions.types",
        "google.genai._gaos",
        "google.genai._gaos.types",
        "google.genai._gaos.types.interactions",
        "google.genai._gaos.types.interactions.step",
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
        interaction_types = types.ModuleType("google.genai._interactions.types")
        interaction_types_module: Any = interaction_types
        interaction_types_module.FunctionCallStep = type(
            "FunctionCallStep",
            (),
            {
                "__annotations__": {
                    "arguments": dict[str, Any],
                    "id": str,
                    "name": str,
                    "type": str,
                }
                if function_call_annotations is None
                else function_call_annotations
            },
        )
        interaction_types_module.FunctionResultStep = type(
            "FunctionResultStep",
            (),
            {
                "__annotations__": {
                    "call_id": str,
                    "name": str,
                    "result": dict[str, Any],
                    "type": str,
                }
                if function_result_annotations is None
                else function_result_annotations
            },
        )
        if interaction_type_module == "legacy":
            interactions = types.ModuleType("google.genai._interactions")
            interactions.__path__ = []  # type: ignore[attr-defined]
            interactions_module: Any = interactions
            genai_module._interactions = interactions
            interactions_module.types = interaction_types
            monkeypatch.setitem(sys.modules, "google.genai._interactions", interactions)
            monkeypatch.setitem(
                sys.modules,
                "google.genai._interactions.types",
                interaction_types,
            )
        elif interaction_type_module == "gaos":
            gaos = types.ModuleType("google.genai._gaos")
            gaos.__path__ = []  # type: ignore[attr-defined]
            gaos_types = types.ModuleType("google.genai._gaos.types")
            gaos_types.__path__ = []  # type: ignore[attr-defined]
            interactions = types.ModuleType("google.genai._gaos.types.interactions")
            interactions.__path__ = []  # type: ignore[attr-defined]
            interaction_types.__name__ = "google.genai._gaos.types.interactions.step"
            gaos_module: Any = gaos
            gaos_types_module: Any = gaos_types
            interactions_module: Any = interactions
            genai_module._gaos = gaos
            gaos_module.types = gaos_types
            gaos_types_module.interactions = interactions
            interactions_module.step = interaction_types
            monkeypatch.setitem(sys.modules, "google.genai._gaos", gaos)
            monkeypatch.setitem(sys.modules, "google.genai._gaos.types", gaos_types)
            monkeypatch.setitem(
                sys.modules,
                "google.genai._gaos.types.interactions",
                interactions,
            )
            monkeypatch.setitem(
                sys.modules,
                "google.genai._gaos.types.interactions.step",
                interaction_types,
            )
        else:
            raise ValueError(
                f"Unknown interaction_type_module: {interaction_type_module}"
            )
    return genai
