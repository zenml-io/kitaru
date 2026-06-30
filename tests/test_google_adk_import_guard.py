"""Import-time guard tests for the Google ADK adapter."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType

import pytest

from google_adk_fakes import install_fake_google_adk, purge_google_adk_adapter_modules
from kitaru.errors import KitaruFeatureNotAvailableError


def test_import_without_google_adk_raises_feature_not_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    purge_google_adk_adapter_modules(monkeypatch)
    monkeypatch.setitem(sys.modules, "google.adk", None)

    with pytest.raises(KitaruFeatureNotAvailableError, match="google-adk"):
        importlib.import_module("kitaru.adapters.google_adk")


def test_transitive_google_adk_import_error_is_not_masked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    purge_google_adk_adapter_modules(monkeypatch)
    for cached in list(sys.modules):
        if cached == "google.adk" or cached.startswith("kitaru.adapters.google_adk"):
            monkeypatch.delitem(sys.modules, cached, raising=False)

    class BrokenGoogleADKImporter:
        def find_spec(self, fullname: str, path: object = None, target: object = None):
            if fullname == "google.adk":
                raise ModuleNotFoundError("No module named 'grpc'", name="grpc")
            return None

    importer = BrokenGoogleADKImporter()
    monkeypatch.setattr(sys, "meta_path", [importer, *sys.meta_path])

    with pytest.raises(ModuleNotFoundError, match="grpc"):
        importlib.import_module("kitaru.adapters.google_adk")


def test_incomplete_google_adk_base_modules_error_is_not_masked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    purge_google_adk_adapter_modules(monkeypatch)
    for cached in list(sys.modules):
        if cached.startswith("google.adk."):
            monkeypatch.delitem(sys.modules, cached, raising=False)
    google = ModuleType("google")
    google.__path__ = []  # type: ignore[attr-defined]
    adk = ModuleType("google.adk")
    adk.__path__ = []  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "google", google)
    monkeypatch.setitem(sys.modules, "google.adk", adk)

    with pytest.raises(ModuleNotFoundError, match=r"google\.adk\.models"):
        importlib.import_module("kitaru.adapters.google_adk")


def test_fake_google_adk_allows_adapter_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    purge_google_adk_adapter_modules(monkeypatch)
    install_fake_google_adk(monkeypatch)

    module = importlib.import_module("kitaru.adapters.google_adk")

    assert module.KitaruADKRunner.__name__ == "KitaruADKRunner"
    assert "KitaruADKPlugin" in module.__all__
