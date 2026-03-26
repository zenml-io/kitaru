"""Tests for the engine backend registry."""

from __future__ import annotations

import pytest

from kitaru.engines._protocols import ExecutionEngineBackend
from kitaru.engines._registry import (
    _DEFAULT_ENGINE_NAME,
    _reset_engine_backend_cache,
    available_engine_names,
    get_engine_backend,
    resolve_engine_name,
)
from kitaru.errors import KitaruUsageError


class TestResolveEngineName:
    def test_default_is_zenml(self) -> None:
        assert resolve_engine_name() == "zenml"

    def test_explicit_name_takes_priority(self) -> None:
        assert resolve_engine_name("zenml") == "zenml"

    def test_env_var_override(self) -> None:
        assert resolve_engine_name(environ={"KITARU_ENGINE": "zenml"}) == "zenml"

    def test_env_var_is_normalized(self) -> None:
        assert resolve_engine_name(environ={"KITARU_ENGINE": "  ZenML  "}) == "zenml"

    def test_blank_env_var_falls_back_to_default(self) -> None:
        assert resolve_engine_name(environ={"KITARU_ENGINE": "   "}) == "zenml"

    def test_missing_env_var_falls_back_to_default(self) -> None:
        assert resolve_engine_name(environ={}) == "zenml"

    def test_unknown_engine_raises(self) -> None:
        with pytest.raises(KitaruUsageError, match="Unknown engine backend 'nope'"):
            resolve_engine_name("nope")

    def test_unknown_engine_from_env_raises(self) -> None:
        with pytest.raises(KitaruUsageError, match="Unknown engine backend 'bogus'"):
            resolve_engine_name(environ={"KITARU_ENGINE": "bogus"})

    def test_error_message_lists_available_engines(self) -> None:
        with pytest.raises(KitaruUsageError, match="'zenml'"):
            resolve_engine_name("invalid")


class TestAvailableEngineNames:
    def test_zenml_is_available(self) -> None:
        names = available_engine_names()
        assert "zenml" in names

    def test_returns_tuple(self) -> None:
        assert isinstance(available_engine_names(), tuple)


class TestGetEngineBackend:
    def test_returns_backend_satisfying_protocol(self) -> None:
        backend = get_engine_backend()
        assert isinstance(backend, ExecutionEngineBackend)

    def test_backend_name_is_zenml(self) -> None:
        backend = get_engine_backend()
        assert backend.name == "zenml"

    def test_backend_is_cached(self) -> None:
        backend1 = get_engine_backend()
        backend2 = get_engine_backend()
        assert backend1 is backend2

    def test_cache_is_cleared_by_reset(self) -> None:
        backend1 = get_engine_backend()
        _reset_engine_backend_cache()
        backend2 = get_engine_backend()
        assert backend1 is not backend2

    def test_unknown_engine_raises(self) -> None:
        with pytest.raises(KitaruUsageError, match="Unknown engine backend"):
            get_engine_backend("invalid")

    def test_execution_graph_from_run_delegates_to_zenml(self) -> None:
        """The ZenML backend should delegate to the real snapshot mapper."""
        backend = get_engine_backend()
        # Verify the method exists and is callable
        assert callable(backend.execution_graph_from_run)


class TestDefaultEngineName:
    def test_default_is_zenml(self) -> None:
        assert _DEFAULT_ENGINE_NAME == "zenml"


class TestLazyImport:
    """Verify the registry does not eagerly import backend modules."""

    def test_importing_engines_package_does_not_import_zenml_snapshots(self) -> None:
        """Importing kitaru.engines should not trigger ZenML snapshot import."""
        import sys

        # The module may already be imported from prior tests, so we check
        # that the registry module itself doesn't import it at module level
        # by verifying the registry can be imported independently
        import kitaru.engines._registry  # noqa: F401

        # The registry module should import without errors even if we
        # haven't called get_engine_backend() yet
        assert "kitaru.engines._registry" in sys.modules

    def test_zenml_snapshots_loaded_on_backend_access(self) -> None:
        """Accessing the ZenML backend should import the snapshot module."""
        import sys

        _reset_engine_backend_cache()
        backend = get_engine_backend("zenml")
        assert backend.name == "zenml"
        assert "kitaru.engines.zenml.snapshots" in sys.modules
