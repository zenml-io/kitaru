"""Lazy engine backend registry.

Resolves the active backend from ``KITARU_ENGINE`` (default ``"zenml"``),
lazily imports the backend implementation on first access, and caches the
result for the lifetime of the process.
"""

from __future__ import annotations

import importlib
import os
import threading
from collections.abc import Callable, Mapping
from typing import Any

from kitaru._env import KITARU_ENGINE_ENV
from kitaru.engines._protocols import ExecutionEngineBackend
from kitaru.engines._types import ExecutionGraphSnapshot
from kitaru.errors import KitaruRuntimeError, KitaruUsageError

_DEFAULT_ENGINE_NAME = "zenml"


class _SnapshotOnlyBackend:
    """Minimal backend wrapper that delegates to an existing mapper function."""

    __slots__ = ("_mapper", "_name")

    def __init__(
        self,
        name: str,
        mapper: Callable[[Any], ExecutionGraphSnapshot],
    ) -> None:
        self._name = name
        self._mapper = mapper

    @property
    def name(self) -> str:
        return self._name

    def execution_graph_from_run(self, run: Any) -> ExecutionGraphSnapshot:
        return self._mapper(run)


def _load_zenml_backend() -> ExecutionEngineBackend:
    """Lazily import the ZenML snapshot mapper and wrap it."""
    try:
        module = importlib.import_module("kitaru.engines.zenml.snapshots")
    except ImportError as exc:
        raise KitaruRuntimeError(
            "Failed to load the ZenML engine backend. "
            "Ensure kitaru is installed with ZenML support."
        ) from exc

    mapper = getattr(module, "execution_graph_from_run", None)
    if mapper is None:
        raise KitaruRuntimeError(
            "ZenML engine module is missing 'execution_graph_from_run'. "
            "This is a Kitaru internal error — please report it."
        )

    return _SnapshotOnlyBackend(name="zenml", mapper=mapper)


_BACKEND_LOADERS: dict[str, Callable[[], ExecutionEngineBackend]] = {
    "zenml": _load_zenml_backend,
}

_BACKEND_CACHE: dict[str, ExecutionEngineBackend] = {}
_CACHE_LOCK = threading.RLock()


def available_engine_names() -> tuple[str, ...]:
    """Return the names of all registered engine backends."""
    return tuple(sorted(_BACKEND_LOADERS))


def resolve_engine_name(
    name: str | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    """Resolve the active engine name from an explicit value or environment.

    Resolution order:
    1. Explicit ``name`` argument (if provided)
    2. ``KITARU_ENGINE`` environment variable
    3. Default: ``"zenml"``

    Raises ``KitaruUsageError`` if the resolved name is not registered.
    """
    if name is None:
        env = environ if environ is not None else os.environ
        raw = env.get(KITARU_ENGINE_ENV, "")
        name = raw.strip().lower() or _DEFAULT_ENGINE_NAME
    else:
        name = name.strip().lower()

    if name not in _BACKEND_LOADERS:
        available = ", ".join(f"'{n}'" for n in available_engine_names())
        raise KitaruUsageError(
            f"Unknown engine backend '{name}'. "
            f"Set {KITARU_ENGINE_ENV} to one of: {available}."
        )

    return name


def get_engine_backend(
    name: str | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> ExecutionEngineBackend:
    """Return the engine backend for the given (or resolved) engine name.

    The backend is lazily imported on first access and cached for subsequent
    calls with the same engine name.
    """
    resolved = resolve_engine_name(name, environ=environ)

    with _CACHE_LOCK:
        if resolved in _BACKEND_CACHE:
            return _BACKEND_CACHE[resolved]

        loader = _BACKEND_LOADERS[resolved]
        backend = loader()
        _BACKEND_CACHE[resolved] = backend
        return backend


def _reset_engine_backend_cache() -> None:
    """Clear the backend cache. Intended for test isolation only."""
    with _CACHE_LOCK:
        _BACKEND_CACHE.clear()
