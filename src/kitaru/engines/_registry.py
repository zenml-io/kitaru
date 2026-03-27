"""Lazy engine backend registry.

Resolves the active backend from ``KITARU_ENGINE`` (default ``"zenml"``),
lazily imports the backend implementation on first access, and caches the
result for the lifetime of the process.
"""

from __future__ import annotations

import importlib
import os
import threading
import warnings
from collections.abc import Callable, Mapping

from kitaru._env import KITARU_ENABLE_EXPERIMENTAL_DAPR_ENV, KITARU_ENGINE_ENV
from kitaru.engines._protocols import ExecutionEngineBackend
from kitaru.errors import KitaruRuntimeError, KitaruUsageError

_DEFAULT_ENGINE_NAME = "zenml"
_EXPERIMENTAL_ENGINES: frozenset[str] = frozenset({"dapr"})
_EXPERIMENTAL_WARNING_EMITTED: set[str] = set()


def _load_zenml_backend() -> ExecutionEngineBackend:
    """Lazily import the ZenML backend module and instantiate it."""
    try:
        module = importlib.import_module("kitaru.engines.zenml.backend")
    except ImportError as exc:
        raise KitaruRuntimeError(
            "Failed to load the ZenML engine backend. "
            "Ensure kitaru is installed with ZenML support."
        ) from exc

    backend_cls = getattr(module, "ZenMLExecutionEngineBackend", None)
    if backend_cls is None:
        raise KitaruRuntimeError(
            "ZenML engine module is missing 'ZenMLExecutionEngineBackend'. "
            "This is a Kitaru internal error — please report it."
        )

    return backend_cls()


def _load_dapr_backend() -> ExecutionEngineBackend:
    """Lazily import the Dapr backend module and instantiate it."""
    try:
        module = importlib.import_module("kitaru.engines.dapr.backend")
    except ImportError as exc:
        raise KitaruRuntimeError(
            "Failed to load the Dapr engine backend. "
            "Ensure kitaru is installed with: pip install kitaru[dapr]"
        ) from exc

    backend_cls = getattr(module, "DaprExecutionEngineBackend", None)
    if backend_cls is None:
        raise KitaruRuntimeError(
            "Dapr engine module is missing 'DaprExecutionEngineBackend'. "
            "This is a Kitaru internal error — please report it."
        )

    return backend_cls()


_BACKEND_LOADERS: dict[str, Callable[[], ExecutionEngineBackend]] = {
    "zenml": _load_zenml_backend,
    "dapr": _load_dapr_backend,
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


def _is_env_truthy(value: str | None) -> bool:
    """Return True if an env var value is truthy (1/true/t/yes/y/on)."""
    if not value:
        return False
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _enforce_experimental_opt_in(
    name: str,
    *,
    environ: Mapping[str, str] | None = None,
) -> None:
    """Require explicit opt-in for experimental engines.

    Raises ``KitaruUsageError`` if the engine is experimental and the
    opt-in env var is not set. Emits a ``UserWarning`` once per process
    when opt-in is present.
    """
    if name not in _EXPERIMENTAL_ENGINES:
        return

    env = environ if environ is not None else os.environ
    enabled = _is_env_truthy(env.get(KITARU_ENABLE_EXPERIMENTAL_DAPR_ENV))

    if not enabled:
        raise KitaruUsageError(
            f"The '{name}' execution engine is experimental and requires "
            f"explicit opt-in. Set {KITARU_ENABLE_EXPERIMENTAL_DAPR_ENV}=1 "
            "to enable it."
        )

    if name not in _EXPERIMENTAL_WARNING_EMITTED:
        _EXPERIMENTAL_WARNING_EMITTED.add(name)
        warnings.warn(
            f"The '{name}' execution engine is experimental and may change "
            "without notice. It is intended for evaluation only.",
            UserWarning,
            stacklevel=3,
        )


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

        # Gate experimental engines only on first load, not on cache hits.
        _enforce_experimental_opt_in(resolved, environ=environ)

        loader = _BACKEND_LOADERS[resolved]
        backend = loader()
        _BACKEND_CACHE[resolved] = backend
        return backend


def _reset_engine_backend_cache() -> None:
    """Clear the backend cache and warning state. Intended for test isolation."""
    with _CACHE_LOCK:
        _BACKEND_CACHE.clear()
        _EXPERIMENTAL_WARNING_EMITTED.clear()
