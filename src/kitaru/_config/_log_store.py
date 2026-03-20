"""Log-store and persisted global config helpers."""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import click
from pydantic import BaseModel, field_validator
from zenml.client import Client

from kitaru._config import _core as _config_core
from kitaru._config import _env as _config_env
from kitaru._config._models import ModelRegistryConfig
from kitaru._env import (
    KITARU_ANALYTICS_OPT_IN_ENV,
    KITARU_AUTH_TOKEN_ENV,
    KITARU_DEBUG_ENV,
    KITARU_PROJECT_ENV,
    KITARU_SERVER_URL_ENV,
    ZENML_CONFIG_PATH_ENV,
)
from kitaru.errors import KitaruUsageError

_DEFAULT_LOG_STORE_BACKEND = "artifact-store"
_LOG_STORE_SOURCE_DEFAULT = "default"
_LOG_STORE_SOURCE_ENVIRONMENT = "environment"
_LOG_STORE_SOURCE_GLOBAL_USER_CONFIG = "global user config"
_LOG_STORE_BACKEND_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_KITARU_GLOBAL_CONFIG_FILENAME = _config_core._KITARU_GLOBAL_CONFIG_FILENAME

KITARU_LOG_STORE_BACKEND_ENV = _config_env.KITARU_LOG_STORE_BACKEND_ENV
KITARU_LOG_STORE_ENDPOINT_ENV = _config_env.KITARU_LOG_STORE_ENDPOINT_ENV
KITARU_LOG_STORE_API_KEY_ENV = _config_env.KITARU_LOG_STORE_API_KEY_ENV
KITARU_STACK_ENV = _config_env.KITARU_STACK_ENV
KITARU_CACHE_ENV = _config_env.KITARU_CACHE_ENV
KITARU_RETRIES_ENV = _config_env.KITARU_RETRIES_ENV
KITARU_IMAGE_ENV = _config_env.KITARU_IMAGE_ENV
KITARU_DEFAULT_MODEL_ENV = _config_env.KITARU_DEFAULT_MODEL_ENV
KITARU_CONFIG_PATH_ENV = _config_env.KITARU_CONFIG_PATH_ENV

ActiveEnvironmentVariable = _config_core.ActiveEnvironmentVariable


class LogStoreOverride(BaseModel):
    """Global log-store override values for non-default backends."""

    backend: str
    endpoint: str
    api_key: str | None = None

    @field_validator("backend")
    @classmethod
    def _validate_backend(cls, value: str) -> str:
        normalized_value = value.strip().lower()
        if not normalized_value:
            raise KitaruUsageError("Log-store backend cannot be empty.")

        if not _LOG_STORE_BACKEND_PATTERN.fullmatch(normalized_value):
            raise KitaruUsageError(
                "Invalid log-store backend. Use lowercase letters, numbers, "
                "dots, underscores, or hyphens."
            )

        return normalized_value

    @field_validator("endpoint")
    @classmethod
    def _validate_endpoint(cls, value: str) -> str:
        normalized_value = value.strip().rstrip("/")
        if not normalized_value:
            raise KitaruUsageError("Log-store endpoint cannot be empty.")

        parsed = urlparse(normalized_value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise KitaruUsageError(
                "Invalid log-store endpoint. Please use an http:// or https:// URL."
            )

        return normalized_value

    @field_validator("api_key")
    @classmethod
    def _validate_api_key(cls, value: str | None) -> str | None:
        if value is None:
            return None

        normalized_value = value.strip()
        if not normalized_value:
            raise KitaruUsageError("Log-store API key cannot be empty.")

        return normalized_value


class ResolvedLogStore(BaseModel):
    """Effective log-store configuration after applying precedence rules."""

    backend: str
    endpoint: str | None = None
    api_key: str | None = None
    source: Literal[
        "default",
        "environment",
        "global user config",
    ]


class ActiveStackLogStore(BaseModel):
    """Active stack log-store backend resolved from the current ZenML stack."""

    backend: str
    endpoint: str | None = None
    stack_name: str | None = None


class _KitaruGlobalConfig(BaseModel):
    """Persisted Kitaru global configuration."""

    version: int = 1
    log_store: LogStoreOverride | None = None
    model_registry: ModelRegistryConfig | None = None


def _kitaru_config_dir() -> Path:
    """Return the Kitaru-owned global config directory."""
    return _config_core._kitaru_config_dir_impl(
        config_path_env_name=KITARU_CONFIG_PATH_ENV,
        app_dir_getter=click.get_app_dir,
        fallback_config_path_env_name=ZENML_CONFIG_PATH_ENV,
    )


def _kitaru_global_config_path() -> Path:
    """Return the path to Kitaru's global config file."""
    return _config_core._kitaru_global_config_path_impl(
        config_dir_getter=_kitaru_config_dir,
        filename=_KITARU_GLOBAL_CONFIG_FILENAME,
    )


def _parse_kitaru_config_file(config_path: Path) -> _KitaruGlobalConfig | None:
    """Parse a Kitaru global config file, returning ``None`` if absent."""
    return _config_core._parse_kitaru_config_file(
        config_path,
        global_config_model=_KitaruGlobalConfig,
    )


def _read_kitaru_global_config() -> _KitaruGlobalConfig:
    """Read Kitaru global config from disk."""
    return _config_core._read_kitaru_global_config_impl(
        config_path_getter=_kitaru_global_config_path,
        global_config_model=_KitaruGlobalConfig,
    )


def _write_kitaru_global_config(config: _KitaruGlobalConfig) -> None:
    """Write Kitaru global config to disk."""
    _config_core._write_kitaru_global_config_impl(
        config,
        config_path_getter=_kitaru_global_config_path,
    )


def _read_kitaru_global_config_for_update() -> _KitaruGlobalConfig:
    """Read global config for mutation, recovering from malformed files."""
    return _config_core._read_kitaru_global_config_for_update_impl(
        reader=_read_kitaru_global_config,
        global_config_model=_KitaruGlobalConfig,
    )


def _update_kitaru_global_config(
    mutator: Callable[[_KitaruGlobalConfig], None],
) -> _KitaruGlobalConfig:
    """Apply an in-place mutation and persist the resulting global config."""
    return _config_core._update_kitaru_global_config_impl(
        mutator,
        read_for_update=_read_kitaru_global_config_for_update,
        write=_write_kitaru_global_config,
    )


def _resolved_log_store_from_override(
    override: LogStoreOverride,
    *,
    source: Literal[
        "environment",
        "global user config",
    ],
) -> ResolvedLogStore:
    """Convert a persisted/env override into a resolved log-store view."""
    return ResolvedLogStore(
        backend=override.backend,
        endpoint=override.endpoint,
        api_key=override.api_key,
        source=source,
    )


def _read_log_store_env_override(
    *,
    environ: Mapping[str, str] | None = None,
) -> ResolvedLogStore | None:
    """Parse an optional log-store override from environment variables."""
    env = os.environ if environ is None else environ
    raw_backend = env.get(KITARU_LOG_STORE_BACKEND_ENV)
    raw_endpoint = env.get(KITARU_LOG_STORE_ENDPOINT_ENV)
    raw_api_key = env.get(KITARU_LOG_STORE_API_KEY_ENV)

    if raw_backend is None and raw_endpoint is None and raw_api_key is None:
        return None

    if raw_backend is None:
        raise KitaruUsageError(
            f"{KITARU_LOG_STORE_BACKEND_ENV} must be set when defining a log-store "
            "environment override."
        )

    normalized_backend = raw_backend.strip().lower()
    if normalized_backend == _DEFAULT_LOG_STORE_BACKEND:
        if raw_endpoint not in (None, ""):
            raise KitaruUsageError(
                f"{KITARU_LOG_STORE_ENDPOINT_ENV} must be unset when "
                f"{KITARU_LOG_STORE_BACKEND_ENV}=artifact-store."
            )
        if raw_api_key not in (None, ""):
            raise KitaruUsageError(
                f"{KITARU_LOG_STORE_API_KEY_ENV} must be unset when "
                f"{KITARU_LOG_STORE_BACKEND_ENV}=artifact-store."
            )

        return ResolvedLogStore(
            backend=_DEFAULT_LOG_STORE_BACKEND,
            endpoint=None,
            api_key=None,
            source=_LOG_STORE_SOURCE_ENVIRONMENT,
        )

    if raw_endpoint is None:
        raise KitaruUsageError(
            f"{KITARU_LOG_STORE_ENDPOINT_ENV} must be set when "
            f"{KITARU_LOG_STORE_BACKEND_ENV} is configured."
        )

    override = LogStoreOverride(
        backend=normalized_backend,
        endpoint=raw_endpoint,
        api_key=raw_api_key,
    )
    return _resolved_log_store_from_override(
        override,
        source=_LOG_STORE_SOURCE_ENVIRONMENT,
    )


def resolve_log_store(
    *,
    read_log_store_env_override: Callable[[], ResolvedLogStore | None] = (
        _read_log_store_env_override
    ),
    read_global_config: Callable[[], _KitaruGlobalConfig] = _read_kitaru_global_config,
) -> ResolvedLogStore:
    """Resolve the effective runtime log-store backend."""
    env_override = read_log_store_env_override()
    if env_override is not None:
        return env_override

    global_config = read_global_config()
    if global_config.log_store is not None:
        return _resolved_log_store_from_override(
            global_config.log_store,
            source=_LOG_STORE_SOURCE_GLOBAL_USER_CONFIG,
        )

    return ResolvedLogStore(
        backend=_DEFAULT_LOG_STORE_BACKEND,
        endpoint=None,
        api_key=None,
        source=_LOG_STORE_SOURCE_DEFAULT,
    )


def _normalize_log_store_backend_name(raw_backend: str | None) -> str:
    """Normalize backend identifiers for user-facing comparisons."""
    if raw_backend is None:
        return "unknown"

    normalized = raw_backend.strip().lower().replace("_", "-")
    if not normalized:
        return "unknown"

    if normalized in {"artifact", "artifact-store", "artifactstore"}:
        return "artifact-store"
    if normalized in {"datadog"}:
        return "datadog"
    if normalized in {"otel", "otlp", "open-telemetry", "open telemetry"}:
        return "otel"
    return normalized


def _extract_log_store_endpoint(log_store: Any) -> str | None:
    """Best-effort extraction of an endpoint from a log-store component."""
    config = getattr(log_store, "config", None)
    if config is None:
        return None

    endpoint = getattr(config, "endpoint", None)
    if not isinstance(endpoint, str):
        return None

    normalized = endpoint.strip().rstrip("/")
    if not normalized:
        return None
    return normalized


def active_stack_log_store(
    *,
    client_factory: Callable[[], Any] = Client,
    normalize_log_store_backend_name: Callable[[str | None], str] = (
        _normalize_log_store_backend_name
    ),
    extract_log_store_endpoint: Callable[[Any], str | None] = (
        _extract_log_store_endpoint
    ),
) -> ActiveStackLogStore | None:
    """Return the runtime log-store backend from the active stack."""
    try:
        client = client_factory()
        active_stack = client.active_stack
        active_stack_model = client.active_stack_model
        log_store = active_stack.log_store
    except Exception:
        return None

    flavor = getattr(log_store, "flavor", None)
    raw_backend = flavor if isinstance(flavor, str) else log_store.__class__.__name__

    stack_name = getattr(active_stack_model, "name", None)
    if not isinstance(stack_name, str):
        stack_name = None

    return ActiveStackLogStore(
        backend=normalize_log_store_backend_name(raw_backend),
        endpoint=extract_log_store_endpoint(log_store),
        stack_name=stack_name,
    )


def set_global_log_store(
    backend: str,
    *,
    endpoint: str,
    api_key: str | None = None,
    update_global_config: Callable[
        [Callable[[_KitaruGlobalConfig], None]], _KitaruGlobalConfig
    ] = _update_kitaru_global_config,
    resolve_log_store_fn: Callable[[], ResolvedLogStore] = resolve_log_store,
) -> ResolvedLogStore:
    """Persist a global log-store override backend."""
    if backend.strip().lower() == _DEFAULT_LOG_STORE_BACKEND:
        raise KitaruUsageError(
            "The artifact-store backend is already the default. Use "
            "`kitaru log-store reset` to return to defaults."
        )

    def _mutate(global_config: _KitaruGlobalConfig) -> None:
        global_config.log_store = LogStoreOverride(
            backend=backend,
            endpoint=endpoint,
            api_key=api_key,
        )

    update_global_config(_mutate)

    return resolve_log_store_fn()


def reset_global_log_store(
    *,
    update_global_config: Callable[
        [Callable[[_KitaruGlobalConfig], None]], _KitaruGlobalConfig
    ] = _update_kitaru_global_config,
    resolve_log_store_fn: Callable[[], ResolvedLogStore] = resolve_log_store,
) -> ResolvedLogStore:
    """Clear the persisted global log-store override."""

    def _mutate(global_config: _KitaruGlobalConfig) -> None:
        global_config.log_store = None

    update_global_config(_mutate)

    return resolve_log_store_fn()


def _mask_environment_value(name: str, value: str) -> str:
    """Mask secret-like environment values for status surfaces."""
    if name not in {KITARU_AUTH_TOKEN_ENV, KITARU_LOG_STORE_API_KEY_ENV}:
        return value

    if len(value) >= 8:
        return f"{value[:8]}***"
    if len(value) >= 6:
        return f"{value[:6]}***"
    return "***"


def list_active_kitaru_environment_variables(
    *,
    environ: Mapping[str, str] | None = None,
    mask_environment_value: Callable[[str, str], str] = _mask_environment_value,
) -> list[ActiveEnvironmentVariable]:
    """Return the active public Kitaru environment variables in stable order."""
    env = os.environ if environ is None else environ
    ordered_env_vars = (
        KITARU_SERVER_URL_ENV,
        KITARU_AUTH_TOKEN_ENV,
        KITARU_PROJECT_ENV,
        "KITARU_RUNNER",
        KITARU_STACK_ENV,
        KITARU_CACHE_ENV,
        KITARU_RETRIES_ENV,
        KITARU_IMAGE_ENV,
        KITARU_LOG_STORE_BACKEND_ENV,
        KITARU_LOG_STORE_ENDPOINT_ENV,
        KITARU_LOG_STORE_API_KEY_ENV,
        KITARU_DEFAULT_MODEL_ENV,
        KITARU_CONFIG_PATH_ENV,
        KITARU_DEBUG_ENV,
        KITARU_ANALYTICS_OPT_IN_ENV,
    )

    active: list[ActiveEnvironmentVariable] = []
    for env_name in ordered_env_vars:
        raw_value = env.get(env_name)
        if raw_value is None:
            continue
        active.append(
            ActiveEnvironmentVariable(
                name=env_name,
                value=mask_environment_value(env_name, raw_value),
            )
        )
    return active
