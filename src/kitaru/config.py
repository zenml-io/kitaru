"""Configuration and connection management.

``kitaru.configure()`` will eventually set project-level runtime defaults.
``kitaru.connect()`` already establishes a connection to a Kitaru server
(which is a ZenML server under the hood).

Configuration precedence (highest to lowest):
1. Invocation-time overrides
2. Decorator defaults
3. ``kitaru.configure()``
4. Environment variables
5. ``pyproject.toml`` under ``[tool.kitaru]``
6. Global user config
7. Built-in defaults

Phase 7b adds global log-store configuration helpers used by
``kitaru log-store set/show/reset``.
"""

from __future__ import annotations

import importlib
import logging
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal
from unittest.mock import patch
from urllib.parse import urlparse

import click
from pydantic import BaseModel, ValidationError, field_validator
from zenml.cli.login import connect_to_pro_server as _zenml_connect_to_pro_server
from zenml.cli.login import connect_to_server as _zenml_connect_to_server
from zenml.cli.login import is_pro_server as _zenml_is_pro_server
from zenml.config.global_config import GlobalConfiguration
from zenml.exceptions import AuthorizationException
from zenml.utils import io_utils, yaml_utils

from kitaru.runtime import _not_implemented

zenml_cli_utils = importlib.import_module("zenml.cli.utils")

_DEFAULT_LOG_STORE_BACKEND = "artifact-store"
_KITARU_GLOBAL_CONFIG_FILENAME = "kitaru.yaml"
_LOG_STORE_SOURCE_DEFAULT = "default"
_LOG_STORE_SOURCE_ENVIRONMENT = "environment"
_LOG_STORE_SOURCE_GLOBAL_USER_CONFIG = "global user config"
_LOG_STORE_BACKEND_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]*$")

KITARU_LOG_STORE_BACKEND_ENV = "KITARU_LOG_STORE_BACKEND"
KITARU_LOG_STORE_ENDPOINT_ENV = "KITARU_LOG_STORE_ENDPOINT"
KITARU_LOG_STORE_API_KEY_ENV = "KITARU_LOG_STORE_API_KEY"


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
            raise ValueError("Log-store backend cannot be empty.")

        if not _LOG_STORE_BACKEND_PATTERN.fullmatch(normalized_value):
            raise ValueError(
                "Invalid log-store backend. Use lowercase letters, numbers, "
                "dots, underscores, or hyphens."
            )

        return normalized_value

    @field_validator("endpoint")
    @classmethod
    def _validate_endpoint(cls, value: str) -> str:
        normalized_value = value.strip().rstrip("/")
        if not normalized_value:
            raise ValueError("Log-store endpoint cannot be empty.")

        parsed = urlparse(normalized_value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(
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
            raise ValueError("Log-store API key cannot be empty.")

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


class _KitaruGlobalConfig(BaseModel):
    """Persisted Kitaru global configuration.

    This currently stores only the optional runtime log-store override.
    """

    version: int = 1
    log_store: LogStoreOverride | None = None


def _normalize_server_url(server_url: str) -> str:
    """Validate and normalize a Kitaru server URL.

    Args:
        server_url: Candidate Kitaru server URL.

    Returns:
        The normalized server URL without a trailing slash.

    Raises:
        ValueError: If the URL is empty or is not an HTTP(S) URL.
    """
    normalized_url = server_url.strip().rstrip("/")
    if not normalized_url:
        raise ValueError("Kitaru server URL cannot be empty.")

    parsed = urlparse(normalized_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(
            "Invalid Kitaru server URL. Please use an http:// or https:// URL."
        )

    return normalized_url


def _normalize_login_target(server: str) -> str:
    """Normalize a CLI login target while preserving workspace names/IDs.

    Args:
        server: Kitaru server URL, workspace name, or workspace ID.

    Returns:
        The normalized target value.

    Raises:
        ValueError: If the target is empty or looks like an invalid URL.
    """
    normalized_target = server.strip().rstrip("/")
    if not normalized_target:
        raise ValueError("Kitaru server target cannot be empty.")

    if normalized_target.startswith(("http:", "https:")):
        return _normalize_server_url(normalized_target)

    if _looks_like_server_address_without_scheme(normalized_target):
        raise ValueError(
            "Invalid Kitaru server URL. Please use an http:// or https:// URL, "
            "or pass a managed workspace name or ID."
        )

    return normalized_target


def _is_server_url(server: str) -> bool:
    """Return whether a normalized login target is an HTTP(S) server URL."""
    parsed = urlparse(server)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _looks_like_server_address_without_scheme(target: str) -> bool:
    """Return whether a target resembles a host/URL but lacks http(s)://."""
    localhost_names = {"localhost", "127.0.0.1", "::1"}
    return (
        target in localhost_names
        or any(target.startswith(f"{name}:") for name in localhost_names)
        or "." in target
        or ":" in target
        or "/" in target
    )


@contextmanager
def _suppress_zenml_cli_messages() -> Iterator[None]:
    """Silence ZenML success/progress chatter while Kitaru reuses its helpers.

    This keeps the user-facing CLI output in Kitaru terms while still using
    ZenML's connection/authentication machinery underneath.
    """
    previous_disable_level = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        with (
            patch.object(zenml_cli_utils, "declare", return_value=None),
            patch.object(zenml_cli_utils, "success", return_value=None),
        ):
            yield
    finally:
        logging.disable(previous_disable_level)


def _kitaru_global_config_path() -> Path:
    """Return the path to Kitaru's global config file."""
    config_directory = Path(GlobalConfiguration().config_directory)
    return config_directory / _KITARU_GLOBAL_CONFIG_FILENAME


def _read_kitaru_global_config() -> _KitaruGlobalConfig:
    """Read Kitaru global config from disk.

    Returns:
        Parsed Kitaru global config.

    Raises:
        ValueError: If the config file exists but is malformed.
    """
    config_path = _kitaru_global_config_path()
    if not config_path.exists():
        return _KitaruGlobalConfig()

    config_values = yaml_utils.read_yaml(str(config_path))
    if config_values is None:
        return _KitaruGlobalConfig()

    if not isinstance(config_values, dict):
        raise ValueError(
            "The Kitaru global config file is invalid. Expected a YAML mapping at "
            f"{config_path}."
        )

    try:
        return _KitaruGlobalConfig.model_validate(config_values)
    except ValidationError as exc:
        raise ValueError(
            "The Kitaru global config file is invalid. Fix or delete "
            f"{config_path} and try again."
        ) from exc


def _write_kitaru_global_config(config: _KitaruGlobalConfig) -> None:
    """Write Kitaru global config to disk."""
    config_path = _kitaru_global_config_path()
    io_utils.create_dir_recursive_if_not_exists(str(config_path.parent))
    yaml_utils.write_yaml(
        str(config_path), config.model_dump(mode="json", exclude_none=True)
    )


def _read_log_store_env_override() -> ResolvedLogStore | None:
    """Parse an optional log-store override from environment variables.

    Returns:
        A resolved override if configured via environment variables, otherwise
        ``None``.

    Raises:
        ValueError: If the environment variables are set incompletely or with
            invalid values.
    """
    raw_backend = os.environ.get(KITARU_LOG_STORE_BACKEND_ENV)
    raw_endpoint = os.environ.get(KITARU_LOG_STORE_ENDPOINT_ENV)
    raw_api_key = os.environ.get(KITARU_LOG_STORE_API_KEY_ENV)

    if raw_backend is None and raw_endpoint is None and raw_api_key is None:
        return None

    if raw_backend is None:
        raise ValueError(
            f"{KITARU_LOG_STORE_BACKEND_ENV} must be set when defining a log-store "
            "environment override."
        )

    normalized_backend = raw_backend.strip().lower()
    if normalized_backend == _DEFAULT_LOG_STORE_BACKEND:
        if raw_endpoint not in (None, ""):
            raise ValueError(
                f"{KITARU_LOG_STORE_ENDPOINT_ENV} must be unset when "
                f"{KITARU_LOG_STORE_BACKEND_ENV}=artifact-store."
            )
        if raw_api_key not in (None, ""):
            raise ValueError(
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
        raise ValueError(
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


def resolve_log_store() -> ResolvedLogStore:
    """Resolve the effective runtime log-store backend.

    Resolution order (highest to lowest):
    1. Environment variables
    2. Kitaru global user config
    3. Built-in default (artifact store)

    Returns:
        The effective log-store configuration.

    Raises:
        ValueError: If persisted or environment config is malformed.
    """
    env_override = _read_log_store_env_override()
    if env_override is not None:
        return env_override

    global_config = _read_kitaru_global_config()
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


def set_global_log_store(
    backend: str,
    *,
    endpoint: str,
    api_key: str | None = None,
) -> ResolvedLogStore:
    """Persist a global log-store override backend.

    Args:
        backend: External runtime log backend name (for example ``datadog``).
        endpoint: HTTP(S) endpoint for the log backend.
        api_key: Optional API key or secret placeholder.

    Returns:
        The effective resolved log-store configuration after persisting.

    Raises:
        ValueError: If validation fails.
    """
    if backend.strip().lower() == _DEFAULT_LOG_STORE_BACKEND:
        raise ValueError(
            "The artifact-store backend is already the default. Use "
            "`kitaru log-store reset` to return to defaults."
        )

    _write_kitaru_global_config(
        _KitaruGlobalConfig(
            log_store=LogStoreOverride(
                backend=backend,
                endpoint=endpoint,
                api_key=api_key,
            )
        )
    )

    return resolve_log_store()


def reset_global_log_store() -> ResolvedLogStore:
    """Clear the persisted global log-store override.

    Returns:
        The effective resolved log-store configuration after clearing.

    Raises:
        ValueError: If persisted or environment config is malformed.
    """
    _write_kitaru_global_config(_KitaruGlobalConfig())

    return resolve_log_store()


def _login_to_server_target(
    server: str,
    *,
    api_key: str | None = None,
    refresh: bool = False,
    project: str | None = None,
    verify_ssl: bool | str = True,
    cloud_api_url: str | None = None,
) -> None:
    """Connect to a Kitaru server URL or managed workspace target.

    Args:
        server: Kitaru server URL, workspace name, or workspace ID.
        api_key: API key used to authenticate with the server.
        refresh: Force a fresh authentication flow.
        project: Project name or ID to activate after connecting.
        verify_ssl: TLS verification mode or CA bundle path.
        cloud_api_url: Optional managed-cloud API URL used for staging or
            custom control planes.

    Raises:
        RuntimeError: If the underlying ZenML login flow fails.
        ValueError: If the login target is malformed.
    """
    normalized_target = _normalize_login_target(server)

    try:
        with _suppress_zenml_cli_messages():
            if _is_server_url(normalized_target):
                if cloud_api_url:
                    _zenml_connect_to_pro_server(
                        pro_server=normalized_target,
                        api_key=api_key,
                        refresh=refresh,
                        pro_api_url=cloud_api_url,
                        verify_ssl=verify_ssl,
                        project=project,
                    )
                    return

                server_is_pro, detected_cloud_api_url = _zenml_is_pro_server(
                    normalized_target
                )
                if server_is_pro:
                    _zenml_connect_to_pro_server(
                        pro_server=normalized_target,
                        api_key=api_key,
                        refresh=refresh,
                        pro_api_url=detected_cloud_api_url,
                        verify_ssl=verify_ssl,
                        project=project,
                    )
                    return

                _zenml_connect_to_server(
                    url=normalized_target,
                    api_key=api_key,
                    verify_ssl=verify_ssl,
                    refresh=refresh,
                    project=project,
                )
                return

            _zenml_connect_to_pro_server(
                pro_server=normalized_target,
                api_key=api_key,
                refresh=refresh,
                pro_api_url=cloud_api_url,
                verify_ssl=verify_ssl,
                project=project,
            )
    except click.ClickException as exc:
        raise RuntimeError(exc.format_message()) from exc
    except AuthorizationException as exc:
        raise RuntimeError(str(exc)) from exc
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def configure(**kwargs: Any) -> None:
    """Set project-level runtime defaults.

    Args:
        **kwargs: Configuration key-value pairs (e.g. ``cache=False``).
    """
    _not_implemented("configure")


def connect(
    server_url: str,
    *,
    api_key: str | None = None,
    refresh: bool = False,
    project: str | None = None,
    no_verify_ssl: bool = False,
    ssl_ca_cert: str | None = None,
    cloud_api_url: str | None = None,
) -> None:
    """Connect to a Kitaru server.

    Under the hood, this connects to a ZenML server and stores the resolved
    connection/auth state in ZenML's global user configuration.

    Args:
        server_url: URL of the Kitaru server.
        api_key: API key used to authenticate with the server.
        refresh: Force a fresh authentication flow.
        project: Project name or ID to activate after connecting.
        no_verify_ssl: Disable TLS certificate verification.
        ssl_ca_cert: Path to a CA bundle used to verify the server.
        cloud_api_url: Optional managed-cloud API URL used when the server URL
            points at a managed Kitaru deployment or staging environment.

    Raises:
        ValueError: If the server URL is invalid.
        RuntimeError: If the underlying ZenML connection flow fails.
    """
    normalized_url = _normalize_server_url(server_url)
    verify_ssl: bool | str = (
        ssl_ca_cert if ssl_ca_cert is not None else not no_verify_ssl
    )
    _login_to_server_target(
        normalized_url,
        api_key=api_key,
        refresh=refresh,
        project=project,
        verify_ssl=verify_ssl,
        cloud_api_url=cloud_api_url,
    )


def login_to_server(
    server: str,
    *,
    api_key: str | None = None,
    refresh: bool = False,
    project: str | None = None,
    no_verify_ssl: bool = False,
    ssl_ca_cert: str | None = None,
    cloud_api_url: str | None = None,
) -> None:
    """Connect to a Kitaru server URL or managed workspace target.

    Args:
        server: Kitaru server URL, workspace name, or workspace ID.
        api_key: API key used to authenticate with the server.
        refresh: Force a fresh authentication flow.
        project: Project name or ID to activate after connecting.
        no_verify_ssl: Disable TLS certificate verification.
        ssl_ca_cert: Path to a CA bundle used to verify the server.
        cloud_api_url: Optional managed-cloud API URL used when connecting to
            staging or another non-default control plane.
    """
    verify_ssl: bool | str = (
        ssl_ca_cert if ssl_ca_cert is not None else not no_verify_ssl
    )
    _login_to_server_target(
        server,
        api_key=api_key,
        refresh=refresh,
        project=project,
        verify_ssl=verify_ssl,
        cloud_api_url=cloud_api_url,
    )
