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

Example::

    kitaru.configure(cache=False)
    kitaru.connect("https://my-server.example.com")

Note: runtime configuration is still scaffolding. Only ``connect()`` is
implemented in this phase.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch
from urllib.parse import urlparse

import click
from zenml.cli.login import connect_to_pro_server as _zenml_connect_to_pro_server
from zenml.cli.login import connect_to_server as _zenml_connect_to_server
from zenml.cli.login import is_pro_server as _zenml_is_pro_server
from zenml.exceptions import AuthorizationException

from kitaru.runtime import _not_implemented

zenml_cli_utils = importlib.import_module("zenml.cli.utils")


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
