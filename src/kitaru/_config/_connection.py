"""Connection and login helpers for Kitaru configuration."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlparse

import click
from zenml.exceptions import AuthorizationException

from kitaru.errors import KitaruBackendError, KitaruUsageError


def _normalize_server_url(server_url: str) -> str:
    """Validate and normalize a Kitaru server URL."""
    normalized_url = server_url.strip().rstrip("/")
    if not normalized_url:
        raise KitaruUsageError("Kitaru server URL cannot be empty.")

    parsed = urlparse(normalized_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise KitaruUsageError(
            "Invalid Kitaru server URL. Please use an http:// or https:// URL."
        )

    return normalized_url


def _normalize_login_target(server: str) -> str:
    """Normalize a CLI login target while preserving workspace names/IDs."""
    normalized_target = server.strip().rstrip("/")
    if not normalized_target:
        raise KitaruUsageError("Kitaru server target cannot be empty.")

    if normalized_target.startswith(("http:", "https:")):
        return _normalize_server_url(normalized_target)

    if _looks_like_server_address_without_scheme(normalized_target):
        raise KitaruUsageError(
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


def _noop_zenml_cli_message(*args: Any, **kwargs: Any) -> None:
    """Discard ZenML CLI progress/success messages while Kitaru owns UX."""
    del args, kwargs


@contextmanager
def _suppress_zenml_cli_messages_impl(
    *,
    zenml_cli_utils_module: Any,
    logging_module: Any = logging,
) -> Iterator[None]:
    """Silence ZenML success/progress chatter while Kitaru reuses its helpers."""
    cli_utils = zenml_cli_utils_module
    original_declare = cli_utils.declare
    original_success = cli_utils.success
    previous_disable_level = logging_module.root.manager.disable
    try:
        logging_module.disable(logging_module.CRITICAL)
        cli_utils.declare = _noop_zenml_cli_message
        cli_utils.success = _noop_zenml_cli_message
        yield
    finally:
        cli_utils.declare = original_declare
        cli_utils.success = original_success
        logging_module.disable(previous_disable_level)


def _login_to_server_target_impl(
    server: str,
    *,
    api_key: str | None = None,
    refresh: bool = False,
    project: str | None = None,
    verify_ssl: bool | str = True,
    cloud_api_url: str | None = None,
    suppress_zenml_cli_messages: Callable[[], Any],
    zenml_connect_to_server: Callable[..., None],
    zenml_connect_to_pro_server: Callable[..., None],
    zenml_is_pro_server: Callable[[str], tuple[bool | None, str | None]],
) -> None:
    """Connect to a Kitaru server URL or managed workspace target."""
    normalized_target = _normalize_login_target(server)

    try:
        with suppress_zenml_cli_messages():
            if _is_server_url(normalized_target):
                if cloud_api_url:
                    zenml_connect_to_pro_server(
                        pro_server=normalized_target,
                        api_key=api_key,
                        refresh=refresh,
                        pro_api_url=cloud_api_url,
                        verify_ssl=verify_ssl,
                        project=project,
                    )
                    return

                server_is_pro, detected_cloud_api_url = zenml_is_pro_server(
                    normalized_target
                )
                if server_is_pro:
                    zenml_connect_to_pro_server(
                        pro_server=normalized_target,
                        api_key=api_key,
                        refresh=refresh,
                        pro_api_url=detected_cloud_api_url,
                        verify_ssl=verify_ssl,
                        project=project,
                    )
                    return

                zenml_connect_to_server(
                    url=normalized_target,
                    api_key=api_key,
                    verify_ssl=verify_ssl,
                    refresh=refresh,
                    project=project,
                )
                return

            zenml_connect_to_pro_server(
                pro_server=normalized_target,
                api_key=api_key,
                refresh=refresh,
                pro_api_url=cloud_api_url,
                verify_ssl=verify_ssl,
                project=project,
            )
    except click.ClickException as exc:
        raise KitaruBackendError(exc.format_message()) from exc
    except AuthorizationException as exc:
        raise KitaruBackendError(str(exc)) from exc
    except Exception as exc:
        raise KitaruBackendError(str(exc)) from exc
