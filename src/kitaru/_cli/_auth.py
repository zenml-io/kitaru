"""Authentication helper CLI commands."""

from __future__ import annotations

from typing import Any

from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.analytics import AnalyticsEvent, track
from kitaru.cli_output import CLIOutputFormat
from kitaru.config import resolve_connection_config
from kitaru.errors import KitaruBackendError, KitaruUsageError

from . import auth_app
from ._helpers import (
    OutputFormatOption,
    _emit_json_item,
    _exit_with_error,
    _facade_module,
    _resolve_output_format,
)


def _bearer_token_from_header(header: str | None) -> str | None:
    """Extract a bearer token from an Authorization header value."""
    if not header:
        return None
    prefix = "Bearer "
    if not header.startswith(prefix):
        return None
    token = header.removeprefix(prefix).strip()
    return token or None


def _active_server_access_token() -> str:
    """Return a short-lived bearer token for the active Kitaru server."""
    resolve_connection_config(validate_for_use=True)
    try:
        client = _facade_module().Client()
        store = client.zen_store
    except Exception as exc:
        raise KitaruUsageError(
            "Could not load the active Kitaru server connection. Run `kitaru login` "
            "or set KITARU_SERVER_URL, KITARU_AUTH_TOKEN, and KITARU_PROJECT."
        ) from exc

    token_getter = getattr(store, "get_or_generate_api_token", None)
    if callable(token_getter):
        try:
            token = token_getter()
        except Exception as exc:
            raise KitaruBackendError(
                "Could not create a server access token for the active Kitaru "
                f"server: {exc}"
            ) from exc
        if isinstance(token, str) and token.strip():
            return token.strip()

    authenticate = getattr(store, "authenticate", None)
    session = getattr(store, "session", None)
    if callable(authenticate):
        try:
            authenticate()
        except Exception as exc:
            raise KitaruBackendError(
                f"Could not authenticate to the active Kitaru server: {exc}"
            ) from exc
        session = getattr(store, "session", None)

    headers: Any = getattr(session, "headers", None)
    header = headers.get("Authorization") if hasattr(headers, "get") else None
    token = _bearer_token_from_header(header if isinstance(header, str) else None)
    if token:
        return token

    raise KitaruBackendError(
        "The active Kitaru server connection did not provide a bearer token. "
        "Run `kitaru login` again or check KITARU_SERVER_URL, KITARU_AUTH_TOKEN, "
        "and KITARU_PROJECT."
    )


@auth_app.command
def token(output: OutputFormatOption = "text") -> None:
    """Print a short-lived bearer token for the active Kitaru server."""
    command = "auth.token"
    output_format = _resolve_output_format(output)

    access_token = run_with_cli_error_boundary(
        _active_server_access_token,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )
    track(AnalyticsEvent.AUTH_TOKEN_PRINTED, {"command": command})

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, {"token": access_token}, output=output_format)
        return

    print(access_token)
