"""Authentication helper CLI commands."""

from __future__ import annotations

from zenml.zen_stores.rest_zen_store import RestZenStore

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

    if not isinstance(store, RestZenStore):
        raise KitaruUsageError(
            "Minting a server bearer token requires a remote Kitaru server. "
            "Run `kitaru login <server>` or set KITARU_SERVER_URL, "
            "KITARU_AUTH_TOKEN, and KITARU_PROJECT."
        )

    try:
        return store.get_or_generate_api_token()
    except Exception as exc:
        raise KitaruBackendError(
            f"Could not create a server access token for the active Kitaru "
            f"server: {exc}"
        ) from exc


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
