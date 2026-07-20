"""Authentication helper CLI commands."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any

from cyclopts import Parameter
from zenml.zen_stores.rest_zen_store import RestZenStore

from kitaru._client._models import AuthAPIKey, AuthAPIKeyWithValue, AuthServiceAccount
from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.analytics import AnalyticsEvent, track
from kitaru.cli_output import CLIOutputFormat
from kitaru.config import resolve_connection_config
from kitaru.errors import KitaruBackendError, KitaruUsageError

from . import auth_api_keys_app, auth_app, auth_service_accounts_app
from ._dependencies import cli_dependencies
from ._helpers import (
    DEFAULT_LIST_PAGE,
    DEFAULT_LIST_SIZE,
    OutputFormatOption,
    PaginationPageOption,
    PaginationSizeOption,
    _confirm_delete,
    _emit_json_item,
    _emit_json_items,
    _emit_pagination_note,
    _emit_snapshot,
    _emit_table,
    _exit_with_error,
    _format_table_timestamp,
    _format_timestamp,
    _print_success,
    _resolve_output_format,
    _validate_pagination,
)

# ---------------------------------------------------------------------------
# Shared serializers/renderers
# ---------------------------------------------------------------------------


def _datetime_payload(value: datetime | str | None) -> str | None:
    """Return a JSON-safe optional timestamp."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _service_account_payload(account: AuthServiceAccount) -> dict[str, Any]:
    """Serialize a service-account metadata DTO for CLI JSON output."""
    return {
        "service_account_id": account.service_account_id,
        "name": account.name,
        "full_name": account.full_name,
        "description": account.description,
        "active": account.active,
        "created_at": _datetime_payload(account.created_at),
        "updated_at": _datetime_payload(account.updated_at),
        "avatar_url": account.avatar_url,
    }


def _api_key_payload(api_key: AuthAPIKey) -> dict[str, Any]:
    """Serialize API-key metadata without the raw key value."""
    return {
        "api_key_id": api_key.api_key_id,
        "name": api_key.name,
        "service_account_id": api_key.service_account_id,
        "service_account_name": api_key.service_account_name,
        "description": api_key.description,
        "active": api_key.active,
        "created_at": _datetime_payload(api_key.created_at),
        "updated_at": _datetime_payload(api_key.updated_at),
        "last_login": _datetime_payload(api_key.last_login),
        "last_rotated": _datetime_payload(api_key.last_rotated),
        "retain_period_minutes": api_key.retain_period_minutes,
    }


def _api_key_value_payload(result: AuthAPIKeyWithValue) -> dict[str, Any]:
    """Serialize create/rotate output including the one-time raw key."""
    return {
        **_api_key_payload(result.api_key),
        "key": result.key,
        "local_key_activation_requested": result.local_key_activation_requested,
        "local_key_activation_succeeded": result.local_key_activation_succeeded,
        "local_key_activation_error": result.local_key_activation_error,
        "local_key_rollback_attempted": result.local_key_rollback_attempted,
        "local_key_rollback_succeeded": result.local_key_rollback_succeeded,
        "local_key_rollback_error": result.local_key_rollback_error,
        "local_key_rollback_reason": result.local_key_rollback_reason,
    }


def _service_account_rows(account: AuthServiceAccount) -> list[tuple[str, str]]:
    """Return text snapshot rows for one service account."""
    return [
        ("ID", account.service_account_id),
        ("Name", account.name),
        ("Full name", account.full_name or "-"),
        ("Description", account.description or "-"),
        ("Active", "yes" if account.active else "no"),
        ("Created", _format_timestamp(account.created_at)),
        ("Updated", _format_timestamp(account.updated_at)),
        ("Avatar URL", account.avatar_url or "-"),
    ]


def _api_key_rows(api_key: AuthAPIKey) -> list[tuple[str, str]]:
    """Return text snapshot rows for one API-key metadata record."""
    return [
        ("ID", api_key.api_key_id),
        ("Name", api_key.name),
        ("Service account", api_key.service_account_name or api_key.service_account_id),
        ("Service account ID", api_key.service_account_id or "-"),
        ("Description", api_key.description or "-"),
        ("Active", "yes" if api_key.active else "no"),
        ("Created", _format_timestamp(api_key.created_at)),
        ("Updated", _format_timestamp(api_key.updated_at)),
        ("Last login", _format_timestamp(api_key.last_login)),
        ("Last rotated", _format_timestamp(api_key.last_rotated)),
        ("Retain period", f"{api_key.retain_period_minutes} minutes"),
    ]


def _api_key_value_rows(result: AuthAPIKeyWithValue) -> list[tuple[str, str]]:
    """Return text snapshot rows for create/rotate including activation status."""
    rows = [*_api_key_rows(result.api_key), ("Key", result.key)]
    if result.local_key_activation_requested:
        activation_status = (
            "succeeded" if result.local_key_activation_succeeded else "failed"
        )
        rows.append(("Local activation", activation_status))
        if result.local_key_activation_succeeded is False:
            if (
                result.local_key_rollback_attempted
                and result.local_key_rollback_succeeded
            ):
                rollback_status = "restored previous credential"
            elif result.local_key_rollback_attempted:
                rollback_status = "failed"
            elif result.local_key_rollback_reason:
                rollback_status = "not possible"
            else:
                rollback_status = "not attempted"
            rows.append(("Credential rollback", rollback_status))
    return rows


def _api_key_value_warning(result: AuthAPIKeyWithValue) -> str:
    """Return the one-time-key warning plus any local activation warning."""
    warning = "Store this key now; it cannot be retrieved later."
    if not result.local_key_activation_error:
        return warning
    repair_hint = ""
    if result.local_key_rollback_succeeded is not True:
        repair_hint = (
            "\n\nTo repair local credentials, run "
            "`kitaru login <server-url> --api-key <key>` or restore the "
            "previous credential manually. Use the one-time key shown above "
            "for `<key>` if you want this new API key to become active locally."
        )
    return f"{warning}\n\n{result.local_key_activation_error}{repair_hint}"


def _service_account_table_rows(
    accounts: list[AuthServiceAccount],
) -> list[list[str]]:
    """Return table rows for service-account list output."""
    return [
        [
            account.service_account_id,
            account.name,
            "yes" if account.active else "no",
            _format_table_timestamp(account.created_at),
        ]
        for account in accounts
    ]


def _api_key_table_rows(api_keys: list[AuthAPIKey]) -> list[list[str]]:
    """Return table rows for API-key list output without raw key values."""
    return [
        [
            api_key.api_key_id,
            api_key.name,
            "yes" if api_key.active else "no",
            _format_table_timestamp(api_key.last_rotated),
        ]
        for api_key in api_keys
    ]


def _auth_management_client() -> Any:
    """Return the public SDK client configured for server-level auth management."""
    return cli_dependencies().auth_management_client()


# ---------------------------------------------------------------------------
# Existing short-lived server token helper
# ---------------------------------------------------------------------------


def _active_server_access_token() -> str:
    """Return a short-lived bearer token for the active Kitaru server."""
    resolve_connection_config(validate_for_use=True, require_project=False)
    try:
        client = cli_dependencies().zenml_client()
        store = client.zen_store
    except Exception as exc:
        raise KitaruUsageError(
            "Could not load the active Kitaru server connection. Run `kitaru login` "
            "or set KITARU_SERVER_URL and KITARU_AUTH_TOKEN."
        ) from exc

    if not isinstance(store, RestZenStore):
        raise KitaruUsageError(
            "Minting a server bearer token requires a remote Kitaru server. "
            "Run `kitaru login <server>` or set KITARU_SERVER_URL and "
            "KITARU_AUTH_TOKEN."
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


# ---------------------------------------------------------------------------
# Service-account commands
# ---------------------------------------------------------------------------


@auth_service_accounts_app.command(name="create")
def service_accounts_create(
    name: Annotated[str, Parameter(help="Service-account name.")],
    *,
    full_name: Annotated[
        str | None,
        Parameter(help="Optional human-readable full name."),
    ] = None,
    description: Annotated[
        str,
        Parameter(help="Optional service-account description."),
    ] = "",
    output: OutputFormatOption = "text",
) -> None:
    """Create a service account."""
    command = "auth.service-accounts.create"
    output_format = _resolve_output_format(output)

    account = run_with_cli_error_boundary(
        lambda: _auth_management_client().auth.service_accounts.create(
            name,
            full_name=full_name,
            description=description,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )
    track(
        AnalyticsEvent.AUTH_SERVICE_ACCOUNT_CREATED,
        {"command": command, "has_description": bool(description)},
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command, _service_account_payload(account), output=output_format
        )
        return

    _print_success(f"Created service account: {account.name}")
    _emit_snapshot("Service account", _service_account_rows(account))


@auth_service_accounts_app.command(name="list")
def service_accounts_list(
    *,
    active: Annotated[
        bool | None,
        Parameter(help="Filter by active state."),
    ] = None,
    name: Annotated[str | None, Parameter(help="Filter by exact name.")] = None,
    page: PaginationPageOption = DEFAULT_LIST_PAGE,
    size: PaginationSizeOption = DEFAULT_LIST_SIZE,
    output: OutputFormatOption = "text",
) -> None:
    """List service accounts."""
    command = "auth.service-accounts.list"
    output_format = _resolve_output_format(output)
    page, size = _validate_pagination(
        page=page,
        size=size,
        command=command,
        output=output_format,
    )

    accounts = run_with_cli_error_boundary(
        lambda: _auth_management_client().auth.service_accounts.list(
            active=active,
            name=name,
            page=page,
            size=size,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [_service_account_payload(account) for account in accounts],
            output=output_format,
        )
        return

    _emit_table(
        "Service accounts",
        ["ID", "Name", "Active", "Created"],
        _service_account_table_rows(accounts),
    )
    _emit_pagination_note(
        page=page,
        size=size,
        returned_count=len(accounts),
        output=output_format,
    )


@auth_service_accounts_app.command(name="show")
def service_accounts_show(
    name_or_id: Annotated[
        str,
        Parameter(help="Service-account name or ID."),
    ],
    *,
    output: OutputFormatOption = "text",
) -> None:
    """Show service-account metadata."""
    command = "auth.service-accounts.show"
    output_format = _resolve_output_format(output)

    account = run_with_cli_error_boundary(
        lambda: _auth_management_client().auth.service_accounts.get(name_or_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command, _service_account_payload(account), output=output_format
        )
        return

    _emit_snapshot("Service account", _service_account_rows(account))


@auth_service_accounts_app.command(name="update")
def service_accounts_update(
    name_or_id: Annotated[
        str,
        Parameter(help="Service-account name or ID."),
    ],
    *,
    name: Annotated[str | None, Parameter(help="New service-account name.")] = None,
    description: Annotated[
        str | None,
        Parameter(help="New service-account description."),
    ] = None,
    active: Annotated[
        bool | None,
        Parameter(help="Set whether the service account is active."),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Update service-account metadata."""
    command = "auth.service-accounts.update"
    output_format = _resolve_output_format(output)

    account = run_with_cli_error_boundary(
        lambda: _auth_management_client().auth.service_accounts.update(
            name_or_id,
            name=name,
            description=description,
            active=active,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )
    track(
        AnalyticsEvent.AUTH_SERVICE_ACCOUNT_UPDATED,
        {
            "command": command,
            "renamed": name is not None,
            "description_changed": description is not None,
            "active_changed": active is not None,
        },
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command, _service_account_payload(account), output=output_format
        )
        return

    _print_success(f"Updated service account: {account.name}")
    _emit_snapshot("Service account", _service_account_rows(account))


@auth_service_accounts_app.command(name="delete")
def service_accounts_delete(
    name_or_id: Annotated[
        str,
        Parameter(help="Service-account name or ID."),
    ],
    *,
    yes: Annotated[
        bool,
        Parameter(
            alias=["-y"],
            help="Skip confirmation prompt. Required with --output json.",
        ),
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Delete a service account."""
    command = "auth.service-accounts.delete"
    output_format = _resolve_output_format(output)
    _confirm_delete(
        command=command,
        output=output_format,
        yes=yes,
        description=f"service account `{name_or_id}`",
    )

    run_with_cli_error_boundary(
        lambda: _auth_management_client().auth.service_accounts.delete(name_or_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )
    track(AnalyticsEvent.AUTH_SERVICE_ACCOUNT_DELETED, {"command": command})
    item = {"name_or_id": name_or_id, "deleted": True}

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, item, output=output_format)
        return

    _print_success(f"Deleted service account: {name_or_id}")


# ---------------------------------------------------------------------------
# API-key commands
# ---------------------------------------------------------------------------


@auth_api_keys_app.command(name="create")
def api_keys_create(
    service_account: Annotated[
        str,
        Parameter(help="Owning service-account name or ID."),
    ],
    name: Annotated[str, Parameter(help="API-key name.")],
    *,
    description: Annotated[str, Parameter(help="Optional API-key description.")] = "",
    set_key: Annotated[
        bool,
        Parameter(help="Use this API key as the active local server credential."),
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Create a service-account API key and print its one-time value."""
    command = "auth.api-keys.create"
    output_format = _resolve_output_format(output)

    result = run_with_cli_error_boundary(
        lambda: _auth_management_client().auth.api_keys.create(
            service_account,
            name,
            description=description,
            set_key=set_key,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )
    track(
        AnalyticsEvent.AUTH_API_KEY_CREATED,
        {
            "command": command,
            "has_description": bool(description),
            "set_key": set_key,
        },
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, _api_key_value_payload(result), output=output_format)
        return

    _print_success(f"Created API key: {result.api_key.name}")
    _emit_snapshot(
        "API key",
        _api_key_value_rows(result),
        warning=_api_key_value_warning(result),
    )


@auth_api_keys_app.command(name="list")
def api_keys_list(
    service_account: Annotated[
        str,
        Parameter(help="Owning service-account name or ID."),
    ],
    *,
    active: Annotated[
        bool | None,
        Parameter(help="Filter by active state."),
    ] = None,
    name: Annotated[str | None, Parameter(help="Filter by exact name.")] = None,
    page: PaginationPageOption = DEFAULT_LIST_PAGE,
    size: PaginationSizeOption = DEFAULT_LIST_SIZE,
    output: OutputFormatOption = "text",
) -> None:
    """List API-key metadata for a service account."""
    command = "auth.api-keys.list"
    output_format = _resolve_output_format(output)
    page, size = _validate_pagination(
        page=page,
        size=size,
        command=command,
        output=output_format,
    )

    api_keys = run_with_cli_error_boundary(
        lambda: _auth_management_client().auth.api_keys.list(
            service_account,
            active=active,
            name=name,
            page=page,
            size=size,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [_api_key_payload(api_key) for api_key in api_keys],
            output=output_format,
        )
        return

    _emit_table(
        "API keys",
        ["ID", "Name", "Active", "Last rotated"],
        _api_key_table_rows(api_keys),
    )
    _emit_pagination_note(
        page=page,
        size=size,
        returned_count=len(api_keys),
        output=output_format,
    )


@auth_api_keys_app.command(name="show")
def api_keys_show(
    service_account: Annotated[
        str,
        Parameter(help="Owning service-account name or ID."),
    ],
    name_or_id: Annotated[str, Parameter(help="API-key name or ID.")],
    *,
    output: OutputFormatOption = "text",
) -> None:
    """Show API-key metadata without revealing the raw key value."""
    command = "auth.api-keys.show"
    output_format = _resolve_output_format(output)

    api_key = run_with_cli_error_boundary(
        lambda: _auth_management_client().auth.api_keys.get(
            service_account,
            name_or_id,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, _api_key_payload(api_key), output=output_format)
        return

    _emit_snapshot("API key", _api_key_rows(api_key))


@auth_api_keys_app.command(name="update")
def api_keys_update(
    service_account: Annotated[
        str,
        Parameter(help="Owning service-account name or ID."),
    ],
    name_or_id: Annotated[str, Parameter(help="API-key name or ID.")],
    *,
    name: Annotated[str | None, Parameter(help="New API-key name.")] = None,
    description: Annotated[
        str | None,
        Parameter(help="New API-key description."),
    ] = None,
    active: Annotated[
        bool | None,
        Parameter(help="Set whether the API key is active."),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Update API-key metadata without revealing the raw key value."""
    command = "auth.api-keys.update"
    output_format = _resolve_output_format(output)

    api_key = run_with_cli_error_boundary(
        lambda: _auth_management_client().auth.api_keys.update(
            service_account,
            name_or_id,
            name=name,
            description=description,
            active=active,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )
    track(
        AnalyticsEvent.AUTH_API_KEY_UPDATED,
        {
            "command": command,
            "renamed": name is not None,
            "description_changed": description is not None,
            "active_changed": active is not None,
        },
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, _api_key_payload(api_key), output=output_format)
        return

    _print_success(f"Updated API key: {api_key.name}")
    _emit_snapshot("API key", _api_key_rows(api_key))


@auth_api_keys_app.command(name="rotate")
def api_keys_rotate(
    service_account: Annotated[
        str,
        Parameter(help="Owning service-account name or ID."),
    ],
    name_or_id: Annotated[str, Parameter(help="API-key name or ID.")],
    *,
    retain_minutes: Annotated[
        int,
        Parameter(help="Minutes to keep the old key valid during rotation."),
    ] = 0,
    set_key: Annotated[
        bool,
        Parameter(help="Use the rotated key as the active local server credential."),
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Rotate an API key and print its one-time replacement value."""
    command = "auth.api-keys.rotate"
    output_format = _resolve_output_format(output)

    result = run_with_cli_error_boundary(
        lambda: _auth_management_client().auth.api_keys.rotate(
            service_account,
            name_or_id,
            retain_period_minutes=retain_minutes,
            set_key=set_key,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )
    track(
        AnalyticsEvent.AUTH_API_KEY_ROTATED,
        {
            "command": command,
            "retain_period_configured": retain_minutes > 0,
            "set_key": set_key,
        },
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, _api_key_value_payload(result), output=output_format)
        return

    _print_success(f"Rotated API key: {result.api_key.name}")
    _emit_snapshot(
        "API key",
        _api_key_value_rows(result),
        warning=_api_key_value_warning(result),
    )


@auth_api_keys_app.command(name="delete")
def api_keys_delete(
    service_account: Annotated[
        str,
        Parameter(help="Owning service-account name or ID."),
    ],
    name_or_id: Annotated[str, Parameter(help="API-key name or ID.")],
    *,
    yes: Annotated[
        bool,
        Parameter(
            alias=["-y"],
            help="Skip confirmation prompt. Required with --output json.",
        ),
    ] = False,
    output: OutputFormatOption = "text",
) -> None:
    """Delete an API key."""
    command = "auth.api-keys.delete"
    output_format = _resolve_output_format(output)
    _confirm_delete(
        command=command,
        output=output_format,
        yes=yes,
        description=f"API key `{name_or_id}` for service account `{service_account}`",
    )

    run_with_cli_error_boundary(
        lambda: _auth_management_client().auth.api_keys.delete(
            service_account,
            name_or_id,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )
    track(AnalyticsEvent.AUTH_API_KEY_DELETED, {"command": command})
    item = {
        "service_account": service_account,
        "name_or_id": name_or_id,
        "deleted": True,
    }

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, item, output=output_format)
        return

    _print_success(f"Deleted API key: {name_or_id}")
