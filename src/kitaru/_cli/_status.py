"""Connection, status, and log-store CLI commands."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Annotated, Any

from cyclopts import Parameter

from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.analytics import track
from kitaru.cli_output import CLIOutputFormat
from kitaru.config import (
    KITARU_AUTH_TOKEN_ENV,
    KITARU_SERVER_URL_ENV,
    ZENML_STORE_API_KEY_ENV,
    ZENML_STORE_URL_ENV,
    ActiveEnvironmentVariable,
    ResolvedLogStore,
    active_stack_log_store,
)
from kitaru.inspection import (
    RuntimeSnapshot,
    describe_local_server,
    serialize_resolved_log_store,
    serialize_runtime_snapshot,
)
from kitaru.inspection import combine_warnings as _combine_warnings

from . import app, log_store_app
from ._helpers import (
    OutputFormatOption,
    SnapshotSection,
    _emit_json_item,
    _emit_snapshot,
    _emit_snapshot_sections,
    _exit_with_error,
    _facade_module,
    _print_success,
    _resolve_output_format,
)


@dataclass(frozen=True)
class LogoutResult:
    """Structured logout result for text and JSON output."""

    mode: str
    target: str | None = None
    local_fallback_available: bool | None = None

    def __str__(self) -> str:
        """Render the legacy logout message string."""
        return _logout_result_message(self)

    def __contains__(self, needle: str) -> bool:
        """Support substring assertions against the rendered logout message."""
        return needle in str(self)

    def __eq__(self, other: object) -> bool:
        """Preserve dataclass equality while allowing direct string comparison."""
        if isinstance(other, str):
            return str(self) == other
        if isinstance(other, LogoutResult):
            return (
                self.mode,
                self.target,
                self.local_fallback_available,
            ) == (
                other.mode,
                other.target,
                other.local_fallback_available,
            )
        return NotImplemented


def _describe_local_server() -> str:
    """Expose the local-server summary helper for CLI tests and snapshots."""
    return describe_local_server()


def _ensure_no_auth_environment_overrides(
    *,
    command: str = "auth",
    output: CLIOutputFormat = CLIOutputFormat.TEXT,
) -> None:
    """Fail early if auth environment variables would override the CLI."""
    present: list[str] = []

    if KITARU_SERVER_URL_ENV in os.environ:
        present.append(KITARU_SERVER_URL_ENV)
    elif ZENML_STORE_URL_ENV in os.environ:
        present.append(ZENML_STORE_URL_ENV)

    if KITARU_AUTH_TOKEN_ENV in os.environ:
        present.append(KITARU_AUTH_TOKEN_ENV)
    elif ZENML_STORE_API_KEY_ENV in os.environ:
        present.append(ZENML_STORE_API_KEY_ENV)

    for env_var in ("ZENML_STORE_USERNAME", "ZENML_STORE_PASSWORD"):
        if env_var in os.environ:
            present.append(env_var)

    if present:
        joined = ", ".join(present)
        _exit_with_error(
            command,
            "Kitaru login/logout cannot override existing auth environment "
            f"variables ({joined}). Unset them first, or rely on those "
            "environment variables directly.",
            output=output,
        )


def _clear_persisted_store_configuration(gc: Any) -> None:
    """Clear persisted global store state when no local fallback store exists."""
    gc.store = None
    gc._zen_store = None
    gc.active_stack_id = None
    gc.active_project_id = None
    gc._active_stack = None
    gc._active_project = None
    gc._write_config()


def _get_connected_server_url() -> str | None:
    """Read the currently configured remote server URL, if available."""
    facade = _facade_module()
    try:
        store_configuration = facade.GlobalConfiguration().store_configuration
    except Exception:
        return None

    server_url = getattr(store_configuration, "url", None)
    if not server_url:
        return None

    return str(server_url).rstrip("/")


def _status_rows(snapshot: RuntimeSnapshot) -> list[tuple[str, str]]:
    """Build the label/value pairs for the compact status view."""
    rows: list[tuple[str, str]] = [
        ("SDK version", snapshot.sdk_version),
        ("Connection", snapshot.connection),
    ]
    if snapshot.server_url:
        rows.append(("Server URL", snapshot.server_url))
    rows.extend(
        [
            ("Active user", snapshot.active_user or "unavailable"),
            ("Active stack", snapshot.active_stack or "unavailable"),
            ("Config directory", snapshot.config_directory),
        ]
    )
    if snapshot.local_server_status:
        rows.append(("Local server", snapshot.local_server_status))
    if snapshot.log_store_status:
        rows.append(("Log store", snapshot.log_store_status))
    return rows


def _info_rows(snapshot: RuntimeSnapshot) -> list[tuple[str, str]]:
    """Build the label/value pairs for the detailed info view."""
    rows = [
        ("SDK version", snapshot.sdk_version),
        ("Connection", snapshot.connection),
        ("Connection target", snapshot.connection_target),
        ("Server URL", snapshot.server_url or "not connected"),
        ("Server version", snapshot.server_version or "unavailable"),
        ("Server database", snapshot.server_database or "unavailable"),
        ("Server deployment", snapshot.server_deployment_type or "unavailable"),
        ("Active user", snapshot.active_user or "unavailable"),
        ("Active stack", snapshot.active_stack or "unavailable"),
        ("Repository root", snapshot.repository_root or "not set"),
        ("Config directory", snapshot.config_directory),
        ("Local server", snapshot.local_server_status or "not started"),
    ]
    if snapshot.project_override:
        rows.append(("Project override", snapshot.project_override))
    return rows


def _log_store_rows(snapshot: ResolvedLogStore) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru log-store show`."""
    return [
        ("Backend", snapshot.backend),
        ("Endpoint", snapshot.endpoint or "not set"),
        ("API key", "configured" if snapshot.api_key else "not set"),
        ("Source", snapshot.source),
    ]


def _log_store_detail(snapshot: ResolvedLogStore) -> str:
    """Build a follow-up detail line for set/reset success messages."""
    if snapshot.source == "global user config":
        return f"Effective backend: {snapshot.backend}"

    return f"Effective backend: {snapshot.backend} (from {snapshot.source} settings)"


def _environment_rows(
    environment: list[ActiveEnvironmentVariable],
) -> list[tuple[str, str]]:
    """Build label/value rows for the active environment section."""
    return [(entry.name, entry.value) for entry in environment]


def _logout_current_connection() -> LogoutResult:
    """Reset the active connection and clear current stored credentials."""
    facade = _facade_module()
    gc = facade.GlobalConfiguration()

    if facade._connected_to_local_server():
        facade.LocalServerDeployer().remove_server()
        return LogoutResult(mode="local_server")

    try:
        if gc.uses_local_store:
            return LogoutResult(mode="local_store")
        server_url = gc.store_configuration.url.rstrip("/")
    except ImportError:
        return LogoutResult(mode="unavailable")

    local_fallback_available = True
    try:
        gc.set_default_store()
    except ImportError:
        local_fallback_available = False
        _clear_persisted_store_configuration(gc)

    if server_url.startswith(("http://", "https://")):
        facade.get_credentials_store().clear_credentials(server_url)
        return LogoutResult(
            mode="remote_server",
            target=server_url,
            local_fallback_available=local_fallback_available,
        )

    return LogoutResult(
        mode="remote_store",
        target=server_url,
        local_fallback_available=local_fallback_available,
    )


def _logout_result_payload(result: LogoutResult) -> dict[str, Any]:
    """Serialize a logout result for JSON output."""
    return {
        "mode": result.mode,
        "target": result.target,
        "local_fallback_available": result.local_fallback_available,
    }


def _logout_result_message(result: LogoutResult) -> str:
    """Render the legacy text message for a logout result."""
    if result.mode == "local_server":
        return "Logged out from the local Kitaru server."
    if result.mode == "local_store":
        return "Kitaru is already using its local default store."
    if result.mode == "unavailable":
        return (
            "Kitaru is not connected to a remote server, and local mode is "
            "unavailable in this environment."
        )

    suffix = ""
    if result.local_fallback_available is False:
        suffix = " (local fallback unavailable in this environment)"

    if result.mode == "remote_server":
        return f"Logged out from Kitaru server: {result.target}{suffix}"
    return f"Disconnected from store: {result.target}{suffix}"


def _log_store_payload(snapshot: ResolvedLogStore) -> dict[str, Any]:
    """Serialize effective log-store state for JSON output."""
    facade = _facade_module()
    _, mismatch_warning = facade._log_store_mismatch_details(snapshot)
    return serialize_resolved_log_store(
        snapshot,
        active_store=active_stack_log_store(),
        warning=mismatch_warning,
    )


@app.command
def login(
    server: Annotated[
        str,
        Parameter(
            help=(
                "Kitaru server URL, managed workspace name, or managed workspace ID."
            ),
            alias=["--server-url"],
        ),
    ],
    api_key: Annotated[
        str | None,
        Parameter(help="API key used to authenticate with the server."),
    ] = None,
    refresh: Annotated[
        bool,
        Parameter(help="Force a fresh authentication flow."),
    ] = False,
    project: Annotated[
        str | None,
        Parameter(help="Project name or ID to activate after connecting."),
    ] = None,
    no_verify_ssl: Annotated[
        bool,
        Parameter(help="Disable TLS certificate verification."),
    ] = False,
    ssl_ca_cert: Annotated[
        str | None,
        Parameter(help="Path to a CA bundle used to verify the server."),
    ] = None,
    cloud_api_url: Annotated[
        str | None,
        Parameter(
            help=(
                "Managed-cloud API URL used for staging or another custom "
                "control plane."
            ),
            alias=["--pro-api-url"],
        ),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Connect to a Kitaru server and persist the session globally."""
    command = "login"
    output_format = _resolve_output_format(output)
    _ensure_no_auth_environment_overrides(command=command, output=output_format)

    facade = _facade_module()
    run_with_cli_error_boundary(
        lambda: facade.login_to_server(
            server,
            api_key=api_key,
            refresh=refresh,
            project=project,
            no_verify_ssl=no_verify_ssl,
            ssl_ca_cert=ssl_ca_cert,
            cloud_api_url=cloud_api_url,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        handled_exceptions=(RuntimeError, ValueError),
    )

    track("Kitaru server connected")

    connected_server_url = facade._get_connected_server_url() or server.rstrip("/")
    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            {
                "server_url": connected_server_url,
                "project": project,
            },
            output=output_format,
        )
        return

    _print_success(f"Connected to Kitaru server: {connected_server_url}")


@app.command
def logout(output: OutputFormatOption = "text") -> None:
    """Log out from the current Kitaru server and clear stored auth state."""
    command = "logout"
    output_format = _resolve_output_format(output)
    _ensure_no_auth_environment_overrides(command=command, output=output_format)
    result = _logout_current_connection()
    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            _logout_result_payload(result),
            output=output_format,
        )
        return
    _print_success(_logout_result_message(result))


@log_store_app.command
def set(
    backend: Annotated[
        str,
        Parameter(help="External runtime log backend name (for example datadog)."),
    ],
    *,
    endpoint: Annotated[
        str,
        Parameter(help="HTTP(S) endpoint for the configured log backend."),
    ],
    api_key: Annotated[
        str | None,
        Parameter(help="Optional API key or secret placeholder."),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Set the global runtime log-store backend override."""
    command = "log-store.set"
    output_format = _resolve_output_format(output)
    snapshot = run_with_cli_error_boundary(
        lambda: _facade_module().set_global_log_store(
            backend,
            endpoint=endpoint,
            api_key=api_key,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        handled_exceptions=(ValueError,),
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, _log_store_payload(snapshot), output=output_format)
        return

    _print_success(
        "Saved global log-store override.",
        detail=_log_store_detail(snapshot),
    )


@log_store_app.command
def show__(output: OutputFormatOption = "text") -> None:
    """Show the effective global runtime log-store configuration."""
    command = "log-store.show"
    output_format = _resolve_output_format(output)
    facade = _facade_module()
    snapshot = run_with_cli_error_boundary(
        facade.resolve_log_store,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        handled_exceptions=(ValueError,),
    )

    _, mismatch_warning = facade._log_store_mismatch_details(snapshot)
    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, _log_store_payload(snapshot), output=output_format)
        return
    _emit_snapshot("Kitaru log store", _log_store_rows(snapshot), mismatch_warning)


@log_store_app.command
def reset(output: OutputFormatOption = "text") -> None:
    """Clear the persisted global runtime log-store override."""
    command = "log-store.reset"
    output_format = _resolve_output_format(output)
    snapshot = run_with_cli_error_boundary(
        _facade_module().reset_global_log_store,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        handled_exceptions=(ValueError,),
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, _log_store_payload(snapshot), output=output_format)
        return

    _print_success(
        "Cleared global log-store override.",
        detail=_log_store_detail(snapshot),
    )


@app.command
def status(output: OutputFormatOption = "text") -> None:
    """Show the current connection state and active stack context."""
    output_format = _resolve_output_format(output)
    snapshot = _facade_module()._build_runtime_snapshot()
    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            "status",
            serialize_runtime_snapshot(snapshot),
            output=output_format,
        )
        return

    sections = [SnapshotSection(title=None, rows=_status_rows(snapshot))]
    if snapshot.environment:
        sections.append(
            SnapshotSection(
                title="Environment",
                rows=_environment_rows(snapshot.environment),
            )
        )
    _emit_snapshot_sections(
        "Kitaru status",
        sections,
        _combine_warnings(snapshot.warning, snapshot.log_store_warning),
    )


@app.command
def info(output: OutputFormatOption = "text") -> None:
    """Show detailed environment information for the current setup."""
    output_format = _resolve_output_format(output)
    snapshot = _facade_module()._build_runtime_snapshot()
    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            "info",
            serialize_runtime_snapshot(snapshot),
            output=output_format,
        )
        return

    _emit_snapshot(
        "Kitaru info",
        _info_rows(snapshot),
        _combine_warnings(snapshot.warning, snapshot.log_store_warning),
    )
