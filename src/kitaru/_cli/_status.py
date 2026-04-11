"""Connection, status, and log-store CLI commands."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlparse

from cyclopts import Parameter

from kitaru._interface_errors import run_with_cli_error_boundary
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
    is_registered_local_server_url,
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
    _print_warning,
    _resolve_output_format,
)


@dataclass(frozen=True)
class LogoutResult:
    """Structured logout result for text and JSON output."""

    mode: str
    target: str | None = None
    local_fallback_available: bool | None = None
    local_server_stopped: bool = False
    local_server_url: str | None = None

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
                self.local_server_stopped,
                self.local_server_url,
            ) == (
                other.mode,
                other.target,
                other.local_fallback_available,
                other.local_server_stopped,
                other.local_server_url,
            )
        return NotImplemented


def _describe_local_server() -> str:
    """Expose the local-server summary helper for CLI tests and snapshots."""
    return describe_local_server()


def _active_auth_environment_overrides() -> list[str]:
    """Return active auth environment variable overrides in display order."""
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

    return present


def _ensure_no_auth_environment_overrides(
    *,
    command: str = "auth",
    output: CLIOutputFormat = CLIOutputFormat.TEXT,
) -> None:
    """Fail early if auth environment variables would override the CLI."""
    present = _active_auth_environment_overrides()
    if not present:
        return

    joined = ", ".join(present)
    _exit_with_error(
        command,
        "Kitaru login/logout cannot override existing auth environment "
        f"variables ({joined}). Unset them first, or rely on those "
        "environment variables directly.",
        output=output,
    )


def _emit_warning(
    message: str,
    *,
    output: CLIOutputFormat,
    detail: str | None = None,
) -> None:
    """Emit a non-fatal warning without breaking JSON stdout payloads."""
    if output == CLIOutputFormat.JSON:
        print(f"Warning: {message}", file=sys.stderr)
        if detail:
            print(f"  {detail}", file=sys.stderr)
        return
    _print_warning(message, detail)


def _warn_for_auth_environment_overrides(*, output: CLIOutputFormat) -> None:
    """Warn when auth env vars are active but local login will continue."""
    present = _active_auth_environment_overrides()
    if not present:
        return

    joined = ", ".join(present)
    _emit_warning(
        f"Auth environment variables are active ({joined}).",
        output=output,
        detail=(
            "The local server will start, but runtime connections may still use "
            "those environment variables."
        ),
    )


def _validate_local_login_flags(
    *,
    api_key: str | None,
    refresh: bool,
    project: str | None,
    no_verify_ssl: bool,
    ssl_ca_cert: str | None,
    command: str,
    output: CLIOutputFormat,
) -> None:
    """Reject remote-only flags in local-login mode."""
    invalid_options: list[tuple[bool, str]] = [
        (api_key is not None, "--api-key"),
        (refresh, "--refresh"),
        (project is not None, "--project"),
        (no_verify_ssl, "--no-verify-ssl"),
        (ssl_ca_cert is not None, "--ssl-ca-cert"),
    ]
    for active, option in invalid_options:
        if active:
            _exit_with_error(
                command,
                f"{option} is only used when connecting to a remote server.",
                output=output,
            )


def _validate_remote_login_flags(
    *,
    port: int | None,
    command: str,
    output: CLIOutputFormat,
) -> None:
    """Reject local-only flags in remote-login mode."""
    if port is not None:
        _exit_with_error(
            command,
            "--port is only used for local server startup.",
            output=output,
        )


def _is_localhost_url(url: str | None) -> bool:
    """Return whether a URL matches the registered local server."""
    return is_registered_local_server_url(url)


def _login_payload_local(result: Any) -> dict[str, Any]:
    """Serialize a local-login result for JSON output."""
    return {"mode": "local", "url": result.url}


def _login_payload_remote(url: str, project: str | None) -> dict[str, Any]:
    """Serialize a remote-login result for JSON output."""
    return {"mode": "remote", "url": url, "project": project}


def _render_local_login_messages(result: Any) -> None:
    """Render the text output for a successful local login."""
    if result.action == "started":
        _print_success("Starting local Kitaru server...")
        _print_success(f"Server running at {result.url}")
        _print_success("Connected to local Kitaru server.")
        return

    if result.action == "connected":
        _print_success(f"Server already running at {result.url}")
        _print_success("Connected to local Kitaru server.")
        return

    port = urlparse(result.url).port
    if port is None:
        _print_success("Restarting local Kitaru server...")
    else:
        _print_success(f"Restarting local Kitaru server on port {port}...")
    _print_success(f"Server running at {result.url}")
    _print_success("Connected to local Kitaru server.")


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
    """Build the label/value pairs for the detailed info overview section."""
    rows = [
        ("SDK version", snapshot.sdk_version),
        ("ZenML version", snapshot.zenml_version or "unavailable"),
        ("Connection", snapshot.connection),
        ("Connection target", snapshot.connection_target),
        ("Server URL", snapshot.server_url or "not connected"),
        ("Server version", snapshot.server_version or "unavailable"),
        ("Server database", snapshot.server_database or "unavailable"),
        ("Server deployment", snapshot.server_deployment_type or "unavailable"),
        ("Active user", snapshot.active_user or "unavailable"),
    ]
    if snapshot.active_project:
        rows.append(("Active project", snapshot.active_project))
    rows.extend(
        [
            ("Active stack", snapshot.active_stack or "unavailable"),
            ("Repository root", snapshot.repository_root or "not set"),
            ("Config directory", snapshot.config_directory),
            ("Local server", snapshot.local_server_status or "not started"),
        ]
    )
    if snapshot.log_store_status:
        rows.append(("Log store", snapshot.log_store_status))
    if snapshot.project_override:
        source = ""
        if snapshot.connection_sources and "project" in snapshot.connection_sources:
            source = f" ({snapshot.connection_sources['project']})"
        rows.append(("Project override", f"{snapshot.project_override}{source}"))
    return rows


def _config_provenance_rows(snapshot: RuntimeSnapshot) -> list[tuple[str, str]]:
    """Build label/value rows for the config provenance section."""
    rows: list[tuple[str, str]] = []
    if snapshot.kitaru_global_config_path:
        rows.append(("Kitaru global config", snapshot.kitaru_global_config_path))
    if snapshot.zenml_global_config_path:
        rows.append(("ZenML global config", snapshot.zenml_global_config_path))
    if snapshot.local_stores_path:
        rows.append(("Local stores", snapshot.local_stores_path))
    if snapshot.repository_config_path:
        rows.append(("Repository config", snapshot.repository_config_path))
    return rows


def _connection_source_rows(snapshot: RuntimeSnapshot) -> list[tuple[str, str]]:
    """Build label/value rows for the connection source section."""
    if not snapshot.connection_sources:
        return []
    return [(key, value) for key, value in snapshot.connection_sources.items()]


def _system_rows(snapshot: RuntimeSnapshot) -> list[tuple[str, str]]:
    """Build label/value rows for the system info section."""
    rows: list[tuple[str, str]] = []
    if snapshot.python_version:
        rows.append(("Python version", snapshot.python_version))
    if snapshot.system_info:
        if "os" in snapshot.system_info:
            rows.append(("OS", snapshot.system_info["os"]))
        if "architecture" in snapshot.system_info:
            rows.append(("Architecture", snapshot.system_info["architecture"]))
    if snapshot.environment_type:
        rows.append(("Environment type", snapshot.environment_type))
    return rows


def _package_rows(snapshot: RuntimeSnapshot) -> list[tuple[str, str]]:
    """Build label/value rows for the packages section."""
    if not snapshot.packages:
        return []
    return [(name, version) for name, version in snapshot.packages.items()]


def _build_info_sections(snapshot: RuntimeSnapshot) -> list[SnapshotSection]:
    """Build multi-section layout for the info command."""
    sections = [SnapshotSection(title=None, rows=_info_rows(snapshot))]

    provenance = _config_provenance_rows(snapshot)
    if provenance:
        sections.append(SnapshotSection(title="Config provenance", rows=provenance))

    conn_sources = _connection_source_rows(snapshot)
    if conn_sources:
        sections.append(SnapshotSection(title="Connection source", rows=conn_sources))

    if snapshot.environment:
        sections.append(
            SnapshotSection(
                title="Environment",
                rows=_environment_rows(snapshot.environment),
            )
        )

    sys_rows = _system_rows(snapshot)
    if sys_rows:
        sections.append(SnapshotSection(title="System", rows=sys_rows))

    pkg_rows = _package_rows(snapshot)
    if pkg_rows:
        sections.append(SnapshotSection(title="Packages", rows=pkg_rows))

    return sections


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
    connected_server_url = facade._get_connected_server_url()
    was_connected_to_local_server = facade._connected_to_local_server()
    stop_result = facade.stop_registered_local_server()

    if was_connected_to_local_server:
        return LogoutResult(
            mode="local_server",
            local_server_stopped=stop_result.stopped,
            local_server_url=stop_result.url,
        )

    try:
        if gc.uses_local_store:
            return LogoutResult(
                mode="local_store",
                local_server_stopped=stop_result.stopped,
                local_server_url=stop_result.url,
            )
        server_url = gc.store_configuration.url.rstrip("/")
    except ImportError:
        return LogoutResult(
            mode="unavailable",
            local_server_stopped=stop_result.stopped,
            local_server_url=stop_result.url,
        )

    local_fallback_available = True
    try:
        gc.set_default_store()
    except ImportError:
        local_fallback_available = False
        _clear_persisted_store_configuration(gc)

    if _is_localhost_url(server_url or connected_server_url):
        return LogoutResult(
            mode="local_server",
            local_fallback_available=local_fallback_available,
            local_server_stopped=stop_result.stopped,
            local_server_url=stop_result.url or server_url,
        )

    if server_url.startswith(("http://", "https://")):
        facade.get_credentials_store().clear_credentials(server_url)
        return LogoutResult(
            mode="remote_server",
            target=server_url,
            local_fallback_available=local_fallback_available,
            local_server_stopped=stop_result.stopped,
            local_server_url=stop_result.url,
        )

    return LogoutResult(
        mode="remote_store",
        target=server_url,
        local_fallback_available=local_fallback_available,
        local_server_stopped=stop_result.stopped,
        local_server_url=stop_result.url,
    )


def _logout_result_payload(result: LogoutResult) -> dict[str, Any]:
    """Serialize a logout result for JSON output."""
    return {
        "mode": result.mode,
        "target": result.target,
        "local_fallback_available": result.local_fallback_available,
        "local_server_stopped": result.local_server_stopped,
    }


def _stopped_local_server_message(url: str | None) -> str:
    """Render the follow-up line when logout also stopped a local server."""
    if url:
        port = urlparse(url).port
        if port is not None:
            return f"Stopped local server (port {port})."
    return "Stopped local server."


def _logout_result_message(result: LogoutResult) -> str:
    """Render the legacy text message for a logout result."""
    if result.mode == "local_server":
        return "Logged out from the local Kitaru server."
    elif result.mode == "local_store":
        message = "Kitaru is already using its local default store."
    elif result.mode == "unavailable":
        message = (
            "Kitaru is not connected to a remote server, and local mode is "
            "unavailable in this environment."
        )
    else:
        suffix = ""
        if result.local_fallback_available is False:
            suffix = " (local fallback unavailable in this environment)"
        if result.mode == "remote_server":
            message = f"Logged out from Kitaru server: {result.target}{suffix}"
        else:
            message = f"Disconnected from store: {result.target}{suffix}"

    if result.local_server_stopped:
        return "\n".join(
            [message, _stopped_local_server_message(result.local_server_url)]
        )
    return message


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
        str | None,
        Parameter(
            help=(
                "Kitaru server URL, managed workspace name, or managed "
                "workspace ID. Omit to start a local server."
            )
        ),
    ] = None,
    *,
    api_key: Annotated[
        str | None,
        Parameter(help="API key for remote server authentication."),
    ] = None,
    refresh: Annotated[
        bool,
        Parameter(help="Force a fresh authentication flow (remote only)."),
    ] = False,
    project: Annotated[
        str | None,
        Parameter(help="Project to activate after connecting (remote only)."),
    ] = None,
    no_verify_ssl: Annotated[
        bool,
        Parameter(help="Disable TLS certificate verification (remote only)."),
    ] = False,
    ssl_ca_cert: Annotated[
        str | None,
        Parameter(help="Path to a CA bundle for server verification (remote only)."),
    ] = None,
    port: Annotated[
        int | None,
        Parameter(help="Port for the local server (default: 8383)."),
    ] = None,
    timeout: Annotated[
        int,
        Parameter(help="Timeout in seconds for server startup or connection."),
    ] = 60,
    output: OutputFormatOption = "text",
) -> None:
    """Connect to a remote server, or start and connect to a local server."""
    command = "login"
    output_format = _resolve_output_format(output)
    facade = _facade_module()
    if server is None:
        _validate_local_login_flags(
            api_key=api_key,
            refresh=refresh,
            project=project,
            no_verify_ssl=no_verify_ssl,
            ssl_ca_cert=ssl_ca_cert,
            command=command,
            output=output_format,
        )
        _warn_for_auth_environment_overrides(output=output_format)

        connected_server_url = facade._get_connected_server_url()
        if (
            connected_server_url
            and not connected_server_url.startswith("sqlite:")
            and not facade._connected_to_local_server()
            and not _is_localhost_url(connected_server_url)
        ):
            _emit_warning(
                f"Disconnecting from remote server: {connected_server_url}",
                output=output_format,
            )

        result = run_with_cli_error_boundary(
            lambda: facade.start_or_connect_local_server(
                port=port,
                timeout=timeout,
            ),
            command=command,
            output=output_format,
            exit_with_error=_exit_with_error,
            handled_exceptions=(Exception,),
        )

        from kitaru.analytics import AnalyticsEvent, track

        track(
            AnalyticsEvent.LOGIN_COMPLETED,
            {"mode": "local", "action": result.action},
        )

        if output_format == CLIOutputFormat.JSON:
            _emit_json_item(
                command,
                _login_payload_local(result),
                output=output_format,
            )
            return

        _render_local_login_messages(result)
        return

    _validate_remote_login_flags(
        port=port,
        command=command,
        output=output_format,
    )
    _ensure_no_auth_environment_overrides(command=command, output=output_format)

    run_with_cli_error_boundary(
        lambda: facade.login_to_server(
            server,
            api_key=api_key,
            refresh=refresh,
            project=project,
            no_verify_ssl=no_verify_ssl,
            ssl_ca_cert=ssl_ca_cert,
            timeout=timeout,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        handled_exceptions=(Exception,),
    )

    from kitaru.analytics import AnalyticsEvent, track

    track(
        AnalyticsEvent.LOGIN_COMPLETED,
        {"mode": "remote", "project_provided": project is not None},
    )

    connected_server_url = facade._get_connected_server_url() or server.rstrip("/")
    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            _login_payload_remote(connected_server_url, project),
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
    result = run_with_cli_error_boundary(
        _logout_current_connection,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
        handled_exceptions=(Exception,),
    )
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

    from kitaru.analytics import AnalyticsEvent, track

    track(AnalyticsEvent.STATUS_VIEWED)

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


def _write_info_file(
    snapshot: RuntimeSnapshot,
    file_path: str,
) -> None:
    """Write serialized snapshot to a file, inferring format from extension."""
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = serialize_runtime_snapshot(snapshot)
    suffix = path.suffix.lower()

    if suffix in (".yaml", ".yml"):
        try:
            from zenml.utils import yaml_utils

            yaml_utils.write_yaml(str(path), data)
        except ImportError:
            try:
                import yaml  # PyYAML — transitive ZenML dependency
            except ImportError:
                raise ValueError(
                    "YAML export requires PyYAML. "
                    "Install it with: uv pip install pyyaml"
                ) from None
            with open(path, "w") as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    else:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
            f.write("\n")


@app.command
def info(
    *,
    all: Annotated[
        bool,
        Parameter(
            alias=["-a"],
            help="Full diagnostic: include all packages and environment type.",
        ),
    ] = False,
    all_packages: Annotated[
        bool,
        Parameter(help="Include all installed package versions."),
    ] = False,
    packages: Annotated[
        tuple[str, ...],
        Parameter(
            alias=["-p"],
            help="Include specific package versions (repeatable).",
        ),
    ] = (),
    file: Annotated[
        str | None,
        Parameter(
            alias=["-f"],
            help="Export diagnostics to file (format from extension: .json, .yaml).",
        ),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Show detailed environment information for the current setup."""
    output_format = _resolve_output_format(output)
    command = "info"

    include_packages = all or all_packages
    package_names = None if include_packages else (list(packages) if packages else None)
    include_environment_type = all

    snapshot = _facade_module()._build_runtime_snapshot(
        include_packages=include_packages,
        package_names=package_names,
        include_environment_type=include_environment_type,
    )

    from kitaru.analytics import AnalyticsEvent, track

    track(
        AnalyticsEvent.INFO_VIEWED,
        {
            "all": all,
            "packages_requested": include_packages or bool(packages),
        },
    )

    if file is not None:
        run_with_cli_error_boundary(
            lambda: _write_info_file(snapshot, file),
            command=command,
            output=output_format,
            exit_with_error=_exit_with_error,
            handled_exceptions=(OSError, ValueError),
        )

        file_format = (
            "yaml" if Path(file).suffix.lower() in (".yaml", ".yml") else "json"
        )
        track(AnalyticsEvent.INFO_EXPORTED, {"format": file_format})

        if output_format == CLIOutputFormat.JSON:
            _emit_json_item(
                command,
                {"file": file, "format": file_format},
                output=output_format,
            )
            return
        _print_success(f"Diagnostics written to {file}")
        return

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            serialize_runtime_snapshot(snapshot),
            output=output_format,
        )
        return

    sections = _build_info_sections(snapshot)
    _emit_snapshot_sections(
        "Kitaru info",
        sections,
        _combine_warnings(snapshot.warning, snapshot.log_store_warning),
    )
