"""Kitaru command-line interface."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from importlib.metadata import version as get_version
from typing import Annotated
from urllib.parse import urlparse

import cyclopts
from cyclopts import Parameter
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text
from zenml.client import Client
from zenml.config.global_config import GlobalConfiguration
from zenml.login.credentials_store import get_credentials_store
from zenml.utils.server_utils import connected_to_local_server, get_local_server
from zenml.zen_server.deploy.deployer import LocalServerDeployer

from kitaru.config import (
    ResolvedLogStore,
    StackInfo,
    login_to_server,
    reset_global_log_store,
    resolve_log_store,
    set_global_log_store,
)
from kitaru.config import (
    current_stack as get_current_stack,
)
from kitaru.config import (
    list_stacks as get_available_stacks,
)
from kitaru.config import (
    use_stack as set_active_stack,
)

SDK_VERSION = get_version("kitaru")
AUTH_ENV_VARS = (
    "ZENML_STORE_URL",
    "ZENML_STORE_API_KEY",
    "ZENML_STORE_USERNAME",
    "ZENML_STORE_PASSWORD",
)

app = cyclopts.App(
    name="kitaru",
    help="Durable execution for AI agents.",
    version=SDK_VERSION,
    version_flags=["--version", "-V"],
)

log_store_app = cyclopts.App(
    name="log-store",
    help="Manage global runtime log-store settings.",
)
stack_app = cyclopts.App(
    name="stack",
    help="Inspect and switch the active stack.",
)
app.command(log_store_app)
app.command(stack_app)


@dataclass
class RuntimeSnapshot:
    """Resolved runtime information for `kitaru status` and `kitaru info`."""

    sdk_version: str
    connection: str
    connection_target: str
    config_directory: str
    local_stores_path: str
    server_url: str | None = None
    active_user: str | None = None
    active_project: str | None = None
    active_stack: str | None = None
    repository_root: str | None = None
    server_version: str | None = None
    server_database: str | None = None
    server_deployment_type: str | None = None
    local_server_status: str | None = None
    warning: str | None = None


def _is_interactive(*, stderr: bool = False) -> bool:
    """Check whether the target stream is an interactive terminal."""
    stream = sys.stderr if stderr else sys.stdout
    return hasattr(stream, "isatty") and stream.isatty()


def _value_style(value: str) -> str:
    """Choose a Rich style based on the value content."""
    if value in ("unavailable", "not set", "not started"):
        return "dim"
    if value.startswith(("http://", "https://")):
        return "underline"
    return ""


def _render_rich_snapshot(
    title: str,
    rows: list[tuple[str, str]],
    warning: str | None = None,
) -> None:
    """Render a snapshot as a styled Rich panel with key/value lines."""
    lines = Text()
    for i, (label, value) in enumerate(rows):
        lines.append(f"  {label}: ", style="bold cyan")
        lines.append(value, style=_value_style(value))
        if i < len(rows) - 1:
            lines.append("\n")

    elements: list[Text] = [lines]
    if warning:
        warn_text = Text("\n\n  Warning: ", style="bold yellow")
        warn_text.append(warning, style="yellow")
        elements.append(warn_text)

    Console().print(
        Panel(
            Group(*elements),
            title=f"[bold]{title}[/bold]",
            title_align="left",
            border_style="dim",
            expand=False,
            padding=(0, 1),
        )
    )


def _print_success(message: str, detail: str | None = None) -> None:
    """Print a success message, styled when the terminal is interactive."""
    if _is_interactive():
        c = Console()
        c.print(Text(message, style="green"))
        if detail:
            c.print(Text(f"  {detail}", style="dim"))
    else:
        print(message)
        if detail:
            print(f"  {detail}")


def _exit_with_error(message: str) -> None:
    """Print a friendly CLI error and exit with a non-zero status."""
    if _is_interactive(stderr=True):
        err = Text("Error: ", style="bold red")
        err.append(message, style="red")
        Console(stderr=True).print(err)
    else:
        print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def _describe_local_server() -> str:
    """Summarize the state of the local Kitaru-compatible server, if any."""
    try:
        local_server = get_local_server()
    except ImportError:
        return "unavailable (local runtime support not installed)"
    if local_server is None:
        return "not started"

    provider = local_server.config.provider.value
    if local_server.status and local_server.status.url:
        return f"running at {local_server.status.url} ({provider})"

    if local_server.status and local_server.status.status_message:
        return (
            f"registered but unavailable ({provider}: "
            f"{local_server.status.status_message})"
        )

    return f"registered but unavailable ({provider})"


def _connected_to_local_server() -> bool:
    """Safely check whether the current client is bound to a local server."""
    try:
        return connected_to_local_server()
    except ImportError:
        return False


def _ensure_no_auth_environment_overrides() -> None:
    """Fail early if auth environment variables would override the CLI."""
    present = [env_var for env_var in AUTH_ENV_VARS if env_var in os.environ]
    if present:
        joined = ", ".join(present)
        _exit_with_error(
            "Kitaru login/logout cannot override existing auth environment "
            f"variables ({joined}). Unset them first, or rely on those "
            "environment variables directly."
        )


def _build_snapshot_without_local_store(
    gc: GlobalConfiguration, _exc: Exception
) -> RuntimeSnapshot:
    """Build a degraded snapshot when local Kitaru runtime support is unavailable."""
    return RuntimeSnapshot(
        sdk_version=SDK_VERSION,
        connection="local mode (unavailable)",
        connection_target="unavailable",
        config_directory=str(gc.config_directory),
        local_stores_path=str(gc.local_stores_path),
        local_server_status=_describe_local_server(),
        warning=(
            "Local Kitaru runtime support is unavailable in this environment. "
            "Connect to a Kitaru server to keep working, or install the local "
            "runtime dependencies if you want the built-in local stack."
        ),
    )


def _uses_stale_local_server_url(
    server_url: str | None, local_server_status: str | None
) -> bool:
    """Check for a localhost URL that points at a stopped local server."""
    if not server_url or not local_server_status:
        return False

    hostname = urlparse(server_url).hostname
    return hostname in {"127.0.0.1", "localhost", "::1"} and (
        "unavailable" in local_server_status
    )


def _clear_persisted_store_configuration(gc: GlobalConfiguration) -> None:
    """Clear persisted global store state when no local fallback store exists."""
    gc.store = None
    gc._zen_store = None
    gc.active_stack_id = None
    gc.active_project_id = None
    gc._active_stack = None
    gc._active_project = None
    gc._write_config()


def _build_runtime_snapshot() -> RuntimeSnapshot:
    """Resolve the current Kitaru runtime state from ZenML-backed config."""
    gc = GlobalConfiguration()
    try:
        store_cfg = gc.store_configuration
        uses_local_store = gc.uses_local_store
    except ImportError as exc:
        return _build_snapshot_without_local_store(gc, exc)

    if uses_local_store:
        connection = "local database"
        server_url = None
    elif _connected_to_local_server():
        connection = "local Kitaru server"
        server_url = store_cfg.url
    else:
        connection = "remote Kitaru server"
        server_url = store_cfg.url

    snapshot = RuntimeSnapshot(
        sdk_version=SDK_VERSION,
        connection=connection,
        connection_target=store_cfg.url,
        server_url=server_url,
        config_directory=str(gc.config_directory),
        local_stores_path=str(gc.local_stores_path),
        local_server_status=_describe_local_server(),
    )

    if _uses_stale_local_server_url(server_url, snapshot.local_server_status):
        snapshot.warning = (
            "The configured Kitaru server points to a stopped local server. "
            "Start it again or run `kitaru logout` to clear the stale "
            "connection."
        )
        return snapshot

    try:
        client = Client()
        store_info = client.zen_store.get_store_info()
        snapshot.active_user = client.active_user.name
        try:
            snapshot.active_project = client.active_project.name
        except RuntimeError:
            snapshot.active_project = None
        snapshot.active_stack = client.active_stack_model.name
        snapshot.repository_root = str(client.root) if client.root else None
        snapshot.server_version = str(store_info.version)
        snapshot.server_database = str(store_info.database_type)
        snapshot.server_deployment_type = str(store_info.deployment_type)
    except Exception as exc:  # pragma: no cover - exercised via CLI behavior
        snapshot.warning = f"Unable to query the configured store: {exc}"

    return snapshot


def _get_connected_server_url() -> str | None:
    """Read the currently configured remote server URL, if available."""
    try:
        store_configuration = GlobalConfiguration().store_configuration
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
            ("Active project", snapshot.active_project or "not set"),
            ("Active stack", snapshot.active_stack or "unavailable"),
            ("Config directory", snapshot.config_directory),
            ("Local stores path", snapshot.local_stores_path),
        ]
    )
    if snapshot.local_server_status:
        rows.append(("Local server", snapshot.local_server_status))
    return rows


def _info_rows(snapshot: RuntimeSnapshot) -> list[tuple[str, str]]:
    """Build the label/value pairs for the detailed info view."""
    return [
        ("SDK version", snapshot.sdk_version),
        ("Connection", snapshot.connection),
        ("Connection target", snapshot.connection_target),
        ("Server URL", snapshot.server_url or "not connected"),
        ("Server version", snapshot.server_version or "unavailable"),
        ("Server database", snapshot.server_database or "unavailable"),
        ("Server deployment", snapshot.server_deployment_type or "unavailable"),
        ("Active user", snapshot.active_user or "unavailable"),
        ("Active project", snapshot.active_project or "not set"),
        ("Active stack", snapshot.active_stack or "unavailable"),
        ("Repository root", snapshot.repository_root or "not set"),
        ("Config directory", snapshot.config_directory),
        ("Local stores path", snapshot.local_stores_path),
        ("Local server", snapshot.local_server_status or "not started"),
    ]


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


def _stack_list_rows(stacks: list[StackInfo]) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru stack list`."""
    if not stacks:
        return [("Stacks", "none found")]

    return [
        (
            stack.name,
            f"{stack.id}{' (active)' if stack.is_active else ''}",
        )
        for stack in stacks
    ]


def _current_stack_rows(stack: StackInfo) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru stack current`."""
    return [
        ("Active stack", stack.name),
        ("Stack ID", stack.id),
    ]


def _render_plain_snapshot(
    title: str,
    rows: list[tuple[str, str]],
    warning: str | None = None,
) -> str:
    """Render a snapshot as plain indented text for non-TTY output."""
    lines = [title]
    for label, value in rows:
        lines.append(f"  {label}: {value}")
    if warning:
        lines.append(f"  Warning: {warning}")
    return "\n".join(lines)


def _emit_snapshot(
    title: str,
    rows: list[tuple[str, str]],
    warning: str | None = None,
) -> None:
    """Emit a snapshot view, choosing Rich or plain text based on the terminal."""
    if _is_interactive():
        _render_rich_snapshot(title, rows, warning)
    else:
        print(_render_plain_snapshot(title, rows, warning))


def _logout_current_connection() -> str:
    """Reset the active connection and clear current stored credentials."""
    gc = GlobalConfiguration()

    if _connected_to_local_server():
        LocalServerDeployer().remove_server()
        return "Logged out from the local Kitaru server."

    try:
        if gc.uses_local_store:
            return "Kitaru is already using its local default store."
        server_url = gc.store_configuration.url.rstrip("/")
    except ImportError:
        return (
            "Kitaru is not connected to a remote server, and local mode is "
            "unavailable in this environment."
        )
    local_fallback_available = True
    try:
        gc.set_default_store()
    except ImportError:
        local_fallback_available = False
        _clear_persisted_store_configuration(gc)

    suffix = ""
    if not local_fallback_available:
        suffix = " (local fallback unavailable in this environment)"

    if server_url.startswith(("http://", "https://")):
        get_credentials_store().clear_credentials(server_url)
        return f"Logged out from Kitaru server: {server_url}{suffix}"

    return f"Disconnected from store: {server_url}{suffix}"


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
) -> None:
    """Connect to a Kitaru server and persist the session globally."""
    _ensure_no_auth_environment_overrides()

    try:
        login_to_server(
            server,
            api_key=api_key,
            refresh=refresh,
            project=project,
            no_verify_ssl=no_verify_ssl,
            ssl_ca_cert=ssl_ca_cert,
            cloud_api_url=cloud_api_url,
        )
    except (RuntimeError, ValueError) as exc:
        _exit_with_error(str(exc))

    connected_server_url = _get_connected_server_url() or server.rstrip("/")
    _print_success(
        f"Connected to Kitaru server: {connected_server_url}",
        detail=f"Active project: {project}" if project else None,
    )


@app.command
def logout() -> None:
    """Log out from the current Kitaru server and clear stored auth state."""
    _ensure_no_auth_environment_overrides()
    _print_success(_logout_current_connection())


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
) -> None:
    """Set the global runtime log-store backend override."""
    try:
        snapshot = set_global_log_store(
            backend,
            endpoint=endpoint,
            api_key=api_key,
        )
    except ValueError as exc:
        _exit_with_error(str(exc))

    _print_success(
        "Saved global log-store override.",
        detail=_log_store_detail(snapshot),
    )


@log_store_app.command
def show() -> None:
    """Show the effective global runtime log-store configuration."""
    try:
        snapshot = resolve_log_store()
    except ValueError as exc:
        _exit_with_error(str(exc))

    _emit_snapshot("Kitaru log store", _log_store_rows(snapshot))


@log_store_app.command
def reset() -> None:
    """Clear the persisted global runtime log-store override."""
    try:
        snapshot = reset_global_log_store()
    except ValueError as exc:
        _exit_with_error(str(exc))

    _print_success(
        "Cleared global log-store override.",
        detail=_log_store_detail(snapshot),
    )


@stack_app.command
def list_() -> None:
    """List stacks visible to the current user."""
    try:
        stacks = get_available_stacks()
    except Exception as exc:  # pragma: no cover - exercised via CLI behavior
        _exit_with_error(str(exc))

    _emit_snapshot("Kitaru stacks", _stack_list_rows(stacks))


@stack_app.command
def current() -> None:
    """Show the currently active stack."""
    try:
        stack = get_current_stack()
    except Exception as exc:  # pragma: no cover - exercised via CLI behavior
        _exit_with_error(str(exc))

    _emit_snapshot("Kitaru stack", _current_stack_rows(stack))


@stack_app.command
def use(
    stack: Annotated[
        str,
        Parameter(help="Stack name or ID to activate."),
    ],
) -> None:
    """Set the active stack by name or ID."""
    try:
        selected_stack = set_active_stack(stack)
    except Exception as exc:  # pragma: no cover - exercised via CLI behavior
        _exit_with_error(str(exc))

    _print_success(
        f"Activated stack: {selected_stack.name}",
        detail=f"Stack ID: {selected_stack.id}",
    )


@app.command
def status() -> None:
    """Show the current connection state and active stack context."""
    snapshot = _build_runtime_snapshot()
    _emit_snapshot("Kitaru status", _status_rows(snapshot), snapshot.warning)


@app.command
def info() -> None:
    """Show detailed environment information for the current setup."""
    snapshot = _build_runtime_snapshot()
    _emit_snapshot("Kitaru info", _info_rows(snapshot), snapshot.warning)


@app.default
def main() -> None:
    """Show help when invoked without arguments."""
    app.help_print()


def cli() -> None:
    """Entry point for the `kitaru` console script."""
    app()
