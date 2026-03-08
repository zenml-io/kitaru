"""Kitaru command-line interface."""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from importlib.metadata import version as get_version
from pathlib import Path
from types import ModuleType
from typing import Annotated, Any, Protocol, runtime_checkable
from urllib.parse import urlparse

import cyclopts
from cyclopts import Parameter
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text
from zenml.client import Client
from zenml.config.global_config import GlobalConfiguration
from zenml.exceptions import EntityExistsError, ZenKeyError
from zenml.login.credentials_store import get_credentials_store
from zenml.models import SecretResponse
from zenml.utils.server_utils import connected_to_local_server, get_local_server
from zenml.zen_server.deploy.deployer import LocalServerDeployer

from kitaru.client import Execution, ExecutionStatus, KitaruClient
from kitaru.config import (
    ModelAliasEntry,
    ResolvedLogStore,
    StackInfo,
    list_model_aliases,
    login_to_server,
    register_model_alias,
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
_SECRET_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

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
secrets_app = cyclopts.App(
    name="secrets",
    help="Manage centralized runtime secrets.",
)
model_app = cyclopts.App(
    name="model",
    help="Manage local model aliases for kitaru.llm().",
)
executions_app = cyclopts.App(
    name="executions",
    help="Inspect and manage flow executions.",
)
app.command(log_store_app)
app.command(stack_app)
app.command(secrets_app)
app.command(model_app)
app.command(executions_app)


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


@runtime_checkable
class _FlowHandleLike(Protocol):
    """Protocol for flow handles returned by `.start()` / `.deploy()`."""

    @property
    def exec_id(self) -> str: ...


@runtime_checkable
class _FlowTarget(Protocol):
    """Protocol for CLI-runnable flow objects."""

    def start(self, *args: Any, **kwargs: Any) -> _FlowHandleLike: ...

    def deploy(self, *args: Any, **kwargs: Any) -> _FlowHandleLike: ...


def _load_module_from_python_path(module_path: str) -> ModuleType:
    """Load a Python module from a filesystem path."""
    path = Path(module_path).expanduser().resolve()
    if not path.exists():
        raise ValueError(f"Flow module path does not exist: {module_path}")
    if path.suffix != ".py":
        raise ValueError(
            "Flow target file must be a Python file ending in `.py` "
            f"(received: {module_path})."
        )

    module_name = f"_kitaru_cli_run_target_{abs(hash(path))}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Unable to load Python module from path: {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_flow_target(target: str) -> _FlowTarget:
    """Load `<module_or_file>:<flow_name>` into a runnable flow object."""
    module_ref, separator, attr_name = target.partition(":")
    if separator != ":" or not module_ref or not attr_name:
        raise ValueError(
            "Flow target must use `<module_or_file>:<flow_name>` format "
            f"(received: {target!r})."
        )

    try:
        if module_ref.endswith(".py"):
            module = _load_module_from_python_path(module_ref)
        else:
            module = importlib.import_module(module_ref)
    except Exception as exc:
        raise ValueError(f"Unable to import flow module `{module_ref}`: {exc}") from exc

    try:
        flow_obj = getattr(module, attr_name)
    except AttributeError as exc:
        raise ValueError(
            f"Flow target `{target}` was not found: module `{module_ref}` "
            f"has no attribute `{attr_name}`."
        ) from exc

    if not isinstance(flow_obj, _FlowTarget):
        raise ValueError(
            f"Target `{target}` is not a Kitaru flow object. "
            "Expected an object created by `@kitaru.flow` with `.start()` support."
        )

    return flow_obj


def _parse_json_value(raw_value: str, *, option_name: str) -> Any:
    """Parse a CLI JSON option value and surface user-friendly errors."""
    try:
        return json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON for `{option_name}`: {exc.msg} "
            f"(line {exc.lineno}, column {exc.colno})."
        ) from exc


def _parse_json_object(
    raw_value: str | None,
    *,
    option_name: str,
) -> dict[str, Any]:
    """Parse a CLI JSON option that must decode to an object."""
    if raw_value is None:
        return {}
    parsed = _parse_json_value(raw_value, option_name=option_name)
    if not isinstance(parsed, dict):
        raise ValueError(
            f"`{option_name}` must be a JSON object "
            '(for example: \'{"topic": "AI"}\').'
        )
    return parsed


def _format_timestamp(value: datetime | None) -> str:
    """Format optional timestamps for CLI output."""
    if value is None:
        return "not available"
    return value.isoformat(timespec="seconds")


def _status_label(status: ExecutionStatus | str) -> str:
    """Return a string label for execution/checkpoint statuses."""
    if isinstance(status, ExecutionStatus):
        return status.value
    return str(status)


def _checkpoint_summary(checkpoints: list[Any], *, max_items: int = 4) -> str:
    """Render a compact checkpoint status summary string."""
    if not checkpoints:
        return "none"

    entries: list[str] = []
    for checkpoint in checkpoints[:max_items]:
        name = str(getattr(checkpoint, "name", "unknown"))
        raw_status = getattr(checkpoint, "status", "unknown")
        status = _status_label(raw_status)
        entries.append(f"{name} ({status})")

    remaining = len(checkpoints) - len(entries)
    if remaining > 0:
        entries.append(f"... (+{remaining} more)")

    return ", ".join(entries)


def _execution_rows(execution: Execution) -> list[tuple[str, str]]:
    """Build label/value rows for execution details output."""
    pending_wait_name = "none"
    pending_wait_question = "none"
    if execution.pending_wait is not None:
        pending_wait_name = execution.pending_wait.name
        pending_wait_question = execution.pending_wait.question or "not set"

    failure_summary = "none"
    if execution.failure is not None:
        failure_summary = execution.failure.message

    rows: list[tuple[str, str]] = [
        ("Execution ID", execution.exec_id),
        ("Flow", execution.flow_name or "not available"),
        ("Status", execution.status.value),
        ("Started", _format_timestamp(execution.started_at)),
        ("Ended", _format_timestamp(execution.ended_at)),
        ("Stack", execution.stack_name or "not available"),
        ("Pending wait", pending_wait_name),
        ("Wait question", pending_wait_question),
        ("Failure", failure_summary),
        ("Checkpoints", _checkpoint_summary(execution.checkpoints)),
    ]

    return rows


def _execution_list_rows(executions: list[Execution]) -> list[tuple[str, str]]:
    """Build label/value rows for execution list output."""
    if not executions:
        return [("Executions", "none found")]

    rows: list[tuple[str, str]] = []
    for execution in executions:
        detail = (
            f"{execution.flow_name or 'unknown flow'} | "
            f"{execution.status.value} | "
            f"stack={execution.stack_name or 'not set'}"
        )
        rows.append((execution.exec_id, detail))
    return rows


def _run_rows(
    *,
    target: str,
    stack: str | None,
    execution: Execution,
) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru run` output."""
    invocation = "deploy" if stack else "start"
    return [
        ("Target", target),
        ("Invocation", invocation),
        *_execution_rows(execution),
    ]


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


def _model_rows(entries: list[ModelAliasEntry]) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru model list`."""
    if not entries:
        return [("Models", "none found")]

    rows: list[tuple[str, str]] = []
    for entry in entries:
        detail = entry.model
        if entry.secret:
            detail += f" (secret={entry.secret})"
        if entry.is_default:
            detail += " [default]"
        rows.append((entry.alias, detail))

    return rows


def _secret_visibility(secret: SecretResponse) -> str:
    """Return a human-readable visibility label for a secret."""
    return "private" if secret.private else "public"


def _parse_secret_assignments(raw_assignments: list[str]) -> dict[str, str]:
    """Parse `--KEY=value` style assignment tokens into a dictionary."""
    if not raw_assignments:
        raise ValueError(
            "Provide at least one secret assignment (for example "
            "`--OPENAI_API_KEY=sk-...`)."
        )

    parsed: dict[str, str] = {}
    idx = 0
    while idx < len(raw_assignments):
        token = raw_assignments[idx]

        if token == "--":
            idx += 1
            continue

        if not token.startswith("--"):
            raise ValueError(
                f"Invalid secret assignment `{token}`. Use `--KEY=value` format."
            )

        key_part = token[2:]
        if not key_part:
            raise ValueError("Secret key cannot be empty.")

        if "=" in key_part:
            key, value = key_part.split("=", 1)
        else:
            if idx + 1 >= len(raw_assignments):
                raise ValueError(
                    f"Missing value for secret key `{key_part}`. Use `--KEY=value`."
                )
            key = key_part
            value = raw_assignments[idx + 1]
            if value == "--" or value.startswith("--"):
                raise ValueError(
                    f"Missing value for secret key `{key}`. Use `--KEY=value`."
                )
            idx += 1

        if not _SECRET_KEY_PATTERN.fullmatch(key):
            raise ValueError(
                "Invalid secret key "
                f"`{key}`. Use env-var style names "
                "(letters, numbers, underscores; cannot start with a number)."
            )

        if value == "":
            raise ValueError(f"Secret value for key `{key}` cannot be empty.")

        if key in parsed:
            raise ValueError(f"Duplicate secret key `{key}` in one command.")

        parsed[key] = value
        idx += 1

    if not parsed:
        raise ValueError(
            "Provide at least one secret assignment (for example "
            "`--OPENAI_API_KEY=sk-...`)."
        )

    return parsed


def _resolve_secret_exact(client: Client, name_or_id: str) -> SecretResponse:
    """Fetch one secret by exact name or exact ID."""
    try:
        return client.get_secret(
            name_id_or_prefix=name_or_id,
            allow_partial_name_match=False,
            allow_partial_id_match=False,
        )
    except KeyError as exc:
        raise ValueError(f"Secret `{name_or_id}` was not found.") from exc
    except ZenKeyError as exc:
        raise ValueError(str(exc)) from exc


def _list_accessible_secrets(client: Client) -> list[SecretResponse]:
    """List all accessible secrets across all pages."""
    first_page = client.list_secrets(page=1)
    secrets = list(first_page.items)

    for page_number in range(2, first_page.total_pages + 1):
        page = client.list_secrets(page=page_number, size=first_page.max_size)
        secrets.extend(page.items)

    return secrets


def _secret_show_rows(
    secret: SecretResponse,
    *,
    show_values: bool,
) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru secrets show`."""
    keys = sorted(secret.values.keys())
    rows: list[tuple[str, str]] = [
        ("Name", secret.name),
        ("Secret ID", str(secret.id)),
        ("Visibility", _secret_visibility(secret)),
        ("Keys", ", ".join(keys) if keys else "none"),
        ("Missing values", "yes" if secret.has_missing_values else "no"),
    ]

    if show_values and keys:
        for key in keys:
            value = secret.secret_values.get(key, "unavailable")
            rows.append((f"Value ({key})", value))

    return rows


def _secret_list_rows(secrets: list[SecretResponse]) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru secrets list`."""
    if not secrets:
        return [("Secrets", "none found")]

    ordered = sorted(secrets, key=lambda secret: (secret.name.lower(), str(secret.id)))
    return [
        (secret.name, f"{secret.id} ({_secret_visibility(secret)})")
        for secret in ordered
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


@model_app.command
def register(
    alias: Annotated[
        str,
        Parameter(help="Local alias name (for example `fast`)."),
    ],
    *,
    model: Annotated[
        str,
        Parameter(
            help="Concrete LiteLLM model identifier (for example openai/gpt-4o-mini)."
        ),
    ],
    secret: Annotated[
        str | None,
        Parameter(help="Optional secret name/ID containing provider credentials."),
    ] = None,
) -> None:
    """Create or update a local model alias used by `kitaru.llm()`."""
    try:
        if secret is not None:
            _resolve_secret_exact(Client(), secret)
        alias_entry = register_model_alias(alias, model=model, secret=secret)
    except Exception as exc:
        _exit_with_error(str(exc))

    detail = f"Model: {alias_entry.model}"
    if alias_entry.secret:
        detail += f" | Secret: {alias_entry.secret}"
    if alias_entry.is_default:
        detail += " | Default alias"

    _print_success(
        f"Saved model alias: {alias_entry.alias}",
        detail=detail,
    )


@model_app.command
def list___() -> None:
    """List local model aliases used by `kitaru.llm()`."""
    try:
        aliases = list_model_aliases()
    except Exception as exc:
        _exit_with_error(str(exc))

    _emit_snapshot("Kitaru models", _model_rows(aliases))


@secrets_app.command
def set_(
    name: Annotated[
        str,
        Parameter(help="Secret name."),
    ],
    assignments: Annotated[
        list[str],
        Parameter(
            help="One or more secret assignments in `--KEY=value` form.",
            allow_leading_hyphen=True,
        ),
    ],
) -> None:
    """Create or update a secret with env-var-style key names."""
    try:
        parsed_assignments = _parse_secret_assignments(assignments)
        client = Client()

        try:
            secret = client.create_secret(
                name=name,
                values=parsed_assignments,
                private=True,
            )
            action = "Created"
        except EntityExistsError:
            existing_secret = _resolve_secret_exact(client, name)
            secret = client.update_secret(
                name_id_or_prefix=existing_secret.id,
                add_or_update_values=parsed_assignments,
            )
            action = "Updated"
    except Exception as exc:
        _exit_with_error(str(exc))

    _print_success(
        f"{action} secret: {secret.name}",
        detail=f"Secret ID: {secret.id}",
    )


@secrets_app.command
def show_(
    name_or_id: Annotated[
        str,
        Parameter(help="Secret name or ID."),
    ],
    show_values: Annotated[
        bool,
        Parameter(help="Display raw secret values in command output."),
    ] = False,
) -> None:
    """Show a secret with metadata and optional raw values."""
    try:
        secret = _resolve_secret_exact(Client(), name_or_id)
    except Exception as exc:
        _exit_with_error(str(exc))

    _emit_snapshot(
        "Kitaru secret",
        _secret_show_rows(secret, show_values=show_values),
    )


@secrets_app.command
def list__() -> None:
    """List all secrets visible to the current user context."""
    try:
        secrets = _list_accessible_secrets(Client())
    except Exception as exc:
        _exit_with_error(str(exc))

    _emit_snapshot("Kitaru secrets", _secret_list_rows(secrets))


@secrets_app.command
def delete_(
    name_or_id: Annotated[
        str,
        Parameter(help="Secret name or ID."),
    ],
) -> None:
    """Delete a secret by exact name or exact ID."""
    try:
        client = Client()
        secret = _resolve_secret_exact(client, name_or_id)
        client.delete_secret(name_id_or_prefix=str(secret.id))
    except Exception as exc:
        _exit_with_error(str(exc))

    _print_success(
        f"Deleted secret: {secret.name}",
        detail=f"Secret ID: {secret.id}",
    )


@app.command
def run(
    target: Annotated[
        str,
        Parameter(
            help=(
                "Flow target in `<module_or_file>:<flow_name>` format "
                "(for example `agent.py:content_pipeline`)."
            )
        ),
    ],
    *,
    args: Annotated[
        str | None,
        Parameter(
            help=(
                "Flow input arguments as a JSON object "
                '(for example \'{"topic": "AI safety"}\').'
            )
        ),
    ] = None,
    stack: Annotated[
        str | None,
        Parameter(help="Optional stack name/ID for deploy-style execution."),
    ] = None,
) -> None:
    """Start a flow execution from a module/file target."""
    try:
        flow_target = _load_flow_target(target)
        flow_inputs = _parse_json_object(args, option_name="--args")

        if stack:
            handle = flow_target.deploy(stack=stack, **flow_inputs)
        else:
            handle = flow_target.start(**flow_inputs)

        if not isinstance(handle, _FlowHandleLike):
            raise ValueError(
                "Flow execution did not return a valid handle with an `exec_id`."
            )
    except Exception as exc:
        _exit_with_error(str(exc))

    _print_success(f"Started flow execution: {handle.exec_id}")

    try:
        execution = KitaruClient().executions.get(handle.exec_id)
    except Exception as exc:
        _emit_snapshot(
            "Kitaru run",
            [
                ("Target", target),
                ("Invocation", "deploy" if stack else "start"),
                ("Execution ID", handle.exec_id),
            ],
            warning=(
                "Execution started successfully, but details are not available yet: "
                f"{exc}"
            ),
        )
        return

    _emit_snapshot(
        "Kitaru run",
        _run_rows(target=target, stack=stack, execution=execution),
    )


@executions_app.command
def get_(
    exec_id: Annotated[
        str,
        Parameter(help="Execution ID."),
    ],
) -> None:
    """Show detailed information for one execution."""
    try:
        execution = KitaruClient().executions.get(exec_id)
    except Exception as exc:
        _exit_with_error(str(exc))

    _emit_snapshot("Kitaru execution", _execution_rows(execution))


@executions_app.command
def list____(
    *,
    status: Annotated[
        str | None,
        Parameter(
            help="Optional status filter (running/waiting/completed/failed/cancelled)."
        ),
    ] = None,
    flow: Annotated[
        str | None,
        Parameter(help="Optional flow-name filter."),
    ] = None,
    limit: Annotated[
        int | None,
        Parameter(help="Maximum number of executions to return."),
    ] = None,
) -> None:
    """List executions with optional filters."""
    try:
        executions = KitaruClient().executions.list(
            status=status,
            flow=flow,
            limit=limit,
        )
    except Exception as exc:
        _exit_with_error(str(exc))

    _emit_snapshot("Kitaru executions", _execution_list_rows(executions))


@executions_app.command
def retry_(
    exec_id: Annotated[
        str,
        Parameter(help="Execution ID."),
    ],
) -> None:
    """Retry a failed execution as same-execution recovery."""
    try:
        execution = KitaruClient().executions.retry(exec_id)
    except Exception as exc:
        _exit_with_error(str(exc))

    _print_success(
        f"Retried execution: {execution.exec_id}",
        detail=f"Status: {execution.status.value}",
    )


@executions_app.command
def cancel_(
    exec_id: Annotated[
        str,
        Parameter(help="Execution ID."),
    ],
) -> None:
    """Cancel a running execution."""
    try:
        execution = KitaruClient().executions.cancel(exec_id)
    except Exception as exc:
        _exit_with_error(str(exc))

    _print_success(
        f"Cancelled execution: {execution.exec_id}",
        detail=f"Status: {execution.status.value}",
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
