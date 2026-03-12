"""Kitaru command-line interface."""

from __future__ import annotations

import builtins
import importlib
import importlib.util
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Annotated, Any, Protocol, runtime_checkable

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
from zenml.zen_server.deploy.deployer import LocalServerDeployer

from _kitaru_bootstrap import resolve_installed_version
from kitaru.cli_output import (
    CLIOutputFormat,
    CommandEnvelope,
    emit_command_envelope,
    normalize_output_format,
)
from kitaru.cli_output import (
    exit_with_error as _structured_exit_with_error,
)
from kitaru.client import Execution, ExecutionStatus, KitaruClient, LogEntry
from kitaru.config import (
    KITARU_AUTH_TOKEN_ENV,
    KITARU_SERVER_URL_ENV,
    ZENML_STORE_API_KEY_ENV,
    ZENML_STORE_URL_ENV,
    ActiveEnvironmentVariable,
    ModelAliasEntry,
    ResolvedLogStore,
    RunnerInfo,
    active_runner_log_store,
    list_model_aliases,
    login_to_server,
    register_model_alias,
    reset_global_log_store,
    resolve_log_store,
    set_global_log_store,
)
from kitaru.config import (
    current_runner as get_current_runner,
)
from kitaru.config import (
    list_runners as get_available_runners,
)
from kitaru.config import (
    use_runner as set_active_runner,
)
from kitaru.inspection import (
    RuntimeSnapshot,
    describe_local_server,
    serialize_execution,
    serialize_execution_summary,
    serialize_log_entry,
    serialize_model_alias,
    serialize_resolved_log_store,
    serialize_runner,
    serialize_runtime_snapshot,
    serialize_secret_detail,
    serialize_secret_summary,
)
from kitaru.inspection import (
    build_runtime_snapshot as _build_runtime_snapshot,
)
from kitaru.inspection import (
    combine_warnings as _combine_warnings,
)
from kitaru.inspection import (
    connected_to_local_server_safe as _connected_to_local_server,
)
from kitaru.inspection import (
    log_store_mismatch_details as _log_store_mismatch_details,
)
from kitaru.runtime import _submission_observer
from kitaru.terminal import (
    LiveExecutionRenderer,
    _suppress_zenml_console,
)
from kitaru.terminal import (
    is_interactive as is_terminal_interactive,
)

_UNKNOWN_VERSION = "unknown"
_SECRET_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

app = cyclopts.App(
    name="kitaru",
    help="Durable execution for AI agents.",
    version=_UNKNOWN_VERSION,
    version_flags=["--version", "-V"],
)

log_store_app = cyclopts.App(
    name="log-store",
    help="Manage global runtime log-store settings.",
)
runner_app = cyclopts.App(
    name="runner",
    help="Inspect and switch the active runner.",
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
app.command(runner_app)
app.command(secrets_app)
app.command(model_app)
app.command(executions_app)


def _sdk_version() -> str:
    """Resolve the installed SDK version lazily."""
    return resolve_installed_version()


def _apply_runtime_version() -> None:
    """Populate the CLI app version just before command dispatch."""
    app.version = _sdk_version()


@dataclass(frozen=True)
class SnapshotSection:
    """One renderable snapshot section."""

    title: str | None
    rows: list[tuple[str, str]]


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


OutputFormatOption = Annotated[
    str,
    Parameter(
        alias=["-o"],
        help='Output format: "text" (default) or "json".',
    ),
]


@runtime_checkable
class _FlowHandleLike(Protocol):
    """Protocol for flow handles returned by `.run()` / `.deploy()`."""

    @property
    def exec_id(self) -> str: ...


@runtime_checkable
class _FlowTarget(Protocol):
    """Protocol for CLI-runnable flow objects."""

    def run(self, *args: Any, **kwargs: Any) -> _FlowHandleLike: ...

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
            "Expected an object created by `@flow` with `.run()` support."
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
        ("Runner", execution.runner_name or "not available"),
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
            f"runner={execution.runner_name or 'not set'}"
        )
        rows.append((execution.exec_id, detail))
    return rows


def _run_rows(
    *,
    target: str,
    runner: str | None,
    execution: Execution,
) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru run` output."""
    invocation = "deploy" if runner else "run"
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


def _render_rich_snapshot_sections(
    title: str,
    sections: list[SnapshotSection],
    warning: str | None = None,
) -> None:
    """Render a multi-section snapshot as a styled Rich panel."""
    lines = Text()
    for index, section in enumerate(sections):
        if index > 0:
            lines.append("\n\n")
        if section.title:
            lines.append(section.title, style="bold magenta")
            lines.append("\n")
        for row_index, (label, value) in enumerate(section.rows):
            lines.append(f"  {label}: ", style="bold cyan")
            lines.append(value, style=_value_style(value))
            if row_index < len(section.rows) - 1:
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


def _resolve_output_format(raw_output: str) -> CLIOutputFormat:
    """Normalize a CLI output mode and fail with a text error if invalid."""
    try:
        return normalize_output_format(raw_output)
    except ValueError as exc:
        _structured_exit_with_error(
            "cli",
            str(exc),
            output=CLIOutputFormat.TEXT,
            error_type=type(exc).__name__,
        )


def _exit_with_error(
    command: str,
    message: str | None = None,
    *,
    output: CLIOutputFormat = CLIOutputFormat.TEXT,
    error_type: str | None = None,
) -> None:
    """Print a format-aware CLI error and exit with a non-zero status."""
    if message is None:
        message = command
        command = "cli"

    if output == CLIOutputFormat.JSON:
        _structured_exit_with_error(
            command,
            message,
            output=output,
            error_type=error_type,
        )
    if _is_interactive(stderr=True):
        err = Text("Error: ", style="bold red")
        err.append(message, style="red")
        Console(stderr=True).print(err)
    else:
        print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


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


def _clear_persisted_store_configuration(gc: GlobalConfiguration) -> None:
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
            ("Active runner", snapshot.active_runner or "unavailable"),
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
        ("Active runner", snapshot.active_runner or "unavailable"),
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


def _runner_list_rows(runners: list[RunnerInfo]) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru runner list`."""
    if not runners:
        return [("Runners", "none found")]

    return [
        (
            runner.name,
            f"{runner.id}{' (active)' if runner.is_active else ''}",
        )
        for runner in runners
    ]


def _current_runner_rows(runner: RunnerInfo) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru runner current`."""
    return [
        ("Active runner", runner.name),
        ("Runner ID", runner.id),
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


def _render_plain_snapshot_sections(
    title: str,
    sections: list[SnapshotSection],
    warning: str | None = None,
) -> str:
    """Render a multi-section snapshot as plain indented text."""
    lines = [title]
    for section in sections:
        if section.title:
            lines.append("")
            lines.append(section.title)
        for label, value in section.rows:
            lines.append(f"  {label}: {value}")
    if warning:
        lines.append("")
        lines.append(f"  Warning: {warning}")
    return "\n".join(lines)


def _emit_snapshot(
    title: str,
    rows: list[tuple[str, str]],
    warning: str | None = None,
) -> None:
    """Render key/value snapshots in rich or plain-text mode."""
    if _is_interactive():
        _render_rich_snapshot(title, rows, warning)
    else:
        print(_render_plain_snapshot(title, rows, warning))


def _emit_snapshot_sections(
    title: str,
    sections: list[SnapshotSection],
    warning: str | None = None,
) -> None:
    """Render multi-section snapshots in rich or plain-text mode."""
    if _is_interactive():
        _render_rich_snapshot_sections(title, sections, warning)
    else:
        print(_render_plain_snapshot_sections(title, sections, warning))


def _emit_json_item(
    command: str,
    item: dict[str, Any],
    *,
    output: CLIOutputFormat,
) -> None:
    """Emit a single structured JSON item when JSON mode is enabled."""
    emit_command_envelope(
        CommandEnvelope(command=command, item=item),
        output=output,
    )


def _emit_json_items(
    command: str,
    items: list[dict[str, Any]],
    *,
    output: CLIOutputFormat,
) -> None:
    """Emit a structured JSON list result when JSON mode is enabled."""
    emit_command_envelope(
        CommandEnvelope(command=command, items=items, count=len(items)),
        output=output,
    )


def _environment_rows(
    environment: list[ActiveEnvironmentVariable],
) -> list[tuple[str, str]]:
    """Build label/value rows for the active environment section."""
    return [(entry.name, entry.value) for entry in environment]


def _emit_run_snapshot(
    *,
    target: str,
    runner: str | None,
    exec_id: str,
) -> None:
    """Emit the post-run snapshot for non-interactive or deploy paths."""
    try:
        execution = KitaruClient().executions.get(exec_id)
    except Exception as exc:
        _emit_snapshot(
            "Kitaru run",
            [
                ("Target", target),
                ("Invocation", "deploy" if runner else "run"),
                ("Execution ID", exec_id),
            ],
            warning=(
                "Execution started successfully, but details are not available yet: "
                f"{exc}"
            ),
        )
        return

    _emit_snapshot(
        "Kitaru run",
        _run_rows(target=target, runner=runner, execution=execution),
    )


def _run_with_live_display(
    *,
    flow_target: _FlowTarget,
    target: str,
    flow_inputs: dict[str, Any],
) -> _FlowHandleLike:
    """Execute a flow with the live terminal renderer active."""
    renderer = LiveExecutionRenderer(target=target)
    with (
        renderer,
        _suppress_zenml_console(),
        _submission_observer(renderer.publish_exec_id),
    ):
        handle = flow_target.run(**flow_inputs)
    return handle


def _format_log_timestamp(value: str | None) -> str:
    """Render an optional ISO timestamp in a compact CLI-friendly shape."""
    if value is None:
        return "-"

    raw_value = value.strip()
    if not raw_value:
        return "-"

    normalized = raw_value
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return raw_value

    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _format_log_entry(entry: LogEntry, *, verbosity: int) -> str:
    """Format one log entry for CLI text output."""
    if verbosity <= 0:
        return entry.message

    timestamp = _format_log_timestamp(entry.timestamp)
    level = (entry.level or "INFO").upper()
    if verbosity == 1:
        return f"{timestamp} {level:<5} {entry.message}"

    module = entry.module or entry.checkpoint_name or "unknown"
    return f"{timestamp} {level:<5} [{module}] {entry.message}"


def _emit_control_message(message: str, *, output: CLIOutputFormat) -> None:
    """Emit non-log control/status text for text output mode."""
    if output == CLIOutputFormat.TEXT:
        print(message)


def _emit_json_log_event(event: str, item: dict[str, Any]) -> None:
    """Emit one JSONL log-stream event."""
    print(
        json.dumps(
            {
                "command": "executions.logs",
                "event": event,
                "item": item,
            }
        )
    )


def _emit_log_entries(
    entries: list[LogEntry],
    *,
    output: CLIOutputFormat,
    grouped: bool,
    verbosity: int,
) -> None:
    """Emit log entries in text or JSON follow-stream format."""
    if output == CLIOutputFormat.JSON:
        for entry in entries:
            _emit_json_log_event("log", serialize_log_entry(entry))
        return

    if not grouped:
        for entry in entries:
            print(_format_log_entry(entry, verbosity=verbosity))
        return

    grouped_entries: dict[str, list[LogEntry]] = {}
    for entry in entries:
        checkpoint_name = entry.checkpoint_name or "unknown"
        grouped_entries.setdefault(checkpoint_name, []).append(entry)

    for idx, (checkpoint_name, checkpoint_entries) in enumerate(
        grouped_entries.items()
    ):
        if idx > 0:
            print("")
        print(f"── checkpoint: {checkpoint_name} " + "─" * 30)
        for entry in checkpoint_entries:
            print(_format_log_entry(entry, verbosity=verbosity))


def _emit_empty_logs_message(
    exec_id: str,
    *,
    output: CLIOutputFormat,
) -> None:
    """Emit a friendly empty-state message when no runtime logs were found."""
    if output == CLIOutputFormat.JSON:
        _emit_json_items("executions.logs", [], output=output)
        return

    _emit_control_message(
        f"No log entries found for execution {exec_id}.",
        output=output,
    )
    _emit_control_message(
        "The execution may still be starting, or step logging may be disabled.",
        output=output,
    )


def _log_entry_dedup_key(entry: LogEntry) -> tuple[Any, ...]:
    """Build a stable key for follow-mode log deduplication."""
    return (
        entry.timestamp,
        entry.level,
        entry.checkpoint_name,
        entry.module,
        entry.filename,
        entry.lineno,
        entry.message,
    )


def _follow_execution_logs(
    *,
    client: KitaruClient,
    exec_id: str,
    checkpoint: str | None,
    source: str,
    limit: int | None,
    output: CLIOutputFormat,
    grouped: bool,
    verbosity: int,
    interval: float,
) -> int:
    """Poll execution logs until terminal status and stream only new entries."""
    seen_entries: builtins.set[tuple[Any, ...]] = builtins.set()
    last_wait_name: str | None = None

    while True:
        entries = client.executions.logs(
            exec_id,
            checkpoint=checkpoint,
            source=source,
            limit=limit,
        )

        new_entries: list[LogEntry] = []
        for entry in entries:
            key = _log_entry_dedup_key(entry)
            if key in seen_entries:
                continue
            seen_entries.add(key)
            new_entries.append(entry)

        if new_entries:
            _emit_log_entries(
                new_entries,
                output=output,
                grouped=grouped,
                verbosity=verbosity,
            )

        execution = client.executions.get(exec_id)
        if execution.status == ExecutionStatus.COMPLETED:
            if output == CLIOutputFormat.JSON:
                _emit_json_log_event(
                    "terminal",
                    {
                        "status": ExecutionStatus.COMPLETED.value,
                        "message": "Execution completed successfully",
                    },
                )
            else:
                _emit_control_message(
                    "[Execution completed successfully]",
                    output=output,
                )
            return 0
        if execution.status == ExecutionStatus.FAILED:
            failure_reason = execution.status_reason or "execution failed"
            if execution.failure is not None:
                failure_reason = execution.failure.message
            if output == CLIOutputFormat.JSON:
                _emit_json_log_event(
                    "terminal",
                    {
                        "status": ExecutionStatus.FAILED.value,
                        "message": failure_reason,
                    },
                )
            else:
                _emit_control_message(
                    f"[Execution failed: {failure_reason}]",
                    output=output,
                )
            return 1
        if execution.status == ExecutionStatus.CANCELLED:
            if output == CLIOutputFormat.JSON:
                _emit_json_log_event(
                    "terminal",
                    {
                        "status": ExecutionStatus.CANCELLED.value,
                        "message": "Execution cancelled",
                    },
                )
            else:
                _emit_control_message("[Execution cancelled]", output=output)
            return 1

        if execution.status == ExecutionStatus.WAITING:
            wait_name = "unknown"
            wait_id: str | None = None
            wait_question: str | None = None
            if execution.pending_wait is not None:
                wait_name = execution.pending_wait.name
                wait_id = execution.pending_wait.wait_id
                wait_question = execution.pending_wait.question
            if wait_name != last_wait_name:
                if output == CLIOutputFormat.JSON:
                    _emit_json_log_event(
                        "waiting",
                        {
                            "wait_name": wait_name,
                            "wait_id": wait_id,
                            "question": wait_question,
                        },
                    )
                else:
                    _emit_control_message(
                        f"[Execution is waiting for input on: {wait_name}]",
                        output=output,
                    )
                last_wait_name = wait_name

        time.sleep(interval)


def _logout_current_connection() -> LogoutResult:
    """Reset the active connection and clear current stored credentials."""
    gc = GlobalConfiguration()

    if _connected_to_local_server():
        LocalServerDeployer().remove_server()
        return LogoutResult(mode="local_server")

    try:
        if gc.uses_local_store:
            return LogoutResult(mode="local_store")
        server_url = gc.store_configuration.url.rstrip("/")
    except ImportError:
        return LogoutResult(
            mode="unavailable",
        )
    local_fallback_available = True
    try:
        gc.set_default_store()
    except ImportError:
        local_fallback_available = False
        _clear_persisted_store_configuration(gc)

    if server_url.startswith(("http://", "https://")):
        get_credentials_store().clear_credentials(server_url)
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
    _, mismatch_warning = _log_store_mismatch_details(snapshot)
    return serialize_resolved_log_store(
        snapshot,
        active_store=active_runner_log_store(),
        warning=mismatch_warning,
    )


def _run_payload(
    *,
    target: str,
    runner: str | None,
    exec_id: str,
) -> dict[str, Any]:
    """Build a structured payload for `kitaru run` JSON output."""
    payload: dict[str, Any] = {
        "target": target,
        "invocation": "deploy" if runner else "run",
        "exec_id": exec_id,
        "execution": None,
        "warning": None,
    }
    try:
        execution = KitaruClient().executions.get(exec_id)
    except Exception as exc:
        payload["warning"] = (
            f"Execution started successfully, but details are not available yet: {exc}"
        )
        return payload

    payload["execution"] = serialize_execution(execution)
    return payload


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
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    connected_server_url = _get_connected_server_url() or server.rstrip("/")
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
    try:
        snapshot = set_global_log_store(
            backend,
            endpoint=endpoint,
            api_key=api_key,
        )
    except ValueError as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, _log_store_payload(snapshot), output=output_format)
        return

    _print_success(
        "Saved global log-store override.",
        detail=_log_store_detail(snapshot),
    )


@log_store_app.command
def show(output: OutputFormatOption = "text") -> None:
    """Show the effective global runtime log-store configuration."""
    command = "log-store.show"
    output_format = _resolve_output_format(output)
    try:
        snapshot = resolve_log_store()
    except ValueError as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    _, mismatch_warning = _log_store_mismatch_details(snapshot)
    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, _log_store_payload(snapshot), output=output_format)
        return
    _emit_snapshot("Kitaru log store", _log_store_rows(snapshot), mismatch_warning)


@log_store_app.command
def reset(output: OutputFormatOption = "text") -> None:
    """Clear the persisted global runtime log-store override."""
    command = "log-store.reset"
    output_format = _resolve_output_format(output)
    try:
        snapshot = reset_global_log_store()
    except ValueError as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, _log_store_payload(snapshot), output=output_format)
        return

    _print_success(
        "Cleared global log-store override.",
        detail=_log_store_detail(snapshot),
    )


@runner_app.command
def list_(output: OutputFormatOption = "text") -> None:
    """List runners visible to the current user."""
    command = "runner.list"
    output_format = _resolve_output_format(output)
    try:
        runners = get_available_runners()
    except Exception as exc:  # pragma: no cover - exercised via CLI behavior
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [serialize_runner(runner) for runner in runners],
            output=output_format,
        )
        return

    _emit_snapshot("Kitaru runners", _runner_list_rows(runners))


@runner_app.command
def current(output: OutputFormatOption = "text") -> None:
    """Show the currently active runner."""
    command = "runner.current"
    output_format = _resolve_output_format(output)
    try:
        runner = get_current_runner()
    except Exception as exc:  # pragma: no cover - exercised via CLI behavior
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_runner(runner), output=output_format)
        return

    _emit_snapshot("Kitaru runner", _current_runner_rows(runner))


@runner_app.command
def use(
    runner: Annotated[
        str,
        Parameter(help="Runner name or ID to activate."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Use a runner as the active default by name or ID."""
    command = "runner.use"
    output_format = _resolve_output_format(output)
    try:
        selected_runner = set_active_runner(runner)
    except Exception as exc:  # pragma: no cover - exercised via CLI behavior
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command, serialize_runner(selected_runner), output=output_format
        )
        return

    _print_success(
        f"Activated runner: {selected_runner.name}",
        detail=f"Runner ID: {selected_runner.id}",
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
    output: OutputFormatOption = "text",
) -> None:
    """Register or update a local model alias used by `kitaru.llm()`."""
    command = "model.register"
    output_format = _resolve_output_format(output)
    try:
        if secret is not None:
            _resolve_secret_exact(Client(), secret)
        alias_entry = register_model_alias(alias, model=model, secret=secret)
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command, serialize_model_alias(alias_entry), output=output_format
        )
        return

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
def list___(output: OutputFormatOption = "text") -> None:
    """List local model aliases used by `kitaru.llm()`."""
    command = "model.list"
    output_format = _resolve_output_format(output)
    try:
        aliases = list_model_aliases()
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [serialize_model_alias(entry) for entry in aliases],
            output=output_format,
        )
        return

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
    output: OutputFormatOption = "text",
) -> None:
    """Set a secret with env-var-style key names, creating it if needed."""
    command = "secrets.set"
    output_format = _resolve_output_format(output)
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
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        payload = serialize_secret_summary(secret)
        payload["result"] = action.lower()
        _emit_json_item(command, payload, output=output_format)
        return

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
    output: OutputFormatOption = "text",
) -> None:
    """Show a secret with metadata and optional raw values."""
    command = "secrets.show"
    output_format = _resolve_output_format(output)
    try:
        secret = _resolve_secret_exact(Client(), name_or_id)
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            serialize_secret_detail(secret, show_values=show_values),
            output=output_format,
        )
        return

    _emit_snapshot(
        "Kitaru secret",
        _secret_show_rows(secret, show_values=show_values),
    )


@secrets_app.command
def list__(output: OutputFormatOption = "text") -> None:
    """List all secrets visible to the current user context."""
    command = "secrets.list"
    output_format = _resolve_output_format(output)
    try:
        secrets = _list_accessible_secrets(Client())
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        ordered = sorted(
            secrets, key=lambda secret: (secret.name.lower(), str(secret.id))
        )
        _emit_json_items(
            command,
            [serialize_secret_summary(secret) for secret in ordered],
            output=output_format,
        )
        return

    _emit_snapshot("Kitaru secrets", _secret_list_rows(secrets))


@secrets_app.command
def delete_(
    name_or_id: Annotated[
        str,
        Parameter(help="Secret name or ID."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Delete a secret by exact name or exact ID."""
    command = "secrets.delete"
    output_format = _resolve_output_format(output)
    try:
        client = Client()
        secret = _resolve_secret_exact(client, name_or_id)
        client.delete_secret(name_id_or_prefix=str(secret.id))
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        payload = serialize_secret_summary(secret)
        payload["result"] = "deleted"
        _emit_json_item(command, payload, output=output_format)
        return

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
    runner: Annotated[
        str | None,
        Parameter(help="Optional runner name/ID for deploy-style execution."),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Run a flow from a module/file target."""
    command = "run"
    output_format = _resolve_output_format(output)
    use_live = (
        is_terminal_interactive()
        and not runner
        and output_format == CLIOutputFormat.TEXT
    )

    try:
        flow_target = _load_flow_target(target)
        flow_inputs = _parse_json_object(args, option_name="--args")

        if use_live:
            handle = _run_with_live_display(
                flow_target=flow_target,
                target=target,
                flow_inputs=flow_inputs,
            )
        elif runner:
            handle = flow_target.deploy(runner=runner, **flow_inputs)
        else:
            handle = flow_target.run(**flow_inputs)

        if not isinstance(handle, _FlowHandleLike):
            raise ValueError(
                "Flow execution did not return a valid handle with an `exec_id`."
            )
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if use_live:
        return

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            _run_payload(target=target, runner=runner, exec_id=handle.exec_id),
            output=output_format,
        )
        return

    _print_success(f"Started flow execution: {handle.exec_id}")
    _emit_run_snapshot(target=target, runner=runner, exec_id=handle.exec_id)


@executions_app.command
def get_(
    exec_id: Annotated[
        str,
        Parameter(help="Execution ID."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Show detailed information for one execution."""
    command = "executions.get"
    output_format = _resolve_output_format(output)
    try:
        execution = KitaruClient().executions.get(exec_id)
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_execution(execution), output=output_format)
        return

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
    output: OutputFormatOption = "text",
) -> None:
    """List executions with optional filters."""
    command = "executions.list"
    output_format = _resolve_output_format(output)
    try:
        executions = KitaruClient().executions.list(
            status=status,
            flow=flow,
            limit=limit,
        )
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [serialize_execution_summary(execution) for execution in executions],
            output=output_format,
        )
        return

    _emit_snapshot("Kitaru executions", _execution_list_rows(executions))


@executions_app.command
def logs_(
    exec_id: Annotated[
        str,
        Parameter(help="Execution ID."),
    ],
    *,
    checkpoint: Annotated[
        str | None,
        Parameter(help="Optional checkpoint function name to filter by."),
    ] = None,
    source: Annotated[
        str,
        Parameter(
            help='Log source (default: "step"; use "runner" for run-level logs).'
        ),
    ] = "step",
    limit: Annotated[
        int | None,
        Parameter(help="Maximum total log entries to return."),
    ] = None,
    follow: Annotated[
        bool,
        Parameter(
            help="Stream new log entries until execution reaches a terminal state."
        ),
    ] = False,
    interval: Annotated[
        float,
        Parameter(help="Polling interval in seconds for `--follow`."),
    ] = 3.0,
    grouped: Annotated[
        bool,
        Parameter(help="Group output by checkpoint with section headers."),
    ] = False,
    output: OutputFormatOption = "text",
    verbosity: Annotated[
        int,
        Parameter(
            alias=["-v"],
            count=True,
            help="Increase verbosity (`-v` for level+timestamp, `-vv` for module).",
        ),
    ] = 0,
) -> None:
    """Fetch runtime log entries for an execution."""
    command = "executions.logs"
    output_format = _resolve_output_format(output)

    if grouped and output_format == CLIOutputFormat.JSON:
        _exit_with_error(
            command,
            "`--grouped` cannot be combined with `--output json`.",
            output=output_format,
        )

    if checkpoint and source.strip().lower() == "runner":
        _exit_with_error(
            command,
            "`--checkpoint` cannot be combined with `--source runner`.",
            output=output_format,
        )

    if interval <= 0:
        _exit_with_error(
            command,
            "`--interval` must be > 0.",
            output=output_format,
        )

    verbosity = min(verbosity, 2)

    client = KitaruClient()

    if follow:
        try:
            exit_code = _follow_execution_logs(
                client=client,
                exec_id=exec_id,
                checkpoint=checkpoint,
                source=source,
                limit=limit,
                output=output_format,
                grouped=grouped,
                verbosity=verbosity,
                interval=interval,
            )
        except KeyboardInterrupt:
            if output_format == CLIOutputFormat.JSON:
                _emit_json_log_event(
                    "interrupted",
                    {"message": "Log follow interrupted"},
                )
            else:
                _emit_control_message("[Log follow interrupted]", output=output_format)
            raise SystemExit(1) from None
        except Exception as exc:
            _exit_with_error(
                command,
                str(exc),
                output=output_format,
                error_type=type(exc).__name__,
            )

        raise SystemExit(exit_code)

    try:
        entries = client.executions.logs(
            exec_id,
            checkpoint=checkpoint,
            source=source,
            limit=limit,
        )
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [serialize_log_entry(entry) for entry in entries],
            output=output_format,
        )
        return

    if not entries:
        _emit_empty_logs_message(exec_id, output=output_format)
        return

    _emit_log_entries(
        entries,
        output=output_format,
        grouped=grouped,
        verbosity=verbosity,
    )


@executions_app.command
def input_(
    exec_id: Annotated[
        str,
        Parameter(help="Execution ID."),
    ],
    *,
    wait: Annotated[
        str,
        Parameter(help="Pending wait name or wait-condition ID."),
    ],
    value: Annotated[
        str,
        Parameter(
            help=(
                "Resolved wait input as JSON "
                '(for example `true` or `{"approved": false}`).'
            )
        ),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Resolve pending wait input for an execution."""
    command = "executions.input"
    output_format = _resolve_output_format(output)
    try:
        parsed_value = _parse_json_value(value, option_name="--value")
        execution = KitaruClient().executions.input(
            exec_id,
            wait=wait,
            value=parsed_value,
        )
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_execution(execution), output=output_format)
        return

    _print_success(
        f"Resolved wait input for execution: {execution.exec_id}",
        detail=f"Status: {execution.status.value}",
    )


@executions_app.command
def replay_(
    exec_id: Annotated[
        str,
        Parameter(help="Execution ID."),
    ],
    *,
    from_: Annotated[
        str,
        Parameter(
            help="Checkpoint selector (name, invocation ID, or call ID).",
            alias=["--from"],
        ),
    ],
    args: Annotated[
        str | None,
        Parameter(
            help=(
                "Flow input overrides as a JSON object "
                '(for example \'{"topic": "New topic"}\').'
            )
        ),
    ] = None,
    overrides: Annotated[
        str | None,
        Parameter(help=("Replay overrides as a JSON object with `checkpoint.*` keys.")),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Replay an execution from a checkpoint boundary."""
    command = "executions.replay"
    output_format = _resolve_output_format(output)
    try:
        flow_inputs = _parse_json_object(args, option_name="--args")
        parsed_overrides = _parse_json_object(overrides, option_name="--overrides")
        execution = KitaruClient().executions.replay(
            exec_id,
            from_=from_,
            overrides=parsed_overrides or None,
            **flow_inputs,
        )
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_execution(execution), output=output_format)
        return

    _print_success(
        f"Replayed execution: {execution.exec_id}",
        detail=f"Status: {execution.status.value}",
    )


@executions_app.command
def retry_(
    exec_id: Annotated[
        str,
        Parameter(help="Execution ID."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Retry a failed execution as same-execution recovery."""
    command = "executions.retry"
    output_format = _resolve_output_format(output)
    try:
        execution = KitaruClient().executions.retry(exec_id)
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_execution(execution), output=output_format)
        return

    _print_success(
        f"Retried execution: {execution.exec_id}",
        detail=f"Status: {execution.status.value}",
    )


@executions_app.command
def resume_(
    exec_id: Annotated[
        str,
        Parameter(help="Execution ID."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Resume a paused execution after wait input is resolved."""
    command = "executions.resume"
    output_format = _resolve_output_format(output)
    try:
        execution = KitaruClient().executions.resume(exec_id)
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_execution(execution), output=output_format)
        return

    _print_success(
        f"Resumed execution: {execution.exec_id}",
        detail=f"Status: {execution.status.value}",
    )


@executions_app.command
def cancel_(
    exec_id: Annotated[
        str,
        Parameter(help="Execution ID."),
    ],
    output: OutputFormatOption = "text",
) -> None:
    """Cancel a running execution."""
    command = "executions.cancel"
    output_format = _resolve_output_format(output)
    try:
        execution = KitaruClient().executions.cancel(exec_id)
    except Exception as exc:
        _exit_with_error(
            command,
            str(exc),
            output=output_format,
            error_type=type(exc).__name__,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_execution(execution), output=output_format)
        return

    _print_success(
        f"Cancelled execution: {execution.exec_id}",
        detail=f"Status: {execution.status.value}",
    )


@app.command
def status(output: OutputFormatOption = "text") -> None:
    """Show the current connection state and active runner context."""
    output_format = _resolve_output_format(output)
    snapshot = _build_runtime_snapshot()
    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            "status", serialize_runtime_snapshot(snapshot), output=output_format
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
    snapshot = _build_runtime_snapshot()
    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            "info", serialize_runtime_snapshot(snapshot), output=output_format
        )
        return

    _emit_snapshot(
        "Kitaru info",
        _info_rows(snapshot),
        _combine_warnings(snapshot.warning, snapshot.log_store_warning),
    )


@app.default
def main() -> None:
    """Show help when invoked without arguments."""
    app.help_print()


def cli() -> None:
    """Entry point for the `kitaru` console script."""
    _apply_runtime_version()
    app()
