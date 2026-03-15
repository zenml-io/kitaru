"""Execution and run CLI commands."""

from __future__ import annotations

import json
from datetime import datetime
from types import ModuleType
from typing import Annotated, Any

from cyclopts import Parameter

from kitaru import _flow_loading
from kitaru._flow_loading import _FlowHandleLike, _FlowTarget
from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat
from kitaru.client import Execution, ExecutionStatus, LogEntry
from kitaru.inspection import (
    serialize_execution,
    serialize_execution_summary,
    serialize_log_entry,
)
from kitaru.runtime import _submission_observer
from kitaru.terminal import LiveExecutionRenderer, _suppress_zenml_console
from kitaru.terminal import is_interactive as is_terminal_interactive

from . import app, executions_app
from ._helpers import (
    OutputFormatOption,
    _emit_json_item,
    _emit_json_items,
    _emit_snapshot,
    _emit_table,
    _exit_with_error,
    _facade_module,
    _format_timestamp,
    _print_success,
    _resolve_output_format,
)


def _load_module_from_python_path(module_path: str) -> ModuleType:
    """Load a Python module from a filesystem path."""
    return _flow_loading._load_module_from_python_path(
        module_path,
        module_name_prefix="_kitaru_cli_run_target_",
    )


def _load_flow_target(target: str) -> _FlowTarget:
    """Load `<module_or_file>:<flow_name>` into a runnable flow object."""
    return _flow_loading._load_flow_target(
        target,
        module_name_prefix="_kitaru_cli_run_target_",
    )


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

    return [
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


def _execution_list_table(executions: list[Execution]) -> list[list[str]]:
    """Build columnar rows for execution list output."""
    return [
        [
            execution.exec_id,
            execution.flow_name or "unknown flow",
            execution.status.value,
            execution.stack_name or "not set",
        ]
        for execution in executions
    ]


def _run_rows(
    *,
    target: str,
    stack: str | None,
    execution: Execution,
) -> list[tuple[str, str]]:
    """Build label/value rows for `kitaru run` output."""
    invocation = "deploy" if stack else "run"
    return [
        ("Target", target),
        ("Invocation", invocation),
        *_execution_rows(execution),
    ]


def _emit_run_snapshot(
    *,
    target: str,
    stack: str | None,
    exec_id: str,
) -> None:
    """Emit the post-run snapshot for non-interactive or deploy paths."""
    facade = _facade_module()
    try:
        execution = facade.KitaruClient().executions.get(exec_id)
    except Exception as exc:
        _emit_snapshot(
            "Kitaru run",
            [
                ("Target", target),
                ("Invocation", "deploy" if stack else "run"),
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
        _run_rows(target=target, stack=stack, execution=execution),
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
    client: Any,
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
    seen_entries: set[tuple[Any, ...]] = set()
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

        _facade_module().time.sleep(interval)


def _run_payload(
    *,
    target: str,
    stack: str | None,
    exec_id: str,
) -> dict[str, Any]:
    """Build a structured payload for `kitaru run` JSON output."""
    payload: dict[str, Any] = {
        "target": target,
        "invocation": "deploy" if stack else "run",
        "exec_id": exec_id,
        "execution": None,
        "warning": None,
    }
    try:
        execution = _facade_module().KitaruClient().executions.get(exec_id)
    except Exception as exc:
        payload["warning"] = (
            f"Execution started successfully, but details are not available yet: {exc}"
        )
        return payload

    payload["execution"] = serialize_execution(execution)
    return payload


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
    output: OutputFormatOption = "text",
) -> None:
    """Run a flow from a module/file target."""
    command = "run"
    output_format = _resolve_output_format(output)
    use_live = (
        is_terminal_interactive()
        and not stack
        and output_format == CLIOutputFormat.TEXT
    )

    facade = _facade_module()

    def _start_execution() -> _FlowHandleLike:
        flow_target = facade._load_flow_target(target)
        flow_inputs = _parse_json_object(args, option_name="--args")

        if use_live:
            handle = _run_with_live_display(
                flow_target=flow_target,
                target=target,
                flow_inputs=flow_inputs,
            )
        elif stack:
            handle = flow_target.deploy(stack=stack, **flow_inputs)
        else:
            handle = flow_target.run(**flow_inputs)

        if not isinstance(handle, _FlowHandleLike):
            raise ValueError(
                "Flow execution did not return a valid handle with an `exec_id`."
            )
        return handle

    handle = run_with_cli_error_boundary(
        _start_execution,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if use_live:
        return

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            _run_payload(target=target, stack=stack, exec_id=handle.exec_id),
            output=output_format,
        )
        return

    _print_success(f"Started flow execution: {handle.exec_id}")
    _emit_run_snapshot(target=target, stack=stack, exec_id=handle.exec_id)


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
    execution = run_with_cli_error_boundary(
        lambda: _facade_module().KitaruClient().executions.get(exec_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
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
    executions = run_with_cli_error_boundary(
        lambda: (
            _facade_module()
            .KitaruClient()
            .executions.list(
                status=status,
                flow=flow,
                limit=limit,
            )
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_items(
            command,
            [serialize_execution_summary(execution) for execution in executions],
            output=output_format,
        )
        return

    _emit_table(
        "Kitaru executions",
        ["ID", "Flow", "Status", "Stack"],
        _execution_list_table(executions),
    )


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
    client = _facade_module().KitaruClient()

    if follow:
        try:
            exit_code = run_with_cli_error_boundary(
                lambda: _follow_execution_logs(
                    client=client,
                    exec_id=exec_id,
                    checkpoint=checkpoint,
                    source=source,
                    limit=limit,
                    output=output_format,
                    grouped=grouped,
                    verbosity=verbosity,
                    interval=interval,
                ),
                command=command,
                output=output_format,
                exit_with_error=_exit_with_error,
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

        raise SystemExit(exit_code)

    entries = run_with_cli_error_boundary(
        lambda: client.executions.logs(
            exec_id,
            checkpoint=checkpoint,
            source=source,
            limit=limit,
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
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

    def _resolve_input() -> Execution:
        parsed_value = _parse_json_value(value, option_name="--value")
        return (
            _facade_module()
            .KitaruClient()
            .executions.input(
                exec_id,
                wait=wait,
                value=parsed_value,
            )
        )

    execution = run_with_cli_error_boundary(
        _resolve_input,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
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

    def _replay_execution() -> Execution:
        flow_inputs = _parse_json_object(args, option_name="--args")
        parsed_overrides = _parse_json_object(overrides, option_name="--overrides")
        return (
            _facade_module()
            .KitaruClient()
            .executions.replay(
                exec_id,
                from_=from_,
                overrides=parsed_overrides or None,
                **flow_inputs,
            )
        )

    execution = run_with_cli_error_boundary(
        _replay_execution,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
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
    execution = run_with_cli_error_boundary(
        lambda: _facade_module().KitaruClient().executions.retry(exec_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
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
    execution = run_with_cli_error_boundary(
        lambda: _facade_module().KitaruClient().executions.resume(exec_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
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
    execution = run_with_cli_error_boundary(
        lambda: _facade_module().KitaruClient().executions.cancel(exec_id),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, serialize_execution(execution), output=output_format)
        return

    _print_success(
        f"Cancelled execution: {execution.exec_id}",
        detail=f"Status: {execution.status.value}",
    )
