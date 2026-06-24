"""Execution CLI commands."""

from __future__ import annotations

import json
import math
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

from cyclopts import Parameter

from kitaru._client._statistics import (
    normalize_execution_statistics_metrics,
    validate_statistics_max_groups,
)
from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat
from kitaru.client import (
    Execution,
    ExecutionStatistics,
    ExecutionStatus,
    LogEntry,
    PendingWait,
)
from kitaru.errors import (
    KitaruUsageError,
    build_recovery_command,
    format_recovery_hint,
)
from kitaru.inspection import (
    serialize_execution,
    serialize_execution_statistics,
    serialize_execution_summary,
    serialize_log_entry,
)

from . import executions_app
from ._dependencies import cli_dependencies
from ._helpers import (
    DEFAULT_LIST_PAGE,
    DEFAULT_LIST_SIZE,
    OutputFormatOption,
    _emit_json_item,
    _emit_json_items,
    _emit_pagination_note,
    _emit_snapshot,
    _emit_table,
    _exit_with_error,
    _format_table_timestamp,
    _format_timestamp,
    _is_input_interactive,
    _is_interactive,
    _paginate_items,
    _print_success,
    _resolve_output_format,
    _validate_pagination,
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
    allow_file: bool = False,
) -> dict[str, Any]:
    """Parse a CLI JSON option that must decode to an object.

    With `allow_file=True`, a leading ``@`` resolves the remainder as a
    filesystem path whose contents are parsed as JSON instead.
    """
    if raw_value is None:
        return {}

    source_label = option_name
    payload = raw_value
    if allow_file and raw_value.startswith("@"):
        path = Path(raw_value[1:]).expanduser()
        source_label = f"{option_name} file '{path}'"
        try:
            payload = path.read_text()
        except OSError as exc:
            raise ValueError(
                f"Unable to read `{option_name}` file '{path}': {exc}"
            ) from exc

    parsed = _parse_json_value(payload, option_name=source_label)
    if not isinstance(parsed, dict):
        raise ValueError(
            f"`{option_name}` must be a JSON object "
            '(for example: \'{"topic": "AI"}\').'
        )
    return parsed


def _parse_image_option(
    raw_value: str | None,
    *,
    option_name: str,
    allow_file: bool = False,
) -> str | dict[str, Any] | None:
    """Parse an image CLI option as a string shorthand or JSON object."""
    if raw_value is None:
        return None

    source_label = option_name
    payload = raw_value
    from_file = False
    path: Path | None = None
    if allow_file and raw_value.startswith("@"):
        from_file = True
        path = Path(raw_value[1:]).expanduser()
        source_label = f"{option_name} file '{path}'"
        try:
            payload = path.read_text()
        except OSError as exc:
            raise ValueError(
                f"Unable to read `{option_name}` file '{path}': {exc}"
            ) from exc

    try:
        parsed = _parse_json_value(payload, option_name=source_label)
    except ValueError:
        fallback_value = payload.strip() if from_file else raw_value
        if fallback_value.lstrip().startswith(("{", "[", '"')):
            raise
        return fallback_value

    if isinstance(parsed, (dict, str)):
        return parsed

    if not from_file:
        stripped = raw_value.lstrip()
        if not stripped.startswith(("{", "[")):
            return raw_value

    if from_file and path is not None:
        raise ValueError(
            f"`{option_name}` file '{path}' must contain a JSON string or object."
        )
    raise ValueError(
        f"`{option_name}` must be either a base image string or a JSON object."
    )


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


def _display_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _display_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        float_value = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(float_value):
        return None
    return float_value


def _format_llm_usage_summary(summary: Mapping[str, Any] | None) -> str:
    """Render a compact LLM usage summary for execution details."""
    if not summary:
        return "none"
    usage_record_count = _display_int(summary.get("usage_record_count"))
    incurred = _display_int(summary.get("incurred_usage_record_count"))
    reused = _display_int(summary.get("reused_usage_record_count"))
    total_tokens = _display_int(summary.get("total_tokens"))
    display_cost = _display_float(summary.get("display_cost_usd"))
    actual_cost = _display_float(summary.get("actual_cost_usd"))
    estimated_cost = _display_float(summary.get("estimated_cost_usd"))
    if None in (
        usage_record_count,
        incurred,
        reused,
        total_tokens,
        display_cost,
        actual_cost,
        estimated_cost,
    ):
        return "summary metadata is malformed"
    return (
        f"{usage_record_count} usage records ({incurred} incurred, {reused} reused), "
        f"{total_tokens} tokens, display cost ${display_cost:.6f} "
        f"(actual ${actual_cost:.6f}, estimated ${estimated_cost:.6f})"
    )


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
        ("LLM usage", _format_llm_usage_summary(execution.llm_usage_summary)),
    ]


def _execution_list_table(executions: list[Execution]) -> list[list[str]]:
    """Build columnar rows for execution list output."""
    return [
        [
            execution.exec_id,
            execution.flow_name or "unknown flow",
            execution.status.value,
            _format_table_timestamp(execution.started_at),
            _format_table_timestamp(execution.ended_at),
            execution.stack_name or "not set",
        ]
        for execution in executions
    ]


def _statistics_column_label(key: str) -> str:
    """Render a statistics key as a readable table column label."""
    if key == "flow_id":
        return "Flow ID"
    if key == "stack_id":
        return "Stack ID"
    words = key.replace("_", " ").replace("-", " ").split()
    return " ".join(
        word.upper() if word.lower() == "id" else word.capitalize() for word in words
    )


def _ordered_mapping_keys(mappings: Iterable[Mapping[str, Any]]) -> list[str]:
    """Return mapping keys in first-seen order across all mappings."""
    columns: list[str] = []
    for mapping in mappings:
        for key in mapping:
            if key not in columns:
                columns.append(key)
    return columns


def _execution_statistics_metric_columns(
    statistics: ExecutionStatistics,
    requested_metric_names: Sequence[str] = (),
) -> list[str]:
    """Return metric columns, preferring the user's requested metric order."""
    response_columns = _ordered_mapping_keys(
        group.metrics for group in statistics.groups
    )
    columns = [
        metric_name
        for metric_name in requested_metric_names
        if metric_name in response_columns
    ]
    columns.extend(
        metric_name for metric_name in response_columns if metric_name not in columns
    )
    return columns


def _execution_statistics_table(
    statistics: ExecutionStatistics,
    *,
    requested_metric_names: Sequence[str] = (),
) -> tuple[list[str], list[list[str]]]:
    """Build dynamic table headers and rows for execution statistics."""
    group_columns = _ordered_mapping_keys(group.keys for group in statistics.groups)
    metric_columns = _execution_statistics_metric_columns(
        statistics,
        requested_metric_names=requested_metric_names,
    )
    columns = [
        *[_statistics_column_label(column) for column in group_columns],
        "Executions",
        *[_statistics_column_label(column) for column in metric_columns],
    ]
    rows = [
        [
            *[str(group.keys.get(column, "")) for column in group_columns],
            str(group.execution_count),
            *[
                "" if group.metrics.get(column) is None else str(group.metrics[column])
                for column in metric_columns
            ],
        ]
        for group in statistics.groups
    ]
    return columns, rows


def _format_log_timestamp(value: str | None) -> str:
    """Render an optional ISO timestamp in a compact CLI-friendly shape."""
    return _format_table_timestamp(value)


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


def _emit_json_log_event(
    event: str,
    item: dict[str, Any],
    *,
    command: str = "executions.logs",
) -> None:
    """Emit one JSONL log-stream event."""
    print(
        json.dumps(
            {
                "command": command,
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
    command: str = "executions.logs",
) -> None:
    """Emit log entries in text or JSON follow-stream format."""
    if output == CLIOutputFormat.JSON:
        for entry in entries:
            _emit_json_log_event("log", serialize_log_entry(entry), command=command)
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
    command: str = "executions.logs",
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
                command=command,
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
                    command=command,
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
            status_value = ExecutionStatus.FAILED.value
            recovery_cmd = build_recovery_command(exec_id, status=status_value)
            if output == CLIOutputFormat.JSON:
                terminal_item: dict[str, Any] = {
                    "status": status_value,
                    "message": failure_reason,
                }
                if recovery_cmd:
                    terminal_item["recovery_command"] = recovery_cmd
                _emit_json_log_event("terminal", terminal_item, command=command)
            else:
                _emit_control_message(
                    f"[Execution failed: {failure_reason}]",
                    output=output,
                )
                hint = format_recovery_hint(exec_id, status=status_value)
                if hint:
                    _emit_control_message(hint, output=output)
            return 1
        if execution.status == ExecutionStatus.CANCELLED:
            if output == CLIOutputFormat.JSON:
                _emit_json_log_event(
                    "terminal",
                    {
                        "status": ExecutionStatus.CANCELLED.value,
                        "message": "Execution cancelled",
                    },
                    command=command,
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
                        command=command,
                    )
                else:
                    _emit_control_message(
                        f"[Execution is waiting for input on: {wait_name}]",
                        output=output,
                    )
                last_wait_name = wait_name

        cli_dependencies().sleep(interval)


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
        lambda: cli_dependencies().kitaru_client().executions.get(exec_id),
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
        Parameter(
            help=(
                "Legacy shortcut for the first page size. Cannot be combined "
                "with `--page` or `--size`."
            ),
        ),
    ] = None,
    page: Annotated[
        int | None, Parameter(help="1-based page number to return.")
    ] = None,
    size: Annotated[
        int | None, Parameter(help="Number of items to return per page.")
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """List executions with optional filters."""
    command = "executions.list"
    output_format = _resolve_output_format(output)
    if limit is not None:
        if isinstance(limit, bool) or limit < 1:
            _exit_with_error(
                command,
                "`--limit` must be >= 1 when provided.",
                output=output_format,
            )
        if page is not None or size is not None:
            _exit_with_error(
                command,
                (
                    "`--limit` cannot be combined with `--page` or `--size`; "
                    "use either `--limit N` or `--page 1 --size N`."
                ),
                output=output_format,
            )

    resolved_page = DEFAULT_LIST_PAGE if page is None else page
    resolved_size = DEFAULT_LIST_SIZE if size is None else size
    resolved_page, resolved_size = _validate_pagination(
        page=resolved_page,
        size=resolved_size,
        command=command,
        output=output_format,
    )

    def _list_executions() -> list[Execution]:
        client = cli_dependencies().kitaru_client()
        if limit is not None:
            return client.executions.list(status=status, flow=flow, limit=limit)
        return client.executions.list(
            status=status,
            flow=flow,
            page=resolved_page,
            size=resolved_size,
        )

    executions = run_with_cli_error_boundary(
        _list_executions,
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
        ["ID", "Flow", "Status", "Started", "Ended", "Stack"],
        _execution_list_table(executions),
    )
    if limit is None:
        _emit_pagination_note(
            page=resolved_page,
            size=resolved_size,
            returned_count=len(executions),
            output=output_format,
        )


@executions_app.command
def statistics(
    *,
    group_by: Annotated[
        list[str] | None,
        Parameter(
            name=["--group-by"],
            help=(
                "Grouping to apply. Repeat for multiple groupings. Supported "
                "values: status, flow, stack, tag, time:hour, time:day, "
                "time:week, time:month, metadata:<key>."
            ),
        ),
    ] = None,
    metric: Annotated[
        list[str] | None,
        Parameter(
            name=["--metric"],
            help=(
                "Metric to compute. Repeat for multiple metrics. Use "
                "<name>:<source>:<avg|sum|min|max> for built-in sources "
                "(duration, step_count, cached_step_count, output_artifact_count) "
                "or <name>:metadata:<metadata_key>:<avg|sum|min|max>."
            ),
        ),
    ] = None,
    flow: Annotated[
        str | None,
        Parameter(help="Optional flow-name or flow-ID filter."),
    ] = None,
    status: Annotated[
        str | None,
        Parameter(
            help="Optional status filter (running/waiting/completed/failed/cancelled)."
        ),
    ] = None,
    stack: Annotated[
        str | None,
        Parameter(help="Optional stack-name or stack-ID filter."),
    ] = None,
    tag: Annotated[
        list[str] | None,
        Parameter(
            name=["--tag"],
            help="Tag filter. Repeat to require multiple tags.",
        ),
    ] = None,
    max_groups: Annotated[
        int,
        Parameter(help="Maximum number of public statistics groups to return."),
    ] = 1000,
    page: Annotated[
        int | None, Parameter(help="1-based page number to return.")
    ] = None,
    size: Annotated[
        int | None, Parameter(help="Number of groups to return per page.")
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Show grouped execution statistics."""
    command = "executions.statistics"
    output_format = _resolve_output_format(output)
    pagination_requested = page is not None or size is not None
    resolved_page = DEFAULT_LIST_PAGE if page is None else page
    resolved_size = DEFAULT_LIST_SIZE if size is None else size
    if pagination_requested:
        resolved_page, resolved_size = _validate_pagination(
            page=resolved_page,
            size=resolved_size,
            command=command,
            output=output_format,
        )
    try:
        validate_statistics_max_groups(max_groups, label="--max-groups")
    except KitaruUsageError as exc:
        _exit_with_error(command, str(exc), output=output_format)

    statistics_result = run_with_cli_error_boundary(
        lambda: (
            cli_dependencies()
            .kitaru_client()
            .executions.statistics(
                group_by=group_by or [],
                metrics=metric or [],
                flow=flow,
                status=status,
                stack=stack,
                tags=tag,
                max_groups=max_groups,
            )
        ),
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    display_statistics = statistics_result
    if pagination_requested:
        display_statistics = ExecutionStatistics(
            groups=_paginate_items(
                statistics_result.groups,
                page=resolved_page,
                size=resolved_size,
            ),
            truncated=statistics_result.truncated,
        )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(
            command,
            serialize_execution_statistics(display_statistics),
            output=output_format,
        )
        return

    requested_metric_names = [
        metric.name for metric in normalize_execution_statistics_metrics(metric or [])
    ]
    columns, rows = _execution_statistics_table(
        display_statistics,
        requested_metric_names=requested_metric_names,
    )
    _emit_table("Kitaru execution statistics", columns, rows)
    if pagination_requested:
        _emit_pagination_note(
            page=resolved_page,
            size=resolved_size,
            returned_count=len(display_statistics.groups),
            total_count=len(statistics_result.groups),
            output=output_format,
        )
    if display_statistics.truncated:
        print(
            f"Results truncated at --max-groups {max_groups}. "
            "Narrow filters or increase --max-groups."
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
    client = cli_dependencies().kitaru_client()

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


@dataclass(frozen=True)
class _InteractiveWaitCandidate:
    """One wait condition to present during interactive resolution."""

    execution: Execution
    wait: PendingWait


def _auto_detect_single_pending_wait(
    client: Any,
    exec_id: str,
) -> PendingWait:
    """Return the single pending wait for an execution, or raise."""
    pending = client.executions.pending_waits(exec_id)
    if not pending:
        raise ValueError(f"Execution '{exec_id}' has no pending waits to resolve.")
    if len(pending) > 1:
        names = ", ".join(w.name for w in pending)
        raise ValueError(
            f"Execution '{exec_id}' has multiple pending waits ({names}). "
            "Use `kitaru executions input --interactive` to resolve them."
        )
    return pending[0]


def _collect_interactive_wait_candidates(
    client: Any,
    exec_id: str | None,
) -> list[_InteractiveWaitCandidate]:
    """Build the list of wait candidates for interactive resolution."""
    candidates: list[_InteractiveWaitCandidate] = []

    if exec_id is not None:
        execution = client.executions.get(exec_id)
        pending = client.executions.pending_waits(exec_id)
        for wait in pending:
            candidates.append(_InteractiveWaitCandidate(execution=execution, wait=wait))
    else:
        executions = client.executions.list(status="waiting")
        for execution in executions:
            pending = client.executions.pending_waits(execution.exec_id)
            for wait in pending:
                candidates.append(
                    _InteractiveWaitCandidate(execution=execution, wait=wait)
                )

    return candidates


def _render_interactive_wait_candidate(
    candidate: _InteractiveWaitCandidate,
    index: int,
    total: int,
) -> None:
    """Print one wait candidate for interactive review."""
    wait = candidate.wait
    execution = candidate.execution
    print(f"\n{'─' * 50}")
    print(f"  [{index + 1}/{total}]")
    print(f"  Execution:  {execution.exec_id}")
    print(f"  Flow:       {execution.flow_name or 'unknown'}")
    print(f"  Status:     {execution.status.value}")
    print(f"  Wait name:  {wait.name}")
    print(f"  Wait ID:    {wait.wait_id}")
    print(f"  Question:   {wait.question or 'not set'}")
    if wait.entered_waiting_at is not None:
        print(f"  Waiting since: {_format_timestamp(wait.entered_waiting_at)}")
    if wait.schema:
        print("  JSON schema:")
        print(json.dumps(wait.schema, indent=2, sort_keys=True))
    else:
        print("  JSON schema: not provided")
    print(f"{'─' * 50}")


def _prompt_interactive_action() -> str:
    """Prompt for an interactive action choice."""
    while True:
        try:
            response = (
                input("Action [c=continue, a=abort, s=skip, q=quit]: ").strip().lower()
            )
        except EOFError:
            return "quit"
        if response in ("c", "continue"):
            return "continue"
        if response in ("a", "abort"):
            return "abort"
        if response in ("s", "skip"):
            return "skip"
        if response in ("q", "quit"):
            return "quit"
        print(f"Unknown action: {response!r}. Use c, a, s, or q.")


def _prompt_interactive_value() -> Any:
    """Prompt for a JSON value to continue a wait condition."""
    while True:
        try:
            raw = input("JSON value for result (empty for null): ").strip()
        except EOFError:
            return None
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            print(
                f"Invalid JSON: {exc.msg} "
                f"(line {exc.lineno}, column {exc.colno}). "
                "Try again."
            )


def _run_interactive_input_flow(
    client: Any,
    exec_id: str | None,
) -> int:
    """Run the interactive wait-resolution loop. Returns exit code."""
    candidates = _collect_interactive_wait_candidates(client, exec_id)
    if not candidates:
        scope = f"execution '{exec_id}'" if exec_id else "any execution"
        print(f"No pending waits found for {scope}.")
        return 0

    had_failures = False
    for index, candidate in enumerate(candidates):
        _render_interactive_wait_candidate(candidate, index, len(candidates))
        action = _prompt_interactive_action()

        if action == "quit":
            break

        if action == "skip":
            continue

        wait = candidate.wait
        execution = candidate.execution

        if action == "continue":
            value = _prompt_interactive_value()
            try:
                updated = client.executions.input(
                    execution.exec_id,
                    wait=wait.wait_id,
                    value=value,
                )
                _print_success(
                    f"Resolved wait input for execution: {updated.exec_id}",
                    detail=f"Status: {updated.status.value}",
                )
            except Exception as exc:
                print(f"Error resolving wait '{wait.name}': {exc}", file=sys.stderr)
                had_failures = True

        elif action == "abort":
            try:
                updated = client.executions.abort_wait(
                    execution.exec_id,
                    wait=wait.wait_id,
                )
                _print_success(
                    f"Aborted wait for execution: {updated.exec_id}",
                    detail=f"Status: {updated.status.value}",
                )
            except Exception as exc:
                print(f"Error aborting wait '{wait.name}': {exc}", file=sys.stderr)
                had_failures = True

    return 1 if had_failures else 0


@executions_app.command
def input_(
    exec_id: Annotated[
        str | None,
        Parameter(
            help=(
                "Execution ID. Required in non-interactive mode. "
                "Omit with --interactive to sweep all waiting executions."
            ),
        ),
    ] = None,
    *,
    interactive: Annotated[
        bool,
        Parameter(
            alias=["-i"],
            help="Interactively review and resolve pending waits.",
        ),
    ] = False,
    abort: Annotated[
        bool,
        Parameter(help="Abort the pending wait instead of continuing."),
    ] = False,
    value: Annotated[
        str | None,
        Parameter(
            help=(
                "Resolved wait input as JSON "
                '(for example `true` or `{"approved": false}`). '
                "Required in non-interactive continue mode."
            )
        ),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Resolve pending wait input for a non-interactive or timed-out execution."""
    command = "executions.input"
    output_format = _resolve_output_format(output)

    if interactive:
        if value is not None:
            _exit_with_error(
                command,
                "`--value` cannot be used with `--interactive`.",
                output=output_format,
            )
        if abort:
            _exit_with_error(
                command,
                "`--abort` cannot be used with `--interactive`.",
                output=output_format,
            )
        if output_format == CLIOutputFormat.JSON:
            _exit_with_error(
                command,
                "`--output json` cannot be used with `--interactive`.",
                output=output_format,
            )
        # stdin (for prompts) and stdout (for formatted output) must both be TTYs
        if not _is_input_interactive() or not _is_interactive():
            _exit_with_error(
                command,
                "Interactive mode requires a terminal (TTY) for input and output.",
                output=output_format,
            )

        client = cli_dependencies().kitaru_client()
        try:
            exit_code = _run_interactive_input_flow(client, exec_id)
        except KeyboardInterrupt:
            print("\nInterrupted.", file=sys.stderr)
            raise SystemExit(1) from None
        raise SystemExit(exit_code)

    if exec_id is None:
        _exit_with_error(
            command,
            "Execution ID is required in non-interactive mode. "
            "Use `--interactive` to sweep all waiting executions.",
            output=output_format,
        )

    if abort:
        if value is not None:
            _exit_with_error(
                command,
                "`--value` cannot be used with `--abort`.",
                output=output_format,
            )

        def _abort_wait() -> Execution:
            client = cli_dependencies().kitaru_client()
            wait = _auto_detect_single_pending_wait(client, exec_id)
            return client.executions.abort_wait(exec_id, wait=wait.wait_id)

        execution = run_with_cli_error_boundary(
            _abort_wait,
            command=command,
            output=output_format,
            exit_with_error=_exit_with_error,
        )

        if output_format == CLIOutputFormat.JSON:
            _emit_json_item(
                command, serialize_execution(execution), output=output_format
            )
            return

        _print_success(
            f"Aborted wait for execution: {execution.exec_id}",
            detail=f"Status: {execution.status.value}",
        )
        return

    if value is None:
        _exit_with_error(
            command,
            "`--value` is required (or use `--abort` / `--interactive`).",
            output=output_format,
        )

    def _resolve_input() -> Execution:
        parsed_value = _parse_json_value(value, option_name="--value")
        client = cli_dependencies().kitaru_client()
        wait = _auto_detect_single_pending_wait(client, exec_id)
        return client.executions.input(exec_id, wait=wait.wait_id, value=parsed_value)

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
    at: Annotated[
        str,
        Parameter(
            help=(
                "Checkpoint selector for the replay cut "
                "(name, invocation ID, or call ID)."
            ),
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
    input: Annotated[
        str | None,
        Parameter(
            help="Checkpoint input overrides as a JSON object keyed by selector."
        ),
    ] = None,
    mock_output: Annotated[
        str | None,
        Parameter(
            help=(
                "Checkpoint output mocks as a JSON object keyed by "
                "tool/checkpoint name."
            ),
            alias=["--mock-output"],
        ),
    ] = None,
    tool: Annotated[
        str | None,
        Parameter(
            help="Tool implementation overrides as a JSON object of import paths."
        ),
    ] = None,
    llm_model: Annotated[
        str | None,
        Parameter(help="Model alias applied to llm_call checkpoints in the live tail."),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Replay an execution from a checkpoint cut point."""
    command = "executions.replay"
    output_format = _resolve_output_format(output)

    def _replay_execution() -> Execution:
        flow_inputs = _parse_json_object(args, option_name="--args")
        parsed_input = _parse_json_object(input, option_name="--input")
        parsed_output = _parse_json_object(mock_output, option_name="--mock-output")
        parsed_tool = _parse_json_object(tool, option_name="--tool")
        return (
            cli_dependencies()
            .kitaru_client()
            .executions.replay(
                exec_id,
                at=at,
                input=parsed_input or None,
                output=parsed_output or None,
                tool=parsed_tool or None,
                llm_model=llm_model,
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
        lambda: cli_dependencies().kitaru_client().executions.retry(exec_id),
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
    """Resume a paused execution when it did not continue automatically after input."""
    command = "executions.resume"
    output_format = _resolve_output_format(output)
    execution = run_with_cli_error_boundary(
        lambda: cli_dependencies().kitaru_client().executions.resume(exec_id),
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
        lambda: cli_dependencies().kitaru_client().executions.cancel(exec_id),
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
