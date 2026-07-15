"""External trace import CLI commands."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any

from cyclopts import Parameter

from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat
from kitaru.imports import (
    ImportOutcomeStatus,
    LangfuseImportResult,
    TraceImportOutcome,
)

from . import import_app
from ._dependencies import cli_dependencies
from ._helpers import (
    OutputFormatOption,
    _emit_json_item,
    _emit_snapshot,
    _emit_table,
    _exit_with_error,
    _resolve_output_format,
)

_UNSUCCESSFUL_STATUSES = {
    ImportOutcomeStatus.CONFLICT,
    ImportOutcomeStatus.REJECTED,
    ImportOutcomeStatus.FAILED,
}


def _serialize_outcome(outcome: TraceImportOutcome) -> dict[str, Any]:
    return {
        "trace_id": outcome.trace_id,
        "integrity": outcome.integrity.value,
        "observation_count": outcome.observation_count,
        "status": outcome.status.value,
        "execution_id": outcome.execution_id,
        "reason": outcome.reason,
    }


def _serialize_result(result: LangfuseImportResult) -> dict[str, Any]:
    return {
        "dry_run": result.dry_run,
        "source_project_id": result.source_project_id,
        "agent_name": result.agent_name,
        "total_trace_count": result.total_trace_count,
        "selected_trace_count": result.selected_trace_count,
        "counts": result.counts,
        "storage_warning": result.storage_warning,
        "outcomes": [_serialize_outcome(outcome) for outcome in result.outcomes],
    }


def _render_result(result: LangfuseImportResult) -> None:
    mode = "Preview" if result.dry_run else "Write"
    counts = (
        ", ".join(f"{status}={count}" for status, count in result.counts.items())
        or "none"
    )
    _emit_snapshot(
        "Langfuse trace import",
        [
            ("Mode", mode),
            ("Source project", result.source_project_id),
            ("Agent", result.agent_name),
            ("Available traces", str(result.total_trace_count)),
            ("Selected traces", str(result.selected_trace_count)),
            ("Outcomes", counts),
        ],
        warning=result.storage_warning,
    )
    _emit_table(
        "Trace outcomes",
        ["Trace ID", "Integrity", "Observations", "Status", "Execution ID", "Reason"],
        [
            [
                outcome.trace_id,
                outcome.integrity.value,
                str(outcome.observation_count),
                outcome.status.value,
                outcome.execution_id or "-",
                outcome.reason or "-",
            ]
            for outcome in result.outcomes
        ],
    )


@import_app.command
def langfuse(
    path: Annotated[
        Path,
        Parameter(help="Path to a Langfuse observations JSONL export."),
    ],
    *,
    source_project_id: Annotated[
        str,
        Parameter(help="Stable ID of the source Langfuse project."),
    ],
    agent_name: Annotated[
        str,
        Parameter(help="Kitaru flow name used to group imported executions."),
    ],
    trace_id: Annotated[
        list[str] | None,
        Parameter(
            name=["--trace-id"],
            negative_iterable=(),
            help="Source trace ID to import. Repeat to preserve an explicit order.",
        ),
    ] = None,
    limit: Annotated[
        int | None,
        Parameter(help="Maximum selected traces to process."),
    ] = None,
    write: Annotated[
        bool,
        Parameter(help="Persist traces instead of running a read-only preview."),
    ] = False,
    confirm_data_storage: Annotated[
        bool,
        Parameter(help="Confirm storage of full trace input and output payloads."),
    ] = False,
    allow_fragmented: Annotated[
        bool,
        Parameter(help="Allow traces whose exported graph has multiple components."),
    ] = False,
    max_workers: Annotated[
        int,
        Parameter(help="Concurrent trace operations, from 1 to 8."),
    ] = 1,
    output: OutputFormatOption = "text",
) -> None:
    """Preview or import Langfuse traces as synthetic Kitaru executions."""
    command = "import.langfuse"
    output_format = _resolve_output_format(output)
    if write and not confirm_data_storage:
        _exit_with_error(
            command,
            "`--write` requires `--confirm-data-storage` because full trace "
            "inputs and outputs will be persisted.",
            output=output_format,
        )
    if confirm_data_storage and not write:
        _exit_with_error(
            command,
            "`--confirm-data-storage` requires `--write`.",
            output=output_format,
        )

    def _import_traces() -> LangfuseImportResult:
        client = cli_dependencies().kitaru_client()
        return client.imports.langfuse(
            path,
            source_project_id=source_project_id,
            agent_name=agent_name,
            trace_ids=trace_id,
            limit=limit,
            dry_run=not write,
            confirm_data_storage=confirm_data_storage,
            allow_fragmented=allow_fragmented,
            max_workers=max_workers,
        )

    result = run_with_cli_error_boundary(
        _import_traces,
        command=command,
        output=output_format,
        exit_with_error=_exit_with_error,
    )

    if output_format == CLIOutputFormat.JSON:
        _emit_json_item(command, _serialize_result(result), output=output_format)
    else:
        _render_result(result)

    if any(outcome.status in _UNSUCCESSFUL_STATUSES for outcome in result.outcomes):
        raise SystemExit(1)
