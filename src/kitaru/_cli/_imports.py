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
    _emit_warning,
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
        "existing_execution_id": outcome.existing_execution_id,
        "reason": outcome.reason,
        "resolution": outcome.resolution,
    }


def _serialize_result(result: LangfuseImportResult) -> dict[str, Any]:
    return {
        "dry_run": result.dry_run,
        "source_project_id": result.source_project_id,
        "agent_name": result.agent_name,
        "project": {"name": result.project_name, "id": result.project_id},
        "stack": {
            "name": result.stack_name,
            "id": result.stack_id,
            "explicitly_selected": result.stack_was_explicit,
        },
        "artifact_store": {
            "type": result.artifact_store_type,
            "is_local": result.artifact_store_is_local,
            "is_remotely_accessible": (result.artifact_store_is_remotely_accessible),
        },
        "flow_name": result.flow_name,
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
            ("Project", f"{result.project_name} ({result.project_id})"),
            ("Stack", f"{result.stack_name} ({result.stack_id})"),
            ("Stack selection", "explicit" if result.stack_was_explicit else "active"),
            ("Storage type", result.artifact_store_type),
            (
                "Storage access",
                "local only"
                if result.artifact_store_is_local
                else "remotely accessible",
            ),
            ("Flow", result.flow_name),
            ("Available traces", str(result.total_trace_count)),
            ("Selected traces", str(result.selected_trace_count)),
            ("Outcomes", counts),
        ],
        warning=result.storage_warning,
    )
    _emit_table(
        "Trace outcomes",
        [
            "Trace ID",
            "Integrity",
            "Observations",
            "Status",
            "Execution ID",
            "Existing execution",
            "Reason",
            "Next action",
        ],
        [
            [
                outcome.trace_id,
                outcome.integrity.value,
                str(outcome.observation_count),
                outcome.status.value,
                outcome.execution_id or "-",
                outcome.existing_execution_id or "-",
                outcome.reason or "-",
                outcome.resolution or "-",
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
        Parameter(
            help="Grouping label for imported executions; the generated flow "
            "name is reported in the result."
        ),
    ],
    stack: Annotated[
        str | None,
        Parameter(
            help="Kitaru stack name or ID used for payload storage and the "
            "synthetic execution snapshot. Defaults to the active stack."
        ),
    ] = None,
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
    if write and stack is None:
        _emit_warning(
            "No --stack was specified. This import will use the active stack.",
            output=output_format,
            detail=(
                "Run a preview first to inspect the active stack and storage "
                "accessibility, or pass --stack <name-or-id>."
            ),
        )

    def _import_traces() -> LangfuseImportResult:
        client = cli_dependencies().kitaru_client()
        return client.imports.langfuse(
            path,
            source_project_id=source_project_id,
            agent_name=agent_name,
            stack=stack,
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
