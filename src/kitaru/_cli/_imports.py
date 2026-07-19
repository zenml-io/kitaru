"""External trace import CLI commands."""

from __future__ import annotations

from typing import Annotated, Any

from cyclopts import Parameter

from kitaru._interface_errors import run_with_cli_error_boundary
from kitaru.cli_output import CLIOutputFormat
from kitaru.imports import (
    ImportOutcomeStatus,
    LangfuseImportError,
    LangfuseImportResult,
    LangfuseSourceKind,
    ReplayReadinessSummary,
    TraceImportOutcome,
)
from kitaru.imports._source import parse_langfuse_source

from . import import_app
from ._dependencies import cli_dependencies
from ._helpers import (
    OutputFormatOption,
    _emit_json_item,
    _emit_snapshot,
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
        "attribution": (
            outcome.attribution.model_dump(mode="json")
            if outcome.attribution is not None
            else None
        ),
        "raw_evidence_digest": outcome.raw_evidence_digest,
        "raw_evidence_artifact_id": outcome.raw_evidence_artifact_id,
        "raw_evidence_schema_version": outcome.raw_evidence_schema_version,
        "replay_bundle_digest": outcome.replay_bundle_digest,
        "replay_bundle_artifact_id": outcome.replay_bundle_artifact_id,
        "replay_bundle_schema_version": outcome.replay_bundle_schema_version,
        "replay_readiness": (
            outcome.replay_readiness.model_dump(mode="json")
            if outcome.replay_readiness is not None
            else None
        ),
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
        "agent": {"name": result.agent_name, "id": result.agent_id},
        "agent_version": {
            "id": result.agent_version_id,
            "pipeline_id": result.pipeline_id,
            "pipeline_name": result.pipeline_name,
            "requested_version": result.requested_version,
            "requested_alias": result.requested_alias,
        },
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
        "attribution_counts": result.attribution_counts,
        "cohort_tag": result.cohort_tag,
        "storage_warning": result.storage_warning,
        "fetch_provenance": (
            {
                "api_resource": result.fetch_provenance.api_resource,
                "base_url": result.fetch_provenance.base_url,
                "base_url_source": result.fetch_provenance.base_url_source,
                "field_groups": list(result.fetch_provenance.field_groups),
                "page_count": result.fetch_provenance.page_count,
            }
            if result.fetch_provenance is not None
            else None
        ),
        "outcomes": [_serialize_outcome(outcome) for outcome in result.outcomes],
    }


def _render_result(result: LangfuseImportResult) -> None:
    mode = "Preview" if result.dry_run else "Write"
    counts = (
        ", ".join(f"{status}={count}" for status, count in result.counts.items())
        or "none"
    )
    attribution_counts = (
        ", ".join(
            f"{status}={count}" for status, count in result.attribution_counts.items()
        )
        or "none"
    )
    _emit_snapshot(
        "Langfuse trace import",
        [
            ("Mode", mode),
            ("Source project", result.source_project_id),
            ("Agent", f"{result.agent_name} ({result.agent_id})"),
            ("Agent version ID", result.agent_version_id or "-"),
            ("Requested version", result.requested_version or "-"),
            ("Requested label", result.requested_alias or "-"),
            ("Agent Project", f"{result.project_name} ({result.project_id})"),
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
            ("Attribution", attribution_counts),
            (
                "Fetch provenance",
                (
                    f"{result.fetch_provenance.api_resource} at "
                    f"{result.fetch_provenance.base_url}; "
                    f"pages={result.fetch_provenance.page_count}; "
                    "fields=" + ",".join(result.fetch_provenance.field_groups)
                    if result.fetch_provenance is not None
                    else "-"
                ),
            ),
            ("Cohort tag", result.cohort_tag or "-"),
            ("Outcomes", counts),
        ],
        warning=result.storage_warning,
    )
    for outcome in result.outcomes:
        rows = [
            ("Status", outcome.status.value),
            ("Integrity", outcome.integrity.value),
            ("Observations", str(outcome.observation_count)),
            (
                "Attribution",
                (
                    outcome.attribution.status.value
                    if outcome.attribution is not None
                    else "-"
                ),
            ),
            ("Provider evidence", _render_provider_stamps(outcome)),
            ("Replay readiness", _render_readiness(outcome.replay_readiness)),
            ("Raw evidence SHA-256", outcome.raw_evidence_digest or "-"),
            ("Replay bundle SHA-256", outcome.replay_bundle_digest or "-"),
        ]
        if outcome.execution_id is not None:
            rows.append(("Execution ID", outcome.execution_id))
        if outcome.existing_execution_id is not None:
            rows.append(("Existing execution", outcome.existing_execution_id))
        if outcome.reason is not None:
            rows.append(("Problem", outcome.reason))
        if outcome.resolution is not None:
            rows.append(("Next action", outcome.resolution))
        _emit_snapshot(f"Trace: {outcome.trace_id}", rows)


def _render_provider_stamps(outcome: TraceImportOutcome) -> str:
    if outcome.attribution is None or not outcome.attribution.stamps:
        return "-"
    return ", ".join(
        f"{stamp.kind.value}:{stamp.value}" for stamp in outcome.attribution.stamps
    )


def _render_readiness(readiness: ReplayReadinessSummary | None) -> str:
    if readiness is None:
        return "-"
    return ", ".join(
        f"{capability.capability.value}={capability.status.value}"
        for capability in (
            readiness.root_input_candidate_rerun,
            readiness.model_message_reconstruction,
            readiness.tool_result_boundary_reconstruction,
            readiness.recorded_response_matching,
            readiness.candidate_tool_compatibility,
        )
    )


@import_app.command
def langfuse(
    source: Annotated[
        str,
        Parameter(
            help="Langfuse observations JSONL path or langfuse://trace/TRACE_ID URI."
        ),
    ],
    *,
    source_project_id: Annotated[
        str | None,
        Parameter(
            help="Stable source Langfuse project ID. Required for JSONL; "
            "optional and validated for trace URIs."
        ),
    ] = None,
    agent: Annotated[
        str,
        Parameter(help="Exact Kitaru Agent name or ID that produced the traces."),
    ],
    version: Annotated[
        str,
        Parameter(
            name="--agent-version",
            help="Exact source AgentVersion ID or label.",
        ),
    ],
    stack: Annotated[
        str | None,
        Parameter(
            help="Kitaru stack name or ID used for payload storage and the "
            "imported execution. Defaults to the active stack."
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
        Parameter(
            help="Confirm storage of raw trace rows and normalized replay evidence."
        ),
    ] = False,
    allow_fragmented: Annotated[
        bool,
        Parameter(help="Allow traces whose exported graph has multiple components."),
    ] = False,
    max_workers: Annotated[
        int,
        Parameter(help="Concurrent trace operations, from 1 to 8."),
    ] = 1,
    cohort_tag: Annotated[
        str | None,
        Parameter(
            help="Optional stable cohort label attached to every imported execution."
        ),
    ] = None,
    output: OutputFormatOption = "text",
) -> None:
    """Preview or import Langfuse traces as observed Kitaru executions."""
    command = "import.langfuse"
    output_format = _resolve_output_format(output)
    try:
        source_kind, _trace_id = parse_langfuse_source(source)
    except LangfuseImportError as exc:
        _exit_with_error(command, str(exc), output=output_format)
    if source_kind is LangfuseSourceKind.JSONL and source_project_id is None:
        _exit_with_error(
            command,
            "`--source-project-id` is required for JSONL sources.",
            output=output_format,
        )
    if write and not confirm_data_storage:
        _exit_with_error(
            command,
            "`--write` requires `--confirm-data-storage` because raw trace rows "
            "and normalized replay evidence will be persisted.",
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
            source,
            source_project_id=source_project_id,
            agent=agent,
            version=version,
            stack=stack,
            trace_ids=trace_id,
            limit=limit,
            dry_run=not write,
            confirm_data_storage=confirm_data_storage,
            allow_fragmented=allow_fragmented,
            max_workers=max_workers,
            cohort_tag=cohort_tag,
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
