#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Workspace-confined local experiment export handler."""

from pathlib import Path

from pydantic import BaseModel

from kitaru.exports.config import ExportRequest
from kitaru.exports.models import ExportError, get_export_error_kind
from kitaru.exports.operation import ExportOperationStateMachine, export_experiment
from kitaru.mcp.errors import MCPToolError
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.exports import (
    ExperimentExportReceipt,
    ExperimentExportRequest,
)


def _is_within(path: Path, roots: tuple[Path, ...]) -> bool:
    return any(path.is_relative_to(root) for root in roots)


def _resolve_source(path: str, roots: tuple[Path, ...]) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        raise MCPToolError("invalid_arguments", "source_root must be absolute.")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as error:
        raise MCPToolError(
            "invalid_arguments", "source_root must be an existing directory."
        ) from error
    if not resolved.is_dir() or not _is_within(resolved, roots):
        raise MCPToolError(
            "path_not_allowed", "source_root is outside the configured workspace roots."
        )
    return resolved


def _resolve_destination(path: str, roots: tuple[Path, ...]) -> Path:
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        raise MCPToolError("invalid_arguments", "destination must be absolute.")
    try:
        parent = candidate.parent.resolve(strict=True)
    except OSError as error:
        raise MCPToolError(
            "invalid_arguments", "destination parent must be an existing directory."
        ) from error
    resolved = (parent / candidate.name).resolve(strict=False)
    if not _is_within(parent, roots):
        raise MCPToolError(
            "path_not_allowed",
            "destination parent is outside the configured workspace roots.",
        )
    if not _is_within(resolved, roots):
        raise MCPToolError(
            "path_not_allowed", "destination is outside the configured workspace roots."
        )
    if candidate.exists() and candidate.resolve(strict=True) != resolved:
        raise MCPToolError(
            "path_not_allowed", "destination resolves outside its workspace path."
        )
    return resolved


def _map_export_error(error: ExportError) -> MCPToolError:
    recovery = {
        "destination_conflict": "Choose a new destination path.",
        "archive_conflict": "Choose a destination whose ZIP path does not exist.",
        "missing_source_include": "Check source_policy.include against source_root.",
    }.get(error.code)
    return MCPToolError(
        get_export_error_kind(error),
        error.message,
        details={"export_code": error.code},
        recovery=recovery,
    )


async def handle_experiment_export(
    state: MCPServerState,
    request: ExperimentExportRequest,
    *,
    operation: ExportOperationStateMachine | None = None,
) -> ExperimentExportReceipt:
    """Run one exact-ID local export inside configured workspace roots."""
    roots = state.settings.workspace_roots
    if not roots:
        raise MCPToolError(
            "invalid_configuration",
            "Experiment export requires KITARU_MCP_WORKSPACE_ROOTS.",
            recovery="Set KITARU_MCP_WORKSPACE_ROOTS to allowed absolute directories.",
        )
    source_root = _resolve_source(request.source_root, roots)
    destination = _resolve_destination(request.destination, roots)
    try:
        export_kwargs = {"operation": operation} if operation is not None else {}
        receipt = await export_experiment(
            state.client,
            ExportRequest(
                experiment_id=request.experiment_id,
                cohort_version_id=request.cohort_version_id,
                agent_version_id=request.agent_version_id,
                format=request.format,
                source_root=source_root,
                destination=destination,
                primary_reward=request.primary_reward,
                required_environment_names=tuple(request.required_environment_names),
                content_policy=request.content_policy,
                environment_policy=request.environment_policy,
                source_policy=request.source_policy,
                trace_format=request.trace_format,
                trace_path=request.trace_path,
                archive=request.archive,
                dry_run=request.dry_run,
            ),
            **export_kwargs,
        )
    except ExportError as error:
        raise _map_export_error(error) from error
    payload = (
        receipt.model_dump(mode="json") if isinstance(receipt, BaseModel) else receipt
    )
    return ExperimentExportReceipt.model_validate(payload)
