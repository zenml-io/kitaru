#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Capability-filtered public MCP tool registry."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.types import CallToolResult, ToolAnnotations
from pydantic import BaseModel, ValidationError

from kitaru.mcp.errors import (
    MCPOutputValidationError,
    error_result,
    protocol_result,
    success_result,
)
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.activity import ActivityReadRequest
from kitaru.mcp.models.common import (
    ActivityReadResult,
    CohortsManageResult,
    DeleteResult,
    EvaluatorsManageResult,
    ExperimentsManageResult,
    RegistryReadResult,
    ReviewManageResult,
    ReviewReadResult,
    SessionImportResult,
    ToolResult,
    ToolSuccessPayload,
    WorkflowCancelResult,
    WorkflowStartResult,
)
from kitaru.mcp.models.evaluators import EvaluatorsManageRequest
from kitaru.mcp.models.management import CohortsManageRequest, ExperimentsManageRequest
from kitaru.mcp.models.registry import RegistryReadRequest
from kitaru.mcp.models.review import ReviewManageRequest, ReviewReadRequest
from kitaru.mcp.models.workflows import (
    DeleteRequest,
    SessionImportRequest,
    WorkflowCancelRequest,
    WorkflowStartRequest,
)
from kitaru.mcp.redaction import redact_data
from kitaru.mcp.settings import CapabilityMode
from kitaru.mcp.tools.activity import handle_activity_read
from kitaru.mcp.tools.cohorts import handle_cohorts_manage
from kitaru.mcp.tools.destructive import handle_delete, handle_workflow_cancel
from kitaru.mcp.tools.evaluators import handle_evaluators_manage
from kitaru.mcp.tools.experiments import handle_experiments_manage
from kitaru.mcp.tools.registry import handle_registry_read
from kitaru.mcp.tools.review import handle_review_manage, handle_review_read
from kitaru.mcp.tools.workflow_start import handle_workflow_start
from kitaru.mcp.tools.workflows import handle_session_import

ToolHandler = Callable[[MCPServerState, Any], Awaitable[object]]


@dataclass(frozen=True, slots=True)
class ToolSpec:
    """One public tool and its minimum cumulative capability."""

    name: str
    minimum_mode: CapabilityMode
    description: str
    annotations: ToolAnnotations
    handler: Callable[..., Awaitable[BaseModel]]


def _annotations(
    *, read_only: bool, destructive: bool, idempotent: bool
) -> ToolAnnotations:
    return ToolAnnotations(
        read_only_hint=read_only,
        destructive_hint=destructive,
        idempotent_hint=idempotent,
        open_world_hint=True,
    )


async def registry_read_tool(
    request: RegistryReadRequest, context: Context
) -> RegistryReadResult:
    """Read registry parents, versions, tags, and workers with bounded operations."""
    return cast(
        RegistryReadResult,
        await _invoke(context, request, RegistryReadResult, handle_registry_read),
    )


async def activity_read_tool(
    request: ActivityReadRequest, context: Context
) -> ActivityReadResult:
    """Read Kitaru sessions, runs, jobs, and bounded child pages."""
    return cast(
        ActivityReadResult,
        await _invoke(context, request, ActivityReadResult, handle_activity_read),
    )


async def review_read_tool(
    request: ReviewReadRequest, context: Context
) -> ReviewReadResult:
    """Read investigations, annotations, and ordered investigation sessions."""
    return cast(
        ReviewReadResult,
        await _invoke(context, request, ReviewReadResult, handle_review_read),
    )


async def cohorts_manage_tool(
    request: CohortsManageRequest, context: Context
) -> CohortsManageResult:
    """Create or update cohorts and cohort versions by exact identifiers."""
    return cast(
        CohortsManageResult,
        await _invoke(context, request, CohortsManageResult, handle_cohorts_manage),
    )


async def experiments_manage_tool(
    request: ExperimentsManageRequest, context: Context
) -> ExperimentsManageResult:
    """Create or update experiments with exact evaluator versions."""
    return cast(
        ExperimentsManageResult,
        await _invoke(
            context, request, ExperimentsManageResult, handle_experiments_manage
        ),
    )


async def session_import_tool(
    request: SessionImportRequest, context: Context
) -> SessionImportResult:
    """Import sessions from one existing blob and return the queued job."""
    return cast(
        SessionImportResult,
        await _invoke(context, request, SessionImportResult, handle_session_import),
    )


async def review_manage_tool(
    request: ReviewManageRequest, context: Context
) -> ReviewManageResult:
    """Create or update investigations, annotations, tags, and tag links."""
    return cast(
        ReviewManageResult,
        await _invoke(context, request, ReviewManageResult, handle_review_manage),
    )


async def workflow_start_tool(
    request: WorkflowStartRequest, context: Context
) -> WorkflowStartResult:
    """Start an evaluation batch or experiment run and return immediately."""
    return cast(
        WorkflowStartResult,
        await _invoke(context, request, WorkflowStartResult, handle_workflow_start),
    )


async def evaluators_manage_tool(
    request: EvaluatorsManageRequest, context: Context
) -> EvaluatorsManageResult:
    """Create or update evaluator parents and blob- or package-backed versions."""
    return cast(
        EvaluatorsManageResult,
        await _invoke(
            context, request, EvaluatorsManageResult, handle_evaluators_manage
        ),
    )


async def workflow_cancel_tool(
    request: WorkflowCancelRequest, context: Context
) -> WorkflowCancelResult:
    """Request cancellation of one exact job or experiment run."""
    return cast(
        WorkflowCancelResult,
        await _invoke(context, request, WorkflowCancelResult, handle_workflow_cancel),
    )


async def delete_tool(request: DeleteRequest, context: Context) -> DeleteResult:
    """Delete one exact allowlisted resource or tag link."""
    return cast(
        DeleteResult, await _invoke(context, request, DeleteResult, handle_delete)
    )


async def _invoke(
    context: Context,
    request: object,
    result_type: type[ToolResult],
    handler: ToolHandler,
) -> CallToolResult:
    state = cast(MCPServerState, context.request_context.lifespan_context)
    try:
        data = await state.execute(lambda: handler(state, request))
        if isinstance(data, ToolSuccessPayload):
            json_data = redact_data(data.data)
            envelope = success_result(
                result_type,
                json_data,
                warnings=data.warnings,
                links=data.links,
            )
        else:
            json_data = redact_data(data)
            envelope = success_result(result_type, json_data)
    except ValidationError as error:
        envelope = error_result(result_type, MCPOutputValidationError(error))
    except Exception as error:
        envelope = error_result(result_type, error)
    return protocol_result(envelope)


TOOL_SPECS = (
    ToolSpec(
        "kitaru_registry_read",
        CapabilityMode.READ_ONLY,
        registry_read_tool.__doc__ or "",
        _annotations(read_only=True, destructive=False, idempotent=True),
        registry_read_tool,
    ),
    ToolSpec(
        "kitaru_activity_read",
        CapabilityMode.READ_ONLY,
        activity_read_tool.__doc__ or "",
        _annotations(read_only=True, destructive=False, idempotent=True),
        activity_read_tool,
    ),
    ToolSpec(
        "kitaru_review_read",
        CapabilityMode.READ_ONLY,
        review_read_tool.__doc__ or "",
        _annotations(read_only=True, destructive=False, idempotent=True),
        review_read_tool,
    ),
    ToolSpec(
        "kitaru_cohorts_manage",
        CapabilityMode.STANDARD,
        cohorts_manage_tool.__doc__ or "",
        _annotations(read_only=False, destructive=False, idempotent=False),
        cohorts_manage_tool,
    ),
    ToolSpec(
        "kitaru_experiments_manage",
        CapabilityMode.STANDARD,
        experiments_manage_tool.__doc__ or "",
        _annotations(read_only=False, destructive=False, idempotent=False),
        experiments_manage_tool,
    ),
    ToolSpec(
        "kitaru_session_import",
        CapabilityMode.STANDARD,
        session_import_tool.__doc__ or "",
        _annotations(read_only=False, destructive=False, idempotent=False),
        session_import_tool,
    ),
    ToolSpec(
        "kitaru_review_manage",
        CapabilityMode.STANDARD,
        review_manage_tool.__doc__ or "",
        _annotations(read_only=False, destructive=False, idempotent=False),
        review_manage_tool,
    ),
    ToolSpec(
        "kitaru_workflow_start",
        CapabilityMode.STANDARD,
        workflow_start_tool.__doc__ or "",
        _annotations(read_only=False, destructive=False, idempotent=False),
        workflow_start_tool,
    ),
    ToolSpec(
        "kitaru_evaluators_manage",
        CapabilityMode.STANDARD,
        evaluators_manage_tool.__doc__ or "",
        _annotations(read_only=False, destructive=False, idempotent=False),
        evaluators_manage_tool,
    ),
    ToolSpec(
        "kitaru_workflow_cancel",
        CapabilityMode.DESTRUCTIVE,
        workflow_cancel_tool.__doc__ or "",
        _annotations(read_only=False, destructive=True, idempotent=False),
        workflow_cancel_tool,
    ),
    ToolSpec(
        "kitaru_delete",
        CapabilityMode.DESTRUCTIVE,
        delete_tool.__doc__ or "",
        _annotations(read_only=False, destructive=True, idempotent=False),
        delete_tool,
    ),
)


def get_tool_specs(mode: CapabilityMode) -> tuple[ToolSpec, ...]:
    """Return only tools permitted by the selected mode before discovery."""
    return tuple(spec for spec in TOOL_SPECS if spec.minimum_mode.rank <= mode.rank)


def register_tools(server: MCPServer[Any], mode: CapabilityMode) -> None:
    """Register the filtered public surface through SDK v2 public APIs."""
    for spec in get_tool_specs(mode):
        server.add_tool(
            spec.handler,
            name=spec.name,
            description=spec.description,
            annotations=spec.annotations,
            structured_output=True,
        )
