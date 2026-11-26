#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Capability-filtered public MCP tool registry."""

import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.types import CallToolResult, ToolAnnotations
from pydantic import ValidationError

from kitaru.api_models.v1.base import JsonValue
from kitaru.mcp.apps import FAILURE_MATRIX_URI, SESSION_VIEWER_URI, ui_meta
from kitaru.mcp.errors import (
    MCPOutputValidationError,
    error_result,
    protocol_result,
    success_result,
)
from kitaru.mcp.lifecycle import MCPServerState
from kitaru.mcp.models.activity import ActivityReadRequest
from kitaru.mcp.models.analyzers import AnalyzersManageRequest
from kitaru.mcp.models.common import (
    ActivityReadResult,
    AnalyzersManageResult,
    CohortsManageResult,
    ConnectionReadResult,
    ConnectionsManageResult,
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
from kitaru.mcp.models.connections import (
    ConnectionReadRequest,
    ConnectionsManageRequest,
)
from kitaru.mcp.models.docs import DocsSearchRequest, DocsSearchResult
from kitaru.mcp.models.evaluators import EvaluatorsManageRequest
from kitaru.mcp.models.failure_matrix import (
    FailureCellRequest,
    FailureCellResult,
    FailureMatrixRequest,
    FailureMatrixResult,
)
from kitaru.mcp.models.feedback import FeedbackDraftRequest, FeedbackDraftResult
from kitaru.mcp.models.management import CohortsManageRequest, ExperimentsManageRequest
from kitaru.mcp.models.registry import RegistryReadRequest
from kitaru.mcp.models.review import ReviewManageRequest, ReviewReadRequest
from kitaru.mcp.models.sessions import SessionReadRequest, SessionReadResult
from kitaru.mcp.models.viewer import (
    SessionViewRequest,
    SessionViewResult,
    ViewerReadRequest,
    ViewerReadResult,
    ViewerWriteRequest,
    ViewerWriteResult,
)
from kitaru.mcp.models.workflows import (
    DeleteRequest,
    SessionImportRequest,
    WorkflowCancelRequest,
    WorkflowStartRequest,
)
from kitaru.mcp.redaction import redact_data
from kitaru.mcp.settings import CapabilityMode
from kitaru.mcp.tools.activity import handle_activity_read
from kitaru.mcp.tools.analyzers import handle_analyzers_manage
from kitaru.mcp.tools.cohorts import handle_cohorts_manage
from kitaru.mcp.tools.connections import (
    handle_connection_read,
    handle_connections_manage,
)
from kitaru.mcp.tools.destructive import handle_delete, handle_workflow_cancel
from kitaru.mcp.tools.docs import handle_docs_search
from kitaru.mcp.tools.evaluators import handle_evaluators_manage
from kitaru.mcp.tools.experiments import handle_experiments_manage
from kitaru.mcp.tools.failure_matrix import handle_failure_cell, handle_failure_matrix
from kitaru.mcp.tools.feedback import handle_feedback_draft
from kitaru.mcp.tools.registry import handle_registry_read
from kitaru.mcp.tools.review import handle_review_manage, handle_review_read
from kitaru.mcp.tools.sessions import handle_session_read
from kitaru.mcp.tools.viewer import (
    handle_session_view,
    handle_viewer_read,
    handle_viewer_write,
)
from kitaru.mcp.tools.workflow_start import handle_workflow_start
from kitaru.mcp.tools.workflows import handle_session_import

logger = logging.getLogger("kitaru.mcp.registry")

ToolHandler = Callable[[MCPServerState, Any], Awaitable[object]]


@dataclass(frozen=True, slots=True, kw_only=True)
class ToolSpec:
    """One public tool and its minimum cumulative capability.

    Attributes:
        name: Tool name.
        minimum_mode: Lowest capability mode that lists the tool.
        description: Tool description.
        annotations: MCP tool annotations.
        request_type: The tool's `request` argument: a request model, or an
            annotated union of them.
        result_type: The tool's result envelope model.
        run: The tool's work, taking server state and the validated request
            model, whose validation holds the input checks. It returns
            the tool's data, or a `ToolSuccessPayload`, unredacted and without
            a result envelope, timeout, or concurrency bound, and raises on
            failure. `invoke_tool` adds those.
        meta: MCP `_meta` for the tool, if any.
        needs_server: Whether `run` uses the state's Kitaru client. A tool
            that does not runs without a server connection or sign-in.
    """

    name: str
    minimum_mode: CapabilityMode
    description: str
    annotations: ToolAnnotations
    request_type: Any
    result_type: type[ToolResult]
    run: ToolHandler
    meta: dict[str, Any] | None = None
    needs_server: bool = True


async def invoke_tool(
    spec: ToolSpec,
    state: MCPServerState,
    request: object,
    *,
    passthrough: tuple[type[Exception], ...] = (),
) -> CallToolResult:
    """Run one tool and render its result as the Kitaru MCP server does.

    `spec.run` runs through `state.execute`, so the state's settings bound its
    time and concurrency. Its data is redacted, unless it returns a
    `ToolSuccessPayload` with `redact=False`, and wrapped in the tool's result
    envelope. A failure becomes an error envelope, and its message is never
    logged, since a backend's message can echo a secret in a shape no
    redaction pattern knows: the log has the failure's type, or, for a result
    that fails validation, the locations of the invalid fields.

    Args:
        spec: The tool.
        state: Server state the tool runs with.
        request: The validated request, an instance of `spec.request_type`.
        passthrough: Exception types raised to the caller instead of being
            returned as an error envelope.

    Returns:
        The tool's structured result and its text.
    """
    text: str | None = None
    redacting = True
    try:
        data = await state.execute(lambda: spec.run(state, request))
        if isinstance(data, ToolSuccessPayload):
            redacting = data.redact
            json_data = (
                redact_data(data.data) if redacting else cast(JsonValue, data.data)
            )
            envelope = success_result(
                spec.result_type,
                json_data,
                warnings=data.warnings,
                links=data.links,
            )
            text = data.text
        else:
            envelope = success_result(spec.result_type, redact_data(data))
    except passthrough:
        raise
    # Handlers marshal their own request payloads and raise `MCPToolError` when a
    # payload is invalid, so a `ValidationError` here comes from the remote
    # response or from the result envelope.
    except ValidationError as error:
        # Log only locations: error inputs can carry unredacted response data.
        logger.debug(
            "Tool result failed output validation: %s",
            [detail["loc"] for detail in error.errors(include_url=False)],
        )
        envelope = error_result(spec.result_type, MCPOutputValidationError(error))
    except Exception as error:
        logger.debug("Tool handler raised %s", type(error).__name__)
        envelope = error_result(spec.result_type, error)
    return protocol_result(envelope, text, mask_credentials=redacting)


def _describe(text: str) -> str:
    return inspect.cleandoc(text)


def _annotations(
    *, read_only: bool, destructive: bool, idempotent: bool
) -> ToolAnnotations:
    return ToolAnnotations(
        read_only_hint=read_only,
        destructive_hint=destructive,
        idempotent_hint=idempotent,
        open_world_hint=True,
    )


def _get_tool_function(spec: ToolSpec) -> Callable[..., Awaitable[CallToolResult]]:
    """Build the MCP tool function that runs `spec` with the server's state."""

    async def call_tool(request: object, context: Context) -> CallToolResult:
        state = cast(MCPServerState, context.request_context.lifespan_context)
        return await invoke_tool(spec, state, request)

    # The MCP SDK builds the tool's schemas from the signature and finds the
    # context argument through the annotations.
    kind = inspect.Parameter.POSITIONAL_OR_KEYWORD
    parameters = [
        inspect.Parameter("request", kind, annotation=spec.request_type),
        inspect.Parameter("context", kind, annotation=Context),
    ]
    call_tool.__signature__ = inspect.Signature(  # ty: ignore[missing-attribute]
        parameters, return_annotation=spec.result_type
    )
    call_tool.__annotations__ = {
        "request": spec.request_type,
        "context": Context,
        "return": spec.result_type,
    }
    # The SDK names the input schema after the function.
    call_tool.__name__ = f"{spec.name.removeprefix('kitaru_')}_tool"
    return call_tool


TOOL_SPECS = (
    ToolSpec(
        name="kitaru_registry_read",
        minimum_mode=CapabilityMode.READ_ONLY,
        description=(
            "Read registry parents, versions, tags, and workers with bounded "
            "operations."
        ),
        annotations=_annotations(read_only=True, destructive=False, idempotent=True),
        request_type=RegistryReadRequest,
        result_type=RegistryReadResult,
        run=handle_registry_read,
    ),
    ToolSpec(
        name="kitaru_activity_read",
        minimum_mode=CapabilityMode.READ_ONLY,
        description=_describe(
            """
        Read Kitaru sessions, runs, jobs, and bounded child pages.

        To read whole sessions as text rather than node pages, use
        `kitaru_session_read`.
        """
        ),
        annotations=_annotations(read_only=True, destructive=False, idempotent=True),
        request_type=ActivityReadRequest,
        result_type=ActivityReadResult,
        run=handle_activity_read,
    ),
    ToolSpec(
        name="kitaru_session_read",
        minimum_mode=CapabilityMode.READ_ONLY,
        description=_describe(
            """
        Read whole sessions as compact transcripts, one node per line.

        Prefer this over paging `kitaru_activity_read` session nodes with payloads.

        Each model call shows only its new text, tool calls show short arguments and
        results, errors are never cut, and every cut ends in `[+N chars]`. Step
        numbers are node positions; pass `node` with `full` to read one node whole.
        """
        ),
        annotations=_annotations(read_only=True, destructive=False, idempotent=True),
        request_type=SessionReadRequest,
        result_type=SessionReadResult,
        run=handle_session_read,
    ),
    ToolSpec(
        name="kitaru_review_read",
        minimum_mode=CapabilityMode.READ_ONLY,
        description=(
            "Read investigations, annotations, insights, and investigation sessions."
        ),
        annotations=_annotations(read_only=True, destructive=False, idempotent=True),
        request_type=ReviewReadRequest,
        result_type=ReviewReadResult,
        run=handle_review_read,
    ),
    ToolSpec(
        name="kitaru_connection_read",
        minimum_mode=CapabilityMode.READ_ONLY,
        description="Read provider connections without their secret values.",
        annotations=_annotations(read_only=True, destructive=False, idempotent=True),
        request_type=ConnectionReadRequest,
        result_type=ConnectionReadResult,
        run=handle_connection_read,
    ),
    ToolSpec(
        name="kitaru_docs_search",
        minimum_mode=CapabilityMode.READ_ONLY,
        description=(
            "Search bundled Kitaru guides; return short excerpts and published "
            "source URLs."
        ),
        annotations=_annotations(read_only=True, destructive=False, idempotent=True),
        request_type=DocsSearchRequest,
        result_type=DocsSearchResult,
        run=handle_docs_search,
        needs_server=False,
    ),
    ToolSpec(
        name="kitaru_feedback_draft",
        minimum_mode=CapabilityMode.READ_ONLY,
        description=_describe(
            """
        Draft a GitHub issue about a Kitaru bug or confusing tool; send nothing.

        Use this when a Kitaru tool fails, returns something surprising, or its
        description misled you. Nothing is sent: show the user `issue_url` so they
        can review the prefilled issue and submit it themselves. Describe the tool,
        arguments, and result; leave out session content the user has not agreed
        to share.
        """
        ),
        annotations=_annotations(read_only=True, destructive=False, idempotent=True),
        request_type=FeedbackDraftRequest,
        result_type=FeedbackDraftResult,
        run=handle_feedback_draft,
        needs_server=False,
    ),
    ToolSpec(
        name="kitaru_failure_matrix",
        minimum_mode=CapabilityMode.READ_ONLY,
        description=_describe(
            """
        Show where sessions first go wrong as an interactive transition matrix.

        Rows are the last step that went right and columns the first step that went
        wrong, counted over the sessions `filter` selects (for example an agent_id or
        cohort_version_id). Pass `compare_filter`, such as a replay's
        experiment_run_id, to compare two groups. A reviewer places a failure that
        raised no error by annotating the failing node with the value
        `{"first_failure": true, "note": "..."}`.

        The result's snapshot_id lets a cell drill into exactly these sessions; it
        is null on a server that keeps no matrix in memory.
        """
        ),
        annotations=_annotations(read_only=True, destructive=False, idempotent=True),
        request_type=FailureMatrixRequest,
        result_type=FailureMatrixResult,
        run=handle_failure_matrix,
        meta=ui_meta(FAILURE_MATRIX_URI),
    ),
    ToolSpec(
        name="kitaru_failure_matrix_cell",
        minimum_mode=CapabilityMode.READ_ONLY,
        description=_describe(
            """
        List the sessions and repeated notes behind one transition matrix cell.

        On a server that keeps no matrix in memory, the matrix has no snapshot_id
        and the cell reads the sessions again.
        """
        ),
        annotations=_annotations(read_only=True, destructive=False, idempotent=True),
        request_type=FailureCellRequest,
        result_type=FailureCellResult,
        run=handle_failure_cell,
        # Only the matrix view calls this; hosts keep it out of the model's tools.
        meta=ui_meta(FAILURE_MATRIX_URI, visibility=["app"]),
    ),
    ToolSpec(
        name="kitaru_session_view",
        minimum_mode=CapabilityMode.READ_ONLY,
        description=_describe(
            """
        Open one session in an interactive trace viewer where a reviewer annotates it.

        Pass `session_id`, or `investigation_id` with `position` to review an
        investigation's sessions with its questions and verdicts. `viewer` picks the
        page: `default`, or a named custom viewer if this server enables custom
        viewers. The result names the session; the viewer page reads its payloads
        itself. Notes, answers, and verdicts the reviewer saves in the viewer go to
        Kitaru; read them back with `kitaru_review_read`.
        """
        ),
        annotations=_annotations(read_only=True, destructive=False, idempotent=True),
        request_type=SessionViewRequest,
        result_type=SessionViewResult,
        run=handle_session_view,
        meta=ui_meta(SESSION_VIEWER_URI),
    ),
    ToolSpec(
        name="kitaru_viewer_read",
        minimum_mode=CapabilityMode.READ_ONLY,
        description=(
            "Read a session for a viewer page, a viewer's HTML, or more of its nodes."
        ),
        annotations=_annotations(read_only=True, destructive=False, idempotent=True),
        request_type=ViewerReadRequest,
        result_type=ViewerReadResult,
        run=handle_viewer_read,
        # Only viewer pages call this and `kitaru_viewer_write`; hosts keep them
        # out of the model's tools so a saved note is one the reviewer wrote in
        # the page.
        meta=ui_meta(SESSION_VIEWER_URI, visibility=["app"]),
    ),
    ToolSpec(
        name="kitaru_cohorts_manage",
        minimum_mode=CapabilityMode.STANDARD,
        description=(
            "Create or update cohorts and cohort versions by exact identifiers."
        ),
        annotations=_annotations(read_only=False, destructive=False, idempotent=False),
        request_type=CohortsManageRequest,
        result_type=CohortsManageResult,
        run=handle_cohorts_manage,
    ),
    ToolSpec(
        name="kitaru_experiments_manage",
        minimum_mode=CapabilityMode.STANDARD,
        description="Create or update experiments with exact evaluator versions.",
        annotations=_annotations(read_only=False, destructive=False, idempotent=False),
        request_type=ExperimentsManageRequest,
        result_type=ExperimentsManageResult,
        run=handle_experiments_manage,
    ),
    ToolSpec(
        name="kitaru_session_import",
        minimum_mode=CapabilityMode.STANDARD,
        description="Import sessions from one existing blob and return the queued job.",
        annotations=_annotations(read_only=False, destructive=False, idempotent=False),
        request_type=SessionImportRequest,
        result_type=SessionImportResult,
        run=handle_session_import,
    ),
    ToolSpec(
        name="kitaru_review_manage",
        minimum_mode=CapabilityMode.STANDARD,
        description=(
            "Create or update investigations, annotations, insights, tags, and tag "
            "links."
        ),
        annotations=_annotations(read_only=False, destructive=False, idempotent=False),
        request_type=ReviewManageRequest,
        result_type=ReviewManageResult,
        run=handle_review_manage,
    ),
    ToolSpec(
        name="kitaru_workflow_start",
        minimum_mode=CapabilityMode.STANDARD,
        description=(
            "Start an evaluation batch, an analysis, or an experiment run; return "
            "at once."
        ),
        annotations=_annotations(read_only=False, destructive=False, idempotent=False),
        request_type=WorkflowStartRequest,
        result_type=WorkflowStartResult,
        run=handle_workflow_start,
    ),
    ToolSpec(
        name="kitaru_evaluators_manage",
        minimum_mode=CapabilityMode.STANDARD,
        description=(
            "Create or update evaluator parents and blob- or package-backed versions."
        ),
        annotations=_annotations(read_only=False, destructive=False, idempotent=False),
        request_type=EvaluatorsManageRequest,
        result_type=EvaluatorsManageResult,
        run=handle_evaluators_manage,
    ),
    ToolSpec(
        name="kitaru_analyzers_manage",
        minimum_mode=CapabilityMode.STANDARD,
        description=(
            "Create or update analyzer parents and blob- or package-backed versions."
        ),
        annotations=_annotations(read_only=False, destructive=False, idempotent=False),
        request_type=AnalyzersManageRequest,
        result_type=AnalyzersManageResult,
        run=handle_analyzers_manage,
    ),
    ToolSpec(
        name="kitaru_connections_manage",
        minimum_mode=CapabilityMode.STANDARD,
        description=(
            "Create or update provider connections and select provider defaults."
        ),
        annotations=_annotations(read_only=False, destructive=False, idempotent=False),
        request_type=ConnectionsManageRequest,
        result_type=ConnectionsManageResult,
        run=handle_connections_manage,
    ),
    ToolSpec(
        name="kitaru_viewer_write",
        minimum_mode=CapabilityMode.STANDARD,
        description=(
            "Save a reviewer's note, investigation answer, edit, or session verdict."
        ),
        annotations=_annotations(read_only=False, destructive=False, idempotent=False),
        request_type=ViewerWriteRequest,
        result_type=ViewerWriteResult,
        run=handle_viewer_write,
        meta=ui_meta(SESSION_VIEWER_URI, visibility=["app"]),
    ),
    ToolSpec(
        name="kitaru_workflow_cancel",
        minimum_mode=CapabilityMode.DESTRUCTIVE,
        description="Request cancellation of one exact job or experiment run.",
        annotations=_annotations(read_only=False, destructive=True, idempotent=False),
        request_type=WorkflowCancelRequest,
        result_type=WorkflowCancelResult,
        run=handle_workflow_cancel,
    ),
    ToolSpec(
        name="kitaru_delete",
        minimum_mode=CapabilityMode.DESTRUCTIVE,
        description="Delete one exact allowlisted resource or tag link.",
        annotations=_annotations(read_only=False, destructive=True, idempotent=False),
        request_type=DeleteRequest,
        result_type=DeleteResult,
        run=handle_delete,
    ),
)


def get_tool_specs(mode: CapabilityMode) -> tuple[ToolSpec, ...]:
    """Return only tools permitted by the selected mode before discovery."""
    return tuple(spec for spec in TOOL_SPECS if spec.minimum_mode.rank <= mode.rank)


def register_tools(server: MCPServer[Any], mode: CapabilityMode) -> None:
    """Register the filtered public surface through SDK v2 public APIs."""
    for spec in get_tool_specs(mode):
        server.add_tool(
            _get_tool_function(spec),
            name=spec.name,
            description=spec.description,
            annotations=spec.annotations,
            structured_output=True,
            meta=spec.meta,
        )
