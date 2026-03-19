"""Shared execution helpers for CLI and MCP transport layers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import kitaru.inspection as inspection
from kitaru import _flow_loading
from kitaru.client import Execution, KitaruClient, LogEntry


@dataclass(frozen=True)
class StartedExecutionDetails:
    """Post-launch execution lookup result shared across interfaces."""

    exec_id: str
    execution: Execution | None
    warning: str | None


@dataclass(frozen=True)
class FlowInvocationResult:
    """Structured result from starting a flow target."""

    handle: _flow_loading._FlowHandleLike


def list_executions_filtered(
    client: KitaruClient,
    *,
    flow: str | None,
    status: str | None,
    stack: str | None,
    limit: int | None,
) -> list[Execution]:
    """List executions with optional client-side stack filtering."""
    if limit is not None and limit < 1:
        raise ValueError("`limit` must be >= 1 when provided.")

    if stack is None:
        return client.executions.list(flow=flow, status=status, limit=limit)

    executions = client.executions.list(flow=flow, status=status, limit=None)
    filtered = [execution for execution in executions if execution.stack_name == stack]
    if limit is not None:
        return filtered[:limit]
    return filtered


def latest_execution_filtered(
    client: KitaruClient,
    *,
    flow: str | None,
    status: str | None,
    stack: str | None,
) -> Execution:
    """Resolve the latest execution with optional client-side stack filtering."""
    if stack is None:
        return client.executions.latest(flow=flow, status=status)

    executions = list_executions_filtered(
        client,
        flow=flow,
        status=status,
        stack=stack,
        limit=1,
    )
    if executions:
        return executions[0]

    filters: list[str] = []
    if flow is not None:
        filters.append(f"flow={flow!r}")
    if status is not None:
        filters.append(f"status={status!r}")
    filters.append(f"stack={stack!r}")
    raise LookupError(f"No executions found for {' and '.join(filters)}.")


def _validate_schema_type(value: Any, schema_type: str) -> bool:
    """Validate one value against a shallow JSON-schema `type` label."""
    if schema_type == "null":
        return value is None
    if schema_type == "boolean":
        return isinstance(value, bool)
    if schema_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if schema_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if schema_type == "string":
        return isinstance(value, str)
    if schema_type == "array":
        return isinstance(value, list)
    if schema_type == "object":
        return isinstance(value, dict)
    return True


def validate_wait_input_schema(
    *, wait_schema: dict[str, Any] | None, value: Any
) -> None:
    """Run lightweight wait-input validation when a schema type is present."""
    if wait_schema is None:
        return

    schema_type = wait_schema.get("type")
    if schema_type is None:
        return

    schema_types = [schema_type] if isinstance(schema_type, str) else list(schema_type)
    if any(_validate_schema_type(value, item) for item in schema_types):
        return

    raise ValueError(
        f"Wait input does not match the pending wait schema type ({schema_types!r})."
    )


def validate_pending_wait_input(
    *,
    execution: Execution,
    wait: str,
    value: Any,
) -> None:
    """Validate input against the current pending wait when the target matches."""
    pending_wait = execution.pending_wait
    if pending_wait is None:
        return

    if wait not in {pending_wait.name, pending_wait.wait_id}:
        return

    validate_wait_input_schema(wait_schema=pending_wait.schema, value=value)


def _format_mcp_log_entry(entry: LogEntry) -> str:
    """Render a readable one-line text representation for MCP log output."""
    parts: list[str] = []
    if entry.timestamp:
        parts.append(entry.timestamp)
    if entry.level:
        parts.append(str(entry.level).upper())
    if entry.checkpoint_name:
        parts.append(f"[{entry.checkpoint_name}]")

    if parts:
        return f"{' '.join(parts)} {entry.message}"
    return entry.message


def format_mcp_execution_logs(entries: Sequence[LogEntry]) -> str:
    """Render execution logs as plain text for MCP agent readability."""
    if not entries:
        return "No log entries found."
    return "\n".join(_format_mcp_log_entry(entry) for entry in entries)


def invoke_flow_target(
    *,
    target: str,
    args: dict[str, Any] | None,
    stack: str | None,
    module_name_prefix: str,
) -> FlowInvocationResult:
    """Load and invoke a flow target for CLI or MCP run surfaces."""
    if args is not None and not isinstance(args, dict):
        raise ValueError("`args` must be an object when provided.")

    flow_target = _flow_loading._load_flow_target(
        target,
        module_name_prefix=module_name_prefix,
    )
    flow_inputs = args or {}

    if stack:
        handle = flow_target.run(stack=stack, **flow_inputs)
    else:
        handle = flow_target.run(**flow_inputs)

    if not isinstance(handle, _flow_loading._FlowHandleLike):
        raise ValueError(
            "Flow execution did not return a valid handle with an `exec_id`."
        )

    return FlowInvocationResult(handle=handle)


def resolve_started_execution_details(
    *,
    exec_id: str,
    client: KitaruClient,
) -> StartedExecutionDetails:
    """Best-effort execution lookup after a flow launch."""
    try:
        execution = client.executions.get(exec_id)
    except Exception as exc:
        return StartedExecutionDetails(
            exec_id=exec_id,
            execution=None,
            warning=(
                "Execution started successfully, but details are not available yet: "
                f"{exc}"
            ),
        )

    return StartedExecutionDetails(exec_id=exec_id, execution=execution, warning=None)


def build_started_execution_payload(
    *,
    target: str,
    details: StartedExecutionDetails,
) -> dict[str, Any]:
    """Build the shared structured payload for flow launch responses."""
    return {
        "exec_id": details.exec_id,
        "target": target,
        "execution": (
            inspection.serialize_execution(details.execution)
            if details.execution is not None
            else None
        ),
        "warning": details.warning,
    }
