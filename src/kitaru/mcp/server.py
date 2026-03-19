"""Kitaru MCP server tools.

This module exposes structured MCP tools for querying and managing Kitaru
executions. The server is intentionally thin: it owns FastMCP wiring,
transport-specific validation, and delegation into shared interface helpers.
"""

from __future__ import annotations

import importlib
from typing import Any, Literal

import kitaru._interface_executions as execution_interface
import kitaru._interface_stacks as stack_interface
import kitaru.client as client_api
import kitaru.inspection as inspection
from kitaru._config import _stacks as stack_ops
from kitaru._interface_errors import run_with_mcp_error_boundary

_MCP_INSTALL_ERROR = (
    "MCP server dependencies are not installed. Install with: pip install kitaru[mcp]"
)


def _load_fastmcp_class() -> type[Any]:
    """Load the FastMCP class from optional dependencies."""
    try:
        module = importlib.import_module("mcp.server.fastmcp")
    except ImportError:
        raise ImportError(_MCP_INSTALL_ERROR) from None

    fastmcp = getattr(module, "FastMCP", None)
    if fastmcp is None:
        raise ImportError(_MCP_INSTALL_ERROR)
    return fastmcp


mcp = _load_fastmcp_class()("kitaru")


@mcp.tool()
def kitaru_executions_list(
    status: str | None = None,
    flow: str | None = None,
    stack: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """List executions with optional status/flow/stack filters."""

    def _list_executions() -> list[dict[str, Any]]:
        client = client_api.KitaruClient()
        executions = execution_interface.list_executions_filtered(
            client,
            flow=flow,
            status=status,
            stack=stack,
            limit=limit,
        )
        return [
            inspection.serialize_execution_summary(execution)
            for execution in executions
        ]

    return run_with_mcp_error_boundary(_list_executions)


@mcp.tool()
def kitaru_executions_get(exec_id: str) -> dict[str, Any]:
    """Get detailed information for one execution."""
    return run_with_mcp_error_boundary(
        lambda: inspection.serialize_execution(
            client_api.KitaruClient().executions.get(exec_id)
        )
    )


@mcp.tool()
def kitaru_executions_latest(
    status: str | None = None,
    flow: str | None = None,
    stack: str | None = None,
) -> dict[str, Any]:
    """Get the most recent execution matching the provided filters."""

    def _latest_execution() -> dict[str, Any]:
        client = client_api.KitaruClient()
        execution = execution_interface.latest_execution_filtered(
            client,
            flow=flow,
            status=status,
            stack=stack,
        )
        return inspection.serialize_execution(execution)

    return run_with_mcp_error_boundary(_latest_execution)


@mcp.tool()
def get_execution_logs(
    exec_id: str,
    checkpoint: str | None = None,
    source: str = "step",
    limit: int = 200,
) -> str:
    """Fetch runtime log entries for a Kitaru execution."""

    def _get_logs() -> str:
        if limit < 1:
            raise ValueError("`limit` must be >= 1.")

        entries = client_api.KitaruClient().executions.logs(
            exec_id,
            checkpoint=checkpoint,
            source=source,
            limit=limit,
        )
        return execution_interface.format_mcp_execution_logs(entries)

    return run_with_mcp_error_boundary(_get_logs)


@mcp.tool()
def kitaru_executions_run(
    target: str,
    args: dict[str, Any] | None = None,
    stack: str | None = None,
) -> dict[str, Any]:
    """Start a flow from `<module_or_file>:<flow_name>` target."""

    def _start_execution() -> dict[str, Any]:
        client = client_api.KitaruClient()
        result = execution_interface.invoke_flow_target(
            target=target,
            args=args,
            stack=stack,
            module_name_prefix="_kitaru_mcp_run_target_",
        )
        details = execution_interface.resolve_started_execution_details(
            exec_id=result.handle.exec_id,
            client=client,
        )
        return execution_interface.build_started_execution_payload(
            target=target,
            details=details,
        )

    return run_with_mcp_error_boundary(_start_execution)


@mcp.tool()
def kitaru_executions_cancel(exec_id: str) -> dict[str, Any]:
    """Cancel one execution and return updated details."""
    return run_with_mcp_error_boundary(
        lambda: inspection.serialize_execution(
            client_api.KitaruClient().executions.cancel(exec_id)
        )
    )


@mcp.tool()
def kitaru_executions_input(exec_id: str, wait: str, value: Any) -> dict[str, Any]:
    """Provide input to a waiting execution and return updated details."""

    def _provide_input() -> dict[str, Any]:
        client = client_api.KitaruClient()
        current_execution = client.executions.get(exec_id)
        execution_interface.validate_pending_wait_input(
            execution=current_execution,
            wait=wait,
            value=value,
        )

        updated_execution = client.executions.input(exec_id, wait=wait, value=value)
        return inspection.serialize_execution(updated_execution)

    return run_with_mcp_error_boundary(_provide_input)


@mcp.tool()
def kitaru_executions_retry(exec_id: str) -> dict[str, Any]:
    """Retry one failed execution and return updated details."""
    return run_with_mcp_error_boundary(
        lambda: inspection.serialize_execution(
            client_api.KitaruClient().executions.retry(exec_id)
        )
    )


@mcp.tool()
def kitaru_executions_replay(
    exec_id: str,
    from_: str,
    overrides: dict[str, Any] | None = None,
    flow_inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay an execution and return structured replay details."""

    def _replay_execution() -> dict[str, Any]:
        if flow_inputs is not None and not isinstance(flow_inputs, dict):
            raise ValueError("`flow_inputs` must be an object when provided.")

        replay_inputs = flow_inputs or {}
        execution = client_api.KitaruClient().executions.replay(
            exec_id,
            from_=from_,
            overrides=overrides,
            **replay_inputs,
        )

        return {
            "available": True,
            "operation": "replay",
            "execution": inspection.serialize_execution(execution),
        }

    return run_with_mcp_error_boundary(_replay_execution)


@mcp.tool()
def kitaru_artifacts_list(
    exec_id: str,
    name: str | None = None,
    kind: str | None = None,
    producing_call: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """List artifact metadata for one execution."""
    return run_with_mcp_error_boundary(
        lambda: [
            inspection.serialize_artifact_ref(artifact)
            for artifact in client_api.KitaruClient().artifacts.list(
                exec_id,
                name=name,
                kind=kind,
                producing_call=producing_call,
                limit=limit,
            )
        ]
    )


@mcp.tool()
def kitaru_artifacts_get(artifact_id: str) -> dict[str, Any]:
    """Get one artifact's metadata and loaded value."""

    def _get_artifact() -> dict[str, Any]:
        artifact = client_api.KitaruClient().artifacts.get(artifact_id)
        loaded = artifact.load()

        payload = inspection.serialize_artifact_ref(artifact)
        payload.update(inspection.serialize_artifact_value(loaded))
        return payload

    return run_with_mcp_error_boundary(_get_artifact)


@mcp.tool()
def kitaru_status() -> dict[str, Any]:
    """Return structured status details for the current Kitaru connection."""

    def _status() -> dict[str, Any]:
        snapshot = inspection.build_runtime_snapshot()
        return inspection.serialize_runtime_snapshot(snapshot)

    return run_with_mcp_error_boundary(_status)


@mcp.tool()
def kitaru_stacks_list() -> list[dict[str, Any]]:
    """List available stacks from the active connection context."""
    return run_with_mcp_error_boundary(
        lambda: [
            inspection.serialize_stack(entry.stack, is_managed=entry.is_managed)
            for entry in stack_ops._list_stack_entries()
        ]
    )


@mcp.tool()
def manage_stack(
    action: Literal["create", "delete"],
    name: str,
    activate: bool = True,
    recursive: bool = False,
    force: bool = False,
    stack_type: str = "local",
    artifact_store: str | None = None,
    container_registry: str | None = None,
    cluster: str | None = None,
    region: str | None = None,
    subscription_id: str | None = None,
    resource_group: str | None = None,
    workspace: str | None = None,
    execution_role: str | None = None,
    namespace: str | None = None,
    credentials: str | None = None,
    extra: dict[str, Any] | None = None,
    async_mode: bool = False,
    verify: bool = True,
) -> dict[str, Any]:
    """Create or delete a local, Kubernetes-backed, Vertex AI, SageMaker,
    or AzureML stack. `async_mode` is the MCP equivalent of CLI `--async`."""

    def _manage_stack() -> dict[str, Any]:
        request = stack_interface.build_manage_stack_request(
            action=action,
            name=name,
            activate=activate,
            recursive=recursive,
            force=force,
            stack_type=stack_type,
            artifact_store=artifact_store,
            container_registry=container_registry,
            cluster=cluster,
            region=region,
            subscription_id=subscription_id,
            resource_group=resource_group,
            workspace=workspace,
            execution_role=execution_role,
            namespace=namespace,
            credentials=credentials,
            extra=extra,
            async_mode=async_mode,
            verify=verify,
        )

        if isinstance(request, stack_interface.ManageStackCreateRequest):
            create_kwargs: dict[str, Any] = {
                "activate": request.activate,
                "stack_type": request.stack_type,
                "remote_spec": request.remote_spec,
            }
            if not request.component_overrides.is_empty():
                create_kwargs["component_overrides"] = request.component_overrides
            result = stack_ops._create_stack_operation(request.name, **create_kwargs)
            return inspection.serialize_stack_create_result(result)

        assert isinstance(request, stack_interface.ManageStackDeleteRequest)
        result = stack_ops._delete_stack_operation(
            request.name,
            recursive=request.recursive,
            force=request.force,
        )
        return inspection.serialize_stack_delete_result(result)

    return run_with_mcp_error_boundary(_manage_stack)


def main() -> None:
    """Entry point for the `kitaru-mcp` console script."""
    mcp.run(transport="stdio")
