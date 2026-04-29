"""Kitaru MCP server tools.

This module exposes structured MCP tools for querying and managing Kitaru
executions. The server is intentionally thin: it owns FastMCP wiring,
transport-specific validation, and delegation into shared interface helpers.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from functools import wraps
from typing import Any, Literal, TypeVar

import kitaru._cleanup as cleanup
import kitaru._interface_executions as execution_interface
import kitaru._interface_memory as memory_interface
import kitaru._interface_stacks as stack_interface
import kitaru.client as client_api
import kitaru.inspection as inspection
import kitaru.secrets as secrets_api
from kitaru._client._deployments import (
    DEFAULT_DEPLOYMENT_TAG,
    resolve_deployment_exclusive,
)
from kitaru._config import _stacks as stack_ops
from kitaru._flow_loading import _load_deployable_flow_target
from kitaru._interface_deployments import (
    build_deployment_deploy_kwargs,
    resolve_deployment_selector,
    validate_deployment_selector,
)
from kitaru._interface_errors import run_with_mcp_error_boundary
from kitaru._local_server import (
    start_or_connect_local_server,
    stop_registered_local_server,
)
from kitaru.analytics import AnalyticsEvent, set_source, track

_MCP_INSTALL_ERROR = (
    "MCP server dependencies are not installed. Install with: pip install kitaru[mcp]"
)

_T = TypeVar("_T")


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


def tracked_mcp_tool(func: Callable[..., _T]) -> Callable[..., _T]:
    """Register a function as a Kitaru MCP tool with automatic analytics tracking.

    Combines ``@mcp.tool()`` registration with per-call analytics.  Every
    invocation fires ``AnalyticsEvent.MCP_TOOL_CALLED`` with the tool name
    (derived from ``func.__name__``) and the success/failure outcome.
    """
    tool_name: str = getattr(func, "__name__", type(func).__name__)

    @wraps(func)
    def _wrapper(*args: Any, **kwargs: Any) -> _T:
        try:
            result = func(*args, **kwargs)
            track(
                AnalyticsEvent.MCP_TOOL_CALLED,
                {"tool_name": tool_name, "success": True},
            )
            return result
        except Exception as exc:
            track(
                AnalyticsEvent.MCP_TOOL_CALLED,
                {
                    "tool_name": tool_name,
                    "success": False,
                    "error_type": type(exc).__name__,
                },
            )
            raise

    return mcp.tool()(_wrapper)


@tracked_mcp_tool
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


@tracked_mcp_tool
def kitaru_executions_get(exec_id: str) -> dict[str, Any]:
    """Get detailed information for one execution."""
    return run_with_mcp_error_boundary(
        lambda: inspection.serialize_execution(
            client_api.KitaruClient().executions.get(exec_id)
        )
    )


@tracked_mcp_tool
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


@tracked_mcp_tool
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


@tracked_mcp_tool
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
            exec_id=result.exec_id,
            client=client,
        )
        return execution_interface.build_started_execution_payload(
            target=target,
            details=details,
        )

    return run_with_mcp_error_boundary(_start_execution)


@tracked_mcp_tool
def kitaru_deployments_deploy(
    target: str,
    inputs: dict[str, Any] | None = None,
    tag: str = DEFAULT_DEPLOYMENT_TAG,
    exclusive: bool = False,
    stack: str | None = None,
    image: str | dict[str, Any] | None = None,
    cache: bool | None = None,
    retries: int | None = None,
) -> dict[str, Any]:
    """Deploy a new flow version from a `<module_or_file>:<flow_name>` target."""

    def _deploy() -> dict[str, Any]:
        flow_inputs = execution_interface.ensure_inputs_object(inputs)
        _, normalized_tag = validate_deployment_selector(
            tag=tag,
            require_one=True,
        )
        assert normalized_tag is not None  # guaranteed by require_one=True
        resolved_tags = {
            normalized_tag: resolve_deployment_exclusive(normalized_tag, exclusive)
        }
        deploy_kwargs = build_deployment_deploy_kwargs(
            stack=stack,
            image=image,
            cache=cache,
            retries=retries,
            tags=resolved_tags,
            inputs=flow_inputs,
            image_label="`image`",
        )
        flow_target = _load_deployable_flow_target(
            target,
            module_name_prefix="_kitaru_mcp_deploy_target_",
        )
        deployment = flow_target.deploy(**deploy_kwargs)
        return inspection.serialize_deployment(deployment)

    return run_with_mcp_error_boundary(_deploy)


@tracked_mcp_tool
def kitaru_deployments_invoke(
    flow: str,
    version: int | None = None,
    tag: str | None = None,
    inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Invoke a deployed flow snapshot by version or tag."""

    def _invoke() -> dict[str, Any]:
        flow_inputs = execution_interface.ensure_inputs_object(inputs)
        selector = resolve_deployment_selector(
            version=version,
            tag=tag,
            default_tag=DEFAULT_DEPLOYMENT_TAG,
        )
        client = client_api.KitaruClient()
        handle = client.deployments.invoke(
            flow=flow,
            version=selector.version,
            tag=selector.tag,
            selector_source=selector.source,
            inputs=flow_inputs,
        )
        exec_id = execution_interface.flow_handle_exec_id(handle)
        details = execution_interface.resolve_started_execution_details(
            exec_id=exec_id,
            client=client,
        )
        return execution_interface.build_started_deployment_payload(
            flow=flow,
            version=selector.version,
            tag=selector.tag,
            details=details,
        )

    return run_with_mcp_error_boundary(_invoke)


@tracked_mcp_tool
def kitaru_deployments_list(flow: str | None = None) -> list[dict[str, Any]]:
    """List flow deployments, optionally filtered to one flow."""

    def _list_deployments() -> list[dict[str, Any]]:
        return [
            inspection.serialize_deployment(deployment)
            for deployment in client_api.KitaruClient().deployments.list(flow=flow)
        ]

    return run_with_mcp_error_boundary(_list_deployments)


@tracked_mcp_tool
def kitaru_deployments_get(
    flow: str,
    version: int | None = None,
    tag: str | None = None,
) -> dict[str, Any]:
    """Get one flow deployment by version or tag."""

    def _get() -> dict[str, Any]:
        resolved_version, resolved_tag = validate_deployment_selector(
            version=version,
            tag=tag,
            default_tag=DEFAULT_DEPLOYMENT_TAG,
        )
        deployment = client_api.KitaruClient().deployments.get(
            flow=flow,
            version=resolved_version,
            tag=resolved_tag,
        )
        return inspection.serialize_deployment(deployment)

    return run_with_mcp_error_boundary(_get)


@tracked_mcp_tool
def kitaru_deployments_delete(flow: str, version: int) -> dict[str, Any]:
    """Delete one deployment version when no exclusive tag protects it."""

    def _delete() -> dict[str, Any]:
        client_api.KitaruClient().deployments.delete(flow=flow, version=version)
        return {"flow": flow, "version": version, "deleted": True}

    return run_with_mcp_error_boundary(_delete)


@tracked_mcp_tool
def kitaru_deployments_tag(
    flow: str,
    version: int,
    tag: str,
    exclusive: bool = False,
) -> dict[str, Any]:
    """Attach a public tag to one deployment version."""

    def _tag() -> dict[str, Any]:
        deployment = client_api.KitaruClient().deployments.tag(
            flow=flow,
            version=version,
            tag=tag,
            exclusive=exclusive,
        )
        return inspection.serialize_deployment(deployment)

    return run_with_mcp_error_boundary(_tag)


@tracked_mcp_tool
def kitaru_deployments_untag(flow: str, version: int, tag: str) -> dict[str, Any]:
    """Remove a public tag from one deployment version."""

    def _untag() -> dict[str, Any]:
        deployment = client_api.KitaruClient().deployments.untag(
            flow=flow,
            version=version,
            tag=tag,
        )
        return inspection.serialize_deployment(deployment)

    return run_with_mcp_error_boundary(_untag)


@tracked_mcp_tool
def kitaru_executions_cancel(exec_id: str) -> dict[str, Any]:
    """Cancel one execution and return updated details."""
    return run_with_mcp_error_boundary(
        lambda: inspection.serialize_execution(
            client_api.KitaruClient().executions.cancel(exec_id)
        )
    )


@tracked_mcp_tool
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


@tracked_mcp_tool
def kitaru_executions_retry(exec_id: str) -> dict[str, Any]:
    """Retry one failed execution and return updated details."""
    return run_with_mcp_error_boundary(
        lambda: inspection.serialize_execution(
            client_api.KitaruClient().executions.retry(exec_id)
        )
    )


@tracked_mcp_tool
def kitaru_executions_replay(
    exec_id: str,
    from_: str,
    overrides: dict[str, Any] | None = None,
    flow_inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay an execution and return structured replay details."""

    def _replay_execution() -> dict[str, Any]:
        replay_inputs = execution_interface.ensure_inputs_object(
            flow_inputs,
            input_label="`flow_inputs`",
        )
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


@tracked_mcp_tool
def kitaru_secrets_create(
    name: str,
    values: dict[str, Any],
    private: bool = False,
) -> dict[str, Any]:
    """Create a Kitaru secret and return metadata without raw values."""
    return run_with_mcp_error_boundary(
        lambda: inspection.serialize_secret_summary(
            secrets_api.create_secret(name, values, private=private)
        )
    )


@tracked_mcp_tool
def kitaru_memory_list(
    scope: str,
    scope_type: str,
    prefix: str | None = None,
) -> list[dict[str, Any]]:
    """List memory entries for one explicit scope."""
    return run_with_mcp_error_boundary(
        lambda: memory_interface.list_memory_payload(
            client_api.KitaruClient(),
            scope=scope,
            scope_type=scope_type,
            prefix=prefix,
        )
    )


@tracked_mcp_tool
def kitaru_memory_get(
    key: str,
    scope: str,
    scope_type: str,
    version: int | None = None,
    strict: bool = False,
) -> dict[str, Any] | None:
    """Get one memory value from one explicit scope.

    Lenient mode (``strict=False``, the default) returns a payload with
    ``value_available: False`` and nested ``value_unavailable`` diagnostics
    when the backing artifact cannot be loaded from the current runtime.
    Strict mode raises ``KitaruMemoryArtifactUnavailableError`` instead.
    """
    return run_with_mcp_error_boundary(
        lambda: memory_interface.get_memory_payload(
            client_api.KitaruClient(),
            key=key,
            scope=scope,
            scope_type=scope_type,
            version=version,
            strict=strict,
        )
    )


@tracked_mcp_tool
def kitaru_memory_set(
    key: str,
    value: Any,
    scope: str,
    scope_type: str,
) -> dict[str, Any]:
    """Write one memory value into one explicit scope."""
    return run_with_mcp_error_boundary(
        lambda: memory_interface.set_memory_payload(
            client_api.KitaruClient(),
            key=key,
            value=value,
            scope=scope,
            scope_type=scope_type,
        )
    )


@tracked_mcp_tool
def kitaru_memory_delete(
    key: str,
    scope: str,
    scope_type: str,
) -> dict[str, Any] | None:
    """Soft-delete one memory key from one explicit scope."""
    return run_with_mcp_error_boundary(
        lambda: memory_interface.delete_memory_payload(
            client_api.KitaruClient(),
            key=key,
            scope=scope,
            scope_type=scope_type,
        )
    )


@tracked_mcp_tool
def kitaru_memory_history(
    key: str,
    scope: str,
    scope_type: str,
) -> list[dict[str, Any]]:
    """Show all versions for one memory key in one explicit scope."""
    return run_with_mcp_error_boundary(
        lambda: memory_interface.history_memory_payload(
            client_api.KitaruClient(),
            key=key,
            scope=scope,
            scope_type=scope_type,
        )
    )


@tracked_mcp_tool
def kitaru_memory_purge(
    key: str,
    scope: str,
    scope_type: str,
    keep: int | None = None,
) -> dict[str, Any]:
    """Physically delete old versions of one memory key."""
    return run_with_mcp_error_boundary(
        lambda: memory_interface.purge_memory_payload(
            client_api.KitaruClient(),
            key=key,
            scope=scope,
            scope_type=scope_type,
            keep=keep,
        )
    )


@tracked_mcp_tool
def kitaru_memory_purge_scope(
    scope: str,
    scope_type: str,
    keep: int | None = None,
    include_deleted: bool = False,
) -> dict[str, Any]:
    """Purge old versions across all keys in one scope."""
    return run_with_mcp_error_boundary(
        lambda: memory_interface.purge_scope_memory_payload(
            client_api.KitaruClient(),
            scope=scope,
            scope_type=scope_type,
            keep=keep,
            include_deleted=include_deleted,
        )
    )


@tracked_mcp_tool
def kitaru_memory_compact(
    scope: str,
    scope_type: str,
    key: str | None = None,
    keys: list[str] | None = None,
    source_mode: Literal["current", "history"] = "current",
    target_key: str | None = None,
    instruction: str | None = None,
    model: str | None = None,
    max_tokens: int | None = None,
) -> dict[str, Any]:
    """Summarize memory values using an LLM and write the result."""
    return run_with_mcp_error_boundary(
        lambda: memory_interface.compact_memory_payload(
            client_api.KitaruClient(),
            scope=scope,
            scope_type=scope_type,
            key=key,
            keys=keys,
            source_mode=source_mode,
            target_key=target_key,
            instruction=instruction,
            model=model,
            max_tokens=max_tokens,
        )
    )


@tracked_mcp_tool
def kitaru_memory_compaction_log(
    scope: str,
    scope_type: str,
) -> list[dict[str, Any]]:
    """Show the compaction audit log for one scope."""
    return run_with_mcp_error_boundary(
        lambda: memory_interface.compaction_log_memory_payload(
            client_api.KitaruClient(),
            scope=scope,
            scope_type=scope_type,
        )
    )


@tracked_mcp_tool
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


@tracked_mcp_tool
def kitaru_artifacts_get(artifact_id: str) -> dict[str, Any]:
    """Get one artifact's metadata and loaded value."""

    def _get_artifact() -> dict[str, Any]:
        artifact = client_api.KitaruClient().artifacts.get(artifact_id)
        loaded = artifact.load()

        payload = inspection.serialize_artifact_ref(artifact)
        payload.update(inspection.serialize_artifact_value(loaded))
        return payload

    return run_with_mcp_error_boundary(_get_artifact)


@tracked_mcp_tool
def kitaru_start_local_server(
    port: int | None = None,
    timeout: int = 60,
) -> dict[str, Any]:
    """Start or connect to the local Kitaru server."""

    def _start_local_server() -> dict[str, Any]:
        result = start_or_connect_local_server(port=port, timeout=timeout)
        return {
            "mode": "local",
            "url": result.url,
            "action": result.action,
        }

    return run_with_mcp_error_boundary(_start_local_server)


@tracked_mcp_tool
def kitaru_stop_local_server() -> dict[str, Any]:
    """Stop the registered local Kitaru server, if one exists."""

    def _stop_local_server() -> dict[str, Any]:
        result = stop_registered_local_server()
        return {
            "stopped": result.stopped,
            "url": result.url,
        }

    return run_with_mcp_error_boundary(_stop_local_server)


@tracked_mcp_tool
def kitaru_status() -> dict[str, Any]:
    """Return structured status details for the current Kitaru connection."""

    def _status() -> dict[str, Any]:
        snapshot = inspection.build_runtime_snapshot()
        return inspection.serialize_runtime_snapshot(snapshot)

    return run_with_mcp_error_boundary(_status)


@tracked_mcp_tool
def kitaru_stacks_list() -> list[dict[str, Any]]:
    """List available stacks from the active connection context."""
    return run_with_mcp_error_boundary(
        lambda: [
            inspection.serialize_stack(entry.stack, is_managed=entry.is_managed)
            for entry in stack_ops._list_stack_entries()
        ]
    )


@tracked_mcp_tool
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


@tracked_mcp_tool
def kitaru_info(
    all: bool = False,
    all_packages: bool = False,
    packages: list[str] | None = None,
) -> dict[str, Any]:
    """Return detailed environment diagnostics for the current Kitaru setup.

    Equivalent to `kitaru info --output json`. Use `all=True` for the full
    diagnostic including all packages and environment type.
    """

    def _info() -> dict[str, Any]:
        include_packages = all or all_packages
        package_names = None if include_packages else (packages if packages else None)
        include_environment_type = all
        include_provenance_details = all

        snapshot = inspection.build_runtime_snapshot(
            include_packages=include_packages,
            package_names=package_names,
            include_environment_type=include_environment_type,
            include_provenance_details=include_provenance_details,
        )
        return inspection.serialize_runtime_snapshot(
            snapshot,
            include_provenance_details=include_provenance_details,
        )

    return run_with_mcp_error_boundary(_info)


@tracked_mcp_tool
def kitaru_clean_preview(
    scope: Literal["project", "global", "all"] = "project",
) -> dict[str, Any]:
    """Preview what `kitaru clean <scope>` would delete (dry-run only).

    Returns the same payload as `kitaru clean <scope> --dry-run --output json`.
    This tool is strictly read-only and never performs actual cleanup.
    To execute cleanup, use the CLI: `kitaru clean <scope> --yes`.
    """

    def _preview() -> dict[str, Any]:
        clean_scope = cleanup.CleanScope(scope)
        plan = cleanup.build_cleanup_plan(clean_scope)
        result = cleanup.build_cleanup_preview_result(plan)
        return cleanup.serialize_cleanup_result(result)

    return run_with_mcp_error_boundary(_preview)


def main() -> None:
    """Entry point for the `kitaru-mcp` console script."""
    set_source("mcp")
    track(AnalyticsEvent.MCP_SERVER_STARTED)
    mcp.run(transport="stdio")
