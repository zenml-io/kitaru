"""Kitaru MCP server tools.

This module exposes structured MCP tools for querying and managing Kitaru
executions. The server reuses `KitaruClient` and selected CLI-equivalent logic
for status and stack inspection.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol, runtime_checkable
from urllib.parse import urlparse

from zenml.client import Client
from zenml.config.global_config import GlobalConfiguration
from zenml.utils.server_utils import connected_to_local_server, get_local_server

from _kitaru_bootstrap import resolve_installed_version
from kitaru.client import (
    ArtifactRef,
    CheckpointAttempt,
    CheckpointCall,
    Execution,
    FailureInfo,
    KitaruClient,
    LogEntry,
    PendingWait,
)
from kitaru.config import (
    KITARU_PROJECT_ENV,
    ActiveEnvironmentVariable,
    ResolvedLogStore,
    StackInfo,
    _kitaru_config_dir,
    _read_runtime_connection_config,
    active_stack_log_store,
    list_active_kitaru_environment_variables,
    resolve_log_store,
)
from kitaru.config import list_stacks as get_available_stacks

_MCP_INSTALL_ERROR = (
    "MCP server dependencies are not installed. Install with: pip install kitaru[mcp]"
)


@runtime_checkable
class _FlowHandleLike(Protocol):
    """Protocol for flow handles returned by `.run()` / `.deploy()`."""

    @property
    def exec_id(self) -> str: ...


@runtime_checkable
class _FlowTarget(Protocol):
    """Protocol for MCP-runnable flow objects."""

    def run(self, *args: Any, **kwargs: Any) -> _FlowHandleLike: ...

    def deploy(self, *args: Any, **kwargs: Any) -> _FlowHandleLike: ...


@dataclass
class RuntimeSnapshot:
    """Resolved runtime information for status-style MCP responses."""

    sdk_version: str
    connection: str
    connection_target: str
    config_directory: str
    server_url: str | None = None
    active_user: str | None = None
    project_override: str | None = None
    active_stack: str | None = None
    repository_root: str | None = None
    server_version: str | None = None
    server_database: str | None = None
    server_deployment_type: str | None = None
    local_server_status: str | None = None
    warning: str | None = None
    log_store_status: str | None = None
    log_store_warning: str | None = None
    environment: list[ActiveEnvironmentVariable] = field(default_factory=list)


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


def _load_module_from_python_path(module_path: str) -> ModuleType:
    """Load a Python module from a filesystem path."""
    path = Path(module_path).expanduser().resolve()
    if not path.exists():
        raise ValueError(f"Flow module path does not exist: {module_path}")
    if path.suffix != ".py":
        raise ValueError(
            "Flow target file must be a Python file ending in `.py` "
            f"(received: {module_path})."
        )

    module_name = f"_kitaru_mcp_run_target_{abs(hash(path))}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Unable to load Python module from path: {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_flow_target(target: str) -> _FlowTarget:
    """Load `<module_or_file>:<flow_name>` into a runnable flow object."""
    module_ref, separator, attr_name = target.partition(":")
    if separator != ":" or not module_ref or not attr_name:
        raise ValueError(
            "Flow target must use `<module_or_file>:<flow_name>` format "
            f"(received: {target!r})."
        )

    try:
        if module_ref.endswith(".py"):
            module = _load_module_from_python_path(module_ref)
        else:
            module = importlib.import_module(module_ref)
    except Exception as exc:
        raise ValueError(f"Unable to import flow module `{module_ref}`: {exc}") from exc

    try:
        flow_obj = getattr(module, attr_name)
    except AttributeError as exc:
        raise ValueError(
            f"Flow target `{target}` was not found: module `{module_ref}` "
            f"has no attribute `{attr_name}`."
        ) from exc

    if not isinstance(flow_obj, _FlowTarget):
        raise ValueError(
            f"Target `{target}` is not a Kitaru flow object. "
            "Expected an object created by `@flow` with `.run()` support."
        )

    return flow_obj


def _qualified_type_name(value: Any) -> str:
    """Return the fully qualified runtime type name for a value."""
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _to_jsonable(value: Any, *, fallback_repr: bool) -> Any:
    """Convert a value into a JSON-serializable representation."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, Mapping):
        return {
            str(key): _to_jsonable(item, fallback_repr=fallback_repr)
            for key, item in value.items()
        }

    if isinstance(value, (set, frozenset)):
        return [
            _to_jsonable(item, fallback_repr=fallback_repr)
            for item in sorted(value, key=repr)
        ]

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_to_jsonable(item, fallback_repr=fallback_repr) for item in value]

    if is_dataclass(value) and not isinstance(value, type):
        return _to_jsonable(asdict(value), fallback_repr=fallback_repr)

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return _to_jsonable(model_dump(mode="python"), fallback_repr=fallback_repr)

    if fallback_repr:
        return repr(value)

    raise TypeError(
        "Value cannot be serialized to JSON-compatible data: "
        f"{_qualified_type_name(value)}"
    )


def _serialize_failure(failure: FailureInfo | None) -> dict[str, Any] | None:
    """Serialize optional failure details."""
    if failure is None:
        return None

    return {
        "message": failure.message,
        "exception_type": failure.exception_type,
        "traceback": failure.traceback,
        "origin": _to_jsonable(failure.origin, fallback_repr=True),
    }


def _serialize_pending_wait(wait: PendingWait | None) -> dict[str, Any] | None:
    """Serialize optional pending wait details."""
    if wait is None:
        return None

    return {
        "wait_id": wait.wait_id,
        "name": wait.name,
        "question": wait.question,
        "schema": _to_jsonable(wait.schema, fallback_repr=True),
        "metadata": _to_jsonable(wait.metadata, fallback_repr=True),
        "entered_waiting_at": _to_jsonable(wait.entered_waiting_at, fallback_repr=True),
    }


def _serialize_artifact_ref(artifact: ArtifactRef) -> dict[str, Any]:
    """Serialize artifact metadata."""
    return {
        "artifact_id": artifact.artifact_id,
        "name": artifact.name,
        "kind": artifact.kind,
        "save_type": artifact.save_type,
        "producing_call": artifact.producing_call,
        "metadata": _to_jsonable(artifact.metadata, fallback_repr=True),
    }


def _serialize_checkpoint_attempt(attempt: CheckpointAttempt) -> dict[str, Any]:
    """Serialize checkpoint-attempt details."""
    return {
        "attempt_id": attempt.attempt_id,
        "status": attempt.status.value,
        "started_at": _to_jsonable(attempt.started_at, fallback_repr=True),
        "ended_at": _to_jsonable(attempt.ended_at, fallback_repr=True),
        "metadata": _to_jsonable(attempt.metadata, fallback_repr=True),
        "failure": _serialize_failure(attempt.failure),
    }


def _serialize_checkpoint_call(checkpoint: CheckpointCall) -> dict[str, Any]:
    """Serialize checkpoint-call details."""
    return {
        "call_id": checkpoint.call_id,
        "name": checkpoint.name,
        "status": checkpoint.status.value,
        "started_at": _to_jsonable(checkpoint.started_at, fallback_repr=True),
        "ended_at": _to_jsonable(checkpoint.ended_at, fallback_repr=True),
        "metadata": _to_jsonable(checkpoint.metadata, fallback_repr=True),
        "original_call_id": checkpoint.original_call_id,
        "parent_call_ids": checkpoint.parent_call_ids,
        "failure": _serialize_failure(checkpoint.failure),
        "attempts": [
            _serialize_checkpoint_attempt(attempt) for attempt in checkpoint.attempts
        ],
        "artifacts": [
            _serialize_artifact_ref(artifact) for artifact in checkpoint.artifacts
        ],
    }


def _serialize_execution_summary(execution: Execution) -> dict[str, Any]:
    """Serialize execution list-item details."""
    return {
        "exec_id": execution.exec_id,
        "flow_name": execution.flow_name,
        "status": execution.status.value,
        "started_at": _to_jsonable(execution.started_at, fallback_repr=True),
        "ended_at": _to_jsonable(execution.ended_at, fallback_repr=True),
        "stack_name": execution.stack_name,
        "status_reason": execution.status_reason,
        "pending_wait": _serialize_pending_wait(execution.pending_wait),
        "failure": _serialize_failure(execution.failure),
        "metadata": _to_jsonable(execution.metadata, fallback_repr=True),
        "checkpoint_count": len(execution.checkpoints),
        "artifact_count": len(execution.artifacts),
    }


def _serialize_execution(execution: Execution) -> dict[str, Any]:
    """Serialize full execution details."""
    return {
        **_serialize_execution_summary(execution),
        "frozen_execution_spec": _to_jsonable(
            execution.frozen_execution_spec,
            fallback_repr=True,
        ),
        "original_exec_id": execution.original_exec_id,
        "checkpoints": [
            _serialize_checkpoint_call(checkpoint)
            for checkpoint in execution.checkpoints
        ],
        "artifacts": [
            _serialize_artifact_ref(artifact) for artifact in execution.artifacts
        ],
    }


def _serialize_artifact_value(value: Any) -> dict[str, Any]:
    """Serialize an artifact payload value for MCP transport."""
    value_type = _qualified_type_name(value)
    try:
        serialized_value = _to_jsonable(value, fallback_repr=False)
        return {
            "value": serialized_value,
            "value_format": "json",
            "value_type": value_type,
        }
    except TypeError:
        return {
            "value": repr(value),
            "value_format": "repr",
            "value_type": value_type,
        }


def _serialize_stack(stack: StackInfo) -> dict[str, Any]:
    """Serialize stack information for MCP output."""
    return {
        "id": stack.id,
        "name": stack.name,
        "is_active": stack.is_active,
    }


def _serialize_runtime_snapshot(snapshot: RuntimeSnapshot) -> dict[str, Any]:
    """Serialize runtime status details for MCP output."""
    return _to_jsonable(snapshot, fallback_repr=True)


def _log_store_mismatch_details(
    preferred: ResolvedLogStore,
) -> tuple[str | None, str | None]:
    """Return status-row + warning text when preferred and active backends differ."""
    if preferred.source == "default":
        return None, None

    active_store = active_stack_log_store()
    if active_store is None:
        return None, None

    if active_store.backend == preferred.backend:
        return None, None

    status_row = f"{preferred.backend} (preferred) ⚠ stack uses {active_store.backend}"

    active_label = active_store.backend
    if active_store.stack_name:
        active_label = f"{active_store.backend} (stack: {active_store.stack_name})"

    warning = "\n".join(
        [
            f"Active ZenML stack uses: {active_label}",
            "The Kitaru log-store preference is not wired into stack selection yet.",
            "Actual runtime logs go to the stack's log store, not this preference.",
        ]
    )
    return status_row, warning


def _combine_warnings(*warnings: str | None) -> str | None:
    """Combine non-empty warning messages into one multiline block."""
    rendered = [warning for warning in warnings if warning]
    if not rendered:
        return None
    return "\n".join(rendered)


def _format_log_entry(entry: LogEntry) -> str:
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


def _format_execution_logs(entries: list[LogEntry]) -> str:
    """Render execution logs as plain text for MCP agent readability."""
    if not entries:
        return "No log entries found."
    return "\n".join(_format_log_entry(entry) for entry in entries)


def _describe_local_server() -> str:
    """Summarize the state of the local Kitaru-compatible server, if any."""
    try:
        local_server = get_local_server()
    except ImportError:
        return "unavailable (local runtime support not installed)"
    if local_server is None:
        return "not started"

    provider = local_server.config.provider.value
    if local_server.status and local_server.status.url:
        return f"running at {local_server.status.url} ({provider})"

    if local_server.status and local_server.status.status_message:
        return (
            f"registered but unavailable ({provider}: "
            f"{local_server.status.status_message})"
        )

    return f"registered but unavailable ({provider})"


def _connected_to_local_server() -> bool:
    """Safely check whether the current client is bound to a local server."""
    try:
        return connected_to_local_server()
    except ImportError:
        return False


def _build_snapshot_without_local_store(
    gc: GlobalConfiguration,
) -> RuntimeSnapshot:
    """Build a degraded snapshot when local runtime support is unavailable."""
    return RuntimeSnapshot(
        sdk_version=resolve_installed_version(),
        connection="local mode (unavailable)",
        connection_target="unavailable",
        config_directory=str(_kitaru_config_dir()),
        local_server_status=_describe_local_server(),
        warning=(
            "Local Kitaru runtime support is unavailable in this environment. "
            "Connect to a Kitaru server to keep working, or install the local "
            "runtime dependencies if you want the built-in local stack."
        ),
        environment=list_active_kitaru_environment_variables(),
    )


def _uses_stale_local_server_url(
    server_url: str | None,
    local_server_status: str | None,
) -> bool:
    """Check for a localhost URL that points at a stopped local server."""
    if not server_url or not local_server_status:
        return False

    hostname = urlparse(server_url).hostname
    return hostname in {"127.0.0.1", "localhost", "::1"} and (
        "unavailable" in local_server_status
    )


def _build_runtime_snapshot() -> RuntimeSnapshot:
    """Resolve the current Kitaru runtime state."""
    gc = GlobalConfiguration()
    try:
        store_cfg = gc.store_configuration
        uses_local_store = gc.uses_local_store
    except ImportError:
        return _build_snapshot_without_local_store(gc)

    connection_target = str(store_cfg.url)
    if uses_local_store:
        connection = "local database"
        server_url = None
    elif _connected_to_local_server():
        connection = "local Kitaru server"
        server_url = connection_target
    else:
        connection = "remote Kitaru server"
        server_url = connection_target

    snapshot = RuntimeSnapshot(
        sdk_version=resolve_installed_version(),
        connection=connection,
        connection_target=connection_target,
        server_url=server_url,
        config_directory=str(_kitaru_config_dir()),
        local_server_status=_describe_local_server(),
        environment=list_active_kitaru_environment_variables(),
    )

    if _uses_stale_local_server_url(server_url, snapshot.local_server_status):
        snapshot.warning = (
            "The configured Kitaru server points to a stopped local server. "
            "Start it again or run `kitaru logout` to clear the stale "
            "connection."
        )
        return snapshot

    # Detect explicit project override (env var or runtime configure())
    project_env = os.environ.get(KITARU_PROJECT_ENV)
    runtime_conn = _read_runtime_connection_config()
    if project_env:
        snapshot.project_override = project_env
    elif runtime_conn.project:
        snapshot.project_override = runtime_conn.project

    try:
        client = Client()
        store_info = client.zen_store.get_store_info()
        snapshot.active_user = client.active_user.name
        snapshot.active_stack = client.active_stack_model.name
        snapshot.repository_root = str(client.root) if client.root else None
        snapshot.server_version = str(store_info.version)
        snapshot.server_database = str(store_info.database_type)
        snapshot.server_deployment_type = str(store_info.deployment_type)
    except Exception as exc:  # pragma: no cover - backend-dependent runtime path
        snapshot.warning = f"Unable to query the configured store: {exc}"

    try:
        preferred_log_store = resolve_log_store()
    except ValueError as exc:
        snapshot.log_store_warning = (
            f"Unable to resolve Kitaru log-store preference: {exc}"
        )
        return snapshot

    log_store_status, log_store_warning = _log_store_mismatch_details(
        preferred_log_store
    )
    snapshot.log_store_status = log_store_status
    snapshot.log_store_warning = log_store_warning

    return snapshot


def _list_executions_filtered(
    client: KitaruClient,
    *,
    flow: str | None,
    status: str | None,
    stack: str | None,
    limit: int | None,
) -> list[Execution]:
    """List executions with optional post-filtering for stack."""
    if limit is not None and limit < 1:
        raise ValueError("`limit` must be >= 1 when provided.")

    if stack is None:
        return client.executions.list(flow=flow, status=status, limit=limit)

    executions = client.executions.list(flow=flow, status=status, limit=None)
    filtered = [execution for execution in executions if execution.stack_name == stack]
    if limit is not None:
        return filtered[:limit]
    return filtered


def _latest_execution_filtered(
    client: KitaruClient,
    *,
    flow: str | None,
    status: str | None,
    stack: str | None,
) -> Execution:
    """Resolve the latest execution with optional stack filtering."""
    if stack is None:
        return client.executions.latest(flow=flow, status=status)

    executions = _list_executions_filtered(
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
    """Validate one value against a simple JSON-schema `type` label."""
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


def _validate_wait_input_schema(
    *, wait_schema: dict[str, Any] | None, value: Any
) -> None:
    """Run lightweight type validation against wait schema when available."""
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


@mcp.tool()
def kitaru_executions_list(
    status: str | None = None,
    flow: str | None = None,
    stack: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """List executions with optional status/flow/stack filters."""
    client = KitaruClient()
    executions = _list_executions_filtered(
        client,
        flow=flow,
        status=status,
        stack=stack,
        limit=limit,
    )
    return [_serialize_execution_summary(execution) for execution in executions]


@mcp.tool()
def kitaru_executions_get(exec_id: str) -> dict[str, Any]:
    """Get detailed information for one execution."""
    execution = KitaruClient().executions.get(exec_id)
    return _serialize_execution(execution)


@mcp.tool()
def kitaru_executions_latest(
    status: str | None = None,
    flow: str | None = None,
    stack: str | None = None,
) -> dict[str, Any]:
    """Get the most recent execution matching the provided filters."""
    client = KitaruClient()
    execution = _latest_execution_filtered(
        client,
        flow=flow,
        status=status,
        stack=stack,
    )
    return _serialize_execution(execution)


@mcp.tool()
def get_execution_logs(
    exec_id: str,
    checkpoint: str | None = None,
    source: str = "step",
    limit: int = 200,
) -> str:
    """Fetch runtime log entries for a Kitaru execution."""
    if limit < 1:
        raise ValueError("`limit` must be >= 1.")

    entries = KitaruClient().executions.logs(
        exec_id,
        checkpoint=checkpoint,
        source=source,
        limit=limit,
    )
    return _format_execution_logs(entries)


@mcp.tool()
def kitaru_executions_run(
    target: str,
    args: dict[str, Any] | None = None,
    stack: str | None = None,
) -> dict[str, Any]:
    """Start or deploy a flow from `<module_or_file>:<flow_name>` target."""
    if args is not None and not isinstance(args, dict):
        raise ValueError("`args` must be an object when provided.")

    flow_target = _load_flow_target(target)
    flow_inputs = args or {}

    if stack:
        handle = flow_target.deploy(stack=stack, **flow_inputs)
        invocation = "deploy"
    else:
        handle = flow_target.run(**flow_inputs)
        invocation = "run"

    if not isinstance(handle, _FlowHandleLike):
        raise ValueError(
            "Flow execution did not return a valid handle with an `exec_id`."
        )

    payload: dict[str, Any] = {
        "exec_id": handle.exec_id,
        "invocation": invocation,
        "target": target,
        "execution": None,
        "warning": None,
    }

    try:
        execution = KitaruClient().executions.get(handle.exec_id)
    except Exception as exc:
        payload["warning"] = (
            f"Execution started successfully, but details are not available yet: {exc}"
        )
        return payload

    payload["execution"] = _serialize_execution(execution)
    return payload


@mcp.tool()
def kitaru_executions_cancel(exec_id: str) -> dict[str, Any]:
    """Cancel one execution and return updated details."""
    execution = KitaruClient().executions.cancel(exec_id)
    return _serialize_execution(execution)


@mcp.tool()
def kitaru_executions_input(exec_id: str, wait: str, value: Any) -> dict[str, Any]:
    """Provide input to a waiting execution and return updated details."""
    client = KitaruClient()

    current_execution = client.executions.get(exec_id)
    pending_wait = current_execution.pending_wait
    if pending_wait is not None and wait in {pending_wait.name, pending_wait.wait_id}:
        _validate_wait_input_schema(wait_schema=pending_wait.schema, value=value)

    updated_execution = client.executions.input(exec_id, wait=wait, value=value)
    return _serialize_execution(updated_execution)


@mcp.tool()
def kitaru_executions_retry(exec_id: str) -> dict[str, Any]:
    """Retry one failed execution and return updated details."""
    execution = KitaruClient().executions.retry(exec_id)
    return _serialize_execution(execution)


@mcp.tool()
def kitaru_executions_replay(
    exec_id: str,
    from_: str,
    overrides: dict[str, Any] | None = None,
    flow_inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay an execution and return structured replay details."""
    if flow_inputs is not None and not isinstance(flow_inputs, dict):
        raise ValueError("`flow_inputs` must be an object when provided.")

    replay_inputs = flow_inputs or {}
    execution = KitaruClient().executions.replay(
        exec_id,
        from_=from_,
        overrides=overrides,
        **replay_inputs,
    )

    return {
        "available": True,
        "operation": "replay",
        "execution": _serialize_execution(execution),
    }


@mcp.tool()
def kitaru_artifacts_list(
    exec_id: str,
    name: str | None = None,
    kind: str | None = None,
    producing_call: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """List artifact metadata for one execution."""
    artifacts = KitaruClient().artifacts.list(
        exec_id,
        name=name,
        kind=kind,
        producing_call=producing_call,
        limit=limit,
    )
    return [_serialize_artifact_ref(artifact) for artifact in artifacts]


@mcp.tool()
def kitaru_artifacts_get(artifact_id: str) -> dict[str, Any]:
    """Get one artifact's metadata and loaded value."""
    artifact = KitaruClient().artifacts.get(artifact_id)
    loaded = artifact.load()

    payload = _serialize_artifact_ref(artifact)
    payload.update(_serialize_artifact_value(loaded))
    return payload


@mcp.tool()
def kitaru_status() -> dict[str, Any]:
    """Return structured status details for the current Kitaru connection."""
    snapshot = _build_runtime_snapshot()
    return _serialize_runtime_snapshot(snapshot)


@mcp.tool()
def kitaru_stacks_list() -> list[dict[str, Any]]:
    """List available stacks from the active connection context."""
    return [_serialize_stack(stack) for stack in get_available_stacks()]


def main() -> None:
    """Entry point for the `kitaru-mcp` console script."""
    mcp.run(transport="stdio")
