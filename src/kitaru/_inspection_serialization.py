"""JSON-compatible serialization helpers for Kitaru transport surfaces."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from zenml.models import SecretResponse

from kitaru._client._models import (
    ArtifactRef,
    CheckpointAttempt,
    CheckpointCall,
    Execution,
    ExecutionStatistics,
    ExecutionStatisticsGroup,
    FailureInfo,
    LogEntry,
    PendingWait,
)
from kitaru._inspection_runtime import RuntimeSnapshot
from kitaru.config import (
    ActiveStackLogStore,
    ModelAliasEntry,
    ProjectInfo,
    ResolvedLogStore,
    StackComponentDetails,
    StackDetails,
    StackInfo,
    _StackCreateResult,
    _StackDeleteResult,
)


def _qualified_type_name(value: Any) -> str:
    """Return the fully qualified runtime type name for a value."""
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def to_jsonable(value: Any, *, fallback_repr: bool) -> Any:
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
            str(key): to_jsonable(item, fallback_repr=fallback_repr)
            for key, item in value.items()
        }

    if isinstance(value, (set, frozenset)):
        return [
            to_jsonable(item, fallback_repr=fallback_repr)
            for item in sorted(value, key=repr)
        ]

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [to_jsonable(item, fallback_repr=fallback_repr) for item in value]

    if is_dataclass(value) and not isinstance(value, type):
        return to_jsonable(asdict(value), fallback_repr=fallback_repr)

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return to_jsonable(model_dump(mode="python"), fallback_repr=fallback_repr)

    if fallback_repr:
        return repr(value)

    raise TypeError(
        "Value cannot be serialized to JSON-compatible data: "
        f"{_qualified_type_name(value)}"
    )


def serialize_failure(failure: FailureInfo | None) -> dict[str, Any] | None:
    """Serialize optional failure details."""
    if failure is None:
        return None

    return {
        "message": failure.message,
        "exception_type": failure.exception_type,
        "traceback": failure.traceback,
        "origin": to_jsonable(failure.origin, fallback_repr=True),
    }


def serialize_pending_wait(wait: PendingWait | None) -> dict[str, Any] | None:
    """Serialize optional pending wait details."""
    if wait is None:
        return None

    return {
        "wait_id": wait.wait_id,
        "name": wait.name,
        "question": wait.question,
        "schema": to_jsonable(wait.schema, fallback_repr=True),
        "metadata": to_jsonable(wait.metadata, fallback_repr=True),
        "entered_waiting_at": to_jsonable(wait.entered_waiting_at, fallback_repr=True),
    }


def serialize_artifact_ref(artifact: ArtifactRef) -> dict[str, Any]:
    """Serialize artifact metadata."""
    payload = {
        "artifact_id": artifact.artifact_id,
        "name": artifact.name,
        "kind": artifact.kind,
        "save_type": artifact.save_type,
        "producing_call": artifact.producing_call,
        "metadata": to_jsonable(artifact.metadata, fallback_repr=True),
    }
    if artifact.direction == "input":
        payload["direction"] = "input"
    if artifact.input_type is not None:
        payload["input_type"] = artifact.input_type
    return payload


def serialize_artifact_value(value: Any) -> dict[str, Any]:
    """Serialize an artifact payload value for MCP transport."""
    value_type = _qualified_type_name(value)
    try:
        serialized_value = to_jsonable(value, fallback_repr=False)
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


def serialize_checkpoint_attempt(attempt: CheckpointAttempt) -> dict[str, Any]:
    """Serialize checkpoint-attempt details."""
    return {
        "attempt_id": attempt.attempt_id,
        "status": attempt.status.value,
        "started_at": to_jsonable(attempt.started_at, fallback_repr=True),
        "ended_at": to_jsonable(attempt.ended_at, fallback_repr=True),
        "metadata": to_jsonable(attempt.metadata, fallback_repr=True),
        "failure": serialize_failure(attempt.failure),
        "llm_usage_records": to_jsonable(
            attempt.llm_usage_records,
            fallback_repr=True,
        ),
    }


def serialize_checkpoint_call(checkpoint: CheckpointCall) -> dict[str, Any]:
    """Serialize checkpoint-call details."""
    return {
        "call_id": checkpoint.call_id,
        "name": checkpoint.name,
        "checkpoint_type": checkpoint.checkpoint_type,
        "checkpoint_origin": checkpoint.checkpoint_origin,
        "adapter": checkpoint.adapter,
        "adapter_checkpoint_kind": checkpoint.adapter_checkpoint_kind,
        "replay_input_slots": list(checkpoint.replay_input_slots),
        "replay_output_slots": list(checkpoint.replay_output_slots),
        "status": checkpoint.status.value,
        "started_at": to_jsonable(checkpoint.started_at, fallback_repr=True),
        "ended_at": to_jsonable(checkpoint.ended_at, fallback_repr=True),
        "metadata": to_jsonable(checkpoint.metadata, fallback_repr=True),
        "original_call_id": checkpoint.original_call_id,
        "parent_call_ids": checkpoint.parent_call_ids,
        "failure": serialize_failure(checkpoint.failure),
        "attempts": [
            serialize_checkpoint_attempt(attempt) for attempt in checkpoint.attempts
        ],
        "artifacts": [
            serialize_artifact_ref(artifact) for artifact in checkpoint.artifacts
        ],
        "llm_usage_records": to_jsonable(
            checkpoint.llm_usage_records,
            fallback_repr=True,
        ),
    }


def serialize_execution_summary(execution: Execution) -> dict[str, Any]:
    """Serialize execution list-item details."""
    return {
        "exec_id": execution.exec_id,
        "flow_id": execution.flow_id,
        "flow_name": execution.flow_name,
        "status": execution.status.value,
        "started_at": to_jsonable(execution.started_at, fallback_repr=True),
        "ended_at": to_jsonable(execution.ended_at, fallback_repr=True),
        "stack_name": execution.stack_name,
        "status_reason": execution.status_reason,
        "pending_wait": serialize_pending_wait(execution.pending_wait),
        "failure": serialize_failure(execution.failure),
        "metadata": to_jsonable(execution.metadata, fallback_repr=True),
        "checkpoint_count": len(execution.checkpoints),
        "artifact_count": len(execution.artifacts),
        "llm_usage_summary": to_jsonable(
            execution.llm_usage_summary,
            fallback_repr=True,
        ),
    }


def serialize_execution(execution: Execution) -> dict[str, Any]:
    """Serialize full execution details."""
    return {
        **serialize_execution_summary(execution),
        "frozen_execution_spec": to_jsonable(
            execution.frozen_execution_spec,
            fallback_repr=True,
        ),
        "original_exec_id": execution.original_exec_id,
        "llm_usage_records": to_jsonable(
            execution.llm_usage_records,
            fallback_repr=True,
        ),
        "checkpoints": [
            serialize_checkpoint_call(checkpoint)
            for checkpoint in execution.checkpoints
        ],
        "artifacts": [
            serialize_artifact_ref(artifact) for artifact in execution.artifacts
        ],
    }


def serialize_execution_statistics_group(
    group: ExecutionStatisticsGroup,
) -> dict[str, Any]:
    """Serialize one execution-statistics group."""
    return {
        "keys": to_jsonable(group.keys, fallback_repr=True),
        "execution_count": group.execution_count,
        "metrics": to_jsonable(group.metrics, fallback_repr=True),
    }


def serialize_execution_statistics(
    statistics: ExecutionStatistics,
) -> dict[str, Any]:
    """Serialize grouped execution statistics."""
    groups = [
        serialize_execution_statistics_group(group) for group in statistics.groups
    ]
    return {
        "groups": groups,
        "truncated": statistics.truncated,
        "group_count": len(groups),
    }


def serialize_deployment(deployment: Any) -> dict[str, Any]:
    """Serialize a deployment facade or record for CLI transport."""
    return {
        "deployment_id": deployment.deployment_id,
        "flow": deployment.flow,
        "version": deployment.version,
        "tags": to_jsonable(deployment.tags, fallback_repr=True),
        "commit_sha": getattr(deployment, "commit_sha", None),
        "commit_dirty": getattr(deployment, "commit_dirty", None),
        "image_digest": getattr(deployment, "image_digest", None),
        "created_at": to_jsonable(
            getattr(deployment, "created_at", None),
            fallback_repr=True,
        ),
        "schema": to_jsonable(getattr(deployment, "schema", None), fallback_repr=True),
        "stack": getattr(deployment, "stack", None),
    }


def serialize_flow_deployment_summary(
    flow: str,
    deployments: Sequence[Any],
) -> dict[str, Any]:
    """Serialize a summary for one deployment-backed flow."""
    ordered = sorted(deployments, key=lambda deployment: deployment.version)
    latest = ordered[-1] if ordered else None
    public_tags: dict[str, list[int]] = {}
    default_version: int | None = None
    for deployment in ordered:
        for tag in deployment.tags:
            public_tags.setdefault(tag, []).append(deployment.version)
            if tag == "default":
                default_version = deployment.version

    return {
        "flow": flow,
        "deployment_count": len(ordered),
        "latest_version": latest.version if latest is not None else None,
        "default_version": default_version,
        "tags": public_tags,
        "deployments": [serialize_deployment(deployment) for deployment in ordered],
    }


def serialize_project(project: ProjectInfo) -> dict[str, Any]:
    """Serialize project information for structured output."""
    return {
        "id": project.id,
        "name": project.name,
        "display_name": project.display_name,
        "description": project.description,
        "is_active": project.is_active,
    }


def serialize_stack(
    stack: StackInfo,
    *,
    is_managed: bool | None = None,
) -> dict[str, Any]:
    """Serialize stack information for structured output."""
    payload = {
        "id": stack.id,
        "name": stack.name,
        "is_active": stack.is_active,
    }
    if is_managed is not None:
        payload["is_managed"] = is_managed
    return payload


def serialize_stack_create_result(result: _StackCreateResult) -> dict[str, Any]:
    """Serialize stack-create operation details."""
    payload = serialize_stack(result.stack)
    payload["previous_active_stack"] = result.previous_active_stack
    payload["components_created"] = list(result.components_created)
    payload["stack_type"] = result.stack_type
    if result.service_connectors_created:
        payload["service_connectors_created"] = list(result.service_connectors_created)
    if result.resources:
        payload["resources"] = result.resources
    return payload


def serialize_stack_delete_result(result: _StackDeleteResult) -> dict[str, Any]:
    """Serialize stack-delete operation details."""
    return {
        "deleted_stack": result.deleted_stack,
        "components_deleted": list(result.components_deleted),
        "new_active_stack": result.new_active_stack,
        "recursive": result.recursive,
    }


def _serialize_stack_component_details(
    component: StackComponentDetails,
) -> dict[str, Any]:
    """Serialize one translated stack component for structured stack output."""
    payload: dict[str, Any] = {
        "role": component.role,
        "name": component.name,
    }
    if component.backend is not None:
        payload["backend"] = component.backend
    if component.details:
        payload["details"] = dict(component.details)
    if component.purpose is not None:
        payload["purpose"] = component.purpose
    return payload


def serialize_stack_details(details: StackDetails) -> dict[str, Any]:
    """Serialize stack inspection details for `stack show` style output."""
    payload = serialize_stack(details.stack, is_managed=details.is_managed)
    payload["stack_type"] = details.stack_type
    payload["components"] = [
        _serialize_stack_component_details(component)
        for component in details.components
    ]
    return payload


def serialize_runtime_snapshot(
    snapshot: RuntimeSnapshot,
    *,
    include_provenance_details: bool = False,
) -> dict[str, Any]:
    """Serialize runtime status details for structured output.

    Runtime snapshots keep active stack/project provenance internally so normal
    status/info calls can still produce safety warnings. The detailed raw
    provenance is diagnostic material, though, so structured outputs expose it
    only when callers opt in (``kitaru info --all`` or MCP ``all=True``).
    """
    payload = to_jsonable(snapshot, fallback_repr=True)
    if not include_provenance_details:
        payload["active_stack_provenance"] = None
        payload["active_project_provenance"] = None
    return payload


def serialize_log_entry(entry: LogEntry) -> dict[str, Any]:
    """Serialize one log entry for JSON output."""
    payload: dict[str, Any] = {"message": entry.message}
    for key, value in (
        ("level", entry.level),
        ("timestamp", entry.timestamp),
        ("source", entry.source),
        ("checkpoint_name", entry.checkpoint_name),
        ("module", entry.module),
        ("filename", entry.filename),
        ("lineno", entry.lineno),
    ):
        if value is not None:
            payload[key] = value
    return payload


def serialize_model_alias(entry: ModelAliasEntry) -> dict[str, Any]:
    """Serialize model alias information."""
    return {
        "alias": entry.alias,
        "model": entry.model,
        "secret": entry.secret,
        "is_default": entry.is_default,
    }


def serialize_secret_summary(secret: Any) -> dict[str, Any]:
    """Serialize secret summary information without raw secret values.

    Accepts either a ZenML ``SecretResponse`` (which exposes ``.values``
    as a mapping) or a Kitaru ``SecretSummary`` (which exposes ``.keys``
    as a list).
    """
    from kitaru.secrets import SecretSummary

    if isinstance(secret, SecretSummary):
        keys = sorted(secret.keys)
    else:
        keys = sorted(str(key) for key in secret.values)

    return {
        "id": str(secret.id),
        "name": secret.name,
        "visibility": "private" if secret.private else "public",
        "keys": keys,
        "has_missing_values": bool(getattr(secret, "has_missing_values", False)),
    }


def serialize_secret_detail(
    secret: SecretResponse,
    *,
    show_values: bool,
) -> dict[str, Any]:
    """Serialize secret detail information."""
    payload = serialize_secret_summary(secret)
    if show_values:
        payload["values"] = {
            key: secret.secret_values.get(key, "unavailable")
            for key in sorted(secret.values.keys())
        }
    else:
        payload["values"] = None
    return payload


def serialize_resolved_log_store(
    snapshot: ResolvedLogStore,
    *,
    active_store: ActiveStackLogStore | None = None,
    warning: str | None = None,
) -> dict[str, Any]:
    """Serialize effective log-store information."""
    return {
        "backend": snapshot.backend,
        "endpoint": snapshot.endpoint,
        "api_key_configured": bool(snapshot.api_key),
        "source": snapshot.source,
        "active_stack_backend": active_store.backend if active_store else None,
        "active_stack_name": active_store.stack_name if active_store else None,
        "warning": warning,
    }
