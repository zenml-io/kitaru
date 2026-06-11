"""Compatibility facade for runtime inspection and serialization helpers.

The implementation is split internally between runtime/environment inspection
and JSON-compatible serialization. Keep importing from ``kitaru.inspection`` for
public compatibility.
"""

from __future__ import annotations

from kitaru._inspection_runtime import (
    ActiveConfigSelectionProvenance,
    ConfigProvenance,
    RuntimeSnapshot,
    build_runtime_snapshot,
    combine_warnings,
    connected_to_local_server_safe,
    describe_local_server,
    is_registered_local_server_url,
    log_store_mismatch_details,
    uses_stale_local_server_url,
)
from kitaru._inspection_serialization import (
    serialize_artifact_ref,
    serialize_artifact_value,
    serialize_checkpoint_attempt,
    serialize_checkpoint_call,
    serialize_deployment,
    serialize_execution,
    serialize_execution_statistics,
    serialize_execution_statistics_group,
    serialize_execution_summary,
    serialize_failure,
    serialize_flow_deployment_summary,
    serialize_log_entry,
    serialize_model_alias,
    serialize_pending_wait,
    serialize_resolved_log_store,
    serialize_runtime_snapshot,
    serialize_secret_detail,
    serialize_secret_summary,
    serialize_stack,
    serialize_stack_create_result,
    serialize_stack_delete_result,
    serialize_stack_details,
    to_jsonable,
)

__all__ = [
    "ActiveConfigSelectionProvenance",
    "ConfigProvenance",
    "RuntimeSnapshot",
    "build_runtime_snapshot",
    "combine_warnings",
    "connected_to_local_server_safe",
    "describe_local_server",
    "is_registered_local_server_url",
    "log_store_mismatch_details",
    "serialize_artifact_ref",
    "serialize_artifact_value",
    "serialize_checkpoint_attempt",
    "serialize_checkpoint_call",
    "serialize_deployment",
    "serialize_execution",
    "serialize_execution_statistics",
    "serialize_execution_statistics_group",
    "serialize_execution_summary",
    "serialize_failure",
    "serialize_flow_deployment_summary",
    "serialize_log_entry",
    "serialize_model_alias",
    "serialize_pending_wait",
    "serialize_resolved_log_store",
    "serialize_runtime_snapshot",
    "serialize_secret_detail",
    "serialize_secret_summary",
    "serialize_stack",
    "serialize_stack_create_result",
    "serialize_stack_delete_result",
    "serialize_stack_details",
    "to_jsonable",
    "uses_stale_local_server_url",
]
