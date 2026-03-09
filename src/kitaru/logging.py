"""Structured metadata logging.

``kitaru.log()`` attaches structured key-value metadata to the current
checkpoint or execution. It is context-sensitive: inside a checkpoint
it attaches to that checkpoint; inside a flow but outside a checkpoint
it attaches to the execution.

Example::

    from kitaru import checkpoint

    @checkpoint
    def call_model(prompt: str) -> str:
        response = model.generate(prompt)
        kitaru.log(
            tokens=response.usage.total_tokens,
            cost=response.usage.cost,
            model=response.model,
        )
        return response.text
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from zenml.client import Client
from zenml.enums import MetadataResourceTypes
from zenml.models.v2.misc.run_metadata import RunMetadataResource

from kitaru.errors import KitaruContextError, KitaruStateError
from kitaru.runtime import (
    _get_current_checkpoint_id,
    _get_current_execution_id,
    _is_inside_checkpoint,
    _is_inside_flow,
)

_LOG_OUTSIDE_FLOW_ERROR = "kitaru.log() can only be called inside a @flow."
_LOG_MISSING_EXECUTION_ID_ERROR = (
    "kitaru.log() requires an active execution ID inside @flow."
)
_LOG_MISSING_CHECKPOINT_ID_ERROR = (
    "kitaru.log() requires an active checkpoint ID inside @checkpoint."
)


def _parse_scope_uuid(scope_id: str, *, scope_name: str) -> UUID:
    """Parse a runtime scope identifier as a UUID.

    Args:
        scope_id: Raw scope identifier from runtime context.
        scope_name: Human-readable scope name for error messages.

    Returns:
        Parsed UUID.

    Raises:
        KitaruStateError: If the scope identifier is not a valid UUID.
    """
    try:
        return UUID(scope_id)
    except ValueError as exc:
        raise KitaruStateError(
            f"kitaru.log() found an invalid {scope_name} ID in runtime scope:"
            f" {scope_id!r}."
        ) from exc


def _resolve_log_target() -> tuple[RunMetadataResource, UUID | None]:
    """Resolve the metadata target resource for `kitaru.log()`."""
    if _is_inside_checkpoint():
        checkpoint_id = _get_current_checkpoint_id()
        if checkpoint_id is None:
            raise KitaruStateError(_LOG_MISSING_CHECKPOINT_ID_ERROR)
        checkpoint_uuid = _parse_scope_uuid(checkpoint_id, scope_name="checkpoint")
        return (
            RunMetadataResource(
                id=checkpoint_uuid,
                type=MetadataResourceTypes.STEP_RUN,
            ),
            checkpoint_uuid,
        )

    if _is_inside_flow():
        execution_id = _get_current_execution_id()
        if execution_id is None:
            raise KitaruStateError(_LOG_MISSING_EXECUTION_ID_ERROR)
        execution_uuid = _parse_scope_uuid(execution_id, scope_name="execution")
        return (
            RunMetadataResource(
                id=execution_uuid,
                type=MetadataResourceTypes.PIPELINE_RUN,
            ),
            None,
        )

    raise KitaruContextError(_LOG_OUTSIDE_FLOW_ERROR)


def log(**kwargs: Any) -> None:
    """Attach structured metadata to the current checkpoint or execution.

    Standard keys include ``cost``, ``tokens``, ``latency``, but arbitrary
    user-defined keys are accepted.

    Notes:
        Values should be JSON-serializable. Metadata is persisted through
        ZenML's run-metadata APIs. Multiple calls in the same scope append
        metadata entries; repeated keys with dictionary values are merged on
        hydration, while repeated non-dictionary keys resolve to latest value.

    Args:
        **kwargs: Key-value pairs to attach as metadata.

    Raises:
        KitaruContextError: If called outside a flow.
        KitaruStateError: If runtime scope IDs are missing/invalid.
    """
    resource, publisher_step_id = _resolve_log_target()
    Client().create_run_metadata(
        metadata=kwargs,
        resources=[resource],
        publisher_step_id=publisher_step_id,
    )
