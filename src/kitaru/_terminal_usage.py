"""Terminal execution metadata aggregation helpers."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from zenml.models import PipelineRunResponse

from kitaru._checkpoint_steps import visible_checkpoint_step_for_lineage
from kitaru._client._mappers import (
    _list_checkpoint_attempts_for_run_with_zenml_client,
    _to_plain_dict,
)
from kitaru._llm_usage import (
    execution_metadata_from_records,
    flat_usage_metadata_from_records,
    metadata_has_complete_usage_summary,
    metadata_matches_flat_usage_metadata,
    metadata_matches_usage_metadata,
    usage_records_from_metadata,
    usage_reuse_classification,
)
from kitaru._source_aliases import (
    normalize_checkpoint_name as _normalize_checkpoint_name,
)
from kitaru.errors import KitaruBackendError
from kitaru.replay import (
    parse_replay_skipped_steps_metadata,
    replay_step_invocation_id,
)

logger = logging.getLogger(__name__)


def _metadata_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return _to_plain_dict(value)
    return {}


def _run_has_llm_usage_summary(run: PipelineRunResponse) -> bool:
    metadata = _metadata_mapping(getattr(run, "run_metadata", None))
    return metadata_has_complete_usage_summary(metadata)


def _list_checkpoint_attempts_for_run(
    *,
    run: PipelineRunResponse,
    client: Any,
) -> dict[str, list[Any]]:
    """Fetch checkpoint attempts with the ZenML client valid in this context."""
    return _list_checkpoint_attempts_for_run_with_zenml_client(
        run=run,
        zenml_client=client,
    )


def _persist_terminal_llm_usage_metadata(
    run: PipelineRunResponse,
    *,
    zenml_client: Any | None = None,
) -> bool:
    """Aggregate LLM usage records and write terminal cost metadata."""
    from zenml.client import Client

    from kitaru.logging import log_to_checkpoint, log_to_execution

    run_metadata = _metadata_mapping(getattr(run, "run_metadata", None))
    replay_skipped_steps = parse_replay_skipped_steps_metadata(run_metadata)
    client = zenml_client or Client()
    try:
        attempts_by_lineage = _list_checkpoint_attempts_for_run(run=run, client=client)
    except KitaruBackendError:
        logger.debug(
            "Skipping terminal LLM usage aggregation because all checkpoint "
            "attempts could not be fetched.",
            exc_info=True,
        )
        return False

    records: list[dict[str, Any]] = []
    checkpoint_metadata_writes: list[tuple[str, dict[str, int | float]]] = []
    execution_id = str(run.id)
    records.extend(
        usage_records_from_metadata(
            run_metadata,
            source_attempt_id=f"run:{execution_id}",
        )
    )
    for attempts in attempts_by_lineage.values():
        checkpoint_records: list[dict[str, Any]] = []
        for step in attempts:
            reused, reused_cache_status = usage_reuse_classification(
                replay_reused=replay_step_invocation_id(step) in replay_skipped_steps,
                checkpoint_status=getattr(step, "status", None),
            )
            step_records = usage_records_from_metadata(
                _metadata_mapping(getattr(step, "run_metadata", None)),
                source_attempt_id=str(step.id),
                default_checkpoint_id=str(step.id),
                default_checkpoint_name=_normalize_checkpoint_name(
                    str(getattr(step, "name", ""))
                ),
                reused=reused,
                reused_cache_status=reused_cache_status,
            )
            checkpoint_records.extend(step_records)
            records.extend(step_records)

        checkpoint_metadata = flat_usage_metadata_from_records(checkpoint_records)
        if not checkpoint_metadata:
            continue
        visible_step = visible_checkpoint_step_for_lineage(attempts)
        if visible_step is None:
            continue
        visible_step_metadata = _metadata_mapping(
            getattr(visible_step, "run_metadata", None)
        )
        if metadata_matches_flat_usage_metadata(
            visible_step_metadata,
            checkpoint_metadata,
        ):
            continue
        checkpoint_metadata_writes.append((str(visible_step.id), checkpoint_metadata))

    metadata_write_failed = False
    metadata = execution_metadata_from_records(records)
    if metadata and not metadata_matches_usage_metadata(run_metadata, metadata):
        try:
            log_to_execution(str(run.id), _client=client, **metadata)
        except Exception:
            metadata_write_failed = True
            logger.debug(
                "Failed to persist terminal LLM usage metadata on execution %s.",
                run.id,
                exc_info=True,
            )

    for checkpoint_id, checkpoint_metadata in checkpoint_metadata_writes:
        try:
            log_to_checkpoint(checkpoint_id, _client=client, **checkpoint_metadata)
        except Exception:
            metadata_write_failed = True
            logger.debug(
                "Failed to persist terminal LLM usage metadata on checkpoint %s.",
                checkpoint_id,
                exc_info=True,
            )

    return not metadata_write_failed


def _safe_persist_terminal_llm_usage_metadata(
    run: PipelineRunResponse,
    *,
    zenml_client: Any | None = None,
) -> bool:
    """Best-effort terminal LLM usage aggregation."""
    try:
        return _persist_terminal_llm_usage_metadata(run, zenml_client=zenml_client)
    except Exception:
        logger.debug(
            "Failed to persist terminal LLM usage metadata.",
            exc_info=True,
        )
        return False
