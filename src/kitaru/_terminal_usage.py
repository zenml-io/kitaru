"""Terminal execution metadata aggregation helpers."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from zenml.models import PipelineRunResponse

from kitaru._client._mappers import _list_checkpoint_attempts_for_run, _to_plain_dict
from kitaru._llm_usage import (
    cache_status_for_checkpoint_status,
    execution_metadata_from_records,
    metadata_has_complete_usage_summary,
    usage_records_from_metadata,
)
from kitaru._source_aliases import (
    normalize_checkpoint_name as _normalize_checkpoint_name,
)
from kitaru.errors import KitaruBackendError

logger = logging.getLogger(__name__)


def _metadata_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return _to_plain_dict(value)
    return {}


def _run_has_llm_usage_summary(run: PipelineRunResponse) -> bool:
    metadata = _metadata_mapping(getattr(run, "run_metadata", None))
    return metadata_has_complete_usage_summary(metadata)


def _persist_terminal_llm_usage_metadata(run: PipelineRunResponse) -> bool:
    """Aggregate LLM usage records and write execution-level metadata."""
    from kitaru.client import KitaruClient
    from kitaru.logging import log_to_execution

    if _run_has_llm_usage_summary(run):
        return True

    client = KitaruClient()
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
    execution_id = str(run.id)
    records.extend(
        usage_records_from_metadata(
            _metadata_mapping(getattr(run, "run_metadata", None)),
            source_attempt_id=f"run:{execution_id}",
        )
    )
    for attempts in attempts_by_lineage.values():
        for step in attempts:
            cache_status = cache_status_for_checkpoint_status(
                getattr(step, "status", None)
            )
            records.extend(
                usage_records_from_metadata(
                    _metadata_mapping(getattr(step, "run_metadata", None)),
                    source_attempt_id=str(step.id),
                    default_checkpoint_name=_normalize_checkpoint_name(
                        str(getattr(step, "name", ""))
                    ),
                    reused=cache_status is not None,
                    reused_cache_status=cache_status or "checkpoint_cache_hit",
                )
            )
    metadata = execution_metadata_from_records(records)
    if not metadata:
        return True
    log_to_execution(str(run.id), **metadata)
    return True


def _safe_persist_terminal_llm_usage_metadata(run: PipelineRunResponse) -> bool:
    """Best-effort terminal LLM usage aggregation."""
    try:
        return _persist_terminal_llm_usage_metadata(run)
    except Exception:
        logger.debug(
            "Failed to persist terminal LLM usage metadata.",
            exc_info=True,
        )
        return False
