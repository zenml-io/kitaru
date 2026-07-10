"""Shared helpers for ZenML checkpoint step runs."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, TypeVar

_RETRIED_STEP_STATUS = "retried"
_TStep = TypeVar("_TStep")


def _step_status_value(step: Any) -> str:
    status = getattr(step, "status", "")
    return str(getattr(status, "value", status)).strip().lower()


def visible_checkpoint_step_for_lineage(
    attempts: Sequence[_TStep],
) -> _TStep | None:
    """Return the checkpoint step run exposed by ZenML's run DAG.

    Args:
        attempts: Step-run attempts for one checkpoint lineage, sorted by
            ascending creation time. The attempt fetcher that calls this helper
            requests ``sort_by="asc:created"`` from ZenML before grouping steps.

    Returns:
        The newest non-retried attempt, or the newest attempt when every attempt
        is marked retried.
    """
    for step in reversed(attempts):
        if _step_status_value(step) != _RETRIED_STEP_STATUS:
            return step
    if attempts:
        return attempts[-1]
    return None
