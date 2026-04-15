"""OpenTelemetry span correlation; no-ops when opentelemetry is absent."""

from __future__ import annotations

from typing import Any

from ._kitaru_internal import get_current_checkpoint, get_current_execution_id
from ._logging import logger
from ._tracking import ModelEventContext, ToolEventContext

_get_current_span: Any
try:
    from opentelemetry.trace import get_current_span

    _get_current_span = get_current_span
except ImportError:
    _get_current_span = None


def _attach_kitaru_attrs(event_kind: str, event_id: str, sequence_index: int, turn_index: int) -> None:
    if _get_current_span is None:
        return
    try:
        span = _get_current_span()
        if not span.is_recording():
            return
        checkpoint = get_current_checkpoint()
        span.set_attributes(
            {
                'kitaru.event_id': event_id,
                'kitaru.event_kind': event_kind,
                'kitaru.exec_id': get_current_execution_id() or '',
                'kitaru.checkpoint_id': checkpoint.checkpoint_id if checkpoint is not None else '',
                'kitaru.sequence_index': sequence_index,
                'kitaru.turn_index': turn_index,
            }
        )
    except Exception:
        logger.warning('Failed to attach OpenTelemetry correlation for PydanticAI event.', exc_info=True)


def attach_model_correlation(event_id: str, event_context: ModelEventContext) -> None:
    _attach_kitaru_attrs('llm_call', event_id, event_context.sequence_index, event_context.turn_index)


def attach_tool_correlation(event_id: str, event_context: ToolEventContext) -> None:
    _attach_kitaru_attrs('tool_call', event_id, event_context.sequence_index, event_context.turn_index)
