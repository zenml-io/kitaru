"""Reconstruct a stable Gemini interaction from stream events."""

from dataclasses import dataclass, field
from typing import Any

from kitaru.errors import KitaruRuntimeError

from ._stream_shapes import (
    _STREAM_RECONSTRUCTION_POLICY,
    ARGUMENT_DELTA_TYPES,
    CONTENT_DELTA_EVENT_TYPE,
    DELTA_EVENT_TYPES,
    ERROR_EVENT_TYPE,
    INTERACTION_COMPLETED_EVENT_TYPES,
    INTERACTION_EVENT_TYPES,
    MEDIA_DELTA_TYPES,
    START_EVENT_TYPES,
    STOP_EVENT_TYPES,
    TEXT_DELTA_TYPES,
    THOUGHT_DELTA_TYPES,
    coerce_string,
    delta_from_event,
    delta_text,
    environment_id,
    event_type,
    extract,
    function_name_from_step,
    interaction_from_event,
    is_safe_output_text_source,
    normalized_delta_type,
    role_from_step,
    sequence_or_empty,
    step_from_event,
    step_index,
    step_type_from_step,
    usage_from,
)
from ._types import GeminiInteractionRequest

_MAX_ACCUMULATED_STREAM_STEPS = 1_024
_MAX_ACCUMULATED_STREAM_TEXT_CHARS = 1_000_000
_MAX_ACCUMULATED_STREAM_ARGUMENT_CHARS = 1_000_000


@dataclass
class _AccumulatedStep:
    """Interaction step reconstructed from Gemini stream events."""

    index: int
    step_id: str | None = None
    type: str | None = None
    role: str | None = None
    status: str | None = None
    call_id: str | None = None
    tool_name: str | None = None
    text_chunks: list[str] = field(default_factory=list)
    argument_chunks: list[str] = field(default_factory=list)
    text_char_count: int = 0
    argument_char_count: int = 0
    text_truncated: bool = False
    arguments_truncated: bool = False
    thought_chunk_count: int = 0
    media_delta_count: int = 0

    def update_from_step(self, value: Any) -> None:
        self.step_id = coerce_string(extract(value, "id")) or self.step_id
        self.type = coerce_string(extract(value, "type")) or self.type
        self.role = role_from_step(value) or self.role
        self.status = coerce_string(extract(value, "status")) or self.status
        self.call_id = coerce_string(extract(value, "call_id")) or self.call_id
        self.tool_name = function_name_from_step(value) or self.tool_name

    def append_text(self, value: str, remaining_chars: int) -> int:
        accepted = self._append_bounded(
            self.text_chunks,
            value,
            remaining_chars,
            truncated_attr="text_truncated",
        )
        self.text_char_count += accepted
        return accepted

    def append_arguments(self, value: str, remaining_chars: int) -> int:
        accepted = self._append_bounded(
            self.argument_chunks,
            value,
            remaining_chars,
            truncated_attr="arguments_truncated",
        )
        self.argument_char_count += accepted
        return accepted

    def as_step(self) -> dict[str, Any]:
        step_type = self.type or (
            "function_call" if self.argument_chunks else "message"
        )
        step: dict[str, Any] = {
            "type": step_type,
            "status": self.status or "completed",
        }
        if self.step_id is not None:
            step["id"] = self.step_id
        if self.call_id is not None:
            step["call_id"] = self.call_id
        if self.tool_name is not None:
            step["name"] = self.tool_name
        if self.role is not None:
            step["role"] = self.role
        if self.text_chunks:
            step["text"] = "".join(self.text_chunks)
        if self.argument_chunks:
            step["arguments_delta"] = "".join(self.argument_chunks)
        if self.text_truncated:
            step["text_delta_truncated"] = True
        if self.arguments_truncated:
            step["arguments_delta_truncated"] = True
        if self.thought_chunk_count:
            step["thought_summary_delta_count"] = self.thought_chunk_count
        if self.media_delta_count:
            step["media_delta_count"] = self.media_delta_count
        return step

    def _append_bounded(
        self,
        chunks: list[str],
        value: str,
        remaining_chars: int,
        *,
        truncated_attr: str,
    ) -> int:
        if remaining_chars <= 0:
            setattr(self, truncated_attr, True)
            return 0
        if len(value) <= remaining_chars:
            chunks.append(value)
            return len(value)
        chunks.append(value[:remaining_chars])
        setattr(self, truncated_attr, True)
        return remaining_chars


@dataclass
class _StreamAccumulator:
    """Build a stable interaction-like object from Gemini stream events."""

    request: GeminiInteractionRequest
    event_count: int = 0
    counts_by_event_type: dict[str, int] = field(default_factory=dict)
    last_event_id: str | None = None
    interaction_id: str | None = None
    previous_interaction_id: str | None = None
    status: str | None = None
    model: str | None = None
    agent: str | None = None
    environment_id: str | None = None
    usage: dict[str, Any] | None = None
    latest_interaction: Any | None = None
    steps: list[_AccumulatedStep] = field(default_factory=list)
    steps_by_index: dict[int, _AccumulatedStep] = field(default_factory=dict)
    steps_by_id: dict[str, _AccumulatedStep] = field(default_factory=dict)
    total_text_chars: int = 0
    total_argument_chars: int = 0
    text_truncated: bool = False
    arguments_truncated: bool = False

    def seed_interaction(self, interaction: Any) -> None:
        """Seed metadata from an interaction created before observation starts."""
        self._merge_interaction_metadata(interaction)

    def record(self, event: Any) -> None:
        self.event_count += 1
        kind = event_type(event)
        self.counts_by_event_type[kind] = self.counts_by_event_type.get(kind, 0) + 1
        self.last_event_id = (
            coerce_string(extract(event, "id"))
            or coerce_string(extract(event, "event_id"))
            or self.last_event_id
        )
        if kind == ERROR_EVENT_TYPE:
            raise KitaruRuntimeError(
                "Gemini streamed interaction yielded a provider error event. "
                "Kitaru will not store an incomplete streamed provider result."
            )
        if kind in INTERACTION_EVENT_TYPES:
            self._record_interaction_event(event)
            if kind in INTERACTION_COMPLETED_EVENT_TYPES:
                terminal_status = coerce_string(
                    extract(event, "status")
                ) or coerce_string(extract(interaction_from_event(event), "status"))
                if terminal_status is None:
                    self.status = "completed"
            return
        if kind in START_EVENT_TYPES:
            self._step_for_event(event).update_from_step(step_from_event(event))
            return
        if kind in DELTA_EVENT_TYPES:
            self._record_step_delta(event, kind=kind)
            return
        if kind in STOP_EVENT_TYPES:
            step = self._step_for_event(event)
            step.update_from_step(step_from_event(event))
            step.status = step.status or "completed"

    def build_interaction(self) -> dict[str, Any]:
        final = self.latest_interaction
        interaction_id = (
            coerce_string(extract(final, "id"))
            or self.interaction_id
            or coerce_string(self.request.interaction_id)
        )
        final_steps = sequence_or_empty(extract(final, "steps"))
        final_outputs = sequence_or_empty(extract(final, "outputs"))
        accumulated_steps = [step.as_step() for step in self.steps]
        return {
            "id": interaction_id,
            "status": coerce_string(extract(final, "status"))
            or self.status
            or "unknown",
            "previous_interaction_id": coerce_string(
                extract(final, "previous_interaction_id")
            )
            or self.previous_interaction_id,
            "model": coerce_string(extract(final, "model")) or self.model,
            "agent": coerce_string(extract(final, "agent")) or self.agent,
            "environment_id": environment_id(final) or self.environment_id,
            "usage": usage_from(final) or self.usage,
            "steps": final_steps or accumulated_steps,
            "outputs": final_outputs,
            "output_text": coerce_string(extract(final, "output_text"))
            or self._output_text(),
        }

    def stream_metadata(self) -> dict[str, Any]:
        return {
            "event_count": self.event_count,
            "counts_by_event_type": dict(self.counts_by_event_type),
            "last_event_id": self.last_event_id,
            "final_status": self.status,
            "reconstruction": _STREAM_RECONSTRUCTION_POLICY,
            "accumulated_step_count": len(self.steps),
            "text_truncated": self.text_truncated,
            "arguments_truncated": self.arguments_truncated,
        }

    def _record_interaction_event(self, event: Any) -> None:
        interaction = interaction_from_event(event)
        self.latest_interaction = interaction
        self._merge_interaction_metadata(
            interaction,
            interaction_id=coerce_string(extract(event, "interaction_id")),
            status=coerce_string(extract(event, "status")),
            usage=usage_from(event),
        )

    def _merge_interaction_metadata(
        self,
        interaction: Any,
        *,
        interaction_id: str | None = None,
        status: str | None = None,
        usage: dict[str, Any] | None = None,
    ) -> None:
        self.interaction_id = (
            interaction_id
            or coerce_string(extract(interaction, "id"))
            or self.interaction_id
        )
        self.previous_interaction_id = (
            coerce_string(extract(interaction, "previous_interaction_id"))
            or self.previous_interaction_id
        )
        self.status = (
            status or coerce_string(extract(interaction, "status")) or self.status
        )
        self.model = coerce_string(extract(interaction, "model")) or self.model
        self.agent = coerce_string(extract(interaction, "agent")) or self.agent
        self.environment_id = environment_id(interaction) or self.environment_id
        usage = usage or usage_from(interaction)
        if usage is not None:
            self.usage = usage

    def _record_step_delta(self, event: Any, *, kind: str) -> None:
        step = self._step_for_event(event)
        step.update_from_step(step_from_event(event))
        delta = delta_from_event(event)
        normalized_type = normalized_delta_type(event, delta)
        if normalized_type in TEXT_DELTA_TYPES:
            if kind == CONTENT_DELTA_EVENT_TYPE and step.type in (None, "text"):
                step.type = "output_text"
            text = delta_text(delta)
            if text is not None:
                accepted = step.append_text(
                    text,
                    _MAX_ACCUMULATED_STREAM_TEXT_CHARS - self.total_text_chars,
                )
                self.total_text_chars += accepted
                self.text_truncated = self.text_truncated or step.text_truncated
            if step.type is None:
                step.type = "message"
            return
        if normalized_type in ARGUMENT_DELTA_TYPES:
            arguments = delta_text(delta, include_arguments_delta=True)
            if arguments is not None:
                accepted = step.append_arguments(
                    arguments,
                    _MAX_ACCUMULATED_STREAM_ARGUMENT_CHARS - self.total_argument_chars,
                )
                self.total_argument_chars += accepted
                self.arguments_truncated = (
                    self.arguments_truncated or step.arguments_truncated
                )
            if step.type is None:
                step.type = "function_call"
            step.call_id = step.call_id or step.step_id
            return
        if normalized_type in THOUGHT_DELTA_TYPES:
            step.thought_chunk_count += 1
            return
        if normalized_type in MEDIA_DELTA_TYPES:
            step.media_delta_count += 1

    def _step_for_event(self, event: Any) -> _AccumulatedStep:
        step_value = step_from_event(event)
        index = step_index(event, step_value)
        step_id = coerce_string(extract(step_value, "id")) or coerce_string(
            extract(event, "step_id")
        )
        if index is not None and index in self.steps_by_index:
            step = self.steps_by_index[index]
            self._maybe_register_step_id(step, step_id)
            return step
        if step_id is not None and step_id in self.steps_by_id:
            step = self.steps_by_id[step_id]
            if index is not None:
                self.steps_by_index.setdefault(index, step)
            return step
        if len(self.steps) >= _MAX_ACCUMULATED_STREAM_STEPS:
            raise KitaruRuntimeError(
                "Gemini streamed interaction exceeded Kitaru's step accumulation "
                "limit. Kitaru will not store an incomplete streamed provider result."
            )
        step = _AccumulatedStep(
            index=index if index is not None else len(self.steps),
            step_id=step_id,
        )
        self.steps.append(step)
        if index is not None:
            self.steps_by_index[index] = step
        self._maybe_register_step_id(step, step_id)
        return step

    def _output_text(self) -> str | None:
        chunks: list[str] = []
        for step in self.steps:
            if step.text_chunks and is_safe_output_text_source(
                role=step.role,
                step_type=step_type_from_step({"type": step.type}),
            ):
                chunks.extend(step.text_chunks)
        text = "".join(chunks)
        return text or None

    def _maybe_register_step_id(
        self,
        step: _AccumulatedStep,
        step_id: str | None,
    ) -> None:
        if step_id is None:
            return
        step.step_id = step.step_id or step_id
        self.steps_by_id.setdefault(step_id, step)
