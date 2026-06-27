"""Output extraction and preview helpers for Google ADK runner results."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ._serialization import safe_repr, to_json_safe

FINAL_OUTPUT_KEYS = ("final_output", "final_response", "output", "content", "text")


def extract_final_output(
    raw_events: list[Any],
    *,
    include_raw: bool = False,
) -> Any | None:
    """Return the last ADK event's final output payload, if one is identifiable."""
    if not raw_events:
        return None
    last = raw_events[-1]
    if isinstance(last, Mapping):
        for key in FINAL_OUTPUT_KEYS:
            if key in last and last[key] is not None:
                return last[key]
        return to_json_safe(last, include_raw=include_raw)
    for attr in FINAL_OUTPUT_KEYS:
        if hasattr(last, attr):
            value = getattr(last, attr)
            if value is not None:
                return value
    return to_json_safe(last, include_raw=include_raw)


def final_output_preview(final_output: Any, *, max_chars: int = 500) -> str:
    """Return bounded display text from a Google ADK final output value.

    ADK runner events usually expose final model text as a Google GenAI
    ``Content`` object whose ``parts`` contain text. Tests, docs, examples, and
    smoke output should use this helper instead of assuming the raw
    ``ADKRunResult.final_output`` is already a string.
    """
    if max_chars < 1:
        raise ValueError("max_chars must be at least 1")
    state = _PreviewState(max_chars=max_chars)
    _append_preview(final_output, state=state, seen=set())
    text = state.render().strip()
    if state.truncated or len(text) > max_chars:
        return f"{text[: max_chars - 1]}…"
    return text


@dataclass
class _PreviewState:
    max_chars: int
    chunks: list[str] = field(default_factory=list)
    char_count: int = 0
    truncated: bool = False

    @property
    def remaining(self) -> int:
        return self.max_chars - self.char_count

    def append(self, text: str) -> None:
        if not text or self.truncated:
            return
        remaining = self.remaining
        if remaining <= 0:
            self.truncated = True
            return
        if len(text) > remaining:
            self.chunks.append(text[:remaining])
            self.char_count += remaining
            self.truncated = True
            return
        self.chunks.append(text)
        self.char_count += len(text)

    def render(self) -> str:
        return "".join(self.chunks)


def _append_preview(value: Any, *, state: _PreviewState, seen: set[int]) -> None:
    if state.truncated or value is None:
        return
    if isinstance(value, str):
        state.append(value)
        return
    if isinstance(value, bytes | bytearray):
        chunk = value[: state.remaining] if state.remaining > 0 else b""
        state.append(chunk.decode(errors="replace"))
        if len(value) > len(chunk):
            state.truncated = True
        return

    value_id = id(value)
    if value_id in seen:
        return
    seen.add(value_id)

    text_attr = getattr(value, "text", None)
    if isinstance(text_attr, str) and text_attr:
        state.append(text_attr)
        return

    parts = getattr(value, "parts", None)
    if parts is not None:
        _append_preview(parts, state=state, seen=seen)
        if state.char_count or state.truncated:
            return

    if isinstance(value, Mapping):
        for key in FINAL_OUTPUT_KEYS:
            if key in value:
                before = state.char_count
                _append_preview(value[key], state=state, seen=seen)
                if state.truncated or state.char_count > before:
                    return
        if "parts" in value:
            _append_preview(value["parts"], state=state, seen=seen)
            if state.char_count or state.truncated:
                return
        _append_repr(value, state=state)
        return

    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        wrote_item = False
        for item in value:
            if state.truncated:
                return
            item_budget = state.remaining - (1 if wrote_item else 0)
            if item_budget < 1:
                state.truncated = True
                return
            item_state = _PreviewState(max_chars=item_budget)
            _append_preview(item, state=item_state, seen=seen)
            item_text = item_state.render().strip()
            if not item_text:
                state.truncated = state.truncated or item_state.truncated
                continue
            if wrote_item:
                state.append(" ")
            state.append(item_text)
            wrote_item = True
            state.truncated = state.truncated or item_state.truncated
        return

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            dumped = model_dump(mode="python")
        except Exception:
            pass
        else:
            _append_preview(dumped, state=state, seen=seen)
            if state.char_count or state.truncated:
                return

    _append_repr(value, state=state)


def _append_repr(value: Any, *, state: _PreviewState) -> None:
    state.append(safe_repr(value))
