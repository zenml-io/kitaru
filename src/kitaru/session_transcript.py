#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Compact, deterministic transcripts of recorded sessions."""

import asyncio
import hashlib
import json
import uuid
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from kitaru.api_models.v1.session import SessionOrigin, SessionStatus
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeResponse,
    SessionWithNodesResponse,
)
from kitaru.base import FrozenModel
from kitaru.json_pointer import resolve_json_pointer
from kitaru.redaction import redact_data

DEFAULT_MAX_CHARS = 300
# Repeats shorter than this cost less to print again than to point back at.
_MIN_DEDUPLICATED_CHARS = 80

_REASONING_PART_TYPES = frozenset({"thinking", "reasoning", "redacted_thinking"})
_TOOL_CALL_PART_TYPES = frozenset(
    {"tool_call", "tool-call", "tool_use", "function_call"}
)


class TranscriptStep(FrozenModel):
    """One shown node of a session transcript."""

    index: int
    node_id: uuid.UUID
    node_type: NodeType
    name: str
    status: NodeStatus
    model: str | None = None
    duration_seconds: float | None = None
    system_prompt: str | None = None
    input: str | None = None
    output: str | None = None
    reasoning: str | None = None
    error: str | None = None


class SessionTranscript(FrozenModel):
    """Session header, shown steps, and whether any text was cut short."""

    session_id: uuid.UUID
    name: str | None
    status: SessionStatus
    origin: SessionOrigin
    imported_from: str | None
    framework: str | None
    started_at: datetime | None
    duration_seconds: float | None
    cost: Decimal | None
    llm_call_count: int
    tool_call_count: int
    node_count: int
    error: str | None
    system_prompt: str | None
    request: str | None
    final_output: str | None
    steps: list[TranscriptStep]
    truncated: bool


class SessionSource(Protocol):
    """Anything that fetches one session with every node."""

    async def get_with_nodes(self, session_id: uuid.UUID) -> SessionWithNodesResponse:
        """Get a session together with every one of its nodes."""
        ...


def _select(document: Any, pointer: str | None) -> Any | None:
    if pointer is None:
        return None
    found, value = resolve_json_pointer(document, pointer)
    return value if found else None


def _select_or_whole(document: Any, pointer: str | None) -> Any:
    selected = _select(document, pointer)
    return document if selected is None else selected


def _to_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    # Redact while keys still exist: a credential under `api_key` is invisible to
    # the string redactor once the mapping is flattened into JSON text.
    return json.dumps(
        redact_data(value), default=str, ensure_ascii=False, separators=(",", ":")
    )


def _get_duration(
    started_at: datetime | None, ended_at: datetime | None
) -> float | None:
    if started_at is None or ended_at is None:
        return None
    return round((ended_at - started_at).total_seconds(), 3)


def _summarize_model_output(outputs: Any) -> str | None:
    """Keep a model output's visible text and requested tool names, not reasoning."""
    texts: list[str] = []
    calls: list[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                walk(item)
            return
        if not isinstance(value, dict):
            return
        kind = value.get("type")
        if kind in _REASONING_PART_TYPES:
            return
        function = value.get("function")
        if kind in _TOOL_CALL_PART_TYPES or isinstance(function, dict):
            name = value.get("name") or value.get("tool_name")
            if name is None and isinstance(function, dict):
                name = function.get("name")
            if isinstance(name, str):
                calls.append(name)
                return
        text = value.get("text", value.get("content"))
        if isinstance(text, str) and (
            kind == "text" or value.get("role") == "assistant"
        ):
            texts.append(text)
        for item in value.values():
            walk(item)

    walk(outputs)
    if not texts and not calls:
        return None
    parts = [" ".join(texts)] if texts else []
    if calls:
        parts.append(f"calls {', '.join(calls)}")
    return " → ".join(parts)


class _Renderer:
    """Turn payloads into short text, remembering what it has already shown."""

    def __init__(self, max_chars: int | None, deduplicate: bool) -> None:
        self.max_chars = max_chars
        self.deduplicate = deduplicate
        self.truncated = False
        # Key on a digest so whole payloads are not kept alive just to spot repeats.
        self._seen: dict[bytes, str] = {}

    def show(self, value: Any, label: str) -> str | None:
        """Clip a value, or point back at where the same text first appeared."""
        if value is None:
            return None
        text = _to_text(value)
        if self.max_chars is None:
            return text
        normalized = " ".join(text.split())
        if not normalized:
            return None
        if self.deduplicate and len(normalized) >= _MIN_DEDUPLICATED_CHARS:
            digest = hashlib.blake2b(normalized.encode(), digest_size=16).digest()
            if (first := self._seen.get(digest)) is not None:
                return f"(same as {first})"
            self._seen[digest] = label
        if len(normalized) <= self.max_chars:
            return normalized
        self.truncated = True
        cut = len(normalized) - self.max_chars
        return f"{normalized[: self.max_chars]}… [+{cut} chars]"

    def omit(self, value: Any) -> str | None:
        """Name an unselected payload by size instead of printing it."""
        if value is None:
            return None
        text = _to_text(value)
        if self.max_chars is None:
            return text
        self.truncated = True
        return f"[raw payload, {len(text)} chars]"


def _is_shown(node: SessionNodeResponse) -> bool:
    return node.node_type is not NodeType.SPAN or node.status is NodeStatus.FAILED


def _build_step(
    renderer: _Renderer,
    index: int,
    node: SessionNodeResponse,
    *,
    system_prompt: str | None,
    include_reasoning: bool,
) -> TranscriptStep:
    label = f"[{index}]"
    is_model_call = node.node_type is NodeType.LLM_CALL
    if is_model_call and _select(node.inputs, node.input_text_selector) is None:
        shown_input = renderer.omit(node.inputs)
    else:
        shown_input = renderer.show(
            _select_or_whole(node.inputs, node.input_text_selector), label
        )
    output = _select(node.outputs, node.output_text_selector)
    if output is None and is_model_call:
        output = _summarize_model_output(node.outputs)
    shown_output = renderer.show(node.outputs if output is None else output, label)
    return TranscriptStep(
        index=index,
        node_id=node.id,
        node_type=node.node_type,
        name=node.tool_name or node.name,
        status=node.status,
        model=node.model or node.requested_model,
        duration_seconds=_get_duration(node.started_at, node.ended_at),
        system_prompt=system_prompt,
        input=shown_input,
        output=shown_output,
        reasoning=renderer.show(node.reasoning, label) if include_reasoning else None,
        error=node.error,
    )


def _find_request(session: SessionWithNodesResponse) -> Any:
    detail = session.session
    if (request := _select(detail.inputs, detail.input_text_selector)) is not None:
        return request
    # Imported sessions often wrap the whole conversation in their inputs, so the
    # first model call's selected input is the cleanest statement of the request.
    for node in session.nodes:
        request = _select(node.inputs, node.input_text_selector)
        if node.node_type is NodeType.LLM_CALL and request is not None:
            return request
    return detail.inputs


def build_transcript(
    session: SessionWithNodesResponse,
    *,
    max_chars: int | None = DEFAULT_MAX_CHARS,
    include_reasoning: bool = False,
    node_index: int | None = None,
) -> SessionTranscript:
    """Build a compact transcript of one session.

    Plain spans are hidden unless they failed, every other node becomes a step,
    and node errors are never shortened. Steps keep their node's position in the
    session, so `[3]` always names the fourth node.

    Args:
        session: Session with every node, ordered by position.
        max_chars: Longest text kept per field, or None to keep everything
            and skip deduplication.
        include_reasoning: Whether to show the visible reasoning of model calls.
        node_index: Show only the node at this position, plain span or not.

    Raises:
        IndexError: `node_index` is outside the session's nodes.

    Returns:
        Transcript whose `truncated` flag records whether any text was cut.
    """
    detail = session.session
    if node_index is not None and not 0 <= node_index < len(session.nodes):
        raise IndexError(
            f"Node {node_index} is outside this session's {len(session.nodes)} nodes."
        )
    prompts = [
        None
        if (prompt := _select(node.inputs, node.system_prompt_selector)) is None
        else _to_text(prompt)
        for node in session.nodes
    ]
    first_prompt = next((prompt for prompt in prompts if prompt is not None), None)
    whole = node_index is None
    renderer = _Renderer(max_chars, deduplicate=whole)
    # Render the header first so later repeats point back at it, not the reverse.
    system_prompt = renderer.show(first_prompt, "system prompt") if whole else None
    request = renderer.show(_find_request(session), "request") if whole else None
    steps: list[TranscriptStep] = []
    current_prompt = first_prompt
    for index, node in enumerate(session.nodes):
        changed_prompt = None
        if prompts[index] is not None and prompts[index] != current_prompt:
            changed_prompt = current_prompt = prompts[index]
        if (whole and not _is_shown(node)) or (not whole and index != node_index):
            continue
        steps.append(
            _build_step(
                renderer,
                index,
                node,
                system_prompt=renderer.show(changed_prompt, f"[{index}]"),
                include_reasoning=include_reasoning,
            )
        )
    final_output = _select_or_whole(detail.outputs, detail.output_text_selector)
    return SessionTranscript(
        session_id=detail.id,
        name=detail.name,
        status=detail.status,
        origin=detail.origin,
        imported_from=detail.imported_from,
        framework=detail.framework,
        started_at=detail.started_at,
        duration_seconds=_get_duration(detail.started_at, detail.ended_at),
        cost=detail.cost,
        llm_call_count=detail.llm_call_count,
        tool_call_count=detail.tool_call_count,
        node_count=len(session.nodes),
        error=detail.error,
        system_prompt=system_prompt,
        request=request,
        final_output=renderer.show(final_output, "final output") if whole else None,
        steps=steps,
        truncated=renderer.truncated,
    )


async def read_transcripts(
    source: SessionSource,
    session_ids: Sequence[uuid.UUID],
    *,
    max_chars: int | None = DEFAULT_MAX_CHARS,
    include_reasoning: bool = False,
    node_index: int | None = None,
    concurrency: int = 4,
) -> list[SessionTranscript]:
    """Fetch and transcribe sessions a few at a time, keeping the given order.

    Args:
        source: Fetches one session with every node.
        session_ids: Sessions to read.
        max_chars: Longest text kept per field, or None to keep everything.
        include_reasoning: Whether to show the visible reasoning of model calls.
        node_index: Show only the node at this position; needs one session.
        concurrency: Most sessions fetched at once.

    Raises:
        ValueError: `node_index` is set for several sessions or lies outside
            the session's nodes.

    Returns:
        One transcript per session, in `session_ids` order.
    """
    if node_index is not None and len(session_ids) != 1:
        raise ValueError("Showing one node needs exactly one session.")
    limit = asyncio.Semaphore(concurrency)

    async def read(session_id: uuid.UUID) -> SessionTranscript:
        async with limit:
            session = await source.get_with_nodes(session_id)
        # Build inside the task so each full session is freed once it is rendered.
        try:
            return build_transcript(
                session,
                max_chars=max_chars,
                include_reasoning=include_reasoning,
                node_index=node_index,
            )
        except IndexError as error:
            raise ValueError(str(error)) from None

    return list(await asyncio.gather(*(read(session_id) for session_id in session_ids)))


def _format_seconds(seconds: float | None) -> str | None:
    return None if seconds is None else f"{seconds:.1f}s"


def _join(parts: list[str | None]) -> str:
    return " · ".join(part for part in parts if part)


def _format_step(step: TranscriptStep) -> list[str]:
    is_tool = step.node_type is NodeType.TOOL_CALL
    lines = [
        _join(
            [
                f"[{step.index}] {step.node_type.value} {step.name}",
                step.model,
                step.status.value,
                _format_seconds(step.duration_seconds),
            ]
        )
    ]
    for label, value in (
        ("system prompt changed", step.system_prompt),
        ("args" if is_tool else "in", step.input),
        ("reasoning", step.reasoning),
        ("result" if is_tool else "out", step.output),
        ("error", step.error),
    ):
        if value is not None:
            lines.append(f"    {label}: {value}")
    return lines


def format_transcript(transcript: SessionTranscript) -> str:
    """Render a transcript as plain text, one node per line plus its fields.

    Args:
        transcript: Transcript to render.

    Returns:
        Text without a trailing newline.
    """
    imported = (
        f"imported from {transcript.imported_from}"
        if transcript.imported_from
        else transcript.origin.value
    )
    lines = [
        _join(
            [
                f"session {transcript.session_id}",
                transcript.name,
                transcript.status.value,
                imported,
                transcript.framework,
            ]
        ),
        _join(
            [
                transcript.started_at.isoformat() if transcript.started_at else None,
                _format_seconds(transcript.duration_seconds),
                None if transcript.cost is None else f"cost {transcript.cost}",
                f"{transcript.llm_call_count} LLM calls",
                f"{transcript.tool_call_count} tool calls",
                f"{transcript.node_count} nodes",
            ]
        ),
    ]
    for label, value in (
        ("error", transcript.error),
        ("system prompt", transcript.system_prompt),
        ("request", transcript.request),
    ):
        if value is not None:
            lines.append(f"{label}: {value}")
    for step in transcript.steps:
        lines.extend(_format_step(step))
    if transcript.final_output is not None:
        lines.append(f"final output: {transcript.final_output}")
    return "\n".join(lines)


def format_transcripts(transcripts: Sequence[SessionTranscript]) -> str:
    """Render several transcripts as plain text separated by blank lines.

    Args:
        transcripts: Transcripts to render.

    Returns:
        Text without a trailing newline.
    """
    return "\n\n".join(format_transcript(transcript) for transcript in transcripts)
