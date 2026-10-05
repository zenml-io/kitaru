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
"""Normalize ElevenLabs Agents conversations without inventing model requests."""

import json
import math
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import quote

from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus, TokenUsage
from kitaru.api_models.v1.session_node import NodeStatus, NodeType
from kitaru.task.importer import ImportedNode, ImportedSession, flatten_nodes


class InvalidImport(ValueError):
    """Raised when an upload or parser parameters cannot be interpreted."""


def _get_object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def _get_identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")
    return value


def _get_number(value: Any, name: str) -> float | None:
    if value is None:
        return None
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{name} must be finite and nonnegative")
    return float(value)


def _get_time(start: datetime | None, entry: dict[str, Any]) -> datetime | None:
    offset = _get_number(entry.get("time_in_call_secs"), "time_in_call_secs")
    return (
        start + timedelta(seconds=offset)
        if start is not None and offset is not None
        else None
    )


def _decode_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            pass
    return value


def _get_usage(entry: dict[str, Any]) -> dict[str, Any]:
    raw = entry.get("llm_usage")
    if raw is None:
        return {}
    models = _get_object(
        _get_object(raw, "llm_usage").get("model_usage", {}), "model_usage"
    )
    # Several model invocations cannot be represented as one served model.
    if len(models) != 1:
        return {}
    model, raw_usage = next(iter(models.items()))
    usage = _get_object(raw_usage, "model usage")
    tokens: dict[str, int] = {}
    input_counts: list[int] = []
    prices: list[Decimal] = []
    for category, data in usage.items():
        if category not in (
            "input",
            "input_cache_read",
            "input_cache_write",
            "output_total",
        ):
            continue
        data = _get_object(data, "usage category")
        count = data.get("tokens")
        if count is not None:
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise ValueError("token counts must be nonnegative integers")
            if category == "output_total":
                tokens["output_tokens"] = count
            else:
                input_counts.append(count)
                if category == "input_cache_read":
                    tokens["cached_input_tokens"] = count
        price = data.get("price")
        if price is not None:
            _get_number(price, "model usage price")
            prices.append(Decimal(str(price)))
    if input_counts:
        tokens["input_tokens"] = sum(input_counts)
    return {
        "model": _get_identifier(model, "model name"),
        "tokens": TokenUsage(**tokens) if tokens else None,
        "cost": sum(prices, Decimal(0)) if prices else None,
    }


def _get_tool_records(entry: dict[str, Any], key: str) -> list[dict[str, Any]]:
    records = entry.get(key)
    if records is None:
        return []
    if not isinstance(records, list):
        raise ValueError(f"{key} must be an array")
    return [_get_object(record, key) for record in records]


def _create_tool_node(
    conversation_id: str,
    request_id: str,
    call: tuple[int, dict[str, Any]] | None,
    result: tuple[int, dict[str, Any]] | None,
    transcript: list[dict[str, Any]],
    start: datetime | None,
) -> ImportedNode:
    source = call[1] if call is not None else result[1] if result is not None else {}
    name = _get_identifier(source.get("tool_name"), "tool_name")
    if call is not None and result is not None and result[1].get("tool_name") != name:
        raise ValueError("tool call and result names disagree")
    result_value = result[1].get("result_value") if result is not None else None
    outputs = _decode_json(result_value)
    is_error = result[1].get("is_error", False) if result is not None else False
    if not isinstance(is_error, bool):
        raise ValueError("is_error must be a boolean")
    started_at = _get_time(start, transcript[call[0]]) if call is not None else None
    ended_at = _get_time(start, transcript[result[0]]) if result is not None else None
    if started_at is not None and ended_at is not None and ended_at < started_at:
        raise ValueError("tool result precedes its call")
    error = None
    if is_error and result is not None:
        raw_error = result[1].get("raw_error_message")
        error = (
            raw_error
            if isinstance(raw_error, str) and raw_error
            else (
                result_value
                if isinstance(result_value, str)
                else json.dumps(result_value)
            )
        )
    return ImportedNode(
        external_id=f"{conversation_id}:tool:{request_id}",
        parent_external_id=f"{conversation_id}:transcript:{call[0]}"
        if call is not None
        else None,
        node_type=NodeType.TOOL_CALL,
        name=name,
        tool_name=name,
        status=NodeStatus.FAILED
        if is_error
        else NodeStatus.COMPLETED
        if result is not None
        else NodeStatus.IN_PROGRESS,
        error=error,
        started_at=started_at,
        ended_at=ended_at,
        inputs=_decode_json(call[1].get("params_as_json"))
        if call is not None
        else None,
        outputs=outputs,
        output_text_selector="" if isinstance(outputs, str) else None,
        attributes={},
        metadata={
            "elevenlabs": {
                "request_id": request_id,
                "tool_call": call[1] if call is not None else None,
                "tool_result": result[1] if result is not None else None,
                "call_transcript_index": call[0] if call is not None else None,
                "result_transcript_index": result[0] if result is not None else None,
                "call_missing": call is None,
                "result_missing": result is None,
            }
        },
    )


def _create_session(
    record: dict[str, Any], provenance: dict[str, Any]
) -> ImportedSession:
    conversation_id = _get_identifier(record.get("conversation_id"), "conversation_id")
    status = record.get("status")
    if status not in ("done", "failed"):
        raise ValueError("conversation must have terminal status done or failed")
    raw_transcript = record.get("transcript")
    if not isinstance(raw_transcript, list):
        raise ValueError("transcript must be an array of full conversation entries")
    transcript = [_get_object(entry, "transcript entry") for entry in raw_transcript]
    raw_metadata = record.get("metadata")
    provider_metadata = _get_object(
        raw_metadata if raw_metadata is not None else {}, "metadata"
    )
    start_secs = _get_number(
        provider_metadata.get("start_time_unix_secs"), "start_time_unix_secs"
    )
    duration = _get_number(
        provider_metadata.get("call_duration_secs"), "call_duration_secs"
    )
    start = datetime.fromtimestamp(start_secs, UTC) if start_secs is not None else None
    end = (
        start + timedelta(seconds=duration)
        if start is not None and duration is not None
        else None
    )
    calls: dict[str, tuple[int, dict[str, Any]]] = {}
    results: dict[str, tuple[int, dict[str, Any]]] = {}
    for index, entry in enumerate(transcript):
        for key, bucket in (("tool_calls", calls), ("tool_results", results)):
            for tool in _get_tool_records(entry, key):
                request_id = _get_identifier(tool.get("request_id"), "request_id")
                if request_id in bucket:
                    raise ValueError(f"duplicate request_id in {key}")
                bucket[request_id] = (index, tool)

    nodes: list[ImportedNode] = []
    user_messages: list[dict[str, str]] = []
    agent_messages: list[dict[str, str]] = []
    for index, entry in enumerate(transcript):
        role = entry.get("role")
        if role not in ("agent", "user"):
            raise ValueError("transcript role must be agent or user")
        message = entry.get("message")
        if message is not None and not isinstance(message, str):
            raise ValueError("transcript message must be a string or null")
        if "interrupted" in entry and not isinstance(entry["interrupted"], bool):
            raise ValueError("interrupted must be a boolean")
        text = {"message": message} if message is not None else None
        if text is not None:
            (user_messages if role == "user" else agent_messages).append(text)
        nodes.append(
            ImportedNode(
                external_id=f"{conversation_id}:transcript:{index}",
                node_type=NodeType.SPAN,
                name=f"{role} utterance" if message is not None else f"{role} event",
                status=NodeStatus.COMPLETED,
                started_at=_get_time(start, entry),
                inputs=text if role == "user" else None,
                outputs=text if role == "agent" else None,
                input_text_selector="/message"
                if role == "user" and text is not None
                else None,
                output_text_selector="/message"
                if role == "agent" and text is not None
                else None,
                attributes={"role": role},
                metadata={"elevenlabs": {**entry, "transcript_index": index}},
                **_get_usage(entry),
            )
        )
        for tool in _get_tool_records(entry, "tool_calls"):
            request_id = tool["request_id"]
            nodes.append(
                _create_tool_node(
                    conversation_id,
                    request_id,
                    calls[request_id],
                    results.get(request_id),
                    transcript,
                    start,
                )
            )
        for tool in _get_tool_records(entry, "tool_results"):
            request_id = tool["request_id"]
            if request_id not in calls:
                nodes.append(
                    _create_tool_node(
                        conversation_id,
                        request_id,
                        None,
                        results[request_id],
                        transcript,
                        start,
                    )
                )

    excluded_metadata = {
        "phone_call",
        "batch_call",
        "whatsapp",
        "sms",
        "initiator_id",
        "authorization_method",
    }
    metadata = {
        key: record[key]
        for key in (
            "agent_id",
            "agent_name",
            "branch_id",
            "version_id",
            "analysis",
            "visited_agents",
            "conversation_product",
            "environment",
            "tag_ids",
            "has_audio",
            "has_user_audio",
            "has_response_audio",
            "has_auxiliary_audio",
        )
        if key in record
    }
    metadata.update(
        {
            "conversation_id": conversation_id,
            "source_status": status,
            "provider_metadata": {
                key: value
                for key, value in provider_metadata.items()
                if key not in excluded_metadata
            },
            "provenance": {
                **provenance,
                "session_input": "first_user_message",
                "session_output": "last_agent_message",
            },
        }
    )
    if record.get("has_audio") is True:
        metadata["recording_url"] = (
            "https://api.elevenlabs.io/v1/convai/conversations/"
            f"{quote(conversation_id, safe='')}/audio"
        )
        metadata["recording_requires_authentication"] = True
    error = provider_metadata.get("error")
    session = ImportedSession(
        external_id=conversation_id,
        name=record.get("agent_name"),
        status=SessionStatus.FAILED if status == "failed" else SessionStatus.COMPLETED,
        error=error
        if isinstance(error, str)
        else json.dumps(error)
        if error is not None
        else None,
        started_at=start,
        ended_at=end,
        inputs=user_messages[0] if user_messages else None,
        outputs=agent_messages[-1] if agent_messages else None,
        input_text_selector="/message" if user_messages else None,
        output_text_selector="/message" if agent_messages else None,
        framework="elevenlabs",
        metadata={"elevenlabs": metadata},
        nodes=nodes,
    )
    flatten_nodes(session.nodes)
    session.model_dump_json()
    return session


def parse(
    payload: bytes, params: dict[str, Any]
) -> Iterator[ImportedSession | ImportFailure]:
    """Parse conversation details, batches, or transcription webhook exports.

    Args:
        payload: UTF-8 JSON with complete conversation details.
        params: Empty parameter object.

    Yields:
        One session per conversation, or a failure for an invalid record.

    Raises:
        InvalidImport: Parameters or the outer JSON document are invalid.
    """
    if not isinstance(params, dict) or params:
        raise InvalidImport("ElevenLabs parser accepts no parameters")
    try:
        document = json.loads(payload.decode("utf-8"))
    except (ValueError, RecursionError) as exc:
        raise InvalidImport("ElevenLabs upload must be valid UTF-8 JSON") from exc
    provenance: dict[str, Any] = {"format": "conversation"}
    if isinstance(document, dict) and "type" in document:
        if document["type"] != "post_call_transcription":
            raise InvalidImport(
                "Only post_call_transcription webhook exports are supported"
            )
        provenance = {
            "format": "post_call_transcription",
            "event_timestamp": document.get("event_timestamp"),
        }
        records = [document.get("data")]
    elif isinstance(document, dict) and "conversations" in document:
        records = document["conversations"]
        provenance = {"format": "conversations"}
    elif isinstance(document, list):
        records = document
        provenance = {"format": "array"}
    elif isinstance(document, dict):
        records = [document]
    else:
        raise InvalidImport("ElevenLabs upload must contain conversation objects")
    if not isinstance(records, list):
        raise InvalidImport("conversations must be an array")
    grouped: dict[str, list[Any]] = {}
    for value in records:
        identifier = value.get("conversation_id") if isinstance(value, dict) else None
        if isinstance(identifier, str):
            grouped.setdefault(identifier, []).append(value)
    seen: set[str] = set()
    for index, value in enumerate(records, start=1):
        external_id = value.get("conversation_id") if isinstance(value, dict) else None
        try:
            record = _get_object(value, "conversation")
            identifier = _get_identifier(external_id, "conversation_id")
            if identifier in seen:
                continue
            seen.add(identifier)
            if any(other != record for other in grouped[identifier]):
                raise ValueError("conflicting duplicate conversation_id")
            session = _create_session(record, provenance)
            yield session
        except (ValueError, OverflowError, RecursionError) as exc:
            yield ImportFailure(
                line=index,
                external_id=external_id if isinstance(external_id, str) else None,
                error=f"Invalid ElevenLabs conversation: {exc}",
            )


class ElevenLabsImporter:
    """Parse exported conversations and fetch them from ElevenLabs."""

    def parse(
        self, payload: bytes, params: dict[str, Any]
    ) -> Iterator[ImportedSession | ImportFailure]:
        """Parse complete conversation exports."""
        return parse(payload, params)

    async def fetch(self, query: dict[str, Any]) -> AsyncIterator[bytes]:
        """Fetch complete conversation details matching a validated query."""
        from kitaru_elevenlabs_importer.api import fetch

        async for payload in fetch(query):
            yield payload


importer = ElevenLabsImporter()
