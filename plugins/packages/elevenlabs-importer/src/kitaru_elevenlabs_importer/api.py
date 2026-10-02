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
"""Read ElevenLabs conversation details with worker-supplied credentials."""

import json
import math
import os
from collections.abc import AsyncIterator
from contextlib import aclosing
from datetime import UTC, datetime
from functools import partial
from typing import Any, Self

import httpx
from pydantic import ConfigDict, Field, field_validator, model_validator

from kitaru.api_models.v1.imports import ImportQuery
from kitaru.task.importer import retry_rate_limited, stream_bounded

_BASE_URL = "https://api.elevenlabs.io/v1/convai/conversations"


def _validate_identifier(value: str) -> str:
    if (
        not value
        or len(value) > 256
        or not all(
            character.isascii() and (character.isalnum() or character in "_-")
            for character in value
        )
    ):
        raise ValueError(
            "ElevenLabs IDs must contain only ASCII letters, digits, "
            "underscores or hyphens"
        )
    return value


class ElevenLabsImportQuery(ImportQuery):
    """Bounded conversation selection using standard Kitaru import queries."""

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    trace_ids: list[str] | None = Field(default=None, min_length=1, max_length=10000)
    concurrency: int = Field(default=4, ge=1, le=16, strict=True)
    agent_id: str | None = None
    page_size: int = Field(default=100, ge=1, le=100, strict=True)
    limit: int = Field(default=1000, ge=1, le=10000, strict=True)

    @field_validator("trace_ids")
    @classmethod
    def _validate_ids(cls, value: list[str] | None) -> list[str] | None:
        if value is not None:
            for identifier in value:
                _validate_identifier(identifier)
            if len(value) != len(set(value)):
                raise ValueError("trace_ids must not contain duplicates")
        return value

    @field_validator("agent_id")
    @classmethod
    def _validate_agent(cls, value: str | None) -> str | None:
        return _validate_identifier(value) if value is not None else None

    @model_validator(mode="after")
    def _validate_selection(self) -> Self:
        if self.trace_ids is not None:
            if (
                self.since is not None
                or self.until is not None
                or self.agent_id is not None
            ):
                raise ValueError(
                    "trace_ids cannot be combined with time or agent filters"
                )
            if len(self.trace_ids) > self.limit:
                raise ValueError("trace_ids count exceeds limit")
        return self


class _RateLimited(ValueError):
    def __init__(self, delay: float) -> None:
        super().__init__("ElevenLabs rate limit exceeded")
        self.delay = delay


def _get_retry_after(exc: Exception) -> float | None:
    return exc.delay if isinstance(exc, _RateLimited) else None


async def _get_response(
    client: httpx.AsyncClient, url: str, params: Any = None
) -> httpx.Response:
    try:
        response = await client.get(url, params=params)
    except httpx.HTTPError:
        raise ValueError("ElevenLabs request failed") from None
    if response.status_code == 429:
        try:
            delay = float(response.headers.get("retry-after", "1"))
        except ValueError:
            delay = 1
        if not math.isfinite(delay):
            delay = 1
        raise _RateLimited(min(max(delay, 0), 60))
    if not response.is_success:
        raise ValueError(f"ElevenLabs request failed (HTTP {response.status_code})")
    return response


async def _get_json(
    client: httpx.AsyncClient, url: str, params: Any = None
) -> dict[str, Any]:
    response = await retry_rate_limited(
        partial(_get_response, client, url, params), _get_retry_after
    )
    try:
        document = response.json()
    except ValueError:
        raise ValueError("ElevenLabs returned invalid JSON") from None
    if not isinstance(document, dict):
        raise ValueError("ElevenLabs returned a non-object response")
    return document


async def _fetch_conversation(client: httpx.AsyncClient, conversation_id: str) -> bytes:
    document = await _get_json(client, f"{_BASE_URL}/{conversation_id}")
    if document.get("conversation_id") != conversation_id:
        raise ValueError("ElevenLabs returned a different conversation_id")
    return json.dumps(document).encode("utf-8")


async def fetch(query: dict[str, Any]) -> AsyncIterator[bytes]:
    """Fetch details by conversation IDs or a paginated start-time window.

    Args:
        query: Standard trace_ids or since/until selection with optional agent_id,
            page_size, limit, and concurrency bounds.

    Yields:
        JSON details for each selected conversation.

    Raises:
        ValueError: The query, credentials, or provider response is invalid.
    """
    parsed = ElevenLabsImportQuery.model_validate(query)
    api_key = os.environ.get("ELEVENLABS_API_KEY")
    if not api_key or not api_key.strip():
        raise ValueError("ELEVENLABS_API_KEY must be set in the worker environment")
    async with httpx.AsyncClient(
        headers={"xi-api-key": api_key}, timeout=60, follow_redirects=False
    ) as client:
        if parsed.trace_ids is not None:
            async with aclosing(
                stream_bounded(
                    (
                        _fetch_conversation(client, identifier)
                        for identifier in parsed.trace_ids
                    ),
                    parsed.concurrency,
                )
            ) as payloads:
                async for payload in payloads:
                    yield payload
            return
        since, until = parsed.get_window()
        params: dict[str, Any] = {
            "call_start_after_unix": math.floor(since.timestamp()),
            "call_start_before_unix": math.ceil(until.timestamp()),
            "page_size": parsed.page_size,
            "exclude_statuses": ["initiated", "in-progress", "processing"],
        }
        if parsed.agent_id is not None:
            params["agent_id"] = parsed.agent_id
        cursors: set[str] = set()
        seen_ids: set[str] = set()
        scanned = 0
        pages = 0
        while scanned < parsed.limit:
            pages += 1
            params["page_size"] = min(parsed.page_size, parsed.limit - scanned)
            page = await _get_json(client, _BASE_URL, params)
            rows = page.get("conversations")
            if not isinstance(rows, list):
                raise ValueError("ElevenLabs listing lacks conversations array")
            identifiers: list[str] = []
            for row in rows[: parsed.limit - scanned]:
                scanned += 1
                if not isinstance(row, dict):
                    raise ValueError(
                        "ElevenLabs listing contains a non-object conversation"
                    )
                identifier = row.get("conversation_id")
                if not isinstance(identifier, str):
                    raise ValueError("ElevenLabs listing lacks conversation_id")
                _validate_identifier(identifier)
                if identifier in seen_ids:
                    continue
                seen_ids.add(identifier)
                if row.get("status") in ("done", "failed"):
                    identifiers.append(identifier)
            async with aclosing(
                stream_bounded(
                    (
                        _fetch_conversation(client, identifier)
                        for identifier in identifiers
                    ),
                    parsed.concurrency,
                )
            ) as payloads:
                async for payload in payloads:
                    # Retention or processing may change between listing and detail.
                    detail = json.loads(payload)
                    metadata = detail.get("metadata")
                    start = (
                        metadata.get("start_time_unix_secs")
                        if isinstance(metadata, dict)
                        else None
                    )
                    if (
                        isinstance(start, bool)
                        or not isinstance(start, int | float)
                        or not math.isfinite(start)
                    ):
                        raise ValueError("ElevenLabs detail lacks a valid start time")
                    started_at = datetime.fromtimestamp(start, UTC)
                    if since <= started_at < until and detail.get("status") in (
                        "done",
                        "failed",
                    ):
                        yield payload
            if scanned >= parsed.limit or page.get("has_more") is not True:
                return
            if pages >= parsed.limit:
                raise ValueError("ElevenLabs pagination exceeded page-request limit")
            cursor = page.get("next_cursor")
            if not isinstance(cursor, str) or not cursor or cursor in cursors:
                raise ValueError(
                    "ElevenLabs listing has a missing or repeated pagination cursor"
                )
            cursors.add(cursor)
            params["cursor"] = cursor
