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
"""Shared Logfire SDK and Query API fakes for the Logfire adapter."""

import asyncio
import json
import re
import secrets
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

import kitaru_logfire_importer.adapter as adapter_module
import kitaru_logfire_importer.api as api_module

RowsBuilder = Callable[[str], list[dict[str, Any]]]

PROJECT_ID = "project-1"
READ_TOKEN = "test-read-token"

_BASE_URL = "https://logfire-api.test"
_POLL_COLUMNS = ("span_id", "parent_span_id", "end_timestamp")
_POLL_SQL_PATTERN = re.compile(
    r"SELECT span_id, parent_span_id, end_timestamp FROM records"
    r" WHERE trace_id = '(?P<trace_id>[0-9a-f]{32})'"
)
_FETCH_SQL_PATTERN = re.compile(
    r"SELECT \* FROM records WHERE trace_id = '(?P<trace_id>[0-9a-f]{32})'"
)
_LIST_SQL_PATTERN = re.compile(
    r"SELECT DISTINCT trace_id, start_timestamp FROM records "
    r"WHERE parent_span_id IS NULL "
    r"AND start_timestamp >= '(?P<since>[^']+)' "
    r"AND start_timestamp <= '(?P<until>[^']+)' "
    r"ORDER BY start_timestamp"
)


def build_row(
    span_id: str,
    trace_id: str,
    *,
    parent_span_id: str | None = None,
    span_name: str | None = None,
    start_timestamp: str = "2026-07-24T10:00:00Z",
    end_timestamp: str | None = "2026-07-24T10:01:00Z",
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one Logfire records row."""
    return {
        "project_id": PROJECT_ID,
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "span_name": span_name or span_id,
        "message": span_name or span_id,
        "kind": "span",
        "level": 9,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "service_name": "support-agent",
        "deployment_environment": "test",
        "otel_scope_name": "pydantic-ai",
        "attributes": {
            "gen_ai.conversation.id": "conversation-1",
            **(attributes or {}),
        },
    }


def build_complete_rows(trace_id: str) -> list[dict[str, Any]]:
    """Build finished rows with a root span and a nested LLM span."""
    return [
        build_row("root", trace_id, span_name="kitaru-run"),
        build_row(
            "llm",
            trace_id,
            parent_span_id="root",
            span_name="llm-call",
            start_timestamp="2026-07-24T10:00:01Z",
            attributes={
                "gen_ai.operation.name": "chat",
                "gen_ai.request.model": "gpt-5-nano",
            },
        ),
    ]


def build_conversation_rows(
    trace_id: str,
    conversation_id: str,
    *,
    start_timestamp: str = "2026-07-24T10:00:00Z",
) -> list[dict[str, Any]]:
    """Build finished rows carrying an explicit conversation id."""
    return [
        build_row(
            "root",
            trace_id,
            span_name="kitaru-run",
            start_timestamp=start_timestamp,
            attributes={"gen_ai.conversation.id": conversation_id},
        ),
        build_row(
            "llm",
            trace_id,
            parent_span_id="root",
            span_name="llm-call",
            start_timestamp=start_timestamp,
            attributes={
                "gen_ai.conversation.id": conversation_id,
                "gen_ai.operation.name": "chat",
                "gen_ai.request.model": "gpt-5-nano",
            },
        ),
    ]


def build_list_row(trace_id: str, start_timestamp: str) -> dict[str, Any]:
    """Build one trace-listing result row."""
    return {"trace_id": trace_id, "start_timestamp": start_timestamp}


def ndjson(rows: list[dict[str, Any]]) -> bytes:
    """Encode records rows as a Query API NDJSON stream."""
    messages = (
        {"type": "schema", "schema": {"fields": []}},
        {"type": "data", "rows": rows},
        {"type": "end", "row_count": len(rows)},
    )
    return b"\n".join(json.dumps(message).encode() for message in messages)


class _FakeQueryClient:
    """Query API client fake routing polls to the scripted rows."""

    def __init__(self, fake: "FakeLogfire", read_token: str) -> None:
        assert read_token == READ_TOKEN
        self._fake = fake

    async def __aenter__(self) -> "_FakeQueryClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def query_json_rows(
        self, sql: str, *, min_timestamp: datetime
    ) -> dict[str, Any]:
        match = _POLL_SQL_PATTERN.fullmatch(sql)
        assert match is not None, f"unexpected poll query: {sql}"
        self._fake.requested.append(match["trace_id"])
        self._fake.poll_min_timestamps.append(min_timestamp)
        self._fake.events.append("poll")
        assert self._fake.poll_builders, "unexpected records poll"
        rows = self._fake.poll_builders.pop(0)(match["trace_id"])
        return {
            "columns": [],
            "rows": [
                {column: row.get(column) for column in _POLL_COLUMNS} for row in rows
            ],
        }


class _FakeResponse:
    """Query API response fake."""

    def __init__(self, content: bytes = b"", error: Exception | None = None) -> None:
        self.content = content
        self._error = error

    def raise_for_status(self) -> None:
        if self._error is not None:
            raise self._error


class _FakeAsyncClient:
    """HTTP client fake routing NDJSON posts to the scripted rows."""

    def __init__(self, fake: "FakeLogfire", base_url: str) -> None:
        assert base_url == _BASE_URL
        self._fake = fake

    async def __aenter__(self) -> "_FakeAsyncClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def post(
        self, url: str, *, headers: dict[str, str], json: dict[str, Any]
    ) -> _FakeResponse:
        assert url == "/v2/query"
        assert headers == {
            "accept": "application/x-ndjson",
            "authorization": f"Bearer {READ_TOKEN}",
        }
        fetch_match = _FETCH_SQL_PATTERN.fullmatch(json["sql"])
        if fetch_match is not None:
            trace_id = fetch_match["trace_id"]
            self._fake.in_flight += 1
            self._fake.peak_in_flight = max(
                self._fake.peak_in_flight, self._fake.in_flight
            )
            try:
                if self._fake.fetch_delays:
                    await asyncio.sleep(self._fake.fetch_delays.pop(0))
                if self._fake.raise_once is not None:
                    error = self._fake.raise_once
                    self._fake.raise_once = None
                    return _FakeResponse(error=error)
                self._fake.requested.append(trace_id)
                self._fake.fetch_min_timestamps.append(json["min_timestamp"])
                self._fake.events.append("fetch")
                assert self._fake.fetch_builders, "unexpected records fetch"
                return _FakeResponse(ndjson(self._fake.fetch_builders.pop(0)(trace_id)))
            finally:
                self._fake.in_flight -= 1

        list_match = _LIST_SQL_PATTERN.fullmatch(json["sql"])
        assert list_match is not None, f"unexpected query: {json['sql']}"
        self._fake.list_min_timestamps.append(json["min_timestamp"])
        self._fake.list_max_timestamps.append(json["max_timestamp"])
        self._fake.events.append("list")
        assert self._fake.list_builders, "unexpected trace listing"
        return _FakeResponse(ndjson(self._fake.list_builders.pop(0)()))


class FakeLogfire:
    """Logfire SDK and Query API fake with scripted records rows."""

    def __init__(self) -> None:
        self.poll_builders: list[RowsBuilder] = []
        self.fetch_builders: list[RowsBuilder] = []
        self.list_builders: list[Callable[[], list[dict[str, Any]]]] = []
        self.requested: list[str] = []
        self.poll_min_timestamps: list[datetime] = []
        self.fetch_min_timestamps: list[str] = []
        self.list_min_timestamps: list[str] = []
        self.list_max_timestamps: list[str] = []
        self.events: list[str] = []
        self.trace_ids: list[int] = []
        self.fetch_delays: list[float] = []
        self.in_flight = 0
        self.peak_in_flight = 0
        self.raise_once: Exception | None = None

    @contextmanager
    def span(self, name: str) -> Iterator[SimpleNamespace]:
        assert name == "kitaru-run"
        self.events.append("span-enter")
        trace_id = secrets.randbits(128)
        self.trace_ids.append(trace_id)
        yield SimpleNamespace(context=SimpleNamespace(trace_id=trace_id))
        self.events.append("span-exit")

    def force_flush(self) -> None:
        self.events.append("flush")

    def get_base_url_from_token(self, token: str) -> str:
        assert token == READ_TOKEN
        return _BASE_URL


@pytest.fixture(autouse=True)
def _fast_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_module, "_POLL_INTERVAL", 0.0)


@pytest.fixture
def fake_logfire(monkeypatch: pytest.MonkeyPatch) -> FakeLogfire:
    """Create a fake Logfire SDK and route the adapter to it."""
    fake = FakeLogfire()
    monkeypatch.setenv("LOGFIRE_READ_TOKEN", READ_TOKEN)
    monkeypatch.setattr(adapter_module, "span", fake.span)
    monkeypatch.setattr(adapter_module, "force_flush", fake.force_flush)
    monkeypatch.setattr(
        api_module,
        "AsyncLogfireQueryClient",
        lambda read_token: _FakeQueryClient(fake, read_token),
    )
    monkeypatch.setattr(
        api_module, "get_base_url_from_token", fake.get_base_url_from_token
    )
    monkeypatch.setattr(
        api_module,
        "httpx",
        SimpleNamespace(
            AsyncClient=lambda *, base_url: _FakeAsyncClient(fake, base_url),
            HTTPStatusError=httpx.HTTPStatusError,
        ),
    )
    return fake
