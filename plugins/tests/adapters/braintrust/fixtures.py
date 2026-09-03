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
"""Shared Braintrust SDK and BTQL fakes for the Braintrust adapter."""

import re
import secrets
from collections.abc import Callable
from types import SimpleNamespace, TracebackType
from typing import Any

import pytest
from braintrust import NOOP_SPAN, SpanImpl

import kitaru_braintrust_importer.adapter as adapter_module
import kitaru_braintrust_importer.api as api_module

RowsBuilder = Callable[[str], list[dict[str, Any]]]

_QUERY_PATTERN = re.compile(
    r"select: \* \| from: project_logs\('(?P<project_id>[^']+)'\) spans"
    r" \| filter: root_span_id = '(?P<root_span_id>[^']+)'"
)

_LIST_QUERY_PATTERN = re.compile(
    r"select: root_span_id \| from: project_logs\('(?P<project_id>[^']+)'\) spans"
    r" \| filter: NOT EXISTS\(span_parents\) AND"
    r" \(\(created >= '(?P<since>[^']+)' AND created <= '(?P<until>[^']+)'\)"
    r" OR \(metrics\.start >= (?P<since_ts>[-0-9.]+)"
    r" AND metrics\.start <= (?P<until_ts>[-0-9.]+)\)\)"
    r" \| sort: created asc"
    r" \| limit: (?P<limit>\d+)"
)


def build_row(
    span_id: str,
    root_span_id: str,
    *,
    parents: list[str] | None = None,
    span_type: str = "task",
    end: float | None = 1_785_000_000.5,
    **extra: Any,
) -> dict[str, Any]:
    """Build one Braintrust BTQL span row."""
    metrics: dict[str, Any] = {"start": 1_785_000_000.0}
    if end is not None:
        metrics["end"] = end
    return {
        "id": f"event-{span_id}",
        "project_id": "project-1",
        "span_id": span_id,
        "root_span_id": root_span_id,
        "span_parents": parents or [],
        "span_attributes": {"name": span_id, "type": span_type},
        "input": {"messages": [{"role": "user", "content": "Weather?"}]},
        "output": {"role": "assistant", "content": "Sunny."},
        "metrics": metrics,
        "created": "2026-07-24T10:00:00Z",
        **extra,
    }


def build_complete_rows(root_span_id: str) -> list[dict[str, Any]]:
    """Build finished rows with a root span and a nested LLM span."""
    return [
        build_row(root_span_id, root_span_id),
        build_row(
            "span-llm",
            root_span_id,
            parents=[root_span_id],
            span_type="llm",
            metadata={"model": "gpt-5-nano"},
        ),
    ]


def build_session_rows(session_id: str) -> RowsBuilder:
    """Build a rows builder whose root span joins the given session id."""

    def _builder(root_span_id: str) -> list[dict[str, Any]]:
        rows = build_complete_rows(root_span_id)
        rows[0] = {**rows[0], "metadata": {"session_id": session_id}}
        return rows

    return _builder


class FakeSpan(SpanImpl):
    """Braintrust span fake carrying only the ids the adapter reads."""

    def __init__(self, fake: "FakeBraintrust") -> None:
        self._fake = fake
        self.span_id = secrets.token_hex(8)
        self.root_span_id = secrets.token_hex(16)

    def __enter__(self) -> "FakeSpan":
        self._fake.events.append("span-enter")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._fake.events.append("span-exit")


class _FakeResponse:
    """BTQL response fake."""

    def __init__(self, rows: list[dict[str, Any]], cursor: str | None = None) -> None:
        self._rows = rows
        self._cursor = cursor

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"data": self._rows}
        if self._cursor is not None:
            payload["cursor"] = self._cursor
        return payload


class _FakeAsyncClient:
    """HTTP client fake routing BTQL posts to the scripted rows."""

    def __init__(self, fake: "FakeBraintrust") -> None:
        self._fake = fake

    async def __aenter__(self) -> "_FakeAsyncClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def post(
        self, url: str, *, headers: dict[str, str], json: dict[str, Any]
    ) -> _FakeResponse:
        return self._fake.post(url, headers, json)


class FakeBraintrust:
    """Braintrust SDK and BTQL API fake with scripted span rows."""

    def __init__(self) -> None:
        self.rows_builders: list[RowsBuilder] = []
        self.requested: list[str] = []
        self.events: list[str] = []
        self.spans: list[FakeSpan] = []
        self.no_active_logger = False
        self.project_id = "project-1"
        # Each list page is (root_span_ids, cursor_for_next_page).
        self.list_pages: list[tuple[list[str], str | None]] = []
        self.list_queries: list[dict[str, str]] = []
        self.list_cursors_received: list[str | None] = []

    def start_span(self, *, name: str) -> Any:
        if self.no_active_logger:
            return NOOP_SPAN
        span = FakeSpan(self)
        self.spans.append(span)
        return span

    def flush(self) -> None:
        self.events.append("flush")

    def current_logger(self) -> Any:
        if self.no_active_logger:
            return None
        return SimpleNamespace(id=self.project_id)

    def post(
        self, url: str, headers: dict[str, str], json: dict[str, Any]
    ) -> _FakeResponse:
        assert url == "https://api.braintrust.dev/btql"
        assert headers == {"Authorization": "Bearer test-key"}
        list_match = _LIST_QUERY_PATTERN.fullmatch(json["query"])
        if list_match is not None:
            assert list_match["project_id"] == self.project_id
            self.list_queries.append(list_match.groupdict())
            self.list_cursors_received.append(json.get("cursor"))
            self.events.append("list")
            assert self.list_pages, "unexpected BTQL list query"
            root_span_ids, cursor = self.list_pages.pop(0)
            return _FakeResponse(
                [{"root_span_id": root_span_id} for root_span_id in root_span_ids],
                cursor=cursor,
            )
        match = _QUERY_PATTERN.fullmatch(json["query"])
        assert match is not None, f"unexpected BTQL query: {json['query']}"
        assert match["project_id"] == self.project_id
        self.requested.append(match["root_span_id"])
        self.events.append("poll")
        assert self.rows_builders, "unexpected BTQL poll"
        return _FakeResponse(self.rows_builders.pop(0)(match["root_span_id"]))


@pytest.fixture(autouse=True)
def _fast_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api_module, "_POLL_INTERVAL", 0.0)


@pytest.fixture
def fake_braintrust(monkeypatch: pytest.MonkeyPatch) -> FakeBraintrust:
    """Create a fake Braintrust SDK and route the adapter to it."""
    fake = FakeBraintrust()
    monkeypatch.setenv("BRAINTRUST_API_KEY", "test-key")
    monkeypatch.delenv("BRAINTRUST_API_URL", raising=False)
    monkeypatch.setattr(adapter_module, "start_span", fake.start_span)
    monkeypatch.setattr(adapter_module, "flush", fake.flush)
    monkeypatch.setattr(adapter_module, "current_logger", fake.current_logger)
    monkeypatch.setattr(
        api_module,
        "httpx",
        SimpleNamespace(AsyncClient=lambda: _FakeAsyncClient(fake)),
    )
    return fake
