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
"""Contract tests for the MLflow API fetch entrypoint."""

import copy
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import mlflow
import pytest
from mlflow.entities import Trace, TraceData

from kitaru.task.importer import ImportedSession
from kitaru_mlflow_importer.api import fetch, serialize_traces
from kitaru_mlflow_importer.importer import importer, parse

from ..fetch_helpers import collect_payloads

FIXTURE = (
    Path(__file__).parents[2]
    / "importers"
    / "fixtures"
    / "mlflow"
    / "3.16.1"
    / "traces.json"
)
SINCE = datetime(2026, 9, 1, tzinfo=UTC)


@dataclass
class FakeMlflow:
    """In-memory stand-in for the MLflow tracing SDK search and get calls."""

    traces: dict[str, Trace]
    searches: list[dict[str, Any]] = field(default_factory=list)
    gets: list[str] = field(default_factory=list)

    def search_traces(self, **kwargs: Any) -> list[Trace]:
        """Return every stored trace oldest first, without spans when asked."""
        self.searches.append(kwargs)
        ordered = sorted(self.traces.values(), key=lambda t: t.info.request_time)
        if kwargs["include_spans"]:
            return ordered
        return [Trace(info=t.info, data=TraceData(spans=[])) for t in ordered]

    def get_trace(self, trace_id: str, silent: bool = False) -> Trace | None:
        """Return one stored trace, None when absent."""
        self.gets.append(trace_id)
        return self.traces.get(trace_id)


def _load_traces() -> list[dict[str, Any]]:
    return json.loads(FIXTURE.read_text())["traces"]


def _standalone(document: dict[str, Any], index: int) -> dict[str, Any]:
    """Copy a recorded trace under a new id, with or without its session."""
    clone = copy.deepcopy(document)
    clone["info"]["trace_id"] = f"tr-{index:032x}"
    clone["info"]["request_time"] = f"2026-09-29T12:{index // 60:02}:{index % 60:02}Z"
    clone["info"]["trace_metadata"].pop("mlflow.trace.session", None)
    return clone


def _install(
    monkeypatch: pytest.MonkeyPatch, documents: list[dict[str, Any]]
) -> FakeMlflow:
    fake = FakeMlflow(
        {doc["info"]["trace_id"]: Trace.from_dict(doc) for doc in documents}
    )
    monkeypatch.setattr(mlflow, "search_traces", fake.search_traces)
    monkeypatch.setattr(mlflow, "get_trace", fake.get_trace)
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://mlflow.invalid")
    monkeypatch.setenv("MLFLOW_EXPERIMENT_ID", "1")
    return fake


def _sessions(payloads: list[bytes]) -> list[list[ImportedSession]]:
    return [
        [item for item in parse(payload, {}) if isinstance(item, ImportedSession)]
        for payload in payloads
    ]


async def test_window_lists_without_spans_and_fetches_complete_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _install(monkeypatch, _load_traces())
    until = SINCE + timedelta(days=60)

    payloads = await collect_payloads(
        fetch(
            {
                "since": SINCE.isoformat(),
                "until": until.isoformat(),
                "filter_string": "trace.status = 'OK'",
            }
        )
    )

    [search] = fake.searches
    assert search["locations"] == ["1"]
    assert search["include_spans"] is False
    assert search["order_by"] == ["timestamp_ms ASC"]
    assert search["filter_string"] == (
        f"trace.timestamp_ms >= {int(SINCE.timestamp() * 1000)} "
        f"AND trace.timestamp_ms <= {int(until.timestamp() * 1000)} "
        "AND trace.status = 'OK'"
    )
    assert len(fake.gets) == 5
    [sessions] = _sessions(payloads)
    weather = next(s for s in sessions if s.external_id == "1:session-weather")
    assert len(weather.inputs["turns"]) == 2
    assert len(sessions) == 4


async def test_window_batches_never_split_a_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded = _load_traces()
    standalone = [_standalone(recorded[0], index) for index in range(1, 25)]
    session = [
        doc
        for doc in recorded
        if doc["info"]["trace_metadata"].get("mlflow.trace.session")
        == "session-weather"
    ]
    for doc in session:
        doc["info"]["request_time"] = "2026-09-29T13:00:00Z"
    tail = [_standalone(recorded[0], 99)]
    tail[0]["info"]["request_time"] = "2026-09-29T14:00:00Z"
    _install(monkeypatch, [*standalone, *session, *tail])

    payloads = await collect_payloads(
        fetch({"since": SINCE.isoformat(), "experiment_ids": ["1"]})
    )

    batches = [json.loads(payload)["traces"] for payload in payloads]
    assert [len(batch) for batch in batches] == [26, 1]
    [first, _] = _sessions(payloads)
    weather = next(s for s in first if s.external_id == "1:session-weather")
    assert len(weather.inputs["turns"]) == 2


async def test_trace_ids_hold_sessions_and_skip_missing_traces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded = _load_traces()
    fake = _install(monkeypatch, recorded)
    by_session: dict[str | None, list[str]] = {}
    for doc in recorded:
        key = doc["info"]["trace_metadata"].get("mlflow.trace.session")
        by_session.setdefault(key, []).append(doc["info"]["trace_id"])
    [weather_1, weather_2] = by_session["session-weather"]
    standalone = by_session[None][0]
    missing = "tr-" + "0" * 32

    payloads = await collect_payloads(
        fetch({"trace_ids": [weather_1, standalone, missing, weather_2]})
    )

    assert not fake.searches
    assert fake.gets == [weather_1, standalone, missing, weather_2]
    [immediate, held] = _sessions(payloads)
    assert [s.metadata["mlflow.trace_ids"] for s in immediate] == [[standalone]]
    [weather] = held
    assert weather.external_id == "1:session-weather"
    assert set(weather.metadata["mlflow.trace_ids"]) == {weather_1, weather_2}


async def test_window_requires_an_experiment(monkeypatch: pytest.MonkeyPatch) -> None:
    _install(monkeypatch, _load_traces())
    monkeypatch.delenv("MLFLOW_EXPERIMENT_ID")

    with pytest.raises(ValueError, match="experiment_ids is required"):
        await collect_payloads(fetch({"since": SINCE.isoformat()}))


async def test_fetch_requires_a_tracking_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _install(monkeypatch, _load_traces())
    monkeypatch.delenv("MLFLOW_TRACKING_URI")

    with pytest.raises(RuntimeError, match="MLFLOW_TRACKING_URI is not set"):
        await collect_payloads(fetch({"trace_ids": ["tr-a"]}))
    assert not fake.gets


@pytest.mark.parametrize(
    "query",
    [
        {"experiment_ids": ["1"]},
        {"since": SINCE.isoformat(), "experiment_ids": []},
        {"since": SINCE.isoformat(), "unknown": True},
    ],
)
async def test_rejects_invalid_queries(
    monkeypatch: pytest.MonkeyPatch, query: dict[str, Any]
) -> None:
    _install(monkeypatch, _load_traces())

    with pytest.raises(ValueError):
        await collect_payloads(fetch(query))


async def test_importer_fetch_matches_api_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, _load_traces())
    query = {"since": SINCE.isoformat()}

    assert await collect_payloads(importer.fetch(query)) == await collect_payloads(
        fetch(query)
    )


def test_serialized_traces_parse_like_the_cli_export() -> None:
    documents = _load_traces()
    traces = [Trace.from_dict(doc) for doc in documents]

    assert list(parse(serialize_traces(traces), {})) == list(
        parse(json.dumps({"traces": documents}).encode(), {})
    )
