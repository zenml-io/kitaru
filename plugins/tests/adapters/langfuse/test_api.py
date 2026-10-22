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
"""Focused contract tests for the Langfuse fetch entrypoint."""

import asyncio
import json
from contextlib import aclosing
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from types import SimpleNamespace

import httpx
import pytest
from langfuse.api.client import AsyncLangfuseAPI
from langfuse.api.core import ApiError

import kitaru_langfuse_importer.api as api_module
from kitaru.task.importer import ImportedNode, ImportedSession
from kitaru_langfuse_importer.api import fetch, fetch_trace
from kitaru_langfuse_importer.importer import importer, parse

from ..fetch_helpers import collect_payloads
from .fixtures import (
    FakeLangfuseClient,
    build_complete_trace,
    build_observation_v2,
    build_observations_page,
    seed_default_observations,
)

_WINDOW = {"since": "2026-07-24T10:00:00Z", "until": "2026-07-24T11:00:00Z"}


def _flatten(nodes: list[ImportedNode]) -> list[ImportedNode]:
    return [node for root in nodes for node in (root, *_flatten(root.children))]


def _sessions(payloads: list[bytes]) -> list[ImportedSession]:
    results = [item for payload in payloads for item in parse(payload, {})]
    assert all(isinstance(item, ImportedSession) for item in results), results
    return [item for item in results if isinstance(item, ImportedSession)]


def _seed(fake: FakeLangfuseClient, *sessions: str | None) -> list[str]:
    trace_ids = [f"trace-{index}" for index in range(len(sessions))]
    for index, (trace_id, session_id) in enumerate(
        zip(trace_ids, sessions, strict=True)
    ):
        fake.observations.append(
            build_observation_v2(
                f"root-{index}",
                trace_id,
                session_id=session_id,
                start_time=f"2026-07-24T10:{index:02d}:00Z",
                end_time=f"2026-07-24T10:{index:02d}:01Z",
            )
        )
    return trace_ids


async def test_exact_ids_use_observations_and_keep_requested_order(fake_langfuse):
    seed_default_observations(fake_langfuse, ["trace-1", "trace-2"])
    payloads = await collect_payloads(fetch({"trace_ids": ["trace-2", "trace-1"]}))
    assert fake_langfuse.requested == ["trace-2", "trace-1"]
    assert [item.metadata["langfuse.trace_ids"] for item in _sessions(payloads)] == [
        ["trace-2"],
        ["trace-1"],
    ]
    assert all(len(_flatten(item.nodes)) == 2 for item in _sessions(payloads))
    assert all(
        call["request_options"]["timeout_in_seconds"] == 60
        for call in fake_langfuse.observation_calls
    )


async def test_exact_ids_hold_shared_sessions_until_all_requested_ids_arrive(
    fake_langfuse,
):
    fake_langfuse.trace_builders = [
        partial(build_complete_trace, session_id="A"),
        build_complete_trace,
        partial(build_complete_trace, session_id="A"),
    ]
    sessions = _sessions(
        await collect_payloads(
            fetch({"trace_ids": ["one", "two", "three"], "concurrency": 1})
        )
    )
    assert [item.metadata["langfuse.trace_ids"] for item in sessions] == [
        ["two"],
        ["one", "three"],
    ]
    assert len(_flatten(sessions[1].nodes)) == 4


async def test_exact_ids_ignore_window_and_repeated_ids(fake_langfuse):
    seed_default_observations(fake_langfuse, ["trace-1"])
    sessions = _sessions(
        await collect_payloads(
            fetch(
                {"trace_ids": ["trace-1", "trace-1"], "since": "2020-01-01T00:00:00Z"}
            )
        )
    )
    assert len(sessions) == 1
    assert len(fake_langfuse.observation_calls) == 1
    assert "from_start_time" not in fake_langfuse.observation_calls[0]


async def test_exact_id_collects_every_cursor_page(fake_langfuse):
    seed_default_observations(fake_langfuse, ["trace-1"])
    fake_langfuse.observation_page_size = 1
    [session] = _sessions(await collect_payloads(fetch({"trace_ids": ["trace-1"]})))
    assert len(_flatten(session.nodes)) == 2
    assert [call["cursor"] for call in fake_langfuse.observation_calls] == [
        None,
        "offset-1",
    ]


async def test_importer_fetch_matches_api_and_file_identity(fake_langfuse):
    seed_default_observations(fake_langfuse, ["trace-1"])
    expected = await collect_payloads(fetch({"trace_ids": ["trace-1"]}))
    assert (
        await collect_payloads(importer.fetch({"trace_ids": ["trace-1"]})) == expected
    )
    [file_session] = list(
        parse(api_module.serialize_trace(build_complete_trace("trace-1")), {})
    )
    assert (
        _sessions(expected)[0].external_id
        == file_session.external_id
        == "project-1:trace-1"
    )


@pytest.mark.parametrize(
    "session_ids,expected",
    [
        (("A", None, "A"), [["trace-1"], ["trace-0", "trace-2"]]),
        (("A", "A", None), [["trace-0", "trace-1"], ["trace-2"]]),
        ((None, None, None), [["trace-0"], ["trace-1"], ["trace-2"]]),
    ],
)
async def test_window_restores_chronology_and_releases_complete_sessions(
    fake_langfuse, session_ids, expected
):
    trace_ids = _seed(fake_langfuse, *session_ids)
    fake_langfuse.observation_page_size = 1
    payloads = await collect_payloads(fetch({**_WINDOW, "concurrency": 1}))
    assert fake_langfuse.requested == trace_ids
    assert [
        item.metadata["langfuse.trace_ids"] for item in _sessions(payloads)
    ] == expected
    discovery = [
        call for call in fake_langfuse.observation_calls if "trace_id" not in call
    ]
    assert [call["cursor"] for call in discovery] == [None, "offset-1", "offset-2"]
    assert all(
        call["from_start_time"] == datetime(2026, 7, 24, 10, tzinfo=UTC)
        for call in discovery
    )
    assert all(
        call["to_start_time"] == datetime(2026, 7, 24, 11, tzinfo=UTC)
        for call in discovery
    )
    assert all("filter" not in call and "order_by" not in call for call in discovery)


async def test_window_retains_late_children_and_excludes_traces_starting_before_since(
    fake_langfuse,
):
    fake_langfuse.observations = [
        build_observation_v2("root", "selected", start_time="2026-07-24T10:59:00Z"),
        build_observation_v2(
            "child", "selected", parent_id="root", start_time="2026-07-24T11:01:00Z"
        ),
        build_observation_v2("older-root", "older", start_time="2026-07-24T09:59:00Z"),
        build_observation_v2(
            "older-child",
            "older",
            parent_id="older-root",
            start_time="2026-07-24T10:10:00Z",
        ),
    ]
    fake_langfuse.observation_page_size = 1
    [session] = _sessions(await collect_payloads(fetch(_WINDOW)))
    assert session.metadata["langfuse.trace_ids"] == ["selected"]
    assert {node.name for node in _flatten(session.nodes)} == {"root", "child"}


async def test_window_orphan_trace_is_retained_without_inventing_root_io(fake_langfuse):
    fake_langfuse.observations = [
        build_observation_v2(
            "orphan",
            "trace",
            parent_id="unexported-parent",
            is_root_observation=False,
            input='{"child_only":true}',
            output='{"child_result":42}',
        )
    ]
    [session] = _sessions(await collect_payloads(fetch(_WINDOW)))
    assert session.inputs["turns"][0]["inputs"] is None
    assert session.inputs["turns"][0]["outputs"] is None
    assert session.nodes[0].inputs == {"child_only": True}
    assert any(
        "no exported root" in warning
        for warning in session.metadata["normalization_warnings"]
    )


async def test_logical_root_with_physical_parent_supplies_trace_io(fake_langfuse):
    fake_langfuse.observations = [
        build_observation_v2("parent", "trace", input='{"outer":true}'),
        build_observation_v2(
            "root",
            "trace",
            parent_id="parent",
            is_root_observation=True,
            level="ERROR",
            status_message="logical root failed",
            input='{"question":"hello"}',
            output='{"answer":"world"}',
        ),
    ]
    [session] = _sessions(await collect_payloads(fetch({"trace_ids": ["trace"]})))
    assert session.inputs["turns"][0]["inputs"] == {"question": "hello"}
    assert session.nodes[0].children[0].name == "root"
    assert session.error == "logical root failed"


async def test_window_empty_and_default_until(fake_langfuse):
    before = datetime.now(UTC)
    assert await collect_payloads(fetch({"since": _WINDOW["since"]})) == []
    after = datetime.now(UTC)
    assert before <= fake_langfuse.observation_calls[0]["to_start_time"] <= after
    assert not fake_langfuse.requested


@pytest.mark.parametrize("window", [False, True])
async def test_capped_consumption_only_starts_bounded_trace_fetches(
    fake_langfuse, window
):
    trace_ids = _seed(fake_langfuse, None, None, None, None)
    query = _WINDOW if window else {"trace_ids": trace_ids}
    async with aclosing(fetch({**query, "concurrency": 1})) as payloads:
        first = await anext(payloads)
        assert _sessions([first])[0].metadata["langfuse.trace_ids"] == ["trace-0"]
    assert fake_langfuse.requested == ["trace-0"]


async def test_concurrency_is_bounded_and_ordered_despite_completion_order(
    fake_langfuse,
):
    trace_ids = _seed(fake_langfuse, None, None, None, None)
    fake_langfuse.observation_delays = [0.03, 0.0, 0.02, 0.0]
    payloads = await collect_payloads(fetch({"trace_ids": trace_ids, "concurrency": 2}))
    assert fake_langfuse.observation_peak_in_flight == 2
    assert [
        item.metadata["langfuse.trace_ids"][0] for item in _sessions(payloads)
    ] == trace_ids


async def test_early_close_cancels_started_trace_reads(fake_langfuse, monkeypatch):
    trace_ids = _seed(fake_langfuse, None, None, None)
    original = fake_langfuse.async_api.observations.get_many
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def get_many(**kwargs):
        if kwargs.get("trace_id") == "trace-1":
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        return await original(**kwargs)

    monkeypatch.setattr(fake_langfuse.async_api.observations, "get_many", get_many)
    async with aclosing(fetch({"trace_ids": trace_ids, "concurrency": 2})) as payloads:
        await anext(payloads)
        await started.wait()
    assert cancelled.is_set()
    assert "trace-2" not in fake_langfuse.requested


async def test_query_validation_precedes_requests(fake_langfuse):
    with pytest.raises(ValueError, match="Extra inputs"):
        await collect_payloads(fetch({"trace_ids": ["trace"], "filter": "arbitrary"}))
    assert not fake_langfuse.observation_calls


async def test_rate_limits_retry_observation_pages(fake_langfuse, monkeypatch):
    sleeps = []

    async def sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", sleep)
    fake_langfuse.trace_builders = [
        ApiError(status_code=429, headers={"retry-after": "3"}, body={}),
        build_complete_trace,
    ]
    assert len(await collect_payloads(fetch({"trace_ids": ["trace"]}))) == 1
    assert sleeps == [3.0]


async def test_non_rate_limit_api_errors_propagate(fake_langfuse):
    fake_langfuse.trace_builders = [ApiError(status_code=500, body={})]
    with pytest.raises(ApiError):
        await collect_payloads(fetch({"trace_ids": ["trace"]}))


@pytest.mark.parametrize("window", [False, True])
async def test_real_sdk_http_transport_deserializes_rich_trace_into_parser(
    monkeypatch, window
):
    requests = []
    root = {
        "id": "root",
        "traceId": "trace",
        "projectId": "project-1",
        "type": "SPAN",
        "startTime": "2026-07-24T10:00:00Z",
        "endTime": "2026-07-24T10:00:02Z",
        "isRootObservation": True,
        "sessionId": "conversation",
        "traceName": "Answer question",
        "name": "agent",
        "tags": ["release-test"],
        "release": "v5",
        "input": '{"question":"hello"}',
        "output": '{"answer":"world"}',
    }
    generation = {
        **root,
        "id": "llm",
        "parentObservationId": "root",
        "isRootObservation": False,
        "type": "GENERATION",
        "name": "model",
        "model": "gpt-5-nano",
        "input": '[{"role":"user","content":"hello"}]',
        "output": '{"content":"world"}',
        "modelParameters": {"temperature": 0.2},
        "usageDetails": {"input": 3, "output": 5},
        "costDetails": {"total": 0.001},
        "totalCost": 0.001,
        "promptName": "answer",
        "promptVersion": 2,
        "metadata": {"attributes": {"gen_ai.provider.name": "openai"}},
    }

    def respond(request):
        requests.append(request)
        assert request.method == "GET"
        assert request.url.path == "/api/public/v2/observations"
        params = request.url.params
        if "traceId" not in params:
            assert window
            assert params["fields"] == "core,basic"
            assert datetime.fromisoformat(params["fromStartTime"]) == datetime(
                2026, 7, 24, 10, tzinfo=UTC
            )
            assert datetime.fromisoformat(params["toStartTime"]) == datetime(
                2026, 7, 24, 11, tzinfo=UTC
            )
            return httpx.Response(200, json={"data": [root], "meta": {"cursor": None}})
        assert params["traceId"] == "trace"
        assert set(params["fields"].split(",")) == {
            "core",
            "basic",
            "time",
            "io",
            "metadata",
            "model",
            "usage",
            "prompt",
            "trace_context",
        }
        assert set(params["expandMetadata"].split(",")) == {
            "attributes",
            "resourceAttributes",
        }
        assert (
            "parseIoAsJson" not in params
            and "orderBy" not in params
            and "filter" not in params
        )
        if "cursor" not in params:
            return httpx.Response(
                200, json={"data": [generation], "meta": {"cursor": "next"}}
            )
        assert params["cursor"] == "next"
        return httpx.Response(200, json={"data": [root], "meta": {"cursor": None}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        api = AsyncLangfuseAPI(
            base_url="https://langfuse.invalid",
            username="public",
            password="secret",
            httpx_client=http,
        )
        monkeypatch.setattr(
            api_module, "get_client", lambda: SimpleNamespace(async_api=api)
        )
        query = _WINDOW if window else {"trace_ids": ["trace"]}
        [session] = _sessions(await collect_payloads(fetch(query)))
    assert len(requests) == (3 if window else 2)
    assert session.external_id == "project-1:conversation"
    assert session.name == "Answer question"
    assert session.inputs["turns"][0]["inputs"] == {"question": "hello"}
    assert session.inputs["turns"][0]["outputs"] == {"answer": "world"}
    [node] = session.nodes[0].children
    assert node.inputs == [{"role": "user", "content": "hello"}]
    assert node.model == "gpt-5-nano"
    assert node.model_provider == "openai"
    assert node.tokens is not None
    assert node.tokens.input_tokens == 3 and node.tokens.output_tokens == 5
    assert node.cost == Decimal("0.001")
    assert node.model_params == {"temperature": 0.2}
    assert node.metadata["langfuse.prompt_name"] == "answer"
    assert node.metadata["langfuse.prompt_version"] == 2
    assert node.metadata["langfuse.trace.tags"] == ["release-test"]
    assert session.metadata["langfuse.releases"] == ["v5"]


async def test_custom_metadata_is_expanded_before_parser_join(fake_langfuse):
    row = build_observation_v2(
        "root", "trace", metadata={"conversation_key": "truncated"}
    )
    full = row.model_copy(update={"metadata": {"conversation_key": "x" * 300}})
    fake_langfuse.observation_pages["trace"] = [
        build_observations_page([row]),
        build_observations_page([full]),
    ]
    trace = await fetch_trace("trace")
    assert trace.observations[0].metadata == {"conversation_key": "x" * 300}
    assert (
        fake_langfuse.observation_calls[1]["expand_metadata"]
        == "attributes,conversation_key,resourceAttributes"
    )
    [session] = list(
        parse(
            api_module.serialize_trace(trace), {"join_on": "metadata.conversation_key"}
        )
    )
    assert isinstance(session, ImportedSession)
    assert session.metadata["langfuse.session_id"] == "x" * 300


@pytest.mark.parametrize(
    "wire_value,expected",
    [
        ('{"key":[1,true]}', {"key": [1, True]}),
        ("[1,2]", [1, 2]),
        ('"hello"', "hello"),
        ("42", 42),
        ("true", True),
        ("null", None),
        ("plain text", "plain text"),
        (json.dumps('{"nested":1}'), '{"nested":1}'),
        (json.dumps("[1,2]"), "[1,2]"),
        (json.dumps('"quoted"'), '"quoted"'),
        (json.dumps("42"), "42"),
    ],
)
async def test_v2_raw_json_io_is_decoded_without_changing_file_parser(
    fake_langfuse, wire_value, expected
):
    fake_langfuse.observations = [
        build_observation_v2("root", "trace", input=wire_value, output=wire_value)
    ]
    [session] = _sessions(await collect_payloads(fetch({"trace_ids": ["trace"]})))
    assert session.inputs["turns"][0]["inputs"] == expected
    assert session.inputs["turns"][0]["outputs"] == expected
    assert session.nodes[0].inputs == expected
    assert session.nodes[0].outputs == expected


@pytest.mark.parametrize("child_after_until", [False, True])
async def test_window_resolves_session_context_missing_from_roots(
    fake_langfuse, child_after_until
):
    child_time = "2026-07-24T11:01:00Z" if child_after_until else "2026-07-24T10:02:00Z"
    fake_langfuse.observations = [
        build_observation_v2("root-a", "trace-a", start_time="2026-07-24T10:00:00Z"),
        build_observation_v2("root-b", "trace-b", start_time="2026-07-24T10:01:00Z"),
        build_observation_v2(
            "child-a",
            "trace-a",
            parent_id="root-a",
            session_id="conversation",
            start_time=child_time,
        ),
        build_observation_v2(
            "child-b",
            "trace-b",
            parent_id="root-b",
            session_id="conversation",
            start_time=child_time,
        ),
    ]
    async with aclosing(fetch({**_WINDOW, "concurrency": 1})) as payloads:
        # A capped consumer must receive all turns in its first payload.
        [session] = _sessions([await anext(payloads)])
        assert session.external_id == "project-1:conversation"
        assert session.metadata["langfuse.trace_ids"] == ["trace-a", "trace-b"]
        assert len(session.inputs["turns"]) == 2


async def test_ambiguous_application_roots_leave_trace_io_unknown(fake_langfuse):
    fake_langfuse.observations = [
        build_observation_v2(
            "first", "trace", is_root_observation=True, input='{"first":1}'
        ),
        build_observation_v2(
            "second", "trace", is_root_observation=True, input='{"second":2}'
        ),
    ]
    [session] = _sessions(await collect_payloads(fetch({"trace_ids": ["trace"]})))
    assert session.inputs["turns"][0]["inputs"] is None
    assert any(
        "2 root observations" in warning
        for warning in session.metadata["normalization_warnings"]
    )


@pytest.mark.parametrize(
    "session_ids,expected,requested",
    [
        (("A", "A", None), ["trace-0", "trace-1"], ["trace-0", "trace-1"]),
        (("A", None, "A"), ["trace-1"], ["trace-0", "trace-1"]),
    ],
)
async def test_capped_window_preserves_complete_session_release_order(
    fake_langfuse, session_ids, expected, requested
):
    _seed(fake_langfuse, *session_ids)
    async with aclosing(fetch({**_WINDOW, "concurrency": 1})) as payloads:
        [session] = _sessions([await anext(payloads)])
        assert session.metadata["langfuse.trace_ids"] == expected
    assert fake_langfuse.requested == requested
