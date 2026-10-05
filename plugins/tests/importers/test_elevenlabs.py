"""ElevenLabs conversation parsing and read-only API import contracts."""

import copy
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import partial
from pathlib import Path
from typing import Any

import httpx
import pytest

import kitaru_elevenlabs_importer.api as api_module
from kitaru.api_models.v1.imports import ImportFailure
from kitaru.api_models.v1.session import SessionStatus
from kitaru.api_models.v1.session_node import (
    NodeStatus,
    NodeType,
    SessionNodeCreateRequest,
)
from kitaru.json_pointer import resolve_json_pointer
from kitaru.task.importer import ImportedSession, flatten_nodes
from kitaru_elevenlabs_importer.api import fetch
from kitaru_elevenlabs_importer.importer import InvalidImport, importer, parse

FIXTURES = Path(__file__).parent / "fixtures" / "elevenlabs"
FIXTURE_NAMES = [
    "happy_path",
    "multiple_tools",
    "tool_error",
    "interruption",
    "not_found",
]


def _load(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"{name}.json").read_bytes())


def _parse(value: Any, **params: Any) -> list[ImportedSession | ImportFailure]:
    return list(parse(json.dumps(value).encode(), params))


def _get_session(value: Any) -> ImportedSession:
    [item] = _parse(value)
    assert isinstance(item, ImportedSession), item
    return item


def _tools(session: ImportedSession) -> list[SessionNodeCreateRequest]:
    return [
        node
        for node in flatten_nodes(session.nodes)
        if node.node_type == NodeType.TOOL_CALL
    ]


def _conversation(conversation_id: str = "conv_test") -> dict[str, Any]:
    return {
        "agent_id": "agent_test",
        "conversation_id": conversation_id,
        "status": "done",
        "metadata": {"start_time_unix_secs": 1_790_899_200, "call_duration_secs": 5},
        "transcript": [
            {"role": "user", "message": "Where is my order?", "time_in_call_secs": 0},
            {"role": "agent", "message": "It has shipped.", "time_in_call_secs": 2},
        ],
    }


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_real_voice_exports_preserve_session_messages_and_source_order(
    name: str,
) -> None:
    source = _load(name)
    session = _get_session(source)
    assert session.external_id == source["conversation_id"]
    assert session.framework == "elevenlabs"
    assert session.status == SessionStatus.COMPLETED
    first_user = next(
        turn["message"] for turn in source["transcript"] if turn["role"] == "user"
    )
    last_agent = next(
        turn["message"]
        for turn in reversed(source["transcript"])
        if turn["role"] == "agent" and turn["message"]
    )
    assert session.input_text_selector == "/message"
    assert session.output_text_selector == "/message"
    assert resolve_json_pointer(session.inputs, session.input_text_selector) == (
        True,
        first_user,
    )
    assert resolve_json_pointer(session.outputs, session.output_text_selector) == (
        True,
        last_agent,
    )
    nodes = flatten_nodes(session.nodes)
    events = [node for node in nodes if node.node_type == NodeType.SPAN]
    assert [node.external_id for node in events] == [
        f"{source['conversation_id']}:transcript:{index}"
        for index in range(len(source["transcript"]))
    ]
    assert not any(node.node_type == NodeType.LLM_CALL for node in nodes)
    assert len({node.external_id for node in nodes}) == len(nodes)
    assert session.started_at == datetime.fromtimestamp(
        source["metadata"]["start_time_unix_secs"], UTC
    )
    assert session.started_at is not None
    assert session.ended_at == session.started_at + timedelta(
        seconds=source["metadata"]["call_duration_secs"]
    )
    for node, turn in zip(events, source["transcript"], strict=True):
        assert node.started_at == session.started_at + timedelta(
            seconds=turn["time_in_call_secs"]
        )
        if turn["message"]:
            value, selector = (
                (node.inputs, node.input_text_selector)
                if turn["role"] == "user"
                else (node.outputs, node.output_text_selector)
            )
            assert selector is not None
            assert resolve_json_pointer(value, selector) == (True, turn["message"])
    session.model_dump_json()


def test_tool_arguments_results_and_parent_link_are_preserved() -> None:
    source = _load("happy_path")
    session = _get_session(source)
    [tool] = _tools(session)
    assert tool.tool_name == "get_order"
    assert tool.inputs == {"order_id": "1001"}
    assert tool.outputs == {
        "found": True,
        "order_id": "1001",
        "status": "shipped",
        "estimated_delivery": "October 7",
        "product_type": "standard",
    }
    assert (
        tool.external_id == "conv_fixture_happy_path:tool:request_fixture_happy_path_1"
    )
    assert tool.parent_external_id == "conv_fixture_happy_path:transcript:2"
    assert tool.status == NodeStatus.COMPLETED
    assert (
        tool.metadata["elevenlabs"]["tool_call"]
        == source["transcript"][2]["tool_calls"][0]
    )
    assert (
        tool.metadata["elevenlabs"]["tool_result"]
        == source["transcript"][3]["tool_results"][0]
    )


def test_multiple_distinct_tools_preserve_order_and_arguments() -> None:
    [order, policy] = _tools(_get_session(_load("multiple_tools")))
    assert [order.tool_name, policy.tool_name] == ["get_order", "get_return_policy"]
    assert order.inputs == {"order_id": "1002"}
    assert policy.inputs == {"product_type": "standard"}
    assert order.status == policy.status == NodeStatus.COMPLETED


def test_repeated_tool_names_join_by_request_id_instead_of_name_or_position() -> None:
    source = _load("not_found")
    first = source["transcript"][3]["tool_results"][0]
    second = source["transcript"][7]["tool_results"][0]
    source["transcript"][2]["tool_calls"].extend(source["transcript"][6]["tool_calls"])
    source["transcript"][6]["tool_calls"] = []
    source["transcript"][3]["tool_results"] = [second, first]
    source["transcript"][7]["tool_results"] = []
    [one, two] = _tools(_get_session(source))
    assert one.tool_name == two.tool_name == "get_order"
    assert one.inputs == {"order_id": "4040"}
    assert one.outputs == {"found": False, "order_id": "4040"}
    assert two.inputs == {"order_id": "404040"}
    assert two.outputs == {"found": False, "order_id": "404040"}
    assert one.external_id != two.external_id


def test_execution_error_and_business_not_found_have_different_statuses() -> None:
    failed_session = _get_session(_load("tool_error"))
    [failed_tool] = _tools(failed_session)
    assert failed_tool.status == NodeStatus.FAILED
    assert failed_tool.error is not None
    assert "order service unavailable" in failed_tool.error
    assert failed_session.status == SessionStatus.COMPLETED
    missing_tools = _tools(_get_session(_load("not_found")))
    assert all(
        node.status == NodeStatus.COMPLETED and node.error is None
        for node in missing_tools
    )
    assert all(node.outputs["found"] is False for node in missing_tools)


def test_interruption_keeps_spoken_message_and_original_message_distinct() -> None:
    source = _load("interruption")
    session = _get_session(source)
    node = next(
        node for node in session.nodes if node.external_id.endswith(":transcript:2")
    )
    assert node.outputs == {
        "message": "[friendly] I can certainly help you with that!..."
    }
    metadata = node.metadata["elevenlabs"]
    assert metadata["interrupted"] is True
    assert metadata["original_message"] == source["transcript"][2]["original_message"]
    assert metadata["original_message"] != node.outputs["message"]


@pytest.mark.parametrize("name", FIXTURE_NAMES)
def test_usage_uses_each_transcript_event_once_without_recounting_aggregates(
    name: str,
) -> None:
    source = _load(name)
    session = _get_session(source)
    nodes = flatten_nodes(session.nodes)
    turns = [turn for turn in source["transcript"] if turn.get("llm_usage")]
    expected_input = sum(
        turn["llm_usage"]["model_usage"]["qwen35-397b-a17b"]["input"]["tokens"]
        for turn in turns
    )
    expected_output = sum(
        turn["llm_usage"]["model_usage"]["qwen35-397b-a17b"]["output_total"]["tokens"]
        for turn in turns
    )
    expected_cost = sum(
        (
            Decimal(str(category["price"]))
            for turn in turns
            for category in turn["llm_usage"]["model_usage"][
                "qwen35-397b-a17b"
            ].values()
        ),
        Decimal(0),
    )
    assert (
        sum(node.tokens.input_tokens or 0 for node in nodes if node.tokens)
        == expected_input
    )
    assert (
        sum(node.tokens.output_tokens or 0 for node in nodes if node.tokens)
        == expected_output
    )
    assert sum((node.cost or Decimal(0) for node in nodes), Decimal(0)) == expected_cost
    assert expected_cost != Decimal(str(source["metadata"]["cost_fiat"]))
    assert all(node.tokens is None and node.cost is None for node in _tools(session))
    assert all(
        node.model_provider is None and node.requested_model is None for node in nodes
    )
    assert {node.model for node in nodes if node.model} == {"qwen35-397b-a17b"}
    assert (
        session.metadata["elevenlabs"]["provider_metadata"]["charging"]
        == source["metadata"]["charging"]
    )


def test_absent_per_turn_usage_does_not_copy_conversation_totals_onto_nodes() -> None:
    source = _load("happy_path")
    for turn in source["transcript"]:
        turn["llm_usage"] = None
    session = _get_session(source)
    assert all(
        node.tokens is None and node.cost is None
        for node in flatten_nodes(session.nodes)
    )
    assert (
        session.metadata["elevenlabs"]["provider_metadata"]["cost_fiat"]
        == source["metadata"]["cost_fiat"]
    )


def test_missing_prices_and_unknown_categories_do_not_invent_node_costs() -> None:
    source = _load("happy_path")
    for turn in source["transcript"]:
        if not turn.get("llm_usage"):
            continue
        usage = turn["llm_usage"]["model_usage"]["qwen35-397b-a17b"]
        for category in usage.values():
            category.pop("price")
        usage["future_category"] = {"tokens": 100_000, "price": 999}
    session = _get_session(source)
    assert any(node.tokens is not None for node in session.nodes)
    assert all(node.cost is None for node in flatten_nodes(session.nodes))
    node = next(
        node for node in session.nodes if node.external_id.endswith(":transcript:2")
    )
    assert node.metadata["elevenlabs"]["llm_usage"]["model_usage"]["qwen35-397b-a17b"][
        "future_category"
    ] == {"tokens": 100_000, "price": 999}


def test_multiple_models_remain_raw_without_a_guessed_served_model() -> None:
    source = _conversation()
    source["transcript"][1]["llm_usage"] = {
        "model_usage": {
            "first-model": {"input": {"tokens": 12, "price": 0.1}},
            "second-model": {"output_total": {"tokens": 34, "price": 0.2}},
        }
    }
    session = _get_session(source)
    node = session.nodes[1]
    assert node.model is None and node.tokens is None and node.cost is None
    assert (
        node.metadata["elevenlabs"]["llm_usage"] == source["transcript"][1]["llm_usage"]
    )


def test_cached_token_counts_preserve_zero_and_do_not_invent_reasoning_tokens() -> None:
    source = _load("happy_path")
    usage = source["transcript"][2]["llm_usage"]["model_usage"]["qwen35-397b-a17b"]
    usage["input_cache_read"] = {"tokens": 25, "price": 0}
    usage["input_cache_write"] = {"tokens": 31, "price": 0}
    session = _get_session(source)
    node = next(
        node for node in session.nodes if node.external_id.endswith(":transcript:2")
    )
    assert node.tokens is not None
    assert node.tokens.cached_input_tokens == 25
    assert node.tokens.reasoning_tokens is None
    assert node.tokens.input_tokens == 1218


def test_audio_is_an_authenticated_provider_reference_without_downloading_data() -> (
    None
):
    source = _load("happy_path")
    session = _get_session(source)
    metadata = session.metadata["elevenlabs"]
    assert metadata["has_audio"] is True
    assert metadata["has_user_audio"] is True
    assert metadata["has_response_audio"] is True
    assert (
        metadata["recording_url"]
        == "https://api.elevenlabs.io/v1/convai/conversations/conv_fixture_happy_path/audio"
    )
    source["has_audio"] = False
    assert "recording_url" not in _get_session(source).metadata["elevenlabs"]


@pytest.mark.parametrize("container", ["single", "array", "conversations", "webhook"])
def test_supported_containers_keep_stable_source_and_node_identity(
    container: str,
) -> None:
    source = _load("happy_path")
    value: Any = source
    if container == "array":
        value = [source]
    elif container == "conversations":
        value = {"conversations": [source]}
    elif container == "webhook":
        value = {
            "type": "post_call_transcription",
            "event_timestamp": 1_759_440_100,
            "data": source,
        }
    session = _get_session(value)
    expected = _get_session(source)
    assert session.external_id == expected.external_id
    assert flatten_nodes(session.nodes) == flatten_nodes(expected.nodes)
    assert session.inputs == expected.inputs
    assert session.outputs == expected.outputs
    assert _get_session(value) == session


def test_array_keeps_all_five_conversations_separate() -> None:
    items = _parse([_load(name) for name in FIXTURE_NAMES])
    assert len(items) == 5
    assert all(isinstance(item, ImportedSession) for item in items)
    assert {item.external_id for item in items} == {
        f"conv_fixture_{name}" for name in FIXTURE_NAMES
    }


@pytest.mark.parametrize(
    "params",
    [{"join_on": "agent_id"}, {"source_namespace": "tenant"}, {"download_audio": True}],
)
def test_unknown_parser_parameters_are_rejected(params: dict[str, Any]) -> None:
    with pytest.raises(InvalidImport):
        list(parse(json.dumps(_conversation()).encode(), params))


@pytest.mark.parametrize("payload", [b"{", b"\xff\xfe", b"[]{}"])
def test_unreadable_documents_raise_the_public_parser_exception(payload: bytes) -> None:
    with pytest.raises(InvalidImport):
        list(parse(payload, {}))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("conversation_id", None),
        ("conversation_id", ""),
        ("conversation_id", False),
        ("transcript", None),
        ("transcript", {}),
        ("transcript", [False]),
        ("transcript", [{"role": "unknown", "message": "hello"}]),
        ("transcript", [{"role": "user", "message": []}]),
        ("metadata", False),
    ],
)
def test_malformed_conversation_is_isolated_from_healthy_neighbors(
    field: str, value: Any
) -> None:
    broken = _conversation("conv_broken")
    broken[field] = value
    items = _parse([_conversation("conv_before"), broken, _conversation("conv_after")])
    assert [
        item.external_id for item in items if isinstance(item, ImportedSession)
    ] == ["conv_before", "conv_after"]
    failures = [item for item in items if isinstance(item, ImportFailure)]
    assert len(failures) == 1
    for item in items:
        item.model_dump_json()


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_call_id",
        "duplicate_call_id",
        "duplicate_result_id",
    ],
)
def test_tool_structure_errors_fail_only_the_containing_conversation(
    mutation: str,
) -> None:
    source = _load("happy_path")
    call = source["transcript"][2]["tool_calls"][0]
    result = source["transcript"][3]["tool_results"][0]
    if mutation == "missing_call_id":
        del call["request_id"]
    elif mutation == "duplicate_call_id":
        source["transcript"][2]["tool_calls"].append(copy.deepcopy(call))
    elif mutation == "duplicate_result_id":
        source["transcript"][3]["tool_results"].append(copy.deepcopy(result))
    items = _parse([source, _conversation("conv_healthy")])
    assert len(items) == 2
    assert isinstance(items[0], ImportFailure)
    assert isinstance(items[1], ImportedSession)
    assert items[1].external_id == "conv_healthy"


def test_missing_tool_result_remains_explicitly_incomplete() -> None:
    source = _load("happy_path")
    source["transcript"][3]["tool_results"] = []
    [tool] = _tools(_get_session(source))
    assert tool.inputs == {"order_id": "1001"}
    assert tool.outputs is None
    assert tool.status == NodeStatus.IN_PROGRESS
    assert tool.metadata["elevenlabs"]["result_missing"] is True


def test_orphan_tool_result_does_not_invent_arguments_or_parent_call() -> None:
    source = _load("happy_path")
    source["transcript"][2]["tool_calls"] = []
    [tool] = _tools(_get_session(source))
    assert tool.inputs is None
    assert tool.outputs["found"] is True
    assert tool.status == NodeStatus.COMPLETED
    assert tool.parent_external_id != "conv_fixture_happy_path:transcript:2"


def test_non_json_tool_values_remain_raw_text() -> None:
    source = _load("happy_path")
    source["transcript"][2]["tool_calls"][0]["params_as_json"] = "{unparsed source"
    source["transcript"][3]["tool_results"][0]["result_value"] = (
        "Order service is unavailable."
    )
    [tool] = _tools(_get_session(source))
    assert tool.inputs == "{unparsed source"
    assert tool.outputs == "Order service is unavailable."
    assert tool.output_text_selector == ""


def test_identical_conversations_are_deduplicated() -> None:
    source = _conversation()
    [session] = _parse([source, copy.deepcopy(source)])
    assert isinstance(session, ImportedSession)
    assert session.external_id == "conv_test"


@pytest.mark.parametrize("event_type", ["post_call_audio", "call_initiation_failure"])
def test_non_transcription_webhooks_are_rejected(event_type: str) -> None:
    with pytest.raises(InvalidImport):
        _parse({"type": event_type, "data": _conversation()})


def test_conflicting_conversation_ids_are_order_independent() -> None:
    source = _conversation()
    changed = copy.deepcopy(source)
    changed["transcript"][-1]["message"] = "It was cancelled."
    for order in ([source, changed], [changed, source]):
        items = _parse([*order, _conversation("conv_healthy")])
        assert [
            item.external_id for item in items if isinstance(item, ImportedSession)
        ] == ["conv_healthy"]
        failures = [item for item in items if isinstance(item, ImportFailure)]
        assert len(failures) == 1
        assert failures[0].external_id == "conv_test"


def _mock_api(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[httpx.Request], httpx.Response],
) -> list[httpx.Request]:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return handler(request)

    client = httpx.AsyncClient
    monkeypatch.setattr(
        api_module.httpx,
        "AsyncClient",
        partial(client, transport=httpx.MockTransport(respond)),
    )
    monkeypatch.setenv("ELEVENLABS_API_KEY", "elevenlabs-fixture-secret")
    return requests


async def _fetch(query: dict[str, Any]) -> list[dict[str, Any]]:
    return [json.loads(payload) async for payload in fetch(query)]


async def test_api_uses_environment_credential_and_exact_explicit_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = _mock_api(
        monkeypatch,
        lambda request: httpx.Response(
            200, json=_conversation(request.url.path.rsplit("/", 1)[-1])
        ),
    )
    records = await _fetch(
        {"trace_ids": ["conv_second", "conv_first"], "concurrency": 1}
    )
    assert [record["conversation_id"] for record in records] == [
        "conv_second",
        "conv_first",
    ]
    assert [request.url.path for request in requests] == [
        "/v1/convai/conversations/conv_second",
        "/v1/convai/conversations/conv_first",
    ]
    assert all(
        request.headers["xi-api-key"] == "elevenlabs-fixture-secret"
        for request in requests
    )
    assert all(
        request.url.host == "api.elevenlabs.io" and not request.url.query
        for request in requests
    )
    items = [item for record in records for item in _parse(record)]
    assert all(isinstance(item, ImportedSession) for item in items)


async def test_api_pagination_preserves_time_and_agent_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/convai/conversations":
            if request.url.params.get("cursor") == "page-two":
                return httpx.Response(
                    200,
                    json={
                        "conversations": [
                            {"conversation_id": "conv_second", "status": "failed"}
                        ],
                        "has_more": False,
                        "next_cursor": None,
                    },
                )
            return httpx.Response(
                200,
                json={
                    "conversations": [
                        {"conversation_id": "conv_first", "status": "done"},
                        {"conversation_id": "conv_live", "status": "in-progress"},
                        {"conversation_id": "conv_processing", "status": "processing"},
                    ],
                    "has_more": True,
                    "next_cursor": "page-two",
                },
            )
        record = _conversation(request.url.path.rsplit("/", 1)[-1])
        if record["conversation_id"] == "conv_second":
            record["status"] = "failed"
        return httpx.Response(200, json=record)

    requests = _mock_api(monkeypatch, respond)
    records = await _fetch(
        {
            "since": "2026-10-02T00:00:00Z",
            "until": "2026-10-03T00:00:00Z",
            "agent_id": "agent_test",
            "page_size": 2,
            "concurrency": 1,
        }
    )
    assert [record["conversation_id"] for record in records] == [
        "conv_first",
        "conv_second",
    ]
    listings = [
        request
        for request in requests
        if request.url.path == "/v1/convai/conversations"
    ]
    assert len(listings) == 2
    for request in listings:
        assert request.url.params["agent_id"] == "agent_test"
        assert request.url.params["page_size"] == "2"
        assert request.url.params["call_start_after_unix"] == "1790899200"
        assert request.url.params["call_start_before_unix"] == "1790985600"
    assert listings[1].url.params["cursor"] == "page-two"
    assert not any(
        request.url.path.endswith(("/conv_live", "/conv_processing", "/audio"))
        for request in requests
    )


async def test_api_empty_listing_yields_no_payloads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_api(
        monkeypatch,
        lambda request: httpx.Response(
            200, json={"conversations": [], "has_more": False, "next_cursor": None}
        ),
    )
    assert await _fetch({"since": "2026-10-02T00:00:00Z"}) == []


async def test_api_filters_rounded_provider_window_with_exact_detail_times(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start_times = {
        "conv_before": 1_790_899_200,
        "conv_lower": 1_790_899_200.5,
        "conv_inside": 1_790_899_201,
        "conv_upper": 1_790_899_202.5,
        "conv_after": 1_790_899_203,
    }

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/convai/conversations":
            return httpx.Response(
                200,
                json={
                    "conversations": [
                        {"conversation_id": identifier, "status": "done"}
                        for identifier in start_times
                    ],
                    "has_more": False,
                },
            )
        identifier = request.url.path.rsplit("/", 1)[-1]
        record = _conversation(identifier)
        record["metadata"]["start_time_unix_secs"] = start_times[identifier]
        return httpx.Response(200, json=record)

    requests = _mock_api(monkeypatch, respond)
    records = await _fetch(
        {"since": "2026-10-02T00:00:00.500000Z", "until": "2026-10-02T00:00:02.500000Z"}
    )
    assert [record["conversation_id"] for record in records] == [
        "conv_lower",
        "conv_inside",
    ]
    listing = requests[0]
    assert listing.url.params["call_start_after_unix"] == "1790899200"
    assert listing.url.params["call_start_before_unix"] == "1790899203"


@pytest.mark.parametrize("cursor", [None, ""])
async def test_api_rejects_missing_cursor_when_more_pages_are_claimed(
    monkeypatch: pytest.MonkeyPatch,
    cursor: str | None,
) -> None:
    _mock_api(
        monkeypatch,
        lambda request: httpx.Response(
            200, json={"conversations": [], "has_more": True, "next_cursor": cursor}
        ),
    )
    with pytest.raises(ValueError, match="cursor"):
        await _fetch({"since": "2026-10-02T00:00:00Z"})


async def test_api_rejects_repeated_cursor_without_looping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = _mock_api(
        monkeypatch,
        lambda request: httpx.Response(
            200,
            json={"conversations": [], "has_more": True, "next_cursor": "same-cursor"},
        ),
    )
    with pytest.raises(ValueError, match="cursor"):
        await _fetch({"since": "2026-10-02T00:00:00Z"})
    assert len(requests) == 2


async def test_api_bounds_empty_pages_with_fresh_cursors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal pages
        pages += 1
        assert pages <= 2, "Empty pagination exceeded the request bound"
        return httpx.Response(
            200,
            json={
                "conversations": [],
                "has_more": True,
                "next_cursor": f"page-{pages}",
            },
        )

    requests = _mock_api(monkeypatch, respond)
    with pytest.raises(ValueError, match="pagination exceeded page-request limit"):
        await _fetch({"since": "2026-10-02T00:00:00Z", "limit": 2})
    assert len(requests) == 2
    assert requests[1].url.params["cursor"] == "page-1"


async def test_api_limits_scanned_conversations_including_unfinished_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = _mock_api(
        monkeypatch,
        lambda request: httpx.Response(
            200,
            json={
                "conversations": [
                    {"conversation_id": "conv_live", "status": "in-progress"}
                ],
                "has_more": True,
                "next_cursor": "more",
            },
        ),
    )
    assert await _fetch({"since": "2026-10-02T00:00:00Z", "limit": 1}) == []
    assert len(requests) == 1
    assert requests[0].url.params["page_size"] == "1"


async def test_api_retries_rate_limit_without_exposing_provider_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(
                429, headers={"retry-after": "0"}, text="private diagnostic"
            )
        return httpx.Response(200, json=_conversation())

    _mock_api(monkeypatch, respond)
    [record] = await _fetch({"trace_ids": ["conv_test"]})
    assert record["conversation_id"] == "conv_test"
    assert attempts == 2


async def test_api_rejects_a_different_returned_conversation_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_api(
        monkeypatch,
        lambda request: httpx.Response(200, json=_conversation("conv_wrong")),
    )
    with pytest.raises(ValueError, match="conversation_id"):
        await _fetch({"trace_ids": ["conv_requested"]})


@pytest.mark.parametrize(
    "response",
    [httpx.Response(200, text="private broken JSON"), httpx.Response(200, json=[])],
)
async def test_api_rejects_unreadable_responses_without_printing_payloads(
    monkeypatch: pytest.MonkeyPatch,
    response: httpx.Response,
) -> None:
    _mock_api(monkeypatch, lambda request: response)
    with pytest.raises(ValueError) as error:
        await _fetch({"trace_ids": ["conv_test"]})
    assert "private broken JSON" not in str(error.value)


async def test_importer_instance_exposes_the_same_fetch_and_parse_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_api(monkeypatch, lambda request: httpx.Response(200, json=_conversation()))
    [payload] = [
        payload async for payload in importer.fetch({"trace_ids": ["conv_test"]})
    ]
    [session] = list(importer.parse(payload, {}))
    assert isinstance(session, ImportedSession)
    assert session.external_id == "conv_test"


@pytest.mark.parametrize(
    "query",
    [
        {},
        {"conversation_ids": ["conv_test"]},
        {"trace_ids": []},
        {"trace_ids": [False]},
        {"trace_ids": ["conv_test"], "api_key": "do-not-send"},
        {"since": "2026-10-02"},
        {"since": "2026-10-03T00:00:00Z", "until": "2026-10-02T00:00:00Z"},
        {"trace_ids": ["conv_test"], "concurrency": 0},
        {"trace_ids": ["conv_test"], "page_size": 101},
    ],
)
async def test_api_rejects_invalid_queries_before_network_access(
    monkeypatch: pytest.MonkeyPatch, query: dict[str, Any]
) -> None:
    requests = _mock_api(
        monkeypatch, lambda request: pytest.fail("Invalid query made a network request")
    )
    with pytest.raises(ValueError):
        await _fetch(query)
    assert requests == []


async def test_api_missing_environment_credential_is_clear_and_offline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = _mock_api(
        monkeypatch,
        lambda request: pytest.fail("Missing credential made a network request"),
    )
    monkeypatch.delenv("ELEVENLABS_API_KEY", raising=False)
    with pytest.raises(ValueError, match="ELEVENLABS_API_KEY"):
        await _fetch({"trace_ids": ["conv_test"]})
    assert requests == []


@pytest.mark.parametrize("status", [401, 403, 404, 500])
async def test_api_http_errors_do_not_turn_into_empty_success(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    _mock_api(
        monkeypatch,
        lambda request: httpx.Response(status, json={"detail": "fixture error"}),
    )
    with pytest.raises(ValueError, match=f"HTTP {status}") as error:
        await _fetch({"trace_ids": ["conv_test"]})
    assert "fixture error" not in str(error.value)
    assert "elevenlabs-fixture-secret" not in str(error.value)


@pytest.mark.parametrize("status", ["initiated", "in-progress", "processing"])
async def test_api_explicit_ids_do_not_import_partial_conversations(
    monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    record = _conversation()
    record["status"] = status
    _mock_api(monkeypatch, lambda request: httpx.Response(200, json=record))
    [record] = await _fetch({"trace_ids": ["conv_test"]})
    [failure] = _parse(record)
    assert isinstance(failure, ImportFailure)
    assert failure.external_id == "conv_test"
