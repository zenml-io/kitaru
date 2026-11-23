"""Exercise the SDK 5 observation export without providers or a Langfuse server."""

import asyncio
import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import langfuse
import pytest
from kitaru_langfuse_importer.importer import parse
from langfuse.api.client import LangfuseAPI

from returns_agent import generate_traces as exporter

TRACE_ID = "a" * 32


@pytest.mark.parametrize("original", ['{"nested":1}', "[1,2]", '"quoted"', "42"])
def test_exported_json_looking_text_imports_as_text(original: str) -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": [
                    _get_observation(
                        "root", input=json.dumps(original), output=json.dumps(original)
                    )
                ],
                "meta": {},
            },
        )

    document = exporter._get_trace(_get_client(respond), TRACE_ID)
    [session] = list(parse(json.dumps(document).encode(), {}))
    assert session.inputs["turns"][0]["inputs"] == original
    assert session.nodes[0].inputs == original
    assert session.nodes[0].outputs == original


def _get_observation(identifier: str, **fields: Any) -> dict[str, Any]:
    return {
        "id": identifier,
        "traceId": TRACE_ID,
        "projectId": "private-project",
        "startTime": "2026-10-01T12:00:00Z",
        "endTime": "2026-10-01T12:00:01Z",
        "type": "SPAN",
        **fields,
    }


def _get_client(handler: Any) -> SimpleNamespace:
    return SimpleNamespace(
        api=LangfuseAPI(
            base_url="https://langfuse.test",
            httpx_client=httpx.Client(transport=httpx.MockTransport(handler)),
        )
    )


def test_export_reads_every_sdk_page_and_preserves_trace_data() -> None:
    requests = []
    metadata_value = "complete metadata " * 40

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        assert request.url.path == "/api/public/v2/observations"
        query = request.url.params
        assert query["traceId"] == TRACE_ID
        assert set(query["fields"].split(",")) >= {
            "basic",
            "io",
            "metadata",
            "model",
            "usage",
            "prompt",
            "trace_context",
        }
        assert "parseIoAsJson" not in query
        if query.get("cursor") == "page-2":
            data = [
                _get_observation(
                    "root",
                    name="agent",
                    traceName="Returns ticket",
                    sessionId="returns-001",
                    userId="synthetic-user",
                    tags=["returns-resolution"],
                    release="baseline-v1",
                    version="v1",
                    environment="quickstart-example",
                )
            ]
            cursor = None
        else:
            expanded = query.get("expandMetadata")
            if expanded:
                assert set(expanded.split(",")) == {"reasoning", "public_key"}
            data = [
                _get_observation(
                    "generation",
                    parentObservationId="root",
                    type="GENERATION",
                    input='{"messages":[{"role":"user","content":"Return this"}]}',
                    output="Plain text response",
                    model="gpt-5-nano",
                    usageDetails={"input": 42, "output": 7},
                    costDetails={"input": 0.001, "output": 0.002},
                    totalCost=0.003,
                    promptName="returns",
                    promptVersion=3,
                    metadata={
                        "reasoning": metadata_value if expanded else "truncated",
                        "public_key": "do-not-export",
                    },
                )
            ]
            cursor = "page-2"
        return httpx.Response(200, json={"data": data, "meta": {"cursor": cursor}})

    document = exporter._get_trace(_get_client(respond), TRACE_ID)

    assert len(requests) == 3
    assert document["id"] == TRACE_ID
    assert document["name"] == "Returns ticket"
    assert document["timestamp"] == "2026-10-01T12:00:00Z"
    assert document["sessionId"] == "returns-001"
    assert document["tags"] == ["returns-resolution"]
    assert document["release"] == "baseline-v1"
    assert document["userId"] == "synthetic-user"
    generation, root = document["observations"]
    assert root["id"] == generation["parentObservationId"]
    assert generation["input"] == {
        "messages": [{"role": "user", "content": "Return this"}]
    }
    assert generation["output"] == "Plain text response"
    assert generation["metadata"]["reasoning"] == metadata_value
    assert generation["usageDetails"] == {"input": 42, "output": 7}
    assert generation["costDetails"] == {"input": 0.001, "output": 0.002}
    assert generation["totalCost"] == 0.003
    assert generation["model"] == "gpt-5-nano"
    assert generation["promptVersion"] == 3
    sanitized = exporter._sanitize_export(document)
    assert "projectId" not in sanitized["observations"][0]
    assert sanitized["observations"][0]["metadata"] == {"reasoning": metadata_value}


@pytest.mark.parametrize(
    "initial",
    [
        [],
        [_get_observation("child", parentObservationId="missing")],
        [_get_observation("root", endTime=None)],
        [
            _get_observation("root"),
            _get_observation("orphan", parentObservationId="missing"),
        ],
        [
            _get_observation("root"),
            _get_observation("a", parentObservationId="b"),
            _get_observation("b", parentObservationId="a"),
        ],
    ],
)
def test_export_retries_incomplete_graphs(
    initial: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    requests = []
    sleeps = []
    monkeypatch.setattr(exporter.time, "sleep", sleeps.append)

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        data = initial if len(requests) == 1 else [_get_observation("root")]
        return httpx.Response(200, json={"data": data, "meta": {}})

    document = exporter._get_trace(_get_client(respond), TRACE_ID)

    assert len(requests) == 2
    assert sleeps == [2]
    assert [item["id"] for item in document["observations"]] == ["root"]


def test_export_accepts_app_root_and_event_without_end_time() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": [
                    _get_observation(
                        "root",
                        parentObservationId="outside-app",
                        isRootObservation=True,
                    ),
                    _get_observation(
                        "event",
                        parentObservationId="root",
                        type="EVENT",
                        endTime=None,
                        input='["event", 1]',
                        output='"ok"',
                    ),
                ],
                "meta": {},
            },
        )

    document = exporter._get_trace(_get_client(respond), TRACE_ID)

    assert document["observations"][0]["parentObservationId"] == "outside-app"
    assert document["observations"][1]["input"] == ["event", 1]
    assert document["observations"][1]["output"] == '"ok"'


def test_export_times_out_for_empty_data(monkeypatch: pytest.MonkeyPatch) -> None:
    ticks = iter([0, 181])
    monkeypatch.setattr(exporter.time, "monotonic", lambda: next(ticks))

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": [], "meta": {}})

    with pytest.raises(RuntimeError, match="not queryable after 180 seconds"):
        exporter._get_trace(_get_client(respond), TRACE_ID)


def test_generation_writes_ticket_io_and_redacts_sdk_export(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in ("OPENAI_API_KEY", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"):
        monkeypatch.setenv(name, "synthetic-test-value")
    ticket = exporter.CASES[0]
    monkeypatch.setattr(exporter, "CASES", [ticket])
    monkeypatch.setattr(exporter.Agent, "instrument_all", lambda: None)

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": [
                    _get_observation(
                        "root",
                        input='{"original":true}',
                        output='{"original":true}',
                        metadata={"public_key": "secret", "ticket_id": "001"},
                    )
                ],
                "meta": {},
            },
        )

    client = _get_client(respond)
    client.flush = lambda: None
    monkeypatch.setattr(langfuse, "Langfuse", lambda: client)
    monkeypatch.setattr(langfuse, "propagate_attributes", lambda **kw: nullcontext())

    async def run(prompt: str) -> SimpleNamespace:
        assert ticket.body in prompt
        return SimpleNamespace(
            _traceparent=lambda: f"00-{TRACE_ID}-{'b' * 16}-01",
            output=SimpleNamespace(model_dump=lambda **kw: {"reply": "Refund issued"}),
        )

    monkeypatch.setattr(exporter, "build_agent", lambda *args: SimpleNamespace(run=run))
    output = tmp_path / "nested" / "traces.jsonl"

    assert asyncio.run(exporter.generate_traces(output)) == output
    document = json.loads(output.read_text())
    assert document["input"] == ticket.model_dump(mode="json")
    assert document["output"] == {"reply": "Refund issued"}
    assert document["observations"][0]["input"] == {"original": True}
    assert document["metadata"] == {"ticket_id": "001"}
    assert "secret" not in output.read_text()
    assert "projectId" not in output.read_text()
