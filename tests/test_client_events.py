"""Tests for SDK execution event watching."""

from __future__ import annotations

import json
from collections.abc import Generator, Iterable
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import call, patch
from uuid import uuid4

import pytest
from zenml.enums import ExecutionStatus as ZenMLExecutionStatus

from kitaru._client._events import (
    SSEFrame,
    StreamingStore,
    ensure_stream_response_supported,
    map_stream_event,
    open_rest_sse_stream,
    parse_sse_lines,
    watch_execution_events,
)
from kitaru.client import ExecutionEvent, KitaruClient
from kitaru.config import ResolvedConnectionConfig
from kitaru.errors import (
    KitaruBackendError,
    KitaruFeatureNotAvailableError,
    KitaruUsageError,
)


class _FakeResponse:
    def __init__(
        self,
        lines: list[str] | None = None,
        *,
        status_code: int = 200,
        text: str = "",
        content_chunks: list[bytes | str] | None = None,
    ) -> None:
        self._lines = lines or []
        self._content_chunks = content_chunks
        self.status_code = status_code
        self.text = text
        self.closed = False
        self.iter_content_chunks_read = 0

    def iter_content(
        self,
        chunk_size: int = 1,
        decode_unicode: bool = False,
    ) -> Iterable[bytes | str]:
        chunks = self._content_chunks
        if chunks is None:
            chunks = [self.text.encode("utf-8")]
        for raw_chunk in chunks:
            chunk = (
                raw_chunk.encode("utf-8") if isinstance(raw_chunk, str) else raw_chunk
            )
            for offset in range(0, len(chunk), chunk_size):
                self.iter_content_chunks_read += 1
                part = chunk[offset : offset + chunk_size]
                yield part.decode("utf-8") if decode_unicode else part

    def iter_lines(self, *, decode_unicode: bool = False) -> Iterable[str | bytes]:
        del decode_unicode
        yield from self._lines

    def close(self) -> None:
        self.closed = True


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = responses
        self.calls: list[dict[str, Any]] = []

    def get(
        self,
        url: str,
        *,
        params: list[tuple[str, str]],
        headers: dict[str, str],
        stream: bool,
        timeout: Any,
        verify: Any,
    ) -> _FakeResponse:
        self.calls.append(
            {
                "url": url,
                "params": params,
                "headers": headers,
                "stream": stream,
                "timeout": timeout,
                "verify": verify,
            }
        )
        if not self._responses:
            raise RuntimeError("no fake streaming response left")
        return self._responses.pop(0)


class _FakeStore:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.url = "https://kitaru.example.test"
        self.config = SimpleNamespace(http_timeout=17, verify_ssl=False)
        self.session = _FakeSession(responses)
        self.authenticate_calls: list[bool] = []

    def authenticate(self, force: bool = False) -> None:
        self.authenticate_calls.append(force)


def _resolved_connection(project: str | None = "default") -> ResolvedConnectionConfig:
    return ResolvedConnectionConfig(
        server_url=None,
        auth_token=None,
        project=project,
    )


def _run(
    *,
    steps: dict[str, Any] | None = None,
    status: ZenMLExecutionStatus = ZenMLExecutionStatus.RUNNING,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        status=status,
        steps=steps or {},
    )


def _step(name: str) -> SimpleNamespace:
    return SimpleNamespace(id=uuid4(), name=name)


def _event_frame(
    *,
    cursor: str,
    kind: str = "kitaru.checkpoint.progress",
    checkpoint_name: str = "research",
    checkpoint_id: str = "checkpoint-1",
    correlation_id: str = "corr-1",
    index: int = 1,
    step_name: str = "research",
) -> list[str]:
    payload = {
        "pipeline_run_id": "run-1",
        "step_run_id": "step-1",
        "step_name": step_name,
        "kind": kind,
        "correlation_id": correlation_id,
        "index": index,
        "payload": {
            "message": "working",
            "kitaru": {
                "checkpoint_id": checkpoint_id,
                "checkpoint_name": checkpoint_name,
                "correlation_id": correlation_id,
            },
        },
    }
    return [
        f"id: {cursor}",
        f"event: {kind}",
        f"data: {json.dumps(payload)}",
        "",
    ]


def test_execution_event_dto_uses_cursor_and_correlation_id_not_stream_timestamp() -> (
    None
):
    fields = ExecutionEvent.__dataclass_fields__

    assert "correlation_id" in fields
    assert "cursor" in fields
    assert "stream_id" not in fields
    assert "timestamp" not in fields


def test_sse_parser_ignores_heartbeats_and_supports_multiline_data() -> None:
    frames = list(
        parse_sse_lines(
            [
                ": ping",
                "",
                "id: cursor-1",
                "event: custom.kind",
                'data: {"part": 1,',
                'data: "ok": true}',
                "",
            ]
        )
    )

    assert len(frames) == 1
    assert frames[0].event == "custom.kind"
    assert frames[0].cursor == "cursor-1"
    assert frames[0].data == '{"part": 1,\n"ok": true}'


def test_data_bearing_eventless_frame_maps_kind_from_json() -> None:
    payload = {
        "pipeline_run_id": "run-1",
        "kind": "custom.kind",
        "payload": {"message": "event without SSE event field"},
    }
    response = _FakeResponse(
        [f"data: {json.dumps(payload)}", "", "event: end", "data: {}", ""]
    )

    events = list(
        watch_execution_events(
            open_stream=lambda last_event_id: response,
            fallback_exec_id="fallback-run",
            reconnect=False,
        )
    )

    assert [event.kind for event in events] == ["custom.kind"]
    assert events[0].payload["message"] == "event without SSE event field"


def test_watch_execution_events_closes_response_when_consumer_stops_early() -> None:
    response = _FakeResponse(_event_frame(cursor="cursor-1"))

    stream = watch_execution_events(
        open_stream=lambda last_event_id: response,
        fallback_exec_id="run-1",
        reconnect=False,
    )

    event = next(stream)

    assert event.cursor == "cursor-1"
    assert response.closed is False

    stream_generator = cast(Generator[ExecutionEvent, None, None], stream)
    stream_generator.close()

    assert response.closed is True


def test_watch_execution_events_closes_response_when_line_iteration_raises() -> None:
    class _RaisingIterLinesResponse(_FakeResponse):
        def iter_lines(self, *, decode_unicode: bool = False) -> Iterable[str | bytes]:
            del decode_unicode
            yield from _event_frame(cursor="cursor-1")
            raise RuntimeError("socket broke")

    response = _RaisingIterLinesResponse()

    stream = watch_execution_events(
        open_stream=lambda last_event_id: response,
        fallback_exec_id="run-1",
        reconnect=False,
    )

    event = next(stream)

    assert event.cursor == "cursor-1"
    assert response.closed is False

    with pytest.raises(KitaruBackendError, match="socket broke"):
        next(stream)

    assert response.closed is True


def test_stream_event_mapper_reads_kitaru_envelope_and_cursor() -> None:
    frame = next(iter(parse_sse_lines(_event_frame(cursor="42-0"))))

    event = map_stream_event(frame, fallback_exec_id="fallback-run")

    assert event.exec_id == "run-1"
    assert event.kind == "kitaru.checkpoint.progress"
    assert event.cursor == "42-0"
    assert event.correlation_id == "corr-1"
    assert event.index == 1
    assert event.checkpoint_id == "checkpoint-1"
    assert event.checkpoint_name == "research"
    assert event.step_name == "research"
    assert event.payload["message"] == "working"


def test_stream_event_mapper_falls_back_to_step_fields() -> None:
    payload = {
        "pipeline_run_id": "run-1",
        "step_run_id": "step-1",
        "step_name": "__kitaru_checkpoint_source_fetch_data",
        "kind": "custom.kind",
        "payload": {},
    }
    frame = SSEFrame(
        event="custom.kind",
        data_lines=[json.dumps(payload)],
        cursor="cursor-1",
    )

    event = map_stream_event(frame, fallback_exec_id="fallback-run")

    assert event.checkpoint_id == "step-1"
    assert event.checkpoint_name == "fetch_data"


def test_stream_event_mapper_rejects_non_string_kind() -> None:
    payload = {
        "pipeline_run_id": "run-1",
        "kind": 123,
        "payload": {},
    }
    frame = SSEFrame(
        event="custom.kind",
        data_lines=[json.dumps(payload)],
        cursor="cursor-1",
    )

    with pytest.raises(KitaruBackendError, match="`kind` must be a string"):
        map_stream_event(frame, fallback_exec_id="fallback-run")


def test_stream_event_mapper_rejects_non_string_metadata_field() -> None:
    payload = {
        "pipeline_run_id": "run-1",
        "kind": "custom.kind",
        "correlation_id": 123,
        "payload": {},
    }
    frame = SSEFrame(
        event="custom.kind",
        data_lines=[json.dumps(payload)],
        cursor="cursor-1",
    )

    with pytest.raises(KitaruBackendError, match="`correlation_id` must be a string"):
        map_stream_event(frame, fallback_exec_id="fallback-run")


def test_stream_event_mapper_rejects_non_string_kitaru_metadata_field() -> None:
    payload = {
        "pipeline_run_id": "run-1",
        "kind": "custom.kind",
        "payload": {
            "kitaru": {
                "checkpoint_name": 123,
            }
        },
    }
    frame = SSEFrame(
        event="custom.kind",
        data_lines=[json.dumps(payload)],
        cursor="cursor-1",
    )

    with pytest.raises(
        KitaruBackendError, match=r"`kitaru\.checkpoint_name` must be a string"
    ):
        map_stream_event(frame, fallback_exec_id="fallback-run")


def test_events_api_does_not_send_step_names_for_live_checkpoint_watch() -> None:
    research_step = _step("research")
    write_step = _step("write")
    run = _run(steps={"research": research_step, "write": write_step})
    fake_store = _FakeStore(
        [
            _FakeResponse(
                [*_event_frame(cursor="cursor-1"), "event: end", "data: {}", ""]
            )
        ]
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch(
            "kitaru.client._ExecutionsAPI._event_rest_store", return_value=fake_store
        ),
    ):
        client_cls.return_value.get_pipeline_run.return_value = run
        events = list(
            KitaruClient().executions.events(
                str(run.id),
                kinds=["kitaru.checkpoint.progress", "custom.kind"],
                checkpoint="research",
                correlation_ids=["corr-1", "corr-2"],
                since="cursor-0",
                reconnect=False,
            )
        )

    assert [event.kind for event in events] == ["kitaru.checkpoint.progress"]
    client_cls.return_value.get_pipeline_run.assert_called_once_with(
        name_id_or_prefix=str(run.id),
        allow_name_prefix_match=False,
        project="default",
        hydrate=False,
    )
    assert fake_store.session.calls[0]["url"] == (
        f"https://kitaru.example.test/api/v1/runs/{run.id}/events/stream"
    )
    assert fake_store.session.calls[0]["stream"] is True
    assert fake_store.session.calls[0]["timeout"] == 17
    assert fake_store.session.calls[0]["verify"] is False
    assert fake_store.session.calls[0]["params"] == [
        ("since", "cursor-0"),
        ("kinds", "kitaru.checkpoint.progress"),
        ("kinds", "custom.kind"),
        ("correlation_ids", "corr-1"),
        ("correlation_ids", "corr-2"),
    ]


def test_events_api_sends_step_names_for_terminal_checkpoint_watch() -> None:
    research_step = _step("research")
    write_step = _step("write")
    run = _run(status=ZenMLExecutionStatus.COMPLETED)
    hydrated_run = _run(
        steps={"research": research_step, "write": write_step},
        status=ZenMLExecutionStatus.COMPLETED,
    )
    hydrated_run.id = run.id
    fake_store = _FakeStore(
        [
            _FakeResponse(
                [*_event_frame(cursor="cursor-1"), "event: end", "data: {}", ""]
            )
        ]
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch(
            "kitaru.client._ExecutionsAPI._event_rest_store", return_value=fake_store
        ),
    ):
        client_cls.return_value.get_pipeline_run.side_effect = [run, hydrated_run]
        events = list(
            KitaruClient().executions.events(
                str(run.id),
                checkpoint="research",
                reconnect=False,
            )
        )

    assert [event.checkpoint_name for event in events] == ["research"]
    assert client_cls.return_value.get_pipeline_run.call_args_list == [
        call(
            name_id_or_prefix=str(run.id),
            allow_name_prefix_match=False,
            project="default",
            hydrate=False,
        ),
        call(
            name_id_or_prefix=str(run.id),
            allow_name_prefix_match=False,
            project="default",
            hydrate=True,
        ),
    ]
    assert ("step_names", "research") in fake_store.session.calls[0]["params"]
    assert ("step_names", "write") not in fake_store.session.calls[0]["params"]


def test_events_api_uses_unhydrated_run_when_checkpoint_filter_is_absent() -> None:
    run = _run(steps={})
    fake_store = _FakeStore(
        [
            _FakeResponse(
                [*_event_frame(cursor="cursor-1"), "event: end", "data: {}", ""]
            )
        ]
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch(
            "kitaru.client._ExecutionsAPI._event_rest_store", return_value=fake_store
        ),
    ):
        client_cls.return_value.get_pipeline_run.return_value = run
        events = list(KitaruClient().executions.events(str(run.id), reconnect=False))

    assert len(events) == 1
    client_cls.return_value.get_pipeline_run.assert_called_once_with(
        name_id_or_prefix=str(run.id),
        allow_name_prefix_match=False,
        project="default",
        hydrate=False,
    )


def test_events_api_keeps_client_side_checkpoint_filter_when_step_not_hydrated() -> (
    None
):
    run = _run(steps={})
    fake_store = _FakeStore(
        [
            _FakeResponse(
                [
                    *_event_frame(cursor="cursor-1", checkpoint_name="other"),
                    *_event_frame(cursor="cursor-2", checkpoint_name="research"),
                    "event: end",
                    "data: {}",
                    "",
                ]
            )
        ]
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch(
            "kitaru.client._ExecutionsAPI._event_rest_store", return_value=fake_store
        ),
    ):
        client_cls.return_value.get_pipeline_run.return_value = run
        events = list(
            KitaruClient().executions.events(
                str(run.id),
                checkpoint="research",
                reconnect=False,
            )
        )

    assert [event.checkpoint_name for event in events] == ["research"]
    assert ("step_names", "research") not in fake_store.session.calls[0]["params"]


def test_events_api_validates_filters() -> None:
    with patch(
        "kitaru.client.resolve_connection_config", return_value=_resolved_connection()
    ):
        client = KitaruClient()

    with pytest.raises(KitaruUsageError, match="kinds"):
        client.executions.events("run-1", kinds=[" "])
    with pytest.raises(KitaruUsageError, match="kinds"):
        client.executions.events("run-1", kinds=cast(Any, "custom.kind"))
    with pytest.raises(KitaruUsageError, match="kinds"):
        client.executions.events("run-1", kinds=cast(Any, 123))
    with pytest.raises(KitaruUsageError, match="newline"):
        client.executions.events("run-1", kinds=["custom\nevent"])
    with pytest.raises(KitaruUsageError, match="newline"):
        client.executions.events("run-1", kinds=["custom\revent"])
    with pytest.raises(KitaruUsageError, match="correlation_ids"):
        client.executions.events("run-1", correlation_ids=cast(Any, "corr-1"))
    with pytest.raises(KitaruUsageError, match="correlation_ids"):
        client.executions.events("run-1", correlation_ids=cast(Any, object()))
    with pytest.raises(KitaruUsageError, match="correlation_ids"):
        client.executions.events("run-1", correlation_ids=cast(Any, [None]))
    with pytest.raises(KitaruUsageError, match="since"):
        client.executions.events("run-1", since=cast(Any, 123))


def test_events_api_requires_server_backed_connection() -> None:
    run = _run()

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_cls.return_value.get_pipeline_run.return_value = run
        client_cls.return_value.zen_store = object()
        client = KitaruClient()

        with pytest.raises(KitaruFeatureNotAvailableError, match="server-backed"):
            client.executions.events(str(run.id))

    client_cls.return_value.get_pipeline_run.assert_not_called()


def test_open_rest_sse_stream_authenticates_and_retries_once_after_401() -> None:
    first_response = _FakeResponse(status_code=401, text="expired")
    second_response = _FakeResponse(status_code=200)
    store = _FakeStore([first_response, second_response])

    response = open_rest_sse_stream(
        cast(StreamingStore, store),
        path="/runs/run-1/events/stream",
        params=[("kinds", "custom.kind")],
        last_event_id="cursor-1",
    )

    assert response is second_response
    assert first_response.closed is True
    assert store.authenticate_calls == [False, True]
    assert len(store.session.calls) == 2
    assert store.session.calls[1]["headers"]["Accept"] == "text/event-stream"
    assert store.session.calls[1]["headers"]["Last-Event-ID"] == "cursor-1"
    assert store.session.calls[1]["params"] == [("kinds", "custom.kind")]


def test_streaming_http_errors_map_to_clear_kitaru_errors() -> None:
    with pytest.raises(KitaruFeatureNotAvailableError, match="streaming is disabled"):
        ensure_stream_response_supported(
            _FakeResponse(status_code=501, text="streaming disabled")
        )
    with pytest.raises(KitaruFeatureNotAvailableError, match="streaming endpoint"):
        ensure_stream_response_supported(_FakeResponse(status_code=404, text="missing"))
    with pytest.raises(KitaruFeatureNotAvailableError, match="streaming endpoint"):
        ensure_stream_response_supported(_FakeResponse(status_code=405, text="nope"))
    with pytest.raises(KitaruBackendError, match="not authorized"):
        ensure_stream_response_supported(_FakeResponse(status_code=401, text="expired"))
    with pytest.raises(KitaruBackendError, match="permission"):
        ensure_stream_response_supported(
            _FakeResponse(status_code=403, text="forbidden")
        )
    with pytest.raises(KitaruBackendError, match="temporarily unavailable"):
        ensure_stream_response_supported(_FakeResponse(status_code=503, text="busy"))


def test_streaming_http_errors_expose_only_safe_json_detail() -> None:
    detail = "token expired\nplease log in again"

    with pytest.raises(KitaruBackendError) as exc_info:
        ensure_stream_response_supported(
            _FakeResponse(status_code=401, text=json.dumps({"detail": detail}))
        )

    message = str(exc_info.value)
    assert "Server response detail: token expired please log in again" in message


def test_streaming_http_errors_omit_raw_html_body() -> None:
    body = "<html><body>proxy.internal.example stack trace</body></html>"

    with pytest.raises(KitaruBackendError) as exc_info:
        ensure_stream_response_supported(_FakeResponse(status_code=401, text=body))

    message = str(exc_info.value)
    assert "not authorized" in message
    assert "proxy.internal.example" not in message
    assert "<html>" not in message


def test_streaming_http_error_json_detail_is_truncated() -> None:
    long_detail = "x" * 400

    with pytest.raises(KitaruBackendError) as exc_info:
        ensure_stream_response_supported(
            _FakeResponse(status_code=503, text=json.dumps({"detail": long_detail}))
        )

    message = str(exc_info.value)
    assert message.endswith("...")
    assert "x" * 300 not in message


def test_streaming_http_error_reads_only_bounded_body_prefix() -> None:
    response = _FakeResponse(
        status_code=401,
        text="this fallback text should not be used",
        content_chunks=[
            b"<html>proxy.internal.example",
            *[b"x" * 1024 for _ in range(20)],
        ],
    )

    with pytest.raises(KitaruBackendError) as exc_info:
        ensure_stream_response_supported(response)

    message = str(exc_info.value)
    assert "not authorized" in message
    assert "proxy.internal.example" not in message
    assert response.iter_content_chunks_read < 20


def test_http_503_reconnects_when_reconnect_is_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        _FakeResponse(status_code=503, text="busy"),
        _FakeResponse([*_event_frame(cursor="cursor-1"), "event: end", "data: {}", ""]),
    ]
    seen_last_event_ids: list[str | None] = []
    monkeypatch.setattr("kitaru._client._events.time.sleep", lambda delay: None)

    def _open(last_event_id: str | None) -> _FakeResponse:
        seen_last_event_ids.append(last_event_id)
        return responses.pop(0)

    events = list(
        watch_execution_events(
            open_stream=_open,
            fallback_exec_id="run-1",
            reconnect=True,
        )
    )

    assert [event.cursor for event in events] == ["cursor-1"]
    assert seen_last_event_ids == [None, None]


def test_reconnect_resets_after_filtered_but_valid_data_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        _FakeResponse(_event_frame(cursor="cursor-1", checkpoint_name="other")),
        _FakeResponse(_event_frame(cursor="cursor-2", checkpoint_name="other")),
        _FakeResponse(
            [
                *_event_frame(cursor="cursor-3", checkpoint_name="research"),
                "event: end",
                "data: {}",
                "",
            ]
        ),
    ]
    seen_last_event_ids: list[str | None] = []
    monkeypatch.setattr("kitaru._client._events.time.sleep", lambda delay: None)

    def _open(last_event_id: str | None) -> _FakeResponse:
        seen_last_event_ids.append(last_event_id)
        return responses.pop(0)

    events = list(
        watch_execution_events(
            open_stream=_open,
            fallback_exec_id="run-1",
            checkpoint="research",
            reconnect=True,
            backoff_seconds=(0,),
        )
    )

    assert [event.cursor for event in events] == ["cursor-3"]
    assert seen_last_event_ids == [None, "cursor-1", "cursor-2"]


def test_reconnect_uses_sse_cursor_not_index_or_correlation_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        _FakeResponse(
            _event_frame(cursor="cursor-1", correlation_id="corr-a", index=100)
        ),
        _FakeResponse(
            [
                *_event_frame(cursor="cursor-2", correlation_id="corr-b", index=7),
                "event: end",
                "data: {}",
                "",
            ]
        ),
    ]
    seen_last_event_ids: list[str | None] = []
    monkeypatch.setattr("kitaru._client._events.time.sleep", lambda delay: None)

    def _open(last_event_id: str | None) -> _FakeResponse:
        seen_last_event_ids.append(last_event_id)
        return responses.pop(0)

    events = list(
        watch_execution_events(
            open_stream=_open,
            fallback_exec_id="run-1",
            reconnect=True,
        )
    )

    assert [event.cursor for event in events] == ["cursor-1", "cursor-2"]
    assert [event.index for event in events] == [100, 7]
    assert [event.correlation_id for event in events] == ["corr-a", "corr-b"]
    assert seen_last_event_ids == [None, "cursor-1"]


def test_reconnect_false_surfaces_connection_loss() -> None:
    with pytest.raises(KitaruBackendError, match="ended before"):
        list(
            watch_execution_events(
                open_stream=lambda last_event_id: _FakeResponse(
                    _event_frame(cursor="1")
                ),
                fallback_exec_id="run-1",
                reconnect=False,
            )
        )


def test_gap_frame_surfaces_immediately_even_when_reconnect_enabled() -> None:
    calls = 0

    def _open(last_event_id: str | None) -> _FakeResponse:
        nonlocal calls
        calls += 1
        return _FakeResponse(["event: gap", 'data: {"reason": "overflow"}', ""])

    with pytest.raises(KitaruBackendError, match="delivery gap") as exc_info:
        list(
            watch_execution_events(
                open_stream=_open,
                fallback_exec_id="run-1",
                reconnect=True,
            )
        )

    assert type(exc_info.value) is KitaruBackendError
    assert calls == 1


def test_gap_frame_omits_non_string_control_reason() -> None:
    def _open(last_event_id: str | None) -> _FakeResponse:
        return _FakeResponse(["event: gap", 'data: {"reason": 123}', ""])

    with pytest.raises(KitaruBackendError) as exc_info:
        list(
            watch_execution_events(
                open_stream=_open,
                fallback_exec_id="run-1",
                reconnect=False,
            )
        )

    message = str(exc_info.value)
    assert "delivery gap" in message
    assert "123" not in message


def test_malformed_json_surfaces_immediately_without_skipping_cursor() -> None:
    seen_last_event_ids: list[str | None] = []

    def _open(last_event_id: str | None) -> _FakeResponse:
        seen_last_event_ids.append(last_event_id)
        return _FakeResponse(
            ["id: bad-cursor", "event: custom.kind", "data: not-json", ""]
        )

    with pytest.raises(
        KitaruBackendError, match="Malformed execution event"
    ) as exc_info:
        list(
            watch_execution_events(
                open_stream=_open,
                fallback_exec_id="run-1",
                reconnect=True,
            )
        )

    assert type(exc_info.value) is KitaruBackendError
    assert seen_last_event_ids == [None]


def test_reconnect_gives_up_after_bounded_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    monkeypatch.setattr("kitaru._client._events.time.sleep", lambda delay: None)

    def _open(last_event_id: str | None) -> _FakeResponse:
        nonlocal calls
        calls += 1
        return _FakeResponse([])

    with pytest.raises(KitaruBackendError, match="gave up reconnecting"):
        list(
            watch_execution_events(
                open_stream=_open,
                fallback_exec_id="run-1",
                reconnect=True,
                backoff_seconds=(0, 0),
            )
        )

    assert calls == 3


def test_cursor_frame_advances_reconnect_state_without_yielding_public_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        _FakeResponse(["id: cursor-only", "event: cursor", "data: {}", ""]),
        _FakeResponse(
            [*_event_frame(cursor="event-cursor"), "event: end", "data: {}", ""]
        ),
    ]
    seen_last_event_ids: list[str | None] = []
    monkeypatch.setattr("kitaru._client._events.time.sleep", lambda delay: None)

    def _open(last_event_id: str | None) -> _FakeResponse:
        seen_last_event_ids.append(last_event_id)
        return responses.pop(0)

    events = list(
        watch_execution_events(
            open_stream=_open,
            fallback_exec_id="run-1",
            reconnect=True,
        )
    )

    assert [event.cursor for event in events] == ["event-cursor"]
    assert seen_last_event_ids == [None, "cursor-only"]
