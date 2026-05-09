"""Tests for checkpoint-level live event streaming."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest

import kitaru
from kitaru import events
from kitaru.checkpoint import _wrap_entrypoint
from kitaru.client import KitaruClient
from kitaru.config import ResolvedConnectionConfig
from kitaru.errors import (
    KitaruBackendError,
    KitaruContextError,
    KitaruFeatureNotAvailableError,
    KitaruUsageError,
)
from kitaru.runtime import (
    _checkpoint_scope,
    _flow_scope,
    _get_current_checkpoint_event_stream_id,
    _next_checkpoint_event_index,
)


class FakeZenMLStreams:
    def __init__(self, *, fail_publish: bool = False, fail_flush: bool = False) -> None:
        self.fail_publish = fail_publish
        self.fail_flush = fail_flush
        self.published: list[dict[str, Any]] = []
        self.flushes: list[float] = []

    def publish(
        self,
        payload: dict[str, Any],
        *,
        kind: str,
        stream_id: str | None,
        index: int | None,
    ) -> None:
        if self.fail_publish:
            raise RuntimeError("publisher offline")
        self.published.append(
            {
                "kind": kind,
                "payload": payload,
                "stream_id": stream_id,
                "index": index,
            }
        )

    def flush(self, timeout: float = 2.0) -> None:
        if self.fail_flush:
            raise RuntimeError("flush offline")
        self.flushes.append(timeout)


def _scope_ids() -> tuple[str, str]:
    return str(uuid4()), str(uuid4())


def _resolved_connection(project: str | None = None) -> ResolvedConnectionConfig:
    return ResolvedConnectionConfig(
        server_url=None,
        auth_token=None,
        project=project,
    )


def test_runtime_checkpoint_scope_assigns_stream_id_and_monotonic_indexes() -> None:
    execution_id, checkpoint_id = _scope_ids()

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="train_model",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        stream_id = _get_current_checkpoint_event_stream_id()
        assert stream_id is not None
        assert stream_id.startswith("kitaru.checkpoint:train_model:")
        assert _next_checkpoint_event_index() == 0
        assert _next_checkpoint_event_index() == 1
        assert _next_checkpoint_event_index(7) == 7
        assert _next_checkpoint_event_index() == 8


def test_progress_inside_checkpoint_publishes_enriched_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_id, checkpoint_id = _scope_ids()
    fake_streams = FakeZenMLStreams()
    monkeypatch.setattr(events, "_load_zenml_streams", lambda: fake_streams)

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="train_model",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        kitaru.progress("Loading data", percent=0.2, rows=100)

    assert len(fake_streams.published) == 1
    event = fake_streams.published[0]
    assert event["kind"] == events.CHECKPOINT_PROGRESS_KIND
    assert event["index"] == 0
    assert event["stream_id"].startswith("kitaru.checkpoint:train_model:")
    assert event["payload"]["message"] == "Loading data"
    assert event["payload"]["data"] == {"percent": 0.2, "rows": 100}
    assert event["payload"]["kitaru"] == {
        "source": "kitaru",
        "execution_id": execution_id,
        "checkpoint_id": checkpoint_id,
        "checkpoint_name": "train_model",
        "checkpoint_type": "llm_call",
    }


def test_publish_outside_checkpoint_raises_context_error() -> None:
    with pytest.raises(KitaruContextError, match=r"inside a @checkpoint"):
        events.publish("training.batch.completed", {"batch": 1})

    with pytest.raises(KitaruContextError, match=r"kitaru.progress\(\)"):
        kitaru.progress("outside")


def test_publish_degrades_when_zenml_streams_are_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_id, checkpoint_id = _scope_ids()
    monkeypatch.setattr(events, "_load_zenml_streams", lambda: None)

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="checkpoint_a",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        events.publish("custom.event", {"ok": True})
        kitaru.progress("still ok")


def test_explicit_flush_true_still_flushes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_id, checkpoint_id = _scope_ids()
    fake_streams = FakeZenMLStreams()
    monkeypatch.setattr(events, "_load_zenml_streams", lambda: fake_streams)

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="checkpoint_a",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        events.publish("custom.event", {"ok": True}, flush=True)

    assert fake_streams.flushes == [2.0]


def test_explicit_index_advances_counter_to_avoid_collisions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_id, checkpoint_id = _scope_ids()
    fake_streams = FakeZenMLStreams()
    monkeypatch.setattr(events, "_load_zenml_streams", lambda: fake_streams)

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="checkpoint_a",
            checkpoint_type=None,
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        events.publish("custom.first", index=5)
        events.publish("custom.second")

    assert [event["index"] for event in fake_streams.published] == [5, 6]


def test_explicit_index_cannot_reuse_lifecycle_started_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streams = FakeZenMLStreams()
    monkeypatch.setattr(events, "_load_zenml_streams", lambda: fake_streams)

    def user_function() -> str:
        with pytest.raises(KitaruUsageError, match="lower than the next"):
            events.publish("custom.event", index=0)
        return "done"

    wrapped = _wrap_entrypoint(user_function, checkpoint_type=None)

    assert wrapped() == "done"
    assert [event["kind"] for event in fake_streams.published] == [
        events.CHECKPOINT_STARTED_KIND,
        events.CHECKPOINT_COMPLETED_KIND,
    ]
    assert [event["index"] for event in fake_streams.published] == [0, 1]


def test_checkpoint_lifecycle_publishes_started_completed_and_flushes_terminal_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streams = FakeZenMLStreams()
    monkeypatch.setattr(events, "_load_zenml_streams", lambda: fake_streams)

    def user_function() -> str:
        return "done"

    wrapped = _wrap_entrypoint(user_function, checkpoint_type="tool_call")

    assert wrapped() == "done"

    assert [event["kind"] for event in fake_streams.published] == [
        events.CHECKPOINT_STARTED_KIND,
        events.CHECKPOINT_COMPLETED_KIND,
    ]
    assert [event["index"] for event in fake_streams.published] == [0, 1]
    assert [event["payload"]["status"] for event in fake_streams.published] == [
        "started",
        "completed",
    ]
    assert fake_streams.flushes == [2.0]


def test_checkpoint_lifecycle_publishes_failed_and_flushes_terminal_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streams = FakeZenMLStreams()
    monkeypatch.setattr(events, "_load_zenml_streams", lambda: fake_streams)

    def user_function() -> str:
        raise ValueError("bad data")

    wrapped = _wrap_entrypoint(user_function, checkpoint_type=None)

    with pytest.raises(ValueError, match="bad data"):
        wrapped()

    assert [event["kind"] for event in fake_streams.published] == [
        events.CHECKPOINT_STARTED_KIND,
        events.CHECKPOINT_FAILED_KIND,
    ]
    failed_payload = fake_streams.published[1]["payload"]
    assert failed_payload["status"] == "failed"
    assert failed_payload["error_type"] == "ValueError"
    assert failed_payload["message"] == "bad data"
    assert fake_streams.flushes == [2.0]


def test_lifecycle_publish_failure_does_not_mask_user_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streams = FakeZenMLStreams(fail_publish=True)
    monkeypatch.setattr(events, "_load_zenml_streams", lambda: fake_streams)

    def user_function() -> str:
        raise RuntimeError("real failure")

    wrapped = _wrap_entrypoint(user_function, checkpoint_type=None)

    with pytest.raises(RuntimeError, match="real failure"):
        wrapped()


class FakeRestStore:
    pass


class FakeZenMLClientWithEvents:
    def __init__(self, raw_events: list[Any]) -> None:
        self.zen_store = FakeRestStore()
        self.raw_events = raw_events
        self.calls: list[dict[str, Any]] = []

    def iter_run_events(
        self,
        exec_id: str,
        *,
        since: str | None,
        kinds: list[str] | None,
        reconnect: bool,
    ) -> Any:
        self.calls.append(
            {
                "exec_id": exec_id,
                "since": since,
                "kinds": kinds,
                "reconnect": reconnect,
            }
        )
        return iter(self.raw_events)


def test_sdk_events_maps_kitaru_payload_and_filters_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exec_id = str(uuid4())
    checkpoint_id = str(uuid4())
    now = datetime(2026, 5, 9, 12, 0, tzinfo=UTC)
    raw_events = [
        SimpleNamespace(
            pipeline_run_id=exec_id,
            kind="kitaru.checkpoint.progress",
            payload={
                "kitaru": {
                    "checkpoint_id": checkpoint_id,
                    "checkpoint_name": "train",
                },
                "message": "halfway",
            },
            stream_id="stream-1",
            index=3,
            ts=now,
            step_run_id="ignored-step-id",
            step_name="ignored_step",
        ),
        SimpleNamespace(
            pipeline_run_id=exec_id,
            kind="external.event",
            payload={"value": 42},
            stream_id="stream-2",
            index=4,
            ts=now,
            step_run_id="step-2",
            step_name="fallback_step",
        ),
    ]
    fake_client = FakeZenMLClientWithEvents(raw_events)
    monkeypatch.setattr("kitaru.client.RestZenStore", FakeRestStore)

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client", return_value=fake_client),
    ):
        client = KitaruClient()
        filtered = list(
            client.executions.events(
                exec_id,
                kinds=["kitaru.checkpoint.progress"],
                checkpoint="train",
                since="evt-1",
                reconnect=False,
            )
        )

    assert fake_client.calls == [
        {
            "exec_id": exec_id,
            "since": "evt-1",
            "kinds": ["kitaru.checkpoint.progress"],
            "reconnect": False,
        }
    ]
    assert len(filtered) == 1
    event = filtered[0]
    assert event.exec_id == exec_id
    assert event.kind == "kitaru.checkpoint.progress"
    assert event.payload["message"] == "halfway"
    assert event.stream_id == "stream-1"
    assert event.index == 3
    assert event.timestamp == now
    assert event.checkpoint_id == checkpoint_id
    assert event.checkpoint_name == "train"


def test_sdk_events_falls_back_to_zenml_step_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exec_id = str(uuid4())
    raw_event = SimpleNamespace(
        pipeline_run_id=exec_id,
        kind="external.event",
        payload={"value": 42},
        stream_id=None,
        index=None,
        ts="2026-05-09T12:00:00Z",
        step_run_id="step-2",
        step_name="__kitaru_checkpoint_source_train",
    )
    fake_client = FakeZenMLClientWithEvents([raw_event])
    monkeypatch.setattr("kitaru.client.RestZenStore", FakeRestStore)

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client", return_value=fake_client),
    ):
        client = KitaruClient()
        event = next(client.executions.events(exec_id, checkpoint="train"))

    assert event.checkpoint_id == "step-2"
    assert event.checkpoint_name == "train"
    assert event.timestamp == datetime(2026, 5, 9, 12, 0, tzinfo=UTC)


def test_sdk_events_raise_feature_unavailable_without_iter_run_events() -> None:
    fake_client = SimpleNamespace(zen_store=FakeRestStore())

    with (
        patch("kitaru.client.RestZenStore", FakeRestStore),
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client", return_value=fake_client),
    ):
        client = KitaruClient()
        with pytest.raises(KitaruFeatureNotAvailableError, match="iter_run_events"):
            list(client.executions.events(str(uuid4())))


def test_sdk_events_raise_feature_unavailable_for_local_store() -> None:
    fake_client = SimpleNamespace(
        zen_store=object(),
        iter_run_events=lambda *_args, **_kwargs: iter([]),
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client", return_value=fake_client),
    ):
        client = KitaruClient()
        with pytest.raises(KitaruFeatureNotAvailableError, match="Local database"):
            list(client.executions.events(str(uuid4())))


def test_sdk_events_raise_feature_unavailable_for_not_implemented(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def iter_run_events(*_args: Any, **_kwargs: Any) -> Any:
        raise NotImplementedError("stream events are not supported")

    fake_client = SimpleNamespace(
        zen_store=FakeRestStore(),
        iter_run_events=iter_run_events,
    )
    monkeypatch.setattr("kitaru.client.RestZenStore", FakeRestStore)

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client", return_value=fake_client),
    ):
        client = KitaruClient()
        with pytest.raises(KitaruFeatureNotAvailableError, match="not supported"):
            list(client.executions.events(str(uuid4())))


def test_sdk_events_raise_backend_error_for_stream_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_events() -> Any:
        yield SimpleNamespace(kind="first", payload={})
        raise RuntimeError("broker went away")

    fake_client = FakeZenMLClientWithEvents(broken_events())
    monkeypatch.setattr("kitaru.client.RestZenStore", FakeRestStore)

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client", return_value=fake_client),
    ):
        client = KitaruClient()
        with pytest.raises(KitaruBackendError, match="Live event streaming failed"):
            list(client.executions.events(str(uuid4())))
