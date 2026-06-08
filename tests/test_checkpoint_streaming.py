"""Tests for checkpoint-level live event publishing."""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from typing import Any, cast
from uuid import uuid4

import pytest

import kitaru
from kitaru import events
from kitaru._serialization import to_json_safe
from kitaru.checkpoint import _wrap_entrypoint
from kitaru.errors import KitaruContextError, KitaruUsageError
from kitaru.runtime import (
    _checkpoint_scope,
    _flow_scope,
    _get_current_checkpoint_event_correlation_id,
    _next_checkpoint_event_index,
)


class BrokenStrError(Exception):
    """Exception whose message rendering fails."""

    def __str__(self) -> str:
        raise RuntimeError("broken __str__")


class SensitiveRepr:
    """Object whose repr should not leak through live-event safe fallback."""

    def __repr__(self) -> str:
        return "secret-token-from-repr"


class FakeZenMLStreaming:
    """Small fake for ZenML's ``zenml.streaming`` module."""

    def __init__(
        self,
        *,
        fail_publish: bool = False,
        fail_flush: bool = False,
        flush_result: bool = True,
    ) -> None:
        self.fail_publish = fail_publish
        self.fail_flush = fail_flush
        self.flush_result = flush_result
        self.published: list[dict[str, Any]] = []
        self.flushes: list[float] = []

    def publish(
        self,
        payload: dict[str, Any],
        *,
        kind: str,
        correlation_id: str | None,
        index: int | None,
    ) -> None:
        if self.fail_publish:
            raise RuntimeError("publisher offline")
        self.published.append(
            {
                "kind": kind,
                "payload": payload,
                "correlation_id": correlation_id,
                "index": index,
            }
        )

    def flush(self, timeout: float = 2.0) -> bool:
        self.flushes.append(timeout)
        if self.fail_flush:
            raise RuntimeError("flush offline")
        return self.flush_result


def _scope_ids() -> tuple[str, str]:
    return str(uuid4()), str(uuid4())


def _patch_streaming(
    monkeypatch: pytest.MonkeyPatch,
    fake_streaming: FakeZenMLStreaming | None = None,
) -> FakeZenMLStreaming:
    streaming = fake_streaming or FakeZenMLStreaming()
    monkeypatch.setattr(events, "_load_zenml_streaming", lambda: streaming)
    return streaming


def test_runtime_checkpoint_scope_assigns_correlation_id_and_monotonic_indexes() -> (
    None
):
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
        correlation_id = _get_current_checkpoint_event_correlation_id()
        assert correlation_id is not None
        assert correlation_id.startswith("kitaru.checkpoint:train_model:")
        assert _next_checkpoint_event_index() == 0
        assert _next_checkpoint_event_index() == 1
        assert _next_checkpoint_event_index(7) == 7
        assert _next_checkpoint_event_index() == 8


def test_checkpoint_scopes_get_distinct_correlation_ids_for_fan_out_steps() -> None:
    execution_id, checkpoint_id = _scope_ids()

    with _checkpoint_scope(
        name="fan_out",
        checkpoint_type=None,
        execution_id=execution_id,
        checkpoint_id=checkpoint_id,
    ):
        first_correlation_id = _get_current_checkpoint_event_correlation_id()

    with _checkpoint_scope(
        name="fan_out",
        checkpoint_type=None,
        execution_id=execution_id,
        checkpoint_id=str(uuid4()),
    ):
        second_correlation_id = _get_current_checkpoint_event_correlation_id()

    assert first_correlation_id is not None
    assert second_correlation_id is not None
    assert first_correlation_id != second_correlation_id


def test_async_child_tasks_share_checkpoint_event_index_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)

    async def publish_from_child_task(task_index: int) -> None:
        await asyncio.sleep(0)
        events.publish("fanout.child", {"task": task_index})

    async def publish_all() -> None:
        await asyncio.gather(
            *(publish_from_child_task(task_index) for task_index in range(8))
        )

    with _checkpoint_scope(name="fanout", checkpoint_type=None):
        asyncio.run(publish_all())

    indexes = [event["index"] for event in fake_streaming.published]
    assert len(indexes) == 8
    assert sorted(indexes) == list(range(8))


def test_copied_thread_contexts_share_checkpoint_event_index_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)

    def publish_in_context(context_index: int) -> None:
        events.publish("fanout.thread", {"context": context_index})

    with _checkpoint_scope(name="fanout", checkpoint_type=None):
        contexts = [copy_context() for _ in range(8)]
        with ThreadPoolExecutor(max_workers=4) as executor:
            list(
                executor.map(
                    lambda item: item[0].run(publish_in_context, item[1]),
                    zip(contexts, range(8), strict=True),
                )
            )

    indexes = [event["index"] for event in fake_streaming.published]
    assert len(indexes) == 8
    assert sorted(indexes) == list(range(8))


def test_progress_inside_checkpoint_publishes_enriched_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_id, checkpoint_id = _scope_ids()
    fake_streaming = _patch_streaming(monkeypatch)

    with (
        _flow_scope(name="demo_flow", execution_id=execution_id),
        _checkpoint_scope(
            name="train_model",
            checkpoint_type="llm_call",
            execution_id=execution_id,
            checkpoint_id=checkpoint_id,
        ),
    ):
        correlation_id = _get_current_checkpoint_event_correlation_id()
        kitaru.progress("Loading data", percent=0.2, rows=100)

    assert len(fake_streaming.published) == 1
    event = fake_streaming.published[0]
    assert event["kind"] == events.CHECKPOINT_PROGRESS_KIND
    assert event["index"] == 0
    assert event["correlation_id"] == correlation_id
    assert event["payload"]["message"] == "Loading data"
    assert event["payload"]["data"] == {"percent": 0.2, "rows": 100}
    assert event["payload"]["kitaru"] == {
        "source": "kitaru",
        "execution_id": execution_id,
        "checkpoint_id": checkpoint_id,
        "checkpoint_name": "train_model",
        "checkpoint_type": "llm_call",
        "correlation_id": correlation_id,
    }


def test_publish_custom_event_uses_data_payload_and_optional_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)

    with _checkpoint_scope(name="checkpoint_a", checkpoint_type=None):
        events.publish(
            "training.batch.completed",
            {"batch": 1, "ok": True},
            message="Batch done",
        )

    event = fake_streaming.published[0]
    assert event["kind"] == "training.batch.completed"
    assert event["payload"]["message"] == "Batch done"
    assert event["payload"]["data"] == {"batch": 1, "ok": True}
    assert event["payload"]["kitaru"]["checkpoint_name"] == "checkpoint_a"
    assert "execution_id" not in event["payload"]["kitaru"]
    assert "checkpoint_id" not in event["payload"]["kitaru"]
    assert "checkpoint_type" not in event["payload"]["kitaru"]


def test_live_event_serialization_preserves_metadata_when_data_is_bad(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)
    circular_payload: dict[str, Any] = {}
    circular_payload["self"] = circular_payload

    with _checkpoint_scope(name="checkpoint_a", checkpoint_type=None):
        events.publish("custom.circular", circular_payload)

    payload = fake_streaming.published[0]["payload"]
    assert payload["kitaru"]["checkpoint_name"] == "checkpoint_a"
    assert payload["data"] == {
        "python_type": "dict",
        "serialization_error": "Circular reference detected (id repeated)",
    }


def test_live_event_serialization_omits_repr_but_default_helper_keeps_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)

    with _checkpoint_scope(name="checkpoint_a", checkpoint_type=None):
        events.publish("custom.sensitive", {"secret": SensitiveRepr()})

    assert "secret-token-from-repr" in to_json_safe(SensitiveRepr())
    assert fake_streaming.published[0]["payload"]["data"] == {
        "secret": {
            "python_type": "SensitiveRepr",
            "serialization_error": "Value is not JSON serializable.",
        }
    }


def test_public_exports_include_progress_and_events_module() -> None:
    assert kitaru.progress is events.progress
    assert kitaru.events.publish is events.publish
    assert "progress" in kitaru.__all__
    assert "events" in kitaru.__all__


def test_publish_outside_checkpoint_raises_context_error() -> None:
    with pytest.raises(KitaruContextError, match=r"inside a @checkpoint"):
        events.publish("training.batch.completed", {"batch": 1})

    with pytest.raises(KitaruContextError, match=r"kitaru.progress\(\)"):
        kitaru.progress("outside")


def test_publish_validates_kind_payload_percent_and_index() -> None:
    with pytest.raises(KitaruUsageError, match="non-empty string"):
        events.publish("  ")

    with pytest.raises(KitaruUsageError, match="reserved"):
        events.publish("cursor")

    with pytest.raises(KitaruUsageError, match="newline"):
        events.publish("custom\nevent")

    with pytest.raises(KitaruUsageError, match="newline"):
        events.publish("custom\revent")

    with pytest.raises(KitaruUsageError, match="payload must be a mapping"):
        events.publish("custom.event", cast(Any, ["not", "a", "mapping"]))

    with pytest.raises(KitaruUsageError, match="percent"):
        kitaru.progress("bad", percent=1.5)

    with (
        _checkpoint_scope(name="checkpoint_a", checkpoint_type=None),
        pytest.raises(KitaruUsageError, match="non-negative integer"),
    ):
        events.publish("custom.event", index=-1)


def test_publish_degrades_when_zenml_streaming_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_id, checkpoint_id = _scope_ids()
    monkeypatch.setattr(events, "_load_zenml_streaming", lambda: None)

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


def test_dropped_publish_still_consumes_event_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(events, "_load_zenml_streaming", lambda: None)

    with _checkpoint_scope(name="checkpoint_a", checkpoint_type=None):
        events.publish("custom.event", index=5)
        assert _next_checkpoint_event_index() == 6


def test_explicit_flush_true_flushes_without_checkpoint_requirement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)

    assert events.flush(timeout=0.5) is True

    with _checkpoint_scope(name="checkpoint_a", checkpoint_type=None):
        events.publish("custom.event", {"ok": True}, flush=True)

    assert fake_streaming.flushes == [0.5, 2.0]


def test_flush_returns_false_when_zenml_flush_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_streaming(monkeypatch, FakeZenMLStreaming(fail_flush=True))

    assert events.flush() is False


def test_explicit_index_advances_counter_to_avoid_collisions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)

    with _checkpoint_scope(name="checkpoint_a", checkpoint_type=None):
        events.publish("custom.first", index=5)
        events.publish("custom.second")

    assert [event["index"] for event in fake_streaming.published] == [5, 6]


def test_explicit_index_cannot_reuse_lifecycle_started_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)

    def user_function() -> str:
        with pytest.raises(KitaruUsageError, match="lower than the next"):
            events.publish("custom.event", index=0)
        return "done"

    wrapped = _wrap_entrypoint(user_function, checkpoint_type=None)

    assert wrapped() == "done"
    assert [event["kind"] for event in fake_streaming.published] == [
        events.CHECKPOINT_STARTED_KIND,
        events.CHECKPOINT_RETURNED_KIND,
    ]
    assert [event["index"] for event in fake_streaming.published] == [0, 1]


def test_checkpoint_lifecycle_publishes_started_user_progress_returned_and_flushes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)

    def user_function() -> str:
        kitaru.progress("halfway")
        return "done"

    wrapped = _wrap_entrypoint(user_function, checkpoint_type="tool_call")

    assert wrapped() == "done"

    assert [event["kind"] for event in fake_streaming.published] == [
        events.CHECKPOINT_STARTED_KIND,
        events.CHECKPOINT_PROGRESS_KIND,
        events.CHECKPOINT_RETURNED_KIND,
    ]
    assert [event["index"] for event in fake_streaming.published] == [0, 1, 2]
    assert [event["payload"].get("status") for event in fake_streaming.published] == [
        "started",
        None,
        "returned",
    ]
    assert len({event["correlation_id"] for event in fake_streaming.published}) == 1
    assert fake_streaming.flushes == [events._LIFECYCLE_FLUSH_TIMEOUT]


def test_checkpoint_lifecycle_publishes_failed_and_flushes_terminal_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)

    def user_function() -> str:
        raise ValueError("bad data")

    wrapped = _wrap_entrypoint(user_function, checkpoint_type=None)

    with pytest.raises(ValueError, match="bad data"):
        wrapped()

    assert [event["kind"] for event in fake_streaming.published] == [
        events.CHECKPOINT_STARTED_KIND,
        events.CHECKPOINT_FAILED_KIND,
    ]
    failed_payload = fake_streaming.published[1]["payload"]
    assert failed_payload["status"] == "failed"
    assert failed_payload["error_type"] == "ValueError"
    assert failed_payload["message"] == events._CHECKPOINT_FAILED_SAFE_MESSAGE
    assert "bad data" not in repr(failed_payload)
    assert fake_streaming.flushes == [events._LIFECYCLE_FLUSH_TIMEOUT]


def test_lifecycle_publish_failure_does_not_mask_user_result_or_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_streaming(monkeypatch, FakeZenMLStreaming(fail_publish=True))

    def successful_function() -> str:
        return "done"

    assert _wrap_entrypoint(successful_function, checkpoint_type=None)() == "done"

    def failing_function() -> str:
        raise RuntimeError("real failure")

    with pytest.raises(RuntimeError, match="real failure"):
        _wrap_entrypoint(failing_function, checkpoint_type=None)()


def test_failed_lifecycle_does_not_publish_raw_exception_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)
    secret = "api_key=sk-test-secret-123"

    def failing_function() -> str:
        raise RuntimeError(f"backend exploded with {secret}")

    with pytest.raises(RuntimeError, match=secret):
        _wrap_entrypoint(failing_function, checkpoint_type=None)()

    assert [event["kind"] for event in fake_streaming.published] == [
        events.CHECKPOINT_STARTED_KIND,
        events.CHECKPOINT_FAILED_KIND,
    ]
    failed_payload = fake_streaming.published[1]["payload"]
    assert failed_payload["status"] == "failed"
    assert failed_payload["error_type"] == "RuntimeError"
    assert failed_payload["message"] == events._CHECKPOINT_FAILED_SAFE_MESSAGE
    assert secret not in repr(failed_payload)


def test_failed_lifecycle_does_not_stringify_user_exception_unsafely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)

    def failing_function() -> str:
        raise BrokenStrError()

    with pytest.raises(BrokenStrError):
        _wrap_entrypoint(failing_function, checkpoint_type=None)()

    assert [event["kind"] for event in fake_streaming.published] == [
        events.CHECKPOINT_STARTED_KIND,
        events.CHECKPOINT_FAILED_KIND,
    ]
    failed_payload = fake_streaming.published[1]["payload"]
    assert failed_payload["error_type"] == "BrokenStrError"
    assert failed_payload["message"] == events._CHECKPOINT_FAILED_SAFE_MESSAGE


def test_terminal_flush_false_is_logged_without_masking_return(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_streaming = _patch_streaming(
        monkeypatch,
        FakeZenMLStreaming(flush_result=False),
    )

    def user_function() -> str:
        return "done"

    wrapped = _wrap_entrypoint(user_function, checkpoint_type=None)

    with caplog.at_level(logging.WARNING, logger="kitaru.events"):
        assert wrapped() == "done"

    assert pytest.approx(0.25) == events._LIFECYCLE_FLUSH_TIMEOUT
    assert fake_streaming.flushes == [events._LIFECYCLE_FLUSH_TIMEOUT]
    assert "pending events did not flush" in caplog.text
    assert events.CHECKPOINT_RETURNED_KIND in caplog.text


def test_terminal_flush_exception_is_logged_without_masking_user_exception(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_streaming = _patch_streaming(
        monkeypatch,
        FakeZenMLStreaming(fail_flush=True),
    )

    def user_function() -> str:
        raise ValueError("real failure")

    wrapped = _wrap_entrypoint(user_function, checkpoint_type=None)

    with (
        caplog.at_level(logging.WARNING, logger="kitaru.events"),
        pytest.raises(ValueError, match="real failure"),
    ):
        wrapped()

    assert fake_streaming.flushes == [events._LIFECYCLE_FLUSH_TIMEOUT]
    assert "Failed to flush Kitaru checkpoint events" in caplog.text
    assert "pending events did not flush" in caplog.text
    assert events.CHECKPOINT_FAILED_KIND in caplog.text


def test_failed_lifecycle_publish_boundary_does_not_mask_user_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_failed_publish(
        kind: str,
        envelope_fields: Any = None,
        **kwargs: Any,
    ) -> None:
        del kind, kwargs
        if envelope_fields and envelope_fields.get("status") == "failed":
            raise RuntimeError("lifecycle broke")

    monkeypatch.setattr(events, "_publish_envelope", broken_failed_publish)

    def failing_function() -> str:
        raise ValueError("real failure")

    with pytest.raises(ValueError, match="real failure"):
        _wrap_entrypoint(failing_function, checkpoint_type=None)()


def test_checkpoint_lifecycle_explicit_index_advances_next_automatic_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_streaming = _patch_streaming(monkeypatch)

    def user_function() -> str:
        events.publish("custom.event", index=7)
        return "done"

    assert _wrap_entrypoint(user_function, checkpoint_type=None)() == "done"
    assert [event["index"] for event in fake_streaming.published] == [0, 7, 8]
