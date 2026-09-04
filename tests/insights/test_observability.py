#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""Tests for metadata-only insight generation observation."""

import asyncio
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

from kitaru.insights.observability import (
    GenerationEvent,
    LangfuseGenerationObserver,
    observe_safely,
)


async def test_observer_receives_metadata_only() -> None:
    events: list[GenerationEvent] = []

    class Observer:
        async def record(self, event: GenerationEvent) -> None:
            events.append(event)

    await observe_safely(
        Observer(),
        GenerationEvent(
            name="analyst",
            run_id="opaque-run",
            stage="analyst",
            metadata={"candidate_count": 4, "outcome": "succeeded"},
        ),
    )
    assert events[0].metadata == {"candidate_count": 4, "outcome": "succeeded"}
    serialized = events[0].model_dump_json()
    assert "prompt" not in serialized
    assert "response" not in serialized


async def test_observer_failure_is_ignored() -> None:
    class BrokenObserver:
        async def record(self, event: GenerationEvent) -> None:
            raise RuntimeError("unavailable")

    await observe_safely(
        BrokenObserver(),
        GenerationEvent(name="validation", run_id="run", metadata={}),
    )


async def test_slow_observer_is_bounded_and_ignored() -> None:
    completed = False

    class SlowObserver:
        async def record(self, event: GenerationEvent) -> None:
            nonlocal completed
            await asyncio.sleep(1)
            completed = True

    await observe_safely(
        SlowObserver(),
        GenerationEvent(name="validation", run_id="run", metadata={}),
        timeout_seconds=0.001,
    )

    assert completed is False


def test_langfuse_observer_ignores_generic_project_credentials(monkeypatch) -> None:
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "generic-public")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "generic-secret")
    monkeypatch.delenv("KITARU_INSIGHTS_LANGFUSE_PUBLIC_KEY", raising=False)
    monkeypatch.delenv("KITARU_INSIGHTS_LANGFUSE_SECRET_KEY", raising=False)

    with pytest.raises(ValueError, match="insight-specific"):
        LangfuseGenerationObserver()


async def test_langfuse_observer_uses_dedicated_config_and_metadata_only(
    monkeypatch,
) -> None:
    constructed: dict[str, object] = {}
    observations: list[dict[str, object]] = []
    ended: list[bool] = []

    class FakeObservation:
        def end(self) -> None:
            ended.append(True)

    class FakeLangfuse:
        def __init__(self, **kwargs) -> None:
            constructed.update(kwargs)

        def start_observation(self, **kwargs):
            observations.append(kwargs)
            return FakeObservation()

    monkeypatch.setattr(
        "kitaru.insights.observability.importlib.import_module",
        lambda name: SimpleNamespace(Langfuse=FakeLangfuse),
    )
    monkeypatch.setenv("KITARU_INSIGHTS_LANGFUSE_PUBLIC_KEY", "insight-public")
    monkeypatch.setenv("KITARU_INSIGHTS_LANGFUSE_SECRET_KEY", "insight-secret")
    monkeypatch.setenv("KITARU_INSIGHTS_LANGFUSE_BASE_URL", "https://lf.example")
    observer = LangfuseGenerationObserver()

    await observer.record(
        GenerationEvent(
            name="validation",
            run_id="01990000-0000-7000-8000-000000000001",
            metadata={"outcome": "deterministic", "insight_count": 2},
        )
    )

    assert constructed == {
        "public_key": "insight-public",
        "secret_key": "insight-secret",
        "base_url": "https://lf.example",
    }
    assert observations == [
        {
            "trace_context": {
                "trace_id": "01990000000070008000000000000001",
            },
            "name": "insight-generation.validation",
            "as_type": "span",
            "metadata": {
                "stage": None,
                "outcome": "deterministic",
                "insight_count": 2,
            },
        }
    ]
    assert ended == [True]
    assert "insight-secret" not in repr(observer)


async def test_langfuse_sync_exception_is_best_effort() -> None:
    class BrokenClient:
        def start_observation(self, **kwargs):
            raise RuntimeError("unavailable")

    observer = object.__new__(LangfuseGenerationObserver)
    observer._client = BrokenClient()
    observer._sync_call_lock = threading.Lock()

    await observe_safely(
        observer,
        GenerationEvent(name="validation", run_id="run", metadata={}),
    )


def test_blocking_langfuse_call_does_not_delay_event_loop_shutdown() -> None:
    script = """
import asyncio
import threading
import time
from kitaru.insights.observability import (
    GenerationEvent,
    LangfuseGenerationObserver,
    observe_safely,
)

class BlockingClient:
    def start_observation(self, **kwargs):
        time.sleep(10)

observer = object.__new__(LangfuseGenerationObserver)
observer._client = BlockingClient()
observer._sync_call_lock = threading.Lock()
asyncio.run(
    observe_safely(
        observer,
        GenerationEvent(name="validation", run_id="run", metadata={}),
        timeout_seconds=0.01,
    )
)
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=2,
    )

    assert result.returncode == 0, result.stderr


async def test_repeated_timeouts_keep_one_live_sync_call_per_observer() -> None:
    started = threading.Event()
    release = threading.Event()

    class FakeObservation:
        def end(self) -> None:
            pass

    class BlockingClient:
        def __init__(self) -> None:
            self.calls = 0

        def start_observation(self, **kwargs):
            self.calls += 1
            started.set()
            release.wait(timeout=2)
            return FakeObservation()

    client = BlockingClient()
    observer = object.__new__(LangfuseGenerationObserver)
    observer._client = client
    observer._sync_call_lock = threading.Lock()
    event = GenerationEvent(name="validation", run_id="run", metadata={})

    await observe_safely(observer, event, timeout_seconds=0.001)
    assert started.is_set()
    for _ in range(20):
        await observe_safely(observer, event, timeout_seconds=0.001)

    assert client.calls == 1
    assert observer._sync_call_lock.locked()

    release.set()
    for _ in range(20):
        if not observer._sync_call_lock.locked():
            break
        await asyncio.sleep(0.001)
    assert not observer._sync_call_lock.locked()
