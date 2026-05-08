"""Foundation tests for the LangGraph adapter scaffold."""

from __future__ import annotations

import importlib
import inspect
import sys
import types
from collections.abc import Callable
from types import SimpleNamespace
from typing import cast

import pytest
from pydantic import ValidationError

from kitaru.analytics import AnalyticsEvent
from kitaru.errors import KitaruUsageError


@pytest.fixture
def langgraph_adapter(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    """Import the adapter with a fake optional SDK module installed."""
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.langgraph"):
            monkeypatch.delitem(sys.modules, cached, raising=False)
    monkeypatch.setitem(sys.modules, "langgraph", types.ModuleType("langgraph"))
    return importlib.import_module("kitaru.adapters.langgraph")


def test_public_import_surface(langgraph_adapter: types.ModuleType) -> None:
    assert langgraph_adapter.KitaruGraphRunner
    assert langgraph_adapter.LangGraphRunRequest
    assert langgraph_adapter.LangGraphRunResult
    assert langgraph_adapter.LangGraphCapturePolicy
    assert langgraph_adapter.LangGraphDurabilityPolicy
    assert langgraph_adapter.build_resume_request
    assert langgraph_adapter.wait_for_interrupt

    public_names = set(langgraph_adapter.__all__)
    assert "graph_call" not in public_names

    signature = inspect.signature(langgraph_adapter.KitaruGraphRunner)
    assert "checkpoint_strategy" in signature.parameters
    assert "durability_mode" not in signature.parameters
    assert not hasattr(langgraph_adapter.KitaruGraphRunner, "stream")
    assert not hasattr(langgraph_adapter.KitaruGraphRunner, "astream")


def test_langgraph_analytics_events_exist() -> None:
    values = [
        event.value for event in AnalyticsEvent if event.name.startswith("LANGGRAPH_")
    ]

    assert values == [
        "Kitaru LangGraph wrapped",
        "Kitaru LangGraph run completed",
        "Kitaru LangGraph interrupted",
    ]


def test_runner_requires_stable_name(langgraph_adapter: types.ModuleType) -> None:
    with pytest.raises(KitaruUsageError, match="stable `name`"):
        langgraph_adapter.KitaruGraphRunner(SimpleNamespace(invoke=lambda *_: None))


def test_runner_only_accepts_graph_call_strategy(
    langgraph_adapter: types.ModuleType,
) -> None:
    runner = langgraph_adapter.KitaruGraphRunner(
        SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
        checkpoint_strategy="graph_call",
    )

    assert runner.checkpoint_strategy == "graph_call"
    with pytest.raises(KitaruUsageError, match="graph_call"):
        langgraph_adapter.KitaruGraphRunner(
            SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
            checkpoint_strategy="nodes",
        )


def test_run_request_start_resume_and_config_merge(
    langgraph_adapter: types.ModuleType,
) -> None:
    start = langgraph_adapter.LangGraphRunRequest.start(
        {"count": 1},
        thread_id="thread-1",
        configurable={"tenant": "acme", "thread_id": "wrong"},
        config={"configurable": {"region": "eu", "thread_id": "older"}},
        checkpoint_id="checkpoint-1",
        checkpoint_ns="ns",
    )

    merged = langgraph_adapter.merge_config(start)

    assert start.kind == "start"
    assert merged["configurable"] == {
        "region": "eu",
        "tenant": "acme",
        "thread_id": "thread-1",
        "checkpoint_id": "checkpoint-1",
        "checkpoint_ns": "ns",
    }

    command = SimpleNamespace(resume=True)
    resume = langgraph_adapter.LangGraphRunRequest.resume(
        command,
        thread_id="thread-1",
    )
    assert resume.kind == "resume"
    assert resume.command is command

    with pytest.raises(ValidationError, match="thread_id"):
        langgraph_adapter.LangGraphRunRequest.start({"count": 1}, thread_id="")
    with pytest.raises(ValidationError, match="requires command"):
        langgraph_adapter.LangGraphRunRequest(kind="resume", thread_id="thread-1")


def test_capture_policy_and_checkpoint_config_validation(
    langgraph_adapter: types.ModuleType,
) -> None:
    with pytest.raises(ValidationError):
        langgraph_adapter.LangGraphCapturePolicy(max_stream_events=-1)
    with pytest.raises(ValidationError):
        langgraph_adapter.LangGraphCapturePolicy(save_everything=True)

    with pytest.raises(KitaruUsageError, match="runtime='isolated'"):
        langgraph_adapter.KitaruGraphRunner(
            SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
            run_checkpoint_config={"runtime": "isolated"},
        )
    with pytest.raises(KitaruUsageError, match="cache must be a boolean"):
        langgraph_adapter.KitaruGraphRunner(
            SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
            run_checkpoint_config={"cache": "nope"},
        )


def test_outer_checkpoint_defaults_disable_cache_and_retries(
    langgraph_adapter: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_module = importlib.import_module("kitaru.adapters.langgraph._agent")
    seen: dict[str, object] = {}

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **_kwargs: object) -> object:
            return input

    def fake_run_sync_in_checkpoint(**kwargs: object) -> object:
        seen.update(kwargs)
        body = cast(Callable[[], object], kwargs["body"])
        return body()

    monkeypatch.setattr(agent_module, "is_inside_flow", lambda: True)
    monkeypatch.setattr(agent_module, "is_inside_checkpoint", lambda: False)
    monkeypatch.setattr(
        agent_module, "run_sync_in_checkpoint", fake_run_sync_in_checkpoint
    )

    runner = langgraph_adapter.KitaruGraphRunner(FakeGraph())
    result = runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"count": 1},
            thread_id="thread-1",
        )
    )

    assert result.output == {"count": 1}
    assert seen["config"] == {
        "retries": 0,
        "cache": False,
        "type": "graph_call",
        "runtime": "inline",
    }


def test_outer_checkpoint_overrides_are_honored(
    langgraph_adapter: types.ModuleType,
) -> None:
    runner = langgraph_adapter.KitaruGraphRunner(
        SimpleNamespace(name="graph", invoke=lambda *_args, **_kwargs: {}),
        run_checkpoint_config={"cache": True, "retries": 2, "type": "custom"},
    )

    assert runner._graph_call_checkpoint_config() == {
        "cache": True,
        "retries": 2,
        "type": "custom",
        "runtime": "inline",
    }


def test_durability_policy_supplies_default_graph_durability(
    langgraph_adapter: types.ModuleType,
) -> None:
    seen: dict[str, object] = {}

    class FakeGraph:
        name = "fake"
        checkpointer = object()

        def invoke(self, input: object, **kwargs: object) -> object:
            seen.update(kwargs)
            return input

    runner = langgraph_adapter.KitaruGraphRunner(
        FakeGraph(),
        durability=langgraph_adapter.LangGraphDurabilityPolicy(mode="exit"),
    )
    runner.invoke(
        langgraph_adapter.LangGraphRunRequest.start(
            {"count": 1},
            thread_id="thread-1",
        )
    )

    assert seen["durability"] == "exit"


def test_redaction_handles_common_secret_key_forms(
    langgraph_adapter: types.ModuleType,
) -> None:
    serialization = importlib.import_module("kitaru.adapters.langgraph._serialization")

    redacted = serialization.redact_config(
        {
            "x-api-key": "secret",
            "Authorization": "Bearer secret",
            "safe": "value",
        }
    )

    assert redacted == {
        "x-api-key": "[REDACTED]",
        "Authorization": "[REDACTED]",
        "safe": "value",
    }
