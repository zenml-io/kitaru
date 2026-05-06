"""Tests for the `@checkpoint` implementation."""

from __future__ import annotations

import sys
from collections.abc import Callable
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from zenml.config.retry_config import StepRetryConfig
from zenml.enums import StepRuntime, StepType

from kitaru.analytics import AnalyticsEvent
from kitaru.checkpoint import checkpoint
from kitaru.errors import KitaruContextError, KitaruUsageError
from kitaru.flow import flow
from kitaru.runtime import (
    _flow_scope,
    _get_current_checkpoint,
    _get_current_checkpoint_id,
    _get_current_execution_id,
    _get_current_flow,
    _is_inside_checkpoint,
    _is_inside_flow,
)


class _FakeStep:
    """Small fake ZenML step object for checkpoint wrapper tests."""

    def __init__(self, func: Callable[..., Any]) -> None:
        self._func = func
        self.args: dict[str, tuple[tuple[Any, ...], dict[str, Any]]] = {}
        self.results: dict[str, object] = {
            "submit": object(),
            "map": object(),
            "product": object(),
        }
        self.side_effects: dict[str, BaseException] = {}
        # Orders surface calls relative to the telemetry mock for post-step
        # assertions.
        self.events: list[str] = []

    @property
    def call_args(self) -> tuple[tuple[Any, ...], dict[str, Any]] | None:
        return self.args.get("call")

    @property
    def submit_args(self) -> tuple[tuple[Any, ...], dict[str, Any]] | None:
        return self.args.get("submit")

    def _record_and_raise(self, surface: str) -> None:
        self.events.append(surface)
        exc = self.side_effects.get(surface)
        if exc is not None:
            raise exc

    def __call__(
        self,
        *args: Any,
        id: str | None = None,
        after: Any = None,
        **kwargs: Any,
    ) -> Any:
        self.args["call"] = (args, {"id": id, "after": after, **kwargs})
        self._record_and_raise("call")
        return self._func(*args, **kwargs)

    def submit(
        self,
        *args: Any,
        id: str | None = None,
        after: Any = None,
        **kwargs: Any,
    ) -> object:
        self.args["submit"] = (args, {"id": id, "after": after, **kwargs})
        self._record_and_raise("submit")
        return self.results["submit"]

    def map(self, *args: Any, after: Any = None, **kwargs: Any) -> object:
        self.args["map"] = (args, {"after": after, **kwargs})
        self._record_and_raise("map")
        return self.results["map"]

    def product(self, *args: Any, after: Any = None, **kwargs: Any) -> object:
        self.args["product"] = (args, {"after": after, **kwargs})
        self._record_and_raise("product")
        return self.results["product"]


def _build_checkpoint(
    func: Callable[..., Any],
    *,
    retries: int = 0,
    checkpoint_type: str | None = None,
    runtime: StepRuntime | str | None = None,
    cache: bool | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Create a checkpoint with a fake ZenML step decorator."""
    captured: dict[str, Any] = {}

    def _fake_step(
        *,
        name: str | None = None,
        retry: StepRetryConfig | None,
        enable_cache: bool | None = None,
        extra: dict[str, Any],
        step_type: StepType | None = None,
        runtime: StepRuntime | None = None,
    ) -> Any:
        captured["name"] = name
        captured["retry"] = retry
        captured["enable_cache"] = enable_cache
        captured["extra"] = extra
        captured["step_type"] = step_type
        captured["runtime"] = runtime

        def _decorate(step_func: Callable[..., Any]) -> _FakeStep:
            fake_step = _FakeStep(step_func)
            captured["step"] = fake_step
            return fake_step

        return _decorate

    with patch("kitaru.checkpoint.step", side_effect=_fake_step):
        wrapped = checkpoint(
            retries=retries,
            type=checkpoint_type,
            runtime=runtime,
            cache=cache,
        )(func)

    return wrapped, captured


@contextmanager
def _zenml_contexts(
    *,
    compilation_active: bool = False,
    step_active: bool = False,
    dynamic_run_active: bool = False,
    flow_active: bool = False,
) -> Any:
    """Patch ZenML context checks for deterministic checkpoint tests."""
    with (
        patch(
            "kitaru.checkpoint.PipelineCompilationContext.is_active",
            return_value=compilation_active,
        ),
        patch(
            "kitaru.checkpoint.StepContext.is_active",
            return_value=step_active,
        ),
        patch(
            "kitaru.checkpoint.DynamicPipelineRunContext.is_active",
            return_value=dynamic_run_active,
        ),
        patch("kitaru.checkpoint._is_inside_flow", return_value=flow_active),
    ):
        yield


def test_checkpoint_maps_retries_and_type_to_step_config() -> None:
    wrapped, captured = _build_checkpoint(
        lambda: "ok",
        retries=3,
        checkpoint_type="tool_call",
    )

    retry_config = captured["retry"]
    assert isinstance(retry_config, StepRetryConfig)
    assert retry_config.max_retries == 3
    assert captured["extra"] == {
        "kitaru": {"boundary": "checkpoint", "type": "tool_call"}
    }
    assert captured["step_type"] == StepType.TOOL_CALL
    assert callable(wrapped)


def test_checkpoint_allows_zero_retries_without_retry_config() -> None:
    _, captured = _build_checkpoint(lambda: "ok", retries=0)
    assert captured["retry"] is None


def test_checkpoint_forwards_cache_true_to_zenml_step() -> None:
    _, captured = _build_checkpoint(lambda: "ok", cache=True)
    assert captured["enable_cache"] is True


def test_checkpoint_forwards_cache_false_to_zenml_step() -> None:
    _, captured = _build_checkpoint(lambda: "ok", cache=False)
    assert captured["enable_cache"] is False


def test_checkpoint_omitted_cache_forwards_none() -> None:
    _, captured = _build_checkpoint(lambda: "ok")
    assert captured["enable_cache"] is None


def test_well_known_types_map_to_step_type() -> None:
    _, llm_captured = _build_checkpoint(lambda: "ok", checkpoint_type="llm_call")
    assert llm_captured["step_type"] == StepType.LLM_CALL
    assert llm_captured["extra"] == {
        "kitaru": {"boundary": "checkpoint", "type": "llm_call"}
    }

    _, tool_captured = _build_checkpoint(lambda: "ok", checkpoint_type="tool_call")
    assert tool_captured["step_type"] == StepType.TOOL_CALL
    assert tool_captured["extra"] == {
        "kitaru": {"boundary": "checkpoint", "type": "tool_call"}
    }

    _, memory_captured = _build_checkpoint(lambda: "ok", checkpoint_type="memory_call")
    assert memory_captured["step_type"] == StepType.MEMORY_CALL
    assert memory_captured["extra"] == {
        "kitaru": {"boundary": "checkpoint", "type": "memory_call"}
    }


def test_custom_type_stays_in_extra_only() -> None:
    _, captured = _build_checkpoint(lambda: "ok", checkpoint_type="retrieval")
    assert captured["step_type"] is None
    assert captured["extra"] == {
        "kitaru": {"boundary": "checkpoint", "type": "retrieval"}
    }


def test_none_type_produces_no_step_type() -> None:
    _, captured = _build_checkpoint(lambda: "ok", checkpoint_type=None)
    assert captured["step_type"] is None
    assert "type" not in captured["extra"]["kitaru"]


def test_checkpoint_passes_plain_name_to_zenml_step() -> None:
    def my_example_checkpoint(value: int) -> int:
        return value

    _, captured = _build_checkpoint(my_example_checkpoint)
    assert captured["name"] == "my_example_checkpoint"


def test_checkpoint_registers_source_alias_for_step_reload() -> None:
    def my_example_checkpoint(value: int) -> int:
        return value

    wrapped, captured = _build_checkpoint(my_example_checkpoint)

    alias = "__kitaru_checkpoint_source_my_example_checkpoint"
    step_obj = captured["step"]
    assert step_obj is not None

    module = sys.modules[my_example_checkpoint.__module__]
    try:
        assert getattr(module, alias) is step_obj
    finally:
        delattr(module, alias)

    assert callable(wrapped)


def test_checkpoint_rejects_negative_retries() -> None:
    with pytest.raises(KitaruUsageError, match="Checkpoint retries must be >= 0"):
        checkpoint(retries=-1)(lambda: None)


def test_checkpoint_forwards_runtime_to_zenml_step() -> None:
    _, captured = _build_checkpoint(lambda: "ok", runtime="isolated")
    assert captured["runtime"] == StepRuntime.ISOLATED


def test_checkpoint_omitted_runtime_forwards_none() -> None:
    _, captured = _build_checkpoint(lambda: "ok")
    assert captured["runtime"] is None


def test_checkpoint_normalizes_runtime_string() -> None:
    _, captured = _build_checkpoint(lambda: "ok", runtime=" Isolated ")
    assert captured["runtime"] == StepRuntime.ISOLATED


def test_checkpoint_accepts_runtime_enum_directly() -> None:
    _, captured = _build_checkpoint(lambda: "ok", runtime=StepRuntime.INLINE)
    assert captured["runtime"] == StepRuntime.INLINE


def test_checkpoint_rejects_invalid_runtime_string() -> None:
    with pytest.raises(KitaruUsageError, match="Unsupported checkpoint runtime"):
        checkpoint(runtime="remote")(lambda: None)


def test_checkpoint_rejects_invalid_runtime_type() -> None:
    with pytest.raises(KitaruUsageError, match="Unsupported checkpoint runtime"):
        checkpoint(runtime=123)(lambda: None)  # ty: ignore[invalid-argument-type]


def _invoke_surface(wrapped: Any, method: str) -> None:
    """Invoke a checkpoint surface by name for parametrized tests."""
    if method == "call":
        wrapped()
    elif method == "submit":
        wrapped.submit()
    elif method == "map":
        wrapped.map([1, 2])
    elif method == "product":
        wrapped.product([1, 2])
    else:
        raise AssertionError(f"unknown surface: {method}")


@pytest.mark.parametrize("method", ["call", "submit", "map", "product"])
@pytest.mark.parametrize(
    ("cache", "explicitly_set"),
    [(True, True), (False, True), (None, False)],
)
def test_checkpoint_surface_tracks_invocation_after_step(
    method: str, cache: bool | None, explicitly_set: bool
) -> None:
    """Every public checkpoint surface must emit telemetry post-step."""
    wrapped, captured = _build_checkpoint(lambda *a, **kw: "ok", cache=cache)

    contexts = (
        _zenml_contexts(compilation_active=True, flow_active=True)
        if method == "call"
        else _zenml_contexts(dynamic_run_active=True, flow_active=True)
    )

    with contexts, patch("kitaru.checkpoint.track") as track_mock:
        _invoke_surface(wrapped, method)

    track_mock.assert_called_once_with(
        AnalyticsEvent.CHECKPOINT_INVOKED,
        {"method": method, "cache_explicitly_set": explicitly_set},
    )
    # Telemetry must fire AFTER the underlying ZenML surface, matching the
    # post-success convention used by FLOW_SUBMITTED / LLM_CALLED. Tracking
    # before the call would leak events on failed submissions.
    assert captured["step"].events == [method]


@pytest.mark.parametrize("method", ["call", "submit", "map", "product"])
def test_checkpoint_surface_does_not_track_when_step_raises(method: str) -> None:
    """Failures from the underlying ZenML surface must suppress telemetry."""
    wrapped, captured = _build_checkpoint(lambda *a, **kw: "ok")
    fake_step: _FakeStep = captured["step"]
    fake_step.side_effects[method] = RuntimeError("boom")

    contexts = (
        _zenml_contexts(compilation_active=True, flow_active=True)
        if method == "call"
        else _zenml_contexts(dynamic_run_active=True, flow_active=True)
    )

    with (
        contexts,
        patch("kitaru.checkpoint.track") as track_mock,
        pytest.raises(RuntimeError, match="boom"),
    ):
        _invoke_surface(wrapped, method)

    track_mock.assert_not_called()


def test_checkpoint_does_not_track_when_called_outside_flow_context() -> None:
    """Guardrail rejections must not leak analytics events."""
    wrapped, _ = _build_checkpoint(lambda: "ok")

    with (
        _zenml_contexts(
            compilation_active=False,
            step_active=False,
            dynamic_run_active=False,
        ),
        patch("kitaru.checkpoint.track") as track_mock,
        pytest.raises(KitaruContextError, match=r"inside a @flow"),
    ):
        wrapped()

    track_mock.assert_not_called()


def test_checkpoint_rejects_call_outside_flow_context() -> None:
    wrapped, captured = _build_checkpoint(lambda: "ok")

    with (
        _zenml_contexts(
            compilation_active=False,
            step_active=False,
            dynamic_run_active=False,
        ),
        pytest.raises(KitaruContextError, match=r"inside a @flow"),
    ):
        wrapped()

    assert captured["step"].call_args is None


def test_checkpoint_rejects_call_in_non_kitaru_compilation_context() -> None:
    wrapped, _ = _build_checkpoint(lambda value: value + 1)

    with (
        _zenml_contexts(compilation_active=True, flow_active=False),
        pytest.raises(KitaruContextError, match=r"inside a @flow"),
    ):
        wrapped(41)


def test_checkpoint_rejects_call_in_non_kitaru_dynamic_context() -> None:
    wrapped, _ = _build_checkpoint(lambda value: value + 1)

    with (
        _zenml_contexts(dynamic_run_active=True, flow_active=False),
        pytest.raises(KitaruContextError, match=r"inside a @flow"),
    ):
        wrapped(41)


def test_checkpoint_allows_call_during_flow_compilation() -> None:
    wrapped, captured = _build_checkpoint(lambda value: value + 1)

    with _zenml_contexts(compilation_active=True, flow_active=True):
        result = wrapped(41)

    assert result == 42
    assert captured["step"].call_args is not None


def test_checkpoint_rejects_nested_checkpoint_calls() -> None:
    wrapped, _ = _build_checkpoint(lambda: "ok")

    with (
        _zenml_contexts(
            compilation_active=False,
            step_active=True,
            dynamic_run_active=True,
            flow_active=True,
        ),
        pytest.raises(KitaruContextError, match="Nested checkpoint calls"),
    ):
        wrapped()


def test_submit_requires_running_flow_context() -> None:
    wrapped, captured = _build_checkpoint(lambda: "ok")

    with (
        _zenml_contexts(step_active=False, dynamic_run_active=False),
        pytest.raises(KitaruContextError, match="Concurrent checkpoint execution"),
    ):
        wrapped.submit("payload")

    assert captured["step"].submit_args is None


def test_submit_rejects_nested_checkpoint_calls() -> None:
    wrapped, _ = _build_checkpoint(lambda: "ok")

    with (
        _zenml_contexts(
            step_active=True,
            dynamic_run_active=True,
            flow_active=True,
        ),
        pytest.raises(KitaruContextError, match="Nested checkpoint calls"),
    ):
        wrapped.submit("payload")


def test_submit_returns_zenml_future_object() -> None:
    wrapped, captured = _build_checkpoint(lambda: "ok")
    expected_future = object()
    captured["step"].results["submit"] = expected_future

    with _zenml_contexts(
        step_active=False,
        dynamic_run_active=True,
        flow_active=True,
    ):
        returned_future = wrapped.submit("payload", id="checkpoint-1")

    assert returned_future is expected_future
    assert captured["step"].submit_args == (
        ("payload",),
        {"id": "checkpoint-1", "after": None},
    )


def test_checkpoint_runtime_scope_is_set_while_user_code_runs() -> None:
    fake_step_context = SimpleNamespace(
        pipeline_run=SimpleNamespace(
            id="exec-123",
            pipeline=SimpleNamespace(name="my_flow"),
        ),
        step_run=SimpleNamespace(id="checkpoint-456"),
    )

    def _user_step(value: str) -> str:
        assert _is_inside_flow()
        flow_scope = _get_current_flow()
        assert flow_scope is not None
        assert flow_scope.name == "my_flow"
        assert flow_scope.execution_id == "exec-123"

        assert _is_inside_checkpoint()
        checkpoint_scope = _get_current_checkpoint()
        assert checkpoint_scope is not None
        assert checkpoint_scope.name == "_user_step"
        assert checkpoint_scope.type == "llm_call"
        assert checkpoint_scope.execution_id == "exec-123"
        assert checkpoint_scope.checkpoint_id == "checkpoint-456"

        assert _get_current_execution_id() == "exec-123"
        assert _get_current_checkpoint_id() == "checkpoint-456"
        return value.upper()

    wrapped, _ = _build_checkpoint(
        _user_step,
        checkpoint_type="llm_call",
    )

    with (
        patch("kitaru.runtime.StepContext.get", return_value=fake_step_context),
        _zenml_contexts(
            step_active=False,
            dynamic_run_active=True,
            flow_active=True,
        ),
    ):
        result = wrapped("hi")

    assert result == "HI"
    assert not _is_inside_flow()
    assert _get_current_flow() is None
    assert _get_current_execution_id() is None
    assert not _is_inside_checkpoint()
    assert _get_current_checkpoint() is None
    assert _get_current_checkpoint_id() is None


def test_checkpoint_runtime_scope_allows_unknown_flow_name() -> None:
    fake_step_context = SimpleNamespace(
        pipeline_run=SimpleNamespace(
            id="exec-123",
            pipeline=SimpleNamespace(name=None),
        ),
        step_run=SimpleNamespace(id="checkpoint-456"),
    )

    def _user_step(value: str) -> str:
        flow_scope = _get_current_flow()
        assert flow_scope is not None
        assert flow_scope.name is None
        assert _get_current_execution_id() == "exec-123"
        return value.upper()

    wrapped, _ = _build_checkpoint(_user_step)

    with (
        patch("kitaru.runtime.StepContext.get", return_value=fake_step_context),
        _zenml_contexts(
            step_active=False,
            dynamic_run_active=True,
            flow_active=True,
        ),
    ):
        result = wrapped("hello")

    assert result == "HELLO"
    assert _get_current_flow() is None


def test_checkpoint_runtime_scope_restores_existing_flow_scope() -> None:
    def _user_step(value: str) -> str:
        assert _is_inside_flow()
        flow_scope = _get_current_flow()
        assert flow_scope is not None
        assert flow_scope.name == "outer_flow"
        assert flow_scope.execution_id == "outer-exec"

        assert _is_inside_checkpoint()
        checkpoint_scope = _get_current_checkpoint()
        assert checkpoint_scope is not None
        assert checkpoint_scope.execution_id == "outer-exec"
        return value.upper()

    wrapped, _ = _build_checkpoint(_user_step)

    with _flow_scope(name="outer_flow", execution_id="outer-exec"):
        with _zenml_contexts(
            step_active=False,
            dynamic_run_active=True,
            flow_active=True,
        ):
            result = wrapped("hello")

        assert _is_inside_flow()
        restored = _get_current_flow()
        assert restored is not None
        assert restored.name == "outer_flow"
        assert restored.execution_id == "outer-exec"

    assert result == "HELLO"
    assert not _is_inside_flow()
    assert _get_current_flow() is None
    assert not _is_inside_checkpoint()
    assert _get_current_checkpoint() is None


def test_flow_body_output_handle_string_conversion_is_helpful(
    primed_zenml: None,
) -> None:
    """Checkpoint output handles should display Kitaru guidance, not metadata."""
    observed: dict[str, str] = {}

    @checkpoint
    def produce_issue_127_value():
        return "hello"

    @checkpoint
    def consume_issue_127_value(value):
        assert isinstance(value, str)
        return f"{value}|{type(value).__name__}"

    @flow
    def issue_127_handle_display_flow():
        handle = produce_issue_127_value()
        downstream = consume_issue_127_value(handle)
        observed.update(
            {
                "stringified": str(handle),
                "repr": repr(handle),
                "loaded": handle.load(),
                "downstream": downstream.load(),
            }
        )

    issue_127_handle_display_flow.run(cache=False)

    assert observed["loaded"] == "hello"
    assert observed["downstream"] == "hello|str"

    for rendered in (observed["stringified"], observed["repr"]):
        assert "Kitaru checkpoint output handle" in rendered
        assert ".load()" in rendered
        assert "ArtifactVersionResponse" not in rendered
        assert "ZenML" not in rendered
