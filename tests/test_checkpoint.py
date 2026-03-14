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

from kitaru.checkpoint import checkpoint
from kitaru.errors import KitaruContextError, KitaruUsageError
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
        self.call_args: tuple[tuple[Any, ...], dict[str, Any]] | None = None
        self.submit_args: tuple[tuple[Any, ...], dict[str, Any]] | None = None
        self.submit_result: object = object()

    def __call__(
        self,
        *args: Any,
        id: str | None = None,
        after: Any = None,
        **kwargs: Any,
    ) -> Any:
        self.call_args = (args, {"id": id, "after": after, **kwargs})
        return self._func(*args, **kwargs)

    def submit(
        self,
        *args: Any,
        id: str | None = None,
        after: Any = None,
        **kwargs: Any,
    ) -> object:
        self.submit_args = (args, {"id": id, "after": after, **kwargs})
        return self.submit_result

    def map(self, *args: Any, after: Any = None, **kwargs: Any) -> object:
        return ("map", args, after, kwargs)

    def product(self, *args: Any, after: Any = None, **kwargs: Any) -> object:
        return ("product", args, after, kwargs)


def _build_checkpoint(
    func: Callable[..., Any],
    *,
    retries: int = 0,
    checkpoint_type: str | None = None,
) -> tuple[Any, dict[str, Any]]:
    """Create a checkpoint with a fake ZenML step decorator."""
    captured: dict[str, Any] = {}

    def _fake_step(*, retry: StepRetryConfig | None, extra: dict[str, Any]) -> Any:
        captured["retry"] = retry
        captured["extra"] = extra

        def _decorate(step_func: Callable[..., Any]) -> _FakeStep:
            fake_step = _FakeStep(step_func)
            captured["step"] = fake_step
            return fake_step

        return _decorate

    with patch("kitaru.checkpoint.step", side_effect=_fake_step):
        wrapped = checkpoint(retries=retries, type=checkpoint_type)(func)

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
    assert callable(wrapped)


def test_checkpoint_allows_zero_retries_without_retry_config() -> None:
    _, captured = _build_checkpoint(lambda: "ok", retries=0)
    assert captured["retry"] is None


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
    captured["step"].submit_result = expected_future

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
            pipeline=SimpleNamespace(name="__kitaru_pipeline_source_my_flow"),
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
