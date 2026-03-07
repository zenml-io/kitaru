"""Tests for the `@kitaru.checkpoint` implementation."""

from __future__ import annotations

import sys
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

import pytest
from zenml.config.retry_config import StepRetryConfig

from kitaru.checkpoint import checkpoint
from kitaru.runtime import _get_current_checkpoint, _is_inside_checkpoint


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
    with pytest.raises(ValueError, match="Checkpoint retries must be >= 0"):
        checkpoint(retries=-1)(lambda: None)


def test_checkpoint_rejects_call_outside_flow_context() -> None:
    wrapped, captured = _build_checkpoint(lambda: "ok")

    with (
        _zenml_contexts(
            compilation_active=False,
            step_active=False,
            dynamic_run_active=False,
        ),
        pytest.raises(RuntimeError, match=r"inside a @kitaru\.flow"),
    ):
        wrapped()

    assert captured["step"].call_args is None


def test_checkpoint_rejects_call_in_non_kitaru_compilation_context() -> None:
    wrapped, _ = _build_checkpoint(lambda value: value + 1)

    with (
        _zenml_contexts(compilation_active=True, flow_active=False),
        pytest.raises(RuntimeError, match=r"inside a @kitaru\.flow"),
    ):
        wrapped(41)


def test_checkpoint_rejects_call_in_non_kitaru_dynamic_context() -> None:
    wrapped, _ = _build_checkpoint(lambda value: value + 1)

    with (
        _zenml_contexts(dynamic_run_active=True, flow_active=False),
        pytest.raises(RuntimeError, match=r"inside a @kitaru\.flow"),
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
        pytest.raises(RuntimeError, match="Nested checkpoint calls"),
    ):
        wrapped()


def test_submit_requires_running_flow_context() -> None:
    wrapped, captured = _build_checkpoint(lambda: "ok")

    with (
        _zenml_contexts(step_active=False, dynamic_run_active=False),
        pytest.raises(RuntimeError, match="Concurrent checkpoint execution"),
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
        pytest.raises(RuntimeError, match="Nested checkpoint calls"),
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
    def _user_step(value: str) -> str:
        assert _is_inside_checkpoint()
        current = _get_current_checkpoint()
        assert current is not None
        assert current.name == "_user_step"
        assert current.type == "llm_call"
        return value.upper()

    wrapped, _ = _build_checkpoint(
        _user_step,
        checkpoint_type="llm_call",
    )

    with _zenml_contexts(
        step_active=False,
        dynamic_run_active=True,
        flow_active=True,
    ):
        result = wrapped("hi")

    assert result == "HI"
    assert not _is_inside_checkpoint()
    assert _get_current_checkpoint() is None
