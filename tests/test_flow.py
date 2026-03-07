"""Tests for the `@kitaru.flow` implementation."""

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock, call, patch
from uuid import UUID, uuid4

import pytest
from zenml.config.docker_settings import DockerSettings
from zenml.enums import ExecutionStatus
from zenml.models import PipelineRunResponse

from kitaru.config import ResolvedExecutionConfig
from kitaru.errors import FailureOrigin, KitaruStateError, KitaruUserCodeError
from kitaru.flow import FlowHandle, _wrap_flow_entrypoint, flow
from kitaru.runtime import _get_current_execution_id, _get_current_flow, _is_inside_flow


def _as_pipeline_run(run: _DummyRun) -> PipelineRunResponse:
    return cast(PipelineRunResponse, run)


def _resolved_execution(
    *,
    stack: str = "local",
    cache: bool = True,
    retries: int = 0,
) -> ResolvedExecutionConfig:
    return ResolvedExecutionConfig(
        stack=stack,
        image=None,
        cache=cache,
        retries=retries,
    )


class _DummyArtifact:
    def __init__(self, value: object) -> None:
        self._value = value

    def load(self) -> object:
        return self._value


class _DummyRun:
    def __init__(
        self,
        *,
        status: ExecutionStatus,
        outputs: list[tuple[str, str, object]] | None = None,
        run_id: UUID | None = None,
        status_reason: str | None = None,
        traceback: str | None = None,
    ) -> None:
        self.id = run_id or uuid4()
        self.status = status
        self.status_reason = status_reason
        self.exception_info = (
            SimpleNamespace(traceback=traceback) if traceback else None
        )

        outputs = outputs or []
        output_specs: list[SimpleNamespace] = []
        step_outputs: dict[str, dict[str, _DummyArtifact]] = {}
        for step_name, output_name, value in outputs:
            output_specs.append(
                SimpleNamespace(step_name=step_name, output_name=output_name)
            )
            step_outputs.setdefault(step_name, {})[output_name] = _DummyArtifact(value)

        self.snapshot = SimpleNamespace(
            pipeline_spec=SimpleNamespace(outputs=output_specs)
        )
        self.steps = {
            step_name: SimpleNamespace(regular_outputs=regular_outputs)
            for step_name, regular_outputs in step_outputs.items()
        }

    def get_hydrated_version(self) -> _DummyRun:
        return self


def test_flow_decorator_creates_callable_with_start_and_deploy() -> None:
    run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock(return_value=run)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator) as pipeline_mock,
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
    ):
        wrapped = flow(lambda x: x)
        handle = wrapped.start(123)

    pipeline_mock.assert_called_once_with(dynamic=True)
    assert callable(wrapped)
    assert hasattr(wrapped, "start")
    assert hasattr(wrapped, "deploy")
    assert isinstance(handle, FlowHandle)
    base_pipeline.with_options.assert_called_once_with(
        enable_cache=True,
        retry=None,
        settings=None,
    )


def test_flow_registers_pipeline_source_alias_for_dynamic_reload() -> None:
    run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock(return_value=run)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    def my_example_flow(value: int) -> int:
        return value

    alias = "__kitaru_pipeline_source_my_example_flow"

    with patch("kitaru.flow.pipeline", return_value=zenml_decorator):
        flow(my_example_flow)

    wrapped_entrypoint = zenml_decorator.call_args.args[0]
    assert wrapped_entrypoint.__name__ == alias

    module = sys.modules[my_example_flow.__module__]
    try:
        assert getattr(module, alias) is base_pipeline
    finally:
        delattr(module, alias)


def test_deploy_is_start_sugar_with_stack_override() -> None:
    run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock(return_value=run)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    old_stack_id = uuid4()
    client_mock = MagicMock()
    client_mock.active_stack_model = SimpleNamespace(id=old_stack_id)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client", return_value=client_mock),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
    ):
        wrapped = flow(
            stack="dev",
            image="python:3.12",
            cache=False,
            retries=2,
        )(lambda x: x)
        wrapped.deploy(
            1,
            stack="prod",
            image=DockerSettings(parent_image="python:3.13"),
            cache=True,
            retries=0,
        )

    settings = base_pipeline.with_options.call_args.kwargs["settings"]
    assert settings is None
    assert base_pipeline.with_options.call_args.kwargs["enable_cache"] is True
    assert base_pipeline.with_options.call_args.kwargs["retry"] is None
    assert client_mock.activate_stack.call_args_list == [
        call("prod"),
        call(old_stack_id),
    ]


def test_direct_call_forwards_user_kwargs_without_treating_them_as_overrides() -> None:
    completed = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("step", "output", "done")],
    )
    configured_pipeline = MagicMock(return_value=completed)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client", return_value=client_mock),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
    ):
        wrapped = flow(lambda stack: stack)
        result = wrapped(stack="input-value")

    assert result == "done"
    configured_pipeline.assert_called_once_with(stack="input-value")


def test_start_restores_previous_stack_if_submission_fails() -> None:
    configured_pipeline = MagicMock(side_effect=RuntimeError("submission failed"))
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    old_stack_id = uuid4()
    client_mock = MagicMock()
    client_mock.active_stack_model = SimpleNamespace(id=old_stack_id)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client", return_value=client_mock),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        pytest.raises(RuntimeError, match="submission failed"),
    ):
        wrapped = flow(lambda: None)
        wrapped.start(stack="prod")

    assert client_mock.activate_stack.call_args_list == [
        call("prod"),
        call(old_stack_id),
    ]


def test_start_allows_submission_when_other_compilation_context_is_active() -> None:
    run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock(return_value=run)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "zenml.pipelines.compilation_context.PipelineCompilationContext.is_active",
            return_value=True,
        ),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
    ):
        wrapped = flow(lambda: None)
        handle = wrapped.start()

    assert isinstance(handle, FlowHandle)


def test_start_resolves_config_and_persists_frozen_spec() -> None:
    """start should resolve execution config and persist the frozen spec."""
    run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock(return_value=run)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    frozen_spec = object()

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="resolved-stack", cache=False),
        ) as resolve_execution_config_mock,
        patch("kitaru.flow.resolve_connection_config") as resolve_connection_mock,
        patch(
            "kitaru.flow.build_frozen_execution_spec",
            return_value=frozen_spec,
        ) as build_frozen_spec_mock,
        patch(
            "kitaru.flow.persist_frozen_execution_spec"
        ) as persist_frozen_execution_spec_mock,
        patch("kitaru.flow.Client") as client_cls,
    ):
        client_cls.return_value.active_stack_model.id = "old-stack-id"
        wrapped = flow(stack="decorator-stack", cache=True, retries=2)(lambda x: x)
        wrapped.start("payload", stack="invocation-stack", retries=3)

    resolve_execution_config_mock.assert_called_once()
    resolve_connection_mock.assert_called_once()

    resolve_call = resolve_execution_config_mock.call_args.kwargs
    decorator_overrides = resolve_call["decorator_overrides"]
    invocation_overrides = resolve_call["invocation_overrides"]
    assert decorator_overrides.stack == "decorator-stack"
    assert decorator_overrides.cache is True
    assert decorator_overrides.retries == 2
    assert invocation_overrides.stack == "invocation-stack"
    assert invocation_overrides.retries == 3

    build_frozen_spec_mock.assert_called_once()
    persist_frozen_execution_spec_mock.assert_called_once_with(
        run_id=run.id,
        frozen_execution_spec=frozen_spec,
    )
    configured_pipeline.assert_called_once_with("payload")


def test_flow_handle_wait_polls_until_complete() -> None:
    run_id = uuid4()
    initial = _DummyRun(status=ExecutionStatus.RUNNING, run_id=run_id)
    finished = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        run_id=run_id,
        outputs=[("step", "output", 42)],
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.side_effect = [initial, finished]

    handle = FlowHandle(_as_pipeline_run(initial))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        patch("kitaru.flow.time.sleep") as sleep_mock,
    ):
        result = handle.wait()

    assert result == 42
    sleep_mock.assert_called_once_with(1)


def test_flow_handle_get_raises_when_still_running() -> None:
    running = _DummyRun(status=ExecutionStatus.RUNNING)
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = running

    handle = FlowHandle(_as_pipeline_run(running))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        pytest.raises(KitaruStateError, match="still running") as exc_info,
    ):
        handle.get()

    assert exc_info.value.args


def test_flow_handle_get_raises_with_failure_context() -> None:
    failed = _DummyRun(
        status=ExecutionStatus.FAILED,
        status_reason="upstream failure",
        traceback="Traceback\nValueError: boom",
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = failed

    handle = FlowHandle(_as_pipeline_run(failed))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        pytest.raises(KitaruUserCodeError, match="upstream failure") as exc_info,
    ):
        handle.get()

    assert exc_info.value.exec_id == str(failed.id)
    assert exc_info.value.status == failed.status.value
    assert exc_info.value.failure_origin == FailureOrigin.USER_CODE


def test_flow_handle_get_returns_tuple_for_multiple_outputs() -> None:
    completed = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            ("step_a", "output", "a"),
            ("step_b", "output", "b"),
        ],
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    handle = FlowHandle(_as_pipeline_run(completed))
    with patch("kitaru.flow.Client", return_value=client_mock):
        result = handle.get()

    assert result == ("a", "b")


def test_flow_handle_get_returns_none_when_no_outputs() -> None:
    completed = _DummyRun(status=ExecutionStatus.COMPLETED)
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    handle = FlowHandle(_as_pipeline_run(completed))
    with patch("kitaru.flow.Client", return_value=client_mock):
        result = handle.get()

    assert result is None


def test_flow_handle_get_falls_back_to_terminal_step_outputs() -> None:
    completed = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("final_step", "output", "done")],
    )
    completed.snapshot.pipeline_spec.outputs = []

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    handle = FlowHandle(_as_pipeline_run(completed))
    with patch("kitaru.flow.Client", return_value=client_mock):
        result = handle.get()

    assert result == "done"


def test_flow_handle_get_raises_on_ambiguous_terminal_fallback() -> None:
    completed = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            ("final_a", "output", "a"),
            ("final_b", "output", "b"),
        ],
    )
    completed.snapshot.pipeline_spec.outputs = []

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    handle = FlowHandle(_as_pipeline_run(completed))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        pytest.raises(RuntimeError, match="fallback extraction is ambiguous"),
    ):
        handle.get()


def test_flow_handle_get_raises_when_step_metadata_is_missing() -> None:
    completed = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("missing_step", "output", "value")],
    )
    completed.steps = {}

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    handle = FlowHandle(_as_pipeline_run(completed))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        pytest.raises(RuntimeError, match="missing step output metadata"),
    ):
        handle.get()


def test_flow_handle_get_raises_when_output_artifact_is_missing() -> None:
    completed = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("step", "output", "value")],
    )
    completed.steps["step"].regular_outputs = {}

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    handle = FlowHandle(_as_pipeline_run(completed))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        pytest.raises(RuntimeError, match="missing output 'output'"),
    ):
        handle.get()


def test_flow_runtime_scope_sets_execution_id_from_zenml_run_context() -> None:
    def _user_flow() -> str:
        assert _is_inside_flow()
        current = _get_current_flow()
        assert current is not None
        assert current.name == "_user_flow"
        assert current.execution_id == "exec-123"
        assert _get_current_execution_id() == "exec-123"
        return "ok"

    wrapped = _wrap_flow_entrypoint(_user_flow)

    with patch(
        "kitaru.runtime.DynamicPipelineRunContext.get",
        return_value=SimpleNamespace(run=SimpleNamespace(id="exec-123")),
    ):
        result = wrapped()

    assert result == "ok"
    assert not _is_inside_flow()
    assert _get_current_flow() is None


def test_flow_runtime_scope_keeps_execution_id_none_without_zenml_context() -> None:
    def _user_flow() -> None:
        assert _is_inside_flow()
        current = _get_current_flow()
        assert current is not None
        assert current.execution_id is None
        assert _get_current_execution_id() is None

    wrapped = _wrap_flow_entrypoint(_user_flow)

    with (
        patch("kitaru.runtime.StepContext.get", return_value=None),
        patch("kitaru.runtime.DynamicPipelineRunContext.get", return_value=None),
    ):
        wrapped()

    assert not _is_inside_flow()
    assert _get_current_flow() is None
    assert _get_current_execution_id() is None


def test_execution_id_lookup_requires_active_kitaru_scope() -> None:
    with patch(
        "kitaru.runtime.DynamicPipelineRunContext.get",
        return_value=SimpleNamespace(run=SimpleNamespace(id="exec-raw-context")),
    ):
        assert _get_current_execution_id() is None
