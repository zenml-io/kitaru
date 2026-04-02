"""Tests for the `@flow` implementation."""

from __future__ import annotations

import sys
import threading
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock, call, patch
from uuid import UUID, uuid4

import pytest
from zenml.config.docker_settings import DockerSettings
from zenml.enums import ExecutionStatus
from zenml.models import PipelineRunResponse

from kitaru.analytics import AnalyticsEvent
from kitaru.config import (
    KITARU_MODEL_REGISTRY_ENV,
    ImageSettings,
    ModelAliasConfig,
    ModelRegistryConfig,
    ResolvedExecutionConfig,
)
from kitaru.errors import (
    FailureOrigin,
    KitaruBackendError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
    KitaruUserCodeError,
)
from kitaru.flow import (
    FlowHandle,
    _inject_model_registry_env,
    _temporary_active_stack,
    _wrap_flow_entrypoint,
    flow,
)
from kitaru.replay import ReplayPlan
from kitaru.runtime import _get_current_execution_id, _get_current_flow, _is_inside_flow


def _as_pipeline_run(run: _DummyRun) -> PipelineRunResponse:
    return cast(PipelineRunResponse, run)


def _resolved_execution(
    *,
    stack: str | None = None,
    cache: bool = True,
    retries: int = 0,
) -> ResolvedExecutionConfig:
    return ResolvedExecutionConfig(
        stack=stack,
        image=None,
        cache=cache,
        retries=retries,
    )


def _empty_registry_payload() -> str:
    """Return the serialized empty transported registry payload."""
    return ModelRegistryConfig().model_dump_json(exclude_none=True)


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


def test_inject_model_registry_env_adds_registry_to_empty_image() -> None:
    """Submission should transport even an empty registry snapshot."""
    image, registry, did_inject = _inject_model_registry_env(
        None,
        read_local_registry=ModelRegistryConfig,
    )

    assert did_inject is True
    assert registry == ModelRegistryConfig()
    assert image.environment == {KITARU_MODEL_REGISTRY_ENV: _empty_registry_payload()}


def test_inject_model_registry_env_preserves_existing_override() -> None:
    """A preconfigured image env registry should win over local config."""
    transported_registry = ModelRegistryConfig(
        aliases={
            "fast": ModelAliasConfig(
                model="openai/gpt-4.1-mini",
                secret="remote-secret",
            )
        },
        default="fast",
    )

    local_registry_reader = MagicMock(
        return_value=ModelRegistryConfig(
            aliases={"fast": ModelAliasConfig(model="openai/gpt-4o-mini")}
        )
    )
    image, registry, did_inject = _inject_model_registry_env(
        ImageSettings(
            environment={
                KITARU_MODEL_REGISTRY_ENV: transported_registry.model_dump_json(
                    exclude_none=True
                ),
                "OPENAI_API_KEY": "already-there",
            }
        ),
        read_local_registry=local_registry_reader,
    )

    assert did_inject is False
    assert registry == transported_registry
    local_registry_reader.assert_not_called()
    assert image.environment == {
        KITARU_MODEL_REGISTRY_ENV: transported_registry.model_dump_json(
            exclude_none=True
        ),
        "OPENAI_API_KEY": "already-there",
    }


def test_inject_model_registry_env_replaces_blank_override() -> None:
    """Blank image env values should be treated as missing and replaced."""
    image, registry, did_inject = _inject_model_registry_env(
        ImageSettings(environment={KITARU_MODEL_REGISTRY_ENV: "   "}),
        read_local_registry=lambda: ModelRegistryConfig(
            aliases={"fast": ModelAliasConfig(model="openai/gpt-4o-mini")},
            default="fast",
        ),
    )

    assert did_inject is True
    assert registry.default == "fast"
    assert image.environment == {
        KITARU_MODEL_REGISTRY_ENV: registry.model_dump_json(exclude_none=True)
    }


def test_inject_model_registry_env_rejects_invalid_override() -> None:
    """Invalid preconfigured transport payloads should fail before submission."""
    with pytest.raises(KitaruUsageError, match=KITARU_MODEL_REGISTRY_ENV):
        _inject_model_registry_env(
            ImageSettings(environment={KITARU_MODEL_REGISTRY_ENV: "not-json"}),
            read_local_registry=ModelRegistryConfig,
        )


def test_flow_decorator_creates_wrapper_with_run() -> None:
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
        handle = wrapped.run(123)

    pipeline_mock.assert_called_once_with(dynamic=True, name="_lambda_")
    assert hasattr(wrapped, "run")
    assert not hasattr(wrapped, "deploy")
    assert not hasattr(wrapped, "start")
    assert isinstance(handle, FlowHandle)
    call_kwargs = base_pipeline.with_options.call_args
    assert call_kwargs == call(
        enable_cache=True,
        retry=None,
        settings={
            "docker": DockerSettings(
                requirements=["kitaru"],
                environment={KITARU_MODEL_REGISTRY_ENV: _empty_registry_payload()},
            )
        },
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


def test_direct_call_raises_usage_error() -> None:
    zenml_decorator = MagicMock(return_value=MagicMock())

    with patch("kitaru.flow.pipeline", return_value=zenml_decorator):
        wrapped = flow(lambda x: x)

    with pytest.raises(KitaruUsageError, match="Direct flow calls are not supported"):
        wrapped("input")


def test_run_restores_previous_stack_if_submission_fails() -> None:
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
        wrapped.run(stack="prod")

    assert client_mock.activate_stack.call_args_list == [
        call("prod"),
        call(old_stack_id),
    ]


def test_run_allows_submission_when_other_compilation_context_is_active() -> None:
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
        handle = wrapped.run()

    assert isinstance(handle, FlowHandle)


def test_run_resolves_config_and_persists_frozen_spec() -> None:
    """run should resolve execution config and persist the frozen spec."""
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
        wrapped.run("payload", stack="invocation-stack", retries=3)

    resolve_execution_config_mock.assert_called_once()
    resolve_connection_mock.assert_called_once_with(validate_for_use=True)

    resolve_call = resolve_execution_config_mock.call_args.kwargs
    decorator_overrides = resolve_call["decorator_overrides"]
    invocation_overrides = resolve_call["invocation_overrides"]
    assert decorator_overrides.stack == "decorator-stack"
    assert decorator_overrides.cache is True
    assert decorator_overrides.retries == 2
    assert invocation_overrides.stack == "invocation-stack"
    assert invocation_overrides.retries == 3

    build_frozen_spec_mock.assert_called_once()
    assert (
        build_frozen_spec_mock.call_args.kwargs["model_registry"]
        == ModelRegistryConfig()
    )
    persist_frozen_execution_spec_mock.assert_called_once_with(
        run_id=run.id,
        frozen_execution_spec=frozen_spec,
    )
    configured_pipeline.assert_called_once_with("payload")


def test_run_resolves_config_with_decorator_stack_when_invocation_omits_it() -> None:
    """Decorator stack defaults should flow into config resolution unchanged."""
    run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock(return_value=run)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="decorator-stack"),
        ) as resolve_execution_config_mock,
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch("kitaru.flow.Client") as client_cls,
    ):
        client_cls.return_value.active_stack_model.id = "old-stack-id"
        wrapped = flow(stack="decorator-stack")(lambda: None)
        wrapped.run()

    resolve_call = resolve_execution_config_mock.call_args.kwargs
    assert resolve_call["decorator_overrides"].stack == "decorator-stack"
    assert resolve_call["invocation_overrides"].stack is None


def test_replay_submits_pipeline_replay_and_persists_frozen_spec() -> None:
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    replayed_run = _DummyRun(status=ExecutionStatus.RUNNING)

    configured_pipeline = MagicMock()
    configured_pipeline.replay.return_value = replayed_run

    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    replay_plan = ReplayPlan(
        original_run_id=str(source_run.id),
        steps_to_skip={"fetch"},
        input_overrides={"topic": "new topic"},
        step_input_overrides={"write": {"research": "edited"}},
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client") as client_cls,
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch(
            "kitaru.flow.resolve_connection_config", return_value=object()
        ) as resolve_connection_mock,
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec") as persist_mock,
        patch("kitaru.flow.build_replay_plan", return_value=replay_plan),
    ):
        client_instance = client_cls.return_value
        client_instance.active_stack_model.id = "old-stack-id"
        client_instance.get_pipeline_run.return_value = source_run

        wrapped = flow(lambda topic: topic)
        handle = wrapped.replay(
            str(source_run.id),
            from_="write",
            topic="new topic",
            overrides={"checkpoint.research": "edited"},
        )

    assert isinstance(handle, FlowHandle)
    configured_pipeline.replay.assert_called_once_with(
        pipeline_run=source_run.id,
        skip={"fetch"},
        skip_successful_steps=False,
        input_overrides={"topic": "new topic"},
        step_input_overrides={"write": {"research": "edited"}},
    )
    resolve_connection_mock.assert_called_once_with(validate_for_use=True)
    persist_mock.assert_called_once()
    assert persist_mock.call_args.kwargs["run_id"] == replayed_run.id
    build_frozen_spec_call = base_pipeline.with_options.call_args
    assert build_frozen_spec_call.kwargs["settings"] == {
        "docker": DockerSettings(
            requirements=["kitaru"],
            environment={KITARU_MODEL_REGISTRY_ENV: _empty_registry_payload()},
        )
    }


def test_replay_resolves_config_with_invocation_stack_override() -> None:
    """Replay should pass invocation stack overrides through the shared resolver."""
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    replayed_run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock()
    configured_pipeline.replay.return_value = replayed_run
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client") as client_cls,
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="invocation-stack"),
        ) as resolve_execution_config_mock,
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch(
            "kitaru.flow.build_replay_plan",
            return_value=ReplayPlan(
                original_run_id=str(source_run.id),
                steps_to_skip=set(),
                input_overrides={},
                step_input_overrides={},
            ),
        ),
    ):
        client_instance = client_cls.return_value
        client_instance.active_stack_model.id = "old-stack-id"
        client_instance.get_pipeline_run.return_value = source_run

        wrapped = flow(stack="decorator-stack")(lambda topic: topic)
        wrapped.replay(str(source_run.id), from_="write", stack="invocation-stack")

    resolve_call = resolve_execution_config_mock.call_args.kwargs
    assert resolve_call["decorator_overrides"].stack == "decorator-stack"
    assert resolve_call["invocation_overrides"].stack == "invocation-stack"


def test_replay_validates_connection_before_loading_source_run() -> None:
    """Replay should fail before touching ZenML if env project validation fails."""
    base_pipeline = MagicMock()
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_connection_config",
            side_effect=KitaruUsageError("Set KITARU_PROJECT"),
        ) as resolve_connection_mock,
        patch("kitaru.flow.Client") as client_cls,
        pytest.raises(KitaruUsageError, match="KITARU_PROJECT"),
    ):
        wrapped = flow(lambda topic: topic)
        wrapped.replay("run-123", from_="write")

    resolve_connection_mock.assert_called_once_with(validate_for_use=True)
    client_cls.return_value.get_pipeline_run.assert_not_called()


def test_temporary_active_stack_serializes_concurrent_bindings() -> None:
    """Concurrent temporary stack bindings should not interleave within one process."""
    first_entered = threading.Event()
    second_attempted = threading.Event()
    release_first = threading.Event()
    second_client_created = threading.Event()
    activation_order: list[str] = []
    thread_errors: list[Exception] = []

    client_one = MagicMock()
    client_one.active_stack_model = SimpleNamespace(id="old-stack-1")

    client_two = MagicMock()
    client_two.active_stack_model = SimpleNamespace(id="old-stack-2")

    def _activate_one(stack_name_or_id: str) -> None:
        activation_order.append(stack_name_or_id)
        if stack_name_or_id == "stack-1":
            first_entered.set()
            assert release_first.wait(timeout=1), (
                "First stack binding was not released."
            )

    def _activate_two(stack_name_or_id: str) -> None:
        activation_order.append(stack_name_or_id)

    client_one.activate_stack.side_effect = _activate_one
    client_two.activate_stack.side_effect = _activate_two

    def _client_factory() -> MagicMock:
        if not first_entered.is_set():
            return client_one
        second_client_created.set()
        return client_two

    def _worker(
        stack_name_or_id: str, *, mark_attempt: threading.Event | None = None
    ) -> None:
        try:
            if mark_attempt is not None:
                mark_attempt.set()
            with _temporary_active_stack(stack_name_or_id):
                return
        except Exception as exc:  # pragma: no cover - propagated via assertion below
            thread_errors.append(exc)

    with patch("kitaru.flow.Client", side_effect=_client_factory):
        first_thread = threading.Thread(target=_worker, args=("stack-1",))
        second_thread = threading.Thread(
            target=_worker,
            args=("stack-2",),
            kwargs={"mark_attempt": second_attempted},
        )

        first_thread.start()
        assert first_entered.wait(timeout=1), "First stack binding never entered."

        second_thread.start()
        assert second_attempted.wait(timeout=1), "Second stack binding never attempted."
        assert not second_client_created.wait(timeout=0.1)

        release_first.set()
        first_thread.join(timeout=1)
        second_thread.join(timeout=1)

    assert not thread_errors
    assert activation_order == ["stack-1", "old-stack-1", "stack-2", "old-stack-2"]


def test_temporary_active_stack_serializes_default_stack_reads() -> None:
    """A submission without an explicit stack should still wait for a temporary bind."""
    first_entered = threading.Event()
    release_first = threading.Event()
    second_attempted = threading.Event()
    second_entered = threading.Event()
    thread_errors: list[Exception] = []

    client = MagicMock()
    client.active_stack_model = SimpleNamespace(id="old-stack-id")

    def _activate(stack_name_or_id: str) -> None:
        if stack_name_or_id == "stack-1":
            first_entered.set()
            assert release_first.wait(timeout=1), (
                "First stack binding was not released."
            )

    client.activate_stack.side_effect = _activate

    def _worker_explicit() -> None:
        try:
            with _temporary_active_stack("stack-1"):
                return
        except Exception as exc:  # pragma: no cover - propagated via assertion below
            thread_errors.append(exc)

    def _worker_default() -> None:
        try:
            second_attempted.set()
            with _temporary_active_stack(None):
                second_entered.set()
        except Exception as exc:  # pragma: no cover - propagated via assertion below
            thread_errors.append(exc)

    with patch("kitaru.flow.Client", return_value=client):
        first_thread = threading.Thread(target=_worker_explicit)
        second_thread = threading.Thread(target=_worker_default)

        first_thread.start()
        assert first_entered.wait(timeout=1), "First stack binding never entered."

        second_thread.start()
        assert second_attempted.wait(timeout=1), "Second stack binding never attempted."
        assert not second_entered.wait(timeout=0.1)

        release_first.set()
        first_thread.join(timeout=1)
        second_thread.join(timeout=1)

    assert not thread_errors
    assert second_entered.is_set()


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
        pytest.raises(KitaruRuntimeError, match="fallback extraction is ambiguous"),
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
        pytest.raises(KitaruRuntimeError, match="missing step output metadata"),
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
        pytest.raises(KitaruRuntimeError, match="missing output 'output'"),
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


# ── Analytics instrumentation tests ──────────────────────────────────────────


def test_submit_emits_flow_submitted_event() -> None:
    """_submit should emit FLOW_SUBMITTED after successful run creation."""
    run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock(return_value=run)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch("kitaru.flow.track") as track_mock,
    ):
        wrapped = flow(lambda x: x)
        wrapped.run(123)

    track_mock.assert_called_once_with(
        AnalyticsEvent.FLOW_SUBMITTED,
        {"flow_name": "<lambda>", "execution_id": str(run.id)},
    )


def test_submit_does_not_emit_when_run_is_none() -> None:
    """FLOW_SUBMITTED should NOT fire when the pipeline returns None."""
    configured_pipeline = MagicMock(return_value=None)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch("kitaru.flow.track") as track_mock,
        pytest.raises(KitaruRuntimeError, match="did not produce"),
    ):
        wrapped = flow(lambda: None)
        wrapped.run()

    track_mock.assert_not_called()


def test_replay_success_emits_requested_and_replayed_events() -> None:
    """Successful replay should emit REPLAY_REQUESTED then FLOW_REPLAYED."""
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    replayed_run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock()
    configured_pipeline.replay.return_value = replayed_run
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client") as client_cls,
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch(
            "kitaru.flow.build_replay_plan",
            return_value=ReplayPlan(
                original_run_id=str(source_run.id),
                steps_to_skip=set(),
                input_overrides={},
                step_input_overrides={},
            ),
        ),
        patch("kitaru.flow.track") as track_mock,
    ):
        client_cls.return_value.get_pipeline_run.return_value = source_run
        wrapped = flow(lambda topic: topic)
        wrapped.replay(str(source_run.id), from_="write")

    assert track_mock.call_count == 2
    requested_call = track_mock.call_args_list[0]
    assert requested_call.args[0] == AnalyticsEvent.REPLAY_REQUESTED
    assert requested_call.args[1]["replay_path"] == "flow_wrapper"
    assert requested_call.args[1]["from_checkpoint"] == "write"

    replayed_call = track_mock.call_args_list[1]
    assert replayed_call.args[0] == AnalyticsEvent.FLOW_REPLAYED
    assert replayed_call.args[1]["execution_id"] == str(replayed_run.id)


def test_replay_failure_emits_requested_then_failed_events() -> None:
    """Failed replay should emit REPLAY_REQUESTED then REPLAY_FAILED."""
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    configured_pipeline = MagicMock()
    configured_pipeline.replay.side_effect = RuntimeError("backend crash")
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client") as client_cls,
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch(
            "kitaru.flow.build_replay_plan",
            return_value=ReplayPlan(
                original_run_id=str(source_run.id),
                steps_to_skip=set(),
                input_overrides={},
                step_input_overrides={},
            ),
        ),
        patch("kitaru.flow.track") as track_mock,
        pytest.raises(KitaruBackendError, match="backend crash"),
    ):
        client_cls.return_value.get_pipeline_run.return_value = source_run
        wrapped = flow(lambda topic: topic)
        wrapped.replay(str(source_run.id), from_="write")

    assert track_mock.call_count == 2
    requested_call = track_mock.call_args_list[0]
    assert requested_call.args[0] == AnalyticsEvent.REPLAY_REQUESTED

    failed_call = track_mock.call_args_list[1]
    assert failed_call.args[0] == AnalyticsEvent.REPLAY_FAILED
    assert failed_call.args[1]["error_type"] == "RuntimeError"
    assert "failure_origin" in failed_call.args[1]


def test_replay_none_run_emits_replay_failed_with_runtime_origin() -> None:
    """Replay returning None should emit REPLAY_FAILED with runtime origin."""
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    configured_pipeline = MagicMock()
    configured_pipeline.replay.return_value = None
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client") as client_cls,
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch(
            "kitaru.flow.build_replay_plan",
            return_value=ReplayPlan(
                original_run_id=str(source_run.id),
                steps_to_skip=set(),
                input_overrides={},
                step_input_overrides={},
            ),
        ),
        patch("kitaru.flow.track") as track_mock,
        pytest.raises(KitaruRuntimeError, match="did not produce"),
    ):
        client_cls.return_value.get_pipeline_run.return_value = source_run
        wrapped = flow(lambda topic: topic)
        wrapped.replay(str(source_run.id), from_="write")

    assert track_mock.call_count == 2
    failed_call = track_mock.call_args_list[1]
    assert failed_call.args[0] == AnalyticsEvent.REPLAY_FAILED
    assert failed_call.args[1]["error_type"] == "KitaruRuntimeError"
    assert failed_call.args[1]["failure_origin"] == FailureOrigin.RUNTIME.value
