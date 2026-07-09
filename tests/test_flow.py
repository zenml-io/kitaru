"""Tests for the `@flow` implementation."""

from __future__ import annotations

import os
import sys
import threading
from collections import namedtuple
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, call, patch
from uuid import uuid4

import pytest
from zenml.config.docker_settings import DockerSettings
from zenml.constants import ENV_ZENML_ACTIVE_STACK_ID
from zenml.enums import ArtifactType, ExecutionStatus
from zenml.execution.pipeline.dynamic.outputs import OutputArtifact
from zenml.models import PipelineRunResponse

import kitaru
from kitaru._client._models import ExecutionStatus as KitaruExecutionStatus
from kitaru._config._core import ExecutionStackSource
from kitaru._env import ZENML_ACTIVE_PROJECT_ID_ENV
from kitaru._run_identity import extract_run_project_identity
from kitaru._ui_urls import UiUrlContext
from kitaru.analytics import AnalyticsEvent
from kitaru.checkpoint import checkpoint
from kitaru.config import (
    KITARU_MODEL_REGISTRY_ENV,
    ImageSettings,
    ModelAliasConfig,
    ModelRegistryConfig,
    ResolvedExecutionConfig,
)
from kitaru.errors import (
    FailureOrigin,
    KitaruAmbiguousFlowResultError,
    KitaruBackendError,
    KitaruExecutionError,
    KitaruRuntimeError,
    KitaruStackIntegrationDependencyError,
    KitaruStateError,
    KitaruUsageError,
    KitaruUserCodeError,
    build_recovery_command,
    execution_error_from_failure,
    format_recovery_hint,
)
from kitaru.flow import (
    _FLOW_RESULT_ARTIFACT_NAME,
    _FLOW_RESULT_NONE_METADATA_KEY,
    _FLOW_RESULT_REF_METADATA_KEY,
    _FLOW_RESULT_ROLE_METADATA_KEY,
    _FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME,
    _FLOW_RESULT_TUPLE_METADATA_MARKER,
    _FLOW_RESULT_TUPLE_METADATA_ROLE,
    FlowHandle,
    _apply_active_stack_image_defaults,
    _build_kitaru_execution_url,
    _build_pipeline_options,
    _checkpoint_count_from_run,
    _coerce_flow_return_for_zenml,
    _duration_metadata_from_run,
    _extract_flow_result,
    _extract_run_pipeline_id,
    _guard_implicit_active_stack_fallback,
    _inject_model_registry_env,
    _is_flow_result_candidate_step,
    _is_multiple_terminal_steps_output_error,
    _suspend_flow_return_coercion,
    _temporary_active_project,
    _temporary_active_stack,
    _wrap_flow_entrypoint,
    flow,
)
from kitaru.inspection import ActiveConfigSelectionProvenance
from kitaru.replay import ReplayPlan
from kitaru.replay_context import (
    KITARU_REPLAY_CONTEXT_ENV,
    ReplayRuntimeContext,
    get_replay_runtime_context,
)
from kitaru.runtime import _get_current_execution_id, _get_current_flow, _is_inside_flow


def _as_pipeline_run(run: _DummyRun) -> PipelineRunResponse:
    return cast(PipelineRunResponse, run)


def _resolved_execution(
    *,
    stack: str | None = None,
    stack_source: ExecutionStackSource | None = None,
    cache: bool | None = None,
    retries: int = 0,
    image: ImageSettings | None = None,
) -> ResolvedExecutionConfig:
    return ResolvedExecutionConfig(
        stack=stack,
        stack_source=stack_source,
        image=image,
        cache=cache,
        retries=retries,
    )


def _empty_registry_payload() -> str:
    """Return the serialized empty transported registry payload."""
    return ModelRegistryConfig().model_dump_json(exclude_none=True)


class _ClientWithMissingStackDependency:
    def __init__(self, *, old_stack_id: object, stack_name: str = "prod") -> None:
        self.old_stack_model = SimpleNamespace(id=old_stack_id, name="old")
        self.selected_stack_model = SimpleNamespace(id=stack_name, name=stack_name)
        self.active_stack_model = self.old_stack_model
        self.zen_store = object()
        self.activate_stack = MagicMock(side_effect=self._activate_stack)
        self.get_pipeline_run = MagicMock()

    def _activate_stack(self, stack_name_or_id: object) -> None:
        if stack_name_or_id == self.selected_stack_model.name:
            self.active_stack_model = self.selected_stack_model
        elif stack_name_or_id == self.old_stack_model.id:
            self.active_stack_model = self.old_stack_model

    @property
    def active_stack(self) -> object:
        if self.active_stack_model is not self.selected_stack_model:
            raise AssertionError("active stack must be selected before hydration")
        raise ImportError(
            "Install the missing integration with "
            "`zenml integration install s3`.\n"
            "Export stack requirements with "
            "`zenml stack export-requirements 'prod' "
            "-o stack-requirements.txt`."
        )


class _EnvPoisonedActiveStackClient:
    """Client double that fails active-stack reads while a stale env override exists."""

    def __init__(self) -> None:
        self._active_stack_id = "saved-stack-id"
        self.zen_store = object()
        self.activate_stack = MagicMock(side_effect=self._activate_stack)

    @property
    def active_stack_model(self) -> SimpleNamespace:
        if os.environ.get(ENV_ZENML_ACTIVE_STACK_ID) == "deleted-stack-id":
            raise RuntimeError("Stack with id=deleted-stack-id does not exist")
        name = "prod" if self._active_stack_id == "prod-stack-id" else "saved"
        return SimpleNamespace(id=self._active_stack_id, name=name)

    @property
    def active_stack(self) -> object:
        if self._active_stack_id != "prod-stack-id":
            raise AssertionError("requested stack must be active before hydration")
        return object()

    def _activate_stack(self, stack_name_or_id: object) -> None:
        if stack_name_or_id == "prod":
            self._active_stack_id = "prod-stack-id"
        elif stack_name_or_id == "saved-stack-id":
            self._active_stack_id = "saved-stack-id"
        else:
            self._active_stack_id = str(stack_name_or_id)


@dataclass(frozen=True)
class _DummyOutput:
    step_name: str
    output_name: str
    value: object
    artifact_name: str | None = None
    run_metadata: Mapping[str, object] | None = None
    config_extra: Mapping[str, object] | None = None
    spec_extra: Mapping[str, object] | None = None
    upstream_steps: list[str] | None = None


@dataclass(frozen=True)
class _DummyRunOutput:
    output_name: str
    value: object
    artifact_name: str | None = None
    run_metadata: Mapping[str, object] | None = None


class _DummyArtifact:
    def __init__(
        self,
        value: object,
        *,
        name: str = "output",
        run_metadata: dict[str, object] | None = None,
    ) -> None:
        self._value = value
        self.name = name
        self.run_metadata = run_metadata or {}

    def load(self) -> object:
        return self._value


class _DummyRun:
    def __init__(
        self,
        *,
        status: ExecutionStatus,
        outputs: list[tuple[str, str, object] | _DummyOutput] | None = None,
        run_outputs: list[tuple[str, object] | _DummyRunOutput] | None = None,
        run_id: object | None = None,
        pipeline_id: object | None = None,
        pipeline: object | None = None,
        snapshot_pipeline_id: object | None = None,
        snapshot_pipeline: object | None = None,
        source_snapshot: object | None = None,
        resources: object | None = None,
        project: object | None = None,
        status_reason: str | None = None,
        traceback: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        parameters: Mapping[str, object] | None = None,
    ) -> None:
        self.id = run_id or uuid4()
        self.pipeline_id = pipeline_id
        self.pipeline = pipeline
        self.project = project
        self.source_snapshot = source_snapshot
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.status_reason = status_reason
        self.run_metadata: dict[str, object] = {}
        if parameters is not None:
            self.config = SimpleNamespace(parameters=dict(parameters))
        self.exception_info = (
            SimpleNamespace(traceback=traceback) if traceback else None
        )

        self.outputs: dict[str, _DummyArtifact] = {}
        for output in run_outputs or []:
            if isinstance(output, _DummyRunOutput):
                output_name = output.output_name
                value = output.value
                artifact_name = output.artifact_name or output_name
                run_metadata = dict(output.run_metadata or {})
            else:
                output_name, value = output
                artifact_name = output_name
                run_metadata = None

            self.outputs[output_name] = _DummyArtifact(
                value,
                name=artifact_name,
                run_metadata=run_metadata,
            )

        outputs = outputs or []
        output_specs: list[SimpleNamespace] = []
        step_outputs: dict[str, dict[str, _DummyArtifact]] = {}
        step_config_extras: dict[str, Mapping[str, object] | None] = {}
        step_spec_extras: dict[str, Mapping[str, object] | None] = {}
        step_upstream_steps: dict[str, list[str]] = {}
        for output in outputs:
            if isinstance(output, _DummyOutput):
                step_name = output.step_name
                output_name = output.output_name
                value = output.value
                artifact_name = output.artifact_name or output_name
                run_metadata = dict(output.run_metadata or {})
                step_config_extras[step_name] = output.config_extra
                step_spec_extras[step_name] = output.spec_extra
                step_upstream_steps[step_name] = list(output.upstream_steps or [])
            else:
                step_name, output_name, value = output
                artifact_name = output_name
                run_metadata = None

            output_specs.append(
                SimpleNamespace(step_name=step_name, output_name=output_name)
            )
            step_outputs.setdefault(step_name, {})[output_name] = _DummyArtifact(
                value,
                name=artifact_name,
                run_metadata=run_metadata,
            )

        self.snapshot = SimpleNamespace(
            pipeline_spec=SimpleNamespace(outputs=output_specs),
            pipeline_id=snapshot_pipeline_id,
            pipeline=snapshot_pipeline,
        )
        self.resources = resources
        self.steps = {
            step_name: SimpleNamespace(
                regular_outputs=regular_outputs,
                config=SimpleNamespace(extra=step_config_extras.get(step_name)),
                spec=SimpleNamespace(
                    extra=step_spec_extras.get(step_name),
                    upstream_steps=step_upstream_steps.get(step_name, []),
                ),
            )
            for step_name, regular_outputs in step_outputs.items()
        }

    def get_hydrated_version(self) -> _DummyRun:
        return self

    def get_resources(self) -> object:
        return self.resources or SimpleNamespace(active_wait_condition=None)


def _dummy_wait_condition(
    *,
    name: str = "approve_release",
    question: str = "Approve release?",
    data_schema: Mapping[str, object] | None = None,
    metadata: Mapping[str, object] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        name=name,
        question=question,
        data_schema=data_schema,
        run_metadata=dict(metadata or {}),
        created=None,
    )


def _add_linked_flow_result_metadata(
    run: _DummyRun,
    artifact_id: str = "result-art-id",
) -> None:
    run.run_metadata = {_FLOW_RESULT_REF_METADATA_KEY: artifact_id}


def _add_flow_result_none_metadata(run: _DummyRun) -> None:
    run.run_metadata = {_FLOW_RESULT_NONE_METADATA_KEY: True}


def _linked_flow_result_client(value: object) -> MagicMock:
    client_mock = MagicMock()
    client_mock.get_artifact_version.return_value = SimpleNamespace(load=lambda: value)
    return client_mock


def _assert_linked_flow_result_loaded(
    client_mock: MagicMock,
    artifact_id: str = "result-art-id",
) -> None:
    client_mock.get_artifact_version.assert_called_once_with(artifact_id, project=None)


def _zero_output_terminal_run() -> _DummyRun:
    run = _DummyRun(status=ExecutionStatus.COMPLETED)
    run.snapshot.pipeline_spec.outputs = []
    run.steps = {
        "finalize": SimpleNamespace(
            regular_outputs={},
            config=SimpleNamespace(extra=None),
            spec=SimpleNamespace(extra=None, upstream_steps=[]),
        )
    }
    return run


def _stale_default_stack_provenance() -> ActiveConfigSelectionProvenance:
    return ActiveConfigSelectionProvenance(
        resource="active_stack",
        effective_source="repo-local config",
        effective_source_detail="/work/repo/.kitaru/config.yaml",
        effective_id="stale-stack-id",
        repository_config_path="/work/repo/.kitaru/config.yaml",
        repository_id="stale-stack-id",
    )


def test_extract_run_project_identity_does_not_touch_lazy_project() -> None:
    class LazyProjectRun:
        id = "run-123"
        project_id = "project-direct"
        resources = None

        @property
        def project(self) -> object:
            raise AssertionError("default extraction must not lazy-load project")

        def get_body(self) -> SimpleNamespace:
            return SimpleNamespace(project_id="project-body")

    identity = extract_run_project_identity(LazyProjectRun())

    assert identity.project_id == "project-direct"
    assert identity.project_name is None
    assert identity.name_or_id == "project-direct"


def test_extract_run_project_identity_can_lazy_load_for_one_off_logging() -> None:
    class LazyProjectRun:
        id = "run-123"
        project_id = "project-direct"
        resources = None

        @property
        def project(self) -> SimpleNamespace:
            return SimpleNamespace(name="default")

        def get_body(self) -> SimpleNamespace:
            return SimpleNamespace(project_id="project-body")

    identity = extract_run_project_identity(
        LazyProjectRun(),
        allow_lazy_project_lookup=True,
    )

    assert identity.project_id == "project-direct"
    assert identity.project_name == "default"
    assert identity.name_or_id == "default"


def test_extract_run_pipeline_id_prefers_direct_pipeline_id() -> None:
    run = _DummyRun(
        status=ExecutionStatus.RUNNING,
        pipeline_id="flow-direct",
        pipeline=SimpleNamespace(id="flow-related"),
    )

    assert _extract_run_pipeline_id(_as_pipeline_run(run)) == "flow-direct"


def test_extract_run_pipeline_id_falls_back_to_related_pipeline() -> None:
    run = _DummyRun(
        status=ExecutionStatus.RUNNING,
        pipeline=SimpleNamespace(id="flow-related"),
    )

    assert _extract_run_pipeline_id(_as_pipeline_run(run)) == "flow-related"


def test_build_kitaru_execution_url_uses_flow_execution_route() -> None:
    run = _DummyRun(
        status=ExecutionStatus.RUNNING,
        run_id="run/123",
        pipeline_id="flow 456",
    )

    assert (
        _build_kitaru_execution_url(
            _as_pipeline_run(run),
            server_url="http://127.0.0.1:8383/",
        )
        == "http://127.0.0.1:8383/flows/flow%20456/executions/run%2F123"
    )


def test_build_kitaru_execution_url_uses_local_when_metadata_unavailable() -> None:
    class NonHydratedRun:
        id = "run-123"
        pipeline_id = "flow-456"
        source_snapshot = None
        resources = None

        @property
        def run_metadata(self) -> object:
            raise RuntimeError("metadata is not hydrated yet")

    assert (
        _build_kitaru_execution_url(
            cast(PipelineRunResponse, NonHydratedRun()),
            server_url="http://127.0.0.1:8383/",
        )
        == "http://127.0.0.1:8383/flows/flow-456/executions/run-123"
    )


def test_build_kitaru_execution_url_uses_pro_route_when_context_is_pro() -> None:
    run = _DummyRun(
        status=ExecutionStatus.RUNNING,
        run_id="run/123",
        pipeline_id="flow 456",
        project=SimpleNamespace(name="default"),
    )

    assert _build_kitaru_execution_url(
        _as_pipeline_run(run),
        ui_context=UiUrlContext(
            base_url="https://staging.cloud.zenml.io",
            route_kind="pro",
            source="server_info",
            workspace="kitaru-dev",
        ),
    ) == (
        "https://staging.cloud.zenml.io/kitaru-workspaces/kitaru-dev"
        "/projects/default/flows/flow%20456/v/local/executions/run%2F123"
    )


def test_build_kitaru_execution_url_uses_deployment_snapshot_version() -> None:
    run = _DummyRun(
        status=ExecutionStatus.RUNNING,
        run_id="run-123",
        pipeline_id="flow-456",
        project=SimpleNamespace(name="default"),
        source_snapshot=SimpleNamespace(name="kitaru::support_copilot_flow::v3"),
    )

    url = _build_kitaru_execution_url(
        _as_pipeline_run(run),
        ui_context=UiUrlContext(
            base_url="https://staging.cloud.zenml.io",
            route_kind="pro",
            source="server_info",
            workspace="kitaru-dev",
        ),
    )

    assert url is not None
    assert "/v/3/executions/run-123" in url


def test_build_kitaru_execution_url_returns_none_when_required_data_missing() -> None:
    run = _DummyRun(
        status=ExecutionStatus.RUNNING,
        run_id="run-123",
        pipeline_id="flow-456",
    )
    run_without_pipeline = _DummyRun(
        status=ExecutionStatus.RUNNING,
        run_id="run-123",
    )

    assert _build_kitaru_execution_url(_as_pipeline_run(run), server_url=None) is None
    assert (
        _build_kitaru_execution_url(
            _as_pipeline_run(run_without_pipeline),
            server_url="http://127.0.0.1:8383",
        )
        is None
    )


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
    from kitaru._terminal_hooks import aggregate_llm_usage_on_run_end

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

    pipeline_mock.assert_called_once_with(
        dynamic=True,
        name="_lambda_",
        on_end=aggregate_llm_usage_on_run_end,
    )
    assert hasattr(wrapped, "run")
    assert hasattr(wrapped, "deploy")
    assert hasattr(wrapped, "invoke")
    assert not hasattr(wrapped, "start")
    assert isinstance(handle, FlowHandle)
    call_kwargs = base_pipeline.with_options.call_args
    assert call_kwargs == call(
        retry=None,
        settings={
            "docker": DockerSettings(
                requirements=["kitaru"],
                environment={KITARU_MODEL_REGISTRY_ENV: _empty_registry_payload()},
            )
        },
    )
    # Unset execution-level cache must not be forwarded as enable_cache, because
    # ZenML's compiler treats any concrete run-level enable_cache as a per-step
    # override that overwrites @checkpoint(cache=...) settings.
    assert "enable_cache" not in call_kwargs.kwargs


def test_flow_run_activates_resolved_project_for_submission() -> None:
    run = _DummyRun(status=ExecutionStatus.RUNNING)
    project_events: list[tuple[str, str | None]] = []

    @contextmanager
    def _project_context(project: str | None) -> Iterator[None]:
        project_events.append(("enter", project))
        try:
            yield
        finally:
            project_events.append(("exit", project))

    def _submit_pipeline(*_args: Any, **_kwargs: Any) -> _DummyRun:
        assert project_events == [("enter", "production")]
        return run

    def _persist_spec(**_kwargs: Any) -> None:
        assert project_events == [("enter", "production")]

    configured_pipeline = MagicMock(side_effect=_submit_pipeline)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config", return_value=_resolved_execution()
        ),
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(project="production"),
        ),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec", side_effect=_persist_spec),
        patch("kitaru.flow._temporary_active_project", side_effect=_project_context),
        patch("kitaru.flow._temporary_active_stack", return_value=nullcontext()),
        patch("kitaru.flow._preflight_active_stack_implementation_hydration"),
    ):
        wrapped = flow(lambda: None)
        handle = wrapped.run()

    assert handle._project == "production"
    assert project_events == [("enter", "production"), ("exit", "production")]


def test_implicit_default_stack_fallback_guard_fails_closed() -> None:
    client = SimpleNamespace(
        active_stack_model=SimpleNamespace(name="default", id="default-stack-id")
    )

    with pytest.raises(KitaruUsageError) as exc_info:
        _guard_implicit_active_stack_fallback(
            operation="run this flow",
            resolved_execution=_resolved_execution(
                stack="default",
                stack_source="zenml_active_stack",
            ),
            raw_active_stack_provenance=_stale_default_stack_provenance(),
            client_factory=MagicMock(return_value=client),
        )

    message = str(exc_info.value)
    assert "refused to run this flow" in message
    assert "fallback stack `default` implicitly" in message
    assert "stale-stack-id" in message
    assert "default-stack-id" in message
    assert "pass `stack=...`" in message


@pytest.mark.parametrize(
    "stack_source",
    ["project_config", "environment", "runtime", "decorator", "invocation"],
)
def test_explicit_default_stack_bypasses_fallback_guard(
    stack_source: ExecutionStackSource,
) -> None:
    client_factory = MagicMock()

    _guard_implicit_active_stack_fallback(
        operation="run this flow",
        resolved_execution=_resolved_execution(
            stack="default",
            stack_source=stack_source,
        ),
        raw_active_stack_provenance=_stale_default_stack_provenance(),
        client_factory=client_factory,
    )

    client_factory.assert_not_called()


def test_run_fails_closed_before_submitting_on_implicit_default_fallback() -> None:
    configured_pipeline = MagicMock(
        return_value=_DummyRun(status=ExecutionStatus.RUNNING)
    )
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    client = SimpleNamespace(
        active_stack_model=SimpleNamespace(name="default", id="default-stack-id")
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow._capture_active_stack_provenance_for_guard",
            return_value=_stale_default_stack_provenance(),
        ),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(
                stack="default",
                stack_source="zenml_active_stack",
            ),
        ),
        patch("kitaru.flow.Client", return_value=client),
        patch("kitaru.flow.resolve_connection_config") as resolve_connection_mock,
        pytest.raises(KitaruUsageError, match="fallback stack `default` implicitly"),
    ):
        wrapped = flow(lambda: None)
        wrapped.run()

    resolve_connection_mock.assert_not_called()
    base_pipeline.with_options.assert_not_called()
    configured_pipeline.assert_not_called()


def test_deploy_fails_closed_before_submitting_on_implicit_default_fallback() -> None:
    configured_pipeline = MagicMock()
    configured_pipeline._run_args = {}
    configured_pipeline._parameters = {}
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    client = SimpleNamespace(
        active_stack_model=SimpleNamespace(name="default", id="default-stack-id")
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow._capture_active_stack_provenance_for_guard",
            return_value=_stale_default_stack_provenance(),
        ),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(
                stack="default",
                stack_source="zenml_active_stack",
            ),
        ),
        patch("kitaru.flow.Client", return_value=client),
        patch("kitaru.flow._prepare_model_registry_transport") as transport_mock,
        pytest.raises(KitaruUsageError, match="fallback stack `default` implicitly"),
    ):
        wrapped = flow(lambda: None)
        wrapped.deploy()

    transport_mock.assert_not_called()
    base_pipeline.with_options.assert_not_called()
    configured_pipeline.prepare.assert_not_called()


def test_flow_deploy_creates_snapshot_and_forwards_raw_tags() -> None:
    source_snapshot = SimpleNamespace(id=uuid4(), name="temporary-source")
    public_deployment = object()
    configured_pipeline = MagicMock()
    configured_pipeline._run_args = {}
    configured_pipeline._parameters = {"x": 1}
    configured_pipeline._create_snapshot.return_value = source_snapshot
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    deployments_api = SimpleNamespace(
        create=MagicMock(return_value=public_deployment),
    )
    client = SimpleNamespace(deployments=deployments_api)

    stack_client = SimpleNamespace(
        active_stack_model=SimpleNamespace(name="prod"),
        zen_store=object(),
        active_stack=object(),
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(project=None),
        ),
        patch(
            "kitaru.flow._prepare_model_registry_transport",
            return_value=(None, ModelRegistryConfig()),
        ),
        patch("kitaru.flow._temporary_active_stack", return_value=nullcontext()),
        patch("kitaru.flow.Client", return_value=stack_client),
        patch("kitaru.flow.ensure_stack_is_server_runnable") as validate_stack_mock,
        patch("kitaru.client.KitaruClient", return_value=client),
    ):
        wrapped = flow(lambda x: x)
        deployment = wrapped.deploy(1, tags={"canary": False})

    validate_stack_mock.assert_called_once_with(
        zen_store=stack_client.zen_store,
        stack=stack_client.active_stack_model,
        operation="deploy",
        flow="_lambda_",
    )

    assert deployment is public_deployment
    configured_pipeline.prepare.assert_called_once_with(1)
    create_kwargs = configured_pipeline._create_snapshot.call_args.kwargs
    assert create_kwargs["replace"] is False
    assert create_kwargs["extra"]["kitaru_deployment"]["stack"] == "prod"
    schema = create_kwargs["extra"]["kitaru_deployment"]["schema"]
    assert schema["type"] == "object"
    assert "x" not in schema.get("required", [])
    deployments_api.create.assert_called_once_with(
        flow="_lambda_",
        source_snapshot=source_snapshot,
        tags={"canary": False},
    )


def test_flow_deploy_activates_resolved_project_for_snapshot_creation() -> None:
    source_snapshot = SimpleNamespace(id=uuid4(), name="temporary-source")
    project_events: list[tuple[str, str | None]] = []

    @contextmanager
    def _project_context(project: str | None) -> Iterator[None]:
        project_events.append(("enter", project))
        try:
            yield
        finally:
            project_events.append(("exit", project))

    def _prepare(*_args: Any, **_kwargs: Any) -> None:
        assert project_events == [("enter", "production")]

    configured_pipeline = MagicMock()
    configured_pipeline._run_args = {}
    configured_pipeline._parameters = {}
    configured_pipeline.prepare.side_effect = _prepare
    configured_pipeline._create_snapshot.return_value = source_snapshot
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    deployments_api = SimpleNamespace(create=MagicMock(return_value=object()))
    client = SimpleNamespace(deployments=deployments_api)
    stack_client = SimpleNamespace(
        active_stack_model=SimpleNamespace(name="prod"),
        zen_store=object(),
        active_stack=object(),
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config", return_value=_resolved_execution()
        ),
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(project="production"),
        ),
        patch(
            "kitaru.flow._prepare_model_registry_transport",
            return_value=(None, ModelRegistryConfig()),
        ),
        patch("kitaru.flow._temporary_active_project", side_effect=_project_context),
        patch("kitaru.flow._temporary_active_stack", return_value=nullcontext()),
        patch("kitaru.flow.Client", return_value=stack_client),
        patch("kitaru.flow.ensure_stack_is_server_runnable"),
        patch("kitaru.client.KitaruClient", return_value=client),
    ):
        wrapped = flow(lambda: None)
        wrapped.deploy()

    assert project_events == [("enter", "production"), ("exit", "production")]
    configured_pipeline._create_snapshot.assert_called_once()


def test_flow_deploy_prepare_does_not_persist_flow_result_artifacts() -> None:
    """Deployment snapshot preparation is a dry run, not a flow execution."""
    source_snapshot = SimpleNamespace(id=uuid4(), name="temporary-source")
    configured_pipeline = MagicMock()
    configured_pipeline._run_args = {}
    configured_pipeline._parameters = {"x": 1}
    configured_pipeline._create_snapshot.return_value = source_snapshot
    captured: dict[str, Callable[..., Any]] = {}

    def _decorate(entrypoint: Callable[..., Any]) -> object:
        captured["entrypoint"] = entrypoint
        return base_pipeline

    def _prepare(*args: Any, **kwargs: Any) -> None:
        captured["entrypoint"](*args, **kwargs)

    configured_pipeline.prepare.side_effect = _prepare
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(side_effect=_decorate)
    deployments_api = SimpleNamespace(create=MagicMock(return_value=object()))
    client = SimpleNamespace(deployments=deployments_api)
    stack_client = SimpleNamespace(
        active_stack_model=SimpleNamespace(name="prod"),
        zen_store=object(),
        active_stack=object(),
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(project=None),
        ),
        patch(
            "kitaru.flow._prepare_model_registry_transport",
            return_value=(None, ModelRegistryConfig()),
        ),
        patch("kitaru.flow._temporary_active_stack", return_value=nullcontext()),
        patch("kitaru.flow.Client", return_value=stack_client),
        patch("kitaru.flow.ensure_stack_is_server_runnable"),
        patch("kitaru.flow.save_artifact") as save_mock,
        patch("kitaru.client.KitaruClient", return_value=client),
    ):
        wrapped = flow(lambda x: {"answer": x})
        wrapped.deploy(1)

    configured_pipeline.prepare.assert_called_once_with(1)
    save_mock.assert_not_called()
    configured_pipeline._create_snapshot.assert_called_once()
    deployments_api.create.assert_called_once()


def test_real_zenml_prepare_does_not_persist_flow_result_artifacts() -> None:
    """Real ZenML prepare must stay a dry run for Kitaru result artifacts."""
    calls: list[int] = []

    @flow
    def prepare_regression_flow(x):
        calls.append(x)
        return {"answer": x}

    coerce_mock = MagicMock(side_effect=AssertionError("coercion called"))
    save_mock = MagicMock(side_effect=AssertionError("save_artifact called"))

    with (
        patch("kitaru.flow._coerce_flow_return_for_zenml", coerce_mock),
        patch("kitaru.flow.save_artifact", save_mock),
        _suspend_flow_return_coercion(),
    ):
        prepare_regression_flow._pipeline.prepare(1)

    # Real ZenML prepare currently compiles the dynamic pipeline without
    # executing the user body. The deploy-level mock test above covers the
    # defensive Kitaru suspension path if an entrypoint is invoked during
    # preparation.
    assert calls == []
    coerce_mock.assert_not_called()
    save_mock.assert_not_called()


def test_flow_deploy_resolves_invocation_image_and_threads_it_to_with_options() -> None:
    """Deploy should pass image overrides into config resolution and Docker settings."""
    source_snapshot = SimpleNamespace(id=uuid4(), name="temporary-source")
    public_deployment = object()
    configured_pipeline = MagicMock()
    configured_pipeline._run_args = {}
    configured_pipeline._parameters = {"x": 1}
    configured_pipeline._create_snapshot.return_value = source_snapshot
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    deployments_api = SimpleNamespace(create=MagicMock(return_value=public_deployment))
    client = SimpleNamespace(deployments=deployments_api)
    stack_client = SimpleNamespace(
        active_stack_model=SimpleNamespace(name="prod"),
        zen_store=object(),
        active_stack=object(),
    )
    resolved = _resolved_execution(
        stack="prod",
        image=ImageSettings(
            base_image="python:3.12-slim",
            secret_environment_from=["openai-creds"],
        ),
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=resolved,
        ) as resolve_execution_config_mock,
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(project=None),
        ),
        patch(
            "kitaru.flow._prepare_model_registry_transport",
            return_value=(
                ImageSettings(
                    base_image="python:3.12-slim",
                    secret_environment_from=["openai-creds"],
                    environment={KITARU_MODEL_REGISTRY_ENV: _empty_registry_payload()},
                ),
                ModelRegistryConfig(),
            ),
        ),
        patch("kitaru.flow._temporary_active_stack", return_value=nullcontext()),
        patch("kitaru.flow.Client", return_value=stack_client),
        patch("kitaru.flow.ensure_stack_is_server_runnable"),
        patch("kitaru.client.KitaruClient", return_value=client),
    ):
        wrapped = flow(lambda x: x)
        wrapped.deploy(1, image="python:3.12-slim")

    resolve_call = resolve_execution_config_mock.call_args.kwargs
    assert resolve_call["invocation_overrides"].image == ImageSettings(
        base_image="python:3.12-slim"
    )

    call_kwargs = base_pipeline.with_options.call_args.kwargs
    assert call_kwargs["secrets"] == ["openai-creds"]
    docker_settings = call_kwargs["settings"]["docker"]
    assert docker_settings.parent_image == "python:3.12-slim"
    assert docker_settings.environment == {
        KITARU_MODEL_REGISTRY_ENV: _empty_registry_payload()
    }
    deployments_api.create.assert_called_once_with(
        flow="_lambda_",
        source_snapshot=source_snapshot,
        tags=None,
    )


def test_flow_deploy_can_skip_first_deploy_default_publish() -> None:
    configured_pipeline = MagicMock()
    configured_pipeline._run_args = {}
    configured_pipeline._parameters = {}
    source_snapshot = object()
    configured_pipeline._create_snapshot.return_value = source_snapshot
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    deployments_api = SimpleNamespace(create=MagicMock(return_value=object()))
    client = SimpleNamespace(deployments=deployments_api)
    stack_client = SimpleNamespace(
        active_stack_model=SimpleNamespace(name="prod"),
        zen_store=object(),
        active_stack=object(),
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(project=None),
        ),
        patch(
            "kitaru.flow._prepare_model_registry_transport",
            return_value=(None, ModelRegistryConfig()),
        ),
        patch("kitaru.flow._temporary_active_stack", return_value=nullcontext()),
        patch("kitaru.flow.Client", return_value=stack_client),
        patch("kitaru.flow.ensure_stack_is_server_runnable"),
        patch("kitaru.client.KitaruClient", return_value=client),
    ):
        wrapped = flow(lambda x: x)
        wrapped.deploy(1, publish_default_on_first_deploy=False)

    deployments_api.create.assert_called_once_with(
        flow="_lambda_",
        source_snapshot=source_snapshot,
        tags=None,
        publish_default_on_first_deploy=False,
    )


def test_flow_deploy_rejects_non_server_runnable_stack_before_prepare() -> None:
    configured_pipeline = MagicMock()
    configured_pipeline._run_args = {}
    configured_pipeline._parameters = {}
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    deployments_api = SimpleNamespace(create=MagicMock())
    client = SimpleNamespace(deployments=deployments_api)
    stack_client = SimpleNamespace(
        active_stack_model=SimpleNamespace(name="local"),
        zen_store=object(),
        active_stack=object(),
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="local"),
        ),
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(project=None),
        ),
        patch(
            "kitaru.flow._prepare_model_registry_transport",
            return_value=(None, ModelRegistryConfig()),
        ),
        patch("kitaru.flow._temporary_active_stack", return_value=nullcontext()),
        patch("kitaru.flow.Client", return_value=stack_client),
        patch("kitaru.flow.ensure_stack_is_server_runnable") as validate_stack_mock,
        patch("kitaru.client.KitaruClient", return_value=client),
    ):
        validate_stack_mock.side_effect = KitaruUsageError(
            "the Kitaru server cannot run that stack"
        )
        wrapped = flow(lambda x: x)
        with pytest.raises(KitaruUsageError, match="cannot run that stack"):
            wrapped.deploy(1)

    configured_pipeline.prepare.assert_not_called()
    configured_pipeline._create_snapshot.assert_not_called()
    deployments_api.create.assert_not_called()


def test_deploy_translates_active_stack_hydration_import_error_before_prepare() -> None:
    configured_pipeline = MagicMock()
    configured_pipeline._run_args = {}
    configured_pipeline._parameters = {}
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    deployments_api = SimpleNamespace(create=MagicMock())
    client = SimpleNamespace(deployments=deployments_api)
    old_stack_id = uuid4()
    stack_client = _ClientWithMissingStackDependency(old_stack_id=old_stack_id)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(project=None),
        ),
        patch(
            "kitaru.flow._prepare_model_registry_transport",
            return_value=(None, ModelRegistryConfig()),
        ),
        patch("kitaru.flow.Client", return_value=stack_client),
        patch("kitaru.flow.ensure_stack_is_server_runnable") as validate_stack_mock,
        patch("kitaru.client.KitaruClient", return_value=client),
        pytest.raises(KitaruStackIntegrationDependencyError) as exc_info,
    ):
        wrapped = flow(lambda x: x)
        wrapped.deploy(1, stack="prod")

    message = str(exc_info.value)
    assert "Cannot submit this Kitaru flow" in message
    assert "stack integration dependency appears to be missing" in message
    assert "zenml integration install s3" in message
    assert "zenml stack export-requirements" in message
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True
    configured_pipeline.prepare.assert_not_called()
    configured_pipeline._create_snapshot.assert_not_called()
    deployments_api.create.assert_not_called()
    validate_stack_mock.assert_called_once_with(
        zen_store=stack_client.zen_store,
        stack=stack_client.selected_stack_model,
        operation="deploy",
        flow="_lambda_",
    )
    assert stack_client.activate_stack.call_args_list == [
        call("prod"),
        call(old_stack_id),
    ]


def test_flow_deploy_rewords_input_defaults_error() -> None:
    configured_pipeline = MagicMock()
    configured_pipeline._run_args = {}
    configured_pipeline._parameters = {}
    configured_pipeline.prepare.side_effect = ValueError("missing input")
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    stack_client = SimpleNamespace(
        active_stack_model=SimpleNamespace(name="prod"),
        zen_store=object(),
        active_stack=object(),
    )
    deployments_api = SimpleNamespace(create=MagicMock())
    client = SimpleNamespace(deployments=deployments_api)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(project=None),
        ),
        patch(
            "kitaru.flow._prepare_model_registry_transport",
            return_value=(None, ModelRegistryConfig()),
        ),
        patch("kitaru.flow._temporary_active_stack", return_value=nullcontext()),
        patch("kitaru.flow.Client", return_value=stack_client),
        patch("kitaru.flow.ensure_stack_is_server_runnable"),
        patch("kitaru.client.KitaruClient", return_value=client),
    ):
        wrapped = flow(lambda x: x)
        with pytest.raises(KitaruUsageError) as exc_info:
            wrapped.deploy(1)

    message = str(exc_info.value)
    assert "needs concrete input values" in message
    assert "saved deployment snapshot" in message
    assert "flow.deploy(...)" in message
    assert "deployment-time input defaults" not in message
    assert "ZenML currently" not in message
    deployments_api.create.assert_not_called()


def test_flow_deployment_and_deployments_delegate_to_client() -> None:
    listed = [object()]
    selected = object()
    deployments_api = SimpleNamespace(
        list=MagicMock(return_value=listed),
        get=MagicMock(return_value=selected),
    )
    client = SimpleNamespace(deployments=deployments_api)

    with patch("kitaru.client.KitaruClient", return_value=client):
        wrapped = flow(lambda: None)
        assert wrapped.deployments() == listed
        assert wrapped.deployment() is selected

    deployments_api.list.assert_called_once_with(flow="_lambda_")
    deployments_api.get.assert_called_once_with(
        flow="_lambda_",
        version=None,
        tag="default",
    )


def test_flow_invoke_validates_selectors_and_delegates_to_client() -> None:
    handle = object()
    deployments_api = SimpleNamespace(invoke=MagicMock(return_value=handle))
    client = SimpleNamespace(deployments=deployments_api)

    with patch("kitaru.client.KitaruClient", return_value=client):
        wrapped = flow(lambda answer=0: answer)
        with pytest.raises(KitaruUsageError, match="mutually exclusive"):
            wrapped.invoke(version=1, tag="default")
        assert wrapped.invoke(answer=21) is handle
        assert wrapped.invoke(version=7, answer=42) is handle

    assert deployments_api.invoke.call_args_list == [
        call(
            flow="_lambda_",
            version=None,
            tag="default",
            selector_source="implicit_default",
            inputs={"answer": 21},
        ),
        call(
            flow="_lambda_",
            version=7,
            tag=None,
            selector_source="version",
            inputs={"answer": 42},
        ),
    ]


def test_build_pipeline_options_omits_enable_cache_when_unset() -> None:
    options = _build_pipeline_options(
        resolved_execution=_resolved_execution(cache=None),
        transport_image=None,
    )
    assert "enable_cache" not in options


@pytest.mark.parametrize("cache_value", [True, False])
def test_build_pipeline_options_forwards_explicit_cache(
    cache_value: bool,
) -> None:
    options = _build_pipeline_options(
        resolved_execution=_resolved_execution(cache=cache_value),
        transport_image=None,
    )
    assert options["enable_cache"] is cache_value


def test_build_pipeline_options_forwards_secret_environment_from() -> None:
    """Non-empty secret refs must be forwarded via with_options(secrets=...)."""
    transport_image = ImageSettings(secret_environment_from=["openai-creds"])
    options = _build_pipeline_options(
        resolved_execution=_resolved_execution(),
        transport_image=transport_image,
    )
    assert options["secrets"] == ["openai-creds"]
    # The forwarded list must be a fresh copy so downstream mutation cannot
    # corrupt the model-owned list (see list(...) wrap in _build_pipeline_options).
    options["secrets"].append("mutation")
    assert transport_image.secret_environment_from == ["openai-creds"]


@pytest.mark.parametrize(
    "transport_image",
    [
        None,
        ImageSettings(),
        ImageSettings(secret_environment_from=None),
        ImageSettings(secret_environment_from=[]),
    ],
)
def test_build_pipeline_options_omits_secrets_when_unset_or_empty(
    transport_image: ImageSettings | None,
) -> None:
    """``secrets`` must stay absent so ZenML defaults are not overwritten."""
    options = _build_pipeline_options(
        resolved_execution=_resolved_execution(),
        transport_image=transport_image,
    )
    assert "secrets" not in options


def test_build_pipeline_options_keeps_secret_refs_out_of_docker_environment() -> None:
    """Secret refs must never enter DockerSettings.environment."""
    options = _build_pipeline_options(
        resolved_execution=_resolved_execution(),
        transport_image=ImageSettings(
            environment={"PLAIN": "value"},
            secret_environment_from=["openai-creds"],
        ),
    )
    docker_settings = options["settings"]["docker"]
    assert docker_settings.environment == {"PLAIN": "value"}
    assert options["secrets"] == ["openai-creds"]


def test_apply_active_stack_image_defaults_sets_modal_platform() -> None:
    """Modal runs need amd64 images when Docker defaults to local ARM."""
    client = SimpleNamespace(
        active_stack=SimpleNamespace(
            orchestrator=SimpleNamespace(flavor="modal"),
        )
    )

    image = _apply_active_stack_image_defaults(
        ImageSettings(environment={"PLAIN": "value"}),
        client_factory=lambda: client,
    )

    assert isinstance(image, ImageSettings)
    assert image.platform == "linux/amd64"
    assert image.environment == {"PLAIN": "value"}


def test_apply_active_stack_image_defaults_creates_modal_image_when_missing() -> None:
    """Modal defaults must apply even when no image settings exist yet."""
    client = SimpleNamespace(
        active_stack=SimpleNamespace(
            orchestrator=SimpleNamespace(flavor="modal"),
        )
    )

    image = _apply_active_stack_image_defaults(None, client_factory=lambda: client)

    assert isinstance(image, ImageSettings)
    assert image.platform == "linux/amd64"


def test_apply_active_stack_image_defaults_keeps_explicit_platform() -> None:
    """A user-specified image platform must win over Modal's default."""
    client = SimpleNamespace(
        active_stack=SimpleNamespace(
            orchestrator=SimpleNamespace(flavor="modal"),
        )
    )

    image = _apply_active_stack_image_defaults(
        ImageSettings(platform="linux/arm64"),
        client_factory=lambda: client,
    )

    assert isinstance(image, ImageSettings)
    assert image.platform == "linux/arm64"


def test_apply_active_stack_image_defaults_ignores_non_modal_stack() -> None:
    """Only Modal stacks need Kitaru to force the default platform."""
    client = SimpleNamespace(
        active_stack=SimpleNamespace(
            orchestrator=SimpleNamespace(flavor="kubernetes"),
        )
    )

    image = _apply_active_stack_image_defaults(
        ImageSettings(),
        client_factory=lambda: client,
    )

    assert isinstance(image, ImageSettings)
    assert image.platform is None


def test_flow_run_forwards_secrets_and_preserves_model_registry_env() -> None:
    """``.run()`` threads secret refs while keeping model registry in Docker env."""
    run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock(return_value=run)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    resolved = _resolved_execution(
        image=ImageSettings(secret_environment_from=["openai-creds"]),
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.resolve_execution_config", return_value=resolved),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
    ):
        flow(lambda x: x).run(123)

    call_kwargs = base_pipeline.with_options.call_args.kwargs
    assert call_kwargs["secrets"] == ["openai-creds"]

    docker_settings = call_kwargs["settings"]["docker"]
    assert docker_settings.environment == {
        KITARU_MODEL_REGISTRY_ENV: _empty_registry_payload(),
    }
    assert "enable_cache" not in call_kwargs


def test_flow_run_omits_secrets_when_none_configured() -> None:
    """Without secret refs configured, ``secrets`` must not be passed at all."""
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
    ):
        flow(lambda x: x).run(123)

    call_kwargs = base_pipeline.with_options.call_args.kwargs
    assert "secrets" not in call_kwargs


def test_flow_return_coercion_preserves_zenml_output_handles() -> None:
    """Compilation-time checkpoint output handles must not be re-saved."""
    artifact = OutputArtifact.model_construct(
        id=uuid4(),
        step_name="produce_value",
        output_name="output",
    )
    tuple_metadata = object()

    with patch("kitaru.flow.save_artifact", return_value=tuple_metadata) as save_mock:
        result = _coerce_flow_return_for_zenml(artifact)
        tuple_result = _coerce_flow_return_for_zenml((artifact,))

    assert result is artifact
    assert tuple_result == (artifact, tuple_metadata)
    save_mock.assert_called_once()
    assert save_mock.call_args.kwargs["data"] == {
        "kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER,
        "version": 1,
        "length": 1,
    }
    assert save_mock.call_args.kwargs["user_metadata"] == {
        "kitaru_artifact_type": "output",
        _FLOW_RESULT_ROLE_METADATA_KEY: _FLOW_RESULT_TUPLE_METADATA_ROLE,
    }


def test_flow_return_coercion_saves_plain_values_as_pipeline_artifacts() -> None:
    """Plain Kitaru flow results must satisfy ZenML pipeline output validation."""
    artifact = object()

    with patch("kitaru.flow.save_artifact", return_value=artifact) as save_mock:
        result = _coerce_flow_return_for_zenml({"answer": 42})

    assert result is artifact
    save_mock.assert_called_once()
    assert save_mock.call_args.kwargs == {
        "data": {"answer": 42},
        "name": _FLOW_RESULT_ARTIFACT_NAME,
        "artifact_type": ArtifactType.DATA,
        "user_metadata": {"kitaru_artifact_type": "output"},
    }


def test_flow_return_coercion_records_explicit_none_result() -> None:
    """Explicit None returns get metadata so extraction can beat heuristics."""
    with (
        patch("kitaru.flow.save_artifact") as save_mock,
        patch("kitaru.flow._get_current_execution_id", return_value="run-id"),
        patch("kitaru.logging.log") as log_mock,
    ):
        result = _coerce_flow_return_for_zenml(None)

    assert result is None
    save_mock.assert_not_called()
    log_mock.assert_called_once_with(**{_FLOW_RESULT_NONE_METADATA_KEY: True})


def test_flow_return_coercion_fails_closed_when_none_marker_fails() -> None:
    """A None result must not become an inferred terminal checkpoint later."""
    with (
        patch("kitaru.flow.save_artifact") as save_mock,
        patch("kitaru.flow._get_current_execution_id", return_value="run-id"),
        patch("kitaru.logging.log", side_effect=RuntimeError("metadata down")),
        pytest.raises(KitaruRuntimeError, match="flow returned None") as exc_info,
    ):
        _coerce_flow_return_for_zenml(None)

    assert "terminal checkpoint output" in str(exc_info.value)
    save_mock.assert_not_called()


def test_flow_return_coercion_wraps_artifact_save_failures() -> None:
    """Internal result-artifact failures should not look like user-code errors."""
    with (
        patch("kitaru.flow.save_artifact", side_effect=RuntimeError("store down")),
        pytest.raises(KitaruRuntimeError, match="could not persist") as exc_info,
    ):
        _coerce_flow_return_for_zenml({"answer": 42})

    message = str(exc_info.value)
    assert "returned successfully" in message
    assert "after user code returned" in message
    assert "retries" in message
    assert "side effects" in message
    assert isinstance(exc_info.value.__cause__, RuntimeError)


def test_post_return_artifact_save_failure_does_not_retry_inside_wrapper() -> None:
    """Kitaru reports the retry risk but does not add its own inner retry."""
    side_effects = {"count": 0}

    def user_flow() -> dict[str, int]:
        side_effects["count"] += 1
        return {"answer": 42}

    wrapped = _wrap_flow_entrypoint(user_flow)

    with (
        patch("kitaru.runtime.StepContext.get", return_value=None),
        patch("kitaru.runtime.DynamicPipelineRunContext.get", return_value=None),
        patch("kitaru.flow.save_artifact", side_effect=RuntimeError("store down")),
        pytest.raises(KitaruRuntimeError) as exc_info,
    ):
        wrapped()

    message = str(exc_info.value)
    assert "after user code returned" in message
    assert "retries" in message
    assert "side effects" in message
    assert side_effects["count"] == 1


def test_flow_return_coercion_preserves_plain_tuple_as_one_artifact() -> None:
    """Plain tuples are ordinary Python return values, not pipeline fan-out."""
    saved_tuple = object()

    with patch("kitaru.flow.save_artifact", return_value=saved_tuple) as save_mock:
        result = _coerce_flow_return_for_zenml((1,))

    assert result is saved_tuple
    save_mock.assert_called_once()
    assert save_mock.call_args.kwargs == {
        "data": (1,),
        "name": _FLOW_RESULT_ARTIFACT_NAME,
        "artifact_type": ArtifactType.DATA,
        "user_metadata": {"kitaru_artifact_type": "output"},
    }


def test_flow_return_coercion_preserves_namedtuple_as_one_artifact() -> None:
    """Tuple subclasses should keep their user-facing object semantics."""
    Pair = namedtuple("Pair", ["left", "right"])
    value = Pair(left=1, right=2)
    saved_pair = object()

    with patch("kitaru.flow.save_artifact", return_value=saved_pair) as save_mock:
        result = _coerce_flow_return_for_zenml(value)

    assert result is saved_pair
    save_mock.assert_called_once()
    assert save_mock.call_args.kwargs["data"] == value
    assert save_mock.call_args.kwargs["name"] == _FLOW_RESULT_ARTIFACT_NAME


def test_flow_return_coercion_preserves_mixed_tuple_outputs() -> None:
    """Mixed artifact/plain tuples should remain tuple-shaped pipeline outputs."""
    handle = OutputArtifact.model_construct(
        id=uuid4(),
        step_name="produce_value",
        output_name="output",
    )
    saved_plain = object()
    tuple_metadata = object()

    with patch(
        "kitaru.flow.save_artifact",
        side_effect=[saved_plain, tuple_metadata],
    ) as save_mock:
        result = _coerce_flow_return_for_zenml((handle, 1))

    assert result == (handle, saved_plain, tuple_metadata)
    assert save_mock.call_count == 2
    assert save_mock.call_args_list[0].kwargs == {
        "data": 1,
        "name": f"{_FLOW_RESULT_ARTIFACT_NAME}_1",
        "artifact_type": ArtifactType.DATA,
        "user_metadata": {"kitaru_artifact_type": "output"},
    }
    assert save_mock.call_args_list[1].kwargs["data"] == {
        "kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER,
        "version": 1,
        "length": 2,
    }
    assert save_mock.call_args_list[1].kwargs["user_metadata"] == {
        "kitaru_artifact_type": "output",
        _FLOW_RESULT_ROLE_METADATA_KEY: _FLOW_RESULT_TUPLE_METADATA_ROLE,
    }


def test_flow_result_extraction_restores_singleton_tuple_outputs() -> None:
    """Hidden tuple metadata keeps one-item artifact tuples tuple-shaped."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            ("step", "output_0", "value"),
            _DummyOutput(
                step_name="step",
                output_name="output_1",
                value={
                    "kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER,
                    "version": 1,
                    "length": 1,
                },
                artifact_name=_FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME,
                run_metadata={
                    _FLOW_RESULT_ROLE_METADATA_KEY: _FLOW_RESULT_TUPLE_METADATA_ROLE
                },
            ),
        ],
    )

    assert _extract_flow_result(_as_pipeline_run(run)) == ("value",)


def test_flow_result_extraction_prefers_run_outputs_over_ambiguous_terminal_steps() -> (
    None
):
    """Persisted run outputs are authoritative when terminal checkpoints fan out."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            _DummyOutput(step_name="model_call", output_name="output", value="model"),
            _DummyOutput(step_name="tool_call", output_name="output", value="tool"),
        ],
        run_outputs=[("final_output", "final answer")],
    )
    run.snapshot.pipeline_spec.outputs = []

    assert _extract_flow_result(_as_pipeline_run(run)) == "final answer"


def test_flow_result_extraction_prefers_run_outputs_over_linked_result() -> None:
    """Run outputs are explicit, so they beat the linked fallback artifact."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        run_outputs=[("final_output", "run output")],
    )
    _add_linked_flow_result_metadata(run)

    client_mock = MagicMock()
    with patch("kitaru.flow.Client", return_value=client_mock):
        result = _extract_flow_result(_as_pipeline_run(run))

    assert result == "run output"
    client_mock.get_artifact_version.assert_not_called()


def test_flow_result_extraction_prefers_output_specs_over_linked_result() -> None:
    """Declared pipeline output specs are explicit, so they beat the link."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("declared_step", "output", "declared result")],
    )
    _add_linked_flow_result_metadata(run)

    client_mock = MagicMock()
    with patch("kitaru.flow.Client", return_value=client_mock):
        result = _extract_flow_result(_as_pipeline_run(run))

    assert result == "declared result"
    client_mock.get_artifact_version.assert_not_called()


def test_flow_result_extraction_returns_multiple_run_outputs_in_persisted_order() -> (
    None
):
    """Multiple run outputs follow ZenML's persisted output order."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        run_outputs=[
            ("second_declared_output", "second"),
            ("first_declared_output", "first"),
        ],
    )

    assert _extract_flow_result(_as_pipeline_run(run)) == ("second", "first")


def test_flow_result_extraction_restores_tuple_metadata_from_run_outputs() -> None:
    """Run-level output artifacts use the same tuple metadata reconstruction."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        run_outputs=[
            ("output_0", "left"),
            ("output_1", "right"),
            _DummyRunOutput(
                output_name="output_2",
                value={
                    "kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER,
                    "version": 1,
                    "length": 2,
                },
                artifact_name=_FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME,
                run_metadata={
                    _FLOW_RESULT_ROLE_METADATA_KEY: _FLOW_RESULT_TUPLE_METADATA_ROLE
                },
            ),
        ],
    )

    assert _extract_flow_result(_as_pipeline_run(run)) == ("left", "right")


def test_flow_result_extraction_restores_singleton_tuple_from_run_outputs() -> None:
    """A single user output plus tuple metadata stays tuple-shaped."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        run_outputs=[
            ("output_0", "only"),
            _DummyRunOutput(
                output_name="output_1",
                value={
                    "kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER,
                    "version": 1,
                    "length": 1,
                },
                artifact_name=_FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME,
                run_metadata={
                    _FLOW_RESULT_ROLE_METADATA_KEY: _FLOW_RESULT_TUPLE_METADATA_ROLE
                },
            ),
        ],
    )

    assert _extract_flow_result(_as_pipeline_run(run)) == ("only",)


def test_flow_result_extraction_empty_run_outputs_fall_back_to_output_specs() -> None:
    """Empty run outputs preserve the previous output-spec extraction path."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("declared_step", "output", "declared result")],
        run_outputs=[],
    )

    assert _extract_flow_result(_as_pipeline_run(run)) == "declared result"


def test_flow_result_extraction_output_spec_none_beats_terminal_fallback() -> None:
    """An output-spec ``None`` beats a discarded terminal checkpoint output."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            ("declared_step", "output", None),
            _DummyOutput(
                step_name="one_value",
                output_name="output",
                value="solo",
                upstream_steps=["declared_step"],
            ),
        ],
        run_outputs=[],
    )
    run.snapshot.pipeline_spec.outputs = [
        SimpleNamespace(step_name="declared_step", output_name="output")
    ]

    assert _extract_flow_result(_as_pipeline_run(run)) is None


def test_flow_result_extraction_missing_run_outputs_falls_back_to_output_specs() -> (
    None
):
    """Old run records without an outputs field still use output specs."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("declared_step", "output", "declared result")],
    )
    delattr(run, "outputs")

    assert _extract_flow_result(_as_pipeline_run(run)) == "declared result"


def test_flow_result_extraction_preserves_marker_shaped_single_output() -> None:
    """A user value shaped like tuple metadata should still round-trip as data."""
    value = {
        "kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER,
        "version": 1,
        "length": 0,
    }
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("step", "output", value)],
    )

    assert _extract_flow_result(_as_pipeline_run(run)) == value


def test_flow_result_extraction_preserves_marker_shaped_last_output() -> None:
    """Tuple metadata detection must not rely on loaded dict shape alone."""
    marker_shaped_user_value = {
        "kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER,
        "version": 1,
        "length": 1,
    }
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            ("step", "output_0", "value"),
            ("step", "output_1", marker_shaped_user_value),
        ],
    )

    assert _extract_flow_result(_as_pipeline_run(run)) == (
        "value",
        marker_shaped_user_value,
    )


def test_flow_result_extraction_preserves_reserved_name_without_metadata_role() -> None:
    """Reserved names alone should not turn user artifacts into metadata."""
    value = {
        "kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER,
        "version": 1,
        "length": 1,
    }
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            _DummyOutput(
                step_name="step",
                output_name="output",
                value=value,
                artifact_name=_FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME,
            ),
        ],
    )

    assert _extract_flow_result(_as_pipeline_run(run)) == value


def test_flow_result_extraction_rejects_unexpected_tuple_metadata_role() -> None:
    """Reserved tuple metadata artifacts must not carry another Kitaru role."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            ("step", "output_0", "value"),
            _DummyOutput(
                step_name="step",
                output_name="output_1",
                value={
                    "kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER,
                    "version": 1,
                    "length": 1,
                },
                artifact_name=_FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME,
                run_metadata={_FLOW_RESULT_ROLE_METADATA_KEY: "not_tuple_metadata"},
            ),
        ],
    )

    with pytest.raises(KitaruRuntimeError, match="unexpected role"):
        _extract_flow_result(_as_pipeline_run(run))


def _run_with_reserved_tuple_metadata_artifact(
    metadata_value: Mapping[str, object],
) -> _DummyRun:
    """Build a run whose final output is Kitaru's reserved tuple metadata artifact."""
    return _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            ("step", "output_0", "value"),
            _DummyOutput(
                step_name="step",
                output_name="output_1",
                value=metadata_value,
                artifact_name=_FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME,
                run_metadata={
                    _FLOW_RESULT_ROLE_METADATA_KEY: _FLOW_RESULT_TUPLE_METADATA_ROLE
                },
            ),
        ],
    )


def test_flow_result_extraction_rejects_malformed_tuple_metadata() -> None:
    """Reserved tuple metadata artifacts must contain valid marker payloads."""
    run = _run_with_reserved_tuple_metadata_artifact(
        {"kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER}
    )

    with pytest.raises(KitaruRuntimeError, match="valid Kitaru tuple metadata"):
        _extract_flow_result(_as_pipeline_run(run))


@pytest.mark.parametrize("length", [True, 0, -1])
def test_flow_result_extraction_rejects_invalid_tuple_metadata_lengths(
    length: object,
) -> None:
    """Reserved tuple metadata artifacts must contain a positive integer length."""
    run = _run_with_reserved_tuple_metadata_artifact(
        {
            "kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER,
            "version": 1,
            "length": length,
        }
    )

    with pytest.raises(KitaruRuntimeError, match="valid Kitaru tuple metadata"):
        _extract_flow_result(_as_pipeline_run(run))


def test_flow_result_extraction_rejects_multiple_tuple_metadata_artifacts() -> None:
    """Only one hidden tuple metadata artifact may describe a flow result."""
    metadata_value = {
        "kitaru_artifact_type": _FLOW_RESULT_TUPLE_METADATA_MARKER,
        "version": 1,
        "length": 1,
    }
    metadata_output = _DummyOutput(
        step_name="step",
        output_name="output_1",
        value=metadata_value,
        artifact_name=_FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME,
        run_metadata={_FLOW_RESULT_ROLE_METADATA_KEY: _FLOW_RESULT_TUPLE_METADATA_ROLE},
    )
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            ("step", "output_0", "value"),
            metadata_output,
            _DummyOutput(
                step_name="step",
                output_name="output_2",
                value=metadata_value,
                artifact_name=_FLOW_RESULT_TUPLE_METADATA_ARTIFACT_NAME,
                run_metadata={
                    _FLOW_RESULT_ROLE_METADATA_KEY: _FLOW_RESULT_TUPLE_METADATA_ROLE
                },
            ),
        ],
    )

    with pytest.raises(KitaruRuntimeError, match="multiple Kitaru tuple metadata"):
        _extract_flow_result(_as_pipeline_run(run))


def test_terminal_hook_fetches_current_run_and_aggregates(monkeypatch) -> None:
    """The run-end hook should fetch a fresh run model before aggregation."""
    from kitaru import _terminal_hooks

    fetched_run = SimpleNamespace(id="fresh-run")
    aggregation_calls: list[tuple[object, object]] = []
    client = SimpleNamespace(
        get_pipeline_run=MagicMock(return_value=fetched_run),
    )

    monkeypatch.setattr(
        _terminal_hooks.DynamicPipelineRunContext,
        "get",
        lambda: SimpleNamespace(run=SimpleNamespace(id="run-1")),
    )
    monkeypatch.setattr(_terminal_hooks, "Client", lambda: client)
    monkeypatch.setattr(
        _terminal_hooks,
        "_safe_persist_terminal_llm_usage_metadata",
        lambda run, **kwargs: (
            aggregation_calls.append((run, kwargs.get("zenml_client"))) or True
        ),
    )

    _terminal_hooks.aggregate_llm_usage_on_run_end()

    client.get_pipeline_run.assert_called_once_with(
        "run-1",
        allow_name_prefix_match=False,
    )
    assert aggregation_calls == [(fetched_run, client)]


def test_terminal_hook_returns_silently_without_run_context(monkeypatch) -> None:
    """The run-end hook should be safe to import and call outside ZenML hooks."""
    from kitaru import _terminal_hooks

    def missing_context() -> object:
        raise RuntimeError("no active dynamic run")

    monkeypatch.setattr(
        _terminal_hooks.DynamicPipelineRunContext,
        "get",
        missing_context,
    )
    monkeypatch.setattr(
        _terminal_hooks,
        "Client",
        lambda: (_ for _ in ()).throw(AssertionError("client should not run")),
    )

    _terminal_hooks.aggregate_llm_usage_on_run_end()


def test_terminal_hook_catches_aggregation_failures(monkeypatch) -> None:
    """A cost-summary failure must not change the user run outcome."""
    from kitaru import _terminal_hooks

    monkeypatch.setattr(
        _terminal_hooks.DynamicPipelineRunContext,
        "get",
        lambda: SimpleNamespace(run=SimpleNamespace(id="run-1")),
    )
    monkeypatch.setattr(
        _terminal_hooks,
        "Client",
        lambda: SimpleNamespace(
            get_pipeline_run=lambda *_args, **_kwargs: SimpleNamespace(id="run-1")
        ),
    )
    monkeypatch.setattr(
        _terminal_hooks,
        "_safe_persist_terminal_llm_usage_metadata",
        lambda _run, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("aggregation failed")
        ),
    )

    _terminal_hooks.aggregate_llm_usage_on_run_end()


def test_terminal_hook_imports_do_not_create_flow_cycle() -> None:
    """The hook module and flow module should be importable in either order."""
    import importlib

    terminal_hooks = importlib.import_module("kitaru._terminal_hooks")
    flow_module = importlib.import_module("kitaru.flow")

    assert terminal_hooks.aggregate_llm_usage_on_run_end is not None
    assert flow_module.FlowHandle is FlowHandle


def test_flow_return_coercion_rejects_mixed_tuple_subclasses() -> None:
    """Tuple subclasses with handles would otherwise lose their field semantics."""
    Pair = namedtuple("Pair", ["left", "right"])
    handle = OutputArtifact.model_construct(
        id=uuid4(),
        step_name="produce_value",
        output_name="output",
    )

    with pytest.raises(KitaruUsageError, match="tuple subclass"):
        _coerce_flow_return_for_zenml(Pair(handle, 1))


def test_flow_handle_persists_terminal_llm_usage_once(monkeypatch) -> None:
    """One handle should not fetch attempts again after successful aggregation."""
    flow_module = sys.modules["kitaru.flow"]
    calls: list[str] = []

    def fake_safe(run: PipelineRunResponse, **_kwargs: object) -> bool:
        calls.append(str(run.id))
        return True

    monkeypatch.setattr(
        flow_module, "_safe_persist_terminal_llm_usage_metadata", fake_safe
    )

    handle = FlowHandle(_as_pipeline_run(_DummyRun(status=ExecutionStatus.COMPLETED)))
    run = _as_pipeline_run(_DummyRun(status=ExecutionStatus.COMPLETED, run_id="run-1"))
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = run
    monkeypatch.setattr(flow_module, "Client", lambda: client_mock)

    handle._persist_terminal_llm_usage_once(run)
    handle._persist_terminal_llm_usage_once(run)

    assert calls == ["run-1"]
    client_mock.get_pipeline_run.assert_called_once_with(
        run.id,
        allow_name_prefix_match=False,
        project=None,
    )


def test_flow_handle_does_not_persist_terminal_llm_usage_when_refresh_fails(
    monkeypatch,
) -> None:
    """Terminal aggregation should retry later instead of using stale metadata."""
    flow_module = sys.modules["kitaru.flow"]
    run = _as_pipeline_run(_DummyRun(status=ExecutionStatus.COMPLETED, run_id="run-1"))
    handle = FlowHandle(run)
    client_mock = MagicMock()
    client_mock.get_pipeline_run.side_effect = RuntimeError("refresh failed")

    monkeypatch.setattr(flow_module, "Client", lambda: client_mock)
    monkeypatch.setattr(
        flow_module,
        "_safe_persist_terminal_llm_usage_metadata",
        lambda _run, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not aggregate stale run")
        ),
    )

    handle._persist_terminal_llm_usage_once(run)

    assert handle._terminal_llm_usage_metadata_persisted is False
    client_mock.get_pipeline_run.assert_called_once_with(
        "run-1",
        allow_name_prefix_match=False,
        project=None,
    )


def test_flow_handle_init_persists_terminal_llm_usage_for_finished_run(
    monkeypatch,
) -> None:
    """Already-finished handles should still write terminal LLM rollups."""
    flow_module = sys.modules["kitaru.flow"]
    run = _as_pipeline_run(
        _DummyRun(status=ExecutionStatus.COMPLETED, run_id="run-finished")
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = run
    aggregation_calls: list[tuple[str, object]] = []
    analytics_events: list[AnalyticsEvent] = []

    monkeypatch.setattr(flow_module, "Client", lambda: client_mock)
    monkeypatch.setattr(
        flow_module,
        "_safe_persist_terminal_llm_usage_metadata",
        lambda persisted_run, **kwargs: (
            aggregation_calls.append(
                (str(persisted_run.id), kwargs.get("zenml_client"))
            )
            or True
        ),
    )
    monkeypatch.setattr(
        flow_module,
        "track",
        lambda event, _metadata: analytics_events.append(event),
    )

    FlowHandle(run, track_terminal_if_finished=True)

    assert analytics_events == [AnalyticsEvent.FLOW_TERMINAL]
    assert aggregation_calls == [("run-finished", client_mock)]
    client_mock.get_pipeline_run.assert_called_once_with(
        "run-finished",
        allow_name_prefix_match=False,
        project=None,
    )


def test_flow_handle_aggregates_fresh_run_even_when_summary_exists(
    monkeypatch,
) -> None:
    """A stale summary should not permanently block recomputation."""
    from kitaru._llm_usage import (
        LLM_USAGE_SUMMARY_METADATA_KEY,
        empty_usage_summary,
        serialize_summary_for_metadata,
        summary_to_flat_metadata,
    )

    flow_module = sys.modules["kitaru.flow"]
    summary = empty_usage_summary()
    fresh_run = cast(
        PipelineRunResponse,
        SimpleNamespace(
            id="run-1",
            run_metadata={
                LLM_USAGE_SUMMARY_METADATA_KEY: serialize_summary_for_metadata(summary),
                **summary_to_flat_metadata(summary),
            },
        ),
    )
    stale_run = cast(
        PipelineRunResponse,
        SimpleNamespace(id="run-1", run_metadata={}),
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = fresh_run
    monkeypatch.setattr(flow_module, "Client", lambda: client_mock)
    aggregation_calls: list[tuple[PipelineRunResponse, object]] = []

    def fake_safe(run: PipelineRunResponse, **kwargs: object) -> bool:
        aggregation_calls.append((run, kwargs.get("zenml_client")))
        return True

    monkeypatch.setattr(
        flow_module,
        "_safe_persist_terminal_llm_usage_metadata",
        fake_safe,
    )

    handle = FlowHandle(_as_pipeline_run(_DummyRun(status=ExecutionStatus.COMPLETED)))
    handle._persist_terminal_llm_usage_once(stale_run)
    handle._persist_terminal_llm_usage_once(stale_run)

    client_mock.get_pipeline_run.assert_called_once_with(
        "run-1",
        allow_name_prefix_match=False,
        project=None,
    )
    assert handle._run is fresh_run
    assert aggregation_calls == [(fresh_run, client_mock)]


def test_terminal_llm_usage_metadata_skips_write_when_summary_count_matches(
    monkeypatch,
) -> None:
    """A matching existing summary should suppress only the metadata write."""
    from kitaru._llm_usage import (
        LLM_USAGE_SUMMARY_METADATA_KEY,
        empty_usage_summary,
        serialize_summary_for_metadata,
        summary_to_flat_metadata,
    )
    from kitaru.flow import _persist_terminal_llm_usage_metadata

    terminal_usage_module = sys.modules["kitaru._terminal_usage"]

    fetch_calls = 0

    def fake_fetch(*, run: PipelineRunResponse, client: object) -> object:
        nonlocal fetch_calls
        fetch_calls += 1
        return {}

    monkeypatch.setattr(
        terminal_usage_module, "_list_checkpoint_attempts_for_run", fake_fetch
    )
    monkeypatch.setattr("kitaru.client.KitaruClient", lambda: SimpleNamespace())
    monkeypatch.setattr(
        "kitaru.logging.log_to_execution",
        lambda run_id, **metadata: (_ for _ in ()).throw(
            AssertionError("metadata write should not run")
        ),
    )

    summary = empty_usage_summary()
    persisted = _persist_terminal_llm_usage_metadata(
        cast(
            PipelineRunResponse,
            SimpleNamespace(
                id="run-1",
                run_metadata={
                    LLM_USAGE_SUMMARY_METADATA_KEY: serialize_summary_for_metadata(
                        summary
                    ),
                    **summary_to_flat_metadata(summary),
                },
            ),
        )
    )

    assert persisted is True
    assert fetch_calls == 1


def test_terminal_llm_usage_metadata_uses_zenml_client_without_kitaru_auth(
    monkeypatch,
) -> None:
    """Run-end aggregation must not construct the strict user-facing client."""
    from kitaru._llm_usage import (
        LLM_USAGE_METADATA_KEY,
        LLM_USAGE_SUMMARY_METADATA_KEY,
        build_usage_record,
        parse_usage_summary,
    )
    from kitaru.flow import _persist_terminal_llm_usage_metadata

    terminal_usage_module = sys.modules["kitaru._terminal_usage"]
    record = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="remote-env-call",
        total_tokens=11,
    )
    written: dict[str, Any] = {}

    monkeypatch.setenv("KITARU_SERVER_URL", "https://kitaru.example.test")
    monkeypatch.delenv("KITARU_AUTH_TOKEN", raising=False)
    monkeypatch.setattr(
        "kitaru.client.KitaruClient",
        lambda: (_ for _ in ()).throw(
            AssertionError("terminal aggregation must not construct KitaruClient")
        ),
    )
    monkeypatch.setattr(
        terminal_usage_module,
        "_list_checkpoint_attempts_for_run",
        lambda *, run, client: {},
    )

    def fake_log_to_execution(run_id: str, **metadata: Any) -> None:
        written.update(metadata)

    monkeypatch.setattr("kitaru.logging.log_to_execution", fake_log_to_execution)

    persisted = _persist_terminal_llm_usage_metadata(
        cast(
            PipelineRunResponse,
            SimpleNamespace(
                id="run-1",
                run_metadata={LLM_USAGE_METADATA_KEY: {"call": record}},
            ),
        ),
        zenml_client=SimpleNamespace(),
    )

    summary = parse_usage_summary(written[LLM_USAGE_SUMMARY_METADATA_KEY])
    assert persisted is True
    assert summary is not None
    assert summary["usage_record_count"] == 1
    assert summary["total_tokens"] == 11


def test_terminal_llm_usage_metadata_rewrites_stale_complete_summary(
    monkeypatch,
) -> None:
    """Visible attempt records should beat an old complete undercount."""
    from kitaru._llm_usage import (
        LLM_FLAT_USAGE_RECORD_COUNT_KEY,
        LLM_USAGE_METADATA_KEY,
        LLM_USAGE_SUMMARY_METADATA_KEY,
        build_usage_record,
        execution_metadata_from_records,
        parse_usage_summary,
    )
    from kitaru.flow import _persist_terminal_llm_usage_metadata

    first = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="visible-call-1",
        total_tokens=10,
    )
    second = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="visible-call-2",
        total_tokens=15,
    )
    stale_metadata = execution_metadata_from_records([first])
    attempts_by_lineage = {
        "checkpoint": [
            SimpleNamespace(
                id="attempt-1",
                name="checkpoint",
                status="completed",
                run_metadata={LLM_USAGE_METADATA_KEY: {"first": first}},
            ),
            SimpleNamespace(
                id="attempt-2",
                name="checkpoint",
                status="completed",
                run_metadata={LLM_USAGE_METADATA_KEY: {"second": second}},
            ),
        ]
    }
    written: dict[str, Any] = {}

    terminal_usage_module = sys.modules["kitaru._terminal_usage"]
    monkeypatch.setattr(
        terminal_usage_module,
        "_list_checkpoint_attempts_for_run",
        lambda *, run, client: attempts_by_lineage,
    )

    def fake_log_to_execution(run_id: str, **metadata: Any) -> None:
        written.update(metadata)

    monkeypatch.setattr("kitaru.logging.log_to_execution", fake_log_to_execution)

    persisted = _persist_terminal_llm_usage_metadata(
        cast(
            PipelineRunResponse,
            SimpleNamespace(id="run-1", run_metadata=stale_metadata),
        ),
        zenml_client=SimpleNamespace(),
    )

    summary = parse_usage_summary(written[LLM_USAGE_SUMMARY_METADATA_KEY])
    assert persisted is True
    assert summary is not None
    assert summary["usage_record_count"] == 2
    assert summary["total_tokens"] == 25
    assert written[LLM_FLAT_USAGE_RECORD_COUNT_KEY] == 2


def test_terminal_llm_usage_metadata_rewrites_same_count_stale_summary(
    monkeypatch,
) -> None:
    """A matching record count is not enough if totals changed."""
    from kitaru._llm_usage import (
        LLM_FLAT_TOTAL_TOKENS_KEY,
        LLM_USAGE_METADATA_KEY,
        LLM_USAGE_SUMMARY_METADATA_KEY,
        build_usage_record,
        execution_metadata_from_records,
        parse_usage_summary,
    )
    from kitaru.flow import _persist_terminal_llm_usage_metadata

    stale_record = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="same-count-call",
        total_tokens=10,
    )
    fresh_record = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="same-count-call",
        total_tokens=2000,
    )
    stale_metadata = execution_metadata_from_records([stale_record])
    attempts_by_lineage = {
        "checkpoint": [
            SimpleNamespace(
                id="attempt-1",
                name="checkpoint",
                status="completed",
                run_metadata={LLM_USAGE_METADATA_KEY: {"fresh": fresh_record}},
            )
        ]
    }
    written: dict[str, Any] = {}

    terminal_usage_module = sys.modules["kitaru._terminal_usage"]
    monkeypatch.setattr(
        terminal_usage_module,
        "_list_checkpoint_attempts_for_run",
        lambda *, run, client: attempts_by_lineage,
    )

    def fake_log_to_execution(run_id: str, **metadata: Any) -> None:
        written.update(metadata)

    monkeypatch.setattr("kitaru.logging.log_to_execution", fake_log_to_execution)

    persisted = _persist_terminal_llm_usage_metadata(
        cast(
            PipelineRunResponse,
            SimpleNamespace(id="run-1", run_metadata=stale_metadata),
        ),
        zenml_client=SimpleNamespace(),
    )

    summary = parse_usage_summary(written[LLM_USAGE_SUMMARY_METADATA_KEY])
    assert persisted is True
    assert summary is not None
    assert summary["usage_record_count"] == 1
    assert summary["total_tokens"] == 2000
    assert written[LLM_FLAT_TOTAL_TOKENS_KEY] == 2000


def test_terminal_llm_usage_metadata_does_not_skip_partial_summary(
    monkeypatch,
) -> None:
    """A schema marker alone is not enough to trust terminal aggregation."""
    from kitaru._llm_usage import (
        LLM_USAGE_METADATA_KEY,
        LLM_USAGE_SUMMARY_METADATA_KEY,
        build_usage_record,
        parse_usage_summary,
        serialize_summary_for_metadata,
    )
    from kitaru.flow import _persist_terminal_llm_usage_metadata

    terminal_usage_module = sys.modules["kitaru._terminal_usage"]
    record = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="after-partial-summary",
        total_tokens=9,
    )
    fetch_calls = 0
    written: dict[str, Any] = {}

    def fake_fetch(*, run: PipelineRunResponse, client: object) -> object:
        nonlocal fetch_calls
        fetch_calls += 1
        return {}

    def fake_log_to_execution(run_id: str, **metadata: Any) -> None:
        written.update(metadata)

    monkeypatch.setattr("kitaru.client.KitaruClient", lambda: SimpleNamespace())
    monkeypatch.setattr(
        terminal_usage_module, "_list_checkpoint_attempts_for_run", fake_fetch
    )
    monkeypatch.setattr("kitaru.logging.log_to_execution", fake_log_to_execution)

    persisted = _persist_terminal_llm_usage_metadata(
        cast(
            PipelineRunResponse,
            SimpleNamespace(
                id="run-1",
                run_metadata={
                    LLM_USAGE_METADATA_KEY: {"call": record},
                    LLM_USAGE_SUMMARY_METADATA_KEY: serialize_summary_for_metadata(
                        {"schema_version": 1}
                    ),
                },
            ),
        )
    )

    summary = parse_usage_summary(written[LLM_USAGE_SUMMARY_METADATA_KEY])
    assert persisted is True
    assert fetch_calls == 1
    assert summary is not None
    assert summary["usage_record_count"] == 1
    assert summary["total_tokens"] == 9


def test_terminal_llm_usage_metadata_marks_empty_successful_fetch_done(
    monkeypatch,
) -> None:
    """A no-LLM run should not be retried forever by the same handle."""
    from kitaru.flow import _persist_terminal_llm_usage_metadata

    terminal_usage_module = sys.modules["kitaru._terminal_usage"]
    fetch_calls = 0

    def fake_fetch(*, run: PipelineRunResponse, client: object) -> object:
        nonlocal fetch_calls
        fetch_calls += 1
        return {}

    monkeypatch.setattr("kitaru.client.KitaruClient", lambda: SimpleNamespace())
    monkeypatch.setattr(
        terminal_usage_module, "_list_checkpoint_attempts_for_run", fake_fetch
    )
    monkeypatch.setattr(
        "kitaru.logging.log_to_execution",
        lambda run_id, **metadata: (_ for _ in ()).throw(
            AssertionError("no metadata should be written")
        ),
    )

    handle = FlowHandle(_as_pipeline_run(_DummyRun(status=ExecutionStatus.COMPLETED)))
    run = cast(PipelineRunResponse, SimpleNamespace(id="run-1", run_metadata={}))
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = run
    monkeypatch.setattr(sys.modules["kitaru.flow"], "Client", lambda: client_mock)

    handle._persist_terminal_llm_usage_once(run)
    handle._persist_terminal_llm_usage_once(run)

    assert fetch_calls == 1
    client_mock.get_pipeline_run.assert_called_once_with(
        "run-1",
        allow_name_prefix_match=False,
        project=None,
    )
    assert _persist_terminal_llm_usage_metadata(run) is True


def test_terminal_llm_usage_metadata_counts_retry_attempts(monkeypatch) -> None:
    """Terminal aggregation counts separate retry attempts for the same call."""
    from kitaru._llm_usage import (
        LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY,
        LLM_FLAT_INCURRED_USAGE_RECORD_COUNT_KEY,
        LLM_USAGE_METADATA_KEY,
        LLM_USAGE_SUMMARY_METADATA_KEY,
        build_usage_record,
        parse_usage_summary,
    )
    from kitaru.flow import _persist_terminal_llm_usage_metadata

    first = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="summary_call",
        total_tokens=10,
    )
    second = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="summary_call",
        total_tokens=15,
    )
    attempts_by_lineage = {
        "summary_call": [
            SimpleNamespace(
                id="attempt-1",
                name="summary_call",
                status="completed",
                run_metadata={LLM_USAGE_METADATA_KEY: {"call": first}},
            ),
            SimpleNamespace(
                id="attempt-2",
                name="summary_call",
                status="completed",
                run_metadata={LLM_USAGE_METADATA_KEY: {"call": second}},
            ),
        ]
    }
    written: dict[str, Any] = {}

    terminal_usage_module = sys.modules["kitaru._terminal_usage"]
    monkeypatch.setattr("kitaru.client.KitaruClient", lambda: SimpleNamespace())
    monkeypatch.setattr(
        terminal_usage_module,
        "_list_checkpoint_attempts_for_run",
        lambda *, run, client: attempts_by_lineage,
    )

    def fake_log_to_execution(run_id: str, **metadata: Any) -> None:
        written.update(metadata)

    monkeypatch.setattr("kitaru.logging.log_to_execution", fake_log_to_execution)

    _persist_terminal_llm_usage_metadata(
        cast(PipelineRunResponse, SimpleNamespace(id="run-1", run_metadata={}))
    )

    summary = parse_usage_summary(written[LLM_USAGE_SUMMARY_METADATA_KEY])
    assert summary is not None
    assert summary["usage_record_count"] == 2
    assert written[LLM_FLAT_INCURRED_USAGE_RECORD_COUNT_KEY] == 2
    assert written[LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY] == 25


def test_terminal_llm_usage_metadata_includes_execution_metadata(monkeypatch) -> None:
    """Terminal aggregation includes flow-level usage records."""
    from kitaru._llm_usage import (
        LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY,
        LLM_FLAT_INCURRED_USAGE_RECORD_COUNT_KEY,
        LLM_USAGE_METADATA_KEY,
        LLM_USAGE_SUMMARY_METADATA_KEY,
        build_usage_record,
        parse_usage_summary,
    )
    from kitaru.flow import _persist_terminal_llm_usage_metadata

    record = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="flow-level-call",
        input_tokens=30,
        output_tokens=12,
    )
    written: dict[str, Any] = {}

    terminal_usage_module = sys.modules["kitaru._terminal_usage"]
    monkeypatch.setattr("kitaru.client.KitaruClient", lambda: SimpleNamespace())
    monkeypatch.setattr(
        terminal_usage_module,
        "_list_checkpoint_attempts_for_run",
        lambda *, run, client: {},
    )

    def fake_log_to_execution(run_id: str, **metadata: Any) -> None:
        written.update(metadata)

    monkeypatch.setattr("kitaru.logging.log_to_execution", fake_log_to_execution)

    _persist_terminal_llm_usage_metadata(
        cast(
            PipelineRunResponse,
            SimpleNamespace(
                id="run-1",
                run_metadata={LLM_USAGE_METADATA_KEY: {"flow-level-call": record}},
            ),
        )
    )

    summary = parse_usage_summary(written[LLM_USAGE_SUMMARY_METADATA_KEY])
    assert summary is not None
    assert summary["usage_record_count"] == 1
    assert summary["incurred_total_tokens"] == 42
    assert written[LLM_FLAT_INCURRED_USAGE_RECORD_COUNT_KEY] == 1
    assert written[LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY] == 42


def test_terminal_llm_usage_metadata_is_idempotent(monkeypatch) -> None:
    """Repeated terminal aggregation writes the same serialized summary."""
    from kitaru._llm_usage import (
        LLM_USAGE_METADATA_KEY,
        LLM_USAGE_SUMMARY_METADATA_KEY,
        build_usage_record,
        parse_usage_summary,
    )
    from kitaru.flow import _persist_terminal_llm_usage_metadata

    record = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="flow-level-call",
        input_tokens=3,
        output_tokens=4,
        estimated_cost_usd=0.01,
    )
    written: list[dict[str, Any]] = []

    terminal_usage_module = sys.modules["kitaru._terminal_usage"]
    monkeypatch.setattr("kitaru.client.KitaruClient", lambda: SimpleNamespace())
    monkeypatch.setattr(
        terminal_usage_module,
        "_list_checkpoint_attempts_for_run",
        lambda *, run, client: {},
    )

    def fake_log_to_execution(run_id: str, **metadata: Any) -> None:
        written.append(dict(metadata))

    monkeypatch.setattr("kitaru.logging.log_to_execution", fake_log_to_execution)
    run = cast(
        PipelineRunResponse,
        SimpleNamespace(
            id="run-1",
            run_metadata={LLM_USAGE_METADATA_KEY: {"flow-level-call": record}},
        ),
    )

    _persist_terminal_llm_usage_metadata(run)
    _persist_terminal_llm_usage_metadata(run)

    assert len(written) == 2
    assert written[0] == written[1]
    assert isinstance(written[0][LLM_USAGE_SUMMARY_METADATA_KEY], str)
    assert parse_usage_summary(
        written[0][LLM_USAGE_SUMMARY_METADATA_KEY]
    ) == parse_usage_summary(written[1][LLM_USAGE_SUMMARY_METADATA_KEY])


def test_terminal_llm_usage_metadata_marks_cached_attempts_reused(monkeypatch) -> None:
    """Cached checkpoint records should not count as new incurred spend."""
    from kitaru._llm_usage import (
        LLM_FLAT_ACTUAL_COST_USD_KEY,
        LLM_FLAT_DISPLAY_COST_USD_KEY,
        LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY,
        LLM_FLAT_REUSED_TOTAL_TOKENS_KEY,
        LLM_USAGE_METADATA_KEY,
        LLM_USAGE_SUMMARY_METADATA_KEY,
        build_usage_record,
        parse_usage_summary,
    )
    from kitaru.flow import _persist_terminal_llm_usage_metadata

    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="cached-call",
        total_tokens=100,
        actual_cost_usd=1.25,
        billing_effect="incurred",
        cache_status="executed",
    )
    attempts_by_lineage = {
        "cached_call": [
            SimpleNamespace(
                id="attempt-cached",
                name="cached_call",
                status="cached",
                run_metadata={LLM_USAGE_METADATA_KEY: {"cached-call": record}},
            )
        ]
    }
    written: dict[str, Any] = {}

    terminal_usage_module = sys.modules["kitaru._terminal_usage"]
    monkeypatch.setattr("kitaru.client.KitaruClient", lambda: SimpleNamespace())
    monkeypatch.setattr(
        terminal_usage_module,
        "_list_checkpoint_attempts_for_run",
        lambda *, run, client: attempts_by_lineage,
    )

    def fake_log_to_execution(run_id: str, **metadata: Any) -> None:
        written.update(metadata)

    monkeypatch.setattr("kitaru.logging.log_to_execution", fake_log_to_execution)

    _persist_terminal_llm_usage_metadata(
        cast(PipelineRunResponse, SimpleNamespace(id="run-1", run_metadata={}))
    )

    summary = parse_usage_summary(written[LLM_USAGE_SUMMARY_METADATA_KEY])
    assert summary is not None
    assert summary["usage_record_count"] == 1
    assert summary["incurred_usage_record_count"] == 0
    assert summary["reused_usage_record_count"] == 1
    assert written[LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY] == 0
    assert written[LLM_FLAT_REUSED_TOTAL_TOKENS_KEY] == 100
    assert written[LLM_FLAT_ACTUAL_COST_USD_KEY] == 0.0
    assert written[LLM_FLAT_DISPLAY_COST_USD_KEY] == 0.0


def test_terminal_llm_usage_metadata_preserves_replay_tail_incurred_record(
    monkeypatch,
) -> None:
    """Replay-like raw statuses should not hide provider calls that executed."""
    from kitaru._llm_usage import (
        LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY,
        LLM_FLAT_REUSED_TOTAL_TOKENS_KEY,
        LLM_USAGE_METADATA_KEY,
        LLM_USAGE_SUMMARY_METADATA_KEY,
        build_usage_record,
        parse_usage_summary,
    )
    from kitaru.flow import _persist_terminal_llm_usage_metadata
    from kitaru.replay import REPLAY_SKIPPED_STEPS_METADATA_KEY

    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="tail-call",
        total_tokens=37,
        billing_effect="incurred",
        cache_status="executed",
    )
    attempts_by_lineage = {
        "write": [
            SimpleNamespace(
                id="attempt-write",
                name="write",
                spec=SimpleNamespace(invocation_id="write"),
                status="replay_reused",
                run_metadata={LLM_USAGE_METADATA_KEY: {"tail-call": record}},
            )
        ]
    }
    written: dict[str, Any] = {}
    terminal_usage_module = sys.modules["kitaru._terminal_usage"]
    monkeypatch.setattr(
        terminal_usage_module,
        "_list_checkpoint_attempts_for_run",
        lambda *, run, client: attempts_by_lineage,
    )
    monkeypatch.setattr(
        "kitaru.logging.log_to_execution",
        lambda run_id, **metadata: written.update(metadata),
    )

    _persist_terminal_llm_usage_metadata(
        cast(
            PipelineRunResponse,
            SimpleNamespace(
                id="run-1",
                run_metadata={REPLAY_SKIPPED_STEPS_METADATA_KEY: ["fetch"]},
            ),
        )
    )

    summary = parse_usage_summary(written[LLM_USAGE_SUMMARY_METADATA_KEY])
    assert summary is not None
    assert summary["incurred_usage_record_count"] == 1
    assert summary["reused_usage_record_count"] == 0
    assert written[LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY] == 37
    assert written[LLM_FLAT_REUSED_TOTAL_TOKENS_KEY] == 0


def test_terminal_llm_usage_metadata_marks_replay_skipped_attempt_reused(
    monkeypatch,
) -> None:
    """The persisted replay skip list is direct evidence of replay reuse."""
    from kitaru._llm_usage import (
        LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY,
        LLM_FLAT_REUSED_TOTAL_TOKENS_KEY,
        LLM_USAGE_METADATA_KEY,
        LLM_USAGE_SUMMARY_METADATA_KEY,
        build_usage_record,
        parse_usage_summary,
    )
    from kitaru.flow import _persist_terminal_llm_usage_metadata
    from kitaru.replay import REPLAY_SKIPPED_STEPS_METADATA_KEY

    record = build_usage_record(
        adapter="openai_agents",
        surface="runner_call",
        record_id="upstream-call",
        total_tokens=37,
        billing_effect="incurred",
        cache_status="executed",
    )
    attempts_by_lineage = {
        "fetch": [
            SimpleNamespace(
                id="attempt-fetch",
                name="fetch",
                spec=SimpleNamespace(invocation_id="fetch"),
                status="replay_reused",
                run_metadata={LLM_USAGE_METADATA_KEY: {"upstream-call": record}},
            )
        ]
    }
    written: dict[str, Any] = {}
    terminal_usage_module = sys.modules["kitaru._terminal_usage"]
    monkeypatch.setattr(
        terminal_usage_module,
        "_list_checkpoint_attempts_for_run",
        lambda *, run, client: attempts_by_lineage,
    )
    monkeypatch.setattr(
        "kitaru.logging.log_to_execution",
        lambda run_id, **metadata: written.update(metadata),
    )

    _persist_terminal_llm_usage_metadata(
        cast(
            PipelineRunResponse,
            SimpleNamespace(
                id="run-1",
                run_metadata={REPLAY_SKIPPED_STEPS_METADATA_KEY: ["fetch"]},
            ),
        )
    )

    summary = parse_usage_summary(written[LLM_USAGE_SUMMARY_METADATA_KEY])
    assert summary is not None
    assert summary["incurred_usage_record_count"] == 0
    assert summary["reused_usage_record_count"] == 1
    assert written[LLM_FLAT_INCURRED_TOTAL_TOKENS_KEY] == 0
    assert written[LLM_FLAT_REUSED_TOTAL_TOKENS_KEY] == 37


def test_terminal_llm_usage_metadata_skips_when_attempt_fetch_fails(
    monkeypatch,
) -> None:
    """Do not write authoritative cost metadata from degraded attempt data."""
    from kitaru._llm_usage import LLM_USAGE_METADATA_KEY, build_usage_record
    from kitaru.flow import _persist_terminal_llm_usage_metadata

    record = build_usage_record(
        adapter="kitaru.llm",
        surface="direct_llm",
        record_id="flow-level-call",
        total_tokens=42,
    )
    written: dict[str, Any] = {}

    monkeypatch.setattr("kitaru.client.KitaruClient", lambda: SimpleNamespace())

    def fail_fetch(*, run: PipelineRunResponse, client: object) -> object:
        raise KitaruBackendError("attempt fetch failed")

    terminal_usage_module = sys.modules["kitaru._terminal_usage"]
    monkeypatch.setattr(
        terminal_usage_module, "_list_checkpoint_attempts_for_run", fail_fetch
    )

    def fake_log_to_execution(run_id: str, **metadata: Any) -> None:
        written.update(metadata)

    monkeypatch.setattr("kitaru.logging.log_to_execution", fake_log_to_execution)

    _persist_terminal_llm_usage_metadata(
        cast(
            PipelineRunResponse,
            SimpleNamespace(
                id="run-1",
                run_metadata={LLM_USAGE_METADATA_KEY: {"flow-level-call": record}},
            ),
        )
    )

    assert written == {}


def test_checkpoint_cache_survives_pipeline_with_options_when_execution_unset() -> None:
    """Mixed ``@checkpoint(cache=...)`` values must survive ``with_options``.

    A concrete ``enable_cache`` passed at run level would be applied per-step
    during ZenML compilation and overwrite the decorator-set value; the flow
    therefore omits ``enable_cache`` entirely when execution cache is unset.
    """

    @checkpoint(cache=False)
    def never_cache() -> int:
        return 1

    @checkpoint(cache=True)
    def always_cache() -> int:
        return 2

    @flow
    def mixed_flow() -> int:
        return never_cache() + always_cache()

    assert never_cache._step.enable_cache is False
    assert always_cache._step.enable_cache is True

    options = _build_pipeline_options(
        resolved_execution=_resolved_execution(cache=None),
        transport_image=None,
    )
    configured = mixed_flow._pipeline.with_options(**options)
    assert configured.configuration.enable_cache is None


@pytest.mark.parametrize("cache_value", [True, False])
def test_flow_run_forwards_explicit_cache_to_with_options(
    cache_value: bool,
) -> None:
    run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock(return_value=run)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(cache=cache_value),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
    ):
        flow(lambda x: x).run(123)

    call_kwargs = base_pipeline.with_options.call_args
    assert call_kwargs.kwargs["enable_cache"] is cache_value


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


def test_temporary_active_stack_clears_stale_env_before_explicit_activation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _EnvPoisonedActiveStackClient()
    monkeypatch.setenv(ENV_ZENML_ACTIVE_STACK_ID, "deleted-stack-id")

    with (
        patch("kitaru.flow.Client", return_value=client),
        _temporary_active_stack("prod"),
    ):
        assert os.environ[ENV_ZENML_ACTIVE_STACK_ID] == "prod-stack-id"
        assert client.active_stack_model.id == "prod-stack-id"

    assert os.environ[ENV_ZENML_ACTIVE_STACK_ID] == "deleted-stack-id"
    assert client.activate_stack.call_args_list == [
        call("prod"),
        call("saved-stack-id"),
    ]


def test_run_explicit_stack_uses_requested_stack_env_when_zenml_env_is_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _DummyRun(status=ExecutionStatus.RUNNING)

    def _submit_pipeline() -> _DummyRun:
        assert os.environ[ENV_ZENML_ACTIVE_STACK_ID] == "prod-stack-id"
        return run

    configured_pipeline = MagicMock(side_effect=_submit_pipeline)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    client = _EnvPoisonedActiveStackClient()
    monkeypatch.setenv(ENV_ZENML_ACTIVE_STACK_ID, "deleted-stack-id")

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client", return_value=client),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec") as persist_mock,
        patch("kitaru.flow._deployment_metadata_for_stack", return_value={}),
        patch("kitaru.flow._emit_kitaru_execution_url"),
        patch("kitaru.flow.track"),
    ):
        wrapped = flow(lambda: None)
        handle = wrapped.run(stack="prod")

    assert handle.exec_id == str(run.id)
    configured_pipeline.assert_called_once_with()
    persist_mock.assert_called_once()
    assert os.environ[ENV_ZENML_ACTIVE_STACK_ID] == "deleted-stack-id"
    assert client.activate_stack.call_args_list == [
        call("prod"),
        call("saved-stack-id"),
    ]


@pytest.mark.parametrize("exc_type", [KeyboardInterrupt, SystemExit])
def test_temporary_active_stack_does_not_swallow_base_exceptions(
    monkeypatch: pytest.MonkeyPatch,
    exc_type: type[BaseException],
) -> None:
    client = _EnvPoisonedActiveStackClient()
    monkeypatch.setenv(ENV_ZENML_ACTIVE_STACK_ID, "deleted-stack-id")

    with (
        patch("kitaru.flow.Client", return_value=client),
        pytest.raises(exc_type),
        _temporary_active_stack("prod"),
    ):
        raise exc_type("stop now")

    assert os.environ[ENV_ZENML_ACTIVE_STACK_ID] == "deleted-stack-id"
    assert client.activate_stack.call_args_list == [
        call("prod"),
        call("saved-stack-id"),
    ]


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


def _client_with_stale_previous_stack_restore() -> MagicMock:
    """Return a client whose saved previous stack id no longer exists."""
    client_mock = MagicMock()
    client_mock.active_stack_model = SimpleNamespace(id="2")

    def _activate_stack(stack_name_or_id: object) -> None:
        if stack_name_or_id == "2":
            raise RuntimeError("Stack with id=2 does not exist")

    client_mock.activate_stack.side_effect = _activate_stack
    return client_mock


def test_run_preserves_handle_if_previous_stack_restore_is_stale() -> None:
    """A successful submission should not fail because the old stack id vanished."""
    run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock(return_value=run)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    client_mock = _client_with_stale_previous_stack_restore()

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client", return_value=client_mock),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec") as persist_mock,
        patch("kitaru.flow._deployment_metadata_for_stack", return_value={}),
        patch("kitaru.flow._emit_kitaru_execution_url"),
        patch("kitaru.flow.track"),
        patch("kitaru.flow.logger.warning") as warning_mock,
    ):
        wrapped = flow(lambda: None)
        handle = wrapped.run(stack="prod")

    assert isinstance(handle, FlowHandle)
    assert handle.exec_id == str(run.id)
    configured_pipeline.assert_called_once_with()
    persist_mock.assert_called_once()
    assert client_mock.activate_stack.call_args_list == [call("prod"), call("2")]
    warning_mock.assert_called_once()
    warning_message = warning_mock.call_args.args[0]
    assert "Failed to restore previous active stack" in warning_message
    assert warning_mock.call_args.args[1] == "2"


def test_run_preserves_submission_error_if_stack_restore_also_fails() -> None:
    configured_pipeline = MagicMock(side_effect=RuntimeError("submission failed"))
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    client_mock = _client_with_stale_previous_stack_restore()

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
        patch("kitaru.flow._deployment_metadata_for_stack", return_value={}),
        patch("kitaru.flow.track"),
        patch("kitaru.flow.logger.warning") as warning_mock,
        pytest.raises(RuntimeError, match="submission failed") as exc_info,
    ):
        wrapped = flow(lambda: None)
        wrapped.run(stack="prod")

    assert str(exc_info.value) == "submission failed"
    assert client_mock.activate_stack.call_args_list == [call("prod"), call("2")]
    warning_mock.assert_called_once()


def test_run_propagates_requested_stack_activation_failure_before_submission() -> None:
    configured_pipeline = MagicMock()
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    client_mock = MagicMock()
    client_mock.active_stack_model = SimpleNamespace(id="old-stack-id")
    client_mock.activate_stack.side_effect = RuntimeError(
        "Stack with name=prod does not exist"
    )

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
        patch("kitaru.flow.logger.warning") as warning_mock,
        pytest.raises(RuntimeError, match="Stack with name=prod does not exist"),
    ):
        wrapped = flow(lambda: None)
        wrapped.run(stack="prod")

    configured_pipeline.assert_not_called()
    client_mock.activate_stack.assert_called_once_with("prod")
    warning_mock.assert_not_called()


def test_run_translates_active_stack_hydration_import_error() -> None:
    """Missing active-stack integration dependencies should fail before submit."""
    configured_pipeline = MagicMock()
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    class _ClientWithMissingStackDependency:
        def __init__(self) -> None:
            self.active_stack_model = SimpleNamespace(id=old_stack_id)
            self.activate_stack = MagicMock()

        @property
        def active_stack(self) -> object:
            raise ImportError(
                "Install the missing integration with "
                "`zenml integration install s3`.\n"
                "Export stack requirements with "
                "`zenml stack export-requirements 'prod' "
                "-o stack-requirements.txt`."
            )

    old_stack_id = uuid4()
    client_mock = _ClientWithMissingStackDependency()
    client_mock.active_stack_model = SimpleNamespace(id=old_stack_id)
    client_mock.activate_stack = MagicMock()

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client", return_value=client_mock),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec") as persist_mock,
        pytest.raises(KitaruStackIntegrationDependencyError) as exc_info,
    ):
        wrapped = flow(lambda: None)
        wrapped.run(stack="prod")

    message = str(exc_info.value)
    assert "Cannot submit this Kitaru flow" in message
    assert "stack integration dependency appears to be missing" in message
    assert "zenml integration install s3" in message
    assert "zenml stack export-requirements" in message
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True
    configured_pipeline.assert_not_called()
    persist_mock.assert_not_called()
    assert client_mock.activate_stack.call_args_list == [
        call("prod"),
        call(old_stack_id),
    ]


def test_run_does_not_translate_user_import_error_from_submission() -> None:
    """User-code ImportError during ZenML submission should remain raw."""
    configured_pipeline = MagicMock(
        side_effect=ImportError("No module named user_dependency")
    )
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    old_stack_id = uuid4()
    client_mock = MagicMock()
    client_mock.active_stack_model = SimpleNamespace(id=old_stack_id)
    client_mock.active_stack = object()

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
        pytest.raises(ImportError, match="user_dependency") as exc_info,
    ):
        wrapped = flow(lambda: None)
        wrapped.run(stack="prod")

    assert not isinstance(exc_info.value, KitaruStackIntegrationDependencyError)
    configured_pipeline.assert_called_once_with()
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


def test_run_logs_kitaru_native_execution_url() -> None:
    run = _DummyRun(
        status=ExecutionStatus.RUNNING,
        run_id="run-123",
        pipeline_id="flow-456",
    )
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
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(server_url="http://127.0.0.1:8383/"),
        ),
        patch(
            "kitaru.flow.resolve_ui_url_context",
            return_value=UiUrlContext(
                base_url="http://127.0.0.1:8383",
                route_kind="legacy",
                source="connection_config",
            ),
        ),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch("kitaru.flow.logger.info") as logger_info_mock,
    ):
        wrapped = flow(lambda x: x)
        wrapped.run(123)

    logger_info_mock.assert_any_call(
        "Execution URL: %s",
        "http://127.0.0.1:8383/flows/flow-456/executions/run-123",
    )


def test_run_logs_kitaru_pro_execution_url() -> None:
    run = _DummyRun(
        status=ExecutionStatus.RUNNING,
        run_id="run-123",
        pipeline_id="flow-456",
        project=SimpleNamespace(name="default"),
    )
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
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(server_url="http://127.0.0.1:8383/"),
        ),
        patch(
            "kitaru.flow.resolve_ui_url_context",
            return_value=UiUrlContext(
                base_url="https://staging.cloud.zenml.io",
                route_kind="pro",
                source="server_info",
                workspace="kitaru-dev",
            ),
        ),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch("kitaru.flow.logger.info") as logger_info_mock,
    ):
        wrapped = flow(lambda x: x)
        wrapped.run(123)

    logger_info_mock.assert_any_call(
        "Execution URL: %s",
        "https://staging.cloud.zenml.io/kitaru-workspaces/kitaru-dev"
        "/projects/default/flows/flow-456/v/local/executions/run-123",
    )


def test_run_skips_execution_url_when_ui_context_is_missing() -> None:
    run = _DummyRun(
        status=ExecutionStatus.RUNNING,
        run_id="run-123",
        pipeline_id="flow-456",
        project=SimpleNamespace(name="default"),
    )
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
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(
                server_url="https://67e44b28-zenml.staging.cloudinfra.zenml.io"
            ),
        ),
        patch("kitaru.flow.resolve_ui_url_context", return_value=None),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch("kitaru.flow.logger.info") as logger_info_mock,
    ):
        wrapped = flow(lambda x: x)
        wrapped.run(123)

    logger_info_mock.assert_not_called()


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


def test_replay_translates_active_stack_hydration_import_error_before_replay() -> None:
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    configured_pipeline = MagicMock()
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    replay_plan = ReplayPlan(
        original_run_id=str(source_run.id),
        steps_to_skip={"fetch"},
        input_overrides={},
        step_input_overrides={},
        runtime_context=ReplayRuntimeContext(at="write"),
    )
    old_stack_id = uuid4()
    client = _ClientWithMissingStackDependency(old_stack_id=old_stack_id)
    client.get_pipeline_run.return_value = source_run

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client", return_value=client),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec") as persist_mock,
        patch("kitaru.flow.build_replay_plan", return_value=replay_plan),
        patch("kitaru.flow.track") as track_mock,
        pytest.raises(KitaruStackIntegrationDependencyError) as exc_info,
    ):
        wrapped = flow(lambda topic: topic)
        wrapped.replay(str(source_run.id), at="write", stack="prod", wait=False)

    message = str(exc_info.value)
    assert "Cannot submit this Kitaru flow" in message
    assert "stack integration dependency appears to be missing" in message
    assert "zenml integration install s3" in message
    assert "zenml stack export-requirements" in message
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True
    configured_pipeline.replay.assert_not_called()
    persist_mock.assert_not_called()
    track_mock.assert_not_called()
    assert client.activate_stack.call_args_list == [
        call("prod"),
        call(old_stack_id),
    ]


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
        runtime_context=ReplayRuntimeContext(at="write"),
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
        patch(
            "kitaru.flow.safe_persist_replay_submission_metadata"
        ) as persist_replay_metadata_mock,
        patch("kitaru.flow.build_replay_plan", return_value=replay_plan),
    ):
        client_instance = client_cls.return_value
        client_instance.active_stack_model.id = "old-stack-id"
        client_instance.get_pipeline_run.return_value = source_run

        wrapped = flow(lambda topic: topic)
        submission = wrapped.replay(
            str(source_run.id),
            at="write",
            flow_overrides={"topic": "new topic"},
            invocation_overrides={"research": {"output": "edited"}},
            wait=False,
        )

    assert submission.results[0].replay_exec_id == str(replayed_run.id)
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
    persist_replay_metadata_mock.assert_called_once_with(
        replay_exec_id=str(replayed_run.id),
        original_exec_id=str(source_run.id),
        submission_id=submission.submission_id,
        tag=None,
        steps_to_skip={"fetch"},
    )
    build_frozen_spec_call = base_pipeline.with_options.call_args
    docker_settings = build_frozen_spec_call.kwargs["settings"]["docker"]
    assert docker_settings.requirements == ["kitaru"]
    assert (
        docker_settings.environment[KITARU_MODEL_REGISTRY_ENV]
        == _empty_registry_payload()
    )
    assert "KITARU_REPLAY_CONTEXT" in docker_settings.environment


def test_replay_preserves_unoverridden_recorded_flow_parameters() -> None:
    source_run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        parameters={"topic": "recorded topic", "model": "model-a"},
    )
    replayed_run = _DummyRun(status=ExecutionStatus.RUNNING)

    configured_pipeline = MagicMock()
    configured_pipeline.replay.return_value = replayed_run

    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    replay_plan = ReplayPlan(
        original_run_id=str(source_run.id),
        steps_to_skip=set(),
        input_overrides={"topic": "new topic", "model": "model-a"},
        step_input_overrides={},
        runtime_context=ReplayRuntimeContext(at="write"),
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client") as client_cls,
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="prod"),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch("kitaru.flow.safe_persist_replay_submission_metadata"),
        patch("kitaru.flow.build_replay_plan", return_value=replay_plan),
    ):
        client_instance = client_cls.return_value
        client_instance.active_stack_model.id = "old-stack-id"
        client_instance.get_pipeline_run.return_value = source_run

        wrapped = flow(lambda topic, model=None: (topic, model))
        wrapped.replay(
            str(source_run.id),
            at="write",
            flow_overrides={"topic": "new topic"},
            wait=False,
        )

    configured_pipeline.replay.assert_called_once_with(
        pipeline_run=source_run.id,
        skip=set(),
        skip_successful_steps=False,
        input_overrides={"topic": "new topic", "model": "model-a"},
        step_input_overrides=None,
    )


def test_replay_uses_resolved_project_for_source_lookup_and_replay_write() -> None:
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    replayed_run = _DummyRun(status=ExecutionStatus.RUNNING)
    project_events: list[tuple[str, str | None]] = []

    @contextmanager
    def _project_context(project: str | None) -> Iterator[None]:
        project_events.append(("enter", project))
        try:
            yield
        finally:
            project_events.append(("exit", project))

    def _replay(**_kwargs: Any) -> _DummyRun:
        assert project_events == [("enter", "production")]
        return replayed_run

    def _persist_spec(**_kwargs: Any) -> None:
        assert project_events == [("enter", "production")]

    def _persist_submission(**_kwargs: Any) -> None:
        assert project_events == [("enter", "production")]

    configured_pipeline = MagicMock()
    configured_pipeline.replay.side_effect = _replay
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    replay_plan = ReplayPlan(
        original_run_id=str(source_run.id),
        steps_to_skip=set(),
        input_overrides={},
        step_input_overrides={},
        runtime_context=ReplayRuntimeContext(at="write"),
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client") as client_cls,
        patch(
            "kitaru.flow.resolve_execution_config", return_value=_resolved_execution()
        ),
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(project="production"),
        ),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec", side_effect=_persist_spec),
        patch("kitaru.flow.build_replay_plan", return_value=replay_plan),
        patch(
            "kitaru.flow.safe_persist_replay_submission_metadata",
            side_effect=_persist_submission,
        ),
        patch("kitaru.flow._temporary_active_project", side_effect=_project_context),
        patch("kitaru.flow._temporary_active_stack", return_value=nullcontext()),
        patch("kitaru.flow._preflight_active_stack_implementation_hydration"),
    ):
        client_instance = client_cls.return_value
        client_instance.get_pipeline_run.return_value = source_run
        wrapped = flow(lambda topic: topic)
        submission = wrapped.replay("source-run-id", at="write", wait=False)

    client_instance.get_pipeline_run.assert_called_once_with(
        name_id_or_prefix="source-run-id",
        allow_name_prefix_match=False,
        hydrate=True,
        project="production",
    )
    replay_handle = submission.results[0].handle
    assert replay_handle is not None
    assert replay_handle._project == "production"
    assert project_events == [
        ("enter", "production"),
        ("exit", "production"),
    ]


def test_replay_sets_process_runtime_context_during_local_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    replayed_run = _DummyRun(status=ExecutionStatus.RUNNING)
    runtime_context = ReplayRuntimeContext(
        at="lookup_policy_tool",
        code_overrides={"lookup_policy_tool": "tests._replay_tool_stub.lookup_policy"},
    )
    replay_plan = ReplayPlan(
        original_run_id=str(source_run.id),
        steps_to_skip=set(),
        input_overrides={},
        step_input_overrides={},
        runtime_context=runtime_context,
    )

    monkeypatch.delenv(KITARU_REPLAY_CONTEXT_ENV, raising=False)
    get_replay_runtime_context.cache_clear()
    assert get_replay_runtime_context() is None

    def replay_side_effect(**_kwargs: Any) -> _DummyRun:
        raw_context = os.environ[KITARU_REPLAY_CONTEXT_ENV]
        parsed_context = ReplayRuntimeContext.from_json(raw_context)
        assert parsed_context.code_overrides == runtime_context.code_overrides
        cached_context = get_replay_runtime_context()
        assert cached_context is not None
        assert cached_context.code_overrides == runtime_context.code_overrides
        return replayed_run

    configured_pipeline = MagicMock()
    configured_pipeline.replay.side_effect = replay_side_effect

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
        patch("kitaru.flow.build_replay_plan", return_value=replay_plan),
    ):
        client_instance = client_cls.return_value
        client_instance.active_stack_model.id = "old-stack-id"
        client_instance.get_pipeline_run.return_value = source_run

        wrapped = flow(lambda topic: topic)
        wrapped.replay(str(source_run.id), at="lookup_policy_tool", wait=False)

    assert KITARU_REPLAY_CONTEXT_ENV not in os.environ
    assert get_replay_runtime_context() is None


def test_replay_logs_kitaru_native_execution_url() -> None:
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    replayed_run = _DummyRun(
        status=ExecutionStatus.RUNNING,
        run_id="replay-run-123",
        pipeline_id="flow-456",
    )

    configured_pipeline = MagicMock()
    configured_pipeline.replay.return_value = replayed_run

    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    replay_plan = ReplayPlan(
        original_run_id=str(source_run.id),
        steps_to_skip=set(),
        input_overrides={},
        step_input_overrides={},
        runtime_context=ReplayRuntimeContext(at="write"),
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client") as client_cls,
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(),
        ),
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(server_url="http://127.0.0.1:8383"),
        ),
        patch(
            "kitaru.flow.resolve_ui_url_context",
            return_value=UiUrlContext(
                base_url="http://127.0.0.1:8383",
                route_kind="legacy",
                source="connection_config",
            ),
        ),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch("kitaru.flow.build_replay_plan", return_value=replay_plan),
        patch("kitaru.flow.logger.info") as logger_info_mock,
    ):
        client_cls.return_value.get_pipeline_run.return_value = source_run
        client_cls.return_value.active_stack_model = SimpleNamespace(
            name="default",
            id="default-stack-id",
        )
        wrapped = flow(lambda topic: topic)
        wrapped.replay(str(source_run.id), at="write", wait=False)

    logger_info_mock.assert_any_call(
        "Execution URL: %s",
        "http://127.0.0.1:8383/flows/flow-456/executions/replay-run-123",
    )


def test_replay_forwards_secret_environment_from_to_with_options() -> None:
    """Replay submission must also thread secret refs through with_options."""
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    replayed_run = _DummyRun(status=ExecutionStatus.RUNNING)

    configured_pipeline = MagicMock()
    configured_pipeline.replay.return_value = replayed_run

    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    replay_plan = ReplayPlan(
        original_run_id=str(source_run.id),
        steps_to_skip=set(),
        input_overrides={},
        step_input_overrides={},
        runtime_context=ReplayRuntimeContext(at="write"),
    )

    resolved = _resolved_execution(
        stack="prod",
        image=ImageSettings(secret_environment_from=["openai-creds"]),
    )

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.Client") as client_cls,
        patch("kitaru.flow.resolve_execution_config", return_value=resolved),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch("kitaru.flow.build_replay_plan", return_value=replay_plan),
    ):
        client_instance = client_cls.return_value
        client_instance.active_stack_model.id = "old-stack-id"
        client_instance.get_pipeline_run.return_value = source_run

        wrapped = flow(lambda topic: topic)
        wrapped.replay(
            str(source_run.id),
            at="write",
            flow_overrides={"topic": "t"},
            wait=False,
        )

    call_kwargs = base_pipeline.with_options.call_args.kwargs
    assert call_kwargs["secrets"] == ["openai-creds"]
    docker_settings = call_kwargs["settings"]["docker"]
    assert (
        docker_settings.environment[KITARU_MODEL_REGISTRY_ENV]
        == _empty_registry_payload()
    )
    assert "KITARU_REPLAY_CONTEXT" in docker_settings.environment


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
                runtime_context=ReplayRuntimeContext(at="write"),
            ),
        ),
    ):
        client_instance = client_cls.return_value
        client_instance.active_stack_model.id = "old-stack-id"
        client_instance.get_pipeline_run.return_value = source_run

        wrapped = flow(stack="decorator-stack")(lambda topic: topic)
        wrapped.replay(
            str(source_run.id),
            at="write",
            stack="invocation-stack",
            wait=False,
        )

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
        wrapped.replay("run-123", at="write", wait=False)

    resolve_connection_mock.assert_called_once_with(validate_for_use=True)
    client_cls.return_value.get_pipeline_run.assert_not_called()


def test_replay_fails_closed_before_submitting_on_implicit_default_fallback() -> None:
    """Replay should fail closed before compile/submit on implicit fallback."""
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    base_pipeline = MagicMock()
    configured_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch(
            "kitaru.flow._capture_active_stack_provenance_for_guard",
            return_value=_stale_default_stack_provenance(),
        ),
        patch("kitaru.flow.Client") as client_cls,
        patch(
            "kitaru.flow.build_replay_plan",
            return_value=ReplayPlan(
                original_run_id=str(source_run.id),
                steps_to_skip=set(),
                input_overrides={},
                step_input_overrides={},
                runtime_context=ReplayRuntimeContext(at="write"),
            ),
        ),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(
                stack="default",
                stack_source="zenml_active_stack",
            ),
        ),
        patch("kitaru.flow.build_frozen_execution_spec") as build_spec_mock,
        pytest.raises(KitaruUsageError, match="fallback stack `default` implicitly"),
    ):
        client_cls.return_value.get_pipeline_run.return_value = source_run
        client_cls.return_value.active_stack_model = SimpleNamespace(
            name="default",
            id="default-stack-id",
        )
        wrapped = flow(lambda topic: topic)
        wrapped.replay(str(source_run.id), at="write", wait=False)

    build_spec_mock.assert_not_called()
    base_pipeline.with_options.assert_not_called()
    configured_pipeline.replay.assert_not_called()


def test_temporary_active_project_sets_env_and_restores_previous_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Temporary project activation should restore env and persisted state."""
    monkeypatch.setenv(ZENML_ACTIVE_PROJECT_ID_ENV, "previous-env-project")
    previous = SimpleNamespace(id="old-project-id", name="default")
    production = SimpleNamespace(id="prod-project-id", name="production")
    activation_order: list[str] = []

    class _ProjectClient:
        def __init__(self) -> None:
            self.active_project = previous

        def get_project(
            self,
            selector: str,
            *,
            allow_name_prefix_match: bool,
            hydrate: bool,
        ) -> object:
            assert (allow_name_prefix_match, hydrate) == (False, True)
            assert selector == "production"
            return production

        def set_active_project(self, project_id: str) -> object:
            activation_order.append(project_id)
            self.active_project = (
                production if project_id == "prod-project-id" else previous
            )
            return self.active_project

    client = _ProjectClient()

    with (
        patch("kitaru.flow.Client", return_value=client),
        _temporary_active_project("production"),
    ):
        assert os.environ[ZENML_ACTIVE_PROJECT_ID_ENV] == "prod-project-id"
        assert client.active_project is production

    assert os.environ[ZENML_ACTIVE_PROJECT_ID_ENV] == "previous-env-project"
    assert client.active_project is previous
    assert activation_order == ["prod-project-id", "old-project-id"]


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


def test_flow_handle_wait_raises_when_execution_is_waiting_for_input() -> None:
    run_id = uuid4()
    wait_condition = _dummy_wait_condition()
    waiting = _DummyRun(
        status=ExecutionStatus.PAUSED,
        run_id=run_id,
        resources=SimpleNamespace(active_wait_condition=wait_condition),
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = waiting
    client_mock.list_run_wait_conditions.return_value = SimpleNamespace(items=[])

    handle = FlowHandle(_as_pipeline_run(waiting))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        patch("kitaru.flow.time.sleep") as sleep_mock,
        pytest.raises(KitaruStateError) as exc_info,
    ):
        handle.wait()

    message = str(exc_info.value)
    assert str(run_id) in message
    assert "waiting for input" in message
    assert f"kitaru executions input {run_id} --value" in message
    assert "'<json>'" in message
    assert f"kitaru executions resume {run_id}" not in message
    client_mock.list_run_wait_conditions.assert_called_once_with(
        pipeline_run=run_id,
        project=None,
        status="pending",
        hydrate=True,
        sort_by="asc:created",
        size=200,
    )
    sleep_mock.assert_not_called()


def test_flow_handle_wait_paused_without_pending_waits_guides_resume() -> None:
    run_id = uuid4()
    waiting = _DummyRun(status=ExecutionStatus.PAUSED, run_id=run_id)
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = waiting
    client_mock.list_run_wait_conditions.return_value = SimpleNamespace(items=[])

    handle = FlowHandle(_as_pipeline_run(waiting))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        patch("kitaru.flow.time.sleep") as sleep_mock,
        pytest.raises(KitaruStateError) as exc_info,
    ):
        handle.wait()

    message = str(exc_info.value)
    assert str(run_id) in message
    assert "found no pending wait input" in message
    assert f"kitaru executions resume {run_id}" in message
    assert f"kitaru executions input {run_id}" not in message
    sleep_mock.assert_not_called()


def test_flow_handle_wait_pending_wait_lookup_failure_gives_cautious_guidance() -> None:
    run_id = uuid4()
    waiting = _DummyRun(status=ExecutionStatus.PAUSED, run_id=run_id)
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = waiting
    client_mock.list_run_wait_conditions.side_effect = RuntimeError(
        "backend unavailable"
    )

    handle = FlowHandle(_as_pipeline_run(waiting))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        patch("kitaru.flow.time.sleep") as sleep_mock,
        pytest.raises(KitaruStateError) as exc_info,
    ):
        handle.wait()

    message = str(exc_info.value)
    assert str(run_id) in message
    assert "could not determine whether it has pending wait input" in message
    assert "backend unavailable" in message
    assert f"kitaru executions input {run_id} --value '<json>'" in message
    assert f"kitaru executions resume {run_id}" in message
    assert isinstance(exc_info.value.__cause__, KitaruBackendError)
    sleep_mock.assert_not_called()


def test_flow_handle_status_returns_kitaru_execution_status() -> None:
    running = _DummyRun(status=ExecutionStatus.RUNNING)
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = running

    handle = FlowHandle(_as_pipeline_run(running))
    with patch("kitaru.flow.Client", return_value=client_mock):
        status = handle.status

    assert status == KitaruExecutionStatus.RUNNING
    assert isinstance(status, KitaruExecutionStatus)
    assert status.is_finished is False
    assert status.is_successful is False


def test_flow_handle_refresh_uses_handle_project() -> None:
    running = _DummyRun(status=ExecutionStatus.RUNNING)
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = running

    handle = FlowHandle(_as_pipeline_run(running), project="production")
    with patch("kitaru.flow.Client", return_value=client_mock):
        assert handle.status == KitaruExecutionStatus.RUNNING

    client_mock.get_pipeline_run.assert_called_once_with(
        running.id,
        allow_name_prefix_match=False,
        project="production",
    )


def test_execution_status_compatibility_helpers() -> None:
    assert KitaruExecutionStatus.RUNNING.is_finished is False
    assert KitaruExecutionStatus.WAITING.is_finished is False
    assert KitaruExecutionStatus.COMPLETED.is_finished is True
    assert KitaruExecutionStatus.FAILED.is_finished is True
    assert KitaruExecutionStatus.CANCELLED.is_finished is True

    assert KitaruExecutionStatus.RUNNING.is_successful is False
    assert KitaruExecutionStatus.WAITING.is_successful is False
    assert KitaruExecutionStatus.FAILED.is_successful is False
    assert KitaruExecutionStatus.CANCELLED.is_successful is False
    assert KitaruExecutionStatus.COMPLETED.is_successful is True


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
    assert exc_info.value.status == KitaruExecutionStatus.FAILED
    assert isinstance(exc_info.value.status, KitaruExecutionStatus)
    assert exc_info.value.failure_origin == FailureOrigin.USER_CODE


def test_flow_handle_get_classifies_result_save_failure_as_runtime() -> None:
    """Kitaru's internal result save failures should not blame user code."""
    failed = _DummyRun(
        status=ExecutionStatus.FAILED,
        status_reason="Kitaru could not persist the flow return value",
        traceback=(
            "Traceback\n"
            "kitaru.errors.KitaruRuntimeError: Kitaru could not persist "
            "the flow return value as a ZenML artifact. The user flow returned "
            "successfully, but the backend artifact save failed after user "
            "code returned. If ZenML retries this flow body, non-idempotent "
            "side effects in the flow may run again: store down"
        ),
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = failed

    handle = FlowHandle(_as_pipeline_run(failed))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        pytest.raises(KitaruExecutionError, match="could not persist") as exc_info,
    ):
        handle.get()

    assert not isinstance(exc_info.value, KitaruUserCodeError)
    assert exc_info.value.failure_origin == FailureOrigin.RUNTIME


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


def test_flow_result_extraction_run_output_none_beats_terminal_fallback() -> None:
    """An explicit run-level ``None`` beats a discarded terminal checkpoint."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("one_value", "output", "solo")],
        run_outputs=[("output", None)],
    )
    run.snapshot.pipeline_spec.outputs = []

    assert _extract_flow_result(_as_pipeline_run(run)) is None


def test_flow_result_linked_none_beats_terminal_fallback() -> None:
    """A linked explicit ``None`` beats legacy single-terminal inference."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("one_value", "output", "solo")],
    )
    run.snapshot.pipeline_spec.outputs = []
    run.run_metadata = {_FLOW_RESULT_REF_METADATA_KEY: "result-art-id"}

    client_mock = MagicMock()
    client_mock.get_artifact_version.return_value = _DummyArtifact(None)

    with patch("kitaru.flow.Client", return_value=client_mock):
        result = _extract_flow_result(_as_pipeline_run(run))

    assert result is None
    client_mock.get_artifact_version.assert_called_once_with(
        "result-art-id", project=None
    )


def test_flow_result_linked_plain_value_beats_terminal_fallback() -> None:
    """Any linked plain flow result beats legacy single-terminal inference."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("one_value", "output", "solo")],
    )
    run.snapshot.pipeline_spec.outputs = []
    run.run_metadata = {_FLOW_RESULT_REF_METADATA_KEY: "result-art-id"}

    client_mock = MagicMock()
    client_mock.get_artifact_version.return_value = _DummyArtifact({"answer": 42})

    with patch("kitaru.flow.Client", return_value=client_mock):
        result = _extract_flow_result(_as_pipeline_run(run))

    assert result == {"answer": 42}
    client_mock.get_artifact_version.assert_called_once_with(
        "result-art-id", project=None
    )


def test_flow_result_recovered_from_metadata_when_terminals_non_candidate() -> None:
    """Explicit linked flow results are recovered before terminal inference.

    An adapter can create several non-result model/tool checkpoints. The flow's
    real return value is recovered from the ``kitaru_flow_result`` artifact
    linked in execution metadata before Kitaru tries to infer from terminals.
    """
    non_candidate = {"kitaru": {"flow_result_candidate": False}}
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            _DummyOutput("model_a", "output", "resp-a", config_extra=non_candidate),
            _DummyOutput("model_b", "output", "resp-b", config_extra=non_candidate),
        ],
    )
    run.snapshot.pipeline_spec.outputs = []
    _add_linked_flow_result_metadata(run)

    client_mock = _linked_flow_result_client({"answer": "the real return value"})

    with patch("kitaru.flow.Client", return_value=client_mock):
        result = _extract_flow_result(_as_pipeline_run(run))

    assert result == {"answer": "the real return value"}
    _assert_linked_flow_result_loaded(client_mock)


def test_linked_flow_result_wins_over_unique_terminal_candidate() -> None:
    """The saved flow return beats a terminal step that heuristics could choose."""
    non_candidate = {"kitaru": {"flow_result_candidate": False}}
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            _DummyOutput("model_call", "output", "model", config_extra=non_candidate),
            _DummyOutput("tool_call", "output", "tool", spec_extra=non_candidate),
            ("finalize", "output", "heuristic"),
        ],
    )
    run.snapshot.pipeline_spec.outputs = []
    _add_linked_flow_result_metadata(run)

    client_mock = _linked_flow_result_client("linked result")

    with patch("kitaru.flow.Client", return_value=client_mock):
        result = _extract_flow_result(_as_pipeline_run(run))

    assert result == "linked result"
    _assert_linked_flow_result_loaded(client_mock)


def test_linked_flow_result_wins_over_single_non_candidate_terminal() -> None:
    """A linked flow result beats the legacy single-terminal fallback."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            _DummyOutput(
                "adapter_call",
                "output",
                "synthetic",
                config_extra={"kitaru": {"flow_result_candidate": False}},
            ),
        ],
    )
    run.snapshot.pipeline_spec.outputs = []
    _add_linked_flow_result_metadata(run)

    client_mock = _linked_flow_result_client("linked result")

    with patch("kitaru.flow.Client", return_value=client_mock):
        result = _extract_flow_result(_as_pipeline_run(run))

    assert result == "linked result"
    _assert_linked_flow_result_loaded(client_mock)


def test_linked_flow_result_wins_over_zero_output_terminal() -> None:
    """A linked flow result returns before a terminal step with no outputs errors."""
    run = _zero_output_terminal_run()
    _add_linked_flow_result_metadata(run)

    client_mock = _linked_flow_result_client("linked result")

    with patch("kitaru.flow.Client", return_value=client_mock):
        result = _extract_flow_result(_as_pipeline_run(run))

    assert result == "linked result"
    _assert_linked_flow_result_loaded(client_mock)


def test_broken_linked_flow_result_does_not_fall_back_to_terminal_output() -> None:
    """Broken linked metadata raises instead of returning a heuristic terminal."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("finalize", "output", "heuristic fallback")],
    )
    run.snapshot.pipeline_spec.outputs = []
    _add_linked_flow_result_metadata(run)

    client_mock = MagicMock()
    client_mock.get_artifact_version.return_value.load.side_effect = RuntimeError(
        "artifact backend unavailable"
    )

    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        pytest.raises(KitaruRuntimeError, match="Could not load the linked"),
    ):
        _extract_flow_result(_as_pipeline_run(run))

    _assert_linked_flow_result_loaded(client_mock)


def test_flow_result_none_marker_wins_over_unique_terminal_candidate() -> None:
    """An explicit None return beats a terminal checkpoint heuristic."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            _DummyOutput(
                "adapter_call",
                "output",
                "synthetic",
                config_extra={"kitaru": {"flow_result_candidate": False}},
            ),
            ("finalize", "output", "heuristic fallback"),
        ],
    )
    run.snapshot.pipeline_spec.outputs = []
    _add_flow_result_none_metadata(run)

    assert _extract_flow_result(_as_pipeline_run(run)) is None


def test_flow_result_none_marker_wins_over_zero_output_terminal() -> None:
    """An explicit None return beats a terminal checkpoint error path."""
    run = _zero_output_terminal_run()
    _add_flow_result_none_metadata(run)

    assert _extract_flow_result(_as_pipeline_run(run)) is None


def test_linked_flow_result_ref_beats_none_marker() -> None:
    """If both markers exist, the explicit artifact reference wins."""
    run = _DummyRun(status=ExecutionStatus.COMPLETED)
    run.run_metadata = {
        _FLOW_RESULT_REF_METADATA_KEY: "result-art-id",
        _FLOW_RESULT_NONE_METADATA_KEY: True,
    }

    client_mock = _linked_flow_result_client("linked result")

    with patch("kitaru.flow.Client", return_value=client_mock):
        result = _extract_flow_result(_as_pipeline_run(run))

    assert result == "linked result"
    _assert_linked_flow_result_loaded(client_mock)


def test_flow_result_run_outputs_beat_none_marker() -> None:
    """Run outputs stay more explicit than the None marker."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        run_outputs=[("final_output", "run output")],
    )
    _add_flow_result_none_metadata(run)

    assert _extract_flow_result(_as_pipeline_run(run)) == "run output"


def test_flow_result_output_specs_beat_none_marker() -> None:
    """Declared output specs stay more explicit than the None marker."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("declared_step", "output", "declared result")],
    )
    _add_flow_result_none_metadata(run)

    assert _extract_flow_result(_as_pipeline_run(run)) == "declared result"


def test_flow_result_without_none_marker_uses_terminal_fallback() -> None:
    """Old runs without the None marker keep using terminal inference."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("finalize", "output", "heuristic fallback")],
    )
    run.snapshot.pipeline_spec.outputs = []

    assert _extract_flow_result(_as_pipeline_run(run)) == "heuristic fallback"


def test_flow_result_ambiguous_terminals_still_raise_without_reference() -> None:
    """With no linked result, ambiguous terminals raise as before (no regression)."""
    non_candidate = {"kitaru": {"flow_result_candidate": False}}
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            _DummyOutput("model_a", "output", "resp-a", config_extra=non_candidate),
            _DummyOutput("model_b", "output", "resp-b", config_extra=non_candidate),
        ],
    )
    run.snapshot.pipeline_spec.outputs = []
    run.run_metadata = {}

    with (
        patch("kitaru.flow.Client", return_value=MagicMock()),
        pytest.raises(KitaruRuntimeError) as exc_info,
    ):
        _extract_flow_result(_as_pipeline_run(run))

    assert _is_multiple_terminal_steps_output_error(exc_info.value)


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


@pytest.mark.parametrize(
    ("step_run", "expected"),
    [
        (SimpleNamespace(), True),
        (
            SimpleNamespace(
                config=SimpleNamespace(
                    extra={"kitaru": {"flow_result_candidate": False}}
                )
            ),
            False,
        ),
        (
            SimpleNamespace(
                spec=SimpleNamespace(extra={"kitaru": {"flow_result_candidate": False}})
            ),
            False,
        ),
        (
            SimpleNamespace(
                config=SimpleNamespace(extra={"kitaru": "malformed"}),
                spec=SimpleNamespace(
                    extra={"kitaru": {"flow_result_candidate": False}}
                ),
            ),
            False,
        ),
        (
            SimpleNamespace(
                config=SimpleNamespace(extra={"kitaru": {"type": "llm_call"}}),
                spec=SimpleNamespace(
                    extra={"kitaru": {"flow_result_candidate": False}}
                ),
            ),
            False,
        ),
        (
            SimpleNamespace(
                config=SimpleNamespace(
                    extra={"kitaru": {"flow_result_candidate": "false"}}
                )
            ),
            True,
        ),
        (
            SimpleNamespace(
                config=SimpleNamespace(
                    extra={"other": {"flow_result_candidate": False}}
                )
            ),
            True,
        ),
    ],
)
def test_flow_result_candidate_step_reads_kitaru_extra(
    step_run: object,
    expected: bool,
) -> None:
    assert _is_flow_result_candidate_step(step_run) is expected


def test_flow_handle_get_terminal_fallback_uses_graph_sink_candidate() -> None:
    completed = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            _DummyOutput(
                step_name="adapter_call",
                output_name="output",
                value="synthetic",
                config_extra={"kitaru": {"flow_result_candidate": False}},
            ),
            _DummyOutput(
                step_name="finalize",
                output_name="output",
                value="done",
                upstream_steps=["adapter_call"],
            ),
        ],
    )
    completed.snapshot.pipeline_spec.outputs = []

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    handle = FlowHandle(_as_pipeline_run(completed))
    with patch("kitaru.flow.Client", return_value=client_mock):
        result = handle.get()

    assert result == "done"


def test_flow_handle_get_uses_unique_terminal_result_candidate() -> None:
    completed = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            _DummyOutput(
                step_name="adapter_call",
                output_name="output",
                value="synthetic",
                config_extra={"kitaru": {"flow_result_candidate": False}},
            ),
            _DummyOutput(
                step_name="adapter_tool_call",
                output_name="output",
                value="tool result",
                spec_extra={"kitaru": {"flow_result_candidate": False}},
            ),
            ("finalize", "output", "done"),
        ],
    )
    completed.snapshot.pipeline_spec.outputs = []

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    handle = FlowHandle(_as_pipeline_run(completed))
    with patch("kitaru.flow.Client", return_value=client_mock):
        result = handle.get()

    assert result == "done"


def test_flow_handle_get_single_terminal_non_candidate_still_returns() -> None:
    completed = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            _DummyOutput(
                step_name="adapter_call",
                output_name="output",
                value="synthetic",
                config_extra={"kitaru": {"flow_result_candidate": False}},
            ),
        ],
    )
    completed.snapshot.pipeline_spec.outputs = []

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    handle = FlowHandle(_as_pipeline_run(completed))
    with patch("kitaru.flow.Client", return_value=client_mock):
        result = handle.get()

    assert result == "synthetic"


def test_flow_handle_get_raises_when_terminal_filter_leaves_zero_candidates() -> None:
    completed = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            _DummyOutput(
                step_name="adapter_a",
                output_name="output",
                value="a",
                config_extra={"kitaru": {"flow_result_candidate": False}},
            ),
            _DummyOutput(
                step_name="adapter_b",
                output_name="output",
                value="b",
                spec_extra={"kitaru": {"flow_result_candidate": False}},
            ),
        ],
    )
    completed.snapshot.pipeline_spec.outputs = []

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    handle = FlowHandle(_as_pipeline_run(completed))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        pytest.raises(KitaruAmbiguousFlowResultError) as exc_info,
    ):
        handle.get()

    assert _is_multiple_terminal_steps_output_error(exc_info.value)


def test_zero_output_terminal_still_raises_without_linked_result() -> None:
    """Old/no-link runs still error when the selected terminal has no outputs."""
    run = _zero_output_terminal_run()

    with pytest.raises(KitaruRuntimeError, match="no regular outputs"):
        _extract_flow_result(_as_pipeline_run(run))


def test_flow_handle_get_output_specs_ignore_candidate_metadata() -> None:
    completed = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            _DummyOutput(
                step_name="adapter_call",
                output_name="output",
                value="synthetic",
                config_extra={"kitaru": {"flow_result_candidate": False}},
            ),
            ("finalize", "output", "done"),
        ],
    )

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    handle = FlowHandle(_as_pipeline_run(completed))
    with patch("kitaru.flow.Client", return_value=client_mock):
        result = handle.get()

    assert result == ("synthetic", "done")


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
        pytest.raises(KitaruAmbiguousFlowResultError) as exc_info,
    ):
        handle.get()
    assert _is_multiple_terminal_steps_output_error(exc_info.value)

    message = str(exc_info.value)
    assert "final_a" in message and "final_b" in message
    assert "KitaruClient" in message


def test_flow_handle_get_ambiguous_terminal_outputs_message_lists_outputs() -> None:
    completed = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[
            ("final", "output_a", "a"),
            ("final", "output_b", "b"),
        ],
    )
    completed.snapshot.pipeline_spec.outputs = []

    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = completed

    handle = FlowHandle(_as_pipeline_run(completed))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        pytest.raises(KitaruAmbiguousFlowResultError) as exc_info,
    ):
        handle.get()

    message = str(exc_info.value)
    assert "'final'" in message
    assert "output_a" in message and "output_b" in message


def test_flow_ambiguous_flow_result_error_subclasses_runtime_error() -> None:
    """Callers using `except KitaruRuntimeError` continue to catch ambiguity,
    but more specific code can target `KitaruAmbiguousFlowResultError` to
    avoid swallowing real execution failures.
    """
    assert issubclass(KitaruAmbiguousFlowResultError, KitaruRuntimeError)
    assert not issubclass(KitaruExecutionError, KitaruAmbiguousFlowResultError)


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

    with (
        patch(
            "kitaru.runtime.DynamicPipelineRunContext.get",
            return_value=SimpleNamespace(
                run=SimpleNamespace(
                    id="exec-123",
                    pipeline=SimpleNamespace(id="flow-abc", name="_user_flow"),
                ),
                pipeline=SimpleNamespace(id=None, name=None),
            ),
        ),
        patch(
            "kitaru.flow._coerce_flow_return_for_zenml",
            side_effect=lambda value: value,
        ),
    ):
        result = wrapped()

    assert result == "ok"
    assert not _is_inside_flow()
    assert _get_current_flow() is None


def test_public_current_execution_id_reads_flow_scope_only() -> None:
    def _user_flow() -> str | None:
        return kitaru.current_execution_id()

    wrapped = _wrap_flow_entrypoint(_user_flow)

    with (
        patch(
            "kitaru.runtime.DynamicPipelineRunContext.get",
            return_value=SimpleNamespace(
                run=SimpleNamespace(
                    id="exec-public-123",
                    pipeline=SimpleNamespace(id="flow-abc", name="_user_flow"),
                ),
                pipeline=SimpleNamespace(id=None, name=None),
            ),
        ),
        patch(
            "kitaru.flow._coerce_flow_return_for_zenml",
            side_effect=lambda value: value,
        ),
    ):
        result = wrapped()

    assert result == "exec-public-123"
    assert kitaru.current_execution_id() is None


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


def test_flow_wrapper_records_explicit_none_with_discovered_execution_id() -> None:
    def _user_flow() -> None:
        return None

    wrapped = _wrap_flow_entrypoint(_user_flow)
    run_context = SimpleNamespace(
        run=SimpleNamespace(
            id="exec-none-123",
            pipeline=SimpleNamespace(id="flow-abc", name="_user_flow"),
        ),
        pipeline=SimpleNamespace(id=None, name=None),
    )

    with (
        patch("kitaru.runtime.StepContext.get", return_value=None),
        patch("kitaru.runtime.DynamicPipelineRunContext.get", return_value=run_context),
        patch("kitaru.logging.log") as log_mock,
    ):
        result = wrapped()

    assert result is None
    log_mock.assert_called_once_with(**{_FLOW_RESULT_NONE_METADATA_KEY: True})
    assert not _is_inside_flow()


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
        patch("kitaru._telemetry.classify_stack_deployment_type", return_value="local"),
        patch("kitaru.flow.track") as track_mock,
    ):
        wrapped = flow(lambda x: x)
        wrapped.run(123)

    assert track_mock.call_args_list == [
        call(
            AnalyticsEvent.FLOW_ATTEMPTED,
            {"submission_path": "flow_wrapper"},
        ),
        call(
            AnalyticsEvent.FLOW_SUBMITTED,
            {
                "kitaru_deployment_type": "local",
                "deployment_type_source": "kitaru_stack_inference",
            },
        ),
    ]


def test_submit_emits_terminal_event_when_run_already_completed() -> None:
    """_submit should emit FLOW_TERMINAL immediately for already terminal runs."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("step", "output", 42)],
    )
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
        patch("kitaru._telemetry.classify_stack_deployment_type", return_value="local"),
        patch("kitaru.flow.track") as track_mock,
    ):
        wrapped = flow(lambda x: x)
        wrapped.run(123)

    assert track_mock.call_count == 3
    attempted_call, submitted_call, terminal_call = track_mock.call_args_list
    assert attempted_call.args[0] == AnalyticsEvent.FLOW_ATTEMPTED
    assert submitted_call.args[0] == AnalyticsEvent.FLOW_SUBMITTED
    assert terminal_call.args[0] == AnalyticsEvent.FLOW_TERMINAL
    terminal_metadata = terminal_call.args[1]
    assert terminal_metadata["status"] == ExecutionStatus.COMPLETED.value
    assert terminal_metadata["kitaru_deployment_type"] == "local"
    assert terminal_metadata["deployment_type_source"] == "kitaru_stack_inference"


def test_submit_emits_terminal_event_with_failure_origin_when_run_already_failed() -> (
    None
):
    """_submit terminal telemetry should include failure origin for failed runs."""
    run = _DummyRun(
        status=ExecutionStatus.FAILED,
        status_reason="user error",
        traceback="Traceback\nValueError: boom",
    )
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
        patch("kitaru._telemetry.classify_stack_deployment_type", return_value="local"),
        patch("kitaru.flow.track") as track_mock,
    ):
        wrapped = flow(lambda x: x)
        wrapped.run(123)

    assert track_mock.call_count == 3
    attempted_call, submitted_call, terminal_call = track_mock.call_args_list
    assert attempted_call.args[0] == AnalyticsEvent.FLOW_ATTEMPTED
    assert submitted_call.args[0] == AnalyticsEvent.FLOW_SUBMITTED
    assert terminal_call.args[0] == AnalyticsEvent.FLOW_TERMINAL
    terminal_metadata = terminal_call.args[1]
    assert terminal_metadata["status"] == ExecutionStatus.FAILED.value
    assert terminal_metadata["failure_origin"] == FailureOrigin.USER_CODE.value


def test_submit_time_terminal_event_is_not_reemitted_by_wait_or_get() -> None:
    """Submit-time FLOW_TERMINAL should not be duplicated by wait()/get()."""
    run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("step", "output", 42)],
    )
    configured_pipeline = MagicMock(return_value=run)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = run

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch("kitaru._telemetry.classify_stack_deployment_type", return_value="local"),
        patch("kitaru.flow.Client", return_value=client_mock),
        patch("kitaru.flow.track") as track_mock,
    ):
        wrapped = flow(lambda x: x)
        handle = wrapped.run(123)
        handle.wait()
        handle.get()

    events = [call_args.args[0] for call_args in track_mock.call_args_list]
    assert events.count(AnalyticsEvent.FLOW_ATTEMPTED) == 1
    assert events.count(AnalyticsEvent.FLOW_SUBMITTED) == 1
    assert events.count(AnalyticsEvent.FLOW_TERMINAL) == 1


def test_submit_defers_terminal_event_when_run_still_running() -> None:
    """_submit should only emit FLOW_SUBMITTED for non-terminal runs."""
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
        patch("kitaru._telemetry.classify_stack_deployment_type", return_value="local"),
        patch("kitaru.flow.track") as track_mock,
    ):
        wrapped = flow(lambda x: x)
        wrapped.run(123)

    assert [call_args.args[0] for call_args in track_mock.call_args_list] == [
        AnalyticsEvent.FLOW_ATTEMPTED,
        AnalyticsEvent.FLOW_SUBMITTED,
    ]


def test_submit_classification_failure_does_not_break_flow_execution() -> None:
    """Deployment classification failures should become unknown metadata only."""
    run = _DummyRun(status=ExecutionStatus.RUNNING)
    configured_pipeline = MagicMock(return_value=run)
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="private-stack-name"),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch("kitaru.flow._temporary_active_stack", return_value=nullcontext()),
        patch(
            "kitaru._telemetry.classify_stack_deployment_type",
            side_effect=RuntimeError("backend unavailable"),
        ),
        patch("kitaru.flow.track") as track_mock,
    ):
        wrapped = flow(lambda x: x)
        handle = wrapped.run(123)

    assert isinstance(handle, FlowHandle)
    assert track_mock.call_args_list == [
        call(
            AnalyticsEvent.FLOW_ATTEMPTED,
            {"submission_path": "flow_wrapper"},
        ),
        call(
            AnalyticsEvent.FLOW_SUBMITTED,
            {
                "kitaru_deployment_type": "unknown",
                "deployment_type_source": "kitaru_stack_inference_failed",
            },
        ),
    ]
    metadata = track_mock.call_args_list[1].args[1]
    assert "private-stack-name" not in metadata.values()


def test_submit_does_not_emit_flow_failed_after_run_exists() -> None:
    """Post-run persistence failures should not become FLOW_FAILED events."""
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
        patch(
            "kitaru.flow.persist_frozen_execution_spec",
            side_effect=RuntimeError("persistence backend failed"),
        ),
        patch("kitaru._telemetry.classify_stack_deployment_type", return_value="local"),
        patch("kitaru.flow.track") as track_mock,
        pytest.raises(RuntimeError, match="persistence backend failed"),
    ):
        wrapped = flow(lambda: None)
        wrapped.run()

    assert [call_args.args[0] for call_args in track_mock.call_args_list] == [
        AnalyticsEvent.FLOW_ATTEMPTED,
        AnalyticsEvent.FLOW_SUBMITTED,
    ]


def test_submit_failed_event_when_run_is_none() -> None:
    """FLOW_FAILED should fire when the pipeline returns None."""
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
        patch("kitaru._telemetry.classify_stack_deployment_type", return_value="local"),
        patch("kitaru.flow.track") as track_mock,
        pytest.raises(KitaruRuntimeError, match="did not produce"),
    ):
        wrapped = flow(lambda: None)
        wrapped.run()

    assert [call_args.args[0] for call_args in track_mock.call_args_list] == [
        AnalyticsEvent.FLOW_ATTEMPTED,
        AnalyticsEvent.FLOW_FAILED,
    ]
    failed_metadata = track_mock.call_args_list[1].args[1]
    assert failed_metadata["submission_path"] == "flow_wrapper"
    assert failed_metadata["error_type"] == "KitaruRuntimeError"
    assert failed_metadata["failure_origin"] == FailureOrigin.RUNTIME.value
    assert failed_metadata["kitaru_deployment_type"] == "local"
    assert failed_metadata["deployment_type_source"] == "kitaru_stack_inference"


def test_submit_failed_event_when_project_activation_fails() -> None:
    """Activating a bad resolved project is a tracked pre-submission failure."""
    configured_pipeline = MagicMock()
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(),
        ),
        patch(
            "kitaru.flow.resolve_connection_config",
            return_value=SimpleNamespace(project="ghost"),
        ),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch(
            "kitaru.flow._temporary_active_project",
            side_effect=KitaruBackendError("Failed to activate project 'ghost'"),
        ),
        patch("kitaru.flow.track") as track_mock,
        pytest.raises(KitaruBackendError, match="ghost"),
    ):
        wrapped = flow(lambda: None)
        wrapped.run()

    assert [call_args.args[0] for call_args in track_mock.call_args_list] == [
        AnalyticsEvent.FLOW_ATTEMPTED,
        AnalyticsEvent.FLOW_FAILED,
    ]
    configured_pipeline.assert_not_called()
    failed_metadata = track_mock.call_args_list[1].args[1]
    assert failed_metadata["submission_path"] == "flow_wrapper"
    assert failed_metadata["error_type"] == "KitaruBackendError"


def test_submit_failed_event_when_configured_pipeline_raises() -> None:
    """Submission exceptions before a run exists should emit FLOW_FAILED."""
    configured_pipeline = MagicMock(
        side_effect=RuntimeError("backend crash with private-stack-name and /tmp/path")
    )
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            return_value=_resolved_execution(stack="private-stack-name"),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.build_frozen_execution_spec", return_value=object()),
        patch("kitaru.flow.persist_frozen_execution_spec"),
        patch("kitaru.flow._temporary_active_stack", return_value=nullcontext()),
        patch("kitaru._telemetry.classify_stack_deployment_type", return_value="local"),
        patch("kitaru.flow.track") as track_mock,
        pytest.raises(RuntimeError, match="backend crash"),
    ):
        wrapped = flow(lambda: None)
        wrapped.run()

    assert [call_args.args[0] for call_args in track_mock.call_args_list] == [
        AnalyticsEvent.FLOW_ATTEMPTED,
        AnalyticsEvent.FLOW_FAILED,
    ]
    failed_metadata = track_mock.call_args_list[1].args[1]
    assert failed_metadata["submission_path"] == "flow_wrapper"
    assert failed_metadata["error_type"] == "RuntimeError"
    assert "failure_origin" in failed_metadata
    assert failed_metadata["kitaru_deployment_type"] == "local"
    assert failed_metadata["deployment_type_source"] == "kitaru_stack_inference"
    assert "private-stack-name" not in failed_metadata.values()
    assert "/tmp/path" not in failed_metadata.values()


def test_submit_failed_event_when_setup_fails_before_deployment_metadata() -> None:
    """Early setup failures should still emit attempted + failed analytics."""
    zenml_decorator = MagicMock(return_value=MagicMock())

    with (
        patch("kitaru.flow.pipeline", return_value=zenml_decorator),
        patch(
            "kitaru.flow.resolve_execution_config",
            side_effect=RuntimeError("private-stack-name and /tmp/path"),
        ),
        patch("kitaru.flow.track") as track_mock,
        pytest.raises(RuntimeError, match="private-stack-name"),
    ):
        wrapped = flow(lambda: None)
        wrapped.run()

    assert [call_args.args[0] for call_args in track_mock.call_args_list] == [
        AnalyticsEvent.FLOW_ATTEMPTED,
        AnalyticsEvent.FLOW_FAILED,
    ]
    failed_metadata = track_mock.call_args_list[1].args[1]
    assert failed_metadata["submission_path"] == "flow_wrapper"
    assert failed_metadata["error_type"] == "RuntimeError"
    assert "failure_origin" in failed_metadata
    assert "kitaru_deployment_type" not in failed_metadata
    assert "deployment_type_source" not in failed_metadata
    assert "private-stack-name" not in failed_metadata.values()
    assert "/tmp/path" not in failed_metadata.values()


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
            "kitaru._telemetry.classify_stack_deployment_type",
            return_value="kubernetes",
        ),
        patch(
            "kitaru.flow.build_replay_plan",
            return_value=ReplayPlan(
                original_run_id=str(source_run.id),
                steps_to_skip=set(),
                input_overrides={},
                step_input_overrides={},
                runtime_context=ReplayRuntimeContext(at="write"),
            ),
        ),
        patch("kitaru.flow.track") as track_mock,
    ):
        client_cls.return_value.get_pipeline_run.return_value = source_run
        wrapped = flow(lambda topic: topic)
        wrapped.replay(str(source_run.id), at="write", wait=False)

    assert track_mock.call_count == 2
    requested_call = track_mock.call_args_list[0]
    assert requested_call.args[0] == AnalyticsEvent.REPLAY_REQUESTED
    assert requested_call.args[1]["replay_path"] == "flow_wrapper"
    assert requested_call.args[1]["at_checkpoint"] == "write"
    assert requested_call.args[1]["kitaru_deployment_type"] == "kubernetes"
    assert requested_call.args[1]["deployment_type_source"] == "kitaru_stack_inference"

    replayed_call = track_mock.call_args_list[1]
    assert replayed_call.args[0] == AnalyticsEvent.FLOW_REPLAYED
    assert replayed_call.args[1]["replay_path"] == "flow_wrapper"
    assert replayed_call.args[1]["kitaru_deployment_type"] == "kubernetes"
    assert replayed_call.args[1]["deployment_type_source"] == "kitaru_stack_inference"


def test_replay_emits_immediate_terminal_event_when_replayed_run_completed() -> None:
    """Already-completed replayed runs should emit FLOW_TERMINAL immediately."""
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    replayed_run = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        outputs=[("step", "output", 42)],
    )
    configured_pipeline = MagicMock()
    configured_pipeline.replay.return_value = replayed_run
    base_pipeline = MagicMock()
    base_pipeline.with_options.return_value = configured_pipeline
    zenml_decorator = MagicMock(return_value=base_pipeline)
    client_mock = MagicMock()
    client_mock.get_pipeline_run.side_effect = [source_run, replayed_run]

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
        patch(
            "kitaru._telemetry.classify_stack_deployment_type",
            return_value="kubernetes",
        ),
        patch(
            "kitaru.flow.build_replay_plan",
            return_value=ReplayPlan(
                original_run_id=str(source_run.id),
                steps_to_skip=set(),
                input_overrides={},
                step_input_overrides={},
                runtime_context=ReplayRuntimeContext(at="write"),
            ),
        ),
        patch("kitaru.flow.track") as track_mock,
    ):
        wrapped = flow(lambda topic: topic)
        wrapped.replay(str(source_run.id), at="write", wait=False)

    events = [call_args.args[0] for call_args in track_mock.call_args_list]
    assert events == [
        AnalyticsEvent.REPLAY_REQUESTED,
        AnalyticsEvent.FLOW_REPLAYED,
        AnalyticsEvent.FLOW_TERMINAL,
    ]
    terminal_metadata = track_mock.call_args_list[2].args[1]
    assert terminal_metadata["status"] == ExecutionStatus.COMPLETED.value
    assert terminal_metadata["kitaru_deployment_type"] == "kubernetes"
    assert terminal_metadata["deployment_type_source"] == "kitaru_stack_inference"


def test_replay_emits_immediate_terminal_event_when_replayed_run_failed() -> None:
    """Already-failed replayed runs should emit FLOW_TERMINAL with origin."""
    source_run = _DummyRun(status=ExecutionStatus.COMPLETED)
    replayed_run = _DummyRun(
        status=ExecutionStatus.FAILED,
        status_reason="user error",
        traceback="Traceback\nValueError: boom",
    )
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
            "kitaru._telemetry.classify_stack_deployment_type",
            return_value="kubernetes",
        ),
        patch(
            "kitaru.flow.build_replay_plan",
            return_value=ReplayPlan(
                original_run_id=str(source_run.id),
                steps_to_skip=set(),
                input_overrides={},
                step_input_overrides={},
                runtime_context=ReplayRuntimeContext(at="write"),
            ),
        ),
        patch("kitaru.flow.track") as track_mock,
    ):
        client_cls.return_value.get_pipeline_run.return_value = source_run
        wrapped = flow(lambda topic: topic)
        wrapped.replay(str(source_run.id), at="write", wait=False)

    events = [call_args.args[0] for call_args in track_mock.call_args_list]
    assert events == [
        AnalyticsEvent.REPLAY_REQUESTED,
        AnalyticsEvent.FLOW_REPLAYED,
        AnalyticsEvent.FLOW_TERMINAL,
    ]
    terminal_metadata = track_mock.call_args_list[2].args[1]
    assert terminal_metadata["status"] == ExecutionStatus.FAILED.value
    assert terminal_metadata["failure_origin"] == FailureOrigin.USER_CODE.value
    assert terminal_metadata["kitaru_deployment_type"] == "kubernetes"


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
            "kitaru._telemetry.classify_stack_deployment_type",
            return_value="kubernetes",
        ),
        patch(
            "kitaru.flow.build_replay_plan",
            return_value=ReplayPlan(
                original_run_id=str(source_run.id),
                steps_to_skip=set(),
                input_overrides={},
                step_input_overrides={},
                runtime_context=ReplayRuntimeContext(at="write"),
            ),
        ),
        patch("kitaru.flow.track") as track_mock,
        pytest.raises(KitaruBackendError, match="backend crash"),
    ):
        client_cls.return_value.get_pipeline_run.return_value = source_run
        wrapped = flow(lambda topic: topic)
        wrapped.replay(str(source_run.id), at="write", wait=False)

    assert track_mock.call_count == 2
    requested_call = track_mock.call_args_list[0]
    assert requested_call.args[0] == AnalyticsEvent.REPLAY_REQUESTED
    assert requested_call.args[1]["kitaru_deployment_type"] == "kubernetes"
    assert requested_call.args[1]["deployment_type_source"] == "kitaru_stack_inference"

    failed_call = track_mock.call_args_list[1]
    assert failed_call.args[0] == AnalyticsEvent.REPLAY_FAILED
    assert failed_call.args[1]["error_type"] == "RuntimeError"
    assert failed_call.args[1]["kitaru_deployment_type"] == "kubernetes"
    assert failed_call.args[1]["deployment_type_source"] == "kitaru_stack_inference"
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
            "kitaru._telemetry.classify_stack_deployment_type",
            return_value="kubernetes",
        ),
        patch(
            "kitaru.flow.build_replay_plan",
            return_value=ReplayPlan(
                original_run_id=str(source_run.id),
                steps_to_skip=set(),
                input_overrides={},
                step_input_overrides={},
                runtime_context=ReplayRuntimeContext(at="write"),
            ),
        ),
        patch("kitaru.flow.track") as track_mock,
        pytest.raises(KitaruRuntimeError, match="did not produce"),
    ):
        client_cls.return_value.get_pipeline_run.return_value = source_run
        wrapped = flow(lambda topic: topic)
        wrapped.replay(str(source_run.id), at="write", wait=False)

    assert track_mock.call_count == 2
    requested_call = track_mock.call_args_list[0]
    assert requested_call.args[0] == AnalyticsEvent.REPLAY_REQUESTED
    assert requested_call.args[1]["kitaru_deployment_type"] == "kubernetes"
    assert requested_call.args[1]["deployment_type_source"] == "kitaru_stack_inference"

    failed_call = track_mock.call_args_list[1]
    assert failed_call.args[0] == AnalyticsEvent.REPLAY_FAILED
    assert failed_call.args[1]["error_type"] == "KitaruRuntimeError"
    assert failed_call.args[1]["failure_origin"] == FailureOrigin.RUNTIME.value
    assert failed_call.args[1]["kitaru_deployment_type"] == "kubernetes"
    assert failed_call.args[1]["deployment_type_source"] == "kitaru_stack_inference"


def test_flow_handle_wait_emits_flow_terminal_on_success() -> None:
    """FlowHandle.wait() should emit enriched FLOW_TERMINAL metadata."""
    run_id = uuid4()
    started_at = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
    finished = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        run_id=run_id,
        outputs=[("step", "output", 42)],
        start_time=started_at,
        end_time=started_at + timedelta(seconds=2.3456),
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = finished

    handle = FlowHandle(
        _as_pipeline_run(finished),
        analytics_metadata={
            "kitaru_deployment_type": "local",
            "deployment_type_source": "kitaru_stack_inference",
        },
    )
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        patch("kitaru.flow.time.sleep"),
        patch("kitaru.flow.track") as track_mock,
    ):
        handle.wait()

    track_mock.assert_called_once()
    assert track_mock.call_args.args[0] == AnalyticsEvent.FLOW_TERMINAL
    metadata = track_mock.call_args.args[1]
    assert metadata["status"] == "completed"
    assert metadata["kitaru_deployment_type"] == "local"
    assert metadata["deployment_type_source"] == "kitaru_stack_inference"
    assert metadata["duration_seconds"] == 2.346
    assert metadata["duration_source"] == "backend_timestamps"
    assert metadata["checkpoint_count"] == 1
    assert metadata["checkpoint_count_source"] == "hydrated_run_steps"


def test_flow_handle_wait_emits_flow_terminal_on_failure() -> None:
    """FlowHandle.wait() should emit FLOW_TERMINAL with failure_origin on failure."""
    run_id = uuid4()
    started_at = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
    failed = _DummyRun(
        status=ExecutionStatus.FAILED,
        run_id=run_id,
        status_reason="user error",
        traceback="Traceback\nValueError: boom",
        start_time=started_at,
        end_time=started_at + timedelta(seconds=1.0),
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = failed

    handle = FlowHandle(
        _as_pipeline_run(failed),
        analytics_metadata={
            "kitaru_deployment_type": "kubernetes",
            "deployment_type_source": "kitaru_stack_inference",
        },
    )
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        patch("kitaru.flow.time.sleep"),
        patch("kitaru.flow.track") as track_mock,
        pytest.raises(KitaruUserCodeError),
    ):
        handle.wait()

    track_mock.assert_called_once()
    assert track_mock.call_args.args[0] == AnalyticsEvent.FLOW_TERMINAL
    metadata = track_mock.call_args.args[1]
    assert metadata["status"] == "failed"
    assert metadata["failure_origin"] == FailureOrigin.USER_CODE.value
    assert metadata["kitaru_deployment_type"] == "kubernetes"
    assert metadata["duration_seconds"] == 1.0
    assert metadata["duration_source"] == "backend_timestamps"


def test_flow_handle_get_emits_flow_terminal_on_success() -> None:
    """FlowHandle.get() should fall back to SDK-observed duration."""
    run_id = uuid4()
    finished = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        run_id=run_id,
        outputs=[("step", "output", 99)],
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = finished

    handle = FlowHandle(_as_pipeline_run(finished), observed_started_at=10.0)
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        patch("kitaru.flow.time.perf_counter", return_value=12.345),
        patch("kitaru.flow.track") as track_mock,
    ):
        handle.get()

    track_mock.assert_called_once()
    assert track_mock.call_args.args[0] == AnalyticsEvent.FLOW_TERMINAL
    metadata = track_mock.call_args.args[1]
    assert metadata["status"] == "completed"
    assert metadata["duration_seconds"] == 2.345
    assert metadata["duration_source"] == "sdk_observed"
    assert metadata["checkpoint_count"] == 1
    assert metadata["checkpoint_count_source"] == "hydrated_run_steps"


def test_flow_handle_terminal_event_emitted_only_once() -> None:
    """Repeated wait()/get() calls on same handle should emit FLOW_TERMINAL once."""
    run_id = uuid4()
    finished = _DummyRun(
        status=ExecutionStatus.COMPLETED,
        run_id=run_id,
        outputs=[("step", "output", 42)],
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = finished

    handle = FlowHandle(_as_pipeline_run(finished))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        patch("kitaru.flow.time.sleep"),
        patch("kitaru.flow.track") as track_mock,
    ):
        handle.wait()
        handle.get()

    track_mock.assert_called_once()


def test_flow_handle_wait_still_raises_when_classify_fails() -> None:
    """If _classify_run_failure crashes, user should still see their real error."""
    run_id = uuid4()
    failed = _DummyRun(
        status=ExecutionStatus.FAILED,
        run_id=run_id,
        status_reason="user error",
        traceback="Traceback\nValueError: boom",
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = failed

    handle = FlowHandle(_as_pipeline_run(failed))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        patch("kitaru.flow.time.sleep"),
        patch("kitaru.flow.track") as track_mock,
        patch(
            "kitaru.flow._classify_run_failure",
            side_effect=RuntimeError("unexpected shape"),
        ),
        patch(
            "kitaru.flow._duration_metadata_from_run",
            side_effect=RuntimeError("bad timestamps"),
        ),
        patch(
            "kitaru.flow._checkpoint_count_from_run",
            return_value=None,
        ),
        pytest.raises(KitaruExecutionError, match="finished with status"),
    ):
        handle.wait()

    track_mock.assert_called_once()
    assert track_mock.call_args.args[0] == AnalyticsEvent.FLOW_TERMINAL
    metadata = track_mock.call_args.args[1]
    assert metadata["status"] == "failed"
    assert metadata["failure_origin"] == FailureOrigin.UNKNOWN.value
    assert "duration_seconds" not in metadata
    assert "checkpoint_count" not in metadata
    assert "checkpoint_count_source" not in metadata


def test_flow_handle_constructor_terminal_failure_classification_falls_back() -> None:
    """Constructor-time terminal telemetry should not raise if classification fails."""
    failed = _DummyRun(
        status=ExecutionStatus.FAILED,
        status_reason="user error",
        traceback="Traceback\nValueError: boom",
    )

    with (
        patch(
            "kitaru.flow._classify_run_failure",
            side_effect=RuntimeError("unexpected shape"),
        ),
        patch("kitaru.flow.track") as track_mock,
    ):
        handle = FlowHandle(
            _as_pipeline_run(failed),
            track_terminal_if_finished=True,
        )

    assert handle.exec_id == str(failed.id)
    track_mock.assert_called_once()
    assert track_mock.call_args.args[0] == AnalyticsEvent.FLOW_TERMINAL
    metadata = track_mock.call_args.args[1]
    assert metadata["status"] == ExecutionStatus.FAILED.value
    assert metadata["failure_origin"] == FailureOrigin.UNKNOWN.value


# ---------------------------------------------------------------------------
# Direct unit tests for analytics helper functions
# ---------------------------------------------------------------------------


class TestDurationMetadataFromRun:
    """Direct tests for _duration_metadata_from_run edge cases."""

    def test_backend_timestamps_produce_backend_source(self) -> None:
        start = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
        run = SimpleNamespace(
            start_time=start,
            end_time=start + timedelta(seconds=5.678),
        )
        result = _duration_metadata_from_run(
            cast(PipelineRunResponse, run), observed_started_at=0.0
        )
        assert result == {
            "duration_seconds": 5.678,
            "duration_source": "backend_timestamps",
        }

    def test_negative_backend_duration_clamped_to_zero(self) -> None:
        start = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
        run = SimpleNamespace(
            start_time=start,
            end_time=start - timedelta(seconds=1),
        )
        result = _duration_metadata_from_run(
            cast(PipelineRunResponse, run), observed_started_at=0.0
        )
        assert result["duration_seconds"] == 0.0
        assert result["duration_source"] == "backend_timestamps"

    def test_missing_timestamps_falls_back_to_sdk_observed(self) -> None:
        run = SimpleNamespace(start_time=None, end_time=None)
        with patch("kitaru.flow.time.perf_counter", return_value=15.0):
            result = _duration_metadata_from_run(
                cast(PipelineRunResponse, run), observed_started_at=10.0
            )
        assert result == {
            "duration_seconds": 5.0,
            "duration_source": "sdk_observed",
        }

    def test_no_timestamps_and_no_observed_returns_empty(self) -> None:
        run = SimpleNamespace(start_time=None, end_time=None)
        result = _duration_metadata_from_run(
            cast(PipelineRunResponse, run), observed_started_at=None
        )
        assert result == {}

    def test_non_datetime_timestamps_fall_back_to_sdk(self) -> None:
        run = SimpleNamespace(start_time="not-a-datetime", end_time="also-not")
        with patch("kitaru.flow.time.perf_counter", return_value=20.0):
            result = _duration_metadata_from_run(
                cast(PipelineRunResponse, run), observed_started_at=18.0
            )
        assert result["duration_source"] == "sdk_observed"

    def test_missing_start_time_attr_falls_back(self) -> None:
        run = SimpleNamespace()
        result = _duration_metadata_from_run(
            cast(PipelineRunResponse, run), observed_started_at=None
        )
        assert result == {}


class TestCheckpointCountFromRun:
    """Direct tests for _checkpoint_count_from_run edge cases."""

    def test_returns_step_count_from_hydrated_run(self) -> None:
        hydrated = SimpleNamespace(steps={"step_a": object(), "step_b": object()})
        run = SimpleNamespace(get_hydrated_version=lambda: hydrated)
        assert _checkpoint_count_from_run(cast(PipelineRunResponse, run)) == 2

    def test_returns_none_when_steps_not_a_mapping(self) -> None:
        hydrated = SimpleNamespace(steps="not-a-mapping")
        run = SimpleNamespace(get_hydrated_version=lambda: hydrated)
        assert _checkpoint_count_from_run(cast(PipelineRunResponse, run)) is None

    def test_returns_none_when_steps_attr_missing(self) -> None:
        hydrated = SimpleNamespace()
        run = SimpleNamespace(get_hydrated_version=lambda: hydrated)
        assert _checkpoint_count_from_run(cast(PipelineRunResponse, run)) is None

    def test_returns_none_when_hydration_raises(self) -> None:
        def explode() -> None:
            raise RuntimeError("backend unavailable")

        run = SimpleNamespace(get_hydrated_version=explode)
        assert _checkpoint_count_from_run(cast(PipelineRunResponse, run)) is None

    def test_returns_zero_for_empty_steps(self) -> None:
        hydrated = SimpleNamespace(steps={})
        run = SimpleNamespace(get_hydrated_version=lambda: hydrated)
        assert _checkpoint_count_from_run(cast(PipelineRunResponse, run)) == 0


class TestRecoveryHintHelpers:
    """Tests for the recovery hint formatting helpers in errors.py."""

    def test_execution_error_status_coerces_to_kitaru_enum(self) -> None:
        error = execution_error_from_failure(
            "run failed",
            exec_id="exec-123",
            status="failed",
            origin=FailureOrigin.UNKNOWN,
        )
        assert error.status == KitaruExecutionStatus.FAILED
        assert isinstance(error.status, KitaruExecutionStatus)

    def test_build_recovery_command_for_failed(self) -> None:
        assert build_recovery_command("kr-abc", status="failed") == (
            "kitaru executions retry kr-abc"
        )

    def test_build_recovery_command_returns_none_for_completed(self) -> None:
        assert build_recovery_command("kr-abc", status="completed") is None

    def test_build_recovery_command_returns_none_for_running(self) -> None:
        assert build_recovery_command("kr-abc", status="running") is None

    def test_format_recovery_hint_for_failed(self) -> None:
        hint = format_recovery_hint("kr-abc", status="failed")
        assert hint is not None
        assert "kitaru executions retry kr-abc" in hint
        assert "To retry" in hint

    def test_build_recovery_command_returns_none_for_cancelled(self) -> None:
        assert build_recovery_command("kr-abc", status="cancelled") is None

    def test_format_recovery_hint_returns_none_for_completed(self) -> None:
        assert format_recovery_hint("kr-abc", status="completed") is None

    def test_format_recovery_hint_returns_none_for_cancelled(self) -> None:
        assert format_recovery_hint("kr-abc", status="cancelled") is None


def test_flow_handle_get_includes_retry_hint_on_failure() -> None:
    """FlowHandle.get() error message should include a retry CLI hint."""
    run_id = uuid4()
    failed = _DummyRun(
        status=ExecutionStatus.FAILED,
        run_id=run_id,
        status_reason="upstream failure",
        traceback="Traceback\nValueError: boom",
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = failed

    handle = FlowHandle(_as_pipeline_run(failed))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        patch("kitaru.flow.track"),
        pytest.raises(KitaruUserCodeError, match="kitaru executions retry") as exc_info,
    ):
        handle.get()

    message = str(exc_info.value)
    assert f"kitaru executions retry {run_id}" in message
    assert "To retry this failed execution" in message


def test_flow_handle_wait_includes_retry_hint_on_failure() -> None:
    """FlowHandle.wait() error message should include a retry CLI hint."""
    run_id = uuid4()
    failed = _DummyRun(
        status=ExecutionStatus.FAILED,
        run_id=run_id,
        traceback="Traceback\nRuntimeError: connection lost",
    )
    client_mock = MagicMock()
    client_mock.get_pipeline_run.return_value = failed

    handle = FlowHandle(_as_pipeline_run(failed))
    with (
        patch("kitaru.flow.Client", return_value=client_mock),
        patch("kitaru.flow.time.sleep"),
        patch("kitaru.flow.track"),
        pytest.raises(KitaruExecutionError) as exc_info,
    ):
        handle.wait()

    message = str(exc_info.value)
    assert f"kitaru executions retry {run_id}" in message


def _replay_many_step(*, name: str, invocation_id: str) -> SimpleNamespace:
    started = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    return SimpleNamespace(
        id=uuid4(),
        name=name,
        start_time=started,
        spec=SimpleNamespace(invocation_id=invocation_id, upstream_steps=[]),
    )


def test_flow_definition_does_not_expose_replay_many() -> None:
    wrapped = kitaru.flow(lambda topic: topic)

    assert not hasattr(wrapped, "replay_many")


def test_unified_replay_single_defaults_to_wait_and_fail() -> None:
    run = SimpleNamespace(
        id="run-1",
        steps={"write": _replay_many_step(name="write", invocation_id="write")},
    )
    handle = MagicMock(exec_id="replay-1")
    plan = ReplayPlan(
        original_run_id="run-1",
        steps_to_skip=set(),
        input_overrides={},
        step_input_overrides={},
        runtime_context=ReplayRuntimeContext(at="write"),
    )
    wrapped = kitaru.flow(lambda topic: topic)
    with (
        patch("kitaru.flow.Client") as client_cls,
        patch.object(wrapped, "_replay_one_handle", return_value=(handle, run, plan)),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.safe_persist_replay_submission_metadata"),
    ):
        client_cls.return_value.get_pipeline_run.return_value = run
        submission = wrapped.replay("run-1", at="write")

    handle.wait.assert_called_once()
    assert submission.wait is True
    assert submission.results[0].status == "completed"


def test_unified_replay_batch_defaults_to_collect_and_skips_missing_at() -> None:
    run_with_at = SimpleNamespace(
        id="run-with",
        steps={"write": _replay_many_step(name="write", invocation_id="write")},
    )
    run_without_at = SimpleNamespace(
        id="run-without",
        steps={"fetch": _replay_many_step(name="fetch", invocation_id="fetch")},
    )
    handle = MagicMock(exec_id="replay-1")
    plan = ReplayPlan(
        original_run_id="run-with",
        steps_to_skip=set(),
        input_overrides={},
        step_input_overrides={},
        runtime_context=ReplayRuntimeContext(at="write"),
    )
    wrapped = kitaru.flow(lambda topic: topic)
    with (
        patch("kitaru.flow.Client") as client_cls,
        patch.object(
            wrapped,
            "_replay_one_handle",
            return_value=(handle, run_with_at, plan),
        ),
        patch("kitaru.flow.resolve_connection_config", return_value=object()),
        patch("kitaru.flow.safe_persist_replay_submission_metadata"),
    ):

        def _get_pipeline_run(name_id_or_prefix, **_):
            return {
                "exec-with": run_with_at,
                "exec-without": run_without_at,
            }[name_id_or_prefix]

        client_cls.return_value.get_pipeline_run.side_effect = _get_pipeline_run
        submission = wrapped.replay(["exec-with", "exec-without"], at="write")

    handle.wait.assert_not_called()
    assert submission.wait is False
    assert len(submission.results) == 1
    assert len(submission.skipped) == 1
    assert submission.skipped[0].original_exec_ref == "exec-without"
