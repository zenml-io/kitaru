"""Tests for the Phase 11 `KitaruClient` implementation."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, call, patch
from uuid import UUID, uuid4

import pytest
from zenml.enums import ArtifactSaveType
from zenml.enums import ExecutionStatus as ZenMLExecutionStatus
from zenml.models import PipelineRunResponse, StepRunResponse
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse

from kitaru.client import ExecutionStatus, KitaruClient
from kitaru.config import (
    FrozenExecutionSpec,
    KitaruConfig,
    ResolvedConnectionConfig,
    ResolvedExecutionConfig,
)
from kitaru.errors import (
    FailureOrigin,
    KitaruFeatureNotAvailableError,
    KitaruStateError,
    KitaruWaitValidationError,
)


def _as_pipeline_run(run: _DummyRun) -> PipelineRunResponse:
    return cast(PipelineRunResponse, run)


def _as_step_run(step: _DummyStep) -> StepRunResponse:
    return cast(StepRunResponse, step)


def _as_artifact(artifact: _DummyArtifact) -> ArtifactVersionResponse:
    return cast(ArtifactVersionResponse, artifact)


class _DummyArtifact:
    def __init__(
        self,
        *,
        name: str,
        save_type: ArtifactSaveType,
        value: Any,
        artifact_id: UUID | None = None,
        metadata: dict[str, Any] | None = None,
        producer_step_run_id: UUID | None = None,
    ) -> None:
        self.id = artifact_id or uuid4()
        self.name = name
        self.save_type = save_type
        self.run_metadata = metadata or {}
        self.producer_step_run_id = producer_step_run_id
        self._value = value

    def load(self) -> Any:
        return self._value


class _DummyStep:
    def __init__(
        self,
        *,
        name: str,
        status: Any,
        outputs: dict[str, list[_DummyArtifact]],
        step_id: UUID | None = None,
        original_step_run_id: UUID | None = None,
        run_metadata: dict[str, Any] | None = None,
        exception_traceback: str | None = None,
    ) -> None:
        self.id = step_id or uuid4()
        self.name = name
        self.status = status
        self.start_time = None
        self.end_time = None
        self.run_metadata = run_metadata or {}
        self.original_step_run_id = original_step_run_id
        self.parent_step_ids: list[UUID] = []
        self.outputs = outputs
        self.exception_info = (
            SimpleNamespace(traceback=exception_traceback)
            if exception_traceback is not None
            else None
        )


class _DummyRun:
    def __init__(
        self,
        *,
        status: Any,
        flow_name: str,
        run_metadata: dict[str, Any] | None = None,
        steps: dict[str, _DummyStep] | None = None,
        stack_name: str | None = "local",
        snapshot: Any = None,
        run_id: UUID | None = None,
        status_reason: str | None = None,
        exception_traceback: str | None = None,
        active_wait_condition: Any = None,
    ) -> None:
        self.id = run_id or uuid4()
        self.status = status
        self.status_reason = status_reason
        self.start_time = None
        self.end_time = None
        self.run_metadata = run_metadata or {}
        self.pipeline = SimpleNamespace(name=flow_name)
        self.stack = SimpleNamespace(name=stack_name) if stack_name else None
        self.snapshot = snapshot
        self.original_run = None
        self._steps = steps or {}
        self.exception_info = (
            SimpleNamespace(traceback=exception_traceback)
            if exception_traceback is not None
            else None
        )
        self._active_wait_condition = active_wait_condition

    @property
    def steps(self) -> dict[str, _DummyStep]:
        return self._steps

    def get_resources(self) -> Any:
        return SimpleNamespace(active_wait_condition=self._active_wait_condition)


def _resolved_connection(project: str | None = None) -> ResolvedConnectionConfig:
    return ResolvedConnectionConfig(
        server_url=None,
        auth_token=None,
        project=project,
    )


def _dummy_wait_condition(
    *,
    key: str,
    wait_id: UUID | None = None,
    question: str | None = None,
    data_schema: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> Any:
    return SimpleNamespace(
        id=wait_id or uuid4(),
        wait_condition_key=key,
        question=question,
        data_schema=data_schema,
        wait_metadata=metadata or {},
        created=None,
    )


def _paused_status() -> Any:
    return SimpleNamespace(value="paused")


def test_client_initializes_namespaces() -> None:
    with patch(
        "kitaru.client.resolve_connection_config", return_value=_resolved_connection()
    ):
        client = KitaruClient()

    assert hasattr(client, "executions")
    assert hasattr(client, "artifacts")


def test_client_rejects_connection_overrides() -> None:
    with pytest.raises(
        KitaruFeatureNotAvailableError,
        match="Per-client connection overrides",
    ):
        KitaruClient(server_url="https://example.com")


def test_get_maps_execution_details() -> None:
    frozen = FrozenExecutionSpec(
        version=1,
        resolved_execution=ResolvedExecutionConfig(
            stack="local",
            image=None,
            cache=True,
            retries=0,
        ),
        flow_defaults=KitaruConfig(),
        connection=_resolved_connection(),
    ).model_dump(mode="json")

    artifact = _DummyArtifact(
        name="research_context",
        save_type=ArtifactSaveType.MANUAL,
        value={"topic": "kitaru"},
        metadata={"kitaru_artifact_type": "context"},
    )
    step = _DummyStep(
        name="__kitaru_checkpoint_source_research",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={"research_context": [artifact]},
    )
    run = _DummyRun(
        status=ZenMLExecutionStatus.COMPLETED,
        flow_name="__kitaru_pipeline_source_content_flow",
        run_metadata={"kitaru_execution_spec": frozen},
        steps={step.name: step},
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)

        client = KitaruClient()
        execution = client.executions.get(str(run.id))

    assert execution.exec_id == str(run.id)
    assert execution.flow_name == "content_flow"
    assert execution.status == ExecutionStatus.COMPLETED
    assert execution.frozen_execution_spec is not None
    assert execution.frozen_execution_spec.resolved_execution.stack == "local"
    assert execution.failure is None

    assert len(execution.checkpoints) == 1
    checkpoint = execution.checkpoints[0]
    assert checkpoint.name == "research"
    assert checkpoint.failure is None
    assert len(checkpoint.attempts) == 1

    assert len(execution.artifacts) == 1
    artifact_ref = execution.artifacts[0]
    assert artifact_ref.name == "research_context"
    assert artifact_ref.kind == "context"


def test_get_surfaces_checkpoint_attempt_history() -> None:
    attempt_one = _DummyStep(
        name="__kitaru_checkpoint_source_research",
        status=ZenMLExecutionStatus.RETRIED,
        outputs={},
        exception_traceback="Traceback\nValueError: boom",
    )
    attempt_two = _DummyStep(
        name="__kitaru_checkpoint_source_research",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={},
        original_step_run_id=attempt_one.id,
    )

    run = _DummyRun(
        status=ZenMLExecutionStatus.COMPLETED,
        flow_name="flow_a",
        steps={attempt_two.name: attempt_two},
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)
        client_mock.list_run_steps.return_value = SimpleNamespace(
            items=[_as_step_run(attempt_one), _as_step_run(attempt_two)]
        )

        client = KitaruClient()
        execution = client.executions.get(str(run.id))

    checkpoint = execution.checkpoints[0]
    assert len(checkpoint.attempts) == 2
    assert checkpoint.attempts[0].status == ExecutionStatus.FAILED
    assert checkpoint.attempts[0].failure is not None
    assert checkpoint.attempts[0].failure.origin == FailureOrigin.USER_CODE
    assert checkpoint.attempts[0].failure.exception_type == "ValueError"
    assert checkpoint.failure is None


def test_get_surfaces_execution_failure_origin() -> None:
    failed_run = _DummyRun(
        status=ZenMLExecutionStatus.FAILED,
        flow_name="flow_a",
        status_reason="Serialization failure while materializing output.",
        exception_traceback="Traceback\nRuntimeError: serialization failed",
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(failed_run)
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        execution = client.executions.get(str(failed_run.id))

    assert execution.failure is not None
    assert execution.failure.origin == FailureOrigin.RUNTIME
    assert "Serialization failure" in execution.failure.message


def test_get_degrades_when_attempt_history_lookup_fails() -> None:
    step = _DummyStep(
        name="__kitaru_checkpoint_source_research",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={},
    )
    run = _DummyRun(
        status=ZenMLExecutionStatus.COMPLETED,
        flow_name="flow_a",
        steps={step.name: step},
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)
        client_mock.list_run_steps.side_effect = RuntimeError("backend unavailable")

        client = KitaruClient()
        execution = client.executions.get(str(run.id))

    assert len(execution.checkpoints) == 1
    assert len(execution.checkpoints[0].attempts) == 1


def test_non_failed_execution_has_no_failure_payload() -> None:
    run = _DummyRun(
        status=ZenMLExecutionStatus.STOPPED,
        flow_name="flow_a",
        status_reason="Stopped by user.",
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        execution = client.executions.get(str(run.id))

    assert execution.status == ExecutionStatus.CANCELLED
    assert execution.failure is None


def test_list_filters_flow_status_and_limit() -> None:
    run_1 = _DummyRun(
        status=ZenMLExecutionStatus.COMPLETED,
        flow_name="flow_a",
    )
    run_2 = _DummyRun(
        status=ZenMLExecutionStatus.FAILED,
        flow_name="flow_a",
    )
    run_3 = _DummyRun(
        status=ZenMLExecutionStatus.COMPLETED,
        flow_name="flow_b",
    )

    run_page = SimpleNamespace(
        items=[
            _as_pipeline_run(run_1),
            _as_pipeline_run(run_2),
            _as_pipeline_run(run_3),
        ]
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.list_pipeline_runs.return_value = run_page

        client = KitaruClient()
        executions = client.executions.list(
            flow="flow_a",
            status="completed",
            limit=1,
        )

    assert len(executions) == 1
    assert executions[0].exec_id == str(run_1.id)


def test_latest_raises_when_no_execution_matches() -> None:
    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.list_pipeline_runs.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        with pytest.raises(LookupError, match="No executions found"):
            client.executions.latest(flow="missing")


def test_cancel_delegates_stop_run_and_returns_refreshed_execution() -> None:
    run_id = uuid4()
    running = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        run_id=run_id,
    )
    stopped = _DummyRun(
        status=ZenMLExecutionStatus.STOPPED,
        flow_name="flow_a",
        run_id=run_id,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch("kitaru.client.stop_run") as stop_run_mock,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.side_effect = [
            _as_pipeline_run(running),
            _as_pipeline_run(stopped),
        ]

        client = KitaruClient()
        execution = client.executions.cancel(str(run_id))

    stop_run_mock.assert_called_once_with(run=_as_pipeline_run(running), graceful=False)
    assert execution.status == ExecutionStatus.CANCELLED


def test_retry_restarts_failed_execution() -> None:
    run_id = uuid4()
    snapshot_stack_id = uuid4()
    failed = _DummyRun(
        status=ZenMLExecutionStatus.FAILED,
        flow_name="flow_a",
        run_id=run_id,
        snapshot=SimpleNamespace(stack=SimpleNamespace(id=snapshot_stack_id)),
    )
    retried = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        run_id=run_id,
        snapshot=SimpleNamespace(stack=SimpleNamespace(id=snapshot_stack_id)),
    )

    old_stack_id = uuid4()
    active_stack = SimpleNamespace(orchestrator=SimpleNamespace(restart=MagicMock()))

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.active_stack_model = SimpleNamespace(id=old_stack_id)
        client_mock.active_stack = active_stack
        client_mock.get_pipeline_run.side_effect = [
            _as_pipeline_run(failed),
            _as_pipeline_run(retried),
        ]

        client = KitaruClient()
        execution = client.executions.retry(str(run_id))

    active_stack.orchestrator.restart.assert_called_once_with(
        snapshot=failed.snapshot,
        run=_as_pipeline_run(failed),
        stack=active_stack,
    )
    assert client_mock.activate_stack.call_args_list == [
        call(str(snapshot_stack_id)),
        call(old_stack_id),
    ]
    assert execution.status == ExecutionStatus.RUNNING


def test_retry_rejects_non_failed_execution() -> None:
    run = _DummyRun(
        status=ZenMLExecutionStatus.COMPLETED,
        flow_name="flow_a",
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)

        client = KitaruClient()
        with pytest.raises(RuntimeError, match="Only failed executions can be retried"):
            client.executions.retry(str(run.id))


def test_input_resolves_pending_wait_condition() -> None:
    run_id = uuid4()
    wait_condition = _dummy_wait_condition(
        key="approve_deploy",
        question="Deploy to prod?",
        data_schema={"type": "boolean"},
    )
    waiting_run = _DummyRun(
        status=_paused_status(),
        flow_name="flow_a",
        run_id=run_id,
        active_wait_condition=wait_condition,
    )
    resumed_run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        run_id=run_id,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.side_effect = [
            _as_pipeline_run(waiting_run),
            _as_pipeline_run(resumed_run),
        ]
        client_mock.list_run_wait_conditions.side_effect = [
            SimpleNamespace(items=[wait_condition]),
            SimpleNamespace(items=[]),
        ]
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        execution = client.executions.input(
            str(run_id),
            wait="approve_deploy",
            value=True,
        )

    client_mock.resolve_run_wait_condition.assert_called_once_with(
        run_wait_condition_id=wait_condition.id,
        status="resolved",
        resolution="continue",
        result=True,
    )
    assert execution.status == ExecutionStatus.RUNNING


def test_get_surfaces_waiting_status_for_running_wait_condition() -> None:
    wait_condition = _dummy_wait_condition(
        key="review_draft",
        question="Approve this draft?",
        data_schema={"type": "boolean"},
        metadata={"section": "intro"},
    )
    run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        active_wait_condition=wait_condition,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(items=[])
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        execution = client.executions.get(str(run.id))

    assert execution.status == ExecutionStatus.WAITING
    assert execution.pending_wait is not None
    assert execution.pending_wait.name == "review_draft"
    assert execution.pending_wait.question == "Approve this draft?"
    assert execution.pending_wait.schema == {"type": "boolean"}
    assert execution.pending_wait.metadata == {"section": "intro"}


def test_get_surfaces_waiting_status_for_running_execution_with_listed_wait() -> None:
    wait_condition = _dummy_wait_condition(
        key="approve_release:0",
        question="Approve release?",
        data_schema={"type": "boolean"},
        metadata={"topic": "kitaru-1"},
    )
    run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        active_wait_condition=None,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(
            items=[wait_condition]
        )
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        execution = client.executions.get(str(run.id))

    assert execution.status == ExecutionStatus.WAITING
    assert execution.pending_wait is not None
    assert execution.pending_wait.wait_id == str(wait_condition.id)
    assert execution.pending_wait.name == "approve_release:0"
    assert execution.pending_wait.metadata == {"topic": "kitaru-1"}


def test_input_rejects_missing_pending_wait() -> None:
    run = _DummyRun(
        status=_paused_status(),
        flow_name="flow_a",
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        with pytest.raises(KitaruStateError, match="no pending waits"):
            client.executions.input(str(run.id), wait="approve", value=True)


def test_input_rejects_unknown_wait_name() -> None:
    wait_condition = _dummy_wait_condition(key="approve")
    run = _DummyRun(
        status=_paused_status(),
        flow_name="flow_a",
        active_wait_condition=wait_condition,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(
            items=[wait_condition]
        )

        client = KitaruClient()
        with pytest.raises(KitaruStateError, match="no pending wait 'review'"):
            client.executions.input(str(run.id), wait="review", value=True)


def test_input_maps_validation_error() -> None:
    wait_condition = _dummy_wait_condition(key="approve")
    run = _DummyRun(
        status=_paused_status(),
        flow_name="flow_a",
        active_wait_condition=wait_condition,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(
            items=[wait_condition]
        )
        client_mock.resolve_run_wait_condition.side_effect = ValueError(
            "result does not match schema"
        )

        client = KitaruClient()
        with pytest.raises(KitaruWaitValidationError, match="failed validation"):
            client.executions.input(str(run.id), wait="approve", value="yes")


def test_resume_restarts_paused_execution() -> None:
    run_id = uuid4()
    snapshot_stack_id = uuid4()
    paused = _DummyRun(
        status=_paused_status(),
        flow_name="flow_a",
        run_id=run_id,
        snapshot=SimpleNamespace(stack=SimpleNamespace(id=snapshot_stack_id)),
    )
    resumed = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        run_id=run_id,
        snapshot=SimpleNamespace(stack=SimpleNamespace(id=snapshot_stack_id)),
    )

    old_stack_id = uuid4()
    active_stack = SimpleNamespace(orchestrator=SimpleNamespace(restart=MagicMock()))

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.active_stack_model = SimpleNamespace(id=old_stack_id)
        client_mock.active_stack = active_stack
        client_mock.get_pipeline_run.side_effect = [
            _as_pipeline_run(paused),
            _as_pipeline_run(resumed),
        ]
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(items=[])
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        execution = client.executions.resume(str(run_id))

    active_stack.orchestrator.restart.assert_called_once_with(
        snapshot=paused.snapshot,
        run=_as_pipeline_run(paused),
        stack=active_stack,
    )
    assert client_mock.activate_stack.call_args_list == [
        call(str(snapshot_stack_id)),
        call(old_stack_id),
    ]
    assert execution.status == ExecutionStatus.RUNNING


def test_resume_rejects_when_pending_waits_exist() -> None:
    wait_condition = _dummy_wait_condition(key="approve")
    run = _DummyRun(
        status=_paused_status(),
        flow_name="flow_a",
        active_wait_condition=wait_condition,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(
            items=[wait_condition]
        )

        client = KitaruClient()
        with pytest.raises(KitaruStateError, match="Resolve pending wait input"):
            client.executions.resume(str(run.id))


def test_resume_rejects_non_paused_execution() -> None:
    run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        with pytest.raises(KitaruStateError, match="Only paused executions"):
            client.executions.resume(str(run.id))


def test_replay_still_raises_not_implemented() -> None:
    with patch(
        "kitaru.client.resolve_connection_config", return_value=_resolved_connection()
    ):
        client = KitaruClient()

    with pytest.raises(KitaruFeatureNotAvailableError, match="replay"):
        client.executions.replay("exec-1", from_="checkpoint")


def test_artifact_get_maps_producing_call_and_loads_value() -> None:
    step_id = uuid4()
    artifact = _DummyArtifact(
        name="payload",
        save_type=ArtifactSaveType.MANUAL,
        value={"ok": True},
        producer_step_run_id=step_id,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_artifact_version.return_value = _as_artifact(artifact)
        client_mock.get_run_step.return_value = _as_step_run(
            _DummyStep(
                name="__kitaru_checkpoint_source_writer",
                status=ZenMLExecutionStatus.COMPLETED,
                outputs={},
                step_id=step_id,
            )
        )

        client = KitaruClient()
        artifact_ref = client.artifacts.get(str(artifact.id))

        assert artifact_ref.producing_call == "writer"
        value = artifact_ref.load()

    assert value == {"ok": True}
    assert client_mock.get_artifact_version.call_count == 2
