"""Tests for the Phase 11 `KitaruClient` implementation."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, Mock, call, patch
from uuid import UUID, uuid4

import pytest
from zenml.enums import ArtifactSaveType
from zenml.enums import ExecutionStatus as ZenMLExecutionStatus
from zenml.models import PipelineRunResponse, StepRunResponse
from zenml.models.v2.core.artifact_version import ArtifactVersionResponse

from kitaru.analytics import AnalyticsEvent
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
    KitaruLogRetrievalError,
    KitaruRuntimeError,
    KitaruStateError,
    KitaruUsageError,
    KitaruWaitValidationError,
)
from kitaru.memory import MemoryEntry, MemoryScopeType, _MemoryScope


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
        spec: Any | None = None,
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
        self.spec = spec
        self.type = None
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
    name: str,
    wait_id: UUID | None = None,
    question: str | None = None,
    data_schema: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> Any:
    return SimpleNamespace(
        id=wait_id or uuid4(),
        name=name,
        question=question,
        data_schema=data_schema,
        run_metadata=metadata or {},
        created=None,
    )


def _sample_memory_entry(
    *,
    key: str = "prefs",
    scope: str = "demo_scope",
    scope_type: MemoryScopeType = "namespace",
    version: int = 1,
    is_deleted: bool = False,
) -> MemoryEntry:
    return MemoryEntry(
        key=key,
        value_type="dict",
        version=version,
        scope=scope,
        scope_type=scope_type,
        created_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
        is_deleted=is_deleted,
        artifact_id=str(uuid4()),
        execution_id=None,
    )


def _paused_status() -> Any:
    return SimpleNamespace(value="paused")


def _snapshot_source(module: str, attribute: str) -> Any:
    return SimpleNamespace(
        module=module,
        attribute=attribute,
        import_path=f"{module}.{attribute}",
    )


def test_client_initializes_namespaces() -> None:
    with patch(
        "kitaru.client.resolve_connection_config", return_value=_resolved_connection()
    ):
        client = KitaruClient()

    assert hasattr(client, "executions")
    assert hasattr(client, "artifacts")
    assert hasattr(client, "memories")


def test_client_rejects_connection_overrides() -> None:
    with pytest.raises(
        KitaruFeatureNotAvailableError,
        match="Per-client connection overrides",
    ):
        KitaruClient(server_url="https://example.com")


def test_client_requires_project_for_env_driven_remote_connection() -> None:
    """Client init should fail fast when env remote config has no project."""
    with (
        patch(
            "kitaru.client.resolve_connection_config",
            side_effect=KitaruUsageError("Set KITARU_PROJECT before using the SDK."),
        ) as resolve_connection,
        pytest.raises(KitaruUsageError, match="KITARU_PROJECT"),
    ):
        KitaruClient()

    resolve_connection.assert_called_once_with(validate_for_use=True)


def test_memories_get_delegates_to_entry_impl() -> None:
    entry = _sample_memory_entry(scope="repo_scope", version=2)

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client._get_entry_impl", return_value=entry) as get_entry_impl,
    ):
        client = KitaruClient()
        result = client.memories.get(
            "prefs",
            scope="repo_scope",
            scope_type="namespace",
            version=2,
        )

    assert result == entry
    assert get_entry_impl.call_args.args[:2] == (
        _MemoryScope(scope="repo_scope", scope_type="namespace"),
        "prefs",
    )
    assert get_entry_impl.call_args.kwargs["version"] == 2


def test_memories_list_passes_prefix_to_storage_impl() -> None:
    entry = _sample_memory_entry(key="repo_alpha", scope="repo_scope")

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client._list_impl", return_value=[entry]) as list_impl,
    ):
        client = KitaruClient()
        result = client.memories.list(
            scope="repo_scope",
            scope_type="namespace",
            prefix="repo_",
        )

    assert result == [entry]
    assert list_impl.call_args.args == (
        _MemoryScope(scope="repo_scope", scope_type="namespace"),
    )
    assert list_impl.call_args.kwargs["prefix"] == "repo_"


def test_memories_history_delegates_to_history_impl() -> None:
    history = [
        _sample_memory_entry(scope="repo_scope", version=2, is_deleted=True),
        _sample_memory_entry(scope="repo_scope", version=1),
    ]

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client._history_impl", return_value=history) as history_impl,
    ):
        client = KitaruClient()
        result = client.memories.history(
            "prefs",
            scope="repo_scope",
            scope_type="namespace",
        )

    assert result == history
    assert history_impl.call_args.args == (
        _MemoryScope(scope="repo_scope", scope_type="namespace"),
        "prefs",
    )


def test_memories_set_delegates_to_set_entry_impl() -> None:
    entry = _sample_memory_entry(scope="repo_scope", scope_type="flow", version=3)

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client._set_entry_impl", return_value=entry) as set_entry_impl,
    ):
        client = KitaruClient()
        result = client.memories.set(
            "prefs",
            {"theme": "dark"},
            scope="repo_scope",
            scope_type="flow",
        )

    assert result == entry
    assert set_entry_impl.call_args.args == (
        _MemoryScope(scope="repo_scope", scope_type="flow"),
        "prefs",
        {"theme": "dark"},
    )


def test_memories_delete_delegates_to_delete_impl() -> None:
    tombstone = _sample_memory_entry(
        scope="repo_scope",
        version=2,
        is_deleted=True,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client._delete_impl", return_value=tombstone) as delete_impl,
    ):
        client = KitaruClient()
        result = client.memories.delete(
            "prefs",
            scope="repo_scope",
            scope_type="namespace",
        )

    assert result == tombstone
    assert delete_impl.call_args.args == (
        _MemoryScope(scope="repo_scope", scope_type="namespace"),
        "prefs",
    )


def test_memories_compact_delegates_to_compact_impl_with_source_mode() -> None:
    compact_result = MagicMock()

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch(
            "kitaru.client._compact_impl", return_value=compact_result
        ) as compact_impl,
    ):
        client = KitaruClient()
        result = client.memories.compact(
            scope="repo_scope",
            scope_type="namespace",
            key="prefs",
            source_mode="history",
        )

    assert result == compact_result
    assert compact_impl.call_args.args == (
        _MemoryScope(scope="repo_scope", scope_type="namespace"),
    )
    assert compact_impl.call_args.kwargs["key"] == "prefs"
    assert compact_impl.call_args.kwargs["keys"] is None
    assert compact_impl.call_args.kwargs["source_mode"] == "history"
    assert compact_impl.call_args.kwargs["target_key"] is None


def test_memories_compact_rejects_invalid_source_mode() -> None:
    with patch(
        "kitaru.client.resolve_connection_config", return_value=_resolved_connection()
    ):
        client = KitaruClient()

    with pytest.raises(KitaruUsageError, match="source_mode"):
        client.memories.compact(
            scope="repo_scope",
            scope_type="namespace",
            key="prefs",
            source_mode="future",  # type: ignore[arg-type]
        )


def test_memories_methods_validate_scope_key_version_and_scope_type() -> None:
    with patch(
        "kitaru.client.resolve_connection_config", return_value=_resolved_connection()
    ):
        client = KitaruClient()

    with pytest.raises(KitaruUsageError, match="Memory scope"):
        client.memories.get("prefs", scope="bad:scope", scope_type="namespace")

    with pytest.raises(KitaruUsageError, match="Memory key"):
        client.memories.history("bad:key", scope="repo_scope", scope_type="namespace")

    with pytest.raises(KitaruUsageError, match="Memory version"):
        client.memories.get(
            "prefs",
            scope="repo_scope",
            scope_type="namespace",
            version=0,
        )

    with pytest.raises(KitaruUsageError, match="Memory prefix"):
        client.memories.list(
            scope="repo_scope",
            scope_type="namespace",
            prefix="bad:prefix",
        )

    with pytest.raises(KitaruUsageError, match="Memory scope_type"):
        client.memories.set(
            "prefs",
            {"theme": "dark"},
            scope="repo_scope",
            scope_type="bogus",  # type: ignore[arg-type]
        )


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
        name="research",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={"research_context": [artifact]},
    )
    run = _DummyRun(
        status=ZenMLExecutionStatus.COMPLETED,
        flow_name="content_flow",
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
        name="research",
        status=ZenMLExecutionStatus.RETRIED,
        outputs={},
        exception_traceback="Traceback\nValueError: boom",
    )
    attempt_two = _DummyStep(
        name="research",
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
        name="research",
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
    active_stack = SimpleNamespace(orchestrator=SimpleNamespace(resume_run=MagicMock()))

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

    active_stack.orchestrator.resume_run.assert_called_once_with(
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
        name="approve_deploy",
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
        resolution="continue",
        result=True,
    )
    assert execution.status == ExecutionStatus.RUNNING


def test_get_surfaces_waiting_status_for_running_wait_condition() -> None:
    wait_condition = _dummy_wait_condition(
        name="review_draft",
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
        name="approve_release:0",
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
    wait_condition = _dummy_wait_condition(name="approve")
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
    wait_condition = _dummy_wait_condition(name="approve")
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


def test_pending_waits_returns_mapped_waits() -> None:
    wait_condition = _dummy_wait_condition(
        name="approve_deploy",
        question="Deploy to prod?",
        data_schema={"type": "boolean"},
    )
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
        pending = client.executions.pending_waits(str(run.id))

    assert len(pending) == 1
    assert pending[0].name == "approve_deploy"
    assert pending[0].question == "Deploy to prod?"
    assert pending[0].schema == {"type": "boolean"}


def test_pending_waits_returns_empty_list_when_none() -> None:
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
        pending = client.executions.pending_waits(str(run.id))

    assert pending == []


def test_abort_wait_resolves_with_abort_resolution() -> None:
    run_id = uuid4()
    wait_condition = _dummy_wait_condition(
        name="approve_deploy",
        question="Deploy to prod?",
    )
    waiting_run = _DummyRun(
        status=_paused_status(),
        flow_name="flow_a",
        run_id=run_id,
        active_wait_condition=wait_condition,
    )
    aborted_run = _DummyRun(
        status=ZenMLExecutionStatus.FAILED,
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
            _as_pipeline_run(aborted_run),
        ]
        client_mock.list_run_wait_conditions.side_effect = [
            SimpleNamespace(items=[wait_condition]),
            SimpleNamespace(items=[]),
        ]
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        execution = client.executions.abort_wait(str(run_id), wait="approve_deploy")

    client_mock.resolve_run_wait_condition.assert_called_once_with(
        run_wait_condition_id=wait_condition.id,
        resolution="abort",
        result=None,
    )
    assert execution.status == ExecutionStatus.FAILED


def test_abort_wait_rejects_when_no_pending_waits() -> None:
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
            client.executions.abort_wait(str(run.id), wait="approve")


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
    active_stack = SimpleNamespace(orchestrator=SimpleNamespace(resume_run=MagicMock()))

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

    active_stack.orchestrator.resume_run.assert_called_once_with(
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
    wait_condition = _dummy_wait_condition(name="approve")
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


def test_replay_delegates_to_flow_wrapper_when_available() -> None:
    source_run = _DummyRun(
        status=ZenMLExecutionStatus.COMPLETED,
        flow_name="sample_flow",
        snapshot=SimpleNamespace(
            pipeline_spec=SimpleNamespace(
                source=_snapshot_source(
                    module="example.flow_module",
                    attribute="__kitaru_pipeline_source_sample_flow",
                )
            )
        ),
    )
    replayed_run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="sample_flow",
    )

    replay_handle = SimpleNamespace(exec_id=str(replayed_run.id))
    replay_flow = SimpleNamespace(replay=MagicMock(return_value=replay_handle))
    replay_module = SimpleNamespace(
        sample_flow=replay_flow,
        __kitaru_pipeline_source_sample_flow=object(),
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch("kitaru.client.importlib.import_module", return_value=replay_module),
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.side_effect = [
            _as_pipeline_run(source_run),
            _as_pipeline_run(replayed_run),
        ]
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        execution = client.executions.replay(
            str(source_run.id),
            from_="write_summary",
            topic="new topic",
        )

    replay_flow.replay.assert_called_once_with(
        str(source_run.id),
        from_="write_summary",
        overrides=None,
        topic="new topic",
    )
    assert execution.exec_id == str(replayed_run.id)


def test_replay_falls_back_to_pipeline_source_when_flow_missing() -> None:
    fetch_step = _DummyStep(
        name="fetch",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={"output": []},
    )
    fetch_step.spec = SimpleNamespace(
        invocation_id="fetch",
        upstream_steps=[],
        inputs_v2={},
    )

    write_step = _DummyStep(
        name="write",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={"output": []},
    )
    write_step.spec = SimpleNamespace(
        invocation_id="write",
        upstream_steps=["fetch"],
        inputs_v2={},
    )

    source_run = _DummyRun(
        status=ZenMLExecutionStatus.COMPLETED,
        flow_name="sample_flow",
        steps={fetch_step.name: fetch_step, write_step.name: write_step},
        snapshot=SimpleNamespace(
            pipeline_spec=SimpleNamespace(
                source=_snapshot_source(
                    module="example.flow_module",
                    attribute="__kitaru_pipeline_source_sample_flow",
                )
            )
        ),
    )
    replayed_run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="sample_flow",
    )

    replay_pipeline = SimpleNamespace(
        replay=MagicMock(return_value=_as_pipeline_run(replayed_run))
    )
    replay_module = SimpleNamespace(
        __kitaru_pipeline_source_sample_flow=replay_pipeline,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch(
            "kitaru.client._resolve_flow_for_replay",
            side_effect=KitaruRuntimeError("no replay flow"),
        ),
        patch("kitaru.client.importlib.import_module", return_value=replay_module),
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.side_effect = [
            _as_pipeline_run(source_run),
            _as_pipeline_run(replayed_run),
        ]
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        execution = client.executions.replay(
            str(source_run.id),
            from_="write",
        )

    replay_pipeline.replay.assert_called_once()
    replay_kwargs = replay_pipeline.replay.call_args.kwargs
    assert replay_kwargs["pipeline_run"] == source_run.id
    assert replay_kwargs["skip"] == {"fetch"}
    assert execution.exec_id == str(replayed_run.id)


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
                name="writer",
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


def test_logs_merges_step_entries_in_timestamp_order() -> None:
    step_research = _DummyStep(
        name="research",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={},
    )
    step_write = _DummyStep(
        name="write",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={},
    )
    step_research.start_time = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    step_write.start_time = datetime(2026, 3, 9, 10, 5, tzinfo=UTC)

    run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        steps={
            step_write.name: step_write,
            step_research.name: step_research,
        },
    )

    fake_store = Mock()

    def _get(path: str, params: dict[str, str]) -> list[dict[str, Any]]:
        assert params == {"source": "step"}
        if path == f"/steps/{step_research.id}/logs":
            return [
                {
                    "message": "research-start",
                    "timestamp": "2026-03-09T10:00:01+00:00",
                    "level": "INFO",
                },
                {
                    "message": "research-end",
                    "timestamp": "2026-03-09T10:00:03+00:00",
                    "level": "INFO",
                },
            ]
        if path == f"/steps/{step_write.id}/logs":
            return [
                {
                    "message": "write-start",
                    "timestamp": "2026-03-09T10:00:02+00:00",
                    "level": "INFO",
                }
            ]
        raise AssertionError(f"Unexpected path: {path}")

    fake_store.get.side_effect = _get

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch("kitaru.client._ExecutionsAPI._rest_store", return_value=fake_store),
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)

        client = KitaruClient()
        entries = client.executions.logs(str(run.id))

    assert [entry.message for entry in entries] == [
        "research-start",
        "write-start",
        "research-end",
    ]
    assert [entry.checkpoint_name for entry in entries] == [
        "research",
        "write",
        "research",
    ]


def test_logs_filters_by_checkpoint_name() -> None:
    step_research = _DummyStep(
        name="research",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={},
    )
    step_write = _DummyStep(
        name="write",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={},
    )
    step_research.start_time = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    step_write.start_time = datetime(2026, 3, 9, 10, 5, tzinfo=UTC)

    run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        steps={step_research.name: step_research, step_write.name: step_write},
    )

    fake_store = Mock()
    fake_store.get.return_value = [
        {"message": "research-only", "timestamp": "2026-03-09T10:00:01+00:00"}
    ]

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch("kitaru.client._ExecutionsAPI._rest_store", return_value=fake_store),
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)

        client = KitaruClient()
        entries = client.executions.logs(str(run.id), checkpoint="research")

    assert len(entries) == 1
    assert entries[0].checkpoint_name == "research"
    fake_store.get.assert_called_once_with(
        f"/steps/{step_research.id}/logs", params={"source": "step"}
    )


def test_logs_runner_source_uses_run_endpoint() -> None:
    run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        steps={},
    )

    fake_store = Mock()
    fake_store.get.return_value = [
        {"message": "stack-log", "timestamp": "2026-03-09T10:00:01+00:00"}
    ]

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch("kitaru.client._ExecutionsAPI._rest_store", return_value=fake_store),
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)

        client = KitaruClient()
        entries = client.executions.logs(str(run.id), source="runner")

    assert len(entries) == 1
    assert entries[0].source == "runner"
    fake_store.get.assert_called_once_with(
        f"/runs/{run.id}/logs", params={"source": "runner"}
    )


def test_logs_rejects_checkpoint_with_runner_source() -> None:
    run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        steps={},
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
        with pytest.raises(KitaruUsageError, match="checkpoint"):
            client.executions.logs(
                str(run.id),
                checkpoint="research",
                source="runner",
            )


def test_logs_early_stops_when_limit_is_reached() -> None:
    first_step = _DummyStep(
        name="first",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={},
    )
    second_step = _DummyStep(
        name="second",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={},
    )
    first_step.start_time = datetime(2026, 3, 9, 10, 0, tzinfo=UTC)
    second_step.start_time = datetime(2026, 3, 9, 10, 5, tzinfo=UTC)

    run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        steps={first_step.name: first_step, second_step.name: second_step},
    )

    fake_store = Mock()

    def _get(path: str, params: dict[str, str]) -> list[dict[str, Any]]:
        if path == f"/steps/{first_step.id}/logs":
            return [
                {"message": "first-1", "timestamp": "2026-03-09T10:00:01+00:00"},
                {"message": "first-2", "timestamp": "2026-03-09T10:00:02+00:00"},
            ]
        if path == f"/steps/{second_step.id}/logs":
            return [{"message": "second-1", "timestamp": "2026-03-09T10:00:03+00:00"}]
        raise AssertionError(f"Unexpected path: {path}")

    fake_store.get.side_effect = _get

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch("kitaru.client._ExecutionsAPI._rest_store", return_value=fake_store),
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)

        client = KitaruClient()
        entries = client.executions.logs(str(run.id), limit=2)

    assert [entry.message for entry in entries] == ["first-1", "first-2"]
    assert fake_store.get.call_count == 1


def test_logs_require_server_backed_connection() -> None:
    step = _DummyStep(
        name="research",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={},
    )
    run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
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
        client_mock.zen_store = object()

        client = KitaruClient()
        with pytest.raises(KitaruLogRetrievalError, match="server-backed"):
            client.executions.logs(str(run.id))


def test_logs_map_otel_retrieval_errors_to_kitaru_error() -> None:
    step = _DummyStep(
        name="research",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={},
    )
    run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        steps={step.name: step},
    )

    fake_store = Mock()
    fake_store.get.side_effect = RuntimeError(
        "NotImplementedError: OTEL log store fetch is not implemented"
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch("kitaru.client._ExecutionsAPI._rest_store", return_value=fake_store),
        patch(
            "kitaru.client.active_stack_log_store",
            return_value=SimpleNamespace(
                backend="otel",
                endpoint="https://logs.example.com",
                stack_name="prod",
            ),
        ),
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)

        client = KitaruClient()
        with pytest.raises(KitaruLogRetrievalError, match="OTEL backend"):
            client.executions.logs(str(run.id))


def test_logs_return_empty_list_when_backend_reports_no_entries() -> None:
    step = _DummyStep(
        name="research",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={},
    )
    run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="flow_a",
        steps={step.name: step},
    )

    fake_store = Mock()
    fake_store.get.side_effect = RuntimeError(
        f"No logs found for source 'step' in step {step.id}"
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch("kitaru.client._ExecutionsAPI._rest_store", return_value=fake_store),
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(run)

        client = KitaruClient()
        entries = client.executions.logs(str(run.id))

    assert entries == []


# ── Replay analytics instrumentation tests ───────────────────────────────────


def test_replay_fallback_emits_requested_and_replayed_events() -> None:
    """Successful fallback replay should emit REPLAY_REQUESTED then FLOW_REPLAYED."""
    fetch_step = _DummyStep(
        name="fetch",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={"output": []},
    )
    fetch_step.spec = SimpleNamespace(
        invocation_id="fetch",
        upstream_steps=[],
        inputs_v2={},
    )
    write_step = _DummyStep(
        name="write",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={"output": []},
    )
    write_step.spec = SimpleNamespace(
        invocation_id="write",
        upstream_steps=["fetch"],
        inputs_v2={},
    )

    source_run = _DummyRun(
        status=ZenMLExecutionStatus.COMPLETED,
        flow_name="sample_flow",
        steps={fetch_step.name: fetch_step, write_step.name: write_step},
        snapshot=SimpleNamespace(
            pipeline_spec=SimpleNamespace(
                source=_snapshot_source(
                    module="example.flow_module",
                    attribute="__kitaru_pipeline_source_sample_flow",
                )
            )
        ),
    )
    replayed_run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="sample_flow",
    )

    replay_pipeline = SimpleNamespace(
        replay=MagicMock(return_value=_as_pipeline_run(replayed_run))
    )
    replay_module = SimpleNamespace(
        __kitaru_pipeline_source_sample_flow=replay_pipeline,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch(
            "kitaru.client._resolve_flow_for_replay",
            side_effect=KitaruRuntimeError("no replay flow"),
        ),
        patch("kitaru.client.track") as track_mock,
        patch("kitaru.client.importlib.import_module", return_value=replay_module),
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.side_effect = [
            _as_pipeline_run(source_run),
            _as_pipeline_run(replayed_run),
        ]
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        client.executions.replay(str(source_run.id), from_="write")

    assert track_mock.call_count == 2
    requested_call = track_mock.call_args_list[0]
    assert requested_call.args[0] == AnalyticsEvent.REPLAY_REQUESTED
    assert requested_call.args[1]["replay_path"] == "pipeline_fallback"
    assert requested_call.args[1]["from_checkpoint"] == "write"

    replayed_call = track_mock.call_args_list[1]
    assert replayed_call.args[0] == AnalyticsEvent.FLOW_REPLAYED
    assert replayed_call.args[1]["replay_path"] == "pipeline_fallback"


def test_replay_fallback_failure_emits_requested_then_failed() -> None:
    """Failed fallback replay should emit REPLAY_REQUESTED then REPLAY_FAILED."""
    fetch_step = _DummyStep(
        name="fetch",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={"output": []},
    )
    fetch_step.spec = SimpleNamespace(
        invocation_id="fetch",
        upstream_steps=[],
        inputs_v2={},
    )
    write_step = _DummyStep(
        name="write",
        status=ZenMLExecutionStatus.COMPLETED,
        outputs={"output": []},
    )
    write_step.spec = SimpleNamespace(
        invocation_id="write",
        upstream_steps=["fetch"],
        inputs_v2={},
    )

    source_run = _DummyRun(
        status=ZenMLExecutionStatus.COMPLETED,
        flow_name="sample_flow",
        steps={fetch_step.name: fetch_step, write_step.name: write_step},
        snapshot=SimpleNamespace(
            pipeline_spec=SimpleNamespace(
                source=_snapshot_source(
                    module="example.flow_module",
                    attribute="__kitaru_pipeline_source_sample_flow",
                )
            )
        ),
    )

    replay_pipeline = SimpleNamespace(
        replay=MagicMock(side_effect=RuntimeError("backend crash"))
    )
    replay_module = SimpleNamespace(
        __kitaru_pipeline_source_sample_flow=replay_pipeline,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch(
            "kitaru.client._resolve_flow_for_replay",
            side_effect=KitaruRuntimeError("no replay flow"),
        ),
        patch("kitaru.client.track") as track_mock,
        patch("kitaru.client.importlib.import_module", return_value=replay_module),
        pytest.raises(Exception, match="backend crash"),
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.return_value = _as_pipeline_run(source_run)

        client = KitaruClient()
        client.executions.replay(str(source_run.id), from_="write")

    assert track_mock.call_count == 2
    requested_call = track_mock.call_args_list[0]
    assert requested_call.args[0] == AnalyticsEvent.REPLAY_REQUESTED

    failed_call = track_mock.call_args_list[1]
    assert failed_call.args[0] == AnalyticsEvent.REPLAY_FAILED
    assert failed_call.args[1]["error_type"] == "RuntimeError"
    assert "failure_origin" in failed_call.args[1]


def test_replay_delegate_does_not_emit_fallback_analytics() -> None:
    """Delegated replay (via flow wrapper) should NOT emit analytics from client."""
    source_run = _DummyRun(
        status=ZenMLExecutionStatus.COMPLETED,
        flow_name="sample_flow",
        snapshot=SimpleNamespace(
            pipeline_spec=SimpleNamespace(
                source=_snapshot_source(
                    module="example.flow_module",
                    attribute="__kitaru_pipeline_source_sample_flow",
                )
            )
        ),
    )
    replayed_run = _DummyRun(
        status=ZenMLExecutionStatus.RUNNING,
        flow_name="sample_flow",
    )

    replay_handle = SimpleNamespace(exec_id=str(replayed_run.id))
    replay_flow = SimpleNamespace(replay=MagicMock(return_value=replay_handle))
    replay_module = SimpleNamespace(
        sample_flow=replay_flow,
        __kitaru_pipeline_source_sample_flow=object(),
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.Client") as client_cls,
        patch("kitaru.client.track") as track_mock,
        patch("kitaru.client.importlib.import_module", return_value=replay_module),
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.side_effect = [
            _as_pipeline_run(source_run),
            _as_pipeline_run(replayed_run),
        ]
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        client.executions.replay(str(source_run.id), from_="write")

    track_mock.assert_not_called()


def test_retry_emits_execution_retried_event() -> None:
    """Successful retry should emit EXECUTION_RETRIED analytics event."""
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
    active_stack = SimpleNamespace(orchestrator=SimpleNamespace(resume_run=MagicMock()))

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.track") as track_mock,
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.active_stack_model = SimpleNamespace(id=old_stack_id)
        client_mock.active_stack = active_stack
        client_mock.get_pipeline_run.side_effect = [
            _as_pipeline_run(failed),
            _as_pipeline_run(retried),
        ]
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        client.executions.retry(str(run_id))

    track_mock.assert_called_once_with(
        AnalyticsEvent.EXECUTION_RETRIED,
        {},
    )


def test_resume_emits_execution_resumed_event() -> None:
    """Successful resume should emit EXECUTION_RESUMED analytics event."""
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
    active_stack = SimpleNamespace(orchestrator=SimpleNamespace(resume_run=MagicMock()))

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.track") as track_mock,
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
        client.executions.resume(str(run_id))

    track_mock.assert_called_once_with(
        AnalyticsEvent.EXECUTION_RESUMED,
        {},
    )


def test_cancel_emits_execution_cancelled_event() -> None:
    """Successful cancel should emit EXECUTION_CANCELLED analytics event."""
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
        patch("kitaru.client.track") as track_mock,
        patch("kitaru.client.Client") as client_cls,
        patch("kitaru.client.stop_run"),
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.side_effect = [
            _as_pipeline_run(running),
            _as_pipeline_run(stopped),
        ]
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])
        client_mock.list_run_wait_conditions.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        client.executions.cancel(str(run_id))

    track_mock.assert_called_once_with(
        AnalyticsEvent.EXECUTION_CANCELLED,
        {},
    )


def test_input_emits_wait_resolved_event() -> None:
    """Providing input should emit WAIT_RESOLVED with continue resolution."""
    run_id = uuid4()
    wait_condition = _dummy_wait_condition(
        name="approve_deploy",
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
        patch("kitaru.client.track") as track_mock,
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
        client.executions.input(
            str(run_id),
            wait="approve_deploy",
            value=True,
        )

    track_mock.assert_called_once_with(
        AnalyticsEvent.WAIT_RESOLVED,
        {
            "resolution": "continue",
        },
    )


def test_abort_wait_emits_wait_resolved_event() -> None:
    """Aborting a wait should emit WAIT_RESOLVED with abort resolution."""
    run_id = uuid4()
    wait_condition = _dummy_wait_condition(
        name="approve_deploy",
        question="Deploy to prod?",
    )
    waiting_run = _DummyRun(
        status=_paused_status(),
        flow_name="flow_a",
        run_id=run_id,
        active_wait_condition=wait_condition,
    )
    aborted_run = _DummyRun(
        status=ZenMLExecutionStatus.FAILED,
        flow_name="flow_a",
        run_id=run_id,
    )

    with (
        patch(
            "kitaru.client.resolve_connection_config",
            return_value=_resolved_connection(),
        ),
        patch("kitaru.client.track") as track_mock,
        patch("kitaru.client.Client") as client_cls,
    ):
        client_mock = client_cls.return_value
        client_mock.get_pipeline_run.side_effect = [
            _as_pipeline_run(waiting_run),
            _as_pipeline_run(aborted_run),
        ]
        client_mock.list_run_wait_conditions.side_effect = [
            SimpleNamespace(items=[wait_condition]),
            SimpleNamespace(items=[]),
        ]
        client_mock.list_run_steps.return_value = SimpleNamespace(items=[])

        client = KitaruClient()
        client.executions.abort_wait(str(run_id), wait="approve_deploy")

    track_mock.assert_called_once_with(
        AnalyticsEvent.WAIT_RESOLVED,
        {
            "resolution": "abort",
        },
    )
