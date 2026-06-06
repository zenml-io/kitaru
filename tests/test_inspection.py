"""Tests for `kitaru.inspection` serialization helpers."""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, call, patch
from uuid import uuid4

import pytest

from kitaru._client._mappers import _map_checkpoint_call
from kitaru.client import (
    ArtifactRef,
    CheckpointAttempt,
    CheckpointCall,
    Execution,
    ExecutionStatistics,
    ExecutionStatisticsGroup,
    ExecutionStatus,
    FailureInfo,
    LogEntry,
    PendingWait,
)
from kitaru.config import (
    ActiveEnvironmentVariable,
    ActiveStackLogStore,
    FrozenExecutionSpec,
    ImageSettings,
    KitaruConfig,
    ModelAliasEntry,
    ResolvedConnectionConfig,
    ResolvedExecutionConfig,
    ResolvedLogStore,
    StackComponentDetails,
    StackDetails,
    StackInfo,
    _StackCreateResult,
    _StackDeleteResult,
)
from kitaru.errors import FailureOrigin
from kitaru.inspection import (
    ActiveConfigSelectionProvenance,
    RuntimeSnapshot,
    build_runtime_snapshot,
    is_registered_local_server_url,
    serialize_artifact_ref,
    serialize_artifact_value,
    serialize_checkpoint_attempt,
    serialize_checkpoint_call,
    serialize_execution,
    serialize_execution_statistics,
    serialize_execution_statistics_group,
    serialize_execution_summary,
    serialize_failure,
    serialize_log_entry,
    serialize_model_alias,
    serialize_pending_wait,
    serialize_resolved_log_store,
    serialize_runtime_snapshot,
    serialize_secret_detail,
    serialize_secret_summary,
    serialize_stack,
    serialize_stack_create_result,
    serialize_stack_delete_result,
    serialize_stack_details,
    to_jsonable,
    uses_stale_local_server_url,
)


@dataclass(frozen=True)
class _NestedData:
    label: str
    created_at: datetime
    tags: tuple[str, ...]


class _Color(Enum):
    RED = "red"


class _ModelDumpable:
    def model_dump(self, *, mode: str) -> dict[str, object]:
        assert mode == "python"
        return {
            "timestamp": datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
            "values": {3, 1},
        }


class _Unjsonable:
    def __repr__(self) -> str:
        return "<unjsonable>"


class _BrokenGlobalConfig:
    config_directory = "/tmp/fake-kitaru-config"

    @property
    def store_configuration(self) -> Any:
        raise ImportError("missing local runtime support")

    @property
    def uses_local_store(self) -> bool:
        raise ImportError("missing local runtime support")


def _sample_failure() -> FailureInfo:
    return FailureInfo(
        message="Checkpoint failed",
        exception_type="ValueError",
        traceback="Traceback...\nValueError: boom",
        origin=FailureOrigin.USER_CODE,
    )


def _sample_pending_wait() -> PendingWait:
    return PendingWait(
        wait_id="wait-1",
        name="approve_draft",
        question="Approve this draft?",
        schema={"type": "boolean"},
        metadata={"attempt": 1},
        entered_waiting_at=datetime(2026, 3, 14, 10, 30, tzinfo=UTC),
    )


def _sample_artifact(name: str = "research_context") -> ArtifactRef:
    return ArtifactRef(
        artifact_id="artifact-1",
        name=name,
        kind="context",
        save_type="manual",
        producing_call="research",
        metadata={"source": "notes"},
        _client=cast(Any, SimpleNamespace()),
    )


def _sample_checkpoint_attempt() -> CheckpointAttempt:
    return CheckpointAttempt(
        attempt_id="attempt-1",
        status=ExecutionStatus.FAILED,
        started_at=datetime(2026, 3, 14, 10, 0, tzinfo=UTC),
        ended_at=datetime(2026, 3, 14, 10, 5, tzinfo=UTC),
        metadata={"retry": 1},
        failure=_sample_failure(),
    )


def _sample_checkpoint_call() -> CheckpointCall:
    return CheckpointCall(
        call_id="call-1",
        name="research",
        checkpoint_type="tool_call",
        status=ExecutionStatus.FAILED,
        started_at=datetime(2026, 3, 14, 10, 0, tzinfo=UTC),
        ended_at=datetime(2026, 3, 14, 10, 10, tzinfo=UTC),
        metadata={"latency_ms": 321},
        original_call_id="call-0",
        parent_call_ids=["parent-1"],
        failure=_sample_failure(),
        attempts=[_sample_checkpoint_attempt()],
        artifacts=[_sample_artifact()],
    )


def _sample_execution() -> Execution:
    return Execution(
        exec_id="kr-123",
        flow_id="flow-123",
        flow_name="content_pipeline",
        status=ExecutionStatus.WAITING,
        started_at=datetime(2026, 3, 14, 9, 55, tzinfo=UTC),
        ended_at=None,
        stack_name="prod",
        metadata={"owner": "alice"},
        status_reason="Waiting for human input",
        failure=None,
        pending_wait=_sample_pending_wait(),
        frozen_execution_spec=FrozenExecutionSpec(
            resolved_execution=ResolvedExecutionConfig(
                stack=None,
                image=None,
                cache=False,
                retries=0,
            ),
            flow_defaults=KitaruConfig(
                image=ImageSettings(dockerfile="Dockerfile"),
            ),
            connection=ResolvedConnectionConfig(),
        ),
        original_exec_id="kr-100",
        checkpoints=[_sample_checkpoint_call()],
        artifacts=[_sample_artifact("final_summary")],
        _client=cast(Any, SimpleNamespace()),
    )


def _sample_secret(*, private: bool) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        name="openai-credentials",
        private=private,
        values={"API_KEY": object(), "REGION": object()},
        has_missing_values=True,
        secret_values={"API_KEY": "secret-value"},
    )


def test_to_jsonable_converts_supported_values() -> None:
    payload = {
        1: "one",
        "timestamp": datetime(2026, 3, 14, 12, 0, tzinfo=UTC),
        "color": _Color.RED,
        "path": Path("docs/reference.mdx"),
        "items": ("a", "b"),
        "tags": {"beta", "alpha"},
        "dumped": _ModelDumpable(),
    }

    assert to_jsonable(payload, fallback_repr=False) == {
        "1": "one",
        "timestamp": "2026-03-14T12:00:00+00:00",
        "color": "red",
        "path": "docs/reference.mdx",
        "items": ["a", "b"],
        "tags": ["alpha", "beta"],
        "dumped": {
            "timestamp": "2026-03-14T12:00:00+00:00",
            "values": [1, 3],
        },
    }


def test_to_jsonable_recurses_through_dataclasses() -> None:
    value = _NestedData(
        label="snapshot",
        created_at=datetime(2026, 3, 14, 13, 15, tzinfo=UTC),
        tags=("sdk", "tests"),
    )

    assert to_jsonable(value, fallback_repr=False) == {
        "label": "snapshot",
        "created_at": "2026-03-14T13:15:00+00:00",
        "tags": ["sdk", "tests"],
    }


def test_to_jsonable_raises_for_uuid_without_fallback_repr() -> None:
    with pytest.raises(TypeError, match=r"uuid\.UUID"):
        to_jsonable(uuid4(), fallback_repr=False)


def test_to_jsonable_uses_repr_when_requested() -> None:
    assert to_jsonable(_Unjsonable(), fallback_repr=True) == "<unjsonable>"


def test_serialize_failure_contract() -> None:
    assert serialize_failure(None) is None
    assert serialize_failure(_sample_failure()) == {
        "message": "Checkpoint failed",
        "exception_type": "ValueError",
        "traceback": "Traceback...\nValueError: boom",
        "origin": "user_code",
    }


def test_serialize_pending_wait_contract() -> None:
    assert serialize_pending_wait(None) is None
    assert serialize_pending_wait(_sample_pending_wait()) == {
        "wait_id": "wait-1",
        "name": "approve_draft",
        "question": "Approve this draft?",
        "schema": {"type": "boolean"},
        "metadata": {"attempt": 1},
        "entered_waiting_at": "2026-03-14T10:30:00+00:00",
    }


def test_serialize_artifact_ref_contract() -> None:
    assert serialize_artifact_ref(_sample_artifact()) == {
        "artifact_id": "artifact-1",
        "name": "research_context",
        "kind": "context",
        "save_type": "manual",
        "producing_call": "research",
        "metadata": {"source": "notes"},
    }


def test_serialize_input_artifact_ref_includes_direction_fields() -> None:
    artifact = ArtifactRef(
        artifact_id="artifact-input-1",
        name="messages",
        kind="prompt",
        save_type="step_output",
        producing_call=None,
        metadata={},
        direction="input",
        input_type="step_output",
        _client=cast(Any, SimpleNamespace()),
    )

    assert serialize_artifact_ref(artifact) == {
        "artifact_id": "artifact-input-1",
        "name": "messages",
        "kind": "prompt",
        "save_type": "step_output",
        "producing_call": None,
        "metadata": {},
        "direction": "input",
        "input_type": "step_output",
    }


def test_checkpoint_mapping_includes_structural_input_artifacts() -> None:
    input_artifact = SimpleNamespace(
        id="input-artifact-id",
        name="raw-zenml-input-artifact-name",
        run_metadata={},
        save_type=SimpleNamespace(value="step_output"),
        input_type=SimpleNamespace(value="step_output"),
    )
    output_artifact = SimpleNamespace(
        id="output-artifact-id",
        name="output",
        run_metadata={"kitaru_artifact_type": "response"},
        save_type=SimpleNamespace(value="step_output"),
    )
    step = SimpleNamespace(
        id="step-id",
        name="agent_model_request",
        status="completed",
        start_time=None,
        end_time=None,
        run_metadata={},
        original_step_run_id=None,
        parent_step_ids=[],
        type=SimpleNamespace(value="llm_call"),
        inputs={"input": [input_artifact]},
        outputs={"output": [output_artifact]},
    )

    checkpoint = _map_checkpoint_call(
        step=cast(Any, step),
        client=cast(Any, SimpleNamespace()),
        attempts_by_lineage={},
    )

    assert [
        (artifact.name, artifact.direction) for artifact in checkpoint.artifacts
    ] == [
        ("input", "input"),
        ("output", "output"),
    ]
    input_ref = checkpoint.artifacts[0]
    assert input_ref.kind == "prompt"
    assert input_ref.producing_call is None
    assert input_ref.input_type == "step_output"
    assert checkpoint.artifacts[1].kind == "response"


def test_checkpoint_mapping_includes_tool_call_structural_input_artifacts() -> None:
    input_artifact = SimpleNamespace(
        id="input-artifact-id",
        name="raw-zenml-tool-args-artifact-name",
        run_metadata={},
        save_type=SimpleNamespace(value="step_output"),
        input_type=SimpleNamespace(value="step_output"),
    )
    output_artifact = SimpleNamespace(
        id="output-artifact-id",
        name="output",
        run_metadata={},
        save_type=SimpleNamespace(value="step_output"),
    )
    step = SimpleNamespace(
        id="step-id",
        name="agent_tool_call",
        status="completed",
        start_time=None,
        end_time=None,
        run_metadata={},
        original_step_run_id=None,
        parent_step_ids=[],
        type=SimpleNamespace(value="tool_call"),
        inputs={"tool_args": [input_artifact]},
        outputs={"output": [output_artifact]},
    )

    checkpoint = _map_checkpoint_call(
        step=cast(Any, step),
        client=cast(Any, SimpleNamespace()),
        attempts_by_lineage={},
    )

    assert [
        (artifact.name, artifact.direction) for artifact in checkpoint.artifacts
    ] == [
        ("tool_args", "input"),
        ("output", "output"),
    ]
    assert checkpoint.artifacts[0].kind == "input"
    assert checkpoint.artifacts[0].producing_call is None
    assert checkpoint.artifacts[0].input_type == "step_output"
    assert checkpoint.artifacts[1].kind == "output"


def test_checkpoint_mapping_does_not_expose_generic_input_artifacts() -> None:
    input_artifact = SimpleNamespace(
        id="input-artifact-id",
        name="raw-zenml-input-artifact-name",
        run_metadata={},
        save_type=SimpleNamespace(value="step_output"),
        input_type=SimpleNamespace(value="step_output"),
    )
    step = SimpleNamespace(
        id="step-id",
        name="ordinary_checkpoint",
        status="completed",
        start_time=None,
        end_time=None,
        run_metadata={},
        original_step_run_id=None,
        parent_step_ids=[],
        type=SimpleNamespace(value="checkpoint"),
        inputs={"messages": [input_artifact]},
        outputs={},
    )

    checkpoint = _map_checkpoint_call(
        step=cast(Any, step),
        client=cast(Any, SimpleNamespace()),
        attempts_by_lineage={},
    )

    assert checkpoint.artifacts == []


def test_serialize_artifact_value_json_contract() -> None:
    payload = serialize_artifact_value(
        {
            "timestamp": datetime(2026, 3, 14, 14, 0, tzinfo=UTC),
            "tags": {"beta", "alpha"},
        }
    )

    assert payload == {
        "value": {
            "timestamp": "2026-03-14T14:00:00+00:00",
            "tags": ["alpha", "beta"],
        },
        "value_format": "json",
        "value_type": "builtins.dict",
    }


def test_serialize_artifact_value_repr_fallback_contract() -> None:
    payload = serialize_artifact_value(_Unjsonable())

    assert payload == {
        "value": "<unjsonable>",
        "value_format": "repr",
        "value_type": "tests.test_inspection._Unjsonable",
    }


def test_serialize_checkpoint_attempt_contract() -> None:
    assert serialize_checkpoint_attempt(_sample_checkpoint_attempt()) == {
        "attempt_id": "attempt-1",
        "status": "failed",
        "started_at": "2026-03-14T10:00:00+00:00",
        "ended_at": "2026-03-14T10:05:00+00:00",
        "metadata": {"retry": 1},
        "failure": {
            "message": "Checkpoint failed",
            "exception_type": "ValueError",
            "traceback": "Traceback...\nValueError: boom",
            "origin": "user_code",
        },
        "llm_usage_records": [],
    }


def test_serialize_checkpoint_call_contract() -> None:
    payload = serialize_checkpoint_call(_sample_checkpoint_call())

    assert payload == {
        "call_id": "call-1",
        "name": "research",
        "checkpoint_type": "tool_call",
        "status": "failed",
        "started_at": "2026-03-14T10:00:00+00:00",
        "ended_at": "2026-03-14T10:10:00+00:00",
        "metadata": {"latency_ms": 321},
        "original_call_id": "call-0",
        "parent_call_ids": ["parent-1"],
        "failure": {
            "message": "Checkpoint failed",
            "exception_type": "ValueError",
            "traceback": "Traceback...\nValueError: boom",
            "origin": "user_code",
        },
        "attempts": [
            {
                "attempt_id": "attempt-1",
                "status": "failed",
                "started_at": "2026-03-14T10:00:00+00:00",
                "ended_at": "2026-03-14T10:05:00+00:00",
                "metadata": {"retry": 1},
                "failure": {
                    "message": "Checkpoint failed",
                    "exception_type": "ValueError",
                    "traceback": "Traceback...\nValueError: boom",
                    "origin": "user_code",
                },
                "llm_usage_records": [],
            }
        ],
        "artifacts": [
            {
                "artifact_id": "artifact-1",
                "name": "research_context",
                "kind": "context",
                "save_type": "manual",
                "producing_call": "research",
                "metadata": {"source": "notes"},
            }
        ],
        "llm_usage_records": [],
    }


def test_serialize_execution_statistics_contract() -> None:
    statistics = ExecutionStatistics(
        groups=[
            ExecutionStatisticsGroup(
                keys={"status": "completed", "day": "2026-03-14"},
                execution_count=12,
                metrics={"duration_avg": 15.5},
            ),
            ExecutionStatisticsGroup(keys={"status": "failed"}, execution_count=2),
        ],
        truncated=True,
    )

    assert serialize_execution_statistics_group(statistics.groups[0]) == {
        "keys": {"status": "completed", "day": "2026-03-14"},
        "execution_count": 12,
        "metrics": {"duration_avg": 15.5},
    }
    assert serialize_execution_statistics(statistics) == {
        "groups": [
            {
                "keys": {"status": "completed", "day": "2026-03-14"},
                "execution_count": 12,
                "metrics": {"duration_avg": 15.5},
            },
            {
                "keys": {"status": "failed"},
                "execution_count": 2,
                "metrics": {},
            },
        ],
        "truncated": True,
        "group_count": 2,
    }


def test_serialize_execution_summary_contract() -> None:
    payload = serialize_execution_summary(_sample_execution())

    assert payload == {
        "exec_id": "kr-123",
        "flow_id": "flow-123",
        "flow_name": "content_pipeline",
        "status": "waiting",
        "started_at": "2026-03-14T09:55:00+00:00",
        "ended_at": None,
        "stack_name": "prod",
        "status_reason": "Waiting for human input",
        "pending_wait": {
            "wait_id": "wait-1",
            "name": "approve_draft",
            "question": "Approve this draft?",
            "schema": {"type": "boolean"},
            "metadata": {"attempt": 1},
            "entered_waiting_at": "2026-03-14T10:30:00+00:00",
        },
        "failure": None,
        "metadata": {"owner": "alice"},
        "checkpoint_count": 1,
        "artifact_count": 1,
        "llm_usage_summary": None,
    }


def test_serialize_execution_contract() -> None:
    payload = serialize_execution(_sample_execution())

    assert set(payload) == {
        "exec_id",
        "flow_id",
        "flow_name",
        "status",
        "started_at",
        "ended_at",
        "stack_name",
        "status_reason",
        "pending_wait",
        "failure",
        "metadata",
        "checkpoint_count",
        "artifact_count",
        "llm_usage_summary",
        "frozen_execution_spec",
        "original_exec_id",
        "checkpoints",
        "artifacts",
    }
    spec = payload["frozen_execution_spec"]
    assert spec["resolved_execution"] == {
        "stack": None,
        "image": None,
        "cache": False,
        "retries": 0,
    }
    assert spec["flow_defaults"] == {
        "stack": None,
        "image": {
            "base_image": None,
            "requirements": None,
            "dockerfile": "Dockerfile",
            "build_context_root": None,
            "environment": None,
            "secret_environment_from": None,
            "apt_packages": None,
            "replicate_local_python_environment": None,
            "image_tag": None,
            "target_repository": None,
            "user": None,
            "platform": None,
        },
        "cache": None,
        "retries": None,
        "server_url": None,
        "auth_token": None,
        "project": None,
    }
    assert spec["connection"] == {
        "server_url": None,
        "auth_token": None,
        "project": None,
    }
    assert payload["original_exec_id"] == "kr-100"
    assert payload["checkpoints"][0]["name"] == "research"
    assert payload["checkpoints"][0]["checkpoint_type"] == "tool_call"
    assert payload["artifacts"][0]["name"] == "final_summary"
    assert payload["pending_wait"]["wait_id"] == "wait-1"


def test_serialize_stack_contract() -> None:
    stack = StackInfo(id="stack-1", name="prod", is_active=True)

    assert serialize_stack(stack) == {
        "id": "stack-1",
        "name": "prod",
        "is_active": True,
    }
    assert serialize_stack(stack, is_managed=False) == {
        "id": "stack-1",
        "name": "prod",
        "is_active": True,
        "is_managed": False,
    }


def test_serialize_stack_create_result_contract() -> None:
    result = _StackCreateResult(
        stack=StackInfo(id="stack-1", name="prod", is_active=True),
        previous_active_stack="default",
        components_created=("prod (orchestrator)", "prod (artifact_store)"),
        stack_type="kubernetes",
        service_connectors_created=("prod-connector",),
        resources={"cluster": "demo"},
    )

    assert serialize_stack_create_result(result) == {
        "id": "stack-1",
        "name": "prod",
        "is_active": True,
        "previous_active_stack": "default",
        "components_created": ["prod (orchestrator)", "prod (artifact_store)"],
        "stack_type": "kubernetes",
        "service_connectors_created": ["prod-connector"],
        "resources": {"cluster": "demo"},
    }


def test_serialize_stack_create_result_omits_empty_optional_fields() -> None:
    result = _StackCreateResult(
        stack=StackInfo(id="stack-2", name="dev", is_active=False),
        previous_active_stack=None,
        components_created=("dev (orchestrator)",),
        stack_type="local",
    )

    payload = serialize_stack_create_result(result)

    assert payload == {
        "id": "stack-2",
        "name": "dev",
        "is_active": False,
        "previous_active_stack": None,
        "components_created": ["dev (orchestrator)"],
        "stack_type": "local",
    }
    assert "service_connectors_created" not in payload
    assert "resources" not in payload


def test_serialize_stack_delete_result_contract() -> None:
    result = _StackDeleteResult(
        deleted_stack="prod",
        components_deleted=("prod (orchestrator)", "prod (artifact_store)"),
        new_active_stack="default",
        recursive=True,
    )

    assert serialize_stack_delete_result(result) == {
        "deleted_stack": "prod",
        "components_deleted": ["prod (orchestrator)", "prod (artifact_store)"],
        "new_active_stack": "default",
        "recursive": True,
    }


def test_serialize_stack_details_contract() -> None:
    details = StackDetails(
        stack=StackInfo(id="stack-1", name="prod", is_active=True),
        is_managed=True,
        stack_type="kubernetes",
        components=(
            StackComponentDetails(
                role="runner",
                name="prod-runner",
                backend="kubernetes",
                details=(("cluster", "demo"), ("namespace", "default")),
            ),
            StackComponentDetails(
                role="storage",
                name="prod-storage",
                purpose="stores artifacts",
            ),
        ),
    )

    assert serialize_stack_details(details) == {
        "id": "stack-1",
        "name": "prod",
        "is_active": True,
        "is_managed": True,
        "stack_type": "kubernetes",
        "components": [
            {
                "role": "runner",
                "name": "prod-runner",
                "backend": "kubernetes",
                "details": {"cluster": "demo", "namespace": "default"},
            },
            {
                "role": "storage",
                "name": "prod-storage",
                "purpose": "stores artifacts",
            },
        ],
    }


def test_serialize_runtime_snapshot_contract() -> None:
    snapshot = RuntimeSnapshot(
        sdk_version="0.1.0",
        connection="remote Kitaru server",
        connection_target="https://example.com",
        config_directory="/tmp/kitaru-config",
        server_url="https://example.com",
        active_user="alice",
        project_override="demo-project",
        active_stack="prod",
        repository_root="/work/repo",
        server_version="0.42.0",
        server_database="postgres",
        server_deployment_type="kubernetes",
        local_server_status="not started",
        warning="Careful now",
        log_store_status="datadog",
        log_store_warning="not wired yet",
        environment=[
            ActiveEnvironmentVariable(
                name="KITARU_SERVER_URL", value="https://example.com"
            ),
            ActiveEnvironmentVariable(name="KITARU_PROJECT", value="demo-project"),
        ],
    )

    payload = serialize_runtime_snapshot(snapshot)

    assert payload == {
        "sdk_version": "0.1.0",
        "connection": "remote Kitaru server",
        "connection_target": "https://example.com",
        "config_directory": "/tmp/kitaru-config",
        "server_url": "https://example.com",
        "active_user": "alice",
        "project_override": "demo-project",
        "active_stack": "prod",
        "repository_root": "/work/repo",
        "server_version": "0.42.0",
        "server_database": "postgres",
        "server_deployment_type": "kubernetes",
        "local_server_status": "not started",
        "warning": "Careful now",
        "log_store_status": "datadog",
        "log_store_warning": "not wired yet",
        "environment": [
            {"name": "KITARU_SERVER_URL", "value": "https://example.com"},
            {"name": "KITARU_PROJECT", "value": "demo-project"},
        ],
        "kitaru_global_config_path": None,
        "zenml_global_config_path": None,
        "local_stores_path": None,
        "repository_config_path": None,
        "uses_repo_local_config": False,
        "connection_sources": None,
        "active_project": None,
        "active_stack_provenance": None,
        "active_project_provenance": None,
        "python_version": None,
        "system_info": None,
        "environment_type": None,
        "zenml_version": None,
        "packages": None,
    }


def test_serialize_runtime_snapshot_preserves_none_fields() -> None:
    payload = serialize_runtime_snapshot(
        RuntimeSnapshot(
            sdk_version="0.1.0",
            connection="local database",
            connection_target="local",
            config_directory="/tmp/kitaru-config",
        )
    )

    assert payload["server_url"] is None
    assert payload["active_user"] is None
    assert payload["warning"] is None
    assert payload["environment"] == []


def test_serialize_runtime_snapshot_hides_provenance_details_by_default() -> None:
    snapshot = RuntimeSnapshot(
        sdk_version="0.1.0",
        connection="local database",
        connection_target="local",
        config_directory="/tmp/kitaru-config",
        active_stack_provenance=ActiveConfigSelectionProvenance(
            resource="active_stack",
            effective_source="repo-local config",
            effective_source_detail="/work/repo/.kitaru/config.yaml",
            effective_id="repo-stack-id",
            resolved_id="resolved-stack-id",
        ),
        active_project_provenance=ActiveConfigSelectionProvenance(
            resource="active_project",
            effective_source="environment",
            effective_source_detail="KITARU_PROJECT -> ZENML_ACTIVE_PROJECT_ID",
            effective_id="production",
            resolved_id="project-uuid",
        ),
    )

    default_payload = serialize_runtime_snapshot(snapshot)
    detailed_payload = serialize_runtime_snapshot(
        snapshot,
        include_provenance_details=True,
    )

    assert default_payload["active_stack_provenance"] is None
    assert default_payload["active_project_provenance"] is None
    assert detailed_payload["active_stack_provenance"]["effective_id"] == (
        "repo-stack-id"
    )
    assert detailed_payload["active_project_provenance"]["effective_id"] == (
        "production"
    )


def _write_active_config(
    directory: Path,
    *,
    active_stack_id: str | None = None,
    active_project_id: str | None = None,
) -> Path:
    """Write a minimal raw ZenML/Kitaru active context config file."""
    directory.mkdir(parents=True, exist_ok=True)
    config_path = directory / "config.yaml"
    lines: list[str] = []
    if active_stack_id is not None:
        lines.append(f"active_stack_id: {active_stack_id}")
    if active_project_id is not None:
        lines.append(f"active_project_id: {active_project_id}")
    config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return config_path


def _fake_global_config(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        uses_local_store=False,
        store_configuration=SimpleNamespace(url="https://example.com"),
        config_directory=str(tmp_path / "global"),
    )


def _patch_snapshot_dependencies(
    fake_gc: SimpleNamespace,
    fake_client_cls: Any,
) -> tuple[Any, ...]:
    return (
        patch("kitaru._inspection_runtime.GlobalConfiguration", return_value=fake_gc),
        patch(
            "kitaru._inspection_runtime.connected_to_local_server_safe",
            return_value=False,
        ),
        patch(
            "kitaru._inspection_runtime.describe_local_server",
            return_value="not started",
        ),
        patch(
            "kitaru._inspection_runtime.resolve_installed_version", return_value="1.2.3"
        ),
        patch(
            "kitaru._inspection_runtime.list_active_kitaru_environment_variables",
            return_value=[],
        ),
        patch(
            "kitaru._inspection_runtime._collect_connection_sources", return_value={}
        ),
        patch(
            "kitaru._inspection_runtime._read_runtime_connection_config",
            return_value=SimpleNamespace(project=None),
        ),
        patch("kitaru._inspection_runtime.Client", fake_client_cls),
        patch("kitaru._config._active_context.Client", fake_client_cls),
        patch(
            "kitaru._inspection_runtime.resolve_log_store",
            return_value=ResolvedLogStore(
                backend="artifact-store",
                endpoint=None,
                api_key=None,
                source="default",
            ),
        ),
    )


def test_build_runtime_snapshot_collects_provenance_by_default(
    tmp_path: Path,
) -> None:
    fake_gc = _fake_global_config(tmp_path)
    fake_client_cls = MagicMock(side_effect=RuntimeError("store offline"))

    with ExitStack() as stack:
        for context_manager in _patch_snapshot_dependencies(fake_gc, fake_client_cls):
            stack.enter_context(context_manager)
        snapshot = build_runtime_snapshot()

    assert isinstance(snapshot.active_stack_provenance, ActiveConfigSelectionProvenance)
    assert isinstance(
        snapshot.active_project_provenance, ActiveConfigSelectionProvenance
    )


def test_build_runtime_snapshot_collects_active_context_source_precedence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("ZENML_ACTIVE_STACK_ID", "env-stack-id")
    monkeypatch.setenv("KITARU_PROJECT", "kitaru-project-id")
    monkeypatch.setenv("ZENML_ACTIVE_PROJECT_ID", "zenml-project-id")
    monkeypatch.setenv("KITARU_STACK", "execution-default-stack")

    repo_root = tmp_path / "repo"
    repo_config = _write_active_config(
        repo_root / ".kitaru",
        active_stack_id="repo-stack-id",
        active_project_id="repo-project-id",
    )
    fake_gc = _fake_global_config(tmp_path)
    global_config = _write_active_config(
        Path(fake_gc.config_directory),
        active_stack_id="global-stack-id",
        active_project_id="global-project-id",
    )
    fake_client = SimpleNamespace(
        active_user=SimpleNamespace(name="alice"),
        active_stack_model=SimpleNamespace(
            name="resolved-stack",
            id="resolved-stack-id",
        ),
        active_project=SimpleNamespace(
            name="resolved-project",
            id="resolved-project-id",
        ),
        root=repo_root,
        zen_store=SimpleNamespace(
            get_store_info=lambda: SimpleNamespace(
                version="0.42.0",
                database_type="postgres",
                deployment_type="kubernetes",
            )
        ),
    )
    fake_client_cls = MagicMock(return_value=fake_client)
    fake_client_cls.find_repository.return_value = repo_root

    with ExitStack() as stack:
        for context_manager in _patch_snapshot_dependencies(fake_gc, fake_client_cls):
            stack.enter_context(context_manager)
        snapshot = build_runtime_snapshot(include_provenance_details=True)

    stack_provenance = snapshot.active_stack_provenance
    project_provenance = snapshot.active_project_provenance
    assert isinstance(stack_provenance, ActiveConfigSelectionProvenance)
    assert isinstance(project_provenance, ActiveConfigSelectionProvenance)

    assert stack_provenance.effective_source == "environment"
    assert stack_provenance.effective_source_detail == "ZENML_ACTIVE_STACK_ID"
    assert stack_provenance.effective_id == "env-stack-id"
    assert stack_provenance.environment_id == "env-stack-id"
    assert stack_provenance.repository_id == "repo-stack-id"
    assert stack_provenance.global_id == "global-stack-id"
    assert stack_provenance.repository_config_path == str(repo_config)
    assert stack_provenance.global_config_path == str(global_config)
    assert stack_provenance.resolved_id == "resolved-stack-id"
    assert stack_provenance.resolved_name == "resolved-stack"
    assert any(
        "KITARU_STACK is an execution default" in note
        for note in stack_provenance.notes
    )

    assert project_provenance.effective_source == "environment"
    assert (
        project_provenance.effective_source_detail
        == "KITARU_PROJECT -> ZENML_ACTIVE_PROJECT_ID"
    )
    assert project_provenance.effective_id == "kitaru-project-id"
    assert project_provenance.environment_id == "kitaru-project-id"
    assert project_provenance.repository_id == "repo-project-id"
    assert project_provenance.global_id == "global-project-id"
    assert project_provenance.resolved_id == "resolved-project-id"
    assert project_provenance.resolved_name == "resolved-project"
    assert any(
        "Both KITARU_PROJECT and ZENML_ACTIVE_PROJECT_ID" in note
        for note in project_provenance.notes
    )


def test_build_runtime_snapshot_preserves_raw_provenance_when_client_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    for name in (
        "ZENML_ACTIVE_STACK_ID",
        "ZENML_ACTIVE_PROJECT_ID",
        "KITARU_PROJECT",
        "KITARU_STACK",
    ):
        monkeypatch.delenv(name, raising=False)

    repo_root = tmp_path / "repo"
    custom_repo_config_dir = repo_root / ".custom-kitaru"
    _write_active_config(
        custom_repo_config_dir,
        active_stack_id="stale-repo-stack-id",
        active_project_id="stale-repo-project-id",
    )
    fake_gc = _fake_global_config(tmp_path)
    _write_active_config(
        Path(fake_gc.config_directory),
        active_stack_id="global-stack-id",
        active_project_id="global-project-id",
    )
    fake_client_cls = MagicMock(side_effect=RuntimeError("store offline"))
    fake_client_cls.find_repository.return_value = repo_root

    with ExitStack() as stack:
        for context_manager in _patch_snapshot_dependencies(fake_gc, fake_client_cls):
            stack.enter_context(context_manager)
        stack.enter_context(
            patch(
                "kitaru._config._active_context.KITARU_REPOSITORY_DIRECTORY_NAME",
                ".custom-kitaru",
            )
        )
        snapshot = build_runtime_snapshot(include_provenance_details=True)

    stack_provenance = snapshot.active_stack_provenance
    project_provenance = snapshot.active_project_provenance
    assert isinstance(stack_provenance, ActiveConfigSelectionProvenance)
    assert isinstance(project_provenance, ActiveConfigSelectionProvenance)

    assert stack_provenance.effective_source == "repo-local config"
    assert stack_provenance.effective_id == "stale-repo-stack-id"
    assert stack_provenance.repository_id == "stale-repo-stack-id"
    assert stack_provenance.repository_config_path == str(
        custom_repo_config_dir / "config.yaml"
    )
    assert stack_provenance.global_id == "global-stack-id"
    assert stack_provenance.resolved_id is None
    assert stack_provenance.resolved_name is None
    assert any(
        "Client() failed (RuntimeError): store offline" in note
        for note in stack_provenance.notes
    )

    assert project_provenance.effective_source == "repo-local config"
    assert project_provenance.effective_id == "stale-repo-project-id"
    assert project_provenance.repository_id == "stale-repo-project-id"
    assert project_provenance.global_id == "global-project-id"
    assert project_provenance.resolved_id is None
    assert project_provenance.resolved_name is None
    assert any(
        "Client() failed (RuntimeError): store offline" in note
        for note in project_provenance.notes
    )

    assert snapshot.warning == (
        "Unable to query the configured store (RuntimeError): store offline"
    )
    assert fake_client_cls.mock_calls[:2] == [
        call.find_repository(enable_warnings=False),
        call(),
    ]


def test_build_runtime_snapshot_appends_legacy_warning_when_local_store_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KITARU_RUNNER", "legacy-runner")

    with (
        patch(
            "kitaru._inspection_runtime.GlobalConfiguration",
            return_value=_BrokenGlobalConfig(),
        ),
        patch(
            "kitaru._inspection_runtime.get_local_server",
            side_effect=ImportError("missing"),
        ),
        patch(
            "kitaru._inspection_runtime.resolve_installed_version", return_value="1.2.3"
        ),
        patch(
            "kitaru._inspection_runtime.list_active_kitaru_environment_variables",
            return_value=[],
        ),
    ):
        snapshot = build_runtime_snapshot()

    assert snapshot.connection == "local mode (unavailable)"
    assert snapshot.warning is not None
    assert "Local Kitaru runtime support is unavailable" in snapshot.warning
    assert "`KITARU_RUNNER` was renamed to `KITARU_STACK`" in snapshot.warning


def test_build_runtime_snapshot_appends_legacy_warning_for_stale_local_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KITARU_RUNNER", "legacy-runner")
    fake_gc = SimpleNamespace(
        uses_local_store=False,
        store_configuration=SimpleNamespace(url="http://127.0.0.1:8237"),
        config_directory="/tmp/fake-kitaru-config",
    )

    with (
        patch("kitaru._inspection_runtime.GlobalConfiguration", return_value=fake_gc),
        patch(
            "kitaru._inspection_runtime.connected_to_local_server_safe",
            return_value=False,
        ),
        patch(
            "kitaru._inspection_runtime.describe_local_server",
            return_value="registered but unavailable (daemon: stopped)",
        ),
        patch(
            "kitaru._inspection_runtime.get_local_server",
            return_value=SimpleNamespace(
                status=SimpleNamespace(url=None),
                config=SimpleNamespace(url=None, port=8237, ip_address="127.0.0.1"),
            ),
        ),
        patch(
            "kitaru._inspection_runtime.resolve_installed_version", return_value="1.2.3"
        ),
        patch(
            "kitaru._inspection_runtime.list_active_kitaru_environment_variables",
            return_value=[],
        ),
        patch(
            "kitaru._inspection_runtime.Client",
            side_effect=AssertionError("Client should not be queried"),
        ),
    ):
        snapshot = build_runtime_snapshot()

    assert snapshot.warning is not None
    assert "stopped local server" in snapshot.warning
    assert "`KITARU_RUNNER` was renamed to `KITARU_STACK`" in snapshot.warning


def test_registered_local_server_url_matches_localhost_aliases() -> None:
    local_server = SimpleNamespace(
        status=SimpleNamespace(url="http://127.0.0.1:8383"),
        config=SimpleNamespace(url="http://127.0.0.1:8383"),
    )

    with patch(
        "kitaru._inspection_runtime.get_local_server", return_value=local_server
    ):
        assert is_registered_local_server_url("http://localhost:8383") is True
        assert is_registered_local_server_url("http://127.0.0.1:8383") is True
        assert is_registered_local_server_url("http://localhost:8080") is False


def test_uses_stale_local_server_url_ignores_non_local_daemon_port() -> None:
    local_server = SimpleNamespace(
        status=SimpleNamespace(url="http://127.0.0.1:8383"),
        config=SimpleNamespace(url="http://127.0.0.1:8383"),
    )

    with patch(
        "kitaru._inspection_runtime.get_local_server", return_value=local_server
    ):
        assert (
            uses_stale_local_server_url(
                "http://localhost:8080",
                "registered but unavailable (daemon: stopped)",
            )
            is False
        )
        assert (
            uses_stale_local_server_url(
                "http://localhost:8383",
                "registered but unavailable (daemon: stopped)",
            )
            is True
        )


def test_build_runtime_snapshot_populates_log_store_mismatch_details() -> None:
    fake_gc = SimpleNamespace(
        uses_local_store=False,
        store_configuration=SimpleNamespace(url="https://example.com"),
        config_directory="/tmp/fake-kitaru-config",
    )
    fake_client = SimpleNamespace(
        active_user=SimpleNamespace(name="alice"),
        active_stack_model=SimpleNamespace(name="prod"),
        root=Path("/tmp/worktree"),
        zen_store=SimpleNamespace(
            get_store_info=lambda: SimpleNamespace(
                version="0.42.0",
                database_type="postgres",
                deployment_type="kubernetes",
            )
        ),
    )

    with (
        patch("kitaru._inspection_runtime.GlobalConfiguration", return_value=fake_gc),
        patch(
            "kitaru._inspection_runtime.connected_to_local_server_safe",
            return_value=False,
        ),
        patch(
            "kitaru._inspection_runtime.describe_local_server",
            return_value="not started",
        ),
        patch(
            "kitaru._inspection_runtime.resolve_installed_version", return_value="1.2.3"
        ),
        patch(
            "kitaru._inspection_runtime.list_active_kitaru_environment_variables",
            return_value=[],
        ),
        patch(
            "kitaru._inspection_runtime._read_runtime_connection_config",
            return_value=SimpleNamespace(project=None),
        ),
        patch("kitaru._inspection_runtime.Client", return_value=fake_client),
        patch(
            "kitaru._inspection_runtime.resolve_log_store",
            return_value=ResolvedLogStore(
                backend="datadog",
                endpoint="https://logs.example.com",
                api_key=None,
                source="environment",
            ),
        ),
        patch(
            "kitaru._inspection_runtime.active_stack_log_store",
            return_value=ActiveStackLogStore(
                backend="artifact-store",
                endpoint=None,
                stack_name="prod",
            ),
        ),
    ):
        snapshot = build_runtime_snapshot()

    assert (
        snapshot.log_store_status == "datadog (preferred) ⚠ stack uses artifact-store"
    )
    assert snapshot.log_store_warning is not None
    assert (
        "Active stack uses: artifact-store (stack: prod)" in snapshot.log_store_warning
    )
    assert "not wired into stack selection yet" in snapshot.log_store_warning


def test_build_runtime_snapshot_returns_early_when_log_store_resolution_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KITARU_RUNNER", "legacy-runner")
    fake_gc = SimpleNamespace(
        uses_local_store=False,
        store_configuration=SimpleNamespace(url="https://example.com"),
        config_directory="/tmp/fake-kitaru-config",
    )

    with (
        patch("kitaru._inspection_runtime.GlobalConfiguration", return_value=fake_gc),
        patch(
            "kitaru._inspection_runtime.connected_to_local_server_safe",
            return_value=False,
        ),
        patch(
            "kitaru._inspection_runtime.describe_local_server",
            return_value="not started",
        ),
        patch(
            "kitaru._inspection_runtime.resolve_installed_version", return_value="1.2.3"
        ),
        patch(
            "kitaru._inspection_runtime.list_active_kitaru_environment_variables",
            return_value=[],
        ),
        patch(
            "kitaru._inspection_runtime._read_runtime_connection_config",
            return_value=SimpleNamespace(project=None),
        ),
        patch(
            "kitaru._inspection_runtime.Client",
            side_effect=RuntimeError("store offline"),
        ),
        patch(
            "kitaru._inspection_runtime.resolve_log_store",
            side_effect=ValueError("bad config"),
        ),
    ):
        snapshot = build_runtime_snapshot()

    assert snapshot.warning == (
        "Unable to query the configured store (RuntimeError): store offline"
    )
    assert snapshot.log_store_warning == (
        "Unable to resolve Kitaru log-store preference: bad config"
    )
    assert "`KITARU_RUNNER` was renamed to `KITARU_STACK`" not in snapshot.warning


def test_serialize_log_entry_contract() -> None:
    entry = LogEntry(
        message="Starting flow",
        level="INFO",
        timestamp="2026-03-14T10:00:00+00:00",
        source="runner",
        checkpoint_name="research",
        module="kitaru.flow",
        filename="flow.py",
        lineno=42,
    )

    assert serialize_log_entry(entry) == {
        "message": "Starting flow",
        "level": "INFO",
        "timestamp": "2026-03-14T10:00:00+00:00",
        "source": "runner",
        "checkpoint_name": "research",
        "module": "kitaru.flow",
        "filename": "flow.py",
        "lineno": 42,
    }


def test_serialize_log_entry_omits_none_fields() -> None:
    assert serialize_log_entry(LogEntry(message="Only message")) == {
        "message": "Only message"
    }


def test_serialize_model_alias_contract() -> None:
    entry = ModelAliasEntry(
        alias="gpt-4o",
        model="openai/gpt-4o",
        secret="openai-key",
        is_default=True,
    )

    assert serialize_model_alias(entry) == {
        "alias": "gpt-4o",
        "model": "openai/gpt-4o",
        "secret": "openai-key",
        "is_default": True,
    }


def test_serialize_secret_summary_contract() -> None:
    secret = _sample_secret(private=False)
    payload = serialize_secret_summary(cast(Any, secret))

    assert payload["id"] == str(secret.id)
    assert payload["name"] == "openai-credentials"
    assert payload["visibility"] == "public"
    assert payload["keys"] == ["API_KEY", "REGION"]
    assert payload["has_missing_values"] is True


def test_serialize_secret_detail_contract() -> None:
    secret = _sample_secret(private=True)

    hidden_payload = serialize_secret_detail(cast(Any, secret), show_values=False)
    shown_payload = serialize_secret_detail(cast(Any, secret), show_values=True)

    assert hidden_payload["id"] == str(secret.id)
    assert hidden_payload["visibility"] == "private"
    assert hidden_payload["values"] is None
    assert shown_payload["id"] == str(secret.id)
    assert shown_payload["values"] == {
        "API_KEY": "secret-value",
        "REGION": "unavailable",
    }


def test_serialize_resolved_log_store_contract() -> None:
    payload = serialize_resolved_log_store(
        ResolvedLogStore(
            backend="datadog",
            endpoint="https://logs.example.com",
            api_key="secret-key",
            source="environment",
        ),
        active_store=ActiveStackLogStore(
            backend="artifact-store",
            endpoint=None,
            stack_name="prod",
        ),
        warning="stack backend differs",
    )

    assert payload == {
        "backend": "datadog",
        "endpoint": "https://logs.example.com",
        "api_key_configured": True,
        "source": "environment",
        "active_stack_backend": "artifact-store",
        "active_stack_name": "prod",
        "warning": "stack backend differs",
    }


# ---------------------------------------------------------------------------
# Degraded diagnostic snapshot tests
# ---------------------------------------------------------------------------


def test_build_runtime_snapshot_degrades_on_corrupt_global_config() -> None:
    """Corrupt GlobalConfiguration should produce a degraded snapshot."""
    with (
        patch(
            "kitaru._inspection_runtime.GlobalConfiguration",
            side_effect=RuntimeError("corrupt config"),
        ),
        patch(
            "kitaru._inspection_runtime.resolve_installed_version", return_value="0.5.0"
        ),
        patch(
            "kitaru._inspection_runtime.list_active_kitaru_environment_variables",
            return_value=[],
        ),
    ):
        snapshot = build_runtime_snapshot()

    assert snapshot.connection == "unavailable"
    assert snapshot.warning is not None
    assert "corrupt config" in snapshot.warning
    assert snapshot.sdk_version == "0.5.0"


def test_build_runtime_snapshot_degrades_on_store_config_error() -> None:
    """Non-ImportError from store_configuration should degrade gracefully."""
    from unittest.mock import PropertyMock

    fake_gc = MagicMock()
    fake_gc.config_directory = "/tmp/fake"
    type(fake_gc).store_configuration = PropertyMock(
        side_effect=ValueError("bad store config")
    )

    with (
        patch("kitaru._inspection_runtime.GlobalConfiguration", return_value=fake_gc),
        patch(
            "kitaru._inspection_runtime.resolve_installed_version", return_value="0.5.0"
        ),
        patch(
            "kitaru._inspection_runtime.list_active_kitaru_environment_variables",
            return_value=[],
        ),
        patch(
            "kitaru._inspection_runtime.describe_local_server",
            return_value="not started",
        ),
    ):
        snapshot = build_runtime_snapshot()

    assert snapshot.connection == "local mode (unavailable)"
    assert snapshot.warning is not None


def test_describe_local_server_handles_non_import_error() -> None:
    """Non-ImportError from get_local_server should degrade, not crash."""
    from kitaru.inspection import describe_local_server

    with patch(
        "kitaru._inspection_runtime.get_local_server",
        side_effect=RuntimeError("corrupt server metadata"),
    ):
        result = describe_local_server()

    assert "query failed" in result


def _clear_active_context_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "ZENML_ACTIVE_STACK_ID",
        "ZENML_ACTIVE_PROJECT_ID",
        "KITARU_PROJECT",
        "KITARU_STACK",
    ):
        monkeypatch.delenv(name, raising=False)


def test_repo_local_provenance_reads_kitaru_directory_not_zen(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Regression: provenance must read `.kitaru/config.yaml`, never `.zen/`."""
    _clear_active_context_env(monkeypatch)

    repo_root = tmp_path / "repo"
    _write_active_config(
        repo_root / ".zen",
        active_stack_id="zen-should-be-ignored",
        active_project_id="zen-should-be-ignored",
    )
    _write_active_config(
        repo_root / ".kitaru",
        active_stack_id="kitaru-wins",
        active_project_id="kitaru-wins",
    )
    fake_gc = _fake_global_config(tmp_path)
    _write_active_config(
        Path(fake_gc.config_directory),
        active_stack_id="global-stack-id",
    )
    fake_client_cls = MagicMock(side_effect=RuntimeError("store offline"))
    fake_client_cls.find_repository.return_value = repo_root

    with ExitStack() as stack:
        for context_manager in _patch_snapshot_dependencies(fake_gc, fake_client_cls):
            stack.enter_context(context_manager)
        snapshot = build_runtime_snapshot(include_provenance_details=True)

    stack_provenance = snapshot.active_stack_provenance
    assert isinstance(stack_provenance, ActiveConfigSelectionProvenance)
    assert stack_provenance.repository_id == "kitaru-wins"
    assert stack_provenance.repository_config_path is not None
    assert ".kitaru" in stack_provenance.repository_config_path
    assert ".zen" not in stack_provenance.repository_config_path


def test_build_runtime_snapshot_surfaces_provenance_collector_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When provenance collection itself fails, emit stub records so the user
    sees *why* — silently dropping the section defeats the point of `--all`.
    """
    _clear_active_context_env(monkeypatch)

    fake_gc = _fake_global_config(tmp_path)
    fake_client = SimpleNamespace(
        active_user=SimpleNamespace(name="alice"),
        active_stack_model=SimpleNamespace(name="resolved", id="resolved-id"),
        active_project=SimpleNamespace(name="resolved-project", id="resolved-id"),
        root=None,
        zen_store=SimpleNamespace(
            get_store_info=lambda: SimpleNamespace(
                version="0.42.0",
                database_type="sqlite",
                deployment_type="local",
            )
        ),
    )
    fake_client_cls = MagicMock(return_value=fake_client)

    with ExitStack() as stack:
        for context_manager in _patch_snapshot_dependencies(fake_gc, fake_client_cls):
            stack.enter_context(context_manager)
        stack.enter_context(
            patch(
                "kitaru._inspection_runtime.collect_active_context_provenance",
                side_effect=RuntimeError("yaml exploded"),
            )
        )
        snapshot = build_runtime_snapshot(include_provenance_details=True)

    stack_provenance = snapshot.active_stack_provenance
    project_provenance = snapshot.active_project_provenance
    assert isinstance(stack_provenance, ActiveConfigSelectionProvenance)
    assert isinstance(project_provenance, ActiveConfigSelectionProvenance)
    assert stack_provenance.effective_source == "unknown"
    assert project_provenance.effective_source == "unknown"
    assert stack_provenance.effective_id is None
    assert project_provenance.effective_id is None
    assert any(
        "Could not collect active context provenance (RuntimeError): yaml exploded"
        in note
        for note in stack_provenance.notes
    )
    assert any(
        "Could not collect active context provenance (RuntimeError): yaml exploded"
        in note
        for note in project_provenance.notes
    )


def test_build_runtime_snapshot_does_not_warn_for_env_name_to_uuid_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Explicit KITARU_PROJECT names may resolve to UUIDs without fallback."""
    _clear_active_context_env(monkeypatch)
    monkeypatch.setenv("KITARU_PROJECT", "production")

    fake_gc = _fake_global_config(tmp_path)
    fake_client = SimpleNamespace(
        active_user=SimpleNamespace(name="alice"),
        active_stack_model=SimpleNamespace(name="prod", id="stack-id"),
        active_project=SimpleNamespace(name="production", id="project-uuid"),
        root=None,
        zen_store=SimpleNamespace(
            get_store_info=lambda: SimpleNamespace(
                version="0.42.0",
                database_type="sqlite",
                deployment_type="local",
            )
        ),
    )
    fake_client_cls = MagicMock(return_value=fake_client)
    fake_client_cls.find_repository.return_value = None

    with ExitStack() as stack:
        for context_manager in _patch_snapshot_dependencies(fake_gc, fake_client_cls):
            stack.enter_context(context_manager)
        snapshot = build_runtime_snapshot(include_provenance_details=True)

    project_provenance = snapshot.active_project_provenance
    assert isinstance(project_provenance, ActiveConfigSelectionProvenance)
    assert project_provenance.effective_source == "environment"
    assert project_provenance.effective_id == "production"
    assert project_provenance.resolved_id == "project-uuid"
    assert snapshot.warning is None


def test_build_runtime_snapshot_preserves_raw_when_client_sanitizes_stale_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When Client() succeeds but returns a different ID than the raw config
    value, both must coexist — raw IDs preserved, resolved reflects whatever
    ZenML actually picked.
    """
    _clear_active_context_env(monkeypatch)

    repo_root = tmp_path / "repo"
    _write_active_config(
        repo_root / ".kitaru",
        active_stack_id="stale-repo-stack-id",
    )
    fake_gc = _fake_global_config(tmp_path)
    _write_active_config(
        Path(fake_gc.config_directory),
        active_stack_id="stale-global-stack-id",
    )
    fake_client = SimpleNamespace(
        active_user=SimpleNamespace(name="alice"),
        active_stack_model=SimpleNamespace(
            name="sanitized-stack",
            id="different-resolved-id",
        ),
        active_project=SimpleNamespace(
            name="some-project",
            id="project-id",
        ),
        root=repo_root,
        zen_store=SimpleNamespace(
            get_store_info=lambda: SimpleNamespace(
                version="0.42.0",
                database_type="sqlite",
                deployment_type="local",
            )
        ),
    )
    fake_client_cls = MagicMock(return_value=fake_client)
    fake_client_cls.find_repository.return_value = repo_root

    with ExitStack() as stack:
        for context_manager in _patch_snapshot_dependencies(fake_gc, fake_client_cls):
            stack.enter_context(context_manager)
        snapshot = build_runtime_snapshot(include_provenance_details=True)

    stack_provenance = snapshot.active_stack_provenance
    assert isinstance(stack_provenance, ActiveConfigSelectionProvenance)
    assert stack_provenance.repository_id == "stale-repo-stack-id"
    assert stack_provenance.global_id == "stale-global-stack-id"
    assert stack_provenance.effective_id == "stale-repo-stack-id"
    assert stack_provenance.resolved_id == "different-resolved-id"
    assert stack_provenance.resolved_name == "sanitized-stack"
    assert stack_provenance.resolved_id != stack_provenance.effective_id
    assert snapshot.warning is not None
    assert "saved active context changed" in snapshot.warning
    assert "stale-repo-stack-id" in snapshot.warning
    assert "sanitized-stack (different-resolved-id)" in snapshot.warning


def test_build_runtime_snapshot_preserves_raw_when_yaml_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Malformed repo-local YAML must not crash the snapshot build."""
    _clear_active_context_env(monkeypatch)

    repo_root = tmp_path / "repo"
    repo_config_dir = repo_root / ".kitaru"
    repo_config_dir.mkdir(parents=True)
    (repo_config_dir / "config.yaml").write_text(
        "active_stack_id: [unclosed-list\n",
        encoding="utf-8",
    )
    fake_gc = _fake_global_config(tmp_path)
    _write_active_config(
        Path(fake_gc.config_directory),
        active_stack_id="global-stack-id",
    )
    fake_client_cls = MagicMock(side_effect=RuntimeError("store offline"))
    fake_client_cls.find_repository.return_value = repo_root

    with ExitStack() as stack:
        for context_manager in _patch_snapshot_dependencies(fake_gc, fake_client_cls):
            stack.enter_context(context_manager)
        snapshot = build_runtime_snapshot(include_provenance_details=True)

    stack_provenance = snapshot.active_stack_provenance
    assert isinstance(stack_provenance, ActiveConfigSelectionProvenance)
    assert stack_provenance.global_id == "global-stack-id"
    assert any("Could not read config file" in note for note in stack_provenance.notes)


def test_build_runtime_snapshot_yaml_error_note_omits_body(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """YAML error notes must reference the file and exception type but not
    echo the config body — defense-in-depth even though config files hold
    IDs and URLs, not credentials.
    """
    _clear_active_context_env(monkeypatch)

    repo_root = tmp_path / "repo"
    repo_config_dir = repo_root / ".kitaru"
    repo_config_dir.mkdir(parents=True)
    secret_marker = "SUPER_SECRET_VALUE_SHOULD_NOT_APPEAR_IN_NOTE"
    (repo_config_dir / "config.yaml").write_text(
        f"active_stack_id: [{secret_marker}\n",
        encoding="utf-8",
    )
    fake_gc = _fake_global_config(tmp_path)
    fake_client_cls = MagicMock(side_effect=RuntimeError("store offline"))
    fake_client_cls.find_repository.return_value = repo_root

    with ExitStack() as stack:
        for context_manager in _patch_snapshot_dependencies(fake_gc, fake_client_cls):
            stack.enter_context(context_manager)
        snapshot = build_runtime_snapshot(include_provenance_details=True)

    stack_provenance = snapshot.active_stack_provenance
    assert isinstance(stack_provenance, ActiveConfigSelectionProvenance)
    read_notes = [
        note for note in stack_provenance.notes if "Could not read config file" in note
    ]
    assert read_notes, "Expected a 'Could not read config file' note"
    for note in read_notes:
        assert secret_marker not in note


def test_serialize_runtime_snapshot_does_not_leak_yaml_body(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A secret-looking value in a malformed config file must never appear in
    the serialized snapshot JSON.
    """
    import json

    _clear_active_context_env(monkeypatch)

    repo_root = tmp_path / "repo"
    repo_config_dir = repo_root / ".kitaru"
    repo_config_dir.mkdir(parents=True)
    secret_marker = "CREDENTIAL_XYZ_MUST_NOT_APPEAR_IN_JSON"
    (repo_config_dir / "config.yaml").write_text(
        f"active_stack_id: [{secret_marker}\n",
        encoding="utf-8",
    )
    fake_gc = _fake_global_config(tmp_path)
    fake_client_cls = MagicMock(side_effect=RuntimeError("store offline"))
    fake_client_cls.find_repository.return_value = repo_root

    with ExitStack() as stack:
        for context_manager in _patch_snapshot_dependencies(fake_gc, fake_client_cls):
            stack.enter_context(context_manager)
        snapshot = build_runtime_snapshot(include_provenance_details=True)

    serialized = json.dumps(serialize_runtime_snapshot(snapshot))
    assert secret_marker not in serialized


def test_build_runtime_snapshot_reports_global_source_when_only_global_set(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Only the global config has an ID → effective_source == 'global config'."""
    _clear_active_context_env(monkeypatch)

    fake_gc = _fake_global_config(tmp_path)
    global_config = _write_active_config(
        Path(fake_gc.config_directory),
        active_stack_id="global-only-id",
        active_project_id="global-only-project-id",
    )
    fake_client_cls = MagicMock(side_effect=RuntimeError("store offline"))
    fake_client_cls.find_repository.return_value = None

    with ExitStack() as stack:
        for context_manager in _patch_snapshot_dependencies(fake_gc, fake_client_cls):
            stack.enter_context(context_manager)
        snapshot = build_runtime_snapshot(include_provenance_details=True)

    stack_provenance = snapshot.active_stack_provenance
    project_provenance = snapshot.active_project_provenance
    assert isinstance(stack_provenance, ActiveConfigSelectionProvenance)
    assert isinstance(project_provenance, ActiveConfigSelectionProvenance)
    assert stack_provenance.effective_source == "global config"
    assert stack_provenance.effective_source_detail == str(global_config)
    assert stack_provenance.effective_id == "global-only-id"
    assert project_provenance.effective_source == "global config"
    assert project_provenance.effective_id == "global-only-project-id"


def test_build_runtime_snapshot_reports_unset_when_nothing_is_set(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """No env, no repo, no global → effective_source == 'unset'."""
    _clear_active_context_env(monkeypatch)

    fake_gc = _fake_global_config(tmp_path)
    Path(fake_gc.config_directory).mkdir(parents=True, exist_ok=True)
    fake_client_cls = MagicMock(side_effect=RuntimeError("store offline"))
    fake_client_cls.find_repository.return_value = None

    with ExitStack() as stack:
        for context_manager in _patch_snapshot_dependencies(fake_gc, fake_client_cls):
            stack.enter_context(context_manager)
        snapshot = build_runtime_snapshot(include_provenance_details=True)

    stack_provenance = snapshot.active_stack_provenance
    project_provenance = snapshot.active_project_provenance
    assert isinstance(stack_provenance, ActiveConfigSelectionProvenance)
    assert isinstance(project_provenance, ActiveConfigSelectionProvenance)
    assert stack_provenance.effective_source == "unset"
    assert stack_provenance.effective_source_detail is None
    assert stack_provenance.effective_id is None
    assert project_provenance.effective_source == "unset"
    assert project_provenance.effective_id is None
