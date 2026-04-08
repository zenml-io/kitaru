"""Tests for `kitaru.inspection` serialization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch
from uuid import uuid4

import pytest

from kitaru.client import (
    ArtifactRef,
    CheckpointAttempt,
    CheckpointCall,
    Execution,
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
    RuntimeSnapshot,
    build_runtime_snapshot,
    is_registered_local_server_url,
    serialize_artifact_ref,
    serialize_artifact_value,
    serialize_checkpoint_attempt,
    serialize_checkpoint_call,
    serialize_execution,
    serialize_execution_summary,
    serialize_failure,
    serialize_log_entry,
    serialize_memory_entry,
    serialize_memory_history,
    serialize_memory_value,
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
from kitaru.memory import MemoryEntry


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


def _sample_memory_entry(
    *,
    key: str = "prefs",
    value_type: str = "dict",
    version: int = 2,
    scope: str = "repo_scope",
    scope_type: str = "namespace",
    is_deleted: bool = False,
    execution_id: str | None = None,
    flow_id: str | None = None,
    flow_name: str | None = None,
) -> MemoryEntry:
    return MemoryEntry(
        key=key,
        value_type=value_type,
        version=version,
        scope=scope,
        scope_type=scope_type,
        created_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
        is_deleted=is_deleted,
        artifact_id="artifact-123",
        execution_id=(
            execution_id
            if execution_id is not None
            else ("exec-123" if scope_type != "namespace" else None)
        ),
        flow_id=flow_id,
        flow_name=flow_name,
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


def test_serialize_memory_entry_contract() -> None:
    assert serialize_memory_entry(_sample_memory_entry(scope_type="flow")) == {
        "key": "prefs",
        "value_type": "dict",
        "version": 2,
        "scope": "repo_scope",
        "scope_type": "flow",
        "created_at": "2026-04-01T12:00:00+00:00",
        "is_deleted": False,
        "artifact_id": "artifact-123",
        "execution_id": "exec-123",
        "flow_id": None,
        "flow_name": None,
    }


def test_serialize_memory_entry_includes_flow_context_when_present() -> None:
    payload = serialize_memory_entry(
        MemoryEntry(
            key="prefs",
            value_type="dict",
            version=2,
            scope="exec-123",
            scope_type="execution",
            created_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
            is_deleted=False,
            artifact_id="artifact-123",
            execution_id=None,
            flow_id="flow-456",
            flow_name="repo_memory_demo",
        )
    )

    assert payload["scope"] == "exec-123"
    assert payload["scope_type"] == "execution"
    assert payload["execution_id"] is None
    assert payload["flow_id"] == "flow-456"
    assert payload["flow_name"] == "repo_memory_demo"


def test_serialize_memory_history_contract() -> None:
    payload = serialize_memory_history(
        [
            _sample_memory_entry(version=2, is_deleted=True),
            _sample_memory_entry(version=1),
        ]
    )

    assert [entry["version"] for entry in payload] == [2, 1]
    assert [entry["is_deleted"] for entry in payload] == [True, False]


def test_serialize_memory_value_reuses_artifact_value_rules() -> None:
    json_payload = serialize_memory_value({"tags": {"beta", "alpha"}})
    repr_payload = serialize_memory_value(_Unjsonable())

    assert json_payload == {
        "value": {"tags": ["alpha", "beta"]},
        "value_format": "json",
        "value_type": "builtins.dict",
    }
    assert repr_payload == {
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
    }


def test_serialize_execution_summary_contract() -> None:
    payload = serialize_execution_summary(_sample_execution())

    assert payload == {
        "exec_id": "kr-123",
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
    }


def test_serialize_execution_contract() -> None:
    payload = serialize_execution(_sample_execution())

    assert set(payload) == {
        "exec_id",
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


def test_build_runtime_snapshot_appends_legacy_warning_when_local_store_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KITARU_RUNNER", "legacy-runner")

    with (
        patch(
            "kitaru.inspection.GlobalConfiguration",
            return_value=_BrokenGlobalConfig(),
        ),
        patch("kitaru.inspection.get_local_server", side_effect=ImportError("missing")),
        patch("kitaru.inspection.resolve_installed_version", return_value="1.2.3"),
        patch(
            "kitaru.inspection.list_active_kitaru_environment_variables",
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
        patch("kitaru.inspection.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.inspection.connected_to_local_server_safe", return_value=False),
        patch(
            "kitaru.inspection.describe_local_server",
            return_value="registered but unavailable (daemon: stopped)",
        ),
        patch(
            "kitaru.inspection.get_local_server",
            return_value=SimpleNamespace(
                status=SimpleNamespace(url=None),
                config=SimpleNamespace(url=None, port=8237, ip_address="127.0.0.1"),
            ),
        ),
        patch("kitaru.inspection.resolve_installed_version", return_value="1.2.3"),
        patch(
            "kitaru.inspection.list_active_kitaru_environment_variables",
            return_value=[],
        ),
        patch(
            "kitaru.inspection.Client",
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

    with patch("kitaru.inspection.get_local_server", return_value=local_server):
        assert is_registered_local_server_url("http://localhost:8383") is True
        assert is_registered_local_server_url("http://127.0.0.1:8383") is True
        assert is_registered_local_server_url("http://localhost:8080") is False


def test_uses_stale_local_server_url_ignores_non_local_daemon_port() -> None:
    local_server = SimpleNamespace(
        status=SimpleNamespace(url="http://127.0.0.1:8383"),
        config=SimpleNamespace(url="http://127.0.0.1:8383"),
    )

    with patch("kitaru.inspection.get_local_server", return_value=local_server):
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
        patch("kitaru.inspection.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.inspection.connected_to_local_server_safe", return_value=False),
        patch("kitaru.inspection.describe_local_server", return_value="not started"),
        patch("kitaru.inspection.resolve_installed_version", return_value="1.2.3"),
        patch(
            "kitaru.inspection.list_active_kitaru_environment_variables",
            return_value=[],
        ),
        patch(
            "kitaru.inspection._read_runtime_connection_config",
            return_value=SimpleNamespace(project=None),
        ),
        patch("kitaru.inspection.Client", return_value=fake_client),
        patch(
            "kitaru.inspection.resolve_log_store",
            return_value=ResolvedLogStore(
                backend="datadog",
                endpoint="https://logs.example.com",
                api_key=None,
                source="environment",
            ),
        ),
        patch(
            "kitaru.inspection.active_stack_log_store",
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
        patch("kitaru.inspection.GlobalConfiguration", return_value=fake_gc),
        patch("kitaru.inspection.connected_to_local_server_safe", return_value=False),
        patch("kitaru.inspection.describe_local_server", return_value="not started"),
        patch("kitaru.inspection.resolve_installed_version", return_value="1.2.3"),
        patch(
            "kitaru.inspection.list_active_kitaru_environment_variables",
            return_value=[],
        ),
        patch(
            "kitaru.inspection._read_runtime_connection_config",
            return_value=SimpleNamespace(project=None),
        ),
        patch(
            "kitaru.inspection.Client",
            side_effect=RuntimeError("store offline"),
        ),
        patch(
            "kitaru.inspection.resolve_log_store",
            side_effect=ValueError("bad config"),
        ),
    ):
        snapshot = build_runtime_snapshot()

    assert snapshot.warning == "Unable to query the configured store: store offline"
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
