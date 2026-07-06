from __future__ import annotations

import builtins
import importlib
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kitaru._client._models import ExecutionStatus
from remote_stack_smoke import (
    RemoteSmokeError,
    run_log_readback_with_timeout,
    run_remote_flow_check,
    validate_stack_show_payload,
)


class FakeHandle:
    def __init__(
        self,
        *,
        exec_id: str = "exec-remote-smoke",
        statuses: list[Any] | None = None,
        result: str = "",
    ) -> None:
        self._exec_id = exec_id
        self._statuses = list(statuses or [ExecutionStatus.COMPLETED])
        self._result = result

    @property
    def exec_id(self) -> str:
        return self._exec_id

    @property
    def status(self) -> Any:
        if len(self._statuses) > 1:
            return self._statuses.pop(0)
        return self._statuses[0]

    def get(self) -> str:
        return self._result


class FakeFlow:
    def __init__(
        self,
        *,
        statuses: list[Any] | None = None,
        result: str | None = None,
    ) -> None:
        self.statuses = statuses
        self.result = result
        self.submitted_marker: str | None = None
        self.run_kwargs: dict[str, Any] | None = None

    def run(self, marker: str, **kwargs: Any) -> FakeHandle:
        self.submitted_marker = marker
        self.run_kwargs = kwargs
        return FakeHandle(
            statuses=self.statuses,
            result=marker if self.result is None else self.result,
        )


class FakeExecutionsAPI:
    def __init__(
        self,
        *,
        status: Any = ExecutionStatus.COMPLETED,
        logs_error: Exception | None = None,
    ) -> None:
        self.status = status
        self.logs_error = logs_error

    def get(self, exec_id: str) -> SimpleNamespace:
        return SimpleNamespace(exec_id=exec_id, status=self.status)

    def logs(self, exec_id: str, *, source: str, limit: int) -> list[dict[str, str]]:
        if self.logs_error is not None:
            raise self.logs_error
        return [{"exec_id": exec_id, "source": source, "message": "marker logged"}][
            :limit
        ]


class FakeArtifactsAPI:
    def __init__(self, artifacts: builtins.list[Any] | None = None) -> None:
        self._artifacts = (
            artifacts
            if artifacts is not None
            else [SimpleNamespace(artifact_id="artifact-remote-smoke")]
        )
        self.get_calls: builtins.list[str] = []

    def list(self, exec_id: str, *, limit: int) -> builtins.list[Any]:
        return self._artifacts[:limit]

    def get(self, artifact_id: str) -> SimpleNamespace:
        self.get_calls.append(artifact_id)
        return SimpleNamespace(artifact_id=artifact_id)


class FakeClient:
    def __init__(
        self,
        *,
        execution_status: Any = ExecutionStatus.COMPLETED,
        artifacts: builtins.list[Any] | None = None,
        logs_error: Exception | None = None,
    ) -> None:
        self.executions = FakeExecutionsAPI(
            status=execution_status,
            logs_error=logs_error,
        )
        self.artifacts = FakeArtifactsAPI(artifacts)


def successful_log_command() -> list[str]:
    return [sys.executable, "-c", "import json; print(json.dumps({'count': 1}))"]


def failing_log_command() -> list[str]:
    return [
        sys.executable,
        "-c",
        "import sys; print('logs down', file=sys.stderr); sys.exit(1)",
    ]


def hanging_log_command() -> list[str]:
    return [sys.executable, "-c", "import time; time.sleep(30)"]


def _stack_payload(
    *,
    stack_type: str = "kubernetes",
    runner_backend: str = "kubernetes",
    storage_backend: str = "s3",
) -> dict[str, Any]:
    return {
        "command": "stack.show",
        "item": {
            "id": "stack-id-not-private",
            "name": "stack-name-not-used-as-evidence",
            "stack_type": stack_type,
            "components": [
                {"role": "runner", "name": "runner", "backend": runner_backend},
                {"role": "storage", "name": "storage", "backend": storage_backend},
            ],
        },
    }


def test_kubernetes_stack_shape_is_accepted() -> None:
    validation = validate_stack_show_payload(_stack_payload(), category="kubernetes")

    assert validation.valid is True
    assert validation.evidence == {
        "category": "kubernetes",
        "runner_backend": "kubernetes",
        "stack_type": "kubernetes",
        "storage_backend": "s3",
    }


def test_local_runner_remote_artifact_stack_shape_is_accepted() -> None:
    validation = validate_stack_show_payload(
        _stack_payload(
            stack_type="local", runner_backend="local", storage_backend="gcs"
        ),
        category="local-remote-artifact",
    )

    assert validation.valid is True
    assert validation.evidence["runner_backend"] == "local"
    assert validation.evidence["storage_backend"] == "gcs"


def test_local_runner_local_storage_is_rejected_for_remote_artifact_stack() -> None:
    validation = validate_stack_show_payload(
        _stack_payload(
            stack_type="local", runner_backend="local", storage_backend="local"
        ),
        category="local-remote-artifact",
    )

    assert validation.valid is False
    assert "remote storage backend" in validation.reason


def test_malformed_stack_json_fails_with_useful_reason() -> None:
    validation = validate_stack_show_payload(
        {"command": "stack.show"}, category="kubernetes"
    )

    assert validation.valid is False
    assert "missing object field 'item'" in validation.reason


def test_remote_flow_success_evidence_is_sanitized() -> None:
    flow = FakeFlow()
    client = FakeClient()

    evidence = run_remote_flow_check(
        stack="private-stack-name",
        category="kubernetes",
        image="private-registry.example/team/image:latest",
        execution_timeout=5,
        log_timeout=5,
        run_prefix="test-prefix",
        flow=flow,
        client_factory=lambda: client,
        log_command=successful_log_command(),
    )

    assert evidence == {
        "artifact_count": 1,
        "artifact_get_succeeded": True,
        "category": "kubernetes",
        "exec_id": "exec-remote-smoke",
        "flow_result_marker_matched": True,
        "log_entry_count": 1,
        "log_readback_succeeded": True,
        "status": "completed",
    }
    assert flow.run_kwargs == {
        "cache": False,
        "image": "private-registry.example/team/image:latest",
        "stack": "private-stack-name",
    }
    serialized = repr(evidence)
    assert "private-stack-name" not in serialized
    assert "private-registry.example" not in serialized


def test_remote_flow_failed_terminal_status_raises() -> None:
    flow = FakeFlow(statuses=[ExecutionStatus.FAILED])

    with pytest.raises(RemoteSmokeError, match="unsuccessful terminal status: failed"):
        run_remote_flow_check(
            stack="stack",
            category="kubernetes",
            image="image",
            execution_timeout=5,
            log_timeout=5,
            run_prefix="test",
            flow=flow,
            client_factory=FakeClient,
        )


def test_remote_flow_timeout_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    flow = FakeFlow(statuses=[ExecutionStatus.RUNNING])
    monkeypatch.setattr("remote_stack_smoke.time.sleep", lambda _: None)

    with pytest.raises(RemoteSmokeError, match="timed out"):
        run_remote_flow_check(
            stack="stack",
            category="kubernetes",
            image="image",
            execution_timeout=0,
            log_timeout=5,
            run_prefix="test",
            flow=flow,
            client_factory=FakeClient,
        )


def test_remote_flow_no_artifacts_raises() -> None:
    flow = FakeFlow()

    with pytest.raises(RemoteSmokeError, match="produced no artifacts"):
        run_remote_flow_check(
            stack="stack",
            category="kubernetes",
            image="image",
            execution_timeout=5,
            log_timeout=5,
            run_prefix="test",
            flow=flow,
            client_factory=lambda: FakeClient(artifacts=[]),
        )


def test_remote_flow_log_lookup_failure_raises() -> None:
    flow = FakeFlow()

    with pytest.raises(RemoteSmokeError, match="execution log lookup failed"):
        run_remote_flow_check(
            stack="stack",
            category="kubernetes",
            image="image",
            execution_timeout=5,
            log_timeout=5,
            run_prefix="test",
            flow=flow,
            client_factory=FakeClient,
            log_command=failing_log_command(),
        )


def test_remote_flow_log_lookup_timeout_kills_hanging_process() -> None:
    start = time.monotonic()

    with pytest.raises(RemoteSmokeError, match="log lookup exceeded"):
        run_log_readback_with_timeout(
            "exec-remote-smoke",
            log_timeout=1,
            command=hanging_log_command(),
        )

    assert time.monotonic() - start < 5


def test_remote_flow_marker_mismatch_raises() -> None:
    flow = FakeFlow(result="wrong-marker")

    with pytest.raises(RemoteSmokeError, match="marker did not match"):
        run_remote_flow_check(
            stack="stack",
            category="kubernetes",
            image="image",
            execution_timeout=5,
            log_timeout=5,
            run_prefix="test",
            flow=flow,
            client_factory=FakeClient,
        )


def test_private_release_smoke_flow_is_importable_by_module_path() -> None:
    module = importlib.import_module("kitaru._release_smoke.remote_stack_flow")

    assert callable(module.remote_stack_release_smoke.run)
    assert callable(module.build_remote_smoke_marker)


def test_env_provided_private_value_patterns_are_not_in_tracked_files() -> None:
    raw_patterns = os.environ.get("KITARU_REMOTE_SMOKE_PRIVATE_VALUE_PATTERNS", "")
    patterns = [
        pattern.strip()
        for chunk in raw_patterns.split(os.pathsep)
        for pattern in chunk.split(",")
        if pattern.strip()
    ]
    if not patterns:
        pytest.skip("set KITARU_REMOTE_SMOKE_PRIVATE_VALUE_PATTERNS for manual scan")

    repo_root = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        ["git", "ls-files"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    offenders: list[str] = []
    for relative_path in completed.stdout.splitlines():
        path = repo_root / relative_path
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for pattern in patterns:
            if pattern in text:
                offenders.append(relative_path)
                break

    assert offenders == []
